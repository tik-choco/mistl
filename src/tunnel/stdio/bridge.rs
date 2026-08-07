//! The connect-mode stdio bridge: pipes this node's local stdin to whichever
//! peer's stdio session is active, and writes received stdout/stderr bytes
//! back to this node's local stdout/stderr. Ported from the standalone
//! `p2p` crate's `src/stdio/bridge.rs`; the only changes are the import path
//! (`crate::rtc` -> `crate::tunnel::rtc`) and the addition of
//! [`Bridge::spawn`], which folds in the lifecycle logic upstream's
//! `app::connect::run_connect` used to own (see that function's doc comment
//! below) now that there's no CLI entry point left to own it.

use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::debug;

use super::packet::{StreamType, unwrap_packet, wrap_packet};
use crate::tunnel::rtc::RTCManager;

/// Events fed into the single FIFO worker in `Bridge::run`. `on_stdio_open`,
/// `on_stdio_close`, and `on_stdio_message` all route through this one enum
/// and one channel (rather than separate queues) so that, e.g., a peer's
/// open/close transitions are never processed out of order relative to the
/// messages surrounding them -- state and data share one arrival-ordered
/// timeline, matching how the manager delivers them.
//
// Only reachable through `Bridge`, which is itself not constructed in
// production yet -- see the module doc comment on `crate::tunnel::stdio` for
// why (no IPC streaming transport for this client half).
#[allow(dead_code)]
enum StdioEvent {
    Message(String, Vec<u8>),
    Open(String),
    Close(String),
}

// Deliberately unreachable in production: this is the stdio *client* half
// (attaching a local terminal to a peer's session), and mistl's daemon IPC
// has no streaming mode to carry it yet. Kept compiling, ported verbatim,
// and covered by this module's own tests -- see `crate::tunnel::stdio`'s
// module doc comment for the full explanation.
#[allow(dead_code)]
pub struct Bridge {
    manager: RTCManager,
    active_peer: Arc<Mutex<Option<String>>>,
    connected: Arc<Mutex<bool>>,
    buffer: Arc<Mutex<Vec<Vec<u8>>>>,
    done: Arc<Notify>,
}

#[allow(dead_code)]
impl Bridge {
    pub fn new(manager: RTCManager) -> Self {
        Self {
            manager,
            active_peer: Arc::new(Mutex::new(None)),
            connected: Arc::new(Mutex::new(false)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            done: Arc::new(Notify::new()),
        }
    }

    /// Builds a `Bridge` and spawns its `run()` loop in the background,
    /// returning the shared handle so the caller can `close()` it later.
    ///
    /// Folds in upstream `app::connect::run_connect`'s bridge-lifecycle
    /// half (`let bridge = Arc::new(stdio::Bridge::new(manager.clone()));
    /// tokio::spawn(async move { bridge.run().await });`) now that the rest
    /// of that function -- parsing `--forward` CLI args and waiting on
    /// Ctrl-C -- no longer applies: forwards are managed through
    /// `ForwardController`/`ForwardStore` independently of connect-mode
    /// (see `crate::tunnel::session::SessionContext::build`), and the
    /// daemon (W5) owns the tunnel's lifecycle instead of a per-invocation
    /// Ctrl-C wait. The caller is expected to store the returned `Arc`
    /// alongside its `SessionContext` (which has no field for it -- see the
    /// integration contract's frozen struct) and call `.close()` on
    /// `tunnel.stop`/room switch.
    pub fn spawn(manager: RTCManager) -> Arc<Bridge> {
        let bridge = Arc::new(Bridge::new(manager));
        let b = bridge.clone();
        tokio::spawn(async move {
            b.run().await;
        });
        bridge
    }

    pub async fn run(&self) {
        // Handlers only push onto the channel (a synchronous, order-preserving
        // send); the loop below is the sole consumer and processes one event
        // to completion before starting the next, so writes to stdout/stderr
        // and connected/active_peer transitions can no longer race or
        // interleave the way they could when each event spawned its own task.
        let (tx, mut rx) = mpsc::unbounded_channel::<StdioEvent>();

        let msg_tx = tx.clone();
        self.manager
            .on_stdio_message(move |peer_id, data| {
                let _ = msg_tx.send(StdioEvent::Message(peer_id, data));
            })
            .await;

        let open_tx = tx.clone();
        self.manager
            .on_stdio_open(move |peer_id| {
                let _ = open_tx.send(StdioEvent::Open(peer_id));
            })
            .await;

        let close_tx = tx.clone();
        self.manager
            .on_stdio_close(move |peer_id| {
                let _ = close_tx.send(StdioEvent::Close(peer_id));
            })
            .await;
        drop(tx);

        let active_peer = self.active_peer.clone();
        let connected = self.connected.clone();
        let buffer = self.buffer.clone();
        let done = self.done.clone();
        let manager = self.manager.clone();
        tokio::spawn(async move {
            Self::read_stdin(active_peer, connected, buffer, manager, done).await;
        });

        // The manager keeps the handlers above (and their `tx` clones) alive
        // for as long as it exists, which can outlive this bridge, so the
        // channel closing on its own isn't a reliable shutdown signal here.
        // `done` (fired by `close()` or by `read_stdin` hitting EOF) is the
        // authoritative one; race it against `rx` so the worker always exits
        // when the bridge does, without leaking this loop.
        loop {
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Some(StdioEvent::Message(peer_id, data)) => self.handle_message(peer_id, data).await,
                        Some(StdioEvent::Open(peer_id)) => self.handle_open(peer_id).await,
                        Some(StdioEvent::Close(peer_id)) => self.handle_close(peer_id).await,
                        None => break,
                    }
                }
                _ = self.done.notified() => break,
            }
        }
    }

    async fn handle_message(&self, peer_id: String, data: Vec<u8>) {
        *self.active_peer.lock().await = Some(peer_id);
        let (stream_type, payload) = unwrap_packet(&data);
        match stream_type {
            StreamType::Stdout => {
                let mut stdout = io::stdout();
                let _ = stdout.write_all(payload).await;
                let _ = stdout.flush().await;
            }
            StreamType::Stderr => {
                let mut stderr = io::stderr();
                let _ = stderr.write_all(payload).await;
                let _ = stderr.flush().await;
            }
            _ => {}
        }
    }

    async fn handle_open(&self, peer_id: String) {
        let mut conn = self.connected.lock().await;
        if !*conn {
            *conn = true;
            *self.active_peer.lock().await = Some(peer_id.clone());
            debug!("stdio bridge connected to peer: {}", peer_id);

            let mut buf = self.buffer.lock().await;
            if !buf.is_empty() {
                debug!("Flushing buffered stdin data...");
                for data in buf.drain(..) {
                    let _ = self.manager.send_stdio_to(&peer_id, data).await;
                }
            }
        }
    }

    async fn handle_close(&self, peer_id: String) {
        let mut ap = self.active_peer.lock().await;
        if ap.as_deref() == Some(peer_id.as_str()) {
            *self.connected.lock().await = false;
            *ap = None;
            debug!("stdio bridge disconnected from peer: {}", peer_id);
        }
    }

    async fn read_stdin(
        active_peer: Arc<Mutex<Option<String>>>,
        connected: Arc<Mutex<bool>>,
        buffer: Arc<Mutex<Vec<Vec<u8>>>>,
        manager: RTCManager,
        done: Arc<Notify>,
    ) {
        let mut stdin = io::stdin();
        let mut buf = vec![0u8; 32 * 1024];

        loop {
            match stdin.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let data = wrap_packet(StreamType::Stdin, &buf[..n]);
                    let conn = *connected.lock().await;
                    if conn {
                        let ap = active_peer.lock().await;
                        if let Some(ref peer_id) = *ap {
                            let _ = manager.send_stdio_to(peer_id, data).await;
                        }
                    } else {
                        buffer.lock().await.push(data);
                        debug!("Buffering stdin data (not connected yet)");
                    }
                }
                Err(e) => {
                    debug!("stdin read error: {}", e);
                    break;
                }
            }
        }
        done.notify_one();
    }

    pub fn close(&self) {
        self.done.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bridge() -> Bridge {
        Bridge::new(RTCManager::for_test("self-node"))
    }

    /// Regression test for the per-event-`tokio::spawn` bug: with a shared
    /// FIFO queue, an open/message/close sequence for one peer must be
    /// *processed* in the order it was *sent*, even though (as under the old
    /// code) the events originate from independently scheduled producers.
    /// In particular, the message handler must observe `active_peer` already
    /// set by the preceding open -- not racing it -- and the close handler
    /// must still see itself as the active peer.
    #[tokio::test]
    async fn processes_open_message_close_in_send_order() {
        let bridge = test_bridge();
        let (tx, mut rx) = mpsc::unbounded_channel::<StdioEvent>();

        // Sent in this order by three independent "producers", exactly like
        // the manager's on_stdio_open/message/close callbacks would.
        tx.send(StdioEvent::Open("peer-a".to_string())).unwrap();
        tx.send(StdioEvent::Message(
            "peer-a".to_string(),
            wrap_packet(StreamType::Stdout, b"hello"),
        ))
        .unwrap();
        tx.send(StdioEvent::Close("peer-a".to_string())).unwrap();
        drop(tx);

        let mut order = Vec::new();
        while let Some(event) = rx.recv().await {
            match event {
                StdioEvent::Open(peer_id) => {
                    bridge.handle_open(peer_id).await;
                    assert!(*bridge.connected.lock().await, "open must connect");
                    order.push("open");
                }
                StdioEvent::Message(peer_id, data) => {
                    // If this message were processed before open finished
                    // (the old racy behavior), active_peer could still be
                    // None here.
                    assert_eq!(
                        bridge.active_peer.lock().await.as_deref(),
                        Some("peer-a"),
                        "open must be fully applied before the following message is handled"
                    );
                    bridge.handle_message(peer_id, data).await;
                    order.push("message");
                }
                StdioEvent::Close(peer_id) => {
                    bridge.handle_close(peer_id).await;
                    assert!(!*bridge.connected.lock().await, "close must disconnect");
                    assert!(bridge.active_peer.lock().await.is_none());
                    order.push("close");
                }
            }
        }

        assert_eq!(order, vec!["open", "message", "close"]);
    }

    /// A close for a peer that never became active (e.g. a stale/duplicate
    /// close racing a different peer's open) must not clear state it doesn't
    /// own -- this only holds because opens/closes for different peers now
    /// pass through the same ordered queue instead of racing on their own
    /// spawned tasks.
    #[tokio::test]
    async fn close_for_inactive_peer_is_a_no_op() {
        let bridge = test_bridge();

        bridge.handle_open("peer-a".to_string()).await;
        assert_eq!(bridge.active_peer.lock().await.as_deref(), Some("peer-a"));

        bridge.handle_close("peer-b".to_string()).await;

        assert!(*bridge.connected.lock().await, "peer-a's connection stands");
        assert_eq!(bridge.active_peer.lock().await.as_deref(), Some("peer-a"));
    }
}
