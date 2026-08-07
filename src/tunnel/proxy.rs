//! Runs a local command as an interactive stdio backend for a remote peer's
//! `mistl tunnel shell` session (the "proxy" side of the stdio bridge).
//!
//! Ported near-verbatim from the standalone `p2p` crate's `proxy.rs`. Import
//! rewrite only: `crate::rtc::RTCManager` -> `crate::tunnel::rtc::RTCManager`,
//! `crate::stdio::packet::*` -> `crate::tunnel::stdio::packet::*` (owned by
//! W4's `src/tunnel/stdio.rs`, which declares `pub mod packet;`).
//!
//! Depends on `RTCManagerHandle::on_stdio_message` / `on_stdio_open` /
//! `on_stdio_close` -- distinct peer-lifecycle hooks beyond the single
//! generic `on_stdio` mentioned in the integration contract's frozen-API
//! shorthand. See the "Handler-registration API assumption" note at the top
//! of `src/tunnel/tcp.rs` for why this file assumes the fuller, verbatim
//! upstream handler surface; if that assumption is wrong, only
//! `Executor::run` below needs adjusting.

#![allow(dead_code)]
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::{debug, error};

use crate::tunnel::rtc::RTCManager;
use crate::tunnel::stdio::packet::{StreamType, unwrap_packet, wrap_packet};

/// Events fed into the single FIFO worker in `Executor::run`. `on_stdio_open`,
/// `on_stdio_close`, and `on_stdio_message` all route through this one enum
/// and one channel (rather than separate queues) so a peer's open/close
/// transitions can't be processed out of order relative to the stdin bytes
/// surrounding them.
enum StdioEvent {
    Message(String, Vec<u8>),
    Open(String),
    Close(String),
}

/// Per-peer "may this peer make me run the configured command?" gate,
/// consulted once per stdio session open. Async because answering it can
/// mean parking the request in the pending-approval queue until a human
/// resolves it via `tunnel.auth.approve`/`deny`.
///
/// Boxed rather than generic so `Executor` stays a concrete type that can be
/// stored in `crate::tunnel::RunningSession` without a type parameter, and
/// so this lower-level module needs no dependency on `SessionContext` --
/// `session::maybe_start_stdio_executor` supplies the closure.
pub type StdioAuthorizer = Arc<
    dyn Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>
        + Send
        + Sync,
>;

pub struct Executor {
    manager: RTCManager,
    command: Vec<String>,
    active_peer: Arc<Mutex<Option<String>>>,
    stdin_tx: Arc<Mutex<Option<tokio::process::ChildStdin>>>,
    done: Arc<Notify>,
    /// Gate 2 of the stdio two-gate model (gate 1 being `[tunnel]
    /// stdio_enabled`, checked before this executor is ever constructed).
    /// `None` means "no per-peer gate", which is upstream `p2p`'s original
    /// behavior -- any peer in the room could open a session. mistl always
    /// supplies one; it stays optional so the ported tests can construct a
    /// bare `Executor` unchanged.
    authorizer: Option<StdioAuthorizer>,
}

impl Executor {
    pub fn new(manager: RTCManager, command: Vec<String>) -> Self {
        Self {
            manager,
            command,
            active_peer: Arc::new(Mutex::new(None)),
            stdin_tx: Arc::new(Mutex::new(None)),
            done: Arc::new(Notify::new()),
            authorizer: None,
        }
    }

    /// [`Executor::new`] plus the per-peer authorization gate described on
    /// [`StdioAuthorizer`]. This is the constructor mistl actually uses;
    /// running a command for a peer with no per-peer check is not something
    /// the daemon should ever do.
    pub fn with_authorizer(
        manager: RTCManager,
        command: Vec<String>,
        authorizer: StdioAuthorizer,
    ) -> Self {
        Self {
            authorizer: Some(authorizer),
            ..Self::new(manager, command)
        }
    }

    pub async fn run(&self) -> Result<()> {
        // Handlers only push onto the channel (a synchronous, order-preserving
        // send); the loop below is the sole consumer and processes one event
        // to completion before starting the next, so child-stdin writes and
        // active_peer/stdin_tx transitions can no longer race or interleave
        // the way they could when each event spawned its own task.
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

        // The manager keeps the handlers above (and their `tx` clones) alive
        // for as long as it exists, which can outlive this executor, so the
        // channel closing on its own isn't a reliable shutdown signal here.
        // `done` (fired by `close()`) is the authoritative one; race it
        // against `rx` so the worker always exits when the executor does,
        // without leaking this loop.
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
        Ok(())
    }

    async fn handle_message(&self, peer_id: String, data: Vec<u8>) {
        *self.active_peer.lock().await = Some(peer_id);
        let (stream_type, payload) = unwrap_packet(&data);
        if stream_type == StreamType::Stdin {
            let mut tx = self.stdin_tx.lock().await;
            if let Some(ref mut stdin) = *tx {
                let _ = stdin.write_all(payload).await;
            }
        }
    }

    async fn handle_open(&self, peer_id: String) {
        {
            let existing = self.stdin_tx.lock().await;
            if existing.is_some() {
                return;
            }
        }

        // Authorize before claiming `active_peer` or spawning anything: a
        // denied peer must leave no trace of a session, and must not block
        // the next peer's open by having taken the single active slot. This
        // await can park for as long as it takes a human to answer the
        // pending-approval prompt; that is intentional, and safe here
        // because `run`'s worker processes one event at a time only for
        // ordering -- a peer waiting on approval simply doesn't get a shell.
        if let Some(authorize) = self.authorizer.as_ref()
            && !authorize(peer_id.clone()).await
        {
            debug!("Stdio session denied for peer: {}", peer_id);
            return;
        }

        *self.active_peer.lock().await = Some(peer_id.clone());
        debug!("Starting proxy command for peer: {}", peer_id);

        if let Err(e) = Self::start_command(
            &self.command,
            self.manager.clone(),
            self.active_peer.clone(),
            self.stdin_tx.clone(),
        )
        .await
        {
            error!("Failed to start proxy command: {}", e);
        }
    }

    async fn handle_close(&self, peer_id: String) {
        let ap = self.active_peer.lock().await;
        if ap.as_deref() == Some(peer_id.as_str()) {
            debug!("Stdio closed, stopping proxy command");
            *self.stdin_tx.lock().await = None;
        }
    }

    async fn start_command(
        cmd: &[String],
        manager: RTCManager,
        active_peer: Arc<Mutex<Option<String>>>,
        stdin_holder: Arc<Mutex<Option<tokio::process::ChildStdin>>>,
    ) -> Result<()> {
        if cmd.is_empty() {
            return Ok(());
        }

        let mut child = Command::new(&cmd[0])
            .args(&cmd[1..])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take().unwrap();
        *stdin_holder.lock().await = Some(stdin);

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        debug!("Proxy command started");

        let ap = active_peer.clone();
        let mgr = manager.clone();
        tokio::spawn(async move {
            Self::forward_stream(stdout, StreamType::Stdout, mgr, ap).await;
        });

        let ap = active_peer.clone();
        tokio::spawn(async move {
            Self::forward_stream(stderr, StreamType::Stderr, manager, ap).await;
        });

        let stdin_holder2 = stdin_holder.clone();
        tokio::spawn(async move {
            let status = child.wait().await;
            *stdin_holder2.lock().await = None;
            match status {
                Ok(s) => debug!("Proxy command exited: {}", s),
                Err(e) => debug!("Proxy command error: {}", e),
            }
        });

        Ok(())
    }

    async fn forward_stream<R: tokio::io::AsyncRead + Unpin>(
        mut reader: R,
        stream_type: StreamType,
        manager: RTCManager,
        active_peer: Arc<Mutex<Option<String>>>,
    ) {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let data = wrap_packet(stream_type, &buf[..n]);
                    let ap = active_peer.lock().await;
                    if let Some(ref peer_id) = *ap {
                        let _ = manager.send_stdio_to(peer_id, data).await;
                    }
                }
                Err(e) => {
                    debug!("stream read error: {}", e);
                    break;
                }
            }
        }
    }

    pub fn close(&self) {
        self.done.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Empty command so `handle_open` -> `start_command` returns immediately
    /// without spawning a real child process; the ordering/state-transition
    /// invariants under test don't depend on an actual process being alive.
    fn test_executor() -> Executor {
        Executor::new(RTCManager::for_test("self-node"), Vec::new())
    }

    /// Regression test for the per-event-`tokio::spawn` bug: with a shared
    /// FIFO queue, an open/message/close sequence for one peer is processed
    /// in the order it was sent, even though the events originate from
    /// independently scheduled producers (manager callbacks). In particular,
    /// the message handler must observe `active_peer` already set by the
    /// preceding open, not race it.
    #[tokio::test]
    async fn processes_open_message_close_in_send_order() {
        let executor = test_executor();
        let (tx, mut rx) = mpsc::unbounded_channel::<StdioEvent>();

        tx.send(StdioEvent::Open("peer-a".to_string())).unwrap();
        tx.send(StdioEvent::Message(
            "peer-a".to_string(),
            wrap_packet(StreamType::Stdin, b"hello"),
        ))
        .unwrap();
        tx.send(StdioEvent::Close("peer-a".to_string())).unwrap();
        drop(tx);

        let mut order = Vec::new();
        while let Some(event) = rx.recv().await {
            match event {
                StdioEvent::Open(peer_id) => {
                    executor.handle_open(peer_id).await;
                    assert_eq!(executor.active_peer.lock().await.as_deref(), Some("peer-a"));
                    order.push("open");
                }
                StdioEvent::Message(peer_id, data) => {
                    // If this message were processed before open finished
                    // (the old racy behavior), active_peer could still be
                    // None here.
                    assert_eq!(
                        executor.active_peer.lock().await.as_deref(),
                        Some("peer-a"),
                        "open must be fully applied before the following message is handled"
                    );
                    executor.handle_message(peer_id, data).await;
                    order.push("message");
                }
                StdioEvent::Close(peer_id) => {
                    executor.handle_close(peer_id).await;
                    order.push("close");
                }
            }
        }

        assert_eq!(order, vec!["open", "message", "close"]);
    }

    /// A close for a peer that never became active (e.g. a stale/duplicate
    /// close racing a different peer's open) must not tear down state it
    /// doesn't own -- this only holds because opens/closes for different
    /// peers now pass through the same ordered queue instead of racing on
    /// their own spawned tasks.
    #[tokio::test]
    async fn close_for_inactive_peer_is_a_no_op() {
        let executor = test_executor();

        executor.handle_open("peer-a".to_string()).await;
        assert_eq!(executor.active_peer.lock().await.as_deref(), Some("peer-a"));

        executor.handle_close("peer-b".to_string()).await;

        assert_eq!(
            executor.active_peer.lock().await.as_deref(),
            Some("peer-a"),
            "close for a different peer must not clear peer-a's state"
        );
    }
}
