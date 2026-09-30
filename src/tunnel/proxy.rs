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
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tracing::{debug, error};

use crate::tunnel::rtc::RTCManager;
use crate::tunnel::stdio::packet::{StreamType, unwrap_packet, wrap_packet};

/// How many peers may have a stdio open awaiting authorization at once, and
/// how much stdin one of them may queue while it waits. Keeps an unauthorized
/// room peer from pinning unbounded memory behind a human-gated prompt.
const MAX_PENDING_OPENS: usize = 8;
const MAX_PENDING_STDIN_BYTES: usize = 64 * 1024;

/// Events fed into the single FIFO worker in `Executor::run`. `on_stdio_open`,
/// `on_stdio_close`, and `on_stdio_message` all route through this one enum
/// and one channel (rather than separate queues) so a peer's open/close
/// transitions can't be processed out of order relative to the stdin bytes
/// surrounding them. `Authorized` carries an off-loop authorization result
/// back into the same ordered timeline.
enum StdioEvent {
    Message(String, Vec<u8>),
    Open(String),
    Close(String),
    Authorized(String, bool),
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

/// A peer's stdio open that is still waiting on authorization, with the stdin
/// it sent in the meantime (bounded by `MAX_PENDING_STDIN_BYTES`).
#[derive(Default)]
struct PendingOpen {
    queued: Vec<Vec<u8>>,
    queued_bytes: usize,
}

pub struct Executor {
    manager: RTCManager,
    command: Vec<String>,
    /// The peer that owns the running session. Set only by `activate` (after
    /// authorization succeeded in `handle_open`), and cleared when that peer
    /// closes or the child exits -- never assigned from an inbound message,
    /// so an unauthorized room peer cannot hijack an authorized shell.
    active_peer: Arc<Mutex<Option<String>>>,
    stdin_tx: Arc<Mutex<Option<tokio::process::ChildStdin>>>,
    /// Fires the running child's kill switch (see `start_command`).
    kill_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    /// Bumped per spawned child so a late exit-cleanup from an old child
    /// can't clear a newer session's state.
    generation: Arc<AtomicU64>,
    /// Opens whose authorization is in flight, keyed by peer.
    pending_opens: Arc<Mutex<HashMap<String, PendingOpen>>>,
    events_tx: mpsc::UnboundedSender<StdioEvent>,
    events_rx: Mutex<Option<mpsc::UnboundedReceiver<StdioEvent>>>,
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
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        Self {
            manager,
            command,
            active_peer: Arc::new(Mutex::new(None)),
            stdin_tx: Arc::new(Mutex::new(None)),
            kill_tx: Arc::new(Mutex::new(None)),
            generation: Arc::new(AtomicU64::new(0)),
            pending_opens: Arc::new(Mutex::new(HashMap::new())),
            events_tx,
            events_rx: Mutex::new(Some(events_rx)),
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
        let Some(mut rx) = self.events_rx.lock().await.take() else {
            return Ok(());
        };

        let msg_tx = self.events_tx.clone();
        self.manager
            .on_stdio_message(move |peer_id, data| {
                let _ = msg_tx.send(StdioEvent::Message(peer_id, data));
            })
            .await;

        let open_tx = self.events_tx.clone();
        self.manager
            .on_stdio_open(move |peer_id| {
                let _ = open_tx.send(StdioEvent::Open(peer_id));
            })
            .await;

        let close_tx = self.events_tx.clone();
        self.manager
            .on_stdio_close(move |peer_id| {
                let _ = close_tx.send(StdioEvent::Close(peer_id));
            })
            .await;

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
                        Some(event) => self.dispatch(event).await,
                        None => break,
                    }
                }
                _ = self.done.notified() => break,
            }
        }
        Ok(())
    }

    async fn dispatch(&self, event: StdioEvent) {
        match event {
            StdioEvent::Message(peer_id, data) => self.handle_message(peer_id, data).await,
            StdioEvent::Open(peer_id) => self.handle_open(peer_id).await,
            StdioEvent::Close(peer_id) => self.handle_close(peer_id).await,
            StdioEvent::Authorized(peer_id, allowed) => {
                self.handle_authorized(peer_id, allowed).await
            }
        }
    }

    /// Only the session peer's stdin is honored. A peer still awaiting
    /// authorization has its (bounded) stdin queued; anything else is
    /// dropped. This never touches `active_peer`.
    async fn handle_message(&self, peer_id: String, data: Vec<u8>) {
        let is_session_peer = self.active_peer.lock().await.as_deref() == Some(peer_id.as_str());
        let (stream_type, payload) = unwrap_packet(&data);
        if !is_session_peer {
            let mut pending = self.pending_opens.lock().await;
            if let Some(entry) = pending.get_mut(&peer_id) {
                if stream_type == StreamType::Stdin
                    && entry.queued_bytes + payload.len() <= MAX_PENDING_STDIN_BYTES
                {
                    entry.queued_bytes += payload.len();
                    entry.queued.push(payload.to_vec());
                }
            } else {
                debug!("dropping stdio data from non-session peer: {}", peer_id);
            }
            return;
        }
        if stream_type == StreamType::Stdin {
            let mut tx = self.stdin_tx.lock().await;
            if let Some(ref mut stdin) = *tx {
                let _ = stdin.write_all(payload).await;
            }
        }
    }

    async fn handle_open(&self, peer_id: String) {
        if self.active_peer.lock().await.is_some() || self.stdin_tx.lock().await.is_some() {
            return;
        }

        let Some(authorize) = self.authorizer.as_ref() else {
            self.activate(peer_id).await;
            return;
        };

        {
            let mut pending = self.pending_opens.lock().await;
            if pending.contains_key(&peer_id) {
                return;
            }
            if pending.len() >= MAX_PENDING_OPENS {
                debug!("too many pending stdio opens; ignoring {}", peer_id);
                return;
            }
            pending.insert(peer_id.clone(), PendingOpen::default());
        }

        // Authorize off the worker loop: answering can park for as long as it
        // takes a human to resolve the pending-approval prompt, and the
        // worker must keep serving other peers' events meanwhile. The result
        // returns through the same ordered channel (`Authorized`).
        let fut = authorize(peer_id.clone());
        let tx = self.events_tx.clone();
        tokio::spawn(async move {
            let allowed = fut.await;
            let _ = tx.send(StdioEvent::Authorized(peer_id, allowed));
        });
    }

    async fn handle_authorized(&self, peer_id: String, allowed: bool) {
        // The open may have been withdrawn (peer closed/left) while the
        // decision was pending; then there is nothing to grant.
        let Some(entry) = self.pending_opens.lock().await.remove(&peer_id) else {
            return;
        };
        if !allowed {
            debug!("Stdio session denied for peer: {}", peer_id);
            return;
        }
        if self.active_peer.lock().await.is_some() || self.stdin_tx.lock().await.is_some() {
            return;
        }
        self.activate(peer_id).await;
        let mut tx = self.stdin_tx.lock().await;
        if let Some(ref mut stdin) = *tx {
            for chunk in entry.queued {
                let _ = stdin.write_all(&chunk).await;
            }
        }
    }

    /// Claims the session for `peer_id` (already authorized) and spawns the
    /// command. The only place `active_peer` is ever assigned.
    async fn activate(&self, peer_id: String) {
        *self.active_peer.lock().await = Some(peer_id.clone());
        debug!("Starting proxy command for peer: {}", peer_id);

        if let Err(e) = Self::start_command(
            &self.command,
            self.manager.clone(),
            peer_id,
            self.active_peer.clone(),
            self.stdin_tx.clone(),
            self.kill_tx.clone(),
            self.generation.clone(),
        )
        .await
        {
            error!("Failed to start proxy command: {}", e);
            *self.active_peer.lock().await = None;
        }
    }

    async fn handle_close(&self, peer_id: String) {
        // A close for a peer awaiting authorization withdraws its open.
        self.pending_opens.lock().await.remove(&peer_id);

        let mut ap = self.active_peer.lock().await;
        if ap.as_deref() == Some(peer_id.as_str()) {
            debug!("Stdio closed, stopping proxy command");
            *ap = None;
            drop(ap);
            *self.stdin_tx.lock().await = None;
            if let Some(kill) = self.kill_tx.lock().await.take() {
                let _ = kill.send(());
            }
        }
    }

    async fn start_command(
        cmd: &[String],
        manager: RTCManager,
        peer_id: String,
        active_peer: Arc<Mutex<Option<String>>>,
        stdin_holder: Arc<Mutex<Option<tokio::process::ChildStdin>>>,
        kill_holder: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        generation: Arc<AtomicU64>,
    ) -> Result<()> {
        if cmd.is_empty() {
            return Ok(());
        }

        let mut command = Command::new(&cmd[0]);
        command
            .args(&cmd[1..])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        crate::child_process::prepare(&mut command);
        let mut child = command.spawn()?;
        let process_tree = crate::child_process::ProcessTree::attach(&mut child)?;

        let stdin = child.stdin.take().unwrap();
        *stdin_holder.lock().await = Some(stdin);
        let (kill_tx, kill_rx) = oneshot::channel();
        *kill_holder.lock().await = Some(kill_tx);
        let gen_id = generation.fetch_add(1, Ordering::SeqCst) + 1;

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        debug!("Proxy command started");

        let pid = peer_id.clone();
        let mgr = manager.clone();
        tokio::spawn(async move {
            Self::forward_stream(stdout, StreamType::Stdout, mgr, pid).await;
        });

        tokio::spawn(async move {
            Self::forward_stream(stderr, StreamType::Stderr, manager, peer_id).await;
        });

        let stdin_holder2 = stdin_holder.clone();
        tokio::spawn(async move {
            let _process_tree = process_tree;
            let status = tokio::select! {
                status = child.wait() => status,
                _ = kill_rx => {
                    let _ = child.kill().await;
                    child.wait().await
                }
            };
            // Only clear session state if no newer child has taken over.
            if generation.load(Ordering::SeqCst) == gen_id {
                *stdin_holder2.lock().await = None;
                *active_peer.lock().await = None;
            }
            match status {
                Ok(s) => debug!("Proxy command exited: {}", s),
                Err(e) => debug!("Proxy command error: {}", e),
            }
        });

        Ok(())
    }

    /// Streams the child's output to the peer that owns the session it was
    /// spawned for -- fixed at spawn time, never re-read from shared state.
    async fn forward_stream<R: tokio::io::AsyncRead + Unpin>(
        mut reader: R,
        stream_type: StreamType,
        manager: RTCManager,
        peer_id: String,
    ) {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let data = wrap_packet(stream_type, &buf[..n]);
                    let _ = manager.send_stdio_to(&peer_id, data).await;
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

    fn fixed_authorizer(allow: bool) -> StdioAuthorizer {
        Arc::new(move |_peer| Box::pin(async move { allow }))
    }

    /// Feeds the next queued internal event (an `Authorized` result) through
    /// `dispatch`, the way `run`'s loop would.
    async fn pump_one(executor: &Executor) {
        let mut guard = executor.events_rx.lock().await;
        let event = guard.as_mut().unwrap().recv().await.unwrap();
        drop(guard);
        executor.dispatch(event).await;
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

    /// T-01: a message from a peer that never opened (or was never
    /// authorized) must not become the session peer.
    #[tokio::test]
    async fn message_from_other_peer_does_not_hijack_session() {
        let executor = test_executor();
        executor.handle_open("peer-a".to_string()).await;

        executor
            .handle_message(
                "peer-b".to_string(),
                wrap_packet(StreamType::Stdin, b"id\n"),
            )
            .await;
        assert_eq!(executor.active_peer.lock().await.as_deref(), Some("peer-a"));

        // With no session at all, a message must not create one either.
        let executor = test_executor();
        executor
            .handle_message(
                "peer-b".to_string(),
                wrap_packet(StreamType::Stdin, b"id\n"),
            )
            .await;
        assert_eq!(executor.active_peer.lock().await.as_deref(), None);
    }

    /// T-01: a denied open leaves no session, and the denied peer's later
    /// messages cannot claim one.
    #[tokio::test]
    async fn denied_open_leaves_no_session_and_message_cannot_claim_it() {
        let mut executor = test_executor();
        executor.authorizer = Some(fixed_authorizer(false));
        executor.handle_open("peer-a".to_string()).await;
        pump_one(&executor).await;
        executor
            .handle_message("peer-a".to_string(), wrap_packet(StreamType::Stdin, b"x"))
            .await;
        assert_eq!(executor.active_peer.lock().await.as_deref(), None);
        assert!(executor.pending_opens.lock().await.is_empty());
    }

    /// T-07: authorization is off the worker loop, so `handle_open` returns
    /// before the decision and the session is only claimed once it arrives.
    #[tokio::test]
    async fn authorization_does_not_block_the_worker() {
        let mut executor = test_executor();
        executor.authorizer = Some(Arc::new(|_peer| {
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                true
            })
        }));
        executor.handle_open("peer-a".to_string()).await;
        assert_eq!(executor.active_peer.lock().await.as_deref(), None);
        assert!(executor.pending_opens.lock().await.contains_key("peer-a"));

        pump_one(&executor).await;
        assert_eq!(executor.active_peer.lock().await.as_deref(), Some("peer-a"));
    }

    /// T-01: a close from the session peer terminates the child and clears
    /// the session.
    #[tokio::test]
    async fn close_from_session_peer_kills_child() {
        #[cfg(windows)]
        let command = vec![
            "cmd".to_string(),
            "/c".to_string(),
            "ping -n 60 127.0.0.1 >nul".to_string(),
        ];
        #[cfg(not(windows))]
        let command = vec!["sleep".to_string(), "60".to_string()];
        let executor = Executor::new(RTCManager::for_test("self-node"), command);

        executor.handle_open("peer-a".to_string()).await;
        assert!(executor.stdin_tx.lock().await.is_some());
        assert!(executor.kill_tx.lock().await.is_some());

        executor.handle_close("peer-a".to_string()).await;
        assert_eq!(executor.active_peer.lock().await.as_deref(), None);
        assert!(executor.stdin_tx.lock().await.is_none());
        assert!(
            executor.kill_tx.lock().await.is_none(),
            "kill switch must have been fired"
        );
    }
}
