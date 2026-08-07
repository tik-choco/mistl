use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, error, warn};

use crate::tunnel::auth::AuthRequest;
use crate::tunnel::forward_runtime::ForwardPeerRuntime;
use crate::tunnel::rtc::TunnelMessage;

use super::{
    ConnState, MSG_TYPE_CLOSE, MSG_TYPE_CONNECT, MSG_TYPE_DATA, MSG_TYPE_PING,
    PENDING_DATA_BUFFER_MAX_BYTES, PENDING_DATA_BUFFER_MAX_MSGS, TcpManager, log_tcp_io_error,
};

impl TcpManager {
    pub async fn on_tunnel_message(&self, peer_id: &str, data: &[u8]) {
        if data.is_empty() || data[0] != b'{' {
            return;
        }
        let tm: TunnelMessage = match serde_json::from_slice(data) {
            Ok(m) => m,
            Err(_) => return,
        };
        match tm.msg_type.as_str() {
            MSG_TYPE_CONNECT => self.handle_remote_connect(peer_id, &tm).await,
            MSG_TYPE_DATA => self.handle_remote_data(&tm).await,
            MSG_TYPE_CLOSE => self.close_conn(&tm.conn_id, false).await,
            // Keepalive: purely to keep the channel/NAT mapping warm, no
            // action needed on receipt.
            MSG_TYPE_PING => {}
            // Unknown/unrecognized types (older/newer peer versions) are
            // ignored for cross-version compat.
            _ => {}
        }
    }

    /// Handles an inbound `connect`. Deliberately does *not* await
    /// authorization inline (that used to be able to block forever on a
    /// human answering a TUI prompt, stalling this forward's single
    /// message-processing loop -- and with it every peer's connect/data/
    /// close traffic for this target -- Finding (A)): it tracks a `Pending`
    /// placeholder synchronously and hands authorization off to a spawned
    /// task, so this returns immediately either way.
    async fn handle_remote_connect(&self, peer_id: &str, tm: &TunnelMessage) {
        // Finding (B): `connect` is sent via `send_to_with_retry` and can be
        // delivered twice (a retry whose original send in fact succeeded).
        // A conn_id already tracked -- whether still `Pending` or already
        // `Active` -- means this is a duplicate delivery: ignore it rather
        // than re-authorizing, opening a second `TcpStream`, or clobbering
        // the existing entry.
        if self.conns.read().await.contains_key(&tm.conn_id) {
            debug!(
                "ignoring duplicate tunnel connect for already-tracked conn {} from {}",
                tm.conn_id, peer_id
            );
            return;
        }

        self.track_pending_conn(&tm.conn_id, peer_id).await;

        let mgr = Arc::new(self.clone_inner());
        let conn_id = tm.conn_id.clone();
        let pid = peer_id.to_string();
        tokio::spawn(async move { mgr.authorize_and_activate(conn_id, pid).await });
    }

    /// Resolves authorization for a `Pending` conn (spawned by
    /// `handle_remote_connect`) and, if allowed, opens the backend
    /// `TcpStream` and promotes the conn to `Active`. Runs independently of
    /// -- and concurrently with -- this forward's message-processing loop,
    /// so `data`/`close` for `conn_id` are handled the whole time this is in
    /// flight (see `TcpManager::handle_remote_data` and `ConnState::Pending`
    /// for how `data` is buffered until this resolves).
    async fn authorize_and_activate(self: Arc<Self>, conn_id: String, peer_id: String) {
        let req = AuthRequest {
            peer_id: peer_id.clone(),
            forward_key: self.target.clone(),
            target_addr: self.remote_addr.clone(),
            proto: "tcp".to_string(),
        };
        let decision = self.authorizer.authorize(&req).await;

        // The whole forward may have been torn down while authorization was
        // in flight (e.g. the user removed it) -- don't touch sockets/state
        // that shutdown may already be unwinding.
        if self.runtime.is_cancelled() {
            return;
        }

        // The conn may already be gone by the time authorization resolves:
        // closed by the peer, or denied outright after its pending buffer
        // overflowed. Either way there's nothing left to promote.
        if !self.conns.read().await.contains_key(&conn_id) {
            debug!(
                "conn {} no longer tracked once authorization resolved for {}; dropping decision",
                conn_id, peer_id
            );
            return;
        }

        if !decision.is_allowed() {
            debug!(
                "denied tcp tunnel connection from {} to {}",
                peer_id, self.target
            );
            self.close_conn(&conn_id, true).await;
            return;
        }

        let addr = &self.remote_addr;
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                if self.runtime.is_cancelled() {
                    return;
                }
                let (read_half, write_half) = stream.into_split();
                if !self.promote_pending_conn(&conn_id, write_half).await {
                    // Denied/closed/overflowed while the backend TcpStream
                    // was connecting -- let the fresh stream drop rather
                    // than leak it onto an already-gone conn.
                    return;
                }
                let mgr = self.clone();
                let cid = conn_id.clone();
                let pid = peer_id.clone();
                tokio::spawn(async move { mgr.forward_tcp_to_dc(cid, pid, read_half).await });
            }
            Err(e) => {
                error!("failed to connect to remote ({}): {}", addr, e);
                self.close_conn(&conn_id, true).await;
            }
        }
    }

    async fn handle_remote_data(&self, tm: &TunnelMessage) {
        // Look the conn up and run every message (even one with an
        // empty/missing payload) through `recv_seq.observe` *before* any
        // early return on payload emptiness below. A zero-byte `data`
        // message still consumes its seq number; skipping `observe` for it
        // would leave `next_expected` behind, so the *next* (non-empty)
        // message would be misjudged as a gap and the conn would be torn
        // down for no reason.
        let tc = {
            let conns = self.conns.read().await;
            conns.get(&tm.conn_id).cloned()
        };
        let Some(tc) = tc else {
            return;
        };

        // While the conn is still `Pending` (authorization in flight -- see
        // `authorize_and_activate`), there's no backend `TcpStream` to write
        // to yet: buffer the payload instead. `recv_seq.observe` still runs
        // right here, in arrival order, so gap/duplicate detection is
        // identical to the `Active` case -- only the actual write is
        // deferred, to `promote_pending_conn`'s replay.
        let outcome = {
            let mut tc = tc.write().await;
            let decision = tc.recv_seq.observe(tm.seq);
            match decision {
                SeqDecision::Duplicate { received } => DataOutcome::Duplicate { received },
                SeqDecision::Gap { expected, received } => DataOutcome::Gap { expected, received },
                SeqDecision::Inconsistent => DataOutcome::Inconsistent,
                SeqDecision::Accept => match &mut tc.state {
                    ConnState::Active { writer } => DataOutcome::Write {
                        writer: writer.clone(),
                        metrics: tc.metrics.clone(),
                    },
                    ConnState::Pending {
                        buffered,
                        buffered_bytes,
                    } => {
                        let payload = tm.payload.clone().unwrap_or_default();
                        if buffered.len() >= PENDING_DATA_BUFFER_MAX_MSGS
                            || *buffered_bytes + payload.len() > PENDING_DATA_BUFFER_MAX_BYTES
                        {
                            DataOutcome::PendingBufferOverflow
                        } else {
                            *buffered_bytes += payload.len();
                            buffered.push(payload);
                            DataOutcome::Buffered
                        }
                    }
                },
            }
        };

        match outcome {
            DataOutcome::Duplicate { received } => {
                debug!(
                    "dropping duplicate tunnel data for conn {} (seq {})",
                    tm.conn_id, received
                );
            }
            DataOutcome::Gap { expected, received } => {
                warn!(
                    "tunnel data gap for conn {}: expected seq {} but got {}; \
                     closing conn to avoid corrupting the downstream stream",
                    tm.conn_id, expected, received
                );
                self.close_conn(&tm.conn_id, true).await;
            }
            DataOutcome::Inconsistent => {
                warn!(
                    "tunnel data for conn {} switched from sequenced to unsequenced \
                     mid-stream; closing conn",
                    tm.conn_id
                );
                self.close_conn(&tm.conn_id, true).await;
            }
            DataOutcome::Buffered => {}
            DataOutcome::PendingBufferOverflow => {
                warn!(
                    "pending-authorization data buffer for conn {} exceeded {} messages / {} \
                     bytes while authorization was still in flight; denying and closing",
                    tm.conn_id, PENDING_DATA_BUFFER_MAX_MSGS, PENDING_DATA_BUFFER_MAX_BYTES
                );
                self.close_conn(&tm.conn_id, true).await;
            }
            DataOutcome::Write { writer, metrics } => {
                let payload = match &tm.payload {
                    Some(p) if !p.is_empty() => p,
                    _ => return,
                };
                let mut writer = writer.lock().await;
                if let Err(e) = writer.write_all(payload).await {
                    log_tcp_io_error("failed to write to tcp", &e);
                    drop(writer);
                    self.close_conn(&tm.conn_id, true).await;
                } else {
                    metrics.record_bytes_out(payload.len());
                }
            }
        }
    }
}

/// What to do next for one inbound `data` message, decided while holding the
/// conn's lock (so the decision sees a consistent `recv_seq`/`state`) and
/// then acted on after releasing it -- same lock-then-release split
/// `handle_remote_data` already used for [`SeqDecision`], extended to also
/// cover which `ConnState` the conn was in.
enum DataOutcome {
    /// Already-seen seq (see [`SeqDecision::Duplicate`]): drop it.
    Duplicate { received: u64 },
    /// A seq was skipped (see [`SeqDecision::Gap`]): close the conn.
    Gap { expected: u64, received: u64 },
    /// Sequenced/unsequenced mid-stream switch (see
    /// [`SeqDecision::Inconsistent`]): close the conn.
    Inconsistent,
    /// The conn is `Active`: write straight through, same as before
    /// `ConnState::Pending` existed.
    Write {
        writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        metrics: ForwardPeerRuntime,
    },
    /// The conn is `Pending` and the payload was appended to its buffer.
    Buffered,
    /// The conn is `Pending` and buffering this payload would exceed
    /// `PENDING_DATA_BUFFER_MAX_MSGS`/`_BYTES`: deny and close the conn.
    PendingBufferOverflow,
}

/// Per-conn tracking of the receive-side `data` sequence number, used to
/// detect the gaps and duplicates described on [`TunnelMessage::seq`].
///
/// Kept as a small, tokio-free struct so the decision logic can be unit
/// tested directly without spinning up a [`TcpManager`]/real sockets.
#[derive(Debug)]
pub struct SeqState {
    /// Sequence number expected for the next `data` message, starting at 1.
    next_expected: u64,
    /// Whether we've ever observed a sequenced (`Some`) message on this
    /// conn. Used to flag a peer that switches from sequenced to
    /// unsequenced mid-stream (e.g. a bug, or two mismatched builds talking
    /// to each other in an unexpected way) as [`SeqDecision::Inconsistent`]
    /// rather than silently trusting the unsequenced message.
    seq_seen: bool,
}

/// Outcome of [`SeqState::observe`] for one inbound `data` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqDecision {
    /// No gap/duplicate detected (or the sender doesn't use seq numbers at
    /// all yet): write the payload through as usual.
    Accept,
    /// Already-seen seq: a retried send that in fact arrived the first time
    /// (see `send_to_with_retry`). Drop the payload without writing it.
    Duplicate { received: u64 },
    /// A seq was skipped -- mistlib's `ReorderBuffer` most likely dropped a
    /// message delayed more than its reorder window. The conn's byte stream
    /// is now unrecoverably out of sync and must be torn down.
    Gap { expected: u64, received: u64 },
    /// This conn previously received sequenced messages but just received
    /// an unsequenced one (or vice versa isn't representable here -- see
    /// `observe`). Treated conservatively as unsafe to continue.
    Inconsistent,
}

impl SeqState {
    pub fn new() -> Self {
        Self {
            next_expected: 1,
            seq_seen: false,
        }
    }

    /// Validates `seq` (the `data` message's `TunnelMessage::seq`) against
    /// this conn's expected next sequence number, updating internal state
    /// as a side effect for `Accept`/`Duplicate` decisions that involve a
    /// sequenced message.
    fn observe(&mut self, seq: Option<u64>) -> SeqDecision {
        match seq {
            // Legacy sender (predates this field): fall back to unordered,
            // best-effort delivery exactly as before -- unless this conn has
            // already proven the peer *does* send seq numbers, in which case
            // a sudden unsequenced message indicates something is wrong.
            None => {
                if self.seq_seen {
                    SeqDecision::Inconsistent
                } else {
                    SeqDecision::Accept
                }
            }
            Some(received) => {
                self.seq_seen = true;
                if received < self.next_expected {
                    SeqDecision::Duplicate { received }
                } else if received == self.next_expected {
                    self.next_expected += 1;
                    SeqDecision::Accept
                } else {
                    SeqDecision::Gap {
                        expected: self.next_expected,
                        received,
                    }
                }
            }
        }
    }
}

impl Default for SeqState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod seq_state_tests {
    use super::{SeqDecision, SeqState};

    #[test]
    fn in_order_sequence_is_accepted() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(2)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(3)), SeqDecision::Accept);
    }

    #[test]
    fn duplicate_seq_is_flagged_and_does_not_advance_expectation() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(2)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(2)), SeqDecision::Duplicate { received: 2 });
        // The tracker should still expect 3 next, unaffected by the dup.
        assert_eq!(s.observe(Some(3)), SeqDecision::Accept);
    }

    #[test]
    fn gap_in_sequence_is_flagged() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(
            s.observe(Some(3)),
            SeqDecision::Gap {
                expected: 2,
                received: 3
            }
        );
    }

    #[test]
    fn unsequenced_legacy_sender_is_always_accepted() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(None), SeqDecision::Accept);
        assert_eq!(s.observe(None), SeqDecision::Accept);
        assert_eq!(s.observe(None), SeqDecision::Accept);
    }

    #[test]
    fn switching_from_sequenced_to_unsequenced_is_inconsistent() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(s.observe(None), SeqDecision::Inconsistent);
    }

    #[test]
    fn first_expected_seq_is_one() {
        let mut s = SeqState::new();
        assert_eq!(
            s.observe(Some(2)),
            SeqDecision::Gap {
                expected: 1,
                received: 2
            }
        );
    }

    #[test]
    fn switching_from_unsequenced_to_sequenced_is_accepted_and_enables_gap_detection() {
        let mut s = SeqState::new();
        // A legacy (unsequenced) message arrives first -- accepted as
        // before, and must not itself count as "seq 1".
        assert_eq!(s.observe(None), SeqDecision::Accept);
        // The peer then starts sending seq numbers, starting at 1: also
        // accepted, since this is the first sequenced message seen.
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        // Gap detection is now live: a skipped seq 2 is caught.
        assert_eq!(
            s.observe(Some(3)),
            SeqDecision::Gap {
                expected: 2,
                received: 3
            }
        );
    }
}
