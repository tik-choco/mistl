//! Coordinates the forward-negotiation handshake between the network
//! handlers (which only push data, from `crate::tunnel::rtc`) and the
//! session/TUI/dashboard loop (which performs async side-effects like
//! showing an approval prompt or spawning a listener). Ported verbatim from
//! `p2p/src/negotiation.rs` -- no `crate::X` imports to rewrite.
//!
//! [`IncomingForward`] and [`OutgoingForward`] gain a `Serialize` derive
//! (not present upstream) so `crate::tunnel::session::Snapshot::to_json`
//! (owned by W4) can embed them directly into the `tunnel.status` IPC
//! payload without a hand-written conversion; both are plain structs of
//! strings/integers so the derive is a no-risk addition. Default field
//! naming is kept (`serde`'s automatic camelCase-free snake_case passthrough
//! matches the Rust field names already), so:
//!
//! `IncomingForward` -> `{"id":0,"req_id":"..","peer_id":"..","proto":"..","remote_addr":"..","target":".."}`
//! `OutgoingForward` -> `{"req_id":"..","sent_at_ms":0,"peer_id":"..","proto":"..","listen_port":0,"local_addr":"..","remote_addr":"..","target":".."}`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use serde::Serialize;
use tokio::sync::Mutex;

/// Caps on unanswered incoming proposals: per peer, in total, and how long one
/// may sit unanswered before it is swept. Without them any room peer can grow
/// the pending pane (and daemon memory) without bound.
pub const MAX_INCOMING_PER_PEER: usize = 32;
pub const MAX_INCOMING_TOTAL: usize = 128;
pub const INCOMING_TTL: Duration = Duration::from_secs(5 * 60);
/// How long we wait for a peer to answer a proposal we sent. Matches the
/// peer's [`INCOMING_TTL`] (after which it forgets the proposal and can no
/// longer answer it), so a proposal nobody will ever answer does not sit in
/// the UI as "waiting" forever.
pub const OUTGOING_TTL: Duration = Duration::from_secs(5 * 60);
/// `ForwardOutcome::reason` values the negotiator itself produces (the
/// session maps them to notice codes).
pub const REASON_TIMEOUT: &str = "no answer from the peer (timed out)";
pub const REASON_PEER_LEFT: &str = "peer disconnected";
/// Cap on our own unanswered proposals (they are user-driven, so this only
/// guards against a runaway script).
pub const MAX_OUTGOING: usize = 64;

/// An incoming forward proposal from a peer, awaiting a local approve/deny in
/// the TUI pending pane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IncomingForward {
    /// Local, monotonically increasing id used for display and resolution.
    pub id: u64,
    /// Protocol-level request id echoed back in the response.
    pub req_id: String,
    pub peer_id: String,
    pub proto: String,
    /// Address the peer wants to reach on this node (ip:port).
    pub remote_addr: String,
    pub target: String,
}

/// State the requester remembers between sending a `ForwardRequest` and
/// receiving the matching `ForwardResponse`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct OutgoingForward {
    /// Protocol-level request id; filled in by [`ForwardNegotiator::record_outgoing`]
    /// (what `tunnel.forward.propose.cancel` identifies a proposal by).
    pub req_id: String,
    /// Wall-clock send time (ms since the epoch), filled in by
    /// [`ForwardNegotiator::record_outgoing`]; lets the UI show how long we
    /// have been waiting.
    pub sent_at_ms: u64,
    pub peer_id: String,
    pub proto: String,
    pub listen_port: i32,
    pub local_addr: String,
    pub remote_addr: String,
    pub target: String,
}

/// The requester-side outcome of a forward proposal, drained by the session
/// loop.
#[derive(Debug, Clone)]
pub struct ForwardOutcome {
    pub outgoing: OutgoingForward,
    pub accepted: bool,
    /// Set when `accepted` is `false` and the outcome wasn't a genuine
    /// peer-sent response (e.g. the peer disconnected before answering), so
    /// the UI can show something more specific than "peer denied". `None`
    /// for a real `ForwardResponse` from the peer.
    pub reason: Option<String>,
}

/// Coordinates the forward-negotiation handshake between the network handlers
/// (which only push data) and the TUI loop (which performs async side-effects).
#[derive(Debug, Clone, Default)]
pub struct ForwardNegotiator {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    next_id: u64,
    incoming: BTreeMap<u64, IncomingForward>,
    incoming_at: BTreeMap<u64, Instant>,
    outgoing: BTreeMap<String, OutgoingForward>,
    outgoing_at: BTreeMap<String, Instant>,
    outcomes: Vec<ForwardOutcome>,
}

impl ForwardNegotiator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an incoming proposal and returns its local id. A repeat of an
    /// already-pending `(peer, req_id)` returns the existing id; a proposal
    /// over the per-peer or total cap is dropped (`None`). Proposals older than
    /// [`INCOMING_TTL`] are swept first.
    pub async fn record_incoming(
        &self,
        req_id: String,
        peer_id: String,
        proto: String,
        remote_addr: String,
        target: String,
    ) -> Option<u64> {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let expired: Vec<u64> = inner
            .incoming_at
            .iter()
            .filter(|(_, at)| now.duration_since(**at) > INCOMING_TTL)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            inner.incoming.remove(&id);
            inner.incoming_at.remove(&id);
        }
        if let Some(existing) = inner
            .incoming
            .values()
            .find(|f| f.peer_id == peer_id && f.req_id == req_id)
        {
            return Some(existing.id);
        }
        let from_peer = inner
            .incoming
            .values()
            .filter(|f| f.peer_id == peer_id)
            .count();
        if from_peer >= MAX_INCOMING_PER_PEER || inner.incoming.len() >= MAX_INCOMING_TOTAL {
            tracing::warn!(
                "dropping forward request from {}: too many pending",
                peer_id
            );
            return None;
        }
        inner.next_id += 1;
        let id = inner.next_id;
        inner.incoming_at.insert(id, now);
        inner.incoming.insert(
            id,
            IncomingForward {
                id,
                req_id,
                peer_id,
                proto,
                remote_addr,
                target,
            },
        );
        Some(id)
    }

    pub async fn list_incoming(&self) -> Vec<IncomingForward> {
        self.inner.lock().await.incoming.values().cloned().collect()
    }

    /// Removes and returns the incoming proposal with the given local id.
    pub async fn take_incoming(&self, id: u64) -> Option<IncomingForward> {
        let mut inner = self.inner.lock().await;
        inner.incoming_at.remove(&id);
        inner.incoming.remove(&id)
    }

    /// Remembers a request the local node just sent, keyed by protocol req id.
    /// Returns `false` (and records nothing) when [`MAX_OUTGOING`] unanswered
    /// proposals are already pending.
    pub async fn record_outgoing(&self, req_id: String, mut outgoing: OutgoingForward) -> bool {
        let mut inner = self.inner.lock().await;
        if inner.outgoing.len() >= MAX_OUTGOING {
            return false;
        }
        outgoing.req_id = req_id.clone();
        outgoing.sent_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        inner.outgoing_at.insert(req_id.clone(), Instant::now());
        inner.outgoing.insert(req_id, outgoing);
        true
    }

    /// Lists requests the local node has sent and is still waiting on a
    /// response for, ordered by req id.
    pub async fn list_outgoing(&self) -> Vec<OutgoingForward> {
        self.inner.lock().await.outgoing.values().cloned().collect()
    }

    /// Removes and returns the outgoing request with the given req id.
    /// Used to roll back `record_outgoing` when the send that was supposed
    /// to follow it fails.
    pub async fn remove_outgoing(&self, req_id: &str) -> Option<OutgoingForward> {
        let mut inner = self.inner.lock().await;
        inner.outgoing_at.remove(req_id);
        inner.outgoing.remove(req_id)
    }

    /// Withdraws a proposal the user no longer wants answered. No outcome is
    /// queued (the user cancelled it themselves, so there is nothing to
    /// report); a late response for it is dropped as an unknown req id.
    pub async fn cancel_outgoing(&self, req_id: &str) -> Option<OutgoingForward> {
        self.remove_outgoing(req_id).await
    }

    /// Drops the pending incoming proposal `(peer_id, req_id)` because that
    /// peer withdrew it. Bound to the transport sender: only the peer that
    /// sent a proposal can cancel it. Returns whether one was removed.
    pub async fn cancel_incoming(&self, peer_id: &str, req_id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        let Some(id) = inner
            .incoming
            .values()
            .find(|f| f.peer_id == peer_id && f.req_id == req_id)
            .map(|f| f.id)
        else {
            return false;
        };
        inner.incoming.remove(&id);
        inner.incoming_at.remove(&id);
        true
    }

    /// Matches a received response to a pending outgoing request and queues the
    /// outcome for the TUI loop. Unknown req ids are ignored. A response whose
    /// sender doesn't match the peer the request was actually sent to
    /// (`OutgoingForward::peer_id`) is also ignored -- and logged -- rather
    /// than consumed, so an unrelated peer can't spoof or swallow another
    /// peer's answer; the entry stays pending for the real peer to answer.
    pub async fn record_response(&self, req_id: &str, from_peer_id: &str, accepted: bool) {
        let mut inner = self.inner.lock().await;
        let Some(outgoing) = inner.outgoing.get(req_id) else {
            return;
        };
        if outgoing.peer_id != from_peer_id {
            tracing::warn!(
                "ignoring forward response for req {} from {} (expected {})",
                req_id,
                from_peer_id,
                outgoing.peer_id
            );
            return;
        }
        let outgoing = inner.outgoing.remove(req_id).expect("just checked above");
        inner.outgoing_at.remove(req_id);
        inner.outcomes.push(ForwardOutcome {
            outgoing,
            accepted,
            reason: None,
        });
    }

    /// Drains queued outcomes for the requester side to act on. Proposals
    /// that sat unanswered past [`OUTGOING_TTL`] are first failed with a
    /// "timed out" outcome so they do not wait forever.
    pub async fn drain_outcomes(&self) -> Vec<ForwardOutcome> {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let stale: Vec<String> = inner
            .outgoing_at
            .iter()
            .filter(|(_, at)| now.duration_since(**at) > OUTGOING_TTL)
            .map(|(req_id, _)| req_id.clone())
            .collect();
        for req_id in stale {
            inner.outgoing_at.remove(&req_id);
            if let Some(outgoing) = inner.outgoing.remove(&req_id) {
                inner.outcomes.push(ForwardOutcome {
                    outgoing,
                    accepted: false,
                    reason: Some(REASON_TIMEOUT.to_string()),
                });
            }
        }
        std::mem::take(&mut inner.outcomes)
    }

    /// Removes state tied to a departed peer: its incoming proposals (there's
    /// no one left to answer) and any outgoing requests addressed to it,
    /// failing the latter with a `ForwardOutcome` (`accepted: false`,
    /// `reason: Some("peer disconnected")`) so the requester side notices
    /// instead of waiting forever. Returns `(incoming_removed,
    /// outgoing_failed)`.
    pub async fn purge_peer(&self, peer_id: &str) -> (usize, usize) {
        let mut inner = self.inner.lock().await;

        let before = inner.incoming.len();
        inner.incoming.retain(|_, req| req.peer_id != peer_id);
        let live: std::collections::BTreeSet<u64> = inner.incoming.keys().copied().collect();
        inner.incoming_at.retain(|id, _| live.contains(id));
        let incoming_removed = before - inner.incoming.len();

        let stale_req_ids: Vec<String> = inner
            .outgoing
            .iter()
            .filter(|(_, out)| out.peer_id == peer_id)
            .map(|(req_id, _)| req_id.clone())
            .collect();
        let outgoing_failed = stale_req_ids.len();
        for req_id in stale_req_ids {
            inner.outgoing_at.remove(&req_id);
            if let Some(outgoing) = inner.outgoing.remove(&req_id) {
                inner.outcomes.push(ForwardOutcome {
                    outgoing,
                    accepted: false,
                    reason: Some(REASON_PEER_LEFT.to_string()),
                });
            }
        }

        (incoming_removed, outgoing_failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_outgoing() -> OutgoingForward {
        OutgoingForward {
            peer_id: "peer-1".into(),
            proto: "tcp".into(),
            listen_port: 8080,
            local_addr: "127.0.0.1:8080".into(),
            remote_addr: "127.0.0.1:80".into(),
            target: "tcp:127.0.0.1:80".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn incoming_requests_are_listed_and_taken() {
        let neg = ForwardNegotiator::new();
        let id = neg
            .record_incoming(
                "r1".into(),
                "peer-1".into(),
                "tcp".into(),
                "127.0.0.1:80".into(),
                "tcp:127.0.0.1:80".into(),
            )
            .await
            .unwrap();

        assert_eq!(neg.list_incoming().await.len(), 1);
        let taken = neg.take_incoming(id).await.unwrap();
        assert_eq!(taken.req_id, "r1");
        assert!(neg.list_incoming().await.is_empty());
        assert!(neg.take_incoming(id).await.is_none());
    }

    #[tokio::test]
    async fn duplicate_req_id_from_same_peer_is_deduped() {
        let neg = ForwardNegotiator::new();
        let rec = |peer: &str, req: String| {
            neg.record_incoming(
                req,
                peer.into(),
                "tcp".into(),
                "127.0.0.1:80".into(),
                "t".into(),
            )
        };
        let a = rec("p1", "r1".into()).await.unwrap();
        let b = rec("p1", "r1".into()).await.unwrap();
        assert_eq!(a, b);
        assert_eq!(neg.list_incoming().await.len(), 1);
        // Same req_id from another peer is a distinct proposal.
        assert_ne!(rec("p2", "r1".into()).await.unwrap(), a);
    }

    #[tokio::test]
    async fn incoming_is_capped_per_peer_and_in_total() {
        let neg = ForwardNegotiator::new();
        let rec = |peer: String, req: String| {
            neg.record_incoming(req, peer, "tcp".into(), "127.0.0.1:80".into(), "t".into())
        };
        for i in 0..MAX_INCOMING_PER_PEER {
            assert!(rec("p1".into(), format!("r{i}")).await.is_some());
        }
        assert!(rec("p1".into(), "extra".into()).await.is_none());
        // Other peers still fit, up to the global cap.
        let mut n = 0;
        while rec(format!("q{}", n / MAX_INCOMING_PER_PEER), format!("r{n}"))
            .await
            .is_some()
        {
            n += 1;
        }
        assert_eq!(neg.list_incoming().await.len(), MAX_INCOMING_TOTAL);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_incoming_is_swept_after_ttl() {
        let neg = ForwardNegotiator::new();
        neg.record_incoming(
            "r1".into(),
            "p1".into(),
            "tcp".into(),
            "127.0.0.1:80".into(),
            "t".into(),
        )
        .await;
        tokio::time::advance(INCOMING_TTL + Duration::from_secs(1)).await;
        neg.record_incoming(
            "r2".into(),
            "p2".into(),
            "tcp".into(),
            "127.0.0.1:80".into(),
            "t".into(),
        )
        .await;
        let listed = neg.list_incoming().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].req_id, "r2");
    }

    #[tokio::test]
    async fn responses_match_outgoing_and_produce_outcomes() {
        let neg = ForwardNegotiator::new();
        neg.record_outgoing("r1".into(), sample_outgoing()).await;

        // Unknown req id is ignored.
        neg.record_response("nope", "peer-1", true).await;
        assert!(neg.drain_outcomes().await.is_empty());

        neg.record_response("r1", "peer-1", true).await;
        let outcomes = neg.drain_outcomes().await;
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].accepted);
        assert!(outcomes[0].reason.is_none());
        assert_eq!(outcomes[0].outgoing.listen_port, 8080);

        // Drained once only; a second response for the same id no longer matches.
        neg.record_response("r1", "peer-1", true).await;
        assert!(neg.drain_outcomes().await.is_empty());
    }

    #[tokio::test]
    async fn outgoing_requests_are_listed_and_removed() {
        let neg = ForwardNegotiator::new();
        neg.record_outgoing("r1".into(), sample_outgoing()).await;

        let listed = neg.list_outgoing().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].target, "tcp:127.0.0.1:80");

        let removed = neg.remove_outgoing("r1").await.unwrap();
        assert_eq!(removed.peer_id, "peer-1");
        assert!(neg.list_outgoing().await.is_empty());
        assert!(neg.remove_outgoing("r1").await.is_none());
    }

    #[tokio::test]
    async fn response_from_wrong_peer_is_ignored_and_entry_stays() {
        let neg = ForwardNegotiator::new();
        neg.record_outgoing("r1".into(), sample_outgoing()).await;

        // sample_outgoing() was sent to peer-1; a response claiming to come
        // from peer-2 must not be able to answer on peer-1's behalf.
        neg.record_response("r1", "peer-2", true).await;
        assert!(neg.drain_outcomes().await.is_empty());

        // The real peer can still answer afterwards.
        neg.record_response("r1", "peer-1", true).await;
        let outcomes = neg.drain_outcomes().await;
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].accepted);
    }

    #[tokio::test]
    async fn purge_peer_removes_incoming_and_fails_outgoing() {
        let neg = ForwardNegotiator::new();
        neg.record_incoming(
            "in-1".into(),
            "peer-1".into(),
            "tcp".into(),
            "127.0.0.1:80".into(),
            "tcp:127.0.0.1:80".into(),
        )
        .await;
        neg.record_incoming(
            "in-2".into(),
            "peer-2".into(),
            "tcp".into(),
            "127.0.0.1:81".into(),
            "tcp:127.0.0.1:81".into(),
        )
        .await;
        neg.record_outgoing("out-1".into(), sample_outgoing()).await;

        let (incoming_removed, outgoing_failed) = neg.purge_peer("peer-1").await;

        assert_eq!(incoming_removed, 1);
        assert_eq!(outgoing_failed, 1);
        let remaining_incoming = neg.list_incoming().await;
        assert_eq!(remaining_incoming.len(), 1);
        assert_eq!(remaining_incoming[0].peer_id, "peer-2");

        let outcomes = neg.drain_outcomes().await;
        assert_eq!(outcomes.len(), 1);
        assert!(!outcomes[0].accepted);
        assert_eq!(outcomes[0].reason.as_deref(), Some("peer disconnected"));

        // Purging again is a no-op: nothing left for peer-1.
        assert_eq!(neg.purge_peer("peer-1").await, (0, 0));
    }

    #[tokio::test]
    async fn record_outgoing_stamps_req_id_and_time() {
        let neg = ForwardNegotiator::new();
        assert!(neg.record_outgoing("r1".into(), sample_outgoing()).await);
        let listed = neg.list_outgoing().await;
        assert_eq!(listed[0].req_id, "r1");
        assert!(listed[0].sent_at_ms > 0);
    }

    #[tokio::test]
    async fn outgoing_is_capped() {
        let neg = ForwardNegotiator::new();
        for i in 0..MAX_OUTGOING {
            assert!(
                neg.record_outgoing(format!("r{i}"), sample_outgoing())
                    .await
            );
        }
        assert!(!neg.record_outgoing("extra".into(), sample_outgoing()).await);
    }

    #[tokio::test]
    async fn cancel_outgoing_removes_without_an_outcome() {
        let neg = ForwardNegotiator::new();
        neg.record_outgoing("r1".into(), sample_outgoing()).await;
        assert!(neg.cancel_outgoing("r1").await.is_some());
        assert!(neg.list_outgoing().await.is_empty());
        assert!(neg.drain_outcomes().await.is_empty());
        // A late answer for the cancelled proposal is dropped.
        neg.record_response("r1", "peer-1", true).await;
        assert!(neg.drain_outcomes().await.is_empty());
        assert!(neg.cancel_outgoing("r1").await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_outgoing_times_out_into_a_failed_outcome() {
        let neg = ForwardNegotiator::new();
        neg.record_outgoing("r1".into(), sample_outgoing()).await;
        assert!(neg.drain_outcomes().await.is_empty());
        tokio::time::advance(OUTGOING_TTL + Duration::from_secs(1)).await;
        let outcomes = neg.drain_outcomes().await;
        assert_eq!(outcomes.len(), 1);
        assert!(!outcomes[0].accepted);
        assert!(outcomes[0].reason.as_deref().unwrap().contains("timed out"));
        assert!(neg.list_outgoing().await.is_empty());
    }

    #[tokio::test]
    async fn cancel_incoming_is_bound_to_the_sending_peer() {
        let neg = ForwardNegotiator::new();
        neg.record_incoming(
            "r1".into(),
            "p1".into(),
            "tcp".into(),
            "127.0.0.1:80".into(),
            "t".into(),
        )
        .await;
        // Another peer naming the same req id cannot withdraw p1's proposal.
        assert!(!neg.cancel_incoming("p2", "r1").await);
        assert_eq!(neg.list_incoming().await.len(), 1);
        assert!(neg.cancel_incoming("p1", "r1").await);
        assert!(neg.list_incoming().await.is_empty());
        assert!(!neg.cancel_incoming("p1", "r1").await);
    }
}
