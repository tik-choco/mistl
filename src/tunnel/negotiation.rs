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
//! `OutgoingForward` -> `{"peer_id":"..","proto":"..","listen_port":0,"local_addr":"..","remote_addr":"..","target":".."}`

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::Mutex;

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
#[derive(Debug, Clone, Serialize)]
pub struct OutgoingForward {
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
    outgoing: BTreeMap<String, OutgoingForward>,
    outcomes: Vec<ForwardOutcome>,
}

impl ForwardNegotiator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an incoming proposal and returns its local id.
    pub async fn record_incoming(
        &self,
        req_id: String,
        peer_id: String,
        proto: String,
        remote_addr: String,
        target: String,
    ) -> u64 {
        let mut inner = self.inner.lock().await;
        inner.next_id += 1;
        let id = inner.next_id;
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
        id
    }

    pub async fn list_incoming(&self) -> Vec<IncomingForward> {
        self.inner.lock().await.incoming.values().cloned().collect()
    }

    /// Removes and returns the incoming proposal with the given local id.
    pub async fn take_incoming(&self, id: u64) -> Option<IncomingForward> {
        self.inner.lock().await.incoming.remove(&id)
    }

    /// Remembers a request the local node just sent, keyed by protocol req id.
    pub async fn record_outgoing(&self, req_id: String, outgoing: OutgoingForward) {
        self.inner.lock().await.outgoing.insert(req_id, outgoing);
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
        self.inner.lock().await.outgoing.remove(req_id)
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
        inner.outcomes.push(ForwardOutcome {
            outgoing,
            accepted,
            reason: None,
        });
    }

    /// Drains queued outcomes for the requester side to act on.
    pub async fn drain_outcomes(&self) -> Vec<ForwardOutcome> {
        std::mem::take(&mut self.inner.lock().await.outcomes)
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
        let incoming_removed = before - inner.incoming.len();

        let stale_req_ids: Vec<String> = inner
            .outgoing
            .iter()
            .filter(|(_, out)| out.peer_id == peer_id)
            .map(|(req_id, _)| req_id.clone())
            .collect();
        let outgoing_failed = stale_req_ids.len();
        for req_id in stale_req_ids {
            if let Some(outgoing) = inner.outgoing.remove(&req_id) {
                inner.outcomes.push(ForwardOutcome {
                    outgoing,
                    accepted: false,
                    reason: Some("peer disconnected".to_string()),
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
            .await;

        assert_eq!(neg.list_incoming().await.len(), 1);
        let taken = neg.take_incoming(id).await.unwrap();
        assert_eq!(taken.req_id, "r1");
        assert!(neg.list_incoming().await.is_empty());
        assert!(neg.take_incoming(id).await.is_none());
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
}
