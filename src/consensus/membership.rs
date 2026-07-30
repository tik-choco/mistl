//! Pure, unit-testable relay-membership policy: which peers have recently
//! announced themselves as consensus participants in this room, tracked
//! independently of the network layer -- the same shape as
//! `stream::relay`'s `decide_video`/`decide_audio` precedent.
//!
//! Only mistl relay nodes send the periodic hello broadcast
//! (`transport::HELLO_TAG`); tc-chat browser peers sharing the same mist
//! room never do, so they never enter this set even though they may show
//! up in `crate::net::connected_nodes()`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How often a node broadcasts its own hello.
pub const HELLO_INTERVAL: Duration = Duration::from_secs(5);
/// A peer that hasn't sent a hello within this long is considered gone.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(15);

/// Tracks last-seen times for known relay peers (never includes `self_id`).
#[derive(Debug)]
pub struct Membership {
    self_id: String,
    peers: HashMap<String, Instant>,
}

impl Membership {
    pub fn new(self_id: String) -> Self {
        Self {
            self_id,
            peers: HashMap::new(),
        }
    }

    /// Records a hello from `node` at `now`. Returns `true` only when
    /// `node` was previously unknown (i.e. membership actually changed);
    /// a hello from an already-known peer is just a liveness refresh, and
    /// a hello that loops back to ourselves is ignored entirely.
    pub fn record_hello(&mut self, node: &str, now: Instant) -> bool {
        if node == self.self_id {
            return false;
        }
        self.peers.insert(node.to_string(), now).is_none()
    }

    /// Explicit departure (`EVENT_LEAVE`). Returns `true` if the peer was
    /// known (i.e. membership changed).
    pub fn remove(&mut self, node: &str) -> bool {
        self.peers.remove(node).is_some()
    }

    /// Drops any peer not heard from within `timeout` of `now`; returns
    /// the ids that were dropped (empty if none were stale).
    pub fn expire(&mut self, now: Instant, timeout: Duration) -> Vec<String> {
        let stale: Vec<String> = self
            .peers
            .iter()
            .filter(|&(_, &last_seen)| now.saturating_duration_since(last_seen) > timeout)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &stale {
            self.peers.remove(id);
        }
        stale
    }

    /// All currently-known relay peers, including self, sorted for a
    /// deterministic view (`ConsensusView::peers`/`relay_peers()`).
    pub fn peers_with_self(&self) -> Vec<String> {
        let mut all: Vec<String> = self.peers.keys().cloned().collect();
        all.push(self.self_id.clone());
        all.sort();
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_hello_reports_new_peer_once() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        assert!(
            m.record_hello("peer-a", t0),
            "first hello from a peer is new"
        );
        assert!(
            !m.record_hello("peer-a", t0 + Duration::from_secs(1)),
            "second hello from the same peer is a refresh, not new membership"
        );
    }

    #[test]
    fn record_hello_ignores_loopback_from_self() {
        let mut m = Membership::new("self".into());
        assert!(!m.record_hello("self", Instant::now()));
        assert_eq!(m.peers_with_self(), vec!["self".to_string()]);
    }

    #[test]
    fn expire_drops_only_stale_peers() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        m.record_hello("fresh", t0);
        m.record_hello("stale", t0);

        // "fresh" is refreshed right before the timeout check; "stale" is not.
        let t1 = t0 + Duration::from_secs(20);
        m.record_hello("fresh", t1);

        let dropped = m.expire(t1, PEER_TIMEOUT);
        assert_eq!(dropped, vec!["stale".to_string()]);
        assert_eq!(
            m.peers_with_self(),
            vec!["fresh".to_string(), "self".to_string()]
        );
    }

    #[test]
    fn expire_is_a_no_op_when_nothing_is_stale() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        m.record_hello("peer-a", t0);
        let dropped = m.expire(t0 + Duration::from_secs(1), PEER_TIMEOUT);
        assert!(dropped.is_empty());
    }

    #[test]
    fn remove_reports_whether_the_peer_was_known() {
        let mut m = Membership::new("self".into());
        assert!(
            !m.remove("ghost"),
            "removing an unknown peer changes nothing"
        );
        m.record_hello("peer-a", Instant::now());
        assert!(m.remove("peer-a"));
        assert_eq!(m.peers_with_self(), vec!["self".to_string()]);
    }

    #[test]
    fn peers_with_self_is_sorted_and_deduplicated() {
        let mut m = Membership::new("m".into());
        let t0 = Instant::now();
        m.record_hello("z-peer", t0);
        m.record_hello("a-peer", t0);
        m.record_hello("a-peer", t0 + Duration::from_secs(1)); // refresh, not a new entry
        assert_eq!(
            m.peers_with_self(),
            vec!["a-peer".to_string(), "m".to_string(), "z-peer".to_string()]
        );
    }
}
