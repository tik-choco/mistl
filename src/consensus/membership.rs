//! Pure, unit-testable relay-membership policy: which peers have recently
//! announced themselves as consensus participants in this room, tracked
//! independently of the network layer -- the same shape as
//! `stream::relay`'s `decide_video`/`decide_audio` precedent.
//!
//! Only mistl relay nodes send the periodic hello broadcast
//! (`transport::HELLO_TAG`); tc-chat browser peers sharing the same mist
//! room never do, so they never enter this set even though they may show
//! up in `crate::net::connected_nodes()`.
//!
//! Every member is DID-verified: a peer only enters through a signed relay
//! hello whose DID hashes to its node id (checked by the caller before
//! [`Membership::record_hello`]), and its DID is kept here so every later
//! signed Raft message can be matched against it ([`Membership::accept_raft`]),
//! together with a per-sender replay window.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How often a node broadcasts its own hello.
pub const HELLO_INTERVAL: Duration = Duration::from_secs(5);
/// A peer that hasn't sent a hello within this long is considered gone.
pub const PEER_TIMEOUT: Duration = Duration::from_secs(15);

/// Maximum number of tracked relay peers (excluding self); new peers beyond
/// this are ignored.
pub const MAX_PEERS: usize = 64;

/// Width of the per-sender anti-replay window, in messages.
const REPLAY_WINDOW: u64 = 64;

/// Sliding anti-replay window over one sender's `(boot, seq)` numbering
/// (IPsec-style): accepts each sequence number at most once, tolerates
/// reordering within [`REPLAY_WINDOW`], and only switches to a new boot id
/// (the sender restarted) for a message newer than anything seen so far.
#[derive(Debug, Clone)]
struct ReplayWindow {
    boot: u64,
    max_seq: u64,
    bitmap: u64,
    last_ts_ms: u64,
}

impl ReplayWindow {
    fn new(boot: u64, seq: u64, ts_ms: u64) -> Self {
        Self {
            boot,
            max_seq: seq,
            bitmap: 1,
            last_ts_ms: ts_ms,
        }
    }

    fn accept(&mut self, boot: u64, seq: u64, ts_ms: u64) -> bool {
        if boot != self.boot {
            // A replay from an earlier boot is older than what the current
            // boot has already sent; a genuine restart is newer.
            if ts_ms > self.last_ts_ms {
                *self = Self::new(boot, seq, ts_ms);
                return true;
            }
            return false;
        }
        if seq > self.max_seq {
            let shift = seq - self.max_seq;
            self.bitmap = if shift >= REPLAY_WINDOW {
                1
            } else {
                (self.bitmap << shift) | 1
            };
            self.max_seq = seq;
        } else {
            let offset = self.max_seq - seq;
            if offset >= REPLAY_WINDOW || self.bitmap & (1 << offset) != 0 {
                return false;
            }
            self.bitmap |= 1 << offset;
        }
        self.last_ts_ms = self.last_ts_ms.max(ts_ms);
        true
    }
}

#[derive(Debug)]
struct Peer {
    last_seen: Instant,
    did: String,
    replay: Option<ReplayWindow>,
}

/// Tracks last-seen times, DIDs, and replay windows for known relay peers
/// (never includes `self_id`).
#[derive(Debug)]
pub struct Membership {
    self_id: String,
    peers: HashMap<String, Peer>,
}

impl Membership {
    pub fn new(self_id: String) -> Self {
        Self {
            self_id,
            peers: HashMap::new(),
        }
    }

    /// Records a verified hello from `node` (holding `did`) at `now`.
    /// Returns `true` only when `node` was previously unknown (i.e.
    /// membership actually changed); a hello from an already-known peer is
    /// just a liveness refresh, and a hello that loops back to ourselves is
    /// ignored entirely. A known node id presenting a different DID is
    /// ignored too (node ids are a 64-bit DID hash prefix; the first binding
    /// wins).
    pub fn record_hello(&mut self, node: &str, did: &str, now: Instant) -> bool {
        if node == self.self_id {
            return false;
        }
        if let Some(peer) = self.peers.get_mut(node) {
            if peer.did == did {
                peer.last_seen = now;
            }
            return false;
        }
        // Bound the peer set so a flood of distinct ids can't inflate the
        // Raft quorum or grow memory without limit; known peers still refresh.
        if self.peers.len() >= MAX_PEERS {
            return false;
        }
        self.peers.insert(
            node.to_string(),
            Peer {
                last_seen: now,
                did: did.to_string(),
                replay: None,
            },
        );
        true
    }

    /// Whether a signed Raft message from `node`, signed by `did` under
    /// `(boot, seq)` at `ts_ms`, should reach the Raft driver: `node` must be
    /// a current (hello-verified) member, `did` must be the DID it verified
    /// with, and `(boot, seq)` must not be a replay.
    pub fn accept_raft(&mut self, node: &str, did: &str, boot: u64, seq: u64, ts_ms: u64) -> bool {
        let Some(peer) = self.peers.get_mut(node) else {
            return false;
        };
        if peer.did != did {
            return false;
        }
        match &mut peer.replay {
            Some(window) => window.accept(boot, seq, ts_ms),
            None => {
                peer.replay = Some(ReplayWindow::new(boot, seq, ts_ms));
                true
            }
        }
    }

    /// Whether `node` is a current member.
    pub fn contains(&self, node: &str) -> bool {
        self.peers.contains_key(node)
    }

    /// The DID `node` verified with, if it's a current member.
    pub fn did_of(&self, node: &str) -> Option<&str> {
        self.peers.get(node).map(|peer| peer.did.as_str())
    }

    /// Explicit departure (`EVENT_LEAVE`). Returns `true` if the peer was
    /// known (i.e. membership changed).
    pub fn remove(&mut self, node: &str) -> bool {
        self.peers.remove(node).is_some()
    }

    /// Drops any peer not heard from within `timeout` of `now`; returns
    /// the ids that were dropped (empty if none were stale).
    pub fn expire(&mut self, now: Instant, timeout: Duration) -> Vec<String> {
        self.remove_where(|peer| now.saturating_duration_since(peer.last_seen) > timeout)
    }

    /// Drops every peer whose DID `admitted` rejects (e.g. after the
    /// membership allowlist changed); returns the ids that were dropped.
    pub fn retain_admitted(&mut self, admitted: impl Fn(&str) -> bool) -> Vec<String> {
        self.remove_where(|peer| !admitted(&peer.did))
    }

    fn remove_where(&mut self, doomed: impl Fn(&Peer) -> bool) -> Vec<String> {
        let mut dropped: Vec<String> = self
            .peers
            .iter()
            .filter(|&(_, peer)| doomed(peer))
            .map(|(id, _)| id.clone())
            .collect();
        dropped.sort();
        for id in &dropped {
            self.peers.remove(id);
        }
        dropped
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

    impl Membership {
        /// Test shorthand: every peer's DID is `did-<node>`.
        fn record_hello_t(&mut self, node: &str, now: Instant) -> bool {
            self.record_hello(node, &format!("did-{node}"), now)
        }
    }

    #[test]
    fn record_hello_reports_new_peer_once() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        assert!(
            m.record_hello_t("peer-a", t0),
            "first hello from a peer is new"
        );
        assert!(
            !m.record_hello_t("peer-a", t0 + Duration::from_secs(1)),
            "second hello from the same peer is a refresh, not new membership"
        );
    }

    #[test]
    fn record_hello_caps_peer_count() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        for i in 0..MAX_PEERS {
            assert!(m.record_hello_t(&format!("p{i}"), t0));
        }
        assert!(
            !m.record_hello_t("overflow", t0),
            "beyond the cap is ignored"
        );
        assert_eq!(m.peers_with_self().len(), MAX_PEERS + 1);
        // Known peers still refresh without counting as new.
        assert!(!m.record_hello_t("p0", t0));
    }

    #[test]
    fn record_hello_ignores_loopback_from_self() {
        let mut m = Membership::new("self".into());
        assert!(!m.record_hello_t("self", Instant::now()));
        assert_eq!(m.peers_with_self(), vec!["self".to_string()]);
    }

    #[test]
    fn expire_drops_only_stale_peers() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        m.record_hello_t("fresh", t0);
        m.record_hello_t("stale", t0);

        // "fresh" is refreshed right before the timeout check; "stale" is not.
        let t1 = t0 + Duration::from_secs(20);
        m.record_hello_t("fresh", t1);

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
        m.record_hello_t("peer-a", t0);
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
        m.record_hello_t("peer-a", Instant::now());
        assert!(m.remove("peer-a"));
        assert_eq!(m.peers_with_self(), vec!["self".to_string()]);
    }

    #[test]
    fn peers_with_self_is_sorted_and_deduplicated() {
        let mut m = Membership::new("m".into());
        let t0 = Instant::now();
        m.record_hello_t("z-peer", t0);
        m.record_hello_t("a-peer", t0);
        m.record_hello_t("a-peer", t0 + Duration::from_secs(1)); // refresh, not a new entry
        assert_eq!(
            m.peers_with_self(),
            vec!["a-peer".to_string(), "m".to_string(), "z-peer".to_string()]
        );
    }

    #[test]
    fn known_node_with_a_different_did_is_not_rebound() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        assert!(m.record_hello("peer", "did-a", t0));
        assert!(!m.record_hello("peer", "did-b", t0));
        assert_eq!(m.did_of("peer"), Some("did-a"));
    }

    #[test]
    fn raft_from_a_non_member_or_wrong_did_is_rejected() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        assert!(!m.accept_raft("stranger", "did-stranger", 1, 1, 100));
        m.record_hello_t("peer", t0);
        assert!(!m.accept_raft("peer", "did-someone-else", 1, 1, 100));
        assert!(m.accept_raft("peer", "did-peer", 1, 1, 100));
    }

    #[test]
    fn raft_replays_are_rejected_but_reordering_is_tolerated() {
        let mut m = Membership::new("self".into());
        m.record_hello_t("peer", Instant::now());
        assert!(m.accept_raft("peer", "did-peer", 7, 1, 100));
        assert!(m.accept_raft("peer", "did-peer", 7, 3, 102));
        assert!(
            !m.accept_raft("peer", "did-peer", 7, 3, 102),
            "exact replay"
        );
        assert!(m.accept_raft("peer", "did-peer", 7, 2, 101), "late but new");
        assert!(
            !m.accept_raft("peer", "did-peer", 7, 2, 101),
            "replay of the late one"
        );
        assert!(m.accept_raft("peer", "did-peer", 7, 200, 110));
        assert!(
            !m.accept_raft("peer", "did-peer", 7, 100, 105),
            "outside the window"
        );
    }

    #[test]
    fn raft_boot_switch_needs_a_newer_timestamp() {
        let mut m = Membership::new("self".into());
        m.record_hello_t("peer", Instant::now());
        assert!(m.accept_raft("peer", "did-peer", 1, 50, 1_000));
        // Replay from an older boot (older timestamps): rejected.
        assert!(!m.accept_raft("peer", "did-peer", 0, 999, 900));
        // Genuine restart: newer timestamp, fresh numbering.
        assert!(m.accept_raft("peer", "did-peer", 2, 1, 2_000));
        assert!(
            !m.accept_raft("peer", "did-peer", 1, 51, 1_001),
            "old boot again"
        );
    }

    #[test]
    fn retain_admitted_drops_peers_outside_the_allowlist() {
        let mut m = Membership::new("self".into());
        let t0 = Instant::now();
        m.record_hello_t("a", t0);
        m.record_hello_t("b", t0);
        let dropped = m.retain_admitted(|did| did == "did-a");
        assert_eq!(dropped, vec!["b".to_string()]);
        assert_eq!(
            m.peers_with_self(),
            vec!["a".to_string(), "self".to_string()]
        );
    }
}
