//! Raft-based control plane for `stream relay` leader election.
//!
//! Relay nodes sharing a room run this module to elect a LEADER (Trusted /
//! crash-fault-tolerant Raft, via `mistlib-consensus`) that other modules
//! use as the cascade-distribution decision: the leader ingests a tc-chat
//! screen share and re-publishes it, while followers relay from the
//! leader instead of ingesting themselves. This module owns only the
//! control plane -- leader election plus relay-node membership; wiring
//! that decision into `stream::relay`'s actual media pipeline is a
//! separate task, so nothing here is called from `stream::mod` yet.
//!
//! ## Membership
//!
//! Only mistl relay nodes participate -- not the tc-chat browser peers
//! that may share the same mist room. Participation is discovered by a
//! periodic hello broadcast (`transport::HELLO_TAG`) rather than
//! `crate::net::connected_nodes()`, which would also list tc-chat peers.
//! See [`membership::Membership`] for the (pure, unit-tested) tracking
//! policy this module drives.
//!
//! ## Replication
//!
//! v1 does not replicate any log entries -- the elected leader identity
//! *is* the decision the rest of mistl consumes. `RaftNode::propose` is
//! intentionally never called; only `role()`/`leader_hint()` (via
//! [`RelayConsensus::role`]/[`RelayConsensus::leader`]) and membership
//! (`add_peer`/`remove_peer`, driven by hello/`EVENT_LEAVE`) are used.
//!
//! ## Peer authentication
//!
//! Identity comes from DIDs, not transport sender ids (see
//! `crate::net::peer_auth`), so every consensus message is DID-signed
//! (`transport` module doc): a peer becomes a voter only through a signed
//! relay hello whose `did:key` hashes to its node id and which passes the
//! membership allowlist hook (`crate::net::peer_auth::admitted`, open by
//! default), and every Raft RPC must be signed by that same DID, carry the
//! sender's own id inside the RPC, be fresh, and not be a replay. Messages
//! are matched against the room they actually arrived in (a room-aware
//! `crate::net` handler), not a room named inside the JSON. Because the
//! signature covers the whole message, the delivery path doesn't matter.
//!
//! Mixed versions: builds before this change send unsigned `*-v1`
//! envelopes, which are ignored here, and ignore our `*-v2` ones. Old and new
//! relays in one room therefore run two independent control planes (each may
//! elect its own leader); an old relay can never vote in, or lead, the
//! verified group.
//!
//! ## Timing
//!
//! `mistlib-consensus-core`'s defaults (150-300ms election timeout, 50ms
//! heartbeat) are tuned for a low-latency datacenter LAN. Over a p2p mesh
//! whose links are WebRTC connections signaled through Nostr, latency and
//! jitter are both far higher, so this module overrides them with much
//! more conservative timings (see the constants below) to avoid spurious
//! elections when a heartbeat is merely late rather than lost.

// This module's public surface (`RelayConsensus::start`/`subscribe`/
// `shutdown`, and everything that feeds them) is consumed by the
// `stream::relay` integration landing in a separate task, not by anything
// in this crate yet -- so it's all "unused" from `cargo check`'s point of
// view until that wiring exists. `consensus.status` (the only current
// caller, via `daemon::dispatch`) only exercises the read side.
#![allow(dead_code)]

mod membership;
mod transport;

use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use mistlib_consensus_core::{MemoryStorage, NodeId, RaftConfig, RaftNode, RaftTransport};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::daemon::AppState;
use crate::identity::Identity;
use crate::net::peer_auth::{self, DidProof};

use membership::{HELLO_INTERVAL, Membership, PEER_TIMEOUT};
use transport::{MistlRaftTransport, WireMessage};

/// Minimum/maximum randomized election timeout. Conservative for a
/// jittery p2p mesh -- see the module doc's "Timing" section.
const ELECTION_TIMEOUT_MIN_MS: u64 = 1500;
const ELECTION_TIMEOUT_MAX_MS: u64 = 3000;
/// Heartbeat interval; well below the election timeout, as required.
const HEARTBEAT_INTERVAL_MS: u64 = 500;

/// How often the view watch is refreshed from `role()`/`leader_hint()`.
/// The Raft driver updates those internally on its own event loop; this
/// module just needs to notice the change and isn't in a hurry to do so
/// (leader changes are rare compared to steady-state operation).
const VIEW_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// This node's believed role in the control-plane election.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusRole {
    Leader,
    Follower,
    Candidate,
    /// No role assigned yet -- e.g. the node just started and hasn't hit
    /// its first election timeout.
    Unknown,
}

impl From<Option<mistlib_consensus_core::Role>> for ConsensusRole {
    fn from(role: Option<mistlib_consensus_core::Role>) -> Self {
        match role {
            Some(mistlib_consensus_core::Role::Leader) => ConsensusRole::Leader,
            Some(mistlib_consensus_core::Role::Follower) => ConsensusRole::Follower,
            Some(mistlib_consensus_core::Role::Candidate) => ConsensusRole::Candidate,
            None => ConsensusRole::Unknown,
        }
    }
}

/// A point-in-time snapshot of the control plane, delivered over
/// [`RelayConsensus::subscribe`] whenever it changes.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConsensusView {
    pub role: ConsensusRole,
    pub leader: Option<String>,
    pub peers: Vec<String>,
}

/// A running control-plane participant for one room.
pub struct RelayConsensus {
    room: String,
    node_id: String,
    identity: Arc<Identity>,
    raft: Arc<RaftNode<MemoryStorage>>,
    membership: Arc<StdMutex<Membership>>,
    view_tx: watch::Sender<ConsensusView>,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
}

/// The most recently started [`RelayConsensus`], if any -- backs
/// `consensus.status`. This is a plain slot rather than a `OnceCell`:
/// mistl expects at most one relay control plane per process (mirroring
/// `stream::relay`'s single-relay assumption), but a relay that stops and
/// restarts calls [`RelayConsensus::start`] again, which should replace
/// the previous entry rather than be rejected by it.
static CURRENT: RwLock<Option<Arc<RelayConsensus>>> = RwLock::new(None);

impl RelayConsensus {
    /// Joins the consensus control plane for `room`, which must already
    /// be a mist room this process has joined (e.g. via
    /// `net::ensure_started` -- this function does not join it itself,
    /// since by the time a relay wants a control plane it has already
    /// joined the room for media). `node_id` is not a parameter: it's
    /// resolved from the local identity, matching every other module in
    /// this crate.
    ///
    /// A lone node in the room becomes leader by itself once its first
    /// election timeout fires (a few seconds, per [`ELECTION_TIMEOUT_MIN_MS`]).
    pub async fn start(state: &Arc<AppState>, room: String) -> Result<Arc<RelayConsensus>> {
        let identity = crate::identity::current(state)
            .await
            .context("consensus: loading identity")?;
        let node_id = identity.node_id();

        let raft_transport: Arc<dyn RaftTransport> =
            Arc::new(MistlRaftTransport::new(room.clone(), identity.clone()));
        let config = RaftConfig {
            election_timeout_min_ms: ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max_ms: ELECTION_TIMEOUT_MAX_MS,
            heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
        };
        // Peers are not known up front -- they join dynamically as hellos
        // arrive (see `register_net_handler`'s `add_peer` call below).
        let raft = mistlib_consensus_native::spawn_native(
            NodeId(node_id.clone()),
            Vec::new(),
            config,
            raft_transport,
            MemoryStorage::new(),
        );

        let membership = Arc::new(StdMutex::new(Membership::new(node_id.clone())));
        let initial_peers = membership
            .lock()
            .expect("consensus membership lock poisoned")
            .peers_with_self();
        let (view_tx, _view_rx) = watch::channel(ConsensusView {
            role: ConsensusRole::Unknown,
            leader: None,
            peers: initial_peers,
        });

        let consensus = Arc::new(RelayConsensus {
            room: room.clone(),
            node_id,
            identity,
            raft,
            membership,
            view_tx,
            tasks: StdMutex::new(Vec::new()),
        });

        register_net_handler(consensus.clone());

        let hello_task = tokio::spawn(hello_loop(consensus.clone()));
        let view_task = tokio::spawn(view_poll_loop(consensus.clone()));
        consensus
            .tasks
            .lock()
            .expect("consensus tasks lock poisoned")
            .extend([hello_task, view_task]);

        *CURRENT.write().expect("consensus current lock poisoned") = Some(consensus.clone());

        Ok(consensus)
    }

    /// This node's current believed role.
    pub fn role(&self) -> ConsensusRole {
        self.raft.role().into()
    }

    /// The current leader's node id, if known and verified (see
    /// [`Self::is_verified_leader`]).
    pub fn leader(&self) -> Option<String> {
        self.raft
            .leader_hint()
            .map(|id| id.0)
            .filter(|id| self.is_verified_leader(id))
    }

    /// Whether `node` may be followed as leader: ourselves, or a current
    /// member (i.e. it proved its DID with a signed relay hello) whose DID
    /// still passes the membership allowlist. `stream::relay` checks this
    /// before relaying a leader's media.
    pub fn is_verified_leader(&self, node: &str) -> bool {
        if node == self.node_id {
            return true;
        }
        self.membership
            .lock()
            .expect("consensus membership lock poisoned")
            .did_of(node)
            .is_some_and(peer_auth::admitted)
    }

    /// Relay nodes currently known in this room's control plane,
    /// including self, sorted for a deterministic result.
    pub fn relay_peers(&self) -> Vec<String> {
        self.membership
            .lock()
            .expect("consensus membership lock poisoned")
            .peers_with_self()
    }

    /// Subscribes to `{role, leader, peers}` changes -- the stream module
    /// reacts to leader changes through this rather than polling.
    pub fn subscribe(&self) -> watch::Receiver<ConsensusView> {
        self.view_tx.subscribe()
    }

    /// Stops the Raft driver and this instance's background tasks, and
    /// clears it from `consensus.status` if it's still the current one
    /// (a `start()` that ran after this one, replacing `CURRENT`, is left
    /// alone).
    pub async fn shutdown(&self) {
        self.raft.shutdown();
        for task in self
            .tasks
            .lock()
            .expect("consensus tasks lock poisoned")
            .drain(..)
        {
            task.abort();
        }

        let mut current = CURRENT.write().expect("consensus current lock poisoned");
        if let Some(existing) = current.as_ref() {
            if std::ptr::eq(existing.as_ref(), self) {
                *current = None;
            }
        }
    }
}

/// What one inbound consensus message should do, decided without touching
/// the Raft driver so the admission policy is unit-testable.
#[derive(Debug)]
enum Inbound {
    /// A signed RPC from a verified member: feed it to Raft.
    Deliver(mistlib_consensus_core::RaftMessage),
    /// A signed hello from a peer not yet in the membership: add it.
    NewPeer,
    /// Anything else (refresh, replay, non-member, not admitted, ...).
    Nothing,
}

/// Admission policy for an already signature-verified message from `from`:
/// a hello admits its DID into the membership (subject to the allowlist and
/// the shared replay cache, via `accept_proof`); a Raft RPC only counts when
/// `from` is a member verified under the same DID and `(boot, seq)` isn't a
/// replay. Unverified peers therefore never vote, campaign, or lead.
fn gate_inbound(
    membership: &mut Membership,
    msg: WireMessage,
    from: &str,
    now: Instant,
    admitted: impl Fn(&str) -> bool,
    accept_proof: impl FnOnce(&DidProof) -> bool,
) -> Inbound {
    match msg {
        WireMessage::Raft(signed) => {
            if admitted(&signed.did)
                && membership.accept_raft(from, &signed.did, signed.boot, signed.seq, signed.ts_ms)
            {
                Inbound::Deliver(signed.msg)
            } else {
                Inbound::Nothing
            }
        }
        WireMessage::Hello(proof) => {
            if !admitted(&proof.did) || !accept_proof(&proof) {
                return Inbound::Nothing;
            }
            if membership.record_hello(from, &proof.did, now) {
                Inbound::NewPeer
            } else {
                Inbound::Nothing
            }
        }
    }
}

/// Wires a room-aware `crate::net` handler for this instance: verifies and
/// demuxes Raft RPCs (fed into the driver) and relay hellos (tracked in
/// `membership`, promoted to `RaftNode::add_peer` on first sight), and clears
/// departed peers on `EVENT_LEAVE` -- all only for events that actually
/// arrived in this instance's room. Entirely synchronous -- no tokio runtime
/// is available on `crate::net`'s dispatch thread, and nothing here needs
/// one (channel sends and `std::sync::Mutex`/`watch::Sender` don't require
/// an executor).
fn register_net_handler(consensus: Arc<RelayConsensus>) {
    crate::net::register_room_handler(move |event_type, room, from_id, data| {
        if room != consensus.room {
            return; // another joined room's traffic, whatever its JSON claims
        }
        match event_type {
            crate::net::EVENT_RAW => {
                let Some(msg) = transport::decode(data, room, from_id, peer_auth::now_ms()) else {
                    return; // not ours, or failed verification
                };
                let inbound = {
                    let mut membership = consensus
                        .membership
                        .lock()
                        .expect("consensus membership lock poisoned");
                    gate_inbound(
                        &mut membership,
                        msg,
                        from_id,
                        Instant::now(),
                        peer_auth::admitted,
                        |proof| peer_auth::accept_proof(room, proof).is_ok(),
                    )
                };
                match inbound {
                    Inbound::Deliver(msg) => {
                        consensus.raft.deliver(NodeId(from_id.to_string()), msg);
                    }
                    Inbound::NewPeer => {
                        consensus.raft.add_peer(NodeId(from_id.to_string()));
                        publish_view(&consensus);
                    }
                    Inbound::Nothing => {
                        tracing::trace!(from = %from_id, "consensus: message not admitted");
                    }
                }
            }
            crate::net::EVENT_LEAVE => {
                let was_known = consensus
                    .membership
                    .lock()
                    .expect("consensus membership lock poisoned")
                    .remove(from_id);
                if was_known {
                    consensus.raft.remove_peer(NodeId(from_id.to_string()));
                    publish_view(&consensus);
                }
            }
            _ => {}
        }
    });
}

/// Broadcasts this node's hello every [`HELLO_INTERVAL`] and expires
/// stale peers on the same cadence.
async fn hello_loop(consensus: Arc<RelayConsensus>) {
    let mut interval = tokio::time::interval(HELLO_INTERVAL);
    loop {
        interval.tick().await;
        broadcast_hello(&consensus).await;
        expire_stale_peers(&consensus);
    }
}

/// Sends the hello to every node `crate::net` currently sees as connected
/// (there is no room-scoped connected-node list -- see `crate::net`'s
/// module docs -- so, like the ai/chat_relay precedents, this may also reach
/// peers in this process's *other* joined rooms; `send_direct` is scoped
/// to `room` regardless, so it simply fails silently for anyone not
/// actually reachable there).
async fn broadcast_hello(consensus: &RelayConsensus) {
    let bytes = transport::encode_hello(&consensus.room, &consensus.identity);
    for node in crate::net::connected_nodes().await {
        let _ = crate::net::send_direct(&consensus.room, &node, bytes.clone()).await;
    }
}

/// Drops peers that stopped sending hellos, and peers whose DID the
/// membership allowlist no longer admits (it may change at runtime).
fn expire_stale_peers(consensus: &Arc<RelayConsensus>) {
    let stale = {
        let mut membership = consensus
            .membership
            .lock()
            .expect("consensus membership lock poisoned");
        let mut stale = membership.expire(Instant::now(), PEER_TIMEOUT);
        stale.extend(membership.retain_admitted(peer_auth::admitted));
        stale
    };
    if stale.is_empty() {
        return;
    }
    for id in stale {
        consensus.raft.remove_peer(NodeId(id));
    }
    publish_view(consensus);
}

/// Polls `role()`/`leader_hint()` (updated internally by the Raft driver's
/// own event loop) and republishes the view when anything changed.
async fn view_poll_loop(consensus: Arc<RelayConsensus>) {
    let mut interval = tokio::time::interval(VIEW_POLL_INTERVAL);
    loop {
        interval.tick().await;
        publish_view(&consensus);
    }
}

/// Recomputes the view and sends it only if it actually changed --
/// `watch::Sender::send` would otherwise notify subscribers on every
/// poll tick even when nothing moved.
fn publish_view(consensus: &RelayConsensus) {
    let view = ConsensusView {
        role: consensus.role(),
        leader: consensus.leader(),
        peers: consensus.relay_peers(),
    };
    consensus.view_tx.send_if_modified(|current| {
        if *current == view {
            false
        } else {
            *current = view.clone();
            true
        }
    });
}

/// IPC entry point for `consensus.*` commands.
pub async fn handle(cmd: &str, _args: Value, _state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "consensus.status" => Ok(status()),
        _ => bail!("unknown command: {cmd}"),
    }
}

fn status() -> Value {
    match current() {
        Some(consensus) => json!({
            "active": true,
            "room": consensus.room,
            "role": consensus.role(),
            "leader": consensus.leader(),
            "peers": consensus.relay_peers(),
        }),
        None => json!({ "active": false }),
    }
}

fn current() -> Option<Arc<RelayConsensus>> {
    CURRENT
        .read()
        .expect("consensus current lock poisoned")
        .clone()
}

#[cfg(test)]
mod hello_auth_tests {
    use super::*;
    use base64::Engine as _;
    use mistlib_consensus_core::RaftMessage;

    fn heartbeat_from(id: &Identity) -> RaftMessage {
        RaftMessage::AppendEntries {
            term: 4,
            leader_id: NodeId(id.node_id()),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        }
    }

    fn decode_from(bytes: &[u8], id: &Identity) -> WireMessage {
        transport::decode(bytes, "room-a", &id.node_id(), peer_auth::now_ms())
            .expect("a well-formed signed message decodes")
    }

    fn gate(m: &mut Membership, msg: WireMessage, id: &Identity) -> Inbound {
        gate_inbound(m, msg, &id.node_id(), Instant::now(), |_| true, |_| true)
    }

    #[test]
    fn raft_from_an_unverified_peer_is_ignored_until_its_hello() {
        let peer = crate::identity::for_test();
        let mut m = Membership::new("self".into());
        let now = peer_auth::now_ms();

        // Correctly signed, but the sender never proved membership.
        let rpc =
            transport::encode_raft_message("room-a", &peer, 1, 1, now, &heartbeat_from(&peer))
                .unwrap();
        assert!(matches!(
            gate(&mut m, decode_from(&rpc, &peer), &peer),
            Inbound::Nothing
        ));

        let hello = transport::encode_hello("room-a", &peer);
        assert!(matches!(
            gate(&mut m, decode_from(&hello, &peer), &peer),
            Inbound::NewPeer
        ));

        let rpc2 =
            transport::encode_raft_message("room-a", &peer, 1, 2, now, &heartbeat_from(&peer))
                .unwrap();
        assert!(matches!(
            gate(&mut m, decode_from(&rpc2, &peer), &peer),
            Inbound::Deliver(_)
        ));
        // The same signed RPC replayed is not delivered twice.
        assert!(matches!(
            gate(&mut m, decode_from(&rpc2, &peer), &peer),
            Inbound::Nothing
        ));
    }

    #[test]
    fn unsigned_legacy_vote_never_reaches_raft() {
        let peer = crate::identity::for_test();
        let payload = bincode::serialize(&RaftMessage::RequestVoteResponse {
            term: 9,
            vote_granted: true,
            voter_id: NodeId(peer.node_id()),
        })
        .unwrap();
        let legacy = serde_json::to_vec(&json!({
            "t": transport::LEGACY_RAFT_TAG,
            "room": "room-a",
            "payload": base64::engine::general_purpose::STANDARD.encode(payload),
        }))
        .unwrap();
        assert!(
            transport::decode(&legacy, "room-a", &peer.node_id(), peer_auth::now_ms()).is_none()
        );
    }

    #[test]
    fn hello_outside_the_allowlist_does_not_join() {
        let peer = crate::identity::for_test();
        let mut m = Membership::new("self".into());
        let hello = transport::encode_hello("room-a", &peer);
        let allow = vec!["did:key:zSomeoneElse".to_string()];
        let inbound = gate_inbound(
            &mut m,
            decode_from(&hello, &peer),
            &peer.node_id(),
            Instant::now(),
            |did| peer_auth::admits(Some(&allow), did),
            |_| true,
        );
        assert!(matches!(inbound, Inbound::Nothing));
        assert!(!m.contains(&peer.node_id()));
    }

    #[test]
    fn replayed_hello_rejected_by_the_registry_does_not_join() {
        let peer = crate::identity::for_test();
        let mut m = Membership::new("self".into());
        let hello = transport::encode_hello("room-a", &peer);
        let inbound = gate_inbound(
            &mut m,
            decode_from(&hello, &peer),
            &peer.node_id(),
            Instant::now(),
            |_| true,
            |_| false,
        );
        assert!(matches!(inbound, Inbound::Nothing));
        assert!(!m.contains(&peer.node_id()));
    }

    #[test]
    fn a_member_cannot_speak_for_another_member() {
        let a = crate::identity::for_test();
        let b = crate::identity::for_test();
        let mut m = Membership::new("self".into());
        for id in [&a, &b] {
            let hello = transport::encode_hello("room-a", id);
            assert!(matches!(
                gate(&mut m, decode_from(&hello, id), id),
                Inbound::NewPeer
            ));
        }
        // `a` signs a heartbeat naming `b` as leader: rejected at decode,
        // whichever transport id it arrives under.
        let forged = RaftMessage::AppendEntries {
            term: 4,
            leader_id: NodeId(b.node_id()),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        let now = peer_auth::now_ms();
        let bytes = transport::encode_raft_message("room-a", &a, 1, 1, now, &forged).unwrap();
        assert!(transport::decode(&bytes, "room-a", &a.node_id(), now).is_none());
        assert!(transport::decode(&bytes, "room-a", &b.node_id(), now).is_none());
    }
}
