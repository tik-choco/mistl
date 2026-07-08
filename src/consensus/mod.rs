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
            Arc::new(MistlRaftTransport::new(room.clone()));
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

    /// The current leader's node id, if known.
    pub fn leader(&self) -> Option<String> {
        self.raft.leader_hint().map(|id| id.0)
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

/// Wires the shared `crate::net` raw-message handler for this instance:
/// demuxes Raft RPCs (fed straight into the driver) and relay hellos
/// (tracked in `membership`, promoted to `RaftNode::add_peer` on first
/// sight), and clears departed peers on `EVENT_LEAVE`. Entirely
/// synchronous -- no tokio runtime is available on `crate::net`'s
/// dispatch thread, and nothing here needs one (channel sends and
/// `std::sync::Mutex`/`watch::Sender` don't require an executor).
fn register_net_handler(consensus: Arc<RelayConsensus>) {
    crate::net::register_handler(move |event_type, from_id, data| match event_type {
        crate::net::EVENT_RAW => match transport::decode(data, &consensus.room) {
            Some(WireMessage::Raft(msg)) => {
                consensus.raft.deliver(NodeId(from_id.to_string()), msg);
            }
            Some(WireMessage::Hello { node }) => {
                let is_new = consensus
                    .membership
                    .lock()
                    .expect("consensus membership lock poisoned")
                    .record_hello(&node, Instant::now());
                if is_new {
                    consensus.raft.add_peer(NodeId(node));
                    publish_view(&consensus);
                }
            }
            None => {} // Not our envelope shape or a different room's; ignore.
        },
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
/// module docs -- so, like the mailbox/ai precedents, this may also reach
/// peers in this process's *other* joined rooms; `send_direct` is scoped
/// to `room` regardless, so it simply fails silently for anyone not
/// actually reachable there).
async fn broadcast_hello(consensus: &RelayConsensus) {
    let bytes = transport::encode_hello(&consensus.room, &consensus.node_id);
    for node in crate::net::connected_nodes().await {
        let _ = crate::net::send_direct(&consensus.room, &node, bytes.clone()).await;
    }
}

fn expire_stale_peers(consensus: &Arc<RelayConsensus>) {
    let stale = consensus
        .membership
        .lock()
        .expect("consensus membership lock poisoned")
        .expire(Instant::now(), PEER_TIMEOUT);
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
