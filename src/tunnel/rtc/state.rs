//! Manager-internal state: per-peer membership/role/capability maps, event
//! handler registries, and (new in this port -- see seam 8 in
//! `TUNNEL_INTEGRATION_CONTRACT.md`) the room this manager currently
//! addresses.
//!
//! Ported verbatim from `p2p/src/rtc/manager/state.rs`, plus the `room`
//! field. Upstream kept the room entirely outside the manager, because
//! mistlib (as `p2p` used it) only ever addressed one room at a time.
//! mistl's shared `crate::net` layer supports many concurrently-joined
//! rooms per process, so each `RTCManagerHandle` now owns which one it's
//! addressing: `manager::RTCManagerHandle::switch_room` swaps it, and the
//! room-filtered event handler registered in
//! `manager::RTCManagerHandle::new` reads it on every inbound event to tell
//! this manager's traffic apart from another room's (or another tunnel
//! manager's) traffic sharing the same process-wide dispatch fan-out.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::RwLock;

pub(super) type PeerHandler = Arc<dyn Fn(String) + Send + Sync>;
/// A peer join/leave hook: `(peer_id, session epoch)`. See
/// `RTCManagerInner::peer_epochs`.
pub(super) type PeerEpochHandler = Arc<dyn Fn(String, u64) + Send + Sync>;
pub(super) type FwdReqHandler =
    Arc<dyn Fn(String, super::manager::ForwardRequestEvent) + Send + Sync>;
pub(super) type FwdRespHandler =
    Arc<dyn Fn(String, super::manager::ForwardResponseEvent) + Send + Sync>;
/// Handler for `t`-tagged side messages (see [`crate::tunnel::notice`]): `(sender, json)`.
pub(super) type AuxHandler = Arc<dyn Fn(String, serde_json::Value) + Send + Sync>;

pub(super) type DataHandler = Arc<dyn Fn(String, Vec<u8>) + Send + Sync>;

pub(super) struct DataHandlerEntry {
    pub(super) id: u64,
    pub(super) target: Option<String>,
    pub(super) handler: DataHandler,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PeerRole {
    Client,
    Server,
}

impl PeerRole {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            PeerRole::Client => "client",
            PeerRole::Server => "server",
        }
    }

    pub(super) fn from_str(s: &str) -> Self {
        match s {
            "server" => PeerRole::Server,
            _ => PeerRole::Client,
        }
    }
}

pub(super) struct RTCManagerInner {
    pub(super) graph: Arc<crate::tunnel::graph::GraphState>,
    pub(super) self_id: String,
    pub(super) self_role: PeerRole,
    /// The room this manager currently sends to and filters inbound events
    /// against. Behind a lock (rather than e.g. an `ArcSwap`) because
    /// `manager::RTCManagerHandle::switch_room` swaps it out from under a
    /// long-lived manager, and the room-filter closure registered in
    /// `manager::RTCManagerHandle::new` reads it synchronously via
    /// `try_read` -- that closure runs on mistlib's dispatch thread with no
    /// tokio runtime available, so it cannot `.await` a plain `read()` (see
    /// that closure's own comment).
    pub(super) room: RwLock<String>,
    pub(super) peers: RwLock<HashSet<String>>,
    /// Peers for which `stdio_open_handlers` have fired in their current
    /// session (set on `EVENT_JOIN` or on the first `Stdio` payload, cleared on
    /// leave), so a stdio session open is only ever triggered by explicit
    /// stdio traffic or a join -- not by any arbitrary first payload.
    pub(super) stdio_opened: RwLock<HashSet<String>>,
    pub(super) peer_roles: RwLock<HashMap<String, PeerRole>>,
    pub(super) peer_forward_keys: RwLock<HashMap<String, HashSet<String>>>,
    /// Per-peer session epoch, incremented on each `EVENT_JOIN` for that
    /// `peer_id`. Lets later layers (e.g. auth/negotiation purge) tell a
    /// fresh session for a peer apart from one that never disconnected.
    /// `0` means the peer has never joined.
    pub(super) peer_epochs: RwLock<HashMap<String, u64>>,
    pub(super) self_forward_keys: RwLock<HashSet<String>>,
    pub(super) default_tunnel_target: RwLock<Option<String>>,
    /// Serve peer pinned per (unscoped) connect-side target: the first peer
    /// `select_server_peer_for` picks for a target keeps receiving that
    /// target's traffic, so a peer that joins later and advertises the same
    /// target (or `role=server`) cannot take it over. In-memory only; cleared
    /// per target by `clear_route_pin` when the forward is removed.
    pub(super) route_pins: RwLock<HashMap<String, String>>,
    pub(super) next_tunnel_handler_id: AtomicU64,

    pub(super) tunnel_msg_handlers: RwLock<Vec<DataHandlerEntry>>,
    pub(super) stdio_msg_handlers: RwLock<Vec<DataHandler>>,
    pub(super) tunnel_open_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) stdio_open_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) tunnel_close_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) stdio_close_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) peer_conn_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) peer_join_handlers: RwLock<Vec<PeerEpochHandler>>,
    pub(super) peer_leave_handlers: RwLock<Vec<PeerEpochHandler>>,
    pub(super) forward_request_handlers: RwLock<Vec<FwdReqHandler>>,
    pub(super) forward_response_handlers: RwLock<Vec<FwdRespHandler>>,
    pub(super) aux_handlers: RwLock<Vec<AuxHandler>>,
}

impl RTCManagerInner {
    pub(super) fn new(self_id: String, self_role: PeerRole, room: String) -> Self {
        Self {
            graph: Arc::new(crate::tunnel::graph::GraphState::default()),
            self_id,
            self_role,
            room: RwLock::new(room),
            peers: RwLock::new(HashSet::new()),
            stdio_opened: RwLock::new(HashSet::new()),
            peer_roles: RwLock::new(HashMap::new()),
            peer_forward_keys: RwLock::new(HashMap::new()),
            peer_epochs: RwLock::new(HashMap::new()),
            self_forward_keys: RwLock::new(HashSet::new()),
            default_tunnel_target: RwLock::new(None),
            route_pins: RwLock::new(HashMap::new()),
            next_tunnel_handler_id: AtomicU64::new(1),
            tunnel_msg_handlers: RwLock::new(Vec::new()),
            stdio_msg_handlers: RwLock::new(Vec::new()),
            tunnel_open_handlers: RwLock::new(Vec::new()),
            stdio_open_handlers: RwLock::new(Vec::new()),
            tunnel_close_handlers: RwLock::new(Vec::new()),
            stdio_close_handlers: RwLock::new(Vec::new()),
            peer_conn_handlers: RwLock::new(Vec::new()),
            peer_join_handlers: RwLock::new(Vec::new()),
            peer_leave_handlers: RwLock::new(Vec::new()),
            forward_request_handlers: RwLock::new(Vec::new()),
            forward_response_handlers: RwLock::new(Vec::new()),
            aux_handlers: RwLock::new(Vec::new()),
        }
    }
}
