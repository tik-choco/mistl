//! The mistlib-backed peer/session manager for the P2P tunnel: peer role and
//! forward-capability tracking, event routing to the chat/tunnel/stdio/
//! forward-event subscribers registered in `handlers.rs`, and the outbound
//! `send_*`/`publish_*` API the rest of `tunnel` drives.
//!
//! Ported near-verbatim from `p2p/src/rtc/manager.rs`. Everything **except**
//! the seams below is unchanged logic; see `TUNNEL_INTEGRATION_CONTRACT.md`
//! for the full rationale and the frozen API surface every other tunnel
//! worker codes against.
//!
//! ## Seams that changed from upstream
//!
//! 1. **No direct mistlib singleton calls.** Upstream's `new` called
//!    `mistlib::init`/`init_with_config` and `mistlib::join_room` directly,
//!    and read a `P2P_MISTLIB_CONFIG_JSON` env var to build an optional
//!    signaling config override. mistl already has exactly one process-wide
//!    mistlib engine shared by mailbox/ai/stream relay/etc
//!    (`crate::net::ensure_started`, which configures signaling centrally in
//!    `net::start_engine`), so that per-instance override has no meaning
//!    here and is dropped entirely -- there is no `mistlib_config()`
//!    equivalent in this port. `new` instead calls
//!    `crate::net::ensure_started(state, room)`, and takes `self_id` from
//!    the returned `Transport::node_id` (mistl's DID-derived 16-hex-char
//!    node id, an opaque string on the wire, so this is fully
//!    wire-compatible with a `p2p` peer's random per-install uuid).
//! 2. **Room-filtered, room-tagged event registration.** Upstream registered
//!    one process-wide `mistlib::register_raw_handler` (mistlib, as `p2p`
//!    used it, only ever had one room at a time). mistl's `crate::net`
//!    supports many concurrently-joined rooms sharing one dispatch fan-out,
//!    so `new` registers via `crate::net::register_room_handler` instead,
//!    and the registered closure ignores every event whose room doesn't
//!    match this manager's *current* room (see `RTCManagerInner::room` in
//!    `state.rs`) before ever handing it to `event::dispatch_event`. This
//!    also covers mailbox/ai payloads arriving on the same wire:
//!    `event::handle_payload` (ported unchanged) already silently drops
//!    bytes that don't deserialize as a `payload::P2pPayload`.
//! 3. **`send_payload` goes through `crate::net`.** An empty `peer_id` calls
//!    `crate::net::send_broadcast`; otherwise `crate::net::send_direct`.
//!    Both are scoped to [`RTCManagerHandle::current_room`] rather than a
//!    single implicit mistlib room.
//! 4. **`close` only releases this manager's room.** `crate::net::leave_room`
//!    is refcounted and never touches mistlib's single global raw-handler
//!    slot -- unlike upstream's `mistlib::clear_raw_handler()`, which would
//!    silence *every* module sharing the process (mailbox, ai, stream
//!    relay, every other tunnel room). There is deliberately no
//!    `clear_room_handler` call here or anywhere else in this file.
//! 5. **The room lives on the manager, behind a lock**, so `switch_room` can
//!    swap it and a new `current_room()` accessor can read it -- upstream
//!    kept the room entirely outside `RTCManagerHandle` because mistlib (as
//!    `p2p` used it) only ever addressed one.
//!
//! `RTCManagerHandle::new`'s signature is therefore
//! `new(&Arc<AppState>, room, is_server) -> Result<Self>` (upstream:
//! `new(self_id, room_id, is_server) -> Self`, infallible), `switch_room`
//! additionally takes `&Arc<AppState>` and returns `Result<()>`, and there is
//! a new `current_room()` accessor.

use std::sync::Arc;

use anyhow::Result;

use super::event::{self, dispatch_event};
use super::payload::P2pPayload;
use super::state::{PeerRole, RTCManagerInner};
use crate::daemon::AppState;
use crate::tunnel::forward_args::{node_scoped_target, split_node_scope};

/// A peer's proposal to establish a forward: the peer wants to reach
/// `remote_addr` on this node, multiplexed under `target`.
#[derive(Debug, Clone)]
pub struct ForwardRequestEvent {
    pub req_id: String,
    pub proto: String,
    pub remote_addr: String,
    pub target: String,
}

/// A peer's answer to a previously sent [`ForwardRequestEvent`].
#[derive(Debug, Clone)]
pub struct ForwardResponseEvent {
    pub req_id: String,
    pub target: String,
    pub accepted: bool,
}

#[derive(Clone)]
pub struct RTCManagerHandle {
    /// `pub(super)` (upstream: private) so the sibling `tests` module --
    /// which this port's file layout places at `rtc::tests` rather than
    /// upstream's `rtc::manager::tests` (see
    /// `TUNNEL_INTEGRATION_CONTRACT.md`'s file ownership table) -- can still
    /// reach into manager state directly the way upstream's tests did.
    /// Visible throughout `rtc` and its descendants only; never crosses the
    /// `tunnel` module boundary.
    pub(super) inner: Arc<RTCManagerInner>,
}

#[allow(dead_code)]
impl RTCManagerHandle {
    /// Builds a handle backed by fresh in-memory state, without touching
    /// `crate::net` or any real transport. Lets other modules' tests (e.g.
    /// `crate::tunnel::tcp`) register/fire handlers and drive join/leave
    /// notifications deterministically. The room is a fixed placeholder
    /// (`"test-room"`) since a handle built this way never calls
    /// `crate::net`, so no real room filtering is ever exercised against it.
    #[cfg(test)]
    pub fn for_test(self_id: &str) -> Self {
        Self {
            inner: Arc::new(RTCManagerInner::new(
                self_id.to_string(),
                PeerRole::Client,
                "test-room".to_string(),
            )),
        }
    }

    /// Joins `room` via mistl's shared transport and wires this manager's
    /// event pipeline to it. See this module's doc comment (seams 1-2) for
    /// what differs from upstream and why.
    pub async fn new(state: &Arc<AppState>, room: String, is_server: bool) -> Result<Self> {
        let role = if is_server {
            PeerRole::Server
        } else {
            PeerRole::Client
        };

        // `ensure_started` performs the one-time process-wide engine bring-up
        // (identity, signaling config, media-track wiring) the first time
        // *any* module calls it, and joins `room` (additively -- other
        // modules already in `room`, or in a different room, are
        // unaffected). `self_id` is mistl's DID-derived node id, taken from
        // the returned `Transport` rather than generated per-instance the
        // way the standalone `p2p` binary did.
        let transport = crate::net::ensure_started(state, room.clone()).await?;
        let self_id = transport.node_id.clone();

        let handle = Self {
            inner: Arc::new(RTCManagerInner::new(self_id, role, room)),
        };

        let weak = Arc::downgrade(&handle.inner);
        // Single FIFO worker for EVENT_JOIN/EVENT_LEAVE/EVENT_RAW/EVENT_OVERLAY:
        // events reach `dispatch_event` (via the room-filter closure below)
        // in strict order from mistlib's one dispatch thread, and this
        // worker processes each to completion before the next, preserving
        // that order end to end (see `event::dispatch_event` and
        // `event::run_payload_worker`, and the ordering/routing/membership
        // tests under `tests/`).
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(event::run_payload_worker(weak.clone(), rx));

        // `crate::net::register_room_handler` fans every event out to every
        // registered handler for *every* room this process has joined --
        // mailbox, ai, stream relay, and every other `RTCManagerHandle`'s
        // room all share this one fan-out. A manager for one room must
        // ignore another room's traffic, so this closure reads
        // `inner.room` on every single event and drops anything that
        // doesn't match before ever calling `dispatch_event`. `try_read`
        // (not `.read().await`) because this closure runs synchronously on
        // mistlib's dispatch thread with no tokio runtime available; a
        // momentary failure to acquire the lock (e.g. a `switch_room` write
        // in flight) just drops that one event rather than blocking the
        // dispatch thread -- an acceptable trade-off, since `switch_room`
        // re-sends this manager's role once it completes.
        //
        // Bytes that pass the room filter but aren't this manager's own
        // wire shape (mailbox JSON tagged `t`, ai JSON with `v`+`type`) are
        // still silently ignored one layer down, in
        // `event::handle_payload`'s `P2pPayload` deserialize -- ported
        // unchanged from upstream.
        let handler_weak = weak.clone();
        crate::net::register_room_handler(move |event_type, event_room, from, data| {
            let Some(inner) = handler_weak.upgrade() else {
                return;
            };
            let Ok(current_room) = inner.room.try_read() else {
                return;
            };
            if event_room != current_room.as_str() {
                return;
            }
            drop(current_room);
            dispatch_event(
                &handler_weak,
                &tx,
                event_type,
                from.to_string(),
                data.to_vec(),
            );
        });

        handle.send_role_to_all().await;

        Ok(handle)
    }

    pub fn self_id(&self) -> &str {
        &self.inner.self_id
    }

    /// The room this manager currently addresses sends to / filters inbound
    /// events against. New relative to upstream, which kept the room
    /// entirely outside `RTCManagerHandle` (mistlib, as `p2p` used it, only
    /// ever had one).
    pub async fn current_room(&self) -> String {
        self.inner.room.read().await.clone()
    }

    pub async fn get_server_peers(&self) -> Vec<String> {
        self.get_server_peers_for("").await
    }

    /// Roles and forward keys are retained across a peer's leave (see
    /// `event::handle_leave`) so a transient disconnect doesn't lose routing
    /// information, so this filters both branches down to peers currently
    /// present in `peers` -- otherwise a departed peer whose capabilities we
    /// still remember would keep being routed to.
    ///
    /// A node-scoped `target` (`"{base}@{peer_id}"`, see
    /// [`crate::tunnel::forward_args::split_node_scope`]) pins routing to
    /// that one peer: this returns `vec![peer_id]` only if the peer is
    /// currently live AND has advertised the exact scoped target string, and
    /// returns an empty vec otherwise. A scoped target never falls back to
    /// the server-role-peer list below -- that fallback would route pinned
    /// traffic to a node the caller didn't ask for. Unscoped targets keep
    /// their original behavior: exact advertiser match, falling back to
    /// every live server-role peer when nobody advertises the target.
    pub async fn get_server_peers_for(&self, target: &str) -> Vec<String> {
        let live = self.inner.peers.read().await;

        if !target.is_empty() {
            let (_, scope) = split_node_scope(target);
            if let Some(scope) = scope {
                let keys = self.inner.peer_forward_keys.read().await;
                return if live.contains(scope)
                    && keys.get(scope).is_some_and(|k| k.contains(target))
                {
                    vec![scope.to_string()]
                } else {
                    Vec::new()
                };
            }

            let keys = self.inner.peer_forward_keys.read().await;
            let matched = keys
                .iter()
                .filter_map(|(id, keys)| {
                    if keys.contains(target) && live.contains(id) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if !matched.is_empty() {
                return matched;
            }
        }

        let roles = self.inner.peer_roles.read().await;
        roles
            .iter()
            .filter_map(|(id, role)| {
                if *role == PeerRole::Server && live.contains(id) {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Selects a single server peer for the given target, load-balancing across
    /// all peers that advertise the target key using a per-target round-robin
    /// cursor. Peers that disconnect drop out of the advertised key set, so this
    /// also provides failover. Returns `None` when no peer is available yet.
    ///
    /// A node-scoped target (see [`get_server_peers_for`](Self::get_server_peers_for))
    /// always resolves to the same single pinned peer (or `None`) -- the
    /// round-robin cursor is a no-op in that case since there is at most one
    /// candidate.
    pub async fn select_server_peer_for(&self, target: &str) -> Option<String> {
        let mut peers = self.get_server_peers_for(target).await;
        if peers.is_empty() {
            return None;
        }
        // Deterministic ordering so the round-robin cursor is stable regardless
        // of the underlying map iteration order.
        peers.sort();
        let mut cursors = self.inner.peer_rr_cursor.write().await;
        let cursor = cursors.entry(target.to_string()).or_insert(0);
        let idx = *cursor % peers.len();
        *cursor = cursor.wrapping_add(1);
        Some(peers[idx].clone())
    }

    /// The peer's current session epoch: incremented on each `EVENT_JOIN`
    /// for `peer_id`, `0` if the peer has never joined. Lets callers detect
    /// a fresh session for a peer_id (e.g. to purge state scoped to the
    /// prior session) versus a peer that never disconnected.
    pub async fn peer_epoch(&self, peer_id: &str) -> u64 {
        self.inner
            .peer_epochs
            .read()
            .await
            .get(peer_id)
            .copied()
            .unwrap_or(0)
    }

    pub async fn connected_peers(&self) -> Vec<String> {
        let mut peers = self
            .inner
            .peers
            .read()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        peers.sort();
        peers
    }

    pub async fn send_forward_request(&self, peer_id: &str, ev: ForwardRequestEvent) -> Result<()> {
        self.send_payload(
            peer_id,
            P2pPayload::ForwardRequest {
                req_id: ev.req_id,
                proto: ev.proto,
                remote_addr: ev.remote_addr,
                target: ev.target,
            },
        )
        .await
    }

    pub async fn send_forward_response(
        &self,
        peer_id: &str,
        ev: ForwardResponseEvent,
    ) -> Result<()> {
        self.send_payload(
            peer_id,
            P2pPayload::ForwardResponse {
                req_id: ev.req_id,
                target: ev.target,
                accepted: ev.accepted,
            },
        )
        .await
    }

    /// Advertises `target` as servable by this node. When `target` is
    /// unscoped, this also advertises the self-scoped variant
    /// (`"{target}@{self_id}"`, see [`node_scoped_target`]) so a connecting
    /// client can pin to this specific node even though it was started with
    /// a plain, unscoped target. An already-scoped `target` is published
    /// as-is only.
    pub async fn publish_tunnel_target(&self, target: &str) {
        let mut keys = self.inner.self_forward_keys.write().await;
        keys.insert(target.to_string());
        if split_node_scope(target).1.is_none() {
            keys.insert(node_scoped_target(target, &self.inner.self_id));
        }
        drop(keys);
        self.send_capabilities_to_all().await;
    }

    /// Reverses [`publish_tunnel_target`](Self::publish_tunnel_target),
    /// removing the self-scoped variant alongside an unscoped `target` too.
    pub async fn unpublish_tunnel_target(&self, target: &str) {
        let mut keys = self.inner.self_forward_keys.write().await;
        keys.remove(target);
        if split_node_scope(target).1.is_none() {
            keys.remove(&node_scoped_target(target, &self.inner.self_id));
        }
        drop(keys);
        self.send_capabilities_to_all().await;
    }

    pub async fn send_chat_to_all(&self, msg: &str) {
        let _ = self
            .send_payload("", P2pPayload::Chat { text: msg.into() })
            .await;
    }

    pub async fn send_tunnel_to(&self, peer_id: &str, data: Vec<u8>) -> Result<()> {
        self.send_payload(peer_id, P2pPayload::Tunnel { data })
            .await
    }

    pub async fn send_stdio_to(&self, peer_id: &str, data: Vec<u8>) -> Result<()> {
        self.send_payload(peer_id, P2pPayload::Stdio { data }).await
    }

    async fn send_role_to_all(&self) {
        let _ = self
            .send_payload(
                "",
                P2pPayload::Role {
                    role: self.inner.self_role.as_str().to_string(),
                },
            )
            .await;
    }

    async fn send_capabilities_to_all(&self) {
        let forwards = self
            .inner
            .self_forward_keys
            .read()
            .await
            .iter()
            .cloned()
            .collect();
        let _ = self
            .send_payload("", P2pPayload::Capabilities { forwards })
            .await;
    }

    /// Sends `payload` (JSON-encoded, as the `P2pPayload` wire envelope) to
    /// `peer_id` scoped to [`current_room`](Self::current_room), or
    /// broadcasts it room-wide when `peer_id` is empty -- the same
    /// empty-string-means-broadcast convention `crate::net::send_broadcast`
    /// and mistlib-core's underlying send both use. Upstream called
    /// `mistlib::send_message_direct` directly (no room scoping, since
    /// mistlib, as `p2p` used it, only ever addressed one room); this
    /// crate's integration contract forbids that anywhere under
    /// `src/tunnel/`, so this goes through `crate::net` instead (see this
    /// module's doc comment, seam 3).
    async fn send_payload(&self, peer_id: &str, payload: P2pPayload) -> Result<()> {
        let data = serde_json::to_vec(&payload)?;
        let room = self.current_room().await;
        if peer_id.is_empty() {
            crate::net::send_broadcast(&room, data).await
        } else {
            crate::net::send_direct(&room, peer_id, data).await
        }
    }

    /// Releases this manager's interest in its current room via
    /// `crate::net::leave_room` -- refcounted, so the underlying mistlib
    /// session is only actually torn down once every other holder of that
    /// room (if any; e.g. mailbox/ai sharing it) has also released it.
    ///
    /// Deliberately **never** calls anything like upstream's
    /// `mistlib::clear_raw_handler()`: mistl's raw handler slot is
    /// process-wide and shared by every module (mailbox, ai, stream relay,
    /// every other tunnel room), so clearing it here would silence all of
    /// them, not just this manager's traffic. There is no equivalent
    /// `clear_room_handler` to call either -- the room-filter closure
    /// registered in `new` simply stops matching anything once nothing is
    /// joined here, which is sufficient.
    pub async fn close(&self) {
        let room = self.current_room().await;
        let _ = crate::net::leave_room(&room).await;
    }

    /// Switches this manager to a different room without tearing down the
    /// rest of its state (registered handlers, peer role, self_id).
    ///
    /// Upstream achieved the equivalent by pairing `mistlib::leave_room()`/
    /// `join_room()` directly rather than rebuilding a fresh
    /// `RTCManagerHandle`: rebuilding would have called `mistlib::init`
    /// again, which initializes mistlib's process-wide singleton engine and
    /// is not meant to run more than once per process. The same constraint
    /// holds here, one layer further down -- `crate::net::ensure_started` is
    /// what now owns that one-time `mistlib::init`, and it happily reuses
    /// the already-initialized engine on every call -- so there's still no
    /// need to rebuild `RTCManagerHandle`. This instead calls
    /// `crate::net::leave_room(old)` (refcounted: a no-op for mistlib itself
    /// if another module still holds `old`) followed by
    /// `crate::net::ensure_started(state, new)`, then updates `inner.room`
    /// so both `current_room()` and the room-filter closure registered in
    /// `new` immediately start treating `new` as this manager's room.
    ///
    /// Every handler registered via `on_*` stays wired (they live on this
    /// same `inner`, untouched here). Only per-room membership state is
    /// reset: leaving/rejoining tears the underlying session down directly
    /// rather than emitting a real `EVENT_LEAVE` per peer (see
    /// `event::handle_leave`), so `peers`/`peer_roles`/`peer_forward_keys`/
    /// `peer_epochs` are cleared by hand here instead. Callers that need the
    /// equivalent of a per-peer leave notification (e.g.
    /// `SessionContext::switch_room`, which purges pending auth/forward
    /// state scoped to peers about to become unreachable) must snapshot
    /// `connected_peers()` before calling this.
    pub async fn switch_room(&self, state: &Arc<AppState>, room: String) -> Result<()> {
        let old_room = self.current_room().await;
        crate::net::leave_room(&old_room).await?;
        crate::net::ensure_started(state, room.clone()).await?;
        *self.inner.room.write().await = room;

        self.inner.peers.write().await.clear();
        self.inner.peer_roles.write().await.clear();
        self.inner.peer_forward_keys.write().await.clear();
        self.inner.peer_epochs.write().await.clear();
        self.send_role_to_all().await;
        Ok(())
    }
}

/// Backward-compatible alias: the standalone `p2p` crate's
/// `rtc::RTCManager` name for the same type, kept so ported call sites (and
/// this port's frozen API surface) can use either name.
pub type RTCManager = RTCManagerHandle;
