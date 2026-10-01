//! In-order dispatch of raw JOIN/LEAVE/RAW events into manager state and
//! registered handlers.
//!
//! Ported verbatim from `p2p/src/rtc/manager/event.rs`, with two seams (see
//! `TUNNEL_INTEGRATION_CONTRACT.md`):
//! - the outbound sends this module makes on a peer's join (Role,
//!   Capabilities) go through `crate::net::send_direct`, scoped to
//!   `inner.room`, instead of `mistlib::send_message_direct` -- `crate::net`
//!   is the only thing under `src/tunnel/` allowed to touch mistlib's
//!   session-scoped send calls directly;
//! - `crate::rtc::TunnelMessage` / `crate::forward_args::split_node_scope`
//!   become `crate::tunnel::wire::TunnelMessage` /
//!   `crate::tunnel::forward_args::split_node_scope`.
//!
//! The room-level filtering seam 2 of the contract requires (ignore events
//! from a room this manager isn't currently addressing) happens one layer
//! up, in the `crate::net::register_room_handler` closure registered by
//! `manager::RTCManagerHandle::new` -- this module's [`dispatch_event`]
//! itself has no concept of "room" (mistlib's original single-room raw
//! handler didn't either), so it is ported unchanged below.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use super::payload::P2pPayload;
use super::state::{PeerRole, RTCManagerInner};
use crate::tunnel::forward_args::split_node_scope;
use crate::tunnel::wire::TunnelMessage;

/// A membership or data-plane event, queued for in-order processing by
/// [`run_payload_worker`].
pub(super) enum RawEvent {
    Join { peer_id: String },
    Leave { peer_id: String },
    Payload { from: String, data: Vec<u8> },
}

/// Payload backlog caps. Payload events are the only peer-controlled
/// volume on this queue; Join/Leave are never counted or dropped (their rate
/// is bounded by room membership and losing one would desync peer state).
const MAX_PENDING_PAYLOADS: usize = 4096;
const MAX_PENDING_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

/// Backlog accounting shared by both ends of the queue.
#[derive(Default)]
struct Backlog {
    payloads: AtomicUsize,
    bytes: AtomicUsize,
    dropped: AtomicU64,
}

/// Producer half. Still a single FIFO channel so Join/Leave/Payload keep
/// mistlib's delivery order end to end, but payloads are admitted only while
/// the not-yet-processed backlog is under [`MAX_PENDING_PAYLOADS`] /
/// [`MAX_PENDING_PAYLOAD_BYTES`]; beyond that they are dropped (and counted)
/// so this never blocks mistlib's dispatch thread nor grows without bound.
#[derive(Clone)]
pub(super) struct RawEventSender {
    tx: UnboundedSender<RawEvent>,
    backlog: Arc<Backlog>,
}

pub(super) struct RawEventReceiver {
    rx: UnboundedReceiver<RawEvent>,
    backlog: Arc<Backlog>,
}

pub(super) fn raw_event_channel() -> (RawEventSender, RawEventReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let backlog = Arc::new(Backlog::default());
    (
        RawEventSender {
            tx,
            backlog: backlog.clone(),
        },
        RawEventReceiver { rx, backlog },
    )
}

impl RawEventSender {
    /// Never blocks. Join/Leave always enqueue; a payload is dropped when the
    /// backlog is full. Returns whether the event was queued.
    fn send(&self, event: RawEvent) -> bool {
        self.send_bounded(event, MAX_PENDING_PAYLOADS, MAX_PENDING_PAYLOAD_BYTES)
    }

    fn send_bounded(&self, event: RawEvent, max_payloads: usize, max_bytes: usize) -> bool {
        let len = match &event {
            RawEvent::Payload { data, .. } => {
                let len = data.len();
                let payloads = self.backlog.payloads.load(Ordering::Acquire);
                let bytes = self.backlog.bytes.load(Ordering::Acquire);
                if payloads >= max_payloads || bytes.saturating_add(len) > max_bytes {
                    let dropped = self.backlog.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                    if dropped == 1 || dropped.is_multiple_of(1000) {
                        tracing::warn!(
                            dropped,
                            payloads,
                            bytes,
                            "tunnel event backlog full; dropping payload"
                        );
                    }
                    return false;
                }
                self.backlog.payloads.fetch_add(1, Ordering::AcqRel);
                self.backlog.bytes.fetch_add(len, Ordering::AcqRel);
                Some(len)
            }
            _ => None,
        };
        if self.tx.send(event).is_err() {
            if let Some(len) = len {
                self.backlog.payloads.fetch_sub(1, Ordering::AcqRel);
                self.backlog.bytes.fetch_sub(len, Ordering::AcqRel);
            }
            return false;
        }
        true
    }

    #[cfg(test)]
    pub(super) fn dropped(&self) -> u64 {
        self.backlog.dropped.load(Ordering::Relaxed)
    }
}

impl RawEventReceiver {
    pub(super) async fn recv(&mut self) -> Option<RawEvent> {
        let event = self.rx.recv().await?;
        if let RawEvent::Payload { data, .. } = &event {
            self.backlog.payloads.fetch_sub(1, Ordering::AcqRel);
            self.backlog.bytes.fetch_sub(data.len(), Ordering::AcqRel);
        }
        Some(event)
    }
}

/// The event-ingestion entry point: invoked synchronously, in strict FIFO
/// order, from mistlib's single dispatch thread, via the room-filtering
/// wrapper `manager::RTCManagerHandle::new` registers with
/// `crate::net::register_room_handler`.
///
/// `EVENT_JOIN`, `EVENT_LEAVE`, and `EVENT_RAW`/`EVENT_OVERLAY` (tunnel/stdio
/// payloads) all go through `tx`, a FIFO channel (payloads bounded, see
/// [`RawEventSender`]; Join/Leave never dropped) drained by a single
/// FIFO worker (`run_payload_worker`) that awaits each event to completion
/// before starting the next. `UnboundedSender::send` is synchronous and
/// order-preserving, so the enqueue order here matches mistlib's delivery
/// order, and the worker's sequential processing preserves it end to end.
///
/// Join/leave used to be dispatched via independent `tokio::spawn` calls,
/// which let a rapid leave->rejoin for the same peer execute out of order
/// (task scheduling order on a multi-thread runtime is not guaranteed to
/// match spawn order), sometimes leaving a live peer absent from manager
/// state, or a late leave wiping capabilities a since-completed rejoin had
/// just restored. Routing everything through one FIFO worker removes that
/// race: this function itself never blocks/awaits, preserving the
/// non-blocking contract mistlib's dispatch thread requires.
pub(super) fn dispatch_event(
    weak: &Weak<RTCManagerInner>,
    tx: &RawEventSender,
    message_type: u32,
    from: String,
    data: Vec<u8>,
) {
    // Cheap liveness check so we don't queue work for a manager that was
    // dropped without calling `close` (which would otherwise leave the
    // channel filling up with no worker draining it, since the worker also
    // exits once `weak` fails to upgrade).
    if weak.upgrade().is_none() {
        return;
    }

    let event = match message_type {
        mistlib::EVENT_JOIN => RawEvent::Join { peer_id: from },
        mistlib::EVENT_LEAVE => RawEvent::Leave { peer_id: from },
        mistlib::EVENT_RAW | mistlib::EVENT_OVERLAY => RawEvent::Payload { from, data },
        _ => return,
    };
    tx.send(event);
}

/// Drains `rx` and processes one event to completion (`.await`) before
/// starting the next, preserving mistlib's original delivery order across
/// join/leave/payload events alike. Exits once `weak` can no longer be
/// upgraded (the manager was dropped without calling `close`); the channel
/// itself is never explicitly closed the way upstream's could be via
/// `mistlib::clear_raw_handler` (see `manager.rs`'s "never clears the room
/// handler" seam), so this liveness check is the only way the worker task
/// ever stops.
pub(super) async fn run_payload_worker(weak: Weak<RTCManagerInner>, mut rx: RawEventReceiver) {
    while let Some(event) = rx.recv().await {
        let Some(inner) = weak.upgrade() else {
            break;
        };
        match event {
            RawEvent::Join { peer_id } => handle_join(inner, peer_id).await,
            RawEvent::Leave { peer_id } => handle_leave(inner, peer_id).await,
            RawEvent::Payload { from, data } => handle_payload(inner, from, data).await,
        }
    }
}

pub(super) async fn handle_join(inner: Arc<RTCManagerInner>, peer_id: String) {
    if peer_id == inner.self_id {
        return;
    }

    let epoch = {
        let mut epochs = inner.peer_epochs.write().await;
        let epoch = epochs.entry(peer_id.clone()).or_insert(0);
        *epoch += 1;
        *epoch
    };

    // A fresh JOIN starts a new session for this peer_id: drop whatever
    // role/capabilities we cached for it so a stale prior session's
    // advertisements can't linger. The peer (re-)broadcasts Role/Capabilities
    // once joined, which repopulates these via `handle_payload`.
    inner.peer_roles.write().await.remove(&peer_id);
    inner.peer_forward_keys.write().await.remove(&peer_id);
    inner.graph.forget_peer(&peer_id).await;

    inner.peers.write().await.insert(peer_id.clone());
    notify(&inner.peer_conn_handlers, peer_id.clone()).await;
    notify(&inner.tunnel_open_handlers, peer_id.clone()).await;
    inner.stdio_opened.write().await.insert(peer_id.clone());
    notify(&inner.stdio_open_handlers, peer_id.clone()).await;
    notify_epoch(&inner.peer_join_handlers, peer_id.clone(), epoch).await;

    let data = match serde_json::to_vec(&P2pPayload::Role {
        role: inner.self_role.as_str().to_string(),
    }) {
        Ok(data) => data,
        Err(_) => return,
    };
    let room = inner.room.read().await.clone();
    let _ = crate::net::send_direct(&room, &peer_id, data).await;
    send_capabilities_to_peer(&inner, &room, peer_id).await;
}

pub(super) async fn handle_leave(inner: Arc<RTCManagerInner>, peer_id: String) {
    // `peer_roles`/`peer_forward_keys` are intentionally retained across a
    // leave: mistlib can recover a session for the same peer_id without the
    // remote process restarting, and wiping these here would permanently
    // blind `select_server_peer_for` to a peer that never really left. Only
    // `peers` (current-presence) is cleared; routing filters on that, and a
    // fresh JOIN resets the retained state (see `handle_join`).
    inner.peers.write().await.remove(&peer_id);
    inner.stdio_opened.write().await.remove(&peer_id);
    inner.graph.forget_peer(&peer_id).await;
    let epoch = inner
        .peer_epochs
        .read()
        .await
        .get(&peer_id)
        .copied()
        .unwrap_or(0);
    notify(&inner.tunnel_close_handlers, peer_id.clone()).await;
    notify(&inner.stdio_close_handlers, peer_id.clone()).await;
    notify_epoch(&inner.peer_leave_handlers, peer_id, epoch).await;
}

pub(super) async fn handle_payload(inner: Arc<RTCManagerInner>, peer_id: String, data: Vec<u8>) {
    if peer_id == inner.self_id {
        return;
    }

    // Receiving any message proves the peer is connected. `EVENT_JOIN` only
    // fires for peers that join after us, so the later-joining side would
    // otherwise never register peers that were already in the room.
    if inner.peers.write().await.insert(peer_id.clone()) {
        notify(&inner.peer_conn_handlers, peer_id.clone()).await;
        notify(&inner.tunnel_open_handlers, peer_id.clone()).await;
    }

    // Silently ignore anything that isn't our own P2pPayload envelope: this
    // handler receives every raw event for every room this process has
    // joined, and tc-chat (JSON tagged `type`) / ai (JSON with `v`+`type`)
    // payloads land on the same wire (see `TUNNEL_INTEGRATION_CONTRACT.md`
    // seam 2).
    let Ok(payload) = serde_json::from_slice::<P2pPayload>(&data) else {
        return;
    };

    match payload {
        P2pPayload::Graph { room, message } => {
            let current_room = inner.room.read().await;
            if *current_room != room
                || inner
                    .graph
                    .suspended
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return;
            }
            inner.graph.receive(peer_id, message).await;
        }
        P2pPayload::Role { role } => {
            inner
                .peer_roles
                .write()
                .await
                .insert(peer_id, PeerRole::from_str(&role));
        }
        P2pPayload::Capabilities { forwards } => {
            inner
                .peer_forward_keys
                .write()
                .await
                .insert(peer_id, forwards.into_iter().collect());
        }
        P2pPayload::Chat { text } => {
            let handlers = inner.chat_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), text.clone());
            }
        }
        P2pPayload::Tunnel { data } => {
            if !inner.graph.allows_traffic(&peer_id).await {
                return;
            }
            let tunnel_msg = serde_json::from_slice::<TunnelMessage>(&data).ok();
            let default_target = if tunnel_msg.as_ref().is_some_and(|msg| msg.target.is_empty()) {
                inner.default_tunnel_target.read().await.clone()
            } else {
                None
            };
            let handlers = inner.tunnel_msg_handlers.read().await;
            for entry in handlers.iter() {
                if !tunnel_handler_matches(
                    entry.target.as_deref(),
                    tunnel_msg.as_ref(),
                    &default_target,
                    &inner.self_id,
                    &peer_id,
                ) {
                    continue;
                }
                (entry.handler)(peer_id.clone(), data.clone());
            }
        }
        P2pPayload::Stdio { data } => {
            if !inner.graph.allows_traffic(&peer_id).await {
                return;
            }
            // Only an explicit stdio packet (not e.g. a first Chat/Role
            // payload) can open a stdio session for a peer we saw no JOIN for.
            if inner.stdio_opened.write().await.insert(peer_id.clone()) {
                notify(&inner.stdio_open_handlers, peer_id.clone()).await;
            }
            let handlers = inner.stdio_msg_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), data.clone());
            }
        }
        P2pPayload::ForwardRequest {
            req_id,
            proto,
            remote_addr,
            target,
        } => {
            let ev = super::manager::ForwardRequestEvent {
                req_id,
                proto,
                remote_addr,
                target,
            };
            let handlers = inner.forward_request_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), ev.clone());
            }
        }
        P2pPayload::ForwardResponse {
            req_id,
            target,
            accepted,
        } => {
            let ev = super::manager::ForwardResponseEvent {
                req_id,
                target,
                accepted,
            };
            let handlers = inner.forward_response_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), ev.clone());
            }
        }
    }
}

/// Matches an inbound `msg` (sent by `from`) against a handler registered
/// for `handler_target` (`None` means "all targets"). Beyond the exact
/// string match, a node-scoped target (see
/// [`crate::tunnel::forward_args::split_node_scope`]) matches its unscoped
/// counterpart on the node it's pinned to, in either direction:
/// - a message pinned to us (`msg.target == "{handler_target}@{self_id}"`)
///   reaches a handler registered under the plain base target -- a pinned
///   client talking to this serve node;
/// - a message using the plain base target reaches a handler registered
///   under the scoped variant naming the sender
///   (`handler_target == "{msg.target}@{from}"`) -- a serve node replying
///   with its base target to a client that registered under the scoped
///   target it pinned to that node.
///
/// The empty-`msg.target` -> `default_target` rule is unrelated to scoping
/// and is preserved as-is.
fn tunnel_handler_matches(
    handler_target: Option<&str>,
    msg: Option<&TunnelMessage>,
    default_target: &Option<String>,
    self_id: &str,
    from: &str,
) -> bool {
    match handler_target {
        None => true,
        Some(target) => msg.is_some_and(|msg| {
            msg.target == target
                || split_node_scope(&msg.target) == (target, Some(self_id))
                || split_node_scope(target) == (msg.target.as_str(), Some(from))
                || (msg.target.is_empty() && default_target.as_deref() == Some(target))
        }),
    }
}

async fn send_capabilities_to_peer(inner: &Arc<RTCManagerInner>, room: &str, peer_id: String) {
    let forwards = inner
        .self_forward_keys
        .read()
        .await
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let data = match serde_json::to_vec(&P2pPayload::Capabilities { forwards }) {
        Ok(data) => data,
        Err(_) => return,
    };
    let _ = crate::net::send_direct(room, &peer_id, data).await;
}

async fn notify(handlers: &RwLock<Vec<super::state::PeerHandler>>, peer_id: String) {
    let handlers = handlers.read().await;
    for h in handlers.iter() {
        h(peer_id.clone());
    }
}

async fn notify_epoch(
    handlers: &RwLock<Vec<super::state::PeerEpochHandler>>,
    peer_id: String,
    epoch: u64,
) {
    let handlers = handlers.read().await;
    for h in handlers.iter() {
        h(peer_id.clone(), epoch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(len: usize) -> RawEvent {
        RawEvent::Payload {
            from: "p".into(),
            data: vec![0; len],
        }
    }

    #[tokio::test]
    async fn payloads_are_dropped_past_the_cap_but_join_leave_never_are() {
        let (tx, mut rx) = raw_event_channel();
        assert!(tx.send_bounded(payload(1), 2, 1024));
        assert!(tx.send_bounded(payload(1), 2, 1024));
        assert!(!tx.send_bounded(payload(1), 2, 1024));
        assert_eq!(tx.dropped(), 1);
        // Control events pass even while the payload backlog is full.
        assert!(tx.send_bounded(
            RawEvent::Join {
                peer_id: "a".into()
            },
            2,
            1024
        ));
        assert!(tx.send_bounded(
            RawEvent::Leave {
                peer_id: "a".into()
            },
            2,
            1024
        ));
        // FIFO order is preserved across kinds.
        assert!(matches!(rx.recv().await, Some(RawEvent::Payload { .. })));
        assert!(matches!(rx.recv().await, Some(RawEvent::Payload { .. })));
        assert!(matches!(rx.recv().await, Some(RawEvent::Join { .. })));
        assert!(matches!(rx.recv().await, Some(RawEvent::Leave { .. })));
        // Draining frees the budget again.
        assert!(tx.send_bounded(payload(1), 2, 1024));
    }

    #[tokio::test]
    async fn payload_bytes_are_capped() {
        let (tx, mut rx) = raw_event_channel();
        assert!(tx.send_bounded(payload(600), 100, 1000));
        assert!(!tx.send_bounded(payload(600), 100, 1000));
        assert!(rx.recv().await.is_some());
        assert!(tx.send_bounded(payload(600), 100, 1000));
    }
}
