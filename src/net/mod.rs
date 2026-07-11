//! Shared p2p transport over mistlib's process-wide engine singleton.
//!
//! mistlib has exactly one engine, one raw-message handler slot, and one
//! media-track handler slot per process -- but, as of the current mistlib,
//! *many* simultaneously-joined rooms (`join_room` is additive: each call
//! opens a new session alongside any others already joined). This module
//! reflects that split: it initializes the engine, registers the single
//! `register_raw_handler` callback, and wires up media-track delivery
//! exactly once per process, while refcounting the rooms joined so far and
//! only actually joining/leaving mistlib's engine when a room's count
//! crosses 0. That lets independent daemon services (mailbox, ai, stream
//! relay) each live in their own room concurrently, or share one, without
//! one service's [`leave_room`] call kicking another out of a room they
//! both happen to use.
//!
//! Most callers (mailbox, ai, stream relay) join once at service start and
//! hold their room for the process lifetime, never calling [`leave_room`].
//! Storage is the exception: it re-resolves its configured room on every
//! command and calls [`leave_room`]/[`ensure_started`] to hop rooms live,
//! so `storage.room_id` can change without a daemon restart.
//!
//! Modules coexist on the wire by shape: each handler parses inbound bytes
//! against its own schema and silently ignores what it can't parse
//! (mailbox wire messages are JSON tagged by `t`; the ai protocol is JSON
//! with `v: 1` + `type`). This is unchanged by multi-room support, since
//! the raw handler fan-out is still global across every joined room.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;
use tokio::sync::{Mutex, OnceCell};
use tracing::warn;

use crate::daemon::AppState;

/// Default rendezvous room when neither `[mailbox]` nor `[ai]` configures
/// one. Kept at the historical mailbox default for compatibility.
pub const DEFAULT_ROOM: &str = "mistl-mailbox-v1";

/// Cap on any single mistlib network operation (connection lookup, send)
/// so no daemon command can hang indefinitely on an unreachable signaling
/// relay or peer.
pub const NET_TIMEOUT: Duration = Duration::from_secs(5);

/// Event-type constants re-exported for handler implementations.
pub const EVENT_RAW: u32 = mistlib::EVENT_RAW;
pub const EVENT_JOIN: u32 = mistlib::EVENT_JOIN;
pub const EVENT_LEAVE: u32 = mistlib::EVENT_LEAVE;

/// A per-module event handler: `(event_type, from_node_id, payload)`.
///
/// Invoked synchronously on mistlib's dispatch thread (no tokio runtime),
/// so implementations must be cheap and spawn async work onto a captured
/// runtime handle themselves.
pub type EventHandler = Box<dyn Fn(u32, &str, &[u8]) + Send + Sync>;

/// The process-wide engine: identity resolved, mistlib initialized, raw
/// handler and media-track handler registered. Exactly one of these ever
/// exists; rooms are joined and tracked independently (see [`ROOMS`]).
struct Engine {
    node_id: String,
}

/// A joined room, returned by [`ensure_started`] so callers can address
/// sends to the room they asked for.
pub struct Transport {
    /// Our node id on the mist network (16 hex chars of sha256(did)).
    pub node_id: String,
    /// The room this call joined (or already-joined room it resolved to).
    pub room: String,
}

static ENGINE: OnceCell<Arc<Engine>> = OnceCell::const_new();
/// Rooms joined so far this process, refcounted by number of callers
/// holding them (so [`leave_room`] only actually leaves once the last
/// holder releases it). Guarded by an async mutex (rather than
/// `std::sync::RwLock`) because the check-then-join/leave-then-update
/// sequences in [`ensure_started`]/[`leave_room`] span an `.await`.
static ROOMS: LazyLock<Mutex<HashMap<String, usize>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static HANDLERS: RwLock<Vec<EventHandler>> = RwLock::new(Vec::new());

/// Optional consumer of remote WebRTC media tracks (the stream relay).
/// mistlib delivers track events from engine start; while no consumer is
/// registered they are dropped here, so a relay started later only sees
/// tracks from peers that (re)negotiate after it subscribed -- which is the
/// normal case, since tc-chat renegotiates whenever a share starts.
static MEDIA_CONSUMER: RwLock<Option<tokio::sync::mpsc::UnboundedSender<mistlib::MediaTrackEvent>>> =
    RwLock::new(None);

/// Route media track events to `tx` (or drop them again when `None`).
pub fn set_media_consumer(tx: Option<tokio::sync::mpsc::UnboundedSender<mistlib::MediaTrackEvent>>) {
    *MEDIA_CONSUMER.write().expect("media consumer lock poisoned") = tx;
}

/// Ensure the shared transport is up and `room` is joined, then return a
/// [`Transport`] describing that room.
///
/// The one-time engine setup (identity load, mistlib init, media-track and
/// raw-handler registration) happens on the very first call across the
/// whole process, regardless of which room it names. Every call -- first or
/// not -- then joins `room` if this process hasn't joined it yet. Joining a
/// room already joined by a different caller (e.g. mailbox and ai sharing
/// the default room) is a no-op; joining a genuinely new room is legal and
/// additive, so mailbox, ai, and the stream relay can each sit in their own
/// room at the same time.
pub async fn ensure_started(state: &Arc<AppState>, room: String) -> Result<Arc<Transport>> {
    let engine = ENGINE
        .get_or_try_init(|| async { start_engine(state).await })
        .await?;

    {
        let mut rooms = ROOMS.lock().await;
        let count = rooms.entry(room.clone()).or_insert(0);
        if *count == 0 {
            let to_join = room.clone();
            tokio::task::spawn_blocking(move || mistlib::app::join_room(to_join))
                .await
                .context("net: joining room")?;
        }
        *count += 1;
    }

    Ok(Arc::new(Transport {
        node_id: engine.node_id.clone(),
        room,
    }))
}

/// Release this process's interest in `room`, taken out by an earlier
/// [`ensure_started`] call. Only actually leaves the room (via
/// `mistlib::app::leave_room_id`) once every holder has released it --
/// refcounted so independent services sharing a room (e.g. mailbox and ai
/// both defaulting to [`DEFAULT_ROOM`]) don't kick each other out. A no-op
/// if this process never joined `room`, or already fully released it.
pub async fn leave_room(room: &str) -> Result<()> {
    let mut rooms = ROOMS.lock().await;
    let Some(&count) = rooms.get(room) else {
        return Ok(());
    };
    if count > 1 {
        rooms.insert(room.to_string(), count - 1);
        return Ok(());
    }
    // Last holder: actually leave before dropping our bookkeeping, so a
    // failed leave (e.g. the blocking task panics) leaves `room` still
    // marked held rather than silently forgotten.
    let to_leave = room.to_string();
    tokio::task::spawn_blocking(move || mistlib::app::leave_room_id(to_leave))
        .await
        .context("net: leaving room")?;
    rooms.remove(room);
    Ok(())
}

/// Register a module's event handler. May be called before or after
/// [`ensure_started`]; handlers receive every engine event -- from every
/// joined room -- from registration onward, and never get unregistered.
pub fn register_handler(handler: impl Fn(u32, &str, &[u8]) + Send + Sync + 'static) {
    HANDLERS.write().expect("net handler registry poisoned").push(Box::new(handler));
}

/// A per-module event handler that also knows which room the event came
/// from: `(event_type, room_id, from_node_id, payload)`. See
/// [`register_room_handler`].
pub type RoomEventHandler = Box<dyn Fn(u32, &str, &str, &[u8]) + Send + Sync>;

static ROOM_HANDLERS: RwLock<Vec<RoomEventHandler>> = RwLock::new(Vec::new());

/// Register a room-aware handler: like [`register_handler`], but additionally
/// told which room each event arrived from. Needed by callers that join more
/// than one room and must tell them apart -- e.g. the tc-chat relay
/// (`crate::mailbox::chat_relay`), which may join several tc-chat rooms and
/// must route an inbound wire to the right room's on-disk log.
/// [`register_handler`]'s existing event stream is a fan-out across every
/// joined room with no origin tag, so this is a second, independent
/// registration (both fire for every event) rather than a change to that
/// one's signature.
pub fn register_room_handler(handler: impl Fn(u32, &str, &str, &[u8]) + Send + Sync + 'static) {
    ROOM_HANDLERS.write().expect("net room handler registry poisoned").push(Box::new(handler));
}

/// `mistlib::app::register_event_callback_v2`'s callback: same events as the
/// plain raw handler, tagged with the room_id they occurred in. A free
/// function (not a closure) because `EventCallbackV2` is a bare
/// `extern "C" fn` pointer with no captured state -- state lives in the
/// static [`ROOM_HANDLERS`] instead, the same pattern [`HANDLERS`] uses for
/// [`register_handler`].
///
/// # Safety
/// Called only by mistlib's own event dispatch thread, which guarantees each
/// `(ptr, len)` pair is valid for `len` bytes for the duration of this call.
unsafe extern "C" fn dispatch_room_event(
    event_type: u32,
    room_ptr: *const u8,
    room_len: usize,
    from_ptr: *const u8,
    from_len: usize,
    data_ptr: *const u8,
    data_len: usize,
) {
    let room = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(room_ptr, room_len) });
    let from = String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(from_ptr, from_len) });
    let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
    let handlers = ROOM_HANDLERS.read().expect("net room handler registry poisoned");
    for handler in handlers.iter() {
        handler(event_type, &room, &from, data);
    }
}

/// One-time process-wide setup: identity load, engine init, media-track
/// handler, raw handler. Does *not* join any room -- that's [`ensure_started`]'s
/// job on every call, first or not.
async fn start_engine(state: &Arc<AppState>) -> Result<Arc<Engine>> {
    let identity = crate::identity::current(state)
        .await
        .context("net: loading identity")?;
    let node_id = identity.node_id();

    // Same shape tc-storage's mistStorage.ts uses: explicit Nostr signaling
    // with no relay override, which falls back to mistlib's default public
    // relay list (NostrSignalingConfig::effective_relay_list_url).
    let config_json = json!({
        "signaling": { "mode": "nostr", "nostr": { "relays": [] } }
    })
    .to_string();

    // mistlib's sync app entry points block_on its internal runtime, which
    // panics on a tokio worker thread -- run them on a blocking thread.
    {
        let node_id = node_id.clone();
        tokio::task::spawn_blocking(move || {
            if !mistlib::app::init_with_config(node_id.clone(), config_json.as_bytes()) {
                warn!(
                    "net: init_with_config rejected the default signaling config; falling back to init()"
                );
                mistlib::app::init(node_id, String::new());
            }
        })
        .await
        .context("net: initializing mistlib engine")?;
    }

    // Wire up media-track delivery before joining any room so every peer
    // gets the handler (mistlib only wires peers created after this call).
    // register_media_track_handler block_ons the engine runtime, so it needs
    // a blocking thread like the init call above.
    let (media_tx, mut media_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::task::spawn_blocking(move || {
        if let Err(error) = mistlib::app::register_media_track_handler(media_tx) {
            warn!(%error, "net: media track handler registration failed; stream relay unavailable");
        }
    })
    .await
    .context("net: registering media track handler")?;
    tokio::spawn(async move {
        while let Some(event) = media_rx.recv().await {
            let consumer = MEDIA_CONSUMER.read().expect("media consumer lock poisoned");
            if let Some(tx) = consumer.as_ref() {
                let _ = tx.send(event);
            }
        }
    });

    // The one process-wide raw handler: fan out to module handlers,
    // regardless of which of our joined rooms the event came from.
    mistlib::app::register_raw_handler(move |event_type, from_id, data| {
        let handlers = HANDLERS.read().expect("net handler registry poisoned");
        for handler in handlers.iter() {
            handler(event_type, &from_id, &data);
        }
    });

    // Room-tagged sibling of the raw handler above, for callers that need to
    // know which joined room an event arrived from -- see
    // `register_room_handler`'s doc comment. Both this and the plain raw
    // handler fire for every event; registering this one doesn't change the
    // other's behavior.
    mistlib::app::register_event_callback_v2(dispatch_room_event);

    Ok(Arc::new(Engine { node_id }))
}

/// Currently-connected peer node ids, bounded by [`NET_TIMEOUT`] (empty on
/// timeout). A union across every room this process has joined, deduplicated.
pub async fn connected_nodes() -> Vec<String> {
    match tokio::time::timeout(NET_TIMEOUT, mistlib::app::get_connected_nodes_async()).await {
        Ok(nodes) => nodes,
        Err(_) => {
            tracing::debug!("net: get_connected_nodes timed out");
            Vec::new()
        }
    }
}

/// Rooms this process currently holds (refcount > 0), sorted for a
/// deterministic result. Read for `topology.status`'s dashboard view --
/// e.g. mailbox/ai/stream relay each sitting in their own room, or sharing
/// one.
pub async fn joined_rooms() -> Vec<String> {
    let rooms = ROOMS.lock().await;
    let mut list: Vec<String> = rooms
        .iter()
        .filter(|&(_, &count)| count > 0)
        .map(|(room, _)| room.clone())
        .collect();
    list.sort();
    list
}

/// Best-known connection state for `node_id`, bounded by [`NET_TIMEOUT`] the
/// same way [`connected_nodes`] is. Backed by
/// `mistlib::app::get_connection_state_async`'s "most-connected session
/// wins" union across every room this process has joined -- one of
/// `"disconnected"`, `"connecting"`, `"connected"`, `"reconnecting"`,
/// `"failed"` (mistlib's `ConnectionState` `Display` impl), or `"unknown"` on
/// timeout.
///
/// mistlib exposes no finer-grained per-peer detail than this (no ICE state,
/// RTT, or bitrate, and no room-scoped breakdown) -- this is genuinely all
/// that's observable about one peer's link right now.
pub async fn peer_connection_state(node_id: &str) -> String {
    match tokio::time::timeout(NET_TIMEOUT, mistlib::app::get_connection_state_async(node_id)).await {
        Ok(state) => state,
        Err(_) => {
            tracing::debug!(%node_id, "net: get_connection_state timed out");
            "unknown".to_string()
        }
    }
}

/// Reliable direct send to one peer, scoped to `room` (the peer must be
/// reachable in that specific room's session -- unlike a bare broadcast,
/// there's no reasonable cross-room fallback).
pub async fn send_direct(room: &str, to_node: &str, bytes: Vec<u8>) -> Result<()> {
    mistlib::app::try_send_message_in_room(
        room.to_string(),
        to_node.to_string(),
        &bytes,
        mistlib::app::DELIVERY_RELIABLE,
    )
    .map_err(|err| anyhow::anyhow!("net: send in room {room:?} failed: {err}"))
}

/// Reliable room-wide broadcast (every peer in `room`), scoped the same way
/// [`send_direct`] is. An empty target node id is mistlib-core's broadcast
/// sentinel -- the same convention tc-chat's own web client uses
/// (`node.sendMessage(null, ...)`).
pub async fn send_broadcast(room: &str, bytes: Vec<u8>) -> Result<()> {
    mistlib::app::try_send_message_in_room(
        room.to_string(),
        String::new(),
        &bytes,
        mistlib::app::DELIVERY_RELIABLE,
    )
    .map_err(|err| anyhow::anyhow!("net: broadcast in room {room:?} failed: {err}"))
}
