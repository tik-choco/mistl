//! Shared p2p transport over mistlib's process-wide engine singleton.
//!
//! mistlib has exactly one engine, one raw-message handler slot, and one
//! media-track handler slot per process -- but, as of the current mistlib,
//! *many* simultaneously-joined rooms (`join_room` is additive: each call
//! opens a new session alongside any others already joined). This module
//! reflects that split: it initializes the engine, registers the single
//! `register_raw_handler` callback, and wires up media-track delivery
//! exactly once per process, while tracking the *set* of rooms joined so
//! far and joining a room only the first time it's requested. That lets
//! independent daemon services (mailbox, ai, stream relay) each live in
//! their own room concurrently, or share one -- caller's choice.
//!
//! Modules coexist on the wire by shape: each handler parses inbound bytes
//! against its own schema and silently ignores what it can't parse
//! (mailbox wire messages are JSON tagged by `t`; the ai protocol is JSON
//! with `v: 1` + `type`). This is unchanged by multi-room support, since
//! the raw handler fan-out is still global across every joined room.

use std::collections::HashSet;
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
/// Rooms joined so far this process. Guarded by an async mutex (rather
/// than `std::sync::RwLock`) because the check-then-join-then-insert
/// sequence in [`ensure_started`] spans an `.await`.
static ROOMS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
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
        if !rooms.contains(&room) {
            let to_join = room.clone();
            tokio::task::spawn_blocking(move || mistlib::app::join_room(to_join))
                .await
                .context("net: joining room")?;
            rooms.insert(room.clone());
        }
    }

    Ok(Arc::new(Transport {
        node_id: engine.node_id.clone(),
        room,
    }))
}

/// Register a module's event handler. May be called before or after
/// [`ensure_started`]; handlers receive every engine event -- from every
/// joined room -- from registration onward, and never get unregistered.
pub fn register_handler(handler: impl Fn(u32, &str, &[u8]) + Send + Sync + 'static) {
    HANDLERS.write().expect("net handler registry poisoned").push(Box::new(handler));
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
