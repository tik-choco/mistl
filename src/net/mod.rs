//! Shared p2p transport over mistlib's process-wide engine singleton.
//!
//! mistlib supports exactly one engine, one raw-message handler slot, and
//! one room per process (`join_room` merely overwrites the signaling-filter
//! room id -- there is no multi-room membership). This module owns all
//! three so that multiple daemon services (mailbox, ai) can share them:
//! it initializes the engine once, joins a single room once, registers the
//! single `register_raw_handler` callback once, and fans every event out to
//! per-module handlers registered via [`register_handler`].
//!
//! Modules coexist on the wire by shape: each handler parses inbound bytes
//! against its own schema and silently ignores what it can't parse
//! (mailbox wire messages are JSON tagged by `t`; the ai protocol is JSON
//! with `v: 1` + `type`).

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::json;
use tokio::sync::OnceCell;
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

/// The started transport: engine initialized, room joined.
pub struct Transport {
    /// Our node id on the mist network (16 hex chars of sha256(did)).
    pub node_id: String,
    /// The single room this process is a member of.
    pub room: String,
}

static TRANSPORT: OnceCell<Arc<Transport>> = OnceCell::const_new();
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

/// Start the shared transport (identity load, engine init, raw-handler
/// registration, room join) on first call. Subsequent calls return the
/// running transport -- and fail if they ask for a *different* room, since
/// mistlib can only be in one room per process.
pub async fn ensure_started(state: &Arc<AppState>, room: String) -> Result<Arc<Transport>> {
    let transport = TRANSPORT
        .get_or_try_init(|| async { start(state, room.clone()).await })
        .await?;
    if transport.room != room {
        bail!(
            "p2p transport already joined room {:?}; mistlib supports one room per process. \
             Set [mailbox] room_id and [ai] room_id to the same value (or leave one unset).",
            transport.room
        );
    }
    Ok(transport.clone())
}

/// Register a module's event handler. May be called before or after
/// [`ensure_started`]; handlers receive every engine event from
/// registration onward and never get unregistered.
pub fn register_handler(handler: impl Fn(u32, &str, &[u8]) + Send + Sync + 'static) {
    HANDLERS.write().expect("net handler registry poisoned").push(Box::new(handler));
}

async fn start(state: &Arc<AppState>, room: String) -> Result<Arc<Transport>> {
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

    // Wire up media-track delivery before joining the room so every peer
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

    // The one process-wide raw handler: fan out to module handlers.
    mistlib::app::register_raw_handler(move |event_type, from_id, data| {
        let handlers = HANDLERS.read().expect("net handler registry poisoned");
        for handler in handlers.iter() {
            handler(event_type, &from_id, &data);
        }
    });

    {
        let room = room.clone();
        tokio::task::spawn_blocking(move || mistlib::app::join_room(room))
            .await
            .context("net: joining room")?;
    }

    Ok(Arc::new(Transport { node_id, room }))
}

/// Currently-connected peer node ids, bounded by [`NET_TIMEOUT`] (empty on
/// timeout).
pub async fn connected_nodes() -> Vec<String> {
    match tokio::time::timeout(NET_TIMEOUT, mistlib::app::get_connected_nodes_async()).await {
        Ok(nodes) => nodes,
        Err(_) => {
            tracing::debug!("net: get_connected_nodes timed out");
            Vec::new()
        }
    }
}

/// Reliable direct send to one peer, bounded by [`NET_TIMEOUT`].
pub async fn send_direct(to_node: &str, bytes: Vec<u8>) -> Result<()> {
    let outcome = tokio::time::timeout(
        NET_TIMEOUT,
        mistlib::app::send_message_direct(to_node.to_string(), bytes, mistlib::app::DELIVERY_RELIABLE),
    )
    .await
    .context("net: send timed out")?;
    outcome?;
    Ok(())
}
