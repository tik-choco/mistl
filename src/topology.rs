//! `topology.status`: the "who is connected and how" snapshot backing the
//! web dashboard's topology view.
//!
//! This is a read-only join of state other modules already track --
//! `identity` (this node), `net` (joined rooms, connected peers, per-peer
//! connection state), `consensus` (cascade leader election), and `stream`
//! (relay publisher lock / RTSP viewers) -- reusing their existing
//! `*.status`/`*.show` IPC commands (via `Module::handle`) rather than
//! duplicating their internals. Nothing here tracks anything new.
//!
//! ## What's genuinely observable, and what isn't
//!
//! mistlib-native exposes `get_connected_nodes_async` (process-wide, not
//! room-scoped -- a peer connected in *any* room this process joined shows
//! up once; see `net`'s module doc) and `get_connection_state_async` (a
//! "most-connected session wins" union: one of `Disconnected`, `Connecting`,
//! `Connected`, `Reconnecting`, `Failed`). There is no per-peer ICE state,
//! RTT, bitrate, or room-scoped connection list beyond that. So `peers`
//! below answers "who does mistlib currently think we're connected to,
//! anywhere", not "who's in the relay room specifically" -- room/role
//! context comes from the `consensus` (cascade membership, leader/follower)
//! and `stream` (publisher lock, RTSP viewer count) sections layered on top,
//! which *are* room-scoped. No field here is invented: anything mistlib
//! doesn't expose is simply omitted rather than guessed at.
//!
//! Per-room peer membership specifically was checked and is *not*
//! observable from mistlib's public API as vendored: `get_connected_nodes*`
//! unions all sessions, `get_stats()` builds per-room state internally but
//! flattens it before serializing, `SessionCtx`'s transports are
//! `pub(crate)`, and the only room-tagged surface (`EVENT_JOIN`/`EVENT_LEAVE`
//! via `register_event_callback_v2`) is a lossy `extern "C"` delta feed
//! (mistlib counts dropped FFI events) -- deriving membership from it would
//! silently drift. So `modules` below reports each module's *room id* (the
//! same config-or-default resolution the module itself uses) plus whether
//! that room is joined yet, and deliberately does not claim per-room peer
//! lists. The dashboard pairs those room ids with the process-wide `peers`
//! and says so, rather than faking room scoping.

use std::sync::Arc;

use anyhow::{Result, bail};
use serde_json::{Value, json};

use crate::daemon::AppState;

/// IPC entry point for `topology.*` commands.
pub async fn handle(cmd: &str, _args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "topology.status" => status(state).await,
        _ => bail!("unknown command: {cmd}"),
    }
}

/// `topology.status` `{}` -> `{self, rooms, peers, modules, consensus, stream}`:
/// - `self`: `{node_id, did, display_name}` -- this node's identity
/// - `rooms`: joined room ids (`net::joined_rooms`)
/// - `peers`: `[{node_id, state}]` -- every node mistlib currently sees as
///   connected (any joined room), with its best-known connection state
/// - `modules`: `{mailbox: {room, joined}, ai: {room, joined}, store}` --
///   mailbox/ai report a single room id (resolved from config exactly the
///   way the module itself does, without starting it) and whether that room
///   is joined yet; `store` reports every room in `storage.room_ids` instead
///   (the store can join several simultaneously) as `{rooms: [{room,
///   joined}]}`, or `null` when no rooms are configured (purely local
///   store, no network join at all). Deliberately no per-room peer lists --
///   see the module doc for why that isn't observable.
/// - `consensus`: verbatim `consensus.status` shape (cascade leader
///   election: `{active, room?, role?, leader?, peers?}`)
/// - `stream`: verbatim `stream.status` shape (`{running, rtsp_url?,
///   clients?, backend?, flow?, room?, publisher?, cascade?}`)
async fn status(state: &Arc<AppState>) -> Result<Value> {
    let identity = crate::identity::current(state).await?;
    let profile = crate::identity::handle("profile.show", json!({}), state).await?;
    let self_info = json!({
        "node_id": identity.node_id(),
        "did": identity.did(),
        "display_name": profile.get("display_name").cloned().unwrap_or(Value::Null),
    });

    let rooms = crate::net::joined_rooms().await;

    let mut peers = Vec::new();
    for node_id in crate::net::connected_nodes().await {
        let conn_state = crate::net::peer_connection_state(&node_id).await;
        peers.push(json!({ "node_id": node_id, "state": conn_state }));
    }

    // Room resolution mirrors mailbox::service::ensure_started and
    // ai::init_service exactly (config value, ai falling back to mailbox's,
    // then the shared default) -- but reads config only, so asking for the
    // topology never lazily starts a module.
    let config = state.config();
    let module_rooms = ModuleRooms {
        mailbox: config
            .mailbox
            .room_id
            .clone()
            .unwrap_or_else(|| crate::net::DEFAULT_ROOM.to_string()),
        ai: config
            .ai
            .room_id
            .clone()
            .or_else(|| config.mailbox.room_id.clone())
            .unwrap_or_else(|| crate::net::DEFAULT_ROOM.to_string()),
        store: config.storage.room_ids.clone(),
    };

    let consensus = crate::consensus::handle("consensus.status", json!({}), state).await?;
    let stream = crate::stream::handle("stream.status", json!({}), state).await?;

    Ok(build_status(
        self_info,
        rooms,
        peers,
        module_rooms,
        consensus,
        stream,
    ))
}

/// Per-module room resolution passed to [`build_status`]: mailbox and ai
/// always resolve to exactly one room (falling back to the shared default),
/// while the store carries zero or more configured `storage.room_ids`.
struct ModuleRooms {
    mailbox: String,
    ai: String,
    store: Vec<String>,
}

/// Pure JSON assembly for [`status`], decoupled from the live `identity`/
/// `net`/`consensus`/`stream` calls (which need a running mistlib engine) so
/// the response shape is unit-testable -- same precedent as
/// `stream::relay::cascade_status_json`.
fn build_status(
    self_info: Value,
    rooms: Vec<String>,
    peers: Vec<Value>,
    module_rooms: ModuleRooms,
    consensus: Value,
    stream: Value,
) -> Value {
    // The content store is `null` only when no `storage.room_ids` are
    // configured (purely local, no network join at all); otherwise it
    // reports every configured room and whether each is joined yet -- the
    // store can hold several rooms at once (see `storage::Store::sync_rooms`).
    let store = if module_rooms.store.is_empty() {
        Value::Null
    } else {
        let rooms_status: Vec<Value> = module_rooms
            .store
            .iter()
            .map(|room| json!({ "room": room, "joined": rooms.contains(room) }))
            .collect();
        json!({ "rooms": rooms_status })
    };
    let mailbox_joined = rooms.contains(&module_rooms.mailbox);
    let ai_joined = rooms.contains(&module_rooms.ai);
    let modules = json!({
        "mailbox": { "room": module_rooms.mailbox, "joined": mailbox_joined },
        "ai": { "room": module_rooms.ai, "joined": ai_joined },
        "store": store,
    });
    json!({
        "self": self_info,
        "rooms": rooms,
        "peers": peers,
        "modules": modules,
        "consensus": consensus,
        "stream": stream,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_status_has_the_documented_top_level_shape() {
        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": "Ada" }),
            vec!["room-a".to_string(), "room-b".to_string()],
            vec![
                json!({ "node_id": "peer1", "state": "connected" }),
                json!({ "node_id": "peer2", "state": "connecting" }),
            ],
            ModuleRooms {
                mailbox: "room-a".to_string(),
                ai: "room-b".to_string(),
                store: Vec::new(),
            },
            json!({ "active": true, "room": "room-a", "role": "leader", "leader": "abc123", "peers": ["abc123", "peer1"] }),
            json!({ "running": true, "rtsp_url": "rtsp://127.0.0.1:8554/stream", "clients": 2, "backend": "relay" }),
        );

        assert_eq!(value["self"]["node_id"], "abc123");
        assert_eq!(value["self"]["display_name"], "Ada");
        assert_eq!(value["rooms"], json!(["room-a", "room-b"]));
        assert_eq!(value["peers"].as_array().unwrap().len(), 2);
        assert_eq!(value["peers"][0]["node_id"], "peer1");
        assert_eq!(value["peers"][1]["state"], "connecting");
        assert_eq!(value["consensus"]["active"], true);
        assert_eq!(value["consensus"]["role"], "leader");
        assert_eq!(value["stream"]["running"], true);
        assert_eq!(value["stream"]["backend"], "relay");
    }

    #[test]
    fn build_status_modules_report_rooms_and_joined_flags() {
        // Mailbox's room is joined, ai's (different) room isn't yet -- the
        // joined flag must track the joined-rooms list per module, and the
        // local-only store (no `storage.room_ids` configured) must stay an
        // explicit null.
        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": Value::Null }),
            vec!["mistl-mailbox-v1".to_string()],
            Vec::new(),
            ModuleRooms {
                mailbox: "mistl-mailbox-v1".to_string(),
                ai: "ai-room".to_string(),
                store: Vec::new(),
            },
            json!({ "active": false }),
            json!({ "running": false }),
        );

        assert_eq!(value["modules"]["mailbox"]["room"], "mistl-mailbox-v1");
        assert_eq!(value["modules"]["mailbox"]["joined"], true);
        assert_eq!(value["modules"]["ai"]["room"], "ai-room");
        assert_eq!(value["modules"]["ai"]["joined"], false);
        assert_eq!(value["modules"]["store"], Value::Null);
    }

    #[test]
    fn build_status_store_reports_configured_rooms_and_joined_flags() {
        // Two configured storage rooms, only one of which is currently
        // joined -- `store` must switch from `null` to `{rooms: [...]}`,
        // one entry per configured room, each with its own `joined` flag.
        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": Value::Null }),
            vec!["storage-room-a".to_string()],
            Vec::new(),
            ModuleRooms {
                mailbox: "mistl-mailbox-v1".to_string(),
                ai: "mistl-mailbox-v1".to_string(),
                store: vec!["storage-room-a".to_string(), "storage-room-b".to_string()],
            },
            json!({ "active": false }),
            json!({ "running": false }),
        );

        let store_rooms = value["modules"]["store"]["rooms"].as_array().unwrap();
        assert_eq!(store_rooms.len(), 2);
        assert_eq!(store_rooms[0]["room"], "storage-room-a");
        assert_eq!(store_rooms[0]["joined"], true);
        assert_eq!(store_rooms[1]["room"], "storage-room-b");
        assert_eq!(store_rooms[1]["joined"], false);
    }

    #[test]
    fn build_status_passes_through_inactive_consensus_and_stopped_stream() {
        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": Value::Null }),
            Vec::new(),
            Vec::new(),
            ModuleRooms {
                mailbox: "mistl-mailbox-v1".to_string(),
                ai: "mistl-mailbox-v1".to_string(),
                store: Vec::new(),
            },
            json!({ "active": false }),
            json!({ "running": false }),
        );

        assert_eq!(value["rooms"], json!([]));
        assert_eq!(value["peers"], json!([]));
        assert_eq!(value["modules"]["mailbox"]["joined"], false);
        assert_eq!(value["consensus"]["active"], false);
        assert_eq!(value["stream"]["running"], false);
    }
}
