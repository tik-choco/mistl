//! `topology.status`: the "who is connected and how" snapshot backing the
//! web dashboard's topology view.
//!
//! This is a read-only join of state other modules already track --
//! `identity` (this node), `net` (joined rooms, connected peers, per-peer
//! and per-room connection state, send/receive activity), `consensus`
//! (cascade leader election), and `stream` (relay publisher lock / RTSP
//! viewers) -- reusing their existing `*.status`/`*.show` IPC commands (via
//! `Module::handle`) rather than duplicating their internals. Nothing here
//! tracks anything new by itself; it assembles what `net` already tracks.
//!
//! ## What's observable, and where each piece comes from
//!
//! mistlib-native exposes `get_connected_nodes_async` (process-wide, not
//! room-scoped -- a peer connected in *any* room this process joined shows
//! up once) and `get_connection_state_async` (a "most-connected session
//! wins" union: one of `Disconnected`, `Connecting`, `Connected`,
//! `Reconnecting`, `Failed`). `peers` below is built from those two, so it
//! answers "who does mistlib currently think we're connected to, anywhere"
//! -- a process-wide overview kept for compatibility with existing
//! dashboard code, not a room-scoped view.
//!
//! Per-room peer membership -- which peer is connected in which room --
//! comes from mistlib's newer `get_room_connections_async`, a room-scoped
//! counterpart to `get_connected_nodes_async`: one entry per active session,
//! each with its own room id, connected peers, and per-room connection
//! state (no cross-room dedup, so a peer in two rooms appears once under
//! each). `room_peers` below wraps that (via `net::room_connections`),
//! unioned with every room this process has joined via `net::joined_rooms`
//! so a just-joined room with no session-reported peers yet still shows up,
//! with an empty peer list rather than being omitted.
//!
//! `activity` (send/receive counts, bytes, and time since the last event,
//! per room and peer) is *not* something mistlib tracks or exposes at all --
//! it's bookkeeping mistl's own `net` module does at its own send/receive
//! choke points (`net::send_direct`, `net::send_broadcast`, and the raw
//! event dispatch callback), entirely independent of mistlib. See
//! `net::activity_snapshot`'s doc comment for the tracking details.
//!
//! There is still no per-peer ICE state, RTT, or bitrate exposed by
//! mistlib -- `consensus` (cascade membership, leader/follower) and
//! `stream` (publisher lock, RTSP viewer count) remain the only other
//! room-scoped context, layered on top same as before. No field here is
//! invented: anything not exposed by mistlib or tracked by `net` is simply
//! omitted rather than guessed at.

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

/// `topology.status` `{}` -> `{self, rooms, peers, room_peers, activity,
/// modules, consensus, stream}`:
/// - `self`: `{node_id, did, display_name}` -- this node's identity
/// - `rooms`: joined room ids (`net::joined_rooms`)
/// - `peers`: `[{node_id, state}]` -- every node mistlib currently sees as
///   connected (any joined room), with its best-known connection state --
///   a process-wide overview, kept for compatibility; see `room_peers` for
///   the room-scoped view
/// - `room_peers`: `[{room, peers: [{node_id, state}]}]` -- per-room peer
///   membership (`net::room_connections`), one entry for every room this
///   process has joined (`net::joined_rooms`) unioned with every room
///   mistlib currently reports a session for; a joined room with no
///   session-reported peers yet still appears, with an empty `peers` array.
///   Sorted by room id.
/// - `activity`: `[{room, node_id, tx, rx}]` -- send/receive activity
///   tracked by `net` itself at its send/receive choke points
///   (`net::activity_snapshot`), *not* by mistlib. `node_id` is `null` for a
///   room-wide broadcast entry rather than a specific peer. `tx`/`rx` are
///   each either `{count, bytes, age_ms}` or `null` if that direction has
///   seen no events yet. Sorted by room id, then by node_id with the
///   broadcast (`null`) entry first.
/// - `modules`: `{mailbox: {room, joined}, ai: {room, joined}, store}` --
///   mailbox/ai report a single room id (resolved from config exactly the
///   way the module itself does, without starting it) and whether that room
///   is joined yet; `store` reports every room in `storage.room_ids` instead
///   (the store can join several simultaneously) as `{rooms: [{room,
///   joined}]}`, or `null` when no rooms are configured (purely local
///   store, no network join at all).
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

    let net_snapshot = NetSnapshot {
        rooms,
        peers,
        room_connections: crate::net::room_connections().await,
        activity: crate::net::activity_snapshot(),
    };

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

    Ok(build_status(self_info, net_snapshot, module_rooms, consensus, stream))
}

/// Per-module room resolution passed to [`build_status`]: mailbox and ai
/// always resolve to exactly one room (falling back to the shared default),
/// while the store carries zero or more configured `storage.room_ids`.
struct ModuleRooms {
    mailbox: String,
    ai: String,
    store: Vec<String>,
}

/// Everything [`build_status`] reads from `net`, bundled into one struct
/// purely to keep that function's argument list short -- `rooms` and `peers`
/// are unchanged from before this module tracked room-scoped membership and
/// activity; `room_connections` and `activity` are `net::room_connections`'s
/// and `net::activity_snapshot`'s plain-data return types, threaded through
/// as-is rather than re-derived here.
struct NetSnapshot {
    rooms: Vec<String>,
    peers: Vec<Value>,
    room_connections: Vec<(String, Vec<(String, String)>)>,
    activity: Vec<crate::net::ActivityEntry>,
}

/// Pure JSON assembly for [`status`], decoupled from the live `identity`/
/// `net`/`consensus`/`stream` calls (which need a running mistlib engine) so
/// the response shape is unit-testable -- same precedent as
/// `stream::relay::cascade_status_json`.
fn build_status(self_info: Value, net: NetSnapshot, module_rooms: ModuleRooms, consensus: Value, stream: Value) -> Value {
    let NetSnapshot { rooms, peers, room_connections, activity } = net;
    let room_peers = build_room_peers(&rooms, &room_connections);
    let activity_json = build_activity(&activity);

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
        "room_peers": room_peers,
        "activity": activity_json,
        "modules": modules,
        "consensus": consensus,
        "stream": stream,
    })
}

/// Build the `room_peers` array: one entry per room, sorted by room id,
/// covering every room this process has joined (`joined_rooms`, from
/// `net::joined_rooms`) unioned with every room mistlib currently reports a
/// session for (`room_connections`, from `net::room_connections`). A joined
/// room with no session-reported peers yet gets an empty `peers` array
/// rather than being omitted; a room mistlib reports that isn't (yet, or no
/// longer) in `joined_rooms` still appears too, since it's a live session
/// regardless. Peers within a room are sorted by node id for a deterministic
/// result; a `BTreeMap` keyed by room id gives the room ordering for free.
fn build_room_peers(
    joined_rooms: &[String],
    room_connections: &[(String, Vec<(String, String)>)],
) -> Vec<Value> {
    let mut merged: std::collections::BTreeMap<&str, Vec<(&str, &str)>> = std::collections::BTreeMap::new();
    for room in joined_rooms {
        merged.entry(room.as_str()).or_default();
    }
    for (room, peers) in room_connections {
        let entry = merged.entry(room.as_str()).or_default();
        entry.extend(peers.iter().map(|(node_id, state)| (node_id.as_str(), state.as_str())));
    }
    merged
        .into_iter()
        .map(|(room, mut peers)| {
            peers.sort_by(|a, b| a.0.cmp(b.0));
            let peers_json: Vec<Value> = peers
                .into_iter()
                .map(|(node_id, state)| json!({ "node_id": node_id, "state": state }))
                .collect();
            json!({ "room": room, "peers": peers_json })
        })
        .collect()
}

/// Build the `activity` array from `net::activity_snapshot`'s plain-data
/// entries. Sorted by room id, then by node id -- with the room-wide
/// broadcast entry (empty `peer`, JSON `null` node_id) sorted first within
/// its room.
fn build_activity(activity: &[crate::net::ActivityEntry]) -> Vec<Value> {
    let mut entries: Vec<&crate::net::ActivityEntry> = activity.iter().collect();
    entries.sort_by(|a, b| {
        a.room.cmp(&b.room).then_with(|| match (a.peer.is_empty(), b.peer.is_empty()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => a.peer.cmp(&b.peer),
        })
    });
    entries
        .into_iter()
        .map(|entry| {
            let node_id = if entry.peer.is_empty() {
                Value::Null
            } else {
                Value::String(entry.peer.clone())
            };
            json!({
                "room": entry.room,
                "node_id": node_id,
                "tx": direction_json(entry.tx),
                "rx": direction_json(entry.rx),
            })
        })
        .collect()
}

/// `{count, bytes, age_ms}` for a direction that has seen activity, or JSON
/// `null` for one that hasn't (see `net::ActivityEntry`'s doc comment).
fn direction_json(direction: Option<crate::net::DirectionActivity>) -> Value {
    match direction {
        Some(d) => json!({ "count": d.count, "bytes": d.bytes, "age_ms": d.age.as_millis() as u64 }),
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_status_has_the_documented_top_level_shape() {
        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": "Ada" }),
            NetSnapshot {
                rooms: vec!["room-a".to_string(), "room-b".to_string()],
                peers: vec![
                    json!({ "node_id": "peer1", "state": "connected" }),
                    json!({ "node_id": "peer2", "state": "connecting" }),
                ],
                room_connections: vec![(
                    "room-a".to_string(),
                    vec![("peer1".to_string(), "connected".to_string())],
                )],
                activity: Vec::new(),
            },
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
        assert_eq!(value["room_peers"].as_array().unwrap().len(), 2);
        assert_eq!(value["activity"], json!([]));
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
            NetSnapshot {
                rooms: vec!["mistl-mailbox-v1".to_string()],
                peers: Vec::new(),
                room_connections: Vec::new(),
                activity: Vec::new(),
            },
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
            NetSnapshot {
                rooms: vec!["storage-room-a".to_string()],
                peers: Vec::new(),
                room_connections: Vec::new(),
                activity: Vec::new(),
            },
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
            NetSnapshot {
                rooms: Vec::new(),
                peers: Vec::new(),
                room_connections: Vec::new(),
                activity: Vec::new(),
            },
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
        assert_eq!(value["room_peers"], json!([]));
        assert_eq!(value["activity"], json!([]));
        assert_eq!(value["modules"]["mailbox"]["joined"], false);
        assert_eq!(value["consensus"]["active"], false);
        assert_eq!(value["stream"]["running"], false);
    }

    #[test]
    fn build_status_room_peers_includes_joined_but_unreported_rooms() {
        // "room-a" is joined but mistlib hasn't reported a session for it
        // yet (e.g. join is still negotiating) -- it must still appear in
        // `room_peers`, with an empty peers array, not be omitted. "room-b"
        // is both joined and reported, with its peers and states intact.
        // "room-c" is reported by mistlib but not (yet) in the joined list --
        // it must appear too, since it's a live session regardless.
        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": Value::Null }),
            NetSnapshot {
                rooms: vec!["room-a".to_string(), "room-b".to_string()],
                peers: Vec::new(),
                room_connections: vec![
                    (
                        "room-b".to_string(),
                        vec![
                            ("peer2".to_string(), "connected".to_string()),
                            ("peer1".to_string(), "connecting".to_string()),
                        ],
                    ),
                    ("room-c".to_string(), vec![("peer3".to_string(), "reconnecting".to_string())]),
                ],
                activity: Vec::new(),
            },
            ModuleRooms {
                mailbox: "room-a".to_string(),
                ai: "room-a".to_string(),
                store: Vec::new(),
            },
            json!({ "active": false }),
            json!({ "running": false }),
        );

        let room_peers = value["room_peers"].as_array().unwrap();
        assert_eq!(room_peers.len(), 3);

        // Sorted by room id: room-a, room-b, room-c.
        assert_eq!(room_peers[0]["room"], "room-a");
        assert_eq!(room_peers[0]["peers"], json!([]));

        assert_eq!(room_peers[1]["room"], "room-b");
        let room_b_peers = room_peers[1]["peers"].as_array().unwrap();
        assert_eq!(room_b_peers.len(), 2);
        // Peers within a room are sorted by node id.
        assert_eq!(room_b_peers[0]["node_id"], "peer1");
        assert_eq!(room_b_peers[0]["state"], "connecting");
        assert_eq!(room_b_peers[1]["node_id"], "peer2");
        assert_eq!(room_b_peers[1]["state"], "connected");

        assert_eq!(room_peers[2]["room"], "room-c");
        assert_eq!(room_peers[2]["peers"][0]["node_id"], "peer3");
        assert_eq!(room_peers[2]["peers"][0]["state"], "reconnecting");
    }

    #[test]
    fn build_status_activity_reports_tx_only_rx_only_and_broadcast_shapes() {
        use std::time::Duration;

        let entries = vec![
            // Direct peer with only outbound activity so far.
            crate::net::ActivityEntry {
                room: "room-a".to_string(),
                peer: "peer1".to_string(),
                tx: Some(crate::net::DirectionActivity { count: 3, bytes: 1234, age: Duration::from_millis(500) }),
                rx: None,
            },
            // Direct peer with only inbound activity so far.
            crate::net::ActivityEntry {
                room: "room-a".to_string(),
                peer: "peer2".to_string(),
                tx: None,
                rx: Some(crate::net::DirectionActivity { count: 1, bytes: 88, age: Duration::from_millis(12_000) }),
            },
            // Room-wide broadcast (empty peer id -> null node_id).
            crate::net::ActivityEntry {
                room: "room-a".to_string(),
                peer: String::new(),
                tx: Some(crate::net::DirectionActivity { count: 5, bytes: 999, age: Duration::from_millis(10) }),
                rx: None,
            },
        ];

        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": Value::Null }),
            NetSnapshot {
                rooms: vec!["room-a".to_string()],
                peers: Vec::new(),
                room_connections: Vec::new(),
                activity: entries,
            },
            ModuleRooms {
                mailbox: "room-a".to_string(),
                ai: "room-a".to_string(),
                store: Vec::new(),
            },
            json!({ "active": false }),
            json!({ "running": false }),
        );

        let activity = value["activity"].as_array().unwrap();
        assert_eq!(activity.len(), 3);

        // Broadcast (null node_id) sorts first within the room.
        assert_eq!(activity[0]["node_id"], Value::Null);
        assert_eq!(activity[0]["tx"]["count"], 5);
        assert_eq!(activity[0]["tx"]["bytes"], 999);
        assert_eq!(activity[0]["tx"]["age_ms"], 10);
        assert_eq!(activity[0]["rx"], Value::Null);

        assert_eq!(activity[1]["node_id"], "peer1");
        assert_eq!(activity[1]["tx"]["count"], 3);
        assert_eq!(activity[1]["tx"]["bytes"], 1234);
        assert_eq!(activity[1]["tx"]["age_ms"], 500);
        assert_eq!(activity[1]["rx"], Value::Null);

        assert_eq!(activity[2]["node_id"], "peer2");
        assert_eq!(activity[2]["tx"], Value::Null);
        assert_eq!(activity[2]["rx"]["count"], 1);
        assert_eq!(activity[2]["rx"]["bytes"], 88);
        assert_eq!(activity[2]["rx"]["age_ms"], 12_000);
    }
}
