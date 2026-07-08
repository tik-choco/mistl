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

/// `topology.status` `{}` -> `{self, rooms, peers, consensus, stream}`:
/// - `self`: `{node_id, did, display_name}` -- this node's identity
/// - `rooms`: joined room ids (`net::joined_rooms`)
/// - `peers`: `[{node_id, state}]` -- every node mistlib currently sees as
///   connected (any joined room), with its best-known connection state
/// - `consensus`: verbatim `consensus.status` shape (cascade leader
///   election: `{active, room?, role?, leader?, peers?}`)
/// - `stream`: verbatim `stream.status` shape (`{running, rtsp_url?,
///   clients?, backend?, room?, publisher?, cascade?}`)
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

    let consensus = crate::consensus::handle("consensus.status", json!({}), state).await?;
    let stream = crate::stream::handle("stream.status", json!({}), state).await?;

    Ok(build_status(self_info, rooms, peers, consensus, stream))
}

/// Pure JSON assembly for [`status`], decoupled from the live `identity`/
/// `net`/`consensus`/`stream` calls (which need a running mistlib engine) so
/// the response shape is unit-testable -- same precedent as
/// `stream::relay::cascade_status_json`.
fn build_status(self_info: Value, rooms: Vec<String>, peers: Vec<Value>, consensus: Value, stream: Value) -> Value {
    json!({
        "self": self_info,
        "rooms": rooms,
        "peers": peers,
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
    fn build_status_passes_through_inactive_consensus_and_stopped_stream() {
        let value = build_status(
            json!({ "node_id": "abc123", "did": "did:key:z6Mk...", "display_name": Value::Null }),
            Vec::new(),
            Vec::new(),
            json!({ "active": false }),
            json!({ "running": false }),
        );

        assert_eq!(value["rooms"], json!([]));
        assert_eq!(value["peers"], json!([]));
        assert_eq!(value["consensus"]["active"], false);
        assert_eq!(value["stream"]["running"], false);
    }
}
