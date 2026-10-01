//! Unit tests for `crate::tunnel::rtc`, ported from
//! `p2p/src/rtc/manager/tests.rs` (+ its `membership.rs`/`ordering.rs`/
//! `routing.rs` submodules, ported here as `tests/{membership,ordering,
//! routing}.rs` per this port's file ownership -- see
//! `TUNNEL_INTEGRATION_CONTRACT.md`).
//!
//! Dropped relative to upstream: the two `mistlib_config_*` tests for the
//! `P2P_MISTLIB_CONFIG_JSON` env override. That override (and the
//! `mistlib_config()` helper it tested) doesn't exist in this port -- mistl
//! configures signaling centrally in `crate::net::start_engine`, so there is
//! no per-`RTCManagerHandle` config override left to test (see `manager.rs`'s
//! doc comment, seam 1).

use std::sync::{Arc, Mutex};

use super::event::{handle_leave, handle_payload};
use super::manager::RTCManagerHandle;
use super::payload::P2pPayload;
use super::state::{PeerRole, RTCManagerInner};

mod membership;
mod ordering;
mod routing;

#[tokio::test]
async fn graph_blocks_data_and_routing_but_keeps_control_plane_available() {
    use crate::tunnel::graph::{Action, Message, Permissions};
    let (ctx, dir) = crate::tunnel::graph::tests::context().await;
    let manager = ctx.manager.clone();
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = received.clone();
    manager
        .on_tunnel_message(move |peer, _| captured.lock().unwrap().push(peer))
        .await;
    ctx.graph_command("self", Action::Broadcast { enabled: false }, "test-room")
        .await
        .unwrap();
    handle_payload(
        manager.inner.clone(),
        "peer".into(),
        encode(P2pPayload::Role {
            role: "server".into(),
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer".into(),
        encode(P2pPayload::Tunnel { data: vec![1] }),
    )
    .await;
    assert!(received.lock().unwrap().is_empty());
    assert!(manager.select_server_peer_for("service").await.is_none());
    assert!(manager.send_tunnel_to("peer", vec![1]).await.is_err());

    // Transport sender cannot unlock/change broadcast or grant itself rights.
    handle_payload(
        manager.inner.clone(),
        "peer".into(),
        encode(P2pPayload::Graph {
            room: "test-room".into(),
            message: Message::Command {
                id: "denied".into(),
                action: Action::Broadcast { enabled: true },
            },
        }),
    )
    .await;
    ctx.graph_tick().await;
    assert!(!manager.graph().allows_traffic("peer").await);
    ctx.graph_command(
        "self",
        Action::Permission {
            peer_id: "peer".into(),
            permissions: Permissions {
                edit_links: true,
                ..Default::default()
            },
        },
        "test-room",
    )
    .await
    .unwrap();
    // A message queued for another room must never become a command here.
    handle_payload(
        manager.inner.clone(),
        "peer".into(),
        encode(P2pPayload::Graph {
            room: "old-room".into(),
            message: Message::Command {
                id: "old".into(),
                action: Action::Link {
                    peer_id: "peer".into(),
                    connected: true,
                },
            },
        }),
    )
    .await;
    ctx.graph_tick().await;
    assert!(!manager.graph().allows_traffic("peer").await);
    handle_payload(
        manager.inner.clone(),
        "peer".into(),
        encode(P2pPayload::Graph {
            room: "test-room".into(),
            message: Message::Command {
                id: "allowed".into(),
                action: Action::Link {
                    peer_id: "peer".into(),
                    connected: true,
                },
            },
        }),
    )
    .await;
    ctx.graph_tick().await;
    assert_eq!(
        manager.select_server_peer_for("service").await.as_deref(),
        Some("peer")
    );
    handle_payload(
        manager.inner.clone(),
        "peer".into(),
        encode(P2pPayload::Tunnel { data: vec![1] }),
    )
    .await;
    assert_eq!(*received.lock().unwrap(), vec!["peer"]);
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

fn test_manager(self_id: &str, self_role: PeerRole) -> RTCManagerHandle {
    RTCManagerHandle {
        inner: Arc::new(RTCManagerInner::new(
            self_id.to_string(),
            self_role,
            "test-room".to_string(),
        )),
    }
}

fn encode(payload: P2pPayload) -> Vec<u8> {
    serde_json::to_vec(&payload).unwrap()
}

#[tokio::test]
async fn role_payload_tracks_server_peers() {
    let manager = test_manager("self", PeerRole::Client);

    handle_payload(
        manager.inner.clone(),
        "server-1".to_string(),
        encode(P2pPayload::Role {
            role: "server".to_string(),
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "client-1".to_string(),
        encode(P2pPayload::Role {
            role: "client".to_string(),
        }),
    )
    .await;

    assert_eq!(
        manager.get_server_peers().await,
        vec!["server-1".to_string()]
    );
}

#[tokio::test]
async fn message_payloads_are_dispatched_to_registered_handlers() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnels = Arc::new(Mutex::new(Vec::new()));
    let stdio = Arc::new(Mutex::new(Vec::new()));

    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }
    {
        let stdio = stdio.clone();
        manager
            .on_stdio_message(move |peer, data| {
                stdio.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Chat {
            text: "hello".to_string(),
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: vec![1, 2, 3],
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Stdio { data: vec![4, 5] }),
    )
    .await;

    assert_eq!(
        *tunnels.lock().unwrap(),
        vec![("peer-1".to_string(), vec![1, 2, 3])]
    );
    assert_eq!(
        *stdio.lock().unwrap(),
        vec![("peer-1".to_string(), vec![4, 5])]
    );
}

#[test]
fn byte_payloads_are_encoded_as_hex_strings() {
    let encoded_tunnel = encode(P2pPayload::Tunnel {
        data: vec![1, 2, 3, 255],
    });
    let encoded_stdio = encode(P2pPayload::Stdio { data: vec![4, 5] });

    assert_eq!(
        String::from_utf8(encoded_tunnel).unwrap(),
        r#"{"kind":"tunnel","data":"b64:AQID/w=="}"#
    );
    assert_eq!(
        String::from_utf8(encoded_stdio).unwrap(),
        r#"{"kind":"stdio","data":"b64:BAU="}"#
    );
}

#[tokio::test]
async fn legacy_byte_array_payloads_are_still_accepted() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnels = Arc::new(Mutex::new(Vec::new()));

    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        br#"{"kind":"tunnel","data":[1,2,3]}"#.to_vec(),
    )
    .await;

    assert_eq!(
        *tunnels.lock().unwrap(),
        vec![("peer-1".to_string(), vec![1, 2, 3])]
    );
}

#[tokio::test]
async fn legacy_hex_byte_payloads_are_still_accepted() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnels = Arc::new(Mutex::new(Vec::new()));

    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        br#"{"kind":"tunnel","data":"010203"}"#.to_vec(),
    )
    .await;

    assert_eq!(
        *tunnels.lock().unwrap(),
        vec![("peer-1".to_string(), vec![1, 2, 3])]
    );
}

#[tokio::test]
async fn invalid_and_self_payloads_are_ignored() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnels = Arc::new(Mutex::new(Vec::new()));

    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "self".to_string(),
        encode(P2pPayload::Tunnel { data: vec![1] }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        b"not json".to_vec(),
    )
    .await;

    assert!(tunnels.lock().unwrap().is_empty());
}

/// Room chat was removed, but an older mistl / the p2p crate still sends
/// `{"kind":"chat"}`: it must parse (not break the handler) and be dropped.
#[tokio::test]
async fn legacy_chat_payload_is_accepted_and_ignored() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnels = Arc::new(Mutex::new(Vec::new()));
    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Chat {
            text: "hello".to_string(),
        }),
    )
    .await;
    // The sender is still registered as present, and later traffic works.
    assert!(manager.inner.peers.read().await.contains("peer-1"));
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel { data: vec![9] }),
    )
    .await;
    assert_eq!(
        *tunnels.lock().unwrap(),
        vec![("peer-1".to_string(), vec![9])]
    );
}

#[tokio::test]
async fn leave_removes_presence_but_retains_roles_and_notifies_close_handlers() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnel_closed = Arc::new(Mutex::new(Vec::new()));
    let stdio_closed = Arc::new(Mutex::new(Vec::new()));

    manager
        .inner
        .peers
        .write()
        .await
        .insert("peer-1".to_string());
    manager
        .inner
        .peer_roles
        .write()
        .await
        .insert("peer-1".to_string(), PeerRole::Server);

    {
        let tunnel_closed = tunnel_closed.clone();
        manager
            .on_tunnel_close(move |peer| {
                tunnel_closed.lock().unwrap().push(peer);
            })
            .await;
    }
    {
        let stdio_closed = stdio_closed.clone();
        manager
            .on_stdio_close(move |peer| {
                stdio_closed.lock().unwrap().push(peer);
            })
            .await;
    }

    handle_leave(manager.inner.clone(), "peer-1".to_string()).await;

    // Presence is cleared...
    assert!(!manager.inner.peers.read().await.contains("peer-1"));
    // ...but roles/capabilities are retained across a leave (see (B) in the
    // manager-layer audit): a transient mistlib session recovery shouldn't
    // permanently blind routing to a peer that never really left.
    assert_eq!(
        manager.inner.peer_roles.read().await.get("peer-1"),
        Some(&PeerRole::Server)
    );
    assert_eq!(*tunnel_closed.lock().unwrap(), vec!["peer-1".to_string()]);
    assert_eq!(*stdio_closed.lock().unwrap(), vec!["peer-1".to_string()]);
}

/// `t`-tagged tunnel side messages reach `on_aux_message` subscribers with the
/// transport sender; other modules' `t` messages and `kind` envelopes do not.
#[tokio::test]
async fn aux_messages_are_dispatched_by_tag_prefix_with_the_transport_sender() {
    let manager = test_manager("self", PeerRole::Client);
    let seen = Arc::new(Mutex::new(Vec::new()));
    {
        let seen = seen.clone();
        manager
            .on_aux_message(move |peer, value| {
                seen.lock().unwrap().push((peer, value));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        br#"{"t":"mistl-tunnel-auth-v1","state":"pending","forward_key":"k"}"#.to_vec(),
    )
    .await;
    // Another module's `t` message (consensus / DID hello) is not ours.
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        br#"{"t":"mistl-did-hello-v1","room":"r"}"#.to_vec(),
    )
    .await;
    // A `kind` envelope is never an aux message.
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Role {
            role: "client".into(),
        }),
    )
    .await;
    // Our own echo is dropped before any parsing.
    handle_payload(
        manager.inner.clone(),
        "self".to_string(),
        br#"{"t":"mistl-tunnel-auth-v1","state":"pending","forward_key":"k"}"#.to_vec(),
    )
    .await;

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, "peer-1");
    assert_eq!(seen[0].1["state"], "pending");
}
