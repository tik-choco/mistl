//! Ported verbatim from `p2p/src/rtc/manager/tests/routing.rs`, except
//! `crate::rtc::TunnelMessage` -> `crate::tunnel::wire::TunnelMessage`
//! (upstream's `p2p::rtc` re-export of its own `tunnel_message` module is
//! this port's separate `crate::tunnel::wire` module -- see
//! `TUNNEL_INTEGRATION_CONTRACT.md`'s frozen API for `crate::tunnel::wire`).

use std::sync::{Arc, Mutex};

use super::super::event::{handle_join, handle_leave, handle_payload};
use super::super::payload::P2pPayload;
use super::super::state::PeerRole;
use super::{encode, test_manager};
use crate::tunnel::wire::TunnelMessage;

fn encode_tunnel(target: &str, conn_id: &str) -> Vec<u8> {
    serde_json::to_vec(&TunnelMessage {
        msg_type: "data".to_string(),
        conn_id: conn_id.to_string(),
        target: target.to_string(),
        payload: Some(vec![42]),
        seq: None,
    })
    .unwrap()
}

#[test]
fn tunnel_message_payload_is_encoded_as_hex_string() {
    let encoded = encode_tunnel("tcp:80", "conn-1");

    assert_eq!(
        String::from_utf8(encoded).unwrap(),
        r#"{"type":"data","conn_id":"conn-1","target":"tcp:80","payload":"b64:Kg=="}"#
    );
}

#[test]
fn legacy_tunnel_message_payload_array_is_still_accepted() {
    let decoded: TunnelMessage =
        serde_json::from_slice(br#"{"type":"data","conn_id":"conn-1","payload":[42]}"#).unwrap();

    assert_eq!(decoded.payload, Some(vec![42]));
}

#[test]
fn legacy_tunnel_message_payload_hex_is_still_accepted() {
    let decoded: TunnelMessage =
        serde_json::from_slice(br#"{"type":"data","conn_id":"conn-1","payload":"2a"}"#).unwrap();

    assert_eq!(decoded.payload, Some(vec![42]));
}

#[tokio::test]
async fn capability_payload_tracks_targeted_server_peers() {
    let manager = test_manager("self", PeerRole::Client);

    handle_payload(
        manager.inner.clone(),
        "server-1".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string()],
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "server-2".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:5432".to_string()],
        }),
    )
    .await;

    assert_eq!(
        manager.get_server_peers_for("tcp:5432").await,
        vec!["server-2".to_string()]
    );
}

#[tokio::test]
async fn select_server_peer_round_robins_across_advertised_peers() {
    let manager = test_manager("self", PeerRole::Client);

    for peer in ["server-b", "server-a", "server-c"] {
        handle_payload(
            manager.inner.clone(),
            peer.to_string(),
            encode(P2pPayload::Capabilities {
                forwards: vec!["tcp:80".to_string()],
            }),
        )
        .await;
    }

    // Deterministic ordering (sorted) with a round-robin cursor.
    let mut picks = Vec::new();
    for _ in 0..6 {
        picks.push(manager.select_server_peer_for("tcp:80").await.unwrap());
    }
    assert_eq!(
        picks,
        vec![
            "server-a".to_string(),
            "server-b".to_string(),
            "server-c".to_string(),
            "server-a".to_string(),
            "server-b".to_string(),
            "server-c".to_string(),
        ]
    );
}

#[tokio::test]
async fn select_server_peer_returns_none_without_advertised_peers() {
    let manager = test_manager("self", PeerRole::Client);
    assert_eq!(manager.select_server_peer_for("tcp:80").await, None);
}

#[tokio::test]
async fn targeted_tunnel_handlers_receive_only_matching_targets() {
    let manager = test_manager("self", PeerRole::Client);
    let tcp_80 = Arc::new(Mutex::new(Vec::new()));
    let tcp_5432 = Arc::new(Mutex::new(Vec::new()));

    {
        let tcp_80 = tcp_80.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                tcp_80.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }
    {
        let tcp_5432 = tcp_5432.clone();
        manager
            .on_tunnel_message_for("tcp:5432".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                tcp_5432.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:5432", "conn-5432"),
        }),
    )
    .await;

    assert!(tcp_80.lock().unwrap().is_empty());
    assert_eq!(
        *tcp_5432.lock().unwrap(),
        vec![("peer-1".to_string(), "conn-5432".to_string())]
    );
}

#[tokio::test]
async fn empty_tunnel_target_routes_to_first_registered_target() {
    let manager = test_manager("self", PeerRole::Client);
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));

    {
        let first = first.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                first.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }
    {
        let second = second.clone();
        manager
            .on_tunnel_message_for("tcp:5432".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                second.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("", "legacy-conn"),
        }),
    )
    .await;

    assert_eq!(
        *first.lock().unwrap(),
        vec![("peer-1".to_string(), "legacy-conn".to_string())]
    );
    assert!(second.lock().unwrap().is_empty());
}

#[tokio::test]
async fn removed_tunnel_handler_no_longer_receives_targeted_messages() {
    let manager = test_manager("self", PeerRole::Client);
    let received = Arc::new(Mutex::new(Vec::new()));

    let handler_id = {
        let received = received.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                received.lock().unwrap().push((peer, tm.conn_id));
            })
            .await
    };

    assert!(manager.remove_tunnel_message_handler(handler_id).await);
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:80", "conn-80"),
        }),
    )
    .await;

    assert!(received.lock().unwrap().is_empty());
}

/// Coverage for (B)/(4) in the manager-layer audit: capabilities survive a
/// leave (so a transient session recovery doesn't permanently blind
/// routing), routing itself excludes a departed peer regardless, and a
/// rejoin resets the retained capabilities so a subsequent fresh
/// Capabilities broadcast is what repopulates them.
#[tokio::test]
async fn leave_retains_capabilities_but_routing_excludes_departed_peer_until_rejoin_repopulates() {
    let manager = test_manager("self", PeerRole::Client);

    handle_join(manager.inner.clone(), "peer-1".to_string()).await;
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string()],
        }),
    )
    .await;
    assert_eq!(
        manager.get_server_peers_for("tcp:80").await,
        vec!["peer-1".to_string()]
    );

    handle_leave(manager.inner.clone(), "peer-1".to_string()).await;

    // Capabilities are still cached...
    assert!(
        manager
            .inner
            .peer_forward_keys
            .read()
            .await
            .get("peer-1")
            .is_some_and(|keys| keys.contains("tcp:80"))
    );
    // ...but routing only considers currently-present peers.
    assert!(manager.get_server_peers_for("tcp:80").await.is_empty());

    // A fresh JOIN resets the stale capabilities rather than trusting them
    // for a new session.
    handle_join(manager.inner.clone(), "peer-1".to_string()).await;
    assert!(
        manager
            .inner
            .peer_forward_keys
            .read()
            .await
            .get("peer-1")
            .is_none()
    );
    assert!(manager.get_server_peers_for("tcp:80").await.is_empty());

    // The peer's new Capabilities broadcast repopulates routing.
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string()],
        }),
    )
    .await;
    assert_eq!(
        manager.get_server_peers_for("tcp:80").await,
        vec!["peer-1".to_string()]
    );
}

#[tokio::test]
async fn scoped_target_always_selects_pinned_peer_without_round_robin() {
    let manager = test_manager("self", PeerRole::Client);

    // Two peers both advertise the base target; only "peer-a" also
    // advertises the peer-a-scoped variant.
    handle_payload(
        manager.inner.clone(),
        "peer-a".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string(), "tcp:80@peer-a".to_string()],
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-b".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string()],
        }),
    )
    .await;

    for _ in 0..6 {
        assert_eq!(
            manager.select_server_peer_for("tcp:80@peer-a").await,
            Some("peer-a".to_string())
        );
    }
}

#[tokio::test]
async fn scoped_target_returns_none_when_pinned_peer_not_live() {
    let manager = test_manager("self", PeerRole::Client);

    // "peer-b" is live and advertises the base target, but the pinned peer
    // ("peer-a") never joined -- there must be no fallback to peer-b.
    handle_payload(
        manager.inner.clone(),
        "peer-b".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string()],
        }),
    )
    .await;

    assert!(
        manager
            .get_server_peers_for("tcp:80@peer-a")
            .await
            .is_empty()
    );
    assert_eq!(manager.select_server_peer_for("tcp:80@peer-a").await, None);
}

#[tokio::test]
async fn scoped_target_returns_none_when_pinned_peer_advertises_unrelated_key() {
    let manager = test_manager("self", PeerRole::Client);

    // "peer-a" is live, but only advertises an unrelated key -- not the
    // scoped target, and not even the unscoped base target.
    handle_payload(
        manager.inner.clone(),
        "peer-a".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:5432".to_string()],
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-b".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string()],
        }),
    )
    .await;

    assert!(
        manager
            .get_server_peers_for("tcp:80@peer-a")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn scoped_target_ignores_the_key_when_advertised_by_a_different_peer() {
    let manager = test_manager("self", PeerRole::Client);

    // "peer-a" is live but doesn't advertise the scoped key itself; the
    // literal string "tcp:80@peer-a" is instead advertised by "peer-b".
    // The pinned peer's own advertisement is what must be checked, not
    // whether the string is advertised by *someone*.
    handle_payload(
        manager.inner.clone(),
        "peer-a".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:5432".to_string()],
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-b".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80@peer-a".to_string()],
        }),
    )
    .await;

    assert!(
        manager
            .get_server_peers_for("tcp:80@peer-a")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn scoped_message_reaches_base_target_handler_only_when_pinned_to_self() {
    let manager = test_manager("self", PeerRole::Client);
    let received = Arc::new(Mutex::new(Vec::new()));

    {
        let received = received.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                received.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    // Pinned to a different node -- must not reach the "tcp:80" handler.
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:80@other", "conn-other"),
        }),
    )
    .await;
    assert!(received.lock().unwrap().is_empty());

    // Pinned to "self" -- reaches the "tcp:80" handler.
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:80@self", "conn-self"),
        }),
    )
    .await;
    assert_eq!(
        *received.lock().unwrap(),
        vec![("peer-1".to_string(), "conn-self".to_string())]
    );
}

#[tokio::test]
async fn base_target_message_reaches_scoped_handler_only_from_pinned_sender() {
    let manager = test_manager("self", PeerRole::Client);
    let received = Arc::new(Mutex::new(Vec::new()));

    {
        let received = received.clone();
        manager
            .on_tunnel_message_for("tcp:80@peer-a".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                received.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    // Base-target message from an unrelated sender -- must not reach the
    // "tcp:80@peer-a"-scoped handler.
    handle_payload(
        manager.inner.clone(),
        "peer-b".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:80", "conn-from-b"),
        }),
    )
    .await;
    assert!(received.lock().unwrap().is_empty());

    // Base-target message from the pinned sender "peer-a" -- reaches it.
    handle_payload(
        manager.inner.clone(),
        "peer-a".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:80", "conn-from-a"),
        }),
    )
    .await;
    assert_eq!(
        *received.lock().unwrap(),
        vec![("peer-a".to_string(), "conn-from-a".to_string())]
    );
}

#[tokio::test]
async fn publish_tunnel_target_also_advertises_self_scoped_variant() {
    let manager = test_manager("node-x", PeerRole::Server);

    manager.publish_tunnel_target("tcp:80").await;

    let keys = manager.inner.self_forward_keys.read().await;
    assert!(keys.contains("tcp:80"));
    assert!(keys.contains("tcp:80@node-x"));
    drop(keys);

    manager.unpublish_tunnel_target("tcp:80").await;
    let keys = manager.inner.self_forward_keys.read().await;
    assert!(!keys.contains("tcp:80"));
    assert!(!keys.contains("tcp:80@node-x"));
}

#[tokio::test]
async fn publish_tunnel_target_with_scope_does_not_add_a_second_scope() {
    let manager = test_manager("node-x", PeerRole::Server);

    manager.publish_tunnel_target("tcp:80@node-y").await;

    let keys = manager.inner.self_forward_keys.read().await;
    assert!(keys.contains("tcp:80@node-y"));
    assert!(!keys.contains("tcp:80"));
    assert!(!keys.contains("tcp:80@node-x"));
}

#[tokio::test]
async fn empty_tunnel_target_uses_next_handler_after_default_removed() {
    let manager = test_manager("self", PeerRole::Client);
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));

    let first_handler = {
        let first = first.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                first.lock().unwrap().push((peer, tm.conn_id));
            })
            .await
    };
    {
        let second = second.clone();
        manager
            .on_tunnel_message_for("tcp:5432".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                second.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    assert!(manager.remove_tunnel_message_handler(first_handler).await);
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("", "legacy-conn"),
        }),
    )
    .await;

    assert!(first.lock().unwrap().is_empty());
    assert_eq!(
        *second.lock().unwrap(),
        vec![("peer-1".to_string(), "legacy-conn".to_string())]
    );
}
