//! Trust keyed on verified DIDs (see `TrustStore::lookup`): unverified peers
//! are never auto-approved or remembered, legacy node-id rows migrate on the
//! first verified connection, `allow_peers` takes DIDs, and a session stays
//! bound to the DID it was approved for.

use super::{request, temp_store_path, verified_request};
use crate::config::TunnelConfig;
use crate::net::peer_auth;
use crate::tunnel::auth::*;
use crate::tunnel::graph::GraphState;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

async fn first_pending(pending: &PendingAuthorizations) -> PendingAuthorization {
    timeout(Duration::from_secs(1), async {
        loop {
            if let Some(item) = pending.list().await.into_iter().next() {
                return item;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("request should be queued")
}

fn configured(
    config: &TunnelConfig,
    store: &TrustStore,
    pending: &PendingAuthorizations,
) -> SharedAuthorizer {
    crate::tunnel::session::build_authorizer(
        config,
        store.clone(),
        pending.clone(),
        AuthAuditLog::default(),
        Arc::new(GraphState::default()),
    )
}

#[tokio::test]
async fn unverified_peer_is_not_auto_approved_and_not_remembered() {
    let path = temp_store_path("unverified");
    let store = TrustStore::load(&path).await.unwrap();
    let pending = PendingAuthorizations::new();
    let config = TunnelConfig {
        auto_accept: true,
        allow_peers: vec!["node-x".into()],
        ..Default::default()
    };
    let authorizer = configured(&config, &store, &pending);

    // auto_accept / allow_peers do not apply: the request is parked for a
    // human, and shown as unverified.
    let req = request("node-x", "tcp:80");
    let task = {
        let authorizer = authorizer.clone();
        tokio::spawn(async move { authorizer.authorize(&req).await })
    };
    let item = first_pending(&pending).await;
    assert_eq!(item.request.verified_did(), None);

    // "Allow always" on an unverified peer is a one-off: nothing is stored.
    assert!(pending.resolve(item.id, AuthDecision::AllowAlways).await);
    assert_eq!(task.await.unwrap(), AuthDecision::Allow);
    assert!(store.list().await.is_empty());

    // Same for "deny always".
    let req = request("node-x", "tcp:81");
    let task = {
        let authorizer = authorizer.clone();
        tokio::spawn(async move { authorizer.authorize(&req).await })
    };
    let item = first_pending(&pending).await;
    assert!(pending.resolve(item.id, AuthDecision::DenyAlways).await);
    assert_eq!(task.await.unwrap(), AuthDecision::Deny);
    assert!(store.list().await.is_empty());

    // A policy-only authorizer just denies it.
    let policy = PolicyAuthorizer::new(AuthPolicy::AutoAccept, store.clone());
    assert_eq!(
        policy.authorize(&request("node-x", "tcp:80")).await,
        AuthDecision::Deny
    );
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn a_did_that_does_not_hash_to_the_sender_counts_as_unverified() {
    let store = TrustStore::load(temp_store_path("mismatch-did"))
        .await
        .unwrap();
    let (_, _, victim_did) = verified_request("tcp:80");
    let (_, attacker_node, _) = verified_request("tcp:80");
    let mut forged = request(&attacker_node, "tcp:80");
    forged.did = Some(victim_did.clone());
    assert_eq!(forged.verified_did(), None);

    store
        .remember(
            TrustKey {
                peer_id: victim_did,
                forward_key: "tcp:80".into(),
            },
            TrustDecision::Allow,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .lookup(&attacker_node, forged.did.as_deref(), "tcp:80")
            .await,
        None
    );
}

#[tokio::test]
async fn verified_approval_is_remembered_under_the_did() {
    let path = temp_store_path("verified-remember");
    let store = TrustStore::load(&path).await.unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = configured(&TunnelConfig::default(), &store, &pending);
    let (req, node, did) = verified_request("tcp:80");

    let task = {
        let (authorizer, req) = (authorizer.clone(), req.clone());
        tokio::spawn(async move { authorizer.authorize(&req).await })
    };
    let item = first_pending(&pending).await;
    assert_eq!(item.request.verified_did(), Some(did.as_str()));
    assert!(pending.resolve(item.id, AuthDecision::AllowAlways).await);
    assert_eq!(task.await.unwrap(), AuthDecision::AllowAlways);

    let rows = store.list().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key.peer_id, did);
    // Later connections short-circuit without a prompt...
    assert_eq!(authorizer.authorize(&req).await, AuthDecision::Allow);
    // ...but the same node id without a verified DID does not inherit it.
    let unverified = request(&node, "tcp:80");
    assert_eq!(store.lookup(&node, None, "tcp:80").await, None);
    assert!(
        timeout(
            Duration::from_millis(100),
            authorizer.authorize(&unverified)
        )
        .await
        .is_err(),
        "unverified peer must be prompted, not trusted"
    );
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn legacy_node_id_entry_migrates_on_first_verified_connection() {
    let path = temp_store_path("migrate");
    let store = TrustStore::load(&path).await.unwrap();
    let (_, node, did) = verified_request("tcp:80");
    let legacy = TrustKey {
        peer_id: node.clone(),
        forward_key: "tcp:80".into(),
    };
    store
        .remember(legacy.clone(), TrustDecision::Allow)
        .await
        .unwrap();
    // The legacy row still loads from disk...
    let store = TrustStore::load(&path).await.unwrap();
    assert_eq!(store.get(&legacy).await, Some(TrustDecision::Allow));
    // ...but an unverified sender cannot use it,
    assert_eq!(store.lookup(&node, None, "tcp:80").await, None);
    assert_eq!(store.get(&legacy).await, Some(TrustDecision::Allow));
    // and a verified one migrates it (DID attached, node-id row gone).
    assert_eq!(
        store.lookup(&node, Some(&did), "tcp:80").await,
        Some(TrustDecision::Allow)
    );
    assert_eq!(store.get(&legacy).await, None);
    assert_eq!(
        store
            .get(&TrustKey {
                peer_id: did.clone(),
                forward_key: "tcp:80".into()
            })
            .await,
        Some(TrustDecision::Allow)
    );
    // Persisted, and a different DID claiming the node id gets nothing.
    let reloaded = TrustStore::load(&path).await.unwrap();
    assert_eq!(reloaded.list().await.len(), 1);
    let (_, other_node, other_did) = verified_request("tcp:80");
    assert_eq!(
        reloaded
            .lookup(&other_node, Some(&other_did), "tcp:80")
            .await,
        None
    );
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn legacy_deny_still_applies_to_unverified_peers() {
    let store = TrustStore::load(temp_store_path("legacy-deny"))
        .await
        .unwrap();
    store
        .remember(
            TrustKey {
                peer_id: "node-x".into(),
                forward_key: "tcp:80".into(),
            },
            TrustDecision::Deny,
        )
        .await
        .unwrap();
    assert_eq!(
        store.lookup("node-x", None, "tcp:80").await,
        Some(TrustDecision::Deny)
    );
    // A peer id that looks like a DID never aliases a DID-keyed row.
    store
        .remember(
            TrustKey {
                peer_id: "did:key:zVictim".into(),
                forward_key: "tcp:80".into(),
            },
            TrustDecision::Allow,
        )
        .await
        .unwrap();
    assert_eq!(store.lookup("did:key:zVictim", None, "tcp:80").await, None);
}

#[tokio::test]
async fn allow_peers_accepts_dids_and_verified_node_ids() {
    let path = temp_store_path("allow-peers");
    let store = TrustStore::load(&path).await.unwrap();
    let pending = PendingAuthorizations::new();
    let (by_did, _, did) = verified_request("tcp:80");
    let (by_node, node, _) = verified_request("tcp:80");
    let (stranger, _, _) = verified_request("tcp:80");
    let config = TunnelConfig {
        allow_peers: vec![did, node.clone()],
        ..Default::default()
    };
    let authorizer = configured(&config, &store, &pending);

    assert_eq!(authorizer.authorize(&by_did).await, AuthDecision::Allow);
    assert_eq!(authorizer.authorize(&by_node).await, AuthDecision::Allow);
    // Not listed: parked for a human instead.
    assert!(
        timeout(Duration::from_millis(100), authorizer.authorize(&stranger))
            .await
            .is_err()
    );
    // A listed node id does not match when the DID is unverified (spoofed
    // `from`) or does not hash to it.
    let spoofed = request(&node, "tcp:80");
    assert!(
        timeout(Duration::from_millis(100), authorizer.authorize(&spoofed))
            .await
            .is_err()
    );
    let mut wrong_did = request(&node, "tcp:80");
    wrong_did.did = stranger.did.clone();
    assert!(
        timeout(Duration::from_millis(100), authorizer.authorize(&wrong_did))
            .await
            .is_err()
    );
    // Nothing was remembered by policy approvals.
    assert!(store.list().await.is_empty());
    let _ = tokio::fs::remove_file(path).await;
}

fn register(room: &str, identity: &crate::identity::Identity) {
    let now = peer_auth::now_ms();
    let bytes = peer_auth::build_proof(
        peer_auth::HELLO_TAG,
        room,
        identity,
        now,
        &peer_auth::fresh_nonce(),
        false,
    );
    let proof =
        peer_auth::check_proof(&bytes, peer_auth::HELLO_TAG, room, &identity.node_id(), now)
            .unwrap();
    peer_auth::accept_proof(room, &proof).unwrap();
}

#[test]
fn session_binding_rejects_a_sender_that_no_longer_maps_to_the_approved_did() {
    let room = format!("bind-{}", uuid::Uuid::new_v4());
    let a = crate::identity::for_test();
    let b = crate::identity::for_test();
    register(&room, &a);
    register(&room, &b);
    let (a_node, b_node) = (a.node_id(), b.node_id());

    // Approved for A: A's traffic passes.
    assert!(binding_holds(Some(a.did()), Some(&room), &a_node));
    // Payload now arriving under B's node id (or for a DID that isn't the
    // verified one) is dropped.
    assert!(!binding_holds(Some(a.did()), Some(&room), &b_node));
    assert!(!binding_holds(Some(b.did()), Some(&room), &a_node));
    // Verification lapsing (peer left) also drops the session's traffic.
    peer_auth::forget_peer(&room, &a_node);
    assert!(!binding_holds(Some(a.did()), Some(&room), &a_node));
    // A session approved one-off for an unverified peer has no binding.
    assert!(binding_holds(None, Some(&room), &a_node));
}
