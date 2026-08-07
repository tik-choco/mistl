use super::*;

use std::path::PathBuf;

mod pending;

fn request(peer_id: &str, forward_key: &str) -> AuthRequest {
    AuthRequest {
        peer_id: peer_id.to_string(),
        forward_key: forward_key.to_string(),
        target_addr: "127.0.0.1:80".to_string(),
        proto: "tcp".to_string(),
    }
}

fn temp_store_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "mistl-tunnel-auth-{}-{}.json",
        name,
        uuid::Uuid::new_v4()
    ))
}

#[tokio::test]
async fn allow_all_authorizer_allows_any_request() {
    let authorizer = allow_all();

    let decision = authorizer.authorize(&request("peer-a", "tcp:80")).await;

    assert_eq!(decision, AuthDecision::Allow);
    assert!(decision.is_allowed());
}

#[tokio::test]
async fn allowlist_allows_only_listed_peer() {
    let store = TrustStore::load(temp_store_path("allowlist"))
        .await
        .unwrap();
    let authorizer = PolicyAuthorizer::new(AuthPolicy::allow_peers(["peer-a"]), store);

    assert_eq!(
        authorizer.authorize(&request("peer-a", "tcp:80")).await,
        AuthDecision::Allow
    );
    assert_eq!(
        authorizer.authorize(&request("peer-b", "tcp:80")).await,
        AuthDecision::Deny
    );
}

#[tokio::test]
async fn deny_unknown_denies_without_trust_entry() {
    let store = TrustStore::load(temp_store_path("deny")).await.unwrap();
    let authorizer = PolicyAuthorizer::new(AuthPolicy::DenyUnknown, store);

    let decision = authorizer.authorize(&request("peer-a", "tcp:80")).await;

    assert_eq!(decision, AuthDecision::Deny);
    assert!(!decision.is_allowed());
}

#[tokio::test]
async fn trust_store_persists_remembered_decisions() {
    let path = temp_store_path("persist");
    let store = TrustStore::load(&path).await.unwrap();
    store
        .remember(
            TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            },
            TrustDecision::Allow,
        )
        .await
        .unwrap();

    let loaded = TrustStore::load(&path).await.unwrap();
    let decision = loaded
        .get(&TrustKey {
            peer_id: "peer-a".to_string(),
            forward_key: "tcp:80".to_string(),
        })
        .await;

    assert_eq!(decision, Some(TrustDecision::Allow));
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn trust_store_lists_entries_in_stable_order() {
    let path = temp_store_path("list");
    let store = TrustStore::load(&path).await.unwrap();
    store
        .remember(
            TrustKey {
                peer_id: "peer-b".to_string(),
                forward_key: "udp:53".to_string(),
            },
            TrustDecision::Deny,
        )
        .await
        .unwrap();
    store
        .remember(
            TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            },
            TrustDecision::Allow,
        )
        .await
        .unwrap();

    let entries = store.list().await;

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key.peer_id, "peer-a");
    assert_eq!(entries[1].key.peer_id, "peer-b");
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn trust_store_removes_entry() {
    let path = temp_store_path("remove");
    let key = TrustKey {
        peer_id: "peer-a".to_string(),
        forward_key: "tcp:80".to_string(),
    };
    let store = TrustStore::load(&path).await.unwrap();
    store
        .remember(key.clone(), TrustDecision::Allow)
        .await
        .unwrap();

    assert!(store.remove(&key).await.unwrap());
    assert_eq!(store.get(&key).await, None);

    let loaded = TrustStore::load(&path).await.unwrap();
    assert_eq!(loaded.get(&key).await, None);
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn trust_store_takes_precedence_over_policy() {
    let path = temp_store_path("precedence");
    let store = TrustStore::load(&path).await.unwrap();
    store
        .remember(
            TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            },
            TrustDecision::Deny,
        )
        .await
        .unwrap();
    let authorizer = PolicyAuthorizer::new(AuthPolicy::AutoAccept, store);

    let decision = authorizer.authorize(&request("peer-a", "tcp:80")).await;

    assert_eq!(decision, AuthDecision::Deny);
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn policy_authorizer_records_policy_decision_events() {
    let store = TrustStore::load(temp_store_path("audit-policy"))
        .await
        .unwrap();
    let audit_log = AuthAuditLog::default();
    let authorizer =
        PolicyAuthorizer::with_audit_log(AuthPolicy::DenyUnknown, store, audit_log.clone());

    let decision = authorizer.authorize(&request("peer-a", "tcp:80")).await;
    let events = audit_log.list().await;

    assert_eq!(decision, AuthDecision::Deny);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].peer_id, "peer-a");
    assert_eq!(events[0].forward_key, "tcp:80");
    assert_eq!(events[0].decision, AuthDecision::Deny);
    assert_eq!(events[0].source, AuthEventSource::Policy);
}

#[tokio::test]
async fn policy_authorizer_records_trust_store_decision_events() {
    let path = temp_store_path("audit-trust");
    let store = TrustStore::load(&path).await.unwrap();
    store
        .remember(
            TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            },
            TrustDecision::Allow,
        )
        .await
        .unwrap();
    let audit_log = AuthAuditLog::default();
    let authorizer =
        PolicyAuthorizer::with_audit_log(AuthPolicy::DenyUnknown, store, audit_log.clone());

    let decision = authorizer.authorize(&request("peer-a", "tcp:80")).await;
    let events = audit_log.list().await;

    assert_eq!(decision, AuthDecision::Allow);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].decision, AuthDecision::Allow);
    assert_eq!(events[0].source, AuthEventSource::TrustStore);
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn audit_log_retains_only_capacity() {
    let audit_log = AuthAuditLog::with_capacity(1);

    audit_log
        .record(
            &request("peer-a", "tcp:80"),
            AuthDecision::Allow,
            AuthEventSource::Policy,
        )
        .await;
    audit_log
        .record(
            &request("peer-b", "udp:53"),
            AuthDecision::Deny,
            AuthEventSource::Policy,
        )
        .await;

    let events = audit_log.list().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].peer_id, "peer-b");
    assert_eq!(events[0].sequence, 2);
}
