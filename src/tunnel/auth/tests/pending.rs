use super::{request, temp_store_path};
use crate::tunnel::auth::pending::DECISION_TIMEOUT;
use crate::tunnel::auth::*;

use tokio::time::{Duration, sleep, timeout};

#[tokio::test]
async fn pending_authorizer_queues_unknown_until_resolved() {
    let store = TrustStore::load(temp_store_path("pending-queue"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store, pending.clone());

    let task =
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await });
    let items = wait_for_pending(&pending).await;

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].request.peer_id, "peer-a");
    assert!(pending.resolve(items[0].id, AuthDecision::Allow).await);
    assert_eq!(task.await.unwrap(), AuthDecision::Allow);
}

#[tokio::test]
async fn pending_authorizer_remembers_allow_always_under_the_did() {
    let path = temp_store_path("pending-remember");
    let store = TrustStore::load(&path).await.unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store.clone(), pending.clone());
    let (req, _, did) = super::verified_request("tcp:80");

    let task = tokio::spawn(async move { authorizer.authorize(&req).await });
    let id = wait_for_pending(&pending).await[0].id;
    assert!(pending.resolve(id, AuthDecision::AllowAlways).await);

    assert_eq!(task.await.unwrap(), AuthDecision::AllowAlways);
    assert_eq!(
        store
            .get(&TrustKey {
                peer_id: did,
                forward_key: "tcp:80".to_string(),
            })
            .await,
        Some(TrustDecision::Allow)
    );
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn pending_authorizer_uses_trust_store_without_queuing() {
    let path = temp_store_path("pending-trust");
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
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store, pending.clone());

    let decision = authorizer.authorize(&request("peer-a", "tcp:80")).await;

    assert_eq!(decision, AuthDecision::Deny);
    assert!(pending.list().await.is_empty());
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn pending_authorizer_records_pending_decision_events() {
    let store = TrustStore::load(temp_store_path("pending-audit"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let audit_log = AuthAuditLog::default();
    let authorizer = PendingAuthorizer::with_audit_log(store, pending.clone(), audit_log.clone());

    let task =
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await });
    let id = wait_for_pending(&pending).await[0].id;
    assert!(pending.resolve(id, AuthDecision::Deny).await);
    assert_eq!(task.await.unwrap(), AuthDecision::Deny);

    let events = audit_log.list().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].decision, AuthDecision::Deny);
    assert_eq!(events[0].source, AuthEventSource::Pending);
}

#[tokio::test]
async fn purge_peer_denies_and_removes_only_that_peers_entries() {
    let store = TrustStore::load(temp_store_path("purge-peer"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer_a = PendingAuthorizer::new(store.clone(), pending.clone());
    let authorizer_b = PendingAuthorizer::new(store.clone(), pending.clone());

    let task_a =
        tokio::spawn(async move { authorizer_a.authorize(&request("peer-a", "tcp:80")).await });
    let task_b =
        tokio::spawn(async move { authorizer_b.authorize(&request("peer-b", "tcp:81")).await });

    // Wait for both to be queued.
    timeout(Duration::from_secs(1), async {
        loop {
            if pending.list().await.len() == 2 {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both requests should be queued");

    let purged = pending.purge_peer("peer-a").await;
    assert_eq!(purged, 1);

    // peer-a's decide() unblocks with Deny; peer-b's entry is untouched.
    assert_eq!(task_a.await.unwrap(), AuthDecision::Deny);
    let remaining = pending.list().await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].request.peer_id, "peer-b");

    // The purge must not have persisted a Deny to the trust store (only an
    // explicit DenyAlways does that).
    assert_eq!(
        store
            .get(&TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            })
            .await,
        None
    );

    assert!(pending.resolve(remaining[0].id, AuthDecision::Allow).await);
    assert_eq!(task_b.await.unwrap(), AuthDecision::Allow);
}

#[tokio::test]
async fn purge_peer_is_a_noop_when_nothing_matches() {
    let pending = PendingAuthorizations::new();
    assert_eq!(pending.purge_peer("nobody").await, 0);
}

#[tokio::test(start_paused = true)]
async fn decide_times_out_to_deny_and_removes_ghost_entry() {
    let store = TrustStore::load(temp_store_path("pending-timeout"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store.clone(), pending.clone());

    let task =
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await });

    // Let the request get queued and the timeout timer register before
    // jumping the clock (mirrors src/tcp/tests.rs's grace-window tests:
    // advancing before the timer exists would just restart the sleep from
    // the new time instead of firing it).
    let _ = wait_for_pending(&pending).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    tokio::time::advance(DECISION_TIMEOUT + Duration::from_secs(1)).await;

    assert_eq!(task.await.unwrap(), AuthDecision::Deny);
    assert!(
        pending.list().await.is_empty(),
        "timed-out entry must not linger as a ghost row"
    );

    // A timeout-driven Deny must not be persisted to the trust store.
    assert_eq!(
        store
            .get(&TrustKey {
                peer_id: "peer-a".to_string(),
                forward_key: "tcp:80".to_string(),
            })
            .await,
        None
    );
}

async fn wait_for_pending(pending: &PendingAuthorizations) -> Vec<PendingAuthorization> {
    timeout(Duration::from_secs(1), async {
        loop {
            let items = pending.list().await;
            if !items.is_empty() {
                return items;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pending auth request should be queued")
}

#[tokio::test]
async fn pending_rows_are_capped_per_peer_and_extras_are_denied() {
    use crate::tunnel::auth::pending::MAX_PENDING_PER_PEER;

    let store = TrustStore::load(temp_store_path("pending-cap"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store, pending.clone());

    let mut tasks = Vec::new();
    for i in 0..MAX_PENDING_PER_PEER {
        let authorizer = authorizer.clone();
        tasks.push(tokio::spawn(async move {
            authorizer
                .authorize(&request("peer-a", &format!("tcp:{}", 1000 + i)))
                .await
        }));
    }
    timeout(Duration::from_secs(1), async {
        while pending.list().await.len() < MAX_PENDING_PER_PEER {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rows should be queued");

    // One over the cap is denied immediately, without parking a new row...
    assert_eq!(
        authorizer.authorize(&request("peer-a", "tcp:9999")).await,
        AuthDecision::Deny
    );
    assert_eq!(pending.list().await.len(), MAX_PENDING_PER_PEER);

    // ...while another peer is unaffected.
    let other = {
        let authorizer = authorizer.clone();
        tokio::spawn(async move { authorizer.authorize(&request("peer-b", "tcp:80")).await })
    };
    timeout(Duration::from_secs(1), async {
        while pending.list().await.len() <= MAX_PENDING_PER_PEER {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("peer-b row should be queued");

    for item in pending.list().await {
        pending.resolve(item.id, AuthDecision::Deny).await;
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), AuthDecision::Deny);
    }
    let _ = other.await;
}

#[tokio::test]
async fn identical_requests_share_one_row_and_one_decision() {
    let store = TrustStore::load(temp_store_path("pending-coalesce"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store, pending.clone());

    // A browser opening several connections to one service at once.
    let tasks = (0..5)
        .map(|_| {
            let authorizer = authorizer.clone();
            tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await })
        })
        .collect::<Vec<_>>();
    let other = {
        let authorizer = authorizer.clone();
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:81")).await })
    };
    timeout(Duration::from_secs(1), async {
        while pending.list().await.len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rows should be queued");
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let rows = pending.list().await;
    assert_eq!(
        rows.len(),
        2,
        "one row per distinct target, not per connection"
    );
    let shared = rows
        .iter()
        .find(|r| r.request.forward_key == "tcp:80")
        .unwrap();
    assert!(pending.resolve(shared.id, AuthDecision::Allow).await);
    for task in tasks {
        assert_eq!(task.await.unwrap(), AuthDecision::Allow);
    }

    let rest = pending.list().await;
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].request.forward_key, "tcp:81");
    pending.resolve(rest[0].id, AuthDecision::Deny).await;
    assert_eq!(other.await.unwrap(), AuthDecision::Deny);
}

#[tokio::test(start_paused = true)]
async fn a_late_waiter_keeps_a_shared_row_after_the_first_times_out() {
    let store = TrustStore::load(temp_store_path("pending-coalesce-timeout"))
        .await
        .unwrap();
    let pending = PendingAuthorizations::new();
    let authorizer = PendingAuthorizer::new(store, pending.clone());

    let first = {
        let authorizer = authorizer.clone();
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await })
    };
    let _ = wait_for_pending(&pending).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(DECISION_TIMEOUT / 2).await;
    let late = {
        let authorizer = authorizer.clone();
        tokio::spawn(async move { authorizer.authorize(&request("peer-a", "tcp:80")).await })
    };
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    tokio::time::advance(DECISION_TIMEOUT / 2 + Duration::from_secs(1)).await;
    assert_eq!(first.await.unwrap(), AuthDecision::Deny);
    let rows = pending.list().await;
    assert_eq!(rows.len(), 1, "the late waiter still needs the row");

    assert!(pending.resolve(rows[0].id, AuthDecision::Allow).await);
    assert_eq!(late.await.unwrap(), AuthDecision::Allow);
}
