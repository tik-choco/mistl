//! Ported verbatim from `p2p/src/rtc/manager/tests/membership.rs`.

use std::sync::{Arc, Mutex};

use super::super::event::{handle_join, handle_leave};
use super::super::state::PeerRole;
use super::test_manager;

/// (B)/session-epoch coverage: each `EVENT_JOIN` for a peer_id bumps its
/// epoch, regardless of intervening leaves; a peer that never joined reads
/// as epoch `0`.
#[tokio::test]
async fn epoch_increments_on_each_join() {
    let manager = test_manager("self", PeerRole::Client);
    assert_eq!(manager.peer_epoch("peer-1").await, 0);

    handle_join(manager.inner.clone(), "peer-1".to_string()).await;
    assert_eq!(manager.peer_epoch("peer-1").await, 1);

    handle_leave(manager.inner.clone(), "peer-1".to_string()).await;
    handle_join(manager.inner.clone(), "peer-1".to_string()).await;
    assert_eq!(manager.peer_epoch("peer-1").await, 2);

    handle_join(manager.inner.clone(), "peer-1".to_string()).await;
    assert_eq!(manager.peer_epoch("peer-1").await, 3);

    // A different peer_id has its own independent epoch.
    assert_eq!(manager.peer_epoch("peer-2").await, 0);
}

/// `EVENT_JOIN` for our own id is a no-op (mistlib can echo our own presence
/// back), so it must not consume an epoch or fire join hooks.
#[tokio::test]
async fn self_join_does_not_bump_epoch_or_fire_hooks() {
    let manager = test_manager("self", PeerRole::Client);
    let joins = Arc::new(Mutex::new(Vec::new()));
    {
        let joins = joins.clone();
        manager
            .on_peer_join(move |peer_id, epoch| {
                joins.lock().unwrap().push((peer_id, epoch));
            })
            .await;
    }

    handle_join(manager.inner.clone(), "self".to_string()).await;

    assert_eq!(manager.peer_epoch("self").await, 0);
    assert!(joins.lock().unwrap().is_empty());
}

/// on_peer_join/on_peer_leave hooks fire with the peer_id and the epoch the
/// peer held at that point: epoch 1 for the first join, still epoch 1 for
/// the leave that follows it (the leave doesn't bump the epoch), and epoch 2
/// for the rejoin.
#[tokio::test]
async fn join_and_leave_hooks_fire_with_peer_id_and_epoch() {
    let manager = test_manager("self", PeerRole::Client);
    let joins = Arc::new(Mutex::new(Vec::new()));
    let leaves = Arc::new(Mutex::new(Vec::new()));

    {
        let joins = joins.clone();
        manager
            .on_peer_join(move |peer_id, epoch| {
                joins.lock().unwrap().push((peer_id, epoch));
            })
            .await;
    }
    {
        let leaves = leaves.clone();
        manager
            .on_peer_leave(move |peer_id, epoch| {
                leaves.lock().unwrap().push((peer_id, epoch));
            })
            .await;
    }

    handle_join(manager.inner.clone(), "peer-1".to_string()).await;
    handle_leave(manager.inner.clone(), "peer-1".to_string()).await;
    handle_join(manager.inner.clone(), "peer-1".to_string()).await;

    assert_eq!(
        *joins.lock().unwrap(),
        vec![("peer-1".to_string(), 1), ("peer-1".to_string(), 2)]
    );
    assert_eq!(*leaves.lock().unwrap(), vec![("peer-1".to_string(), 1)]);
}

/// Every registered leave hook must fire, not just the first one.
#[tokio::test]
async fn multiple_leave_hooks_all_fire() {
    let manager = test_manager("self", PeerRole::Client);
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));

    {
        let first = first.clone();
        manager
            .on_peer_leave(move |peer_id, epoch| {
                first.lock().unwrap().push((peer_id, epoch));
            })
            .await;
    }
    {
        let second = second.clone();
        manager
            .on_peer_leave(move |peer_id, epoch| {
                second.lock().unwrap().push((peer_id, epoch));
            })
            .await;
    }

    handle_join(manager.inner.clone(), "peer-1".to_string()).await;
    handle_leave(manager.inner.clone(), "peer-1".to_string()).await;

    assert_eq!(*first.lock().unwrap(), vec![("peer-1".to_string(), 1)]);
    assert_eq!(*second.lock().unwrap(), vec![("peer-1".to_string(), 1)]);
}
