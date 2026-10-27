use super::*;
use crate::storage::tests::temp_store;
use mistlib_core::storage::cid::MULTICODEC_RAW;
use mistlib_core::storage::{FileManifest, compute_cid};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Cancelled(Arc<AtomicUsize>);

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn provider_serves_local_manifests_and_chunks_in_other_features_rooms() {
    let (store, _dir) = temp_store("provider").await;
    let data = vec![42; 1024 * 1024];
    let root = store.put("shared.bin", data.clone()).await.unwrap();
    let provider: Arc<dyn BlockProvider> = Arc::new(StoreBlockProvider {
        store: Arc::new(store),
    });
    let _handle = register_block_provider(provider.clone());
    let peer = NodeId("browser-peer".into());
    // Publication has always included rooms joined by other features, even
    // when the store has no configured rooms. No file-index row is needed
    // for the individual chunks.
    let manifest = provider.get_block("bot-room", &peer, &root).await.unwrap();
    let manifest: FileManifest = serde_cbor::from_slice(&manifest).unwrap();
    assert!(provider.has_block("folder-room", &peer, &root).await);
    assert_eq!(manifest.chunks.len(), 1);
    let chunk = &manifest.chunks[0];
    assert!(provider.has_block("bot-room", &peer, chunk).await);
    assert_eq!(
        provider.get_block("bot-room", &peer, chunk).await,
        Some(data)
    );

    let missing = compute_cid(b"not stored", MULTICODEC_RAW);
    assert!(!provider.has_block("bot-room", &peer, &missing).await);
    assert_eq!(provider.get_block("bot-room", &peer, &missing).await, None);
}

#[tokio::test]
async fn remote_fetch_uses_all_scoped_rooms_and_cancels_losing_attempts() {
    let (store, _dir) = temp_store("remote-provider").await;
    let data = b"from a room peer".to_vec();
    store.put("remote.txt", data.clone()).await.unwrap();
    let cid = compute_cid(&data, MULTICODEC_RAW);
    let provider = Arc::new(StoreBlockProvider {
        store: Arc::new(store),
    });
    let cancelled = Arc::new(AtomicUsize::new(0));
    let attempted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let scope = ResolveScope {
        rooms: vec!["silent".into(), "failed".into(), "source".into()],
        timeout: DEFAULT_REMOTE_TIMEOUT,
    };
    let result = with_scope(
        scope,
        RoomPeerResolver::new().resolve_using(&cid, |room, requested, opts| {
            let provider = provider.clone();
            let cancelled = cancelled.clone();
            let attempted = attempted.clone();
            async move {
                assert!(opts.peer.is_none());
                assert_eq!(opts.max_bytes, 8 * 1024 * 1024);
                assert_eq!(opts.timeout, DEFAULT_REMOTE_TIMEOUT);
                attempted.lock().unwrap().push(room.clone());
                match room.as_str() {
                    "silent" => {
                        let _guard = Cancelled(cancelled);
                        std::future::pending().await
                    }
                    "failed" => Err(mistlib::MistError::Network("room unavailable".into())),
                    "source" => Ok(provider
                        .get_block(&room, &NodeId("requester".into()), &requested)
                        .await
                        .unwrap()),
                    _ => panic!("fetch escaped the requested rooms"),
                }
            }
        }),
    )
    .await;
    assert_eq!(result, Some(data));
    assert_eq!(*attempted.lock().unwrap(), ["silent", "failed", "source"]);
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn remote_fetch_does_not_consult_a_provider_outside_its_room_scope() {
    let outside = "another-features-room";
    let cid = compute_cid(b"only available outside scope", MULTICODEC_RAW);
    let scope = ResolveScope {
        rooms: vec!["requested-room".into()],
        timeout: DEFAULT_REMOTE_TIMEOUT,
    };
    let result = with_scope(
        scope,
        RoomPeerResolver::new().resolve_using(&cid, |room, _, _| async move {
            if room == outside {
                Ok(b"only available outside scope".to_vec())
            } else {
                Err(mistlib::MistError::Network("not found in this room".into()))
            }
        }),
    )
    .await;
    assert_eq!(result, None);
}

#[tokio::test(start_paused = true)]
async fn silent_rooms_share_one_timeout_budget_and_are_cancelled() {
    let cancelled = Arc::new(AtomicUsize::new(0));
    let start = tokio::time::Instant::now();
    let scope = ResolveScope {
        rooms: vec!["first".into(), "second".into()],
        timeout: DEFAULT_REMOTE_TIMEOUT,
    };
    let result = with_scope(
        scope,
        RoomPeerResolver::new().resolve_using("cid", |_, _, opts| {
            let cancelled = cancelled.clone();
            async move {
                let _guard = Cancelled(cancelled);
                // The outer deadline also protects against a stalled API call.
                tokio::time::sleep(opts.timeout * 2).await;
                Ok(Vec::new())
            }
        }),
    )
    .await;
    assert_eq!(result, None);
    assert_eq!(start.elapsed(), DEFAULT_REMOTE_TIMEOUT);
    assert_eq!(cancelled.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelling_resolution_drops_all_room_fetches() {
    let cancelled = Arc::new(AtomicUsize::new(0));
    let scope = ResolveScope {
        rooms: vec!["first".into(), "second".into()],
        timeout: DEFAULT_REMOTE_TIMEOUT,
    };
    let resolver = RoomPeerResolver::new();
    let future = with_scope(
        scope,
        resolver.resolve_using("cid", |_, _, _| {
            let cancelled = cancelled.clone();
            async move {
                let _guard = Cancelled(cancelled);
                std::future::pending().await
            }
        }),
    );
    let mut future = Box::pin(future);
    assert!(futures_util::poll!(&mut future).is_pending());
    drop(future);
    assert_eq!(cancelled.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn empty_or_absent_scope_returns_immediately_without_fetching() {
    let resolver = RoomPeerResolver::new();
    let scope = ResolveScope {
        rooms: Vec::new(),
        timeout: Duration::from_secs(30),
    };
    let result = with_scope(
        scope,
        resolver.resolve_using(
            "cid",
            |_, _, _| -> std::future::Ready<mistlib::Result<Vec<u8>>> {
                panic!("empty scope must not fetch")
            },
        ),
    )
    .await;
    assert_eq!(result, None);
    assert_eq!(resolver.resolve_block("cid").await, None);
}

#[tokio::test]
async fn fetch_api_rejects_rooms_without_an_active_session() {
    let cid = compute_cid(b"remote block", MULTICODEC_RAW);
    let scope = ResolveScope {
        rooms: vec!["mistl-storage-test-unjoined".into()],
        timeout: DEFAULT_REMOTE_TIMEOUT,
    };
    let result = with_scope(scope, RoomPeerResolver::new().resolve_block(&cid)).await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn local_store_hits_ignore_unjoined_rooms_and_zero_remote_budget() {
    let (store, _dir) = temp_store("local-provider-hit").await;
    let data = b"already stored locally".to_vec();
    let root = store.put("local.txt", data.clone()).await.unwrap();
    assert_eq!(
        store
            .get_remote(&root, &["unjoined-room".into()], Duration::ZERO)
            .await
            .unwrap(),
        data
    );
}

#[tokio::test]
async fn concurrent_resolutions_keep_independent_room_scopes_and_budgets() {
    async fn resolve(room: &str, budget: Duration) {
        let scope = ResolveScope {
            rooms: vec![room.to_string()],
            timeout: budget,
        };
        let result = with_scope(
            scope,
            RoomPeerResolver::new().resolve_using("cid", |requested_room, _, opts| async move {
                tokio::task::yield_now().await;
                assert_eq!(requested_room, room);
                assert_eq!(opts.timeout, budget);
                Ok(requested_room.into_bytes())
            }),
        )
        .await;
        assert_eq!(result, Some(room.as_bytes().to_vec()));
    }
    tokio::join!(
        resolve("bot-room", Duration::from_millis(100)),
        resolve("storage-room", Duration::from_secs(2)),
    );
}
