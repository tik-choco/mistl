//! Room-scoped block exchange through mistlib's host storage API.
//!
//! `StoreBlockProvider` exposes mistl's local blocks to peers in every joined
//! room, including rooms joined by other features. `RoomPeerResolver` fetches
//! missing blocks only from the caller's `ResolveScope`, concurrently across
//! its rooms under one timeout budget. Browser peers (tc-news/tc-chat/tc-storage)
//! continue to interoperate through mistlib's native/wasm storage protocol.
//!
//! mistlib owns discovery, response chunking, sender/room binding, cancellation
//! and CID verification. No storage frames need to reach mistl's raw handlers.
//! Its process-wide cache stays separate from mistl's Store and metadata:
//! `fetch_block` returns verified bytes without writing to that cache.

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use mistlib::storage::{
    BlockProvider, BlockProviderHandle, FetchOptions, fetch_block, register_block_provider,
};
use mistlib_core::storage::PeerResolver;
use mistlib_core::types::NodeId;

/// End-to-end budget for each missing block resolved by plain `Store::get`.
pub(super) const DEFAULT_REMOTE_TIMEOUT: Duration = Duration::from_secs(5);

/// Preserve the previous resolver's 8 MiB receive ceiling. This bounds a block,
/// not a whole file (whose manifest/chunks are resolved separately).
const MAX_REMOTE_BLOCK_BYTES: usize = 8 * 1024 * 1024;

/// Rooms + timeout a single [`PeerResolver::resolve_block`] call (nested
/// inside a `Store::get`/`Store::get_remote` call) should use.
/// `resolve_block`'s signature is fixed by the trait (no room/timeout
/// params), so the caller threads this through via a task-local instead --
/// see [`with_scope`].
#[derive(Clone, Debug)]
pub(super) struct ResolveScope {
    pub rooms: Vec<String>,
    pub timeout: Duration,
}

tokio::task_local! {
    static SCOPE: ResolveScope;
}

/// Run `fut` with `scope` visible to any [`RoomPeerResolver::resolve_block`]
/// call nested inside it. Task-locals propagate through ordinary `.await`s
/// and through the `FuturesUnordered`-based fan-out `StorageEngine::get`
/// uses internally to fetch chunks concurrently (they only stop propagating
/// across a `tokio::spawn` boundary, which `StorageEngine` never crosses),
/// so a single `with_scope` around the whole `engine.get(cid)` call covers
/// every chunk it may need to resolve remotely.
pub(super) async fn with_scope<F: std::future::Future>(scope: ResolveScope, fut: F) -> F::Output {
    SCOPE.scope(scope, fut).await
}

/// Reads blocks without resolving misses remotely or consulting the file index.
/// mistlib invokes providers for inbound requests in a room session only. As
/// with the former room handler, every joined room may read local blocks;
/// `Store::rooms` restricts default outbound fetching, not inbound publication.
struct StoreBlockProvider {
    store: Arc<super::Store>,
}

#[async_trait]
impl BlockProvider for StoreBlockProvider {
    async fn get_block(&self, _room_id: &str, _from: &NodeId, cid: &str) -> Option<Vec<u8>> {
        self.store.get_local(cid).await.ok().flatten()
    }
}

/// Register after opening STORE and before joining its configured rooms.
/// Holding the handle for the process lifetime keeps the provider registered;
/// repeated `store()` calls do not add providers. Other features' joined rooms
/// are served too, without adding them to `storage.room_ids`.
pub(super) fn ensure_block_provider_registered(store: &Arc<super::Store>) {
    static PROVIDER: OnceLock<BlockProviderHandle> = OnceLock::new();
    PROVIDER.get_or_init(|| {
        register_block_provider(Arc::new(StoreBlockProvider {
            store: store.clone(),
        }))
    });
}

/// Fetches missing blocks using mistlib's room-scoped API.
#[derive(Default)]
pub struct RoomPeerResolver;

impl RoomPeerResolver {
    pub fn new() -> Self {
        Self
    }

    // Keep the API boundary injectable so scope, fan-out and cancellation can
    // be tested without live signaling/WebRTC or process-wide engine mutation.
    async fn resolve_using<F, Fut>(&self, cid: &str, mut fetch: F) -> Option<Vec<u8>>
    where
        F: FnMut(String, String, FetchOptions) -> Fut,
        Fut: Future<Output = mistlib::Result<Vec<u8>>>,
    {
        let scope = SCOPE.try_with(Clone::clone).unwrap_or(ResolveScope {
            rooms: Vec::new(),
            timeout: DEFAULT_REMOTE_TIMEOUT,
        });
        let mut pending = FuturesUnordered::new();
        for room in &scope.rooms {
            pending.push(fetch(
                room.clone(),
                cid.to_string(),
                FetchOptions {
                    peer: None,
                    max_bytes: MAX_REMOTE_BLOCK_BYTES,
                    timeout: scope.timeout,
                },
            ));
        }
        // All rooms share the full budget concurrently: a silent first room
        // must not delay a live second room, or multiply the caller's timeout.
        let receive = async {
            while let Some(result) = pending.next().await {
                match result {
                    Ok(data) => return Some(data),
                    Err(error) => tracing::debug!(%error, %cid, "storage: room block fetch failed"),
                }
            }
            None
        };
        match tokio::time::timeout(scope.timeout, receive).await {
            Ok(data) => data,
            Err(_) => {
                tracing::debug!(%cid, timeout = ?scope.timeout, "storage: block fetch timed out");
                None
            }
        }
        // Dropping pending fetches lets mistlib cancel their registrations.
    }
}

#[async_trait]
impl PeerResolver for RoomPeerResolver {
    async fn resolve_block(&self, cid: &str) -> Option<Vec<u8>> {
        self.resolve_using(cid, |room, cid, opts| async move {
            fetch_block(&room, &cid, opts).await
        })
        .await
    }
}

#[cfg(test)]
mod tests;
