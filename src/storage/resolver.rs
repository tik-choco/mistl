//! P2P block resolver wiring `mistlib_core::storage::PeerResolver` to
//! mistl's own room-based transport (`crate::net`), speaking the same
//! QUERY/WANT/HAVE(_CHUNK) wire protocol
//! (`mistlib_core::storage::protocol`) that mistlib-native's
//! shared `NetworkPeerResolver` speaks --
//! so a CID missing from the local block store can be resolved from any
//! peer sharing a joined room, including a browser tab running
//! tc-news/tc-chat/tc-storage's wasm build.
//!
//! mistlib-native also initializes its own process-wide storage cache when
//! joining a room. That cache is separate from mistl's Store and its metadata,
//! so this resolver uses the public room-scoped `net` APIs to serve and fetch
//! mistl's blocks. Storage frames must reach mistl's room handlers; stock
//! mistlib 0.7.0 consumes them before raw dispatch, so embedding it requires
//! an upstream storage-event forwarding hook.
//!
//! `PeerResolver::resolve_block` is declared in mistlib-core as an
//! `#[async_trait]` method; unlike the old `NoopResolver` (which hand-wrote
//! the desugared signature to avoid a direct `async-trait` dependency),
//! this impl uses the `#[async_trait]` attribute directly -- mistl already
//! depends on `async-trait` for the consensus `RaftTransport` impl.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use mistlib_core::storage::PeerResolver;
use mistlib_core::storage::protocol::{
    WantRegistry, build_have_payload, build_have_status_message, build_query_message,
    build_want_message, chunk_size_for_limit, have_chunk_count, parse_have_chunk_message,
    parse_have_message, parse_have_status_message, parse_query_message, parse_want_message,
};
use mistlib_core::types::NodeId;

/// How long a resolution waits, end to end, when the caller (plain
/// `Store::get`) doesn't ask for a specific budget. Retains mistl's existing
/// five-second budget independently of the shared resolver's longer attempts.
pub(super) const DEFAULT_REMOTE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for at least one HAVE_STATUS reply to a QUERY broadcast
/// before trying sender-bound WANTs to discovered or connected peers.
const DISCOVERY_TIMEOUT: Duration = Duration::from_millis(500);

/// Per-peer WANT timeout during round-robin failover, derived from the
/// overall budget so a slow/dead first peer can't burn the whole budget
/// before a live second peer gets a turn. Clamped to a sane range: never so
/// short a healthy peer has no realistic chance to reply, never so long a
/// single peer can eat an entire multi-second budget by itself.
fn per_peer_timeout(total: Duration) -> Duration {
    (total / 3).clamp(Duration::from_millis(300), Duration::from_secs(3))
}

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

/// Process-wide pending-request/peer-cache bookkeeping, shared between
/// [`RoomPeerResolver::resolve_block`] (which registers oneshot waiters and
/// broadcasts QUERY/WANT) and [`ensure_wire_handler_registered`]'s inbound
/// handler (which fulfills them on HAVE_STATUS/HAVE/HAVE_CHUNK). A
/// `LazyLock`, not a field on [`super::Store`]/[`RoomPeerResolver`], because
/// the wire handler is registered independently of any particular `Store`
/// instance (see `ensure_wire_handler_registered`'s doc comment) and both
/// sides must share the exact same registry.
static REGISTRY: std::sync::LazyLock<WantRegistry> = std::sync::LazyLock::new(WantRegistry::new);

// Drop the resolution future (and its receivers) before pruning. Cancellation
// must never cancel another concurrent request for the same CID.
struct RegistryCleanup;

impl Drop for RegistryCleanup {
    fn drop(&mut self) {
        REGISTRY.cancel_closed();
    }
}

/// [`PeerResolver`] backed by mistl's own room transport (`crate::net`)
/// instead of a no-op. There's only ever one instance (owned by the single
/// process-wide [`super::Store`]), paired with the process-wide [`REGISTRY`]
/// above.
pub struct RoomPeerResolver {
    /// Round-robins WANT targets across known peers so repeated resolutions
    /// don't hammer the same one first every time.
    next_peer: AtomicUsize,
}

impl RoomPeerResolver {
    pub fn new() -> Self {
        Self {
            next_peer: AtomicUsize::new(0),
        }
    }
}

impl Default for RoomPeerResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PeerResolver for RoomPeerResolver {
    async fn resolve_block(&self, cid: &str) -> Option<Vec<u8>> {
        let scope = SCOPE.try_with(Clone::clone).unwrap_or(ResolveScope {
            rooms: Vec::new(),
            timeout: DEFAULT_REMOTE_TIMEOUT,
        });
        if scope.rooms.is_empty() {
            tracing::debug!("storage: resolve_block({cid}) has no rooms in scope, skipping");
            return None;
        }

        let _cleanup = RegistryCleanup;
        match tokio::time::timeout(
            scope.timeout,
            self.resolve_within(cid, &scope.rooms, scope.timeout),
        )
        .await
        {
            Ok(data) => data,
            Err(_) => {
                tracing::debug!(
                    "storage: resolve_block({cid}) timed out after {:?}",
                    scope.timeout
                );
                None
            }
        }
    }
}

impl RoomPeerResolver {
    /// QUERY discovery followed by sender-bound WANTs with failover, fanned
    /// out across only the requested rooms. Bounded by the outer timeout in
    /// [`PeerResolver::resolve_block`] above, not by anything in here.
    async fn resolve_within(
        &self,
        cid: &str,
        rooms: &[String],
        budget: Duration,
    ) -> Option<Vec<u8>> {
        let mut known_peers = REGISTRY.get_peers(cid);
        if known_peers.is_empty() {
            tracing::debug!("storage: discovery phase for {cid}");
            let rx_peer = REGISTRY.register_peer_notifier(cid);
            broadcast_to_rooms(rooms, build_query_message(cid)).await;
            let _ = tokio::time::timeout(DISCOVERY_TIMEOUT, rx_peer).await;
            known_peers = REGISTRY.get_peers(cid);
        }

        let per_attempt = per_peer_timeout(budget);
        if !known_peers.is_empty() {
            let start = self.next_peer.fetch_add(1, Ordering::Relaxed);
            for offset in 0..known_peers.len() {
                let target = &known_peers[(start + offset) % known_peers.len()];
                let attempt = REGISTRY.register_attempt(cid, target.clone());
                send_to_rooms(rooms, &target.0, build_want_message(cid), attempt.id).await;
                if let Ok(Ok(data)) = tokio::time::timeout(per_attempt, attempt.data).await {
                    return Some(data);
                }
                REGISTRY.cancel_closed();
                tracing::debug!(
                    "storage: peer {} did not deliver {cid}, failing over",
                    target.0
                );
            }
        }

        // Snapshot the recipients, then unicast so each waiter is bound to
        // the actual sender and rooms contacted, including routed providers
        // already discovered above. Never accept a reply from an arbitrary peer.
        tracing::debug!("storage: trying connected peers for {cid}");
        let mut peers = known_peers;
        for (room, connections) in crate::net::room_connections().await {
            if rooms.contains(&room) {
                peers.extend(connections.into_iter().map(|(peer, _)| NodeId(peer)));
            }
        }
        peers.sort_by(|a, b| a.0.cmp(&b.0));
        peers.dedup();
        use futures_util::stream::{FuturesUnordered, StreamExt};
        let mut pending = FuturesUnordered::new();
        for peer in peers {
            let attempt = REGISTRY.register_attempt(cid, peer.clone());
            send_to_rooms(rooms, &peer.0, build_want_message(cid), attempt.id).await;
            pending.push(attempt.data);
        }
        while let Some(result) = pending.next().await {
            if let Ok(data) = result {
                return Some(data);
            }
        }
        None
    }
}

/// Broadcasts `data` into every room in `rooms`. Fire-and-forget: a room
/// this QUERY/WANT isn't relevant to simply has no matching peer, and a
/// send failure (e.g. transient "room not joined") isn't worth failing the
/// whole resolution over -- the caller's own timeout is the real backstop.
async fn broadcast_to_rooms(rooms: &[String], data: Vec<u8>) {
    for room in rooms {
        let _ = crate::net::send_broadcast(room, data.clone()).await;
    }
}

/// Sends `data` to `target` in every room in `rooms`. We don't track which
/// room a peer was discovered through -- `HAVE_STATUS` only carries a node
/// id, not a room (see `ensure_wire_handler_registered`'s `HaveStatus`
/// handling below) -- so this fans the unicast WANT out across every room
/// in scope, retaining only paths where the local write succeeds.
async fn send_to_rooms(rooms: &[String], target: &str, data: Vec<u8>, attempt: u64) {
    for room in rooms {
        let attached = REGISTRY.track_path(attempt, room);
        if crate::net::send_direct_wait(room, target, data.clone())
            .await
            .is_err()
            && attached
        {
            REGISTRY.untrack_path(attempt, room);
        }
    }
}

/// Registers the process-wide inbound handler for the storage wire protocol
/// (QUERY/WANT/HAVE/HAVE_STATUS/HAVE_CHUNK), once per process. Idempotent --
/// safe (and expected) to call on every [`super::store`] call.
///
/// Registered via [`crate::net::register_room_handler`], which fires for
/// events from *every* joined room, not just `storage.room_ids` -- so a room
/// joined by another flow entirely (a future bot pipeline's
/// `tc-global-articles` room, a folder sync/share room, `[ai]`'s room) still
/// gets QUERY/WANT served from this node's local blocks, and `get_remote`
/// can still resolve CIDs from peers in it, without that room needing to be
/// added to `storage.room_ids`.
///
/// Serving QUERY/WANT reads the local store via `super::STORE.get()`; if
/// that's not populated yet (a message arriving in the brief window before
/// `Store::open` finishes and installs itself into `STORE`), the request is
/// simply dropped -- harmless, since a real peer's own `resolve_block`
/// already treats "no reply" as "try someone else" / retry, the same way it
/// treats a peer that just doesn't have the block.
pub(super) fn ensure_wire_handler_registered() {
    static REGISTERED: std::sync::Once = std::sync::Once::new();
    REGISTERED.call_once(|| {
        let rt = tokio::runtime::Handle::current();
        crate::net::register_room_handler(move |event_type, room, from, data| {
            if event_type == crate::net::EVENT_LEAVE {
                REGISTRY.sender_reset(&NodeId(from.to_string()), room);
            }
            if event_type != crate::net::EVENT_RAW {
                return;
            }
            if let Some(cid) = parse_want_message(data) {
                let (room, from) = (room.to_string(), from.to_string());
                rt.spawn(async move { reply_have(&room, &from, &cid).await });
            } else if let Some(cid) = parse_query_message(data) {
                let (room, from) = (room.to_string(), from.to_string());
                rt.spawn(async move { reply_have_status(&room, &from, &cid).await });
            } else {
                receive_reply(&REGISTRY, room, from, data);
            }
        });
    });
}

fn receive_reply(registry: &WantRegistry, room: &str, from: &str, data: &[u8]) {
    let sender = NodeId(from.to_string());
    if let Some(cid) = parse_have_status_message(data) {
        registry.register_peer(&cid, sender);
    } else if let Some((cid, payload)) = parse_have_message(data) {
        registry.fulfill_in(room, &cid, &sender, payload);
    } else if let Some((cid, index, total, payload)) = parse_have_chunk_message(data) {
        registry.fulfill_chunk_in(room, &cid, &sender, index, total, payload);
    }
}

/// Answers an inbound QUERY: if we have `cid` locally, tell `from` so (via
/// HAVE_STATUS) so it adds us to its peer list for a follow-up WANT. Silent
/// otherwise -- matches `NativePeerResolver`/wasm's own `handle_query`: no
/// reply means "don't know", not an error.
async fn reply_have_status(room: &str, from: &str, cid: &str) {
    let Some(store) = super::STORE.get() else {
        return;
    };
    let Ok(Some(_)) = store.get_local(cid).await else {
        return;
    };
    let _ = crate::net::send_direct(room, from, build_have_status_message(cid)).await;
}

/// Answers an inbound WANT: if we have `cid` locally, send it back to
/// `from` as one HAVE (small blocks) or a sequence of HAVE_CHUNK frames
/// (blocks over `HAVE_CHUNK_SIZE`), mirroring `handle_want` in both
/// mistlib-native's `storage.rs` and mistlib-wasm's `storage.rs`.
async fn reply_have(room: &str, from: &str, cid: &str) {
    let Some(store) = super::STORE.get() else {
        return;
    };
    let Ok(Some(data)) = store.get_local(cid).await else {
        return;
    };
    // The public app transport does not expose its negotiated per-peer limit.
    // Reserve room for the overlay envelope within the browser's 16 KiB ceiling.
    let chunk_size = chunk_size_for_limit(cid, Some(16 * 1024));
    let Some(total_chunks) = have_chunk_count(data.len(), chunk_size) else {
        tracing::warn!(
            "storage: refusing to serve oversized block {cid} ({} bytes)",
            data.len()
        );
        return;
    };
    for chunk_index in 0..total_chunks {
        let msg = build_have_payload(cid, &data, chunk_index, total_chunks, chunk_size);
        if let Err(error) = crate::net::send_direct_wait(room, from, msg).await {
            tracing::debug!(%error, %cid, "storage: block response failed");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn want_message_roundtrips_with_parse_want_message() {
        let cid =
            mistlib_core::storage::compute_cid(b"test", mistlib_core::storage::cid::MULTICODEC_RAW);
        let msg = build_want_message(&cid);
        assert_eq!(parse_want_message(&msg).as_deref(), Some(cid.as_str()));
    }

    #[test]
    fn storage_replies_only_complete_the_requested_sender_and_room() {
        use mistlib_core::storage::cid::{MULTICODEC_RAW, compute_cid};
        use mistlib_core::storage::protocol::build_have_message;
        let registry = WantRegistry::new();
        let cid = compute_cid(b"test", MULTICODEC_RAW);
        let mut first = registry.register_attempt(&cid, NodeId("first".into()));
        let mut second = registry.register_attempt(&cid, NodeId("second".into()));
        registry.track_path(first.id, "room");
        registry.track_path(second.id, "room");
        let frame = build_have_message(&cid, b"test");
        receive_reply(&registry, "other-room", "first", &frame);
        receive_reply(&registry, "room", "unsolicited", &frame);
        assert!(first.data.try_recv().is_err());
        assert!(second.data.try_recv().is_err());
        receive_reply(&registry, "room", "first", &frame);
        assert_eq!(first.data.try_recv().unwrap(), b"test");
        assert!(second.data.try_recv().is_err());
        receive_reply(&registry, "room", "second", &frame);
        assert_eq!(second.data.try_recv().unwrap(), b"test");
    }

    #[test]
    fn unordered_storage_chunks_remain_separate_across_rooms_and_senders() {
        use mistlib_core::storage::cid::{MULTICODEC_RAW, compute_cid};
        use mistlib_core::storage::protocol::build_have_chunk_message;
        let registry = WantRegistry::new();
        let cid = compute_cid(b"test", MULTICODEC_RAW);
        let mut attempt = registry.register_attempt(&cid, NodeId("peer".into()));
        registry.track_path(attempt.id, "room");
        let tail = build_have_chunk_message(&cid, 1, 2, b"st");
        let head = build_have_chunk_message(&cid, 0, 2, b"te");
        receive_reply(&registry, "room", "peer", &tail);
        receive_reply(&registry, "other-room", "peer", &head);
        receive_reply(&registry, "room", "other-peer", &head);
        assert!(attempt.data.try_recv().is_err());
        receive_reply(&registry, "room", "peer", &head);
        assert_eq!(attempt.data.try_recv().unwrap(), b"test");
    }

    #[test]
    fn cancelling_one_storage_waiter_preserves_another_for_the_same_cid() {
        use mistlib_core::storage::cid::{MULTICODEC_RAW, compute_cid};
        use mistlib_core::storage::protocol::build_have_message;
        let registry = WantRegistry::new();
        let cid = compute_cid(b"test", MULTICODEC_RAW);
        let cancelled = registry.register_attempt(&cid, NodeId("peer".into()));
        let mut active = registry.register_attempt(&cid, NodeId("peer".into()));
        registry.track_path(cancelled.id, "room");
        registry.track_path(active.id, "room");
        drop(cancelled);
        registry.cancel_closed();
        receive_reply(
            &registry,
            "room",
            "peer",
            &build_have_message(&cid, b"test"),
        );
        assert_eq!(active.data.try_recv().unwrap(), b"test");
    }

    #[test]
    fn per_peer_timeout_is_clamped_to_a_sane_range() {
        // Below the floor: too-small a budget still gets a fighting chance.
        assert_eq!(
            per_peer_timeout(Duration::from_millis(1)),
            Duration::from_millis(300)
        );
        // Within range: a third of the budget, unclamped.
        assert_eq!(
            per_peer_timeout(Duration::from_secs(5)),
            Duration::from_secs(5) / 3
        );
        // Above the ceiling: one huge peer attempt can't eat the whole budget.
        assert_eq!(
            per_peer_timeout(Duration::from_secs(30)),
            Duration::from_secs(3)
        );
    }

    #[tokio::test]
    async fn resolve_block_with_no_rooms_in_scope_returns_none_immediately() {
        // Both an explicit empty-rooms scope and no scope at all (e.g. a
        // hypothetical future caller of the trait method directly, which
        // falls back to the same "no rooms" empty scope via
        // `SCOPE.try_with`'s `unwrap_or`) hit the exact same early return in
        // `resolve_block`: an empty room list is resolved as "nothing to
        // query" up front, before the per-call timeout wrapper even starts
        // waiting on anything. Must not actually wait out the 30s budget.
        let resolver = RoomPeerResolver::new();
        let scope = ResolveScope {
            rooms: Vec::new(),
            timeout: Duration::from_secs(30),
        };
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            with_scope(scope, resolver.resolve_block("bsome-cid")),
        )
        .await
        .expect("resolve_block should return promptly with no rooms in scope");
        assert_eq!(result, None);

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            resolver.resolve_block("bsome-other-cid"),
        )
        .await
        .expect("resolve_block should return promptly outside any scope");
        assert_eq!(result, None);
    }
}
