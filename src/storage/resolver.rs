//! No-op [`PeerResolver`]: this daemon is a purely local content store for
//! now, so there is nothing to resolve from peers yet. P2P block exchange
//! (like `mistlib-native`'s `NativePeerResolver`) can replace this later
//! without touching [`super::Store`]'s public API.
//!
//! `PeerResolver::resolve_block` is declared in mistlib-core as an `async fn`
//! under `#[async_trait]`, which desugars the trait method into one that
//! returns a boxed, pinned future. `mistl` does not depend on the
//! `async-trait` crate directly (see the deviation note in the storage module
//! report) -- it's only pulled in transitively via `mistlib-core` /
//! `mistlib-native` -- so this impl reproduces that desugared signature by
//! hand rather than using the `#[async_trait]` attribute macro.

use std::future::Future;
use std::pin::Pin;

use mistlib_core::storage::PeerResolver;

/// Always reports "no such block known to any peer".
pub struct NoopResolver;

impl PeerResolver for NoopResolver {
    fn resolve_block<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _cid: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { None })
    }
}
