//! Automatic slot replication across escrow providers sharing a room.
pub(super) mod decision;
pub(super) mod placement;
mod proxy;
mod service;
#[cfg(test)]
mod tests;
pub(super) mod wire;
pub(super) use service::Mesh;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
