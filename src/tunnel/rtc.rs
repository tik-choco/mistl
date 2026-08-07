//! The mistlib-backed peer/session manager for mistl's P2P tunnel, ported
//! from the standalone `p2p` crate's `rtc` module (`p2p/src/rtc.rs` and its
//! `manager/` submodules).
//!
//! See `manager.rs`'s module doc comment for the seams that changed in this
//! port (re-seating onto `crate::net` instead of mistlib's singleton calls)
//! and `TUNNEL_INTEGRATION_CONTRACT.md` for the full rationale and the
//! frozen `RTCManagerHandle` API every other tunnel worker codes against.
//!
//! This port flattens upstream's `rtc/manager/{event,handlers,payload,
//! state}.rs` (nested under `manager/`) to `rtc/{event,handlers,payload,
//! state}.rs` (siblings of `manager.rs`), per this integration's file
//! ownership table -- cross-module references inside those files are
//! adjusted accordingly (see each file's own doc comment).

mod event;
mod handlers;
pub mod manager;
pub mod payload;
mod state;

#[cfg(test)]
mod tests;

pub use crate::tunnel::wire::TunnelMessage;
// `RTCManagerHandle` itself is reachable as `rtc::manager::RTCManagerHandle`;
// only its `RTCManager` alias is re-exported here, since that is the name
// every consumer (tcp/udp/proxy/controller/session) actually imports.
pub use manager::{ForwardRequestEvent, ForwardResponseEvent, RTCManager};
