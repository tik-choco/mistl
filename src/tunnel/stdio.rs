//! Client-side stdio piping: bridges this node's local stdin/stdout to a
//! peer's stdio session (the `p2p connect`/"get a remote shell" direction).
//! Ported unchanged in structure from the standalone `p2p` crate's
//! `src/stdio.rs`; see `bridge.rs`/`packet.rs` for what actually changed.
//!
//! The *server*-side counterpart -- running a local command for a peer --
//! is `crate::tunnel::proxy::Executor` (owned by W2), not this module; see
//! `crate::tunnel::session::maybe_start_stdio_executor`'s doc comment for
//! the two-gate model gating whether this node ever starts that at all.

pub mod bridge;
pub mod packet;

/// Ported and kept compiling, but not currently reachable: attaching a local
/// terminal to a peer's stdio session needs a byte-stream channel between the
/// CLI process and the daemon that owns the tunnel session, and this daemon's
/// IPC is newline-delimited JSON request/response (see `daemon::ipc`) with no
/// streaming mode. The *server* side of stdio -- running a command on behalf
/// of a peer -- is wired and gated (`crate::tunnel::proxy::Executor`, started
/// by `session::maybe_start_stdio_executor`); only this client half awaits an
/// IPC transport that can carry a stream.
#[allow(unused_imports)]
pub use bridge::Bridge;
