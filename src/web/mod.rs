//! Embedded web dashboard: a single self-contained HTML page served by the
//! daemon, plus a small JSON bridge (`POST /api/call`) that routes straight
//! into [`crate::daemon::dispatch`] -- the same router the CLI IPC uses.
//!
//! Loopback-only by default and unauthenticated; cross-origin requests are
//! rejected via a required custom header (browsers can't send it cross-site
//! without a CORS preflight, which this server never grants).

pub mod autoreopen;
pub mod browser;
pub mod server;
pub mod ui_state;

pub use server::serve;

/// The dashboard page, embedded at compile time so the release exe stays a
/// single file.
pub(crate) const INDEX_HTML: &str = include_str!("assets/index.html");

/// The dashboard favicon, embedded for the same single-file release flow.
pub(crate) const FAVICON_PNG: &[u8] = include_bytes!("assets/favicon.png");
