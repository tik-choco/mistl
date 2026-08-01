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

/// Builds the dashboard URL to show (or open) from a resolved `ui.listen`
/// string (`"host:port"`), replacing an unspecified bind host (`0.0.0.0` or
/// `[::]`) with `127.0.0.1` -- a browser on this machine should always reach
/// the dashboard over loopback, even when the server itself is bound to every
/// interface (e.g. `--host 0.0.0.0` for LAN access), and `http://0.0.0.0:.../`
/// is not a URL a browser can usefully open.
pub fn dashboard_url(listen: &str) -> String {
    let (host, port) = listen.rsplit_once(':').unwrap_or((listen, "6480"));
    let host = match host {
        "0.0.0.0" | "[::]" | "::" => "127.0.0.1",
        other => other,
    };
    format!("http://{host}:{port}/")
}

/// The dashboard page, embedded at compile time so the release exe stays a
/// single file.
pub(crate) const INDEX_HTML: &str = include_str!("assets/index.html");

/// The dashboard favicon, embedded for the same single-file release flow.
pub(crate) const FAVICON_PNG: &[u8] = include_bytes!("assets/favicon.png");

#[cfg(test)]
mod tests {
    use super::dashboard_url;

    #[test]
    fn dashboard_url_keeps_explicit_host() {
        assert_eq!(dashboard_url("127.0.0.1:6480"), "http://127.0.0.1:6480/");
    }

    #[test]
    fn dashboard_url_replaces_unspecified_ipv4_host() {
        assert_eq!(dashboard_url("0.0.0.0:6480"), "http://127.0.0.1:6480/");
    }

    #[test]
    fn dashboard_url_replaces_unspecified_ipv6_host() {
        assert_eq!(dashboard_url("[::]:6480"), "http://127.0.0.1:6480/");
    }
}
