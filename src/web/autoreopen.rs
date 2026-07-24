//! Reopen the dashboard in a browser after a daemon restart when the
//! previous run shut down with the dashboard open.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::daemon::AppState;
use crate::web::ui_state;

/// Fire-and-forget startup task: if `ui-state.json` says the dashboard was
/// open when the previous run shut down, wait a grace period for an existing
/// tab to reconnect (release builds poll every 5s, debug live-reload every
/// 1s); only if nothing polls, open a fresh browser tab. Never fails daemon
/// startup: any problem here (data dir unreadable, corrupt state file, no
/// browser opener available) is logged via `warn!`/`debug!` and simply skips
/// the reopen, exactly as if the user had left the dashboard closed.
pub fn spawn_dashboard_autoreopen(state: Arc<AppState>, listen: String) {
    tokio::spawn(async move {
        let data_dir = match crate::config::data_dir() {
            Ok(dir) => dir,
            Err(err) => {
                warn!(%err, "web: dashboard auto-reopen skipped -- could not resolve the data directory");
                return;
            }
        };
        let persisted = match ui_state::read_state(&data_dir) {
            Ok(state) => state,
            Err(err) => {
                warn!(%err, "web: dashboard auto-reopen skipped -- could not read the persisted UI state");
                return;
            }
        };
        if !persisted.dashboard_open {
            debug!("web: dashboard was not open on the previous run; not auto-reopening");
            return;
        }

        // Grace period: give an already-open tab a chance to reconnect (and
        // thus register activity) before we decide to pop a fresh one. A
        // release dashboard polls `/api/call` every 5s and a debug
        // live-reload poll hits `/api/dev/instance` every 1s, so 8s is ample
        // for an existing tab to be counted.
        tokio::time::sleep(Duration::from_secs(8)).await;

        if state.dashboard_seen_within(Duration::from_secs(8)) {
            debug!("web: an existing dashboard tab reconnected; not opening a duplicate");
            return;
        }

        let url = dashboard_url(&listen);
        if crate::web::browser::open_in_browser(&url) {
            info!(%url, "web: reopened the dashboard after restart");
        } else {
            warn!(%url, "web: failed to reopen the dashboard automatically; open the URL by hand");
        }
    });
}

/// Builds the dashboard URL to open from a resolved `ui.listen` string
/// (`"host:port"`), replacing an unspecified bind host (`0.0.0.0` or `[::]`)
/// with `127.0.0.1` -- the browser we're opening runs on this same machine,
/// so it should always reach the dashboard over loopback even when the
/// server itself is bound to listen on every interface (e.g. `--host
/// 0.0.0.0` for LAN access).
fn dashboard_url(listen: &str) -> String {
    let (host, port) = listen.rsplit_once(':').unwrap_or((listen, "6480"));
    let host = match host {
        "0.0.0.0" | "[::]" | "::" => "127.0.0.1",
        other => other,
    };
    format!("http://{host}:{port}/")
}

#[cfg(test)]
mod tests {
    use super::*;

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
