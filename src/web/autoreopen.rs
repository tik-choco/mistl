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

        let url = crate::web::dashboard_url(&listen);
        if crate::web::browser::open_in_browser(&url) {
            info!(%url, "web: reopened the dashboard after restart");
        } else {
            warn!(%url, "web: failed to reopen the dashboard automatically; open the URL by hand");
        }
    });
}
