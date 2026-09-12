//! Persisted dashboard-open state (`ui-state.json` in the data dir), so a
//! daemon restart can reopen the web UI if it was open when the previous
//! run shut down. Mirrors [`crate::ai::provide_state`]: same `<data_dir>/*
//! .json` "table file" idiom (plain JSON, atomic tmp-then-rename writes, a
//! missing file reads as the default state rather than an error) already
//! used by `storage::folder_owner`'s `shared-folders.json`, `scheduler`'s
//! `scheduler-jobs.json`, and `ai::provide_state`'s
//! `ai-provide-state.json`.
//!
//! `crate::web::autoreopen` reads this at daemon startup and, if
//! `dashboard_open`, reopens the dashboard in the default browser.

use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// On-disk shape of `<data_dir>/ui-state.json`. A single flag today; kept
/// as a struct (rather than a bare bool file) the same way
/// `ai::provide_state::ProvideState` is, so it can grow later without a
/// breaking format migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiState {
    /// `true` while the dashboard is open (actively polling) and until the
    /// tab is closed or the daemon shuts down while it was open.
    #[serde(default)]
    pub dashboard_open: bool,
}

/// `<data_dir>` file this module's state lives in.
const FILE_NAME: &str = "ui-state.json";

/// Only the round-trip test below needs the resolved path; reads and
/// writes go through [`crate::statefile`] by file name.
#[cfg(test)]
fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE_NAME)
}

/// Reads the persisted flag from `ui-state.json`. The missing/empty/corrupt
/// handling is [`crate::statefile`]'s shared contract: a missing file means
/// the dashboard has never been open on this machine and reads as the
/// default, while a *present but corrupt* file is surfaced as an `Err` so
/// the caller can `warn!` rather than silently skip.
pub fn read_state(data_dir: &Path) -> Result<UiState> {
    crate::statefile::read(data_dir, FILE_NAME)
}

/// Atomically writes the flag to `ui-state.json`.
pub fn write_state(data_dir: &Path, state: UiState) -> Result<()> {
    crate::statefile::write(data_dir, FILE_NAME, &state)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The missing/empty/corrupt-file contract and the atomic write are
    /// [`crate::statefile`]'s and are tested once there. What is specific to
    /// this module -- and what `web::autoreopen` depends on -- is
    /// that it round-trips through *its own* `ui-state.json`.
    #[test]
    fn round_trips_the_flag_through_its_own_state_file() {
        let suffix: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("mistl-ui-state-test-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        write_state(
            &dir,
            UiState {
                dashboard_open: true,
            },
        )
        .unwrap();
        assert!(
            state_path(&dir).exists(),
            "expected ui-state.json to be written"
        );
        assert!(read_state(&dir).unwrap().dashboard_open);

        write_state(
            &dir,
            UiState {
                dashboard_open: false,
            },
        )
        .unwrap();
        assert!(!read_state(&dir).unwrap().dashboard_open);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
