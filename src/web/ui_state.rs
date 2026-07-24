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

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
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

fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join("ui-state.json")
}

/// Reads the persisted flag. A missing file is the expected "dashboard has
/// never been open on this machine" state and reads as
/// `dashboard_open: false`, not an error (mirrors
/// `ai::provide_state::read_state`'s missing-file handling). A *present but
/// corrupt* file is surfaced as an `Err` instead of silently defaulting --
/// unlike a missing file, that indicates something wrote a bad file, which
/// is worth a caller-visible `warn!` rather than quietly skipping
/// auto-reopen with no trace of why.
pub fn read_state(data_dir: &Path) -> Result<UiState> {
    let path = state_path(data_dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(UiState::default());
        }
        Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(UiState::default());
    }
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Atomically writes the flag (`<path>.tmp` -> rename), the same idiom as
/// `ai::provide_state::write_state`.
pub fn write_state(data_dir: &Path, state: UiState) -> Result<()> {
    let path = state_path(data_dir);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let text = serde_json::to_string_pretty(&state).context("serializing ui state")?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &text).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir(name: &str) -> PathBuf {
        let suffix: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("mistl-ui-state-test-{name}-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn read_state_missing_file_defaults_to_closed() {
        let dir = scratch_dir("missing");
        let state = read_state(&dir).unwrap();
        assert_eq!(state, UiState::default());
        assert!(!state.dashboard_open);
    }

    #[test]
    fn write_then_read_round_trips_dashboard_open_true() {
        let dir = scratch_dir("roundtrip");
        write_state(&dir, UiState { dashboard_open: true }).unwrap();
        let state = read_state(&dir).unwrap();
        assert!(state.dashboard_open);
    }

    #[test]
    fn writing_false_clears_a_previously_open_flag() {
        let dir = scratch_dir("clear");
        write_state(&dir, UiState { dashboard_open: true }).unwrap();
        write_state(&dir, UiState { dashboard_open: false }).unwrap();
        let state = read_state(&dir).unwrap();
        assert!(!state.dashboard_open);
    }

    #[test]
    fn read_state_surfaces_a_corrupt_file_as_an_error_instead_of_silently_defaulting() {
        let dir = scratch_dir("corrupt");
        std::fs::write(state_path(&dir), b"not json").unwrap();
        let err = read_state(&dir).unwrap_err();
        assert!(err.to_string().contains("parsing"));
    }

    #[test]
    fn read_state_treats_an_empty_file_as_closed() {
        let dir = scratch_dir("empty");
        std::fs::write(state_path(&dir), b"").unwrap();
        let state = read_state(&dir).unwrap();
        assert!(!state.dashboard_open);
    }

    #[test]
    fn write_state_creates_missing_parent_directories() {
        let dir = scratch_dir("nested").join("nested").join("dirs");
        assert!(!dir.exists());
        write_state(&dir, UiState { dashboard_open: true }).unwrap();
        assert!(read_state(&dir).unwrap().dashboard_open);
    }
}
