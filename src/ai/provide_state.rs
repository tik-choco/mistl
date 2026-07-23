//! Persistence for "was `ai provide` left running" across daemon restarts.
//!
//! `ai provide start`/`stop` (and the dashboard's providing toggle, which
//! drives the same `ai.provide.start`/`ai.provide.stop` IPC commands via
//! `ai::handle` -- see `src/web/assets/index.html`'s `btn-ai-provide-toggle`
//! handler) only ever flip an in-memory `AiService::provider` slot:
//! restarting the daemon (a rebuild, a self-update, a crash) always drops
//! back to "not providing" with zero signal to the operator. That's exactly
//! the confusion a real-device test hit: rebuild -> restart -> providing
//! silently off -> every peer reports "no provider found" with nothing in
//! the way pointing at why. This module persists just the boolean *intent*
//! ("should provide be running") to `<data_dir>/ai-provide-state.json`.
//!
//! Deliberately **not** `config.toml`: that file is the user-edited
//! settings surface (see `crate::config`), round-tripped verbatim on every
//! `config.set`, and diffed/read by humans. "Was providing running last
//! time" is daemon-written runtime state, not a setting -- so this instead
//! mirrors the existing `<data_dir>/*.json` "table file" idiom already used
//! by `storage::folder_owner`'s `shared-folders.json` and `scheduler`'s
//! `scheduler-jobs.json`: plain JSON, atomic tmp-then-rename writes, a
//! missing file reads as the empty/default state rather than an error.
//!
//! `crate::ai::spawn_provide_autoresume` reads this at daemon startup and,
//! if `enabled`, attempts to start providing again -- see its doc comment
//! for the "never fail daemon startup" contract that governs what happens
//! when the persisted config no longer resolves (dangling preset, etc.).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// On-disk shape of `<data_dir>/ai-provide-state.json`. A single flag today;
/// kept as a struct (rather than a bare bool file) the same way
/// `storage::folder_owner`'s and `scheduler`'s table files are structs, so
/// it can grow later (e.g. a "why auto-resume last failed" field) without a
/// breaking format migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvideState {
    /// `true` from a successful `ai provide start` (manual, dashboard
    /// toggle, or a prior auto-resume) until the next `ai provide stop`.
    #[serde(default)]
    pub enabled: bool,
}

fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join("ai-provide-state.json")
}

/// Reads the persisted flag. A missing file is the expected "`ai provide
/// start` has never run on this machine" state and reads as
/// `enabled: false`, not an error (mirrors
/// `storage::folder_owner::read_table_file`'s missing-file handling). A
/// *present but corrupt* file is surfaced as an `Err` instead of silently
/// defaulting -- unlike a missing file, that indicates something wrote a
/// bad file, which is worth a caller-visible `warn!` (see
/// `crate::ai::spawn_provide_autoresume`) rather than quietly skipping
/// auto-resume with no trace of why.
pub fn read_state(data_dir: &Path) -> Result<ProvideState> {
    let path = state_path(data_dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ProvideState::default());
        }
        Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(ProvideState::default());
    }
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Atomically writes the flag (`<path>.tmp` -> rename), the same idiom as
/// `storage::folder_owner::write_table_file` /
/// `scheduler::save_jobs_unlocked`.
pub fn write_state(data_dir: &Path, state: ProvideState) -> Result<()> {
    let path = state_path(data_dir);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let text = serde_json::to_string_pretty(&state).context("serializing ai provide state")?;
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
        let dir = std::env::temp_dir().join(format!("mistl-ai-provide-state-test-{name}-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn read_state_missing_file_defaults_to_disabled() {
        let dir = scratch_dir("missing");
        let state = read_state(&dir).unwrap();
        assert_eq!(state, ProvideState::default());
        assert!(!state.enabled);
    }

    #[test]
    fn write_then_read_round_trips_enabled_true() {
        let dir = scratch_dir("roundtrip");
        write_state(&dir, ProvideState { enabled: true }).unwrap();
        let state = read_state(&dir).unwrap();
        assert!(state.enabled);
    }

    #[test]
    fn writing_false_clears_a_previously_enabled_flag() {
        let dir = scratch_dir("clear");
        write_state(&dir, ProvideState { enabled: true }).unwrap();
        write_state(&dir, ProvideState { enabled: false }).unwrap();
        let state = read_state(&dir).unwrap();
        assert!(!state.enabled);
    }

    #[test]
    fn read_state_surfaces_a_corrupt_file_as_an_error_instead_of_silently_defaulting() {
        let dir = scratch_dir("corrupt");
        std::fs::write(state_path(&dir), b"not json").unwrap();
        let err = read_state(&dir).unwrap_err();
        assert!(err.to_string().contains("parsing"));
    }

    #[test]
    fn read_state_treats_an_empty_file_as_disabled() {
        let dir = scratch_dir("empty");
        std::fs::write(state_path(&dir), b"").unwrap();
        let state = read_state(&dir).unwrap();
        assert!(!state.enabled);
    }

    #[test]
    fn write_state_creates_missing_parent_directories() {
        let dir = scratch_dir("nested").join("nested").join("dirs");
        assert!(!dir.exists());
        write_state(&dir, ProvideState { enabled: true }).unwrap();
        assert!(read_state(&dir).unwrap().enabled);
    }
}
