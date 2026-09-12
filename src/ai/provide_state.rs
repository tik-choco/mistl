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

use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use anyhow::Result;
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

/// `<data_dir>` file this module's state lives in.
const FILE_NAME: &str = "ai-provide-state.json";

/// Only the round-trip test below needs the resolved path; reads and
/// writes go through [`crate::statefile`] by file name.
#[cfg(test)]
fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE_NAME)
}

/// Reads the persisted flag from `ai-provide-state.json`. The missing/empty/corrupt
/// handling is [`crate::statefile`]'s shared contract: a missing file means
/// `ai provide start` has never run on this machine and reads as the
/// default, while a *present but corrupt* file is surfaced as an `Err` so
/// the caller can `warn!` rather than silently skip.
pub fn read_state(data_dir: &Path) -> Result<ProvideState> {
    crate::statefile::read(data_dir, FILE_NAME)
}

/// Atomically writes the flag to `ai-provide-state.json`.
pub fn write_state(data_dir: &Path, state: ProvideState) -> Result<()> {
    crate::statefile::write(data_dir, FILE_NAME, &state)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The missing/empty/corrupt-file contract and the atomic write are
    /// [`crate::statefile`]'s and are tested once there. What is specific to
    /// this module -- and what `ai::spawn_provide_autoresume` depends on -- is
    /// that it round-trips through *its own* `ai-provide-state.json`.
    #[test]
    fn round_trips_the_flag_through_its_own_state_file() {
        let suffix: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("mistl-ai-provide-state-test-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        write_state(&dir, ProvideState { enabled: true }).unwrap();
        assert!(
            state_path(&dir).exists(),
            "expected ai-provide-state.json to be written"
        );
        assert!(read_state(&dir).unwrap().enabled);

        write_state(&dir, ProvideState { enabled: false }).unwrap();
        assert!(!read_state(&dir).unwrap().enabled);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
