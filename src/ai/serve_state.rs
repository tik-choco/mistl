//! Persistence for whether the local AI API server was left running.
//!
//! The listener itself is runtime state, so its last requested state is kept
//! outside `config.toml` in `<data_dir>/ai-serve-state.json`.  A successful
//! `ai serve start` stores `enabled: true`, an explicit stop stores `false`,
//! and daemon startup uses the flag to restore the listener.

use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServeState {
    #[serde(default)]
    pub enabled: bool,
}

/// `<data_dir>` file this module's state lives in.
const FILE_NAME: &str = "ai-serve-state.json";

/// Only the round-trip test below needs the resolved path; reads and
/// writes go through [`crate::statefile`] by file name.
#[cfg(test)]
fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE_NAME)
}

/// Reads the persisted flag from `ai-serve-state.json`. The missing/empty/corrupt
/// handling is [`crate::statefile`]'s shared contract: a missing file means
/// `ai serve start` has never run on this machine and reads as the
/// default, while a *present but corrupt* file is surfaced as an `Err` so
/// the caller can `warn!` rather than silently skip.
pub fn read_state(data_dir: &Path) -> Result<ServeState> {
    crate::statefile::read(data_dir, FILE_NAME)
}

/// Atomically writes the flag to `ai-serve-state.json`.
pub fn write_state(data_dir: &Path, state: ServeState) -> Result<()> {
    crate::statefile::write(data_dir, FILE_NAME, &state)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The missing/empty/corrupt-file contract and the atomic write are
    /// [`crate::statefile`]'s and are tested once there. What is specific to
    /// this module -- and what the daemon's API-server restore path depends on -- is
    /// that it round-trips through *its own* `ai-serve-state.json`.
    #[test]
    fn round_trips_the_flag_through_its_own_state_file() {
        let suffix: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("mistl-ai-serve-state-test-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        write_state(&dir, ServeState { enabled: true }).unwrap();
        assert!(
            state_path(&dir).exists(),
            "expected ai-serve-state.json to be written"
        );
        assert!(read_state(&dir).unwrap().enabled);

        write_state(&dir, ServeState { enabled: false }).unwrap();
        assert!(!read_state(&dir).unwrap().enabled);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
