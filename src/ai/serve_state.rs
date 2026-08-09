//! Persistence for whether the local AI API server was left running.
//!
//! The listener itself is runtime state, so its last requested state is kept
//! outside `config.toml` in `<data_dir>/ai-serve-state.json`.  A successful
//! `ai serve start` stores `enabled: true`, an explicit stop stores `false`,
//! and daemon startup uses the flag to restore the listener.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServeState {
    #[serde(default)]
    pub enabled: bool,
}

fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join("ai-serve-state.json")
}

pub fn read_state(data_dir: &Path) -> Result<ServeState> {
    let path = state_path(data_dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ServeState::default());
        }
        Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(ServeState::default());
    }
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

pub fn write_state(data_dir: &Path, state: ServeState) -> Result<()> {
    let path = state_path(data_dir);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let text = serde_json::to_string_pretty(&state).context("serializing ai serve state")?;
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
        let dir =
            std::env::temp_dir().join(format!("mistl-ai-serve-state-test-{name}-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn missing_file_defaults_to_disabled() {
        assert_eq!(
            read_state(&scratch_dir("missing")).unwrap(),
            ServeState::default()
        );
    }

    #[test]
    fn enabled_state_round_trips_and_can_be_cleared() {
        let dir = scratch_dir("roundtrip");
        write_state(&dir, ServeState { enabled: true }).unwrap();
        assert!(read_state(&dir).unwrap().enabled);
        write_state(&dir, ServeState { enabled: false }).unwrap();
        assert!(!read_state(&dir).unwrap().enabled);
    }

    #[test]
    fn corrupt_file_is_reported() {
        let dir = scratch_dir("corrupt");
        std::fs::write(state_path(&dir), b"not json").unwrap();
        assert!(
            read_state(&dir)
                .unwrap_err()
                .to_string()
                .contains("parsing")
        );
    }
}
