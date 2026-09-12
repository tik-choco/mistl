//! Shared read/write for the daemon's small `<data_dir>/*.json` runtime
//! state files.
//!
//! Several services persist a scrap of *runtime* state -- "was `ai provide`
//! running", "was the local API server running", "was the dashboard open"
//! -- so a restart (a rebuild, a self-update, a crash) can resume instead of
//! silently dropping back to off with no signal to the operator.
//!
//! Deliberately **not** `config.toml`: that file is the user-edited settings
//! surface (see [`crate::config`]), round-tripped verbatim on every
//! `config.set` and diffed/read by humans. "What was running last time" is
//! daemon-written runtime state, not a setting -- so this mirrors the
//! existing `<data_dir>/*.json` "table file" idiom already used by
//! `storage::folder_owner`'s `shared-folders.json` and `scheduler`'s
//! `scheduler-jobs.json`.
//!
//! The contract, shared by every caller:
//!
//! - A **missing** file is the expected "this has never run on this machine"
//!   state and reads as `T::default()`, not an error.
//! - An **empty** file reads as `T::default()` too (a torn write, or a file
//!   touched by hand).
//! - A **present but corrupt** file is surfaced as an `Err` instead of
//!   silently defaulting -- unlike a missing file, that means something
//!   wrote a bad file, which is worth a caller-visible `warn!` rather than
//!   quietly skipping resume with no trace of why.
//! - Writes are atomic (`<path>.tmp` then rename), the same idiom as
//!   `storage::folder_owner::write_table_file` /
//!   `scheduler::save_jobs_unlocked`.
//!
//! Callers keep their own one-field state struct and their own filename;
//! only this read/write pair is shared. Each caller's own tests cover that
//! it reads and writes *its* file; the contract above is tested once here.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Reads `<data_dir>/<file_name>`, falling back to `T::default()` for a
/// missing or empty file and erroring on a corrupt one. See the module doc
/// for why those three cases differ.
pub fn read<T: DeserializeOwned + Default>(data_dir: &Path, file_name: &str) -> Result<T> {
    let path = data_dir.join(file_name);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(T::default()),
        Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(T::default());
    }
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Atomically writes `state` to `<data_dir>/<file_name>`, creating the
/// directory if it does not exist yet.
pub fn write<T: Serialize>(data_dir: &Path, file_name: &str, state: &T) -> Result<()> {
    let path = data_dir.join(file_name);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let text =
        serde_json::to_string_pretty(state).with_context(|| format!("serializing {file_name}"))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &text).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::path::PathBuf;

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
    struct Flag {
        #[serde(default)]
        enabled: bool,
    }

    const FILE: &str = "statefile-test.json";

    fn scratch_dir(name: &str) -> PathBuf {
        let suffix: u64 = rand::random();
        let dir = std::env::temp_dir().join(format!("mistl-statefile-test-{name}-{suffix:016x}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn missing_and_empty_files_read_as_the_default() {
        let dir = scratch_dir("default");
        assert_eq!(read::<Flag>(&dir, FILE).unwrap(), Flag::default());

        std::fs::write(dir.join(FILE), b"").unwrap();
        assert_eq!(read::<Flag>(&dir, FILE).unwrap(), Flag::default());
    }

    #[test]
    fn write_then_read_round_trips_and_a_later_write_clears_the_flag() {
        let dir = scratch_dir("roundtrip");
        write(&dir, FILE, &Flag { enabled: true }).unwrap();
        assert!(read::<Flag>(&dir, FILE).unwrap().enabled);

        write(&dir, FILE, &Flag { enabled: false }).unwrap();
        assert!(!read::<Flag>(&dir, FILE).unwrap().enabled);
    }

    #[test]
    fn a_corrupt_file_is_an_error_instead_of_silently_defaulting() {
        let dir = scratch_dir("corrupt");
        std::fs::write(dir.join(FILE), b"not json").unwrap();
        let err = read::<Flag>(&dir, FILE).unwrap_err();
        assert!(err.to_string().contains("parsing"), "{err}");
    }

    #[test]
    fn write_creates_missing_parent_directories() {
        let dir = scratch_dir("nested").join("nested").join("dirs");
        assert!(!dir.exists());
        write(&dir, FILE, &Flag { enabled: true }).unwrap();
        assert!(read::<Flag>(&dir, FILE).unwrap().enabled);
    }
}
