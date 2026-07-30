//! Local content index: a flat JSON array at `<data_dir>/store-index.json`
//! recording every root CID this daemon has put/fetched, so `store.ls` can
//! list them and `store.get` can recover the original file name.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One row of the local index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub cid: String,
    pub name: String,
    pub size: u64,
    pub stored_at: DateTime<Utc>,
}

pub(super) fn index_path(data_dir: &Path) -> PathBuf {
    data_dir.join("store-index.json")
}

/// Load the index, treating a missing or empty file as an empty index.
pub(super) fn load(data_dir: &Path) -> Result<Vec<IndexEntry>> {
    let path = index_path(data_dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn save(data_dir: &Path, entries: &[IndexEntry]) -> Result<()> {
    let path = index_path(data_dir);
    let text = serde_json::to_string_pretty(entries)?;
    std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))
}

/// Insert `entry`, or replace the existing row with the same `cid` (so
/// re-storing identical bytes under a new name updates in place instead of
/// growing the index).
pub(super) fn upsert(data_dir: &Path, entry: IndexEntry) -> Result<()> {
    let mut entries = load(data_dir)?;
    match entries
        .iter_mut()
        .find(|existing| existing.cid == entry.cid)
    {
        Some(existing) => *existing = entry,
        None => entries.push(entry),
    }
    save(data_dir, &entries)
}
