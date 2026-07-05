//! JSON-file spool for the mailbox module: `<data_dir>/mailbox/outbox.json`,
//! `inbox.json`, and `held.json`, each a JSON array of [`SpoolEntry`].
//!
//! Writes are atomic-ish: serialize to a temp file in the same directory,
//! then rename over the target (so a crash mid-write can never leave a
//! truncated/corrupt spool file). Callers (in `service.rs`) additionally
//! serialize concurrent access with an async mutex; this module itself does
//! no locking.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::envelope::Envelope;

/// One spooled envelope plus bookkeeping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpoolEntry {
    pub envelope: Envelope,
    /// RFC 3339 timestamp of when this entry was spooled (queued, held, or
    /// received) here.
    pub received_at: String,
    /// Resolved mistlib node id of the envelope's recipient. Populated for
    /// `held.json` entries so the bot forward loop can match against
    /// `get_connected_nodes()` without recomputing `sha256(did)` every poll;
    /// left `None` elsewhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_node: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpoolKind {
    /// Envelopes we tried to send but couldn't deliver or deposit anywhere;
    /// retried opportunistically on later `send`/`fetch` calls.
    Outbox,
    /// Envelopes addressed to us, waiting to be drained by `mailbox.fetch`.
    Inbox,
    /// Deposits we (as a bot) are holding for other, currently-offline
    /// peers.
    Held,
}

impl SpoolKind {
    fn file_name(self) -> &'static str {
        match self {
            SpoolKind::Outbox => "outbox.json",
            SpoolKind::Inbox => "inbox.json",
            SpoolKind::Held => "held.json",
        }
    }

    fn path(self, data_dir: &Path) -> PathBuf {
        data_dir.join(self.file_name())
    }
}

fn read_entries(path: &Path) -> Result<Vec<SpoolEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn write_entries(path: &Path, entries: &[SpoolEntry]) -> Result<()> {
    let dir = path
        .parent()
        .context("spool path has no parent directory")?;
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating spool directory {}", dir.display()))?;

    // Atomic-ish write: temp file in the same directory (so the rename is
    // same-filesystem) then rename over the target.
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(entries)?;
    std::fs::write(&tmp, &body).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))?;
    Ok(())
}

/// List all entries currently in `kind`'s spool file (missing file = empty).
pub fn list(data_dir: &Path, kind: SpoolKind) -> Result<Vec<SpoolEntry>> {
    read_entries(&kind.path(data_dir))
}

/// Overwrite `kind`'s spool file wholesale.
pub fn write_all(data_dir: &Path, kind: SpoolKind, entries: &[SpoolEntry]) -> Result<()> {
    write_entries(&kind.path(data_dir), entries)
}

/// Append one entry to `kind`'s spool file.
pub fn append(data_dir: &Path, kind: SpoolKind, entry: SpoolEntry) -> Result<()> {
    let path = kind.path(data_dir);
    let mut entries = read_entries(&path)?;
    entries.push(entry);
    write_entries(&path, &entries)
}

/// Remove the entry with envelope id `id` from `kind`'s spool file, if
/// present.
pub fn remove(data_dir: &Path, kind: SpoolKind, id: &str) -> Result<Option<SpoolEntry>> {
    let path = kind.path(data_dir);
    let mut entries = read_entries(&path)?;
    let index = entries.iter().position(|e| e.envelope.id == id);
    let removed = index.map(|i| entries.remove(i));
    if removed.is_some() {
        write_entries(&path, &entries)?;
    }
    Ok(removed)
}

/// Read and clear `kind`'s spool file in one step, returning whatever was in
/// it.
pub fn drain(data_dir: &Path, kind: SpoolKind) -> Result<Vec<SpoolEntry>> {
    let path = kind.path(data_dir);
    let entries = read_entries(&path)?;
    if !entries.is_empty() {
        write_entries(&path, &[])?;
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mailbox::envelope::{Envelope, EnvelopeKind};

    /// A fresh, process-unique scratch directory for one test; removed
    /// before use (in case of a leftover from a prior failed run) and left
    /// in place afterwards for post-mortem inspection is unnecessary here,
    /// so we clean up at the end of each test too.
    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mistl-mailbox-spool-test-{name}-{}-{}",
            std::process::id(),
            random_suffix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn random_suffix() -> String {
        use rand::RngCore;
        let mut bytes = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut bytes);
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn sample_entry() -> SpoolEntry {
        let envelope = Envelope::new(
            "did:key:zFrom".into(),
            "did:key:zTo".into(),
            EnvelopeKind::Message,
        );
        SpoolEntry {
            envelope,
            received_at: "2026-01-01T00:00:00+00:00".into(),
            to_node: None,
        }
    }

    #[test]
    fn missing_file_reads_as_empty() {
        let dir = scratch_dir("missing");
        assert!(list(&dir, SpoolKind::Held).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_list_remove_round_trip() {
        let dir = scratch_dir("append-list-remove");
        let entry = sample_entry();
        let id = entry.envelope.id.clone();

        append(&dir, SpoolKind::Inbox, entry).unwrap();
        let listed = list(&dir, SpoolKind::Inbox).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].envelope.id, id);
        assert_eq!(listed[0].received_at, "2026-01-01T00:00:00+00:00");

        let removed = remove(&dir, SpoolKind::Inbox, &id).unwrap();
        assert!(removed.is_some());
        assert!(list(&dir, SpoolKind::Inbox).unwrap().is_empty());

        // Removing again is a harmless no-op.
        assert!(remove(&dir, SpoolKind::Inbox, &id).unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn drain_returns_prior_entries_and_empties_the_file() {
        let dir = scratch_dir("drain");
        append(&dir, SpoolKind::Outbox, sample_entry()).unwrap();
        append(&dir, SpoolKind::Outbox, sample_entry()).unwrap();

        let drained = drain(&dir, SpoolKind::Outbox).unwrap();
        assert_eq!(drained.len(), 2);
        assert!(list(&dir, SpoolKind::Outbox).unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn spool_kinds_use_distinct_files() {
        let dir = scratch_dir("distinct-files");
        append(&dir, SpoolKind::Outbox, sample_entry()).unwrap();
        append(&dir, SpoolKind::Held, sample_entry()).unwrap();

        assert_eq!(list(&dir, SpoolKind::Outbox).unwrap().len(), 1);
        assert_eq!(list(&dir, SpoolKind::Held).unwrap().len(), 1);
        assert!(list(&dir, SpoolKind::Inbox).unwrap().is_empty());
        assert!(dir.join("outbox.json").exists());
        assert!(dir.join("held.json").exists());
        assert!(!dir.join("inbox.json").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
