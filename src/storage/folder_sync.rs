//! Continuous requester-side folder sync: paste a tc-storage `folder-share`
//! link once (`store folder-sync <url>`) and keep it mirrored from then on.
//! By default the synced files live inside the daemon's path-jailed content
//! sandbox (`<data_dir>/sandbox/<name>`, see [`super::sandbox`]) rather than
//! anywhere on the user's own filesystem -- the user "extracts" individual
//! files out to a real destination on demand via `store.sandbox.export` (see
//! `storage::mod`'s doc and `config::StorageConfig::export_dir`). Passing an
//! explicit `--dir <path>` (CLI power-user escape hatch) keeps the old
//! behavior of a real, user-chosen directory outside the sandbox.
//!
//! Protocol (mirrors the tc-storage web app, see `folder_share`'s module doc
//! for the shared wire/crypto primitives):
//! - Initial sync = the same access-grant handshake + full-bundle fetch as
//!   `store folder-get`, but materialized into a user-designated directory
//!   instead of the content sandbox, and recorded in a persisted sync table.
//! - Incremental sync = a daemon-lifetime background task
//!   ([`spawn_background`]) subscribed to [`folder_share::envelope_bus`]:
//!   - `folder-change` (`file-upserted`/`file-deleted`/`folder-upserted`/
//!     `folder-deleted`) applies the single change directly (fetch just that
//!     file's CID / delete just that path).
//!   - `folder-state` / `folder-share` with a `cid`+`folderSignature`
//!     differing from the persisted ones triggers a full re-fetch + diff
//!     (write new/changed files, delete files gone from the bundle). Owners
//!     re-announce every ~60s and on peer connect, so this doubles as
//!     catch-up after downtime.
//! - Sync state (including the granted folder key) is persisted under the
//!   store's data dir so syncs survive daemon restarts without repeating the
//!   grant handshake. The materialized directory is plaintext on the same
//!   disk, so persisting the key grants no additional exposure.
//! - Each sync joins its share link's own room (multi-room: `net`'s room
//!   registry is refcounted and additive), held for the sync's lifetime.
//!
//! ## Sync destination
//!
//! By default (`dir` omitted -- what the web UI always sends) a sync's
//! `local_dir` is `<data_dir>/sandbox/<name>`, where `<name>` is the shared
//! folder's display name run through [`folder_share::sanitize_name`] (the
//! same sanitizer `file_target_rel` uses for path components), disambiguated
//! against every *other* already-registered sync's directory by appending
//! `-<first 8 chars of folder_id>` on a name collision. [`SyncEntry`]
//! exposes this as `sandbox_dir` (sandbox-relative, forward-slash) --
//! recomputed from `local_dir` on every read rather than trusted verbatim
//! from disk (see [`sandbox_relative_dir`]), so it always reflects
//! `local_dir`'s current relationship to the sandbox root. It's empty for a
//! legacy entry, or a `--dir` override, whose `local_dir` isn't under the
//! sandbox root at all.
//!
//! ## Local directory layout
//!
//! Unlike `folder_share::fetch_folder_share` (which writes into the shared
//! content sandbox and so nests every download under a folder-name-derived
//! top-level directory, since many unrelated shares live side by side
//! there), a sync's `local_dir` -- sandboxed or not -- is dedicated to
//! exactly one shared folder. Materialized paths therefore drop the root
//! folder's own name segment from [`folder_share::folder_path_parts`]'s
//! output -- a file directly in the shared root lands at `<local_dir>/name`,
//! not `<local_dir>/<folder name>/name`. See [`file_target_rel`].
//!
//! ## IPC secrecy note
//!
//! [`SyncEntry::folder_key`] is a secret credential persisted on disk so the
//! grant handshake never has to be repeated across daemon restarts (see
//! above). It must never be echoed back over the daemon IPC/CLI surface,
//! but `SyncEntry` is also the exact type serialized to (and read back from)
//! the persistence file, so the field can't simply be
//! `#[serde(skip_serializing)]` -- that would mean it never survives a
//! restart either. Instead every entry handed back to a caller
//! ([`register_sync`], [`list_syncs`]) is redacted right before it leaves
//! this module; see [`redact`].
//!
//! ## Registration vs. initial sync
//!
//! [`register_sync`] returns as soon as the link is parsed, validated, and a
//! `"connecting"` [`SyncEntry`] is persisted -- it never blocks on the
//! network. The access-grant handshake and first full fetch (which can take
//! anywhere from seconds to minutes, gated on the owner being online and
//! approving) run in a spawned background task that updates the same
//! persisted entry in place: `"synced"` on success, `"error"` (with
//! `last_error`) on failure. [`spawn_background`]'s startup scan and 30s
//! retry tick pick up any entry left in `"connecting"`/`"error"` (including
//! across daemon restarts) and retry it the same way, deduplicated per
//! `folder_id` by [`INFLIGHT_INITIAL_SYNCS`] so a slow owner never causes
//! two concurrent handshakes for the same folder.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tracing::{debug, warn};

use super::domain::{FileRecord, FolderRecord};
use super::folder_share::{self, ShareEnvelope};
use super::sandbox::Sandbox;
use super::sharelink::ShareProfile;
use super::{Store, sha256_hex};
use crate::daemon::AppState;

/// [`SyncEntry::status`] value while the initial access-grant handshake and
/// first full fetch haven't completed yet (freshly [`register_sync`]ed, or
/// mid-retry after a restart).
pub const STATUS_CONNECTING: &str = "connecting";
/// [`SyncEntry::status`] value once the initial fetch has completed and
/// [`SyncEntry::folder_key`] is populated; incremental sync keeps this entry
/// up to date via the envelope bus.
pub const STATUS_SYNCED: &str = "synced";
/// [`SyncEntry::status`] value after the initial handshake or a later
/// full-resync/incremental apply failed; see [`SyncEntry::last_error`].
/// Retried automatically (see the module doc).
pub const STATUS_ERROR: &str = "error";

/// Entries persisted by code predating [`SyncEntry::status`] have no such
/// field in their JSON at all; since that code never returned until the
/// initial sync had fully completed, every entry it wrote was, by
/// construction, in the `"synced"` state.
fn default_status_synced() -> String {
    STATUS_SYNCED.to_string()
}

/// One persisted, active folder sync (the on-disk table row and the
/// `store.folder-sync.ls` wire shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEntry {
    /// tc-storage folder id from the share link.
    pub folder_id: String,
    pub folder_name: String,
    /// Owner's `did:key` DID.
    pub owner_node_id: String,
    /// The share's p2p room (joined for the sync's lifetime).
    pub room_id: String,
    pub folder_key_hash: String,
    /// The granted folder key/passphrase (see module doc on persistence and
    /// on why this is redacted on the way out via [`redact`]). Empty while
    /// `status` is [`STATUS_CONNECTING`] (or [`STATUS_ERROR`] before the
    /// handshake has ever succeeded once) -- the grant hasn't happened yet.
    pub folder_key: String,
    /// Original share URL, kept for display and re-validation.
    pub share_url: String,
    /// Absolute local directory the folder is materialized into (by default
    /// a subdirectory of the content sandbox; see the module doc).
    pub local_dir: PathBuf,
    /// Sandbox-relative directory `local_dir` lives under (e.g. "MyFolder"),
    /// forward-slash normalized, or `""` if `local_dir` isn't under the
    /// sandbox root (a legacy entry, or a `--dir` override pointing
    /// elsewhere). Always recomputed from `local_dir` right before an entry
    /// leaves this module (see [`redact`]) rather than trusted from disk --
    /// `#[serde(default)]` only exists so an old persisted entry predating
    /// this field deserializes at all.
    #[serde(default)]
    pub sandbox_dir: String,
    /// One of [`STATUS_CONNECTING`], [`STATUS_SYNCED`], [`STATUS_ERROR`].
    #[serde(default = "default_status_synced")]
    pub status: String,
    /// CID of the last fully-applied folder bundle. `None` until the
    /// initial fetch completes.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_cid: Option<String>,
    /// `folderSignature` of the last fully-applied state (change detector).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_folder_signature: Option<String>,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_synced_at: Option<String>,
    /// Most recent sync failure, cleared on the next success.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_error: Option<String>,
    /// Every folder record known from the last successfully applied bundle
    /// (root folder included), used to resolve an incremental
    /// `folder-change`'s `folderId` to a local relative path without
    /// re-fetching the whole tree. Rebuilt wholesale on every full resync.
    #[serde(default)]
    pub folders: Vec<FolderRecord>,
    /// Every file currently materialized under `local_dir`, keyed by
    /// tc-storage file id. Drives incremental updates (locate the old path
    /// to remove on a move) and full-resync diffing (delete what vanished).
    #[serde(default)]
    pub files: Vec<SyncedFile>,
}

/// One file materialized under a [`SyncEntry`]'s `local_dir`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncedFile {
    /// tc-storage file id.
    pub file_id: String,
    /// The folder it currently belongs to (bundle's `FileRecord.folderId`).
    pub folder_id: String,
    /// `local_dir`-relative, forward-slash, jail-validated materialized
    /// path (see [`file_target_rel`]).
    pub rel_path: String,
    /// Bundle checksum as of materialization; an unmatched checksum on the
    /// next bundle is what drives a re-fetch instead of a skip.
    pub checksum: String,
}

/// `folder_key` is a secret credential we must keep on disk to survive
/// daemon restarts without repeating the grant handshake (see the module
/// doc), but it must never be echoed back over the daemon IPC/CLI surface.
/// Called on every [`SyncEntry`] returned to a caller, after it has already
/// been persisted (with the real key) if applicable. Also (re)computes
/// `sandbox_dir` from `local_dir` against the live `sandbox_root`, so a
/// caller always sees the current relationship rather than whatever was
/// last persisted.
fn redact(mut entry: SyncEntry, sandbox_root: &Path) -> SyncEntry {
    entry.sandbox_dir = sandbox_relative_dir(&entry.local_dir, sandbox_root).unwrap_or_default();
    entry.folder_key.clear();
    entry
}

/// The sandbox-relative directory `local_dir` lives under, forward-slash
/// normalized, or `None` if `local_dir` isn't under `sandbox_root` at all
/// (a legacy entry, or a `--dir` override pointing elsewhere). `local_dir`
/// is always produced by [`Sandbox::new`] (or equal to `sandbox_root` joined
/// with a subdirectory also absolutized the same way), so a plain
/// `strip_prefix` is enough -- no canonicalization/symlink resolution
/// mismatch to worry about.
fn sandbox_relative_dir(local_dir: &Path, sandbox_root: &Path) -> Option<String> {
    let rel = local_dir.strip_prefix(sandbox_root).ok()?;
    if rel.as_os_str().is_empty() {
        return None;
    }
    Some(rel.to_string_lossy().replace('\\', "/"))
}

/// `sharedFolderSignature` (tc-storage `src/folder/folderSync.ts`): a stable
/// JSON digest over every descendant folder's and file's sync-relevant
/// fields, used as a cheap "did anything change" comparator -- not
/// cryptographic. Ported to match the web app's `JSON.stringify` output
/// byte-for-byte for the common case: insertion-order keys, JS number
/// formatting (integral floats print without a decimal point), and the same
/// descendant filtering + sort. One known divergence: the TS side sorts
/// names with `localeCompare` while this uses plain `str::cmp` (same
/// precedent as `crate::wiresign::stable_stringify`); a mixed-case or non-ASCII name set
/// that orders differently only costs one spurious re-import on the peer
/// (the `lastCid` check gates repeats), so exact ICU collation isn't worth
/// emulating.
pub(super) fn folder_signature(
    root_folder_id: &str,
    folders: &[super::domain::FolderRecord],
    files: &[super::domain::FileRecord],
) -> String {
    use super::domain::{FileRecord, FolderRecord};

    if !folders.iter().any(|f| f.id == root_folder_id) {
        return String::new();
    }

    // descendantFolderIds: fixpoint over parentId edges from the root.
    let mut ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
    ids.insert(root_folder_id);
    let mut changed = true;
    while changed {
        changed = false;
        for folder in folders {
            if let Some(parent) = folder.parent_id.as_deref()
                && ids.contains(parent)
                && !ids.contains(folder.id.as_str())
            {
                ids.insert(&folder.id);
                changed = true;
            }
        }
    }

    // `String(sortOrder ?? 0)` -- JS prints integral numbers without ".0".
    fn js_number_string(n: Option<f64>) -> String {
        let n = n.filter(|v| v.is_finite()).unwrap_or(0.0);
        if n.fract() == 0.0 && n.abs() < 9e15 {
            format!("{}", n as i64)
        } else {
            format!("{n}")
        }
    }
    fn js_number(n: Option<f64>) -> serde_json::Value {
        let n = n.filter(|v| v.is_finite()).unwrap_or(0.0);
        if n.fract() == 0.0 && n.abs() < 9e15 {
            serde_json::Value::from(n as i64)
        } else {
            serde_json::Value::from(n)
        }
    }

    // foldersForSync: descendant filter + folderSortKey sort.
    let folder_sort_key = |folder: &FolderRecord| {
        format!(
            "{}/{:0>12}/{}",
            folder.parent_id.as_deref().unwrap_or(""),
            js_number_string(folder.sort_order),
            folder.name
        )
    };
    let mut sync_folders: Vec<&FolderRecord> = folders
        .iter()
        .filter(|f| ids.contains(f.id.as_str()))
        .collect();
    sync_folders.sort_by(|a, b| {
        folder_sort_key(a)
            .cmp(&folder_sort_key(b))
            .then_with(|| a.id.cmp(&b.id))
    });

    // folderFilesForSync: descendant filter + (folderId, compareFilesForDisplay).
    let mut sync_files: Vec<&FileRecord> = files
        .iter()
        .filter(|f| ids.contains(f.folder_id.as_str()))
        .collect();
    sync_files.sort_by(|a, b| {
        let sort_order = |v: Option<f64>| v.filter(|n| n.is_finite()).unwrap_or(0.0);
        a.folder_id
            .cmp(&b.folder_id)
            .then_with(|| {
                sort_order(a.sort_order)
                    .partial_cmp(&sort_order(b.sort_order))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });

    // pickFolderSyncFields / pickFileSyncFields. Serde serializes struct
    // fields in declaration order, reproducing the TS object literals'
    // insertion-order keys (a serde_json::Map would re-sort them).
    #[derive(Serialize)]
    struct FolderSyncFields {
        id: String,
        name: String,
        #[serde(rename = "parentId")]
        parent_id: Option<String>,
        #[serde(rename = "sortOrder")]
        sort_order: serde_json::Value,
        color: String,
        encrypted: bool,
        #[serde(rename = "shareEnabled")]
        share_enabled: bool,
        #[serde(rename = "deletedAt")]
        deleted_at: String,
    }
    #[derive(Serialize)]
    struct FileSyncFields {
        id: String,
        #[serde(rename = "folderId")]
        folder_id: String,
        #[serde(rename = "sortOrder")]
        sort_order: serde_json::Value,
        name: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        size: i64,
        checksum: String,
        #[serde(rename = "lastCid")]
        last_cid: String,
        version: i32,
        starred: bool,
        #[serde(rename = "deletedAt")]
        deleted_at: String,
    }
    #[derive(Serialize)]
    struct SignaturePayload {
        folders: Vec<FolderSyncFields>,
        files: Vec<FileSyncFields>,
    }

    let payload = SignaturePayload {
        folders: sync_folders
            .iter()
            .map(|f| FolderSyncFields {
                id: f.id.clone(),
                name: f.name.clone(),
                parent_id: f.parent_id.clone(),
                sort_order: js_number(f.sort_order),
                color: f.color.clone(),
                encrypted: f.encrypted,
                share_enabled: f.share_enabled,
                deleted_at: f.deleted_at.clone().unwrap_or_default(),
            })
            .collect(),
        files: sync_files
            .iter()
            .map(|f| FileSyncFields {
                id: f.id.clone(),
                folder_id: f.folder_id.clone(),
                sort_order: js_number(f.sort_order),
                name: f.name.clone(),
                mime_type: f.mime_type.clone(),
                size: f.size,
                checksum: f.checksum.clone(),
                last_cid: f.last_cid.clone().unwrap_or_default(),
                version: f.version,
                starred: f.starred,
                deleted_at: f.deleted_at.clone().unwrap_or_default(),
            })
            .collect(),
    };
    serde_json::to_string(&payload).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::super::domain::{FileRecord, FolderRecord};
    use super::*;

    fn folder(id: &str, parent: Option<&str>, name: &str) -> FolderRecord {
        FolderRecord {
            id: id.to_string(),
            name: name.to_string(),
            parent_id: parent.map(str::to_string),
            sort_order: None,
            color: "blue".to_string(),
            encrypted: true,
            share_enabled: true,
            shared_room_id: "room-1".to_string(),
            last_cid: None,
            last_saved_at: None,
            last_shared_at: None,
            deleted_at: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            field_versions: None,
        }
    }

    fn file(id: &str, folder_id: &str, name: &str) -> FileRecord {
        FileRecord {
            id: id.to_string(),
            folder_id: folder_id.to_string(),
            sort_order: None,
            name: name.to_string(),
            mime_type: "text/plain".to_string(),
            size: 3,
            data_url: None,
            checksum: "abc".to_string(),
            version: 1,
            starred: false,
            last_cid: Some("cid-1".to_string()),
            last_share_cid: None,
            deleted_at: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            field_versions: None,
        }
    }

    /// Expected strings generated with the actual tc-storage
    /// `sharedFolderSignature` (folderSync.ts) in Node.
    #[test]
    fn folder_signature_matches_tc_storage_json() {
        let folders = vec![folder("f1", None, "Root")];
        let files = vec![file("a1", "f1", "a.txt")];
        assert_eq!(
            folder_signature("f1", &folders, &files),
            r#"{"folders":[{"id":"f1","name":"Root","parentId":null,"sortOrder":0,"color":"blue","encrypted":true,"shareEnabled":true,"deletedAt":""}],"files":[{"id":"a1","folderId":"f1","sortOrder":0,"name":"a.txt","mimeType":"text/plain","size":3,"checksum":"abc","lastCid":"cid-1","version":1,"starred":false,"deletedAt":""}]}"#
        );
    }

    #[test]
    fn folder_signature_filters_to_descendants_and_sorts() {
        let folders = vec![
            folder("f2", Some("f1"), "Child"),
            folder("f1", None, "Root"),
            folder("g1", None, "Unrelated"),
        ];
        let files = vec![
            file("b1", "f2", "b.txt"),
            file("a1", "f1", "a.txt"),
            file("z1", "g1", "z.txt"),
        ];
        let signature = folder_signature("f1", &folders, &files);
        assert!(!signature.contains("Unrelated") && !signature.contains("z.txt"));
        let root_pos = signature.find("\"Root\"").expect("root present");
        let child_pos = signature.find("\"Child\"").expect("child present");
        assert!(
            root_pos < child_pos,
            "parent sorts before child: {signature}"
        );
        assert_eq!(folder_signature("missing", &folders, &files), "");
    }
}

// ---------------------------------------------------------------------
// Persistence: `<data_dir>/folder-syncs.json`, guarded by a process-wide
// lock (mirrors `Store::index_lock`'s read-modify-write idiom). Helpers take
// a plain `&Path` rather than `&Store` so the persistence logic itself is
// unit-testable without a live `Store` (which needs an opened block store).
// ---------------------------------------------------------------------

fn table_path(data_dir: &Path) -> PathBuf {
    data_dir.join("folder-syncs.json")
}

fn persist_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn load_entries_unlocked(data_dir: &Path) -> Result<Vec<SyncEntry>> {
    let path = table_path(data_dir);
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

fn save_entries_unlocked(data_dir: &Path, entries: &[SyncEntry]) -> Result<()> {
    let path = table_path(data_dir);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let text = serde_json::to_string_pretty(entries)?;
    std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))
}

async fn read_entries_at(data_dir: &Path) -> Result<Vec<SyncEntry>> {
    let _guard = persist_lock().lock().await;
    load_entries_unlocked(data_dir)
}

/// Read-modify-write under [`persist_lock`]: `f` sees the current table and
/// may mutate it in place; on `Ok`, the (possibly mutated) table is written
/// back before returning `f`'s result, so a duplicate-check `bail!` inside
/// `f` leaves the persisted file untouched.
async fn with_entries_at<F, R>(data_dir: &Path, f: F) -> Result<R>
where
    F: FnOnce(&mut Vec<SyncEntry>) -> Result<R>,
{
    let _guard = persist_lock().lock().await;
    let mut entries = load_entries_unlocked(data_dir)?;
    let result = f(&mut entries)?;
    save_entries_unlocked(data_dir, &entries)?;
    Ok(result)
}

async fn read_entries(store: &Store) -> Result<Vec<SyncEntry>> {
    read_entries_at(store.data_dir()).await
}

async fn with_entries<F, R>(store: &Store, f: F) -> Result<R>
where
    F: FnOnce(&mut Vec<SyncEntry>) -> Result<R>,
{
    with_entries_at(store.data_dir(), f).await
}

async fn find_entry(store: &Store, folder_id: &str) -> Result<Option<SyncEntry>> {
    Ok(read_entries(store)
        .await?
        .into_iter()
        .find(|e| e.folder_id == folder_id))
}

async fn update_entry(
    store: &Store,
    folder_id: &str,
    f: impl FnOnce(&mut SyncEntry),
) -> Result<()> {
    with_entries(store, |entries| {
        if let Some(entry) = entries.iter_mut().find(|e| e.folder_id == folder_id) {
            f(entry);
        }
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------
// Path resolution and diff planning: pure, offline logic shared by the
// initial fetch ([`run_initial_handshake`]) and the background loop's full
// resync.
// ---------------------------------------------------------------------

fn file_deleted(file: &FileRecord) -> bool {
    file.deleted_at.as_deref().is_some_and(|s| !s.is_empty())
}

fn folder_map_from_records(records: &[FolderRecord]) -> HashMap<String, FolderRecord> {
    records
        .iter()
        .filter(|f| !folder_share::folder_deleted(f))
        .map(|f| (f.id.clone(), f.clone()))
        .collect()
}

/// Builds the `{folderId: FolderRecord}` map [`file_target_rel`] resolves
/// paths against, from a freshly-fetched bundle: every non-deleted
/// descendant folder plus the root itself (some owner implementations only
/// enumerate descendants in `bundle.folders`, so the root is added
/// unconditionally rather than only as a fallback).
fn build_folder_map(bundle: &super::domain::FolderBundle) -> HashMap<String, FolderRecord> {
    let mut folders = match &bundle.folders {
        Some(list) => folder_map_from_records(list),
        None => HashMap::new(),
    };
    if !bundle.folder.id.is_empty() && !folder_share::folder_deleted(&bundle.folder) {
        folders
            .entry(bundle.folder.id.clone())
            .or_insert_with(|| bundle.folder.clone());
    }
    folders
}

/// Resolves a file's `local_dir`-relative materialized path: walks
/// [`folder_share::folder_path_parts`]'s root-to-leaf ancestor chain for
/// `folder_id`, drops the root folder's own name segment (`local_dir`
/// already *is* that folder -- see the module doc), and appends the
/// sanitized file name. `None` if `folder_id` doesn't resolve at all (e.g.
/// a `folder-change` referencing a folder this sync hasn't learned about
/// yet); the caller should treat that as "wait for the next full resync",
/// not delete anything.
///
/// Every path component here has already been through
/// [`folder_share::sanitize_name`] (via `folder_path_parts`, and again for
/// the file name below), which replaces path separators and rejects
/// `.`/`..`/empty names outright -- so the joined relative path can never
/// itself contain a traversal sequence. [`Sandbox::resolve`] is still the
/// authoritative jail (defense in depth, and the only guard against a
/// pathologically deep/absolute reconstruction): every write in this module
/// goes through it.
///
/// Known gap: if an *intermediate* ancestor is missing or deleted (a
/// malformed/partial folder tree), the chain stops there instead of at the
/// true root, and the first surviving segment is still stripped as if it
/// were the root. This mirrors this module's general stance elsewhere
/// (`folder_signature`'s `localeCompare` note) of accepting a rare,
/// self-correcting cosmetic misplacement rather than adding real complexity
/// for owner-side data that's already inconsistent.
fn file_target_rel(
    folders: &HashMap<String, FolderRecord>,
    folder_id: &str,
    file_name: &str,
) -> Option<String> {
    let mut parts = folder_share::folder_path_parts(folders, folder_id);
    if parts.is_empty() {
        return None;
    }
    parts.remove(0);
    parts.push(folder_share::sanitize_name(file_name));
    Some(parts.join("/"))
}

/// One bundle file to (re)fetch and materialize, as planned by
/// [`plan_resync`].
#[derive(Debug)]
struct FetchItem {
    file: FileRecord,
    rel: String,
    cid: String,
}

/// Output of [`plan_resync`]: what [`sync_bundle`] should do to bring
/// `local_dir` in line with a freshly-fetched bundle.
#[derive(Debug, Default)]
struct ResyncPlan {
    /// Already-materialized files that are unchanged (same checksum, same
    /// target path) -- kept as-is, no re-fetch.
    reuse: Vec<SyncedFile>,
    /// New or changed files to fetch and (over)write.
    fetch: Vec<FetchItem>,
    /// Bundle files that couldn't be planned (unresolvable folder, or no
    /// content cid), with a human-readable reason. Their previously
    /// materialized copy (if any) is left untouched -- see the module doc
    /// on `file_target_rel`.
    skipped: Vec<String>,
    /// Previously materialized relative paths to delete: files genuinely
    /// gone from the bundle (deleted, or absent entirely), plus the old
    /// path of any file that moved to a different folder under the same
    /// file id (which lands in `fetch`, not `reuse`).
    remove_paths: Vec<String>,
}

/// Pure, offline diff plan for applying a freshly-fetched bundle
/// (`bundle_files` + `folders`) against what's already materialized
/// (`previous_files`). Touches neither the network nor the filesystem, so
/// it's the unit-testable core of [`sync_bundle`]'s full-resync path.
fn plan_resync(
    bundle_files: &[FileRecord],
    folders: &HashMap<String, FolderRecord>,
    previous_files: &[SyncedFile],
) -> ResyncPlan {
    let mut plan = ResyncPlan::default();
    let mut seen_ids: HashSet<String> = HashSet::new();

    for file in bundle_files {
        if file_deleted(file) {
            continue;
        }
        seen_ids.insert(file.id.clone());
        let previous = previous_files.iter().find(|f| f.file_id == file.id);

        let Some(rel) = file_target_rel(folders, &file.folder_id, &file.name) else {
            plan.skipped.push(format!(
                "{}: unresolved folder {}",
                file.name, file.folder_id
            ));
            continue;
        };
        let cid = file
            .last_share_cid
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| file.last_cid.clone().filter(|s| !s.is_empty()));
        let Some(cid) = cid else {
            plan.skipped.push(format!("{}: no content cid", file.name));
            continue;
        };

        if let Some(previous) = previous {
            if previous.checksum == file.checksum && previous.rel_path == rel {
                plan.reuse.push(previous.clone());
                continue;
            }
            if previous.rel_path != rel {
                plan.remove_paths.push(previous.rel_path.clone());
            }
        }
        plan.fetch.push(FetchItem {
            file: file.clone(),
            rel,
            cid,
        });
    }

    for previous in previous_files {
        if !seen_ids.contains(&previous.file_id) {
            plan.remove_paths.push(previous.rel_path.clone());
        }
    }

    plan
}

/// Applies a freshly-fetched bundle to `local_dir`: plans the diff against
/// `previous_files` ([`plan_resync`]), fetches+verifies+writes every new or
/// changed file (skipping -- not failing the whole sync -- on an individual
/// fetch/checksum/jail error), and removes local paths the plan says are
/// stale. `progress` receives one human-readable line per file action (the
/// background full-resync loop passes a no-op sink; [`run_initial_handshake`]
/// logs each line at debug level -- there is no live caller to stream
/// progress to, see the module doc). Returns the new `files`/`folders` table
/// for the caller to persist.
async fn sync_bundle(
    store: &Store,
    local_dir: &Path,
    folder_key: &str,
    bundle: &super::domain::FolderBundle,
    previous_files: &[SyncedFile],
    progress: &mut (dyn FnMut(&str) + Send),
) -> Result<(Vec<SyncedFile>, Vec<FolderRecord>)> {
    let sandbox = Sandbox::new(local_dir)?;
    let folders_map = build_folder_map(bundle);
    let plan = plan_resync(&bundle.files, &folders_map, previous_files);

    for reason in &plan.skipped {
        progress(&format!("skip {reason}"));
    }

    let mut new_files = plan.reuse;
    for item in plan.fetch {
        let data = match folder_share::fetch_file_content(store, &item.cid, folder_key).await {
            Ok(data) => data,
            Err(error) => {
                progress(&format!("skip {} ({error})", item.rel));
                continue;
            }
        };
        if !item.file.checksum.is_empty() && sha256_hex(&data) != item.file.checksum {
            progress(&format!("skip {} (checksum mismatch)", item.rel));
            continue;
        }
        let target = match sandbox.resolve(&item.rel) {
            Ok(target) => target,
            Err(error) => {
                progress(&format!("skip {} ({error})", item.rel));
                continue;
            }
        };
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        tokio::fs::write(&target, &data)
            .await
            .with_context(|| format!("writing {}", target.display()))?;
        progress(&format!("saved {} ({} bytes)", item.rel, data.len()));
        new_files.push(SyncedFile {
            file_id: item.file.id.clone(),
            folder_id: item.file.folder_id.clone(),
            rel_path: item.rel,
            checksum: item.file.checksum.clone(),
        });
    }

    for rel in &plan.remove_paths {
        match sandbox.remove(rel) {
            Ok(()) => progress(&format!("removed {rel}")),
            Err(error) => progress(&format!("could not remove {rel} ({error})")),
        }
    }

    Ok((new_files, folders_map.into_values().collect()))
}

/// Whether a `folder-state`/`folder-share` announce (`new_cid` +
/// `new_signature`) describes a state this sync hasn't already applied.
/// The cid is the authoritative gate; the `folderSignature` is only
/// consulted as an extra "did anything actually change" hint when *both*
/// sides know one (an owner republish always bumps the cid even for a
/// no-op re-save, so cid-only comparison alone would over-trigger less
/// precisely -- but under-triggering on a missing signature would risk
/// missing a real change, so a cid mismatch always wins).
fn resync_needed(entry: &SyncEntry, new_cid: &str, new_signature: Option<&str>) -> bool {
    if entry.last_cid.as_deref() != Some(new_cid) {
        return true;
    }
    if let (Some(old), Some(new)) = (entry.last_folder_signature.as_deref(), new_signature) {
        return old != new;
    }
    false
}

/// Broadcasts a signed `{"type":"hello"}` into `room`, retrying transient
/// "room not joined" failures the same way
/// [`folder_share::send_bytes_retrying`] does for unicasts -- best-effort;
/// a failure here just means we fall back to waiting for the owner's next
/// periodic (up to ~60s) `folder-state`/`folder-share` re-announce.
async fn broadcast_hello(room: &str, state: &Arc<AppState>) {
    let Ok(identity) = crate::identity::current(state).await else {
        return;
    };
    let hello = ShareEnvelope {
        type_: "hello".to_string(),
        from: identity.did().to_string(),
        room_id: room.to_string(),
        sent_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        ..ShareEnvelope::default()
    };
    let Ok(signed) = folder_share::sign_envelope(hello, &identity) else {
        return;
    };
    let Ok(bytes) = serde_json::to_vec(&signed) else {
        return;
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match crate::net::send_broadcast(room, bytes.clone()).await {
            Ok(()) => return,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(error) => {
                warn!(%error, room, "folder-sync: hello broadcast failed");
                return;
            }
        }
    }
}

/// Single-flight registry keyed by `folder_id`, deduplicating concurrent
/// attempts to bring one folder's sync up to date: a freshly
/// [`register_sync`]ed entry's spawned background task, [`spawn_background`]'s
/// startup recovery scan, and its 30s retry tick can all name the same
/// `folder_id` at once (e.g. a slow owner spanning a daemon restart). Same
/// rationale as `folder_share`'s `INFLIGHT_FOLDER_FETCHES`: the access-grant
/// handshake can block for minutes on owner approval, and nothing cancels it
/// if one caller goes away, so an unguarded second attempt would send its
/// own duplicate access request (re-prompting the owner) instead of leaving
/// the one already in flight to finish.
static INFLIGHT_INITIAL_SYNCS: folder_share::SingleFlightRegistry<()> = OnceCell::const_new();

/// Number of leading characters of `folder_id` appended (with a `-`
/// separator) to a sandbox subdirectory name when it collides with another
/// already-registered sync's directory; see [`register_sync`].
const COLLISION_SUFFIX_LEN: usize = 8;

/// Picks the sandbox-relative subdirectory name a new sync's `local_dir`
/// should be created at, given every other already-registered sync
/// (`existing`): [`folder_share::sanitize_name`] of `folder_name`, unless
/// that collides with a *different* sync's directory (matched by
/// `folder_id`, so re-registering the same folder never "collides" with
/// itself), in which case `-<first 8 chars of folder_id>` is appended to
/// disambiguate. Pure and offline so it's unit-testable without a live
/// `Store`; [`register_sync`] is the only caller.
fn sandbox_dir_name_for(
    folder_name: &str,
    folder_id: &str,
    existing: &[SyncEntry],
    sandbox_root: &Path,
) -> String {
    let base_name = folder_share::sanitize_name(folder_name);
    let collides = existing.iter().any(|e| {
        e.folder_id != folder_id
            && sandbox_relative_dir(&e.local_dir, sandbox_root).as_deref()
                == Some(base_name.as_str())
    });
    if collides {
        let suffix: String = folder_id.chars().take(COLLISION_SUFFIX_LEN).collect();
        format!("{base_name}-{suffix}")
    } else {
        base_name
    }
}

/// Register a folder sync: parse and validate `share_url` (and `local_dir`,
/// if given), persist a [`STATUS_CONNECTING`] entry, and return immediately
/// -- the access-grant handshake and first full fetch run in a spawned
/// background task (see [`ensure_initial_sync`]) that updates this same
/// entry in place.
///
/// `local_dir`: `None` (the normal case -- what the web UI always sends)
/// computes a dedicated subdirectory of the content sandbox from the shared
/// folder's name, disambiguated against other syncs' directories on a name
/// collision (see the module doc's "Sync destination" section). `Some(dir)`
/// is the CLI power-user override: `dir` is used verbatim as a real,
/// user-chosen directory outside the sandbox, exactly as this always worked
/// before.
///
/// Errors (synchronously, before anything is persisted) if the link is not a
/// `folder-share`, an explicit `dir` is not absolute/creatable, or a sync for
/// the same `folder_id` already exists.
pub async fn register_sync(
    share_url: &str,
    local_dir: Option<&str>,
    store: &Arc<Store>,
    state: &Arc<AppState>,
) -> Result<SyncEntry> {
    let linked = super::sharelink::parse_share_link(share_url)?;
    let share = linked.share;
    if share.type_ != "folder-share" {
        bail!("not a folder-share link (type {:?})", share.type_);
    }
    let owner = share
        .owner_node_id
        .clone()
        .filter(|s| !s.is_empty())
        .context("share link missing ownerNodeId")?;
    if !folder_share::is_ed25519_did_key(&owner) {
        bail!("share owner is not a valid did:key");
    }
    let folder_id = share
        .folder_id
        .clone()
        .filter(|s| !s.is_empty())
        .context("share link missing folderId")?;
    let folder_key_hash_expected = share
        .folder_key_hash
        .clone()
        .filter(|s| !s.is_empty())
        .context("share link missing folderKeyHash")?;
    if share.room_id.trim().is_empty() {
        bail!("share link missing roomId");
    }

    let existing_entries = read_entries(store).await?;
    if let Some(existing) = existing_entries.iter().find(|e| e.folder_id == folder_id) {
        bail!(
            "already syncing folder {folder_id} ({})",
            existing.folder_name
        );
    }

    let folder_name = share
        .folder_name
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "shared-folder".to_string());

    // Canonicalize + create local_dir (absolutized, created if missing --
    // same semantics as `Sandbox::new`, which this borrows for exactly that
    // reason; every subsequent write in this sync also goes through a
    // `Sandbox` rooted here, so this doubles as an early permissions check).
    let local_dir_abs = match local_dir.map(str::trim).filter(|s| !s.is_empty()) {
        Some(dir) => Sandbox::new(dir)?.root().to_path_buf(),
        None => {
            let sandbox_root = store.data_dir().join("sandbox");
            let dir_name =
                sandbox_dir_name_for(&folder_name, &folder_id, &existing_entries, &sandbox_root);
            Sandbox::new(sandbox_root.join(dir_name))?
                .root()
                .to_path_buf()
        }
    };

    let entry = SyncEntry {
        folder_id: folder_id.clone(),
        folder_name,
        owner_node_id: owner,
        room_id: share.room_id.clone(),
        folder_key_hash: folder_key_hash_expected,
        folder_key: String::new(),
        share_url: share_url.to_string(),
        local_dir: local_dir_abs,
        sandbox_dir: String::new(), // recomputed by `redact` on the way out
        status: STATUS_CONNECTING.to_string(),
        last_cid: None,
        last_folder_signature: None,
        started_at: chrono::Utc::now().to_rfc3339(),
        last_synced_at: None,
        last_error: None,
        folders: Vec::new(),
        files: Vec::new(),
    };

    with_entries(store.as_ref(), |entries| {
        if entries.iter().any(|e| e.folder_id == entry.folder_id) {
            bail!("already syncing folder {}", entry.folder_id);
        }
        entries.push(entry.clone());
        Ok(())
    })
    .await?;

    let store_bg = store.clone();
    let state_bg = state.clone();
    let folder_id_bg = folder_id.clone();
    tokio::spawn(async move {
        ensure_initial_sync(folder_id_bg, store_bg, state_bg).await;
    });

    Ok(redact(entry, &store.data_dir().join("sandbox")))
}

/// Single-flight (per [`INFLIGHT_INITIAL_SYNCS`]) attempt to bring
/// `folder_id`'s persisted entry up to date; swallows and logs its own
/// error (the caller -- a spawned task, a startup scan, or a retry tick --
/// has nothing further to do with it; the failure is already recorded on
/// the entry by [`retry_sync_once`]).
async fn ensure_initial_sync(folder_id: String, store: Arc<Store>, state: Arc<AppState>) {
    folder_share::single_flight(&INFLIGHT_INITIAL_SYNCS, folder_id.clone(), move || async move {
        if let Err(error) = retry_sync_once(&folder_id, &store, &state).await {
            debug!(%error, folder_id = %folder_id, "folder-sync: sync attempt failed, will retry");
        }
    })
    .await;
}

/// Attempts to bring one persisted, not-yet-`"synced"` entry up to date:
/// if the initial grant handshake never completed ([`SyncEntry::folder_key`]
/// still empty -- covers both a fresh [`STATUS_CONNECTING`] entry and one
/// that errored before ever completing it), (re)runs the full handshake
/// ([`run_initial_handshake`]); otherwise (a previously synced entry whose
/// *later* full resync or incremental apply failed, see [`handle_envelope`])
/// the grant is already held, so this just nudges the owner and re-applies
/// the latest state ([`run_resync`]). Either way, on error the entry is
/// updated to [`STATUS_ERROR`] with `last_error` set; no-ops (without error)
/// if the entry was removed ([`stop_sync`]) or already reached
/// [`STATUS_SYNCED`] since being queued -- both routine races with a
/// concurrent caller.
async fn retry_sync_once(folder_id: &str, store: &Store, state: &Arc<AppState>) -> Result<()> {
    let Some(entry) = find_entry(store, folder_id).await? else {
        return Ok(());
    };
    if entry.status == STATUS_SYNCED {
        return Ok(());
    }

    let result = if entry.folder_key.is_empty() {
        run_initial_handshake(&entry, store, state).await
    } else {
        run_resync(&entry, store, state).await
    };

    if let Err(error) = &result {
        let message = format!("{error:#}");
        let _ = update_entry(store, folder_id, move |e| {
            e.status = STATUS_ERROR.to_string();
            e.last_error = Some(message);
        })
        .await;
    }
    result
}

/// Runs the access-grant handshake and first full fetch for `entry` (which
/// must not yet hold a `folder_key`), materializing into `entry.local_dir`
/// and, on success, updating the persisted entry to [`STATUS_SYNCED`] with
/// the granted key and fetched state. Progress is logged at debug level
/// (there is no live CLI caller waiting on this -- see the module doc).
async fn run_initial_handshake(
    entry: &SyncEntry,
    store: &Store,
    state: &Arc<AppState>,
) -> Result<()> {
    let mut progress = |line: &str| debug!(folder_id = %entry.folder_id, "folder-sync: {line}");

    crate::net::ensure_started(state, entry.room_id.clone())
        .await
        .context("folder-sync: joining share room")?;
    broadcast_hello(&entry.room_id, state).await;

    let identity = crate::identity::current(state).await?;

    progress(&format!(
        "connecting to owner {}…",
        folder_share::short(&entry.owner_node_id)
    ));
    folder_share::wait_for_peer(&entry.owner_node_id, Duration::from_secs(40)).await?;

    let request_key = folder_share::create_access_request_key();
    let mut suffix = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut suffix);
    let suffix_hex: String = suffix.iter().map(|b| format!("{b:02x}")).collect();
    let request_id = format!(
        "access-{}-{suffix_hex}",
        chrono::Utc::now().timestamp_millis()
    );

    let request = ShareEnvelope {
        type_: "folder-access-request".to_string(),
        from: identity.did().to_string(),
        room_id: entry.room_id.clone(),
        sent_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        folder_id: Some(entry.folder_id.clone()),
        folder_name: Some(entry.folder_name.clone()),
        access_grant_mode: Some("owner".to_string()),
        folder_key_hash: Some(entry.folder_key_hash.clone()),
        target_node_id: Some(entry.owner_node_id.clone()),
        request_id: Some(request_id.clone()),
        sender_profile: Some(ShareProfile {
            name: "mistl".to_string(),
        }),
        access_public_key: Some(request_key.public_b64url.clone()),
        ..ShareEnvelope::default()
    };
    let signed_request = folder_share::sign_envelope(request, &identity)?;

    let (folder_key, grant_cid) = folder_share::request_and_await_grant(
        &entry.room_id,
        &entry.owner_node_id,
        &request_id,
        &entry.folder_id,
        &entry.folder_key_hash,
        &signed_request,
        &request_key,
        &mut progress,
    )
    .await?;

    let cid = match grant_cid.filter(|s| !s.is_empty()) {
        Some(cid) => cid,
        None => {
            progress("waiting for folder-state…");
            folder_share::await_folder_state_cid(&entry.folder_id, Duration::from_secs(60)).await?
        }
    };

    progress(&format!(
        "fetching folder manifest ({})…",
        folder_share::short(&cid)
    ));
    let bundle = folder_share::fetch_folder_bundle(store, &cid, &folder_key).await?;
    let folder_name = [bundle.folder.name.as_str(), entry.folder_name.as_str()]
        .into_iter()
        .find(|s| !s.trim().is_empty())
        .unwrap_or("shared-folder")
        .to_string();

    progress(&format!(
        "folder {folder_name:?}: {} file(s)",
        bundle.files.len()
    ));
    let (files, folders) = sync_bundle(
        store,
        &entry.local_dir,
        &folder_key,
        &bundle,
        &[],
        &mut progress,
    )
    .await?;

    let now = chrono::Utc::now().to_rfc3339();
    update_entry(store, &entry.folder_id, move |e| {
        e.folder_name = folder_name;
        e.folder_key = folder_key;
        e.last_cid = Some(cid);
        e.last_synced_at = Some(now);
        e.last_error = None;
        e.status = STATUS_SYNCED.to_string();
        e.folders = folders;
        e.files = files;
    })
    .await
}

/// Retries a previously synced entry after a later full-resync or
/// incremental-apply failure (see [`handle_envelope`]): the access-grant
/// handshake already succeeded once, so this skips straight to rejoining
/// the room, nudging the owner for a fresh announce, and re-applying
/// whatever state that produces -- either a fresh `folder-state` cid, or
/// (if the owner doesn't answer quickly) the last known-good cid, covering
/// a failure that was transient/local rather than caused by a stale cid.
async fn run_resync(entry: &SyncEntry, store: &Store, state: &Arc<AppState>) -> Result<()> {
    crate::net::ensure_started(state, entry.room_id.clone())
        .await
        .context("folder-sync: rejoining share room")?;
    broadcast_hello(&entry.room_id, state).await;

    let cid = match folder_share::await_folder_state_cid(&entry.folder_id, Duration::from_secs(15))
        .await
    {
        Ok(cid) => cid,
        Err(_) => entry.last_cid.clone().context(
            "folder-sync: owner did not respond and no known folder state to retry against",
        )?,
    };
    full_resync(store, entry, &cid, None).await
}

/// All persisted syncs, active or errored.
pub async fn list_syncs(store: &Store) -> Result<Vec<SyncEntry>> {
    let sandbox_root = store.data_dir().join("sandbox");
    Ok(read_entries(store)
        .await?
        .into_iter()
        .map(|entry| redact(entry, &sandbox_root))
        .collect())
}

/// Stop and forget the sync for `folder_id` (leaves its room refcount,
/// keeps the materialized files). Returns `false` if no such sync exists.
pub async fn stop_sync(folder_id: &str, store: &Store, _state: &Arc<AppState>) -> Result<bool> {
    let removed = with_entries(store, |entries| {
        let Some(pos) = entries.iter().position(|e| e.folder_id == folder_id) else {
            return Ok(None);
        };
        Ok(Some(entries.remove(pos)))
    })
    .await?;
    let Some(entry) = removed else {
        return Ok(false);
    };
    let _ = crate::net::leave_room(&entry.room_id).await;
    Ok(true)
}

// ---------------------------------------------------------------------
// Background loop: rejoin persisted syncs' rooms, then react to every
// verified `ShareEnvelope` for a folder we're syncing.
// ---------------------------------------------------------------------

async fn apply_file_upsert(
    store: &Store,
    entry: &SyncEntry,
    envelope: &ShareEnvelope,
) -> Result<()> {
    let Some(file) = envelope.file.clone() else {
        return Ok(());
    };
    if file.id.is_empty() {
        return Ok(());
    }
    if file_deleted(&file) {
        return apply_file_delete(store, entry, envelope).await;
    }

    let folders = folder_map_from_records(&entry.folders);
    let Some(rel) = file_target_rel(&folders, &file.folder_id, &file.name) else {
        bail!(
            "file {}: unknown folder {} in local sync tree",
            file.name,
            file.folder_id
        );
    };
    let cid = file
        .last_share_cid
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| file.last_cid.clone().filter(|s| !s.is_empty()));
    let Some(cid) = cid else {
        bail!("file {}: no content cid", file.name);
    };

    let data = folder_share::fetch_file_content(store, &cid, &entry.folder_key)
        .await
        .with_context(|| format!("fetching {}", file.name))?;
    if !file.checksum.is_empty() && sha256_hex(&data) != file.checksum {
        bail!("file {}: checksum mismatch", file.name);
    }

    let sandbox = Sandbox::new(&entry.local_dir)?;
    // A move (same file id, different folder) would otherwise leave a stale
    // copy behind at the old path.
    if let Some(previous) = entry.files.iter().find(|f| f.file_id == file.id)
        && previous.rel_path != rel
    {
        let _ = sandbox.remove(&previous.rel_path);
    }
    let target = sandbox
        .resolve(&rel)
        .with_context(|| format!("resolving {rel}"))?;
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    tokio::fs::write(&target, &data)
        .await
        .with_context(|| format!("writing {}", target.display()))?;

    let synced = SyncedFile {
        file_id: file.id.clone(),
        folder_id: file.folder_id.clone(),
        rel_path: rel,
        checksum: file.checksum.clone(),
    };
    update_entry(store, &entry.folder_id, move |e| {
        e.files.retain(|f| f.file_id != synced.file_id);
        e.files.push(synced);
        e.last_synced_at = Some(chrono::Utc::now().to_rfc3339());
        e.last_error = None;
        e.status = STATUS_SYNCED.to_string();
    })
    .await
}

async fn apply_file_delete(
    store: &Store,
    entry: &SyncEntry,
    envelope: &ShareEnvelope,
) -> Result<()> {
    let file_id = envelope
        .file_id
        .clone()
        .or_else(|| envelope.file.as_ref().map(|f| f.id.clone()))
        .filter(|s| !s.is_empty());
    let Some(file_id) = file_id else {
        return Ok(());
    };
    let Some(previous) = entry.files.iter().find(|f| f.file_id == file_id).cloned() else {
        return Ok(());
    };

    let sandbox = Sandbox::new(&entry.local_dir)?;
    if let Err(error) = sandbox.remove(&previous.rel_path) {
        warn!(%error, folder_id = %entry.folder_id, rel = %previous.rel_path, "folder-sync: failed to remove deleted file");
    }

    update_entry(store, &entry.folder_id, move |e| {
        e.files.retain(|f| f.file_id != file_id);
        e.last_synced_at = Some(chrono::Utc::now().to_rfc3339());
        e.last_error = None;
        e.status = STATUS_SYNCED.to_string();
    })
    .await
}

async fn handle_folder_change(
    store: &Store,
    entry: &SyncEntry,
    envelope: &ShareEnvelope,
) -> Result<()> {
    match envelope.change_type.as_deref() {
        Some("file-upserted") => apply_file_upsert(store, entry, envelope).await,
        Some("file-deleted") => apply_file_delete(store, entry, envelope).await,
        Some("folder-upserted") | Some("folder-deleted") => {
            match envelope.cid.clone().filter(|s| !s.is_empty()) {
                Some(cid) => {
                    full_resync(store, entry, &cid, envelope.folder_signature.clone()).await
                }
                // Structural change with no cid inline: wait for the
                // owner's next `folder-state`/`folder-share` (<=60s, or
                // immediately on our own next `hello`), which always
                // carries one.
                None => Ok(()),
            }
        }
        _ => Ok(()),
    }
}

async fn handle_folder_state(
    store: &Store,
    entry: &SyncEntry,
    envelope: &ShareEnvelope,
) -> Result<()> {
    let Some(cid) = envelope.cid.clone().filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    let signature = envelope.folder_signature.clone();

    if !resync_needed(entry, &cid, signature.as_deref()) {
        // Heartbeat only. Opportunistically capture the announced signature
        // if we didn't already have one, so future comparisons have a real
        // value to compare against (see `resync_needed`'s doc).
        if entry.last_folder_signature.is_none() && signature.is_some() {
            return update_entry(store, &entry.folder_id, move |e| {
                e.last_folder_signature = signature
            })
            .await;
        }
        return Ok(());
    }

    full_resync(store, entry, &cid, signature).await
}

async fn full_resync(
    store: &Store,
    entry: &SyncEntry,
    cid: &str,
    folder_signature: Option<String>,
) -> Result<()> {
    let bundle = folder_share::fetch_folder_bundle(store, cid, &entry.folder_key)
        .await
        .with_context(|| format!("fetching folder bundle {cid}"))?;
    let mut discard = |_line: &str| {};
    let (files, folders) = sync_bundle(
        store,
        &entry.local_dir,
        &entry.folder_key,
        &bundle,
        &entry.files,
        &mut discard,
    )
    .await?;

    let cid = cid.to_string();
    update_entry(store, &entry.folder_id, move |e| {
        e.files = files;
        e.folders = folders;
        e.last_cid = Some(cid);
        if let Some(signature) = folder_signature {
            e.last_folder_signature = Some(signature);
        }
        e.last_synced_at = Some(chrono::Utc::now().to_rfc3339());
        e.last_error = None;
        e.status = STATUS_SYNCED.to_string();
    })
    .await
}

async fn handle_envelope(store: &Store, envelope: &ShareEnvelope) -> Result<()> {
    let Some(folder_id) = envelope.folder_id.clone() else {
        return Ok(());
    };
    let Some(entry) = find_entry(store, &folder_id).await? else {
        return Ok(()); // not one of our syncs
    };

    let result = match envelope.type_.as_str() {
        "folder-change" => handle_folder_change(store, &entry, envelope).await,
        "folder-state" | "folder-share" => handle_folder_state(store, &entry, envelope).await,
        _ => Ok(()),
    };
    if let Err(error) = &result {
        let message = error.to_string();
        let _ = update_entry(store, &folder_id, move |e| {
            e.status = STATUS_ERROR.to_string();
            e.last_error = Some(message);
        })
        .await;
    }
    result
}

/// How often [`run_background`] retries any entry stuck in
/// [`STATUS_CONNECTING`]/[`STATUS_ERROR`] -- the owner may be offline for
/// hours, so this just needs to be frequent enough to recover promptly once
/// they're back, not tight (see `folder_owner`'s `RESCAN_INTERVAL` for the
/// same tradeoff on the owner side).
const RETRY_INTERVAL: Duration = Duration::from_secs(30);

/// Kicks off [`ensure_initial_sync`] for every persisted entry not already
/// [`STATUS_SYNCED`] (spawned individually so one slow/offline owner can't
/// delay the others or block the caller).
fn retry_unsynced_entries(entries: Vec<SyncEntry>, store: &Arc<Store>, state: &Arc<AppState>) {
    for entry in entries {
        if entry.status == STATUS_SYNCED {
            continue;
        }
        debug!(folder_id = %entry.folder_id, status = %entry.status, "folder-sync: retrying unsynced entry");
        let store = store.clone();
        let state = state.clone();
        tokio::spawn(async move {
            ensure_initial_sync(entry.folder_id, store, state).await;
        });
    }
}

async fn run_background(state: Arc<AppState>) -> Result<()> {
    let store = super::store(&state)
        .await
        .context("folder-sync: opening store")?;

    let mut still_connecting = Vec::new();
    for entry in read_entries(&store)
        .await
        .context("folder-sync: loading sync table")?
    {
        if entry.status != STATUS_SYNCED {
            // Handshake never finished (or errored) before the last
            // restart/shutdown: no room to rejoin yet in the "never
            // connected" case, and either way the retry below re-joins as
            // part of the handshake/resync itself.
            still_connecting.push(entry);
            continue;
        }
        if let Err(error) = crate::net::ensure_started(&state, entry.room_id.clone()).await {
            warn!(%error, folder_id = %entry.folder_id, "folder-sync: failed to rejoin room on startup");
            continue;
        }
        broadcast_hello(&entry.room_id, &state).await;
    }
    retry_unsynced_entries(still_connecting, &store, &state);

    let mut rx = folder_share::envelope_bus().await.subscribe();
    let mut retry_ticker = tokio::time::interval(RETRY_INTERVAL);
    retry_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires immediately; the startup scan above already
    // covers that pass, so skip it to avoid an instant duplicate retry.
    retry_ticker.tick().await;

    loop {
        tokio::select! {
            _ = retry_ticker.tick() => {
                match read_entries(&store).await {
                    Ok(entries) => retry_unsynced_entries(entries, &store, &state),
                    Err(error) => warn!(%error, "folder-sync: failed to load sync table for retry tick"),
                }
            }
            received = rx.recv() => {
                let envelope = match received {
                    Ok(envelope) => envelope,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        warn!("folder-sync: envelope bus closed; background sync loop stopping");
                        return Ok(());
                    }
                };
                if let Err(error) = handle_envelope(&store, &envelope).await {
                    warn!(
                        %error,
                        folder_id = ?envelope.folder_id,
                        wire_type = %envelope.type_,
                        "folder-sync: error applying envelope"
                    );
                }
            }
        }
    }
}

/// Daemon-lifetime background task keeping every persisted sync live:
/// rejoin rooms on startup, subscribe to the envelope bus, apply
/// `folder-change` incrementally and `folder-state`/`folder-share` by
/// signature diff, and retry any entry still `"connecting"`/`"error"`
/// (fresh registration or a stalled handshake/resync) every
/// [`RETRY_INTERVAL`]. A no-op when no syncs are persisted. Call once from
/// daemon startup (next to `update::spawn_auto_update`).
pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(error) = run_background(state).await {
            warn!(%error, "folder-sync: background task failed to start");
        }
    });
}

#[cfg(test)]
mod sync_tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mistl-folder-sync-test-{label}-{}-{nanos}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn path(&self) -> PathBuf {
            self.0.clone()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn sample_entry(folder_id: &str, local_dir: &Path) -> SyncEntry {
        SyncEntry {
            folder_id: folder_id.to_string(),
            folder_name: "Shared".to_string(),
            owner_node_id: "did:key:zOwner".to_string(),
            room_id: "room-1".to_string(),
            folder_key_hash: "hash".to_string(),
            folder_key: "super-secret-key".to_string(),
            share_url: "#tc-share=abc".to_string(),
            local_dir: local_dir.to_path_buf(),
            sandbox_dir: String::new(),
            status: STATUS_SYNCED.to_string(),
            last_cid: Some("cid-1".to_string()),
            last_folder_signature: None,
            started_at: "2026-01-01T00:00:00Z".to_string(),
            last_synced_at: None,
            last_error: None,
            folders: Vec::new(),
            files: Vec::new(),
        }
    }

    fn folder_rec(id: &str, parent: Option<&str>, name: &str) -> FolderRecord {
        FolderRecord {
            id: id.to_string(),
            name: name.to_string(),
            parent_id: parent.map(str::to_string),
            sort_order: None,
            color: "blue".to_string(),
            encrypted: true,
            share_enabled: true,
            shared_room_id: "room-1".to_string(),
            last_cid: None,
            last_saved_at: None,
            last_shared_at: None,
            deleted_at: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            field_versions: None,
        }
    }

    fn file_rec(id: &str, folder_id: &str, name: &str, checksum: &str) -> FileRecord {
        FileRecord {
            id: id.to_string(),
            folder_id: folder_id.to_string(),
            sort_order: None,
            name: name.to_string(),
            mime_type: "text/plain".to_string(),
            size: 3,
            data_url: None,
            checksum: checksum.to_string(),
            version: 1,
            starred: false,
            last_cid: Some(format!("{id}-cid")),
            last_share_cid: None,
            deleted_at: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            field_versions: None,
        }
    }

    // -- persistence -----------------------------------------------------

    #[tokio::test]
    async fn persistence_roundtrips_and_survives_a_fresh_read() {
        let dir = TempDir::new("persist");
        let entry = sample_entry("folder-a", &dir.path());

        with_entries_at(&dir.path(), |entries| {
            entries.push(entry.clone());
            Ok(())
        })
        .await
        .expect("write");

        // Fresh read (simulating a daemon restart: no in-memory state,
        // just the file on disk).
        let loaded = read_entries_at(&dir.path()).await.expect("read");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].folder_id, "folder-a");
        assert_eq!(
            loaded[0].folder_key, "super-secret-key",
            "the real key must survive a restart"
        );
        assert_eq!(loaded[0].room_id, "room-1");
    }

    #[tokio::test]
    async fn with_entries_does_not_persist_on_a_failed_closure() {
        let dir = TempDir::new("no-partial-write");
        let entry = sample_entry("folder-a", &dir.path());
        with_entries_at(&dir.path(), |entries| {
            entries.push(entry.clone());
            Ok(())
        })
        .await
        .unwrap();

        let result: Result<()> = with_entries_at(&dir.path(), |entries| {
            entries.push(entry.clone());
            bail!("duplicate");
        })
        .await;
        assert!(result.is_err());

        let loaded = read_entries_at(&dir.path()).await.unwrap();
        assert_eq!(
            loaded.len(),
            1,
            "a failed closure must not persist its partial mutation"
        );
    }

    #[tokio::test]
    async fn missing_table_file_reads_as_empty() {
        let dir = TempDir::new("missing");
        let loaded = read_entries_at(&dir.path()).await.unwrap();
        assert!(loaded.is_empty());
    }

    // -- IPC secrecy -------------------------------------------------------

    #[test]
    fn redact_blanks_the_folder_key_only() {
        let dir = TempDir::new("redact");
        let entry = sample_entry("folder-a", &dir.path());
        let sandbox_root = dir.path().join("unrelated-sandbox-root");
        let redacted = redact(entry.clone(), &sandbox_root);
        assert_eq!(redacted.folder_key, "");
        assert_eq!(
            redacted.folder_id, entry.folder_id,
            "only the key is touched"
        );
        assert_eq!(redacted.room_id, entry.room_id);
    }

    #[test]
    fn redact_computes_sandbox_dir_from_local_dir() {
        let root = TempDir::new("redact-sandbox-root");
        let sandbox_root = root.path();
        let entry = sample_entry("folder-a", &sandbox_root.join("MyFolder"));
        let redacted = redact(entry, &sandbox_root);
        assert_eq!(redacted.sandbox_dir, "MyFolder");
    }

    #[test]
    fn redact_leaves_sandbox_dir_empty_for_a_directory_outside_the_sandbox_root() {
        let root = TempDir::new("redact-outside-root");
        let outside = TempDir::new("redact-outside-dir");
        let entry = sample_entry("folder-a", &outside.path());
        let redacted = redact(entry, &root.path());
        assert_eq!(
            redacted.sandbox_dir, "",
            "legacy / --dir entries have no sandbox_dir"
        );
    }

    // -- sandbox subdirectory naming / collisions --------------------------

    #[test]
    fn sandbox_dir_name_for_uses_the_sanitized_folder_name_when_no_collision() {
        let sandbox_root = Path::new("/data/sandbox");
        let name = sandbox_dir_name_for("My Cool Folder", "folder-a", &[], sandbox_root);
        assert_eq!(name, "My Cool Folder");
    }

    #[test]
    fn sandbox_dir_name_for_disambiguates_on_collision_with_a_different_sync() {
        let sandbox_root = Path::new("/data/sandbox");
        let other = sample_entry("folder-other", &sandbox_root.join("Shared"));
        let name = sandbox_dir_name_for("Shared", "abcdefgh12345", &[other], sandbox_root);
        assert_eq!(
            name, "Shared-abcdefgh",
            "disambiguated with the first 8 chars of the new folder_id"
        );
    }

    #[test]
    fn sandbox_dir_name_for_does_not_collide_with_its_own_prior_entry() {
        // Re-registering the same folder_id (e.g. after a crash mid-registration)
        // must not get a disambiguating suffix just because an entry for the
        // same folder already occupies that directory.
        let sandbox_root = Path::new("/data/sandbox");
        let same = sample_entry("folder-a", &sandbox_root.join("Shared"));
        let name = sandbox_dir_name_for("Shared", "folder-a", &[same], sandbox_root);
        assert_eq!(name, "Shared");
    }

    #[test]
    fn sandbox_dir_name_for_ignores_unrelated_directories() {
        let sandbox_root = Path::new("/data/sandbox");
        let unrelated = sample_entry("folder-other", &sandbox_root.join("Something Else"));
        let name = sandbox_dir_name_for("Shared", "folder-a", &[unrelated], sandbox_root);
        assert_eq!(name, "Shared");
    }

    // -- path jail / target resolution -------------------------------------

    #[test]
    fn local_dir_sandbox_rejects_traversal_and_absolute_paths() {
        let dir = TempDir::new("jail");
        let sandbox = Sandbox::new(dir.path()).unwrap();
        assert!(sandbox.resolve("../escape.txt").is_err());
        assert!(sandbox.resolve("sub/../../escape.txt").is_err());
        let absolute = dir.path().join("nested").join("x.txt");
        assert!(sandbox.resolve(absolute.to_str().unwrap()).is_err());
    }

    #[test]
    fn file_target_rel_drops_the_root_segment_and_keeps_the_rest() {
        let folders: HashMap<String, FolderRecord> = [
            ("root".to_string(), folder_rec("root", None, "Shared")),
            (
                "child".to_string(),
                folder_rec("child", Some("root"), "Sub"),
            ),
        ]
        .into_iter()
        .collect();

        // Root-level file: no folder prefix at all.
        assert_eq!(
            file_target_rel(&folders, "root", "a.txt").as_deref(),
            Some("a.txt")
        );
        // Nested file: keeps the subfolder, drops "Shared".
        assert_eq!(
            file_target_rel(&folders, "child", "b.txt").as_deref(),
            Some("Sub/b.txt")
        );
    }

    #[test]
    fn file_target_rel_sanitizes_traversal_attempts_into_harmless_components() {
        let folders: HashMap<String, FolderRecord> =
            [("root".to_string(), folder_rec("root", None, ".."))]
                .into_iter()
                .collect();

        // A malicious/pathological folder or file name can never smuggle a
        // `..`/`/` *path component* through -- `sanitize_name` replaces
        // every literal separator with `_` before the name ever reaches
        // path construction (so what's left of "../../evil.txt" is the
        // single harmless component ".._.._evil.txt", not a traversal --
        // note it may still contain the two-character substring ".." as
        // plain text, which is fine), and `Sandbox::resolve` (exercised
        // below) is the second, authoritative line of defense.
        let rel = file_target_rel(&folders, "root", "../../evil.txt").expect("resolves");
        assert!(
            std::path::Path::new(&rel)
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_))),
            "every component of the resolved path must be a plain (non-`..`, non-absolute) name: {rel}"
        );

        let dir = TempDir::new("jail-target");
        let sandbox = Sandbox::new(dir.path()).unwrap();
        let target = sandbox.resolve(&rel).expect("resolves under the jail");
        assert!(target.starts_with(dir.path()));
    }

    #[test]
    fn file_target_rel_is_none_for_an_unresolved_folder() {
        let folders: HashMap<String, FolderRecord> = HashMap::new();
        assert_eq!(file_target_rel(&folders, "missing", "a.txt"), None);
    }

    // -- resync diff planning ----------------------------------------------

    fn root_folders() -> HashMap<String, FolderRecord> {
        [("root".to_string(), folder_rec("root", None, "Shared"))]
            .into_iter()
            .collect()
    }

    #[test]
    fn plan_resync_reuses_unchanged_files() {
        let folders = root_folders();
        let bundle_files = vec![file_rec("f1", "root", "a.txt", "sum-1")];
        let previous = vec![SyncedFile {
            file_id: "f1".to_string(),
            folder_id: "root".to_string(),
            rel_path: "a.txt".to_string(),
            checksum: "sum-1".to_string(),
        }];

        let plan = plan_resync(&bundle_files, &folders, &previous);
        assert_eq!(plan.reuse.len(), 1);
        assert!(plan.fetch.is_empty());
        assert!(plan.remove_paths.is_empty());
    }

    #[test]
    fn plan_resync_fetches_new_and_changed_files() {
        let folders = root_folders();
        let bundle_files = vec![
            file_rec("f1", "root", "a.txt", "sum-2"), // changed checksum
            file_rec("f2", "root", "b.txt", "sum-b"), // brand new
        ];
        let previous = vec![SyncedFile {
            file_id: "f1".to_string(),
            folder_id: "root".to_string(),
            rel_path: "a.txt".to_string(),
            checksum: "sum-1".to_string(),
        }];

        let plan = plan_resync(&bundle_files, &folders, &previous);
        assert!(plan.reuse.is_empty());
        assert_eq!(plan.fetch.len(), 2);
        assert!(plan.fetch.iter().any(|f| f.file.id == "f1"));
        assert!(plan.fetch.iter().any(|f| f.file.id == "f2"));
    }

    #[test]
    fn plan_resync_deletes_files_vanished_from_the_bundle() {
        let folders = root_folders();
        let bundle_files = vec![file_rec("f1", "root", "a.txt", "sum-1")];
        let previous = vec![
            SyncedFile {
                file_id: "f1".to_string(),
                folder_id: "root".to_string(),
                rel_path: "a.txt".to_string(),
                checksum: "sum-1".to_string(),
            },
            SyncedFile {
                file_id: "gone".to_string(),
                folder_id: "root".to_string(),
                rel_path: "old.txt".to_string(),
                checksum: "sum-x".to_string(),
            },
        ];

        let plan = plan_resync(&bundle_files, &folders, &previous);
        assert_eq!(plan.reuse.len(), 1);
        assert_eq!(plan.remove_paths, vec!["old.txt".to_string()]);
    }

    #[test]
    fn plan_resync_treats_a_deleted_bundle_entry_like_a_vanished_file() {
        let folders = root_folders();
        let mut deleted = file_rec("f1", "root", "a.txt", "sum-1");
        deleted.deleted_at = Some("2026-02-01T00:00:00Z".to_string());
        let bundle_files = vec![deleted];
        let previous = vec![SyncedFile {
            file_id: "f1".to_string(),
            folder_id: "root".to_string(),
            rel_path: "a.txt".to_string(),
            checksum: "sum-1".to_string(),
        }];

        let plan = plan_resync(&bundle_files, &folders, &previous);
        assert!(plan.reuse.is_empty());
        assert!(plan.fetch.is_empty());
        assert_eq!(plan.remove_paths, vec!["a.txt".to_string()]);
    }

    #[test]
    fn plan_resync_moves_a_file_to_its_new_path_and_cleans_up_the_old_one() {
        let folders: HashMap<String, FolderRecord> = [
            ("root".to_string(), folder_rec("root", None, "Shared")),
            (
                "child".to_string(),
                folder_rec("child", Some("root"), "Sub"),
            ),
        ]
        .into_iter()
        .collect();
        // Same file id, unchanged checksum, but now filed under "child".
        let bundle_files = vec![file_rec("f1", "child", "a.txt", "sum-1")];
        let previous = vec![SyncedFile {
            file_id: "f1".to_string(),
            folder_id: "root".to_string(),
            rel_path: "a.txt".to_string(),
            checksum: "sum-1".to_string(),
        }];

        let plan = plan_resync(&bundle_files, &folders, &previous);
        assert!(
            plan.reuse.is_empty(),
            "the target path changed, so this must be re-materialized"
        );
        assert_eq!(plan.fetch.len(), 1);
        assert_eq!(plan.fetch[0].rel, "Sub/a.txt");
        assert_eq!(
            plan.remove_paths,
            vec!["a.txt".to_string()],
            "the old path must be cleaned up"
        );
    }

    #[test]
    fn plan_resync_preserves_the_local_copy_of_an_unresolvable_bundle_entry() {
        let folders = root_folders();
        // References a folder id that isn't in the map at all.
        let bundle_files = vec![file_rec("f1", "ghost-folder", "a.txt", "sum-1")];
        let previous = vec![SyncedFile {
            file_id: "f1".to_string(),
            folder_id: "root".to_string(),
            rel_path: "a.txt".to_string(),
            checksum: "sum-1".to_string(),
        }];

        let plan = plan_resync(&bundle_files, &folders, &previous);
        assert!(plan.reuse.is_empty());
        assert!(plan.fetch.is_empty());
        assert_eq!(plan.skipped.len(), 1);
        assert!(
            plan.remove_paths.is_empty(),
            "a file the bundle still (nominally) contains must not be deleted just because its \
             folder is currently unresolvable"
        );
    }

    // -- signature/cid gating ------------------------------------------------

    #[test]
    fn resync_needed_gates_on_cid_first() {
        let dir = TempDir::new("gating");
        let mut entry = sample_entry("folder-a", &dir.path());
        entry.last_cid = Some("cid-1".to_string());
        entry.last_folder_signature = Some("sig-1".to_string());

        assert!(
            !resync_needed(&entry, "cid-1", Some("sig-1")),
            "identical state: no resync"
        );
        assert!(
            resync_needed(&entry, "cid-2", Some("sig-1")),
            "cid changed: always resync"
        );
        assert!(
            resync_needed(&entry, "cid-2", None),
            "cid changed even with no signature to compare"
        );
    }

    #[test]
    fn resync_needed_uses_signature_only_when_both_sides_know_one() {
        let dir = TempDir::new("gating-sig");
        let mut entry = sample_entry("folder-a", &dir.path());
        entry.last_cid = Some("cid-1".to_string());

        entry.last_folder_signature = None;
        assert!(
            !resync_needed(&entry, "cid-1", Some("sig-new")),
            "same cid, no local signature to compare against yet: no resync"
        );

        entry.last_folder_signature = Some("sig-old".to_string());
        assert!(
            resync_needed(&entry, "cid-1", Some("sig-new")),
            "same cid but signature diverged"
        );
        assert!(
            !resync_needed(&entry, "cid-1", None),
            "same cid, announce carries no signature: cid alone says nothing changed"
        );
    }
}
