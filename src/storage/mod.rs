//! Storage: content-addressed local block store built on
//! `mistlib_core::storage::StorageEngine` (CIDv1, sha2-256, 1 MiB chunks,
//! CBOR `FileManifest`), same semantics as tc-storage's `storage_add` /
//! `storage_get`.
//!
//! The block backend is `mistlib`'s (mistlib-native's) `NativeBlockStore`
//! (one file per CID under `config.storage.blocks_dir`, default
//! `<data_dir>/blocks`). Peer resolution is [`resolver::RoomPeerResolver`]:
//! a block missing from the local store is resolved over p2p, using the
//! same QUERY/WANT/HAVE(_CHUNK) wire protocol
//! (`mistlib_core::storage::protocol`) mistlib-native's and mistlib-wasm's
//! own resolvers speak, so this interoperates with browser tabs (tc-news,
//! tc-chat, tc-storage) sharing a room -- see the `resolver` module doc
//! comment for why this reimplements the wire protocol on `crate::net`
//! rather than reusing mistlib-native's built-in (but internally-wired,
//! inaccessible from here) P2P storage. [`Store::get`] resolves over
//! whichever rooms are currently joined via `storage.room_ids`;
//! [`Store::get_remote`] lets a caller pick specific rooms/timeout instead
//! (e.g. a bot pipeline resolving a CID from a room it joined itself,
//! without adding that room to `storage.room_ids`). A local JSON index at
//! `<data_dir>/store-index.json` tracks name/size/stored_at per root CID,
//! used by `store.ls` and to recover the original file name on
//! `store.get`.
//!
//! Other modules depend on the exact signatures of [`Store`] and [`store`];
//! do not change them without updating callers.

mod crypto;
mod domain;
pub mod folder_owner;
mod folder_share;
pub mod folder_sync;
mod index;
mod resolver;
mod sandbox;
mod sharelink;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::Utc;
use mistlib::storage::fs::NativeBlockStore;
use mistlib_core::storage::{SpatialPolicy, StorageEngine};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OnceCell};

use crate::config::StorageConfig;
use crate::daemon::AppState;

pub use index::IndexEntry;
use resolver::RoomPeerResolver;

/// Handle to the content-addressed store.
pub struct Store {
    engine: StorageEngine<NativeBlockStore, RoomPeerResolver>,
    data_dir: PathBuf,
    /// Serializes read-modify-write access to `store-index.json`.
    index_lock: Mutex<()>,
    /// The p2p rooms currently joined for peer block exchange, config-driven
    /// (empty if none configured). Reconciled against live config on every
    /// [`store`] call by [`Store::sync_rooms`], so `config.storage.room_ids`
    /// can change (add, remove, or clear rooms) at any time without a daemon
    /// restart. Rooms joined additively by other flows -- folder syncs,
    /// folder shares, an explicit `store.connect --room` -- are never added
    /// here, so config reconciliation never leaves them (see `sync_rooms`).
    rooms: Mutex<std::collections::BTreeSet<String>>,
}

impl Store {
    /// Open (creating directories as needed) a store rooted at `data_dir`,
    /// with blocks written under `storage_cfg.blocks_dir` (default
    /// `<data_dir>/blocks`) and evicted past `storage_cfg.capacity_bytes`.
    async fn open(storage_cfg: &StorageConfig, data_dir: PathBuf) -> Result<Self> {
        let blocks_dir = storage_cfg
            .blocks_dir
            .clone()
            .unwrap_or_else(|| data_dir.join("blocks"));
        let backend = NativeBlockStore::new(&blocks_dir).await.map_err(|error| {
            anyhow::anyhow!("opening block store at {}: {error}", blocks_dir.display())
        })?;
        // No VRChat position source, so blocks are never spatially tagged
        // and the default (decay-disabled) policy applies.
        let engine = StorageEngine::new(
            backend,
            RoomPeerResolver::new(),
            storage_cfg.capacity_bytes,
            None,
            SpatialPolicy::default(),
        );
        Ok(Self {
            engine,
            data_dir,
            index_lock: Mutex::new(()),
            rooms: Mutex::new(std::collections::BTreeSet::new()),
        })
    }

    /// Reconcile the joined p2p rooms against `desired` (the current value of
    /// `config.storage.room_ids`), leaving rooms no longer desired and
    /// joining newly desired ones. Cheap and a no-op when the set is
    /// unchanged since the last call, so callers can call it on every
    /// command. Blank entries in `desired` are dropped.
    ///
    /// Rooms joined by other flows -- folder syncs, folder shares, an
    /// explicit `store.connect --room` -- are never tracked here, so this
    /// reconciliation never leaves them even if they're not in `desired`.
    async fn sync_rooms(&self, state: &Arc<AppState>, desired: &[String]) -> Result<()> {
        let desired: std::collections::BTreeSet<String> = desired
            .iter()
            .map(|room| room.trim().to_string())
            .filter(|room| !room.is_empty())
            .collect();
        let mut current = self.rooms.lock().await;
        if *current == desired {
            return Ok(());
        }
        // Only drop a room from `current` once `leave_room` actually
        // succeeds -- if it errors, `current` still names the room we (as
        // far as we know) hold, so the next call retries leaving it instead
        // of silently losing track.
        for old in current.clone() {
            if !desired.contains(&old) {
                crate::net::leave_room(&old)
                    .await
                    .with_context(|| format!("storage: leaving room {old:?}"))?;
                current.remove(&old);
            }
        }
        for new_room in &desired {
            if !current.contains(new_room) {
                crate::net::ensure_started(state, new_room.clone())
                    .await
                    .context("storage: starting p2p transport")?;
                current.insert(new_room.clone());
            }
        }
        Ok(())
    }

    /// The directory holding this store's index and default download output
    /// (`<data_dir>`; blocks themselves may live elsewhere per config).
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Store bytes under a name, returning the root CID.
    pub async fn put(&self, name: &str, data: Vec<u8>) -> Result<String> {
        let size = data.len() as u64;
        let cid = self.engine.add(name, &data).await?;
        self.record(&cid, name, size).await?;
        Ok(cid)
    }

    /// Retrieve content by root CID: checks the local block store first,
    /// then -- for any manifest/chunk blocks missing there -- resolves them
    /// over p2p, scoped to whichever rooms this store currently has joined
    /// (`storage.room_ids`, reconciled by [`Self::sync_rooms`]) with a
    /// [`resolver::DEFAULT_REMOTE_TIMEOUT`] budget. Equivalent to
    /// `get_remote(cid, <currently joined storage rooms>,
    /// DEFAULT_REMOTE_TIMEOUT)`; see [`Self::get_remote`] to control either.
    pub async fn get(&self, cid: &str) -> Result<Vec<u8>> {
        let rooms: Vec<String> = self.rooms.lock().await.iter().cloned().collect();
        self.get_remote(cid, &rooms, resolver::DEFAULT_REMOTE_TIMEOUT)
            .await
    }

    /// Like [`Self::get`], but lets the caller pick which rooms to resolve
    /// missing blocks from and how long to wait, independent of
    /// `storage.room_ids`. For a bot pipeline (or any other in-process
    /// caller) reading from a room it joined itself -- e.g. a future
    /// `tc-global-articles` source -- without adding that room to this
    /// store's own configured room set. A local hit is still served
    /// immediately regardless of `rooms`/`timeout` (those only matter on a
    /// cache miss); an empty `rooms` list resolves nothing remotely and
    /// fails fast on a miss rather than waiting out `timeout`.
    pub async fn get_remote(
        &self,
        cid: &str,
        rooms: &[String],
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let scope = resolver::ResolveScope {
            rooms: rooms.to_vec(),
            timeout,
        };
        let data = resolver::with_scope(scope, self.engine.get(cid)).await?;
        Ok(data)
    }

    /// Local-only lookup: reads `cid` from this node's own block store
    /// without triggering peer resolution, even for a manifest root the
    /// local index doesn't know about (this only touches the block store,
    /// not `store-index.json`). Used to answer inbound QUERY/WANT (serving
    /// a request must never trigger our *own* remote resolution -- two
    /// nodes both missing a block and asking each other for it would
    /// otherwise loop) and could equally back a future "do we have this
    /// already" check before kicking off a `get_remote`.
    async fn get_local(&self, cid: &str) -> Result<Option<Vec<u8>>> {
        let data = self.engine.get_block(cid).await?;
        Ok(data)
    }

    /// List everything recorded in the local index (most recently touched
    /// first).
    pub async fn list(&self) -> Result<Vec<IndexEntry>> {
        let _guard = self.index_lock.lock().await;
        let mut entries = index::load(&self.data_dir)?;
        entries.sort_by(|a, b| b.stored_at.cmp(&a.stored_at));
        Ok(entries)
    }

    async fn record(&self, cid: &str, name: &str, size: u64) -> Result<()> {
        let _guard = self.index_lock.lock().await;
        index::upsert(
            &self.data_dir,
            IndexEntry {
                cid: cid.to_string(),
                name: name.to_string(),
                size,
                stored_at: Utc::now(),
            },
        )
    }
}

/// Module-internal, lazily-initialized handle to the daemon's store.
static STORE: OnceCell<Arc<Store>> = OnceCell::const_new();

/// Get (lazily opening on first call) the daemon's store, and reconcile its
/// joined p2p rooms against the current `config.storage.room_ids` (join
/// rooms newly added, leave rooms removed, join all of them the first time
/// any are configured). Since this reconciliation runs on every call,
/// `storage.room_ids` can be changed at any time -- via `config.set` or the
/// dashboard -- and takes effect on the very next `store.*` command, no
/// daemon restart needed.
pub async fn store(state: &Arc<AppState>) -> Result<Arc<Store>> {
    let store = STORE
        .get_or_try_init(|| async {
            let storage_cfg = state.config().storage;
            let data_dir = crate::config::data_dir()?;
            Store::open(&storage_cfg, data_dir).await.map(Arc::new)
        })
        .await?;
    // Idempotent (registers once per process); done after `STORE` is
    // populated so the handler's `super::STORE.get()` local-block lookups
    // (answering inbound QUERY/WANT) can actually find it -- see
    // `resolver::ensure_wire_handler_registered`'s doc comment.
    resolver::ensure_wire_handler_registered();
    store
        .sync_rooms(state, &state.config().storage.room_ids)
        .await?;
    Ok(store.clone())
}

/// Where `store.sandbox.export` writes a file when no explicit `output` is
/// given: under `export_dir` (preserving `sandbox_path`'s subdirectory
/// structure, so a whole synced tree can be extracted file by file without
/// same-named files from different folders colliding), or else
/// `<data_dir>/downloads/<name>`. Pure and offline (doesn't touch the
/// filesystem) so it's unit-testable without a live `Store`/`AppState`; the
/// handler creates whatever parent directory the result implies before
/// writing to it.
fn default_export_path(
    sandbox_path: &str,
    name: &str,
    export_dir: Option<&Path>,
    data_dir: &Path,
) -> PathBuf {
    match export_dir {
        Some(export_dir) => export_dir.join(Path::new(sandbox_path)),
        None => data_dir.join("downloads").join(name),
    }
}

/// Where `store.browse-dirs` opens when no explicit `path` is given: the
/// configured `storage.export_dir` if it currently exists as a directory
/// (the natural starting point when picking a *new* export destination --
/// right next to the old one), else the user's home directory, else the
/// store's own `data_dir` as a last resort that always exists. Pure (no
/// filesystem writes) but does stat the candidates, so it's still exercised
/// through `handle()` rather than the pure-logic unit tests below.
fn resolve_default_browse_dir(export_dir: Option<&Path>, data_dir: &Path) -> PathBuf {
    if let Some(export_dir) = export_dir
        && export_dir.is_dir()
    {
        return export_dir.to_path_buf();
    }
    if let Some(home) = directories::UserDirs::new().map(|u| u.home_dir().to_path_buf()) {
        return home;
    }
    data_dir.to_path_buf()
}

/// One entry in `store.browse-dirs`' `dirs` list: a subdirectory's bare name
/// and its full path.
#[derive(Debug, PartialEq, Eq, Serialize)]
struct BrowseDirEntry {
    name: String,
    path: String,
}

/// One entry in `store.browse-dirs`' `roots` list: a shortcut a picker UI
/// can offer directly (home, well-known user folders, drive roots).
#[derive(Debug, PartialEq, Eq, Serialize)]
struct BrowseRoot {
    label: String,
    path: String,
}

/// Immediate subdirectories of `dir` (not recursive), skipping dotfiles and
/// sorted case-insensitively by name -- the listing behind `store.browse-dirs`.
fn list_subdirs(dir: &Path) -> Result<Vec<BrowseDirEntry>> {
    let mut entries = Vec::new();
    let read_dir = std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))?;
    for entry in read_dir {
        let entry = entry.with_context(|| format!("reading entry in {}", dir.display()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if !is_dir {
            continue;
        }
        entries.push(BrowseDirEntry {
            path: entry.path().to_string_lossy().to_string(),
            name,
        });
    }
    entries.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    Ok(entries)
}

/// Shortcut roots offered by `store.browse-dirs`: home + well-known user
/// folders that exist (from `directories::UserDirs`), plus every existing
/// drive letter on Windows (`A:\` .. `Z:\`, labeled by drive) or `/` on
/// other platforms.
fn browse_roots() -> Vec<BrowseRoot> {
    let mut roots = Vec::new();
    if let Some(user_dirs) = directories::UserDirs::new() {
        roots.push(BrowseRoot {
            label: "Home".to_string(),
            path: user_dirs.home_dir().to_string_lossy().to_string(),
        });
        if let Some(dir) = user_dirs.download_dir() {
            roots.push(BrowseRoot {
                label: "Downloads".to_string(),
                path: dir.to_string_lossy().to_string(),
            });
        }
        if let Some(dir) = user_dirs.desktop_dir() {
            roots.push(BrowseRoot {
                label: "Desktop".to_string(),
                path: dir.to_string_lossy().to_string(),
            });
        }
        if let Some(dir) = user_dirs.document_dir() {
            roots.push(BrowseRoot {
                label: "Documents".to_string(),
                path: dir.to_string_lossy().to_string(),
            });
        }
    }
    if cfg!(windows) {
        for letter in b'A'..=b'Z' {
            let drive = format!("{}:\\", letter as char);
            if Path::new(&drive).is_dir() {
                roots.push(BrowseRoot {
                    label: drive.clone(),
                    path: drive,
                });
            }
        }
    } else {
        roots.push(BrowseRoot {
            label: "/".to_string(),
            path: "/".to_string(),
        });
    }
    roots
}

/// Handle `store.*` IPC commands:
/// - `store.put` `{path}` -> `{cid, name, size}` (reads the file server-side)
/// - `store.get` `{id, output?}` -> writes file, returns `{name, size, output}`
/// - `store.ls` `{}` -> `[{cid, name, size, stored_at}]` (from a local index)
///
/// tc-storage interop (web-app-compatible encrypted file bundles, share
/// links, and a path-jailed content sandbox -- see the `storage-cli` Go tool):
/// - `store.put-file` `{path, passphrase}` or `{sandbox, passphrase}` ->
///   `{cid, name, size, encrypted}` (`sandbox` is a sandbox-relative path,
///   jail-validated, used instead of an absolute `path`; exactly one of the
///   two is required)
/// - `store.get-file` `{cid, passphrase, output?}` -> materializes the file;
///   or `{cid, passphrase, to_sandbox: true}` to write it into the content
///   sandbox instead (jail-validated; adds a `sandbox` field with the
///   sandbox-relative path written). `output` and `to_sandbox` are mutually
///   exclusive.
/// - `store.parse-link` `{url}` -> parsed `tc-share` fields (no network)
/// - `store.fetch-share` `{url, output?}` -> resolves a *file* share against
///   the local store and decrypts it (folder shares need the owner online)
/// - `store.sandbox.import` `{path}` / `store.sandbox.ls` `{}`
/// - `store.sandbox.rm` `{path}` -> `{removed}` (jail-validated delete)
/// - `store.sandbox.export` `{path, output?}` -> `{name, size, output}`
///   (jail-validated read, written out to `output`, default
///   `<storage.export_dir>/<sandbox-relative path>` if `storage.export_dir`
///   is configured -- preserving subdirectories -- else
///   `<data_dir>/downloads/<basename>`)
/// - `store.browse-dirs` `{path?}` -> `{path, parent, dirs, roots,
///   export_dir, default_export_dir}` (server-side directory listing backing
///   the dashboard's `storage.export_dir` folder picker)
/// - `store.connect` `{room?}` -> `{node_id, rooms, room?, peers}` (with no
///   `room` arg, ensures all of `storage.room_ids` are joined; `room` is
///   present only when exactly one room is involved -- see
///   [`wait_for_connected_peers`])
/// - `store.folder-get` `{url}` -> `{folder_name, files, skipped, progress}`
///   (networked folder-share receive flow; see [`folder_share`])
/// - `store.folder-sync` `{url, dir?}` -> `{sync}` (registers immediately,
///   returning a `"connecting"` entry; the access-grant handshake and first
///   fetch run in the background -- see [`folder_sync`]. `dir` is optional
///   -- when omitted, the default, the sync materializes into a managed
///   subdirectory of the content sandbox instead of a user-chosen directory;
///   pass `dir` to keep the old behavior)
/// - `store.folder-sync.ls` `{}` -> `{syncs}`
/// - `store.folder-sync.stop` `{folder_id}` -> `{stopped}`
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    let store = store(state).await?;
    match cmd {
        "store.put" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("missing `path`")?;
            let path = PathBuf::from(path);
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file")
                .to_string();
            let data = tokio::fs::read(&path)
                .await
                .with_context(|| format!("reading {}", path.display()))?;
            let size = data.len() as u64;
            let cid = store.put(&name, data).await?;
            Ok(json!({ "cid": cid, "name": name, "size": size }))
        }

        "store.get" => {
            let id = args
                .get("id")
                .and_then(Value::as_str)
                .context("missing `id`")?
                .to_string();
            let output_arg = args
                .get("output")
                .and_then(Value::as_str)
                .map(PathBuf::from);

            let data = store.get(&id).await?;
            let size = data.len() as u64;
            let name = store
                .list()
                .await?
                .into_iter()
                .find(|entry| entry.cid == id)
                .map(|entry| entry.name)
                .unwrap_or_else(|| id.clone());

            let output_path = match output_arg {
                Some(path) => path,
                None => {
                    let downloads = store.data_dir().join("downloads");
                    tokio::fs::create_dir_all(&downloads)
                        .await
                        .with_context(|| format!("creating {}", downloads.display()))?;
                    downloads.join(&name)
                }
            };
            if let Some(parent) = output_path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .with_context(|| format!("creating {}", parent.display()))?;
                }
            }
            tokio::fs::write(&output_path, &data)
                .await
                .with_context(|| format!("writing {}", output_path.display()))?;

            Ok(json!({
                "name": name,
                "size": size,
                "output": output_path.to_string_lossy(),
            }))
        }

        "store.ls" => {
            let entries = store.list().await?;
            Ok(serde_json::to_value(entries)?)
        }

        // Encrypt `path` (or a sandbox-relative `sandbox` path -- exactly one
        // of the two is required) into a tc-storage-compatible `FileBundle`
        // (AES-256-GCM + PBKDF2, web-app format) and store the encrypted
        // JSON, returning its root CID. Mirrors storage-cli's `put-file`,
        // which takes a sandbox-relative path.
        "store.put-file" => {
            let path_arg = args.get("path").and_then(Value::as_str);
            let sandbox_arg = args.get("sandbox").and_then(Value::as_str);
            let passphrase = args
                .get("passphrase")
                .and_then(Value::as_str)
                .context("missing `passphrase`")?;

            let (name, data) = match (path_arg, sandbox_arg) {
                (Some(_), Some(_)) => bail!("provide exactly one of `path` or `sandbox`, not both"),
                (None, None) => bail!("missing `path` (or `sandbox`)"),
                (Some(path), None) => {
                    let path = PathBuf::from(path);
                    let name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("file")
                        .to_string();
                    let data = tokio::fs::read(&path)
                        .await
                        .with_context(|| format!("reading {}", path.display()))?;
                    (name, data)
                }
                (None, Some(sandbox_path)) => {
                    let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox"))?;
                    let (data, _size) = sandbox.read_file(sandbox_path)?;
                    let name = Path::new(sandbox_path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("file")
                        .to_string();
                    (name, data)
                }
            };
            let size = data.len() as u64;
            let (bytes, storage_name) = build_encrypted_file_bundle(&name, &data, passphrase)?;
            let cid = store.put(&storage_name, bytes).await?;
            Ok(json!({ "cid": cid, "name": name, "size": size, "encrypted": true }))
        }

        // Fetch an encrypted `FileBundle` by CID, decrypt it with the
        // passphrase, and materialize the file -- either at `output` (or the
        // default downloads dir) or, if `to_sandbox` is set, into the
        // content sandbox under the bundle's (sanitized) file name. Mirrors
        // `get-file`; `to_sandbox` has no storage-cli equivalent, it exists
        // so the web UI can pull a fetched file straight into the sandbox
        // for further one-shot operations.
        "store.get-file" => {
            let cid = args
                .get("cid")
                .and_then(Value::as_str)
                .context("missing `cid`")?;
            let passphrase = args
                .get("passphrase")
                .and_then(Value::as_str)
                .context("missing `passphrase`")?;
            let output = args
                .get("output")
                .and_then(Value::as_str)
                .map(PathBuf::from);
            let to_sandbox = args
                .get("to_sandbox")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if to_sandbox && output.is_some() {
                bail!("`output` and `to_sandbox` are mutually exclusive");
            }
            let raw = store.get(cid).await?;
            let payload: crypto::EncryptedPayload = serde_json::from_slice(&raw)
                .context("stored object is not an encrypted file bundle")?;
            let bundle: domain::FileBundle = crypto::decrypt_json(&payload, passphrase)?;

            if to_sandbox {
                let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox"))?;
                let safe_name = folder_share::sanitize_name(&bundle.file.name);
                let sandbox_path = sandbox.resolve(&safe_name)?;
                let mut result =
                    write_file_record(store.as_ref(), &bundle.file, Some(sandbox_path)).await?;
                // Only claim a sandbox path was populated if a file was
                // actually written (`write_file_record` writes nothing and
                // skips the `output` field for a metadata-only bundle with
                // no inline `dataUrl`).
                if result.get("output").is_some()
                    && let Value::Object(ref mut map) = result
                {
                    map.insert("sandbox".to_string(), json!(safe_name));
                }
                Ok(result)
            } else {
                write_file_record(store.as_ref(), &bundle.file, output).await
            }
        }

        // Parse a `tc-share` link and report its fields (no network).
        "store.parse-link" => {
            let url = args
                .get("url")
                .and_then(Value::as_str)
                .context("missing `url`")?;
            let linked = sharelink::parse_share_link(url)?;
            Ok(json!({
                "type": linked.share.type_,
                "room_id": linked.share.room_id,
                "cid": linked.share.cid,
                "folder_id": linked.share.folder_id,
                "folder_name": linked.share.folder_name,
                "file_id": linked.share.file_id,
                "file_name": linked.share.file_name,
                "owner_node_id": linked.share.owner_node_id,
                "access_grant_mode": linked.share.access_grant_mode,
                "folder_key_hash": linked.share.folder_key_hash,
                "clock": linked.share.clock,
                "sender": linked.share.sender_profile.as_ref().map(|p| p.name.clone()),
                "has_key": linked.key.is_some(),
            }))
        }

        // Resolve a *file* share link (cid + inline key) against the local
        // store and decrypt it. Folder shares carry no inline key -- they
        // require a live access-grant handshake with the owner, which the
        // local-only store can't do yet, so those are rejected with a clear
        // message (parse-link still works on them).
        "store.fetch-share" => {
            let url = args
                .get("url")
                .and_then(Value::as_str)
                .context("missing `url`")?;
            let output = args
                .get("output")
                .and_then(Value::as_str)
                .map(PathBuf::from);
            let linked = sharelink::parse_share_link(url)?;
            match (linked.share.cid.as_deref(), linked.key.as_deref()) {
                (Some(cid), Some(key)) if !cid.is_empty() && !key.is_empty() => {
                    let raw = store.get(cid).await?;
                    let payload: crypto::EncryptedPayload = serde_json::from_slice(&raw)
                        .context("shared object is not an encrypted file bundle")?;
                    let bundle: domain::FileBundle = crypto::decrypt_json(&payload, key)?;
                    write_file_record(store.as_ref(), &bundle.file, output).await
                }
                _ => bail!(
                    "this is a folder share; folder shares need a live connection to the \
                     owner and are not supported by the local store yet (parse-link still works)"
                ),
            }
        }

        // Import an external file into the content sandbox
        // (`<data_dir>/sandbox`), path-jailed. Mirrors `sandbox-import`.
        "store.sandbox.import" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("missing `path`")?;
            let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox"))?;
            let imported = sandbox.import_file(path)?;
            Ok(json!({ "imported": imported }))
        }

        // List sandbox contents. Mirrors `sandbox-list`.
        "store.sandbox.ls" => {
            let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox"))?;
            let entries = sandbox.list()?;
            Ok(json!({ "entries": entries }))
        }

        // Remove a sandbox-relative file (or directory, recursively),
        // path-jailed. Mirrors `sandbox-rm`. Unlike `Sandbox::remove` on its
        // own (idempotent, matching Go's `os.RemoveAll`), a nonexistent path
        // is an error here so the caller gets clear feedback that nothing
        // was removed.
        "store.sandbox.rm" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("missing `path`")?;
            let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox"))?;
            let resolved = sandbox.resolve(path)?;
            if !resolved.exists() {
                bail!("no such file in sandbox: {path}");
            }
            let removed = resolved
                .strip_prefix(sandbox.root())
                .unwrap_or(&resolved)
                .to_string_lossy()
                .replace('\\', "/");
            sandbox.remove(path)?;
            Ok(json!({ "removed": removed }))
        }

        // Copy a sandbox-relative file out to `output` (default: under
        // `storage.export_dir` if configured, preserving the sandbox-relative
        // subdirectory structure; else `<data_dir>/downloads/<basename>`),
        // path-jailed read. Mirrors `sandbox-export`.
        "store.sandbox.export" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("missing `path`")?;
            let output_arg = args
                .get("output")
                .and_then(Value::as_str)
                .map(PathBuf::from);
            let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox"))?;
            let (data, _size) = sandbox.read_file(path)?;
            let size = data.len() as u64;
            let name = Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file")
                .to_string();

            // `storage.export_dir` is read live (like `storage.room_ids` in
            // `sync_rooms` above) so a `config.set` takes effect on the very
            // next export call, no daemon restart needed.
            let output_path = match output_arg {
                Some(path) => path,
                None => {
                    let export_dir = state.config().storage.export_dir;
                    if export_dir.is_none() {
                        let downloads = store.data_dir().join("downloads");
                        tokio::fs::create_dir_all(&downloads)
                            .await
                            .with_context(|| format!("creating {}", downloads.display()))?;
                    }
                    default_export_path(path, &name, export_dir.as_deref(), store.data_dir())
                }
            };
            if let Some(parent) = output_path.parent()
                && !parent.as_os_str().is_empty()
            {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            tokio::fs::write(&output_path, &data)
                .await
                .with_context(|| format!("writing {}", output_path.display()))?;

            Ok(json!({
                "name": name,
                "size": size,
                "output": output_path.to_string_lossy(),
            }))
        }

        // Server-side directory browser backing the dashboard's
        // `storage.export_dir` folder picker -- the daemon may run on a
        // different machine/account than the browser, so the user can't
        // just type a path, they need to click through directories the
        // daemon itself can see. Loopback-token-gated IPC like every other
        // `store.*` command; lists directory *names* only, never file
        // contents or file entries, which keeps it at the same trust level
        // as `store.put`/`store.sandbox.import` (both already read
        // server-side paths named by the caller).
        "store.browse-dirs" => {
            let path_arg = args.get("path").and_then(Value::as_str);
            let export_dir = state.config().storage.export_dir;
            let default_export_dir = store.data_dir().join("downloads");

            let (start_dir, dirs) = match path_arg {
                Some(path) => {
                    let path = PathBuf::from(path);
                    if !path.is_dir() {
                        bail!("not a directory: {}", path.display());
                    }
                    // Explicit request: a listing failure (e.g. permissions)
                    // is a real error, surfaced to the caller.
                    let dirs = list_subdirs(&path)?;
                    (path, dirs)
                }
                None => {
                    let start_dir =
                        resolve_default_browse_dir(export_dir.as_deref(), store.data_dir());
                    // Implicit start dir: degrade to an empty listing rather
                    // than failing outright -- the picker must still open
                    // even if the resolved default happens to be unreadable.
                    let dirs = list_subdirs(&start_dir).unwrap_or_default();
                    (start_dir, dirs)
                }
            };
            let parent = start_dir.parent().map(|p| p.to_string_lossy().to_string());
            let roots = browse_roots();

            Ok(json!({
                "path": start_dir.to_string_lossy(),
                "parent": parent,
                "dirs": dirs,
                "roots": roots,
                "export_dir": export_dir.map(|p| p.to_string_lossy().to_string()),
                "default_export_dir": default_export_dir.to_string_lossy(),
            }))
        }

        // Report this node's id, the storage room(s), and currently
        // connected peers (`store(state)` above already joined
        // `storage.room_ids` via `sync_rooms`). Mirrors `connect`. An
        // explicit `room` arg joins that one room instead, additively (the
        // store can hold several rooms at once, one per share/sync). With
        // no `room` arg, every configured `storage.room_ids` entry is
        // ensured -- normally already true via `store(state)` above, but
        // repeated here too so `store.connect` alone is self-sufficient.
        // `rooms` (sorted) always lists the room(s) this call touched;
        // `room` is additionally set when exactly one room is involved
        // (the explicit-arg case, or a single configured room), keeping the
        // legacy single-room response shape tc-storage-cli depends on.
        // `peers` is process-wide, not per-room -- `wait_for_connected_peers`
        // has no room-scoped view (see its doc comment).
        "store.connect" => {
            let identity = crate::identity::current(state).await?;
            let rooms: std::collections::BTreeSet<String> = match args
                .get("room")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
            {
                Some(room) => {
                    crate::net::ensure_started(state, room.to_string())
                        .await
                        .context("storage: joining room")?;
                    std::iter::once(room.to_string()).collect()
                }
                None => {
                    let configured = state.config().storage.room_ids;
                    if configured.is_empty() {
                        bail!(
                            "`store connect` requires storage.room_ids to be configured \
                             (see `mistl config set storage.room_ids <room>`) or an explicit --room"
                        );
                    }
                    for room in &configured {
                        crate::net::ensure_started(state, room.clone())
                            .await
                            .context("storage: joining room")?;
                    }
                    configured.into_iter().collect()
                }
            };
            let peers = wait_for_connected_peers(Duration::from_secs(20)).await;
            let rooms_list: Vec<String> = rooms.iter().cloned().collect();
            let mut response = json!({
                "node_id": identity.node_id(),
                "rooms": rooms_list,
                "peers": peers,
            });
            if let [only_room] = rooms_list.as_slice()
                && let Value::Object(ref mut map) = response
            {
                map.insert("room".to_string(), json!(only_room));
            }
            Ok(response)
        }

        // Networked folder-share receive flow: request access from the
        // owner, wait for their grant, and download the folder into the
        // sandbox. Mirrors `folder-get`; see `folder_share`'s module doc for
        // interop caveats and why `progress` is batched rather than
        // streamed.
        "store.folder-get" => {
            let url = args
                .get("url")
                .and_then(Value::as_str)
                .context("missing `url`")?;
            let mut progress_lines = Vec::new();
            let mut progress = |line: &str| progress_lines.push(line.to_string());
            let result =
                folder_share::fetch_folder_share(url, store.as_ref(), state, &mut progress).await?;
            Ok(json!({
                "folder_name": result.folder_name,
                "files": result.files,
                "skipped": result.skipped,
                "progress": progress_lines,
            }))
        }

        // Continuous requester-side sync: paste a folder-share link once,
        // keep a local directory mirrored from then on. Registers and
        // returns immediately with a `"connecting"` entry; the access-grant
        // handshake and first full fetch run in the background and are
        // retried automatically -- see `folder_sync`'s module doc.
        "store.folder-sync" => {
            let url = args
                .get("url")
                .and_then(Value::as_str)
                .context("missing `url`")?;
            let dir = args.get("dir").and_then(Value::as_str);
            let entry = folder_sync::register_sync(url, dir, &store, state).await?;
            Ok(json!({ "sync": entry }))
        }

        "store.folder-sync.ls" => {
            let syncs = folder_sync::list_syncs(store.as_ref()).await?;
            Ok(json!({ "syncs": syncs }))
        }

        "store.folder-sync.stop" => {
            let folder_id = args
                .get("folder_id")
                .and_then(Value::as_str)
                .context("missing `folder_id`")?;
            let stopped = folder_sync::stop_sync(folder_id, store.as_ref(), state).await?;
            Ok(json!({ "stopped": stopped }))
        }

        // Owner role: publish a local directory as a tc-storage shared
        // folder and serve grants/announcements for it. See `folder_owner`.
        "store.folder-share" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("missing `path`")?;
            let passphrase = args
                .get("passphrase")
                .and_then(Value::as_str)
                .context("missing `passphrase`")?;
            let name = args.get("name").and_then(Value::as_str);
            let room = args.get("room").and_then(Value::as_str);
            let mut progress_lines = Vec::new();
            let mut progress = |line: &str| progress_lines.push(line.to_string());
            let outcome = folder_owner::share_folder(
                path,
                passphrase,
                name,
                room,
                store.as_ref(),
                state,
                &mut progress,
            )
            .await?;
            Ok(json!({
                "share_url": outcome.share_url,
                "folder_id": outcome.folder_id,
                "room_id": outcome.room_id,
                "files_published": outcome.files_published,
                "progress": progress_lines,
            }))
        }

        "store.folder-share.ls" => {
            let shares = folder_owner::list_shared(store.as_ref()).await?;
            Ok(json!({ "shares": shares }))
        }

        "store.folder-share.stop" => {
            let folder_id = args
                .get("folder_id")
                .and_then(Value::as_str)
                .context("missing `folder_id`")?;
            let stopped = folder_owner::unshare(folder_id, store.as_ref(), state).await?;
            Ok(json!({ "stopped": stopped }))
        }

        _ => bail!("unknown store command: {cmd}"),
    }
}

/// Poll for peers in a "connected" state (per `net::peer_connection_state`)
/// until at least one is found or `timeout` elapses, then return whatever
/// was found (possibly empty). Backs `store.connect`; mirrors tc-storage-cli
/// `Runtime.waitForPeer`'s polling loop, though it targets *any* connected
/// peer rather than one specific node id (`store connect` has no target,
/// unlike `folder_share::wait_for_peer`, which waits for a specific owner).
async fn wait_for_connected_peers(timeout: Duration) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut peers = Vec::new();
        for node in crate::net::connected_nodes().await {
            if crate::net::peer_connection_state(&node).await == "connected" {
                peers.push(node);
            }
        }
        if !peers.is_empty() || tokio::time::Instant::now() >= deadline {
            return peers;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Lowercase-hex encode a SHA-256 digest of `data` (matches Go's
/// `hex.EncodeToString(sha256.Sum256(...))` used for file checksums / ids).
fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Best-effort MIME guess from a file name extension (a small subset of Go's
/// `mime.TypeByExtension` + `http.DetectContentType`; unknown -> octet-stream).
/// Only affects the `mimeType`/`dataUrl` recorded inside the encrypted bundle.
fn guess_mime(name: &str) -> &'static str {
    let ext = Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "txt" | "text" | "log" | "md" => "text/plain",
        "json" => "application/json",
        "html" | "htm" => "text/html",
        "csv" => "text/csv",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}

/// Build the encrypted `FileBundle` JSON bytes for `name`/`data`, mirroring
/// tc-storage's storage-cli `PutFile`: a single-file `FileBundle` (with the
/// content inlined as a base64 `dataUrl`), encrypted with `passphrase`.
/// Returns `(encrypted_json_bytes, storage_object_name)`.
fn build_encrypted_file_bundle(
    name: &str,
    data: &[u8],
    passphrase: &str,
) -> Result<(Vec<u8>, String)> {
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let checksum = sha256_hex(data);
    // Go uses hex(sum[:8]) == the first 16 hex chars of the full checksum.
    let file_id = format!("file-{}", &checksum[..16]);
    let folder_id = "folder-cli-sandbox".to_string();
    let sort_order = Utc::now().timestamp_millis() as f64;
    let mime = guess_mime(name).to_string();
    let data_url = format!("data:{mime};base64,{}", BASE64_STANDARD.encode(data));

    let folder = domain::FolderRecord {
        id: folder_id.clone(),
        name: "CLI Sandbox".to_string(),
        parent_id: None,
        sort_order: Some(sort_order),
        color: "slate".to_string(),
        encrypted: true,
        share_enabled: false,
        shared_room_id: String::new(),
        last_cid: None,
        last_saved_at: None,
        last_shared_at: None,
        deleted_at: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        field_versions: None,
    };
    let file = domain::FileRecord {
        id: file_id.clone(),
        folder_id,
        sort_order: Some(sort_order),
        name: name.to_string(),
        mime_type: mime,
        size: data.len() as i64,
        data_url: Some(data_url),
        checksum,
        version: 1,
        starred: false,
        last_cid: None,
        last_share_cid: None,
        deleted_at: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        field_versions: None,
    };
    let bundle = domain::FileBundle {
        version: 1,
        exported_at: now,
        origin_node: String::new(),
        folder,
        file,
    };

    let encrypted = crypto::encrypt_json(&bundle, passphrase)?;
    let bytes = serde_json::to_vec(&encrypted).context("serializing encrypted bundle")?;
    Ok((bytes, format!("{file_id}.tc-file.enc.json")))
}

/// Decode a `FileRecord.dataUrl` (`data:<mime>;base64,<b64>`) back into the
/// original file bytes.
fn decode_data_url(data_url: &str) -> Result<Vec<u8>> {
    let comma = data_url
        .find(',')
        .context("dataUrl is missing its payload")?;
    let meta = &data_url[..comma];
    if !meta.contains(";base64") {
        bail!("unsupported dataUrl encoding (expected base64)");
    }
    BASE64_STANDARD
        .decode(&data_url.as_bytes()[comma + 1..])
        .context("decoding dataUrl base64")
}

/// Materialize a decrypted `FileRecord` to disk (or, if it carries no inline
/// `dataUrl`, return just its metadata). Verifies the SHA-256 checksum when
/// content is present. Shared by `store.get-file` and `store.fetch-share`.
async fn write_file_record(
    store: &Store,
    file: &domain::FileRecord,
    output: Option<PathBuf>,
) -> Result<Value> {
    let Some(data_url) = file.data_url.as_deref() else {
        return Ok(json!({
            "name": file.name,
            "size": file.size,
            "checksum": file.checksum,
            "note": "bundle carries no inline content (dataUrl); metadata only",
        }));
    };
    let data = decode_data_url(data_url)?;
    let checksum_ok = sha256_hex(&data) == file.checksum;

    let output_path = match output {
        Some(path) => path,
        None => {
            let downloads = store.data_dir().join("downloads");
            tokio::fs::create_dir_all(&downloads)
                .await
                .with_context(|| format!("creating {}", downloads.display()))?;
            downloads.join(&file.name)
        }
    };
    if let Some(parent) = output_path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    tokio::fs::write(&output_path, &data)
        .await
        .with_context(|| format!("writing {}", output_path.display()))?;

    Ok(json!({
        "name": file.name,
        "size": data.len() as u64,
        "checksum": file.checksum,
        "checksum_ok": checksum_ok,
        "output": output_path.to_string_lossy(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal self-cleaning temp directory (the `tempfile` crate is only a
    /// transitive dependency here, not a direct one mistl can `use`; see the
    /// deviation note in the storage module report).
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
                "mistl-storage-test-{label}-{}-{nanos}-{n}",
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

    fn test_config() -> StorageConfig {
        StorageConfig {
            blocks_dir: None,
            capacity_bytes: 10 * 1024 * 1024 * 1024,
            room_ids: Vec::new(),
            export_dir: None,
        }
    }

    async fn temp_store(label: &str) -> (Store, TempDir) {
        let dir = TempDir::new(label);
        let store = Store::open(&test_config(), dir.path())
            .await
            .expect("open store");
        (store, dir)
    }

    #[tokio::test]
    async fn put_get_roundtrip_small() {
        let (store, _dir) = temp_store("small").await;
        let data = b"hello, mistl storage!".to_vec();
        let cid = store.put("hello.txt", data.clone()).await.unwrap();
        assert!(cid.starts_with('b'), "CIDv1 base32 strings start with 'b'");
        let fetched = store.get(&cid).await.unwrap();
        assert_eq!(fetched, data);
    }

    #[tokio::test]
    async fn put_get_roundtrip_multi_chunk() {
        let (store, _dir) = temp_store("multichunk").await;
        // > 1 MiB (CHUNK_SIZE) so the engine must split it into several
        // chunks and reassemble them on get.
        let data: Vec<u8> = (0..(2 * 1024 * 1024 + 42))
            .map(|i| (i % 251) as u8)
            .collect();
        let cid = store.put("big.bin", data.clone()).await.unwrap();
        let fetched = store.get(&cid).await.unwrap();
        assert_eq!(fetched, data);
    }

    #[tokio::test]
    async fn get_of_unknown_cid_fails() {
        let (store, _dir) = temp_store("missing").await;
        let result = store.get("bunknowncidthatwasneverstored").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cid_matches_manifest_hash_expectations() {
        use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, MULTICODEC_RAW};
        use mistlib_core::storage::{FileManifest, compute_cid};

        let (store, _dir) = temp_store("cid").await;
        let data = b"deterministic content".to_vec();
        let cid = store.put("det.txt", data.clone()).await.unwrap();

        // The returned CID must be the CIDv1/dag-cbor hash of the
        // `FileManifest` (root), not of the raw bytes -- exactly mirroring
        // what `StorageEngine::add` builds internally.
        let chunk_cid = compute_cid(&data, MULTICODEC_RAW);
        let manifest = FileManifest {
            name: "det.txt".to_string(),
            size: data.len() as u64,
            chunks: vec![chunk_cid],
        };
        let manifest_bytes = serde_cbor::to_vec(&manifest).unwrap();
        let expected_root = compute_cid(&manifest_bytes, MULTICODEC_DAG_CBOR);

        assert_eq!(cid, expected_root);
    }

    #[tokio::test]
    async fn index_updates_on_put_and_dedupes_by_cid() {
        let (store, _dir) = temp_store("index").await;
        let data = b"index me".to_vec();
        let cid = store.put("first-name.txt", data.clone()).await.unwrap();

        let entries = store.list().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cid, cid);
        assert_eq!(entries[0].name, "first-name.txt");
        assert_eq!(entries[0].size, data.len() as u64);
        let first_stored_at = entries[0].stored_at;

        // Re-storing the exact same name+bytes is idempotent -- the root CID
        // is a hash of the `FileManifest` (name, size, chunk CIDs), so an
        // unchanged re-put reproduces the same CID -- and must update the
        // existing row (refreshed timestamp) rather than duplicate it.
        let cid_again = store.put("first-name.txt", data.clone()).await.unwrap();
        assert_eq!(cid, cid_again);

        let entries = store.list().await.unwrap();
        assert_eq!(entries.len(), 1, "dedupe by cid: no duplicate row");
        assert!(entries[0].stored_at >= first_stored_at);

        // Distinct content (different bytes, or a different name) hashes to
        // a different root CID and adds a second row.
        store
            .put("other.bin", b"other bytes".to_vec())
            .await
            .unwrap();
        let entries = store.list().await.unwrap();
        assert_eq!(entries.len(), 2);

        // The index file itself is real JSON on disk at the documented path.
        let index_path = store.data_dir().join("store-index.json");
        assert!(index_path.is_file());
    }

    #[tokio::test]
    async fn ls_reflects_persisted_index_across_store_instances() {
        let dir = TempDir::new("persist");
        let cid = {
            let store = Store::open(&test_config(), dir.path()).await.unwrap();
            store.put("persisted.txt", b"abc".to_vec()).await.unwrap()
        };

        // A fresh `Store` opened on the same data dir must see the same
        // index (it's read from disk, not kept only in memory).
        let store2 = Store::open(&test_config(), dir.path()).await.unwrap();
        let entries = store2.list().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cid, cid);
        assert_eq!(entries[0].name, "persisted.txt");
    }

    // -- sandbox-backed `store.sandbox.*` / `put-file` / `get-file`
    // extensions -- these exercise the same sandbox and bundle-building
    // primitives the `handle()` match arms use, rather than `handle()`
    // itself, since that needs a full `AppState` (daemon config, identity,
    // p2p transport) that isn't available offline in a unit test.

    #[test]
    fn sandbox_rm_removes_file_and_rejects_traversal() {
        let root = TempDir::new("sandbox-rm");
        let outside = TempDir::new("sandbox-rm-src");
        let sandbox = sandbox::Sandbox::new(root.path()).unwrap();

        let source = outside.path().join("gone.txt");
        std::fs::write(&source, b"bye").unwrap();
        sandbox.import_file(source.to_str().unwrap()).unwrap();
        assert_eq!(sandbox.list().unwrap(), vec!["gone.txt"]);

        // Mirrors `store.sandbox.rm`'s existence check ahead of the actual
        // removal, so a missing path is reported as an error rather than
        // silently succeeding (`Sandbox::remove` alone is idempotent).
        let resolved = sandbox.resolve("gone.txt").unwrap();
        assert!(resolved.exists());
        sandbox.remove("gone.txt").unwrap();
        assert!(sandbox.list().unwrap().is_empty());
        assert!(!sandbox.resolve("gone.txt").unwrap().exists());

        // Traversal is rejected by `resolve` before any removal is
        // attempted.
        let err = sandbox.resolve("../evil.txt").unwrap_err();
        assert!(sandbox::is_outside_sandbox(&err));
    }

    // -- `store.sandbox.export` default destination ------------------------

    #[test]
    fn default_export_path_resolves_each_configured_and_unconfigured_case() {
        struct Case {
            name: &'static str,
            sandbox_relative: &'static str,
            file_name: &'static str,
            export_dir: Option<&'static str>,
            data_dir: &'static str,
            expected: &'static str,
        }

        let cases = [
            Case {
                name: "falls back to downloads dir when unconfigured",
                sandbox_relative: "MyFolder/sub/a.txt",
                file_name: "a.txt",
                export_dir: None,
                data_dir: "/data",
                expected: "/data/downloads/a.txt",
            },
            Case {
                name: "preserves subdirectories under export dir",
                sandbox_relative: "MyFolder/sub/a.txt",
                file_name: "a.txt",
                export_dir: Some("/export"),
                data_dir: "/data",
                expected: "/export/MyFolder/sub/a.txt",
            },
            Case {
                name: "handles a root-level sandbox file",
                sandbox_relative: "a.txt",
                file_name: "a.txt",
                export_dir: Some("/export"),
                data_dir: "/data",
                expected: "/export/a.txt",
            },
        ];

        for case in cases {
            let export_dir = case.export_dir.map(Path::new);
            let data_dir = Path::new(case.data_dir);
            let path =
                default_export_path(case.sandbox_relative, case.file_name, export_dir, data_dir);
            assert_eq!(
                path,
                Path::new(case.expected),
                "case `{}`: expected {:?}, got {:?}",
                case.name,
                case.expected,
                path
            );
        }
    }

    #[test]
    fn sandbox_export_reads_back_the_exact_bytes() {
        let root = TempDir::new("sandbox-export");
        let outside = TempDir::new("sandbox-export-src");
        let sandbox = sandbox::Sandbox::new(root.path()).unwrap();

        let source = outside.path().join("data.bin");
        std::fs::write(&source, b"exported bytes").unwrap();
        sandbox.import_file(source.to_str().unwrap()).unwrap();

        // Mirrors `store.sandbox.export`'s jail-validated read; the
        // subsequent write to an arbitrary `output` path is plain
        // `tokio::fs::write`, already covered by `store.get`'s tests.
        let (data, size) = sandbox.read_file("data.bin").unwrap();
        assert_eq!(data, b"exported bytes");
        assert_eq!(size, 14);
    }

    #[tokio::test]
    async fn put_file_from_sandbox_source_encrypts_and_roundtrips() {
        let (store, _dir) = temp_store("put-file-sandbox").await;
        let outside = TempDir::new("put-file-sandbox-src");
        let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox")).unwrap();

        let source = outside.path().join("note.txt");
        std::fs::write(&source, b"from the sandbox").unwrap();
        sandbox.import_file(source.to_str().unwrap()).unwrap();

        // Mirrors `store.put-file`'s `sandbox` arg: jail-validated read,
        // basename of the sandbox-relative path as the bundle's file name.
        let (data, _size) = sandbox.read_file("note.txt").unwrap();
        let name = Path::new("note.txt")
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap();
        let (bytes, storage_name) = build_encrypted_file_bundle(name, &data, "hunter2").unwrap();
        assert!(storage_name.ends_with(".tc-file.enc.json"));
        let cid = store.put(&storage_name, bytes).await.unwrap();

        let raw = store.get(&cid).await.unwrap();
        let payload: crypto::EncryptedPayload = serde_json::from_slice(&raw).unwrap();
        let bundle: domain::FileBundle = crypto::decrypt_json(&payload, "hunter2").unwrap();
        assert_eq!(bundle.file.name, "note.txt");
        let decoded = decode_data_url(bundle.file.data_url.as_deref().unwrap()).unwrap();
        assert_eq!(decoded, b"from the sandbox");
    }

    #[tokio::test]
    async fn get_file_to_sandbox_round_trips_into_the_sandbox() {
        let (store, _dir) = temp_store("get-file-to-sandbox").await;
        let (bytes, storage_name) =
            build_encrypted_file_bundle("report.txt", b"sandbox bound content", "s3cret").unwrap();
        let cid = store.put(&storage_name, bytes).await.unwrap();

        let raw = store.get(&cid).await.unwrap();
        let payload: crypto::EncryptedPayload = serde_json::from_slice(&raw).unwrap();
        let bundle: domain::FileBundle = crypto::decrypt_json(&payload, "s3cret").unwrap();

        // Mirrors `store.get-file`'s `to_sandbox` path: sanitize the
        // bundle's file name, resolve it through the jail, and materialize
        // there via the same `write_file_record` the plain path uses.
        let sandbox = sandbox::Sandbox::new(store.data_dir().join("sandbox")).unwrap();
        let safe_name = folder_share::sanitize_name(&bundle.file.name);
        let sandbox_path = sandbox.resolve(&safe_name).unwrap();
        let result = write_file_record(&store, &bundle.file, Some(sandbox_path.clone()))
            .await
            .unwrap();
        assert_eq!(
            result.get("output").and_then(Value::as_str).unwrap(),
            sandbox_path.to_string_lossy()
        );

        let listed = sandbox.list().unwrap();
        assert_eq!(listed, vec![safe_name.clone()]);
        let (data, _size) = sandbox.read_file(&safe_name).unwrap();
        assert_eq!(data, b"sandbox bound content");
    }

    // -- `store.browse-dirs` -------------------------------------------

    #[test]
    fn list_subdirs_skips_dotfiles_and_files_and_sorts_case_insensitively() {
        let root = TempDir::new("browse-dirs-list");
        let root_path = root.path();
        std::fs::create_dir_all(root_path.join("Charlie")).unwrap();
        std::fs::create_dir_all(root_path.join("alpha")).unwrap();
        std::fs::create_dir_all(root_path.join("Bravo")).unwrap();
        std::fs::create_dir_all(root_path.join(".hidden")).unwrap();
        std::fs::write(root_path.join("not-a-dir.txt"), b"file").unwrap();

        let entries = list_subdirs(&root_path).unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "Bravo", "Charlie"]);
        // Each entry's `path` is the full joined path, not just the name.
        assert_eq!(entries[0].path, root_path.join("alpha").to_string_lossy());
    }

    #[test]
    fn list_subdirs_of_a_missing_directory_errors() {
        let missing = std::env::temp_dir().join("mistl-storage-test-browse-dirs-missing-xyz");
        assert!(list_subdirs(&missing).is_err());
    }

    #[test]
    fn resolve_default_browse_dir_prefers_export_dir_then_falls_back() {
        // Case 1: an existing export dir always wins.
        {
            let export = TempDir::new("browse-dirs-export");
            let data_dir = TempDir::new("browse-dirs-data");
            let export_path = export.path();
            let data_dir_path = data_dir.path();
            let resolved = resolve_default_browse_dir(Some(&export_path), &data_dir_path);
            assert_eq!(
                resolved, export_path,
                "case `prefers an existing export dir`: expected the export dir itself"
            );
        }

        // Case 2: a configured but nonexistent export dir is skipped.
        {
            let data_dir = TempDir::new("browse-dirs-data-fallback");
            let data_dir_path = data_dir.path();
            let missing_export = data_dir_path.join("does-not-exist");
            let resolved = resolve_default_browse_dir(Some(&missing_export), &data_dir_path);
            // Falls through to the home dir (if resolvable) or else
            // `data_dir`; either way it must not be the nonexistent export
            // dir.
            assert_ne!(
                resolved, missing_export,
                "case `falls back when export dir does not exist`: must not return the missing export dir"
            );
        }

        // Case 3: with no export dir at all, the function still returns
        // *some* existing-in-principle directory -- when a home dir is
        // resolvable (true on this dev machine) that wins over `data_dir`,
        // but the result is never empty and is always an absolute-looking
        // path.
        {
            let data_dir = TempDir::new("browse-dirs-data-nohome");
            let data_dir_path = data_dir.path();
            let resolved = resolve_default_browse_dir(None, &data_dir_path);
            assert!(
                !resolved.as_os_str().is_empty(),
                "case `falls back to data dir when unconfigured and no home`: result must not be empty"
            );
        }
    }
}
