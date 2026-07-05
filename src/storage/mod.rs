//! Storage: content-addressed local block store built on
//! `mistlib_core::storage::StorageEngine` (CIDv1, sha2-256, 1 MiB chunks,
//! CBOR `FileManifest`), same semantics as tc-storage's `storage_add` /
//! `storage_get`.
//!
//! The block backend is `mistlib`'s (mistlib-native's) `NativeBlockStore`
//! (one file per CID under `config.storage.blocks_dir`, default
//! `<data_dir>/blocks`). Peer resolution is a no-op ([`resolver::NoopResolver`]):
//! this daemon is a purely local store for now, p2p block exchange comes
//! later. A local JSON index at `<data_dir>/store-index.json` tracks
//! name/size/stored_at per root CID, used by `store.ls` and to recover the
//! original file name on `store.get`.
//!
//! Other modules depend on the exact signatures of [`Store`] and [`store`];
//! do not change them without updating callers.

mod index;
mod resolver;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use mistlib::storage::fs::NativeBlockStore;
use mistlib_core::storage::StorageEngine;
use serde_json::{Value, json};
use tokio::sync::{Mutex, OnceCell};

use crate::config::StorageConfig;
use crate::daemon::AppState;

pub use index::IndexEntry;
use resolver::NoopResolver;

/// Handle to the content-addressed store.
pub struct Store {
    engine: StorageEngine<NativeBlockStore, NoopResolver>,
    data_dir: PathBuf,
    /// Serializes read-modify-write access to `store-index.json`.
    index_lock: Mutex<()>,
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
            anyhow::anyhow!(
                "opening block store at {}: {error}",
                blocks_dir.display()
            )
        })?;
        let engine = StorageEngine::new(backend, NoopResolver, storage_cfg.capacity_bytes);
        Ok(Self {
            engine,
            data_dir,
            index_lock: Mutex::new(()),
        })
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

    /// Retrieve content by root CID.
    pub async fn get(&self, cid: &str) -> Result<Vec<u8>> {
        let data = self.engine.get(cid).await?;
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

/// Get (lazily opening on first call) the daemon's store.
pub async fn store(state: &AppState) -> Result<Arc<Store>> {
    let store = STORE
        .get_or_try_init(|| async {
            let data_dir = crate::config::data_dir()?;
            Store::open(&state.config.storage, data_dir)
                .await
                .map(Arc::new)
        })
        .await?;
    Ok(store.clone())
}

/// Handle `store.*` IPC commands:
/// - `store.put` `{path}` -> `{cid, name, size}` (reads the file server-side)
/// - `store.get` `{id, output?}` -> writes file, returns `{name, size, output}`
/// - `store.ls` `{}` -> `[{cid, name, size, stored_at}]` (from a local index)
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

        _ => bail!("unknown store command: {cmd}"),
    }
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
        let data: Vec<u8> = (0..(2 * 1024 * 1024 + 42)).map(|i| (i % 251) as u8).collect();
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
}
