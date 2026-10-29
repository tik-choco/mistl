//! Durable vault block store: `<dir>/<cid>` files, verified on write and read.
use anyhow::{Context, Result, ensure};
use mistlib_core::storage::cid::{MULTICODEC_RAW, cid_codec, verify_cid};
use std::collections::{BTreeMap, HashSet};
use std::io::{ErrorKind, Read};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// The encrypted catalog is the largest kind of vault block.
const MAX_BLOCK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Default)]
struct StoreState {
    files: BTreeMap<String, u64>,
    usage: u64,
    cursor: Option<String>,
}

impl StoreState {
    fn record(&mut self, cid: &str, len: Option<u64>) {
        if let Some(old) = self.files.remove(cid) {
            self.usage = self.usage.saturating_sub(old);
        }
        if let Some(len) = len {
            self.files.insert(cid.to_owned(), len);
            self.usage = self.usage.saturating_add(len);
        }
    }

    fn replace_files(&mut self, files: BTreeMap<String, u64>) {
        self.usage = files
            .values()
            .fold(0u64, |sum, len| sum.saturating_add(*len));
        self.files = files;
    }
}

enum BlockRead {
    Valid(Vec<u8>),
    Missing,
    Corrupt,
    Unavailable,
}

pub struct BlockStore {
    dir: PathBuf,
    state: Mutex<StoreState>,
}

fn valid_cid(cid: &str) -> bool {
    cid_codec(cid) == Some(MULTICODEC_RAW)
}

fn is_write_temp(name: &str) -> bool {
    let Some((cid, suffix)) = name
        .strip_suffix(".tmp")
        .and_then(|stem| stem.rsplit_once('.'))
    else {
        return false;
    };
    valid_cid(cid)
        && suffix.len() == 16
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn scan_files(dir: &Path, clean_temps: bool) -> Result<BTreeMap<String, u64>> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry.with_context(|| format!("reading an entry in {}", dir.display()))?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if valid_cid(name) {
            files.insert(name.to_owned(), entry.metadata()?.len());
        } else if clean_temps && is_write_temp(name) {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(err).context("removing an abandoned vault block write"),
            }
        }
    }
    Ok(files)
}

impl BlockStore {
    /// Opens (creating) the directory and computes current usage.
    pub fn open(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let files = scan_files(&dir, true)?;
        let mut state = StoreState::default();
        state.replace_files(files);
        Ok(Self {
            dir,
            state: Mutex::new(state),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Verify `cid` matches `bytes` (RAW codec), then atomically publish. Idempotent.
    pub fn put(&self, cid: &str, bytes: &[u8]) -> Result<()> {
        ensure!(valid_cid(cid), "invalid RAW block CID");
        ensure!(bytes.len() <= MAX_BLOCK_BYTES, "vault block is too large");
        ensure!(verify_cid(cid, bytes, MULTICODEC_RAW), "block CID mismatch");

        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if matches!(self.read_verified(&mut state, cid), BlockRead::Valid(_)) {
            return Ok(());
        }
        let path = self.dir.join(cid);
        let result = crate::statefile::write_bytes(&path, bytes)
            .with_context(|| format!("writing vault block {cid}"));
        // A directory sync can fail after the rename has already published the file.
        self.refresh_file(&mut state, cid);
        result
    }

    /// Read and verify; a corrupt file is deleted and None returned.
    pub fn get(&self, cid: &str) -> Option<Vec<u8>> {
        if !valid_cid(cid) {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        match self.read_verified(&mut state, cid) {
            BlockRead::Valid(bytes) => Some(bytes),
            BlockRead::Missing | BlockRead::Corrupt | BlockRead::Unavailable => None,
        }
    }

    pub fn has(&self, cid: &str) -> bool {
        if !valid_cid(cid) {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        self.refresh_file(&mut state, cid)
    }

    /// Stored length without reading the block (integrity is left to scrub).
    pub fn size(&self, cid: &str) -> Option<u64> {
        if !valid_cid(cid) {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if !self.refresh_file(&mut state, cid) {
            return None;
        }
        std::fs::symlink_metadata(self.dir.join(cid))
            .ok()
            .map(|metadata| metadata.len())
    }

    pub fn delete(&self, cid: &str) -> Result<()> {
        ensure!(valid_cid(cid), "invalid RAW block CID");
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        self.remove_file(&mut state, cid)?;
        Ok(())
    }

    pub fn list(&self) -> Vec<String> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        match scan_files(&self.dir, false) {
            Ok(files) => state.replace_files(files),
            Err(err) => tracing::warn!(error = %err, "failed to list vault blocks"),
        }
        state.files.keys().cloned().collect()
    }

    /// Total bytes stored.
    pub fn usage(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .usage
    }

    /// Delete every block not in `keep`; returns bytes freed.
    pub fn retain(&self, keep: &HashSet<String>) -> Result<u64> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        state.replace_files(scan_files(&self.dir, false)?);
        let remove: Vec<_> = state
            .files
            .keys()
            .filter(|cid| !keep.contains(*cid))
            .cloned()
            .collect();
        let mut freed = 0u64;
        for cid in remove {
            freed = freed.saturating_add(self.remove_file(&mut state, &cid)?);
        }
        Ok(freed)
    }

    /// Re-hash up to `n` blocks (round-robin); corrupt ones are deleted and returned.
    pub fn scrub_step(&self, n: usize) -> Vec<String> {
        if n == 0 {
            return Vec::new();
        }
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        match scan_files(&self.dir, false) {
            Ok(files) => state.replace_files(files),
            Err(err) => tracing::warn!(error = %err, "failed to scan vault blocks for scrub"),
        }
        let limit = n.min(state.files.len());
        let cids: Vec<_> = match state.cursor.as_ref() {
            Some(cursor) => state
                .files
                .range::<str, _>((Excluded(cursor.as_str()), Unbounded))
                .chain(
                    state
                        .files
                        .range::<str, _>((Unbounded, Included(cursor.as_str()))),
                )
                .take(limit)
                .map(|(cid, _)| cid.clone())
                .collect(),
            None => state.files.keys().take(limit).cloned().collect(),
        };
        let mut corrupt = Vec::new();
        for cid in cids {
            state.cursor = Some(cid.clone());
            if matches!(self.read_verified(&mut state, &cid), BlockRead::Corrupt) {
                corrupt.push(cid);
            }
        }
        corrupt
    }

    // Callers validate the CID before any path is constructed.
    fn refresh_file(&self, state: &mut StoreState, cid: &str) -> bool {
        match std::fs::symlink_metadata(self.dir.join(cid)) {
            Ok(metadata) if metadata.is_file() => {
                state.record(cid, Some(metadata.len()));
                true
            }
            Ok(_) => {
                state.record(cid, None);
                false
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                state.record(cid, None);
                false
            }
            Err(err) => {
                tracing::warn!(%cid, error = %err, "failed to stat vault block");
                false
            }
        }
    }

    fn remove_file(&self, state: &mut StoreState, cid: &str) -> Result<u64> {
        let path = self.dir.join(cid);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => {
                state.record(cid, None);
                return Ok(0);
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                state.record(cid, None);
                return Ok(0);
            }
            Err(err) => return Err(err).with_context(|| format!("stat vault block {cid}")),
        };
        match std::fs::remove_file(&path) {
            Ok(()) => {
                state.record(cid, None);
                Ok(metadata.len())
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                state.record(cid, None);
                Ok(0)
            }
            Err(err) => Err(err).with_context(|| format!("deleting vault block {cid}")),
        }
    }

    fn read_verified(&self, state: &mut StoreState, cid: &str) -> BlockRead {
        if !self.refresh_file(state, cid) {
            return BlockRead::Missing;
        }
        let path = self.dir.join(cid);
        let mut bytes = Vec::new();
        let result = std::fs::File::open(&path).and_then(|file| {
            file.take(MAX_BLOCK_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
        });
        if let Err(err) = result {
            if err.kind() == ErrorKind::NotFound {
                state.record(cid, None);
                return BlockRead::Missing;
            }
            tracing::warn!(%cid, error = %err, "failed to read vault block");
            return BlockRead::Unavailable;
        }
        if bytes.len() > MAX_BLOCK_BYTES || !verify_cid(cid, &bytes, MULTICODEC_RAW) {
            if let Err(err) = self.remove_file(state, cid) {
                tracing::warn!(%cid, error = %err, "failed to delete corrupt vault block");
            }
            return BlockRead::Corrupt;
        }
        state.record(cid, Some(bytes.len() as u64));
        BlockRead::Valid(bytes)
    }
}

/// Free bytes on the volume holding `path`, if the platform query succeeds.
#[cfg(windows)]
pub fn volume_free(path: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let path = std::fs::canonicalize(path).ok()?;
    let dir = if path.is_dir() {
        path.as_path()
    } else {
        path.parent()?
    };
    let mut wide: Vec<u16> = dir.as_os_str().encode_wide().collect();
    if wide.contains(&0) {
        return None;
    }
    if !wide.ends_with(&[b'\\' as u16]) {
        wide.push(b'\\' as u16);
    }
    wide.push(0);
    let mut available = 0u64;
    // SAFETY: the path is NUL-terminated and `available` is a valid output pointer.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some(available)
}

/// Free bytes on the volume holding `path`, if the platform query succeeds.
#[cfg(unix)]
pub fn volume_free(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: the path is NUL-terminated and `stats` is a valid output pointer.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: a successful statvfs initialized the structure.
    let stats = unsafe { stats.assume_init() };
    let bytes = (stats.f_bavail as u128) * (stats.f_frsize as u128);
    Some(bytes.min(u64::MAX as u128) as u64)
}

/// Free bytes on the volume holding `path`, if the platform query succeeds.
#[cfg(not(any(windows, unix)))]
pub fn volume_free(_path: &Path) -> Option<u64> {
    None
}

/// Bytes this node advertises it can still accept:
/// min(capacity - used, volume_free - min_free), clamped at 0.
pub fn advertised_free(capacity: u64, used: u64, volume_free: Option<u64>, min_free: u64) -> u64 {
    let remaining = capacity.saturating_sub(used);
    match volume_free {
        Some(free) => remaining.min(free.saturating_sub(min_free)),
        None => remaining,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, compute_cid};
    use std::sync::Arc;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            loop {
                let path = std::env::temp_dir().join(format!(
                    "mistl-vault-blocks-test-{:016x}",
                    rand::random::<u64>()
                ));
                match std::fs::create_dir(&path) {
                    Ok(()) => return Self(path),
                    Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
                    Err(err) => panic!("creating test directory: {err}"),
                }
            }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if let Ok(entries) = std::fs::read_dir(&self.0) {
                for entry in entries.flatten() {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
            let _ = std::fs::remove_dir(&self.0);
        }
    }

    fn cid(bytes: &[u8]) -> String {
        compute_cid(bytes, MULTICODEC_RAW)
    }

    #[test]
    fn put_get_delete_and_reopen_preserve_usage() {
        let dir = TestDir::new();
        let store = BlockStore::open(dir.0.clone()).unwrap();
        let a = cid(b"hello");
        let b = cid(b"world!");
        assert_eq!(store.dir(), dir.0);
        assert_eq!(store.usage(), 0);
        store.put(&a, b"hello").unwrap();
        store.put(&a, b"hello").unwrap();
        store.put(&b, b"world!").unwrap();
        assert_eq!(store.usage(), 11);
        assert_eq!(store.get(&a).as_deref(), Some(b"hello".as_slice()));
        assert!(store.has(&a));
        assert_eq!(store.list().len(), 2);
        drop(store);

        let store = BlockStore::open(dir.0.clone()).unwrap();
        assert_eq!(store.usage(), 11);
        store.delete(&a).unwrap();
        store.delete(&a).unwrap();
        assert!(!store.has(&a));
        assert_eq!(store.usage(), 6);
        assert_eq!(store.list(), vec![b]);
    }

    #[test]
    fn rejects_invalid_paths_codecs_and_content() {
        let dir = TestDir::new();
        let store = BlockStore::open(dir.0.clone()).unwrap();
        let other_codec = compute_cid(b"hello", MULTICODEC_DAG_CBOR);
        let overlong = format!("b{}", "a".repeat(10_000));
        for bad in [
            "",
            "b",
            "babc",
            "../escape",
            "b/../escape",
            "b\\..\\escape",
            &other_codec,
            &overlong,
        ] {
            assert!(store.put(bad, b"hello").is_err(), "{bad}");
            assert!(store.get(bad).is_none());
            assert!(!store.has(bad));
            assert!(store.delete(bad).is_err());
        }
        assert!(store.put(&cid(b"hello"), b"different").is_err());
        assert_eq!(store.usage(), 0);
        assert!(store.list().is_empty());
    }

    #[test]
    fn corrupt_files_are_deleted_and_can_be_replaced() {
        let dir = TestDir::new();
        let store = BlockStore::open(dir.0.clone()).unwrap();
        let a = cid(b"hello");
        let b = cid(b"other");
        store.put(&a, b"hello").unwrap();
        store.put(&b, b"other").unwrap();
        std::fs::write(dir.0.join(&a), b"tampered and longer").unwrap();
        assert!(store.get(&a).is_none());
        assert!(!store.has(&a));
        assert_eq!(store.usage(), 5);
        store.put(&a, b"hello").unwrap();
        std::fs::write(dir.0.join(&a), b"wrong").unwrap();
        store.put(&a, b"hello").unwrap();
        assert_eq!(store.get(&a).as_deref(), Some(b"hello".as_slice()));
        assert_eq!(store.usage(), 10);
    }

    #[test]
    fn open_scans_blocks_and_removes_only_its_abandoned_temp_files() {
        let dir = TestDir::new();
        let first = cid(b"one");
        let second = cid(b"second");
        let temp = format!("{first}.0123456789abcdef.tmp");
        std::fs::write(dir.0.join(&first), b"one").unwrap();
        std::fs::write(dir.0.join(&second), b"second").unwrap();
        std::fs::write(dir.0.join(&temp), b"unfinished").unwrap();
        std::fs::write(dir.0.join("unrelated.tmp"), b"leave alone").unwrap();
        std::fs::write(dir.0.join("babc"), b"not a CID").unwrap();
        let store = BlockStore::open(dir.0.clone()).unwrap();
        assert_eq!(store.usage(), 9);
        assert_eq!(store.list().len(), 2);
        assert!(!dir.0.join(temp).exists());
        assert!(dir.0.join("unrelated.tmp").exists());
        assert!(dir.0.join("babc").exists());
    }

    #[test]
    fn retain_reports_actual_freed_bytes_and_keeps_requested_blocks() {
        let dir = TestDir::new();
        let store = BlockStore::open(dir.0.clone()).unwrap();
        let a = cid(b"one");
        let b = cid(b"two-two");
        let c = cid(b"");
        store.put(&a, b"one").unwrap();
        store.put(&b, b"two-two").unwrap();
        store.put(&c, b"").unwrap();
        std::fs::write(dir.0.join(&b), b"modified length").unwrap();
        let keep = HashSet::from([a.clone()]);
        assert_eq!(store.retain(&keep).unwrap(), 15);
        assert_eq!(store.usage(), 3);
        assert_eq!(store.list(), vec![a]);
        assert_eq!(store.retain(&keep).unwrap(), 0);
    }

    #[test]
    fn scrub_rotates_and_deletes_only_corrupt_blocks() {
        let dir = TestDir::new();
        let store = BlockStore::open(dir.0.clone()).unwrap();
        for data in [b"one".as_slice(), b"two", b"three", b"four"] {
            store.put(&cid(data), data).unwrap();
        }
        let sorted = store.list();
        std::fs::write(dir.0.join(&sorted[1]), b"wrong").unwrap();
        std::fs::write(dir.0.join(&sorted[3]), b"wrong").unwrap();
        assert!(store.scrub_step(0).is_empty());
        assert!(store.scrub_step(1).is_empty());
        assert_eq!(store.scrub_step(1), vec![sorted[1].clone()]);
        assert!(store.scrub_step(1).is_empty());
        assert_eq!(store.scrub_step(1), vec![sorted[3].clone()]);
        assert!(store.scrub_step(usize::MAX).is_empty());
        assert_eq!(store.list(), vec![sorted[0].clone(), sorted[2].clone()]);
        let expected = std::fs::metadata(dir.0.join(&sorted[0])).unwrap().len()
            + std::fs::metadata(dir.0.join(&sorted[2])).unwrap().len();
        assert_eq!(store.usage(), expected);
    }

    #[test]
    fn bounds_block_reads_and_writes() {
        let dir = TestDir::new();
        let store = BlockStore::open(dir.0.clone()).unwrap();
        let largest = vec![1; MAX_BLOCK_BYTES];
        let largest_cid = cid(&largest);
        store.put(&largest_cid, &largest).unwrap();
        assert_eq!(store.get(&largest_cid), Some(largest));
        store.delete(&largest_cid).unwrap();
        let oversized = vec![42; MAX_BLOCK_BYTES + 1];
        assert!(store.put(&cid(&oversized), &oversized).is_err());
        let small = cid(b"small");
        store.put(&small, b"small").unwrap();
        std::fs::write(dir.0.join(&small), oversized).unwrap();
        assert!(store.get(&small).is_none());
        assert_eq!(store.usage(), 0);
        assert!(!store.has(&small));
    }

    #[test]
    fn concurrent_puts_and_reads_keep_usage_consistent() {
        let dir = TestDir::new();
        let store = Arc::new(BlockStore::open(dir.0.clone()).unwrap());
        let mut threads = Vec::new();
        for worker in 0..4 {
            let store = Arc::clone(&store);
            threads.push(std::thread::spawn(move || {
                for index in 0..8 {
                    let bytes = format!("worker-{worker}-block-{index}").into_bytes();
                    let cid = cid(&bytes);
                    store.put(&cid, &bytes).unwrap();
                    assert_eq!(store.get(&cid), Some(bytes));
                    store.put(&super::tests::cid(b"shared"), b"shared").unwrap();
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let listed = store.list();
        assert_eq!(listed.len(), 33);
        let expected: u64 = listed
            .iter()
            .map(|cid| std::fs::metadata(dir.0.join(cid)).unwrap().len())
            .sum();
        assert_eq!(store.usage(), expected);
        assert_eq!(store.retain(&HashSet::new()).unwrap(), expected);
        assert_eq!(store.usage(), 0);
    }

    #[test]
    fn free_capacity_saturates_and_volume_query_handles_missing_paths() {
        assert_eq!(advertised_free(100, 30, Some(80), 20), 60);
        assert_eq!(advertised_free(100, 30, Some(500), 20), 70);
        assert_eq!(advertised_free(100, 101, Some(500), 20), 0);
        assert_eq!(advertised_free(100, 30, Some(10), 20), 0);
        assert_eq!(advertised_free(100, 30, None, 20), 70);
        let dir = TestDir::new();
        #[cfg(any(windows, unix))]
        assert!(volume_free(&dir.0).is_some());
        assert!(volume_free(&dir.0.join("missing")).is_none());
    }
}
