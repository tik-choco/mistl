//! Immutable encrypted generations, durable before any consensus publication.
use crate::config::EscrowConfig;
use crate::escrow::protocol::{Error, Head, hex_string};
use crate::escrow::repository::{MAX_MANIFEST, validate_blob, validate_manifest};
use anyhow::{Context, Result, ensure};
use mistlib_core::storage::cid::{MULTICODEC_RAW, verify_cid};
use mistlib_core::storage::{CHUNK_SIZE, FileManifest};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAX_ENTRIES: usize = 196608;
const MAX_RECORD: usize = 16 * 1024;

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    head: Head,
    rooms: Vec<String>,
}

struct Entry {
    record: Record,
    size: u64,
}

#[derive(Clone)]
struct Block {
    path: PathBuf,
    offset: u64,
    len: usize,
}

#[derive(Default)]
struct Index {
    entries: HashMap<String, Entry>,
    blocks: HashMap<(String, String), Block>,
    bytes: u64,
    write_failed: bool,
    pins: HashMap<String, usize>,
}

pub(super) struct Archive {
    root: PathBuf,
    cfg: EscrowConfig,
    index: Arc<Mutex<Index>>,
}

pub(super) struct Lease {
    index: Arc<Mutex<Index>>,
    hashes: Vec<String>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut index = self.index.lock().unwrap();
        for hash in &self.hashes {
            if let Some(count) = index.pins.get_mut(hash) {
                *count -= 1;
                if *count == 0 {
                    index.pins.remove(hash);
                }
            }
        }
    }
}

impl Archive {
    pub fn open(root: PathBuf, cfg: EscrowConfig) -> Result<Self> {
        cfg.validate()?;
        crate::statefile::create_private_dir(&root)?;
        sync_dir(root.parent().context("archive parent")?)?;
        let archive = Self {
            root,
            cfg,
            index: Arc::new(Mutex::new(Index::default())),
        };
        let mut index = archive.index.lock().unwrap();
        let mut scanned = 0;
        for item in std::fs::read_dir(&archive.root)? {
            let item = item?;
            let hash = item.file_name().to_string_lossy().into_owned();
            if hash.strip_prefix(".gc-").is_some_and(|h| hex_string(h, 64))
                && item.file_type()?.is_dir()
            {
                std::fs::remove_dir_all(item.path())?;
                continue;
            }
            if !hex_string(&hash, 64) || !item.file_type()?.is_dir() {
                continue;
            }
            scanned += 1;
            ensure!(
                scanned <= MAX_ENTRIES,
                "escrow archive: too many generations"
            );
            let path = item.path();
            match std::fs::symlink_metadata(path.join("record.json")) {
                Ok(metadata) => ensure!(metadata.is_file(), "escrow archive: invalid record file"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::remove_dir_all(&path)?;
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
            let metadata = read_bounded(&path.join("record.json"), MAX_RECORD)?;
            let record: Record = serde_json::from_slice(&metadata)?;
            ensure!(
                record.head.hash() == hash,
                "escrow archive: generation mismatch"
            );
            ensure!(
                !record.rooms.is_empty() && record.rooms.len() <= 64,
                "escrow archive: invalid rooms"
            );
            let mut rooms = HashSet::new();
            for room in &record.rooms {
                ensure!(
                    crate::escrow::valid_room(room) && rooms.insert(room),
                    "escrow archive: invalid room"
                );
            }
            record
                .head
                .validate(1024 * 1024 * 1024)
                .map_err(protocol_error)?;
            record.head.verify().map_err(protocol_error)?;
            let blob_size = std::fs::metadata(path.join("blob.bin"))?.len();
            let manifest_size = std::fs::metadata(path.join("manifest.bin"))?.len();
            let retained_size = blob_size
                .saturating_add(manifest_size)
                .saturating_add(metadata.len() as u64);
            ensure!(
                index.bytes.saturating_add(retained_size) <= archive.cfg.max_total_bytes,
                "escrow archive: retained history exceeds capacity"
            );
            let blob = read_bounded(&path.join("blob.bin"), record.head.size as usize)?;
            let bytes = read_bounded(&path.join("manifest.bin"), MAX_MANIFEST)?;
            let manifest = validate(&record.head, &blob, &bytes).map_err(protocol_error)?;
            let size = blob.len() as u64 + bytes.len() as u64 + metadata.len() as u64;
            ensure!(
                index.bytes.saturating_add(size) <= archive.cfg.max_total_bytes,
                "escrow archive: retained history exceeds capacity"
            );
            cleanup_temps(&path)?;
            archive.index_blocks(&mut index, &hash, &record, &manifest, bytes.len());
            index.bytes += size;
            index.entries.insert(hash, Entry { record, size });
        }
        sync_dir(&archive.root)?;
        drop(index);
        Ok(archive)
    }

    pub fn stage(&self, room: &str, head: &Head, blob: &[u8], bytes: &[u8]) -> Result<(), Error> {
        if !crate::escrow::valid_room(room) {
            return Err(Error::new("bad_request"));
        }
        head.validate(self.cfg.max_blob_bytes)?;
        head.verify()?;
        let manifest = validate(head, blob, bytes)?;
        let hash = head.hash();
        let mut index = self.index.lock().unwrap();
        if index.write_failed {
            return Err(Error::new("fetch_failed"));
        }
        let existing = index.entries.get(&hash);
        if existing.is_some_and(|e| e.record.rooms.iter().any(|r| r == room)) {
            return Ok(());
        }
        if existing.is_none() && index.entries.len() >= MAX_ENTRIES {
            return Err(Error::new("quota"));
        }
        let old_size = existing.map_or(0, |e| e.size);
        let mut rooms = existing.map_or_else(Vec::new, |e| e.record.rooms.clone());
        if rooms.len() >= 64 {
            return Err(Error::new("quota"));
        }
        rooms.push(room.into());
        rooms.sort();
        let record = Record {
            head: head.clone(),
            rooms,
        };
        let metadata = serde_json::to_vec(&record).map_err(|_| Error::new("bad_request"))?;
        if metadata.len() > MAX_RECORD {
            return Err(Error::new("too_large"));
        }
        let size = blob.len() as u64 + bytes.len() as u64 + metadata.len() as u64;
        let total = index.bytes.saturating_sub(old_size).saturating_add(size);
        if total > self.cfg.max_total_bytes {
            return Err(Error::new("quota"));
        }
        let path = self.root.join(&hash);
        let write = (|| -> Result<()> {
            if existing.is_none() {
                crate::statefile::write_private(&path.join("blob.bin"), blob)?;
                crate::statefile::write_private(&path.join("manifest.bin"), bytes)?;
                sync_dir(&self.root)?;
            }
            crate::statefile::write_private(&path.join("record.json"), &metadata)?;
            Ok(())
        })();
        if let Err(error) = write {
            index.write_failed = true;
            tracing::warn!(%error, "escrow: archive persistence failed; staging disabled until restart");
            return Err(Error::new("fetch_failed"));
        }
        self.index_blocks(&mut index, &hash, &record, &manifest, bytes.len());
        index.bytes = total;
        index.entries.insert(hash, Entry { record, size });
        Ok(())
    }

    pub fn has(&self, head: &Head) -> bool {
        let index = self.index.lock().unwrap();
        !index.write_failed
            && index
                .entries
                .get(&head.hash())
                .is_some_and(|e| e.record.head == *head)
    }

    #[cfg(test)]
    pub fn load(&self, head: &Head) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let mut index = self.index.lock().unwrap();
        let hash = head.hash();
        if index.write_failed
            || !index
                .entries
                .get(&hash)
                .is_some_and(|e| e.record.head == *head)
        {
            return Err(Error::new("fetch_failed"));
        }
        let path = self.root.join(hash);
        let read = (|| -> Result<(Vec<u8>, Vec<u8>)> {
            let blob = read_bounded(&path.join("blob.bin"), head.size as usize)?;
            let manifest = read_bounded(&path.join("manifest.bin"), MAX_MANIFEST)?;
            validate(head, &blob, &manifest).map_err(protocol_error)?;
            Ok((blob, manifest))
        })();
        read.map_err(|error| {
            index.write_failed = true;
            tracing::warn!(%error, "escrow: archive read failed; staging disabled until restart");
            Error::new("fetch_failed")
        })
    }

    pub fn pin_heads(&self, heads: &[Head]) -> Result<Lease> {
        let mut index = self.index.lock().unwrap();
        ensure!(!index.write_failed, "escrow archive unavailable");
        let hashes: Vec<String> = heads
            .iter()
            .map(Head::hash)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        ensure!(
            heads.iter().all(|head| index
                .entries
                .get(&head.hash())
                .is_some_and(|entry| entry.record.head == *head)),
            "missing snapshot generation"
        );
        for hash in &hashes {
            *index.pins.entry(hash.clone()).or_default() += 1;
        }
        Ok(Lease {
            index: self.index.clone(),
            hashes,
        })
    }

    pub fn collect(&self, retained: &HashSet<String>) -> Result<usize> {
        let mut index = self.index.lock().unwrap();
        ensure!(!index.write_failed, "escrow archive unavailable");
        let obsolete: Vec<_> = index
            .entries
            .keys()
            .filter(|hash| !retained.contains(*hash) && !index.pins.contains_key(*hash))
            .cloned()
            .collect();
        let result = (|| -> Result<()> {
            for hash in &obsolete {
                let retired = self.root.join(format!(".gc-{hash}"));
                std::fs::rename(self.root.join(hash), &retired)?;
                sync_dir(&self.root)?;
                std::fs::remove_dir_all(&retired)?;
                let removed = index
                    .entries
                    .remove(hash)
                    .expect("indexed obsolete generation");
                index.bytes = index.bytes.saturating_sub(removed.size);
            }
            if !obsolete.is_empty() {
                index.blocks.clear();
                let records: Vec<_> = index
                    .entries
                    .iter()
                    .map(|(hash, entry)| (hash.clone(), entry.record.clone()))
                    .collect();
                for (hash, record) in records {
                    let bytes =
                        read_bounded(&self.root.join(&hash).join("manifest.bin"), MAX_MANIFEST)?;
                    let manifest =
                        validate_manifest(&record.head, &bytes).map_err(protocol_error)?;
                    self.index_blocks(&mut index, &hash, &record, &manifest, bytes.len());
                }
                sync_dir(&self.root)?;
            }
            Ok(())
        })();
        if result.is_err() {
            index.write_failed = true;
        }
        result?;
        Ok(obsolete.len())
    }

    pub fn total_bytes(&self) -> u64 {
        self.index.lock().unwrap().bytes
    }

    pub fn block(&self, room: &str, cid: &str) -> Option<Vec<u8>> {
        let mut index = self.index.lock().unwrap();
        let block = index.blocks.get(&(room.into(), cid.into()))?.clone();
        let read = (|| -> Result<Vec<u8>> {
            let mut file = std::fs::File::open(&block.path)?;
            file.seek(SeekFrom::Start(block.offset))?;
            let mut bytes = vec![0; block.len];
            file.read_exact(&mut bytes)?;
            Ok(bytes)
        })();
        read.map_err(|error| {
            index.write_failed = true;
            tracing::warn!(%error, "escrow: archive block read failed");
        })
        .ok()
    }

    fn index_blocks(
        &self,
        index: &mut Index,
        hash: &str,
        record: &Record,
        manifest: &FileManifest,
        manifest_len: usize,
    ) {
        let path = self.root.join(hash);
        for room in &record.rooms {
            index.blocks.insert(
                (room.clone(), record.head.cid.clone()),
                Block {
                    path: path.join("manifest.bin"),
                    offset: 0,
                    len: manifest_len,
                },
            );
            for (i, cid) in manifest.chunks.iter().enumerate() {
                let offset = i as u64 * CHUNK_SIZE as u64;
                index.blocks.insert(
                    (room.clone(), cid.clone()),
                    Block {
                        path: path.join("blob.bin"),
                        offset,
                        len: (manifest.size - offset).min(CHUNK_SIZE as u64) as usize,
                    },
                );
            }
        }
    }
}

fn validate(head: &Head, blob: &[u8], bytes: &[u8]) -> Result<FileManifest, Error> {
    validate_blob(head, blob)?;
    let manifest = validate_manifest(head, bytes)?;
    for (cid, chunk) in manifest.chunks.iter().zip(blob.chunks(CHUNK_SIZE)) {
        if !verify_cid(cid, chunk, MULTICODEC_RAW) {
            return Err(Error::new("hash_mismatch"));
        }
    }
    Ok(manifest)
}

fn protocol_error(error: Error) -> anyhow::Error {
    anyhow::anyhow!(error.code)
}

fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.len() <= max as u64,
        "escrow archive: oversized file"
    );
    let mut bytes = Vec::new();
    file.take(max as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= max, "escrow archive: oversized file");
    Ok(bytes)
}

fn cleanup_temps(path: &Path) -> Result<()> {
    for item in std::fs::read_dir(path)? {
        let item = item?;
        let name = item.file_name().to_string_lossy().into_owned();
        let Some((stem, nonce)) = name.strip_suffix(".tmp").and_then(|n| n.rsplit_once('.')) else {
            continue;
        };
        if matches!(stem, "record" | "blob" | "manifest")
            && hex_string(nonce, 16)
            && item.file_type()?.is_file()
        {
            std::fs::remove_file(item.path())?;
        }
    }
    sync_dir(path)
}

fn sync_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use ed25519_dalek::{Signer, SigningKey};
    use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, compute_cid};

    pub(super) fn scratch() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mistl-escrow-archive-{:016x}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    pub(super) fn signed(head: &mut Head) {
        let key = SigningKey::from_bytes(&[7; 32]);
        head.writer = URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes());
        head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
    }

    pub(super) fn fixture(blob: &[u8], seq: u64) -> (Head, Vec<u8>) {
        let manifest = FileManifest {
            name: "tc-escrow.bin".into(),
            size: blob.len() as u64,
            chunks: blob
                .chunks(CHUNK_SIZE)
                .map(|c| compute_cid(c, MULTICODEC_RAW))
                .collect(),
        };
        let bytes = serde_cbor::to_vec(&manifest).unwrap();
        let mut head = Head {
            v: 1,
            kind: "tc-escrow-head".into(),
            slot: "ab".repeat(16),
            seq,
            parent: (seq > 1).then(|| "cd".repeat(32)),
            cid: compute_cid(&bytes, MULTICODEC_DAG_CBOR),
            size: blob.len() as u64,
            sha256: crate::escrow::protocol::digest(blob),
            comp: "none".into(),
            schema: "test/1".into(),
            writer: String::new(),
            at: 1,
            sig: String::new(),
        };
        signed(&mut head);
        (head, bytes)
    }

    #[test]
    fn archive_restart_retains_late_head_and_original_transfer_blocks() {
        let root = scratch();
        let blob = vec![9; CHUNK_SIZE + 17];
        let (head, manifest) = fixture(&blob, 9);
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        archive.stage("room-a", &head, &blob, &manifest).unwrap();
        let first_size = archive.total_bytes();
        archive.stage("room-a", &head, &blob, &manifest).unwrap();
        assert_eq!(archive.total_bytes(), first_size);
        archive.stage("room-b", &head, &blob, &manifest).unwrap();
        assert!(archive.total_bytes() > first_size);
        drop(archive);
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        assert!(archive.has(&head));
        assert_eq!(
            archive.load(&head).unwrap(),
            (blob.clone(), manifest.clone())
        );
        for room in ["room-a", "room-b"] {
            assert_eq!(archive.block(room, &head.cid), Some(manifest.clone()));
            let parsed: FileManifest = serde_cbor::from_slice(&manifest).unwrap();
            for (cid, chunk) in parsed.chunks.iter().zip(blob.chunks(CHUNK_SIZE)) {
                assert_eq!(archive.block(room, cid).unwrap(), chunk);
            }
        }
        assert!(archive.block("other-room", &head.cid).is_none());
        drop(archive);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_preserves_extended_original_manifest_and_retained_large_blob() {
        let root = scratch();
        let blob = b"original ciphertext";
        let (mut head, manifest) = fixture(blob, 1);
        let mut value: serde_cbor::Value = serde_cbor::from_slice(&manifest).unwrap();
        if let serde_cbor::Value::Map(fields) = &mut value {
            fields.insert(
                serde_cbor::Value::Text("future".into()),
                serde_cbor::Value::Text("opaque".into()),
            );
        }
        let original = serde_cbor::to_vec(&value).unwrap();
        head.cid = compute_cid(&original, MULTICODEC_DAG_CBOR);
        signed(&mut head);
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        archive.stage("room", &head, blob, &original).unwrap();
        drop(archive);
        let cfg = EscrowConfig {
            max_blob_bytes: 1,
            ..Default::default()
        };
        let archive = Archive::open(root.clone(), cfg).unwrap();
        assert_eq!(archive.block("room", &head.cid), Some(original.clone()));
        assert_eq!(
            archive.load(&head).unwrap(),
            (blob.to_vec(), original.clone())
        );
        assert_eq!(
            archive
                .stage("room", &head, blob, &original)
                .unwrap_err()
                .code,
            "too_large"
        );
        drop(archive);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_rejects_invalid_content_signature_and_chunk_cid() {
        let root = scratch();
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        let (head, manifest) = fixture(b"ciphertext", 1);
        assert_eq!(
            archive
                .stage("room", &head, b"bad", &manifest)
                .unwrap_err()
                .code,
            "hash_mismatch"
        );
        let mut forged = head.clone();
        forged.sha256 = "00".repeat(32);
        assert_eq!(
            archive
                .stage("room", &forged, b"ciphertext", &manifest)
                .unwrap_err()
                .code,
            "bad_sig"
        );
        let mut parsed: FileManifest = serde_cbor::from_slice(&manifest).unwrap();
        parsed.chunks[0] = compute_cid(b"wrong data", MULTICODEC_RAW);
        let bad_manifest = serde_cbor::to_vec(&parsed).unwrap();
        let mut wrong_cid = head.clone();
        wrong_cid.cid = compute_cid(&bad_manifest, MULTICODEC_DAG_CBOR);
        signed(&mut wrong_cid);
        assert_eq!(
            archive
                .stage("room", &wrong_cid, b"ciphertext", &bad_manifest)
                .unwrap_err()
                .code,
            "hash_mismatch"
        );
        assert_eq!(archive.total_bytes(), 0);
        archive
            .stage("room", &head, b"ciphertext", &manifest)
            .unwrap();
        drop(archive);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_quota_includes_metadata_and_all_immutable_history() {
        let root = scratch();
        let (first, first_manifest) = fixture(b"first", 1);
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        archive
            .stage("room", &first, b"first", &first_manifest)
            .unwrap();
        let size = archive.total_bytes();
        assert!(size > first.size + first_manifest.len() as u64);
        drop(archive);
        let cfg = EscrowConfig {
            max_total_bytes: size,
            ..Default::default()
        };
        let archive = Archive::open(root.clone(), cfg.clone()).unwrap();
        archive
            .stage("room", &first, b"first", &first_manifest)
            .unwrap();
        let (second, second_manifest) = fixture(b"second", 2);
        assert_eq!(
            archive
                .stage("room", &second, b"second", &second_manifest)
                .unwrap_err()
                .code,
            "quota"
        );
        assert_eq!(archive.total_bytes(), size);
        assert!(archive.has(&first));
        assert!(!archive.has(&second));
        drop(archive);
        let too_small = EscrowConfig {
            max_total_bytes: size - 1,
            ..cfg
        };
        assert!(Archive::open(root.clone(), too_small).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_restart_removes_unpublished_generation_and_temp_files() {
        let root = scratch();
        let (head, manifest) = fixture(b"saved", 1);
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        archive.stage("room", &head, b"saved", &manifest).unwrap();
        drop(archive);
        let orphan = root.join("aa".repeat(32));
        std::fs::create_dir_all(&orphan).unwrap();
        std::fs::write(orphan.join("blob.bin"), b"unpublished").unwrap();
        let temp = root.join(head.hash()).join("record.0123456789abcdef.tmp");
        std::fs::write(&temp, b"partial").unwrap();
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        assert!(archive.has(&head));
        assert!(!orphan.exists());
        assert!(!temp.exists());
        drop(archive);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_missing_retained_blob_fails_closed_and_restart_rejects_corruption() {
        let root = scratch();
        let (head, manifest) = fixture(b"saved", 1);
        let archive = Archive::open(root.clone(), EscrowConfig::default()).unwrap();
        archive.stage("room", &head, b"saved", &manifest).unwrap();
        std::fs::remove_file(root.join(head.hash()).join("blob.bin")).unwrap();
        assert_eq!(archive.load(&head).unwrap_err().code, "fetch_failed");
        let (next, next_manifest) = fixture(b"next", 2);
        assert_eq!(
            archive
                .stage("room", &next, b"next", &next_manifest)
                .unwrap_err()
                .code,
            "fetch_failed"
        );
        assert!(!archive.has(&head));
        drop(archive);
        assert!(Archive::open(root.clone(), EscrowConfig::default()).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
#[path = "archive_storage_tests.rs"]
mod production_tests;
