//! Authoritative durable copies, independent of both mistlib stores and LRU eviction.
use super::protocol::{Delete, Error, Head, check_cas, digest, hex_string};
use crate::config::EscrowConfig;
use anyhow::{Context, Result, ensure};
use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, MULTICODEC_RAW, verify_cid};
use mistlib_core::storage::{CHUNK_SIZE, FileManifest};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const MAX_MANIFEST: usize = 128 * 1024;

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    room: String,
    current: Head,
    previous: Option<Head>,
}

#[derive(Clone)]
struct Block {
    path: PathBuf,
    offset: u64,
    len: usize,
}

#[derive(Default)]
struct Index {
    records: HashMap<String, Record>,
    blocks: HashMap<(String, String), Block>,
    bytes: u64,
    reserved: u64,
    pending_slots: HashMap<String, String>,
    manifests: HashMap<(String, String), (FileManifest, usize)>,
    write_failed: bool,
}

pub struct Repository {
    root: PathBuf,
    cfg: EscrowConfig,
    index: Mutex<Index>,
    #[cfg(test)]
    fail_after_publish: std::sync::atomic::AtomicBool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ImportOutcome {
    Imported,
    Unchanged,
    Skipped(&'static str),
}

fn key(room: &str, slot: &str) -> String {
    digest(format!("{room}\n{slot}").as_bytes())
}

pub fn validate_blob(head: &Head, blob: &[u8]) -> Result<(), Error> {
    if blob.len() as u64 != head.size || digest(blob) != head.sha256 {
        return Err(Error::new("hash_mismatch"));
    }
    Ok(())
}

#[derive(Deserialize)]
struct BoundedManifest {
    name: String,
    size: u64,
    #[serde(deserialize_with = "bounded_chunks")]
    chunks: Vec<String>,
}

fn bounded_chunks<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    struct Chunks;
    impl<'de> serde::de::Visitor<'de> for Chunks {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("at most 1024 chunk CIDs")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            if seq.size_hint().is_some_and(|n| n > 1024) {
                return Err(serde::de::Error::custom("too many chunk CIDs"));
            }
            let mut chunks = Vec::new();
            while let Some(cid) = seq.next_element::<String>()? {
                if chunks.len() >= 1024 || !mistlib_core::storage::is_valid_cid(&cid) {
                    return Err(serde::de::Error::custom("invalid chunk CIDs"));
                }
                chunks.push(cid);
            }
            Ok(chunks)
        }
    }
    deserializer.deserialize_seq(Chunks)
}

pub fn validate_manifest(head: &Head, bytes: &[u8]) -> Result<FileManifest, Error> {
    if bytes.len() > manifest_limit(head.size) || !verify_cid(&head.cid, bytes, MULTICODEC_DAG_CBOR)
    {
        return Err(Error::new("hash_mismatch"));
    }
    let manifest: BoundedManifest =
        serde_cbor::from_slice(bytes).map_err(|_| Error::new("fetch_failed"))?;
    // Reject malicious manifests before allocating a blob or requesting chunks.
    if manifest.name != "tc-escrow.bin"
        || manifest.size != head.size
        || manifest.chunks.len() as u64 != head.size.div_ceil(CHUNK_SIZE as u64)
        || manifest
            .chunks
            .iter()
            .any(|c| !mistlib_core::storage::is_valid_cid(c))
    {
        return Err(Error::new("hash_mismatch"));
    }
    Ok(FileManifest {
        name: manifest.name,
        size: manifest.size,
        chunks: manifest.chunks,
    })
}

impl Repository {
    pub fn open(root: PathBuf, cfg: EscrowConfig) -> Result<Self> {
        cfg.validate()?;
        crate::statefile::create_private_dir(&root.join("heads"))?;
        crate::statefile::create_private_dir(&root.join("blobs"))?;
        sync_dir(&root)?;
        sync_dir(root.parent().context("escrow parent")?)?;
        let repo = Self {
            root,
            cfg,
            index: Mutex::new(Index::default()),
            #[cfg(test)]
            fail_after_publish: std::sync::atomic::AtomicBool::new(false),
        };
        let mut index = repo.index.lock().unwrap();
        for entry in std::fs::read_dir(repo.root.join("heads"))? {
            let entry = entry?;
            if entry.path().extension().is_none_or(|s| s != "json") {
                continue;
            }
            ensure!(
                index.records.len() < 65536,
                "escrow: too many persisted slots"
            );
            ensure!(
                entry.metadata()?.len() <= 32 * 1024,
                "escrow: oversized state file"
            );
            let record: Record = serde_json::from_slice(&std::fs::read(entry.path())?)?;
            ensure!(
                super::valid_room(&record.room),
                "escrow: invalid persisted room"
            );
            let k = key(&record.room, &record.current.slot);
            ensure!(
                entry.file_name().to_string_lossy() == format!("{k}.json"),
                "escrow: invalid state filename"
            );
            for head in std::iter::once(&record.current).chain(record.previous.iter()) {
                head.validate(1024 * 1024 * 1024)
                    .map_err(|e| anyhow::anyhow!(e.code))?;
                head.verify().map_err(|e| anyhow::anyhow!(e.code))?;
                ensure!(
                    head.slot == record.current.slot && head.writer == record.current.writer,
                    "escrow: invalid backup"
                );
                let dir = repo.generation(&k, head);
                let blob = read_bounded(&dir.join("blob.bin"), head.size as usize)?;
                validate_blob(head, &blob).map_err(|e| anyhow::anyhow!(e.code))?;
                let bytes = read_bounded(&dir.join("manifest.bin"), MAX_MANIFEST)?;
                let manifest =
                    validate_manifest(head, &bytes).map_err(|e| anyhow::anyhow!(e.code))?;
                repo.index_generation(&mut index, &record.room, &k, head, &manifest, bytes.len());
            }
            // An imported current head may omit intermediate history. When a
            // backup exists it must still be the signed direct parent.
            if let Some(previous) = &record.previous {
                ensure!(
                    !check_cas(&record.current, Some(previous))
                        .map_err(|e| anyhow::anyhow!(e.code))?,
                    "escrow: duplicate backup generation"
                );
            }
            index.records.insert(k, record);
        }
        // A crash before the head replacement may leave unreferenced generations.
        // Only our validated hash/slot directory names are eligible for cleanup.
        repo.cleanup(&index)?;
        repo.cleanup_temps()?;
        drop(index);
        Ok(repo)
    }

    fn generation(&self, key: &str, head: &Head) -> PathBuf {
        // A flat digest directory keeps Windows paths below legacy path limits.
        self.root
            .join("blobs")
            .join(digest(format!("{key}\n{}", head.hash()).as_bytes()))
    }

    fn index_generation(
        &self,
        index: &mut Index,
        room: &str,
        k: &str,
        head: &Head,
        manifest: &FileManifest,
        manifest_len: usize,
    ) {
        let dir = self.generation(k, head);
        index
            .manifests
            .insert((k.into(), head.hash()), (manifest.clone(), manifest_len));
        index.bytes += head.size + manifest_len as u64;
        index.blocks.insert(
            (room.into(), head.cid.clone()),
            Block {
                path: dir.join("manifest.bin"),
                offset: 0,
                len: manifest_len,
            },
        );
        for (i, cid) in manifest.chunks.iter().enumerate() {
            let offset = (i * CHUNK_SIZE) as u64;
            index.blocks.insert(
                (room.into(), cid.clone()),
                Block {
                    path: dir.join("blob.bin"),
                    offset,
                    len: (head.size - offset).min(CHUNK_SIZE as u64) as usize,
                },
            );
        }
    }

    pub fn head(&self, room: &str, slot: &str) -> Option<Head> {
        self.index
            .lock()
            .unwrap()
            .records
            .get(&key(room, slot))
            .map(|r| r.current.clone())
    }

    pub fn room_heads(&self, room: &str) -> Vec<Head> {
        let index = self.index.lock().unwrap();
        let mut heads: Vec<_> = index
            .records
            .values()
            .filter(|record| record.room == room)
            .map(|record| record.current.clone())
            .collect();
        heads.sort_by(|a, b| a.slot.cmp(&b.slot));
        heads
    }

    pub fn retire_room(&self, room: &str) -> Result<()> {
        let mut index = self.index.lock().unwrap();
        ensure!(
            !index.write_failed,
            "escrow: legacy repository needs restart reconciliation"
        );
        let keys: Vec<_> = index
            .records
            .iter()
            .filter(|(_, record)| record.room == room)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            if let Err(error) = self.delete_inner(&mut index, &key) {
                index.write_failed = true;
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn generation_bytes(
        &self,
        room: &str,
        head: &Head,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let index = self.index.lock().unwrap();
        let k = key(room, &head.slot);
        let Some(record) = index.records.get(&k) else {
            return Ok(None);
        };
        if record.current != *head && record.previous.as_ref() != Some(head) {
            return Ok(None);
        }
        let dir = self.generation(&k, head);
        let blob = read_bounded(&dir.join("blob.bin"), head.size as usize)
            .map_err(|_| Error::new("fetch_failed"))?;
        let manifest = read_bounded(&dir.join("manifest.bin"), MAX_MANIFEST)
            .map_err(|_| Error::new("fetch_failed"))?;
        validate_blob(head, &blob)?;
        validate_manifest(head, &manifest)?;
        Ok(Some((blob, manifest)))
    }

    /// Export only a still-current generation under the same index lock as reads.
    pub fn current_bytes(
        &self,
        room: &str,
        head: &Head,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let index = self.index.lock().unwrap();
        let k = key(room, &head.slot);
        if index.records.get(&k).is_none_or(|r| r.current != *head) {
            return Ok(None);
        }
        let dir = self.generation(&k, head);
        let blob = read_bounded(&dir.join("blob.bin"), head.size as usize)
            .map_err(|_| Error::new("fetch_failed"))?;
        let manifest = read_bounded(&dir.join("manifest.bin"), MAX_MANIFEST)
            .map_err(|_| Error::new("fetch_failed"))?;
        Ok(Some((blob, manifest)))
    }

    pub fn status(&self, room: &str) -> (usize, u64) {
        let index = self.index.lock().unwrap();
        let records: Vec<_> = index.records.values().filter(|r| r.room == room).collect();
        let bytes = records
            .iter()
            .flat_map(|r| std::iter::once(&r.current).chain(r.previous.iter()))
            .map(|h| h.size)
            .sum();
        (records.len(), bytes)
    }

    pub fn total_bytes(&self) -> u64 {
        self.index.lock().unwrap().bytes
    }

    pub fn rooms(&self) -> Vec<String> {
        let index = self.index.lock().unwrap();
        let mut rooms: Vec<_> = index.records.values().map(|r| r.room.clone()).collect();
        rooms.sort();
        rooms.dedup();
        rooms
    }

    /// Caller holds a slot lock. Reservation includes the original manifest,
    /// and accounts for all concurrent pulls before any blob allocation.
    pub fn reserve<'a>(
        &'a self,
        room: &str,
        head: &Head,
    ) -> Result<Option<Reservation<'a>>, Error> {
        head.validate(self.cfg.max_blob_bytes)?;
        head.verify()?;
        let mut index = self.index.lock().unwrap();
        // Publication/deletion failures can leave retained heads only in memory.
        // Reject even retries until restart has reconciled the authoritative files.
        if index.write_failed {
            return Err(Error::new("quota"));
        }
        let k = key(room, &head.slot);
        let record = index.records.get(&k);
        // Retained accepted heads are retries, never a request to roll back the slot.
        if record.is_some_and(|r| r.previous.as_ref() == Some(head))
            || check_cas(head, record.map(|r| &r.current))?
        {
            return Ok(None);
        }
        let slots = index.records.values().filter(|r| r.room == room).count()
            + index
                .pending_slots
                .values()
                .filter(|r| r.as_str() == room)
                .count();
        if !index.records.contains_key(&k)
            && (slots >= self.cfg.max_slots_per_room
                || index.records.len() + index.pending_slots.len() >= 65536)
        {
            return Err(Error::new("quota"));
        }
        let amount = head.size + manifest_limit(head.size) as u64;
        if index
            .bytes
            .saturating_add(index.reserved)
            .saturating_add(amount)
            > self.cfg.max_total_bytes
        {
            return Err(Error::new("quota"));
        }
        index.reserved += amount;
        index.pending_slots.insert(k.clone(), room.into());
        Ok(Some(Reservation {
            repo: self,
            key: k,
            amount,
        }))
    }

    pub fn commit(
        &self,
        room: &str,
        head: &Head,
        blob: &[u8],
        manifest_bytes: &[u8],
    ) -> Result<(), Error> {
        validate_blob(head, blob)?;
        let manifest = validate_manifest(head, manifest_bytes)?;
        for (cid, data) in manifest.chunks.iter().zip(blob.chunks(CHUNK_SIZE)) {
            if !verify_cid(cid, data, MULTICODEC_RAW) {
                return Err(Error::new("hash_mismatch"));
            }
        }
        let mut index = self.index.lock().unwrap();
        if index.write_failed {
            return Err(Error::new("quota"));
        }
        let previous = index
            .records
            .get(&key(room, &head.slot))
            .map(|r| r.current.clone());
        self.commit_inner(
            &mut index,
            room,
            head,
            previous,
            blob,
            manifest_bytes,
            &manifest,
        )
        .map_err(|error| {
            // Publication may have succeeded before its directory sync failed. Record
            // failure before releasing this same lock: even reserved PUTs must stop.
            index.write_failed = true;
            tracing::warn!(%error, "escrow: durable commit failed; writes disabled until restart");
            Error::new("fetch_failed")
        })
    }

    /// Caller holds the PUT slot stripe. Import can advance over missing history.
    pub fn import(
        &self,
        room: &str,
        head: &Head,
        blob: &[u8],
        manifest_bytes: &[u8],
    ) -> Result<ImportOutcome, Error> {
        if !super::valid_room(room) {
            return Err(Error::new("bad_request"));
        }
        head.validate(self.cfg.max_blob_bytes)?;
        head.verify()?;
        validate_blob(head, blob)?;
        let manifest = validate_manifest(head, manifest_bytes)?;
        for (cid, chunk) in manifest.chunks.iter().zip(blob.chunks(CHUNK_SIZE)) {
            if !verify_cid(cid, chunk, MULTICODEC_RAW) {
                return Err(Error::new("hash_mismatch"));
            }
        }
        let mut index = self.index.lock().unwrap();
        if index.write_failed {
            return Err(Error::new("quota"));
        }
        let k = key(room, &head.slot);
        let mut previous = None;
        if let Some(record) = index.records.get(&k) {
            if record.current == *head || record.previous.as_ref() == Some(head) {
                return Ok(ImportOutcome::Unchanged);
            }
            if record.current.writer != head.writer {
                return Ok(ImportOutcome::Skipped("writer_mismatch"));
            }
            if head.seq < record.current.seq {
                return Ok(ImportOutcome::Skipped("older"));
            }
            if head.seq == record.current.seq {
                return Ok(ImportOutcome::Skipped("conflict"));
            }
            if record.current.seq.checked_add(1) == Some(head.seq)
                && head.parent.as_deref() == Some(record.current.hash().as_str())
            {
                previous = Some(record.current.clone());
            }
        } else {
            let slots = index.records.values().filter(|r| r.room == room).count()
                + index
                    .pending_slots
                    .values()
                    .filter(|r| r.as_str() == room)
                    .count();
            if slots >= self.cfg.max_slots_per_room
                || index.records.len() + index.pending_slots.len() >= 65536
            {
                return Ok(ImportOutcome::Skipped("quota"));
            }
        }
        if index
            .bytes
            .saturating_add(index.reserved)
            .saturating_add(head.size)
            .saturating_add(manifest_bytes.len() as u64)
            > self.cfg.max_total_bytes
        {
            return Ok(ImportOutcome::Skipped("quota"));
        }
        self.commit_inner(
            &mut index,
            room,
            head,
            previous,
            blob,
            manifest_bytes,
            &manifest,
        )
        .map_err(|error| {
            index.write_failed = true;
            tracing::warn!(%error, "escrow: durable import failed; writes disabled until restart");
            Error::new("fetch_failed")
        })?;
        Ok(ImportOutcome::Imported)
    }

    fn commit_inner(
        &self,
        index: &mut Index,
        room: &str,
        head: &Head,
        previous: Option<Head>,
        blob: &[u8],
        bytes: &[u8],
        manifest: &FileManifest,
    ) -> Result<()> {
        let k = key(room, &head.slot);
        let dir = self.generation(&k, head);
        crate::statefile::write_private(&dir.join("blob.bin"), blob)?;
        crate::statefile::write_private(&dir.join("manifest.bin"), bytes)?;
        // Sync new directory entries too, before publishing a reference to them.
        sync_dir(dir.parent().context("generation parent")?)?;
        sync_dir(&self.root.join("blobs"))?;
        let record = Record {
            room: room.into(),
            current: head.clone(),
            previous,
        };
        crate::statefile::write_private(
            &self.root.join("heads").join(format!("{k}.json")),
            &serde_json::to_vec(&record)?,
        )?;
        #[cfg(test)]
        ensure!(
            !self
                .fail_after_publish
                .swap(false, std::sync::atomic::Ordering::SeqCst),
            "injected post-publication sync failure"
        );
        index.records.insert(k.clone(), record);
        // Rebuild from already validated metadata: no fallible I/O after publication.
        self.index_generation(index, room, &k, head, manifest, bytes.len());
        self.rebuild_index(index);
        if let Err(error) = self.cleanup(index) {
            index.write_failed = true;
            tracing::warn!(%error, "escrow: cleanup failed; writes disabled until restart");
        }
        Ok(())
    }

    fn rebuild_index(&self, index: &mut Index) {
        index.blocks.clear();
        index.bytes = 0;
        let records: Vec<_> = index
            .records
            .iter()
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect();
        let mut retained = HashSet::new();
        for (k, record) in records {
            for h in std::iter::once(&record.current).chain(record.previous.iter()) {
                let cache_key = (k.clone(), h.hash());
                retained.insert(cache_key.clone());
                let (manifest, len) = index.manifests[&cache_key].clone();
                self.index_generation(index, &record.room, &k, h, &manifest, len);
            }
        }
        index.manifests.retain(|key, _| retained.contains(key));
    }

    /// Caller holds the same slot lock used for PUT, including pending pulls.
    pub fn delete(&self, room: &str, request: &Delete) -> Result<(), Error> {
        request.validate()?;
        let mut index = self.index.lock().unwrap();
        // An uncertain publication must be reconciled before deciding a slot is absent.
        if index.write_failed {
            return Err(Error::new("quota"));
        }
        let k = key(room, &request.slot);
        let Some(record) = index.records.get(&k) else {
            return Ok(());
        };
        if request.seq != record.current.seq || request.head != record.current.hash() {
            return Err(Error::conflict(&record.current));
        }
        request.verify(&record.current.writer)?;
        self.delete_inner(&mut index, &k).map_err(|error| {
            index.write_failed = true;
            tracing::warn!(%error, "escrow: durable delete failed; writes disabled until restart");
            Error::new("fetch_failed")
        })
    }

    fn delete_inner(&self, index: &mut Index, k: &str) -> Result<()> {
        // Unlink is atomic; sync its directory before removing referenced generations.
        std::fs::remove_file(self.root.join("heads").join(format!("{k}.json")))?;
        sync_dir(&self.root.join("heads"))?;
        #[cfg(test)]
        ensure!(
            !self
                .fail_after_publish
                .swap(false, std::sync::atomic::Ordering::SeqCst),
            "injected post-delete cleanup failure"
        );
        index.records.remove(k);
        self.rebuild_index(index);
        self.cleanup(index)?;
        sync_dir(&self.root.join("blobs"))?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn inject_post_publish_failure(&self) {
        self.fail_after_publish
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn cleanup(&self, index: &Index) -> Result<()> {
        ensure!(
            !index.write_failed,
            "escrow: index requires restart reconciliation"
        );
        let keep: HashSet<_> = index
            .records
            .iter()
            .flat_map(|(k, r)| {
                std::iter::once(&r.current)
                    .chain(r.previous.iter())
                    .map(|head| self.generation(k, head).file_name().unwrap().to_owned())
            })
            .collect();
        for entry in std::fs::read_dir(self.root.join("blobs"))? {
            let entry = entry?;
            let name = entry.file_name();
            if hex_string(&name.to_string_lossy(), 64)
                && entry.file_type()?.is_dir()
                && !keep.contains(&name)
            {
                std::fs::remove_dir_all(entry.path())?;
            }
        }
        Ok(())
    }

    fn cleanup_temps(&self) -> Result<()> {
        remove_temps(&self.root.join("heads"), |stem| hex_string(stem, 64))?;
        for entry in std::fs::read_dir(self.root.join("blobs"))? {
            let entry = entry?;
            if hex_string(&entry.file_name().to_string_lossy(), 64) && entry.file_type()?.is_dir() {
                remove_temps(&entry.path(), |stem| matches!(stem, "blob" | "manifest"))?;
            }
        }
        Ok(())
    }

    /// Read only blocks referenced by a committed current/backup head in this room.
    pub fn block(&self, room: &str, cid: &str) -> Option<Vec<u8>> {
        let index = self.index.lock().unwrap();
        let block = index.blocks.get(&(room.into(), cid.into()))?;
        let mut file = std::fs::File::open(&block.path).ok()?;
        file.seek(SeekFrom::Start(block.offset)).ok()?;
        let mut data = vec![0; block.len];
        file.read_exact(&mut data).ok()?;
        Some(data)
    }
}

pub fn manifest_limit(size: u64) -> usize {
    (1024 + size.div_ceil(CHUNK_SIZE as u64) as usize * 128).min(MAX_MANIFEST)
}

fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.len() <= max as u64,
        "escrow: oversized file"
    );
    let mut data = Vec::new();
    file.take(max as u64 + 1).read_to_end(&mut data)?;
    ensure!(data.len() <= max, "escrow: oversized file");
    Ok(data)
}

// statefile::write_private replaces an extension with <16 hex random digits>.tmp.
// Only regular files with our exact stems and temporary suffix are crash leftovers.
pub(super) fn remove_temps(dir: &Path, valid_stem: impl Fn(&str) -> bool) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some((stem, nonce)) = name.strip_suffix(".tmp").and_then(|s| s.rsplit_once('.')) else {
            continue;
        };
        if valid_stem(stem) && hex_string(nonce, 16) && entry.file_type()?.is_file() {
            std::fs::remove_file(entry.path())?;
        }
    }
    sync_dir(dir)
}

fn sync_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub struct Reservation<'a> {
    repo: &'a Repository,
    key: String,
    amount: u64,
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut index = self.repo.index.lock().unwrap();
        index.reserved -= self.amount;
        index.pending_slots.remove(&self.key);
    }
}
