//! Local encrypted checkpoints of a VM's qcow2 overlay.
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use anyhow::{Context, Result, ensure};
use hmac::{Hmac, Mac};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::{
    sshkey,
    store::{self, VmRecord},
};
use crate::statefile;

pub(crate) const CHUNK_SIZE: usize = 1024 * 1024;
const CHUNK_AAD: &[u8] = b"mistl-vm-chunk/v1";
const MANIFEST_AAD: &[u8] = b"mistl-vm-manifest/v1";
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const MANIFEST_LIMIT: usize = 64 * 1024 * 1024;
const MAX_CHUNKS: u64 = (MANIFEST_LIMIT / 67) as u64;
static KEY_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub v: u32,
    pub server_id: String,
    pub seq: u64,
    pub epoch: u64,
    pub created_at: String,
    pub parent_seq: Option<u64>,
    pub record: VmRecord,
    pub base_sha256: String,
    pub base_url: String,
    pub disk_bytes: u64,
    pub chunk_size: usize,
    pub chunks: Vec<String>,
    pub known_hosts: Vec<String>,
}

impl Manifest {
    fn validate(&self, id: &str) -> Result<()> {
        ensure!(self.v == 1, "unsupported VM checkpoint version");
        ensure!(
            self.server_id == id && self.record.server_id == id,
            "checkpoint belongs to another VM"
        );
        ensure!(
            self.seq > 0 && self.parent_seq.is_none_or(|seq| seq > 0 && seq < self.seq),
            "invalid VM checkpoint sequence"
        );
        ensure!(
            self.base_sha256 == self.record.base_sha256
                && self.base_url == self.record.base_url
                && valid_chunk_id(&self.base_sha256),
            "inconsistent VM checkpoint base image"
        );
        ensure!(
            self.chunk_size == CHUNK_SIZE
                && self.disk_bytes > 0
                && self.chunks.len() as u64 <= MAX_CHUNKS
                && self.disk_bytes.div_ceil(CHUNK_SIZE as u64) == self.chunks.len() as u64,
            "inconsistent VM checkpoint disk or chunk size"
        );
        ensure!(
            self.chunks
                .iter()
                .all(|id| id.is_empty() || valid_chunk_id(id)),
            "invalid VM checkpoint chunk id"
        );
        ensure!(
            self.record.memory_mb > 0 && self.record.cpus > 0 && self.record.disk_gb > 0,
            "invalid VM checkpoint resource allocation"
        );
        chrono::DateTime::parse_from_rfc3339(&self.created_at)
            .context("invalid VM checkpoint creation date")?;
        for key in &self.record.ssh_authorized_keys {
            super::validate_ssh_key(key)?;
        }
        sshkey::validate_known_hosts(id, &self.known_hosts)
    }

    fn chunk_len(&self, index: usize) -> usize {
        let offset = (index as u64) * CHUNK_SIZE as u64;
        (self.disk_bytes - offset).min(CHUNK_SIZE as u64) as usize
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct CheckpointStats {
    pub seq: u64,
    pub chunks: usize,
    pub new_chunks: usize,
    pub bytes: u64,
    pub new_bytes: u64,
    pub pause_ms: u64,
}

fn key_path(root: &Path, id: &str) -> Result<PathBuf> {
    ensure!(store::valid_server_id(id), "invalid VM server id");
    Ok(store::vm_dir(root, id).join("vm.key"))
}

pub(super) fn existing_key(root: &Path, id: &str) -> Result<[u8; 32]> {
    read_key(&key_path(root, id)?)?.context("VM has no checkpoint key")
}

fn read_key(path: &Path) -> Result<Option<[u8; 32]>> {
    match read_bounded(path, 32) {
        Ok(bytes) => Ok(Some(
            bytes.try_into().ok().context("invalid VM key length")?,
        )),
        Err(error) if not_found(&error) => Ok(None),
        Err(error) => Err(error).context("reading VM checkpoint key"),
    }
}

pub(crate) fn load_or_create_key(root: &Path, id: &str) -> Result<[u8; 32]> {
    let path = key_path(root, id)?;
    let _guard = KEY_LOCK.lock().expect("VM checkpoint key lock");
    if let Some(key) = read_key(&path)? {
        return Ok(key);
    }
    let mut key = [0; 32];
    OsRng
        .try_fill_bytes(&mut key)
        .context("generating VM checkpoint key")?;
    statefile::write_private(&path, &key).context("saving VM checkpoint key")?;
    Ok(key)
}

/// Install a key received during group join. An existing key is immutable.
#[allow(dead_code)] // Used by checkpoint replication.
pub(crate) fn install_key(root: &Path, id: &str, key: &[u8; 32]) -> Result<()> {
    let path = key_path(root, id)?;
    let _guard = KEY_LOCK.lock().expect("VM checkpoint key lock");
    if let Some(existing) = read_key(&path)? {
        ensure!(&existing == key, "VM checkpoint key already exists");
        return Ok(());
    }
    statefile::write_private(&path, key).context("saving VM checkpoint key")
}

/// Replication callers hold the per-VM operation lock while installing a batch
/// of chunks and its manifest, so local retention cannot remove staged chunks.
pub(crate) struct CheckpointStore {
    dir: PathBuf,
    id: String,
    key: [u8; 32],
    /// New chunks `capture` wrote as unsynced plaintext: (temporary, final) paths.
    /// `commit` encrypts and syncs them, so the guest pause covers neither. The
    /// plaintext reveals nothing beyond the overlay next to it.
    pending: Mutex<Vec<(PathBuf, PathBuf)>>,
}

impl CheckpointStore {
    pub(crate) fn open(root: &Path, id: &str) -> Result<Self> {
        let key = read_key(&key_path(root, id)?)?.context("VM has no checkpoint key")?;
        Ok(Self {
            dir: store::vm_dir(root, id).join("ckpt"),
            id: id.to_owned(),
            key,
            pending: Mutex::new(Vec::new()),
        })
    }

    fn manifest_path(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("{seq}.manifest"))
    }

    fn chunk_path(&self, id: &str) -> Result<PathBuf> {
        ensure!(valid_chunk_id(id), "invalid VM checkpoint chunk id");
        Ok(self.dir.join("chunks").join(&id[..2]).join(id))
    }

    pub(crate) fn sequences(&self) -> Result<Vec<u64>> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error).context("listing VM checkpoint directory"),
        };
        let mut sequences = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(seq) = name
                .to_str()
                .and_then(|name| name.strip_suffix(".manifest"))
                .and_then(|seq| {
                    seq.parse::<u64>()
                        .ok()
                        .filter(|n| *n > 0 && n.to_string() == seq)
                })
            {
                sequences.push(seq);
            }
        }
        sequences.sort_unstable_by(|a, b| b.cmp(a));
        Ok(sequences)
    }

    #[allow(dead_code)] // Used by checkpoint replication.
    pub(crate) fn list_manifests(&self) -> Result<Vec<Manifest>> {
        self.sequences()?
            .into_iter()
            .map(|seq| self.load_manifest(seq))
            .collect()
    }

    pub(crate) fn read_encrypted_manifest(&self, seq: u64) -> Result<Vec<u8>> {
        read_bounded(&self.manifest_path(seq), MANIFEST_LIMIT)
            .with_context(|| format!("reading VM checkpoint {seq}"))
    }

    pub(crate) fn load_manifest(&self, seq: u64) -> Result<Manifest> {
        let manifest = self.decode_manifest(&self.read_encrypted_manifest(seq)?)?;
        ensure!(
            manifest.seq == seq,
            "VM checkpoint sequence does not match its filename"
        );
        Ok(manifest)
    }

    /// Inspect an authenticated offer before pulling its missing chunks.
    pub(crate) fn decode_manifest(&self, bytes: &[u8]) -> Result<Manifest> {
        ensure!(
            (NONCE_SIZE + TAG_SIZE..=MANIFEST_LIMIT).contains(&bytes.len()),
            "invalid encrypted VM checkpoint manifest size"
        );
        let (nonce, ciphertext) = bytes.split_at(NONCE_SIZE);
        let plain = decrypt(&self.key, nonce, MANIFEST_AAD, ciphertext)?;
        let manifest: Manifest =
            serde_json::from_slice(&plain).context("parsing VM checkpoint manifest")?;
        manifest.validate(&self.id)?;
        Ok(manifest)
    }

    pub(super) fn install_manifest(&self, manifest: &Manifest) -> Result<()> {
        ensure!(
            self.missing_chunks(manifest)?.is_empty(),
            "VM checkpoint is missing chunks"
        );
        let path = self.manifest_path(manifest.seq);
        if path.try_exists()? {
            ensure!(
                self.load_manifest(manifest.seq)? == *manifest,
                "conflicting VM checkpoint sequence"
            );
        } else {
            let bytes = encode_manifest(&self.key, manifest)?;
            statefile::write_bytes(&path, &bytes)
                .context("saving encrypted VM checkpoint manifest")?;
        }
        Ok(())
    }

    pub(crate) fn read_encrypted_chunk(&self, id: &str) -> Result<Vec<u8>> {
        read_bounded(&self.chunk_path(id)?, CHUNK_SIZE + TAG_SIZE)
            .context("reading encrypted VM checkpoint chunk")
    }

    pub(super) fn read_referenced_chunk(&self, seq: u64, id: &str) -> Result<Vec<u8>> {
        ensure!(valid_chunk_id(id), "invalid VM checkpoint chunk id");
        ensure!(
            self.load_manifest(seq)?
                .chunks
                .iter()
                .any(|chunk| chunk == id),
            "VM checkpoint does not reference the requested chunk"
        );
        self.read_encrypted_chunk(id)
    }

    #[allow(dead_code)] // Used by checkpoint replication.
    pub(crate) fn store_encrypted_chunk(&self, id: &str, bytes: &[u8]) -> Result<()> {
        decrypt_chunk(&self.key, id, bytes)?;
        statefile::write_bytes(&self.chunk_path(id)?, bytes)
            .context("saving encrypted VM checkpoint chunk")
    }

    pub(crate) fn missing_chunks(&self, manifest: &Manifest) -> Result<Vec<String>> {
        manifest.validate(&self.id)?;
        let mut missing = HashSet::new();
        for (index, id) in manifest.chunks.iter().enumerate() {
            if !id.is_empty() && !self.has_chunk(id, manifest.chunk_len(index))? {
                missing.insert(id.clone());
            }
        }
        let mut missing: Vec<_> = missing.into_iter().collect();
        missing.sort_unstable();
        Ok(missing)
    }

    pub(super) fn verified_missing_chunks(
        &self,
        manifest: &Manifest,
        repair: bool,
    ) -> Result<Vec<String>> {
        let mut missing: HashSet<_> = self.missing_chunks(manifest)?.into_iter().collect();
        let mut checked = HashSet::new();
        for id in &manifest.chunks {
            if id.is_empty() || missing.contains(id) || !checked.insert(id) {
                continue;
            }
            if self
                .read_encrypted_chunk(id)
                .and_then(|bytes| decrypt_chunk(&self.key, id, &bytes))
                .is_err()
            {
                if repair {
                    fs::remove_file(self.chunk_path(id)?)
                        .context("removing corrupt VM checkpoint chunk")?;
                }
                missing.insert(id.clone());
            }
        }
        let mut missing: Vec<_> = missing.into_iter().collect();
        missing.sort_unstable();
        Ok(missing)
    }

    pub(super) fn newest_complete(
        &self,
        floor: u64,
        eligible: impl Fn(&Manifest) -> bool,
    ) -> Result<u64> {
        for seq in self
            .sequences()?
            .into_iter()
            .take_while(|&seq| seq >= floor)
        {
            if let Ok(manifest) = self.load_manifest(seq)
                && eligible(&manifest)
                && self.verified_missing_chunks(&manifest, false)?.is_empty()
            {
                return Ok(seq);
            }
        }
        anyhow::bail!("no complete VM checkpoint at or above sequence {floor}")
    }

    fn has_chunk(&self, id: &str, plain_len: usize) -> Result<bool> {
        match fs::symlink_metadata(self.chunk_path(id)?) {
            Ok(meta) => Ok(meta.is_file() && meta.len() == (plain_len + TAG_SIZE) as u64),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).context("checking VM checkpoint chunk"),
        }
    }

    pub(super) fn prepare(
        &self,
        record: VmRecord,
        epoch: u64,
        known_hosts: Vec<String>,
    ) -> Result<Manifest> {
        let parent_seq = self.sequences()?.first().copied();
        let seq = parent_seq
            .unwrap_or(0)
            .checked_add(1)
            .context("VM checkpoint sequence exhausted")?;
        Ok(Manifest {
            v: 1,
            server_id: self.id.clone(),
            seq,
            epoch,
            created_at: chrono::Utc::now().to_rfc3339(),
            parent_seq,
            base_sha256: record.base_sha256.clone(),
            base_url: record.base_url.clone(),
            record,
            disk_bytes: 0,
            chunk_size: CHUNK_SIZE,
            chunks: Vec::new(),
            known_hosts,
        })
    }

    /// Read one chunk at a time from a stopped overlay, storing new chunks as we go.
    pub(super) fn capture(&self, disk: &Path, manifest: &mut Manifest) -> Result<CheckpointStats> {
        let mut file = File::open(disk).context("opening VM overlay for checkpoint")?;
        manifest.disk_bytes = file.metadata()?.len();
        ensure!(
            manifest.disk_bytes.div_ceil(CHUNK_SIZE as u64) <= MAX_CHUNKS,
            "VM overlay exceeds checkpoint size limit"
        );
        let mut remaining = manifest.disk_bytes;
        let mut buffer = vec![0; CHUNK_SIZE];
        let mut new_chunks = 0;
        let mut new_bytes = 0;
        let mut pending_ids = HashSet::new();
        let mut pending = self
            .pending
            .lock()
            .expect("checkpoint pending list poisoned");
        while remaining > 0 {
            let len = remaining.min(CHUNK_SIZE as u64) as usize;
            let plain = &mut buffer[..len];
            file.read_exact(plain)
                .context("reading VM overlay for checkpoint")?;
            let id = chunk_id(&self.key, plain);
            if !id.is_empty() && !pending_ids.contains(&id) && !self.has_chunk(&id, len)? {
                let path = self.chunk_path(&id)?;
                let temp = path.with_extension("pending");
                // Unsynced on purpose: the guest may be paused right now.
                fs::create_dir_all(path.parent().context("chunk path has no parent")?)?;
                fs::write(&temp, &*plain).context("saving VM checkpoint chunk")?;
                pending.push((temp, path));
                pending_ids.insert(id.clone());
                new_chunks += 1;
                new_bytes += len as u64;
            }
            manifest.chunks.push(id);
            remaining -= len as u64;
        }
        Ok(CheckpointStats {
            seq: manifest.seq,
            chunks: manifest.chunks.len(),
            new_chunks,
            bytes: manifest.disk_bytes,
            new_bytes,
            pause_ms: 0,
        })
    }

    pub(super) fn commit(&self, manifest: &Manifest) -> Result<()> {
        manifest.validate(&self.id)?;
        let pending = std::mem::take(
            &mut *self
                .pending
                .lock()
                .expect("checkpoint pending list poisoned"),
        );
        for (temp, path) in pending {
            let plain =
                read_bounded(&temp, CHUNK_SIZE).context("reading staged VM checkpoint chunk")?;
            let id = path
                .file_name()
                .and_then(|name| name.to_str())
                .context("invalid chunk path")?;
            let ciphertext = encrypt_chunk(&self.key, id, &plain)?;
            statefile::write_bytes(&path, &ciphertext).context("saving VM checkpoint chunk")?;
            fs::remove_file(&temp).context("removing staged VM checkpoint chunk")?;
        }
        let bytes = encode_manifest(&self.key, manifest)?;
        let path = self.manifest_path(manifest.seq);
        ensure!(!path.try_exists()?, "VM checkpoint sequence already exists");
        statefile::write_bytes(&path, &bytes).context("saving VM checkpoint manifest")
    }

    /// Reconstruct the overlay at a new path. The caller rebases and publishes it.
    pub(super) fn reconstruct(&self, manifest: &Manifest, temp: &Path) -> Result<()> {
        manifest.validate(&self.id)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut out = options
            .open(temp)
            .context("creating temporary restored VM overlay")?;
        let zeros = vec![0; CHUNK_SIZE];
        for (index, id) in manifest.chunks.iter().enumerate() {
            let len = manifest.chunk_len(index);
            if id.is_empty() {
                // Windows needs a separate filesystem control call to mark a file sparse.
                if cfg!(windows) {
                    out.write_all(&zeros[..len])?;
                } else {
                    out.seek(SeekFrom::Current(len as i64))?;
                }
            } else {
                let ciphertext = self.read_encrypted_chunk(id)?;
                let plain = decrypt_chunk(&self.key, id, &ciphertext)?;
                ensure!(
                    plain.len() == len,
                    "VM checkpoint chunk length does not match manifest"
                );
                out.write_all(&plain)
                    .context("writing restored VM overlay")?;
            }
        }
        out.set_len(manifest.disk_bytes)?;
        out.sync_all().context("syncing restored VM overlay")
    }

    /// Keep the newest manifests and collect chunks unreferenced by those manifests.
    pub(super) fn prune(&self, keep: usize, pinned: impl Fn(u64) -> bool) -> Result<()> {
        ensure!(
            (1..=16).contains(&keep),
            "checkpoint_keep must be between 1 and 16"
        );
        let sequences = self.sequences()?;
        let (retained, obsolete): (Vec<_>, Vec<_>) = sequences
            .into_iter()
            .enumerate()
            .partition(|&(index, seq)| index < keep || pinned(seq));
        let live = self.live_chunks(retained.into_iter().map(|(_, seq)| seq))?;
        for (_, seq) in obsolete {
            fs::remove_file(self.manifest_path(seq)).context("pruning VM checkpoint manifest")?;
        }
        self.prune_chunks(&live)
    }

    pub(super) fn collect_chunks(&self) -> Result<()> {
        self.prune_chunks(&self.live_chunks(self.sequences()?)?)
    }

    fn live_chunks(&self, sequences: impl IntoIterator<Item = u64>) -> Result<HashSet<String>> {
        let mut live = HashSet::new();
        for seq in sequences {
            live.extend(
                self.load_manifest(seq)?
                    .chunks
                    .into_iter()
                    .filter(|id| !id.is_empty()),
            );
        }
        Ok(live)
    }

    fn prune_chunks(&self, live: &HashSet<String>) -> Result<()> {
        let shards = match fs::read_dir(self.dir.join("chunks")) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).context("listing VM checkpoint chunks"),
        };
        for shard in shards {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(shard.path())? {
                let entry = entry?;
                // Leftovers of a capture that never committed.
                if entry.path().extension().is_some_and(|ext| ext == "pending") {
                    fs::remove_file(entry.path()).context("removing stale VM checkpoint chunk")?;
                    continue;
                }
                if entry.file_name().to_str().is_some_and(|id| {
                    valid_chunk_id(id) && shard.file_name() == id[..2] && !live.contains(id)
                }) {
                    fs::remove_file(entry.path())
                        .context("pruning unreferenced VM checkpoint chunk")?;
                }
            }
        }
        Ok(())
    }
}

fn not_found(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|e| e.kind() == ErrorKind::NotFound)
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= limit,
        "VM checkpoint file exceeds size limit"
    );
    Ok(bytes)
}

fn valid_chunk_id(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn chunk_mac(key: &[u8; 32], plain: &[u8]) -> Hmac<Sha256> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("valid HMAC key");
    mac.update(plain);
    mac
}

fn chunk_id(key: &[u8; 32], plain: &[u8]) -> String {
    if plain.iter().all(|&b| b == 0) {
        String::new()
    } else {
        hex::encode(chunk_mac(key, plain).finalize().into_bytes())
    }
}

fn chunk_id_bytes(id: &str) -> Result<[u8; 32]> {
    ensure!(valid_chunk_id(id), "invalid VM checkpoint chunk id");
    let mut bytes = [0; 32];
    hex::decode_to_slice(id, &mut bytes)?;
    Ok(bytes)
}

fn encrypt(key: &[u8; 32], nonce: &[u8], aad: &[u8], plain: &[u8]) -> Result<Vec<u8>> {
    Aes256Gcm::new(key.into())
        .encrypt(Nonce::from_slice(nonce), Payload { msg: plain, aad })
        .map_err(|_| anyhow::anyhow!("encrypting VM checkpoint failed"))
}

fn decrypt(key: &[u8; 32], nonce: &[u8], aad: &[u8], bytes: &[u8]) -> Result<Vec<u8>> {
    Aes256Gcm::new(key.into())
        .decrypt(Nonce::from_slice(nonce), Payload { msg: bytes, aad })
        .map_err(|_| anyhow::anyhow!("VM checkpoint authentication failed"))
}

fn encrypt_chunk(key: &[u8; 32], id: &str, plain: &[u8]) -> Result<Vec<u8>> {
    let address = chunk_id_bytes(id)?;
    encrypt(key, &address[..NONCE_SIZE], CHUNK_AAD, plain)
}

fn decrypt_chunk(key: &[u8; 32], id: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        (TAG_SIZE + 1..=CHUNK_SIZE + TAG_SIZE).contains(&bytes.len()),
        "invalid VM checkpoint chunk size"
    );
    let address = chunk_id_bytes(id)?;
    let plain = decrypt(key, &address[..NONCE_SIZE], CHUNK_AAD, bytes)?;
    chunk_mac(key, &plain)
        .verify_slice(&address)
        .map_err(|_| anyhow::anyhow!("VM checkpoint chunk id does not match plaintext"))?;
    ensure!(
        plain.iter().any(|&b| b != 0),
        "zero VM checkpoint chunks must use the empty id"
    );
    Ok(plain)
}

/// Encrypted manifest framing is the 12-byte nonce followed by ciphertext and tag.
fn encode_manifest(key: &[u8; 32], manifest: &Manifest) -> Result<Vec<u8>> {
    let plain = serde_json::to_vec(manifest)?;
    ensure!(
        plain.len() <= MANIFEST_LIMIT - NONCE_SIZE - TAG_SIZE,
        "VM checkpoint manifest exceeds size limit"
    );
    let mut nonce = [0; NONCE_SIZE];
    OsRng
        .try_fill_bytes(&mut nonce)
        .context("generating VM checkpoint nonce")?;
    let ciphertext = encrypt(key, &nonce, MANIFEST_AAD, &plain)?;
    let mut bytes = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    bytes.extend_from_slice(&nonce);
    bytes.extend_from_slice(&ciphertext);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "1234567890abcdef";
    const KEY: [u8; 32] = [7; 32];

    fn record() -> VmRecord {
        VmRecord {
            server_id: ID.into(),
            name: "checkpoint-test".into(),
            owner_did: "did:key:test".into(),
            allowed_dids: Vec::new(),
            memory_mb: 1024,
            cpus: 1,
            disk_gb: 20,
            base_sha256: "a".repeat(64),
            base_url: "https://example.test/base.img".into(),
            ssh_port: 22200,
            autostart: false,
            ssh_authorized_keys: Vec::new(),
            created_at: "2026-11-21T00:00:00Z".into(),
        }
    }

    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            Self(
                std::env::temp_dir()
                    .join(format!("mistl-vm-checkpoint-{}", store::new_server_id())),
            )
        }

        fn store(&self) -> CheckpointStore {
            install_key(&self.0, ID, &KEY).unwrap();
            CheckpointStore::open(&self.0, ID).unwrap()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn chunk_crypto_is_deterministic_and_authenticates_id_and_content() {
        let plain = b"checkpoint";
        let id = chunk_id(&KEY, plain);
        assert_eq!(id, chunk_id(&KEY, plain));
        assert_ne!(id, chunk_id(&[8; 32], plain));
        assert_ne!(id, chunk_id(&KEY, b"different"));
        let ct = encrypt_chunk(&KEY, &id, plain).unwrap();
        assert_eq!(ct, encrypt_chunk(&KEY, &id, plain).unwrap());
        assert_eq!(decrypt_chunk(&KEY, &id, &ct).unwrap(), plain);
        assert!(decrypt_chunk(&[8; 32], &id, &ct).is_err());
        assert!(decrypt_chunk(&KEY, &chunk_id(&KEY, b"wrong"), &ct).is_err());
        let mut tampered = ct;
        tampered[0] ^= 1;
        assert!(decrypt_chunk(&KEY, &id, &tampered).is_err());
        // The forged id uses the original nonce but a different HMAC.
        let wrong_id = format!("{}{}", &id[..24], "0".repeat(40));
        let forged = encrypt_chunk(&KEY, &wrong_id, plain).unwrap();
        assert!(decrypt_chunk(&KEY, &wrong_id, &forged).is_err());
    }

    #[test]
    fn zero_chunks_use_the_empty_id_and_never_a_file() {
        let zeros = [0; 32];
        assert_eq!(chunk_id(&KEY, &zeros), "");
        let id = hex::encode(chunk_mac(&KEY, &zeros).finalize().into_bytes());
        let ciphertext = encrypt_chunk(&KEY, &id, &zeros).unwrap();
        assert!(decrypt_chunk(&KEY, &id, &ciphertext).is_err());
    }

    #[test]
    fn keys_are_stable_and_reject_replacement() {
        let scratch = Scratch::new();
        let key = load_or_create_key(&scratch.0, ID).unwrap();
        assert_eq!(load_or_create_key(&scratch.0, ID).unwrap(), key);
        install_key(&scratch.0, ID, &key).unwrap();
        let mut other = key;
        other[0] ^= 1;
        assert!(install_key(&scratch.0, ID, &other).is_err());
        assert!(load_or_create_key(&scratch.0, "../invalid").is_err());
    }

    #[test]
    fn manifest_round_trip_and_replication_require_complete_authenticated_chunks() {
        let sender_dir = Scratch::new();
        let sender = sender_dir.store();
        let receiver_dir = Scratch::new();
        let receiver = receiver_dir.store();
        let disk = sender_dir.0.join("disk");
        fs::write(&disk, b"checkpoint").unwrap();
        let mut manifest = sender.prepare(record(), 7, Vec::new()).unwrap();
        sender.capture(&disk, &mut manifest).unwrap();
        sender.commit(&manifest).unwrap();
        let bytes = sender.read_encrypted_manifest(1).unwrap();
        assert_ne!(bytes, encode_manifest(&KEY, &manifest).unwrap());
        assert_eq!(sender.load_manifest(1).unwrap(), manifest);
        let offer = receiver.decode_manifest(&bytes).unwrap();
        assert_eq!(receiver.missing_chunks(&offer).unwrap(), manifest.chunks);
        assert!(receiver.install_manifest(&offer).is_err());
        let id = &offer.chunks[0];
        let chunk = sender.read_encrypted_chunk(id).unwrap();
        let mut tampered = chunk.clone();
        tampered[0] ^= 1;
        assert!(receiver.store_encrypted_chunk(id, &tampered).is_err());
        assert!(!receiver.chunk_path(id).unwrap().exists());
        receiver.store_encrypted_chunk(id, &chunk).unwrap();
        receiver.install_manifest(&offer).unwrap();
        assert_eq!(receiver.load_manifest(1).unwrap(), manifest);
        receiver.install_manifest(&offer).unwrap();
        let mut tampered = bytes;
        tampered[0] ^= 1;
        assert!(receiver.decode_manifest(&tampered).is_err());
        let mut invalid = manifest.clone();
        invalid.record.server_id = "abcdef1234567890".into();
        assert!(
            receiver
                .decode_manifest(&encode_manifest(&KEY, &invalid).unwrap())
                .is_err()
        );
        let mut conflict = manifest;
        conflict.epoch += 1;
        assert!(receiver.install_manifest(&conflict).is_err());
    }

    #[test]
    fn reconstructs_a_synthetic_overlay_including_zeros_and_the_short_tail() {
        let scratch = Scratch::new();
        let checkpoints = scratch.store();
        let disk = scratch.0.join("disk");
        let mut original = vec![1; CHUNK_SIZE * 2];
        original.resize(CHUNK_SIZE * 3, 0);
        original.extend_from_slice(b"tail");
        fs::write(&disk, &original).unwrap();
        let mut manifest = checkpoints.prepare(record(), 0, Vec::new()).unwrap();
        let stats = checkpoints.capture(&disk, &mut manifest).unwrap();
        assert_eq!(stats.chunks, 4);
        assert_eq!(stats.new_chunks, 2);
        assert_eq!(stats.bytes, original.len() as u64);
        assert_eq!(stats.new_bytes, CHUNK_SIZE as u64 + 4);
        assert_eq!(manifest.chunks[0], manifest.chunks[1]);
        assert!(manifest.chunks[2].is_empty());
        checkpoints.commit(&manifest).unwrap();
        fs::remove_file(&disk).unwrap();
        let manifest = checkpoints.load_manifest(1).unwrap();
        checkpoints.reconstruct(&manifest, &disk).unwrap();
        assert_eq!(fs::read(&disk).unwrap(), original);
    }

    #[test]
    fn retention_preserves_shared_chunks_and_collects_orphans() {
        let scratch = Scratch::new();
        let checkpoints = scratch.store();
        let disk = scratch.0.join("disk");
        let mut manifests = Vec::new();
        for value in [b"old", b"new", b"new"] {
            fs::write(&disk, value).unwrap();
            let mut manifest = checkpoints.prepare(record(), 0, Vec::new()).unwrap();
            let stats = checkpoints.capture(&disk, &mut manifest).unwrap();
            if manifest.seq == 3 {
                assert_eq!(manifest.parent_seq, Some(2));
                assert_eq!(stats.new_chunks, 0);
            }
            checkpoints.commit(&manifest).unwrap();
            manifests.push(manifest);
        }
        let id = chunk_id(&KEY, b"orphan");
        let ciphertext = encrypt_chunk(&KEY, &id, b"orphan").unwrap();
        checkpoints.store_encrypted_chunk(&id, &ciphertext).unwrap();
        assert!(checkpoints.prune(0, |_| false).is_err());
        checkpoints.prune(2, |_| false).unwrap();
        assert_eq!(
            checkpoints.list_manifests().unwrap(),
            [manifests[2].clone(), manifests[1].clone()]
        );
        assert!(
            checkpoints
                .read_encrypted_chunk(&manifests[0].chunks[0])
                .is_err()
        );
        assert!(checkpoints.read_encrypted_chunk(&id).is_err());
        assert!(
            checkpoints
                .read_encrypted_chunk(&manifests[1].chunks[0])
                .is_ok()
        );
        checkpoints.prune(1, |_| false).unwrap();
        assert_eq!(checkpoints.sequences().unwrap(), [3]);
        assert!(
            checkpoints
                .read_encrypted_chunk(&manifests[2].chunks[0])
                .is_ok()
        );
        assert_eq!(checkpoints.prepare(record(), 9, Vec::new()).unwrap().seq, 4);
    }

    #[test]
    fn election_restore_selects_newest_complete_checkpoint_at_or_above_the_floor() {
        let scratch = Scratch::new();
        let checkpoints = scratch.store();
        let disk = scratch.0.join("disk");
        let mut manifests = Vec::new();
        for content in [b"one", b"two", b"new"] {
            fs::write(&disk, content).unwrap();
            let mut manifest = checkpoints.prepare(record(), 2, Vec::new()).unwrap();
            checkpoints.capture(&disk, &mut manifest).unwrap();
            checkpoints.commit(&manifest).unwrap();
            manifests.push(manifest);
        }
        assert_eq!(checkpoints.newest_complete(1, |_| true).unwrap(), 3);
        let path = checkpoints.chunk_path(&manifests[2].chunks[0]).unwrap();
        fs::write(path, [0; 3 + TAG_SIZE]).unwrap();
        assert_eq!(checkpoints.newest_complete(2, |_| true).unwrap(), 2);
        assert!(checkpoints.newest_complete(3, |_| true).is_err());
    }

    #[test]
    fn only_chunks_referenced_by_the_requested_kept_manifest_are_served() {
        let scratch = Scratch::new();
        let checkpoints = scratch.store();
        let disk = scratch.0.join("disk");
        fs::write(&disk, b"referenced").unwrap();
        let mut first = checkpoints.prepare(record(), 1, Vec::new()).unwrap();
        checkpoints.capture(&disk, &mut first).unwrap();
        checkpoints.commit(&first).unwrap();
        let mut second = checkpoints.prepare(record(), 1, Vec::new()).unwrap();
        checkpoints.capture(&disk, &mut second).unwrap();
        checkpoints.commit(&second).unwrap();
        let orphan = chunk_id(&KEY, b"abandoned");
        checkpoints
            .store_encrypted_chunk(
                &orphan,
                &encrypt_chunk(&KEY, &orphan, b"abandoned").unwrap(),
            )
            .unwrap();
        let id = &first.chunks[0];
        assert!(checkpoints.read_referenced_chunk(1, id).is_ok());
        assert!(checkpoints.read_referenced_chunk(1, &orphan).is_err());
        checkpoints.collect_chunks().unwrap();
        assert!(checkpoints.read_encrypted_chunk(&orphan).is_err());
        checkpoints.prune(1, |_| false).unwrap();
        assert!(checkpoints.read_referenced_chunk(1, id).is_err());
        assert!(checkpoints.read_referenced_chunk(2, id).is_ok());
    }

    #[test]
    fn reusing_a_corrupt_local_chunk_requires_fetching_it_again() {
        let scratch = Scratch::new();
        let checkpoints = scratch.store();
        let disk = scratch.0.join("disk");
        fs::write(&disk, b"verify").unwrap();
        let mut manifest = checkpoints.prepare(record(), 1, Vec::new()).unwrap();
        checkpoints.capture(&disk, &mut manifest).unwrap();
        checkpoints.commit(&manifest).unwrap();
        let id = &manifest.chunks[0];
        let valid = checkpoints.read_encrypted_chunk(id).unwrap();
        let mut invalid = valid.clone();
        invalid[0] ^= 1;
        let path = checkpoints.chunk_path(id).unwrap();
        fs::write(&path, invalid).unwrap();
        assert!(checkpoints.missing_chunks(&manifest).unwrap().is_empty());
        assert_eq!(
            checkpoints
                .verified_missing_chunks(&manifest, true)
                .unwrap(),
            [id.as_str()]
        );
        assert!(!path.exists());
        checkpoints.store_encrypted_chunk(id, &valid).unwrap();
        assert!(
            checkpoints
                .verified_missing_chunks(&manifest, true)
                .unwrap()
                .is_empty()
        );
    }
}
