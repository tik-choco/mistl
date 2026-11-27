//! Persistent VM records (`<data_dir>/vm/<server_id>/vm.json`).
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};

const RECORD_FILE: &str = "vm.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VmRecord {
    pub server_id: String,
    pub name: String,
    /// DID of the identity that requested the VM.
    pub owner_did: String,
    /// Extra DIDs allowed to reach the guest's SSH through the tunnel.
    #[serde(default)]
    pub allowed_dids: Vec<String>,
    pub memory_mb: u32,
    pub cpus: u32,
    pub disk_gb: u32,
    pub base_sha256: String,
    #[serde(default)]
    pub base_url: String,
    pub ssh_port: u16,
    #[serde(default)]
    pub autostart: bool,
    /// OpenSSH public keys installed for the guest user `mistl`.
    #[serde(default)]
    pub ssh_authorized_keys: Vec<String>,
    /// RFC 3339.
    pub created_at: String,
}

pub fn vm_root() -> Result<PathBuf> {
    let root = crate::config::data_dir()?.join("vm");
    crate::statefile::create_private_dir(&root)
        .with_context(|| format!("creating VM data directory {}", root.display()))?;
    Ok(root)
}

pub fn vm_dir(root: &Path, server_id: &str) -> PathBuf {
    root.join(server_id)
}

/// Random 64-bit id in lowercase hexadecimal.
pub fn new_server_id() -> String {
    format!("{:016x}", OsRng.next_u64())
}

pub fn valid_server_id(id: &str) -> bool {
    id.len() == 16 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

pub fn load_all(root: &Path) -> Result<Vec<VmRecord>> {
    load_all_except(root, None)
}

pub(super) fn load_all_except(root: &Path, excluded_id: Option<&str>) -> Result<Vec<VmRecord>> {
    let entries = fs::read_dir(root)
        .with_context(|| format!("reading VM data directory {}", root.display()))?;
    let mut records = Vec::new();
    for entry in entries {
        let record = (|| -> Result<Option<VmRecord>> {
            let entry = entry?;
            let name = entry.file_name();
            if matches!(name.to_str(), Some("images" | "ssh")) || !entry.file_type()?.is_dir() {
                return Ok(None);
            }
            let id = name.to_str().context("VM directory name is not UTF-8")?;
            if excluded_id == Some(id) {
                return Ok(None);
            }
            if entry.path().join("ha.json").is_file()
                && !entry.path().join(RECORD_FILE).try_exists()?
            {
                return Ok(None);
            }
            load(root, id).map(Some)
        })();
        match record {
            Ok(Some(record)) => records.push(record),
            Ok(None) => {}
            Err(error) => tracing::warn!("skipping VM directory: {error:#}"),
        }
    }
    records.sort_unstable_by(|a, b| a.server_id.cmp(&b.server_id));
    Ok(records)
}

pub fn load(root: &Path, server_id: &str) -> Result<VmRecord> {
    ensure!(valid_server_id(server_id), "invalid VM server id");
    let path = vm_dir(root, server_id).join(RECORD_FILE);
    let bytes = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let record: VmRecord = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing VM record {}", path.display()))?;
    ensure!(
        record.server_id == server_id,
        "VM record id does not match directory {server_id}"
    );
    Ok(record)
}

pub fn save(root: &Path, record: &VmRecord) -> Result<()> {
    ensure!(valid_server_id(&record.server_id), "invalid VM server id");
    let path = vm_dir(root, &record.server_id).join(RECORD_FILE);
    let bytes = serde_json::to_vec_pretty(record)?;
    crate::statefile::write_private(&path, &bytes)
        .with_context(|| format!("writing VM record {}", path.display()))
}

/// Deletes the VM directory (record, disk, seed, logs).
pub fn remove(root: &Path, server_id: &str) -> Result<()> {
    ensure!(valid_server_id(server_id), "invalid VM server id");
    let dir = vm_dir(root, server_id);
    fs::remove_dir_all(&dir).with_context(|| format!("removing VM directory {}", dir.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_ids_are_lowercase_hex() {
        assert!(valid_server_id("0000abcdef123456"));
        let id = new_server_id();
        assert!(valid_server_id(&id));
        assert_ne!(id, new_server_id());
        for invalid in [
            "",
            "0000abcdef12345",
            "0000abcdef1234567",
            "0000ABCDEF123456",
            "0000abcdef12345g",
            "../123456789abcd",
            "0000a@cdef123456",
        ] {
            assert!(!valid_server_id(invalid), "{invalid:?}");
        }
    }

    #[test]
    fn records_round_trip_and_listing_skips_corrupt_directories() {
        let root = std::env::temp_dir().join(format!("mistl-vm-store-{}", new_server_id()));
        let mut record = VmRecord {
            server_id: new_server_id(),
            name: "example".into(),
            owner_did: "did:key:test".into(),
            allowed_dids: vec![],
            memory_mb: 1024,
            cpus: 1,
            disk_gb: 20,
            base_sha256: "a".repeat(64),
            base_url: "https://example.test/base.img".into(),
            ssh_port: 22200,
            autostart: false,
            ssh_authorized_keys: vec![],
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        save(&root, &record).unwrap();
        let mut legacy = serde_json::to_value(&record).unwrap();
        legacy.as_object_mut().unwrap().remove("base_url");
        assert_eq!(
            serde_json::from_value::<VmRecord>(legacy).unwrap().base_url,
            ""
        );
        record.autostart = true;
        save(&root, &record).unwrap();
        assert_eq!(load(&root, &record.server_id).unwrap(), record);

        let mismatched = vm_dir(&root, "1111111111111111");
        let corrupt = vm_dir(&root, "2222222222222222");
        for dir in [
            &mismatched,
            &corrupt,
            &root.join("images"),
            &root.join("ssh"),
        ] {
            fs::create_dir(dir).unwrap();
        }
        fs::write(
            mismatched.join(RECORD_FILE),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        fs::write(corrupt.join(RECORD_FILE), b"broken").unwrap();
        assert_eq!(load_all(&root).unwrap(), vec![record.clone()]);

        for invalid in ["../anything", "images", "ssh"] {
            assert!(load(&root, invalid).is_err());
            assert!(remove(&root, invalid).is_err());
        }
        assert!(root.join("images").is_dir());
        remove(&root, &record.server_id).unwrap();
        assert!(load_all(&root).unwrap().is_empty());
        record.server_id = "../anything".into();
        assert!(save(&root, &record).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
