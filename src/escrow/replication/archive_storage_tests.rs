use super::tests::fixture;
use super::*;

#[test]
fn collection_reclaims_quota_retains_shared_blocks_and_pinned_transfers() {
    let dir = TempDir::new();
    let cfg = EscrowConfig {
        max_total_bytes: 4096,
        ..Default::default()
    };
    let archive = Archive::open(dir.path().to_path_buf(), cfg.clone()).unwrap();
    let (old, manifest) = fixture(b"ciphertext", 1);
    let (new, new_manifest) = fixture(b"ciphertext", 2);
    archive
        .stage("room", &old, b"ciphertext", &manifest)
        .unwrap();
    archive
        .stage("room", &new, b"ciphertext", &new_manifest)
        .unwrap();
    let old_size = archive.total_bytes();
    let lease = archive.pin_heads(std::slice::from_ref(&old)).unwrap();
    let retained = HashSet::from([new.hash()]);
    assert_eq!(archive.collect(&retained).unwrap(), 0);
    drop(lease);
    assert_eq!(archive.collect(&retained).unwrap(), 1);
    assert!(archive.total_bytes() < old_size);
    assert!(!archive.has(&old));
    assert!(archive.has(&new));
    let parsed: FileManifest = serde_cbor::from_slice(&new_manifest).unwrap();
    assert_eq!(
        archive.block("room", &parsed.chunks[0]).unwrap(),
        b"ciphertext"
    );
    assert_eq!(archive.collect(&HashSet::new()).unwrap(), 1);
    assert_eq!(archive.total_bytes(), 0);
    drop(archive);
    let archive = Archive::open(dir.path().to_path_buf(), cfg).unwrap();
    assert_eq!(archive.total_bytes(), 0);
    archive
        .stage("room", &old, b"ciphertext", &manifest)
        .unwrap();
}

#[test]
fn interrupted_collection_recovers_retired_directory_on_restart() {
    let dir = TempDir::new();
    let (head, manifest) = fixture(b"old", 1);
    let archive = Archive::open(dir.path().to_path_buf(), EscrowConfig::default()).unwrap();
    archive.stage("room", &head, b"old", &manifest).unwrap();
    drop(archive);
    let retired = dir.path().join(format!(".gc-{}", head.hash()));
    std::fs::rename(dir.path().join(head.hash()), &retired).unwrap();
    let archive = Archive::open(dir.path().to_path_buf(), EscrowConfig::default()).unwrap();
    assert!(!retired.exists());
    assert_eq!(archive.total_bytes(), 0);
    assert!(!archive.has(&head));
}

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("mistl-storage-{:016x}", rand::random::<u64>()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
