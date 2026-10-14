use super::*;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(
        dir.join("raft.json"),
        NodeId("a".into()),
        vec![NodeId("a".into())],
    )
    .unwrap()
}

#[test]
fn more_than_lifetime_log_limit_compacts_and_restarts_at_absolute_indices() {
    let dir = TempDir::new();
    let mut engine = open(dir.path());
    engine.step(Event::ElectionTimeout).unwrap();
    assert_eq!(engine.role(), Role::Leader);
    for index in 1..=4200u64 {
        engine
            .step(Event::ClientPropose {
                command: index.to_le_bytes().to_vec(),
            })
            .unwrap();
        assert_eq!(engine.commit_index(), index);
        engine.mark_applied(index).unwrap();
        if engine.needs_compaction() {
            engine.compact(index.to_le_bytes().to_vec()).unwrap();
        }
        assert!(engine.log().len() < 128);
        if index % 1000 == 0 {
            engine = open(dir.path());
            assert_eq!(engine.commit_index(), index);
            assert_eq!(engine.last_applied(), index);
            engine.step(Event::ElectionTimeout).unwrap();
        }
    }
    engine.compact(b"final".to_vec()).unwrap();
    let durable = std::fs::read(dir.path().join("raft.json")).unwrap();
    assert!(durable.len() < 1024);
    let restored = open(dir.path());
    assert_eq!(restored.snapshot().unwrap().last_included_index, 4200);
    assert_eq!(restored.snapshot().unwrap().data.as_slice(), b"final");
    assert!(restored.log().is_empty());
}

#[test]
fn snapshot_missing_or_corrupt_fails_closed_and_orphans_are_recovered() {
    let dir = TempDir::new();
    let mut engine = open(dir.path());
    engine.step(Event::ElectionTimeout).unwrap();
    engine
        .step(Event::ClientPropose { command: vec![1] })
        .unwrap();
    engine.mark_applied(1).unwrap();
    engine.compact(b"state".to_vec()).unwrap();
    let file = engine.saved_snapshot.as_ref().unwrap().path(&engine.path);
    let orphan = dir
        .path()
        .join(format!("raft.snapshot-{}.bin", "ab".repeat(32)));
    std::fs::write(&orphan, b"orphan").unwrap();
    drop(engine);
    let engine = open(dir.path());
    assert!(!orphan.exists());
    drop(engine);
    std::fs::write(&file, b"wrong").unwrap();
    assert!(
        Engine::open(
            dir.path().join("raft.json"),
            NodeId("a".into()),
            vec![NodeId("a".into())]
        )
        .is_err()
    );
    std::fs::remove_file(file).unwrap();
    assert!(
        Engine::open(
            dir.path().join("raft.json"),
            NodeId("a".into()),
            vec![NodeId("a".into())]
        )
        .is_err()
    );
}

#[test]
fn snapshot_write_failure_never_advertises_snapshot() {
    let dir = TempDir::new();
    let mut engine = open(dir.path());
    engine.step(Event::ElectionTimeout).unwrap();
    engine
        .step(Event::ClientPropose { command: vec![1] })
        .unwrap();
    engine.mark_applied(1).unwrap();
    let blocked = dir.path().join(format!(
        "raft.snapshot-{}.bin",
        crate::escrow::protocol::digest(b"snapshot")
    ));
    std::fs::create_dir(blocked).unwrap();
    assert!(engine.compact(b"snapshot".to_vec()).is_err());
    assert!(engine.stopped());
    assert!(engine.snapshot().is_none());
    assert!(engine.step(Event::HeartbeatTimeout).is_err());
    let restored = open(dir.path());
    assert!(restored.snapshot().is_none());
    assert_eq!(restored.log().len(), 1);
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
