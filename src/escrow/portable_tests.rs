use super::*;
use crate::escrow::portable::{Reader, Writer};
use crate::escrow::portable_io;
use crate::escrow::repository::ImportOutcome::{Imported, Skipped, Unchanged};
use std::io::Cursor;

fn encoded(head: &Head, manifest: &[u8], blob: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new(Vec::new(), 123).unwrap();
    writer.entry("test-room", head, manifest, blob).unwrap();
    writer.finish().unwrap()
}

fn raw_entries(entries: &[(&str, &Head, &[u8], &[u8])], count: u64) -> Vec<u8> {
    let mut bytes =
        b"{\"format\":\"mistl-escrow-export\",\"version\":1,\"created_ms\":123}\n".to_vec();
    for (room, head, manifest, blob) in entries {
        bytes.extend(
            serde_json::to_vec(&json!({"room":room,"head":head,"manifest":manifest.len()}))
                .unwrap(),
        );
        bytes.push(b'\n');
        bytes.extend_from_slice(manifest);
        bytes.extend_from_slice(blob);
    }
    bytes.extend(format!("{{\"end\":true,\"entries\":{count}}}\n").as_bytes());
    bytes
}

fn validate_file(bytes: &[u8]) -> anyhow::Result<usize> {
    let mut reader = Reader::new(Cursor::new(bytes), 32 * 1024 * 1024)?;
    let mut count = 0;
    while reader.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

#[test]
fn escrow_portable_round_trip() {
    let blob = vec![42; CHUNK_SIZE + 9];
    let (head, manifest) = fixture(&blob, 8, Some("ab".repeat(32)));
    let bytes = encoded(&head, &manifest, &blob);
    let mut reader = Reader::new(Cursor::new(bytes), blob.len() as u64).unwrap();
    let entry = reader.next().unwrap().unwrap();
    assert_eq!(entry.room, "test-room");
    assert_eq!(entry.head, head);
    assert_eq!(entry.manifest, manifest);
    assert_eq!(entry.blob, blob);
    assert!(reader.next().unwrap().is_none());
    assert!(reader.next().unwrap().is_none());
    let empty = Writer::new(Vec::new(), 0).unwrap().finish().unwrap();
    assert_eq!(validate_file(&empty).unwrap(), 0);
}

#[test]
fn escrow_portable_rejects_truncation_count_and_trailing_garbage() {
    let (head, manifest) = fixture(b"ciphertext", 1, None);
    let bytes = encoded(&head, &manifest, b"ciphertext");
    for end in [0, 8, bytes.len() - 1, bytes.len() - 30] {
        assert!(validate_file(&bytes[..end]).is_err());
    }
    let wrong_count = raw_entries(&[("test-room", &head, &manifest, b"ciphertext")], 2);
    assert!(
        validate_file(&wrong_count)
            .unwrap_err()
            .to_string()
            .contains("count mismatch")
    );
    let mut garbage = bytes;
    garbage.extend_from_slice(b"junk");
    assert!(
        validate_file(&garbage)
            .unwrap_err()
            .to_string()
            .contains("trailing bytes")
    );
}

#[test]
fn escrow_portable_rejects_tampering_signatures_and_chunk_cids() {
    let (mut head, manifest) = fixture(b"ciphertext", 1, None);
    let corrupted = raw_entries(&[("test-room", &head, &manifest, b"ciphertexX")], 1);
    assert!(format!("{:#}", validate_file(&corrupted).unwrap_err()).contains("hash_mismatch"));
    head.sig = URL_SAFE_NO_PAD.encode([0; 64]);
    let bad_sig = raw_entries(&[("test-room", &head, &manifest, b"ciphertext")], 1);
    assert!(format!("{:#}", validate_file(&bad_sig).unwrap_err()).contains("bad_sig"));
    let (mut head, _) = fixture(b"ciphertext", 1, None);
    let bad_manifest = serde_cbor::to_vec(&FileManifest {
        name: "tc-escrow.bin".into(),
        size: 10,
        chunks: vec![compute_cid(b"wrong", MULTICODEC_RAW)],
    })
    .unwrap();
    head.cid = compute_cid(&bad_manifest, MULTICODEC_DAG_CBOR);
    sign(&mut head, [7; 32]);
    let bad_chunks = raw_entries(&[("test-room", &head, &bad_manifest, b"ciphertext")], 1);
    assert!(format!("{:#}", validate_file(&bad_chunks).unwrap_err()).contains("chunk CID"));
}

#[test]
fn escrow_portable_rejects_oversized_lines_and_payloads_before_allocation() {
    let mut huge_header = vec![b' '; 4096];
    huge_header.push(b'\n');
    assert!(
        validate_file(&huge_header)
            .unwrap_err()
            .to_string()
            .contains("oversized")
    );
    let mut huge = Writer::new(Vec::new(), 0).unwrap().finish().unwrap();
    huge.truncate(huge.iter().position(|b| *b == b'\n').unwrap() + 1);
    huge.extend(vec![b' '; 32768]);
    huge.push(b'\n');
    assert!(
        validate_file(&huge)
            .unwrap_err()
            .to_string()
            .contains("oversized")
    );
    let (head, manifest) = fixture(b"ciphertext", 1, None);
    let bytes = encoded(&head, &manifest, b"ciphertext");
    let mut reader = Reader::new(Cursor::new(bytes), 9).unwrap();
    assert!(format!("{:#}", reader.next().err().unwrap()).contains("too_large"));
    let mut bytes =
        b"{\"format\":\"mistl-escrow-export\",\"version\":1,\"created_ms\":0}\n".to_vec();
    bytes.extend(serde_json::to_vec(&json!({"room":"r","head":head,"manifest":131073})).unwrap());
    bytes.push(b'\n');
    assert!(format!("{:#}", validate_file(&bytes).unwrap_err()).contains("oversized manifest"));
}

#[test]
fn escrow_portable_rejects_duplicate_slots_and_unknown_shapes() {
    let (head, manifest) = fixture(b"ciphertext", 1, None);
    let entry = (
        "test-room",
        &head,
        manifest.as_slice(),
        b"ciphertext".as_slice(),
    );
    let duplicate = raw_entries(&[entry, entry], 2);
    assert!(format!("{:#}", validate_file(&duplicate).unwrap_err()).contains("duplicate slot"));
    let invalid_room = raw_entries(&[("room with spaces", &head, &manifest, b"ciphertext")], 1);
    assert!(validate_file(&invalid_room).is_err());
    for bytes in [
        b"{\"format\":\"other\",\"version\":1,\"created_ms\":0}\n".as_slice(),
        b"{\"format\":\"mistl-escrow-export\",\"version\":2,\"created_ms\":0}\n".as_slice(),
        b"{\"format\":\"mistl-escrow-export\",\"version\":1,\"created_ms\":0}\n{\"other\":1}\n"
            .as_slice(),
    ] {
        assert!(validate_file(bytes).is_err());
    }
}

#[test]
fn escrow_import_repository_history_restart_and_blocks() {
    let dir = scratch();
    let cfg = EscrowConfig::default();
    let repo = Repository::open(dir.clone(), cfg.clone()).unwrap();
    let (first, manifest) = fixture(b"first", 8, Some("ab".repeat(32)));
    assert_eq!(
        repo.import("test-room", &first, b"first", &manifest)
            .unwrap(),
        Imported
    );
    assert_eq!(
        repo.import("test-room", &first, b"first", &manifest)
            .unwrap(),
        Unchanged
    );
    let (next, next_manifest) = fixture(b"next", 9, Some(first.hash()));
    assert_eq!(
        repo.import("test-room", &next, b"next", &next_manifest)
            .unwrap(),
        Imported
    );
    assert_eq!(
        repo.import("test-room", &first, b"first", &manifest)
            .unwrap(),
        Unchanged
    );
    drop(repo);
    let repo = Repository::open(dir.clone(), cfg.clone()).unwrap();
    assert_eq!(repo.head("test-room", &first.slot), Some(next.clone()));
    // An old generation is still served, but export must report it as changed.
    assert!(repo.current_bytes("test-room", &first).unwrap().is_none());
    assert!(
        repo.generation_bytes("test-room", &first)
            .unwrap()
            .is_some()
    );
    assert_eq!(repo.block("test-room", &first.cid), Some(manifest));
    let (jump, bytes) = fixture(b"jump", 19, Some("ef".repeat(32)));
    assert_eq!(
        repo.import("test-room", &jump, b"jump", &bytes).unwrap(),
        Imported
    );
    assert!(repo.block("test-room", &next.cid).is_none());
    assert_eq!(
        repo.import("test-room", &next, b"next", &next_manifest)
            .unwrap(),
        Skipped("older")
    );
    let (conflict, conflict_bytes) = fixture(b"conflict", 19, None);
    assert_eq!(
        repo.import("test-room", &conflict, b"conflict", &conflict_bytes)
            .unwrap(),
        Skipped("conflict")
    );
    let mut other = jump.clone();
    sign(&mut other, [8; 32]);
    assert_eq!(
        repo.import("test-room", &other, b"jump", &bytes).unwrap(),
        Skipped("writer_mismatch")
    );
    drop(repo);
    let repo = Repository::open(dir.clone(), cfg).unwrap();
    assert_eq!(repo.head("test-room", &jump.slot), Some(jump.clone()));
    assert_eq!(repo.block("test-room", &jump.cid), Some(bytes));
    assert_eq!(
        repo.block("test-room", &compute_cid(b"jump", MULTICODEC_RAW)),
        Some(b"jump".to_vec())
    );
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_import_quotas_include_pending_reservations() {
    let dir = scratch();
    let cfg = EscrowConfig {
        max_slots_per_room: 1,
        ..Default::default()
    };
    let repo = Repository::open(dir.clone(), cfg).unwrap();
    let (first, manifest) = fixture(b"first", 1, None);
    let pending = repo.reserve("test-room", &first).unwrap().unwrap();
    let (mut other, bytes) = fixture(b"other", 1, None);
    other.slot = "cd".repeat(16);
    sign(&mut other, [7; 32]);
    assert_eq!(
        repo.import("test-room", &other, b"other", &bytes).unwrap(),
        Skipped("quota")
    );
    drop(pending);
    assert_eq!(
        repo.import("test-room", &first, b"first", &manifest)
            .unwrap(),
        Imported
    );
    assert_eq!(
        repo.import("test-room", &other, b"other", &bytes).unwrap(),
        Skipped("quota")
    );
    drop(repo);
    std::fs::remove_dir_all(&dir).unwrap();
    let cfg = EscrowConfig {
        max_total_bytes: first.size + manifest.len() as u64 - 1,
        ..Default::default()
    };
    let repo = Repository::open(dir.clone(), cfg).unwrap();
    assert_eq!(
        repo.import("test-room", &first, b"first", &manifest)
            .unwrap(),
        Skipped("quota")
    );
    drop(repo);
    std::fs::remove_dir_all(&dir).unwrap();
    let cfg = EscrowConfig {
        max_total_bytes: first.size + crate::escrow::repository::manifest_limit(first.size) as u64,
        ..Default::default()
    };
    let repo = Repository::open(dir.clone(), cfg).unwrap();
    let pending = repo.reserve("test-room", &first).unwrap().unwrap();
    assert_eq!(
        repo.import("test-room", &other, b"other", &bytes).unwrap(),
        Skipped("quota")
    );
    drop(pending);
    assert_eq!(
        repo.import("test-room", &other, b"other", &bytes).unwrap(),
        Imported
    );
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_import_failure_disables_writes_until_restart() {
    let dir = scratch();
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (head, bytes) = fixture(b"saved", 4, Some("ab".repeat(32)));
    repo.inject_post_publish_failure();
    assert_eq!(
        repo.import("test-room", &head, b"saved", &bytes)
            .unwrap_err()
            .code,
        "fetch_failed"
    );
    assert_eq!(
        repo.import("test-room", &head, b"saved", &bytes)
            .unwrap_err()
            .code,
        "quota"
    );
    drop(repo);
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    assert_eq!(
        repo.import("test-room", &head, b"saved", &bytes).unwrap(),
        Unchanged
    );
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_portable_service_transfer_room_filter_and_atomic_files() {
    let dir = scratch();
    let a = Service::open(dir.join("a"), EscrowConfig::default()).unwrap();
    let b = Service::open(dir.join("b"), EscrowConfig::default()).unwrap();
    let (head, manifest) = fixture(b"ciphertext", 4, Some("ab".repeat(32)));
    a.repo
        .import("test-room", &head, b"ciphertext", &manifest)
        .unwrap();
    a.repo
        .import("other-room", &head, b"ciphertext", &manifest)
        .unwrap();
    let path = dir.join("portable.mistl-escrow");
    let result =
        portable_io::handle(&a, true, json!({"path":path,"rooms":["test-room"]}), &[]).unwrap();
    assert_eq!(result["slots"], 1);
    assert_eq!(result["bytes"], 10);
    assert_eq!(validate_file(&std::fs::read(&path).unwrap()).unwrap(), 1);
    assert!(portable_io::handle(&a, true, json!({"path":path}), &[]).is_err());
    let result =
        portable_io::handle(&b, false, json!({"path":path,"rooms":["other-room"]}), &[]).unwrap();
    assert_eq!(result["imported"], 0);
    let result =
        portable_io::handle(&b, false, json!({"path":path}), &["test-room".into()]).unwrap();
    assert_eq!(result["imported"], 1);
    assert_eq!(result["rooms"][0]["configured"], true);
    assert_eq!(b.repo.head("test-room", &head.slot), Some(head.clone()));
    assert_eq!(b.repo.block("test-room", &head.cid), Some(manifest));
    let result = portable_io::handle(&b, false, json!({"path":path}), &[]).unwrap();
    assert_eq!(result["unchanged"], 1);
    portable_io::handle(&a, true, json!({"path":path,"overwrite":true}), &[]).unwrap();
    assert_eq!(validate_file(&std::fs::read(&path).unwrap()).unwrap(), 2);
    let blocked = dir.join("directory");
    std::fs::create_dir(&blocked).unwrap();
    assert!(portable_io::handle(&a, true, json!({"path":blocked,"overwrite":true}), &[]).is_err());
    assert!(std::fs::read_dir(&dir).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
    assert!(portable_io::handle(&a, true, json!({"path":"relative"}), &[]).is_err());
    assert!(
        portable_io::handle(&a, false, json!({"path":path,"rooms":["bad room"]}), &[]).is_err()
    );
    drop((a, b));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_portable_validates_all_rooms_before_applying_any_entry() {
    let dir = scratch();
    let service = Service::open(dir.join("repo"), EscrowConfig::default()).unwrap();
    let (head, bytes) = fixture(b"saved", 1, None);
    let mut bad = head.clone();
    bad.sig = URL_SAFE_NO_PAD.encode([0; 64]);
    let path = dir.join("bad.mistl-escrow");
    let bytes = raw_entries(
        &[
            ("test-room", &head, &bytes, b"saved"),
            ("other-room", &bad, &bytes, b"saved"),
        ],
        2,
    );
    std::fs::write(&path, bytes).unwrap();
    assert!(
        portable_io::handle(
            &service,
            false,
            json!({"path":path,"rooms":["test-room"]}),
            &[]
        )
        .is_err()
    );
    assert!(service.repo.head("test-room", &head.slot).is_none());
    assert_eq!(std::fs::read_dir(dir.join("repo")).unwrap().count(), 2);
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_restart_removes_interrupted_import_spool() {
    let dir = scratch();
    drop(Service::open(dir.join("repo"), EscrowConfig::default()).unwrap());
    let spool = dir.join("repo").join("import.0123456789abcdef.tmp");
    let unrelated = dir.join("repo").join("import.keep");
    std::fs::write(&spool, b"partial").unwrap();
    std::fs::write(&unrelated, b"operator file").unwrap();
    let service = Service::open(dir.join("repo"), EscrowConfig::default()).unwrap();
    assert!(!spool.exists());
    assert!(unrelated.exists());
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}
