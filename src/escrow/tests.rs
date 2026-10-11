use super::protocol::{check_cas, digest};
use super::repository::{validate_blob, validate_manifest};
use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, MULTICODEC_RAW, compute_cid};
use mistlib_core::storage::{CHUNK_SIZE, FileManifest};
use tokio::sync::Semaphore;

fn vector() -> Value {
    serde_json::from_str(include_str!("fixtures/vectors.json")).unwrap()
}
fn sign(head: &mut Head, seed: [u8; 32]) {
    let key = SigningKey::from_bytes(&seed);
    head.writer = URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes());
    head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
}
fn fixture(blob: &[u8], seq: u64, parent: Option<String>) -> (Head, Vec<u8>) {
    let manifest = FileManifest {
        name: "tc-escrow.bin".into(),
        size: blob.len() as u64,
        chunks: blob
            .chunks(CHUNK_SIZE)
            .map(|c| compute_cid(c, MULTICODEC_RAW))
            .collect(),
    };
    let bytes = serde_cbor::to_vec(&manifest).unwrap();
    let mut head: Head = serde_json::from_value(vector()["head_signed"].clone()).unwrap();
    head.seq = seq;
    head.parent = parent;
    head.cid = compute_cid(&bytes, MULTICODEC_DAG_CBOR);
    head.size = blob.len() as u64;
    head.sha256 = digest(blob);
    sign(&mut head, [7; 32]);
    (head, bytes)
}
fn scratch() -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("mistl-escrow-test-{:016x}", rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
fn accept(repo: &Repository, head: &Head, blob: &[u8], bytes: &[u8]) {
    let reservation = repo.reserve("test-room", head).unwrap().unwrap();
    repo.commit("test-room", head, blob, bytes).unwrap();
    drop(reservation);
}

#[test]
fn escrow_vector_is_byte_exact() {
    use aes_gcm::{
        Aes256Gcm, KeyInit, Nonce,
        aead::{Aead, Payload},
    };
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let v = vector();
    let mut master = [0; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(
        b"tc-escrow/v1\ntc-linkai\nTest-Room_42",
        b"tc-escrow/v1/salt",
        200000,
        &mut master,
    );
    assert_eq!(hex::encode(master), v["master_hex"]);
    let hkdf = |info: &[u8]| {
        let mut extract = <Hmac<Sha256> as Mac>::new_from_slice(&[0; 32]).unwrap();
        extract.update(&master);
        let prk = extract.finalize().into_bytes();
        let mut expand = <Hmac<Sha256> as Mac>::new_from_slice(&prk).unwrap();
        expand.update(info);
        expand.update(&[1]);
        expand.finalize().into_bytes().to_vec()
    };
    assert_eq!(hex::encode(&hkdf(b"tc-escrow/v1/slot")[..16]), v["slot"]);
    let enc = hkdf(b"tc-escrow/v1/enc");
    assert_eq!(hex::encode(&enc), v["encKey_hex"]);
    let seed = hkdf(b"tc-escrow/v1/write");
    assert_eq!(hex::encode(&seed), v["writeSeed_hex"]);
    let key = SigningKey::from_bytes(&seed.try_into().unwrap());
    assert_eq!(
        URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
        v["writer_b64u"]
    );
    let iv = hex::decode(v["iv_hex"].as_str().unwrap()).unwrap();
    let mut blob = iv.clone();
    blob.extend(
        Aes256Gcm::new_from_slice(&enc)
            .unwrap()
            .encrypt(
                Nonce::from_slice(&iv),
                Payload {
                    msg: v["plaintext_utf8"].as_str().unwrap().as_bytes(),
                    aad: v["aad_utf8"].as_str().unwrap().as_bytes(),
                },
            )
            .unwrap(),
    );
    assert_eq!(hex::encode(&blob), v["blob_hex"]);
    let head: Head = serde_json::from_value(v["head_signed"].clone()).unwrap();
    assert_eq!(
        head.signing_payload(),
        v["signing_payload_utf8"].as_str().unwrap().as_bytes()
    );
    head.verify().unwrap();
    assert_eq!(
        URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes()),
        head.sig
    );
    assert_eq!(head.hash(), v["head_hash_hex"]);
    validate_blob(&head, &blob).unwrap();
}

#[tokio::test]
async fn escrow_cas_tofu_idempotence_and_restart_serve_both_generations() {
    let dir = scratch();
    let cfg = EscrowConfig {
        enabled: true,
        rooms: vec!["test-room".into()],
        ..Default::default()
    };
    let service = Service::open(dir.clone(), cfg.clone()).unwrap();
    let blob = vec![3; CHUNK_SIZE + 17];
    let (first, first_manifest) = fixture(&blob, 9, None);
    accept(&service.repo, &first, &blob, &first_manifest);
    // Idempotence follows the real service PUT path and never pulls the blob again.
    let ack = service
        .put("test-room", "peer", first.clone(), "retry")
        .await
        .unwrap();
    assert_eq!(ack, first.ack("retry"));
    let mut other = first.clone();
    sign(&mut other, [8; 32]);
    assert_eq!(
        service
            .repo
            .reserve("test-room", &other)
            .err()
            .unwrap()
            .code,
        "writer_mismatch"
    );
    let (mut next, next_manifest) = fixture(b"second", 10, Some(first.hash()));
    next.parent = None;
    sign(&mut next, [7; 32]);
    let error = service.repo.reserve("test-room", &next).err().unwrap();
    assert_eq!(error.code, "conflict");
    assert_eq!(*error.head.unwrap(), first);
    next.parent = Some(first.hash());
    next.seq = 11;
    sign(&mut next, [7; 32]);
    assert_eq!(
        service.repo.reserve("test-room", &next).err().unwrap().code,
        "conflict"
    );
    next.seq = 10;
    sign(&mut next, [7; 32]);
    accept(&service.repo, &next, b"second", &next_manifest);
    drop(service);
    let restarted = Service::open(dir.clone(), cfg.clone()).unwrap();
    assert_eq!(
        restarted.repo.head("test-room", &first.slot),
        Some(next.clone())
    );
    for (head, manifest, data) in [
        (&first, &first_manifest, blob.as_slice()),
        (&next, &next_manifest, b"second".as_slice()),
    ] {
        assert_eq!(
            restarted.repo.block("test-room", &head.cid).unwrap(),
            *manifest
        );
        assert!(restarted.repo.block("other-room", &head.cid).is_none());
        let m: FileManifest = serde_cbor::from_slice(manifest).unwrap();
        for (cid, chunk) in m.chunks.iter().zip(data.chunks(CHUNK_SIZE)) {
            assert_eq!(restarted.repo.block("test-room", cid).unwrap(), chunk);
        }
    }
    assert_eq!(
        restarted
            .request(
                "test-room",
                "peer",
                json!({"v":1,"type":"escrow_get","id":"get","slot":next.slot})
            )
            .await
            .unwrap()["head"],
        json!(next)
    );
    let (third, third_manifest) = fixture(b"third", 11, Some(next.hash()));
    accept(&restarted.repo, &third, b"third", &third_manifest);
    assert!(restarted.repo.block("test-room", &first.cid).is_none());
    assert!(restarted.repo.block("test-room", &next.cid).is_some());
    drop(restarted);
    let restarted = Service::open(dir.clone(), cfg).unwrap();
    assert_eq!(restarted.repo.head("test-room", &first.slot), Some(third));
    drop(restarted);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_rejects_bad_signatures_size_hash_manifest_and_quota() {
    let dir = scratch();
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (head, bytes) = fixture(b"blob", 1, None);
    let mut bad = head.clone();
    bad.at += 1;
    assert_eq!(
        repo.reserve("test-room", &bad).err().unwrap().code,
        "bad_sig"
    );
    bad.sig = "!".repeat(86);
    assert_eq!(bad.verify().unwrap_err().code, "bad_sig");
    assert_eq!(
        validate_blob(&head, b"short").unwrap_err().code,
        "hash_mismatch"
    );
    assert_eq!(
        validate_blob(&head, b"xxxx").unwrap_err().code,
        "hash_mismatch"
    );
    assert_eq!(
        validate_manifest(&head, b"wrong manifest")
            .unwrap_err()
            .code,
        "hash_mismatch"
    );
    bad = head.clone();
    bad.size = 32 * 1024 * 1024 + 1;
    sign(&mut bad, [7; 32]);
    assert_eq!(
        repo.reserve("test-room", &bad).err().unwrap().code,
        "too_large"
    );
    bad = head.clone();
    bad.parent = Some("a".repeat(64));
    sign(&mut bad, [7; 32]);
    assert_eq!(check_cas(&bad, None).unwrap_err().code, "conflict");
    let reservation = repo.reserve("test-room", &head).unwrap().unwrap();
    assert_eq!(
        repo.commit("test-room", &head, b"xxxx", &bytes)
            .unwrap_err()
            .code,
        "hash_mismatch"
    );
    assert!(repo.head("test-room", &head.slot).is_none());
    drop(reservation);
    let low = Repository::open(
        dir.clone(),
        EscrowConfig {
            max_total_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(low.reserve("test-room", &head).err().unwrap().code, "quota");
    let one = Repository::open(
        dir.clone(),
        EscrowConfig {
            max_slots_per_room: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let reservation = one.reserve("test-room", &head).unwrap().unwrap();
    let mut second = head.clone();
    second.slot = "b".repeat(32);
    sign(&mut second, [7; 32]);
    assert_eq!(
        one.reserve("test-room", &second).err().unwrap().code,
        "quota"
    );
    drop(reservation);
    drop(one);
    drop(low);
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_config_validates_and_requires_restart() {
    let cfg = crate::config::Config::default();
    assert!(!cfg.escrow.enabled);
    assert_eq!(cfg.escrow.max_blob_bytes, 32 * 1024 * 1024);
    let cfg = crate::config::set_by_path(&cfg, "escrow.rooms", json!(["app/room:1"])).unwrap();
    assert_eq!(cfg.escrow.rooms, vec!["app/room:1"]);
    for path in [
        "escrow.enabled",
        "escrow.rooms",
        "escrow.max_blob_bytes",
        "escrow.max_total_bytes",
        "escrow.max_slots_per_room",
    ] {
        assert_eq!(crate::config::applies_when(path), "daemon restart");
    }
    for value in [json!([""]), json!(["bad room"]), json!(["a", "a"])] {
        assert!(crate::config::set_by_path(&cfg, "escrow.rooms", value).is_err());
    }
    assert!(crate::config::set_by_path(&cfg, "escrow.max_blob_bytes", json!(0)).is_err());
    assert!(crate::config::set_by_path(&cfg, "escrow.max_slots_per_room", json!(0)).is_err());
}

#[test]
fn escrow_cli_commands_parse() {
    use crate::cli::{Cli, Command, EscrowAction, EscrowRoomsAction};
    use clap::Parser;
    assert!(matches!(
        Cli::try_parse_from(["mistl", "escrow", "status"])
            .unwrap()
            .command,
        Some(Command::Escrow {
            action: EscrowAction::Status
        })
    ));
    assert!(matches!(
        Cli::try_parse_from(["mistl", "escrow", "rooms", "list"])
            .unwrap()
            .command,
        Some(Command::Escrow {
            action: EscrowAction::Rooms {
                action: EscrowRoomsAction::List
            }
        })
    ));
    assert!(
        matches!(Cli::try_parse_from(["mistl","escrow","rooms","add","app/room"]).unwrap().command,
        Some(Command::Escrow { action: EscrowAction::Rooms { action: EscrowRoomsAction::Add { room } } }) if room == "app/room")
    );
    assert!(
        matches!(Cli::try_parse_from(["mistl","escrow","rooms","remove","app/room"]).unwrap().command,
        Some(Command::Escrow { action: EscrowAction::Rooms { action: EscrowRoomsAction::Remove { room } } }) if room == "app/room")
    );
}

#[tokio::test]
async fn escrow_get_shape_unknown_fields_and_busy_are_bounded() {
    let dir = scratch();
    let service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (head, _) = fixture(b"blob", 1, None);
    let reply = service
        .request(
            "test-room",
            "peer",
            json!({"v":1,"type":"escrow_get","slot":head.slot,"id":"get","future":true}),
        )
        .await
        .unwrap();
    assert!(reply["head"].is_null());
    assert_eq!(reply["type"], "escrow_head");
    for msg in [
        json!({"v":1,"type":"escrow_get","slot":"invalid","id":"get"}),
        json!({"v":1,"type":"escrow_put","head":{},"id":"get"}),
        json!({"v":1,"type":"escrow_get","slot":head.slot,"id":"a".repeat(65)}),
    ] {
        assert_eq!(
            service.request("test-room", "peer", msg).await.unwrap()["code"],
            "bad_request"
        );
    }
    assert!(
        service
            .request("test-room", "peer", json!({"v":1,"type":"future_type"}))
            .await
            .is_none()
    );
    assert!(
        service
            .request("test-room", "peer", json!({"v":2,"type":"escrow_hello"}))
            .await
            .is_none()
    );
    let mut future = serde_json::to_value(&head).unwrap();
    future["future"] = json!(true);
    let known: Head = serde_json::from_value(future).unwrap();
    known.verify().unwrap();
    assert_eq!(known.hash(), head.hash());
    service
        .pulling
        .lock()
        .unwrap()
        .extend(["p1", "p2", "p3", "p4"].map(String::from));
    assert_eq!(
        service
            .put("test-room", "p5", head, "put")
            .await
            .unwrap_err()
            .code,
        "busy"
    );
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_failed_publication_preserves_state_and_restart_cleans_orphans() {
    let dir = scratch();
    let (head, manifest) = fixture(b"checkpoint", 1, None);
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    let reservation = repo.reserve("test-room", &head).unwrap().unwrap();
    let key = digest(format!("test-room\n{}", head.slot).as_bytes());
    let blocker = dir.join("heads").join(format!("{key}.json"));
    std::fs::create_dir(&blocker).unwrap();
    assert_eq!(
        repo.commit("test-room", &head, b"checkpoint", &manifest)
            .unwrap_err()
            .code,
        "fetch_failed"
    );
    assert!(repo.head("test-room", &head.slot).is_none());
    assert_eq!(
        repo.reserve("test-room", &head).err().unwrap().code,
        "quota"
    );
    drop(reservation);
    drop(repo);
    std::fs::remove_dir(blocker).unwrap();
    let restarted = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    assert_eq!(std::fs::read_dir(dir.join("blobs")).unwrap().count(), 0);
    accept(&restarted, &head, b"checkpoint", &manifest);
    drop(restarted);
    // Signed metadata must not silently reset TOFU when durable bytes are corrupt.
    let generation = std::fs::read_dir(dir.join("blobs"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(generation.join("blob.bin"), b"corrupted").unwrap();
    assert!(Repository::open(dir.clone(), EscrowConfig::default()).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn accepted_head_retry_must_ack_when_pull_capacity_is_full() {
    let dir = scratch();
    let service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (head, manifest) = fixture(b"checkpoint", 1, None);
    accept(&service.repo, &head, b"checkpoint", &manifest);
    service
        .pulling
        .lock()
        .unwrap()
        .extend(["p1", "p2", "p3", "p4"].map(String::from));
    let reply = service
        .put("test-room", "original-peer", head.clone(), "retry")
        .await;
    assert_eq!(reply.unwrap(), head.ack("retry"));
    // The per-peer limit must not reject an identical head either.
    assert_eq!(
        service
            .put("test-room", "p1", head.clone(), "retry")
            .await
            .unwrap(),
        head.ack("retry")
    );
    assert_eq!(service.pulling.lock().unwrap().len(), 4);
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn escrow_cheap_put_rejections_precede_pull_admission() {
    let dir = scratch();
    let service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (head, manifest) = fixture(b"checkpoint", 1, None);
    accept(&service.repo, &head, b"checkpoint", &manifest);
    service
        .pulling
        .lock()
        .unwrap()
        .extend(["p1", "p2", "p3", "p4"].map(String::from));
    let mut conflict = head.clone();
    conflict.seq += 1;
    sign(&mut conflict, [7; 32]);
    let mut wrong_writer = head.clone();
    sign(&mut wrong_writer, [8; 32]);
    let mut bad_sig = head.clone();
    bad_sig.at += 1;
    for (candidate, code) in [
        (conflict, "conflict"),
        (wrong_writer, "writer_mismatch"),
        (bad_sig, "bad_sig"),
    ] {
        let error = service
            .put("test-room", "p1", candidate, "reject")
            .await
            .unwrap_err();
        assert_eq!(error.code, code);
        if code == "conflict" {
            assert_eq!(*error.head.unwrap(), head);
        }
    }
    // A valid next head still requires capacity, and a failed admission releases its reservation.
    let (next, _) = fixture(b"next", 2, Some(head.hash()));
    assert_eq!(
        service
            .put("test-room", "p5", next.clone(), "busy")
            .await
            .unwrap_err()
            .code,
        "busy"
    );
    let reservation = service.repo.reserve("test-room", &next).unwrap().unwrap();
    drop(reservation);
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_storage_queue_bounds_fairness_and_want_reserve() {
    use super::serving::{MAX_PEER_REQUESTS, MAX_REQUESTS, MAX_SERVING, Queue, Request};
    let request = |from: &str, query| Request {
        room: "test-room".into(),
        from: from.into(),
        cid: "bounded".into(),
        query,
    };
    let mut queue = Queue::default();
    for _ in 0..MAX_PEER_REQUESTS {
        assert!(queue.push(request("a", false)));
    }
    assert!(!queue.push(request("a", false)));
    assert!(!queue.push(request("a", true)));
    // Peer caps apply across rooms too.
    let mut other_room = request("a", false);
    other_room.room = "other-room".into();
    assert!(!queue.push(other_room));
    for from in ["b", "c", "d", "e"] {
        assert!(queue.push(request(from, false)));
    }
    let active: Vec<_> = (0..MAX_SERVING).map(|_| queue.next().unwrap()).collect();
    assert_eq!(
        active.iter().map(|r| r.from.as_str()).collect::<Vec<_>>(),
        ["a", "b", "c", "d"]
    );
    assert!(queue.next().is_none());
    // A backlogged peer rejoins at the tail; the fifth peer gets the next free worker.
    queue.finish("a");
    assert_eq!(queue.next().unwrap().from, "e");
    for from in ["b", "c", "d", "e"] {
        queue.finish(from);
    }
    for _ in 1..MAX_PEER_REQUESTS {
        assert_eq!(queue.next().unwrap().from, "a");
        queue.finish("a");
    }
    assert!(queue.next().is_none());
    // All capacity and peer bookkeeping is reclaimed.
    for i in 0..MAX_REQUESTS {
        assert!(queue.push(request(&format!("peer-{i}"), false)));
    }
    assert!(!queue.push(request("over-global-cap", false)));
    assert!(!queue.push(request("query-over-cap", true)));
    for _ in 0..MAX_REQUESTS {
        let item = queue.next().unwrap();
        assert!(!item.query);
        queue.finish(&item.from);
    }
    assert!(queue.next().is_none());
    // Discovery cannot consume the WANT reserve, either per peer or globally.
    let mut queries = 0;
    while queue.push(request("discoverer", true)) {
        queries += 1;
    }
    assert!(queries < MAX_PEER_REQUESTS);
    for _ in queries..MAX_PEER_REQUESTS {
        assert!(queue.push(request("discoverer", false)));
    }
    let mut count = MAX_PEER_REQUESTS;
    while queue.push(request(&format!("discoverer-{count}"), true)) {
        count += 1;
    }
    assert!(count < MAX_REQUESTS);
    for i in count..MAX_REQUESTS {
        assert!(queue.push(request(&format!("want-{i}"), false)));
    }
    assert!(!queue.push(request("overflow", false)));
}

#[tokio::test]
async fn escrow_concurrent_wants_for_twelve_distinct_blocks_all_reply() {
    use mistlib_core::storage::protocol::{MSG_WANT, parse_have_message};
    use std::time::Duration;
    let dir = scratch();
    let mut service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    service.serving.send = Arc::new({
        let gate = gate.clone();
        move |room, from, bytes| {
            let (tx, started_tx, gate) = (tx.clone(), started_tx.clone(), gate.clone());
            Box::pin(async move {
                started_tx.send(()).unwrap();
                let _permit = gate.acquire().await.unwrap();
                tx.send((room, from, bytes)).unwrap();
                Ok(())
            })
        }
    });
    let mut expected = std::collections::HashMap::new();
    for i in 0..12 {
        let blob = vec![i as u8; 1024];
        let (mut head, manifest) = fixture(&blob, 1, None);
        head.slot = format!("{i:032x}");
        sign(&mut head, [7; 32]);
        accept(&service.repo, &head, &blob, &manifest);
        expected.insert(head.cid, (format!("peer-{i}"), manifest));
    }
    let service = Arc::new(service);
    let rt = tokio::runtime::Handle::current();
    for (cid, (peer, _)) in &expected {
        let mut want = vec![MSG_WANT];
        want.extend_from_slice(cid.as_bytes());
        assert!(service.enqueue_storage(&rt, "test-room", peer, &want));
    }
    // Hold four wire sends open so every other WANT must survive in the bounded queue.
    for _ in 0..serving::MAX_SERVING {
        tokio::time::timeout(Duration::from_secs(3), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(started_rx.try_recv().is_err());
    gate.add_permits(serving::MAX_SERVING);
    for _ in 0..12 {
        let (room, peer, bytes) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(room, "test-room");
        let (cid, data) = parse_have_message(&bytes).unwrap();
        let (expected_peer, manifest) = expected.remove(&cid).expect("one reply per distinct WANT");
        assert_eq!(peer, expected_peer);
        assert_eq!(data, manifest);
    }
    assert!(expected.is_empty());
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn escrow_served_chunks_fit_browser_ceiling_with_overlay_margin() {
    use mistlib_core::storage::protocol::{MSG_WANT, parse_have_chunk_message};
    use std::time::Duration;
    let dir = scratch();
    let mut service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let blob: Vec<_> = (0..CHUNK_SIZE).map(|i| (i % 251) as u8).collect();
    let (head, manifest) = fixture(&blob, 1, None);
    accept(&service.repo, &head, &blob, &manifest);
    let manifest: FileManifest = serde_cbor::from_slice(&manifest).unwrap();
    let cid = &manifest.chunks[0];
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    service.serving.send = Arc::new(move |room, from, bytes| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send((room, from, bytes)).unwrap();
            Ok(())
        })
    });
    let service = Arc::new(service);
    let mut want = vec![MSG_WANT];
    want.extend_from_slice(cid.as_bytes());
    assert!(service.enqueue_storage(
        &tokio::runtime::Handle::current(),
        "test-room",
        "browser",
        &want
    ));
    let mut received = Vec::new();
    let mut count = None;
    let mut index = 0;
    loop {
        let (room, peer, bytes) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((room.as_str(), peer.as_str()), ("test-room", "browser"));
        assert!(
            bytes.len() + 512 <= 16 * 1024,
            "frame must fit after overlay wrapping"
        );
        let (parsed_cid, parsed_index, total, payload) = parse_have_chunk_message(&bytes).unwrap();
        assert_eq!(&parsed_cid, cid);
        assert_eq!(parsed_index, index);
        assert_eq!(*count.get_or_insert(total), total);
        received.extend_from_slice(&payload);
        index += 1;
        if index == total {
            break;
        }
    }
    assert_eq!(received, blob);
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_restart_removes_head_and_retained_generation_temp_files() {
    let dir = scratch();
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (first, manifest) = fixture(b"first", 1, None);
    accept(&repo, &first, b"first", &manifest);
    let (second, second_manifest) = fixture(b"second", 2, Some(first.hash()));
    accept(&repo, &second, b"second", &second_manifest);
    drop(repo);
    let key = digest(format!("test-room\n{}", first.slot).as_bytes());
    let head_temp = dir
        .join("heads")
        .join(format!("{key}.0123456789abcdef.tmp"));
    std::fs::write(&head_temp, b"torn head").unwrap();
    let mut temps = vec![head_temp];
    for entry in std::fs::read_dir(dir.join("blobs")).unwrap() {
        let generation = entry.unwrap().path();
        for stem in ["blob", "manifest"] {
            let path = generation.join(format!("{stem}.0123456789abcdef.tmp"));
            std::fs::write(&path, b"torn bytes").unwrap();
            temps.push(path);
        }
        std::fs::write(generation.join("unrelated.tmp"), b"keep").unwrap();
    }
    let orphan = dir.join("blobs").join("a".repeat(64));
    std::fs::create_dir(&orphan).unwrap();
    std::fs::write(orphan.join("blob.0123456789abcdef.tmp"), b"torn orphan").unwrap();
    let unknown = dir.join("heads").join("unrelated.0123456789abcdef.tmp");
    std::fs::write(&unknown, b"keep").unwrap();
    let restarted = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    assert!(temps.iter().all(|path| !path.exists()));
    assert!(!orphan.exists());
    assert!(unknown.exists());
    assert_eq!(
        restarted.head("test-room", &first.slot),
        Some(second.clone())
    );
    assert_eq!(restarted.block("test-room", &first.cid).unwrap(), manifest);
    assert_eq!(
        restarted.block("test-room", &second.cid).unwrap(),
        second_manifest
    );
    drop(restarted);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn inflight_commit_must_stop_after_durable_write_failure() {
    let dir = scratch();
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (a, am) = fixture(b"a", 1, None);
    let (mut b, bm) = fixture(b"b", 1, None);
    b.slot = "b".repeat(32);
    sign(&mut b, [7; 32]);
    let ra = repo.reserve("test-room", &a).unwrap().unwrap();
    let rb = repo.reserve("test-room", &b).unwrap().unwrap();
    let k = digest(format!("test-room\n{}", a.slot).as_bytes());
    let blocker = dir.join("heads").join(format!("{k}.json"));
    std::fs::create_dir(&blocker).unwrap();
    assert!(repo.commit("test-room", &a, b"a", &am).is_err());
    std::fs::remove_dir(&blocker).unwrap();
    let result = repo.commit("test-room", &b, b"b", &bm);
    drop(rb);
    drop(ra);
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
    assert!(
        result.is_err(),
        "a reserved PUT committed after writes were disabled"
    );
}

#[test]
fn escrow_failed_sync_after_publish_preserves_disk_generations_until_restart() {
    let dir = scratch();
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (first, fm) = fixture(b"first", 1, None);
    accept(&repo, &first, b"first", &fm);
    let (next, nm) = fixture(b"next", 2, Some(first.hash()));
    let (mut other, om) = fixture(b"other", 1, None);
    other.slot = "b".repeat(32);
    sign(&mut other, [7; 32]);
    let rn = repo.reserve("test-room", &next).unwrap().unwrap();
    let ro = repo.reserve("test-room", &other).unwrap().unwrap();
    repo.inject_post_publish_failure();
    assert_eq!(
        repo.commit("test-room", &next, b"next", &nm)
            .unwrap_err()
            .code,
        "fetch_failed"
    );
    // The disk head was replaced, but the in-memory index still names the old head.
    assert_eq!(repo.head("test-room", &first.slot), Some(first.clone()));
    let k = digest(format!("test-room\n{}", next.slot).as_bytes());
    let disk: Value = serde_json::from_slice(
        &std::fs::read(dir.join("heads").join(format!("{k}.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(disk["current"], json!(next));
    let generations = || {
        std::fs::read_dir(dir.join("blobs"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect::<std::collections::HashSet<_>>()
    };
    let retained = generations();
    assert_eq!(retained.len(), 2);
    assert_eq!(
        repo.commit("test-room", &other, b"other", &om)
            .unwrap_err()
            .code,
        "quota"
    );
    assert_eq!(
        generations(),
        retained,
        "stale memory must never drive generation deletion"
    );
    drop(ro);
    drop(rn);
    drop(repo);
    let restarted = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    assert_eq!(restarted.head("test-room", &first.slot), Some(next.clone()));
    assert_eq!(restarted.block("test-room", &next.cid).unwrap(), nm);
    assert_eq!(restarted.block("test-room", &first.cid).unwrap(), fm);
    accept(&restarted, &other, b"other", &om);
    drop(restarted);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn escrow_previous_head_retry_acks_without_rolling_back_current() {
    let dir = scratch();
    let service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (first, fm) = fixture(b"first", 1, None);
    accept(&service.repo, &first, b"first", &fm);
    let (next, nm) = fixture(b"next", 2, Some(first.hash()));
    accept(&service.repo, &next, b"next", &nm);
    service
        .pulling
        .lock()
        .unwrap()
        .extend(["p1", "p2", "p3", "p4"].map(String::from));
    assert_eq!(
        service
            .put("test-room", "p1", first.clone(), "retry-backup")
            .await
            .unwrap(),
        first.ack("retry-backup")
    );
    assert_eq!(
        service.repo.head("test-room", &first.slot),
        Some(next.clone())
    );
    let (third, tm) = fixture(b"third", 3, Some(next.hash()));
    accept(&service.repo, &third, b"third", &tm);
    assert_eq!(
        service
            .put("test-room", "p1", first, "too-old")
            .await
            .unwrap_err()
            .code,
        "conflict"
    );
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_json_admission_bounds_waiters_across_rooms_and_reclaims_capacity() {
    use admission::{MAX_PEER_REQUESTS, MAX_PUT_REQUESTS, MAX_REPLY_REQUESTS};
    let admission = Arc::new(Admission::default());
    let mut puts = Vec::new();
    for _ in 0..MAX_PEER_REQUESTS {
        puts.push(admission.acquire("attacker", Class::Put).unwrap());
    }
    assert!(admission.acquire("attacker", Class::Put).is_none());
    for i in puts.len()..MAX_PUT_REQUESTS {
        puts.push(admission.acquire(&format!("peer-{i}"), Class::Put).unwrap());
    }
    assert!(admission.acquire("another", Class::Put).is_none());
    // Every PUT permit may be parked on a lock without using the reply reserve.
    let replies: Vec<_> = (0..MAX_REPLY_REQUESTS)
        .map(|i| {
            admission
                .acquire(&format!("reader-{i}"), Class::Reply)
                .unwrap()
        })
        .collect();
    assert!(admission.acquire("over-reply-cap", Class::Reply).is_none());
    drop(replies);
    drop(puts);
    let recovered: Vec<_> = (0..MAX_PUT_REQUESTS)
        .map(|i| admission.acquire(&format!("new-{i}"), Class::Put).unwrap())
        .collect();
    drop(recovered);
    assert!(admission.acquire("attacker", Class::Put).is_some());
}

#[tokio::test(start_paused = true)]
async fn escrow_slot_wait_flood_keeps_get_hello_and_retry_responsive() {
    let dir = scratch();
    let mut service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (saved, sm) = fixture(b"saved", 1, None);
    accept(&service.repo, &saved, b"saved", &sm);
    let (mut blocked, _) = fixture(b"not supplied", 1, None);
    for i in 0u64.. {
        blocked.slot = format!("{i:032x}");
        if sha2_byte("test-room", &blocked.slot) != sha2_byte("test-room", &saved.slot) {
            break;
        }
    }
    sign(&mut blocked, [7; 32]);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    service.serving.send = Arc::new(move |room, from, bytes| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send((room, from, serde_json::from_slice::<Value>(&bytes).unwrap()))
                .unwrap();
            Ok(())
        })
    });
    let service = Arc::new(service);
    let rt = tokio::runtime::Handle::current();
    let slot = service.slots[usize::from(sha2_byte("test-room", &blocked.slot))]
        .lock()
        .await;
    for i in 0..32 {
        service.enqueue_request(
            &rt,
            "test-room",
            "attacker",
            json!({"v":1,"type":"escrow_put","id":format!("blocked-{i}"),"head":blocked}),
        );
    }
    assert!(service.requests.acquire("attacker", Class::Put).is_none());
    // The transport peer limit applies even if the next request uses another room.
    service.enqueue_request(
        &rt,
        "other-room",
        "attacker",
        json!({"v":1,"type":"escrow_put","id":"other-room","head":blocked}),
    );
    service.enqueue_request(
        &rt,
        "test-room",
        "reader",
        json!({"v":1,"type":"escrow_get","id":"read","slot":saved.slot}),
    );
    service.enqueue_request(
        &rt,
        "test-room",
        "joiner",
        json!({"v":1,"type":"escrow_hello"}),
    );
    service.enqueue_request(
        &rt,
        "test-room",
        "retry-peer",
        json!({"v":1,"type":"escrow_put","id":"retry","head":saved}),
    );
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let mut replies = Vec::new();
    while let Ok(reply) = rx.try_recv() {
        replies.push(reply);
    }
    assert!(
        replies
            .iter()
            .any(|(_, peer, msg)| peer == "reader" && msg["head"] == json!(saved))
    );
    assert!(
        replies
            .iter()
            .any(|(_, peer, msg)| peer == "joiner" && msg["type"] == "escrow_provider")
    );
    assert!(
        replies
            .iter()
            .any(|(_, peer, msg)| peer == "retry-peer" && *msg == saved.ack("retry"))
    );
    // Fill every remaining PUT permit; GET and HELLO still use their own reserve.
    let held: Vec<_> = (2..admission::MAX_PUT_REQUESTS)
        .map(|i| {
            service
                .requests
                .acquire(&format!("holder-{i}"), Class::Put)
                .unwrap()
        })
        .collect();
    service.enqueue_request(
        &rt,
        "test-room",
        "reader",
        json!({"v":1,"type":"escrow_get","id":"full-read","slot":saved.slot}),
    );
    service.enqueue_request(
        &rt,
        "test-room",
        "joiner",
        json!({"v":1,"type":"escrow_hello"}),
    );
    service.enqueue_request(
        &rt,
        "test-room",
        "busy-peer",
        json!({"v":1,"type":"escrow_put","id":"full-put","head":saved}),
    );
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let mut replies = Vec::new();
    while let Ok(reply) = rx.try_recv() {
        replies.push(reply);
    }
    assert!(replies.iter().any(|(_, peer, msg)| peer == "reader"
        && msg["id"] == "full-read"
        && msg["type"] == "escrow_head"));
    assert!(
        replies
            .iter()
            .any(|(_, peer, msg)| peer == "joiner" && msg["type"] == "escrow_provider")
    );
    assert!(
        replies
            .iter()
            .any(|(_, peer, msg)| peer == "busy-peer" && msg["code"] == "busy")
    );
    tokio::time::advance(SLOT_WAIT + Duration::from_millis(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    let mut expired = std::collections::HashSet::new();
    while let Ok((_, peer, msg)) = rx.try_recv() {
        if peer == "attacker" && msg["code"] == "busy" {
            expired.insert(msg["id"].as_str().unwrap().to_string());
        }
    }
    assert!(expired.contains("blocked-0") && expired.contains("blocked-1"));
    assert!(service.requests.acquire("attacker", Class::Put).is_some());
    drop(held);
    drop(slot);
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(start_paused = true)]
async fn escrow_four_parallel_megabyte_downloads_fit_stalled_peer_queue() {
    use mistlib_core::storage::protocol::{MSG_WANT, parse_have_chunk_message};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = scratch();
    let mut service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let mut expected = std::collections::HashMap::new();
    for i in 0..4 {
        let blob = vec![i as u8; CHUNK_SIZE];
        let (mut head, bytes) = fixture(&blob, 1, None);
        head.slot = format!("{i:032x}");
        sign(&mut head, [7; 32]);
        accept(&service.repo, &head, &blob, &bytes);
        let manifest: FileManifest = serde_cbor::from_slice(&bytes).unwrap();
        expected.insert(manifest.chunks[0].clone(), blob);
    }
    // Model net::send_direct: enqueue success is returned even when the native
    // downstream try_send loses the frame. The drainer starts one second later.
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let dropped = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicUsize::new(0));
    let times = Arc::new(StdMutex::new(Vec::new()));
    service.serving.send = Arc::new({
        let (dropped, high_water, times) = (dropped.clone(), high_water.clone(), times.clone());
        move |room, from, bytes| {
            let (tx, dropped, high_water, times) = (
                tx.clone(),
                dropped.clone(),
                high_water.clone(),
                times.clone(),
            );
            Box::pin(async move {
                assert_eq!((room.as_str(), from.as_str()), ("test-room", "browser"));
                times.lock().unwrap().push(tokio::time::Instant::now());
                if tx.try_send(bytes).is_err() {
                    dropped.fetch_add(1, Ordering::SeqCst);
                }
                high_water.fetch_max(256 - tx.capacity(), Ordering::SeqCst);
                Ok(())
            })
        }
    });
    let service = Arc::new(service);
    let rt = tokio::runtime::Handle::current();
    for cid in expected.keys() {
        let mut want = vec![MSG_WANT];
        want.extend_from_slice(cid.as_bytes());
        assert!(service.enqueue_storage(&rt, "test-room", "browser", &want));
    }
    let mut received: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    let start = tokio::time::Instant::now();
    for _ in 0..800 {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        if tokio::time::Instant::now() - start < Duration::from_secs(1) {
            continue;
        }
        // Drain at the conservative producer rate, rather than acknowledging on enqueue.
        if let Ok(bytes) = rx.try_recv() {
            let (cid, index, _, payload) = parse_have_chunk_message(&bytes).unwrap();
            let assembled = received.entry(cid).or_default();
            if index == 0 {
                assert!(assembled.is_empty());
            }
            assembled.extend_from_slice(&payload);
        }
        if received == expected {
            break;
        }
    }
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        0,
        "native queue overflow must not silently drop chunks"
    );
    assert!(
        high_water.load(Ordering::SeqCst) < 128,
        "keep headroom in the 256-frame peer queue"
    );
    assert_eq!(received, expected);
    let sent = times.lock().unwrap();
    assert!(
        sent.len() > 256,
        "exercise the original four-block overflow"
    );
    assert!(
        sent.windows(2)
            .all(|w| w[1] - w[0] >= serving::FRAME_SPACING),
        "pacing must continue between WANTs"
    );
    drop(sent);
    // Let the last pacing delay release its worker before removing durable data.
    tokio::time::advance(serving::FRAME_SPACING).await;
    tokio::task::yield_now().await;
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(start_paused = true)]
async fn escrow_storage_retries_same_frame_on_observable_enqueue_failure() {
    use mistlib_core::storage::protocol::{MSG_WANT, parse_have_message};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = scratch();
    let mut service = Service::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (head, manifest) = fixture(b"retry", 1, None);
    accept(&service.repo, &head, b"retry", &manifest);
    let attempts = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    service.serving.send = Arc::new({
        let attempts = attempts.clone();
        move |_, _, bytes| {
            let (attempts, tx) = (attempts.clone(), tx.clone());
            Box::pin(async move {
                if attempts.fetch_add(1, Ordering::SeqCst) < 3 {
                    anyhow::bail!("temporarily full");
                }
                tx.send(bytes).unwrap();
                Ok(())
            })
        }
    });
    let service = Arc::new(service);
    let mut want = vec![MSG_WANT];
    want.extend_from_slice(head.cid.as_bytes());
    service.enqueue_storage(
        &tokio::runtime::Handle::current(),
        "test-room",
        "browser",
        &want,
    );
    let bytes = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let (cid, data) = parse_have_message(&bytes).unwrap();
    assert_eq!(cid, head.cid);
    assert_eq!(data, manifest);
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    tokio::time::advance(serving::FRAME_SPACING).await;
    tokio::task::yield_now().await;
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn escrow_cid_matching_malformed_manifest_is_fetch_failure_without_disabling_writes() {
    let dir = scratch();
    let repo = Repository::open(dir.clone(), EscrowConfig::default()).unwrap();
    let (head, _) = fixture(b"blob", 1, None);
    let mut bad = head.clone();
    let malformed = [0xff]; // CBOR break outside an indefinite container.
    bad.cid = compute_cid(&malformed, MULTICODEC_DAG_CBOR);
    sign(&mut bad, [7; 32]);
    let reservation = repo.reserve("test-room", &bad).unwrap().unwrap();
    assert_eq!(
        validate_manifest(&bad, &malformed).unwrap_err().code,
        "fetch_failed"
    );
    assert_eq!(
        repo.commit("test-room", &bad, b"blob", &malformed)
            .unwrap_err()
            .code,
        "fetch_failed"
    );
    drop(reservation);
    assert!(repo.reserve("test-room", &head).unwrap().is_some());
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}
