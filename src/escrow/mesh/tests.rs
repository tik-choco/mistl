use super::*;
use crate::{
    config::EscrowConfig,
    escrow::{
        Service,
        protocol::{Delete, Head, digest},
        repository::{ImportOutcome, Repository},
    },
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use mistlib_core::storage::{
    CHUNK_SIZE, FileManifest,
    cid::{MULTICODEC_DAG_CBOR, MULTICODEC_RAW, compute_cid},
};
use serde_json::json;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
};

const ROOM: &str = "mesh-test";
fn scratch() -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("mistl-escrow-mesh-{:016x}", rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
fn config() -> EscrowConfig {
    EscrowConfig {
        enabled: true,
        rooms: vec![ROOM.into()],
        ..Default::default()
    }
}
fn sign(head: &mut Head, seed: u8) {
    let key = SigningKey::from_bytes(&[seed; 32]);
    head.writer = URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes());
    head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
}
fn fixture(seq: u64, parent: Option<String>, blob: &[u8]) -> (Head, Vec<u8>) {
    let manifest = serde_cbor::to_vec(&FileManifest {
        name: "tc-escrow.bin".into(),
        size: blob.len() as u64,
        chunks: blob
            .chunks(CHUNK_SIZE)
            .map(|c| compute_cid(c, MULTICODEC_RAW))
            .collect(),
    })
    .unwrap();
    let mut head = Head {
        v: 1,
        kind: "tc-escrow-head".into(),
        slot: "aa".repeat(16),
        seq,
        parent,
        cid: compute_cid(&manifest, MULTICODEC_DAG_CBOR),
        size: blob.len() as u64,
        sha256: digest(blob),
        comp: "none".into(),
        schema: "test".into(),
        writer: String::new(),
        at: 1,
        sig: String::new(),
    };
    sign(&mut head, 7);
    (head, manifest)
}
fn delete(head: &Head) -> Delete {
    let mut d = Delete {
        slot: head.slot.clone(),
        seq: head.seq,
        head: head.hash(),
        sig: String::new(),
    };
    d.sig = URL_SAFE_NO_PAD.encode(
        SigningKey::from_bytes(&[7; 32])
            .sign(&d.signing_payload())
            .to_bytes(),
    );
    d
}
fn accept(repo: &Repository, head: &Head, blob: &[u8], manifest: &[u8]) {
    let _reservation = repo.reserve(ROOM, head).unwrap().unwrap();
    repo.commit(ROOM, head, blob, manifest).unwrap();
}

#[test]
fn metadata_acceptance_and_durable_delete_continue() {
    use decision::Acceptance::*;
    let dir = scratch();
    let mut cfg = config();
    cfg.max_slots_per_room = 1;
    let me = crate::identity::for_test();
    let repo = Repository::open(dir.clone(), cfg.clone()).unwrap();
    let (h, m) = fixture(7, None, b"ciphertext");
    assert_eq!(decision::acceptance(None, &h), New);
    assert_eq!(repo.learn(ROOM, &h, Some(me.did()), None).unwrap(), New);
    assert_eq!(repo.head(ROOM, &h.slot), Some(h.clone()));
    assert_eq!(repo.total_bytes(), 0);
    assert!(!repo.held(ROOM, &h));
    assert_eq!(repo.learn(ROOM, &h, None, None).unwrap(), Same);
    let mut fork = h.clone();
    fork.at += 1;
    sign(&mut fork, 7);
    assert_eq!(repo.learn(ROOM, &fork, None, None).unwrap(), Conflict);
    sign(&mut fork, 8);
    assert_eq!(repo.learn(ROOM, &fork, None, None).unwrap(), WriterMismatch);
    let (old, _) = fixture(6, None, b"old");
    assert_eq!(repo.learn(ROOM, &old, None, None).unwrap(), Stale);
    assert_eq!(
        repo.import(ROOM, &h, b"ciphertext", &m).unwrap(),
        ImportOutcome::Imported
    );
    assert!(repo.held(ROOM, &h));
    repo.mesh_delete(ROOM, &delete(&h)).unwrap();
    assert!(repo.head(ROOM, &h.slot).is_none());
    assert!(repo.block(ROOM, &h.cid).is_none());
    assert_eq!(repo.total_bytes(), 0);
    drop(repo);
    let repo = Repository::open(dir.clone(), cfg.clone()).unwrap();
    assert!(repo.head(ROOM, &h.slot).is_none());
    assert_eq!(
        repo.learn(ROOM, &h, None, None).unwrap_err().code,
        "deleted"
    );
    assert_eq!(repo.reserve(ROOM, &h).err().unwrap().code, "deleted");
    let mut another = h.clone();
    another.slot = "bb".repeat(16);
    sign(&mut another, 7);
    assert_eq!(
        repo.learn(ROOM, &another, None, None).unwrap_err().code,
        "quota"
    );
    let (mut next, manifest) = fixture(8, Some(h.hash()), b"successor");
    sign(&mut next, 8);
    assert_eq!(
        repo.reserve(ROOM, &next).err().unwrap().code,
        "writer_mismatch"
    );
    sign(&mut next, 7);
    accept(&repo, &next, b"successor", &manifest);
    assert_eq!(
        repo.learn(ROOM, &h, None, Some(&delete(&h))).unwrap(),
        Stale
    );
    drop(repo);
    let repo = Repository::open(dir.clone(), cfg).unwrap();
    assert_eq!(repo.head(ROOM, &h.slot), Some(next));
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn unheld_restart_capacity_and_scrub_recovery() {
    let dir = scratch();
    let me = crate::identity::for_test();
    let repo = Repository::open(dir.clone(), config()).unwrap();
    let (h, m) = fixture(1, None, b"ciphertext");
    accept(&repo, &h, b"ciphertext", &m);
    repo.set_home(ROOM, &h, me.did()).unwrap();
    repo.drop_holding(ROOM, &h).unwrap();
    assert_eq!(repo.total_bytes(), 0);
    drop(repo);
    let repo = Repository::open(dir.clone(), config()).unwrap();
    assert_eq!(repo.head(ROOM, &h.slot), Some(h.clone()));
    assert!(repo.generation_bytes(ROOM, &h).unwrap().is_none());
    accept(&repo, &h, b"ciphertext", &m);
    drop(repo);
    let generation = std::fs::read_dir(dir.join("blobs"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(generation.join("blob.bin"), b"corruption").unwrap();
    let repo = Repository::open(dir.clone(), config()).unwrap();
    assert_eq!(repo.head(ROOM, &h.slot), Some(h.clone()));
    assert!(!repo.held(ROOM, &h));
    accept(&repo, &h, b"ciphertext", &m);
    assert!(repo.held(ROOM, &h));
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
    let dir = scratch();
    let mut cfg = config();
    cfg.max_total_bytes = 1;
    let repo = Repository::open(dir.clone(), cfg).unwrap();
    repo.learn(ROOM, &h, Some(me.did()), None).unwrap();
    assert_eq!(repo.reserve(ROOM, &h).err().unwrap().code, "quota");
    assert_eq!(repo.mesh_records(ROOM).len(), 1);
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

type Queue = Arc<Mutex<VecDeque<(String, String, Vec<u8>)>>>;
type Sources = Arc<Mutex<HashMap<String, std::sync::Weak<Service>>>>;
fn node(queue: &Queue, cfg: EscrowConfig, sources: &Sources) -> (PathBuf, Arc<Service>, Arc<Mesh>) {
    node_with_age(queue, cfg, sources, true)
}
fn node_with_age(
    queue: &Queue,
    cfg: EscrowConfig,
    sources: &Sources,
    settled: bool,
) -> (PathBuf, Arc<Service>, Arc<Mesh>) {
    let dir = scratch();
    let me = Arc::new(crate::identity::for_test());
    let mut s = Service::open(dir.clone(), cfg).unwrap();
    let fetch_sources = sources.clone();
    s.transfers.fetch = Arc::new(move |room, cid, opts| {
        let sources = fetch_sources.clone();
        Box::pin(async move {
            let source = sources
                .lock()
                .unwrap()
                .get(&opts.peer.unwrap().0)
                .and_then(|s| s.upgrade())
                .unwrap();
            let bytes = source
                .repo
                .block(&room, &cid)
                .ok_or_else(|| mistlib::MistError::Network("missing".into()))?;
            assert!(bytes.len() <= opts.max_bytes);
            Ok(bytes)
        })
    });
    let (queue, from) = (queue.clone(), me.node_id());
    s.send_reply = Arc::new(move |_, to, bytes| {
        queue.lock().unwrap().push_back((from.clone(), to, bytes));
        Box::pin(async { Ok(()) })
    });
    let service = Arc::new(s);
    let mut mesh = Mesh::new(&service, me);
    // Most tests exercise a settled room; the gate itself is tested separately.
    if settled {
        Arc::get_mut(&mut mesh).unwrap().since = 0;
    }
    sources
        .lock()
        .unwrap()
        .insert(mesh.me.node_id(), Arc::downgrade(&service));
    *service.mesh.lock().unwrap() = Some(mesh.clone());
    service.joined.lock().unwrap().insert(ROOM.into());
    (dir, service, mesh)
}
async fn deliver(queue: &Queue, nodes: &[&Arc<Mesh>]) {
    for _ in 0..1000 {
        let message = queue.lock().unwrap().pop_front();
        let Some((from, to, bytes)) = message else {
            return;
        };
        let Some(mesh) = nodes.iter().find(|m| m.me.node_id() == to) else {
            continue;
        };
        // Fresh guard is intentional here: envelope validation is unit-tested separately.
        let message = wire::Guard::default()
            .open(&bytes, ROOM, &from, now_ms())
            .unwrap();
        mesh.handle(ROOM, &from, message).await;
    }
    panic!("non-converging control traffic");
}
async fn connect(queue: &Queue, nodes: &[&Arc<Mesh>]) {
    for a in nodes {
        for b in nodes {
            if a.me.node_id() != b.me.node_id() {
                a.hello(ROOM, Some(&b.me.node_id())).await;
            }
        }
    }
    deliver(queue, nodes).await;
}

#[tokio::test]
async fn signed_discovery_replication_proxy_delete_and_conflicts() {
    use mistlib::storage::BlockProvider;
    use mistlib_core::types::NodeId;
    let queue = Queue::default();
    let sources = Sources::default();
    let (da, a, ma) = node(&queue, config(), &sources);
    let (db, b, mb) = node(&queue, config(), &sources);
    let (dc, c, mc) = node(&queue, config(), &sources);
    let (head, manifest) = fixture(1, None, b"encrypted");
    accept(&a.repo, &head, b"encrypted", &manifest);
    a.repo.set_home(ROOM, &head, ma.did()).unwrap();
    connect(&queue, &[&ma, &mb, &mc]).await;
    assert_eq!(b.repo.head(ROOM, &head.slot), Some(head.clone()));
    assert!(!b.repo.held(ROOM, &head));
    let proxy = crate::escrow::provider::EscrowBlockProvider(Arc::downgrade(&b));
    assert_eq!(
        proxy
            .get_block(ROOM, &NodeId("browser".into()), &head.cid)
            .await,
        Some(manifest.clone())
    );
    let chunk = compute_cid(b"encrypted", MULTICODEC_RAW);
    assert_eq!(
        proxy
            .get_block(ROOM, &NodeId("browser".into()), &chunk)
            .await,
        Some(b"encrypted".to_vec())
    );
    assert!(
        proxy
            .get_block("another-room", &NodeId("browser".into()), &head.cid)
            .await
            .is_none()
    );
    mb.tick().await;
    mc.tick().await;
    for _ in 0..200 {
        if b.repo.held(ROOM, &head) && c.repo.held(ROOM, &head) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(b.repo.held(ROOM, &head));
    assert!(c.repo.held(ROOM, &head));
    connect(&queue, &[&ma, &mb, &mc]).await;
    assert_eq!(ma.status(ROOM)["healthy"], 1);
    assert_eq!(ma.slots(ROOM)["slots"][0]["confirmed"], 3);
    assert!(!ma.slots(ROOM).to_string().contains("sig"));
    let permits = mb.proxies.clone().acquire_many_owned(4).await.unwrap();
    assert!(mb.proxy(ROOM, "browser", &head.cid).await.is_none());
    drop(permits);
    let mut fork = head.clone();
    fork.at += 1;
    sign(&mut fork, 7);
    let msg = wire::seal(
        &ma.me,
        ROOM,
        "head",
        json!({"head":fork,"home":ma.did()}),
        now_ms(),
    )
    .unwrap();
    mb.handle(
        ROOM,
        &ma.me.node_id(),
        wire::Guard::default()
            .open(&msg, ROOM, &ma.me.node_id(), now_ms())
            .unwrap(),
    )
    .await;
    assert_eq!(mb.status(ROOM)["conflicts"], 1);
    a.delete(ROOM, delete(&head), "del").await.unwrap();
    deliver(&queue, &[&ma, &mb, &mc]).await;
    for s in [&a, &b, &c] {
        assert!(s.repo.head(ROOM, &head.slot).is_none());
        assert_eq!(s.repo.total_bytes(), 0);
    }
    assert!(
        proxy
            .get_block(ROOM, &NodeId("browser".into()), &chunk)
            .await
            .is_none()
    );
    for d in [da, db, dc] {
        std::fs::remove_dir_all(d).unwrap();
    }
}

#[tokio::test]
async fn index_pages_converge_without_disclosing_unheld_bytes() {
    let queue = Queue::default();
    let sources = Sources::default();
    let (da, a, ma) = node(&queue, config(), &sources);
    let (db, b, mb) = node(&queue, config(), &sources);
    let mut heads = Vec::new();
    for n in 0..135 {
        let (mut head, _) = fixture(1, None, b"metadata only");
        head.slot = format!("{n:032x}");
        sign(&mut head, 7);
        a.repo.learn(ROOM, &head, Some(ma.did()), None).unwrap();
        if n % 7 == 0 {
            a.repo.mesh_delete(ROOM, &delete(&head)).unwrap();
        }
        heads.push(head);
    }
    connect(&queue, &[&ma, &mb]).await;
    assert!(b.repo.mesh_records(ROOM).len() < 135);
    for _ in 0..20 {
        mb.tick().await;
        deliver(&queue, &[&ma, &mb]).await;
        if b.repo.mesh_records(ROOM).len() == 135 {
            break;
        }
    }
    assert_eq!(b.repo.mesh_records(ROOM).len(), 135);
    assert_eq!(
        service::digests(&a.repo.mesh_records(ROOM)),
        service::digests(&b.repo.mesh_records(ROOM))
    );
    assert_eq!(b.repo.total_bytes(), 0);
    assert_eq!(
        b.repo
            .mesh_records(ROOM)
            .iter()
            .filter(|r| r.deleted.is_some())
            .count(),
        20
    );
    assert!(b.repo.learn(ROOM, &heads[0], None, None).is_err());
    std::fs::remove_dir_all(da).unwrap();
    std::fs::remove_dir_all(db).unwrap();
}

#[tokio::test]
async fn provider_cap_room_binding_and_capacity_status() {
    let queue = Queue::default();
    let sources = Sources::default();
    let mut cfg = config();
    cfg.max_total_bytes = 1;
    let (dir, s, m) = node(&queue, cfg, &sources);
    for _ in 0..20 {
        let peer = crate::identity::for_test();
        let bytes = wire::seal(
            &peer,
            ROOM,
            "hello",
            json!({"free":10,"capacity":10,"since":0,
            "digest":digest(b""),"held_digest":digest(b""),"count":0}),
            now_ms(),
        )
        .unwrap();
        let msg = wire::Guard::default()
            .open(&bytes, ROOM, &peer.node_id(), now_ms())
            .unwrap();
        m.handle(ROOM, &peer.node_id(), msg).await;
    }
    assert_eq!(m.status(ROOM)["providers"].as_array().unwrap().len(), 16);
    let (head, _) = fixture(1, None, b"larger than local capacity");
    s.repo.learn(ROOM, &head, Some(m.did()), None).unwrap();
    assert_eq!(m.status(ROOM)["insufficient_capacity"], true);
    assert_eq!(m.slots(ROOM)["slots"][0]["state"], "degraded");
    assert!(s.mesh("unconfigured").is_none());
    assert!(
        s.provider_for(ROOM)["features"]
            .as_array()
            .unwrap()
            .contains(&json!("delete-continue"))
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn startup_gate_does_not_treat_the_first_hello_as_a_complete_roster() {
    let queue = Queue::default();
    let sources = Sources::default();
    let (da, a, ma) = node(&queue, config(), &sources);
    let (db, b, mb) = node_with_age(&queue, config(), &sources, false);
    let (head, manifest) = fixture(1, None, b"encrypted");
    accept(&a.repo, &head, b"encrypted", &manifest);
    a.repo.set_home(ROOM, &head, ma.did()).unwrap();
    connect(&queue, &[&ma, &mb]).await;
    assert_eq!(b.repo.head(ROOM, &head.slot), Some(head.clone()));
    mb.tick().await;
    tokio::task::yield_now().await;
    assert!(!b.repo.held(ROOM, &head));
    assert!(b.pulling.lock().unwrap().is_empty());
    assert_eq!(mb.status(ROOM)["held"], 0);
    std::fs::remove_dir_all(da).unwrap();
    std::fs::remove_dir_all(db).unwrap();
}

fn corrupt_blob(dir: &std::path::Path, manifest: &[u8]) {
    let generation = std::fs::read_dir(dir.join("blobs"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| std::fs::read(p.join("manifest.bin")).unwrap() == manifest)
        .unwrap();
    std::fs::write(generation.join("blob.bin"), b"corrupt").unwrap();
}

#[test]
fn previous_corruption_preserves_current_at_restart_and_scrub() {
    for restart in [true, false] {
        let dir = scratch();
        let me = crate::identity::for_test();
        let mut repo = Repository::open(dir.clone(), config()).unwrap();
        let (first, m1) = fixture(1, None, b"first");
        let (second, m2) = fixture(2, Some(first.hash()), b"second");
        accept(&repo, &first, b"first", &m1);
        repo.set_home(ROOM, &first, me.did()).unwrap();
        accept(&repo, &second, b"second", &m2);
        corrupt_blob(&dir, &m1);
        if restart {
            drop(repo);
            repo = Repository::open(dir.clone(), config()).unwrap();
        } else {
            repo.verify_holding(ROOM, &second).unwrap();
        }
        assert!(repo.held(ROOM, &second));
        assert_eq!(
            repo.generation_bytes(ROOM, &second).unwrap().unwrap().0,
            b"second"
        );
        assert!(repo.generation_bytes(ROOM, &first).unwrap().is_none());
        assert_eq!(repo.total_bytes(), second.size + m2.len() as u64);
        assert_eq!(std::fs::read_dir(dir.join("blobs")).unwrap().count(), 1);
        assert!(repo.reserve(ROOM, &second).unwrap().is_none());
        drop(repo);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn current_corruption_preserves_previous_at_restart_and_scrub() {
    for restart in [true, false] {
        let dir = scratch();
        let me = crate::identity::for_test();
        let mut repo = Repository::open(dir.clone(), config()).unwrap();
        let (first, m1) = fixture(1, None, b"first");
        let (second, m2) = fixture(2, Some(first.hash()), b"second");
        accept(&repo, &first, b"first", &m1);
        repo.set_home(ROOM, &first, me.did()).unwrap();
        accept(&repo, &second, b"second", &m2);
        corrupt_blob(&dir, &m2);
        if restart {
            drop(repo);
            repo = Repository::open(dir.clone(), config()).unwrap();
        } else {
            repo.verify_holding(ROOM, &second).unwrap();
        }
        assert!(!repo.held(ROOM, &second));
        assert_eq!(
            repo.generation_bytes(ROOM, &first).unwrap().unwrap().0,
            b"first"
        );
        assert_eq!(repo.total_bytes(), first.size + m1.len() as u64);
        assert_eq!(std::fs::read_dir(dir.join("blobs")).unwrap().count(), 1);
        accept(&repo, &second, b"second", &m2);
        assert!(repo.generation_bytes(ROOM, &first).unwrap().is_some());
        drop(repo);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn tombstones_accept_signed_gaps_and_advance_monotonically() {
    use decision::Acceptance::*;
    let dir = scratch();
    let repo = Repository::open(dir.clone(), config()).unwrap();
    let (first, _) = fixture(1, None, b"first");
    let (second, _) = fixture(2, Some(first.hash()), b"second");
    let (third, _) = fixture(3, Some(second.hash()), b"third");
    repo.learn(ROOM, &first, None, Some(&delete(&first)))
        .unwrap();
    let (bad_parent, _) = fixture(2, None, b"bad parent");
    assert_eq!(
        repo.learn(ROOM, &bad_parent, None, None).unwrap_err().code,
        "deleted"
    );
    let mut wrong_writer = third.clone();
    sign(&mut wrong_writer, 8);
    assert_eq!(
        repo.learn(ROOM, &wrong_writer, None, None).unwrap(),
        WriterMismatch
    );
    assert_eq!(repo.learn(ROOM, &third, None, None).unwrap(), New);
    assert_eq!(
        repo.learn(ROOM, &first, None, Some(&delete(&first)))
            .unwrap(),
        Stale
    );
    repo.learn(ROOM, &third, None, Some(&delete(&third)))
        .unwrap();
    let (fifth, _) = fixture(5, Some("ff".repeat(32)), b"fifth");
    assert_eq!(
        repo.learn(ROOM, &fifth, None, Some(&delete(&fifth)))
            .unwrap(),
        New
    );
    assert_eq!(
        repo.learn(ROOM, &third, None, Some(&delete(&third)))
            .unwrap(),
        Stale
    );
    assert_eq!(
        repo.learn(ROOM, &fifth, None, Some(&delete(&fifth)))
            .unwrap(),
        Same
    );
    drop(repo);
    let repo = Repository::open(dir.clone(), config()).unwrap();
    assert_eq!(repo.mesh_records(ROOM)[0].head, fifth);
    assert!(repo.head(ROOM, &first.slot).is_none());
    drop(repo);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn gossiped_successor_keeps_parent_retry_and_held_blob() {
    for held in [true, false] {
        let dir = scratch();
        let me = crate::identity::for_test();
        let mut repo = Repository::open(dir.clone(), config()).unwrap();
        let (first, m1) = fixture(1, None, b"first");
        let (second, m2) = fixture(2, Some(first.hash()), b"second");
        if held {
            accept(&repo, &first, b"first", &m1);
        } else {
            repo.learn(ROOM, &first, Some(me.did()), None).unwrap();
        }
        repo.learn(ROOM, &second, Some(me.did()), None).unwrap();
        for _ in 0..2 {
            assert!(repo.reserve(ROOM, &first).unwrap().is_none());
            assert_eq!(repo.head(ROOM, &first.slot), Some(second.clone()));
            assert_eq!(repo.generation_bytes(ROOM, &first).unwrap().is_some(), held);
            drop(repo);
            repo = Repository::open(dir.clone(), config()).unwrap();
        }
        accept(&repo, &second, b"second", &m2);
        assert!(repo.reserve(ROOM, &first).unwrap().is_none());
        assert_eq!(repo.generation_bytes(ROOM, &first).unwrap().is_some(), held);
        let (third, _) = fixture(3, Some(second.hash()), b"third");
        repo.learn(ROOM, &third, None, None).unwrap();
        assert_eq!(repo.reserve(ROOM, &first).err().unwrap().code, "conflict");
        assert!(repo.generation_bytes(ROOM, &first).unwrap().is_none());
        drop(repo);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn backup_import_skips_deleted_slot_and_continues() {
    let dir = scratch();
    let service = Service::open(dir.clone(), config()).unwrap();
    let (first, m1) = fixture(1, None, b"first");
    let (mut other, m2) = fixture(1, None, b"other");
    other.slot = "bb".repeat(16);
    sign(&mut other, 7);
    accept(&service.repo, &first, b"first", &m1);
    service.repo.mesh_delete(ROOM, &delete(&first)).unwrap();
    let backup = dir.join("old.mistl-escrow");
    let mut writer =
        crate::escrow::portable::Writer::new(std::fs::File::create(&backup).unwrap(), 0).unwrap();
    writer.entry(ROOM, &first, &m1, b"first").unwrap();
    writer.entry(ROOM, &other, &m2, b"other").unwrap();
    writer.finish().unwrap();
    let result =
        crate::escrow::portable_io::import_file(&service, &backup, &None, &[ROOM.into()]).unwrap();
    assert_eq!(result["imported"], 1);
    assert_eq!(result["skipped"][0]["reason"], "deleted");
    assert!(service.repo.head(ROOM, &first.slot).is_none());
    assert_eq!(service.repo.head(ROOM, &other.slot), Some(other));
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn export_counts_metadata_only_slots_as_not_held() {
    let dir = scratch();
    let service = Service::open(dir.clone(), config()).unwrap();
    let (first, _) = fixture(1, None, b"first");
    service.repo.learn(ROOM, &first, None, None).unwrap();
    let result = crate::escrow::portable_io::export_to(&service, Vec::new(), &None).unwrap();
    assert_eq!(result["not_held"], 1);
    assert_eq!(result["slots"], 0);
    assert_eq!(result["skipped"][0]["reason"], "not_held");
    drop(service);
    std::fs::remove_dir_all(dir).unwrap();
}
