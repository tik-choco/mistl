use super::super::{group::InviteCode, keys::OwnerKey, object, state, wire};
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};

struct Fixture(Core);
impl Fixture {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("mistl-vault-engine-{:016x}", rand::random::<u64>()));
        let cfg = crate::config::VaultConfig {
            min_free_bytes: 0,
            ..Default::default()
        };
        let me = Arc::new(OwnerKey::generate().identity());
        Self(Core::open(dir, cfg, me).unwrap())
    }
    fn call(&mut self, cmd: &str, args: Value) -> Value {
        self.0.command(cmd, args, false).unwrap().0
    }
    fn init(&mut self) -> Value {
        self.call("vault.init", json!({}))
    }
    fn put(&mut self, name: &str, bytes: &[u8]) -> Value {
        self.call(
            "vault.put",
            json!({"name":name,"data_b64":STANDARD.encode(bytes)}),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0.dir);
    }
}

#[test]
fn offline_commands_roundtrip_and_restart() {
    let mut f = Fixture::new();
    assert!(f.call("vault.status", json!({}))["group"].is_null());
    let init = f.init();
    assert_ne!(init["owner"], f.0.me.did());
    assert!(f.0.command("vault.init", json!({}), false).is_err());
    let put = f.put("hello.txt", b"hello vault");
    assert_eq!(put["seq"], 2);
    assert_eq!(
        f.call("vault.get", json!({"name":"hello.txt"}))["data_b64"],
        STANDARD.encode(b"hello vault")
    );
    let list = f.call("vault.ls", json!({}));
    assert_eq!(list["files"][0]["name"], "hello.txt");
    assert_eq!(list["files"][0]["size"], 11);
    let status = f.call("vault.status", json!({}));
    assert_eq!(status["owners"][0]["state"], "local_only");
    assert_eq!(status["owners"][0]["confirmed"], 1);
    assert_eq!(status["network"], false);
    assert!(
        !status
            .to_string()
            .contains(init["recovery_code"].as_str().unwrap())
    );
    let mut reopened = Core::open(f.0.dir.clone(), f.0.cfg.clone(), f.0.me.clone()).unwrap();
    assert!(reopened.owners.values().next().unwrap().complete);
    assert_eq!(
        reopened
            .command("vault.get", json!({"name":"hello.txt"}), false)
            .unwrap()
            .0["size"],
        11
    );
    assert_eq!(f.call("vault.rm", json!({"name":"hello.txt"}))["seq"], 3);
    assert_eq!(f.call("vault.ls", json!({}))["files"], json!([]));
    assert!(
        f.0.command("vault.get", json!({"name":"hello.txt"}), false)
            .is_err()
    );
}

#[test]
fn command_bounds_and_corrupt_blocks() {
    let mut f = Fixture::new();
    f.init();
    assert!(
        f.0.command("vault.put", json!({"name":"../bad","data_b64":""}), false)
            .is_err()
    );
    assert!(
        f.0.command("vault.put", json!({"name":"bad","data_b64":"?"}), false)
            .is_err()
    );
    f.0.cfg.max_object_bytes = 2;
    assert!(
        f.0.command(
            "vault.put",
            json!({"name":"large","data_b64":STANDARD.encode(b"abc")}),
            false
        )
        .is_err()
    );
    f.put("empty", b"");
    assert_eq!(f.call("vault.get", json!({"name":"empty"}))["data_b64"], "");
    f.0.cfg.max_object_bytes = 1024;
    f.put("payload", b"content");
    let entry = f.0.catalog().unwrap().files["payload"].clone();
    let descriptor = object::parse_descriptor(&f.0.store.get(&entry.object).unwrap()).unwrap();
    let cid = &descriptor.chunks[0];
    std::fs::write(f.0.store.dir().join(cid), b"tampered").unwrap();
    assert!(
        f.0.command("vault.get", json!({"name":"payload"}), false)
            .is_err()
    );
    assert!(!f.0.store.has(cid));
    let owner = f.0.owners.values().next().unwrap();
    let graph = f.0.graph(&owner.cur.pinset).unwrap();
    assert!(!graph.complete(owner.cur.bytes));
    assert!(graph.missing.contains(cid));
}

#[test]
fn path_input_and_streamed_output_verify_plaintext() {
    let mut f = Fixture::new();
    f.init();
    let input = f.0.dir.join("input.bin");
    let output = f.0.dir.join("output.bin");
    let bytes = vec![71; object::SEGMENT + 11];
    std::fs::write(&input, &bytes).unwrap();
    std::fs::write(&output, b"replace existing output").unwrap();
    let put = f.call("vault.put", json!({"path":input,"name":"stream.bin"}));
    assert_eq!(put["size"], bytes.len());
    let got = f.call("vault.get", json!({"name":"stream.bin","output":output}));
    assert_eq!(got["written"], output.to_str().unwrap());
    assert_eq!(std::fs::read(&output).unwrap(), bytes);
    assert!(got.get("data_b64").is_none());
}

#[test]
fn join_admission_retry_transport_and_membership_gates() {
    let mut inviter = Fixture::new();
    inviter.init();
    let invitation = inviter.call("vault.invite", json!({}));
    let code = InviteCode::parse(invitation["code"].as_str().unwrap()).unwrap();
    let mut joiner = Fixture::new();
    joiner.call("vault.join", json!({"code":invitation["code"]}));
    let reopened = Core::open(
        joiner.0.dir.clone(),
        joiner.0.cfg.clone(),
        joiner.0.me.clone(),
    )
    .unwrap();
    assert!(reopened.join.is_some());
    joiner.0 = reopened;
    let (invite, mac) = code.join_proof(joiner.0.me.did());
    let msg = joiner
        .0
        .message(None, "join", json!({"invite":invite,"mac":mac}))
        .unwrap();
    assert!(
        inviter
            .0
            .receive(&msg.room, "wrong-node", &msg.bytes)
            .is_err()
    );
    let replies = inviter
        .0
        .receive(&msg.room, &joiner.0.me.node_id(), &msg.bytes)
        .unwrap();
    assert!(
        inviter
            .0
            .group
            .as_ref()
            .unwrap()
            .is_member(joiner.0.me.did())
    );
    assert!(inviter.0.group.as_ref().unwrap().invites.is_empty());
    let members = replies.iter().find(|r| r.to.is_some()).unwrap();
    joiner
        .0
        .receive(&members.room, &inviter.0.me.node_id(), &members.bytes)
        .unwrap();
    assert!(joiner.0.join.is_none());
    assert_eq!(
        joiner.0.group.as_ref().unwrap().digest(),
        inviter.0.group.as_ref().unwrap().digest()
    );
    let retry = joiner
        .0
        .message(None, "join", json!({"invite":invite,"mac":mac}))
        .unwrap();
    assert!(
        !inviter
            .0
            .receive(&retry.room, &joiner.0.me.node_id(), &retry.bytes)
            .unwrap()
            .is_empty()
    );
    let outsider = OwnerKey::generate().identity();
    let raw = wire::seal(
        &outsider,
        &code.group,
        "head_req",
        json!({"owner":inviter.0.own_id()}),
        now_ms(),
    )
    .unwrap();
    assert!(
        inviter
            .0
            .receive(&msg.room, &outsider.node_id(), &raw)
            .is_err()
    );
    inviter.call("vault.members.remove", json!({"did":joiner.0.me.did()}));
    let raw = joiner
        .0
        .message(None, "head_req", json!({"owner":inviter.0.own_id()}))
        .unwrap();
    assert!(
        inviter
            .0
            .receive(&raw.room, &joiner.0.me.node_id(), &raw.bytes)
            .is_err()
    );
}

#[test]
fn restore_requires_head_and_adopts_existing_dataset() {
    let mut f = Fixture::new();
    let first = f.init();
    f.put("old", b"recover me");
    let unknown = OwnerKey::generate();
    let restored = f.call("vault.restore", json!({"code":unknown.recovery_code()}));
    assert!(restored.get("seq").is_none());
    assert!(
        f.0.command("vault.put", json!({"name":"unsafe","data_b64":""}), false)
            .is_err()
    );
    assert!(
        f.0.command("vault.restore", json!({"code":"bad"}), false)
            .is_err()
    );
    let restored = f.call("vault.restore", json!({"code":first["recovery_code"]}));
    assert_eq!(restored["seq"], 2);
    assert_eq!(
        f.call("vault.get", json!({"name":"old"}))["data_b64"],
        STANDARD.encode(b"recover me")
    );
    assert_eq!(f.put("new", b"new")["seq"], 3);
}

#[test]
fn hello_scales_to_owner_cap_and_state_reload_rejects_tampering() {
    let mut f = Fixture::new();
    f.init();
    let base = f.0.owners.values().next().unwrap().cur.clone();
    for _ in 1..state::MAX_OWNERS {
        let key = OwnerKey::generate();
        let head = super::super::head::Head::sign(
            &key,
            &base.group,
            None,
            &base.pinset,
            base.bytes,
            0,
            now_ms(),
        )
        .unwrap();
        state::accept_head(&mut f.0.owners, head, f.0.me.did(), now_ms()).unwrap();
    }
    let messages = f.0.hello(None).unwrap();
    assert_eq!(messages.len(), 1);
    assert!(messages[0].bytes.len() <= wire::MAX_MESSAGE);
    let key = OwnerKey::generate();
    let head = super::super::head::Head::sign(
        &key,
        &base.group,
        None,
        &base.pinset,
        base.bytes,
        0,
        now_ms(),
    )
    .unwrap();
    assert!(state::accept_head(&mut f.0.owners, head, f.0.me.did(), now_ms()).is_err());
    f.0.save().unwrap();
    let path = f.0.dir.join("owners.json");
    let mut saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    saved["owners"][0]["cur"]["seq"] = json!(999);
    std::fs::write(path, serde_json::to_vec(&saved).unwrap()).unwrap();
    assert!(Core::open(f.0.dir.clone(), f.0.cfg.clone(), f.0.me.clone()).is_err());
}

#[test]
fn status_counts_only_fresh_exact_head_confirmations() {
    let mut f = Fixture::new();
    f.init();
    let peer = OwnerKey::generate().identity();
    let now = now_ms();
    f.0.group
        .as_mut()
        .unwrap()
        .admit(&f.0.me, peer.did(), now)
        .unwrap();
    let head = f.0.owners.values().next().unwrap().cur.clone();
    let summary = state::Summary {
        owner: head.owner.clone(),
        seq: head.seq,
        hash: "0".repeat(64),
        complete: true,
        bytes: head.bytes,
    };
    f.0.peers.insert(
        peer.did().into(),
        state::Peer {
            first_seen: now,
            last_seen: now,
            free: 1000,
            capacity: 2000,
            heads: vec![],
            holds: vec![summary],
        },
    );
    assert_eq!(f.0.status(true)["owners"][0]["confirmed"], 1);
    f.0.peers.get_mut(peer.did()).unwrap().holds[0].hash = head.hash();
    assert_eq!(f.0.status(true)["owners"][0]["confirmed"], 2);
    assert_eq!(f.0.status(true)["owners"][0]["state"], "healthy");
    f.0.peers.get_mut(peer.did()).unwrap().last_seen = now - state::ONLINE_MS - 1;
    assert_eq!(f.0.status(true)["owners"][0]["state"], "degraded");
}

#[test]
fn quota_failure_does_not_advance_owner_head() {
    let mut f = Fixture::new();
    f.init();
    f.0.cfg.capacity_bytes = f.0.store.usage() + 1;
    let err =
        f.0.command(
            "vault.put",
            json!({"name":"too-large","data_b64":STANDARD.encode(b"hello")}),
            false,
        )
        .err()
        .unwrap();
    assert!(err.to_string().contains("insufficient capacity"));
    assert_eq!(f.0.owners.values().next().unwrap().cur.seq, 1);
    assert!(f.0.catalog().unwrap().files.is_empty());
}

#[test]
fn leave_retains_data_until_restart_and_preserves_owned_files() {
    let mut f = Fixture::new();
    f.init();
    f.put("mine", b"keep my data");
    let orphan = object::raw_cid(b"foreign holding");
    f.0.store.put(&orphan, b"foreign holding").unwrap();
    assert_eq!(f.call("vault.leave", json!({}))["left"], true);
    assert!(f.0.store.has(&orphan));
    let mut reopened = Core::open(f.0.dir.clone(), f.0.cfg.clone(), f.0.me.clone()).unwrap();
    assert!(!reopened.store.has(&orphan));
    assert_eq!(
        reopened
            .command("vault.get", json!({"name":"mine"}), false)
            .unwrap()
            .0["data_b64"],
        STANDARD.encode(b"keep my data")
    );
}
