use super::tests::{Network, wait_for};
use super::*;
use crate::config::EscrowReplication;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};

struct ControlHarness {
    dir: PathBuf,
    network: Arc<Network>,
    identities: Vec<Arc<Identity>>,
    nodes: Vec<Arc<Cluster>>,
    genesis: Vec<String>,
    seed: Option<String>,
    quota: Option<(u64, u64)>,
}
impl ControlHarness {
    fn new(seed: bool) -> Self {
        let dir =
            std::env::temp_dir().join(format!("mistl-control-{:016x}", rand::random::<u64>()));
        let identities: Vec<_> = (0..4)
            .map(|_| Arc::new(crate::identity::for_test()))
            .collect();
        let genesis = identities[..3].iter().map(|id| id.node_id()).collect();
        let migration_seed = seed.then(|| identities[0].node_id());
        Self {
            dir,
            network: Arc::new(Network::default()),
            identities,
            nodes: vec![],
            genesis,
            seed: migration_seed,
            quota: None,
        }
    }
    fn open(&self, index: usize) -> Arc<Cluster> {
        let identity = self.identities[index].clone();
        let id = identity.node_id();
        let cfg = EscrowConfig {
            max_blob_bytes: self
                .quota
                .map_or(EscrowConfig::default().max_blob_bytes, |q| q.0),
            max_total_bytes: self
                .quota
                .map_or(EscrowConfig::default().max_total_bytes, |q| q.1),
            rooms: vec!["test-room".into()],
            replication: vec![EscrowReplication {
                room: "test-room".into(),
                voters: self.genesis.clone(),
                learners: if index == 3 { vec![id.clone()] } else { vec![] },
                migration_seed: self.seed.clone(),
            }],
            ..Default::default()
        };
        let node = Cluster::open_with_sender(
            self.dir.join(&id),
            "test-room".into(),
            self.genesis.clone(),
            identity,
            cfg,
            self.network.sender(id.clone()),
        )
        .unwrap();
        self.network
            .nodes
            .lock()
            .unwrap()
            .insert(id, Arc::downgrade(&node));
        node
    }
    fn start(&self, index: usize) {
        let network = self.network.clone();
        let seed = self.seed.clone();
        let base = network.puller(self.identities[index].node_id());
        let pull: Puller = Arc::new(move |room, from, head| {
            let network = network.clone();
            let base = base.clone();
            let seed = seed.clone();
            Box::pin(async move {
                if seed.as_deref() == Some(&from) {
                    if let Some(bytes) = network.files.lock().unwrap().get(&head.hash()).cloned() {
                        return Ok(bytes);
                    }
                }
                base(room, from, head).await
            })
        });
        self.nodes[index].start(pull).unwrap();
    }
    async fn leader(&self) -> usize {
        wait_for(|| {
            self.nodes
                .iter()
                .filter(|node| node.status()["role"] == "leader")
                .count()
                == 1
        })
        .await;
        self.nodes
            .iter()
            .position(|node| node.status()["role"] == "leader")
            .unwrap()
    }
    async fn put(&self, index: usize, head: &Head) -> Value {
        tokio::time::timeout(
            Duration::from_secs(12),
            self.nodes[index].request(
                "browser",
                &json!({"v":1,"type":"escrow_put","id":"put","head":head}),
            ),
        )
        .await
        .unwrap()
        .unwrap()
    }
}
impl Drop for ControlHarness {
    fn drop(&mut self) {
        for node in &self.nodes {
            node.stop();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn learner_catches_up_then_joint_change_removes_leader_without_losing_data() {
    let mut h = ControlHarness::new(false);
    for index in 0..3 {
        h.nodes.push(h.open(index));
        h.start(index);
    }
    let leader = h.leader().await;
    let head = h
        .network
        .fixture(b"encrypted membership checkpoint", 1, None);
    assert_eq!(h.put(leader, &head).await, head.ack("put"));
    let learner = h.identities[3].node_id();
    assert_eq!(
        h.nodes[leader]
            .change_membership(h.genesis.clone(), vec![learner.clone()])
            .await
            .unwrap()["accepted"],
        true
    );
    h.nodes.push(h.open(3));
    h.start(3);
    wait_for(|| h.nodes[3].inner.lock().unwrap().state.head(&head.slot) == Some(head.clone()))
        .await;
    wait_for(|| {
        let inner = h.nodes[leader].inner.lock().unwrap();
        inner.engine.matched_index(&NodeId(learner.clone())) >= inner.engine.commit_index()
    })
    .await;
    assert_eq!(h.nodes[3].status()["role"], "follower");
    let voters: Vec<_> = h
        .identities
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != leader)
        .map(|(_, id)| id.node_id())
        .collect();
    let reply = h.nodes[leader]
        .change_membership(voters.clone(), vec![])
        .await
        .unwrap();
    assert_eq!(reply["accepted"], true, "{reply}");
    let desired = membership::configuration(&voters, &[]).unwrap();
    wait_for(|| {
        h.nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader)
            .all(|(_, node)| node.inner.lock().unwrap().engine.committed_membership() == desired)
    })
    .await;
    let new_leader = h.leader().await;
    assert_ne!(new_leader, leader);
    let next = h
        .network
        .fixture(b"checkpoint after replacement", 2, Some(head.hash()));
    assert_eq!(h.put(new_leader, &next).await, next.ack("put"));
    wait_for(|| h.nodes[3].inner.lock().unwrap().state.head(&next.slot) == Some(next.clone()))
        .await;
    h.nodes[3].stop();
    h.nodes[3] = h.open(3);
    assert_eq!(
        h.nodes[3]
            .inner
            .lock()
            .unwrap()
            .engine
            .committed_membership(),
        desired
    );
    assert_eq!(
        h.nodes[3].archive.load(&next).unwrap().0,
        b"checkpoint after replacement"
    );
}

#[tokio::test]
async fn explicit_seed_migrates_multiple_batches_and_restart_keeps_sealed_state() {
    let mut h = ControlHarness::new(true);
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut heads = vec![];
    for index in 0..15u64 {
        let mut head = h
            .network
            .fixture(format!("legacy ciphertext {index}").as_bytes(), 1, None);
        let data = h
            .network
            .files
            .lock()
            .unwrap()
            .remove(&head.hash())
            .unwrap();
        head.slot = format!("{index:032x}");
        head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
        h.network.files.lock().unwrap().insert(head.hash(), data);
        heads.push(head);
    }
    for index in 0..3 {
        h.nodes.push(h.open(index));
    }
    h.nodes[0].set_migration_source(heads.clone()).unwrap();
    // A different voter leads, exercising signed seed request/reply and body pulls.
    h.nodes[1].event(Event::ElectionTimeout, &mut Timers::new(), false);
    for index in 0..3 {
        h.start(index);
    }
    wait_for(|| {
        h.nodes
            .iter()
            .all(|node| node.status()["migration_finished"] == true)
    })
    .await;
    for node in &h.nodes {
        for head in &heads {
            assert_eq!(
                node.inner.lock().unwrap().state.head(&head.slot),
                Some(head.clone())
            );
            assert!(node.archive.has(head));
        }
    }
    let leader = h.leader().await;
    h.nodes[leader].stop();
    h.nodes[leader] = h.open(leader);
    assert_eq!(h.nodes[leader].status()["migration_finished"], true);
    for head in &heads {
        assert!(h.nodes[leader].archive.has(head));
    }
}

#[tokio::test]
async fn ciphertext_pressure_compacts_before_entry_threshold_and_reclaims_capacity() {
    let mut h = ControlHarness::new(false);
    h.quota = Some((16 * 1024, 128 * 1024));
    for index in 0..3 {
        h.nodes.push(h.open(index));
        h.start(index);
    }
    let leader = h.leader().await;
    let mut parent = None;
    let mut first = None;
    for seq in 1..=12 {
        let head = h
            .network
            .fixture(&vec![seq as u8; 16 * 1024], seq, parent.clone());
        assert_eq!(h.put(leader, &head).await, head.ack("put"));
        if first.is_none() {
            first = Some(head.clone());
        }
        parent = Some(head.hash());
    }
    assert!(h.nodes[leader].status()["snapshot_index"].as_u64().unwrap() > 0);
    assert!(h.nodes[leader].status()["commit_index"].as_u64().unwrap() < 128);
    assert!(!h.nodes[leader].archive.has(&first.unwrap()));
    assert!(h.nodes[leader].archive.total_bytes() < 128 * 1024);
}
