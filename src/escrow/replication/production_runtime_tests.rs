use super::tests::Harness;
use super::*;

#[tokio::test]
async fn compacted_cluster_installs_snapshot_only_after_ciphertext_and_survives_restart() {
    let mut harness = Harness::new();
    let leader = harness.leader(None).await;
    let follower = (leader + 1) % 3;
    let follower_id = harness.identities[follower].node_id();
    harness
        .network
        .partitioned
        .lock()
        .unwrap()
        .insert(follower_id.clone());
    let mut parent = None;
    let mut heads = Vec::new();
    for seq in 1..=72u64 {
        let head = harness.network.fixture(
            format!("ciphertext generation {seq}").as_bytes(),
            seq,
            parent.clone(),
        );
        assert_eq!(harness.put(leader, &head).await, head.ack("put"));
        parent = Some(head.hash());
        heads.push(head);
    }
    let latest = heads.last().unwrap().clone();
    wait_condition(&harness, "leader compaction", || {
        harness.nodes[leader].status()["snapshot_index"]
            .as_u64()
            .unwrap_or(0)
            > 0
    })
    .await;
    assert!(
        harness.nodes[leader]
            .inner
            .lock()
            .unwrap()
            .engine
            .log()
            .len()
            < 128
    );
    assert!(!harness.nodes[follower].archive.has(&latest));
    harness
        .network
        .stalled
        .lock()
        .unwrap()
        .insert(follower_id.clone());
    harness
        .network
        .partitioned
        .lock()
        .unwrap()
        .remove(&follower_id);
    wait_condition(&harness, "follower ciphertext preparation", || {
        harness.nodes[follower]
            .preparing_snapshot
            .load(Ordering::Relaxed)
    })
    .await;
    assert!(
        harness.nodes[follower]
            .inner
            .lock()
            .unwrap()
            .engine
            .snapshot()
            .is_none(),
        "snapshot must not publish before all referenced ciphertext is durable"
    );
    assert!(!harness.nodes[follower].archive.has(&latest));
    harness.network.stalled.lock().unwrap().remove(&follower_id);
    harness.network.wake.notify_waiters();
    wait_condition(&harness, "follower applied latest after snapshot", || {
        harness
            .nodes
            .iter()
            .all(|node| node.inner.lock().unwrap().state.head(&latest.slot) == Some(latest.clone()))
    })
    .await;
    assert!(
        harness.nodes[follower].status()["snapshot_index"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    for node in &harness.nodes {
        assert_eq!(
            node.archive.load(&latest).unwrap().0,
            b"ciphertext generation 72"
        );
        assert_eq!(node.status()["commit_index"], node.status()["last_applied"]);
        assert!(
            !node.archive.has(&heads[0]),
            "compacted history must release disk capacity"
        );
    }
    for node in &harness.nodes {
        node.stop();
    }
    for index in 0..3 {
        harness.nodes[index] = harness.open(index);
    }
    let leader = harness.leader(None).await;
    let reply = harness.nodes[leader]
        .request(
            "browser",
            &json!({"v":1,"type":"escrow_get","id":"restored","slot":latest.slot}),
        )
        .await
        .unwrap();
    assert_eq!(reply["head"], json!(latest));
    for node in &harness.nodes {
        assert!(node.archive.has(&latest));
        assert!(node.archive.has(&heads[70]));
        assert!(!node.archive.has(&heads[0]));
    }
}

#[tokio::test]
async fn compacted_snapshot_missing_ciphertext_refuses_restart() {
    let harness = Harness::new();
    let leader = harness.leader(None).await;
    let head = harness
        .network
        .fixture(b"encrypted snapshot payload", 1, None);
    assert_eq!(harness.put(leader, &head).await, head.ack("put"));
    {
        let node = &harness.nodes[leader];
        let mut inner = node.inner.lock().unwrap();
        let data = inner.state.encode().unwrap();
        inner.engine.compact(data).unwrap();
    }
    harness.nodes[leader].stop();
    let node_id = harness.identities[leader].node_id();
    std::fs::remove_file(
        harness
            .dir
            .join(&node_id)
            .join("archive")
            .join(head.hash())
            .join("blob.bin"),
    )
    .unwrap();
    let result = Cluster::open_with_sender(
        harness.dir.join(&node_id),
        "test-room".into(),
        harness
            .identities
            .iter()
            .map(|identity| identity.node_id())
            .collect(),
        harness.identities[leader].clone(),
        EscrowConfig::default(),
        harness.network.sender(node_id),
    );
    assert!(
        result.is_err(),
        "snapshot cannot restore without durable referenced ciphertext"
    );
}

async fn wait_condition(harness: &Harness, label: &str, condition: impl Fn() -> bool) {
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if result.is_err() {
        let status: Vec<_> = harness
            .nodes
            .iter()
            .map(|node| {
                let mut status = node.status();
                let inner = node.inner.lock().unwrap();
                status["term"] = json!(inner.engine.current_term());
                status["preparing_snapshot"] =
                    json!(node.preparing_snapshot.load(Ordering::Relaxed));
                status
            })
            .collect();
        panic!("{label} timed out: {status:?}");
    }
}

#[tokio::test]
async fn captured_old_snapshot_uses_current_durable_state_after_pressure_compaction() {
    let dir = RuntimeDir::new();
    let identity = Arc::new(crate::identity::for_test());
    let learner = Arc::new(crate::identity::for_test());
    let cfg = regression_config(&identity, &learner, 32 * 1024, 64 * 1024);
    let (sent, mut received) = mpsc::unbounded_channel::<Value>();
    let sender: Sender = Arc::new(move |_, _, bytes| {
        let sent = sent.clone();
        Box::pin(async move {
            sent.send(serde_json::from_slice(&bytes)?)
                .map_err(|_| anyhow!("test receiver closed"))?;
            Ok(())
        })
    });
    let node = Cluster::open_with_sender(
        dir.0.clone(),
        "test-room".into(),
        vec![identity.node_id()],
        identity.clone(),
        cfg,
        sender,
    )
    .unwrap();
    let network = super::tests::Network::default();
    let mut timers = Timers::new();
    let mut parent = None;
    let mut heads = Vec::new();
    let mut inner = node.inner.lock().unwrap();
    let election = inner.engine.step(Event::ElectionTimeout).unwrap();
    node.outputs(&mut inner, election, &mut timers, false);
    let mut old_index = 0;
    let mut captured = Vec::new();
    for seq in 1..=4u64 {
        let blob = vec![seq as u8; 8 * 1024];
        let head = network.fixture(&blob, seq, parent.clone());
        let manifest = network
            .files
            .lock()
            .unwrap()
            .get(&head.hash())
            .unwrap()
            .1
            .clone();
        node.archive
            .stage("test-room", &head, &blob, &manifest)
            .unwrap();
        let command = Command::Put {
            ticket: format!("generation-{seq}"),
            head: head.clone(),
        };
        captured = inner
            .engine
            .step(Event::ClientPropose {
                command: serde_json::to_vec(&command).unwrap(),
            })
            .unwrap();
        assert_eq!(
            inner.state.apply(&command, "put", APPLY_SLOTS),
            head.ack("put")
        );
        let committed = inner.engine.commit_index();
        inner.engine.mark_applied(committed).unwrap();
        if seq == 1 {
            let data = inner.state.encode().unwrap();
            inner.engine.compact(data).unwrap();
            old_index = inner.engine.snapshot().unwrap().last_included_index;
        }
        parent = Some(head.hash());
        heads.push(head);
    }
    assert!(captured.iter().any(|output| matches!(output,
        Output::SendMessage { msg:RaftMessage::InstallSnapshot {snapshot,..},.. }
            if snapshot.last_included_index == old_index)));
    assert!(node.archive.total_bytes() > node.cfg.max_total_bytes / 2);
    node.outputs(&mut inner, captured, &mut timers, false);
    assert!(
        inner.error.is_none(),
        "GC must not invalidate a captured snapshot RPC: {:?}",
        inner.error
    );
    let current_index = inner.engine.snapshot().unwrap().last_included_index;
    assert!(current_index > old_index);
    assert!(
        !node.archive.has(&heads[0]),
        "old snapshot generation must have been collected"
    );
    drop(inner);
    let offer = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = received
                .recv()
                .await
                .expect("sender closed before snapshot offer");
            if message["body"]["kind"] == "snapshot_offer" {
                break message;
            }
        }
    })
    .await
    .expect("current durable snapshot was not sent");
    assert_eq!(offer["body"]["data"]["index"], json!(current_index));
    assert!(node.running.load(Ordering::Relaxed));
    assert_eq!(node.status()["role"], "leader");
    node.stop();
}

#[tokio::test]
async fn lowered_blob_limit_keeps_existing_snapshot_generations_without_fetching() {
    let dir = RuntimeDir::new();
    let identity = Arc::new(crate::identity::for_test());
    let learner = Arc::new(crate::identity::for_test());
    let cfg = regression_config(&identity, &learner, 32 * 1024, 64 * 1024);
    let sender: Sender = Arc::new(|_, _, _| Box::pin(async { Ok(()) }));
    let node = Cluster::open_with_sender(
        dir.0.clone(),
        "test-room".into(),
        vec![identity.node_id()],
        identity.clone(),
        cfg.clone(),
        sender.clone(),
    )
    .unwrap();
    let network = super::tests::Network::default();
    let blob = vec![7; 8 * 1024];
    let head = network.fixture(&blob, 1, None);
    let manifest = network
        .files
        .lock()
        .unwrap()
        .get(&head.hash())
        .unwrap()
        .1
        .clone();
    node.archive
        .stage("test-room", &head, &blob, &manifest)
        .unwrap();
    {
        let mut inner = node.inner.lock().unwrap();
        inner.engine.step(Event::ElectionTimeout).unwrap();
        let command = Command::Put {
            ticket: "saved".into(),
            head: head.clone(),
        };
        inner
            .engine
            .step(Event::ClientPropose {
                command: serde_json::to_vec(&command).unwrap(),
            })
            .unwrap();
        inner.state.apply(&command, "put", APPLY_SLOTS);
        let committed = inner.engine.commit_index();
        inner.engine.mark_applied(committed).unwrap();
        let data = inner.state.encode().unwrap();
        inner.engine.compact(data).unwrap();
    }
    node.stop();
    drop(node);
    let mut cfg = cfg;
    cfg.max_blob_bytes = 1;
    let retained = Cluster::open_with_sender(
        dir.0.clone(),
        "test-room".into(),
        vec![identity.node_id()],
        identity,
        cfg,
        sender,
    )
    .unwrap();
    let lease = retained
        .stage("unavailable", &head)
        .await
        .expect("existing durable generation must remain usable after lowering upload limit");
    assert_eq!(retained.archive.load(&head).unwrap().0, blob);
    drop(lease);
    let unknown = network.fixture(b"unknown ciphertext", 2, Some(head.hash()));
    assert_eq!(
        retained
            .stage("unavailable", &unknown)
            .await
            .err()
            .unwrap()
            .code,
        "too_large"
    );
    retained.stop();
}

fn regression_config(
    identity: &Identity,
    learner: &Identity,
    max_blob: u64,
    quota: u64,
) -> EscrowConfig {
    EscrowConfig {
        rooms: vec!["test-room".into()],
        replication: vec![crate::config::EscrowReplication {
            room: "test-room".into(),
            voters: vec![identity.node_id()],
            learners: vec![learner.node_id()],
            migration_seed: None,
        }],
        max_blob_bytes: max_blob,
        max_total_bytes: quota,
        ..Default::default()
    }
}
struct RuntimeDir(PathBuf);
impl RuntimeDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mistl-runtime-regression-{:016x}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for RuntimeDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn regained_leader_replaces_old_term_snapshot_offer_without_waiting_for_expiry() {
    let harness = Harness::new();
    let leader = harness.leader(None).await;
    let follower = (leader + 1) % 3;
    let voting_peer = (leader + 2) % 3;
    let follower_id = harness.identities[follower].node_id();
    harness
        .network
        .partitioned
        .lock()
        .unwrap()
        .insert(follower_id.clone());
    let first = harness.network.fixture(b"old encrypted snapshot", 1, None);
    assert_eq!(harness.put(leader, &first).await, first.ack("put"));
    let old_term;
    {
        let node = &harness.nodes[leader];
        let mut inner = node.inner.lock().unwrap();
        let data = inner.state.encode().unwrap();
        inner.engine.compact(data).unwrap();
        old_term = inner.engine.current_term();
        let snapshot = inner.engine.snapshot().unwrap();
        let lease = node
            .archive
            .pin_heads(std::slice::from_ref(&first))
            .unwrap();
        assert!(
            node.snapshots
                .lock()
                .unwrap()
                .offer(&follower_id, old_term, snapshot, lease, Instant::now())
                .unwrap()
                .is_some()
        );
    }
    let second = harness
        .network
        .fixture(b"next encrypted snapshot", 2, Some(first.hash()));
    let latest = harness
        .network
        .fixture(b"latest encrypted snapshot", 3, Some(second.hash()));
    assert_eq!(harness.put(leader, &second).await, second.ack("put"));
    assert_eq!(harness.put(leader, &latest).await, latest.ack("put"));
    let snapshot_index;
    let last_log_term;
    {
        let node = &harness.nodes[leader];
        let mut inner = node.inner.lock().unwrap();
        let data = inner.state.encode().unwrap();
        inner.engine.compact(data).unwrap();
        let snapshot = inner.engine.snapshot().unwrap();
        snapshot_index = snapshot.last_included_index;
        last_log_term = snapshot.last_included_term;
        node.collect(&inner).unwrap();
        assert!(
            node.archive.has(&first),
            "active old offer must retain its ciphertext until term changes"
        );
    }
    let peer = NodeId(harness.identities[voting_peer].node_id());
    let mut timers = Timers::new();
    harness.nodes[leader].event(
        Event::Message {
            from: peer.clone(),
            msg: RaftMessage::RequestVote {
                term: old_term + 1,
                candidate_id: peer,
                last_log_index: snapshot_index,
                last_log_term,
            },
        },
        &mut timers,
        false,
    );
    assert_ne!(harness.nodes[leader].status()["role"], "leader");
    harness.nodes[leader].event(Event::ElectionTimeout, &mut timers, false);
    wait_condition(&harness, "original leader regained newer term", || {
        let inner = harness.nodes[leader].inner.lock().unwrap();
        inner.engine.role() == Role::Leader && inner.engine.current_term() > old_term
    })
    .await;
    harness
        .network
        .partitioned
        .lock()
        .unwrap()
        .remove(&follower_id);
    wait_condition(
        &harness,
        "old-term offered follower caught up without lease expiry",
        || {
            let inner = harness.nodes[follower].inner.lock().unwrap();
            inner
                .engine
                .snapshot()
                .is_some_and(|snapshot| snapshot.last_included_index >= snapshot_index)
                && inner.state.head(&latest.slot) == Some(latest.clone())
        },
    )
    .await;
    assert_eq!(
        harness.nodes[follower].archive.load(&latest).unwrap().0,
        b"latest encrypted snapshot"
    );
    assert!(harness.nodes[leader].running.load(Ordering::Relaxed));
}
