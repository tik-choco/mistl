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
