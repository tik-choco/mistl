use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, MULTICODEC_RAW, compute_cid};
use mistlib_core::storage::{CHUNK_SIZE, FileManifest};
use std::sync::Weak;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_export_uses_local_current_heads_and_pins_their_archive() {
    let harness = Harness::new();
    let leader = harness.leader(None).await;
    let first = harness.network.fixture(b"first", 1, None);
    assert_eq!(harness.put(leader, &first).await["type"], "escrow_ack");
    let current = harness.network.fixture(b"current", 2, Some(first.hash()));
    assert_eq!(harness.put(leader, &current).await["type"], "escrow_ack");
    wait_for(|| {
        harness.nodes.iter().all(|node| {
            node.inner.lock().unwrap().state.head(&current.slot) == Some(current.clone())
        })
    })
    .await;
    for node in &harness.nodes {
        let snapshot = node.export_heads().unwrap();
        assert_eq!(snapshot.heads, vec![current.clone()]);
        let mut writer = crate::escrow::portable::Writer::new(Vec::new(), 0).unwrap();
        for head in &snapshot.heads {
            let (blob, manifest) = node.export_bytes(head).unwrap();
            writer.entry("test-room", head, &manifest, &blob).unwrap();
        }
        let bytes = writer.finish().unwrap();
        let mut reader =
            crate::escrow::portable::Reader::new(std::io::Cursor::new(&bytes), 1024).unwrap();
        let entry = reader.next().unwrap().unwrap();
        assert_eq!(entry.head, current);
        assert_eq!(entry.blob, b"current");
        assert!(reader.next().unwrap().is_none());
        let service_dir = harness
            .dir
            .join(format!("export-test-{}", node.identity.node_id()));
        let service =
            crate::escrow::Service::open(service_dir.clone(), EscrowConfig::default()).unwrap();
        service
            .clusters
            .lock()
            .unwrap()
            .insert("test-room".into(), node.clone());
        let mut exported = Vec::new();
        let result = crate::escrow::portable_io::export_to(&service, &mut exported, &None).unwrap();
        assert_eq!(result["slots"], 1);
        let mut exported =
            crate::escrow::portable::Reader::new(std::io::Cursor::new(exported), 1024).unwrap();
        assert_eq!(exported.next().unwrap().unwrap().head, current);
        assert!(exported.next().unwrap().is_none());
        let path = service_dir.join("replicated.mistl-escrow");
        std::fs::write(&path, bytes).unwrap();
        let result =
            crate::escrow::portable_io::handle(&service, false, json!({"path":path}), &[]).unwrap();
        assert_eq!(result["imported"], 0);
        assert_eq!(result["skipped"][0]["reason"], "replicated");
        // Exercise collection without log/state retention after stopping this
        // test member: only the export lease protects its current generation.
        node.stop();
        node.archive.collect(&HashSet::new()).unwrap();
        assert!(node.archive.has(&current));
        drop(snapshot);
    }
}

#[derive(Default)]
pub(super) struct Network {
    pub(super) nodes: StdMutex<HashMap<String, Weak<Cluster>>>,
    pub(super) partitioned: StdMutex<HashSet<String>>,
    pub(super) stalled: StdMutex<HashSet<String>>,
    pub(super) wake: Notify,
    hidden_commits: StdMutex<HashMap<String, u64>>,
    pub(super) files: StdMutex<HashMap<String, (Vec<u8>, Vec<u8>)>>,
}

impl Network {
    pub(super) fn sender(self: &Arc<Self>, source: String) -> Sender {
        let network = self.clone();
        Arc::new(move |_, target, bytes| {
            let network = network.clone();
            let source = source.clone();
            Box::pin(async move {
                let partitioned = network.partitioned.lock().unwrap();
                if partitioned.contains(&source) || partitioned.contains(&target) {
                    return Ok(());
                }
                drop(partitioned);
                if let Some(boundary) = network.hidden_commits.lock().unwrap().get(&source) {
                    if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                        if let Ok(Body::Raft(RaftMessage::AppendEntries {
                            leader_commit, ..
                        })) = serde_json::from_value(value["body"].clone())
                        {
                            if leader_commit >= *boundary {
                                return Ok(());
                            }
                        }
                    }
                }
                let target = network
                    .nodes
                    .lock()
                    .unwrap()
                    .get(&target)
                    .and_then(Weak::upgrade);
                if let Some(target) = target {
                    target.receive(&source, &bytes);
                }
                Ok(())
            })
        })
    }

    pub(super) fn puller(self: &Arc<Self>, target: String) -> Puller {
        let network = self.clone();
        Arc::new(move |_, source, head| {
            let network = network.clone();
            let target = target.clone();
            Box::pin(async move {
                loop {
                    let wake = network.wake.notified();
                    if !network.stalled.lock().unwrap().contains(&target) {
                        break;
                    }
                    wake.await;
                }
                if source == "browser" {
                    return network
                        .files
                        .lock()
                        .unwrap()
                        .get(&head.hash())
                        .cloned()
                        .ok_or_else(|| Error::new("fetch_failed"));
                }
                let source = network
                    .nodes
                    .lock()
                    .unwrap()
                    .get(&source)
                    .and_then(Weak::upgrade)
                    .ok_or_else(|| Error::new("fetch_failed"))?;
                let bytes = source
                    .block(&head.cid)
                    .ok_or_else(|| Error::new("fetch_failed"))?;
                let manifest: FileManifest =
                    serde_cbor::from_slice(&bytes).map_err(|_| Error::new("fetch_failed"))?;
                let mut blob = Vec::new();
                for cid in &manifest.chunks {
                    blob.extend(
                        source
                            .block(cid)
                            .ok_or_else(|| Error::new("fetch_failed"))?,
                    );
                }
                Ok((blob, bytes))
            })
        })
    }

    pub(super) fn fixture(&self, content: &[u8], seq: u64, parent: Option<String>) -> Head {
        let manifest = FileManifest {
            name: "tc-escrow.bin".into(),
            size: content.len() as u64,
            chunks: content
                .chunks(CHUNK_SIZE)
                .map(|bytes| compute_cid(bytes, MULTICODEC_RAW))
                .collect(),
        };
        let bytes = serde_cbor::to_vec(&manifest).unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut head = Head {
            v: 1,
            kind: "tc-escrow-head".into(),
            slot: "ab".repeat(16),
            seq,
            parent,
            cid: compute_cid(&bytes, MULTICODEC_DAG_CBOR),
            size: content.len() as u64,
            sha256: super::super::protocol::digest(content),
            comp: "none".into(),
            schema: "test/1".into(),
            writer: URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
            at: seq,
            sig: String::new(),
        };
        head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
        self.files
            .lock()
            .unwrap()
            .insert(head.hash(), (content.to_vec(), bytes));
        head
    }
}

pub(super) struct Harness {
    pub(super) dir: PathBuf,
    pub(super) network: Arc<Network>,
    pub(super) identities: Vec<Arc<Identity>>,
    pub(super) nodes: Vec<Arc<Cluster>>,
}

impl Harness {
    pub(super) fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "mistl-replication-network-{:016x}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let network = Arc::new(Network::default());
        let identities: Vec<_> = (0..3)
            .map(|_| Arc::new(crate::identity::for_test()))
            .collect();
        let mut harness = Self {
            dir,
            network,
            identities,
            nodes: Vec::new(),
        };
        for index in 0..3 {
            harness.nodes.push(harness.open(index));
        }
        harness
    }

    pub(super) fn open(&self, index: usize) -> Arc<Cluster> {
        let identity = self.identities[index].clone();
        let id = identity.node_id();
        let node = Cluster::open_with_sender(
            self.dir.join(&id),
            "test-room".into(),
            self.identities
                .iter()
                .map(|identity| identity.node_id())
                .collect(),
            identity,
            EscrowConfig::default(),
            self.network.sender(id.clone()),
        )
        .unwrap();
        self.network
            .nodes
            .lock()
            .unwrap()
            .insert(id.clone(), Arc::downgrade(&node));
        node.start(self.network.puller(id)).unwrap();
        node
    }

    async fn wait_condition(&self, phase: &str, condition: impl Fn() -> bool) {
        let result = tokio::time::timeout(Duration::from_secs(12), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if result.is_err() {
            let status: Vec<_> = self
                .nodes
                .iter()
                .map(|node| {
                    let mut status = node.status();
                    let inner = node.inner.lock().unwrap();
                    status["term"] = json!(inner.engine.current_term());
                    status["tail"] = json!(
                        inner
                            .engine
                            .log()
                            .iter()
                            .map(|entry| (entry.index, entry.term))
                            .collect::<Vec<_>>()
                    );
                    status["preparing"] = json!(node.preparing.lock().unwrap().clone());
                    status
                })
                .collect();
            panic!("{phase} timed out: {status:?}");
        }
    }

    pub(super) async fn leader(&self, excluded: Option<usize>) -> usize {
        self.wait_condition("leader election", || {
            self.nodes
                .iter()
                .enumerate()
                .filter(|(index, node)| {
                    Some(*index) != excluded && node.status()["role"] == "leader"
                })
                .count()
                == 1
        })
        .await;
        self.nodes
            .iter()
            .enumerate()
            .find(|(index, node)| Some(*index) != excluded && node.status()["role"] == "leader")
            .unwrap()
            .0
    }

    pub(super) async fn put(&self, index: usize, head: &Head) -> Value {
        tokio::time::timeout(
            Duration::from_secs(12),
            self.nodes[index].request(
                "browser",
                &json!({
                    "v":1,"type":"escrow_put","id":"put","head":head,
                }),
            ),
        )
        .await
        .expect("write did not reach a quorum")
        .unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        for node in &self.nodes {
            node.stop();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub(super) async fn wait_for(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(12), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cluster condition did not become true");
}

#[tokio::test]
async fn durable_quorum_ack_waits_for_blob_then_all_replicas_serve_identical_data() {
    let harness = Harness::new();
    let leader = harness.leader(None).await;
    let followers: Vec<_> = (0..3).filter(|index| *index != leader).collect();
    for index in &followers {
        harness
            .network
            .stalled
            .lock()
            .unwrap()
            .insert(harness.identities[*index].node_id());
    }
    let head = harness
        .network
        .fixture(b"encrypted shared checkpoint", 1, None);
    let node = harness.nodes[leader].clone();
    let message = json!({"v":1,"type":"escrow_put","id":"put","head":head});
    let write = tokio::spawn(async move { node.request("browser", &message).await.unwrap() });
    wait_for(|| harness.nodes[leader].archive.has(&head)).await;
    wait_for(|| {
        harness.nodes[leader]
            .inner
            .lock()
            .unwrap()
            .engine
            .log()
            .iter()
            .any(|entry| {
                serde_json::from_slice::<Command>(&entry.command)
                    .is_ok_and(|command| matches!(command, Command::Put { .. }))
            })
    })
    .await;
    assert!(
        !write.is_finished(),
        "local blob must not count as a replicated save"
    );
    assert!(
        followers
            .iter()
            .all(|index| !harness.nodes[*index].archive.has(&head))
    );
    harness
        .network
        .stalled
        .lock()
        .unwrap()
        .remove(&harness.identities[followers[0]].node_id());
    harness.network.wake.notify_waiters();
    let response = tokio::time::timeout(Duration::from_secs(12), write)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, head.ack("put"));
    assert!(harness.nodes[followers[0]].archive.has(&head));
    assert!(!harness.nodes[followers[1]].archive.has(&head));
    harness.network.stalled.lock().unwrap().clear();
    harness.network.wake.notify_waiters();
    wait_for(|| {
        harness
            .nodes
            .iter()
            .all(|node| node.inner.lock().unwrap().state.head(&head.slot) == Some(head.clone()))
    })
    .await;
    for node in &harness.nodes {
        assert_eq!(
            node.archive.load(&head).unwrap().0,
            b"encrypted shared checkpoint"
        );
        assert_eq!(node.status()["last_applied"], node.status()["commit_index"]);
    }
}

#[tokio::test]
async fn majority_replaces_disconnected_leader_and_restart_preserves_latest_head() {
    let mut harness = Harness::new();
    let old_leader = harness.leader(None).await;
    let first = harness.network.fixture(b"first", 1, None);
    assert_eq!(harness.put(old_leader, &first).await["type"], "escrow_ack");
    harness
        .wait_condition("all replicas applied first", || {
            harness.nodes.iter().all(|node| {
                node.inner.lock().unwrap().state.head(&first.slot) == Some(first.clone())
            })
        })
        .await;
    harness
        .network
        .partitioned
        .lock()
        .unwrap()
        .insert(harness.identities[old_leader].node_id());
    let read = json!({"v":1,"type":"escrow_get","id":"isolated","slot":first.slot});
    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            harness.nodes[old_leader].request("browser", &read)
        )
        .await
        .is_err(),
        "isolated old leader must not answer a fresh read without quorum"
    );
    let leader = harness.leader(Some(old_leader)).await;
    let second = harness.network.fixture(b"second", 2, Some(first.hash()));
    assert_eq!(harness.put(leader, &second).await["type"], "escrow_ack");
    harness.network.partitioned.lock().unwrap().clear();
    harness
        .wait_condition("rejoined leader caught up", || {
            harness.nodes.iter().all(|node| {
                node.inner.lock().unwrap().state.head(&second.slot) == Some(second.clone())
            })
        })
        .await;
    harness.nodes[old_leader].stop();
    harness.nodes[old_leader] = harness.open(old_leader);
    assert_eq!(
        harness.nodes[old_leader]
            .inner
            .lock()
            .unwrap()
            .state
            .head(&second.slot),
        Some(second.clone())
    );
    assert_eq!(
        harness.nodes[old_leader].archive.load(&second).unwrap().0,
        b"second"
    );
    let read = json!({"v":1,"type":"escrow_get","id":"restored","slot":second.slot});
    harness
        .wait_condition("restarted follower knows leader", || {
            harness.nodes[old_leader].status()["leader"].is_string()
        })
        .await;
    let response = tokio::time::timeout(
        Duration::from_secs(12),
        harness.nodes[old_leader].request("browser", &read),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response["head"], json!(second));
}

#[tokio::test]
async fn new_leader_commits_acknowledged_previous_term_without_a_client_write() {
    let harness = Harness::new();
    let old_leader = harness.leader(None).await;
    let first = harness.network.fixture(b"first", 1, None);
    assert_eq!(harness.put(old_leader, &first).await, first.ack("put"));
    harness
        .wait_condition("initial replicas", || {
            harness.nodes.iter().all(|node| {
                node.inner.lock().unwrap().state.head(&first.slot) == Some(first.clone())
            })
        })
        .await;
    let boundary = harness.nodes[old_leader]
        .inner
        .lock()
        .unwrap()
        .engine
        .commit_index()
        + 2;
    harness
        .network
        .hidden_commits
        .lock()
        .unwrap()
        .insert(harness.identities[old_leader].node_id(), boundary);
    let second = harness
        .network
        .fixture(b"acknowledged ciphertext", 2, Some(first.hash()));
    assert_eq!(harness.put(old_leader, &second).await, second.ack("put"));
    assert!(
        harness
            .nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != old_leader)
            .all(|(_, node)| node.inner.lock().unwrap().engine.commit_index() < boundary)
    );
    harness.nodes[old_leader].stop();
    let new_leader = harness.leader(Some(old_leader)).await;
    harness
        .wait_condition(
            "previous-term ACK recovered without client activity",
            || {
                harness
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != old_leader)
                    .all(|(_, node)| {
                        let inner = node.inner.lock().unwrap();
                        inner.state.head(&second.slot) == Some(second.clone())
                            && inner.engine.commit_index() > boundary
                    })
            },
        )
        .await;
    assert_eq!(
        harness.nodes[new_leader].archive.load(&second).unwrap().0,
        b"acknowledged ciphertext"
    );
}
