//! Durable encrypted escrow replication. Successful writes require durable quorum replicas.
mod archive;
#[cfg(test)]
mod config_tests;
mod engine;
mod membership;
mod snapshots;
mod state;
#[cfg(test)]
mod tests;
mod wire;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use mistlib_consensus_core::state::decode_membership;
use mistlib_consensus_core::storage::RaftMembership;
use mistlib_consensus_core::{Event, LogEntry, NodeId, Output, RaftMessage, Role};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot};
use tokio::time::Instant;

use super::protocol::{Delete, Error, Head, MAX_MESSAGE, hex_string};
use crate::config::EscrowConfig;
use crate::identity::Identity;
use archive::Archive;
use engine::Engine;
use snapshots::{Receive, SnapshotTransfers};
use state::{Command, State};
use wire::{Body, Decoded, Replay};

const MAX_REQUESTS: usize = 32;
const MAX_PEER_QUEUE: usize = 64;
const APPLY_SLOTS: usize = 65536;
const REQUEST_WAIT: Duration = Duration::from_secs(35);
const SEND_WAIT: Duration = Duration::from_secs(2);
const HEARTBEAT: Duration = Duration::from_millis(500);

pub(super) type PullFuture =
    Pin<Box<dyn Future<Output = Result<(Vec<u8>, Vec<u8>), Error>> + Send>>;
pub(super) type Puller = Arc<dyn Fn(String, String, Head) -> PullFuture + Send + Sync>;
type SendFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub(super) type Sender = Arc<dyn Fn(String, String, Vec<u8>) -> SendFuture + Send + Sync>;

struct Pending {
    id: String,
    tx: oneshot::Sender<Value>,
}
struct ForwardPending {
    leader: String,
    tx: oneshot::Sender<Value>,
}
struct MembershipPending {
    target: RaftMembership,
    tx: oneshot::Sender<Value>,
}
struct MigrationPending {
    peer: String,
    offset: u32,
    tx: oneshot::Sender<(Vec<Head>, bool)>,
}
struct Inner {
    engine: Engine,
    state: State,
    pending: HashMap<String, Pending>,
    error: Option<String>,
    membership_pending: Option<MembershipPending>,
}
enum Inbound {
    Wire(Decoded),
    Prepared {
        from: String,
        msg: RaftMessage,
        leases: Vec<archive::Lease>,
    },
    Membership {
        voters: Vec<String>,
        learners: Vec<String>,
        tx: oneshot::Sender<Value>,
    },
    Propose {
        command: Command,
        term: u64,
        id: String,
        tx: oneshot::Sender<Value>,
    },
}
struct Timers {
    election: Instant,
    heartbeat: Instant,
}
impl Timers {
    fn new() -> Self {
        Self {
            election: election_deadline(),
            heartbeat: Instant::now() + HEARTBEAT,
        }
    }
}
fn election_deadline() -> Instant {
    Instant::now() + Duration::from_millis(1500 + u64::from(rand::random::<u16>()) % 1501)
}

pub(super) struct Cluster {
    room: String,
    voters: Vec<String>,
    initial_learners: Vec<String>,
    migration_seed: Option<String>,
    migration_source: OnceLock<Vec<Head>>,
    importing: AtomicBool,
    cluster: String,
    identity: Arc<Identity>,
    cfg: EscrowConfig,
    archive: Archive,
    archive_gate: StdMutex<()>,
    snapshots: StdMutex<SnapshotTransfers>,
    outgoing: StdMutex<HashMap<String, mpsc::Sender<Body>>>,
    weak: OnceLock<Weak<Self>>,
    migration_pending: StdMutex<HashMap<String, MigrationPending>>,
    preparing_snapshot: AtomicBool,
    inner: StdMutex<Inner>,
    replay: StdMutex<Replay>,
    queued: StdMutex<HashMap<String, usize>>,
    preparing: StdMutex<HashSet<String>>,
    forward: StdMutex<HashMap<String, ForwardPending>>,
    tx: mpsc::Sender<Inbound>,
    rx: StdMutex<Option<mpsc::Receiver<Inbound>>>,
    requests: Arc<Semaphore>,
    serial: Mutex<()>,
    pull: OnceLock<Puller>,
    sender: Sender,
    boot: u64,
    seq: AtomicU64,
    running: AtomicBool,
    tasks: StdMutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Cluster {
    pub(super) fn open(
        dir: PathBuf,
        room: String,
        voters: Vec<String>,
        identity: Arc<Identity>,
        cfg: EscrowConfig,
    ) -> Result<Arc<Self>> {
        let sender: Sender = Arc::new(|room, to, bytes| {
            Box::pin(async move { crate::net::send_direct(&room, &to, bytes).await })
        });
        Self::open_with_sender(dir, room, voters, identity, cfg, sender)
    }

    pub(super) fn open_with_sender(
        dir: PathBuf,
        room: String,
        mut voters: Vec<String>,
        identity: Arc<Identity>,
        cfg: EscrowConfig,
        sender: Sender,
    ) -> Result<Arc<Self>> {
        ensure!(super::valid_room(&room), "invalid escrow replication room");
        cfg.validate()?;
        voters.sort_unstable();
        let setting = cfg.replication.iter().find(|setting| setting.room == room);
        let initial_learners = setting.map_or_else(Vec::new, |setting| setting.learners.clone());
        let migration_seed = setting.and_then(|setting| setting.migration_seed.clone());
        let genesis = membership::configuration(&voters, &initial_learners)?;
        ensure!(
            voters.contains(&identity.node_id()) || initial_learners.contains(&identity.node_id()),
            "this daemon is not an enrolled escrow member"
        );
        crate::statefile::create_private_dir(&dir)?;
        let mut engine = Engine::open_with_membership(
            dir.join("raft.json"),
            NodeId(identity.node_id()),
            genesis.voters,
            genesis.learners,
        )?;
        let archive = Archive::open(dir.join("archive"), cfg.clone())?;
        let mut state = match engine.snapshot() {
            Some(snapshot) => State::decode(&snapshot.data, APPLY_SLOTS)?,
            None => State::default(),
        };
        ensure!(
            state.retained_heads().iter().all(|head| archive.has(head)),
            "snapshot generation missing"
        );
        for entry in engine.log() {
            if decode_membership(&entry.command).is_some() {
                continue;
            }
            let command: Command = serde_json::from_slice(&entry.command)?;
            state::validate(&command, 1024 * 1024 * 1024)
                .map_err(|error| anyhow!("invalid persisted escrow command: {}", error.code))?;
            ensure!(
                command_heads(&command).iter().all(|head| archive.has(head)),
                "Raft log refers to a missing durable generation"
            );
            if entry.index <= engine.commit_index() {
                state.apply(&command, "", APPLY_SLOTS);
            }
        }
        while engine.last_applied() < engine.commit_index() {
            engine.mark_applied(engine.last_applied() + 1)?;
        }
        let (tx, rx) = mpsc::channel(MAX_PEER_QUEUE * 18 + MAX_REQUESTS);
        let cluster = Arc::new(Self {
            cluster: wire::cluster_hash(&room, &voters),
            room,
            voters,
            initial_learners,
            migration_seed,
            migration_source: OnceLock::new(),
            importing: AtomicBool::new(false),
            identity,
            cfg,
            archive,
            archive_gate: StdMutex::new(()),
            snapshots: StdMutex::new(SnapshotTransfers::default()),
            outgoing: StdMutex::new(HashMap::new()),
            weak: OnceLock::new(),
            migration_pending: StdMutex::new(HashMap::new()),
            preparing_snapshot: AtomicBool::new(false),
            inner: StdMutex::new(Inner {
                engine,
                state,
                pending: HashMap::new(),
                error: None,
                membership_pending: None,
            }),
            replay: StdMutex::new(Replay::default()),
            queued: StdMutex::new(HashMap::new()),
            preparing: StdMutex::new(HashSet::new()),
            forward: StdMutex::new(HashMap::new()),
            tx,
            rx: StdMutex::new(Some(rx)),
            requests: Arc::new(Semaphore::new(MAX_REQUESTS)),
            serial: Mutex::new(()),
            pull: OnceLock::new(),
            sender,
            boot: rand::random(),
            seq: AtomicU64::new(0),
            running: AtomicBool::new(true),
            tasks: StdMutex::new(Vec::new()),
        });
        let _ = cluster.weak.set(Arc::downgrade(&cluster));
        Ok(cluster)
    }

    pub(super) fn start(self: &Arc<Self>, pull: Puller) -> Result<()> {
        ensure!(
            self.pull.set(pull).is_ok(),
            "escrow replication already started"
        );
        let rx = self
            .rx
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| anyhow!("escrow replication already started"))?;
        let cluster = self.clone();
        let handle = tokio::spawn(async move {
            cluster.drive(rx).await;
        });
        self.tasks.lock().unwrap().push(handle);
        Ok(())
    }

    pub(super) fn receive(&self, from: &str, raw: &[u8]) -> bool {
        if !self.running.load(Ordering::Relaxed) {
            return false;
        }
        let now = crate::net::peer_auth::now_ms();
        let mut admitted = self.voters.clone();
        admitted.extend(self.initial_learners.iter().cloned());
        {
            let inner = self.inner.lock().unwrap();
            admitted.extend(membership::peers(&inner.engine.membership()));
            admitted.extend(membership::peers(&inner.engine.committed_membership()));
        }
        admitted.sort_unstable();
        admitted.dedup();
        let Some(decoded) = wire::decode(raw, &self.room, &self.cluster, from, &admitted, now)
        else {
            return false;
        };
        if !crate::net::peer_auth::admitted(&decoded.did) {
            return true;
        }
        let mut queued = self.queued.lock().unwrap();
        if (!queued.contains_key(from) && queued.len() >= 64)
            || queued.get(from).copied().unwrap_or(0) >= MAX_PEER_QUEUE
        {
            return true;
        }
        if !self.replay.lock().unwrap().accept(&decoded, now) {
            return true;
        }
        if self.tx.try_send(Inbound::Wire(decoded)).is_ok() {
            *queued.entry(from.into()).or_default() += 1;
        }
        true
    }

    pub(super) fn block(&self, cid: &str) -> Option<Vec<u8>> {
        self.archive.block(&self.room, cid)
    }
    pub(super) fn total_bytes(&self) -> u64 {
        self.archive.total_bytes()
    }
    pub(super) fn status(&self) -> Value {
        let inner = self.inner.lock().unwrap();
        let role = match (inner.error.is_some(), inner.engine.role()) {
            (false, Role::Follower) => "follower",
            (false, Role::Candidate) => "candidate",
            (false, Role::Leader) => "leader",
            (true, _) => "stopped",
        };
        let membership = inner.engine.membership();
        let snapshot = inner.engine.snapshot();
        json!({"genesis":self.voters,"learners":membership.learners,"joint_voters":membership.joint_voters,
            "snapshot_index":snapshot.map_or(0, |snapshot| snapshot.last_included_index),
            "log_entries":inner.engine.log().len(),"migration_finished":inner.state.migration_finished(),
            "room":self.room,"role":role,"leader":if inner.error.is_none() {inner.engine.leader().map(|id|id.0)}else{None},
            "voters":inner.engine.voters().iter().map(|id| &id.0).collect::<Vec<_>>(),"quorum":inner.engine.quorum(),"commit_index":inner.engine.commit_index(),
            "last_applied":inner.engine.last_applied(),"slots":inner.state.slots(),"head_bytes":inner.state.total_bytes(),"bytes":self.archive.total_bytes(),"error":inner.error})
    }

    pub(super) async fn request(self: &Arc<Self>, from: &str, msg: &Value) -> Option<Value> {
        if msg["v"] != 1
            || !matches!(
                msg["type"].as_str(),
                Some("escrow_get" | "escrow_put" | "escrow_delete")
            )
        {
            return None;
        }
        let id = msg["id"].as_str().filter(|s| s.len() <= 64).unwrap_or("");
        if msg["id"].as_str().is_none_or(|s| s.len() > 64)
            || !serde_json::to_vec(msg).is_ok_and(|bytes| bytes.len() <= MAX_MESSAGE)
        {
            return Some(Error::new("bad_request").reply(id));
        }
        let Ok(_permit) = self.requests.clone().try_acquire_owned() else {
            return Some(Error::new("busy").reply(id));
        };
        let work = async {
            let (role, leader, failed) = {
                let inner = self.inner.lock().unwrap();
                (
                    inner.engine.role(),
                    inner.engine.leader(),
                    inner.error.is_some(),
                )
            };
            if failed || !self.running.load(Ordering::Relaxed) {
                return Error::new("fetch_failed").reply(id);
            }
            if role == Role::Leader {
                self.leader_request(from, msg, id).await
            } else if let Some(leader) = leader {
                self.forward_request(&leader.0, from, msg, id).await
            } else {
                Error::new("busy").reply(id)
            }
        };
        Some(
            tokio::time::timeout(REQUEST_WAIT, work)
                .await
                .unwrap_or_else(|_| Error::new("busy").reply(id)),
        )
    }

    async fn forward_request(
        &self,
        leader: &str,
        source: &str,
        request: &Value,
        id: &str,
    ) -> Value {
        let ticket = crate::net::peer_auth::fresh_nonce();
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.forward.lock().unwrap();
            pending.retain(|_, item| !item.tx.is_closed());
            if pending.len() >= MAX_REQUESTS {
                return Error::new("busy").reply(id);
            }
            pending.insert(
                ticket.clone(),
                ForwardPending {
                    leader: leader.into(),
                    tx,
                },
            );
        }
        let body = Body::Forward {
            request: request.clone(),
            source: source.into(),
            ticket: ticket.clone(),
        };
        let result = if self.send_wire(leader, &body).await.is_ok() {
            tokio::time::timeout(REQUEST_WAIT, rx)
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or_else(|| Error::new("busy").reply(id))
        } else {
            Error::new("busy").reply(id)
        };
        self.forward.lock().unwrap().remove(&ticket);
        result
    }

    async fn leader_request(&self, from: &str, msg: &Value, id: &str) -> Value {
        if msg["v"] != 1 || msg["id"].as_str().is_none_or(|id| id.len() > 64) {
            return Error::new("bad_request").reply(id);
        }
        // Stateless shape/signature gates do not consume a bounded log entry.
        let command = match msg["type"].as_str() {
            Some("escrow_get") => {
                if !msg["slot"]
                    .as_str()
                    .is_some_and(|slot| hex_string(slot, 32))
                {
                    return Error::new("bad_request").reply(id);
                }
                None
            }
            Some("escrow_put") => {
                if msg["head"].get("parent").is_none() {
                    return Error::new("bad_request").reply(id);
                }
                let Ok(head) = serde_json::from_value::<Head>(msg["head"].clone()) else {
                    return Error::new("bad_request").reply(id);
                };
                Some(Command::Put {
                    ticket: crate::net::peer_auth::fresh_nonce(),
                    head,
                })
            }
            Some("escrow_delete") => {
                let Ok(request) = serde_json::from_value::<Delete>(msg.clone()) else {
                    return Error::new("bad_request").reply(id);
                };
                Some(Command::Delete {
                    ticket: crate::net::peer_auth::fresh_nonce(),
                    request,
                })
            }
            _ => return Error::new("bad_request").reply(id),
        };
        if let Some(command) = &command {
            if let Err(error) = state::validate(command, self.cfg.max_blob_bytes) {
                return error.reply(id);
            }
        }
        let _serial = self.serial.lock().await;
        let term = {
            let inner = self.inner.lock().unwrap();
            if inner.error.is_some() || inner.engine.role() != Role::Leader {
                return Error::new("busy").reply(id);
            }
            if self.migration_seed.is_some() && !inner.state.migration_finished() {
                return Error::new("busy").reply(id);
            }
            inner.engine.current_term()
        };
        let barrier = Command::ReadBarrier {
            ticket: crate::net::peer_auth::fresh_nonce(),
        };
        let ready = self.propose(barrier, term, id).await;
        if ready["type"] != "escrow_read_ready" {
            return ready;
        }
        let Some(command) = command else {
            let slot = msg["slot"].as_str().unwrap();
            let inner = self.inner.lock().unwrap();
            return json!({"v":1,"type":"escrow_head","id":id,"slot":slot,"head":inner.state.head(slot)});
        };
        let (preview, changes) = {
            let inner = self.inner.lock().unwrap();
            if inner.engine.role() != Role::Leader || inner.engine.current_term() != term {
                return Error::new("busy").reply(id);
            }
            let mut next = inner.state.clone();
            let slot = match &command {
                Command::Put { head, .. } => &head.slot,
                Command::Delete { request, .. } => &request.slot,
                _ => unreachable!(),
            };
            let before = inner.state.head(slot);
            let result = next.apply(&command, id, self.cfg.max_slots_per_room);
            (result, before != next.head(slot))
        };
        if preview["type"] == "escrow_error" || !changes {
            return preview;
        }
        let _lease = if let Command::Put { head, .. } = &command {
            match self.stage(from, head).await {
                Ok(lease) => Some(lease),
                Err(error) => return error.reply(id),
            }
        } else {
            None
        };
        self.propose(command, term, id).await
    }

    async fn propose(&self, command: Command, term: u64, id: &str) -> Value {
        let (tx, rx) = oneshot::channel();
        let event = Inbound::Propose {
            command,
            term,
            id: id.into(),
            tx,
        };
        if self.tx.try_send(event).is_err() {
            return Error::new("busy").reply(id);
        }
        rx.await.unwrap_or_else(|_| Error::new("busy").reply(id))
    }

    async fn stage(&self, from: &str, head: &Head) -> Result<archive::Lease, Error> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(Error::new("fetch_failed"));
        }
        {
            let _gate = self.archive_gate.lock().unwrap();
            if self.archive.has(head) {
                return self
                    .archive
                    .pin_heads(&[head.clone()])
                    .map_err(|_| Error::new("fetch_failed"));
            }
        }
        let Some(pull) = self.pull.get() else {
            return Err(Error::new("busy"));
        };
        let (blob, manifest) = tokio::time::timeout(
            Duration::from_secs(30),
            pull(self.room.clone(), from.into(), head.clone()),
        )
        .await
        .map_err(|_| Error::new("fetch_failed"))??;
        if !self.running.load(Ordering::Relaxed) {
            return Err(Error::new("fetch_failed"));
        }
        let _gate = self.archive_gate.lock().unwrap();
        self.archive.stage(&self.room, head, &blob, &manifest)?;
        self.archive
            .pin_heads(&[head.clone()])
            .map_err(|_| Error::new("fetch_failed"))
    }

    fn queue(&self, peer: &str, body: Body) {
        if peer == self.identity.node_id() {
            return;
        }
        let mut outgoing = self.outgoing.lock().unwrap();
        if !outgoing.contains_key(peer) {
            if outgoing.len() >= 18 {
                return;
            }
            let Some(cluster) = self.weak.get().and_then(Weak::upgrade) else {
                return;
            };
            let (tx, mut rx) = mpsc::channel::<Body>(16);
            outgoing.insert(peer.into(), tx);
            let peer = peer.to_owned();
            let worker = cluster.clone();
            cluster.spawn(async move {
                while let Some(body) = rx.recv().await {
                    if !worker.running.load(Ordering::Relaxed) {
                        break;
                    }
                    let _ = worker.send_wire(&peer, &body).await;
                }
            });
        }
        if let Some(tx) = outgoing.get(peer) {
            let _ = tx.try_send(body);
        }
    }

    pub(super) fn set_migration_source(&self, mut heads: Vec<Head>) -> Result<()> {
        ensure!(
            self.migration_seed.as_deref() == Some(self.identity.node_id().as_str()),
            "not the migration seed"
        );
        ensure!(
            heads.len() <= self.cfg.max_slots_per_room,
            "migration exceeds slot quota"
        );
        let mut slots = HashSet::new();
        for head in &heads {
            head.validate(self.cfg.max_blob_bytes)
                .map_err(|error| anyhow!(error.code))?;
            head.verify().map_err(|error| anyhow!(error.code))?;
            ensure!(slots.insert(head.slot.clone()), "duplicate migration slot");
        }
        heads.sort_by(|a, b| a.slot.cmp(&b.slot));
        ensure!(
            self.migration_source.set(heads).is_ok(),
            "migration source already frozen"
        );
        Ok(())
    }

    pub(super) async fn change_membership(
        &self,
        voters: Vec<String>,
        learners: Vec<String>,
    ) -> Result<Value> {
        let _serial = self.serial.lock().await;
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(Inbound::Membership {
                voters,
                learners,
                tx,
            })
            .map_err(|_| anyhow!("cluster busy"))?;
        tokio::time::timeout(Duration::from_secs(60), rx)
            .await
            .map_err(|_| anyhow!("membership change timed out; inspect status before retrying"))?
            .map_err(|_| anyhow!("replication stopped during membership change"))
    }

    async fn send_wire(&self, to: &str, body: &Body) -> Result<()> {
        ensure!(
            self.running.load(Ordering::Relaxed),
            "escrow replication stopped"
        );
        let seq = self
            .seq
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(|| anyhow!("escrow message sequence exhausted"))?;
        let bytes = wire::encode(
            &self.room,
            &self.cluster,
            &self.identity,
            self.boot,
            seq,
            body,
        )?;
        tokio::time::timeout(
            SEND_WAIT,
            (self.sender)(self.room.clone(), to.into(), bytes),
        )
        .await??;
        Ok(())
    }

    async fn drive(self: Arc<Self>, mut rx: mpsc::Receiver<Inbound>) {
        let mut timers = Timers::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        let mut maintenance = Instant::now();
        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                _ = ticker.tick() => {
                    let role = self.inner.lock().unwrap().engine.role();
                    let now = Instant::now();
                    if role == Role::Leader && now >= timers.heartbeat {
                        self.event(Event::HeartbeatTimeout, &mut timers, false);
                        timers.heartbeat = now + HEARTBEAT;
                    } else if role != Role::Leader && now >= timers.election {
                        self.event(Event::ElectionTimeout, &mut timers, false);
                        timers.election = election_deadline();
                    }
                    self.inner.lock().unwrap().pending.retain(|_, pending| !pending.tx.is_closed());
                    self.forward.lock().unwrap().retain(|_, pending| !pending.tx.is_closed());
                    if now >= maintenance {
                        self.snapshots.lock().unwrap().expire(now);
                        let mut inner = self.inner.lock().unwrap();
                        if let Err(error) = self.collect(&inner) { self.fail(&mut inner, error.to_string()); }
                        let peers = inner.engine.peers();
                        let config = inner.engine.membership();
                        drop(inner);
                        for peer in peers { self.queue(&peer.0, Body::Hello { membership: config.clone() }); }
                        maintenance = now + Duration::from_secs(5);
                    }
                    if role == Role::Leader && self.migration_seed.is_some()
                        && !self.inner.lock().unwrap().state.migration_finished()
                        && !self.importing.swap(true, Ordering::Relaxed) {
                        let cluster = self.clone();
                        self.spawn(async move {
                            if let Err(error) = cluster.import_legacy().await {
                                tracing::warn!(%error, "escrow: migration will retry");
                            }
                            cluster.importing.store(false, Ordering::Relaxed);
                        });
                    }
                }
                message = rx.recv() => {
                    let Some(message) = message else { break; };
                    match message {
                        Inbound::Wire(decoded) => {
                            let mut queued = self.queued.lock().unwrap();
                            if let Some(count) = queued.get_mut(&decoded.from) { *count = count.saturating_sub(1); }
                            queued.retain(|_, count| *count > 0);
                            drop(queued);
                            self.incoming(decoded, &mut timers);
                        }
                        Inbound::Prepared { from, msg, leases } => {
                            self.preparing.lock().unwrap().remove(&from);
                            if matches!(msg, RaftMessage::InstallSnapshot { .. }) {
                                self.preparing_snapshot.store(false, Ordering::Relaxed);
                            }
                            if msg.term() >= self.inner.lock().unwrap().engine.current_term() {
                                self.event(Event::Message { from: NodeId(from), msg }, &mut timers, false);
                            }
                            drop(leases);
                        }
                        Inbound::Membership { voters, learners, tx } => {
                            let mut inner = self.inner.lock().unwrap();
                            let current = inner.engine.membership();
                            let committed = inner.engine.commit_index();
                            let matched = membership::peers(&current).into_iter().map(|id| {
                                let index = inner.engine.matched_index(&NodeId(id.clone())); (id, index)
                            }).collect();
                            let target = membership::change(&current, &voters, &learners, committed, &matched);
                            if inner.engine.role() != Role::Leader || inner.membership_pending.is_some() {
                                let _ = tx.send(json!({"accepted":false,"error":"not_leader_or_busy"}));
                            } else if let Ok(target) = target {
                                inner.membership_pending = Some(MembershipPending { target: target.clone(), tx });
                                match inner.engine.step(Event::ChangeMembership { voters: target.voters, learners: target.learners }) {
                                    Ok(outputs) => self.outputs(&mut inner, outputs, &mut timers, false),
                                    Err(error) => self.fail(&mut inner, error.to_string()),
                                }
                            } else {
                                let _ = tx.send(json!({"accepted":false,"error":target.unwrap_err().to_string()}));
                            }
                        }
                        Inbound::Propose { command, term, id, tx } => {
                            self.proposal(command, term, id, tx, &mut timers);
                        }
                    }
                }
            }
        }
    }

    fn contact(&self, from: &str, term: u64, timers: &mut Timers) {
        self.event(
            Event::Message {
                from: NodeId(from.into()),
                msg: RaftMessage::AppendEntries {
                    term,
                    leader_id: NodeId(from.into()),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: Vec::new(),
                    leader_commit: 0,
                },
            },
            timers,
            true,
        );
    }

    fn snapshot_received(self: &Arc<Self>, from: String, result: Receive, _timers: &mut Timers) {
        match result {
            Receive::Request(request) => self.queue(&from, Body::SnapshotRequest(request)),
            Receive::Ignored => {}
            Receive::Complete { term, snapshot } => {
                if term < self.inner.lock().unwrap().engine.current_term()
                    || self.preparing_snapshot.swap(true, Ordering::Relaxed)
                {
                    return;
                }
                let cluster = self.clone();
                self.spawn(async move {
                    let work = async {
                        let state = State::decode(&snapshot.data, APPLY_SLOTS)
                            .map_err(|_| Error::new("bad_request"))?;
                        let mut leases = Vec::new();
                        for head in state.retained_heads() {
                            leases.push(cluster.stage(&from, &head).await?);
                        }
                        let msg = RaftMessage::InstallSnapshot {
                            term,
                            leader_id: NodeId(from.clone()),
                            snapshot,
                        };
                        cluster
                            .tx
                            .try_send(Inbound::Prepared {
                                from: from.clone(),
                                msg,
                                leases,
                            })
                            .map_err(|_| Error::new("busy"))?;
                        Ok::<_, Error>(())
                    };
                    if !tokio::time::timeout(Duration::from_secs(600), work)
                        .await
                        .is_ok_and(|r| r.is_ok())
                    {
                        cluster.preparing_snapshot.store(false, Ordering::Relaxed);
                    }
                });
            }
        }
    }

    fn incoming(self: &Arc<Self>, decoded: Decoded, timers: &mut Timers) {
        let from = decoded.from;
        match decoded.body {
            Body::Reply { ticket, response } => {
                let mut pending = self.forward.lock().unwrap();
                if pending.get(&ticket).is_some_and(|item| item.leader == from) {
                    if let Some(item) = pending.remove(&ticket) {
                        let _ = item.tx.send(response);
                    }
                }
            }
            Body::Forward {
                request,
                source,
                ticket,
            } => {
                let Ok(permit) = self.requests.clone().try_acquire_owned() else {
                    return;
                };
                let cluster = self.clone();
                self.spawn(async move {
                    let _permit = permit;
                    let id = request["id"]
                        .as_str()
                        .filter(|id| id.len() <= 64)
                        .unwrap_or("");
                    let response = tokio::time::timeout(
                        REQUEST_WAIT,
                        cluster.leader_request(&source, &request, id),
                    )
                    .await
                    .unwrap_or_else(|_| Error::new("busy").reply(id));
                    let _ = cluster
                        .send_wire(&from, &Body::Reply { ticket, response })
                        .await;
                });
            }
            Body::Hello { .. } => {
                if self.inner.lock().unwrap().engine.role() == Role::Leader {
                    self.event(Event::HeartbeatTimeout, timers, false);
                }
            }
            Body::SnapshotOffer(offer) => {
                if offer.term < self.inner.lock().unwrap().engine.current_term() {
                    return;
                }
                self.contact(&from, offer.term, timers);
                let result =
                    self.snapshots
                        .lock()
                        .unwrap()
                        .accept_offer(&from, offer, Instant::now());
                if let Ok(result) = result {
                    self.snapshot_received(from, result, timers);
                }
            }
            Body::SnapshotRequest(request) => {
                let chunk =
                    self.snapshots
                        .lock()
                        .unwrap()
                        .reply_chunk(&from, &request, Instant::now());
                if let Some(chunk) = chunk {
                    self.queue(&from, Body::SnapshotChunk(chunk));
                }
            }
            Body::SnapshotChunk(chunk) => {
                let result =
                    self.snapshots
                        .lock()
                        .unwrap()
                        .accept_chunk(&from, chunk, Instant::now());
                if let Ok(result) = result {
                    self.snapshot_received(from, result, timers);
                }
            }
            Body::MigrationRequest { ticket, offset } => {
                if self.migration_seed.as_deref() != Some(self.identity.node_id().as_str()) {
                    return;
                }
                let Some(source) = self.migration_source.get() else {
                    return;
                };
                let offset = offset as usize;
                if offset > source.len() {
                    return;
                }
                let end = (offset + 4).min(source.len());
                self.queue(
                    &from,
                    Body::MigrationReply {
                        ticket,
                        offset: offset as u32,
                        heads: source[offset..end].to_vec(),
                        done: end == source.len(),
                    },
                );
            }
            Body::MigrationReply {
                ticket,
                offset,
                heads,
                done,
            } => {
                let mut pending = self.migration_pending.lock().unwrap();
                if pending
                    .get(&ticket)
                    .is_some_and(|item| item.peer == from && item.offset == offset)
                {
                    if let Some(item) = pending.remove(&ticket) {
                        let _ = item.tx.send((heads, done));
                    }
                }
            }
            Body::Raft(msg) => {
                if let RaftMessage::InstallSnapshotResponse {
                    success: true,
                    last_included_index,
                    ..
                } = &msg
                {
                    self.snapshots
                        .lock()
                        .unwrap()
                        .acknowledged(&from, *last_included_index);
                }
                let current = self.inner.lock().unwrap().engine.current_term();
                if msg.term() >= current {
                    if let RaftMessage::AppendEntries { term, entries, .. } = &msg {
                        if !entries.is_empty() {
                            self.contact(&from, *term, timers);
                            let mut heads = Vec::new();
                            for entry in entries {
                                if decode_membership(&entry.command).is_some() {
                                    continue;
                                }
                                let Ok(command) = serde_json::from_slice::<Command>(&entry.command)
                                else {
                                    return;
                                };
                                if state::validate(&command, 1024 * 1024 * 1024).is_err() {
                                    return;
                                }
                                heads.extend(command_heads(&command));
                            }
                            if !heads.is_empty() {
                                if !self.preparing.lock().unwrap().insert(from.clone()) {
                                    return;
                                }
                                let cluster = self.clone();
                                self.spawn(async move {
                                    let work = async {
                                        let mut leases = Vec::new();
                                        for head in heads {
                                            leases.push(cluster.stage(&from, &head).await?);
                                        }
                                        cluster
                                            .tx
                                            .try_send(Inbound::Prepared {
                                                from: from.clone(),
                                                msg,
                                                leases,
                                            })
                                            .map_err(|_| Error::new("busy"))?;
                                        Ok::<_, Error>(())
                                    };
                                    if !tokio::time::timeout(Duration::from_secs(120), work)
                                        .await
                                        .is_ok_and(|r| r.is_ok())
                                    {
                                        cluster.preparing.lock().unwrap().remove(&from);
                                    }
                                });
                                return;
                            }
                        }
                    }
                }
                self.event(
                    Event::Message {
                        from: NodeId(from),
                        msg,
                    },
                    timers,
                    false,
                );
            }
        }
    }

    fn proposal(
        &self,
        command: Command,
        term: u64,
        id: String,
        tx: oneshot::Sender<Value>,
        timers: &mut Timers,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.pending.retain(|_, pending| !pending.tx.is_closed());
        if inner.error.is_some()
            || inner.engine.role() != Role::Leader
            || inner.engine.current_term() != term
            || inner.pending.len() >= MAX_REQUESTS
        {
            let _ = tx.send(Error::new("busy").reply(&id));
            return;
        }
        let bytes = match serde_json::to_vec(&command) {
            Ok(bytes) => bytes,
            Err(_) => {
                let _ = tx.send(Error::new("bad_request").reply(&id));
                return;
            }
        };
        let previous = inner.engine.log().last();
        let prev_log_index = previous.map_or(0, |entry| entry.index);
        let prev_log_term = previous.map_or(0, |entry| entry.term);
        let probe = Body::Raft(RaftMessage::AppendEntries {
            term,
            leader_id: NodeId(self.identity.node_id()),
            prev_log_index,
            prev_log_term,
            entries: vec![LogEntry {
                term,
                index: prev_log_index + 1,
                command: bytes.clone(),
            }],
            leader_commit: inner.engine.commit_index(),
        });
        if wire::encode(
            &self.room,
            &self.cluster,
            &self.identity,
            self.boot,
            1,
            &probe,
        )
        .is_err()
        {
            let _ = tx.send(Error::new("too_large").reply(&id));
            return;
        }
        let ticket = command.ticket().to_owned();
        inner.pending.insert(ticket.clone(), Pending { id, tx });
        let result = inner.engine.step(Event::ClientPropose { command: bytes });
        match result {
            Ok(outputs) => self.outputs(&mut inner, outputs, timers, false),
            Err(error) => {
                if let Some(pending) = inner.pending.remove(&ticket) {
                    let _ = pending.tx.send(Error::new("busy").reply(&pending.id));
                }
                self.fail(&mut inner, error.to_string());
            }
        }
    }

    fn event(&self, event: Event, timers: &mut Timers, suppress_send: bool) {
        let mut inner = self.inner.lock().unwrap();
        if inner.error.is_some() {
            return;
        }
        match inner.engine.step(event) {
            Ok(outputs) => self.outputs(&mut inner, outputs, timers, suppress_send),
            Err(error) if inner.engine.stopped() => self.fail(&mut inner, error.to_string()),
            Err(error) => tracing::debug!(%error,"escrow: ignored invalid Raft RPC"),
        }
    }

    fn outputs(
        &self,
        inner: &mut Inner,
        outputs: Vec<Output>,
        timers: &mut Timers,
        suppress_send: bool,
    ) {
        for output in &outputs {
            if let Output::InstallSnapshot { snapshot } = output {
                match State::decode(&snapshot.data, APPLY_SLOTS) {
                    Ok(state)
                        if state
                            .retained_heads()
                            .iter()
                            .all(|head| self.archive.has(head)) =>
                    {
                        inner.state = state
                    }
                    _ => {
                        self.fail(inner, "invalid or missing snapshot generation".into());
                        return;
                    }
                }
            }
        }
        let entries: Vec<_> = inner
            .engine
            .log()
            .iter()
            .filter(|entry| {
                entry.index > inner.engine.last_applied()
                    && entry.index <= inner.engine.commit_index()
            })
            .cloned()
            .collect();
        for entry in entries {
            if decode_membership(&entry.command).is_some() {
                if let Err(error) = inner.engine.mark_applied(entry.index) {
                    self.fail(inner, error.to_string());
                    return;
                }
                continue;
            }
            let command: Command = match serde_json::from_slice(&entry.command) {
                Ok(command) => command,
                Err(error) => {
                    self.fail(inner, error.to_string());
                    return;
                }
            };
            if command_heads(&command)
                .iter()
                .any(|head| !self.archive.has(head))
            {
                self.fail(inner, "committed generation unavailable".into());
                return;
            }
            let pending = inner.pending.remove(command.ticket());
            let id = pending.as_ref().map_or("", |pending| pending.id.as_str());
            let response = inner.state.apply(&command, id, APPLY_SLOTS);
            if let Err(error) = inner.engine.mark_applied(entry.index) {
                self.fail(inner, error.to_string());
                return;
            }
            if let Some(pending) = pending {
                let _ = pending.tx.send(response);
            }
        }
        if (inner.engine.needs_compaction()
            || self.archive.total_bytes() > self.cfg.max_total_bytes / 2)
            && inner.engine.last_applied() == inner.engine.commit_index()
            && inner.engine.commit_index()
                > inner.engine.snapshot().map_or(0, |s| s.last_included_index)
        {
            let compact = inner
                .state
                .encode()
                .and_then(|bytes| inner.engine.compact(bytes));
            if let Err(error) = compact {
                self.fail(inner, error.to_string());
                return;
            }
            if let Err(error) = self.collect(inner) {
                self.fail(inner, error.to_string());
                return;
            }
        }
        let desired: HashSet<_> = inner.engine.peers().into_iter().map(|id| id.0).collect();
        self.outgoing
            .lock()
            .unwrap()
            .retain(|id, _| desired.contains(id));
        let rejected = outputs
            .iter()
            .any(|output| matches!(output, Output::MembershipRejected));
        for output in outputs {
            match output {
                Output::ResetElectionTimer => timers.election = election_deadline(),
                Output::ResetHeartbeatTimer => timers.heartbeat = Instant::now() + HEARTBEAT,
                Output::SendMessage { to, mut msg } if !suppress_send => {
                    if let RaftMessage::InstallSnapshot { term, snapshot, .. } = msg {
                        let result = State::decode(&snapshot.data, APPLY_SLOTS).and_then(|state| {
                            let _gate = self.archive_gate.lock().unwrap();
                            let lease = self.archive.pin_heads(&state.retained_heads())?;
                            self.snapshots.lock().unwrap().offer(
                                &to.0,
                                term,
                                snapshot,
                                lease,
                                Instant::now(),
                            )
                        });
                        match result {
                            Ok(Some(offer)) => self.queue(&to.0, Body::SnapshotOffer(offer)),
                            Ok(None) => {}
                            Err(error) => {
                                self.fail(inner, error.to_string());
                                return;
                            }
                        }
                        continue;
                    }
                    if let RaftMessage::AppendEntries {
                        entries,
                        prev_log_index,
                        leader_commit,
                        ..
                    } = &mut msg
                    {
                        entries.truncate(8);
                        *leader_commit =
                            (*leader_commit).min(*prev_log_index + entries.len() as u64);
                    }
                    loop {
                        if wire::encode(
                            &self.room,
                            &self.cluster,
                            &self.identity,
                            self.boot,
                            1,
                            &Body::Raft(msg.clone()),
                        )
                        .is_ok()
                        {
                            break;
                        }
                        if let RaftMessage::AppendEntries {
                            entries,
                            prev_log_index,
                            leader_commit,
                            ..
                        } = &mut msg
                        {
                            if entries.len() > 1 {
                                entries.pop();
                                *leader_commit =
                                    (*leader_commit).min(*prev_log_index + entries.len() as u64);
                                continue;
                            }
                        }
                        self.fail(inner, "Raft message exceeds wire limit".into());
                        return;
                    }
                    self.queue(&to.0, Body::Raft(msg));
                }
                _ => {}
            }
        }
        if let Some(pending) = inner.membership_pending.as_ref() {
            if inner.engine.committed_membership() == pending.target {
                let pending = inner.membership_pending.take().unwrap();
                let _ = pending
                    .tx
                    .send(json!({"accepted":true,"membership":pending.target}));
            } else if rejected || inner.engine.role() != Role::Leader {
                let pending = inner.membership_pending.take().unwrap();
                let _ = pending
                    .tx
                    .send(json!({"accepted":false,"error":"membership_change_rejected"}));
            }
        }
        if inner.engine.role() != Role::Leader {
            for (_, pending) in inner.pending.drain() {
                let _ = pending.tx.send(Error::new("busy").reply(&pending.id));
            }
        }
    }

    fn collect(&self, inner: &Inner) -> Result<()> {
        let mut keep: HashSet<_> = inner
            .state
            .retained_heads()
            .iter()
            .map(Head::hash)
            .collect();
        if let Some(snapshot) = inner.engine.snapshot() {
            keep.extend(
                State::decode(&snapshot.data, APPLY_SLOTS)?
                    .retained_heads()
                    .iter()
                    .map(Head::hash),
            );
        }
        for entry in inner.engine.log() {
            if decode_membership(&entry.command).is_some() {
                continue;
            }
            let command: Command = serde_json::from_slice(&entry.command)?;
            keep.extend(command_heads(&command).iter().map(Head::hash));
        }
        let _gate = self.archive_gate.lock().unwrap();
        self.archive.collect(&keep)?;
        Ok(())
    }

    async fn migration_batch(&self, seed: &str, offset: u32) -> Result<(Vec<Head>, bool)> {
        if seed == self.identity.node_id() {
            let source = self
                .migration_source
                .get()
                .ok_or_else(|| anyhow!("migration source unavailable"))?;
            let offset = offset as usize;
            ensure!(offset <= source.len(), "invalid migration offset");
            let end = (offset + 4).min(source.len());
            return Ok((source[offset..end].to_vec(), end == source.len()));
        }
        let ticket = crate::net::peer_auth::fresh_nonce();
        let (tx, rx) = oneshot::channel();
        self.migration_pending.lock().unwrap().insert(
            ticket.clone(),
            MigrationPending {
                peer: seed.into(),
                offset,
                tx,
            },
        );
        let result = async {
            self.send_wire(
                seed,
                &Body::MigrationRequest {
                    ticket: ticket.clone(),
                    offset,
                },
            )
            .await?;
            Ok::<_, anyhow::Error>(tokio::time::timeout(Duration::from_secs(10), rx).await??)
        }
        .await;
        self.migration_pending.lock().unwrap().remove(&ticket);
        result
    }

    async fn import_legacy(&self) -> Result<()> {
        let _serial = self.serial.lock().await;
        let seed = self
            .migration_seed
            .as_deref()
            .ok_or_else(|| anyhow!("missing migration seed"))?;
        let term = self.inner.lock().unwrap().engine.current_term();
        if !self.inner.lock().unwrap().state.migrating() {
            let reply = self
                .propose(
                    Command::BootstrapBegin {
                        ticket: crate::net::peer_auth::fresh_nonce(),
                    },
                    term,
                    "migration",
                )
                .await;
            ensure!(
                reply["type"] == "escrow_bootstrapped",
                "migration begin rejected: {reply}"
            );
        }
        let mut offset = 0u32;
        loop {
            let (heads, done) = self.migration_batch(seed, offset).await?;
            ensure!(
                done || !heads.is_empty(),
                "migration source did not progress"
            );
            ensure!(
                offset as usize + heads.len() <= self.cfg.max_slots_per_room,
                "migration slot quota"
            );
            for head in &heads {
                let already = self.inner.lock().unwrap().state.head(&head.slot);
                ensure!(
                    already.as_ref().is_none_or(|old| old == head),
                    "migration source changed"
                );
                if already.is_some() {
                    continue;
                }
                let _lease = self
                    .stage(seed, head)
                    .await
                    .map_err(|error| anyhow!(error.code))?;
                let reply = self
                    .propose(
                        Command::Bootstrap {
                            ticket: crate::net::peer_auth::fresh_nonce(),
                            heads: vec![head.clone()],
                        },
                        term,
                        "migration",
                    )
                    .await;
                ensure!(
                    reply["type"] == "escrow_bootstrapped",
                    "migration batch rejected: {reply}"
                );
            }
            offset = offset
                .checked_add(heads.len() as u32)
                .ok_or_else(|| anyhow!("migration offset overflow"))?;
            if done {
                break;
            }
        }
        let reply = self
            .propose(
                Command::BootstrapComplete {
                    ticket: crate::net::peer_auth::fresh_nonce(),
                },
                term,
                "migration",
            )
            .await;
        ensure!(
            reply["type"] == "escrow_bootstrapped",
            "migration completion rejected: {reply}"
        );
        Ok(())
    }

    fn spawn(self: &Arc<Self>, future: impl Future<Output = ()> + Send + 'static) {
        let handle = tokio::spawn(future);
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    fn fail(&self, inner: &mut Inner, error: String) {
        tracing::warn!(%error,"escrow: replication stopped; restart required");
        inner.error = Some(error);
        self.running.store(false, Ordering::Relaxed);
        for (_, pending) in inner.pending.drain() {
            let _ = pending
                .tx
                .send(Error::new("fetch_failed").reply(&pending.id));
        }
        self.forward.lock().unwrap().clear();
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }

    #[cfg(test)]
    pub(super) fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

fn command_heads(command: &Command) -> Vec<Head> {
    match command {
        Command::Put { head, .. } => vec![head.clone()],
        Command::Bootstrap { heads, .. } => heads.clone(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod production_runtime_tests;

#[cfg(test)]
mod control_tests;
