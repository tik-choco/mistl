//! Config-driven tc-escrow v1 service. Settings take effect on daemon restart.
mod admission;
mod client;
mod mesh;
mod portable;
mod portable_io;
mod protocol;
mod provider;
mod replication;
mod repository;
#[cfg(test)]
mod tests;
mod transfer;

use crate::config::EscrowConfig;
use crate::daemon::AppState;
use admission::{Admission, Class};
use anyhow::{Context, Result, bail};
use protocol::{Delete, Error, Head, MAX_MESSAGE, hex_string};
use repository::Repository;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};

const SLOT_WAIT: Duration = Duration::from_secs(5);
const REPLY_WAIT: Duration = Duration::from_secs(2);
use transfer::Transfers;

type ReplyFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type ReplySender = Arc<dyn Fn(String, String, Vec<u8>) -> ReplyFuture + Send + Sync>;

static SERVICE: OnceCell<Arc<Service>> = OnceCell::const_new();

pub fn valid_room(room: &str) -> bool {
    !room.is_empty()
        && room.len() <= 96
        && room
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_./:-".contains(&b))
}

pub fn valid_voter(node: &str) -> bool {
    protocol::hex_string(node, 16)
}

struct Service {
    cfg: EscrowConfig,
    repo: Repository,
    joined: StdMutex<HashSet<String>>,
    slots: Vec<Mutex<()>>,
    transfers: Transfers,
    requests: Arc<Admission>,
    block_provider: StdMutex<Option<mistlib::storage::BlockProviderHandle>>,
    send_reply: ReplySender,
    pulling: StdMutex<HashSet<String>>,
    replication_root: std::path::PathBuf,
    clusters: StdMutex<HashMap<String, Arc<replication::Cluster>>>,
    mesh: StdMutex<Option<Arc<mesh::Mesh>>>,
}

impl Service {
    fn open(dir: std::path::PathBuf, cfg: EscrowConfig) -> Result<Self> {
        cfg.validate()?;
        let replication_root = dir.join("replication");
        if replication_root.exists() {
            let configured: HashSet<_> = cfg
                .replication
                .iter()
                .map(|c| protocol::digest(c.room.as_bytes()))
                .collect();
            for entry in std::fs::read_dir(&replication_root)? {
                let entry = entry?;
                if entry.file_type()?.is_dir()
                    && protocol::hex_string(&entry.file_name().to_string_lossy(), 64)
                {
                    anyhow::ensure!(
                        configured.contains(entry.file_name().to_string_lossy().as_ref()),
                        "escrow: retained replicated room is unconfigured; restore its replication configuration"
                    );
                }
            }
        }
        let mut local_cfg = cfg.clone();
        local_cfg.max_total_bytes /= cfg.replication.len() as u64 + 1;
        let repo = Repository::open(dir.clone(), local_cfg.clone())?;
        // An interrupted import leaves its spooled copy beside the repository.
        repository::remove_temps(&dir, |stem| stem == "import")?;
        anyhow::ensure!(
            repo.total_bytes() <= local_cfg.max_total_bytes,
            "escrow: existing data exceeds replication quota partition"
        );
        Ok(Self {
            repo,
            cfg,
            joined: StdMutex::new(HashSet::new()),
            // Fixed lock stripes serialize each (room,slot) without an unbounded lock map.
            slots: (0..256).map(|_| Mutex::new(())).collect(),
            transfers: Transfers::default(),
            requests: Arc::new(Admission::default()),
            block_provider: StdMutex::new(None),
            send_reply: Arc::new(|room, from, bytes| {
                Box::pin(async move { crate::net::send_direct_wait(&room, &from, bytes).await })
            }),
            pulling: StdMutex::new(HashSet::new()),
            replication_root,
            clusters: StdMutex::new(HashMap::new()),
            mesh: StdMutex::new(None),
        })
    }

    fn provider(&self) -> Value {
        json!({"v":1,"type":"escrow_provider","maxBlob":self.cfg.max_blob_bytes,"version":1,"features":["delete"]})
    }

    fn mesh_room(&self, room: &str) -> bool {
        self.cfg.auto_replicas >= 2
            && self.cfg.rooms.iter().any(|r| r == room)
            && !self.cfg.replication.iter().any(|r| r.room == room)
    }

    fn mesh(&self, room: &str) -> Option<Arc<mesh::Mesh>> {
        if !self.mesh_room(room) {
            return None;
        }
        self.mesh.lock().unwrap().clone()
    }

    fn cluster(&self, room: &str) -> Option<Arc<replication::Cluster>> {
        self.clusters.lock().unwrap().get(room).cloned()
    }

    fn provider_for(&self, room: &str) -> Value {
        let mut provider = self.provider();
        if self.cluster(room).is_some() {
            provider["features"] = json!([
                "delete",
                "raft",
                "snapshot",
                "membership",
                "delete-continue"
            ]);
        } else if self.mesh(room).is_some() {
            provider["features"] = json!(["delete", "delete-continue"]);
        }
        provider
    }

    async fn configure_replication(self: &Arc<Self>, state: &Arc<AppState>) -> Result<()> {
        if self.cfg.replication.is_empty() {
            return Ok(());
        }
        let identity = crate::identity::current(state).await?;
        for setting in &self.cfg.replication {
            let has_data = self.repo.status(&setting.room).0 > 0;
            anyhow::ensure!(
                !has_data || setting.migration_seed.as_deref() == Some(identity.node_id().as_str()),
                "escrow: existing data requires this node as the explicit migration seed"
            );
        }
        let mut cfg = self.cfg.clone();
        cfg.max_total_bytes /= cfg.replication.len() as u64 + 1;
        for setting in &self.cfg.replication {
            let cluster = replication::Cluster::open(
                self.replication_root
                    .join(protocol::digest(setting.room.as_bytes())),
                setting.room.clone(),
                setting.voters.clone(),
                identity.clone(),
                cfg.clone(),
            )?;
            if setting.migration_seed.as_deref() == Some(identity.node_id().as_str()) {
                cluster.set_migration_source(self.repo.room_heads(&setting.room))?;
            }
            self.clusters
                .lock()
                .unwrap()
                .insert(setting.room.clone(), cluster);
        }
        Ok(())
    }

    fn start_replication(self: &Arc<Self>, state: &Arc<AppState>) -> Result<()> {
        let weak = Arc::downgrade(self);
        let state = state.clone();
        let pull: replication::Puller = Arc::new(move |room, from, head| {
            let service = weak.clone();
            let state = state.clone();
            Box::pin(async move {
                let service = service.upgrade().ok_or_else(|| Error::new("busy"))?;
                let _guard = {
                    let mut pulling = service.pulling.lock().unwrap();
                    if pulling.len() >= 4 || !pulling.insert(from.clone()) {
                        return Err(Error::new("busy"));
                    }
                    PullGuard {
                        service: &service,
                        from: from.clone(),
                    }
                };
                if let Some(bytes) = service.repo.generation_bytes(&room, &head)? {
                    return Ok(bytes);
                }
                let store = crate::storage::store(&state)
                    .await
                    .map_err(|_| Error::new("fetch_failed"))?;
                if let Some(bytes) =
                    transfer::local_generation(&head, service.cfg.max_blob_bytes, |cid| {
                        let store = store.clone();
                        async move {
                            store
                                .get_local(&cid)
                                .await
                                .map_err(|_| Error::new("fetch_failed"))
                        }
                    })
                    .await?
                {
                    return Ok(bytes);
                }
                service.transfers.pull(&room, &from, &head).await
            })
        });
        for cluster in self.clusters.lock().unwrap().values() {
            cluster.start(pull.clone())?;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(5));
            loop {
                timer.tick().await;
                let Some(service) = weak.upgrade() else {
                    break;
                };
                for setting in &service.cfg.replication {
                    if setting.migration_seed.is_some()
                        && service
                            .cluster(&setting.room)
                            .is_some_and(|cluster| cluster.status()["migration_finished"] == true)
                    {
                        if let Err(error) = service.repo.retire_room(&setting.room) {
                            tracing::warn!(%error, "escrow: migrated legacy cleanup failed");
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn put(&self, room: &str, from: &str, head: Head, id: &str) -> Result<Value, Error> {
        head.validate(self.cfg.max_blob_bytes)?;
        let stripe = usize::from(sha2_byte(room, &head.slot));
        let _slot = tokio::time::timeout(SLOT_WAIT, self.slots[stripe].lock())
            .await
            .map_err(|_| Error::new("busy"))?;
        let Some(_reservation) = self.repo.reserve(room, &head)? else {
            if let Some(mesh) = self.mesh(room) {
                if self
                    .repo
                    .mesh_records(room)
                    .iter()
                    .any(|r| r.head == head && r.home.is_none())
                {
                    self.repo.set_home(room, &head, mesh.did())?;
                    mesh.notify();
                }
            }
            return Ok(head.ack(id));
        };
        // No waiting task queue: at most four PUTs and one per transport peer globally.
        let _pull = {
            let mut pulling = self.pulling.lock().unwrap();
            if pulling.len() >= 4 || !pulling.insert(from.to_string()) {
                return Err(Error::new("busy"));
            }
            PullGuard {
                service: self,
                from: from.into(),
            }
        };
        let (blob, manifest) = self.transfers.pull(room, from, &head).await?;
        self.repo.commit(room, &head, &blob, &manifest)?;
        if let Some(mesh) = self.mesh(room) {
            self.repo.set_home(room, &head, mesh.did())?;
            mesh.advertise(room, &head.slot, None).await;
            mesh.notify();
        }
        Ok(head.ack(id))
    }

    async fn delete(&self, room: &str, request: Delete, id: &str) -> Result<Value, Error> {
        request.validate()?;
        let stripe = usize::from(sha2_byte(room, &request.slot));
        let _slot = tokio::time::timeout(SLOT_WAIT, self.slots[stripe].lock())
            .await
            .map_err(|_| Error::new("busy"))?;
        if let Some(mesh) = self.mesh(room) {
            self.repo.mesh_delete(room, &request)?;
            mesh.advertise(room, &request.slot, None).await;
            mesh.notify();
        } else {
            self.repo.delete(room, &request)?;
        }
        Ok(request.reply(id))
    }

    async fn request(&self, room: &str, from: &str, msg: Value) -> Option<Value> {
        if msg["v"] != 1 {
            return None;
        }
        if msg["type"] == "escrow_hello" {
            return Some(self.provider_for(room));
        }
        let kind = msg["type"].as_str()?;
        if !matches!(kind, "escrow_get" | "escrow_put" | "escrow_delete") {
            return None;
        }
        if let Some(cluster) = self.cluster(room) {
            return cluster.request(from, &msg).await;
        }
        let Some(id) = msg["id"].as_str().filter(|s| s.len() <= 64) else {
            return Some(Error::new("bad_request").reply(""));
        };
        let result = match kind {
            "escrow_get" => {
                if let Some(slot) = msg["slot"].as_str().filter(|s| hex_string(s, 32)) {
                    Ok(
                        json!({"v":1,"type":"escrow_head","id":id,"slot":slot,"head":self.repo.head(room, slot)}),
                    )
                } else {
                    Err(Error::new("bad_request"))
                }
            }
            "escrow_delete" => {
                if let Ok(request) = serde_json::from_value::<Delete>(msg.clone()) {
                    self.delete(room, request, id).await
                } else {
                    Err(Error::new("bad_request"))
                }
            }
            _ => {
                if msg["head"].get("parent").is_none() {
                    Err(Error::new("bad_request"))
                } else if let Ok(head) = serde_json::from_value::<Head>(msg["head"].clone()) {
                    self.put(room, from, head, id).await
                } else {
                    Err(Error::new("bad_request"))
                }
            }
        };
        Some(result.unwrap_or_else(|error| error.reply(id)))
    }

    // Admission happens synchronously before spawn; parked PUTs count against the
    // sender across rooms. Reply capacity cannot be occupied by a long blob pull.
    fn enqueue_request(
        self: &Arc<Self>,
        rt: &tokio::runtime::Handle,
        room: &str,
        from: &str,
        msg: Value,
    ) {
        if msg["v"] != 1 {
            return;
        }
        let class = if matches!(msg["type"].as_str(), Some("escrow_put" | "escrow_delete")) {
            Class::Put
        } else {
            Class::Reply
        };
        let (permit, busy) = match self.requests.acquire(from, class) {
            Some(permit) => (permit, false),
            None if matches!(class, Class::Put) => {
                let Some(permit) = self.requests.acquire(from, Class::Busy) else {
                    return;
                };
                (permit, true)
            }
            None => return,
        };
        let (service, room, from) = (self.clone(), room.to_string(), from.to_string());
        rt.spawn(async move {
            let _permit = permit;
            let reply = if busy {
                let id = msg["id"].as_str().filter(|s| s.len() <= 64).unwrap_or("");
                Some(Error::new("busy").reply(id))
            } else {
                service.request(&room, &from, msg).await
            };
            if let Some(reply) = reply {
                let bytes = serde_json::to_vec(&reply).unwrap();
                if bytes.len() <= MAX_MESSAGE {
                    let _ =
                        tokio::time::timeout(REPLY_WAIT, (service.send_reply)(room, from, bytes))
                            .await;
                }
            }
        });
    }

    fn register(self: &Arc<Self>) {
        self.register_block_provider();
        let service = self.clone();
        let rt = tokio::runtime::Handle::current();
        crate::net::register_room_handler(move |event, room, from, raw| {
            if !service.cfg.rooms.iter().any(|r| r == room) {
                return;
            }
            if event == crate::net::EVENT_PEER_CONNECTED {
                let Some(permit) = service.requests.acquire(from, Class::Reply) else {
                    return;
                };
                let (service, room) = (service.clone(), room.to_string());
                rt.spawn(async move {
                    let _permit = permit;
                    if let Some(mesh) = service.mesh(&room) {
                        mesh.hello(&room, None).await;
                        mesh.notify();
                    }
                    let bytes = serde_json::to_vec(&service.provider_for(&room)).unwrap();
                    let _ =
                        tokio::time::timeout(REPLY_WAIT, crate::net::send_broadcast(&room, bytes))
                            .await;
                });
                return;
            }
            if event != crate::net::EVENT_RAW {
                return;
            }
            if service
                .mesh(room)
                .is_some_and(|mesh| mesh.receive(room, from, raw))
            {
                return;
            }
            if service
                .cluster(room)
                .is_some_and(|cluster| cluster.receive(from, raw))
            {
                return;
            }
            if raw.len() > MAX_MESSAGE
                || raw.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{')
            {
                return;
            }
            let Ok(msg) = serde_json::from_slice::<Value>(raw) else {
                return;
            };
            if !matches!(
                msg["type"].as_str(),
                Some("escrow_hello" | "escrow_get" | "escrow_put" | "escrow_delete")
            ) {
                return;
            }
            service.enqueue_request(&rt, room, from, msg);
        });
    }
}

fn sha2_byte(room: &str, slot: &str) -> u8 {
    use sha2::{Digest, Sha256};
    Sha256::digest(format!("{room}\n{slot}").as_bytes())[0]
}

struct PullGuard<'a> {
    service: &'a Service,
    from: String,
}
impl Drop for PullGuard<'_> {
    fn drop(&mut self) {
        self.service.pulling.lock().unwrap().remove(&self.from);
    }
}

async fn local(state: &Arc<AppState>) -> Result<Arc<Service>> {
    Ok(SERVICE
        .get_or_try_init(|| async {
            let service = Arc::new(Service::open(
                crate::config::data_dir()?.join("escrow"),
                state.config().escrow,
            )?);
            service.configure_replication(state).await?;
            if service.cfg.rooms.iter().any(|room| service.mesh_room(room)) {
                let identity = crate::identity::current(state).await?;
                *service.mesh.lock().unwrap() = Some(mesh::Mesh::new(&service, identity));
            }
            Ok::<_, anyhow::Error>(service)
        })
        .await?
        .clone())
}

pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(error) = start(&state).await {
            tracing::warn!(%error, "escrow: failed to start");
        }
    });
}

async fn start(state: &Arc<AppState>) -> Result<()> {
    if !state.network.permitted() || !state.config().escrow.enabled {
        return Ok(());
    }
    let service = local(state).await?;
    service.register();
    service.start_replication(state)?;
    if let Some(mesh) = service.mesh.lock().unwrap().clone() {
        mesh.start();
    }
    for room in &service.cfg.rooms {
        match crate::net::ensure_started(state, room.clone()).await {
            Ok(_) => {
                service.joined.lock().unwrap().insert(room.clone());
                if let Some(mesh) = service.mesh(room) {
                    mesh.notify();
                }
            }
            Err(error) => tracing::warn!(%error, "escrow: room join failed"),
        }
    }
    // Joins are asynchronous. Hello requests and peer joins also advertise later.
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    for room in &service.cfg.rooms {
        let _ = crate::net::send_broadcast(room, serde_json::to_vec(&service.provider_for(room))?)
            .await;
    }
    Ok(())
}

pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "escrow.auto.slots" => {
            let room = args["room"]
                .as_str()
                .context("escrow.auto.slots requires room")?;
            anyhow::ensure!(valid_room(room), "invalid escrow room");
            let service = local(state).await?;
            let mesh = service
                .mesh(room)
                .context("room does not use automatic replication")?;
            Ok(mesh.slots(room))
        }
        "escrow.export" | "escrow.import" => {
            let service = local(state).await?;
            let configured = state.config().escrow.rooms;
            let export = cmd == "escrow.export";
            tokio::task::spawn_blocking(move || {
                portable_io::handle(&service, export, args, &configured)
            })
            .await?
        }
        "escrow.client.request" => client::request(args, state).await,
        "escrow.rooms.add" | "escrow.rooms.remove" => {
            let room = args["room"]
                .as_str()
                .context("escrow rooms requires room")?;
            state.set_escrow_room(room, cmd == "escrow.rooms.add")?;
            Ok(
                json!({"saved":true,"applies":"daemon restart","enabled":state.config().escrow.enabled,"rooms":state.config().escrow.rooms}),
            )
        }
        "escrow.replication.membership" => {
            let room = args["room"].as_str().context("membership requires room")?;
            let voters: Vec<String> = serde_json::from_value(args["voters"].clone())?;
            let learners: Vec<String> = if args["learners"].is_null() {
                Vec::new()
            } else {
                serde_json::from_value(args["learners"].clone()).context("invalid learners")?
            };
            let service = local(state).await?;
            let cluster = service.cluster(room).context("room is not replicated")?;
            cluster.change_membership(voters, learners).await
        }
        "escrow.status" | "escrow.rooms.list" => {
            let service = local(state).await?;
            let cfg = state.config().escrow;
            let mut rooms = cfg.rooms.clone();
            rooms.extend(service.repo.rooms());
            rooms.extend(service.cfg.rooms.clone());
            rooms.sort();
            rooms.dedup();
            let joined = service.joined.lock().unwrap();
            let rooms: Vec<_> = rooms.iter().map(|room| {
                let (slots, bytes) = service.repo.status(room);
                let mut status = json!({"room":room,"configured":cfg.rooms.contains(room),"joined":joined.contains(room),"slots":slots,"bytes":bytes});
                if let Some(cluster) = service.cluster(room) {
                    let replication = cluster.status();
                    status["slots"] = replication["slots"].clone();
                    status["bytes"] = json!(cluster.total_bytes());
                    status["replication"] = replication;
                } else if let Some(mesh) = service.mesh(room) {
                    status["auto"] = mesh.status(room);
                }
                status
            }).collect();
            Ok(
                json!({"enabled":cfg.enabled,"running":!joined.is_empty(),"rooms":rooms,"bytes":service.repo.total_bytes() + service.clusters.lock().unwrap().values().map(|cluster| cluster.total_bytes()).sum::<u64>(),"applies":"daemon restart"}),
            )
        }
        _ => bail!("unknown escrow command: {cmd}"),
    }
}
