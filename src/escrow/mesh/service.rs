use super::{
    decision::{self, Acceptance, Entry},
    now_ms, placement, wire,
};
use crate::{
    escrow::{
        Service,
        protocol::{Delete, Head},
        repository::MeshRecord,
    },
    identity::Identity,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore, mpsc};

type Incoming = (String, String, wire::Inbound);

const MAX_PROVIDERS: usize = 16;
const ONLINE_MS: u64 = 45_000;

pub(super) struct Peer {
    pub did: String,
    pub seen: u64,
    pub free: u64,
    pub entries: BTreeMap<String, Entry>,
    digest: String,
    held_digest: String,
    syncing: bool,
    after: Option<String>,
    requested: u64,
    pending: BTreeSet<String>,
}

#[derive(Default)]
pub(super) struct Room {
    pub peers: BTreeMap<String, Peer>,
    conflicts: HashSet<String>,
    release: HashMap<String, u64>,
    fetching: HashSet<String>,
    pub manifests: BTreeMap<String, (Head, mistlib_core::storage::FileManifest, u64)>,
    scrub: usize,
    last_scrub: u64,
}

pub(in crate::escrow) struct Mesh {
    pub(super) service: Weak<Service>,
    pub(super) me: Arc<Identity>,
    pub(super) rooms: Mutex<HashMap<String, Room>>,
    guard: Mutex<wire::Guard>,
    pub(super) since: u64,
    wake: Notify,
    started: AtomicBool,
    pub(super) proxies: Arc<Semaphore>,
    incoming: mpsc::Sender<Incoming>,
    receiver: Mutex<Option<mpsc::Receiver<Incoming>>>,
}

pub(super) fn digests(records: &[MeshRecord]) -> (String, String) {
    use sha2::{Digest, Sha256};
    let mut metadata = Sha256::new();
    let mut holdings = Sha256::new();
    for r in records {
        let e = r.entry();
        let line = format!("{}:{}:{}:{}\n", e.slot, e.seq, e.kind, e.hash);
        metadata.update(line.as_bytes());
        if e.held {
            holdings.update(line.as_bytes());
        }
    }
    (
        hex::encode(metadata.finalize()),
        hex::encode(holdings.finalize()),
    )
}

impl Mesh {
    pub fn new(service: &Arc<Service>, me: Arc<Identity>) -> Arc<Self> {
        let (incoming, receiver) = mpsc::channel(64);
        let rooms = service
            .cfg
            .rooms
            .iter()
            .filter(|room| service.mesh_room(room))
            .map(|room| (room.clone(), Room::default()))
            .collect();
        Arc::new(Self {
            service: Arc::downgrade(service),
            me,
            rooms: Mutex::new(rooms),
            guard: Mutex::new(wire::Guard::default()),
            since: now_ms(),
            wake: Notify::new(),
            started: AtomicBool::new(false),
            proxies: Arc::new(Semaphore::new(4)),
            incoming,
            receiver: Mutex::new(Some(receiver)),
        })
    }
    pub fn did(&self) -> &str {
        self.me.did()
    }
    pub fn notify(&self) {
        self.wake.notify_one();
    }
    pub fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(mut receiver) = self.receiver.lock().unwrap().take() {
            let weak = Arc::downgrade(self);
            tokio::spawn(async move {
                while let Some((room, from, message)) = receiver.recv().await {
                    let Some(mesh) = weak.upgrade() else {
                        break;
                    };
                    mesh.handle(&room, &from, message).await;
                }
            });
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(15));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let Some(mesh) = weak.upgrade() else {
                    break;
                };
                tokio::select! { _ = timer.tick() => {}, _ = mesh.wake.notified() => {} }
                mesh.tick().await;
            }
        });
    }
    pub(super) async fn send(&self, room: &str, peer: Option<&str>, kind: &str, body: Value) {
        let Ok(bytes) = wire::seal(&self.me, room, kind, body, now_ms()) else {
            return;
        };
        let Some(service) = self.service.upgrade() else {
            return;
        };
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            if let Some(peer) = peer {
                (service.send_reply)(room.into(), peer.into(), bytes).await
            } else if kind == "hello" {
                crate::net::send_broadcast(room, bytes).await
            } else {
                let peers: Vec<_> = self
                    .rooms
                    .lock()
                    .unwrap()
                    .get(room)
                    .map(|r| r.peers.keys().cloned().collect())
                    .unwrap_or_default();
                for peer in peers {
                    let _ = (service.send_reply)(room.into(), peer, bytes.clone()).await;
                }
                Ok(())
            }
        })
        .await;
    }
    pub async fn hello(&self, room: &str, peer: Option<&str>) {
        let Some(service) = self.service.upgrade() else {
            return;
        };
        let records = service.repo.mesh_records(room);
        let (digest, held_digest) = digests(&records);
        self.send(
            room,
            peer,
            "hello",
            json!({"free":service.repo.free_bytes(),
            "capacity":service.cfg.max_total_bytes / (service.cfg.replication.len() as u64 + 1),
            "since":self.since,"digest":digest,"count":records.len(),"held_digest":held_digest}),
        )
        .await;
    }
    pub async fn advertise(&self, room: &str, slot: &str, peer: Option<&str>) {
        let Some(service) = self.service.upgrade() else {
            return;
        };
        let Some(r) = service
            .repo
            .mesh_records(room)
            .into_iter()
            .find(|r| r.head.slot == slot)
        else {
            return;
        };
        if let Some(delete) = r.deleted {
            self.send(
                room,
                peer,
                "tombstone",
                json!({"delete":delete,"writer":r.head.writer,"deleted_head":r.head}),
            )
            .await;
        } else {
            self.send(room, peer, "head", json!({"head":r.head,"home":r.home}))
                .await;
        }
    }
    /// One bounded control queue prevents index bursts from occupying client reply permits.
    pub fn receive(self: &Arc<Self>, room: &str, from: &str, raw: &[u8]) -> bool {
        if !wire::is_mesh_message(raw) {
            return false;
        }
        if from == self.me.node_id() {
            return true;
        }
        let Ok(message) = self.guard.lock().unwrap().open(raw, room, from, now_ms()) else {
            return true;
        };
        let _ = self.incoming.try_send((room.into(), from.into(), message));
        true
    }
    pub(super) async fn handle(self: &Arc<Self>, room: &str, from: &str, message: wire::Inbound) {
        let Some(service) = self.service.upgrade() else {
            return;
        };
        let now = now_ms();
        let body = message.body;
        if message.kind == "hello" {
            let (new, request) = {
                let mut rooms = self.rooms.lock().unwrap();
                let Some(state) = rooms.get_mut(room) else {
                    return;
                };
                if !state.peers.contains_key(from) && state.peers.len() >= MAX_PROVIDERS - 1 {
                    let dead = state
                        .peers
                        .iter()
                        .find(|(_, p)| {
                            now.saturating_sub(p.seen)
                                > service.cfg.dead_after_secs.saturating_mul(1000)
                        })
                        .map(|(n, _)| n.clone());
                    if let Some(dead) = dead {
                        state.peers.remove(&dead);
                    } else {
                        return;
                    }
                }
                let new = !state.peers.contains_key(from);
                let peer = state.peers.entry(from.into()).or_insert_with(|| Peer {
                    did: message.from_did,
                    seen: now,
                    free: 0,
                    entries: BTreeMap::new(),
                    digest: String::new(),
                    held_digest: String::new(),
                    syncing: false,
                    after: None,
                    requested: 0,
                    pending: BTreeSet::new(),
                });
                peer.seen = now;
                peer.free = body["free"].as_u64().unwrap_or(0);
                let digest = body["digest"].as_str().unwrap_or("");
                let held = body["held_digest"].as_str().unwrap_or("");
                let local_digest = digests(&service.repo.mesh_records(room)).0;
                let request = !peer.syncing
                    && (new
                        || digest != peer.digest
                        || held != peer.held_digest
                        || (digest != local_digest
                            && peer.pending.is_empty()
                            && now.saturating_sub(peer.requested) > 60_000));
                if request {
                    peer.digest = digest.into();
                    peer.held_digest = held.into();
                    peer.syncing = true;
                    peer.after = None;
                    peer.requested = now;
                    peer.entries.clear();
                    peer.pending.clear();
                }
                (new, request)
            };
            if new {
                tracing::info!(room, peer = from, "escrow mesh: provider joined");
                self.hello(room, Some(from)).await;
                self.notify();
            }
            if request {
                self.send(room, Some(from), "index_req", json!({})).await;
            }
            return;
        }
        let known = self
            .rooms
            .lock()
            .unwrap()
            .get(room)
            .is_some_and(|s| s.peers.contains_key(from));
        if !known {
            self.hello(room, Some(from)).await;
            return;
        }
        match message.kind.as_str() {
            "index_req" => {
                let after = body["after"].as_str().unwrap_or("");
                let records = service.repo.mesh_records(room);
                // 64 leaves room for the signed envelope under the 16 KiB limit.
                let entries: Vec<_> = records
                    .iter()
                    .filter(|r| r.head.slot.as_str() > after)
                    .take(64)
                    .map(MeshRecord::entry)
                    .collect();
                let next = entries
                    .last()
                    .filter(|last| records.iter().any(|r| r.head.slot > last.slot))
                    .map(|e| e.slot.clone());
                self.send(
                    room,
                    Some(from),
                    "index",
                    json!({"entries":entries,"next":next}),
                )
                .await;
            }
            "index" => {
                let Ok(entries) = serde_json::from_value::<Vec<Entry>>(body["entries"].clone())
                else {
                    return;
                };
                let records = service.repo.mesh_records(room);
                let local: BTreeMap<_, _> = records
                    .iter()
                    .map(|r| (r.head.slot.clone(), r.entry()))
                    .collect();
                let next = body["next"].as_str().map(str::to_owned);
                {
                    let mut rooms = self.rooms.lock().unwrap();
                    let Some(peer) = rooms.get_mut(room).and_then(|s| s.peers.get_mut(from)) else {
                        return;
                    };
                    if !peer.syncing
                        || entries
                            .first()
                            .is_some_and(|e| peer.after.as_ref().is_some_and(|a| e.slot <= *a))
                        || peer.entries.len() + entries.len() > service.cfg.max_slots_per_room
                    {
                        return;
                    }
                    for e in entries {
                        if decision::needs_head(local.get(&e.slot), &e)
                            && peer.pending.len() < service.cfg.max_slots_per_room
                        {
                            peer.pending.insert(e.slot.clone());
                        }
                        peer.entries.insert(e.slot.clone(), e);
                    }
                    peer.after = next.clone();
                    peer.requested = if next.is_some() { 0 } else { now };
                    peer.syncing = next.is_some();
                }
                self.request_heads(room, from).await;
                // Continue on the paced tick, leaving room in the peer rate budget for heads.
            }
            "head_req" => {
                if let Some(slot) = body["slot"].as_str() {
                    self.advertise(room, slot, Some(from)).await;
                }
            }
            "head" | "tombstone" => {
                let tombstone = message.kind == "tombstone";
                let Ok(head) = serde_json::from_value::<Head>(
                    body[if tombstone { "deleted_head" } else { "head" }].clone(),
                ) else {
                    return;
                };
                let delete = if tombstone {
                    if body["writer"].as_str() != Some(head.writer.as_str()) {
                        return;
                    }
                    let Ok(d) = serde_json::from_value::<Delete>(body["delete"].clone()) else {
                        return;
                    };
                    Some(d)
                } else {
                    None
                };
                let home = body["home"].as_str().map(str::to_owned);
                let stripe = usize::from(crate::escrow::sha2_byte(room, &head.slot));
                let Ok(_slot) =
                    tokio::time::timeout(Duration::from_secs(5), service.slots[stripe].lock())
                        .await
                else {
                    return;
                };
                let (worker, room_owned, incoming) =
                    (service.clone(), room.to_owned(), head.clone());
                let result = tokio::task::spawn_blocking(move || {
                    worker
                        .repo
                        .learn(&room_owned, &incoming, home.as_deref(), delete.as_ref())
                })
                .await;
                drop(_slot);
                let Ok(result) = result else {
                    return;
                };
                match result {
                    Ok(Acceptance::New) => {
                        tracing::debug!(room, slot=%head.slot, seq=head.seq, tombstone, "escrow mesh: metadata accepted");
                        {
                            let mut rooms = self.rooms.lock().unwrap();
                            if let Some(state) = rooms.get_mut(room) {
                                state.conflicts.remove(&head.slot);
                                state.manifests.remove(&head.slot);
                            }
                        }
                        self.advertise(room, &head.slot, None).await;
                        self.notify();
                    }
                    Ok(Acceptance::Conflict | Acceptance::WriterMismatch) => {
                        if let Some(state) = self.rooms.lock().unwrap().get_mut(room) {
                            state.conflicts.insert(head.slot.clone());
                        }
                        tracing::info!(room, slot=%head.slot, "escrow mesh: conflicting head rejected");
                    }
                    Ok(Acceptance::Stale)
                    | Err(crate::escrow::protocol::Error {
                        code: "deleted", ..
                    }) => self.advertise(room, &head.slot, Some(from)).await,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    async fn request_heads(&self, room: &str, from: &str) {
        let slots = {
            let mut rooms = self.rooms.lock().unwrap();
            let Some(peer) = rooms.get_mut(room).and_then(|r| r.peers.get_mut(from)) else {
                return;
            };
            let slots: Vec<_> = peer.pending.iter().take(8).cloned().collect();
            for slot in &slots {
                peer.pending.remove(slot);
            }
            slots
        };
        for slot in slots {
            self.send(room, Some(from), "head_req", json!({"slot":slot}))
                .await;
        }
    }
    pub(super) fn plan(
        &self,
        service: &Service,
        state: &Room,
        room: &str,
        record: &MeshRecord,
        now: u64,
    ) -> (usize, Vec<String>, Vec<String>) {
        let dead = service.cfg.dead_after_secs.saturating_mul(1000);
        let entry = record.entry();
        let confirms = |p: &Peer| {
            now.saturating_sub(p.seen) <= ONLINE_MS
                && p.entries.get(&entry.slot).is_some_and(|e| {
                    e.kind == "head" && e.hash == entry.hash && e.seq == entry.seq && e.held
                })
        };
        let mut confirmed: Vec<_> = state
            .peers
            .values()
            .filter(|p| confirms(p))
            .map(|p| p.did.clone())
            .collect();
        if record.held {
            confirmed.push(self.me.did().into());
        }
        let mut inputs = vec![decision::ProviderInput {
            did: self.me.did(),
            free: service.repo.free_bytes(),
            last_seen: now,
            held: record.held,
        }];
        inputs.extend(state.peers.values().map(|p| decision::ProviderInput {
            did: &p.did,
            free: p.free,
            last_seen: p.seen,
            held: confirms(p),
        }));
        let (target, candidates, home) = decision::placement_inputs(
            &inputs,
            now,
            dead,
            record.head.size,
            service.cfg.auto_replicas,
            record.home.as_deref(),
        );
        let desired = placement::place(
            &format!("{room}/{}", record.head.slot),
            record.head.size,
            target,
            home.as_deref(),
            &candidates,
        );
        (target, desired, confirmed)
    }
    pub fn slots(&self, room: &str) -> Value {
        let Some(service) = self.service.upgrade() else {
            return json!({"room":room,"slots":[]});
        };
        let records = service.repo.mesh_records(room);
        let rooms = self.rooms.lock().unwrap();
        let Some(state) = rooms.get(room) else {
            return json!({"room":room,"slots":[]});
        };
        let slots: Vec<_> = records.iter().filter(|r| r.deleted.is_none()).take(1024).map(|r| {
            let (target,_,holders) = self.plan(&service,state,room,r,now_ms());
            json!({"slot":r.head.slot,"seq":r.head.seq,"state":decision::classify(state.conflicts.contains(&r.head.slot),holders.len(),target,state.fetching.contains(&r.head.slot)),
                "confirmed":holders.len(),"target":target,"holders":holders})
        }).collect();
        json!({"room":room,"slots":slots})
    }
    pub fn status(&self, room: &str) -> Value {
        let Some(service) = self.service.upgrade() else {
            return Value::Null;
        };
        let records = service.repo.mesh_records(room);
        let rooms = self.rooms.lock().unwrap();
        let Some(state) = rooms.get(room) else {
            return Value::Null;
        };
        let now = now_ms();
        let mut providers = vec![
            json!({"did":self.me.did(),"node":self.me.node_id(),"self":true,"online":true,"free":service.repo.free_bytes(),"last_seen":now}),
        ];
        providers.extend(state.peers.iter().map(|(node,p)| json!({"did":p.did,"node":node,"self":false,"online":now.saturating_sub(p.seen)<=ONLINE_MS,"free":p.free,"last_seen":p.seen})));
        let target = (service.cfg.auto_replicas as usize).min(
            1 + state
                .peers
                .values()
                .filter(|p| {
                    now.saturating_sub(p.seen) <= service.cfg.dead_after_secs.saturating_mul(1000)
                })
                .count(),
        );
        let mut result = json!({"target":target,"providers":providers,"slots":records.iter().filter(|r| r.deleted.is_none()).count(),"deleted":records.iter().filter(|r| r.deleted.is_some()).count(),"healthy":0,"replicating":0,"degraded":0,"conflicts":0,"held":0,"insufficient_capacity":false});
        for record in records.iter().filter(|r| r.deleted.is_none()) {
            let (target, _, confirmed) = self.plan(&service, state, room, record, now);
            let kind = decision::classify(
                state.conflicts.contains(&record.head.slot),
                confirmed.len(),
                target,
                state.fetching.contains(&record.head.slot),
            );
            let key = if kind == "conflict" {
                "conflicts"
            } else {
                kind
            };
            result[key] = json!(result[key].as_u64().unwrap_or(0) + 1);
            if record.held {
                result["held"] = json!(result["held"].as_u64().unwrap_or(0) + 1);
            }
            if !record.held
                && (record.head.size > service.cfg.max_blob_bytes
                    || service.repo.free_bytes()
                        < record.head.size.saturating_add(
                            crate::escrow::repository::manifest_limit(record.head.size) as u64,
                        ))
            {
                result["insufficient_capacity"] = json!(true);
            }
        }
        result
    }
    pub(super) async fn tick(self: &Arc<Self>) {
        let Some(service) = self.service.upgrade() else {
            return;
        };
        let rooms: Vec<_> = self.rooms.lock().unwrap().keys().cloned().collect();
        for room in rooms {
            if !service.joined.lock().unwrap().contains(&room) {
                continue;
            }
            self.hello(&room, None).await;
            let peers: Vec<_> = self
                .rooms
                .lock()
                .unwrap()
                .get(&room)
                .map(|r| r.peers.keys().cloned().collect())
                .unwrap_or_default();
            for peer in peers {
                let page = {
                    let mut rooms = self.rooms.lock().unwrap();
                    rooms
                        .get_mut(&room)
                        .and_then(|r| r.peers.get_mut(&peer))
                        .and_then(|p| {
                            if p.syncing
                                && (p.requested == 0
                                    || now_ms().saturating_sub(p.requested) > 30_000)
                            {
                                p.requested = now_ms();
                                Some(p.after.clone())
                            } else {
                                None
                            }
                        })
                };
                if let Some(after) = page {
                    self.send(&room, Some(&peer), "index_req", json!({"after":after}))
                        .await;
                }
                self.request_heads(&room, &peer).await;
            }
            let records = service.repo.mesh_records(&room);
            for record in records
                .iter()
                .filter(|r| r.deleted.is_none() && r.held && r.home.is_none())
            {
                let stripe = usize::from(crate::escrow::sha2_byte(&room, &record.head.slot));
                let (worker, room, head, home) = (
                    service.clone(),
                    room.clone(),
                    record.head.clone(),
                    self.me.did().to_owned(),
                );
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(_slot) = worker.slots[stripe].try_lock() {
                        let _ = worker.repo.set_home(&room, &head, &home);
                    }
                })
                .await;
            }
            let scrub = {
                let mut rooms = self.rooms.lock().unwrap();
                let Some(state) = rooms.get_mut(&room) else {
                    continue;
                };
                let held: Vec<_> = records.iter().filter(|r| r.held).collect();
                if now_ms().saturating_sub(state.last_scrub) < 60_000 {
                    Vec::new()
                } else {
                    let selected: Vec<_> = (0..held.len().min(2))
                        .map(|i| held[(state.scrub + i) % held.len()].head.clone())
                        .collect();
                    state.scrub = state.scrub.wrapping_add(2);
                    state.last_scrub = now_ms();
                    selected
                }
            };
            for head in scrub {
                let stripe = usize::from(crate::escrow::sha2_byte(&room, &head.slot));
                let (worker, room) = (service.clone(), room.clone());
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(_slot) = worker.slots[stripe].try_lock() {
                        if let Err(error) = worker.repo.verify_holding(&room, &head) {
                            tracing::warn!(room, slot=%head.slot, code=error.code, "escrow mesh: scrub persistence failed");
                        }
                    }
                }).await;
            }
            let mut records = service.repo.mesh_records(&room);
            if !records.is_empty() {
                let shift = (now_ms() / 15_000) as usize % records.len();
                records.rotate_left(shift);
            }
            for record in records.into_iter().filter(|r| r.deleted.is_none()) {
                let now = now_ms();
                let (want, sources) = {
                    let mut rooms = self.rooms.lock().unwrap();
                    let Some(state) = rooms.get_mut(&room) else {
                        continue;
                    };
                    let (target, desired, confirmed) =
                        self.plan(&service, state, &room, &record, now);
                    // Unlike a configured membership roster, room discovery cannot prove
                    // that the first hello represents the complete provider set. Take the
                    // conservative 60-second branch before starting any new holding.
                    let informed = decision::informed(self.since, now, 0, 0);
                    let placed = desired.iter().any(|did| did == self.me.did());
                    let all = desired.len() == target
                        && desired.iter().all(|did| confirmed.contains(did));
                    let (want, release) = decision::holding(
                        placed,
                        record.held,
                        informed,
                        all,
                        state.release.get(&record.head.slot).copied(),
                        now,
                    );
                    if let Some(release) = release {
                        state.release.insert(record.head.slot.clone(), release);
                    } else {
                        state.release.remove(&record.head.slot);
                    }
                    let mut sources: Vec<_> = confirmed
                        .iter()
                        .filter(|did| did.as_str() != self.me.did())
                        .map(|did| crate::identity::node_id_for_did(did))
                        .collect();
                    if let Some(home) = record.home.as_deref().filter(|home| *home != self.me.did())
                    {
                        let node = crate::identity::node_id_for_did(home);
                        if !sources.contains(&node) {
                            sources.push(node);
                        }
                    }
                    (want, sources)
                };
                if !want && record.held {
                    let stripe = usize::from(crate::escrow::sha2_byte(&room, &record.head.slot));
                    let (worker, room) = (service.clone(), room.clone());
                    let _ = tokio::task::spawn_blocking(move || {
                        if let Ok(_slot) = worker.slots[stripe].try_lock() {
                            if worker.repo.drop_holding(&room, &record.head).is_ok() {
                                tracing::info!(room,slot=%record.head.slot,"escrow mesh: released non-placed holding");
                            }
                        }
                    }).await;
                } else if want && !record.held && !sources.is_empty() {
                    self.pull(&service, &room, record.head, sources);
                }
            }
        }
    }
    fn pull(
        self: &Arc<Self>,
        service: &Arc<Service>,
        room: &str,
        head: Head,
        sources: Vec<String>,
    ) {
        let source = sources[(now_ms() / 15_000) as usize % sources.len()].clone();
        {
            let mut pulling = service.pulling.lock().unwrap();
            if pulling.len() >= 4 || !pulling.insert(source.clone()) {
                return;
            }
        }
        {
            let mut rooms = self.rooms.lock().unwrap();
            let Some(state) = rooms.get_mut(room) else {
                service.pulling.lock().unwrap().remove(&source);
                return;
            };
            if !state.fetching.insert(head.slot.clone()) {
                service.pulling.lock().unwrap().remove(&source);
                return;
            }
        }
        let (mesh, service, room) = (self.clone(), service.clone(), room.to_owned());
        tokio::spawn(async move {
            let _pull = crate::escrow::PullGuard {
                service: &service,
                from: source.clone(),
            };
            let stripe = usize::from(crate::escrow::sha2_byte(&room, &head.slot));
            let result = async {
                let _slot =
                    tokio::time::timeout(Duration::from_secs(5), service.slots[stripe].lock())
                        .await
                        .map_err(|_| crate::escrow::protocol::Error::new("busy"))?;
                if service.repo.head(&room, &head.slot).as_ref() != Some(&head) {
                    return Ok(());
                }
                if service.repo.held(&room, &head) {
                    return Ok(());
                }
                let Some(_reservation) = service.repo.reserve(&room, &head)? else {
                    return Ok(());
                };
                let (blob, manifest) = service.transfers.pull(&room, &source, &head).await?;
                let (worker, room_owned, incoming) = (service.clone(), room.clone(), head.clone());
                tokio::task::spawn_blocking(move || {
                    worker.repo.commit(&room_owned, &incoming, &blob, &manifest)
                })
                .await
                .map_err(|_| crate::escrow::protocol::Error::new("fetch_failed"))??;
                tracing::info!(room,slot=%head.slot,seq=head.seq,"escrow mesh: holding committed");
                Ok::<_, crate::escrow::protocol::Error>(())
            }
            .await;
            if let Err(error) = result {
                tracing::debug!(room,slot=%head.slot,code=error.code,"escrow mesh: pull deferred");
            }
            if let Some(state) = mesh.rooms.lock().unwrap().get_mut(&room) {
                state.fetching.remove(&head.slot);
            }
            mesh.hello(&room, None).await;
        });
    }
}
