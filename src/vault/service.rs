//! One process-wide service; all durable and cryptographic work runs off the runtime.
use super::{
    blocks::{self, BlockStore},
    group::{Group, InviteCode},
    head::{Accept, Head},
    keys::OwnerKey,
    state::{self, Owner, Peer, SavedOwner},
    wire,
};
use crate::{config::VaultConfig, daemon::AppState, identity::Identity};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Notify, Semaphore};

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub(super) struct PendingJoin {
    pub code: InviteCode,
    pub text: String,
    pub started: u64,
    pub last_sent: u64,
}

pub(super) struct Core {
    pub dir: PathBuf,
    pub cfg: VaultConfig,
    pub me: Arc<Identity>,
    pub group: Option<Group>,
    pub key: Option<OwnerKey>,
    pub store: BlockStore,
    pub owners: BTreeMap<String, Owner>,
    pub peers: BTreeMap<String, Peer>,
    pub last_seen: BTreeMap<String, u64>,
    pub guard: wire::Guard,
    pub join: Option<PendingJoin>,
    pub since: u64,
    pub last_error: Option<String>,
    pub last_gc: u64,
    pub last_hello: u64,
    pub inflight: HashSet<String>,
}

#[derive(Default, Serialize, Deserialize)]
struct Saved {
    owners: Vec<SavedOwner>,
    #[serde(default)]
    last_seen: BTreeMap<String, u64>,
    #[serde(default)]
    pending_join: Option<SavedJoin>,
}

#[derive(Serialize, Deserialize)]
struct SavedJoin {
    code: String,
    started: u64,
}

pub(super) struct Outbound {
    pub room: String,
    pub to: Option<String>,
    pub bytes: Vec<u8>,
}

impl Core {
    pub fn open(dir: PathBuf, cfg: VaultConfig, me: Arc<Identity>) -> Result<Self> {
        cfg.validate()?;
        let group = Group::load(&dir)?;
        let key = OwnerKey::load(&dir)?;
        let store = BlockStore::open(dir.join("blocks"))?;
        let mut owners = BTreeMap::new();
        let mut last_seen = BTreeMap::new();
        let mut join = None;
        let path = dir.join("owners.json");
        if path.exists() {
            ensure!(
                std::fs::metadata(&path)?.len() <= 4 * 1024 * 1024,
                "vault state too large"
            );
            let saved: Saved = serde_json::from_slice(&std::fs::read(path)?)?;
            ensure!(
                saved.owners.len() <= state::MAX_OWNERS && saved.last_seen.len() <= 64,
                "vault state exceeds bounds"
            );
            if let Some(group) = &group {
                if let Some(pending) = saved.pending_join {
                    ensure!(pending.code.len() <= 4096, "stored invite too large");
                    let code = InviteCode::parse(&pending.code)?;
                    ensure!(code.group == group.id, "stored invite group differs");
                    if !group.is_member(me.did())
                        && now_ms().saturating_sub(pending.started) < 10 * 60 * 1000
                    {
                        join = Some(PendingJoin {
                            code,
                            text: pending.code,
                            started: pending.started,
                            last_sent: 0,
                        });
                    }
                }
                for record in saved.owners {
                    let owner = Owner::load(record, &group.id)?;
                    ensure!(
                        owners.insert(owner.cur.owner.clone(), owner).is_none(),
                        "duplicate stored owner"
                    );
                }
                last_seen = saved
                    .last_seen
                    .into_iter()
                    .filter(|(did, _)| group.is_member(did))
                    .collect();
            }
        }
        let mut core = Self {
            dir,
            cfg,
            me,
            group,
            key,
            store,
            owners,
            peers: BTreeMap::new(),
            last_seen,
            guard: wire::Guard::default(),
            join,
            since: now_ms(),
            last_error: None,
            last_gc: 0,
            last_hello: 0,
            inflight: HashSet::new(),
        };
        let ids: Vec<_> = core.owners.keys().cloned().collect();
        for id in ids {
            let pinset = core.owners[&id].cur.pinset.clone();
            let graph = core.graph(&pinset);
            if let Some(owner) = core.owners.get_mut(&id) {
                match graph {
                    Ok(graph) => {
                        owner.complete = owner.holding && graph.complete(owner.cur.bytes);
                        owner.held_bytes = graph.bytes;
                        owner.missing = graph.missing.len();
                    }
                    Err(error) => owner.last_error = Some(error.to_string()),
                }
            }
        }
        if core.group.as_ref().is_some_and(|g| {
            g.entries
                .iter()
                .any(|e| e["op"] == "revoke" && e["member"] == core.me.did())
        }) {
            let own = core.own_id();
            for (id, owner) in &mut core.owners {
                if own.as_deref() != Some(id.as_str()) {
                    owner.holding = false;
                    owner.complete = false;
                    owner.held_bytes = 0;
                }
            }
            let mut graphs = Vec::new();
            if let Some(owner) = own.as_ref().and_then(|id| core.owners.get(id)) {
                for head in state::retained_heads(owner, true, now_ms()) {
                    graphs.push(core.graph(&head.pinset)?);
                }
            }
            if let Some(keep) = state::gc_keep(&graphs, &HashSet::new()) {
                core.store.retain(&keep)?;
            }
            core.last_error = Some("local node is no longer a group member".into());
            core.save()?;
        }
        Ok(core)
    }

    pub fn save(&self) -> Result<()> {
        let saved = Saved {
            owners: self.owners.values().map(Owner::saved).collect(),
            last_seen: self.last_seen.clone(),
            pending_join: self.join.as_ref().map(|j| SavedJoin {
                code: j.text.clone(),
                started: j.started,
            }),
        };
        crate::statefile::write_private(&self.dir.join("owners.json"), &serde_json::to_vec(&saved)?)
    }

    pub fn own_id(&self) -> Option<String> {
        self.key.as_ref().map(OwnerKey::owner_did)
    }

    pub fn free(&self) -> u64 {
        blocks::advertised_free(
            self.cfg.capacity_bytes,
            self.store.usage(),
            blocks::volume_free(self.store.dir()),
            self.cfg.min_free_bytes,
        )
    }

    pub fn message(&self, to: Option<&str>, kind: &str, body: Value) -> Result<Outbound> {
        let group = self.group.as_ref().context("vault is not initialized")?;
        Ok(Outbound {
            room: group.room(),
            to: to.map(str::to_string),
            bytes: wire::seal(&self.me, &group.id, kind, body, now_ms())?,
        })
    }

    pub fn member_messages(&self, to: Option<&str>) -> Result<Vec<Outbound>> {
        let group = self.group.as_ref().context("vault is not initialized")?;
        group
            .entries
            .chunks(16)
            .map(|entries| self.message(to, "members", json!({"entries":entries})))
            .collect()
    }

    pub fn hello(&self, to: Option<&str>) -> Result<Vec<Outbound>> {
        let group = self.group.as_ref().context("vault is not initialized")?;
        // Holding summaries already carry the head; omitting duplicates fits
        // all 64 owners within the fixed 16 KiB control envelope.
        let heads: Vec<_> = self
            .owners
            .values()
            .filter(|o| !o.holding)
            .map(|o| json!({"owner":o.cur.owner,"seq":o.cur.seq,"hash":o.cur.hash()}))
            .collect();
        let holds: Vec<_> = self.owners.values().filter(|o| o.holding).map(|o| json!({"owner":o.cur.owner,"seq":o.cur.seq,"hash":o.cur.hash(),"complete":o.complete,"bytes":o.held_bytes})).collect();
        let body = json!({"free":self.free(),"capacity":self.cfg.capacity_bytes,"since":self.since,
            "members_digest":group.digest(),"heads":heads,"holds":holds});
        Ok(vec![self.message(to, "hello", body)?])
    }

    pub fn receive(&mut self, room: &str, from: &str, raw: &[u8]) -> Result<Vec<Outbound>> {
        let Some(group) = self.group.as_ref().filter(|g| g.room() == room) else {
            return Ok(Vec::new());
        };
        let msg = self.guard.open(raw, &group.id, from, now_ms())?;
        let mut out = Vec::new();
        let now = now_ms();
        if msg.kind == "join" {
            ensure!(group.is_member(self.me.did()), "local node is not admitted");
            if group.is_member(&msg.from_did) {
                return self.member_messages(Some(from));
            }
            let invite = msg.body["invite"].as_str().context("missing invite")?;
            let mac = msg.body["mac"].as_str().context("missing join proof")?;
            let mut changed = group.clone();
            if changed.redeem(invite, mac, &msg.from_did, now) {
                changed.admit(&self.me, &msg.from_did, now)?;
                changed.save(&self.dir)?;
                self.group = Some(changed);
                tracing::info!(member = %msg.from_did, "vault: member admitted");
                out.extend(self.member_messages(None)?);
                out.extend(self.member_messages(Some(from))?);
            }
            return Ok(out);
        }
        // A joiner has no membership set yet. Only its explicitly trusted inviter
        // may bootstrap it, and every entry is independently verified by Group.
        let bootstrap = msg.kind == "members"
            && self
                .join
                .as_ref()
                .is_some_and(|j| j.code.by == msg.from_did);
        ensure!(
            group.is_member(&msg.from_did) || bootstrap,
            "vault sender is not a member"
        );
        match msg.kind.as_str() {
            "members" => {
                let entries = msg.body["entries"].as_array().context("missing entries")?;
                ensure!(entries.len() <= 64, "too many membership entries");
                let mut changed = group.clone();
                if changed.merge(entries) {
                    changed.save(&self.dir)?;
                    self.peers.retain(|did, _| changed.is_member(did));
                    self.last_seen.retain(|did, _| changed.is_member(did));
                    if changed.is_member(self.me.did()) {
                        self.join = None;
                    }
                    self.group = Some(changed);
                    self.save()?;
                    tracing::debug!("vault: membership merged");
                    out.extend(self.hello(Some(from))?);
                }
            }
            "hello" => {
                let mut heads = state::summaries(&msg.body["heads"])?;
                let holds = state::summaries(&msg.body["holds"])?;
                for held in &holds {
                    if !heads.iter().any(|h| h.owner == held.owner) {
                        heads.push(held.clone());
                    }
                }
                ensure!(
                    heads.len() <= state::MAX_OWNERS,
                    "too many distinct hello owners"
                );
                let capacity = msg.body["capacity"].as_u64().context("missing capacity")?;
                let free = msg.body["free"]
                    .as_u64()
                    .context("missing free")?
                    .min(capacity);
                let first_seen = self.peers.get(&msg.from_did).map_or(now, |p| p.first_seen);
                ensure!(
                    self.peers.contains_key(&msg.from_did) || self.peers.len() < 64,
                    "too many peers"
                );
                for owner in self.owners.values() {
                    if heads
                        .iter()
                        .find(|h| h.owner == owner.cur.owner)
                        .is_none_or(|h| {
                            h.seq < owner.cur.seq
                                || (h.seq == owner.cur.seq && h.hash != owner.cur.hash())
                        })
                    {
                        out.push(self.message(
                            Some(from),
                            "head",
                            json!({"head":owner.cur.raw}),
                        )?);
                    }
                }
                for summary in &heads {
                    if self
                        .owners
                        .get(&summary.owner)
                        .is_none_or(|o| summary.seq > o.cur.seq)
                    {
                        out.push(self.message(
                            Some(from),
                            "head_req",
                            json!({"owner":summary.owner}),
                        )?);
                    }
                }
                if msg.body["members_digest"].as_str() != Some(group.digest().as_str()) {
                    out.extend(self.member_messages(Some(from))?);
                }
                self.peers.insert(
                    msg.from_did.clone(),
                    Peer {
                        first_seen,
                        last_seen: now,
                        free,
                        capacity,
                        heads,
                        holds,
                    },
                );
                self.last_seen.insert(msg.from_did, now);
                self.save()?;
            }
            "head" => {
                let head = Head::verify(&msg.body["head"], &group.id)?;
                let owner = head.owner.clone();
                let previous = self.owners.clone();
                let decision = state::accept_head(&mut self.owners, head, &msg.from_did, now)?;
                if self.own_id().as_deref() == Some(owner.as_str()) {
                    if let Some(o) = self.owners.get_mut(&owner) {
                        o.home = self.me.did().into();
                        o.holding = true;
                    }
                }
                if matches!(decision, Accept::New { .. } | Accept::Conflict) {
                    if let Err(error) = self.save() {
                        self.owners = previous;
                        return Err(error);
                    }
                }
                tracing::debug!(%owner, ?decision, "vault: received head");
                if matches!(decision, Accept::Stale) {
                    out.push(self.message(
                        Some(from),
                        "head",
                        json!({"head":self.owners[&owner].cur.raw}),
                    )?);
                }
            }
            "head_req" => {
                if let Some(owner) = msg.body["owner"]
                    .as_str()
                    .and_then(|id| self.owners.get(id))
                {
                    out.push(self.message(Some(from), "head", json!({"head":owner.cur.raw}))?);
                }
            }
            _ => anyhow::bail!("unknown vault message"),
        }
        Ok(out)
    }
}

pub(super) struct Service {
    pub core: Mutex<Core>,
    pub app: Arc<AppState>,
    pub wake: Notify,
    pub joined: Mutex<Option<String>>,
    pub joining: tokio::sync::Mutex<()>,
    pub provider: Mutex<Option<mistlib::storage::BlockProviderHandle>>,
    pub inbound: Arc<Semaphore>,
    pub serving: Arc<Semaphore>,
}

impl Service {
    pub async fn work<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Core) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut core = service
                .core
                .lock()
                .map_err(|_| anyhow::anyhow!("vault state lock poisoned"))?;
            f(&mut core)
        })
        .await?
    }

    pub async fn send(&self, messages: Vec<Outbound>) {
        for message in messages {
            if !self.app.network.permitted() {
                return;
            }
            let result = if let Some(to) = message.to {
                crate::net::send_direct_wait(&message.room, &to, message.bytes).await
            } else {
                crate::net::send_broadcast(&message.room, message.bytes).await
            };
            if let Err(error) = result {
                tracing::debug!(%error, "vault: send deferred");
            }
        }
    }

    pub fn register(self: &Arc<Self>) {
        self.register_provider();
        let service = self.clone();
        let rt = tokio::runtime::Handle::current();
        crate::net::register_room_handler(move |event, room, from, raw| {
            if !room.starts_with("mistl-vault-") || raw.len() > wire::MAX_MESSAGE {
                return;
            }
            if event != crate::net::EVENT_PEER_CONNECTED
                && (event != crate::net::EVENT_RAW || !wire::is_vault_message(raw))
            {
                return;
            }
            let Ok(permit) = service.inbound.clone().try_acquire_owned() else {
                return;
            };
            let (service, room, from, raw) = (
                service.clone(),
                room.to_string(),
                from.to_string(),
                raw.to_vec(),
            );
            rt.spawn(async move {
                let _permit = permit;
                if !service.app.network.permitted() {
                    return;
                }
                let result = service
                    .work(move |core| {
                        if event == crate::net::EVENT_PEER_CONNECTED {
                            if core.group.as_ref().is_some_and(|g| g.room() == room) {
                                core.hello(Some(&from))
                            } else {
                                Ok(Vec::new())
                            }
                        } else {
                            core.receive(&room, &from, &raw)
                        }
                    })
                    .await;
                match result {
                    Ok(messages) => {
                        service.send(messages).await;
                        service.wake.notify_one();
                    }
                    Err(error) => tracing::debug!(%error, "vault: dropped control message"),
                }
            });
        });
    }
}
