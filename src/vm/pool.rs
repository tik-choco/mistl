//! Pool membership, host capacity announcements, VM placement, and protection.
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{ErrorKind, Read};
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{Context, Result, bail, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::daemon::AppState;
use crate::{identity, net, placement};

use super::{CreateArgs, VmSize, ha, host, store, wire};

const STATE_FILE: &str = "pool.json";
const INVITE_PREFIX: &str = "mistl-pool1:";
const INVITE_LIMIT: usize = 1024;
const HELLO_INTERVAL: Duration = Duration::from_secs(15);
const OFFLINE_AFTER: Duration = Duration::from_secs(60);
const FORGET_AFTER: Duration = Duration::from_secs(24 * 60 * 60);
const HOST_REFRESH: Duration = Duration::from_secs(10 * 60);
const RATE_WINDOW: Duration = Duration::from_secs(10);
const RATE_LIMIT: usize = 16;
const MAX_MEMBERS: usize = 32;
const MAX_NAME_CHARS: usize = 64;
const CREATE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const JOIN_TIMEOUT: Duration = Duration::from_secs(60);
const PROTECT_INTERVAL: Duration = Duration::from_secs(60);
const PROTECT_BACKOFF: Duration = Duration::from_secs(10 * 60);
const MAX_PENDING: usize = 32;
const MAX_REMOTE_CREATES: usize = 2;
const MAX_RECENT_CREATES: usize = 256;
const MAX_PROTECT_BACKOFFS: usize = 256;
const MAX_CLOCK_SKEW_MS: u64 = 120_000;
const MAX_CREATE_LIST: usize = 32;

static RUNTIME: LazyLock<Mutex<Runtime>> = LazyLock::new(Default::default);
static ACCEL: tokio::sync::Mutex<Option<(Instant, String)>> = tokio::sync::Mutex::const_new(None);
static INBOUND_JOINS: Semaphore = Semaphore::const_new(8);
// Each protection can wait for up to four peers.
static PROTECTIONS: Semaphore = Semaphore::const_new(MAX_PENDING / 4);

#[derive(Clone, PartialEq, Eq)]
pub(super) struct PoolState {
    pub pool_id: String,
    pub room: String,
    pub key: [u8; 32],
    created_at: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedPool {
    v: u8,
    pool_id: String,
    room: String,
    key_b64: String,
    created_at: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Invite {
    v: u8,
    pool_id: String,
    room: String,
    key: String,
}

fn decode_key(v: u8, pool_id: &str, room: &str, key: &str) -> Result<[u8; 32]> {
    ensure!(v == 1, "unsupported VM pool version");
    ensure!(store::valid_server_id(pool_id), "invalid VM pool id");
    ensure!(crate::escrow::valid_room(room), "invalid VM pool room");
    URL_SAFE_NO_PAD
        .decode(key)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .context("invalid VM pool key")
}

impl PoolState {
    fn new() -> Self {
        let pool_id = store::new_server_id();
        let mut key = [0; 32];
        OsRng.fill_bytes(&mut key);
        Self {
            room: format!("mistl-vm-pool-{pool_id}"),
            pool_id,
            key,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    fn same_pool(&self, other: &Self) -> bool {
        self.pool_id == other.pool_id && self.room == other.room && self.key == other.key
    }

    fn invite(&self) -> Result<String> {
        let invite = Invite {
            v: 1,
            pool_id: self.pool_id.clone(),
            room: self.room.clone(),
            key: URL_SAFE_NO_PAD.encode(self.key),
        };
        Ok(format!(
            "{INVITE_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&invite)?)
        ))
    }

    fn from_invite(code: &str) -> Result<Self> {
        ensure!(code.len() <= INVITE_LIMIT, "VM pool invite too large");
        let body = code
            .strip_prefix(INVITE_PREFIX)
            .context("invalid VM pool invite")?;
        // Parse errors can include input fragments. Keep invite contents out of errors.
        let bytes = URL_SAFE_NO_PAD
            .decode(body)
            .ok()
            .context("invalid VM pool invite")?;
        let invite: Invite = serde_json::from_slice(&bytes)
            .ok()
            .context("invalid VM pool invite")?;
        let key = decode_key(invite.v, &invite.pool_id, &invite.room, &invite.key)?;
        Ok(Self {
            pool_id: invite.pool_id,
            room: invite.room,
            key,
            created_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    fn load(path: &Path) -> Result<Option<Self>> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("opening VM pool state"),
        };
        let mut bytes = Vec::new();
        file.take((INVITE_LIMIT + 1) as u64)
            .read_to_end(&mut bytes)
            .context("reading VM pool state")?;
        ensure!(bytes.len() <= INVITE_LIMIT, "VM pool state too large");
        let saved: SavedPool = serde_json::from_slice(&bytes)
            .ok()
            .context("invalid VM pool state")?;
        let key = decode_key(saved.v, &saved.pool_id, &saved.room, &saved.key_b64)?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(&saved.created_at).is_ok(),
            "invalid VM pool creation time"
        );
        Ok(Some(Self {
            pool_id: saved.pool_id,
            room: saved.room,
            key,
            created_at: saved.created_at,
        }))
    }

    fn save(&self, path: &Path) -> Result<()> {
        let saved = SavedPool {
            v: 1,
            pool_id: self.pool_id.clone(),
            room: self.room.clone(),
            key_b64: URL_SAFE_NO_PAD.encode(self.key),
            created_at: self.created_at.clone(),
        };
        crate::statefile::write_private(path, &serde_json::to_vec(&saved)?)
            .context("saving VM pool state")
    }
}

/// The packet envelope carries the DID for every message, including Hello.
#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct PoolHello {
    pub name: String,
    pub ha_capable: bool,
    pub accel: String,
    #[serde(flatten)]
    pub resources: host::Resources,
    pub max_vms: u32,
    pub max_memory_mb: u32,
    pub max_cpus: u32,
    pub max_disk_gb: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_slots: Option<u32>,
    pub running_vms: u32,
    pub running_memory_mb: u64,
    pub running_cpus: u64,
}

impl PoolHello {
    fn used_slots(&self) -> u32 {
        self.used_slots.unwrap_or(self.running_vms)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum PoolMessage {
    Hello(PoolHello),
    Create {
        req: String,
        name: Option<String>,
        #[serde(flatten)]
        size: VmSize,
        ssh_keys: Vec<String>,
        allow: Vec<String>,
        autostart: bool,
        start: bool,
    },
    Created {
        req: String,
        vm: Value,
    },
    Failed {
        req: String,
        error: String,
    },
    Group {
        server_id: String,
        sealed: String,
    },
    Joined {
        server_id: String,
    },
}

#[derive(Serialize, Deserialize)]
struct Packet {
    mpool: u8,
    pool_id: String,
    from: String,
    did: String,
    ts: i64,
    #[serde(flatten)]
    message: PoolMessage,
}

type Body = BTreeMap<String, Value>;

impl Packet {
    fn mac(key: &[u8; 32], body: &Body) -> Result<Hmac<Sha256>> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("fixed-size HMAC key");
        mac.update(&serde_json::to_vec(body)?);
        Ok(mac)
    }

    fn encode(&self, key: &[u8; 32]) -> Result<Vec<u8>> {
        let mut body: Body = serde_json::from_value(serde_json::to_value(self)?)?;
        let tag = Self::mac(key, &body)?.finalize().into_bytes();
        body.insert("mac".into(), json!(hex::encode(tag)));
        let bytes = serde_json::to_vec(&body)?;
        ensure!(
            bytes.len() <= wire::CONTROL_LIMIT,
            "VM pool message too large"
        );
        Ok(bytes)
    }

    fn verify(bytes: &[u8], from: &str, pool: &PoolState, now: i64) -> Result<Self> {
        ensure!(
            bytes.len() <= wire::CONTROL_LIMIT,
            "VM pool message too large"
        );
        let mut body: Body = serde_json::from_slice(bytes)?;
        ensure!(
            ["mvm", "kind", "t", "v"]
                .iter()
                .all(|key| !body.contains_key(*key)),
            "invalid VM pool envelope"
        );
        let tag = body.remove("mac").context("missing VM pool MAC")?;
        let tag = hex::decode(tag.as_str().context("invalid VM pool MAC")?)?;
        Self::mac(&pool.key, &body)?.verify_slice(&tag)?;
        let packet: Self = serde_json::from_value(serde_json::to_value(body)?)?;
        ensure!(
            packet.mpool == 1
                && packet.pool_id == pool.pool_id
                && packet.from == from
                && packet.ts.abs_diff(now) <= MAX_CLOCK_SKEW_MS,
            "invalid VM pool envelope"
        );
        identity::pubkey_from_did(&packet.did)?;
        ensure!(
            identity::node_id_for_did(&packet.did) == from,
            "VM pool DID does not match the transport sender"
        );
        Ok(packet)
    }
}

#[derive(Clone)]
pub(super) struct MemberInfo {
    pub node: String,
    pub did: String,
    pub online: bool,
    pub last_seen_secs: u64,
    pub hello: PoolHello,
}

struct Peer {
    did: String,
    hello: PoolHello,
    seen: Instant,
    ts: i64,
    packets: VecDeque<Instant>,
}

#[derive(Default)]
struct MemberTable {
    peers: HashMap<String, Peer>,
    pending: HashMap<String, VecDeque<Instant>>,
}

impl MemberTable {
    fn expire(&mut self, now: Instant) {
        self.peers
            .retain(|_, peer| now.duration_since(peer.seen) < FORGET_AFTER);
        self.pending.retain(|_, packets| {
            packets
                .back()
                .is_some_and(|at| now.duration_since(*at) < RATE_WINDOW)
        });
    }

    fn admit(&mut self, from: &str, now: Instant) -> bool {
        self.expire(now);
        let packets = if let Some(peer) = self.peers.get_mut(from) {
            &mut peer.packets
        } else {
            // Bound pre-authentication state without taking slots from known members.
            if self.peers.len() >= MAX_MEMBERS
                || self.pending.len() >= MAX_MEMBERS && !self.pending.contains_key(from)
            {
                return false;
            }
            self.pending.entry(from.into()).or_default()
        };
        packets.retain(|at| now.duration_since(*at) < RATE_WINDOW);
        if packets.len() == RATE_LIMIT {
            return false;
        }
        packets.push_back(now);
        true
    }

    fn hello(&mut self, from: String, did: String, hello: PoolHello, ts: i64, now: Instant) {
        if let Some(peer) = self.peers.get_mut(&from) {
            // A replay or delayed hello must not extend this peer's presence.
            if ts > peer.ts {
                peer.did = did;
                peer.hello = hello;
                peer.seen = now;
                peer.ts = ts;
            }
            return;
        }
        let packets = self.pending.remove(&from).unwrap_or_default();
        self.peers.insert(
            from,
            Peer {
                did,
                hello,
                seen: now,
                ts,
                packets,
            },
        );
    }

    fn snapshot(&mut self, now: Instant) -> Vec<MemberInfo> {
        self.expire(now);
        let mut members: Vec<_> = self
            .peers
            .iter()
            .map(|(node, peer)| {
                let age = now.duration_since(peer.seen);
                MemberInfo {
                    node: node.clone(),
                    did: peer.did.clone(),
                    online: age < OFFLINE_AFTER,
                    last_seen_secs: age.as_secs(),
                    hello: peer.hello.clone(),
                }
            })
            .collect();
        members.sort_unstable_by(|a, b| a.node.cmp(&b.node));
        members
    }
}

type RequestKey = (String, String);
type Reply = std::result::Result<Value, String>;

#[derive(Default)]
struct Pending(HashMap<RequestKey, oneshot::Sender<Reply>>);

impl Pending {
    fn register(&mut self, key: &RequestKey) -> Result<oneshot::Receiver<Reply>> {
        self.prune();
        ensure!(
            self.0.len() < MAX_PENDING,
            "too many pending VM pool requests"
        );
        ensure!(!self.0.contains_key(key), "VM pool request already pending");
        let (reply, wait) = oneshot::channel();
        self.0.insert(key.clone(), reply);
        Ok(wait)
    }

    fn complete(&mut self, key: RequestKey, reply: Reply) {
        if let Some(waiter) = self.0.remove(&key) {
            let _ = waiter.send(reply);
        }
    }

    fn prune(&mut self) {
        self.0.retain(|_, waiter| !waiter.is_closed());
    }
}

fn valid_create_id(id: &str) -> bool {
    id.split_at_checked(16)
        .is_some_and(|(a, b)| store::valid_server_id(a) && store::valid_server_id(b))
}

#[derive(Default)]
struct Runtime {
    pool: Option<PoolState>,
    joined: Option<Arc<net::Transport>>,
    local: Option<(Instant, MemberInfo)>,
    members: MemberTable,
    creates: Pending,
    joins: Pending,
    remote_creates: HashSet<String>,
    recent_creates: HashMap<RequestKey, Instant>,
    protect_backoff: HashMap<String, Instant>,
}

impl Runtime {
    fn set_pool(&mut self, pool: Option<PoolState>) {
        if self.pool != pool {
            self.pool = pool;
            self.joined = None;
            self.local = None;
            self.members = MemberTable::default();
            self.creates = Pending::default();
            self.joins = Pending::default();
            self.protect_backoff.clear();
        }
    }

    fn receive(&mut self, room: &str, from: &str, bytes: &[u8]) -> Option<(PoolState, Packet)> {
        let pool = self.pool.as_ref()?;
        if self
            .joined
            .as_ref()
            .is_none_or(|joined| joined.node_id == from)
            || pool.room != room
        {
            return None;
        }
        let now = Instant::now();
        if self.members.admit(from, now)
            && let Ok(packet) =
                Packet::verify(bytes, from, pool, chrono::Utc::now().timestamp_millis())
        {
            return self.dispatch(packet, now);
        }
        None
    }

    fn dispatch(&mut self, packet: Packet, now: Instant) -> Option<(PoolState, Packet)> {
        match packet.message {
            PoolMessage::Hello(hello) => {
                if hello.name.len() > MAX_NAME_CHARS * 4
                    || hello.name.chars().any(char::is_control)
                    || hello.accel.len() > 16
                    || !hello.accel.bytes().all(|b| b.is_ascii_alphanumeric())
                {
                    return None;
                }
                self.members
                    .hello(packet.from, packet.did, hello, packet.ts, now);
            }
            PoolMessage::Create { ref req, .. } if valid_create_id(req) => {
                return Some((self.pool.clone()?, packet));
            }
            PoolMessage::Group { ref server_id, .. } if store::valid_server_id(server_id) => {
                return Some((self.pool.clone()?, packet));
            }
            PoolMessage::Created { req, vm } if valid_create_id(&req) => {
                self.creates.complete((packet.from, req), Ok(vm));
            }
            PoolMessage::Joined { server_id } if store::valid_server_id(&server_id) => {
                self.joins
                    .complete((packet.from, server_id), Ok(Value::Null));
            }
            PoolMessage::Failed { req, error } => {
                if valid_create_id(&req) {
                    self.creates.complete((packet.from, req), Err(error));
                } else if store::valid_server_id(&req) {
                    self.joins.complete((packet.from, req), Err(error));
                }
            }
            _ => {}
        }
        None
    }
}

pub(super) fn pool() -> Option<PoolState> {
    RUNTIME.lock().unwrap().pool.clone()
}

/// Snapshot of remote members, including recently offline peers.
pub(super) fn members() -> Vec<MemberInfo> {
    RUNTIME.lock().unwrap().members.snapshot(Instant::now())
}

pub(super) fn election_members(room: &str) -> Vec<MemberInfo> {
    let mut runtime = RUNTIME.lock().unwrap();
    if runtime.pool.as_ref().is_none_or(|pool| pool.room != room)
        || runtime
            .joined
            .as_ref()
            .is_none_or(|joined| joined.room != room)
    {
        return Vec::new();
    }
    let now = Instant::now();
    let mut members = runtime.members.snapshot(now);
    if let Some((seen, local)) = &runtime.local
        && now.duration_since(*seen) < OFFLINE_AFTER
    {
        members.push(local.clone());
    }
    members
}

pub(super) async fn self_info(state: &AppState) -> Result<MemberInfo> {
    let identity = identity::current(state).await?;
    let config = state.config();
    let name = identity::load_profile()
        .ok()
        .and_then(|profile| profile.display_name)
        .or(config.identity.display_name)
        .unwrap_or_default()
        .chars()
        .filter(|ch| !ch.is_control())
        .take(MAX_NAME_CHARS)
        .collect();
    let cfg = config.vm;
    let accel = {
        let mut cached = ACCEL.lock().await;
        let now = Instant::now();
        if cached
            .as_ref()
            .is_none_or(|(checked, _)| now.duration_since(*checked) >= HOST_REFRESH)
        {
            // Reserve the refresh window before awaiting the subprocess probe.
            let (checked, accel) = cached.get_or_insert_with(|| (now, "tcg".into()));
            *checked = now;
            *accel = host::detect(&cfg)
                .await
                .accel
                .unwrap_or_else(|| "tcg".into());
        }
        cached.as_ref().unwrap().1.clone()
    };
    let root = store::vm_root()?;
    let resources = host::resources(&root);
    let used_slots = ha::slots(&root)?.try_into().unwrap_or(u32::MAX);
    let registry = super::REGISTRY.lock().await;
    Ok(MemberInfo {
        node: identity.node_id(),
        did: identity.did().into(),
        online: true,
        last_seen_secs: 0,
        hello: PoolHello {
            name,
            ha_capable: host::require_ha_support(std::env::consts::OS).is_ok(),
            accel,
            resources,
            max_vms: cfg.max_vms,
            max_memory_mb: cfg.max_memory_mb,
            max_cpus: cfg.max_cpus,
            max_disk_gb: cfg.max_disk_gb,
            used_slots: Some(used_slots),
            running_vms: registry.running.len() as u32,
            running_memory_mb: registry
                .running
                .values()
                .map(|vm| u64::from(vm.memory_mb))
                .sum(),
            running_cpus: registry.running.values().map(|vm| u64::from(vm.cpus)).sum(),
        },
    })
}

pub(super) fn score(member: &MemberInfo, memory_mb: u32, cpus: u32, disk_gb: u32) -> Option<f64> {
    let hello = &member.hello;
    let memory_mb = u64::from(memory_mb);
    let cpus = u64::from(cpus);
    if !member.online
        || !hello.ha_capable
        || matches!(hello.accel.as_str(), "" | "tcg")
        || hello.used_slots() >= hello.max_vms
        || hello.running_memory_mb.saturating_add(memory_mb) > u64::from(hello.max_memory_mb)
        || hello.running_cpus.saturating_add(cpus) > u64::from(hello.max_cpus)
        || memory_mb > hello.resources.mem_free_mb
        || disk_gb > hello.max_disk_gb
        || u64::from(disk_gb) > hello.resources.disk_free_gb
    {
        return None;
    }
    // max_disk_gb limits an individual virtual disk. The filesystem probe measures
    // remaining physical space, including space already used by running guests.
    let free_ram = hello.resources.mem_free_mb - memory_mb;
    let free_cpus = u64::from(hello.resources.cpus.min(hello.max_cpus))
        .saturating_sub(hello.running_cpus + cpus) as f64;
    Some(free_ram as f64 + free_cpus / (free_cpus + 1.0))
}

fn joined_pool(state: &AppState) -> Option<(PoolState, String)> {
    if !state.network.permitted() {
        return None;
    }
    let room = state.config().vm.room;
    let runtime = RUNTIME.lock().unwrap();
    let pool = runtime.pool.as_ref().filter(|pool| pool.room == room)?;
    let joined = runtime
        .joined
        .as_ref()
        .filter(|joined| joined.room == room)?;
    Some((pool.clone(), joined.node_id.clone()))
}

pub(super) fn require_current(expected: &PoolState, state: &AppState) -> Result<String> {
    joined_pool(state)
        .filter(|(pool, _)| pool == expected)
        .map(|(_, node)| node)
        .context("VM pool membership or room changed")
}

async fn send(
    state: &AppState,
    pool: &PoolState,
    to: Option<&str>,
    message: PoolMessage,
) -> Result<()> {
    state.network.require_online()?;
    let identity = identity::current(state).await?;
    let node = require_current(pool, state)?;
    ensure!(identity.node_id() == node, "VM pool identity changed");
    let bytes = Packet {
        mpool: 1,
        pool_id: pool.pool_id.clone(),
        from: node,
        did: identity.did().into(),
        ts: chrono::Utc::now().timestamp_millis(),
        message,
    }
    .encode(&pool.key)?;
    match to {
        Some(node) => net::send_direct(&pool.room, node, bytes).await,
        None => net::send_broadcast(&pool.room, bytes).await,
    }
}

async fn exchange(
    state: &AppState,
    pool: &PoolState,
    peer: &str,
    id: String,
    message: PoolMessage,
    timeout: Duration,
) -> Result<Value> {
    let is_create = valid_create_id(&id);
    let key = (peer.to_owned(), id);
    let pending: fn(&mut Runtime) -> &mut Pending = if is_create {
        |rt| &mut rt.creates
    } else {
        |rt| &mut rt.joins
    };
    let wait = {
        let mut runtime = RUNTIME.lock().unwrap();
        ensure!(
            runtime.pool.as_ref() == Some(pool),
            "VM pool membership changed"
        );
        pending(&mut runtime).register(&key)?
    };
    let result = wait_for_reply(wait, send(state, pool, Some(peer), message), timeout).await;
    pending(&mut RUNTIME.lock().unwrap()).prune();
    result
}

async fn wait_for_reply(
    wait: oneshot::Receiver<Reply>,
    send: impl Future<Output = Result<()>>,
    timeout: Duration,
) -> Result<Value> {
    tokio::time::timeout(timeout, async move {
        send.await?;
        wait.await
            .context("VM pool request cancelled")?
            .map_err(anyhow::Error::msg)
    })
    .await
    .context("timed out waiting for VM pool peer")?
}

fn best_host<'a>(
    local: &'a MemberInfo,
    peers: &'a [MemberInfo],
    size: VmSize,
) -> Option<&'a MemberInfo> {
    std::iter::once(local)
        .chain(peers)
        .filter_map(|member| {
            score(member, size.memory_mb, size.cpus, size.disk_gb).map(|value| (member, value))
        })
        .max_by(|(a, a_score), (b, b_score)| {
            a_score
                .total_cmp(b_score)
                .then_with(|| (a.node == local.node).cmp(&(b.node == local.node)))
                .then_with(|| b.node.cmp(&a.node))
        })
        .map(|(member, _)| member)
}

fn validate_create_payload(
    name: &Option<String>,
    ssh_keys: &[String],
    allow: &[String],
) -> Result<()> {
    ensure!(
        ssh_keys.len() <= MAX_CREATE_LIST && allow.len() <= MAX_CREATE_LIST,
        "remote VM create supports up to {MAX_CREATE_LIST} SSH keys and allowed DIDs each"
    );
    // Leave space for the packet envelope and the additional VM fields in Created.
    ensure!(
        serde_json::to_vec(&(name, ssh_keys, allow))?.len() <= 12 * 1024,
        "remote VM create arguments are too large"
    );
    Ok(())
}

pub(super) async fn place_create(mut args: CreateArgs, state: &Arc<AppState>) -> Result<Value> {
    let Some((pool, _)) = joined_pool(state) else {
        return super::create(args, None, None, state).await;
    };
    let size = args.size(&state.config().vm, false)?;
    let local = self_info(state).await?;
    let peers = members();
    let Some(host) = best_host(&local, &peers, size).filter(|host| host.node != local.node) else {
        return super::create(args, None, Some(&pool), state).await;
    };
    let key = super::ensure_client_key(&store::vm_root()?).await?;
    if !args.ssh_keys.contains(&key.public) {
        args.ssh_keys.push(key.public);
    }
    if !args.allow.contains(&local.did) {
        args.allow.push(local.did.clone());
    }
    validate_create_payload(&args.name, &args.ssh_keys, &args.allow)?;
    let req = format!("{}{}", store::new_server_id(), store::new_server_id());
    let vm = exchange(
        state,
        &pool,
        &host.node,
        req.clone(),
        PoolMessage::Create {
            req,
            name: args.name,
            size,
            ssh_keys: args.ssh_keys,
            allow: args.allow,
            autostart: args.autostart,
            start: args.start.unwrap_or(true),
        },
        CREATE_TIMEOUT,
    )
    .await
    .with_context(|| format!("creating VM on pool node {}", host.node))?;
    Ok(json!({"vm": vm, "node": host.node}))
}

struct RemoteCreate(String);

impl Drop for RemoteCreate {
    fn drop(&mut self) {
        RUNTIME.lock().unwrap().remote_creates.remove(&self.0);
    }
}

fn admit_create(peer: &str, req: &str) -> Result<Option<RemoteCreate>> {
    let mut runtime = RUNTIME.lock().unwrap();
    let now = Instant::now();
    runtime.recent_creates.retain(|_, until| *until > now);
    let key = (peer.to_owned(), req.to_owned());
    if runtime.recent_creates.contains_key(&key) {
        return Ok(None);
    }
    ensure!(
        runtime.remote_creates.len() < MAX_REMOTE_CREATES && !runtime.remote_creates.contains(peer),
        "pool host is busy with remote VM creation"
    );
    ensure!(
        runtime.recent_creates.len() < MAX_RECENT_CREATES,
        "pool host has reached its remote VM create rate limit"
    );
    // Cover the entire window in which a packet with a future timestamp can be replayed.
    runtime
        .recent_creates
        .insert(key, now + Duration::from_millis(2 * MAX_CLOCK_SKEW_MS));
    runtime.remote_creates.insert(peer.to_owned());
    Ok(Some(RemoteCreate(peer.to_owned())))
}

async fn respond(
    state: &AppState,
    pool: &PoolState,
    peer: &str,
    message: PoolMessage,
) -> Result<()> {
    let result = send(state, pool, Some(peer), message).await;
    if let Err(error) = &result {
        tracing::debug!(%peer, %error, "could not send VM pool response");
    }
    result
}

async fn fail(state: &AppState, pool: &PoolState, peer: &str, req: String, error: anyhow::Error) {
    let error = error.to_string().chars().take(512).collect();
    let _ = respond(state, pool, peer, PoolMessage::Failed { req, error }).await;
}

async fn inbound(state: Arc<AppState>, mut rx: mpsc::Receiver<(PoolState, Packet)>) {
    while let Some((pool, packet)) = rx.recv().await {
        let Ok(local) = require_current(&pool, &state) else {
            continue;
        };
        let Packet {
            from, did, message, ..
        } = packet;
        match message {
            PoolMessage::Create {
                req,
                name,
                size,
                ssh_keys,
                allow,
                autostart,
                start,
            } => {
                let active = validate_create_payload(&name, &ssh_keys, &allow)
                    .and_then(|()| admit_create(&from, &req));
                let active = match active {
                    Ok(Some(active)) => active,
                    Ok(None) => continue,
                    Err(error) => {
                        fail(&state, &pool, &from, req, error).await;
                        continue;
                    }
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let _active = active;
                    let args = CreateArgs {
                        name,
                        memory_mb: Some(size.memory_mb),
                        cpus: Some(size.cpus),
                        disk_gb: Some(size.disk_gb),
                        ssh_keys,
                        allow,
                        autostart,
                        start: Some(start),
                    };
                    match super::create(args, Some(did), Some(&pool), &state).await {
                        Ok(result) => {
                            let reply = PoolMessage::Created {
                                req: req.clone(),
                                vm: result["vm"].clone(),
                            };
                            if let Err(error) = respond(&state, &pool, &from, reply).await {
                                let id = result["vm"]["server_id"].as_str().unwrap_or("?");
                                fail(
                                    &state,
                                    &pool,
                                    &from,
                                    req,
                                    error.context(format!(
                                        "VM {id} was created but its status could not be sent"
                                    )),
                                )
                                .await;
                            }
                        }
                        Err(error) => fail(&state, &pool, &from, req, error).await,
                    }
                });
            }
            PoolMessage::Group { server_id, sealed } => {
                let Ok(permit) = INBOUND_JOINS.try_acquire() else {
                    fail(
                        &state,
                        &pool,
                        &from,
                        server_id,
                        anyhow::anyhow!("pool host is busy joining VM groups"),
                    )
                    .await;
                    continue;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let result = join_group(&state, &pool, &from, local, &server_id, &sealed).await;
                    match result {
                        Ok(()) => {
                            let _ =
                                respond(&state, &pool, &from, PoolMessage::Joined { server_id })
                                    .await;
                        }
                        Err(error) => fail(&state, &pool, &from, server_id, error).await,
                    }
                });
            }
            _ => {}
        }
    }
}

const NONCE_BYTES: usize = 12;

fn group_cipher(pool: &PoolState) -> Aes256Gcm {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&pool.key).expect("fixed-size HMAC key");
    mac.update(b"mistl-vm-pool/group-token/v1");
    Aes256Gcm::new(&mac.finalize().into_bytes())
}

fn seal_group(pool: &PoolState, id: &str, recipient: &str, token: &str) -> Result<String> {
    let aad = format!("{}|{id}|{recipient}", pool.pool_id);
    let mut nonce = [0; NONCE_BYTES];
    OsRng.fill_bytes(&mut nonce);
    let encrypted = group_cipher(pool)
        .encrypt(
            &Nonce::from(nonce),
            Payload {
                msg: token.as_bytes(),
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| anyhow::anyhow!("could not seal VM group token"))?;
    let mut sealed = nonce.to_vec();
    sealed.extend(encrypted);
    Ok(URL_SAFE_NO_PAD.encode(sealed))
}

fn open_group(pool: &PoolState, id: &str, recipient: &str, sealed: &str) -> Result<String> {
    let aad = format!("{}|{id}|{recipient}", pool.pool_id);
    let bytes = URL_SAFE_NO_PAD
        .decode(sealed)
        .ok()
        .context("invalid sealed VM group token")?;
    let (nonce, cipher) = bytes
        .split_at_checked(NONCE_BYTES)
        .context("invalid VM group nonce")?;
    let nonce: [u8; NONCE_BYTES] = nonce.try_into().expect("fixed-size nonce");
    let token = group_cipher(pool)
        .decrypt(
            &Nonce::from(nonce),
            Payload {
                msg: cipher,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| anyhow::anyhow!("could not open VM group token"))?;
    String::from_utf8(token)
        .ok()
        .context("invalid VM group token")
}

async fn join_group(
    state: &Arc<AppState>,
    pool: &PoolState,
    sender: &str,
    local: String,
    id: &str,
    sealed: &str,
) -> Result<()> {
    let secret = open_group(pool, id, &local, sealed)?;
    let (token, key) = ha::Token::decode(&secret)?;
    token.verify_holder(id, sender)?;
    let _apply = state.config_apply.lock().await;
    // Serialize with CLI joins for the group limit. Create holds the lock only
    // while publishing its record.
    let _create = super::CREATE_LOCK.lock().await;
    ensure!(
        require_current(pool, state)? == local,
        "VM pool identity changed"
    );
    let cfg = ha::group_config(state)?;
    ha::join(store::vm_root()?, &cfg, local, token, key, state).await?;
    Ok(())
}

fn protection_members(
    local: &str,
    id: &str,
    size: VmSize,
    peers: &[MemberInfo],
) -> Vec<ha::Member> {
    const GIB: u64 = 1 << 30;
    let mut online: Vec<_> = peers
        .iter()
        .filter(|p| p.online && p.hello.ha_capable && p.node != local)
        .collect();
    online.sort_unstable_by(|a, b| a.node.cmp(&b.node));
    let candidates: Vec<_> = online
        .iter()
        .filter(|peer| {
            let hello = &peer.hello;
            !matches!(hello.accel.as_str(), "" | "tcg")
                && hello.max_disk_gb >= size.disk_gb
                && hello.max_memory_mb >= size.memory_mb
                && hello.max_cpus >= size.cpus
        })
        .map(|peer| placement::Candidate {
            did: peer.node.clone(),
            available: peer.hello.resources.disk_free_gb.saturating_mul(GIB),
        })
        .collect();
    let hosts = placement::place(
        id,
        u64::from(size.disk_gb) * GIB,
        3,
        Some(local),
        &candidates,
    );
    if hosts.len() == 1 {
        return Vec::new();
    }
    online.retain(|peer| !hosts.contains(&peer.node));
    let voters = hosts.len() + online.len();
    let target = if voters >= 5 {
        5
    } else if voters >= 3 {
        3
    } else {
        2
    };
    let witnesses = target - hosts.len();
    hosts
        .into_iter()
        .map(|node| ha::Member {
            node,
            role: ha::Role::Host,
        })
        .chain(online.into_iter().take(witnesses).map(|peer| ha::Member {
            node: peer.node.clone(),
            role: ha::Role::Witness,
        }))
        .collect()
}

#[derive(Serialize)]
struct JoinResult {
    node: String,
    role: ha::Role,
    joined: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub(super) async fn protect(id: &str, state: &Arc<AppState>) -> Result<Value> {
    let (pool, _) = joined_pool(state).context("VM pool room is not joined")?;
    let members = protect_in_pool(id, &pool, false, None, state)
        .await?
        .context("no other online VM pool member can host this VM")?
        .await?;
    Ok(json!({"server_id": id, "members": members}))
}

async fn protect_in_pool(
    id: &str,
    pool: &PoolState,
    automatic: bool,
    initial_start: Option<oneshot::Receiver<()>>,
    state: &Arc<AppState>,
) -> Result<Option<tokio::task::JoinHandle<Vec<JoinResult>>>> {
    let (group, token, voters, permit) = {
        let _start = if automatic {
            Some(super::lock_vm_start(id).await?)
        } else {
            None
        };
        let _apply = state.config_apply.lock().await;
        let _create = super::CREATE_LOCK.lock().await;
        let local = require_current(pool, state)?;
        let root = store::vm_root()?;
        if ha::group(&root, id)?.is_some() {
            ensure!(automatic, "VM group already exists");
            return Ok(None);
        }
        // A standalone process has no HA lease. Enabling HA would fence it.
        if automatic
            && (store::vm_dir(&root, id).join("ha.disabled").try_exists()?
                || super::REGISTRY.lock().await.running.contains_key(id))
        {
            return Ok(None);
        }
        let record = store::load(&root, id)?;
        let size = VmSize::from(&record);
        let voters = protection_members(&local, id, size, &members());
        if voters.is_empty() {
            return Ok(None);
        }
        let permit = PROTECTIONS
            .try_acquire()
            .context("too many VM protections in progress")?;
        let cfg = ha::group_config(state)?;
        let (group, token) =
            ha::enable(root, id, cfg.room, local, voters.clone(), None, state).await?;
        (group, token, voters, permit)
    };
    let pool = pool.clone();
    let state = state.clone();
    Ok(Some(tokio::spawn(async move {
        let results = join_voters(&group.id, &pool, &token, voters, &state).await;
        if automatic {
            let joined = results.iter().filter(|member| member.joined).count();
            tracing::info!(vm = %group.id, joined, voters = results.len(), "configured automatic VM pool protection");
            if joined != results.len() {
                delay_auto_protect(&group.id, &pool);
            }
        }
        tokio::spawn(async move {
            let _permit = permit;
            // Let create attempt the initial start before taking the group operation lock.
            if let Some(started) = initial_start {
                let _ = started.await;
            }
            match wait_for_lease(&group).await {
                Ok(()) => {
                    if let Err(error) = group.checkpoint().await {
                        tracing::warn!(vm = %group.id, %error, "initial VM pool checkpoint failed");
                    }
                }
                Err(error) => {
                    tracing::warn!(vm = %group.id, %error, "initial VM pool checkpoint skipped")
                }
            }
        });
        results
    })))
}

async fn join_voters(
    id: &str,
    pool: &PoolState,
    token: &str,
    mut voters: Vec<ha::Member>,
    state: &AppState,
) -> Vec<JoinResult> {
    let local = voters.remove(0);
    let mut results = vec![JoinResult {
        node: local.node,
        role: local.role,
        joined: true,
        error: None,
    }];
    results.extend(
        futures_util::future::join_all(voters.into_iter().map(|member| async move {
            let result = async {
                let sealed = seal_group(pool, id, &member.node, token)?;
                exchange(
                    state,
                    pool,
                    &member.node,
                    id.to_owned(),
                    PoolMessage::Group {
                        server_id: id.to_owned(),
                        sealed,
                    },
                    JOIN_TIMEOUT,
                )
                .await
            }
            .await;
            JoinResult {
                node: member.node,
                role: member.role,
                joined: result.is_ok(),
                error: result.err().map(|error| error.to_string()),
            }
        }))
        .await,
    );
    results
}

pub(super) async fn wait_for_lease(group: &ha::Group) -> Result<()> {
    // Joining persists the voter configuration before the first quorum lease.
    tokio::time::timeout(JOIN_TIMEOUT, async {
        while !group.writable() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .context("timed out waiting for a VM quorum lease")
}

pub(super) async fn auto_protect(
    id: &str,
    pool: &PoolState,
    initial_start: Option<oneshot::Receiver<()>>,
    state: &Arc<AppState>,
) {
    if host::require_ha_support(std::env::consts::OS).is_err() {
        return;
    }
    let wait_for_joins = initial_start.is_none();
    let result = match protect_in_pool(id, pool, true, initial_start, state).await {
        Ok(Some(joins)) if wait_for_joins => joins.await.map(|_| ()).map_err(Into::into),
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        tracing::warn!(vm = %id, %error, "automatic VM pool protection failed");
        delay_auto_protect(id, pool);
    }
}

fn delay_auto_protect(id: &str, pool: &PoolState) {
    let now = Instant::now();
    let mut runtime = RUNTIME.lock().unwrap();
    if runtime.pool.as_ref() != Some(pool) {
        return;
    }
    let backoff = &mut runtime.protect_backoff;
    backoff.retain(|_, until| *until > now);
    if backoff.len() >= MAX_PROTECT_BACKOFFS
        && let Some(oldest) = backoff
            .iter()
            .min_by_key(|(_, until)| *until)
            .map(|(id, _)| id.clone())
    {
        backoff.remove(&oldest);
    }
    backoff.insert(id.to_owned(), now + PROTECT_BACKOFF);
}

async fn protect_periodically(state: Arc<AppState>) {
    let mut tick = tokio::time::interval_at(
        tokio::time::Instant::now() + PROTECT_INTERVAL,
        PROTECT_INTERVAL,
    );
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let Some((pool, _)) = joined_pool(&state) else {
            continue;
        };
        let Ok(local) = identity::current(&state).await else {
            continue;
        };
        let peers = members();
        if !peers.iter().any(|peer| peer.online) {
            continue;
        }
        let records = store::vm_root().and_then(|root| store::load_all(&root));
        let records = match records {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(%error, "could not list VMs for pool protection");
                continue;
            }
        };
        for record in records {
            let id = &record.server_id;
            if record.owner_did != local.did()
                && !peers.iter().any(|peer| peer.did == record.owner_did)
            {
                continue;
            }
            if RUNTIME
                .lock()
                .unwrap()
                .protect_backoff
                .get(id)
                .is_some_and(|until| *until > Instant::now())
            {
                continue;
            }
            auto_protect(id, &pool, None, &state).await;
        }
    }
}

pub(super) fn spawn_background(state: Arc<AppState>) {
    match store::vm_root().and_then(|root| PoolState::load(&root.join(STATE_FILE))) {
        Ok(pool) => RUNTIME.lock().unwrap().set_pool(pool),
        Err(error) => tracing::warn!(%error, "could not load VM pool state"),
    }
    let (tx, rx) = mpsc::channel(MAX_MEMBERS);
    net::register_room_handler({
        let state = state.clone();
        move |event, room, from, bytes| {
            if event == net::EVENT_RAW
                && bytes.len() <= wire::CONTROL_LIMIT
                && bytes.starts_with(b"{")
                && bytes.windows(7).any(|w| w == b"\"mpool\"")
                && store::valid_server_id(from)
                && state.network.permitted()
                && let Some(request) = RUNTIME.lock().unwrap().receive(room, from, bytes)
            {
                let _ = tx.try_send(request);
            }
        }
    });
    tokio::spawn(inbound(state.clone(), rx));
    tokio::spawn(protect_periodically(state.clone()));
    tokio::spawn(presence(state));
}

async fn presence(state: Arc<AppState>) {
    let mut held: Option<Arc<net::Transport>> = None;
    let mut next_hello = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let wanted =
            pool().filter(|pool| state.network.permitted() && state.config().vm.room == pool.room);
        let room = wanted.as_ref().map(|pool| pool.room.as_str());
        if let Some(transport) = &held
            && room != Some(transport.room.as_str())
        {
            {
                let mut runtime = RUNTIME.lock().unwrap();
                runtime.joined = None;
                runtime.local = None;
            }
            if net::leave_room(net::RoomUse::Vm, &transport.room)
                .await
                .is_err()
            {
                continue;
            }
            held = None;
        }
        let Some(room) = room else { continue };
        if held.is_none() {
            held = net::ensure_started(&state, net::RoomUse::Vm, room.into())
                .await
                .ok();
        }
        let Some(transport) = &held else { continue };
        let now = Instant::now();
        {
            let mut runtime = RUNTIME.lock().unwrap();
            if runtime.pool != wanted || !state.network.permitted() {
                continue;
            }
            if runtime.joined.is_none() {
                runtime.joined = Some(transport.clone());
                next_hello = now;
            }
            runtime.members.expire(now);
            runtime.creates.prune();
            runtime.joins.prune();
        }
        if now >= next_hello {
            next_hello = now + HELLO_INTERVAL;
            let result = async {
                let info = self_info(&state).await?;
                {
                    let mut runtime = RUNTIME.lock().unwrap();
                    if runtime.pool == wanted && info.node == transport.node_id {
                        runtime.local = Some((Instant::now(), info.clone()));
                    }
                }
                send(
                    &state,
                    wanted.as_ref().unwrap(),
                    None,
                    PoolMessage::Hello(info.hello),
                )
                .await
            }
            .await;
            if let Err(error) = result {
                tracing::debug!(%error, "could not announce VM pool presence");
            }
        }
    }
}

fn remove_state(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("removing VM pool state"),
    }
}

fn member_json(info: &MemberInfo, is_self: bool, cfg: &crate::config::VmConfig) -> Value {
    let hello = &info.hello;
    json!({
        "node": info.node, "did": info.did, "name": hello.name, "online": info.online,
        "last_seen_secs": info.last_seen_secs, "self": is_self, "ha_capable": hello.ha_capable,
        "accel": hello.accel, "cpus": hello.resources.cpus, "mem_total_mb": hello.resources.mem_total_mb,
        "mem_free_mb": hello.resources.mem_free_mb, "disk_free_gb": hello.resources.disk_free_gb,
        "running_vms": hello.running_vms, "used_slots": hello.used_slots(), "max_vms": hello.max_vms,
        "score": score(info, cfg.default_memory_mb, cfg.default_cpus, cfg.default_disk_gb),
    })
}

async fn status(state: &AppState) -> Result<Value> {
    let (pool, peers) = {
        let mut runtime = RUNTIME.lock().unwrap();
        let Some(pool) = runtime.pool.clone() else {
            return Ok(json!({"joined":false,"members":[]}));
        };
        (pool, runtime.members.snapshot(Instant::now()))
    };
    let local = self_info(state).await?;
    let cfg = state.config().vm;
    let mut rows = vec![member_json(&local, true, &cfg)];
    rows.extend(peers.iter().map(|info| member_json(info, false, &cfg)));
    Ok(json!({"joined":true,"pool_id":pool.pool_id,"room":pool.room,"members":rows}))
}

pub(super) async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "vm.pool.status" => return status(state).await,
        "vm.pool.invite" => {
            let pool = pool().context("this node is not in a VM pool")?;
            return Ok(json!({"invite":pool.invite()?}));
        }
        _ => {}
    }

    // Serialize room and membership changes with config.set and VM HA enable/join.
    let _apply = state.config_apply.lock().await;
    let _create = super::CREATE_LOCK.lock().await;
    let root = store::vm_root()?;
    let path = root.join(STATE_FILE);
    if cmd == "vm.pool.leave" {
        ensure!(
            !ha::has_active_groups(&root, None)?,
            "cannot leave the VM pool while this node has active VM HA groups"
        );
        remove_state(&path)?;
        RUNTIME.lock().unwrap().set_pool(None);
        return Ok(json!({"left":true}));
    }

    let current = PoolState::load(&path)?;
    let requested = match cmd {
        "vm.pool.create" => {
            ensure!(current.is_none(), "this node is already in a VM pool");
            PoolState::new()
        }
        "vm.pool.join" => {
            let requested =
                PoolState::from_invite(args["code"].as_str().context("missing VM pool invite")?)?;
            if let Some(current) = &current {
                ensure!(
                    current.same_pool(&requested),
                    "this node is already in a different VM pool"
                );
            }
            requested
        }
        _ => bail!("unknown VM pool command: {cmd}"),
    };
    let old_room = state.config().vm.room;
    let change_room = old_room != requested.room;
    if change_room {
        ensure!(
            !ha::has_active_groups(&root, Some(&old_room))?,
            "cannot change [vm] room while active VM HA groups use it"
        );
        state.set_config_path("vm.room", json!(requested.room))?;
    }
    // Publish membership after committing the room and saving the pool key.
    let pool = match current {
        Some(current) => current,
        None => {
            if let Err(error) = requested.save(&path) {
                // An atomic write can report a directory sync error after replacement.
                remove_state(&path).with_context(|| {
                    format!("{error:#}; clearing the partially saved VM pool failed")
                })?;
                if change_room {
                    state
                        .set_config_path("vm.room", json!(old_room))
                        .with_context(|| {
                            format!("{error:#}; restoring the previous VM room failed")
                        })?;
                }
                return Err(error);
            }
            requested
        }
    };
    let mut result = json!({"pool_id":pool.pool_id,"room":pool.room});
    if cmd == "vm.pool.create" {
        result["invite"] = json!(pool.invite()?);
    }
    let room = pool.room.clone();
    RUNTIME.lock().unwrap().set_pool(Some(pool));
    match crate::tunnel::set_room_if_unset(state, &room).await {
        Ok(configured) if configured != room => {
            result["tunnel_room_hint"] = json!(format!(
                "The tunnel room is {configured:?}. For SSH, pool nodes need the same tunnel room. To use the pool room, run `mistl tunnel room {room}`."
            ));
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, "could not set the default tunnel room for the VM pool");
            result["tunnel_room_hint"] = json!(format!(
                "Could not set the default tunnel room. To use the pool room, run `mistl tunnel room {room}`."
            ));
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn did() -> String {
        let mut bytes = vec![0xed, 0x01];
        bytes.extend([4; 32]);
        format!("did:key:z{}", bs58::encode(bytes).into_string())
    }

    fn packet(pool: &PoolState, ts: i64) -> Packet {
        let did = did();
        Packet {
            mpool: 1,
            pool_id: pool.pool_id.clone(),
            from: identity::node_id_for_did(&did),
            did,
            ts,
            message: PoolMessage::Hello(PoolHello::default()),
        }
    }

    #[test]
    fn invite_and_saved_membership_round_trip() {
        let pool = PoolState::new();
        let code = pool.invite().unwrap();
        assert!(code.starts_with(INVITE_PREFIX));
        assert!(pool.same_pool(&PoolState::from_invite(&code).unwrap()));

        let dir = std::env::temp_dir().join(format!("mistl-vm-pool-{}", store::new_server_id()));
        fs::create_dir(&dir).unwrap();
        let path = dir.join(STATE_FILE);
        assert!(PoolState::load(&path).unwrap().is_none());
        pool.save(&path).unwrap();
        assert!(
            PoolState::load(&path)
                .unwrap()
                .is_some_and(|saved| saved == pool)
        );
        fs::remove_file(path).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn rejects_invalid_invites() {
        let pool = PoolState::new();
        let code = pool.invite().unwrap();
        assert!(PoolState::from_invite(code.strip_prefix(INVITE_PREFIX).unwrap()).is_err());
        assert!(
            PoolState::from_invite(&format!("{INVITE_PREFIX}{}", "A".repeat(INVITE_LIMIT)))
                .is_err()
        );

        for (field, value) in [
            ("key", json!(URL_SAFE_NO_PAD.encode([0; 31]))),
            ("room", json!("invalid room")),
            ("pool_id", json!("not hexadecimal")),
            ("v", json!(2)),
            ("extra", json!(true)),
        ] {
            let bytes = URL_SAFE_NO_PAD
                .decode(code.strip_prefix(INVITE_PREFIX).unwrap())
                .unwrap();
            let mut body: Value = serde_json::from_slice(&bytes).unwrap();
            body[field] = value;
            let code = format!(
                "{INVITE_PREFIX}{}",
                URL_SAFE_NO_PAD.encode(serde_json::to_vec(&body).unwrap())
            );
            assert!(PoolState::from_invite(&code).is_err(), "{field}");
        }
    }

    #[test]
    fn authenticates_pool_packet_and_binds_identity() {
        let pool = PoolState::new();
        let now = 300_000;
        let mut msg = packet(&pool, now);
        let sender = msg.from.clone();
        let bytes = msg.encode(&pool.key).unwrap();
        assert!(Packet::verify(&bytes, &sender, &pool, now).is_ok());
        assert!(Packet::verify(&bytes, &sender, &pool, now + 120_000).is_ok());
        assert!(Packet::verify(&bytes, &sender, &pool, now + 120_001).is_err());
        assert!(Packet::verify(&bytes, &sender, &pool, now - 120_001).is_err());
        assert!(Packet::verify(&bytes, "0000000000000000", &pool, now).is_err());
        let body: Body = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["mpool"], 1);
        assert_eq!(body["type"], "hello");
        assert!(body.contains_key("cpus"));
        for tag in ["kind", "t", "v", "mvm"] {
            assert!(!body.contains_key(tag));
        }

        let mut wrong_pool = pool.clone();
        wrong_pool.key[0] ^= 1;
        assert!(Packet::verify(&bytes, &sender, &wrong_pool, now).is_err());
        wrong_pool.key = pool.key;
        wrong_pool.pool_id.push('0');
        assert!(Packet::verify(&bytes, &sender, &wrong_pool, now).is_err());
        msg.from = "0000000000000000".into();
        let bytes = msg.encode(&pool.key).unwrap();
        assert!(Packet::verify(&bytes, &msg.from, &pool, now).is_err());
    }

    #[test]
    fn receive_checks_room_and_ignores_self() {
        let pool = PoolState::new();
        let msg = packet(&pool, chrono::Utc::now().timestamp_millis());
        let bytes = msg.encode(&pool.key).unwrap();
        let mut runtime = Runtime {
            pool: Some(pool.clone()),
            joined: Some(Arc::new(net::Transport {
                node_id: msg.from.clone(),
                room: pool.room.clone(),
            })),
            ..Runtime::default()
        };
        runtime.receive(&pool.room, &msg.from, &bytes);
        assert!(runtime.members.peers.is_empty());
        Arc::get_mut(runtime.joined.as_mut().unwrap())
            .unwrap()
            .node_id = "0000000000000000".into();
        runtime.receive("other-room", &msg.from, &bytes);
        assert!(runtime.members.peers.is_empty());
        runtime.receive(&pool.room, &msg.from, &bytes);
        assert_eq!(runtime.members.peers.len(), 1);
    }

    #[test]
    fn members_are_capped_and_expire() {
        let now = Instant::now();
        let mut table = MemberTable::default();
        for i in 0..MAX_MEMBERS {
            let node = format!("{i:016x}");
            assert!(table.admit(&node, now));
            table.hello(node, did(), PoolHello::default(), 1, now);
        }
        assert!(!table.admit("new", now));
        assert!(
            table
                .snapshot(now + OFFLINE_AFTER - Duration::from_secs(1))
                .iter()
                .all(|member| member.online)
        );
        let offline = table.snapshot(now + OFFLINE_AFTER);
        assert_eq!(offline.len(), MAX_MEMBERS);
        assert!(
            offline
                .iter()
                .all(|member| !member.online && member.last_seen_secs == 60)
        );
        assert!(table.snapshot(now + FORGET_AFTER).is_empty());
        assert!(table.admit("new", now + FORGET_AFTER));
    }

    #[test]
    fn members_have_a_rolling_rate_limit_and_replays_do_not_keep_them_online() {
        let now = Instant::now();
        let mut table = MemberTable::default();
        assert!(table.admit("peer", now));
        table.hello("peer".into(), did(), PoolHello::default(), 1, now);
        for _ in 1..RATE_LIMIT {
            assert!(table.admit("peer", now + RATE_WINDOW - Duration::from_secs(1)));
        }
        assert!(!table.admit("peer", now + RATE_WINDOW - Duration::from_secs(1)));
        assert!(table.admit("peer", now + RATE_WINDOW));
        table.hello(
            "peer".into(),
            did(),
            PoolHello::default(),
            1,
            now + RATE_WINDOW,
        );
        assert!(!table.snapshot(now + OFFLINE_AFTER)[0].online);
    }

    #[test]
    fn unknown_senders_have_bounded_rate_limit_state() {
        let now = Instant::now();
        let mut table = MemberTable::default();
        for _ in 0..RATE_LIMIT {
            assert!(table.admit("unverified", now));
        }
        assert!(!table.admit("unverified", now));
        for i in 1..MAX_MEMBERS {
            assert!(table.admit(&format!("{i:016x}"), now));
        }
        assert!(!table.admit("new", now));
        assert!(table.admit("new", now + RATE_WINDOW));
        assert_eq!(table.pending.len(), 1);
        assert!(table.peers.is_empty());
    }

    fn candidate() -> MemberInfo {
        MemberInfo {
            node: "0000000000000000".into(),
            did: did(),
            online: true,
            last_seen_secs: 0,
            hello: PoolHello {
                ha_capable: true,
                accel: "kvm".into(),
                resources: host::Resources {
                    cpus: 8,
                    mem_total_mb: 8192,
                    mem_free_mb: 4096,
                    disk_free_gb: 30,
                },
                max_vms: 2,
                max_memory_mb: 4096,
                max_cpus: 4,
                max_disk_gb: 20,
                running_vms: 1,
                running_memory_mb: 2048,
                running_cpus: 1,
                ..PoolHello::default()
            },
        }
    }

    #[test]
    fn score_requires_suitable_host_and_available_capacity() {
        let member = candidate();
        assert!(score(&member, 2048, 3, 20).is_some());
        assert!(score(&member, 2049, 3, 20).is_none());
        assert!(score(&member, 2048, 4, 20).is_none());
        assert!(score(&member, 2048, 3, 21).is_none());
        let score_default = |member: &MemberInfo| score(member, 2048, 2, 20);
        let disqualify: [fn(&mut MemberInfo); 7] = [
            |member| member.online = false,
            |member| member.hello.ha_capable = false,
            |member| member.hello.accel = "tcg".into(),
            |member| member.hello.accel.clear(),
            |member| member.hello.used_slots = Some(2),
            |member| member.hello.resources.mem_free_mb = 2047,
            |member| member.hello.resources.disk_free_gb = 19,
        ];
        for change in disqualify {
            let mut member = member.clone();
            change(&mut member);
            assert!(score_default(&member).is_none());
        }
    }

    #[test]
    fn legacy_hello_uses_running_vms_when_slots_are_absent() {
        let mut member = candidate();
        member.hello.running_vms = member.hello.max_vms;
        let legacy = serde_json::to_value(&member.hello).unwrap();
        assert!(legacy.get("used_slots").is_none());
        member.hello = serde_json::from_value(legacy).unwrap();
        assert!(score(&member, 2048, 2, 20).is_none());

        member.hello.used_slots = Some(0);
        assert!(score(&member, 2048, 2, 20).is_some());
    }

    #[test]
    fn score_prefers_free_ram_then_cpus() {
        let mut member = candidate();
        let score_now = |member: &MemberInfo| score(member, 2048, 2, 20).unwrap();
        let baseline = score_now(&member);
        member.hello.running_cpus = 0;
        let more_cpus = score_now(&member);
        assert!(more_cpus > baseline);
        member.hello.running_cpus = 2;
        member.hello.resources.mem_free_mb += 1;
        assert!(score_now(&member) > more_cpus);
    }

    const SIZE: VmSize = VmSize {
        memory_mb: 2048,
        cpus: 2,
        disk_gb: 20,
    };

    fn peer(i: usize) -> MemberInfo {
        MemberInfo {
            node: format!("{i:016x}"),
            ..candidate()
        }
    }

    #[test]
    fn placement_uses_score_then_prefers_self_and_lowest_node_id() {
        let mut local = peer(7);
        let mut peers = [peer(2), peer(1)];
        assert_eq!(best_host(&local, &peers, SIZE).unwrap().node, local.node);
        peers[0].hello.resources.mem_free_mb += 1;
        assert_eq!(best_host(&local, &peers, SIZE).unwrap().node, peers[0].node);
        peers[1].hello.resources.mem_free_mb += 1;
        assert_eq!(best_host(&local, &peers, SIZE).unwrap().node, peers[1].node);
        for member in std::iter::once(&mut local).chain(&mut peers) {
            member.hello.resources.disk_free_gb = 19;
        }
        assert!(best_host(&local, &peers, SIZE).is_none());
    }

    #[test]
    fn protection_selects_up_to_three_hosts_and_an_odd_voter_count() {
        let local = peer(0).node;
        for (online, hosts, witnesses) in [(1, 0, 0), (2, 2, 0), (3, 3, 0), (5, 3, 2), (8, 3, 2)] {
            let peers: Vec<_> = (1..online).rev().map(peer).collect();
            let voters = protection_members(&local, &local, SIZE, &peers);
            assert_eq!(voters.len(), hosts + witnesses);
            assert_eq!(
                voters
                    .iter()
                    .filter(|member| member.role == ha::Role::Host)
                    .count(),
                hosts
            );
            if hosts > 0 {
                assert_eq!(voters[0].node, local);
                assert_eq!(
                    voters.iter().map(|v| &v.node).collect::<HashSet<_>>().len(),
                    voters.len()
                );
            }
        }
    }

    #[test]
    fn protection_uses_only_online_ha_capable_peers_as_witnesses() {
        let local = peer(0).node;
        let mut peers: Vec<_> = (1..5).map(peer).collect();
        peers[0].online = false;
        peers[1].hello.ha_capable = false;
        peers[2].hello.accel = "tcg".into();
        let voters = protection_members(&local, &local, SIZE, &peers);
        assert_eq!(voters.len(), 3);
        assert_eq!(
            (voters[1].node.as_str(), voters[1].role),
            (peers[3].node.as_str(), ha::Role::Host)
        );
        assert_eq!(
            (voters[2].node.as_str(), voters[2].role),
            (peers[2].node.as_str(), ha::Role::Witness)
        );
        peers[3].hello.resources.disk_free_gb = 19;
        assert!(protection_members(&local, &local, SIZE, &peers).is_empty());
    }

    #[test]
    fn group_seal_binds_pool_server_and_recipient() {
        let pool = PoolState::new();
        let id = "0123456789abcdef";
        let recipient = "abcdef0123456789";
        let token = "mistl-vm1:example";
        let sealed = seal_group(&pool, id, recipient, token).unwrap();
        assert_ne!(sealed, seal_group(&pool, id, recipient, token).unwrap());
        assert_eq!(open_group(&pool, id, recipient, &sealed).unwrap(), token);
        assert!(open_group(&pool, recipient, recipient, &sealed).is_err());
        assert!(open_group(&pool, id, id, &sealed).is_err());
        let mut wrong_pool = pool.clone();
        wrong_pool.pool_id = store::new_server_id();
        assert!(open_group(&wrong_pool, id, recipient, &sealed).is_err());
        wrong_pool.pool_id = pool.pool_id.clone();
        wrong_pool.key[0] ^= 1;
        assert!(open_group(&wrong_pool, id, recipient, &sealed).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn pending_requests_are_bounded_and_timed_out_waiters_are_pruned() {
        let mut pending = Pending::default();
        let mut waiters = Vec::new();
        for i in 0..MAX_PENDING {
            waiters.push(pending.register(&("peer".into(), i.to_string())).unwrap());
        }
        let overflow = ("peer".into(), MAX_PENDING.to_string());
        assert!(pending.register(&overflow).is_err());
        let error = wait_for_reply(waiters.pop().unwrap(), async { Ok(()) }, CREATE_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "timed out waiting for VM pool peer");
        pending.prune();
        assert_eq!(pending.0.len(), MAX_PENDING - 1);
        assert!(pending.register(&overflow).is_ok());
    }

    #[test]
    fn replies_only_complete_requests_from_the_expected_peer_and_message_type() {
        let pool = PoolState::new();
        let now = Instant::now();
        let peer = identity::node_id_for_did(&did());
        let req = "01".repeat(16);
        let server_id = "ab".repeat(8);
        let mut runtime = Runtime::default();
        let mut create = runtime
            .creates
            .register(&(peer.clone(), req.clone()))
            .unwrap();
        let mut join = runtime
            .joins
            .register(&(peer.clone(), server_id.clone()))
            .unwrap();

        let mut reply = packet(&pool, 0);
        reply.from = "cd".repeat(8);
        reply.message = PoolMessage::Created {
            req: req.clone(),
            vm: json!({"server_id": server_id}),
        };
        runtime.dispatch(reply, now);
        assert!(matches!(
            create.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let mut reply = packet(&pool, 0);
        reply.message = PoolMessage::Created {
            req: server_id.clone(),
            vm: json!({}),
        };
        runtime.dispatch(reply, now);
        assert!(matches!(
            join.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let mut reply = packet(&pool, 0);
        reply.message = PoolMessage::Joined { server_id };
        runtime.dispatch(reply, now);
        assert_eq!(join.try_recv().unwrap().unwrap(), Value::Null);
        let mut reply = packet(&pool, 0);
        reply.message = PoolMessage::Failed {
            req,
            error: "host busy".into(),
        };
        runtime.dispatch(reply, now);
        assert_eq!(create.try_recv().unwrap().unwrap_err(), "host busy");
    }

    #[test]
    fn create_packet_carries_resolved_size_and_only_the_envelope_did() {
        let pool = PoolState::new();
        let mut request = packet(&pool, 1);
        request.message = PoolMessage::Create {
            req: "ab".repeat(16),
            name: Some("guest".into()),
            size: SIZE,
            ssh_keys: vec![],
            allow: vec![],
            autostart: true,
            start: false,
        };
        let bytes = request.encode(&pool.key).unwrap();
        let body: Body = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["type"], "create");
        assert_eq!(body["memory_mb"], SIZE.memory_mb);
        assert_eq!(body["did"], request.did);
        assert!(!body.contains_key("owner_did"));
        let verified = Packet::verify(&bytes, &request.from, &pool, 1).unwrap();
        let PoolMessage::Create {
            name,
            size,
            autostart,
            start,
            ..
        } = verified.message
        else {
            panic!("expected a Create message");
        };
        assert_eq!(name.as_deref(), Some("guest"));
        assert_eq!(
            (size.memory_mb, size.cpus, size.disk_gb),
            (SIZE.memory_mb, SIZE.cpus, SIZE.disk_gb)
        );
        assert!(autostart && !start);
    }
}
