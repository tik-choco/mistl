//! Durable promises and checkpoint ownership for VM replication groups.
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Once, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex as AsyncMutex, mpsc};

use super::{
    CheckpointStore, checkpoint, net, store,
    wire::{self, Message, Packet},
};
use crate::daemon::AppState;

const MEMBERS: usize = 32;
const TOKEN_PREFIX: &str = "mistl-vm1:";
const SECOND: u64 = 1000;
const ELECTION_JITTER: u64 = 5 * SECOND;
const BOOT_FAILURE_LIMIT: u8 = 3;
const FENCE_MARGIN: u64 = 3 * SECOND;
const OFFER_PIN_MS: i64 = 10 * 60 * SECOND as i64;
const OFFER_PIN_LIMIT: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Role {
    Host,
    Witness,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Member {
    node: String,
    role: Role,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Timing {
    renew_secs: u64,
    lease_ttl_secs: u64,
    failover_after_secs: u64,
    checkpoint_secs: u64,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            renew_secs: 5,
            lease_ttl_secs: 20,
            failover_after_secs: 45,
            checkpoint_secs: 600,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    v: u8,
    server_id: String,
    room: String,
    members: Vec<Member>,
    owner_did: String,
    timing: Timing,
}

impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.v == 1
                && store::valid_server_id(&self.server_id)
                && crate::escrow::valid_room(&self.room),
            "invalid VM group id, version or room"
        );
        ensure!(
            (2..=MEMBERS).contains(&self.members.len()),
            "VM group needs 2 to {MEMBERS} members"
        );
        let mut nodes = HashSet::new();
        ensure!(
            self.members
                .iter()
                .all(|m| store::valid_server_id(&m.node) && nodes.insert(&m.node)),
            "invalid or duplicate VM member"
        );
        ensure!(self.hosts().count() >= 2, "VM group needs two host members");
        crate::identity::pubkey_from_did(&self.owner_did)?;
        let t = &self.timing;
        ensure!(
            t.renew_secs > 0
                && t.renew_secs < t.lease_ttl_secs
                && t.lease_ttl_secs <= 3600
                && (2 * t.lease_ttl_secs..=86_400).contains(&t.failover_after_secs),
            "invalid VM lease timing"
        );
        Ok(())
    }
    fn hosts(&self) -> impl Iterator<Item = &str> {
        self.members
            .iter()
            .filter(|m| m.role == Role::Host)
            .map(|m| m.node.as_str())
    }
    fn host(&self, node: &str) -> bool {
        self.hosts().any(|n| n == node)
    }
    fn lease_ttl(&self) -> u64 {
        self.timing.lease_ttl_secs * SECOND
    }
    fn delay(&self, me: &str, holder: &str) -> Option<u64> {
        let rank = self
            .hosts()
            .filter(|n| *n != holder)
            .position(|n| n == me)?;
        Some((self.timing.failover_after_secs + rank as u64 * 10) * SECOND)
    }
}

#[derive(Serialize, Deserialize)]
struct Token {
    #[serde(flatten)]
    config: Config,
    holder: String,
    epoch: u64,
    key_b64: String,
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VM join token [secret]")
    }
}
impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl Token {
    fn encode(&self) -> Result<String> {
        Ok(format!(
            "{TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }
    fn decode(secret: &str) -> Result<(Self, [u8; 32])> {
        ensure!(
            secret.len() <= wire::CONTROL_LIMIT,
            "VM join token too large"
        );
        // Do not propagate parse errors: serde can quote input in its diagnostics.
        let bytes = URL_SAFE_NO_PAD
            .decode(
                secret
                    .strip_prefix(TOKEN_PREFIX)
                    .context("invalid VM join token")?,
            )
            .ok()
            .context("invalid VM join token")?;
        let token: Self = serde_json::from_slice(&bytes)
            .ok()
            .context("invalid VM join token")?;
        token
            .config
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid VM group configuration"))?;
        ensure!(
            token.epoch > 0 && token.config.host(&token.holder),
            "invalid VM group holder"
        );
        let key = STANDARD
            .decode(&token.key_b64)
            .ok()
            .and_then(|b| b.try_into().ok())
            .context("invalid VM key")?;
        Ok((token, key))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Release {
    epoch: u64,
    to: String,
    seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MovePhase {
    Stopping,
    Checkpointing,
    Replicating,
    Released,
}

#[derive(Clone, Serialize, Deserialize)]
struct Move {
    to: String,
    phase: MovePhase,
    epoch: u64,
    previous_want_run: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Offered {
    seq: u64,
    at_ms: i64,
}

struct Retention {
    have: u64,
    committed: u64,
    offered: Vec<u64>,
}

impl Retention {
    fn pins(&self, seq: u64) -> bool {
        seq == self.have || seq >= self.committed || self.offered.contains(&seq)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Disk {
    #[serde(flatten)]
    config: Config,
    local_node: String,
    holder: String,
    epoch: u64,
    promised_epoch: u64,
    voted_for: String,
    last_lease_ms: i64,
    committed_seq: u64,
    have_seq: u64,
    #[serde(default)]
    offer_epoch: u64,
    release: Option<Release>,
    stale: bool,
    restore_seq: Option<u64>, // Minimum sequence advertised in the winning vote.
    want_run: bool,
    #[serde(default)]
    boot_failures: u8,
    #[serde(default)]
    last_boot_error: Option<String>,
    #[serde(default)]
    moving: Option<Move>,
    #[serde(default)]
    last_operation_error: Option<String>,
    #[serde(default)]
    offered: Vec<Offered>,
    retiring: bool,
    #[serde(default)]
    retired_peers: HashSet<String>,
    retired: bool,
    detached: bool,
}

impl Disk {
    fn new(config: Config, me: String, holder: String, epoch: u64) -> Self {
        Self {
            config,
            local_node: me,
            voted_for: holder.clone(),
            holder,
            epoch,
            promised_epoch: epoch,
            last_lease_ms: 0,
            committed_seq: 0,
            have_seq: 0,
            offer_epoch: 0,
            release: None,
            stale: false,
            restore_seq: None,
            want_run: false,
            boot_failures: 0,
            last_boot_error: None,
            moving: None,
            last_operation_error: None,
            offered: Vec::new(),
            retiring: false,
            retired_peers: HashSet::new(),
            retired: false,
            detached: false,
        }
    }
    fn leading(&self) -> bool {
        self.holder == self.local_node
            && self.epoch == self.promised_epoch
            && self.release.is_none()
            && !self.retired
            && self.boot_failures < BOOT_FAILURE_LIMIT
    }
    fn waiver(&self, candidate: &str, epoch: u64, seq: u64) -> bool {
        self.release.as_ref().is_some_and(|r| {
            r.epoch.checked_add(1) == Some(epoch) && r.to == candidate && seq >= r.seq
        })
    }
    fn vote(&self, from: &str, epoch: u64, have: u64, handover: bool, since_lease: u64) -> bool {
        !self.retired && !self.retiring && self.config.host(from) && epoch > self.promised_epoch
            && have > 0 && have >= self.committed_seq
            && (!self.config.host(&self.local_node) || have >= self.have_seq)
            // A candidate can miss renewals seen by this voter. Leave time for the old holder to fence.
            && (since_lease >= 2 * self.config.lease_ttl()
                || handover && self.waiver(from, epoch, have))
    }
    fn accepts(&self, from: &str, epoch: u64) -> bool {
        !self.retired
            && self.config.host(from)
            && epoch >= self.promised_epoch
            && (epoch > self.promised_epoch || self.voted_for.is_empty() || self.voted_for == from)
            && self.release.as_ref().is_none_or(|r| r.epoch != epoch)
    }
    fn authoritative_offer(&self, from: &str, epoch: u64, seq: u64) -> bool {
        !self.retired && from == self.holder && epoch == self.epoch && epoch >= self.promised_epoch
            && seq > 0 && seq >= self.committed_seq
            // Once this holder's branch is installed, ignore reordered older offers in its epoch.
            && (epoch > self.offer_epoch || epoch == self.offer_epoch && seq >= self.have_seq)
    }
    fn install_offer(&mut self, epoch: u64, seq: u64) {
        self.offer_epoch = epoch;
        self.have_seq = seq;
        self.stale = false;
    }
    fn record_offer(&mut self, now: i64) {
        self.offered.retain(|offer| {
            offer.seq != self.have_seq && offer.at_ms > now.saturating_sub(OFFER_PIN_MS)
        });
        self.offered.insert(
            0,
            Offered {
                seq: self.have_seq,
                at_ms: now,
            },
        );
        self.offered.truncate(OFFER_PIN_LIMIT);
    }
    fn retention(&self, now: i64) -> Retention {
        Retention {
            have: self.have_seq,
            committed: self.committed_seq,
            offered: self
                .offered
                .iter()
                .filter(|offer| offer.at_ms > now.saturating_sub(OFFER_PIN_MS))
                .take(OFFER_PIN_LIMIT)
                .map(|offer| offer.seq)
                .collect(),
        }
    }
    fn restore_candidate(&self, manifest: &super::Manifest) -> bool {
        manifest.record.owner_did == self.config.owner_did
            && manifest.epoch <= self.epoch
            && (manifest.seq <= self.have_seq || manifest.epoch >= self.offer_epoch)
    }
    fn begin_move(&mut self, to: &str) {
        self.moving = Some(Move {
            to: to.into(),
            phase: MovePhase::Stopping,
            epoch: self.epoch,
            previous_want_run: self.want_run,
        });
        self.last_operation_error = None;
        self.want_run = false;
    }
    fn fail_move(&mut self, error: String) {
        if let Some(moving) = &self.moving
            && moving.phase != MovePhase::Released
        {
            if self.leading() && self.epoch == moving.epoch && !self.stale && !self.retiring {
                self.want_run = moving.previous_want_run;
            }
            self.moving = None;
        }
        self.last_operation_error = Some(error);
    }
    fn recover_operation(&mut self) -> bool {
        if let Some(moving) = &self.moving
            && moving.phase != MovePhase::Released
        {
            self.fail_move("VM move was interrupted before release by a daemon restart".into());
        } else if self.last_operation_error.is_none() && (self.moving.is_some() || self.retiring) {
            self.last_operation_error = Some(
                if self.retiring {
                    "VM group disable was interrupted by a daemon restart; retry disable"
                } else {
                    "VM move was interrupted after release by a daemon restart"
                }
                .into(),
            );
        } else {
            return false;
        }
        true
    }
    fn boot_result(&mut self, error: Option<String>) {
        if error.is_some() {
            self.boot_failures += 1;
            if self.boot_failures == BOOT_FAILURE_LIMIT {
                self.want_run = false;
                self.restore_seq = None;
            }
        } else {
            self.boot_failures = 0;
        }
        self.last_boot_error = error;
    }
}

fn majority(n: usize) -> usize {
    n / 2 + 1
}

fn committed(config: &Config, have: &HashMap<String, u64>, seq: u64) -> bool {
    let members = config.members.len();
    // Require a host majority and enough complete host copies to intersect every voting majority.
    let copies = majority(config.hosts().count()).max(members - majority(members) + 1);
    config
        .hosts()
        .filter(|n| have.get(*n) == Some(&seq))
        .count()
        >= copies
}

fn election_jitter() -> u64 {
    rand::rngs::OsRng.gen_range(0..=ELECTION_JITTER)
}

struct Round {
    epoch: u64,
    nonce: u64,
    sent: u64,
    vote_seq: u64,
    lease_ok: bool,
    acks: HashSet<String>,
}

impl Round {
    fn new(d: &Disk, now: u64, epoch: u64) -> Self {
        Self {
            epoch,
            nonce: rand::random(),
            sent: now,
            vote_seq: d.have_seq,
            lease_ok: false,
            acks: HashSet::from([d.local_node.clone()]),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RunLease {
    epoch: u64,
    generation: u64,
}

struct Core {
    d: Disk,
    last_lease: u64,
    until: u64,
    run_until: u64,
    generation: u64,
    first_renew: bool,
    renew: u64,
    election: Option<Round>,
    jitter: u64,
    retry_at: u64,
    rounds: VecDeque<Round>,
    have: HashMap<String, u64>,
    offer_cleanup_pending: bool,
    remote_running: bool,
    next_checkpoint: u64,
    next_publication: u64,
    // Repeated authenticated lease packets must not refresh a voter's promise.
    seen: HashMap<String, VecDeque<(u64, u64)>>,
}

impl Core {
    fn new(d: Disk) -> Self {
        Self {
            next_checkpoint: d.config.timing.checkpoint_secs.saturating_mul(SECOND),
            d,
            last_lease: 0,
            until: 0,
            run_until: 0,
            generation: 0,
            first_renew: false,
            renew: 0,
            election: None,
            jitter: election_jitter(),
            retry_at: 0,
            rounds: VecDeque::new(),
            have: HashMap::new(),
            offer_cleanup_pending: false,
            remote_running: false,
            next_publication: 0,
            seen: HashMap::new(),
        }
    }
    fn lease(&self, now: u64) -> bool {
        self.d.leading() && self.run_until > now
    }
    fn writable(&self, now: u64) -> bool {
        self.lease(now) && !self.d.stale && self.d.restore_seq.is_none() && !self.d.retiring
    }
    fn same_run(&self, lease: Option<RunLease>) -> bool {
        lease.map_or(self.generation == 0, |lease| {
            lease.epoch == self.d.epoch && lease.generation == self.generation
        })
    }
    fn seen_lease(&mut self, lease_ok: bool, now: u64) {
        if lease_ok {
            self.last_lease = now;
            self.jitter = election_jitter();
            self.retry_at = 0;
            self.election = None;
        }
    }
    fn clear_lease(&mut self) {
        self.until = 0;
        self.run_until = 0;
    }
    fn acknowledge(&mut self, from: &str, epoch: u64, nonce: u64, now: u64) {
        if !self.d.leading() || self.d.epoch != epoch {
            return;
        }
        if let Some(r) = self
            .rounds
            .iter_mut()
            .find(|r| r.epoch == epoch && r.nonce == nonce)
        {
            let ttl = self.d.config.lease_ttl();
            let deadline = r.sent + ttl;
            if deadline > now
                && r.acks.insert(from.into())
                && r.acks.len() >= majority(self.d.config.members.len())
            {
                if self.until <= now && !r.lease_ok {
                    // A probe with a quorum can immediately advertise a healthy round.
                    self.renew = 0;
                }
                self.until = self.until.max(deadline);
                // Only a quorum that saw lease_ok can keep QEMU alive. Probe acks do not
                // postpone elections on the voters, so they cannot extend the fence deadline.
                if r.lease_ok && deadline > self.run_until {
                    if now >= self.run_until + ttl {
                        self.generation += 1;
                    }
                    self.run_until = deadline;
                }
            }
        }
    }
    fn fresh(&mut self, from: &str, nonce: u64, now: u64) -> bool {
        let seen = self.seen.entry(from.into()).or_default();
        while seen.front().is_some_and(|(_, expiry)| *expiry <= now) {
            seen.pop_front();
        }
        if seen.len() >= 128 || seen.iter().any(|(n, _)| *n == nonce) {
            return false;
        }
        seen.push_back((nonce, now + 240_001));
        true
    }
}

pub(super) struct Group {
    pub root: PathBuf,
    pub id: String,
    pub room: String,
    pub me: String,
    pub key: [u8; 32],
    pub inbox: mpsc::Sender<Packet>,
    pub outbox: mpsc::Sender<(String, Vec<u8>)>,
    config: Config,
    state: Arc<AppState>,
    origin: Instant,
    core: Mutex<Core>,
    op: AsyncMutex<()>,
    pull: AsyncMutex<()>,
    boot: AsyncMutex<()>,
    fence: AsyncMutex<()>,
    publication: AsyncMutex<()>,
    joined: AtomicBool,
    closed: AtomicBool,
}

static GROUPS: LazyLock<RwLock<HashMap<String, Arc<Group>>>> = LazyLock::new(Default::default);
static LISTEN: Once = Once::new();

pub(super) fn lookup(id: &str) -> Option<Arc<Group>> {
    GROUPS.read().unwrap().get(id).cloned()
}
fn path(root: &Path, id: &str) -> PathBuf {
    store::vm_dir(root, id).join("ha.json")
}
fn persist(root: &Path, d: &Disk) -> Result<()> {
    crate::statefile::write_private(&path(root, &d.config.server_id), &serde_json::to_vec(d)?)
}

fn drop_abandoned(
    root: &Path,
    id: &str,
    checkpoints: &CheckpointStore,
    retention: &Retention,
) -> Result<()> {
    let dir = store::vm_dir(root, id).join("ckpt");
    for high in checkpoints
        .sequences()?
        .into_iter()
        .take_while(|high| *high > retention.have)
        .filter(|&high| !retention.pins(high))
    {
        std::fs::remove_file(dir.join(format!("{high}.manifest")))
            .context("removing abandoned VM checkpoint")?;
    }
    Ok(())
}

// A damaged or as-yet unloaded group record must fence all local entry points.
pub(super) fn group(root: &Path, id: &str) -> Result<Option<Arc<Group>>> {
    let group = lookup(id);
    ensure!(
        group.is_some() || !path(root, id).try_exists()?,
        "VM group is fenced; see the daemon startup log"
    );
    Ok(group)
}

impl Group {
    fn now(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }
    pub fn online(&self) -> bool {
        self.joined.load(Ordering::Acquire)
            && !self.closed.load(Ordering::Acquire)
            && self.state.network.permitted()
    }
    pub fn member(&self, node: &str) -> bool {
        self.config.members.iter().any(|m| m.node == node)
    }
    pub fn may_serve(&self, epoch: u64) -> bool {
        let c = self.core.lock().unwrap();
        self.online() && c.d.epoch == epoch && c.d.leading() && !c.d.retiring
    }
    pub fn writable(&self) -> bool {
        self.core.lock().unwrap().writable(self.now())
    }
    pub(super) fn run_lease(&self) -> Option<RunLease> {
        let c = self.core.lock().unwrap();
        c.writable(self.now()).then_some(RunLease {
            epoch: c.d.epoch,
            generation: c.generation,
        })
    }
    pub(super) fn can_resume(&self, lease: Option<RunLease>) -> bool {
        let c = self.core.lock().unwrap();
        c.same_run(lease) && c.writable(self.now()) && !self.closed.load(Ordering::Acquire)
    }
    pub fn may_restore(&self, epoch: u64, seq: Option<u64>) -> bool {
        let c = self.core.lock().unwrap();
        c.d.epoch == epoch
            && c.lease(self.now())
            && c.d
                .restore_seq
                .zip(seq)
                .is_some_and(|(floor, seq)| seq >= floor.max(c.d.committed_seq))
    }
    pub(super) fn checkpoint_parent(&self) -> Option<u64> {
        let seq = self.core.lock().unwrap().d.have_seq;
        (seq > 0).then_some(seq)
    }
    pub(super) fn newest_restore(&self, checkpoints: &CheckpointStore, epoch: u64) -> Result<u64> {
        let d = {
            let c = self.core.lock().unwrap();
            ensure!(
                c.d.epoch == epoch && c.lease(self.now()),
                "VM lease changed"
            );
            c.d.clone()
        };
        let voted = d
            .restore_seq
            .context("VM has no pending election restore")?;
        checkpoints.newest_complete(voted.max(d.committed_seq), |manifest| {
            d.restore_candidate(manifest)
        })
    }
    pub(super) fn prune_checkpoints(
        &self,
        checkpoints: &CheckpointStore,
        keep: usize,
    ) -> Result<()> {
        let retention = self
            .core
            .lock()
            .unwrap()
            .d
            .retention(chrono::Utc::now().timestamp_millis());
        checkpoints.prune(keep, |seq| retention.pins(seq))
    }
    pub fn holder(&self) -> String {
        self.core.lock().unwrap().d.holder.clone()
    }
    fn save(&self, c: &mut Core, d: Disk) -> Result<()> {
        persist(&self.root, &d)?;
        c.d = d;
        Ok(())
    }
    fn send(&self, to: &str, epoch: u64, nonce: u64, msg: Message) {
        let _ = wire::send(self, to, epoch, nonce, msg);
    }
    fn broadcast(&self, epoch: u64, nonce: u64, msg: Message) {
        for m in &self.config.members {
            if m.node != self.me {
                self.send(&m.node, epoch, nonce, msg.clone());
            }
        }
    }
    fn status(&self) -> Value {
        let c = self.core.lock().unwrap();
        let d = &c.d;
        let moving = d
            .moving
            .as_ref()
            .map(|moving| json!({"to":moving.to,"phase":moving.phase}));
        let disabling = d.retiring.then(|| {
            json!({"pending_peers": self.config.members.iter()
            .filter(|member| member.node != self.me && !d.retired_peers.contains(&member.node))
            .map(|member| &member.node).collect::<Vec<_>>()})
        });
        json!({"server_id":self.id,"room":self.room,"members":self.config.members,"holder":d.holder,
            "epoch":d.epoch,"promised_epoch":d.promised_epoch,"committed_seq":d.committed_seq,
            "have_seq":d.have_seq,"peer_sequences":c.have,"lease_valid":c.lease(self.now()),
            "stale":d.stale,"restore_seq":d.restore_seq,"released_to":d.release.as_ref().map(|r| &r.to),
            "timing":self.config.timing,"retired":d.retired,"online":self.online(),
            "state":if d.leading() { "holder" } else { "standby" },"moving":moving,"disabling":disabling,
            "last_operation_error":d.last_operation_error,
            "last_error":d.last_operation_error.as_ref().or(d.last_boot_error.as_ref())})
    }
    pub fn decorate(&self, row: &mut Value) {
        let c = self.core.lock().unwrap();
        row["holder"] = json!(c.d.holder);
        row["server_id"] = json!(self.id);
        if c.d.boot_failures >= BOOT_FAILURE_LIMIT && !c.d.retired {
            row["state"] = json!("standby");
        } else if c.d.leading() && c.d.restore_seq.is_some() && !c.d.retired {
            // An elected holder replaces its stale local disk before booting.
            row["state"] = json!("restoring");
        } else if c.d.stale || c.d.retired {
            row["state"] = json!("stale");
        } else if !c.d.leading() {
            row["state"] = json!("standby");
        }
        if let Some(error) = &c.d.last_boot_error {
            row["last_error"] = json!(error);
        }
    }
}

pub(super) fn list(rows: &mut Vec<Value>) {
    for (id, g) in GROUPS.read().unwrap().iter() {
        if let Some(row) = rows.iter_mut().find(|r| r["server_id"] == id.as_str()) {
            g.decorate(row);
        } else {
            let mut row = json!({});
            g.decorate(&mut row);
            rows.push(row);
        }
    }
    rows.sort_by(|a, b| a["server_id"].as_str().cmp(&b["server_id"].as_str()));
}

fn launch(root: PathBuf, d: Disk, key: [u8; 32], state: &Arc<AppState>) -> Arc<Group> {
    LISTEN.call_once(wire::listen);
    let (inbox, mut rx) = mpsc::channel(128);
    let (outbox, mut tx) = mpsc::channel(256);
    let g = Arc::new(Group {
        root,
        id: d.config.server_id.clone(),
        room: d.config.room.clone(),
        me: d.local_node.clone(),
        config: d.config.clone(),
        key,
        state: state.clone(),
        origin: Instant::now(),
        core: Mutex::new(Core::new(d)),
        op: AsyncMutex::new(()),
        pull: AsyncMutex::new(()),
        boot: AsyncMutex::new(()),
        fence: AsyncMutex::new(()),
        publication: AsyncMutex::new(()),
        joined: false.into(),
        closed: false.into(),
        inbox,
        outbox,
    });
    GROUPS.write().unwrap().insert(g.id.clone(), g.clone());
    let weak = Arc::downgrade(&g);
    tokio::spawn(async move {
        while let Some((peer, bytes)) = tx.recv().await {
            let Some(g) = weak.upgrade() else { break };
            if g.online() {
                let _ = crate::net::send_direct(&g.room, &peer, bytes).await;
            }
        }
    });
    tokio::spawn({
        let g = g.clone();
        async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(200));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut fence_checked = 0;
            loop {
                if g.closed.load(Ordering::Acquire) {
                    break;
                }
                let fence_at = {
                    let c = g.core.lock().unwrap();
                    [c.run_until, c.run_until + g.config.lease_ttl()]
                        .into_iter()
                        .find(|deadline| c.d.leading() && *deadline > fence_checked)
                };
                tokio::select! {
                    biased;
                    _ = async {
                        match fence_at {
                            Some(at) => tokio::time::sleep_until((g.origin + Duration::from_millis(at)).into()).await,
                            None => std::future::pending().await,
                        }
                    } => {
                        fence_checked = fence_at.unwrap();
                        g.schedule_fence(false);
                    }
                    _ = ticks.tick() => if let Err(error) = g.tick().await { tracing::warn!(vm = %g.id, %error, "VM HA tick failed"); },
                    Some(p) = rx.recv() => if g.online() && let Err(error) = g.receive(p) {
                        tracing::debug!(vm = %g.id, %error, "VM HA message dropped");
                    }
                }
            }
        }
    });
    tokio::spawn({
        let g = g.clone();
        async move {
            loop {
                let done = g.closed.load(Ordering::Acquire);
                let needed = !done
                    && !g.core.lock().unwrap().d.detached
                    && g.state.network.permitted()
                    && g.state.config().vm.room == g.room;
                if needed && !g.joined.load(Ordering::Acquire) {
                    if let Ok(t) = crate::net::ensure_started(
                        &g.state,
                        crate::net::RoomUse::Vm,
                        g.room.clone(),
                    )
                    .await
                    {
                        if t.node_id == g.me {
                            g.joined.store(true, Ordering::Release);
                        } else {
                            let _ = crate::net::leave_room(crate::net::RoomUse::Vm, &g.room).await;
                        }
                    }
                } else if !needed
                    && g.joined.load(Ordering::Acquire)
                    && crate::net::leave_room(crate::net::RoomUse::Vm, &g.room)
                        .await
                        .is_ok()
                {
                    g.joined.store(false, Ordering::Release);
                }
                if done && !g.joined.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });
    g
}

pub(super) fn load(state: &Arc<AppState>) -> Result<()> {
    let root = store::vm_root()?;
    for entry in std::fs::read_dir(&root)? {
        let entry = entry?;
        let Some(id) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !store::valid_server_id(&id) || !path(&root, &id).is_file() {
            continue;
        }
        let result = (|| -> Result<()> {
            let mut d: Disk = serde_json::from_slice(&std::fs::read(path(&root, &id))?)?;
            d.config.validate()?;
            ensure!(d.config.server_id == id, "VM group directory mismatch");
            super::host::require_ha_support(std::env::consts::OS)?;
            super::host::fence_previous_qemu(&store::vm_dir(&root, &id))?;
            let key = checkpoint::existing_key(&root, &id)?;
            if d.recover_operation() {
                persist(&root, &d)?;
            }
            if d.config.host(&d.local_node) && !d.retired {
                let checkpoints = CheckpointStore::open(&root, &id)?;
                if !d.leading() && d.offer_epoch > 0 {
                    // Finish pruning if the daemon stopped after saving an authoritative offer.
                    drop_abandoned(
                        &root,
                        &id,
                        &checkpoints,
                        &d.retention(chrono::Utc::now().timestamp_millis()),
                    )?;
                } else if let Some(seq) = checkpoints.sequences()?.first().copied()
                    && (seq > d.have_seq || d.stale && !d.leading() && seq == d.have_seq)
                {
                    // Commit publishes the complete manifest before updating ha.json.
                    let m = checkpoints.load_manifest(seq)?;
                    if m.epoch == d.epoch
                        && m.epoch == d.promised_epoch
                        && m.record.owner_did == d.config.owner_did
                        && checkpoints.missing_chunks(&m)?.is_empty()
                    {
                        d.have_seq = seq;
                        if !d.leading() {
                            d.stale = false;
                        }
                        persist(&root, &d)?;
                    }
                }
                if let Err(error) = checkpoints.collect_chunks() {
                    tracing::warn!(%id, %error, "VM checkpoint chunk cleanup failed");
                }
            }
            launch(root.clone(), d, key, state);
            Ok(())
        })();
        if let Err(error) = result {
            tracing::warn!(%id, %error, "VM group is fenced");
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
enum Fence {
    Kill,
    Pause,
    Resume,
    None,
}

fn fence(c: &Core, state: &str, now: u64) -> Fence {
    if !c.d.leading() || c.d.stale || c.d.retiring {
        return Fence::Kill;
    }
    // Voters grant 2 x ttl after the last lease_ok round they received; kill a margin
    // earlier so fence latency (tick interval, QMP, process exit) stays inside that window.
    let kill_at = (c.run_until + c.d.config.lease_ttl()).saturating_sub(FENCE_MARGIN);
    if !c.lease(now) && (state == "stopping" || now >= kill_at) {
        return Fence::Kill;
    }
    match (c.lease(now), state) {
        (false, "starting" | "running") => Fence::Pause,
        (true, "paused") => Fence::Resume,
        _ => Fence::None,
    }
}

impl Group {
    async fn tick(self: &Arc<Self>) -> Result<()> {
        let process = super::REGISTRY
            .lock()
            .await
            .running
            .get(&self.id)
            .map(|vm| vm.process.state);
        let now = self.now();
        let mut c = self.core.lock().unwrap();
        let check_publication = now >= c.next_publication && self.state.network.permitted();
        if check_publication {
            c.next_publication = now + 5 * SECOND;
        }
        if process.is_some() || check_publication {
            self.schedule_fence(check_publication);
        }
        if c.d.retired {
            return Ok(());
        }
        if c.d.leading() {
            if self.online() && now >= c.renew {
                c.renew = now + self.config.timing.renew_secs * SECOND;
                let mut r = Round::new(&c.d, now, c.d.epoch);
                r.lease_ok = c.until > now || c.first_renew;
                if r.lease_ok {
                    let mut d = c.d.clone();
                    d.last_lease_ms = chrono::Utc::now().timestamp_millis();
                    self.save(&mut c, d)?;
                    c.seen_lease(true, now);
                }
                c.first_renew = false;
                while c
                    .rounds
                    .front()
                    .is_some_and(|r| r.sent + self.config.lease_ttl() <= now)
                {
                    c.rounds.pop_front();
                }
                self.broadcast(
                    c.d.epoch,
                    r.nonce,
                    Message::Lease {
                        committed_seq: c.d.committed_seq,
                        lease_ok: r.lease_ok,
                        running: c.writable(now) && process == Some("running"),
                    },
                );
                c.rounds.push_back(r);
                self.offer(&mut c)?;
            }
            if c.lease(now) && !c.d.retiring && c.d.want_run && process.is_none() {
                self.schedule_boot(c.d.epoch);
            }
            if c.writable(now) && self.config.timing.checkpoint_secs > 0 && now >= c.next_checkpoint
            {
                c.next_checkpoint =
                    now.saturating_add(self.config.timing.checkpoint_secs.saturating_mul(SECOND));
                let g = self.clone();
                tokio::spawn(async move {
                    if let Ok(_op) = g.op.try_lock()
                        && let Err(error) = g.capture().await
                    {
                        tracing::warn!(vm = %g.id, %error, "periodic VM checkpoint failed");
                    }
                });
            }
        } else if self.online() {
            if c.d.holder == self.me
                && let Some(r) = &c.d.release
                && now >= c.renew
            {
                self.broadcast(
                    r.epoch,
                    rand::random(),
                    Message::Release {
                        to: r.to.clone(),
                        seq: r.seq,
                    },
                );
                c.renew = now + SECOND;
            }
            // A newer promise can fence this host before anyone announces the winner.
            let holder = if c.d.holder == self.me && c.d.stale && c.d.release.is_none() {
                &c.d.voted_for
            } else {
                &c.d.holder
            };
            let Some(delay) = self.config.delay(&self.me, holder) else {
                return Ok(());
            };
            let epoch =
                c.d.promised_epoch
                    .checked_add(1)
                    .context("VM epoch exhausted")?;
            let handover = c.d.waiver(&self.me, epoch, c.d.have_seq);
            let elapsed = now.saturating_sub(c.last_lease);
            if (handover || elapsed >= delay + c.jitter)
                && now >= c.retry_at
                && !c.offer_cleanup_pending
                && c.d.vote(&self.me, epoch, c.d.have_seq, handover, elapsed)
            {
                let mut d = c.d.clone();
                d.voted_for = self.me.clone();
                d.promised_epoch = epoch;
                self.save(&mut c, d)?;
                let r = Round::new(&c.d, now, epoch);
                self.broadcast(
                    epoch,
                    r.nonce,
                    Message::VoteReq {
                        have_seq: r.vote_seq,
                        handover,
                    },
                );
                c.election = Some(r);
                c.retry_at = now + 10 * SECOND + election_jitter();
            }
        }
        Ok(())
    }

    pub(super) fn schedule_fence(self: &Arc<Self>, check_publication: bool) {
        let g = self.clone();
        tokio::spawn(async move {
            let (control, qmp, action) = {
                let registry = super::REGISTRY.lock().await;
                let Some(vm) = registry.running.get(&g.id) else {
                    if check_publication {
                        g.schedule_publication();
                    }
                    return;
                };
                let c = g.core.lock().unwrap();
                let action = if c.same_run(vm.control.lease) {
                    fence(&c, vm.process.state, g.now())
                } else {
                    Fence::Kill
                };
                (vm.control.clone(), vm.qmp.clone(), action)
            };
            if action == Fence::Kill {
                // Do not wait for a pending pause command or an ACPI shutdown.
                net::unregister_acl(&g.id);
                control.kill.cancel();
                if check_publication {
                    g.schedule_publication();
                }
                return;
            }
            let Ok(_guard) = g.fence.try_lock() else {
                return;
            };
            match action {
                Fence::Pause => {
                    net::unregister_acl(&g.id);
                    let pause = super::qmp_request(&qmp, super::QmpAction::LeasePause);
                    if !matches!(
                        tokio::time::timeout(Duration::from_secs(2), pause).await,
                        Ok(Ok(()))
                    ) {
                        control.kill.cancel();
                    }
                }
                Fence::Resume => {
                    let _ = super::qmp_request(&qmp, super::QmpAction::LeaseResume).await;
                }
                _ => {}
            }
            if action != Fence::None || check_publication {
                g.schedule_publication();
            }
        });
    }

    fn schedule_publication(self: &Arc<Self>) {
        let g = self.clone();
        tokio::spawn(async move {
            let Ok(_guard) = g.publication.try_lock() else {
                return;
            };
            let running = super::REGISTRY
                .lock()
                .await
                .running
                .get(&g.id)
                .is_some_and(|vm| {
                    vm.process.state == "running"
                        && !vm.control.kill.is_cancelled()
                        && g.can_resume(vm.control.lease)
                });
            if running {
                if let Ok(record) = store::load(&g.root, &g.id) {
                    net::register_record(&record);
                    let _ = tokio::time::timeout(
                        super::TUNNEL_TIMEOUT,
                        net::publish_ssh(&g.state, &g.id, record.ssh_port),
                    )
                    .await;
                }
            } else {
                // A crashed holder can leave a persisted Serve forward.
                net::unregister_acl(&g.id);
                let _ = tokio::time::timeout(
                    super::TUNNEL_TIMEOUT,
                    net::unpublish_ssh(&g.state, &g.id),
                )
                .await;
            }
        });
    }

    fn schedule_boot(self: &Arc<Self>, epoch: u64) {
        let g = self.clone();
        tokio::spawn(async move {
            let Ok(_boot) = g.boot.try_lock() else { return };
            let Ok(_op) = g.op.try_lock() else { return };
            let result = async {
                let needs_restore = {
                    let c = g.core.lock().unwrap();
                    ensure!(c.d.epoch == epoch && c.lease(g.now()) && c.d.want_run, "VM lease changed");
                    c.d.restore_seq.is_some()
                };
                if needs_restore {
                    let started = Instant::now();
                    let seq = super::restore_elected(&g, epoch, &g.state).await?;
                    let mut c = g.core.lock().unwrap();
                    ensure!(c.d.epoch == epoch && c.d.leading(), "VM holder changed during restore");
                    let mut d = c.d.clone();
                    d.restore_seq = None;
                    d.stale = false;
                    d.have_seq = seq;
                    g.save(&mut c, d)?;
                    g.offer(&mut c)?;
                    tracing::info!(vm = %g.id, seq, ms = started.elapsed().as_millis(), "VM HA restore complete");
                }
                ensure!(g.writable(), "VM lease expired before boot");
                super::start(&g.root, &g.id, &g.state).await?;
                Ok::<_, anyhow::Error>(())
            }.await;
            if let Err(error) = g.record_boot_result(epoch, &result) {
                tracing::warn!(vm = %g.id, %error, "VM HA boot status could not be saved");
            }
            if let Err(error) = result {
                tracing::warn!(vm = %g.id, %error, "VM HA boot deferred");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    fn record_boot_result<T>(&self, epoch: u64, result: &Result<T>) -> Result<()> {
        let mut c = self.core.lock().unwrap();
        if c.d.epoch != epoch || !c.lease(self.now()) {
            return Ok(());
        }
        let mut d = c.d.clone();
        if result.is_ok() && d.last_boot_error.is_none() {
            return Ok(());
        }
        d.boot_result(result.as_ref().err().map(|error| format!("{error:#}")));
        self.save(&mut c, d)?;
        if !c.d.leading() {
            c.clear_lease();
        }
        Ok(())
    }

    fn offer(&self, c: &mut Core) -> Result<()> {
        if !self.online()
            || c.d.have_seq == 0
            || c.d.restore_seq.is_some()
            || c.d.stale
            || c.d.retiring
        {
            return Ok(());
        }
        let sends: Vec<_> = self
            .config
            .hosts()
            .filter(|p| *p != self.me && c.have.get(*p) != Some(&c.d.have_seq))
            .filter_map(|peer| self.outbox.try_reserve().ok().map(|permit| (peer, permit)))
            .collect();
        if sends.is_empty() {
            return Ok(());
        }
        let bytes = wire::encode(
            self,
            c.d.epoch,
            rand::random(),
            Message::CkptOffer { seq: c.d.have_seq },
        )?;
        let mut d = c.d.clone();
        d.record_offer(chrono::Utc::now().timestamp_millis());
        self.save(c, d)?;
        for (peer, permit) in sends {
            permit.send((peer.into(), bytes.clone()));
        }
        Ok(())
    }

    fn receive(self: &Arc<Self>, p: Packet) -> Result<()> {
        let now = self.now();
        let mut c = self.core.lock().unwrap();
        let from = p.from.as_str();
        if c.d.retired && !matches!(p.msg, Message::Retire | Message::RetireDone) {
            return Ok(());
        }
        match p.msg {
            Message::Lease {
                committed_seq,
                lease_ok,
                running,
            } => {
                if !c.d.accepts(from, p.epoch) {
                    self.send(
                        from,
                        p.epoch,
                        p.nonce,
                        Message::LeaseNack {
                            promised_epoch: c.d.promised_epoch,
                            holder: c.d.holder.clone(),
                        },
                    );
                    return Ok(());
                }
                if !c.fresh(from, p.nonce, now) {
                    return Ok(());
                }
                let mut d = c.d.clone();
                let was_holder = d.holder == self.me;
                if was_holder {
                    d.stale = true;
                }
                d.holder = from.into();
                d.voted_for = from.into();
                d.epoch = p.epoch;
                d.promised_epoch = p.epoch;
                d.committed_seq = d.committed_seq.max(committed_seq);
                d.release = None;
                d.restore_seq = None;
                d.want_run = false;
                if lease_ok
                    && running
                    && d.moving.as_ref().is_some_and(|moving| {
                        moving.phase == MovePhase::Released
                            && moving.to == from
                            && p.epoch > moving.epoch
                    })
                {
                    d.moving = None;
                }
                if lease_ok {
                    d.last_lease_ms = chrono::Utc::now().timestamp_millis();
                }
                self.save(&mut c, d)?;
                c.clear_lease();
                c.remote_running = lease_ok && running;
                c.seen_lease(lease_ok, now);
                if was_holder {
                    self.schedule_fence(true);
                }
                self.send(from, p.epoch, p.nonce, Message::LeaseAck);
            }
            Message::LeaseAck => c.acknowledge(from, p.epoch, p.nonce, now),
            Message::LeaseNack {
                promised_epoch,
                holder,
            } if promised_epoch > c.d.promised_epoch && self.config.host(&holder) => {
                let mut d = c.d.clone();
                d.promised_epoch = promised_epoch;
                if d.holder == self.me {
                    d.stale = true;
                }
                d.holder = holder;
                d.voted_for.clear();
                d.want_run = false;
                d.restore_seq = None;
                d.release = None;
                self.save(&mut c, d)?;
                c.clear_lease();
                c.remote_running = false;
                c.election = None;
                self.schedule_fence(true);
            }
            Message::VoteReq { have_seq, handover } => {
                let granted = c.d.vote(
                    from,
                    p.epoch,
                    have_seq,
                    handover,
                    now.saturating_sub(c.last_lease),
                );
                if granted {
                    let mut d = c.d.clone();
                    d.promised_epoch = p.epoch;
                    d.voted_for = from.into();
                    if d.holder == self.me {
                        d.stale = true;
                    }
                    self.save(&mut c, d)?;
                    c.clear_lease();
                    c.election = None;
                    if c.d.holder == self.me {
                        self.schedule_fence(true);
                    }
                }
                self.send(from, p.epoch, p.nonce, Message::Vote { granted });
                if p.epoch < c.d.promised_epoch {
                    self.send(
                        from,
                        p.epoch,
                        p.nonce,
                        Message::LeaseNack {
                            promised_epoch: c.d.promised_epoch,
                            holder: c.d.holder.clone(),
                        },
                    );
                }
            }
            Message::Vote { granted: true } if p.epoch == c.d.promised_epoch => {
                let Some(r) = &mut c.election else {
                    return Ok(());
                };
                if p.epoch == r.epoch
                    && p.nonce == r.nonce
                    && r.acks.insert(from.into())
                    && r.acks.len() >= majority(self.config.members.len())
                {
                    let vote_seq = r.vote_seq;
                    let mut d = c.d.clone();
                    d.holder = self.me.clone();
                    d.epoch = p.epoch;
                    d.release = None;
                    d.restore_seq = Some(vote_seq);
                    d.stale = true;
                    d.want_run = true;
                    d.boot_failures = 0;
                    self.save(&mut c, d)?;
                    c.clear_lease();
                    c.generation += 1;
                    c.first_renew = true;
                    c.renew = 0;
                    c.rounds.clear();
                    c.have.clear();
                    c.election = None;
                }
            }
            Message::Release { to, seq }
                if from == c.d.holder
                    && c.d.epoch == p.epoch
                    && self.config.host(&to)
                    && to != from
                    && seq >= c.d.committed_seq =>
            {
                if c.d.release.is_none() {
                    let mut d = c.d.clone();
                    d.release = Some(Release {
                        epoch: p.epoch,
                        to,
                        seq,
                    });
                    self.save(&mut c, d)?;
                    c.remote_running = false;
                }
            }
            Message::Get { seq, chunk }
                if c.d.leading()
                    && !c.d.retiring
                    && c.d.epoch == p.epoch
                    && seq > 0
                    && seq <= c.d.have_seq
                    && self.config.host(from) =>
            {
                wire::serve(self.clone(), p.from, p.epoch, seq, chunk, p.nonce);
            }
            Message::CkptOffer { seq }
                if self.config.host(&self.me) && c.d.authoritative_offer(from, p.epoch, seq) =>
            {
                if seq == c.d.have_seq
                    && p.epoch == c.d.offer_epoch
                    && !c.d.stale
                    && !c.offer_cleanup_pending
                {
                    self.send(from, p.epoch, p.nonce, Message::CkptHave { seq });
                } else {
                    self.schedule_pull(p.from, p.epoch, seq);
                }
            }
            Message::CkptHave { seq }
                if c.d.leading()
                    && c.d.epoch == p.epoch
                    && seq == c.d.have_seq
                    && self.config.host(from) =>
            {
                c.have.insert(from.into(), seq);
                c.have.insert(self.me.clone(), seq);
                if seq > c.d.committed_seq && committed(&self.config, &c.have, seq) {
                    let mut d = c.d.clone();
                    d.committed_seq = seq;
                    self.save(&mut c, d)?;
                    c.renew = 0;
                }
            }
            Message::Retire
                if from == c.d.holder && c.d.epoch == p.epoch && c.d.promised_epoch == p.epoch =>
            {
                let mut d = c.d.clone();
                d.retired = true;
                d.stale = true;
                self.save(&mut c, d)?;
                self.send(from, p.epoch, p.nonce, Message::Retired);
            }
            Message::Retired
                if c.d.retiring && c.d.epoch == p.epoch && !c.d.retired_peers.contains(from) =>
            {
                let mut d = c.d.clone();
                d.retired_peers.insert(p.from);
                self.save(&mut c, d)?;
            }
            Message::RetireDone if c.d.retired && from == c.d.holder && c.d.epoch == p.epoch => {
                let mut d = c.d.clone();
                d.detached = true;
                self.save(&mut c, d)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn schedule_pull(self: &Arc<Self>, peer: String, epoch: u64, seq: u64) {
        let g = self.clone();
        tokio::spawn(async move {
            let Ok(_pull) = g.pull.try_lock() else { return };
            if let Err(error) = g.pull_checkpoint(&peer, epoch, seq).await {
                tracing::debug!(vm = %g.id, seq, %error, "VM checkpoint pull interrupted");
            }
        });
    }

    async fn pull_checkpoint(self: &Arc<Self>, peer: &str, epoch: u64, seq: u64) -> Result<()> {
        let _vm = super::lock_vm(&self.id).await?;
        let checkpoints = Arc::new(CheckpointStore::open(&self.root, &self.id)?);
        let result = self
            .receive_checkpoint(peer, epoch, seq, &checkpoints)
            .await;
        if result.is_err() {
            match tokio::task::spawn_blocking(move || checkpoints.collect_chunks()).await {
                Ok(Ok(())) => {}
                cleanup => {
                    tracing::warn!(vm = %self.id, ?cleanup, "VM checkpoint chunk cleanup failed")
                }
            }
        }
        result
    }

    async fn receive_checkpoint(
        self: &Arc<Self>,
        peer: &str,
        epoch: u64,
        seq: u64,
        checkpoints: &Arc<CheckpointStore>,
    ) -> Result<()> {
        let valid = |c: &Core| {
            c.d.authoritative_offer(peer, epoch, seq) && !self.closed.load(Ordering::Acquire)
        };
        ensure!(valid(&self.core.lock().unwrap()), "VM offer superseded");
        let start = Instant::now();
        let bytes = wire::fetch(self, peer, epoch, seq, None).await?;
        let manifest = checkpoints.decode_manifest(&bytes)?;
        drop(bytes);
        ensure!(
            manifest.seq == seq
                && manifest.epoch <= epoch
                && manifest.record.owner_did == self.config.owner_did,
            "VM manifest differs from offer"
        );
        let cfg = self.state.config().vm;
        ensure!(
            manifest.record.disk_gb <= cfg.max_disk_gb
                && manifest.record.memory_mb <= cfg.max_memory_mb
                && manifest.record.cpus <= cfg.max_cpus
                && manifest.disk_bytes <= (u64::from(cfg.max_disk_gb) + 1) * (1 << 30),
            "VM exceeds host limits"
        );
        let (manifest, missing) = tokio::task::spawn_blocking({
            let checkpoints = checkpoints.clone();
            move || {
                let missing = checkpoints.verified_missing_chunks(&manifest, true)?;
                Ok::<_, anyhow::Error>((manifest, missing))
            }
        })
        .await??;
        let count = missing.len();
        let mut transferred = 0;
        for batch in missing.chunks(4) {
            ensure!(valid(&self.core.lock().unwrap()), "VM holder changed");
            let writes = batch.iter().cloned().map(|id| {
                let checkpoints = checkpoints.clone();
                async move {
                    let bytes = wire::fetch(self, peer, epoch, seq, Some(id.clone())).await?;
                    let len = bytes.len();
                    tokio::task::spawn_blocking(move || {
                        checkpoints.store_encrypted_chunk(&id, &bytes)
                    })
                    .await??;
                    Ok::<usize, anyhow::Error>(len)
                }
            });
            // Finish every disk write before releasing the VM lock, including on error.
            for result in futures_util::future::join_all(writes).await {
                transferred += result?;
            }
        }
        ensure!(valid(&self.core.lock().unwrap()), "VM holder changed");
        super::sshkey::merge_known_hosts(&self.root, &self.id, &manifest.known_hosts)?;
        let peer = peer.to_owned();
        tokio::task::spawn_blocking({
            let g = self.clone();
            let checkpoints = checkpoints.clone();
            let peer = peer.clone();
            move || {
                {
                    // Election transitions use this same lock. They see either the entire
                    // installation and its durable have_seq or neither.
                    let mut c = g.core.lock().unwrap();
                    ensure!(
                        c.d.authoritative_offer(&peer, epoch, seq)
                            && !g.closed.load(Ordering::Acquire),
                        "VM holder changed"
                    );
                    checkpoints.install_manifest(&manifest)?;
                    let mut d = c.d.clone();
                    d.install_offer(epoch, seq);
                    g.save(&mut c, d)?;
                    let cleanup = drop_abandoned(
                        &g.root,
                        &g.id,
                        &checkpoints,
                        &c.d.retention(chrono::Utc::now().timestamp_millis()),
                    );
                    c.offer_cleanup_pending = cleanup.is_err();
                    cleanup?;
                }
                g.prune_checkpoints(&checkpoints, cfg.checkpoint_keep)
            }
        })
        .await??;
        self.send(&peer, epoch, rand::random(), Message::CkptHave { seq });
        tracing::info!(vm = %self.id, seq, chunks = count, bytes = transferred,
            ms = start.elapsed().as_millis(), "VM checkpoint received");
        Ok(())
    }

    async fn capture(&self) -> Result<super::CheckpointStats> {
        let epoch = {
            let c = self.core.lock().unwrap();
            ensure!(
                c.writable(self.now()),
                "VM checkpoint requires the current holder's lease"
            );
            c.d.epoch
        };
        let (_, stats) = super::create_checkpoint(&self.id, epoch, &self.state).await?;
        let mut c = self.core.lock().unwrap();
        ensure!(
            c.d.epoch == epoch && c.d.leading(),
            "VM holder changed during checkpoint"
        );
        let mut d = c.d.clone();
        d.have_seq = stats.seq;
        self.save(&mut c, d)?;
        c.have.insert(self.me.clone(), stats.seq);
        self.offer(&mut c)?;
        Ok(stats)
    }
}

pub(super) fn slots(root: &Path) -> Result<usize> {
    let mut n = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(store::valid_server_id)
            && entry.file_type()?.is_dir()
        {
            n += 1;
        }
    }
    Ok(n)
}

pub(super) async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    if cmd == "vm.ha.join" || cmd == "vm.ha.enable" {
        super::host::require_ha_support(std::env::consts::OS)?;
        let root = store::vm_root()?;
        let _create = super::CREATE_LOCK.lock().await;
        let cfg = state.config().vm;
        ensure!(
            crate::escrow::valid_room(&cfg.room),
            "configure a nonempty [vm] room first"
        );
        let identity = crate::identity::current(state).await?;
        let me = identity.node_id();
        if cmd == "vm.ha.join" {
            let (token, key) =
                Token::decode(args["token"].as_str().context("missing VM join token")?)?;
            let id = &token.config.server_id;
            let _vm = super::lock_vm(id).await?;
            ensure!(
                cfg.room == token.config.room,
                "VM token room differs from [vm] room"
            );
            ensure!(
                token.config.members.iter().any(|m| m.node == me) && token.holder != me,
                "this node is not a standby or witness in the group"
            );
            if let Some(g) = group(&root, id)? {
                ensure!(
                    g.key == key
                        && g.config.members.iter().map(|m| (&m.node, m.role)).eq(token
                            .config
                            .members
                            .iter()
                            .map(|m| (&m.node, m.role)))
                        && !g.core.lock().unwrap().d.retired,
                    "a different or retired VM group already exists"
                );
                return Ok(g.status());
            }
            let dir = store::vm_dir(&root, id);
            ensure!(
                !dir.join("vm.json").try_exists()? && !dir.join("disk.qcow2").try_exists()?,
                "refusing to join over an existing VM"
            );
            ensure!(
                !dir.try_exists()? && slots(&root)? < cfg.max_vms as usize,
                "VM directory exists or host VM limit reached"
            );
            let d = Disk::new(token.config, me, token.holder, token.epoch);
            crate::statefile::create_private_dir(&dir)?;
            if let Err(error) = checkpoint::install_key(&root, &d.config.server_id, &key)
                .and_then(|_| persist(&root, &d))
            {
                let _ = store::remove(&root, &d.config.server_id);
                return Err(error);
            }
            return Ok(launch(root, d, key, state).status());
        }
        #[derive(Deserialize)]
        struct Enable {
            peers: Vec<String>,
            #[serde(default)]
            witnesses: Vec<String>,
            checkpoint_secs: Option<u64>,
        }
        let id = super::server_id(&args)?.to_owned();
        let options: Enable = serde_json::from_value(args)?;
        let _vm = super::lock_vm(&id).await?;
        ensure!(group(&root, &id)?.is_none(), "VM group already exists");
        let record = store::load(&root, &id)?;
        let mut members = vec![Member {
            node: me.clone(),
            role: Role::Host,
        }];
        members.extend(options.peers.into_iter().map(|node| Member {
            node,
            role: Role::Host,
        }));
        members.extend(options.witnesses.into_iter().map(|node| Member {
            node,
            role: Role::Witness,
        }));
        let config = Config {
            v: 1,
            server_id: id.clone(),
            room: cfg.room,
            owner_did: record.owner_did,
            members,
            timing: Timing {
                checkpoint_secs: options.checkpoint_secs.unwrap_or(600),
                ..Timing::default()
            },
        };
        config.validate()?;
        let key = checkpoint::load_or_create_key(&root, &id)?;
        let have = CheckpointStore::open(&root, &id)?
            .sequences()?
            .first()
            .copied()
            .unwrap_or(0);
        let archive = store::vm_dir(&root, &id).join("ha.disabled");
        let previous_epoch = if archive.try_exists()? {
            serde_json::from_slice::<Disk>(&std::fs::read(archive)?)?.promised_epoch
        } else {
            0
        };
        let epoch = previous_epoch
            .max(have >> 32)
            .checked_add(1)
            .context("VM epoch exhausted")?;
        let token = Token {
            config: config.clone(),
            key_b64: STANDARD.encode(key),
            holder: me.clone(),
            epoch,
        }
        .encode()?;
        let mut d = Disk::new(config, me.clone(), me, epoch);
        d.have_seq = have;
        d.want_run = super::REGISTRY.lock().await.running.contains_key(&id);
        if !d.want_run {
            super::host::fence_previous_qemu(&store::vm_dir(&root, &id))?;
        }
        persist(&root, &d)?;
        let g = launch(root, d, key, state);
        g.schedule_fence(true);
        return Ok(json!({"token":token}));
    }
    let root = store::vm_root()?;
    let id = super::server_id(&args)?;
    let g = group(&root, id)?.context("VM has no replication group")?;
    match cmd {
        "vm.ha.status" => Ok(g.status()),
        "vm.ha.disable" => tokio::spawn(async move { g.disable().await }).await?,
        "vm.move" => {
            let to = args["to"]
                .as_str()
                .context("vm.move requires a target node")?
                .to_owned();
            tokio::spawn(async move { g.move_to(&to).await }).await?
        }
        _ => bail!("unknown VM HA command"),
    }
}

impl Group {
    pub async fn checkpoint(&self) -> Result<Value> {
        let _op = self.op.lock().await;
        Ok(serde_json::to_value(self.capture().await?)?)
    }

    pub async fn set_running(&self, wanted: bool, force: bool) -> Result<Value> {
        let _op = self.op.lock().await;
        let epoch = {
            let mut c = self.core.lock().unwrap();
            ensure!(
                c.d.leading() && (!wanted || c.writable(self.now())),
                "VM start requires the current holder's lease"
            );
            let mut d = c.d.clone();
            d.want_run = wanted;
            self.save(&mut c, d)?;
            c.d.epoch
        };
        if wanted {
            let result = super::start(&self.root, &self.id, &self.state).await;
            self.record_boot_result(epoch, &result)?;
            result
        } else {
            let record = super::stop(&self.root, &self.id, force).await?;
            super::status(&record, &self.root, &self.state).await
        }
    }

    async fn move_to(&self, to: &str) -> Result<Value> {
        let _op = self.op.lock().await;
        ensure!(
            to != self.me && self.config.host(to),
            "move target must be another host member"
        );
        let epoch = {
            let mut c = self.core.lock().unwrap();
            ensure!(
                c.writable(self.now()),
                "VM move requires the current holder's lease"
            );
            let mut d = c.d.clone();
            d.begin_move(to);
            self.save(&mut c, d)?;
            c.d.epoch
        };
        let result = self.move_guest(to, epoch).await;
        if let Err(error) = &result {
            let mut c = self.core.lock().unwrap();
            let mut d = c.d.clone();
            d.fail_move(format!("{error:#}"));
            self.save(&mut c, d)
                .with_context(|| format!("saving failure after VM move: {error:#}"))?;
        }
        result
    }

    fn move_phase(&self, epoch: u64, phase: MovePhase) -> Result<()> {
        let mut c = self.core.lock().unwrap();
        ensure!(
            c.d.epoch == epoch && c.d.leading(),
            "VM holder changed during move"
        );
        let mut d = c.d.clone();
        d.moving
            .as_mut()
            .context("VM move is no longer active")?
            .phase = phase;
        self.save(&mut c, d)
    }

    async fn move_guest(&self, to: &str, epoch: u64) -> Result<Value> {
        super::stop(&self.root, &self.id, false).await?;
        super::host::fence_previous_qemu(&store::vm_dir(&self.root, &self.id))?;
        self.move_phase(epoch, MovePhase::Checkpointing)?;
        let seq = self.capture().await?.seq;
        self.move_phase(epoch, MovePhase::Replicating)?;
        tokio::time::timeout(Duration::from_secs(600), async {
            loop {
                {
                    let mut c = self.core.lock().unwrap();
                    ensure!(c.d.epoch == epoch && c.d.leading(), "VM holder changed during move");
                    if c.have.get(to) == Some(&seq) && c.lease(self.now()) && self.online()
                        && let Ok(permit) = self.outbox.try_reserve() {
                        let nonce = rand::random();
                        let release = Message::Release { to: to.into(), seq };
                        let bytes = wire::encode(self, epoch, nonce, release.clone())?;
                        let mut d = c.d.clone();
                        d.release = Some(Release { epoch, to: to.into(), seq });
                        d.stale = true;
                        d.moving.as_mut().context("VM move is no longer active")?.phase = MovePhase::Released;
                        self.save(&mut c, d)?;
                        c.clear_lease();
                        c.renew = 0;
                        c.remote_running = false;
                        permit.send((to.into(), bytes));
                        for member in &self.config.members {
                            if member.node != self.me && member.node != to {
                                self.send(&member.node, epoch, nonce, release.clone());
                            }
                        }
                        return Ok::<_, anyhow::Error>(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }).await.context("VM move timed out before release while waiting for the target checkpoint and holder lease")??;
        tokio::time::timeout(Duration::from_secs(600), async {
            loop {
                {
                    let c = self.core.lock().unwrap();
                    if c.d.epoch > epoch && c.d.holder == to && c.remote_running {
                        return json!({"holder":to,"epoch":c.d.epoch,"seq":seq,"state":"running"});
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .context("VM released; timed out waiting for the target to report running")
    }

    async fn disable(&self) -> Result<Value> {
        let _op = self.op.lock().await;
        let epoch = {
            let mut c = self.core.lock().unwrap();
            let mut d = c.d.clone();
            if d.retired {
                // Keep answering Retire until the holder confirms receipt.
                return Ok(json!({"disabled":true,"state":"stale"}));
            }
            ensure!(
                d.leading() && (d.retiring || c.lease(self.now())),
                "disable the group on its current holder"
            );
            d.retiring = true;
            d.want_run = false;
            d.last_operation_error = None;
            self.save(&mut c, d)?;
            c.d.epoch
        };
        let result = self.disable_group(epoch).await;
        if let Err(error) = &result {
            let mut c = self.core.lock().unwrap();
            let mut d = c.d.clone();
            d.last_operation_error = Some(format!("{error:#}"));
            self.save(&mut c, d)
                .with_context(|| format!("saving failure after VM disable: {error:#}"))?;
        }
        result
    }

    async fn disable_group(&self, epoch: u64) -> Result<Value> {
        super::kill(&self.id).await?;
        super::host::fence_previous_qemu(&store::vm_dir(&self.root, &self.id))?;
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let done = {
                    let c = self.core.lock().unwrap();
                    ensure!(
                        c.d.leading() && c.d.epoch == epoch,
                        "VM holder changed during disable"
                    );
                    c.d.retired_peers.len() + 1 == self.config.members.len()
                };
                if done {
                    return Ok::<_, anyhow::Error>(());
                }
                self.broadcast(epoch, rand::random(), Message::Retire);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .context(
            "disable needs every member online; the source remains stopped, retry disable",
        )??;
        for member in &self.config.members {
            if member.node != self.me {
                let bytes = wire::encode(self, epoch, rand::random(), Message::RetireDone)?;
                // The durable retirement acknowledgement already fences this member.
                let _ = crate::net::send_direct_wait(&self.room, &member.node, bytes).await;
            }
        }
        let _vm = super::lock_vm(&self.id).await?;
        std::fs::rename(
            path(&self.root, &self.id),
            store::vm_dir(&self.root, &self.id).join("ha.disabled"),
        )?;
        self.closed.store(true, Ordering::Release);
        GROUPS.write().unwrap().remove(&self.id);
        Ok(json!({"disabled":true,"state":"stopped"}))
    }

    pub fn removable(&self) -> bool {
        self.core.lock().unwrap().d.retired
    }
    pub fn forget(&self) {
        self.closed.store(true, Ordering::Release);
        GROUPS.write().unwrap().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(roles: &[Role]) -> Config {
        let public = [vec![0xed, 1], vec![17; 32]].concat();
        Config {
            v: 1,
            server_id: "f".repeat(16),
            room: "vm-test".into(),
            owner_did: format!("did:key:z{}", bs58::encode(public).into_string()),
            members: roles
                .iter()
                .enumerate()
                .map(|(i, role)| Member {
                    node: format!("{i:016x}"),
                    role: *role,
                })
                .collect(),
            timing: Timing::default(),
        }
    }

    #[test]
    fn storage_and_voting_majorities() {
        use Role::*;
        for (roles, voting, copies) in [
            (vec![Host, Host, Witness], 2, 2),
            (vec![Host; 3], 2, 2),
            (vec![Host, Host, Host, Witness, Witness], 3, 3),
            (vec![Host; 2], 2, 2),
        ] {
            let cfg = config(&roles);
            assert_eq!(majority(roles.len()), voting);
            let mut have = HashMap::new();
            for m in &cfg.members {
                if m.role == Witness {
                    have.insert(m.node.clone(), 8);
                }
            }
            for (i, host) in cfg.hosts().enumerate() {
                have.insert(host.into(), 8);
                assert_eq!(committed(&cfg, &have, 8), i + 1 >= copies);
            }
            assert!(!committed(&cfg, &have, 7));
        }
    }

    #[test]
    fn vote_is_sticky_and_requires_the_committed_checkpoint() {
        use Role::*;
        let cfg = config(&[Host, Host, Witness]);
        let (a, b, w) = (
            cfg.members[0].node.clone(),
            cfg.members[1].node.clone(),
            cfg.members[2].node.clone(),
        );
        let mut d = Disk::new(cfg, w.clone(), a.clone(), 5);
        d.committed_seq = 8;
        assert!(!d.vote(&b, 5, 8, false, 45_000));
        assert!(!d.vote(&b, 6, 8, false, 39_999));
        assert!(!d.vote(&b, 6, 7, false, 40_000));
        assert!(!d.vote(&w, 6, 8, false, 40_000));
        assert!(d.vote(&b, 6, 8, false, 40_000));
        d.release = Some(Release {
            epoch: 5,
            to: b.clone(),
            seq: 9,
        });
        assert!(!d.vote(&b, 6, 8, true, 0));
        assert!(!d.vote(&b, 6, 9, false, 0));
        assert!(!d.vote(&b, 7, 9, true, 0));
        assert!(!d.vote(&a, 6, 9, true, 0));
        assert!(d.vote(&b, 6, 9, true, 0));
        assert!(!d.accepts(&a, 5));
        d.promised_epoch = 6;
        d.voted_for = b.clone();
        let recovered: Disk = serde_json::from_slice(&serde_json::to_vec(&d).unwrap()).unwrap();
        assert!(!recovered.vote(&a, 6, 9, true, 50_000));
        assert!(!recovered.accepts(&a, 6));
        assert!(recovered.accepts(&b, 6));
    }

    #[test]
    fn hosts_vote_only_for_candidates_with_at_least_their_complete_checkpoint() {
        use Role::*;
        let cfg = config(&[Host, Host, Witness]);
        let a = cfg.members[0].node.clone();
        let b = cfg.members[1].node.clone();
        let w = cfg.members[2].node.clone();
        let mut d = Disk::new(cfg, a.clone(), a, 5);
        d.committed_seq = 7;
        d.have_seq = 9;
        d.stale = true;
        assert!(!d.vote(&b, 6, 8, false, 40_000));
        assert!(d.vote(&b, 6, 9, false, 40_000));
        d.local_node = w;
        assert!(d.vote(&b, 6, 8, false, 40_000));
    }

    #[test]
    fn authoritative_offer_keeps_pinned_abandoned_manifests_out_of_restore() {
        let cfg = config(&[Role::Host; 2]);
        let holder = cfg.members[0].node.clone();
        let me = cfg.members[1].node.clone();
        let mut d = Disk::new(cfg, me.clone(), holder.clone(), 5);
        let id = d.config.server_id.clone();
        let root = std::env::temp_dir().join(format!("mistl-vm-ha-{}", store::new_server_id()));
        struct Scratch(PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _scratch = Scratch(root.clone());
        checkpoint::install_key(&root, &id, &[7; 32]).unwrap();
        let checkpoints = CheckpointStore::open(&root, &id).unwrap();
        let record = store::VmRecord {
            server_id: id.clone(),
            name: "ha-test".into(),
            owner_did: d.config.owner_did.clone(),
            allowed_dids: Vec::new(),
            memory_mb: 1024,
            cpus: 1,
            disk_gb: 20,
            base_sha256: "a".repeat(64),
            base_url: "https://example.test/base.img".into(),
            ssh_port: 22200,
            autostart: false,
            ssh_authorized_keys: Vec::new(),
            created_at: "2026-11-21T00:00:00Z".into(),
        };
        let disk = root.join("overlay");
        let mut manifests = Vec::new();
        for value in [b"old", b"new"] {
            std::fs::write(&disk, value).unwrap();
            let mut m = checkpoints.prepare(record.clone(), 4, Vec::new()).unwrap();
            checkpoints.capture(&disk, &mut m).unwrap();
            checkpoints.commit(&m).unwrap();
            manifests.push(m);
        }
        d.have_seq = 2;
        d.stale = true;
        assert!(!d.authoritative_offer(&me, 5, 1));
        assert!(!d.authoritative_offer(&holder, 4, 1));
        assert!(d.authoritative_offer(&holder, 5, 1));
        checkpoints.install_manifest(&manifests[0]).unwrap();
        d.install_offer(5, 1);
        let retention = d.retention(0);
        drop_abandoned(&root, &id, &checkpoints, &retention).unwrap();
        checkpoints.prune(1, |seq| retention.pins(seq)).unwrap();
        assert_eq!(d.have_seq, 1);
        assert!(!d.stale);
        assert_eq!(checkpoints.sequences().unwrap(), [2, 1]);
        assert_eq!(
            checkpoints
                .newest_complete(1, |m| d.restore_candidate(m))
                .unwrap(),
            1
        );
        assert!(
            checkpoints
                .read_encrypted_chunk(&manifests[1].chunks[0])
                .is_ok()
        );
        assert!(
            checkpoints
                .read_encrypted_chunk(&manifests[0].chunks[0])
                .is_ok()
        );
        d.install_offer(5, 2);
        assert!(!d.authoritative_offer(&holder, 5, 1));
    }

    #[test]
    fn retention_pins_have_committed_and_sixteen_recent_offers() {
        let cfg = config(&[Role::Host; 2]);
        let me = cfg.members[0].node.clone();
        let mut d = Disk::new(cfg, me.clone(), me, 1);
        d.committed_seq = 40;
        for seq in 1..=20 {
            d.have_seq = seq;
            d.record_offer(seq as i64);
        }
        d.have_seq = 2;
        let retention = d.retention(20);
        assert!(retention.pins(2));
        assert!(!retention.pins(4));
        assert!(retention.pins(5));
        assert_eq!(retention.offered.len(), 16);
        assert!(!retention.pins(39));
        assert!(retention.pins(40));
        assert!(retention.pins(41));
        assert!(d.retention(OFFER_PIN_MS + 4).pins(5));
        assert!(!d.retention(OFFER_PIN_MS + 5).pins(5));
        assert!(d.retention(OFFER_PIN_MS + 20).offered.is_empty());
    }

    #[test]
    fn move_rollback_restores_run_intent_only_before_release() {
        let cfg = config(&[Role::Host; 2]);
        let me = cfg.members[0].node.clone();
        let target = cfg.members[1].node.clone();
        let mut d = Disk::new(cfg, me.clone(), me, 3);
        d.want_run = true;
        d.begin_move(&target);
        assert!(!d.want_run);
        assert_eq!(d.moving.as_ref().unwrap().phase, MovePhase::Stopping);
        d.moving.as_mut().unwrap().phase = MovePhase::Replicating;
        let mut d: Disk = serde_json::from_slice(&serde_json::to_vec(&d).unwrap()).unwrap();
        assert!(d.recover_operation());
        assert!(d.moving.is_none() && d.leading() && d.want_run);
        assert!(
            d.last_operation_error
                .as_deref()
                .unwrap()
                .contains("before release")
        );

        d.begin_move(&target);
        d.moving.as_mut().unwrap().phase = MovePhase::Released;
        d.release = Some(Release {
            epoch: 3,
            to: target,
            seq: 42,
        });
        d.stale = true;
        d.fail_move("target unavailable".into());
        assert!(!d.want_run && !d.leading());
        assert_eq!(d.moving.as_ref().unwrap().phase, MovePhase::Released);
        assert_eq!(
            d.last_operation_error.as_deref(),
            Some("target unavailable")
        );
    }

    #[test]
    fn unhealthy_renewals_preserve_the_election_timer_and_vote_stickiness() {
        let cfg = config(&[Role::Host, Role::Host, Role::Witness]);
        let a = cfg.members[0].node.clone();
        let b = cfg.members[1].node.clone();
        let mut c = Core::new(Disk::new(cfg, b.clone(), a, 3));
        c.seen_lease(true, 1000);
        let jitter = c.jitter;
        c.retry_at = 2000;
        c.seen_lease(false, 40_000);
        assert_eq!(c.last_lease, 1000);
        assert_eq!(c.jitter, jitter);
        assert_eq!(c.retry_at, 2000);
        assert!(c.d.vote(&b, 4, 1, false, 41_000 - c.last_lease));
        c.seen_lease(true, 40_000);
        assert_eq!(c.last_lease, 40_000);
        assert_eq!(c.retry_at, 0);
        assert!(!c.d.vote(&b, 4, 1, false, 41_000 - c.last_lease));
    }

    #[test]
    fn fencing_uses_round_send_time_and_distinct_voters() {
        use Role::*;
        let cfg = config(&[Host, Host, Witness]);
        let a = cfg.members[0].node.clone();
        let b = cfg.members[1].node.clone();
        let mut c = Core::new(Disk::new(cfg, a.clone(), a.clone(), 3));
        let mut r = Round::new(&c.d, 100, 3);
        r.lease_ok = true;
        let nonce = r.nonce;
        c.rounds.push_back(r);
        c.acknowledge(&a, 3, nonce, 110);
        c.acknowledge(&b, 2, nonce, 110);
        assert_eq!(fence(&c, "running", 110), Fence::Pause);
        c.acknowledge(&b, 3, nonce, 150);
        assert_eq!(fence(&c, "paused", 150), Fence::Resume);
        assert_eq!(fence(&c, "running", 20_099), Fence::None);
        for (state, expiry) in [
            ("starting", Fence::Pause),
            ("running", Fence::Pause),
            ("stopping", Fence::Kill),
            ("paused", Fence::None),
        ] {
            assert_eq!(fence(&c, state, 20_100), expiry, "{state}");
            assert_eq!(fence(&c, state, 40_100), Fence::Kill, "{state}");
        }
        c.acknowledge(&b, 3, nonce, 20_101);
        assert!(!c.lease(20_101));
        let r = Round::new(&c.d, 20_200, 3);
        let nonce = r.nonce;
        c.rounds.push_back(r);
        c.acknowledge(&b, 3, nonce, 20_400);
        assert!(!c.lease(20_400));
        assert!(c.until > 20_400);
        let mut r = Round::new(&c.d, 20_400, 3);
        r.lease_ok = true;
        let nonce = r.nonce;
        c.rounds.push_back(r);
        c.acknowledge(&b, 3, nonce, 20_500);
        assert_eq!(fence(&c, "paused", 20_500), Fence::Resume);
        c.d.promised_epoch = 4;
        for state in ["starting", "running", "stopping", "paused"] {
            assert_eq!(fence(&c, state, 20_501), Fence::Kill, "{state}");
        }
        assert!(!c.writable(20_501));
    }

    #[test]
    fn holder_steps_down_after_three_consecutive_boot_failures() {
        let cfg = config(&[Role::Host; 2]);
        let a = cfg.members[0].node.clone();
        let mut d = Disk::new(cfg, a.clone(), a, 3);
        d.have_seq = 4;
        d.restore_seq = Some(4);
        d.want_run = true;
        d.boot_result(Some("earlier failure".into()));
        d.boot_result(None);
        assert!(d.last_boot_error.is_none());
        for failure in 1..=3 {
            d.boot_result(Some(format!("boot failure {failure}")));
            assert_eq!(d.leading(), failure < 3);
            assert_eq!(d.want_run, failure < 3);
        }
        assert_eq!(d.last_boot_error.as_deref(), Some("boot failure 3"));
        assert_eq!(d.restore_seq, None);
        let saved: Disk = serde_json::from_slice(&serde_json::to_vec(&d).unwrap()).unwrap();
        assert!(!saved.leading());
    }

    #[test]
    fn election_rank_excludes_witnesses_and_holder() {
        use Role::*;
        let cfg = config(&[Host, Witness, Host, Host]);
        let id = |i: usize| cfg.members[i].node.as_str();
        assert_eq!(cfg.delay(id(2), id(0)), Some(45_000));
        assert_eq!(cfg.delay(id(3), id(0)), Some(55_000));
        assert_eq!(cfg.delay(id(0), id(2)), Some(45_000));
        assert_eq!(cfg.delay(id(0), id(0)), None);
        assert_eq!(cfg.delay(id(1), id(0)), None);
    }

    #[test]
    fn election_jitter_stays_within_five_seconds() {
        for _ in 0..128 {
            assert!(election_jitter() <= 5000);
        }
    }

    #[test]
    fn tokens_round_trip_without_formatting_the_key() {
        let cfg = config(&[Role::Host; 2]);
        let key = [19; 32];
        let token = Token {
            holder: cfg.members[0].node.clone(),
            epoch: 1,
            config: cfg,
            key_b64: STANDARD.encode(key),
        };
        let secret = token.encode().unwrap();
        let (decoded, bytes) = Token::decode(&secret).unwrap();
        assert_eq!(bytes, key);
        assert_eq!(decoded.encode().unwrap(), secret);
        for text in [format!("{decoded}"), format!("{decoded:?}")] {
            assert!(!text.contains(&STANDARD.encode(key)) && !text.contains(&secret));
        }
        assert!(Token::decode(&"x".repeat(wire::CONTROL_LIMIT + 1)).is_err());
        let mut bad = decoded;
        bad.config.timing.failover_after_secs = 39;
        assert!(Token::decode(&bad.encode().unwrap()).is_err());
    }
}
