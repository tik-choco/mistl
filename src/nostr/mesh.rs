//! Room-scoped Nostr replication, with authenticated transport senders.
//!
//! The permanent callback only gates and queues input. A blocking worker
//! parses messages and verifies events. Bounded sync sessions assemble
//! size-limited batches without trusting identities supplied in JSON.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use rand::seq::IteratorRandom;
use serde_json::{Value, json};
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::event::Event;
use super::filter::Filter;
use super::interest;
use super::{Fresh, InterestSource, Origin, Relay, now_secs};
use crate::consensus::{ConsensusRole, Plane, RelayConsensus};
use crate::daemon::AppState;

const TAG: &str = "mistl-nostr-v1";
const MAX_WIRE_BYTES: usize = 60_000;
const MAX_INBOUND_BYTES: usize = 65_536;
const MAX_PEERS: usize = 1024;
const MAX_PAGE_EVENTS: usize = 500;
const MAX_SESSION_PAGES: usize = 20;
const MAX_SESSION_EVENTS: usize = 5000;
const SESSION_TTL: Duration = Duration::from_secs(60);
const PEER_IDLE: Duration = Duration::from_secs(600);
const SYNC_SPACING: Duration = Duration::from_secs(10);
const JOIN_DELAY: Duration = Duration::from_secs(3);
/// How long a joined peer may stay unverified (its DID hello not landed
/// yet, e.g. right after we rejoined the room) before its initial sync is
/// given up; until then it is retried every `JOIN_DELAY`.
const JOIN_WAIT: Duration = Duration::from_secs(60);
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const RING_ENTRIES: usize = 4096;
const RING_BYTES: usize = 8 * 1024 * 1024;
/// Leader-change catch-up pulls events from shortly before the previous
/// leader was last seen: anything older already went through it (or through
/// the join sync), so the full `sync_days` window would only resend known
/// events.
const CATCH_UP_MARGIN: u64 = 300;
/// Window of the periodic entropy sync and of the fetch-timeout fallback.
const ENTROPY_WINDOW: u64 = 86_400;
const INTEREST_DEBOUNCE: Duration = Duration::from_secs(1);
const INTEREST_REFRESH: Duration = Duration::from_secs(60);
const INTEREST_TTL: Duration = Duration::from_secs(180);
type Peer = (String, String);

static REGISTER: Once = Once::new();
static CURRENT: RwLock<Option<Arc<Inner>>> = RwLock::new(None);
static LIFECYCLE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug)]
enum Wire {
    Want(Vec<Filter>),
    Event(Value),
    Forward(Value),
    Sequence {
        epoch: String,
        n: u64,
        event: Value,
    },
    Fetch {
        epoch: String,
        from: u64,
    },
    Sync {
        since: u64,
        until: Option<u64>,
        limit: u32,
    },
    Batch {
        events: Vec<Value>,
        more: bool,
        until: Option<u64>,
    },
}

fn looks_like_mesh(bytes: &[u8]) -> bool {
    bytes.len() <= MAX_INBOUND_BYTES && bytes.windows(TAG.len()).any(|part| part == TAG.as_bytes())
}

fn decode(bytes: &[u8]) -> Option<Wire> {
    if !looks_like_mesh(bytes) {
        return None;
    }
    let value: Value = serde_json::from_slice(bytes).ok()?;
    let object = value.as_object()?;
    if object.get("t")?.as_str()? != TAG {
        return None;
    }
    let keys: &[&str] = match object.get("op")?.as_str()? {
        "ev" | "fwd" => &["t", "op", "ev"],
        "seq" => &["t", "op", "ep", "n", "ev"],
        "fetch" => &["t", "op", "ep", "from"],
        "sync" => &["t", "op", "since", "until", "limit"],
        "batch" => &["t", "op", "evs", "more", "until"],
        "want" => &["t", "op", "f"],
        _ => return None,
    };
    if object.len() != keys.len() || keys.iter().any(|key| !object.contains_key(*key)) {
        return None;
    }
    let until = || match object.get("until")? {
        Value::Null => Some(None),
        value => Some(Some(value.as_u64()?)),
    };
    let epoch = || {
        let ep = object.get("ep")?.as_str()?;
        (ep.len() == 16 && ep.bytes().all(|b| b.is_ascii_hexdigit())).then(|| ep.to_string())
    };
    match object.get("op")?.as_str()? {
        "want" => {
            let filters = object.get("f")?.as_array()?;
            if bytes.len() > MAX_WIRE_BYTES || filters.len() > interest::MAX_FILTERS {
                return None;
            }
            Some(Wire::Want(
                filters
                    .iter()
                    .map(Filter::parse)
                    .collect::<Result<_, _>>()
                    .ok()?,
            ))
        }
        "ev" => Some(Wire::Event(object.get("ev")?.clone())),
        "fwd" => Some(Wire::Forward(object.get("ev")?.clone())),
        "seq" => {
            let n = object.get("n")?.as_u64()?;
            if n == 0 {
                return None;
            }
            Some(Wire::Sequence {
                epoch: epoch()?,
                n,
                event: object.get("ev")?.clone(),
            })
        }
        "fetch" => {
            let from = object.get("from")?.as_u64()?;
            if from == 0 {
                return None;
            }
            Some(Wire::Fetch {
                epoch: epoch()?,
                from,
            })
        }
        "sync" => Some(Wire::Sync {
            since: object.get("since")?.as_u64()?,
            until: until()?,
            limit: u32::try_from(object.get("limit")?.as_u64()?).ok()?,
        }),
        "batch" => {
            let events = object.get("evs")?.as_array()?;
            if events.is_empty() || events.len() > MAX_PAGE_EVENTS {
                return None;
            }
            Some(Wire::Batch {
                events: events.clone(),
                more: object.get("more")?.as_bool()?,
                until: until()?,
            })
        }
        _ => None,
    }
}

fn event_bytes(event: &Event) -> Option<Vec<u8>> {
    event_envelope(json!({"t": TAG, "op": "ev", "ev": event}))
}

fn filter_json(filter: &Filter) -> Value {
    let mut value = json!({});
    for (key, field) in [
        ("ids", filter.ids.as_ref().map(|v| json!(v))),
        ("authors", filter.authors.as_ref().map(|v| json!(v))),
        ("kinds", filter.kinds.as_ref().map(|v| json!(v))),
        ("since", filter.since.map(|v| json!(v))),
        ("until", filter.until.map(|v| json!(v))),
        ("limit", filter.limit.map(|v| json!(v))),
    ] {
        if let Some(field) = field {
            value[key] = field;
        }
    }
    for (letter, values) in &filter.tags {
        value[format!("#{letter}")] = json!(values);
    }
    value
}

fn want_bytes(filters: &[Filter]) -> Option<Vec<u8>> {
    if filters.len() > interest::MAX_FILTERS {
        return None;
    }
    let values: Vec<_> = filters.iter().map(filter_json).collect();
    // Merged local filters can exceed the individual wire filter's limits.
    // Do not send a report the receiver cannot parse.
    if values.iter().any(|value| Filter::parse(value).is_err()) {
        return None;
    }
    let bytes = serde_json::to_vec(&json!({"t": TAG, "op": "want", "f": values})).ok()?;
    (bytes.len() <= MAX_WIRE_BYTES).then_some(bytes)
}

struct SentInterest {
    leader: String,
    filters: Arc<Vec<Filter>>,
    at: Instant,
}

fn should_send_interest(
    last: Option<&SentInterest>,
    leader: &str,
    filters: &[Filter],
    now: Instant,
) -> bool {
    let previous = last.filter(|last| last.leader == leader);
    if filters.is_empty() {
        return previous.is_some_and(|last| !last.filters.is_empty());
    }
    previous.is_none_or(|last| {
        last.filters.as_slice() != filters
            || now.saturating_duration_since(last.at) >= INTEREST_REFRESH
    })
}

fn event_envelope(value: Value) -> Option<Vec<u8>> {
    let bytes = serde_json::to_vec(&value).ok()?;
    if bytes.len() > MAX_WIRE_BYTES {
        debug!("nostr: event too large for mesh replication");
        return None;
    }
    Some(bytes)
}

#[derive(Debug, PartialEq, Eq)]
enum Route {
    Sequence,
    Forward(String),
    Broadcast,
    Skip,
}

fn route(leader: Option<&str>, self_id: &str, room: &str, origin: &Origin) -> Route {
    match leader {
        Some(peer) if peer == self_id => Route::Sequence,
        Some(peer) => {
            if matches!(origin, Origin::Mesh { room: source, peer: sender } if source == room && sender == peer)
            {
                Route::Skip
            } else {
                Route::Forward(peer.to_string())
            }
        }
        None if matches!(origin, Origin::Mesh { room: source, .. } if source == room) => {
            Route::Skip
        }
        None => Route::Broadcast,
    }
}

#[derive(Default)]
struct Ring {
    entries: VecDeque<(u64, Vec<u8>)>,
    bytes: usize,
}

impl Ring {
    fn push(&mut self, n: u64, bytes: Vec<u8>) {
        self.bytes += bytes.len();
        self.entries.push_back((n, bytes));
        while self.entries.len() > RING_ENTRIES || self.bytes > RING_BYTES {
            if let Some((_, bytes)) = self.entries.pop_front() {
                self.bytes -= bytes.len();
            }
        }
    }

    fn fetch(&self, from: u64) -> Vec<Vec<u8>> {
        if self.entries.front().is_none_or(|(first, _)| from < *first) {
            return Vec::new();
        }
        self.entries
            .iter()
            .filter(|(n, _)| *n >= from)
            .take(MAX_PAGE_EVENTS)
            .map(|(_, bytes)| bytes.clone())
            .collect()
    }
}

struct Sequencer {
    epoch: String,
    last: u64,
    ring: Ring,
}

impl Sequencer {
    fn new() -> Self {
        Self {
            epoch: format!("{:016x}", rand::random::<u64>()),
            last: 0,
            ring: Ring::default(),
        }
    }

    fn issue(&mut self, event: &Event) -> Option<Vec<u8>> {
        let n = self.last.checked_add(1)?;
        let bytes =
            event_envelope(json!({"t": TAG, "op": "seq", "ep": self.epoch, "n": n, "ev": event}))?;
        self.last = n;
        self.ring.push(n, bytes.clone());
        Some(bytes)
    }

    fn fetch(&self, leading: bool, epoch: &str, from: u64) -> Vec<Vec<u8>> {
        if leading && epoch == self.epoch {
            self.ring.fetch(from)
        } else {
            Vec::new()
        }
    }
}

struct PendingFetch {
    next: u64,
    end: u64,
    seen: BTreeSet<u64>,
    deadline: Instant,
    resync: bool,
}

#[derive(Default)]
struct Tracker {
    epoch: Option<String>,
    next: u64,
    last: u64,
    fetch: Option<PendingFetch>,
}

#[derive(Debug, PartialEq, Eq)]
enum TrackerAction {
    None,
    Sync,
    Fetch(u64),
}

impl Tracker {
    fn on_seq(&mut self, epoch: &str, n: u64, now: Instant) -> TrackerAction {
        if self.epoch.as_deref() != Some(epoch) {
            self.epoch = Some(epoch.to_string());
            self.next = n.saturating_add(1);
            self.last = n;
            self.fetch = None;
            return TrackerAction::Sync;
        }
        if let Some(fetch) = &mut self.fetch {
            if n >= fetch.next && n <= fetch.end && fetch.seen.len() < MAX_PAGE_EVENTS {
                fetch.seen.insert(n);
                while fetch.seen.remove(&fetch.next) {
                    fetch.next += 1;
                }
            }
            // A second gap cannot start another fetch while this one is in flight.
            // Keep its timeout armed so a sync also repairs that later gap.
            fetch.resync |= n > self.next;
            if fetch.next > fetch.end && !fetch.resync {
                self.fetch = None;
            }
        }
        if n < self.next {
            return TrackerAction::None;
        }
        let missing = self.next;
        self.next = n.saturating_add(1);
        self.last = n;
        if n > missing && self.fetch.is_none() {
            self.fetch = Some(PendingFetch {
                next: missing,
                end: n - 1,
                seen: BTreeSet::new(),
                deadline: now + FETCH_TIMEOUT,
                resync: false,
            });
            TrackerAction::Fetch(missing)
        } else {
            TrackerAction::None
        }
    }

    fn expired(&mut self, now: Instant) -> bool {
        if self
            .fetch
            .as_ref()
            .is_some_and(|fetch| now >= fetch.deadline)
        {
            self.fetch = None;
            true
        } else {
            false
        }
    }
}

#[derive(Default)]
struct RoomState {
    leader: Option<String>,
    sequencer: Option<Sequencer>,
    tracker: Tracker,
    aggregate: bool,
    /// Unix seconds a leader was last known in this room.
    leader_seen_at: Option<u64>,
    /// `since` of the catch-up sync after the latest leader change.
    catch_up: Option<u64>,
    interest_sent: Option<SentInterest>,
    /// Transport senders with live interest, bounded per room by MAX_PEERS.
    interest_peers: HashMap<String, Instant>,
}

impl RoomState {
    fn update(&mut self, leader: Option<String>, self_id: &str, now: u64) -> bool {
        let previous_seen = self.leader_seen_at;
        if leader.is_some() {
            self.leader_seen_at = Some(now);
        }
        if self.leader == leader {
            return false;
        }
        self.catch_up = Some(previous_seen.unwrap_or(now).saturating_sub(CATCH_UP_MARGIN));
        self.interest_sent = None;
        self.leader = leader;
        self.tracker = Tracker::default();
        let leading = self.leader.as_deref() == Some(self_id);
        self.sequencer = leading.then(Sequencer::new);
        self.aggregate = leading;
        leading
    }

    fn on_seq(
        &mut self,
        leader: Option<&str>,
        from: &str,
        epoch: &str,
        n: u64,
        now: Instant,
    ) -> TrackerAction {
        if leader == Some(from) {
            self.tracker.on_seq(epoch, n, now)
        } else {
            TrackerAction::None
        }
    }
}

fn sync_bytes(since: u64, until: Option<u64>) -> Vec<u8> {
    json!({"t": TAG, "op": "sync", "since": since, "until": until,
        "limit": MAX_PAGE_EVENTS})
    .to_string()
    .into_bytes()
}

const BATCH_PREFIX: &[u8] = b"{\"t\":\"mistl-nostr-v1\",\"op\":\"batch\",\"evs\":[";

fn batch_suffix(more: bool, until: Option<u64>) -> Vec<u8> {
    format!(
        "],\"more\":{more},\"until\":{}}}",
        until
            .map(|n| n.to_string())
            .unwrap_or_else(|| "null".into())
    )
    .into_bytes()
}

fn finish_batch(events: &[Vec<u8>], more: bool, until: Option<u64>) -> Vec<u8> {
    let mut bytes = BATCH_PREFIX.to_vec();
    for (index, event) in events.iter().enumerate() {
        if index != 0 {
            bytes.push(b',');
        }
        bytes.extend_from_slice(event);
    }
    bytes.extend(batch_suffix(more, until));
    bytes
}

/// Intermediate fragments use `more: true, until: null`; only the final
/// fragment carries a page cursor (or `more: false`). No empty batches are sent.
fn pack_batches(events: &[Event], full: bool) -> Vec<Vec<u8>> {
    let until = full
        .then(|| events.iter().map(|event| event.created_at).min())
        .flatten();
    let overhead = BATCH_PREFIX.len()
        + batch_suffix(true, None)
            .len()
            .max(batch_suffix(full, until).len());
    let mut batches = Vec::new();
    let mut chunk = Vec::new();
    let mut size = overhead;
    for event in events {
        if event_bytes(event).is_none() {
            continue;
        }
        let Ok(bytes) = serde_json::to_vec(event) else {
            continue;
        };
        if overhead + bytes.len() > MAX_WIRE_BYTES {
            debug!(id = %event.id, "nostr: event too large for mesh batch envelope");
            continue;
        }
        let comma = usize::from(!chunk.is_empty());
        if size + comma + bytes.len() > MAX_WIRE_BYTES {
            batches.push(finish_batch(&chunk, true, None));
            chunk.clear();
            size = overhead;
        }
        size += usize::from(!chunk.is_empty()) + bytes.len();
        chunk.push(bytes);
    }
    if !chunk.is_empty() {
        batches.push(finish_batch(&chunk, full, until));
    }
    batches
}

struct Bucket {
    tokens: f64,
    updated: Instant,
}

impl Bucket {
    fn new(cap: f64, now: Instant) -> Self {
        Self {
            tokens: cap,
            updated: now,
        }
    }

    fn take(&mut self, count: usize, cap: f64, refill: f64, now: Instant) -> bool {
        self.tokens = (self.tokens
            + now.saturating_duration_since(self.updated).as_secs_f64() * refill)
            .min(cap);
        self.updated = now;
        if self.tokens < count as f64 {
            return false;
        }
        self.tokens -= count as f64;
        true
    }
}

struct PeerRate {
    events: Bucket,
    leader_events: Bucket,
    syncs: Bucket,
    last: Instant,
}

#[derive(Default)]
struct Rates(HashMap<Peer, PeerRate>);

impl Rates {
    fn take(&mut self, key: &Peer, count: usize, sync: bool, now: Instant) -> bool {
        self.take_budget(key, count, sync, false, now)
    }

    fn take_budget(
        &mut self,
        key: &Peer,
        count: usize,
        sync: bool,
        leader: bool,
        now: Instant,
    ) -> bool {
        if !self.0.contains_key(key) && self.0.len() >= MAX_PEERS {
            self.0
                .retain(|_, rate| now.saturating_duration_since(rate.last) < PEER_IDLE);
            if self.0.len() >= MAX_PEERS {
                return false;
            }
        }
        let rate = self.0.entry(key.clone()).or_insert_with(|| PeerRate {
            events: Bucket::new(2000.0, now),
            leader_events: Bucket::new(16000.0, now),
            syncs: Bucket::new(3.0, now),
            last: now,
        });
        rate.last = now;
        if sync {
            rate.syncs.take(count, 3.0, 0.1, now)
        } else if leader {
            rate.leader_events.take(count, 16000.0, 1600.0, now)
        } else {
            rate.events.take(count, 2000.0, 200.0, now)
        }
    }
}

struct Session {
    since: u64,
    until: Option<u64>,
    pages: usize,
    events: usize,
    page_events: usize,
    deadline: Instant,
    /// A continuation waits for the remote sync bucket to refill.
    due: Option<Instant>,
}

impl Session {
    fn new(since: u64, now: Instant) -> Self {
        Self {
            since,
            until: None,
            pages: 0,
            events: 0,
            page_events: 0,
            deadline: now + SESSION_TTL,
            due: None,
        }
    }

    fn accept(&mut self, count: usize, now: Instant) -> bool {
        if now >= self.deadline
            || self.due.is_some()
            || self.pages >= MAX_SESSION_PAGES
            || self.events + count > MAX_SESSION_EVENTS
            || self.page_events + count > MAX_PAGE_EVENTS
        {
            return false;
        }
        self.events += count;
        self.page_events += count;
        true
    }

    fn finish(&mut self, more: bool, until: Option<u64>, now: Instant) -> bool {
        self.pages += 1;
        let Some(until) = until.filter(|cursor| {
            *cursor >= self.since && self.until.is_none_or(|previous| *cursor < previous)
        }) else {
            return false;
        };
        if !more || self.pages >= MAX_SESSION_PAGES || self.events >= MAX_SESSION_EVENTS {
            return false;
        }
        self.until = Some(until);
        self.page_events = 0;
        self.due = Some(now + SYNC_SPACING);
        self.deadline = now + SYNC_SPACING + SESSION_TTL;
        true
    }
}

struct Inbound {
    room: String,
    from: String,
    bytes: Vec<u8>,
}
struct Reply {
    room: String,
    peer: String,
    bytes: Vec<u8>,
}

struct Inner {
    relay: Arc<Relay>,
    rooms: Vec<String>,
    node_id: String,
    consensus: HashMap<String, Arc<RelayConsensus>>,
    cluster: Mutex<HashMap<String, RoomState>>,
    interest_changed: Notify,
    gateway: watch::Sender<bool>,
    runtime: Handle,
    inbound: mpsc::Sender<Inbound>,
    active: AtomicBool,
    rates: Mutex<Rates>,
    sessions: Mutex<HashMap<Peer, Session>>,
    /// Pending initial syncs: peer -> (next attempt, give up at).
    joins: Mutex<HashMap<Peer, (Instant, Instant)>>,
    sent: AtomicU64,
    received: AtomicU64,
    forwarded: AtomicU64,
    sync_requests: AtomicU64,
    sync_served: AtomicU64,
}

fn admitted(room: &str, peer: &str) -> bool {
    crate::net::peer_auth::admitted_did(room, peer).is_some()
}

impl Inner {
    fn new(relay: Arc<Relay>, rooms: Vec<String>, inbound: mpsc::Sender<Inbound>) -> Self {
        let cluster = rooms
            .iter()
            .map(|room| (room.clone(), RoomState::default()))
            .collect();
        Self {
            relay,
            rooms,
            node_id: String::new(),
            consensus: HashMap::new(),
            cluster: Mutex::new(cluster),
            interest_changed: Notify::new(),
            gateway: watch::channel(false).0,
            runtime: Handle::current(),
            inbound,
            active: AtomicBool::new(true),
            rates: Mutex::new(Rates::default()),
            sessions: Mutex::new(HashMap::new()),
            joins: Mutex::new(HashMap::new()),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
            sync_requests: AtomicU64::new(0),
            sync_served: AtomicU64::new(0),
        }
    }

    fn leader(&self, room: &str) -> Option<String> {
        self.consensus
            .get(room)
            .and_then(|consensus| consensus.leader())
    }

    fn refresh_room(&self, room: &str) {
        self.update_room(room, self.leader(room));
        self.gateway.send_if_modified(|gateway| {
            let value = self.active.load(Ordering::Acquire)
                && self
                    .consensus
                    .values()
                    .any(|consensus| consensus.leader().as_deref() == Some(self.node_id.as_str()));
            if *gateway == value {
                false
            } else {
                *gateway = value;
                true
            }
        });
    }

    fn update_room(&self, room: &str, leader: Option<String>) {
        let mut cluster = self.cluster.lock().expect("nostr mesh cluster poisoned");
        if let Some(state) = cluster.get_mut(room) {
            let changed = state.leader != leader;
            state.update(leader, &self.node_id, now_secs());
            if state.leader.as_deref() != Some(self.node_id.as_str()) {
                self.clear_peer_interest(room, state);
            }
            if changed {
                self.interest_changed.notify_one();
            }
        }
    }

    fn clear_peer_interest(&self, room: &str, state: &mut RoomState) {
        for (peer, _) in state.interest_peers.drain() {
            self.relay.set_interest(
                InterestSource::Peer {
                    room: room.to_string(),
                    peer,
                },
                &[],
                None,
            );
        }
    }

    fn receive_interest(&self, room: &str, from: &str, filters: &[Filter], now: Instant) {
        let mut cluster = self.cluster.lock().expect("nostr mesh cluster poisoned");
        let Some(state) = cluster.get_mut(room) else {
            return;
        };
        if !self.running_in(room) || state.leader.as_deref() != Some(self.node_id.as_str()) {
            return;
        }
        let nonempty = filters
            .iter()
            .any(|filter| interest::mirrored(filter, &self.relay.cfg().mirror_kinds));
        if nonempty {
            if !state.interest_peers.contains_key(from) && state.interest_peers.len() >= MAX_PEERS {
                return;
            }
            state
                .interest_peers
                .insert(from.to_string(), now + INTEREST_TTL);
        } else {
            state.interest_peers.remove(from);
        }
        self.relay.set_interest(
            InterestSource::Peer {
                room: room.to_string(),
                peer: from.to_string(),
            },
            filters,
            Some(INTEREST_TTL),
        );
    }

    fn prune_interest(&self, now: Instant) {
        self.relay.prune_interest(now);
        let mut cluster = self.cluster.lock().expect("nostr mesh cluster poisoned");
        for (room, state) in cluster.iter_mut() {
            state.interest_peers.retain(|peer, expires| {
                if now < *expires {
                    true
                } else {
                    self.relay.set_interest(
                        InterestSource::Peer {
                            room: room.clone(),
                            peer: peer.clone(),
                        },
                        &[],
                        None,
                    );
                    false
                }
            });
        }
    }

    fn since(&self) -> u64 {
        now_secs().saturating_sub(self.relay.cfg().sync_days.saturating_mul(86400))
    }

    /// `since` for a leader-change catch-up in `room` (see [`CATCH_UP_MARGIN`]).
    fn catch_up_since(&self, room: &str) -> u64 {
        let catch_up = self
            .cluster
            .lock()
            .expect("nostr mesh cluster poisoned")
            .get(room)
            .and_then(|state| state.catch_up);
        catch_up
            .unwrap_or_else(|| now_secs().saturating_sub(CATCH_UP_MARGIN))
            .max(self.since())
    }

    fn leadership_replies(&self, room: &str, now: Instant) -> Vec<Reply> {
        self.refresh_room(room);
        let aggregate = {
            let mut cluster = self.cluster.lock().expect("nostr mesh cluster poisoned");
            cluster
                .get_mut(room)
                .is_some_and(|state| std::mem::take(&mut state.aggregate))
        };
        if !aggregate {
            return Vec::new();
        }
        let since = self.catch_up_since(room);
        self.consensus
            .get(room)
            .map(|consensus| consensus.relay_peers())
            .unwrap_or_default()
            .into_iter()
            .filter(|peer| peer != &self.node_id && admitted(room, peer))
            .filter_map(|peer| self.begin_sync((room.to_string(), peer), since, now))
            .collect()
    }

    fn running_in(&self, room: &str) -> bool {
        self.active.load(Ordering::Acquire) && self.rooms.iter().any(|joined| joined == room)
    }

    fn queue_join(&self, room: &str, peer: &str) {
        let Ok(mut joins) = self.joins.try_lock() else {
            return;
        };
        let key = (room.to_string(), peer.to_string());
        if joins.len() < MAX_PEERS {
            joins
                .entry(key)
                .or_insert_with(|| (Instant::now() + JOIN_DELAY, Instant::now() + JOIN_WAIT));
        }
    }

    /// Called before allocating the queued payload; no JSON or crypto here.
    fn gate(
        &self,
        room: &str,
        from: &str,
        bytes: &[u8],
        admitted: &dyn Fn(&str, &str) -> bool,
    ) -> bool {
        if !self.running_in(room) || !looks_like_mesh(bytes) || !admitted(room, from) {
            return false;
        }
        let Ok(mut rates) = self.rates.try_lock() else {
            return false;
        };
        rates.take_budget(
            &(room.to_string(), from.to_string()),
            1,
            false,
            self.leader(room).as_deref() == Some(from),
            Instant::now(),
        )
    }

    /// All expensive inbound work runs on the blocking pool, also in tests.
    fn process(
        &self,
        room: &str,
        from: &str,
        bytes: &[u8],
        admitted: &dyn Fn(&str, &str) -> bool,
    ) -> Vec<Reply> {
        if !self.running_in(room) || !admitted(room, from) {
            return Vec::new();
        }
        let Some(wire) = decode(bytes) else {
            return Vec::new();
        };
        let key = (room.to_string(), from.to_string());
        let now = Instant::now();
        match wire {
            Wire::Want(filters) => {
                self.refresh_room(room);
                self.receive_interest(room, from, &filters, now);
                Vec::new()
            }
            Wire::Event(event) | Wire::Forward(event) => {
                self.submit(room, from, &event);
                Vec::new()
            }
            Wire::Sequence { epoch, n, event } => {
                self.submit(room, from, &event);
                self.refresh_room(room);
                let action = {
                    let mut cluster = self.cluster.lock().expect("nostr mesh cluster poisoned");
                    match cluster.get_mut(room) {
                        Some(state) => {
                            let leader = state.leader.clone();
                            state.on_seq(leader.as_deref(), from, &epoch, n, now)
                        }
                        _ => TrackerAction::None,
                    }
                };
                match action {
                    TrackerAction::None => Vec::new(),
                    TrackerAction::Sync => self
                        .begin_sync(key, self.catch_up_since(room), now)
                        .into_iter()
                        .collect(),
                    TrackerAction::Fetch(first) => vec![Reply {
                        room: room.to_string(),
                        peer: from.to_string(),
                        bytes: json!({"t": TAG, "op": "fetch", "ep": epoch, "from": first})
                            .to_string()
                            .into_bytes(),
                    }],
                }
            }
            Wire::Fetch { epoch, from: first } => {
                if self.leader(room).as_deref() != Some(self.node_id.as_str())
                    || !self
                        .rates
                        .lock()
                        .expect("nostr mesh rates poisoned")
                        .take(&key, 1, true, now)
                {
                    return Vec::new();
                }
                self.refresh_room(room);
                let cluster = self.cluster.lock().expect("nostr mesh cluster poisoned");
                cluster
                    .get(room)
                    .and_then(|state| state.sequencer.as_ref())
                    .map(|sequence| {
                        sequence.fetch(
                            self.leader(room).as_deref() == Some(self.node_id.as_str()),
                            &epoch,
                            first,
                        )
                    })
                    .unwrap_or_default()
                    .into_iter()
                    .map(|bytes| Reply {
                        room: room.to_string(),
                        peer: from.to_string(),
                        bytes,
                    })
                    .collect()
            }
            Wire::Sync {
                since,
                until,
                limit,
            } => {
                if until.is_some_and(|until| until < since)
                    || !self
                        .rates
                        .lock()
                        .expect("nostr mesh rates poisoned")
                        .take(&key, 1, true, now)
                {
                    return Vec::new();
                }
                let limit = (limit as usize).min(MAX_PAGE_EVENTS);
                if limit == 0 {
                    return Vec::new();
                }
                let events = self.relay.query(
                    &[Filter {
                        since: Some(since),
                        until,
                        limit: Some(limit),
                        ..Filter::default()
                    }],
                    MAX_PAGE_EVENTS,
                    now_secs(),
                );
                self.sync_served.fetch_add(1, Ordering::Relaxed);
                pack_batches(&events, events.len() == limit)
                    .into_iter()
                    .map(|bytes| Reply {
                        room: room.to_string(),
                        peer: from.to_string(),
                        bytes,
                    })
                    .collect()
            }
            Wire::Batch {
                events,
                more,
                until,
            } => {
                let mut sessions = self.sessions.lock().expect("nostr mesh sessions poisoned");
                let Some(session) = sessions.get_mut(&key) else {
                    return Vec::new();
                };
                if !session.accept(events.len(), now) {
                    sessions.remove(&key);
                    return Vec::new();
                }
                // The callback has already charged one token for this message.
                if !self.rates.lock().expect("nostr mesh rates poisoned").take(
                    &key,
                    events.len().saturating_sub(1),
                    false,
                    now,
                ) {
                    sessions.remove(&key);
                    return Vec::new();
                }
                if (!more || until.is_some()) && !session.finish(more, until, now) {
                    sessions.remove(&key);
                }
                drop(sessions);
                for event in events {
                    if !self.running_in(room) {
                        break;
                    }
                    self.submit(room, from, &event);
                }
                Vec::new()
            }
        }
    }

    fn submit(&self, room: &str, from: &str, value: &Value) {
        let Ok(event) = Event::parse(value) else {
            return;
        };
        if self.relay.knows(&event.id) {
            return;
        }
        let result = self.relay.submit(
            event,
            &Origin::Mesh {
                room: room.to_string(),
                peer: from.to_string(),
            },
            now_secs(),
        );
        if result.fresh {
            self.received.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn begin_sync(&self, key: Peer, since: u64, now: Instant) -> Option<Reply> {
        let mut sessions = self.sessions.lock().expect("nostr mesh sessions poisoned");
        sessions.retain(|_, session| now < session.deadline);
        if sessions.contains_key(&key) || sessions.len() >= MAX_PEERS {
            return None;
        }
        sessions.insert(key.clone(), Session::new(since, now));
        Some(Reply {
            room: key.0,
            peer: key.1,
            bytes: sync_bytes(since, None),
        })
    }

    fn continuations(&self, now: Instant) -> Vec<Reply> {
        let mut sessions = self.sessions.lock().expect("nostr mesh sessions poisoned");
        sessions.retain(|_, session| now < session.deadline);
        let mut replies = Vec::new();
        for ((room, peer), session) in sessions.iter_mut() {
            if session.due.is_some_and(|due| now >= due) {
                session.due = None;
                session.deadline = now + SESSION_TTL;
                replies.push(Reply {
                    room: room.clone(),
                    peer: peer.clone(),
                    bytes: sync_bytes(session.since, session.until),
                });
            }
        }
        replies
    }

    async fn send(&self, reply: Reply) {
        if !self.running_in(&reply.room) || !admitted(&reply.room, &reply.peer) {
            return;
        }
        let is_sync = matches!(decode(&reply.bytes), Some(Wire::Sync { .. }));
        match tokio::time::timeout(
            SEND_TIMEOUT,
            crate::net::send_direct(&reply.room, &reply.peer, reply.bytes),
        )
        .await
        {
            Ok(Ok(())) => {
                if is_sync {
                    self.sync_requests.fetch_add(1, Ordering::Relaxed);
                }
            }
            result => {
                debug!(room = %reply.room, peer = %reply.peer, ?result, "nostr: mesh direct send failed");
                if is_sync {
                    self.sessions
                        .lock()
                        .expect("nostr mesh sessions poisoned")
                        .remove(&(reply.room, reply.peer));
                }
            }
        }
    }
}

fn register() {
    REGISTER.call_once(|| {
        crate::net::register_room_handler(|event, room, from, bytes| {
            let Ok(current) = CURRENT.try_read() else {
                return;
            };
            let Some(inner) = current.as_ref() else {
                return;
            };
            if !inner.running_in(room) {
                return;
            }
            if event == crate::net::EVENT_PEER_CONNECTED {
                // Admission is checked after the DID hello has had time to arrive.
                inner.queue_join(room, from);
            } else if event == crate::net::EVENT_RAW && inner.gate(room, from, bytes, &admitted) {
                let _ = inner.inbound.try_send(Inbound {
                    room: room.to_string(),
                    from: from.to_string(),
                    bytes: bytes.to_vec(),
                });
            }
        })
    });
}

async fn receive(inner: Arc<Inner>, mut inbound: mpsc::Receiver<Inbound>) {
    while let Some(message) = inbound.recv().await {
        let worker = inner.clone();
        let result = inner
            .runtime
            .spawn_blocking(move || {
                worker.process(&message.room, &message.from, &message.bytes, &admitted)
            })
            .await;
        match result {
            Ok(replies) => {
                for reply in replies {
                    inner.send(reply).await;
                }
            }
            Err(error) => debug!(%error, "nostr: mesh inbound worker failed"),
        }
    }
}

async fn broadcast(inner: Arc<Inner>, mut outbound: mpsc::Receiver<Fresh>) {
    while let Some(Fresh { event, origin }) = outbound.recv().await {
        for room in &inner.rooms {
            if !inner.running_in(room) {
                return;
            }
            inner.refresh_room(room);
            let (decision, sequence) = {
                let mut cluster = inner.cluster.lock().expect("nostr mesh cluster poisoned");
                let Some(state) = cluster.get_mut(room) else {
                    continue;
                };
                let decision = route(state.leader.as_deref(), &inner.node_id, room, &origin);
                let bytes = if decision == Route::Sequence {
                    state
                        .sequencer
                        .as_mut()
                        .and_then(|sequence| sequence.issue(&event))
                } else {
                    None
                };
                (decision, bytes)
            };
            let bytes = match decision {
                Route::Skip => continue,
                Route::Sequence => sequence,
                Route::Broadcast => event_bytes(&event),
                Route::Forward(peer) => {
                    let Some(bytes) = event_envelope(json!({"t": TAG, "op": "fwd", "ev": &*event}))
                    else {
                        continue;
                    };
                    if admitted(room, &peer)
                        && matches!(
                            tokio::time::timeout(
                                SEND_TIMEOUT,
                                crate::net::send_direct(room, &peer, bytes)
                            )
                            .await,
                            Ok(Ok(()))
                        )
                    {
                        inner.forwarded.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    event_bytes(&event)
                }
            };
            if let Some(bytes) = bytes {
                match tokio::time::timeout(SEND_TIMEOUT, crate::net::send_broadcast(room, bytes))
                    .await
                {
                    Ok(Ok(())) => {
                        inner.sent.fetch_add(1, Ordering::Relaxed);
                    }
                    result => debug!(%room, ?result, "nostr: mesh broadcast failed"),
                }
            }
        }
    }
}

async fn watch_leadership(
    inner: Arc<Inner>,
    room: String,
    mut view: watch::Receiver<crate::consensus::ConsensusView>,
) {
    loop {
        // Mark this view observed before reading the live consensus state.
        view.borrow_and_update();
        // Aggregation is sent by the sync loop; transport timeouts must not
        // delay reacting to gateway changes on this watch.
        inner.refresh_room(&room);
        if view.changed().await.is_err() || !inner.running_in(&room) {
            return;
        }
    }
}

async fn report_interest(inner: Arc<Inner>) {
    let mut local = inner.relay.local_interest();
    let mut filters = local.borrow_and_update().clone();
    let mut pending = None;
    let mut tick = tokio::time::interval(INTEREST_DEBOUNCE);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let leadership_changed = tokio::select! {
            changed = local.changed() => {
                if changed.is_err() {
                    return;
                }
                filters = local.borrow_and_update().clone();
                pending = Some(Instant::now() + INTEREST_DEBOUNCE);
                continue;
            }
            _ = tick.tick() => false,
            _ = inner.interest_changed.notified() => true,
        };
        if !inner.active.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let debouncing = pending.is_some_and(|due| now < due);
        if !debouncing {
            pending = None;
        }
        let mut encoded = None;
        for room in &inner.rooms {
            inner.refresh_room(room);
            let leader = {
                let cluster = inner.cluster.lock().expect("nostr mesh cluster poisoned");
                let Some(state) = cluster.get(room) else {
                    continue;
                };
                let Some(leader) = state.leader.as_deref().filter(|id| *id != inner.node_id) else {
                    continue;
                };
                // A new leader learns our current interest even during a burst.
                if (debouncing && !(leadership_changed && state.interest_sent.is_none()))
                    || !should_send_interest(state.interest_sent.as_ref(), leader, &filters, now)
                {
                    continue;
                }
                leader.to_string()
            };
            if !admitted(room, &leader) {
                continue;
            }
            let Some(bytes) = encoded.get_or_insert_with(|| want_bytes(&filters)).as_ref() else {
                debug!(%room, "nostr: local interest exceeds mesh wire limits");
                continue;
            };
            if matches!(
                tokio::time::timeout(
                    SEND_TIMEOUT,
                    crate::net::send_direct(room, &leader, bytes.clone()),
                )
                .await,
                Ok(Ok(()))
            ) {
                let mut cluster = inner.cluster.lock().expect("nostr mesh cluster poisoned");
                if let Some(state) = cluster.get_mut(room)
                    && state.leader.as_deref() == Some(leader.as_str())
                {
                    state.interest_sent = Some(SentInterest {
                        leader,
                        filters: filters.clone(),
                        at: Instant::now(),
                    });
                }
            }
        }
    }
}

async fn synchronize(inner: Arc<Inner>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_entropy = Instant::now() + Duration::from_secs(600);
    loop {
        tick.tick().await;
        if !inner.active.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        inner.prune_interest(now);
        for room in &inner.rooms {
            for reply in inner.leadership_replies(room, now) {
                inner.send(reply).await;
            }
            let catch_up = {
                let mut cluster = inner.cluster.lock().expect("nostr mesh cluster poisoned");
                cluster.get_mut(room).and_then(|state| {
                    state
                        .tracker
                        .expired(now)
                        .then(|| state.leader.clone())
                        .flatten()
                })
            };
            if let Some(peer) = catch_up
                && let Some(reply) = inner.begin_sync(
                    (room.clone(), peer),
                    now_secs().saturating_sub(ENTROPY_WINDOW).max(inner.since()),
                    now,
                )
            {
                inner.send(reply).await;
            }
        }
        let joins: Vec<(Peer, Instant)> = {
            let mut joins = inner.joins.lock().expect("nostr mesh joins poisoned");
            let ready = joins
                .iter()
                .filter(|(_, (due, _))| now >= *due)
                .map(|(key, (_, give_up))| (key.clone(), *give_up))
                .collect::<Vec<_>>();
            for (key, _) in &ready {
                joins.remove(key);
            }
            ready
        };
        for (key, give_up) in joins {
            if admitted(&key.0, &key.1) {
                let preferred = inner
                    .leader(&key.0)
                    .filter(|leader| leader != &inner.node_id && admitted(&key.0, leader));
                let key = (key.0, preferred.unwrap_or(key.1));
                if let Some(reply) = inner.begin_sync(key, inner.since(), now) {
                    inner.send(reply).await;
                }
            } else if now < give_up {
                // Not verified yet: the hello usually lands within seconds.
                let mut joins = inner.joins.lock().expect("nostr mesh joins poisoned");
                if joins.len() < MAX_PEERS {
                    joins.insert(key, (now + JOIN_DELAY, give_up));
                }
            }
        }
        for reply in inner.continuations(now) {
            inner.send(reply).await;
        }
        if now >= next_entropy {
            next_entropy = now + Duration::from_secs(600);
            let peers = crate::net::peer_auth::verified_peers();
            for room in &inner.rooms {
                let peer = inner
                    .leader(room)
                    .filter(|leader| leader != &inner.node_id && admitted(room, leader))
                    .or_else(|| {
                        peers
                            .iter()
                            .filter(|(joined, peer, _)| joined == room && admitted(room, peer))
                            .choose(&mut rand::thread_rng())
                            .map(|(_, peer, _)| peer.clone())
                    });
                if let Some(peer) = peer
                    && let Some(reply) = inner.begin_sync(
                        (room.clone(), peer),
                        now_secs().saturating_sub(ENTROPY_WINDOW).max(inner.since()),
                        now,
                    )
                {
                    inner.send(reply).await;
                }
            }
        }
    }
}

/// A running room mesh. The permanent handler refers only to `CURRENT`.
pub struct Mesh {
    inner: Arc<Inner>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Mesh {
    /// Joins `rooms`, starts replicating: forwards every event from
    /// `outbound` to the rooms and submits peers' events to `relay`.
    pub async fn start(
        state: &Arc<AppState>,
        relay: Arc<Relay>,
        rooms: Vec<String>,
        outbound: mpsc::Receiver<Fresh>,
    ) -> Result<Arc<Mesh>> {
        let _lifecycle = LIFECYCLE.lock().await;
        if CURRENT
            .read()
            .expect("nostr mesh current poisoned")
            .is_some()
        {
            bail!("nostr: mesh is already running")
        }
        register();
        if rooms.len() > MAX_PEERS {
            bail!("nostr: too many mesh rooms");
        }
        let node_id = crate::identity::current(state).await?.node_id();
        let mut joined = Vec::new();
        let mut consensus = HashMap::new();
        for room in rooms {
            if joined.contains(&room) {
                continue;
            }
            match crate::net::ensure_started(state, room.clone()).await {
                Ok(_) => {
                    match RelayConsensus::start_plane(state, room.clone(), Plane::Nostr).await {
                        Ok(instance) => {
                            consensus.insert(room.clone(), instance);
                        }
                        Err(error) => {
                            warn!(%room, %error, "nostr: consensus unavailable, using mesh fallback")
                        }
                    }
                    joined.push(room);
                }
                Err(error) => warn!(%room, %error, "nostr: could not join mesh room"),
            }
        }
        if joined.is_empty() {
            bail!("nostr: could not join any mesh rooms")
        }
        let (tx, rx) = mpsc::channel(256);
        let mut inner = Inner::new(relay, joined, tx);
        inner.node_id = node_id;
        inner.consensus = consensus;
        let inner = Arc::new(inner);
        *CURRENT.write().expect("nostr mesh current poisoned") = Some(inner.clone());
        // Catch connections that predate installation of the current mesh.
        for (room, peer, _) in crate::net::peer_auth::verified_peers() {
            if inner.running_in(&room) {
                inner.queue_join(&room, &peer);
            }
        }
        let mut tasks = vec![
            inner.runtime.spawn(receive(inner.clone(), rx)),
            inner.runtime.spawn(broadcast(inner.clone(), outbound)),
            inner.runtime.spawn(synchronize(inner.clone())),
            inner.runtime.spawn(report_interest(inner.clone())),
        ];
        for (room, consensus) in &inner.consensus {
            tasks.push(inner.runtime.spawn(watch_leadership(
                inner.clone(),
                room.clone(),
                consensus.subscribe(),
            )));
        }
        Ok(Arc::new(Self {
            inner,
            tasks: Mutex::new(tasks),
        }))
    }

    /// Stops replicating and leaves the rooms. Repeated stops are harmless.
    pub async fn stop(&self) {
        let _lifecycle = LIFECYCLE.lock().await;
        if !self.inner.active.swap(false, Ordering::AcqRel) {
            return;
        }
        {
            let mut current = CURRENT.write().expect("nostr mesh current poisoned");
            if current
                .as_ref()
                .is_some_and(|inner| Arc::ptr_eq(inner, &self.inner))
            {
                *current = None;
            }
        }
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("nostr mesh tasks poisoned"));
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        self.inner
            .sessions
            .lock()
            .expect("nostr mesh sessions poisoned")
            .clear();
        self.inner.gateway.send_replace(false);
        {
            let mut cluster = self
                .inner
                .cluster
                .lock()
                .expect("nostr mesh cluster poisoned");
            for (room, state) in cluster.iter_mut() {
                self.inner.clear_peer_interest(room, state);
            }
        }
        for consensus in self.inner.consensus.values() {
            consensus.shutdown().await;
        }
        for room in &self.inner.rooms {
            if let Err(error) = crate::net::leave_room(room).await {
                debug!(%room, %error, "nostr: could not leave mesh room");
            }
        }
    }

    /// `true` while this node should act as the cluster's gateway to
    /// external relays (see [`super::upstream`]): it leads at least one of
    /// its rooms.
    pub fn gateway(&self) -> watch::Receiver<bool> {
        self.inner.gateway.subscribe()
    }

    /// Counters and currently admitted peers in each joined room.
    pub fn status(&self) -> Value {
        let peers = crate::net::peer_auth::verified_peers();
        let rooms: Vec<Value> = self
            .inner
            .rooms
            .iter()
            .map(|room| {
                self.inner.refresh_room(room);
                let count = if self.inner.active.load(Ordering::Acquire) {
                    peers
                        .iter()
                        .filter(|(joined, peer, _)| joined == room && admitted(room, peer))
                        .count()
                } else {
                    0
                };
                let consensus = self.inner.consensus.get(room);
                let role = consensus.map(|c| c.role()).unwrap_or(ConsensusRole::Unknown);
                let leader = self.inner.leader(room);
                let members = consensus.map(|c| c.relay_peers().len()).unwrap_or(0);
                let cluster = self.inner.cluster.lock().expect("nostr mesh cluster poisoned");
                let (epoch, seq) = cluster.get(room).map(|state| {
                    if let Some(sequence) = &state.sequencer { (Some(sequence.epoch.clone()), sequence.last) }
                    else { (state.tracker.epoch.clone(), state.tracker.last) }
                }).unwrap_or((None, 0));
                json!({"room": room, "peers": count, "role": role, "leader": leader, "members": members, "epoch": epoch, "seq": seq})
            })
            .collect();
        let interest_peers: usize = self
            .inner
            .cluster
            .lock()
            .expect("nostr mesh cluster poisoned")
            .values()
            .map(|state| state.interest_peers.len())
            .sum();
        json!({"gateway": *self.inner.gateway.borrow(), "rooms": rooms, "sent": self.inner.sent.load(Ordering::Relaxed),
            "interest_peers": interest_peers,
            "received": self.inner.received.load(Ordering::Relaxed),
            "forwarded": self.inner.forwarded.load(Ordering::Relaxed),
            "sync_requests": self.inner.sync_requests.load(Ordering::Relaxed),
            "sync_served": self.inner.sync_served.load(Ordering::Relaxed)})
    }
}

#[cfg(test)]
mod tests {
    use super::super::event::test_event;
    use super::super::store::{Store, StoreLimits};
    use super::*;
    use crate::config::NostrConfig;

    fn event(created_at: u64, content: &str) -> Event {
        test_event(&[1; 32], created_at, 1, vec![], content)
    }

    fn inner(cfg: NostrConfig) -> Inner {
        let store = Store::open(None, StoreLimits { max_events: 1000 }, 0).unwrap();
        let relay = Arc::new(Relay::new(cfg, store, None, None, None));
        let (tx, _) = mpsc::channel(256);
        Inner::new(relay, vec!["room".into(), "other".into()], tx)
    }

    fn ingest(inner: &Inner, room: &str, peer: &str, bytes: &[u8]) -> Vec<Reply> {
        if inner.gate(room, peer, bytes, &|_, _| true) {
            inner.process(room, peer, bytes, &|_, _| true)
        } else {
            Vec::new()
        }
    }

    fn batch(events: &[Event], more: bool, until: Option<u64>) -> Vec<u8> {
        json!({"t": TAG, "op": "batch", "evs": events, "more": more, "until": until})
            .to_string()
            .into_bytes()
    }

    fn interest_filter(tag: &str) -> Filter {
        Filter::parse(&json!({"kinds": [25049], "#d": [tag]})).unwrap()
    }

    #[test]
    fn want_decode_is_strict_bounded_and_allows_withdrawal() {
        let filters = vec![interest_filter("room")];
        let bytes = want_bytes(&filters).unwrap();
        assert!(matches!(decode(&bytes), Some(Wire::Want(got)) if got == filters));
        assert!(
            matches!(decode(&want_bytes(&[]).unwrap()), Some(Wire::Want(got)) if got.is_empty())
        );
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let mut extra = value.clone();
        extra["peer"] = json!("claimed-sender");
        assert!(decode(extra.to_string().as_bytes()).is_none());
        for key in ["t", "op", "f"] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(decode(missing.to_string().as_bytes()).is_none());
        }
        for bad in [
            Value::Null,
            json!({}),
            json!([null]),
            json!([{"kinds": [-1]}]),
            json!([{"#d": [42]}]),
            json!(vec![json!({}); interest::MAX_FILTERS + 1]),
        ] {
            let mut value = value.clone();
            value["f"] = bad;
            assert!(decode(value.to_string().as_bytes()).is_none());
        }
        let mut exact = want_bytes(&[]).unwrap();
        exact.resize(MAX_WIRE_BYTES, b' ');
        assert!(matches!(decode(&exact), Some(Wire::Want(_))));
        exact.push(b' ');
        assert!(decode(&exact).is_none());
        let wide =
            Filter::parse(&json!({"kinds": [25049], "#d": vec!["x".repeat(1024); 60]})).unwrap();
        assert!(want_bytes(&[wide]).is_none());
        let mut wide = interest_filter("room");
        wide.tags.insert(
            'd',
            vec!["x".into(); super::super::filter::MAX_FILTER_VALUES + 1],
        );
        assert!(want_bytes(&[wide]).is_none());
    }

    #[test]
    fn want_filter_serializer_preserves_all_fields() {
        let filter = Filter::parse(&json!({
            "ids": ["a".repeat(64)], "authors": ["b".repeat(64)],
            "kinds": [25049], "#d": ["room"], "#P": ["peer"],
            "since": 1, "until": 2, "limit": 3,
        }))
        .unwrap();
        assert_eq!(Filter::parse(&filter_json(&filter)).unwrap(), filter);
    }

    #[test]
    fn interest_send_dedupes_changes_leaders_refreshes_and_withdrawals() {
        let now = Instant::now();
        let filters = Arc::new(vec![interest_filter("room")]);
        assert!(!should_send_interest(None, "leader", &[], now));
        assert!(should_send_interest(None, "leader", &filters, now));
        let sent = SentInterest {
            leader: "leader".into(),
            filters: filters.clone(),
            at: now,
        };
        assert!(!should_send_interest(Some(&sent), "leader", &filters, now));
        assert!(!should_send_interest(
            Some(&sent),
            "leader",
            &filters,
            now + INTEREST_REFRESH - Duration::from_millis(1)
        ));
        assert!(should_send_interest(
            Some(&sent),
            "leader",
            &filters,
            now + INTEREST_REFRESH
        ));
        assert!(should_send_interest(
            Some(&sent),
            "new-leader",
            &filters,
            now
        ));
        assert!(should_send_interest(
            Some(&sent),
            "leader",
            &[interest_filter("changed")],
            now
        ));
        assert!(should_send_interest(Some(&sent), "leader", &[], now));
        assert!(!should_send_interest(Some(&sent), "new-leader", &[], now));
        let withdrawn = SentInterest {
            leader: "leader".into(),
            filters: Arc::new(vec![]),
            at: now,
        };
        assert!(!should_send_interest(
            Some(&withdrawn),
            "leader",
            &[],
            now + INTEREST_REFRESH
        ));
        assert!(should_send_interest(
            Some(&withdrawn),
            "leader",
            &filters,
            now
        ));
        let mut state = RoomState::default();
        state.update(Some("leader".into()), "self", 1);
        state.interest_sent = Some(sent);
        state.update(None, "self", 2);
        state.update(Some("leader".into()), "self", 3);
        assert!(should_send_interest(
            state.interest_sent.as_ref(),
            "leader",
            &filters,
            now
        ));
    }

    #[tokio::test]
    async fn want_only_leader_stores_transport_senders_and_withdraws_per_room() {
        let mut inner = inner(NostrConfig::default());
        inner.node_id = "self".into();
        let now = Instant::now();
        let filters = vec![interest_filter("peer-a")];
        let bytes = want_bytes(&filters).unwrap();
        let all = inner.relay.interest();
        inner.update_room("room", Some("other-leader".into()));
        inner.receive_interest("room", "peer-a", &filters, now);
        assert!(all.borrow().is_empty());
        inner.update_room("room", Some("self".into()));
        assert!(!inner.gate("room", "denied", &bytes, &|_, _| false));
        inner.process("room", "denied", &bytes, &|_, _| false);
        assert!(all.borrow().is_empty());
        assert!(inner.gate("room", "peer-a", &bytes, &|_, _| true));
        // Exercise the decoded want handler with the same authenticated sender.
        let Some(Wire::Want(decoded)) = decode(&bytes) else {
            panic!("want expected")
        };
        inner.receive_interest("room", "peer-a", &decoded, now);
        assert_eq!(all.borrow().as_ref(), &filters);
        assert!(inner.relay.local_interest().borrow().is_empty());
        inner.update_room("other", Some("self".into()));
        inner.receive_interest("other", "peer-b", &[interest_filter("peer-b")], now);
        inner.receive_interest("room", "peer-b", &[], now);
        assert_eq!(all.borrow()[0].tags[&'d'], vec!["peer-a", "peer-b"]);
        inner.receive_interest("room", "peer-a", &[], now);
        assert_eq!(all.borrow()[0].tags[&'d'], vec!["peer-b"]);
        assert!(
            inner.cluster.lock().unwrap()["room"]
                .interest_peers
                .is_empty()
        );
        inner.receive_interest("other", "peer-b", &[Filter::default()], now);
        assert!(all.borrow().is_empty());
    }

    #[tokio::test]
    async fn losing_leadership_clears_room_interest_and_expiry_prunes_tracking() {
        let mut inner = inner(NostrConfig::default());
        inner.node_id = "self".into();
        let now = Instant::now();
        inner
            .relay
            .set_interest(InterestSource::Conn(1), &[interest_filter("local")], None);
        for (room, peer) in [("room", "peer-a"), ("other", "peer-b")] {
            inner.update_room(room, Some("self".into()));
            inner.receive_interest(room, peer, &[interest_filter(peer)], now);
        }
        let all = inner.relay.interest();
        inner.update_room("room", Some("new-leader".into()));
        assert_eq!(all.borrow()[0].tags[&'d'], vec!["local", "peer-b"]);
        assert!(
            inner.cluster.lock().unwrap()["room"]
                .interest_peers
                .is_empty()
        );
        inner.update_room("other", None);
        assert_eq!(all.borrow()[0].tags[&'d'], vec!["local"]);
        inner.update_room("room", Some("self".into()));
        inner.receive_interest("room", "peer-a", &[interest_filter("peer-a")], now);
        inner.prune_interest(now + INTEREST_TTL + Duration::from_secs(1));
        assert_eq!(all.borrow()[0].tags[&'d'], vec!["local"]);
        assert!(
            inner.cluster.lock().unwrap()["room"]
                .interest_peers
                .is_empty()
        );
    }

    #[tokio::test]
    async fn want_peer_tracking_and_transport_rate_are_bounded() {
        let mut inner = inner(NostrConfig::default());
        inner.node_id = "self".into();
        inner.update_room("room", Some("self".into()));
        {
            let mut cluster = inner.cluster.lock().unwrap();
            let peers = &mut cluster.get_mut("room").unwrap().interest_peers;
            for i in 0..MAX_PEERS {
                peers.insert(format!("peer-{i}"), Instant::now() + INTEREST_TTL);
            }
        }
        let filters = [interest_filter("new")];
        inner.receive_interest("room", "overflow", &filters, Instant::now());
        assert!(inner.relay.interest().borrow().is_empty());
        inner.receive_interest("room", "peer-0", &filters, Instant::now());
        assert_eq!(inner.relay.interest().borrow()[0], filters[0]);
        assert_eq!(
            inner.cluster.lock().unwrap()["room"].interest_peers.len(),
            MAX_PEERS
        );
        let key = ("room".into(), "rate-peer".into());
        assert!(
            inner
                .rates
                .lock()
                .unwrap()
                .take(&key, 2000, false, Instant::now())
        );
        assert!(!inner.gate(
            "room",
            "rate-peer",
            &want_bytes(&filters).unwrap(),
            &|_, _| true
        ));
    }

    #[test]
    fn routes_by_room_leader_and_transport_origin() {
        let origins = [
            Origin::Client,
            Origin::Upstream {
                url: "ws://relay".into(),
            },
            Origin::Mesh {
                room: "other".into(),
                peer: "leader".into(),
            },
            Origin::Mesh {
                room: "room".into(),
                peer: "follower".into(),
            },
        ];
        for origin in &origins {
            assert_eq!(route(Some("self"), "self", "room", origin), Route::Sequence);
            assert_eq!(
                route(Some("leader"), "self", "room", origin),
                Route::Forward("leader".into())
            );
        }
        let from_leader = Origin::Mesh {
            room: "room".into(),
            peer: "leader".into(),
        };
        assert_eq!(
            route(Some("leader"), "self", "room", &from_leader),
            Route::Skip
        );
        assert_eq!(
            route(Some("self"), "self", "room", &from_leader),
            Route::Sequence
        );
        assert_eq!(
            route(None, "self", "room", &Origin::Client),
            Route::Broadcast
        );
        assert_eq!(route(None, "self", "room", &from_leader), Route::Skip);
        assert_eq!(route(None, "self", "other", &from_leader), Route::Broadcast);
    }

    #[test]
    fn tracker_adopts_advances_repairs_gaps_and_changes_epoch() {
        let now = Instant::now();
        let mut tracker = Tracker::default();
        let epoch = "0123456789abcdef";
        assert_eq!(tracker.on_seq(epoch, 10, now), TrackerAction::Sync);
        assert_eq!(tracker.next, 11);
        assert_eq!(tracker.on_seq(epoch, 10, now), TrackerAction::None);
        assert_eq!(tracker.on_seq(epoch, 11, now), TrackerAction::None);
        assert_eq!(tracker.next, 12);
        assert_eq!(tracker.on_seq(epoch, 14, now), TrackerAction::Fetch(12));
        assert_eq!(tracker.next, 15);
        assert_eq!(tracker.on_seq(epoch, 13, now), TrackerAction::None);
        assert!(tracker.fetch.is_some());
        assert_eq!(tracker.on_seq(epoch, 12, now), TrackerAction::None);
        assert!(tracker.fetch.is_none());
        assert_eq!(tracker.next, 15);
        assert_eq!(tracker.on_seq(epoch, 17, now), TrackerAction::Fetch(15));
        assert_eq!(
            tracker.on_seq("ffffffffffffffff", 1, now),
            TrackerAction::Sync
        );
        assert_eq!(tracker.next, 2);
        assert!(tracker.fetch.is_none());
    }

    #[test]
    fn tracker_allows_one_fetch_then_times_out_to_sync() {
        let now = Instant::now();
        let mut tracker = Tracker::default();
        tracker.on_seq("0123456789abcdef", 1, now);
        assert_eq!(
            tracker.on_seq("0123456789abcdef", 3, now),
            TrackerAction::Fetch(2)
        );
        assert_eq!(
            tracker.on_seq("0123456789abcdef", 5, now),
            TrackerAction::None
        );
        tracker.on_seq("0123456789abcdef", 2, now);
        assert!(!tracker.expired(now + FETCH_TIMEOUT - Duration::from_millis(1)));
        assert!(tracker.expired(now + FETCH_TIMEOUT));
        assert!(!tracker.expired(now + FETCH_TIMEOUT));
        assert_eq!(
            tracker.on_seq("0123456789abcdef", 8, now),
            TrackerAction::Fetch(6)
        );
    }

    #[test]
    fn tracker_pending_reorder_buffer_is_bounded() {
        let now = Instant::now();
        let mut tracker = Tracker::default();
        tracker.on_seq("0123456789abcdef", 1, now);
        tracker.on_seq("0123456789abcdef", 10_000, now);
        for n in 3..1000 {
            tracker.on_seq("0123456789abcdef", n, now);
        }
        assert_eq!(tracker.fetch.as_ref().unwrap().seen.len(), MAX_PAGE_EVENTS);
        assert!(tracker.expired(now + FETCH_TIMEOUT));
    }

    #[test]
    fn sequence_tracking_is_bound_to_current_transport_leader() {
        let now = Instant::now();
        let mut state = RoomState::default();
        state.update(Some("leader".into()), "self", 1_000);
        assert_eq!(
            state.on_seq(Some("leader"), "other", "0123456789abcdef", 10, now),
            TrackerAction::None
        );
        assert!(state.tracker.epoch.is_none());
        assert_eq!(
            state.on_seq(None, "leader", "0123456789abcdef", 10, now),
            TrackerAction::None
        );
        assert_eq!(
            state.on_seq(Some("leader"), "leader", "0123456789abcdef", 10, now),
            TrackerAction::Sync
        );
        state.update(Some("new".into()), "self", 1_000);
        assert!(state.tracker.epoch.is_none());
        assert_eq!(
            state.on_seq(Some("new"), "new", "0123456789abcdef", 1, now),
            TrackerAction::Sync
        );
    }

    #[test]
    fn leadership_gain_rotates_epoch_and_resets_sequence_ring() {
        let mut state = RoomState::default();
        assert!(state.update(Some("self".into()), "self", 1_000));
        assert!(state.aggregate);
        let sequence = state.sequencer.as_mut().unwrap();
        let epoch = sequence.epoch.clone();
        sequence.issue(&event(1, "first")).unwrap();
        assert!(!state.update(Some("self".into()), "self", 1_000));
        assert_eq!(state.sequencer.as_ref().unwrap().last, 1);
        state.update(None, "self", 1_000);
        assert!(state.sequencer.is_none());
        assert!(!state.aggregate);
        assert!(state.update(Some("self".into()), "self", 1_000));
        let sequence = state.sequencer.as_ref().unwrap();
        assert_ne!(sequence.epoch, epoch);
        assert_eq!(sequence.last, 0);
        assert!(sequence.ring.entries.is_empty());
    }

    #[test]
    fn leader_change_catch_up_starts_before_the_previous_leader_was_last_seen() {
        let mut state = RoomState::default();
        state.update(Some("a".into()), "self", 1_000);
        state.update(Some("a".into()), "self", 1_400);
        state.update(None, "self", 1_500);
        state.update(Some("b".into()), "self", 2_000);
        assert_eq!(state.catch_up, Some(1_400 - CATCH_UP_MARGIN));
    }

    #[test]
    fn ring_bounds_count_bytes_and_fetch_range() {
        let mut ring = Ring::default();
        for n in 1..=RING_ENTRIES as u64 + 2 {
            ring.push(n, vec![0]);
        }
        assert_eq!(ring.entries.len(), RING_ENTRIES);
        assert_eq!(ring.bytes, RING_ENTRIES);
        assert!(ring.fetch(2).is_empty());
        assert_eq!(ring.fetch(3).len(), MAX_PAGE_EVENTS);
        assert_eq!(ring.fetch(RING_ENTRIES as u64 + 1).len(), 2);
        assert!(ring.fetch(RING_ENTRIES as u64 + 3).is_empty());
        ring = Ring::default();
        ring.push(1, vec![0; RING_BYTES / 2]);
        ring.push(2, vec![0; RING_BYTES / 2]);
        assert_eq!(ring.bytes, RING_BYTES);
        ring.push(3, vec![0]);
        assert_eq!(ring.entries.front().unwrap().0, 2);
        assert_eq!(ring.bytes, RING_BYTES / 2 + 1);
        assert!(ring.fetch(1).is_empty());
        ring.push(4, vec![0; RING_BYTES + 1]);
        assert_eq!(ring.bytes, 0);
        assert!(ring.entries.is_empty());
    }

    #[test]
    fn fetch_requires_local_leadership_and_current_epoch() {
        let mut sequence = Sequencer::new();
        let first = sequence.issue(&event(1, "one")).unwrap();
        let second = sequence.issue(&event(2, "two")).unwrap();
        assert!(sequence.fetch(false, &sequence.epoch, 1).is_empty());
        assert!(sequence.fetch(true, "wrong-epoch", 1).is_empty());
        assert_eq!(
            sequence.fetch(true, &sequence.epoch, 1),
            vec![first, second.clone()]
        );
        assert_eq!(sequence.fetch(true, &sequence.epoch, 2), vec![second]);
    }

    #[test]
    fn wire_cluster_ops_are_strict_and_sequence_envelope_is_capped() {
        let ev = event(1, "wire");
        let ep = "0123456789abcdef";
        let values = [
            json!({"t": TAG, "op": "fwd", "ev": ev}),
            json!({"t": TAG, "op": "seq", "ep": ep, "n": 1, "ev": ev}),
            json!({"t": TAG, "op": "fetch", "ep": ep, "from": 1}),
        ];
        assert!(matches!(
            decode(values[0].to_string().as_bytes()),
            Some(Wire::Forward(_))
        ));
        assert!(matches!(
            decode(values[1].to_string().as_bytes()),
            Some(Wire::Sequence { n: 1, .. })
        ));
        assert!(matches!(
            decode(values[2].to_string().as_bytes()),
            Some(Wire::Fetch { from: 1, .. })
        ));
        for value in &values {
            let mut extra = value.clone();
            extra["unexpected"] = json!(true);
            assert!(decode(extra.to_string().as_bytes()).is_none());
            for key in value.as_object().unwrap().keys() {
                let mut missing = value.clone();
                missing.as_object_mut().unwrap().remove(key);
                assert!(decode(missing.to_string().as_bytes()).is_none());
            }
        }
        for op in ["seq", "fetch"] {
            for bad in [
                json!(""),
                json!("short"),
                json!("0123456789abcdeg"),
                json!("0123456789abcdef0"),
                json!(123),
                Value::Null,
            ] {
                let mut value = if op == "seq" {
                    values[1].clone()
                } else {
                    values[2].clone()
                };
                value["ep"] = bad;
                assert!(decode(value.to_string().as_bytes()).is_none());
            }
        }
        for bad in [json!(0), json!(-1), json!("1")] {
            let mut value = values[1].clone();
            value["n"] = bad;
            assert!(decode(value.to_string().as_bytes()).is_none());
        }
        let mut zero_fetch = values[2].clone();
        zero_fetch["from"] = json!(0);
        assert!(decode(zero_fetch.to_string().as_bytes()).is_none());
        let mut sequence = Sequencer::new();
        let mut ev = ev;
        ev.content =
            "x".repeat(MAX_WIRE_BYTES - event_bytes(&ev).unwrap().len() + ev.content.len());
        assert_eq!(event_bytes(&ev).unwrap().len(), MAX_WIRE_BYTES);
        assert!(sequence.issue(&ev).is_none());
        assert_eq!(sequence.last, 0);
        assert!(sequence.ring.entries.is_empty());
    }

    #[test]
    fn leader_budget_has_eight_times_burst_and_refill_and_sync_stays_capped() {
        let now = Instant::now();
        let key = ("room".into(), "peer".into());
        let mut rates = Rates::default();
        assert!(rates.take(&key, 2000, false, now));
        assert!(!rates.take(&key, 1, false, now));
        assert!(rates.take_budget(&key, 16000, false, true, now));
        assert!(!rates.take_budget(&key, 1, false, true, now));
        assert!(rates.take_budget(&key, 1600, false, true, now + Duration::from_secs(1)));
        assert!(!rates.take_budget(&key, 1, false, true, now + Duration::from_secs(1)));
        assert!(rates.take_budget(&key, 3, true, true, now));
        assert!(!rates.take_budget(&key, 1, true, true, now));
    }

    #[tokio::test]
    async fn gateway_subscribes_to_retained_sender_and_status_has_cluster_fields() {
        let mesh = Mesh {
            inner: Arc::new(inner(NostrConfig::default())),
            tasks: Mutex::new(Vec::new()),
        };
        let status = mesh.status();
        assert_eq!(status["gateway"], false);
        assert_eq!(status["forwarded"], 0);
        assert_eq!(status["interest_peers"], 0);
        for room in status["rooms"].as_array().unwrap() {
            assert_eq!(room["role"], "unknown");
            assert_eq!(room["leader"], Value::Null);
            assert_eq!(room["members"], 0);
            assert_eq!(room["epoch"], Value::Null);
            assert_eq!(room["seq"], 0);
        }
        let mut gateway = mesh.gateway();
        assert!(!*gateway.borrow());
        mesh.inner.gateway.send_replace(true);
        assert!(gateway.has_changed().unwrap());
        gateway.changed().await.unwrap();
        assert!(*gateway.borrow());
        drop(gateway);
        assert!(*mesh.gateway().borrow());
    }

    #[test]
    fn maximum_sequence_does_not_overflow_tracker_or_sequencer() {
        let now = Instant::now();
        let mut tracker = Tracker::default();
        assert_eq!(
            tracker.on_seq("0123456789abcdef", u64::MAX, now),
            TrackerAction::Sync
        );
        assert_eq!(
            tracker.on_seq("0123456789abcdef", u64::MAX, now),
            TrackerAction::None
        );
        assert_eq!(tracker.last, u64::MAX);
        let mut sequence = Sequencer::new();
        sequence.last = u64::MAX - 1;
        let bytes = sequence.issue(&event(1, "last")).unwrap();
        assert!(matches!(
            decode(&bytes),
            Some(Wire::Sequence { n: u64::MAX, .. })
        ));
        assert!(sequence.issue(&event(2, "overflow")).is_none());
    }

    #[tokio::test]
    async fn cluster_ops_submit_events_even_from_nonleader_but_do_not_track_them() {
        let inner = inner(NostrConfig::default());
        let ep = "0123456789abcdef";
        let forwarded = event(now_secs(), "forwarded");
        let sequenced = event(now_secs(), "sequenced");
        for (op, ev) in [("fwd", &forwarded), ("seq", &sequenced)] {
            let mut value = json!({"t": TAG, "op": op, "ev": ev});
            if op == "seq" {
                value["ep"] = json!(ep);
                value["n"] = json!(1);
            }
            assert!(ingest(&inner, "room", "peer", value.to_string().as_bytes()).is_empty());
            assert!(inner.relay.knows(&ev.id));
        }
        assert!(
            inner.cluster.lock().unwrap()["room"]
                .tracker
                .epoch
                .is_none()
        );
        assert_eq!(inner.received.load(Ordering::Relaxed), 2);
        let fetch = json!({"t": TAG, "op": "fetch", "ep": ep, "from": 1});
        assert!(ingest(&inner, "room", "peer", fetch.to_string().as_bytes()).is_empty());
    }

    #[test]
    fn wire_sync_round_trip() {
        let bytes = sync_bytes(42, Some(99));
        assert!(bytes.len() < MAX_WIRE_BYTES);
        assert!(matches!(
            decode(&bytes),
            Some(Wire::Sync {
                since: 42,
                until: Some(99),
                limit: 500,
            })
        ));
        assert!(matches!(
            decode(&sync_bytes(0, None)),
            Some(Wire::Sync { until: None, .. })
        ));
    }

    #[test]
    fn wire_event_is_nested() {
        let ev = event(100, "hello");
        let bytes = event_bytes(&ev).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        for key in ["type", "kind", "v"] {
            assert!(value.get(key).is_none());
        }
        let Some(Wire::Event(value)) = decode(&bytes) else {
            panic!("expected event")
        };
        assert_eq!(Event::parse(&value).unwrap(), ev);
    }

    #[test]
    fn wire_rejects_foreign_malformed_and_extra_fields() {
        for value in [
            json!({"t":"foreign", "op":"ev", "ev":{"content":TAG}}),
            json!({"t":TAG, "op":"ev", "ev":{}, "kind":1}),
            json!({"t":TAG, "op":"ev", "ev":{}, "from":"forged"}),
            json!({"t":TAG, "op":"sync", "since":0, "until":null}),
            json!({"t":TAG, "op":"sync", "since":-1, "until":null, "limit":5}),
            json!({"t":TAG, "op":"sync", "since":0, "until":false, "limit":5}),
            json!({"t":TAG, "op":"sync", "since":0, "until":null, "limit":u64::MAX}),
            json!({"t":TAG, "op":"batch", "evs":[], "more":false, "until":null}),
            json!({"t":TAG, "op":"batch", "evs":[{}], "more":"yes", "until":null}),
            json!({"t":TAG, "op":"unknown"}),
        ] {
            assert!(decode(value.to_string().as_bytes()).is_none(), "{value}");
        }
        assert!(decode(b"mistl-nostr-v1 broken json").is_none());
        assert!(decode(b"{\"t\":\"mistl-nostr-v1\",\"t\":\"other\"}").is_none());
        let mut bytes = sync_bytes(0, None);
        bytes.resize(MAX_INBOUND_BYTES + 1, b' ');
        assert!(!looks_like_mesh(&bytes));
        assert!(decode(&bytes).is_none());
        let too_many =
            json!({"t":TAG,"op":"batch","evs":vec![json!({});501],"more":false,"until":null});
        assert!(decode(too_many.to_string().as_bytes()).is_none());
    }

    #[test]
    fn batches_pack_greedily_and_skip_oversized_events() {
        let mut normal = event(10, "small");
        normal.content = "x".repeat(20_000);
        let mut giant = normal.clone();
        giant.created_at = 1;
        giant.content = "x".repeat(MAX_WIRE_BYTES);
        assert!(event_bytes(&giant).is_none());
        let events = vec![normal.clone(), normal.clone(), giant, normal.clone()];
        let packed = pack_batches(&events, true);
        assert_eq!(packed.len(), 2);
        let mut counts = Vec::new();
        for (index, bytes) in packed.iter().enumerate() {
            assert!(bytes.len() <= MAX_WIRE_BYTES);
            let Some(Wire::Batch {
                events,
                more,
                until,
            }) = decode(bytes)
            else {
                panic!("batch")
            };
            counts.push(events.len());
            assert!(more);
            assert_eq!(until, if index == 1 { Some(1) } else { None });
        }
        assert_eq!(counts, [2, 1]);
        assert!(pack_batches(&[], false).is_empty());
        let final_page = pack_batches(&[normal], false);
        assert!(matches!(
            decode(&final_page[0]),
            Some(Wire::Batch {
                more: false,
                until: None,
                ..
            })
        ));
    }

    #[test]
    fn batch_envelope_overhead_is_also_capped() {
        let mut ev = event(0, "");
        let base = event_bytes(&ev).unwrap().len();
        ev.content = "x".repeat(MAX_WIRE_BYTES - base);
        assert_eq!(event_bytes(&ev).unwrap().len(), MAX_WIRE_BYTES);
        assert!(pack_batches(&[ev], true).is_empty());
    }

    #[test]
    fn bucket_refills_and_never_exceeds_burst() {
        let now = Instant::now();
        let mut events = Bucket::new(2000.0, now);
        assert!(events.take(2000, 2000.0, 200.0, now));
        assert!(!events.take(1, 2000.0, 200.0, now));
        assert!(events.take(200, 2000.0, 200.0, now + Duration::from_secs(1)));
        assert!(!events.take(1, 2000.0, 200.0, now + Duration::from_secs(1)));
        assert!(!events.take(2001, 2000.0, 200.0, now + Duration::from_secs(100)));
        let mut syncs = Bucket::new(3.0, now);
        assert!(syncs.take(3, 3.0, 0.1, now));
        assert!(!syncs.take(1, 3.0, 0.1, now + Duration::from_secs(9)));
        assert!(syncs.take(1, 3.0, 0.1, now + Duration::from_secs(10)));
    }

    #[test]
    fn peer_rate_table_is_bounded_and_evicts_idle_entries() {
        let now = Instant::now();
        let mut rates = Rates::default();
        for n in 0..MAX_PEERS {
            assert!(rates.take(&("room".into(), n.to_string()), 1, false, now));
        }
        let key = ("room".into(), "extra".into());
        assert!(!rates.take(&key, 1, false, now));
        assert_eq!(rates.0.len(), MAX_PEERS);
        assert!(rates.take(&key, 1, false, now + PEER_IDLE));
        assert_eq!(rates.0.len(), 1);
    }

    #[test]
    fn session_page_and_event_budgets() {
        let mut now = Instant::now();
        let mut session = Session::new(0, now);
        for page in 0..MAX_SESSION_PAGES {
            assert!(session.accept(1, now));
            let keep = session.finish(true, Some(1000 - page as u64), now);
            assert_eq!(keep, page + 1 < MAX_SESSION_PAGES);
            now += SYNC_SPACING;
            session.due = None;
        }
        assert!(!session.accept(1, now));
        let mut session = Session::new(0, now);
        for page in 0..10 {
            assert!(session.accept(500, now));
            assert_eq!(session.finish(true, Some(1000 - page), now), page < 9);
            now += SYNC_SPACING;
            session.due = None;
        }
        assert!(!session.accept(1, now));
    }

    #[test]
    fn session_rejects_expiry_overflow_and_nonprogressing_cursor() {
        let now = Instant::now();
        assert!(!Session::new(0, now).accept(1, now + SESSION_TTL));
        let mut session = Session::new(0, now);
        assert!(session.accept(300, now));
        assert!(!session.accept(201, now));
        assert!(session.finish(true, Some(100), now));
        assert!(!session.accept(1, now));
        session.due = None;
        assert!(!session.finish(true, Some(100), now));
        assert!(!Session::new(100, now).finish(true, Some(99), now));
    }

    #[tokio::test]
    async fn inbound_event_submits_once_and_notifies_live_clients() {
        let inner = inner(NostrConfig::default());
        let mut live = inner.relay.subscribe();
        let ev = event(now_secs(), "live");
        let bytes = event_bytes(&ev).unwrap();
        ingest(&inner, "room", "peer", &bytes);
        assert_eq!(live.try_recv().unwrap().id, ev.id);
        ingest(&inner, "room", "peer", &bytes);
        assert!(live.try_recv().is_err());
        assert!(inner.relay.knows(&ev.id));
        assert_eq!(inner.received.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn inbound_enforces_room_admission_and_stopped_state() {
        let inner = inner(NostrConfig::default());
        let bytes = event_bytes(&event(now_secs(), "blocked")).unwrap();
        assert!(!inner.gate("unjoined", "peer", &bytes, &|_, _| true));
        assert!(!inner.gate("room", "peer", &bytes, &|_, _| false));
        inner.process("unjoined", "peer", &bytes, &|_, _| true);
        inner.process("room", "peer", &bytes, &|_, _| false);
        inner.active.store(false, Ordering::Release);
        ingest(&inner, "room", "peer", &bytes);
        assert_eq!(inner.relay.event_count(), 0);
    }

    #[tokio::test]
    async fn inbound_verifies_signatures_and_write_policy() {
        let inner = inner(NostrConfig {
            write_pubkeys: vec![super::super::event::test_pubkey(&[2; 32])],
            ..NostrConfig::default()
        });
        let ev = event(now_secs(), "not allowlisted");
        ingest(&inner, "room", "peer", &event_bytes(&ev).unwrap());
        assert_eq!(inner.relay.event_count(), 0);
        let inner = self::inner(NostrConfig::default());
        let mut forged = ev;
        forged.sig = "00".repeat(64);
        ingest(&inner, "room", "peer", &event_bytes(&forged).unwrap());
        let malformed = json!({"t":TAG, "op":"ev", "ev":{"id":"bad"}});
        ingest(&inner, "room", "peer", malformed.to_string().as_bytes());
        assert_eq!(inner.relay.event_count(), 0);
        assert_eq!(inner.received.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn batches_require_outstanding_session_bound_to_room_and_sender() {
        let inner = inner(NostrConfig::default());
        let ev = event(now_secs(), "batch");
        let bytes = batch(&[ev.clone()], false, None);
        let key = ("room".into(), "peer".into());
        ingest(&inner, "room", "peer", &bytes);
        assert_eq!(inner.relay.event_count(), 0);
        inner.begin_sync(key.clone(), 0, Instant::now()).unwrap();
        ingest(&inner, "room", "impostor", &bytes);
        ingest(&inner, "other", "peer", &bytes);
        assert_eq!(inner.relay.event_count(), 0);
        ingest(&inner, "room", "peer", &bytes);
        assert!(inner.relay.knows(&ev.id));
        assert!(!inner.sessions.lock().unwrap().contains_key(&key));
    }

    #[tokio::test]
    async fn expired_session_does_not_accept_batch() {
        let inner = inner(NostrConfig::default());
        let key = ("room".into(), "peer".into());
        inner
            .begin_sync(key.clone(), 0, Instant::now() - SESSION_TTL)
            .unwrap();
        ingest(
            &inner,
            "room",
            "peer",
            &batch(&[event(now_secs(), "expired")], false, None),
        );
        assert_eq!(inner.relay.event_count(), 0);
        assert!(inner.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn fragmented_page_schedules_one_continuation_after_final_fragment() {
        let inner = inner(NostrConfig::default());
        let key = ("room".into(), "peer".into());
        let now = Instant::now();
        inner.begin_sync(key.clone(), 0, now).unwrap();
        ingest(
            &inner,
            "room",
            "peer",
            &batch(&[event(100, "first")], true, None),
        );
        assert!(inner.continuations(Instant::now()).is_empty());
        ingest(
            &inner,
            "room",
            "peer",
            &batch(&[event(99, "last")], true, Some(99)),
        );
        assert_eq!(inner.relay.event_count(), 2);
        assert_eq!(inner.sessions.lock().unwrap()[&key].pages, 1);
        assert!(inner.continuations(now).is_empty());
        let replies = inner.continuations(Instant::now() + SYNC_SPACING);
        assert_eq!(replies.len(), 1);
        assert_eq!((&replies[0].room, &replies[0].peer), (&key.0, &key.1));
        assert!(matches!(
            decode(&replies[0].bytes),
            Some(Wire::Sync {
                until: Some(99),
                ..
            })
        ));
        assert!(
            inner
                .continuations(Instant::now() + SYNC_SPACING)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inbound_batch_charges_each_event_to_bucket() {
        let inner = inner(NostrConfig::default());
        let key = ("room".into(), "peer".into());
        let ev = event(now_secs(), "limited");
        let bytes = batch(&[ev.clone(), ev.clone(), ev], false, None);
        let now = Instant::now();
        inner.begin_sync(key.clone(), 0, now).unwrap();
        assert!(inner.rates.lock().unwrap().take(&key, 1998, false, now));
        ingest(&inner, "room", "peer", &bytes);
        assert_eq!(inner.relay.event_count(), 0);
        assert!(inner.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn serves_filtered_sync_to_transport_sender_with_request_limit() {
        let inner = inner(NostrConfig::default());
        for created_at in [10, 20, 30, 40] {
            assert!(
                inner
                    .relay
                    .submit(event(created_at, "history"), &Origin::Client, now_secs())
                    .accepted
            );
        }
        let request = json!({"t":TAG,"op":"sync","since":15,"until":35,"limit":2});
        let replies = ingest(
            &inner,
            "room",
            "transport-peer",
            request.to_string().as_bytes(),
        );
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].peer, "transport-peer");
        assert_eq!(replies[0].room, "room");
        let Some(Wire::Batch {
            events,
            more,
            until,
        }) = decode(&replies[0].bytes)
        else {
            panic!("batch")
        };
        assert_eq!(
            events
                .iter()
                .map(|event| event["created_at"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [30, 20]
        );
        assert!(more);
        assert_eq!(until, Some(20));
        assert_eq!(inner.sync_served.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn sync_requests_are_rate_limited_and_invalid_ranges_ignored() {
        let inner = inner(NostrConfig::default());
        inner
            .relay
            .submit(event(10, "history"), &Origin::Client, now_secs());
        for _ in 0..3 {
            assert_eq!(
                ingest(&inner, "room", "peer", &sync_bytes(0, None)).len(),
                1
            );
        }
        assert!(ingest(&inner, "room", "peer", &sync_bytes(0, None)).is_empty());
        assert!(ingest(&inner, "room", "another", &sync_bytes(20, Some(10))).is_empty());
        assert_eq!(inner.sync_served.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn session_and_join_tables_are_bounded_and_expire() {
        let inner = inner(NostrConfig::default());
        let now = Instant::now();
        for n in 0..MAX_PEERS {
            inner
                .begin_sync(("room".into(), n.to_string()), 0, now)
                .unwrap();
            inner.queue_join("room", &n.to_string());
        }
        assert!(
            inner
                .begin_sync(("room".into(), "extra".into()), 0, now)
                .is_none()
        );
        inner.queue_join("room", "extra");
        assert_eq!(inner.joins.lock().unwrap().len(), MAX_PEERS);
        assert!(inner.continuations(now + SESSION_TTL).is_empty());
        assert!(inner.sessions.lock().unwrap().is_empty());
    }
}
