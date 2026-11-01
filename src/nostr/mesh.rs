//! Room-scoped Nostr replication, with authenticated transport senders.
//!
//! The permanent callback only gates and queues input. A blocking worker
//! parses messages and verifies events. Bounded sync sessions assemble
//! size-limited batches without trusting identities supplied in JSON.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use rand::seq::IteratorRandom;
use serde_json::{Value, json};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::event::Event;
use super::filter::Filter;
use super::{Origin, Relay, now_secs};
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
type Peer = (String, String);

static REGISTER: Once = Once::new();
static CURRENT: RwLock<Option<Arc<Inner>>> = RwLock::new(None);
static LIFECYCLE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug)]
enum Wire {
    Event(Value),
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
        "ev" => &["t", "op", "ev"],
        "sync" => &["t", "op", "since", "until", "limit"],
        "batch" => &["t", "op", "evs", "more", "until"],
        _ => return None,
    };
    if object.len() != keys.len() || keys.iter().any(|key| !object.contains_key(*key)) {
        return None;
    }
    let until = || match object.get("until")? {
        Value::Null => Some(None),
        value => Some(Some(value.as_u64()?)),
    };
    match object.get("op")?.as_str()? {
        "ev" => Some(Wire::Event(object.get("ev")?.clone())),
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
    let bytes = serde_json::to_vec(&json!({"t": TAG, "op": "ev", "ev": event})).ok()?;
    if bytes.len() > MAX_WIRE_BYTES {
        debug!(id = %event.id, "nostr: event too large for mesh replication");
        return None;
    }
    Some(bytes)
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
    syncs: Bucket,
    last: Instant,
}

#[derive(Default)]
struct Rates(HashMap<Peer, PeerRate>);

impl Rates {
    fn take(&mut self, key: &Peer, count: usize, sync: bool, now: Instant) -> bool {
        if !self.0.contains_key(key) && self.0.len() >= MAX_PEERS {
            self.0
                .retain(|_, rate| now.saturating_duration_since(rate.last) < PEER_IDLE);
            if self.0.len() >= MAX_PEERS {
                return false;
            }
        }
        let rate = self.0.entry(key.clone()).or_insert_with(|| PeerRate {
            events: Bucket::new(2000.0, now),
            syncs: Bucket::new(3.0, now),
            last: now,
        });
        rate.last = now;
        if sync {
            rate.syncs.take(count, 3.0, 0.1, now)
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
    runtime: Handle,
    inbound: mpsc::Sender<Inbound>,
    active: AtomicBool,
    rates: Mutex<Rates>,
    sessions: Mutex<HashMap<Peer, Session>>,
    /// Pending initial syncs: peer -> (next attempt, give up at).
    joins: Mutex<HashMap<Peer, (Instant, Instant)>>,
    sent: AtomicU64,
    received: AtomicU64,
    sync_requests: AtomicU64,
    sync_served: AtomicU64,
}

fn admitted(room: &str, peer: &str) -> bool {
    crate::net::peer_auth::admitted_did(room, peer).is_some()
}

impl Inner {
    fn new(relay: Arc<Relay>, rooms: Vec<String>, inbound: mpsc::Sender<Inbound>) -> Self {
        Self {
            relay,
            rooms,
            runtime: Handle::current(),
            inbound,
            active: AtomicBool::new(true),
            rates: Mutex::new(Rates::default()),
            sessions: Mutex::new(HashMap::new()),
            joins: Mutex::new(HashMap::new()),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            sync_requests: AtomicU64::new(0),
            sync_served: AtomicU64::new(0),
        }
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
        rates.take(
            &(room.to_string(), from.to_string()),
            1,
            false,
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
            Wire::Event(event) => {
                self.submit(room, from, &event);
                Vec::new()
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

async fn broadcast(inner: Arc<Inner>, mut outbound: mpsc::Receiver<Arc<Event>>) {
    while let Some(event) = outbound.recv().await {
        let Some(bytes) = event_bytes(&event) else {
            continue;
        };
        for room in &inner.rooms {
            if !inner.running_in(room) {
                return;
            }
            match tokio::time::timeout(
                SEND_TIMEOUT,
                crate::net::send_broadcast(room, bytes.clone()),
            )
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
                let since =
                    now_secs().saturating_sub(inner.relay.cfg().sync_days.saturating_mul(86400));
                if let Some(reply) = inner.begin_sync(key, since, now) {
                    inner.send(reply).await;
                }
            } else if now < give_up {
                // Not verified yet: the hello usually lands within seconds.
                inner
                    .joins
                    .lock()
                    .expect("nostr mesh joins poisoned")
                    .insert(key, (now + JOIN_DELAY, give_up));
            }
        }
        for reply in inner.continuations(now) {
            inner.send(reply).await;
        }
        if now >= next_entropy {
            next_entropy = now + Duration::from_secs(600);
            let peers = crate::net::peer_auth::verified_peers();
            for room in &inner.rooms {
                let peer = peers
                    .iter()
                    .filter(|(joined, peer, _)| joined == room && admitted(room, peer))
                    .choose(&mut rand::thread_rng())
                    .map(|(_, peer, _)| peer.clone());
                if let Some(peer) = peer
                    && let Some(reply) = inner.begin_sync(
                        (room.clone(), peer),
                        now_secs().saturating_sub(86400),
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
        outbound: mpsc::Receiver<Arc<Event>>,
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
        let mut joined = Vec::new();
        for room in rooms {
            if joined.contains(&room) {
                continue;
            }
            match crate::net::ensure_started(state, room.clone()).await {
                Ok(_) => joined.push(room),
                Err(error) => warn!(%room, %error, "nostr: could not join mesh room"),
            }
        }
        if joined.is_empty() {
            bail!("nostr: could not join any mesh rooms")
        }
        let (tx, rx) = mpsc::channel(256);
        let inner = Arc::new(Inner::new(relay, joined, tx));
        *CURRENT.write().expect("nostr mesh current poisoned") = Some(inner.clone());
        // Catch connections that predate installation of the current mesh.
        for (room, peer, _) in crate::net::peer_auth::verified_peers() {
            if inner.running_in(&room) {
                inner.queue_join(&room, &peer);
            }
        }
        let tasks = vec![
            inner.runtime.spawn(receive(inner.clone(), rx)),
            inner.runtime.spawn(broadcast(inner.clone(), outbound)),
            inner.runtime.spawn(synchronize(inner.clone())),
        ];
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
        for room in &self.inner.rooms {
            if let Err(error) = crate::net::leave_room(room).await {
                debug!(%room, %error, "nostr: could not leave mesh room");
            }
        }
    }

    /// Counters and currently admitted peers in each joined room.
    pub fn status(&self) -> Value {
        let peers = crate::net::peer_auth::verified_peers();
        let rooms: Vec<Value> = self
            .inner
            .rooms
            .iter()
            .map(|room| {
                let count = if self.inner.active.load(Ordering::Acquire) {
                    peers
                        .iter()
                        .filter(|(joined, peer, _)| joined == room && admitted(room, peer))
                        .count()
                } else {
                    0
                };
                json!({"room": room, "peers": count})
            })
            .collect();
        json!({"rooms": rooms, "sent": self.inner.sent.load(Ordering::Relaxed),
            "received": self.inner.received.load(Ordering::Relaxed),
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
        let relay = Arc::new(Relay::new(cfg, store, None, None));
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
