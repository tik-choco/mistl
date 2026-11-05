//! Nostr relay hosted by the daemon.
//!
//! A NIP-01 relay (plus NIP-09 deletion, NIP-11 info document, NIP-40
//! expiration and NIP-45 COUNT) listening on `[nostr] listen`, over plain
//! `ws://` by default (loopback) or `wss://` (see [`tls`]). Optionally, the
//! relays in the same mistl rooms (`[nostr] rooms`) form a cluster per room
//! ([`mesh`]): a Raft leader is elected, every node hands the events it
//! accepts to the leader, and the leader re-distributes them to the whole
//! room in one sequence, so a client connected to any node sees the events
//! published through all of them. Only a leader (or a standalone relay)
//! connects to external relays (`[nostr] upstreams`, [`upstream`]).
//!
//! Layout:
//! - [`event`] / [`filter`]: NIP-01 event model, id + signature checks,
//!   filter matching (pure).
//! - [`store`]: bounded event store with replaceable / addressable /
//!   ephemeral / deletion semantics, persisted to `<data_dir>/nostr/`.
//! - [`server`]: TCP listener, TLS, HTTP upgrade, per-connection protocol.
//! - [`mesh`]: the leader-sequenced cluster between mistl nodes.
//! - [`upstream`]: WebSocket client to external relays, gateway-only.
//! - this file: the shared [`Relay`] core every event goes through
//!   ([`Relay::submit`]), the service lifecycle and the `nostr.*` IPC.
//!
//! Events are self-authenticating (BIP-340 signature over the id), so mesh
//! peers are not trusted for content -- every event from a peer is verified
//! and passes the same write policy as one from a local client.
//!
//! IPC (`nostr.*`):
//! - `nostr.status {}` -> `{running, url?, listen, tls, events, connections, mesh}`
//! - `nostr.start {}` / `nostr.stop {}` -> status (also persist `[nostr] enabled`)
//! - `nostr.query {filters: [..] | filter: {..}, limit?}` -> `{events}`

pub mod event;
pub mod filter;
pub mod interest;
pub mod mesh;
pub mod server;
pub mod store;
pub mod tls;
pub mod upstream;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{info, warn};

use crate::config::NostrConfig;
use crate::daemon::AppState;

use event::{Event, EventError};
use filter::Filter;
use store::{Insert, Store, StoreLimits};

/// How often new / removed events are written to disk.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Live fan-out buffer to connections; a connection that falls further
/// behind misses events (it logs and keeps its subscriptions).
const LIVE_BUFFER: usize = 1024;
/// Newly accepted events queued for the mesh; dropped when full.
const MESH_BUFFER: usize = 1024;
/// Accepted distance of `created_at` into the future.
pub const MAX_FUTURE_SECS: u64 = 15 * 60;
/// Recently seen event ids (stored or ephemeral), so ephemeral events and
/// mesh re-forwards are not processed twice.
const SEEN_CAPACITY: usize = 16_384;

/// Where an event came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// A WebSocket client of this relay.
    Client,
    /// A mistl peer, identified by the transport sender in `room`.
    Mesh { room: String, peer: String },
    /// An external relay this node is connected to as the gateway.
    Upstream { url: String },
}

/// A freshly accepted event and where it came from, for the cluster
/// ([`mesh`]) to route.
#[derive(Debug, Clone)]
pub struct Fresh {
    pub event: Arc<Event>,
    pub origin: Origin,
}

/// Who holds a set of mirrored subscriptions (see [`interest`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InterestSource {
    /// A WebSocket client connection of this relay.
    Conn(u64),
    /// A cluster peer's clients, reported over the mesh (transport sender).
    Peer { room: String, peer: String },
}

/// Interest sources kept at most (connections are already capped by
/// `max_connections`; this bounds peers).
const MAX_INTEREST_SOURCES: usize = 2048;

struct InterestEntry {
    filters: Vec<Filter>,
    expires: Option<Instant>,
}

/// Result of [`Relay::submit`], mapped 1:1 onto a NIP-01 `OK` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submitted {
    pub accepted: bool,
    /// NIP-01 machine-readable prefixed message (`duplicate: ...`,
    /// `invalid: ...`, `blocked: ...`, `restricted: ...`) or empty.
    pub message: String,
    /// The event was new (stored or relayed as ephemeral).
    pub fresh: bool,
}

impl Submitted {
    fn ok(fresh: bool, message: impl Into<String>) -> Self {
        Self {
            accepted: true,
            message: message.into(),
            fresh,
        }
    }

    fn rejected(message: impl Into<String>) -> Self {
        Self {
            accepted: false,
            message: message.into(),
            fresh: false,
        }
    }
}

/// Bounded FIFO set of event ids.
#[derive(Default)]
struct Seen {
    order: VecDeque<String>,
    ids: HashMap<String, ()>,
}

impl Seen {
    fn contains(&self, id: &str) -> bool {
        self.ids.contains_key(id)
    }

    /// `false` if already present.
    fn insert(&mut self, id: &str) -> bool {
        if self.ids.contains_key(id) {
            return false;
        }
        if self.order.len() >= SEEN_CAPACITY
            && let Some(old) = self.order.pop_front()
        {
            self.ids.remove(&old);
        }
        self.order.push_back(id.to_string());
        self.ids.insert(id.to_string(), ());
        true
    }
}

/// The shared relay core: store, live fan-out and policy.
pub struct Relay {
    cfg: NostrConfig,
    store: Mutex<Store>,
    seen: Mutex<Seen>,
    live: broadcast::Sender<Arc<Event>>,
    mesh_tx: Option<mpsc::Sender<Fresh>>,
    /// Fresh events not from an upstream, for publishing to the upstreams.
    upstream_tx: Option<mpsc::Sender<Arc<Event>>>,
    /// `<data_dir>/nostr`; `None` keeps everything in memory (tests).
    dir: Option<PathBuf>,
    pub connections: AtomicUsize,
    pub subscriptions: AtomicUsize,
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
    interest: Mutex<HashMap<InterestSource, InterestEntry>>,
    /// Merged interest of every source (what the gateway mirrors).
    interest_all: watch::Sender<Arc<Vec<Filter>>>,
    /// Merged interest of local connections only (what this node reports
    /// to its cluster leader).
    interest_local: watch::Sender<Arc<Vec<Filter>>>,
    next_conn: AtomicU64,
}

impl Relay {
    /// `mesh_tx` receives every fresh event for the cluster (`None`: no
    /// mesh); `upstream_tx` every fresh event that did not come from an
    /// upstream (`None`: no upstreams).
    pub fn new(
        cfg: NostrConfig,
        store: Store,
        dir: Option<PathBuf>,
        mesh_tx: Option<mpsc::Sender<Fresh>>,
        upstream_tx: Option<mpsc::Sender<Arc<Event>>>,
    ) -> Self {
        let (live, _) = broadcast::channel(LIVE_BUFFER);
        Self {
            cfg,
            store: Mutex::new(store),
            seen: Mutex::new(Seen::default()),
            live,
            mesh_tx,
            upstream_tx,
            dir,
            connections: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            interest: Mutex::new(HashMap::new()),
            interest_all: watch::channel(Arc::new(Vec::new())).0,
            interest_local: watch::channel(Arc::new(Vec::new())).0,
            next_conn: AtomicU64::new(0),
        }
    }

    /// A fresh interest key for a client connection.
    pub fn next_conn_id(&self) -> u64 {
        self.next_conn.fetch_add(1, Ordering::Relaxed)
    }

    /// Replaces `source`'s mirrored subscriptions (filters outside
    /// `[nostr] mirror_kinds` are ignored; none left removes the source).
    /// Peer sources should pass a `ttl` and refresh before it runs out.
    pub fn set_interest(&self, source: InterestSource, filters: &[Filter], ttl: Option<Duration>) {
        let kept: Vec<Filter> = filters
            .iter()
            .filter(|f| interest::mirrored(f, &self.cfg.mirror_kinds))
            .cloned()
            .collect();
        let mut map = self.interest.lock().expect("nostr interest poisoned");
        let changed = if kept.is_empty() {
            map.remove(&source).is_some()
        } else if !map.contains_key(&source) && map.len() >= MAX_INTEREST_SOURCES {
            false
        } else {
            let expires = ttl.map(|ttl| Instant::now() + ttl);
            let same = map.get(&source).is_some_and(|e| e.filters == kept);
            map.insert(
                source,
                InterestEntry {
                    filters: kept,
                    expires,
                },
            );
            !same
        };
        if changed {
            self.publish_interest(&map);
        }
    }

    /// Drops peer interest whose TTL ran out.
    pub fn prune_interest(&self, now: Instant) {
        let mut map = self.interest.lock().expect("nostr interest poisoned");
        let before = map.len();
        map.retain(|_, e| e.expires.is_none_or(|at| now < at));
        if map.len() != before {
            self.publish_interest(&map);
        }
    }

    fn publish_interest(&self, map: &HashMap<InterestSource, InterestEntry>) {
        let all = interest::merge(map.values().flat_map(|e| e.filters.iter()));
        let local = interest::merge(
            map.iter()
                .filter(|(k, _)| matches!(k, InterestSource::Conn(_)))
                .flat_map(|(_, e)| e.filters.iter()),
        );
        for (tx, merged) in [(&self.interest_all, all), (&self.interest_local, local)] {
            tx.send_if_modified(|current| {
                if **current == merged {
                    false
                } else {
                    *current = Arc::new(merged);
                    true
                }
            });
        }
    }

    /// Merged interest of the whole cluster as seen here (local clients
    /// plus peers that reported to this node).
    pub fn interest(&self) -> watch::Receiver<Arc<Vec<Filter>>> {
        self.interest_all.subscribe()
    }

    /// Merged interest of this node's own clients.
    pub fn local_interest(&self) -> watch::Receiver<Arc<Vec<Filter>>> {
        self.interest_local.subscribe()
    }

    pub fn cfg(&self) -> &NostrConfig {
        &self.cfg
    }

    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store.lock().expect("nostr store poisoned")
    }

    /// Live feed of every fresh event (stored or ephemeral), for
    /// subscriptions opened after their initial query.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Event>> {
        self.live.subscribe()
    }

    /// Cheap pre-signature duplicate check (for mesh input).
    pub fn knows(&self, id: &str) -> bool {
        self.seen.lock().expect("nostr seen poisoned").contains(id) || self.store().contains(id)
    }

    /// Every event, from a client or a mesh peer, goes through here: shape
    /// and signature, write policy, store semantics, then fan-out to local
    /// subscriptions and (if fresh) to the mesh.
    pub fn submit(&self, ev: Event, origin: &Origin, now: u64) -> Submitted {
        let result = self.submit_inner(ev, origin, now);
        if result.accepted {
            self.accepted.fetch_add(1, Ordering::Relaxed);
        } else {
            self.rejected.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn submit_inner(&self, ev: Event, origin: &Origin, now: u64) -> Submitted {
        // The id is a cheap hash; check it before trusting it as a dedupe
        // key, so a tampered copy of a known event is reported as invalid.
        if ev.compute_id() != ev.id {
            return Submitted::rejected(reject_message(&EventError::BadId));
        }
        if self.knows(&ev.id) {
            return Submitted::ok(false, "duplicate: already have this event");
        }
        if let Err(error) = ev.verify() {
            return Submitted::rejected(reject_message(&error));
        }
        if !self.cfg.write_pubkeys.is_empty()
            && !self
                .cfg
                .write_pubkeys
                .iter()
                .any(|pk| pk.eq_ignore_ascii_case(&ev.pubkey))
        {
            return Submitted::rejected(
                "restricted: this relay only accepts events from listed pubkeys",
            );
        }
        if ev.created_at > now + MAX_FUTURE_SECS {
            return Submitted::rejected("invalid: created_at is too far in the future");
        }
        if ev.expiration().is_some_and(|exp| exp <= now) {
            return Submitted::rejected("invalid: event has expired");
        }
        let ev = Arc::new(ev);
        let outcome = self.store().insert((*ev).clone(), now);
        let result = match outcome {
            Insert::Stored | Insert::Ephemeral => {
                self.seen
                    .lock()
                    .expect("nostr seen poisoned")
                    .insert(&ev.id);
                let _ = self.live.send(ev.clone());
                if let Some(tx) = &self.mesh_tx {
                    // Mesh input goes back to the cluster too: the leader
                    // re-distributes what followers hand it, and events
                    // cross rooms; `seen` + the store stop the echo.
                    let _ = tx.try_send(Fresh {
                        event: ev.clone(),
                        origin: origin.clone(),
                    });
                }
                if let Some(tx) = &self.upstream_tx
                    && !matches!(origin, Origin::Upstream { .. })
                {
                    let _ = tx.try_send(ev.clone());
                }
                Submitted::ok(true, "")
            }
            Insert::Duplicate => Submitted::ok(false, "duplicate: already have this event"),
            Insert::Obsolete => Submitted::ok(false, "duplicate: a newer version is stored"),
            Insert::Deleted => Submitted::rejected("blocked: this event was deleted by its author"),
        };
        if let Origin::Mesh { room, peer } = origin
            && !result.accepted
        {
            tracing::debug!(%room, %peer, id = %ev.id, message = %result.message, "nostr: mesh event not accepted");
        }
        result
    }

    /// Stored events matching any filter, newest first, at most `cap`
    /// (and each filter's own `limit`).
    pub fn query(&self, filters: &[Filter], cap: usize, now: u64) -> Vec<Event> {
        self.store().query(filters, cap, now)
    }

    pub fn count(&self, filters: &[Filter], now: u64) -> usize {
        self.store().count(filters, now)
    }

    pub fn event_count(&self) -> usize {
        self.store().len()
    }

    /// NIP-11 relay information document.
    pub fn info_document(&self) -> Value {
        let cfg = &self.cfg;
        json!({
            "name": cfg.name,
            "description": cfg.description,
            "contact": cfg.contact,
            "software": "https://github.com/tik-choco-lab/mistl",
            "version": env!("CARGO_PKG_VERSION"),
            "supported_nips": [1, 9, 11, 40, 45],
            "limitation": {
                "max_message_length": cfg.max_event_bytes,
                "max_subscriptions": cfg.max_subscriptions,
                "max_filters": cfg.max_filters,
                "max_limit": cfg.max_limit,
                "restricted_writes": !cfg.write_pubkeys.is_empty(),
                "auth_required": false,
                "payment_required": false,
            },
        })
    }

    /// Writes new / removed events to disk.
    async fn flush(self: &Arc<Self>) {
        let Some(dir) = self.dir.clone() else {
            return;
        };
        let pending = self.store().take_pending();
        if pending.is_empty() {
            return;
        }
        let result = tokio::task::spawn_blocking(move || pending.apply(&dir))
            .await
            .map_err(|e| anyhow!("join error: {e}"))
            .and_then(|r| r);
        if let Err(error) = result {
            warn!(%error, "nostr: persisting events failed; will rewrite on next flush");
            self.store().mark_all_dirty();
        }
    }
}

fn reject_message(error: &EventError) -> String {
    let text = error.to_string();
    if text.starts_with("invalid:") {
        text
    } else {
        format!("invalid: {text}")
    }
}

/// Seconds since the Unix epoch.
pub fn now_secs() -> u64 {
    crate::net::peer_auth::now_ms() / 1000
}

/// A running relay.
struct Running {
    relay: Arc<Relay>,
    url: String,
    tls: Option<tls::CertInfo>,
    shutdown: watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    mesh: Option<Arc<mesh::Mesh>>,
    upstreams: Option<Arc<upstream::Upstreams>>,
}

static RUNNING: tokio::sync::Mutex<Option<Running>> = tokio::sync::Mutex::const_new(None);

fn validate(cfg: &NostrConfig) -> Result<()> {
    if cfg.max_events == 0
        || cfg.max_event_bytes < 1024
        || cfg.max_connections == 0
        || cfg.max_subscriptions == 0
        || cfg.max_filters == 0
        || cfg.max_limit == 0
    {
        bail!("[nostr] limits must be positive (max_event_bytes >= 1024)");
    }
    for pk in &cfg.write_pubkeys {
        if pk.len() != 64 || !pk.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("[nostr] write_pubkeys: {pk:?} is not a 64-hex pubkey");
        }
    }
    if cfg.cert_path.trim().is_empty() != cfg.key_path.trim().is_empty() {
        bail!("[nostr] cert_path and key_path must be set together");
    }
    for url in &cfg.upstreams {
        upstream::parse_url(url).with_context(|| format!("[nostr] upstreams: {url:?}"))?;
    }
    upstream_filters(cfg)?;
    for url in &cfg.bridge_relays {
        upstream::parse_url(url).with_context(|| format!("[nostr] bridge_relays: {url:?}"))?;
    }
    Ok(())
}

/// Parses `[nostr] upstream_filters` (JSON objects as text).
fn upstream_filters(cfg: &NostrConfig) -> Result<Vec<Filter>> {
    cfg.upstream_filters
        .iter()
        .map(|text| {
            let value: Value = serde_json::from_str(text)
                .with_context(|| format!("[nostr] upstream_filters: {text:?} is not JSON"))?;
            Filter::parse(&value).map_err(|e| anyhow!("[nostr] upstream_filters: {text:?}: {e}"))
        })
        .collect()
}

async fn start(state: &Arc<AppState>) -> Result<Value> {
    let mut guard = RUNNING.lock().await;
    if guard.is_none() {
        *guard = Some(launch(state).await?);
    }
    persist_enabled(state, true);
    Ok(status_of(guard.as_ref()))
}

async fn launch(state: &Arc<AppState>) -> Result<Running> {
    let cfg = state.config().nostr;
    validate(&cfg)?;
    let dir = crate::config::data_dir()
        .context("nostr: resolving data dir")?
        .join("nostr");

    let store = {
        let dir = dir.clone();
        let limits = StoreLimits {
            max_events: cfg.max_events,
        };
        tokio::task::spawn_blocking(move || Store::open(Some(&dir), limits, now_secs()))
            .await
            .context("nostr: loading events")??
    };

    let acceptor = if cfg.tls {
        let (cfg, dir) = (cfg.clone(), dir.clone());
        let (acceptor, info) = tokio::task::spawn_blocking(move || tls::acceptor(&cfg, &dir))
            .await
            .context("nostr: preparing TLS")??;
        Some((acceptor, info))
    } else {
        None
    };

    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("nostr: binding {}", cfg.listen))?;
    let local = listener.local_addr()?;
    let scheme = if acceptor.is_some() { "wss" } else { "ws" };
    let url = format!("{scheme}://{local}");

    let (mesh_tx, mesh_rx) = if cfg.rooms.is_empty() {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel(MESH_BUFFER);
        (Some(tx), Some(rx))
    };
    let (upstream_tx, upstream_rx) = if cfg.upstreams.is_empty() && !cfg.signaling_bridge {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel(MESH_BUFFER);
        (Some(tx), Some(rx))
    };
    let relay = Arc::new(Relay::new(
        cfg.clone(),
        store,
        Some(dir),
        mesh_tx,
        upstream_tx,
    ));
    let (shutdown, shutdown_rx) = watch::channel(false);

    let mut tasks = Vec::new();
    let (tls_acceptor, tls_info) = match acceptor {
        Some((a, i)) => (Some(a), Some(i)),
        None => (None, None),
    };
    tasks.push(tokio::spawn(server::serve(
        relay.clone(),
        listener,
        tls_acceptor,
        shutdown_rx.clone(),
    )));
    {
        let relay = relay.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            loop {
                tokio::select! {
                    _ = tick.tick() => relay.flush().await,
                    _ = shutdown_rx.changed() => {
                        relay.flush().await;
                        return;
                    }
                }
            }
        }));
    }

    let mesh = match mesh_rx {
        Some(rx) => match mesh::Mesh::start(state, relay.clone(), cfg.rooms.clone(), rx).await {
            Ok(mesh) => Some(mesh),
            Err(error) => {
                // The relay itself still serves clients.
                warn!(%error, "nostr: mesh replication failed to start");
                None
            }
        },
        None => None,
    };
    let upstreams = upstream_rx.map(|rx| {
        // Without a running cluster this relay is its own gateway.
        let gateway = match &mesh {
            Some(mesh) => mesh.gateway(),
            None => watch::channel(true).1,
        };
        upstream::Upstreams::start(
            relay.clone(),
            upstream::Plan {
                upstreams: cfg.upstreams.clone(),
                filters: upstream_filters(&cfg).unwrap_or_default(),
                publish: cfg.upstream_publish,
                bridge: cfg.signaling_bridge.then(|| cfg.bridge_relays.clone()),
                mirror_kinds: cfg.mirror_kinds.clone(),
            },
            gateway,
            rx,
        )
    });
    info!(%url, events = relay.event_count(), "nostr: relay listening");
    Ok(Running {
        relay,
        url,
        tls: tls_info,
        shutdown,
        tasks,
        mesh,
        upstreams,
    })
}

async fn stop(state: &Arc<AppState>) -> Result<Value> {
    persist_enabled(state, false);
    let mut guard = RUNNING.lock().await;
    if let Some(running) = guard.take() {
        let _ = running.shutdown.send(true);
        if let Some(upstreams) = &running.upstreams {
            upstreams.stop().await;
        }
        if let Some(mesh) = &running.mesh {
            mesh.stop().await;
        }
        for task in running.tasks {
            // The flusher exits after its final flush; the server drops its
            // listener and connections on the shutdown signal.
            if tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .is_err()
            {
                warn!("nostr: a relay task did not stop in time");
            }
        }
        info!("nostr: relay stopped");
    }
    Ok(status_of(guard.as_ref()))
}

/// Persists `[nostr] enabled` (best-effort, like `tunnel`'s): the live
/// start/stop already happened and must not be reported as failed.
fn persist_enabled(state: &Arc<AppState>, enabled: bool) {
    let mut cfg = state.config();
    if cfg.nostr.enabled == enabled {
        return;
    }
    cfg.nostr.enabled = enabled;
    match cfg.save() {
        Ok(()) => state.set_config(cfg),
        Err(error) => {
            warn!(%error, enabled, "nostr: failed to persist [nostr] enabled")
        }
    }
}

fn status_of(running: Option<&Running>) -> Value {
    let Some(running) = running else {
        return json!({ "running": false });
    };
    let relay = &running.relay;
    json!({
        "running": true,
        "url": running.url,
        "listen": relay.cfg.listen,
        "tls": running.tls.as_ref().map(|t| json!({
            "source": t.source,
            "cert_path": t.cert_path,
            "sha256": t.sha256_fingerprint,
        })),
        "events": relay.event_count(),
        "connections": relay.connections.load(Ordering::Relaxed),
        "subscriptions": relay.subscriptions.load(Ordering::Relaxed),
        "accepted": relay.accepted.load(Ordering::Relaxed),
        "rejected": relay.rejected.load(Ordering::Relaxed),
        "rooms": relay.cfg.rooms,
        "mesh": running.mesh.as_ref().map(|m| m.status()),
        "upstreams": running.upstreams.as_ref().map(|u| u.status()),
    })
}

/// Starts the relay at daemon startup when `[nostr] enabled`.
pub fn spawn_background(state: Arc<AppState>) {
    if !state.config().nostr.enabled {
        return;
    }
    tokio::spawn(async move {
        let mut guard = RUNNING.lock().await;
        if guard.is_some() {
            return;
        }
        match launch(&state).await {
            Ok(running) => *guard = Some(running),
            Err(error) => warn!(%error, "nostr: relay failed to start"),
        }
    });
}

fn filters_arg(args: &Value) -> Result<Vec<Filter>> {
    let raw: Vec<Value> = match (args.get("filters"), args.get("filter")) {
        (Some(Value::Array(list)), _) => list.clone(),
        (_, Some(single)) if single.is_object() => vec![single.clone()],
        (None, None) => vec![json!({})],
        _ => bail!("`filters` must be an array of objects (or `filter` an object)"),
    };
    raw.iter()
        .map(|v| Filter::parse(v).map_err(|e| anyhow!("invalid filter: {e}")))
        .collect()
}

/// IPC entry point for `nostr.*` commands.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "nostr.status" => Ok(status_of(RUNNING.lock().await.as_ref())),
        "nostr.start" => start(state).await,
        "nostr.stop" => stop(state).await,
        "nostr.query" => {
            let filters = filters_arg(&args)?;
            let guard = RUNNING.lock().await;
            let running = guard.as_ref().context("the nostr relay is not running")?;
            let cap = args
                .get("limit")
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .unwrap_or(running.relay.cfg.max_limit)
                .min(running.relay.cfg.max_limit);
            let events = running.relay.query(&filters, cap, now_secs());
            Ok(json!({ "events": events }))
        }
        _ => bail!("unknown command: {cmd}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event::{test_event, test_pubkey};

    fn relay(cfg: NostrConfig) -> Relay {
        let store = Store::open(None, StoreLimits { max_events: 100 }, 0).unwrap();
        Relay::new(cfg, store, None, None, None)
    }

    #[test]
    fn submit_accepts_then_reports_duplicates() {
        let relay = relay(NostrConfig::default());
        let ev = test_event(&[1; 32], 1_000, 1, vec![], "hi");
        let first = relay.submit(ev.clone(), &Origin::Client, 1_000);
        assert!(first.accepted && first.fresh);
        let again = relay.submit(ev, &Origin::Client, 1_000);
        assert!(again.accepted && !again.fresh);
        assert!(again.message.starts_with("duplicate:"));
    }

    #[test]
    fn tampered_copy_of_a_known_event_is_invalid_not_duplicate() {
        let relay = relay(NostrConfig::default());
        let ev = test_event(&[1; 32], 1_000, 1, vec![], "hi");
        relay.submit(ev.clone(), &Origin::Client, 1_000);
        let mut forged = ev;
        forged.content = "tampered".into();
        let result = relay.submit(forged, &Origin::Client, 1_000);
        assert!(!result.accepted);
        assert!(result.message.starts_with("invalid:"), "{}", result.message);
    }

    #[test]
    fn write_policy_and_clock_checks() {
        let cfg = NostrConfig {
            write_pubkeys: vec![test_pubkey(&[1; 32])],
            ..NostrConfig::default()
        };
        let relay = relay(cfg);
        let other = test_event(&[2; 32], 1_000, 1, vec![], "x");
        let result = relay.submit(other, &Origin::Client, 1_000);
        assert!(result.message.starts_with("restricted:"));
        let future = test_event(&[1; 32], 1_000 + MAX_FUTURE_SECS + 1, 1, vec![], "x");
        assert!(!relay.submit(future, &Origin::Client, 1_000).accepted);
        let expired = test_event(
            &[1; 32],
            900,
            1,
            vec![vec!["expiration".into(), "950".into()]],
            "x",
        );
        assert!(!relay.submit(expired, &Origin::Client, 1_000).accepted);
    }

    #[test]
    fn ephemeral_events_fan_out_once_without_being_stored() {
        let relay = relay(NostrConfig::default());
        let mut live = relay.subscribe();
        let ev = test_event(&[1; 32], 1_000, 20_001, vec![], "typing");
        assert!(relay.submit(ev.clone(), &Origin::Client, 1_000).fresh);
        assert!(!relay.submit(ev.clone(), &Origin::Client, 1_000).fresh);
        assert_eq!(live.try_recv().unwrap().id, ev.id);
        assert!(live.try_recv().is_err());
        assert_eq!(relay.event_count(), 0);
    }
}
