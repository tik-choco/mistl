//! The mistl web host core: known sites, trust, installs and the p2p record
//! exchange. The gateway (stage 3) consumes exactly the public surface here.
//!
//! Pins: the store pins per root CID with no refcount (a re-pin overwrites,
//! an unpin drops the root), so web never unpins ad hoc. Every root it pins
//! is first recorded in the pin ledger (`<data_dir>/web/pins.json`), and
//! [`WebHost::reconcile`] unpins ledger roots that neither an installed
//! version (the ready cache) nor an in-flight publish/install claim needs.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard, OnceCell, Semaphore};

use super::site::{self, Manifest, SiteRecord};
use super::state::{PinLedger, Upsert, WebState, desired_cids, may_install, release_plan};
use super::wire::{self, Msg, Pending, RateLimiter};
use crate::daemon::AppState;
use crate::net::{self, RoomUse};
use crate::storage::Store;

/// Per-block peer timeout while installing.
const BLOCK_TIMEOUT: Duration = Duration::from_secs(5);
/// Concurrent installs.
const MAX_INSTALLS: usize = 2;
/// Concurrent inbound-message tasks (excess messages are dropped).
const MAX_HANDLER_TASKS: usize = 32;
/// Update checks per site.
const UPDATE_INTERVAL: Duration = Duration::from_secs(60);
const MAX_UPDATE_CHECKS: usize = 1024;
/// Re-installs of a site in one install run (a newer record may arrive
/// while one version downloads).
const MAX_INSTALL_ROUNDS: usize = 4;
/// `last_open` alone is persisted at most this often (it is kept in memory
/// and written with any other state change).
const LAST_OPEN_FLUSH: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct SiteView {
    pub record: SiteRecord,
    pub trusted: bool,
    pub own: bool,
    /// The fully installed version (never a partial one).
    pub ready: Option<Arc<Manifest>>,
    pub ready_cid: Option<String>,
    /// Record seq of the installed version (`None` if not installed or
    /// unknown from an older state file).
    pub installed_seq: Option<u64>,
    pub progress: Option<Progress>,
}

#[derive(Clone, Serialize)]
pub struct Progress {
    /// `"installing"` | `"failed"`.
    pub state: &'static str,
    pub done_bytes: u64,
    pub total_bytes: u64,
    pub error: Option<String>,
}

#[derive(Clone)]
struct Ready {
    cid: String,
    manifest: Arc<Manifest>,
}

/// CIDs claimed by publishes / installs in flight (not in the ready cache
/// yet). Only touched in short std-mutex sections, never across an await.
#[derive(Default)]
struct Claims {
    sets: HashMap<u64, HashSet<String>>,
    next: u64,
}

pub struct WebHost {
    pub(super) app: Arc<AppState>,
    store: Arc<Store>,
    path: PathBuf,
    port: u16,
    pub(super) sites: Mutex<WebState>,
    /// Generation of the last state snapshot taken / written, so writes
    /// done off the lock never replace a newer file with an older one.
    state_gen: AtomicU64,
    state_written: Arc<StdMutex<u64>>,
    last_open_flush: StdMutex<Option<Instant>>,
    ready: RwLock<HashMap<String, Ready>>,
    progress: StdMutex<HashMap<String, Progress>>,
    installing: StdMutex<HashSet<String>>,
    install_slots: Arc<Semaphore>,
    pending: StdMutex<Pending>,
    want_limits: StdMutex<RateLimiter>,
    record_limits: StdMutex<RateLimiter>,
    handler_slots: Arc<Semaphore>,
    last_check: StdMutex<HashMap<String, Instant>>,
    claims: StdMutex<Claims>,
    /// The pin ledger. Its lock also serializes [`Self::reconcile`]: a
    /// claim made after reconcile's snapshot must [`Self::track`] (and so
    /// wait) before it pins, so it can never be released under it.
    ledger: Mutex<PinLedger>,
    joined: AtomicBool,
    join_lock: Mutex<()>,
}

static HOST: OnceCell<Arc<WebHost>> = OnceCell::const_new();

/// The process-wide host (lazy): loads `state.json`, opens the store (so
/// installed blocks are served to peers), loads installed manifests and
/// registers the room handler. Fails when `[web] enabled = false`.
pub async fn host(state: &Arc<AppState>) -> Result<Arc<WebHost>> {
    HOST.get_or_try_init(|| WebHost::init(state)).await.cloned()
}

/// The host if it has been started.
pub fn get() -> Option<Arc<WebHost>> {
    HOST.get().cloned()
}

fn lock<T>(m: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().expect("web host lock poisoned")
}

/// Read a site manifest stored locally (≤ [`site::MAX_MANIFEST_BYTES`]).
async fn load_local_manifest(store: &Store, cid: &str, max_total: u64) -> Result<Manifest> {
    let file = store
        .local_file(cid)
        .await?
        .with_context(|| format!("site manifest {cid} is not stored locally"))?;
    if file.size > site::MAX_MANIFEST_BYTES as u64 {
        bail!(
            "site manifest {cid} is larger than {} bytes",
            site::MAX_MANIFEST_BYTES
        );
    }
    let mut bytes = Vec::with_capacity(file.size as usize);
    for chunk in &file.chunks {
        let data = store
            .get_local(chunk)
            .await?
            .with_context(|| format!("site manifest {cid} is incomplete"))?;
        bytes.extend_from_slice(&data);
        if bytes.len() > site::MAX_MANIFEST_BYTES {
            bail!(
                "site manifest {cid} is larger than {} bytes",
                site::MAX_MANIFEST_BYTES
            );
        }
    }
    if bytes.len() as u64 != file.size {
        bail!("site manifest {cid} has the wrong size");
    }
    Manifest::parse(&bytes, max_total)
}

impl WebHost {
    async fn init(app: &Arc<AppState>) -> Result<Arc<Self>> {
        let cfg = app.config().web;
        if !cfg.enabled {
            bail!("mistl web is disabled ([web] enabled = false)");
        }
        let port = cfg
            .listen
            .parse::<SocketAddr>()
            .context("invalid [web] listen")?
            .port();
        // Opening the store registers the block provider, so installed
        // blocks are served to peers right after a daemon restart.
        let store = crate::storage::store(app).await?;
        let dir = crate::config::data_dir()?.join("web");
        let path = dir.join("state.json");
        let mut sites = WebState::load(&path)?;

        // Installed / own versions load under the format cap, not
        // `[web] max_site_bytes` (that only gates new installs/publishes).
        let mut ready = HashMap::new();
        let mut seed = BTreeSet::new();
        let mut changed = false;
        for (id, entry) in sites.sites.iter_mut() {
            let Some(cid) = entry.installed.clone() else {
                continue;
            };
            seed.insert(cid.clone());
            match load_local_manifest(&store, &cid, site::MAX_SITE_BYTES).await {
                Ok(manifest) => {
                    seed.extend(manifest.files.iter().map(|f| f.cid.clone()));
                    ready.insert(
                        id.clone(),
                        Ready {
                            cid,
                            manifest: Arc::new(manifest),
                        },
                    );
                }
                Err(error) => {
                    tracing::warn!(site = %id, %error, "web: installed version is unusable; marking it not installed");
                    entry.installed = None;
                    entry.installed_seq = None;
                    changed = true;
                }
            }
        }
        // Pin ledger; first start seeds it from the
        // installed versions so their existing pins are tracked.
        let ledger_path = dir.join("pins.json");
        let ledger = match PinLedger::load(ledger_path.clone())? {
            Some(ledger) => ledger,
            None => {
                let mut ledger = PinLedger::empty(ledger_path);
                ledger.roots = seed;
                ledger.save()?;
                ledger
            }
        };
        if changed {
            sites.save(&path)?;
        }

        let host = Arc::new(Self {
            app: app.clone(),
            store,
            path,
            port,
            sites: Mutex::new(sites),
            state_gen: AtomicU64::new(0),
            state_written: Arc::new(StdMutex::new(0)),
            last_open_flush: StdMutex::new(None),
            ready: RwLock::new(ready),
            progress: StdMutex::new(HashMap::new()),
            installing: StdMutex::new(HashSet::new()),
            install_slots: Arc::new(Semaphore::new(MAX_INSTALLS)),
            pending: StdMutex::new(Pending::default()),
            want_limits: StdMutex::new(RateLimiter::default()),
            record_limits: StdMutex::new(RateLimiter::default()),
            handler_slots: Arc::new(Semaphore::new(MAX_HANDLER_TASKS)),
            last_check: StdMutex::new(HashMap::new()),
            claims: StdMutex::new(Claims::default()),
            ledger: Mutex::new(ledger),
            joined: AtomicBool::new(false),
            join_lock: Mutex::new(()),
        });
        // Releases unusable installed versions and anything left over from
        // an interrupted publish/install.
        host.reconcile().await;
        register_handler(host.clone(), tokio::runtime::Handle::current());
        Ok(host)
    }

    /// Persist `st` (consuming the guard): serialized under the lock, written
    /// on the blocking pool after it is released. Generations keep a slow
    /// older write from replacing a newer file.
    async fn persist(&self, st: MutexGuard<'_, WebState>) -> Result<()> {
        let bytes = st.to_bytes()?;
        let generation = self.state_gen.fetch_add(1, Ordering::AcqRel) + 1;
        drop(st);
        self.write_snapshot(bytes, generation).await
    }

    /// For install/remove transitions, callers keep the sites lock until the
    /// snapshot is durable, then change the ready cache. Reconcile can still
    /// run in the meantime, but must keep the old installed pins until then.
    async fn persist_transition(&self, st: &WebState) -> Result<()> {
        let bytes = st.to_bytes()?;
        let generation = self.state_gen.fetch_add(1, Ordering::AcqRel) + 1;
        self.write_snapshot(bytes, generation).await
    }

    async fn write_snapshot(&self, bytes: Vec<u8>, generation: u64) -> Result<()> {
        let written = self.state_written.clone();
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut last = written.lock().expect("web state writer poisoned");
            if generation <= *last {
                return Ok(());
            }
            crate::statefile::write_bytes(&path, &bytes)
                .with_context(|| format!("writing {}", path.display()))?;
            *last = generation;
            Ok(())
        })
        .await
        .context("writing web state")?
    }

    async fn persist_logged(&self, st: MutexGuard<'_, WebState>) {
        if let Err(error) = self.persist(st).await {
            tracing::warn!(error = %format!("{error:#}"), "web: saving state");
        }
    }

    /// Gateway port, from `[web] listen`.
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn store(&self) -> Arc<Store> {
        self.store.clone()
    }

    /// `http://<site>.localhost:<port>/`.
    pub fn site_url(&self, site: &str) -> String {
        format!("http://{site}.localhost:{}/", self.port)
    }

    pub fn room_joined(&self) -> bool {
        self.joined.load(Ordering::Acquire)
    }

    /// Join `mistl-web-v1` once, if the network is permitted.
    pub async fn ensure_joined(&self) -> bool {
        if !self.app.network.permitted() || !self.app.config().web.enabled {
            return false;
        }
        if self.room_joined() {
            return true;
        }
        let _guard = self.join_lock.lock().await;
        if !self.app.network.permitted() || !self.app.config().web.enabled {
            return false;
        }
        if self.room_joined() {
            return true;
        }
        match net::ensure_started(&self.app, RoomUse::Web, wire::ROOM.to_string()).await {
            Ok(_) => {
                self.joined.store(true, Ordering::Release);
                true
            }
            Err(error) => {
                tracing::warn!(%error, "web: joining {}", wire::ROOM);
                false
            }
        }
    }

    /// Broadcast in the web room; false when offline or the send failed.
    pub(super) async fn broadcast(&self, bytes: Vec<u8>) -> bool {
        if !self.ensure_joined().await {
            return false;
        }
        match net::send_broadcast(wire::ROOM, bytes).await {
            Ok(()) => true,
            Err(error) => {
                tracing::debug!(%error, "web: broadcast failed");
                false
            }
        }
    }

    fn view_locked(&self, st: &WebState, site: &str) -> Option<SiteView> {
        let entry = st.sites.get(site)?;
        let ready = self
            .ready
            .read()
            .expect("web ready lock poisoned")
            .get(site)
            .cloned();
        Some(SiteView {
            record: entry.record.clone(),
            trusted: st.is_trusted(site),
            own: entry.own,
            ready_cid: ready.as_ref().map(|r| r.cid.clone()),
            installed_seq: ready.as_ref().and(entry.installed_seq),
            ready: ready.map(|r| r.manifest),
            progress: lock(&self.progress).get(site).cloned(),
        })
    }

    /// Local knowledge only.
    pub async fn view(&self, site: &str) -> Option<SiteView> {
        let st = self.sites.lock().await;
        self.view_locked(&st, site)
    }

    pub async fn list(&self) -> Vec<SiteView> {
        let st = self.sites.lock().await;
        let mut views: Vec<SiteView> = st
            .sites
            .keys()
            .filter_map(|id| self.view_locked(&st, id))
            .collect();
        views.sort_by(|a, b| {
            (&a.record.name, &a.record.site).cmp(&(&b.record.name, &b.record.site))
        });
        views
    }

    /// The known record, else ask peers (`want`) and wait up to `wait`.
    pub async fn resolve(self: &Arc<Self>, site: &str, wait: Duration) -> Option<SiteRecord> {
        if !site::is_site_id(site) {
            return None;
        }
        if let Some(view) = self.view(site).await {
            return Some(view.record);
        }
        if !self.ensure_joined().await {
            return None;
        }
        let mut rx = lock(&self.pending).register(site, Instant::now())?;
        if !self.broadcast(wire::want(site)).await {
            return None;
        }
        let _ = tokio::time::timeout(wait, rx.wait_for(|done| *done)).await;
        self.view(site).await.map(|v| v.record)
    }

    /// Trust a known site, or (`publisher`) everything its publisher signs.
    pub async fn trust(&self, site: &str, publisher: bool) -> Result<()> {
        let mut st = self.sites.lock().await;
        let entry = st
            .sites
            .get_mut(site)
            .with_context(|| format!("unknown site {site}"))?;
        if publisher {
            let did = entry.record.did.clone();
            st.trusted_publishers.insert(did);
        } else {
            entry.trusted = true;
        }
        self.persist(st).await
    }

    /// Withdraw trust from a site (and, with `publisher`, from its
    /// publisher). Returns the effective trust afterwards: still `true` when
    /// the publisher remains trusted.
    pub async fn untrust(&self, site: &str, publisher: bool) -> Result<bool> {
        let mut st = self.sites.lock().await;
        let entry = st
            .sites
            .get_mut(site)
            .with_context(|| format!("unknown site {site}"))?;
        entry.trusted = false;
        if publisher {
            let did = entry.record.did.clone();
            st.trusted_publishers.remove(&did);
        }
        let still = st.is_trusted(site);
        self.persist(st).await?;
        Ok(still)
    }

    /// Trust (`true`) or stop trusting a publisher DID directly.
    pub async fn set_publisher_trust(&self, did: &str, trusted: bool) -> Result<()> {
        crate::identity::pubkey_from_did(did).context("not a valid did:key")?;
        let mut st = self.sites.lock().await;
        if trusted {
            st.trusted_publishers.insert(did.to_string());
        } else {
            st.trusted_publishers.remove(did);
        }
        self.persist(st).await
    }

    /// Record a top-level open. `last_open` only orders eviction, so on its
    /// own it is written at most once per [`LAST_OPEN_FLUSH`]; any other
    /// state write carries it too.
    pub async fn mark_opened(&self, site: &str) {
        let mut st = self.sites.lock().await;
        let Some(entry) = st.sites.get_mut(site) else {
            return;
        };
        entry.last_open = Some(net::peer_auth::now_ms());
        let due = {
            let mut last = lock(&self.last_open_flush);
            let now = Instant::now();
            let due = last.is_none_or(|t| now.saturating_duration_since(t) >= LAST_OPEN_FLUSH);
            if due {
                *last = Some(now);
            }
            due
        };
        if due {
            self.persist_logged(st).await;
        }
    }

    /// Forget a site; reconcile then unpins whatever of its installed
    /// version no other site / in-flight operation needs. False if unknown.
    pub async fn remove(&self, site: &str) -> Result<bool> {
        {
            let mut st = self.sites.lock().await;
            let Some(entry) = st.remove(site) else {
                return Ok(false);
            };
            if let Err(error) = self.persist_transition(&st).await {
                st.sites.insert(site.to_string(), entry);
                return Err(error);
            }
            self.ready
                .write()
                .expect("web ready lock poisoned")
                .remove(site);
        }
        lock(&self.progress).remove(site);
        self.reconcile().await;
        Ok(true)
    }

    /// Ask peers for a newer record, at most once per minute per site. A
    /// newer verified record for an installed, trusted site installs in the
    /// background and switches over only when complete.
    pub fn check_update(self: &Arc<Self>, site: &str) {
        let now = Instant::now();
        {
            let mut checks = lock(&self.last_check);
            if checks
                .get(site)
                .is_some_and(|t| now.saturating_duration_since(*t) < UPDATE_INTERVAL)
            {
                return;
            }
            if checks.len() >= MAX_UPDATE_CHECKS {
                checks.retain(|_, t| now.saturating_duration_since(*t) < UPDATE_INTERVAL);
                if checks.len() >= MAX_UPDATE_CHECKS {
                    return;
                }
            }
            checks.insert(site.to_string(), now);
        }
        let host = self.clone();
        let site = site.to_string();
        tokio::spawn(async move {
            host.broadcast(wire::want(&site)).await;
        });
    }

    // ---- Pins ----
    //
    // Protocol for anything that pins: `claim` the CIDs, `track` them (ledger
    // persisted), then pin; when done `drop_claim` and `reconcile`.

    /// Register an in-flight claim on `cids` (kept by reconcile).
    pub(super) fn claim(&self, cids: impl IntoIterator<Item = String>) -> u64 {
        let mut claims = lock(&self.claims);
        claims.next += 1;
        let token = claims.next;
        claims.sets.insert(token, cids.into_iter().collect());
        token
    }

    pub(super) fn claim_extend(&self, token: u64, cids: impl IntoIterator<Item = String>) {
        if let Some(set) = lock(&self.claims).sets.get_mut(&token) {
            set.extend(cids);
        }
    }

    pub(super) fn drop_claim(&self, token: u64) {
        lock(&self.claims).sets.remove(&token);
    }

    /// Record `cids` in the pin ledger (persisted) before they are pinned.
    /// Claim them first, or a concurrent reconcile may release them again.
    pub(super) async fn track(&self, cids: impl IntoIterator<Item = String>) -> Result<()> {
        let mut ledger = self.ledger.lock().await;
        let added: Vec<String> = cids
            .into_iter()
            .filter(|cid| ledger.roots.insert(cid.clone()))
            .collect();
        if added.is_empty() {
            return Ok(());
        }
        if let Err(error) = ledger.save() {
            for cid in &added {
                ledger.roots.remove(cid);
            }
            return Err(error.context("saving the web pin ledger"));
        }
        Ok(())
    }

    /// Unpin every ledger root that no installed version and no in-flight
    /// claim needs. Serialized by the ledger lock; the only other locks
    /// taken are short std sections to snapshot the desired set.
    pub(super) async fn reconcile(&self) {
        let mut ledger = self.ledger.lock().await;
        let desired = {
            let ready = self.ready.read().expect("web ready lock poisoned");
            let claims = lock(&self.claims);
            desired_cids(
                ready.values().map(|r| (r.cid.as_str(), &*r.manifest)),
                claims.sets.values(),
            )
        };
        let plan = release_plan(&ledger.roots, &desired);
        if plan.is_empty() {
            return;
        }
        let mut changed = false;
        for cid in plan {
            // Idempotent: an unknown / already unpinned root is a no-op.
            match self.store.unpin_file(&cid).await {
                Ok(()) => {
                    ledger.roots.remove(&cid);
                    changed = true;
                }
                Err(error) => tracing::warn!(%error, %cid, "web: unpinning"),
            }
        }
        if changed && let Err(error) = ledger.save() {
            tracing::warn!(%error, "web: saving the pin ledger");
        }
    }

    /// Make `cid`/`manifest` (record seq `seq`) the installed version of
    /// `site` (own sites are also marked own + trusted). Returns false, and
    /// changes nothing, unless `seq` is newer than the installed version and
    /// not newer than the held record (see [`may_install`]). The caller
    /// drops its claim and reconciles afterwards, which releases what only
    /// the previous version used. Returns false if the site was removed.
    pub(super) async fn swap_installed(
        &self,
        site: &str,
        seq: u64,
        cid: &str,
        manifest: Arc<Manifest>,
        own: bool,
    ) -> Result<bool> {
        let mut st = self.sites.lock().await;
        let Some(entry) = st.sites.get_mut(site) else {
            return Ok(false);
        };
        if !may_install(entry, seq) {
            return Ok(false);
        }
        let previous = entry.clone();
        if own {
            entry.own = true;
            entry.trusted = true;
        }
        entry.installed = Some(cid.to_string());
        entry.installed_seq = Some(seq);
        if let Err(error) = self.persist_transition(&st).await {
            st.sites.insert(site.to_string(), previous);
            return Err(error);
        }
        self.ready.write().expect("web ready lock poisoned").insert(
            site.to_string(),
            Ready {
                cid: cid.to_string(),
                manifest,
            },
        );
        lock(&self.progress).remove(site);
        Ok(true)
    }

    // ---- Install ----

    fn set_progress(&self, site: &str, progress: Progress) {
        lock(&self.progress).insert(site.to_string(), progress);
    }

    /// Start installing the current version in the background. A no-op if
    /// already installing, up to date, unknown or untrusted. Records progress
    /// before spawning so the caller can immediately report it.
    pub async fn start_install(self: &Arc<Self>, site: &str) {
        let st = self.sites.lock().await;
        let Some(entry) = st.sites.get(site) else {
            return;
        };
        if !st.is_trusted(site) || !may_install(entry, entry.record.seq) {
            return;
        }
        if !lock(&self.installing).insert(site.to_string()) {
            return;
        }
        self.set_progress(
            site,
            Progress {
                state: "installing",
                done_bytes: 0,
                total_bytes: entry.record.size,
                error: None,
            },
        );
        drop(st);
        let host = self.clone();
        let site = site.to_string();
        tokio::spawn(async move {
            host.install_rounds(&site).await;
            lock(&host.installing).remove(&site);
        });
    }

    async fn install_rounds(&self, site: &str) {
        for _ in 0..MAX_INSTALL_ROUNDS {
            let (record, needed, trusted) = {
                let st = self.sites.lock().await;
                let Some(entry) = st.sites.get(site) else {
                    lock(&self.progress).remove(site);
                    return;
                };
                (
                    entry.record.clone(),
                    may_install(entry, entry.record.seq),
                    st.is_trusted(site),
                )
            };
            if !needed {
                lock(&self.progress).remove(site);
                return;
            }
            let fail = |error: String| Progress {
                state: "failed",
                done_bytes: 0,
                total_bytes: record.size,
                error: Some(error),
            };
            if !trusted {
                self.set_progress(site, fail("site is not trusted".into()));
                return;
            }
            self.set_progress(
                site,
                Progress {
                    state: "installing",
                    done_bytes: 0,
                    total_bytes: record.size,
                    error: None,
                },
            );
            let _permit = match self.install_slots.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };
            match self.install_version(&record).await {
                Ok(true) => {
                    lock(&self.progress).remove(site);
                }
                Ok(false) => {
                    lock(&self.progress).remove(site);
                    return;
                }
                Err(error) => {
                    let error = format!("{error:#}");
                    tracing::warn!(site = %site, %error, "web: install failed");
                    self.set_progress(site, fail(error));
                    return;
                }
            }
        }
    }

    async fn fetch_rooms(&self) -> Vec<String> {
        if !self.ensure_joined().await {
            return Vec::new();
        }
        let mut rooms = vec![wire::ROOM.to_string()];
        for room in self.app.config().storage.room_ids {
            let room = room.trim().to_string();
            if !room.is_empty() && !rooms.contains(&room) {
                rooms.push(room);
            }
        }
        rooms
    }

    async fn install_version(&self, record: &SiteRecord) -> Result<bool> {
        let max_total = self.app.config().web.max_site_bytes;
        if record.size > max_total {
            bail!(
                "site is {} bytes, over [web] max_site_bytes ({max_total})",
                record.size
            );
        }
        let rooms = self.fetch_rooms().await;
        let token = self.claim([record.manifest.clone()]);
        let result = self.install_claimed(record, token, &rooms, max_total).await;
        self.drop_claim(token);
        self.reconcile().await;
        result
    }

    async fn install_claimed(
        &self,
        record: &SiteRecord,
        token: u64,
        rooms: &[String],
        max_total: u64,
    ) -> Result<bool> {
        let site = record.site.as_str();
        self.track([record.manifest.clone()]).await?;
        self.store
            .fetch_file_pinned(
                &record.manifest,
                site::MAX_MANIFEST_BYTES as u64,
                rooms,
                BLOCK_TIMEOUT,
                &|_: u64| {},
            )
            .await
            .context("fetching the site manifest")?;
        let manifest = load_local_manifest(&self.store, &record.manifest, max_total).await?;
        if manifest.name != record.name {
            bail!("manifest name does not match the site record");
        }
        if manifest.total_size() != record.size || manifest.files.len() != record.files as usize {
            bail!("manifest size / file count does not match the site record");
        }
        self.claim_extend(token, manifest.files.iter().map(|f| f.cid.clone()));
        self.track(manifest.files.iter().map(|f| f.cid.clone()))
            .await?;

        let total = record.size;
        let mut done = 0u64;
        for file in &manifest.files {
            let base = done;
            let cap = file.size;
            let size = self
                .store
                .fetch_file_pinned(&file.cid, file.size, rooms, BLOCK_TIMEOUT, &|n: u64| {
                    self.set_progress(
                        site,
                        Progress {
                            state: "installing",
                            done_bytes: base + n.min(cap),
                            total_bytes: total,
                            error: None,
                        },
                    )
                })
                .await
                .with_context(|| format!("fetching {}", file.p))?;
            if size != file.size {
                bail!(
                    "{} is {size} bytes, the manifest says {}",
                    file.p,
                    file.size
                );
            }
            done += file.size;
        }
        self.swap_installed(
            site,
            record.seq,
            &record.manifest,
            Arc::new(manifest),
            false,
        )
        .await
    }

    // ---- P2P ----

    async fn on_want(&self, from: &str, site: &str) {
        let raw = {
            let st = self.sites.lock().await;
            st.sites.get(site).map(|e| e.record.raw.clone())
        };
        if let Some(raw) = raw
            && let Err(error) = net::send_direct(wire::ROOM, from, wire::record(&raw)).await
        {
            tracing::debug!(%error, "web: record reply failed");
        }
    }

    async fn on_record(self: &Arc<Self>, raw: Value) {
        let now = net::peer_auth::now_ms();
        let record = match site::verify_record(&raw, now) {
            Ok(record) => record,
            Err(error) => {
                tracing::debug!(%error, "web: dropping an invalid record");
                return;
            }
        };
        let site = record.site.clone();
        let pending = lock(&self.pending).is_pending(&site, Instant::now());
        let start_install = {
            let mut st = self.sites.lock().await;
            if !pending && !st.sites.contains_key(&site) {
                return;
            }
            let outcome = st.upsert_record(record, now);
            let start = outcome == Upsert::Updated
                && st.sites.get(&site).is_some_and(|e| e.installed.is_some())
                && st.is_trusted(&site);
            if matches!(outcome, Upsert::Inserted | Upsert::Updated) {
                self.persist_logged(st).await;
            }
            start
        };
        lock(&self.pending).complete(&site);
        if start_install {
            self.start_install(&site).await;
        }
    }
}

fn register_handler(host: Arc<WebHost>, runtime: tokio::runtime::Handle) {
    net::register_room_handler(move |event_type, room, from, data| {
        if event_type != net::EVENT_RAW || room != wire::ROOM || !wire::looks_like_web(data) {
            return;
        }
        let Some(msg) = wire::parse(data) else {
            return;
        };
        let limits = match msg {
            Msg::Want { .. } => &host.want_limits,
            Msg::Record { .. } => &host.record_limits,
        };
        if !lock(limits).allow(from, Instant::now()) {
            return;
        }
        let Ok(permit) = host.handler_slots.clone().try_acquire_owned() else {
            return;
        };
        let host = host.clone();
        let from = from.to_string();
        runtime.spawn(async move {
            let _permit = permit;
            match msg {
                Msg::Want { site } => host.on_want(&from, &site).await,
                Msg::Record { record } => host.on_record(record).await,
            }
        });
    });
}
