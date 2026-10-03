pub mod ipc;
mod services;

use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::sync::watch;
use tracing::info;

use crate::config::{self, Config};

/// Shared state for all daemon services.
pub struct AppState {
    pub(crate) ai_model_discovery: crate::ai::ModelDiscovery,
    pub network: crate::network::NetworkControl,
    /// Live configuration. Behind a lock so `config.set` can hot-reload it:
    /// services read it when they (re)start, so most changes apply on the
    /// next `*.start` without a daemon restart.
    config: std::sync::RwLock<Config>,
    pub started_at: Instant,
    shutdown: watch::Sender<bool>,
    /// Set by [`AppState::request_restart`]: after the daemon shuts down it
    /// relaunches itself from the (freshly self-updated) executable.
    restart: AtomicBool,
    /// When the daemon last saw an HTTP request that indicates an open
    /// dashboard tab (page load or the dashboard's status poll). Used by
    /// [`crate::web::autoreopen`] to tell "a tab is already open" from "the
    /// dashboard was left closed" both at startup (grace-period check) and
    /// at shutdown (deciding whether to persist `dashboard_open`).
    dashboard_seen: Mutex<Option<Instant>>,
    /// Browsable URL of the dashboard for this run, set once the web server
    /// is bound (`None` while the UI is disabled or failed to bind). Reported
    /// by `daemon.status` so clients -- `mistl daemon start`, `mistl status`
    /// -- can show where the UI actually is, including a `--host` override
    /// that the persisted `ui.listen` doesn't know about.
    dashboard_url: Mutex<Option<String>>,
}

impl AppState {
    /// Snapshot of the current config (cheap; Config is small and cloned).
    pub fn config(&self) -> Config {
        self.config.read().expect("config lock poisoned").clone()
    }

    pub fn set_config(&self, config: Config) {
        *self.config.write().expect("config lock poisoned") = config;
    }

    pub fn set_config_path(&self, path: &str, value: Value) -> Result<()> {
        let mut config = self.config.write().expect("config lock poisoned");
        let updated = config::set_by_path(&config, path, value)?;
        updated.save()?;
        *config = updated;
        Ok(())
    }

    pub fn set_ai_provide(&self, start: bool) -> Result<Vec<String>> {
        let mut config = self.config.write().expect("config lock poisoned");
        let mut updated = config.clone();
        let rooms = crate::ai::set_provide_flags(&mut updated.ai, start);
        updated.save()?;
        *config = updated;
        Ok(rooms)
    }

    pub fn cache_ai_models(
        &self,
        fetched: &crate::config::AiProviderConfig,
        models: &[String],
    ) -> Result<()> {
        let mut config = self.config.write().expect("config lock poisoned");
        let mut updated = config.clone();
        if let Some(provider) = updated.ai.providers.iter_mut().find(|p| {
            p.id == fetched.id
                && p.base_url == fetched.base_url
                && p.api_key == fetched.api_key
                && p.enabled
        }) {
            provider.models = models.to_vec();
            provider.models_fetched_at =
                Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
            updated.save()?;
            *config = updated;
        }
        Ok(())
    }

    pub fn request_shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Shut the daemon down and relaunch it once the sockets are released.
    /// Used by `update apply --restart` so a staged self-update takes effect
    /// immediately instead of on the next manual start.
    pub fn request_restart(&self) {
        self.restart.store(true, Ordering::SeqCst);
        self.request_shutdown();
    }

    fn wants_restart(&self) -> bool {
        self.restart.load(Ordering::SeqCst)
    }

    /// Record that a dashboard HTTP request just came in (see call sites in
    /// `web::server` for exactly which requests count).
    pub fn note_dashboard_activity(&self) {
        *self
            .dashboard_seen
            .lock()
            .expect("dashboard_seen lock poisoned") = Some(Instant::now());
    }

    /// Record the dashboard URL this run is serving (see [`Self::dashboard_url`]).
    pub fn set_dashboard_url(&self, url: String) {
        *self
            .dashboard_url
            .lock()
            .expect("dashboard_url lock poisoned") = Some(url);
    }

    /// The dashboard URL this run is serving, if the UI came up.
    pub fn dashboard_url(&self) -> Option<String> {
        self.dashboard_url
            .lock()
            .expect("dashboard_url lock poisoned")
            .clone()
    }

    /// A minimal [`AppState`] for unit tests: default config, no IPC server,
    /// no web server, nothing spawned. Exists because every field here is
    /// private and the only other constructor is [`daemon_main`], which
    /// stands up the whole daemon -- far too much for a test that just needs
    /// something to hand to a service function taking `&Arc<AppState>` (e.g.
    /// `tunnel::session::SessionContext::switch_room`). The returned state's
    /// shutdown channel has no receiver driving it, so `request_shutdown`
    /// is a no-op rather than actually stopping anything.
    #[cfg(test)]
    pub fn for_test() -> Arc<Self> {
        let (shutdown, _rx) = watch::channel(false);
        // The receiver is dropped immediately: `watch::Sender::send` tolerates
        // having no receivers, so `request_shutdown` stays harmless in tests.
        Arc::new(Self {
            ai_model_discovery: crate::ai::ModelDiscovery::default(),
            network: crate::network::NetworkControl::for_test(),
            config: std::sync::RwLock::new(Config::default()),
            started_at: Instant::now(),
            shutdown,
            restart: AtomicBool::new(false),
            dashboard_seen: Mutex::new(None),
            dashboard_url: Mutex::new(None),
        })
    }

    /// Whether a dashboard request was seen within the last `window`.
    pub fn dashboard_seen_within(&self, window: Duration) -> bool {
        match *self
            .dashboard_seen
            .lock()
            .expect("dashboard_seen lock poisoned")
        {
            Some(seen) => seen.elapsed() <= window,
            None => false,
        }
    }
}

/// Run the daemon in the foreground until Ctrl-C or a `daemon.stop` request.
/// `host_override`, if set, replaces just the host part of the configured
/// `ui.listen` for this run (e.g. `--host 0.0.0.0` to reach the dashboard
/// from another device on the LAN) without touching the persisted config.
pub fn run_foreground(host_override: Option<String>) -> Result<()> {
    crate::runtime::acquire_daemon_lock()?;
    let runtime = tokio::runtime::Runtime::new()?;
    let result = runtime.block_on(daemon_main(host_override));
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
}

async fn daemon_main(host_override: Option<String>) -> Result<()> {
    let config = Config::load()?;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let state = Arc::new(AppState {
        ai_model_discovery: crate::ai::ModelDiscovery::default(),
        network: crate::network::NetworkControl::load(&config::data_dir()?),
        config: std::sync::RwLock::new(config),
        started_at: Instant::now(),
        shutdown: shutdown_tx,
        restart: AtomicBool::new(false),
        dashboard_seen: Mutex::new(None),
        dashboard_url: Mutex::new(None),
    });

    // Membership allowlist must be in place before any room joins.
    // An invalid list refuses startup rather than silently falling open.
    state.config().validate_network()?;
    state.config().network.apply();

    let _ = NETWORK_STATE.set(state.clone());
    let server = ipc::serve(state.clone()).await?;
    info!(port = server.port(), "mistl daemon ready");

    let ui_config = state.config().ui;
    let listen = match &host_override {
        Some(host) => override_listen_host(&ui_config.listen, host),
        None => ui_config.listen.clone(),
    };
    let listen = if state.network.permitted() {
        listen
    } else {
        crate::runtime::offline_listen(&listen)
    };
    let web = if ui_config.enabled {
        match crate::web::serve(state.clone(), &listen).await {
            Ok(web) => {
                // The bind address (`web.url()`) and the URL to actually open
                // differ when bound to a wildcard host (`--host 0.0.0.0`), so
                // report the browsable one -- and print it plainly on stdout,
                // not just as a log line, so `mistl daemon run` in a terminal
                // tells you where the UI is. (In a background start stdout is
                // /dev/null; the `info!` below is what lands in daemon.log,
                // and `daemon.status` carries the URL back to the client.)
                let url = crate::web::dashboard_url(&listen);
                state.set_dashboard_url(url.clone());
                println!("mistl dashboard: {url}");
                info!(url = %crate::web::auth::redact(&url), bind = %web.url(), "web dashboard ready");
                // Restore "the dashboard was open" across restarts: if
                // `ui-state.json` says a tab was open when the previous run
                // shut down, reopen it -- after a grace period that lets an
                // existing tab reconnect first, so a `daemon.restart` (or
                // any fresh start right after one) doesn't pop a duplicate
                // browser tab alongside the one that's already there.
                crate::web::autoreopen::spawn_dashboard_autoreopen(state.clone(), listen.clone());
                Some(web)
            }
            Err(error) => {
                tracing::warn!(%error, "web dashboard failed to start; continuing without it");
                None
            }
        }
    } else {
        None
    };

    services::spawn(state.clone());

    let _tray = crate::tray::spawn(state.clone());

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("interrupted, shutting down"),
        _ = shutdown_rx.wait_for(|&stop| stop) => info!("stop requested, shutting down"),
    }

    if let Some(web) = web {
        web.close().await;
    }
    server.close().await;

    // Persist whether the dashboard was open at shutdown so the next start
    // (in particular, the relaunch below after a `daemon.restart`) knows
    // whether to reopen it -- see `crate::web::autoreopen`. A release
    // dashboard polls `/api/call` every 5s, so "seen within 15s" is a good
    // proxy for "a tab was open just now". Written unconditionally (with the
    // UI disabled there's no activity, so this correctly persists `false`);
    // any failure is logged and never blocks shutdown.
    match config::data_dir() {
        Ok(data_dir) => {
            let ui_state = crate::web::ui_state::UiState {
                dashboard_open: state.dashboard_seen_within(Duration::from_secs(15)),
            };
            if let Err(error) = crate::web::ui_state::write_state(&data_dir, ui_state) {
                tracing::warn!(%error, "failed to persist dashboard UI state");
            }
        }
        Err(error) => tracing::warn!(%error, "failed to resolve data directory for UI state"),
    }

    // A self-update applied with `--restart`, or a `daemon.restart` IPC call,
    // asks us to come back up. The sockets are now released, so relaunch is
    // safe. Forward the same `--host` override this run used -- otherwise a
    // daemon started with `--host 0.0.0.0` for LAN access would come back
    // bound to the configured (typically loopback-only) `ui.listen` host,
    // silently dropping external devices with connection-refused.
    if state.wants_restart() {
        match spawn_daemon(host_override.as_deref(), true) {
            Ok(_) => info!("relaunched daemon from the same instance"),
            Err(error) => tracing::warn!(%error, "failed to relaunch daemon after update"),
        }
    }
    Ok(())
}

/// Substitutes `host` for the host part of `listen` (a `"host:port"`
/// string), keeping the configured port. Backs `--host`: it overrides only
/// the bind address for this run, not the port, and never touches the
/// persisted `ui.listen` config value.
fn override_listen_host(listen: &str, host: &str) -> String {
    let port = listen
        .rsplit_once(':')
        .map(|(_, port)| port)
        .unwrap_or("6480");
    format!("{host}:{port}")
}

/// Relaunch the daemon from the current executable path, detached. The path
/// is unchanged by a self-update (the bytes at it are swapped in place), so
/// this starts the freshly-updated binary. Called from [`daemon_main`] once
/// the IPC/web sockets have been released. `host`, if the daemon that's
/// restarting was itself started with `--host`, is re-forwarded so the
/// relaunched process keeps the same dashboard bind address.
fn spawn_daemon(host: Option<&str>, append: bool) -> Result<std::process::Child> {
    let exe = std::env::current_exe().context("resolving current executable")?;
    let log_path = config::data_dir()?.join("daemon.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(append)
        .write(true)
        .truncate(!append)
        .open(&log_path)
        .with_context(|| format!("opening {}", log_path.display()))?;

    let mut command = std::process::Command::new(exe);
    crate::runtime::child_args(&mut command);
    command.args(["daemon", "run"]);
    if let Some(host) = host {
        command.args(["--host", host]);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    command.spawn().context("spawning daemon process")
}

/// Spawn `mistl daemon run` as a detached background process and wait for it
/// to become reachable, printing its status. `host` is forwarded as
/// `daemon run --host <host>` -- see [`run_foreground`].
pub fn start_background(host: Option<String>) -> Result<()> {
    start_background_impl(false, host.as_deref())
}

/// [`start_background`] without output or a host override, for transparent
/// auto-start on client commands.
pub fn start_background_quiet() -> Result<()> {
    start_background_impl(true, None)
}

fn start_background_impl(quiet: bool, host: Option<&str>) -> Result<()> {
    let existing = ipc::client_request("daemon.status", json!({}));
    if let Err(error) = &existing
        && !error.to_string().contains("daemon is not running")
    {
        bail!("{error:#}");
    }
    if let Ok(status) = existing {
        if quiet {
            return Ok(());
        }
        bail!(
            "daemon already running: {}",
            serde_json::to_string(&status)?
        );
    }

    // Whether to expect a dashboard URL back from `daemon.status` below; a
    // config we can't load is the daemon's problem to report, not ours.
    let ui_enabled = Config::load()
        .map(|config| config.ui.enabled)
        .unwrap_or(false);

    let log_path = config::data_dir()?.join("daemon.log");
    let child = spawn_daemon(host, false)?;

    // Poll until the daemon answers or we give up.
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if let Ok(status) = ipc::client_request("daemon.status", json!({})) {
            if !quiet {
                // The IPC socket comes up a moment before the web listener,
                // so a status that answers immediately can still report no
                // dashboard yet -- re-ask briefly rather than print a URL-less
                // status for a UI that is about to be there.
                let status = if ui_enabled {
                    await_dashboard_url(status)
                } else {
                    status
                };
                println!("{}", serde_json::to_string_pretty(&status)?);
                if let Some(url) = status.get("dashboard").and_then(Value::as_str) {
                    println!("dashboard: {url}");
                }
            }
            return Ok(());
        }
    }
    bail!(
        "daemon (pid {}) did not become ready within 5s; check {}",
        child.id(),
        log_path.display()
    );
}

/// Re-poll `daemon.status` for up to ~2s while it reports no dashboard URL,
/// returning the first status that has one (or the last answer if none
/// appears -- a UI that fails to bind reports `"dashboard": null` forever, and
/// that must not stall the start). Only called when the UI is enabled; with
/// `[ui] enabled = false` there is nothing to wait for.
fn await_dashboard_url(status: Value) -> Value {
    if has_dashboard_url(&status) {
        return status;
    }
    let mut latest = status;
    for _ in 0..20 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        match ipc::client_request("daemon.status", json!({})) {
            Ok(status) => {
                latest = status;
                if has_dashboard_url(&latest) {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    latest
}

fn has_dashboard_url(status: &Value) -> bool {
    status.get("dashboard").is_some_and(|url| !url.is_null())
}

/// The running daemon's state, for services (the local AI API server) that
/// have no `AppState` handle but must honour the network switch.
static NETWORK_STATE: std::sync::OnceLock<Arc<AppState>> = std::sync::OnceLock::new();

/// True while external connections are OFF/switching. False when no daemon
/// state is registered (unit tests), so nothing is gated there.
pub fn external_connections_blocked() -> bool {
    NETWORK_STATE
        .get()
        .is_some_and(|state| !state.network.permitted())
}

/// Who is issuing a command. A dashboard session from this machine (loopback
/// peer) is as trusted as the CLI; the restriction exists for a dashboard
/// exposed on the LAN (`--host 0.0.0.0`), where a remote browser must not be
/// able to run processes, touch arbitrary local paths, or rewire trust.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Caller {
    /// CLI over the token-gated loopback IPC socket.
    Ipc,
    /// Dashboard `POST /api/call`; `remote` is true when the TCP peer is not
    /// a loopback address (see [`is_local_peer`]).
    Http { remote: bool },
    /// Daemon-internal (tray, the dashboard's own upload/download handlers).
    Internal,
}

impl Caller {
    /// The caller for a dashboard HTTP request from `peer`.
    pub fn http_from(peer: std::net::IpAddr) -> Self {
        Caller::Http {
            remote: !is_local_peer(peer),
        }
    }
}

/// Loopback check that also treats IPv4-mapped IPv6 (`::ffff:127.0.0.1`,
/// what a dual-stack listener reports for a local IPv4 client) as local.
pub fn is_local_peer(ip: std::net::IpAddr) -> bool {
    ip.to_canonical().is_loopback()
}

/// `config.set` paths a remote dashboard may not change: they grant code
/// execution, or redirect where the daemon fetches/pushes/stores data or
/// which peers it trusts. (`set_by_path` only accepts `section.field`, so
/// there is no whole-section form to worry about.)
fn remote_config_denied(path: &str) -> bool {
    const DENIED_EXACT: &[&str] = &[
        // stdio sessions run `stdio_command` for remote peers.
        "tunnel.stdio_enabled",
        "tunnel.stdio_command",
        // Auto-approve peers / forwards.
        "tunnel.auto_accept",
        "tunnel.allow_peers",
        // Decide which peers are trusted as providers / relay members.
        "ai.trusted_providers",
        "network.membership_allowlist",
        // Local paths.
        "storage.blocks_dir",
        "storage.export_dir",
        // Redirect where credentials or media are sent.
        "ai.providers",
        "ai.upstream_url",
        "ai.upstream_api_key",
        "ai.api_listen",
        "stream.rtsp_url",
    ];
    // Whole sections: update source/cadence, dashboard bind address, and the
    // scheduler (its jobs are shell commands).
    const DENIED_SECTIONS: &[&str] = &["update.", "ui.", "scheduler."];
    // Catch-all for future path / listen-address fields.
    let field = path.rsplit('.').next().unwrap_or(path);
    let pathlike = field.ends_with("_dir")
        || field.ends_with("_path")
        || field.ends_with("_listen")
        || field == "listen";
    pathlike || DENIED_EXACT.contains(&path) || DENIED_SECTIONS.iter().any(|p| path.starts_with(p))
}

/// Rejects commands (or argument shapes) a remote HTTP caller may not use.
/// Only `Caller::Http { remote: true }` is restricted; the message names the
/// CLI / a local browser as the way out.
fn authorize(caller: Caller, cmd: &str, args: &Value) -> Result<()> {
    if caller != (Caller::Http { remote: true }) {
        return Ok(());
    }
    // A key counts as "present" unless absent or JSON null (the UI sends
    // `output` only when the user typed one).
    let has = |key: &str| args.get(key).is_some_and(|v| !v.is_null());
    let denied = match cmd {
        // Scheduler jobs are shell command lines.
        "sched.add" => Some("defines a shell command"),
        "sched.set" if has("command") => Some("defines a shell command"),
        // Reads an arbitrary local file into the store / sandbox.
        "store.put" | "store.put-file" if has("path") => Some("reads an arbitrary local path"),
        "store.sandbox.import" => Some("reads an arbitrary local path"),
        "store.folder-share" => Some("publishes an arbitrary local directory"),
        // Writes to a caller-chosen local path.
        "store.get" | "store.get-file" | "store.fetch-share" | "store.sandbox.export"
            if has("output") =>
        {
            Some("writes to an arbitrary local path")
        }
        // Directory listing of the daemon host's filesystem.
        "store.browse-dirs" if has("path") => Some("lists an arbitrary local directory"),
        "store.folder-sync" if has("dir") => Some("writes to an arbitrary local directory"),
        "config.set"
            if args
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(remote_config_denied) =>
        {
            Some("changes a setting that grants code execution or redirects trust or paths")
        }
        _ => None,
    };
    match denied {
        Some(why) => bail!(
            "`{cmd}` is not available from a remote dashboard session ({why}); \
             use the mistl CLI or open the dashboard on this machine"
        ),
        None => Ok(()),
    }
}

/// Route a request from daemon-internal code (tray, the dashboard's own
/// upload/download handlers); unrestricted. IPC and HTTP use [`dispatch_as`].
pub async fn dispatch(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    dispatch_as(Caller::Internal, cmd, args, state).await
}

/// Route a request, enforcing the per-caller restrictions of [`authorize`].
pub async fn dispatch_as(
    caller: Caller,
    cmd: &str,
    args: Value,
    state: &Arc<AppState>,
) -> Result<Value> {
    authorize(caller, cmd, &args)?;
    // An explicit `update.apply` skips the default-repo check inside the
    // updater, so a remote session must not be able to apply from a
    // non-default `update.repo`.
    if caller == (Caller::Http { remote: true }) && cmd == "update.apply" {
        let repo = state.config().update.repo;
        let repo = repo.trim();
        if !repo.is_empty() && !repo.eq_ignore_ascii_case(crate::update::REPO_DEFAULT) {
            bail!(
                "`update.apply` is not available from a remote dashboard session (update.repo is not the default); use the mistl CLI"
            );
        }
    }
    if !state.network.permitted() && !crate::network::offline_command_allowed(cmd) {
        state.network.require_online()?;
    }
    match cmd {
        "network.status" => Ok(state.network.status()),
        "network.set" => {
            let enabled = args
                .get("enabled")
                .and_then(Value::as_bool)
                .context("network.set requires enabled: boolean")?;
            match state.network.set(enabled) {
                Ok(true) => {
                    let state = state.clone();
                    // Let IPC/HTTP deliver the pending state before stopping this process.
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        state.request_restart();
                    });
                }
                Ok(false) => {}
                Err(error) => {
                    if !enabled {
                        let state = state.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(300)).await;
                            // Do not restart from a previous persisted ON value.
                            state.request_shutdown();
                        });
                    }
                    return Err(error);
                }
            }
            Ok(state.network.status())
        }
        "daemon.status" => Ok(json!({
            "build": crate::runtime::build_info(),
            "network": state.network.status(),
            "pid": std::process::id(),
            "uptime_secs": state.started_at.elapsed().as_secs(),
            "version": env!("CARGO_PKG_VERSION"),
            // `null` when the UI is disabled or failed to bind.
            "dashboard": state.dashboard_url(),
        })),
        "daemon.stop" => {
            state.request_shutdown();
            Ok(json!({ "stopping": true }))
        }
        "daemon.restart" => {
            state.request_restart();
            Ok(json!({ "restarting": true }))
        }
        "logs.tail" => {
            let since = args.get("since").and_then(Value::as_u64).unwrap_or(0);
            Ok(json!({ "entries": crate::devlog::tail(since) }))
        }
        "logs.clear" => {
            crate::devlog::clear();
            Ok(json!({ "cleared": true }))
        }
        "config.show" => {
            let mut value = serde_json::to_value(state.config())?;
            // Never hand secrets to clients. Mask the per-provider
            // api_key -- an empty string is left as-is so the dashboard can
            // show "not set" vs. "set", and set_by_path substitutes the
            // real value back in when a masked array round-trips.
            if let Some(providers) = value["ai"]["providers"].as_array_mut() {
                for provider in providers {
                    let is_set = matches!(provider["api_key"].as_str(), Some(k) if !k.is_empty());
                    if is_set {
                        provider["api_key"] = json!("***");
                    }
                }
            }
            // Webhook sink header values and URL credentials/query strings
            // are secrets too; set_by_path restores them on a round trip.
            config::mask_bot_webhook_secrets(&mut value);
            Ok(value)
        }
        "config.set" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("config.set needs a string `path` (e.g. \"ai.default_ref\")")?;
            let value = args.get("value").cloned().unwrap_or(Value::Null);
            let previous_ai = state.config().ai;
            state.set_config_path(path, value)?;
            if path == "ai.providers" {
                crate::ai::spawn_http_refresh(state.clone(), Some(&previous_ai));
            }
            if path == "network.membership_allowlist" {
                state.config().network.apply();
            }
            if path == "ai.trusted_providers" {
                crate::ai::apply_trusted_providers(state);
            }
            // Finish applying room membership and advertisements before
            // replying so disabled providers cannot accept another request.
            if state.network.permitted()
                && matches!(
                    path,
                    "ai.providers" | "ai.default_ref" | "ai.tts" | "ai.stt" | "bot.pipelines"
                )
            {
                crate::ai::reload_provider_if_running(state).await?;
            }
            Ok(json!({ "saved": true, "applies": config::applies_when(path) }))
        }
        _ => match cmd.split_once('.').map(|(ns, _)| ns) {
            Some("profile") | Some("key") => crate::identity::handle(cmd, args, state).await,
            Some("store") => crate::storage::handle(cmd, args, state).await,
            Some("stream") => crate::stream::handle(cmd, args, state).await,
            Some("chat") => crate::chat_relay::handle(cmd, args, state).await,
            Some("dm") => crate::dm::handle(cmd, args, state).await,
            Some("peers") => crate::net::peer_profile::handle(cmd, args, state).await,
            Some("sched") => crate::scheduler::handle(cmd, args, state).await,
            Some("bot") => crate::bot::handle(cmd, args, state).await,
            Some("consensus") => crate::consensus::handle(cmd, args, state).await,
            Some("topology") => crate::topology::handle(cmd, args, state).await,
            Some("ai") => crate::ai::handle(cmd, args, state).await,
            Some("tunnel") => crate::tunnel::handle(cmd, args, state).await,
            Some("update") => crate::update::handle(cmd, args, state).await,
            Some("install") | Some("autostart") => crate::install::handle(cmd, args, state).await,
            _ => bail!("unknown command: {cmd}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn config_show_masks_provider_keys_and_keeps_current_ai_shape() {
        let state = AppState::for_test();
        let mut config = Config::default();
        config.ai.providers.push(config::AiProviderConfig {
            id: "http".into(),
            base_url: "http://local/v1".into(),
            api_key: "secret".into(),
            ..Default::default()
        });
        config.ai.default_ref = Some(config::ModelRef {
            provider_id: "http".into(),
            model: "raw".into(),
        });
        state.set_config(config);
        let shown = dispatch("config.show", json!({}), &state).await.unwrap();
        assert_eq!(
            shown["ai"],
            json!({ "default_ref": { "provider_id": "http", "model": "raw" }, "tts": null, "stt": null,
            "providers": [{ "id": "http", "label": "", "base_url": "http://local/v1", "api_key": "***",
                "enabled": true, "models": [], "models_fetched_at": null, "provide": false, "shared": [] }],
            "api_listen": "127.0.0.1:6478", "request_timeout_secs": 120, "trusted_providers": [] })
        );
    }

    const REMOTE: Caller = Caller::Http { remote: true };
    const LOCAL: Caller = Caller::Http { remote: false };
    const MSG: &str = "not available from a remote dashboard session";

    #[test]
    fn remote_http_denies_dangerous_config_keys() {
        for path in [
            "tunnel.stdio_enabled",
            "tunnel.stdio_command",
            "tunnel.auto_accept",
            "tunnel.allow_peers",
            "update.repo",
            "update.auto_check",
            "storage.blocks_dir",
            "storage.export_dir",
            "scheduler.enabled",
            "ui.listen",
            "ai.providers",
            "ai.upstream_url",
            "stream.rtsp_url",
            "foo.some_dir",
            "foo.bind_listen",
        ] {
            let args = json!({"path": path, "value": null});
            let err = authorize(REMOTE, "config.set", &args)
                .unwrap_err()
                .to_string();
            assert!(err.contains(MSG), "{path}: {err}");
        }
        for path in [
            "ai.default_ref",
            "bot.pipelines",
            "tunnel.room_id",
            "identity.display_name",
        ] {
            authorize(REMOTE, "config.set", &json!({"path": path, "value": 1}))
                .unwrap_or_else(|e| panic!("{path}: {e}"));
        }
    }

    #[test]
    fn whole_section_config_set_is_rejected_by_set_by_path() {
        // No `section.field` dot -> invalid, so a whole-section set cannot
        // smuggle stdio keys past the per-key denylist.
        let cfg = Config::default();
        assert!(config::set_by_path(&cfg, "tunnel", json!({"stdio_enabled": true})).is_err());
    }

    #[test]
    fn peer_locality() {
        let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
        for local in ["127.0.0.1", "127.9.9.9", "::1", "::ffff:127.0.0.1"] {
            assert!(is_local_peer(ip(local)), "{local}");
            assert_eq!(Caller::http_from(ip(local)), LOCAL);
        }
        for remote in ["192.168.1.5", "10.0.0.2", "::ffff:192.168.1.5", "fe80::1"] {
            assert!(!is_local_peer(ip(remote)), "{remote}");
            assert_eq!(Caller::http_from(ip(remote)), REMOTE);
        }
    }

    #[test]
    fn remote_http_denies_command_and_path_shapes() {
        let denied = [
            (
                "sched.add",
                json!({"name":"x","schedule":"@daily","command":"id"}),
            ),
            ("sched.set", json!({"id":"a","command":"id"})),
            ("store.put", json!({"path":"/etc/passwd"})),
            ("store.put-file", json!({"path":"/x","passphrase":"p"})),
            ("store.get", json!({"id":"c","output":"/tmp/x"})),
            (
                "store.get-file",
                json!({"cid":"c","passphrase":"p","output":"/x"}),
            ),
            ("store.fetch-share", json!({"url":"u","output":"/x"})),
            ("store.sandbox.export", json!({"path":"a","output":"/x"})),
            ("store.sandbox.import", json!({"path":"/x"})),
            ("store.folder-share", json!({"path":"/","passphrase":"p"})),
            ("store.folder-sync", json!({"url":"u","dir":"/x"})),
            ("store.browse-dirs", json!({"path":"/"})),
        ];
        for (cmd, args) in denied {
            let err = authorize(REMOTE, cmd, &args).unwrap_err().to_string();
            assert!(err.contains(MSG), "{cmd}: {err}");
        }
    }

    #[test]
    fn remote_http_allows_dashboard_shapes() {
        let allowed = [
            ("sched.set", json!({"id":"a","enabled":false})),
            ("sched.run", json!({"id":"a"})),
            ("store.get", json!({"id":"c"})),
            ("store.get", json!({"id":"c","output":null})),
            ("store.put-file", json!({"sandbox":"a","passphrase":"p"})),
            (
                "store.get-file",
                json!({"cid":"c","passphrase":"p","to_sandbox":true}),
            ),
            ("store.sandbox.export", json!({"path":"a"})),
            ("store.folder-sync", json!({"url":"u"})),
            ("config.show", json!({})),
        ];
        for (cmd, args) in allowed {
            authorize(REMOTE, cmd, &args).unwrap_or_else(|e| panic!("{cmd}: {e}"));
        }
    }

    #[test]
    fn loopback_http_ipc_and_internal_are_unrestricted() {
        for caller in [LOCAL, Caller::Ipc, Caller::Internal] {
            let stdio = json!({"path":"tunnel.stdio_command","value":["sh"]});
            authorize(caller, "config.set", &stdio).unwrap();
            authorize(caller, "sched.add", &json!({"command":"id"})).unwrap();
            authorize(caller, "store.put", &json!({"path":"/x"})).unwrap();
            authorize(caller, "store.get", &json!({"id":"c","output":"/x"})).unwrap();
        }
    }

    #[tokio::test]
    async fn dispatch_as_remote_http_rejects_before_running() {
        let state = AppState::for_test();
        let err = dispatch_as(
            REMOTE,
            "sched.add",
            json!({"name":"x","schedule":"@daily","command":"id"}),
            &state,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains(MSG), "{err}");
    }
}
