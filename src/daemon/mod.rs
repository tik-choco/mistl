pub mod ipc;

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::sync::watch;
use tracing::info;

use crate::config::{self, Config};

/// Shared state for all daemon services.
pub struct AppState {
    /// Live configuration. Behind a lock so `config.set` can hot-reload it:
    /// services read it when they (re)start, so most changes apply on the
    /// next `*.start` without a daemon restart.
    config: std::sync::RwLock<Config>,
    pub started_at: Instant,
    shutdown: watch::Sender<bool>,
    /// Set by [`AppState::request_restart`]: after the daemon shuts down it
    /// relaunches itself from the (freshly self-updated) executable.
    restart: AtomicBool,
}

impl AppState {
    /// Snapshot of the current config (cheap; Config is small and cloned).
    pub fn config(&self) -> Config {
        self.config.read().expect("config lock poisoned").clone()
    }

    pub fn set_config(&self, config: Config) {
        *self.config.write().expect("config lock poisoned") = config;
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
}

/// Run the daemon in the foreground until Ctrl-C or a `daemon.stop` request.
/// `host_override`, if set, replaces just the host part of the configured
/// `ui.listen` for this run (e.g. `--host 0.0.0.0` to reach the dashboard
/// from another device on the LAN) without touching the persisted config.
pub fn run_foreground(host_override: Option<String>) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(daemon_main(host_override))
}

async fn daemon_main(host_override: Option<String>) -> Result<()> {
    let config = Config::load()?;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let state = Arc::new(AppState {
        config: std::sync::RwLock::new(config),
        started_at: Instant::now(),
        shutdown: shutdown_tx,
        restart: AtomicBool::new(false),
    });

    let server = ipc::serve(state.clone()).await?;
    info!(port = server.port(), "mistl daemon ready");

    let ui_config = state.config().ui;
    let listen = match &host_override {
        Some(host) => override_listen_host(&ui_config.listen, host),
        None => ui_config.listen.clone(),
    };
    let web = if ui_config.enabled {
        match crate::web::serve(state.clone(), &listen).await {
            Ok(web) => {
                info!(url = %web.url(), "web dashboard ready");
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

    // Background self-update: periodically check GitHub Releases and, if
    // enabled, stage a newer binary (applied on the next daemon start).
    crate::update::spawn_auto_update(state.clone());

    // tc-chat room relay/bot: starts only if `[mailbox] chat_relay` and
    // `chat_rooms` are configured (a no-op otherwise). Spawned eagerly here
    // -- rather than lazily on first `mailbox.chat.*` IPC call, like
    // mailbox/ai/stream's own services -- because the whole point is
    // receiving tc-chat traffic while nobody is asking.
    crate::mailbox::chat_relay::spawn_background(state.clone());

    // tc-storage folder-share sync (requester side) and owner
    // responder/announcer: both no-ops unless syncs/shares are persisted.
    crate::storage::folder_sync::spawn_background(state.clone());
    crate::storage::folder_owner::spawn_background(state.clone());

    // Cron-like job scheduler: fires due jobs on a 1-second tick. A no-op
    // (logged) when `[scheduler] enabled = false`.
    crate::scheduler::spawn_background(state.clone());

    // Bot pipeline engine: source -> transform(s) -> sink(s) automation
    // runs, fired on a 1-second tick. A no-op (logged) when
    // `[bot] enabled = false`.
    crate::bot::spawn_background(state.clone());

    // AI network `provide`: if `ai provide start` (CLI or the dashboard
    // toggle) was left enabled on a previous run, resume it automatically
    // instead of coming back up silently not providing (see
    // `crate::ai::spawn_provide_autoresume`'s doc comment -- this is the fix
    // for a real "rebuild -> restart -> providing was off and nobody
    // noticed" confusion). A no-op (quiet debug log) when it was never
    // enabled, and never fails daemon startup on its own.
    crate::ai::spawn_provide_autoresume(state.clone());

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("interrupted, shutting down"),
        _ = shutdown_rx.wait_for(|&stop| stop) => info!("stop requested, shutting down"),
    }

    if let Some(web) = web {
        web.close().await;
    }
    server.close().await;

    // A self-update applied with `--restart`, or a `daemon.restart` IPC call,
    // asks us to come back up. The sockets are now released, so relaunch is
    // safe. Forward the same `--host` override this run used -- otherwise a
    // daemon started with `--host 0.0.0.0` for LAN access would come back
    // bound to the configured (typically loopback-only) `ui.listen` host,
    // silently dropping external devices with connection-refused.
    if state.wants_restart() {
        match spawn_detached_daemon(host_override.as_deref()) {
            Ok(()) => info!("relaunched daemon on the updated binary"),
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
    let port = listen.rsplit_once(':').map(|(_, port)| port).unwrap_or("6480");
    format!("{host}:{port}")
}

/// Relaunch the daemon from the current executable path, detached. The path
/// is unchanged by a self-update (the bytes at it are swapped in place), so
/// this starts the freshly-updated binary. Called from [`daemon_main`] once
/// the IPC/web sockets have been released. `host`, if the daemon that's
/// restarting was itself started with `--host`, is re-forwarded so the
/// relaunched process keeps the same dashboard bind address.
fn spawn_detached_daemon(host: Option<&str>) -> Result<()> {
    let exe = std::env::current_exe().context("resolving current executable")?;
    let log_path = config::data_dir()?.join("daemon.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("opening {}", log_path.display()))?;

    let mut command = std::process::Command::new(exe);
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

    command.spawn().context("relaunching daemon process")?;
    Ok(())
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
    if let Ok(status) = ipc::client_request("daemon.status", json!({})) {
        if quiet {
            return Ok(());
        }
        bail!(
            "daemon already running: {}",
            serde_json::to_string(&status)?
        );
    }

    let exe = std::env::current_exe().context("resolving current executable")?;
    let log_path = config::data_dir()?.join("daemon.log");
    let log = std::fs::File::create(&log_path)
        .with_context(|| format!("creating {}", log_path.display()))?;

    let mut command = std::process::Command::new(exe);
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

    let child = command.spawn().context("spawning daemon process")?;

    // Poll until the daemon answers or we give up.
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if let Ok(status) = ipc::client_request("daemon.status", json!({})) {
            if !quiet {
                println!("{}", serde_json::to_string_pretty(&status)?);
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

/// Route an IPC request to the owning module.
pub async fn dispatch(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "daemon.status" => Ok(json!({
            "pid": std::process::id(),
            "uptime_secs": state.started_at.elapsed().as_secs(),
            "version": env!("CARGO_PKG_VERSION"),
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
            // Never hand secrets to clients; \"***\" marks \"set\" and is
            // rejected by config.set so it can't be written back.
            if value["ai"]["upstream_api_key"].is_string() {
                value["ai"]["upstream_api_key"] = json!("***");
            }
            // Same masking for the provider/preset shape's per-provider
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
            Ok(value)
        }
        "config.set" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .context("config.set needs a string `path` (e.g. \"ai.default_preset_id\")")?;
            let value = args.get("value").cloned().unwrap_or(Value::Null);
            let updated = config::set_by_path(&state.config(), path, value)?;
            updated.save()?;
            state.set_config(updated);
            // These paths feed the running AI provider (see
            // `ai::build_provider`); reloading it live -- instead of making
            // the user stop/start providing, let alone restart the daemon
            // -- so a preset/model edit is just... immediately true. Fired
            // fire-and-forget (it may hit the network re-fetching upstream
            // models) so this response isn't held up waiting on it.
            if matches!(
                path,
                "ai.providers"
                    | "ai.presets"
                    | "ai.default_preset_id"
                    | "ai.tts_preset_id"
                    | "ai.stt_preset_id"
                    | "ai.advertised_models"
            ) {
                let state = state.clone();
                tokio::spawn(async move { crate::ai::reload_provider_if_running(&state).await });
            }
            Ok(json!({ "saved": true, "applies": config::applies_when(path) }))
        }
        _ => match cmd.split_once('.').map(|(ns, _)| ns) {
            Some("profile") | Some("key") => crate::identity::handle(cmd, args, state).await,
            Some("store") => crate::storage::handle(cmd, args, state).await,
            Some("stream") => crate::stream::handle(cmd, args, state).await,
            Some("mailbox") => crate::mailbox::handle(cmd, args, state).await,
            Some("sched") => crate::scheduler::handle(cmd, args, state).await,
            Some("bot") => crate::bot::handle(cmd, args, state).await,
            Some("consensus") => crate::consensus::handle(cmd, args, state).await,
            Some("topology") => crate::topology::handle(cmd, args, state).await,
            Some("ai") => crate::ai::handle(cmd, args, state).await,
            Some("update") => crate::update::handle(cmd, args, state).await,
            Some("install") | Some("autostart") => crate::install::handle(cmd, args, state).await,
            _ => bail!("unknown command: {cmd}"),
        },
    }
}
