pub mod ipc;

use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::sync::watch;
use tracing::info;

use crate::config::{self, Config};

/// Shared state for all daemon services.
pub struct AppState {
    pub config: Config,
    pub started_at: Instant,
    shutdown: watch::Sender<bool>,
}

impl AppState {
    pub fn request_shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

/// Run the daemon in the foreground until Ctrl-C or a `daemon.stop` request.
pub fn run_foreground() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(daemon_main())
}

async fn daemon_main() -> Result<()> {
    let config = Config::load()?;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let state = Arc::new(AppState {
        config,
        started_at: Instant::now(),
        shutdown: shutdown_tx,
    });

    let server = ipc::serve(state.clone()).await?;
    info!(port = server.port(), "mistl daemon ready");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("interrupted, shutting down"),
        _ = shutdown_rx.wait_for(|&stop| stop) => info!("stop requested, shutting down"),
    }

    server.close().await;
    Ok(())
}

/// Spawn `mistl daemon run` as a detached background process and wait for it
/// to become reachable.
pub fn start_background() -> Result<()> {
    if let Ok(status) = ipc::client_request("daemon.status", json!({})) {
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
    command
        .args(["daemon", "run"])
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
            println!("{}", serde_json::to_string_pretty(&status)?);
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
        _ => match cmd.split_once('.').map(|(ns, _)| ns) {
            Some("profile") | Some("key") => crate::identity::handle(cmd, args, state).await,
            Some("store") => crate::storage::handle(cmd, args, state).await,
            Some("stream") => crate::stream::handle(cmd, args, state).await,
            Some("mailbox") => crate::mailbox::handle(cmd, args, state).await,
            Some("ai") => crate::ai::handle(cmd, args, state).await,
            _ => bail!("unknown command: {cmd}"),
        },
    }
}
