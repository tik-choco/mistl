use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use crate::daemon;

#[derive(Parser)]
#[command(
    name = "mistl",
    version,
    about = "MISTL - unified P2P daemon: identity, storage, VRChat screen share, offline mailbox, AI network",
    after_help = "Running `mistl` with no arguments opens the web dashboard \
                  (starts the daemon if needed) -- double-clicking mistl.exe does the same."
)]
pub struct Cli {
    /// Omitted -> open the web dashboard.
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Manage the background daemon
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
    /// User profile management
    Profile {
        #[command(subcommand)]
        action: ProfileAction,
    },
    /// DID / key management
    Key {
        #[command(subcommand)]
        action: KeyAction,
    },
    /// Content storage
    Store {
        #[command(subcommand)]
        action: StoreAction,
    },
    /// RTSP screen sharing (VRChat video player compatible)
    Stream {
        #[command(subcommand)]
        action: StreamAction,
    },
    /// P2P mailbox: store-and-forward messages/data for offline peers
    Mailbox {
        #[command(subcommand)]
        action: MailboxAction,
    },
    /// P2P AI network: consume or provide LLM inference (mistai compatible)
    Ai {
        #[command(subcommand)]
        action: AiAction,
    },
    /// Open the web dashboard in the default browser (starts the daemon if needed)
    Ui,
    /// Show a combined status overview (daemon, stream, AI)
    Status,
    /// Show or change configuration (applies without editing config.toml)
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Check for and install updates from GitHub Releases
    Update {
        #[command(subcommand)]
        action: Option<UpdateAction>,
    },
    /// Install mistl into a fixed per-user location (%LOCALAPPDATA%\Programs\mistl)
    Install {
        /// Do not enable start-on-login
        #[arg(long)]
        no_autostart: bool,
    },
    /// Remove the per-user install, Start Menu shortcut, and autostart entry
    Uninstall,
    /// Manage start-on-login (launches the daemon headless at login)
    Autostart {
        #[command(subcommand)]
        action: AutostartAction,
    },
}

#[derive(Subcommand)]
pub enum UpdateAction {
    /// Check GitHub for a newer release (this is the default)
    Check,
    /// Download, verify (sha256), and install the latest release
    Apply {
        /// Restart the daemon now so the update takes effect immediately
        /// (otherwise it applies on the next daemon start)
        #[arg(long)]
        restart: bool,
    },
    /// Show updater status (current version, target, auto-update settings)
    Status,
}

#[derive(Subcommand)]
pub enum AutostartAction {
    /// Start the mistl daemon automatically at login
    Enable,
    /// Stop starting the mistl daemon at login
    Disable,
    /// Show whether login autostart is enabled
    Status,
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Show the current configuration (secrets masked)
    Show,
    /// Set one field, e.g. `mistl config set ai.upstream_url http://127.0.0.1:11434/v1`
    Set {
        /// Field path as section.field (see `mistl config show`)
        path: String,
        /// New value; JSON accepted (numbers, true/false, ["a","b"]), empty string clears
        value: String,
    },
}

#[derive(Subcommand)]
pub enum DaemonAction {
    /// Run the daemon in the foreground
    Run,
    /// Start the daemon in the background
    Start,
    /// Stop the running daemon
    Stop,
    /// Restart the running daemon (e.g. after a settings change that needs one)
    Restart,
    /// Show daemon status
    Status,
}

#[derive(Subcommand)]
pub enum ProfileAction {
    /// Show the current profile
    Show,
    /// Set a profile field
    Set {
        /// Field name (e.g. display_name, bio, avatar_cid)
        field: String,
        /// New value
        value: String,
    },
}

#[derive(Subcommand)]
pub enum KeyAction {
    /// Generate a new DID keypair
    Generate,
    /// List keys / DIDs
    List,
    /// Show the primary DID
    Did,
}

#[derive(Subcommand)]
pub enum StoreAction {
    /// Store a file, returns its content id
    Put { path: String },
    /// Retrieve content by id
    Get {
        id: String,
        /// Output file path (stdout if omitted)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// List stored content
    Ls,
}

#[derive(Subcommand)]
pub enum StreamAction {
    /// Start the RTSP screen-share server (local screen capture)
    Start,
    /// Relay a tc-chat screen share to VRChat (p2p -> RTSP, video + audio)
    Relay {
        /// tc-chat room id of the share (default: stream.relay_room)
        #[arg(short, long)]
        room: Option<String>,
    },
    /// Serve a synthetic moving test pattern + tone over RTSP, to verify the
    /// VRChat/AVPro playback path locally (ffprobe/ffplay) without the p2p leg
    Selftest {
        /// Audio track: "aac" (default, what VRChat plays), "opus", or "none"
        #[arg(short, long)]
        audio: Option<String>,
        /// Frame width in pixels (default 640)
        #[arg(long)]
        width: Option<u32>,
        /// Frame height in pixels (default 360)
        #[arg(long)]
        height: Option<u32>,
        /// Frames per second (default 30)
        #[arg(long)]
        fps: Option<u32>,
        /// Stop automatically after N seconds (default: run until `stream stop`)
        #[arg(short, long)]
        seconds: Option<u64>,
    },
    /// Stop the RTSP server
    Stop,
    /// Show stream status and URL
    Status,
}

#[derive(Subcommand)]
pub enum MailboxAction {
    /// Deposit data for a (possibly offline) recipient
    Send {
        /// Recipient DID or peer id
        to: String,
        /// File to send (stdin if omitted)
        #[arg(short, long)]
        file: Option<String>,
        /// Inline text message
        #[arg(short, long)]
        message: Option<String>,
    },
    /// List messages held for me / by me
    Ls,
    /// Fetch pending messages
    Fetch,
}

#[derive(Subcommand)]
pub enum AiAction {
    /// One-shot chat completion (local provider or first p2p provider)
    Chat {
        /// The user prompt
        prompt: String,
        /// Model to request (default: provider's choice)
        #[arg(short, long)]
        model: Option<String>,
    },
    /// Show AI network status (room, provider, API server)
    Status,
    /// List models advertised by the reachable provider
    Models,
    /// Provide inference to the network from the configured upstream
    Provide {
        #[command(subcommand)]
        action: AiToggleAction,
    },
    /// Local OpenAI-compatible API server backed by the network
    Serve {
        #[command(subcommand)]
        action: AiToggleAction,
    },
}

#[derive(Subcommand)]
pub enum AiToggleAction {
    /// Start the service
    Start,
    /// Stop the service
    Stop,
}

/// Dispatch a parsed CLI invocation: either run the daemon, or act as a
/// client sending one request to the running daemon over local IPC.
pub fn dispatch(cli: Cli) -> Result<()> {
    let Some(command) = cli.command else {
        // Bare `mistl` (including an Explorer double-click on the exe):
        // bring the whole thing up and land the user in the dashboard.
        return open_dashboard();
    };
    match command {
        Command::Daemon { action } => match action {
            DaemonAction::Run => daemon::run_foreground(),
            DaemonAction::Start => daemon::start_background(),
            DaemonAction::Stop => client_call("daemon.stop", json!({})),
            DaemonAction::Restart => client_call("daemon.restart", json!({})),
            DaemonAction::Status => client_call("daemon.status", json!({})),
        },
        Command::Profile { action } => match action {
            ProfileAction::Show => client_call("profile.show", json!({})),
            ProfileAction::Set { field, value } => {
                client_call("profile.set", json!({ "field": field, "value": value }))
            }
        },
        Command::Key { action } => match action {
            KeyAction::Generate => client_call("key.generate", json!({})),
            KeyAction::List => client_call("key.list", json!({})),
            KeyAction::Did => client_call("key.did", json!({})),
        },
        Command::Store { action } => match action {
            StoreAction::Put { path } => {
                let abs = std::fs::canonicalize(&path)
                    .with_context(|| format!("file not found: {path}"))?;
                client_call("store.put", json!({ "path": abs.to_string_lossy() }))
            }
            StoreAction::Get { id, output } => {
                client_call("store.get", json!({ "id": id, "output": output }))
            }
            StoreAction::Ls => client_call("store.ls", json!({})),
        },
        Command::Stream { action } => match action {
            StreamAction::Start => client_call("stream.start", json!({})),
            StreamAction::Relay { room } => {
                let response = request("stream.relay.start", json!({ "room": room }))?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                if let Some(url) = response.get("rtsp_url").and_then(Value::as_str) {
                    println!();
                    println!("  Paste this URL into the VRChat video player:");
                    println!();
                    println!("      {url}");
                    println!();
                }
                Ok(())
            }
            StreamAction::Selftest { audio, width, height, fps, seconds } => {
                let response = request(
                    "stream.selftest.start",
                    json!({ "audio": audio, "width": width, "height": height, "fps": fps, "seconds": seconds }),
                )?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                if let Some(url) = response.get("rtsp_url").and_then(Value::as_str) {
                    println!();
                    println!("  Synthetic feed is live. Verify the RTSP/AVPro path with:");
                    println!();
                    println!("      ffprobe -rtsp_transport tcp {url}");
                    println!("      ffplay  -rtsp_transport tcp {url}");
                    println!();
                    println!("  (open two ffplay windows to confirm multi-viewer fan-out)");
                    println!();
                }
                Ok(())
            }
            StreamAction::Stop => client_call("stream.stop", json!({})),
            StreamAction::Status => client_call("stream.status", json!({})),
        },
        Command::Mailbox { action } => match action {
            MailboxAction::Send { to, file, message } => {
                if file.is_none() && message.is_none() {
                    bail!("provide --file or --message");
                }
                client_call(
                    "mailbox.send",
                    json!({ "to": to, "file": file, "message": message }),
                )
            }
            MailboxAction::Ls => client_call("mailbox.ls", json!({})),
            MailboxAction::Fetch => client_call("mailbox.fetch", json!({})),
        },
        Command::Ai { action } => match action {
            AiAction::Chat { prompt, model } => {
                client_call("ai.chat", json!({ "prompt": prompt, "model": model }))
            }
            AiAction::Status => client_call("ai.status", json!({})),
            AiAction::Models => client_call("ai.models", json!({})),
            AiAction::Provide { action } => match action {
                AiToggleAction::Start => client_call("ai.provide.start", json!({})),
                AiToggleAction::Stop => client_call("ai.provide.stop", json!({})),
            },
            AiAction::Serve { action } => match action {
                AiToggleAction::Start => client_call("ai.serve.start", json!({})),
                AiToggleAction::Stop => client_call("ai.serve.stop", json!({})),
            },
        },
        Command::Ui => open_dashboard(),
        Command::Status => status_overview(),
        Command::Config { action } => match action {
            ConfigAction::Show => client_call("config.show", json!({})),
            ConfigAction::Set { path, value } => {
                // Accept JSON for typed values; fall back to a plain string
                // ("30" stays a number, "native" a string). An empty value
                // (also the JSON form `""`, since some shells can't pass a
                // truly empty argument) clears optional fields.
                let value = match serde_json::from_str(&value) {
                    _ if value.is_empty() => Value::Null,
                    Ok(Value::String(s)) if s.is_empty() => Value::Null,
                    Ok(parsed) => parsed,
                    Err(_) => Value::String(value),
                };
                let response = request("config.set", json!({ "path": path, "value": value }))?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                if let Some(applies) = response.get("applies").and_then(Value::as_str) {
                    if applies != "next service start" {
                        println!("note: this change takes effect after a {applies}");
                    }
                }
                Ok(())
            }
        },
        Command::Update { action } => match action.unwrap_or(UpdateAction::Check) {
            UpdateAction::Check => {
                let response = request("update.check", json!({}))?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                if response.get("update_available").and_then(Value::as_bool) == Some(true) {
                    let latest = response.get("latest").and_then(Value::as_str).unwrap_or("?");
                    println!();
                    if response.get("asset_available").and_then(Value::as_bool) == Some(false) {
                        let notes = response.get("notes_url").and_then(Value::as_str).unwrap_or("");
                        println!("  v{latest} is available but has no binary for this platform.");
                        println!("  Download it manually: {notes}");
                    } else {
                        println!("  Run `mistl update apply` to install v{latest}.");
                    }
                    println!();
                }
                Ok(())
            }
            UpdateAction::Apply { restart } => {
                let response = request("update.apply", json!({ "restart": restart }))?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                Ok(())
            }
            UpdateAction::Status => client_call("update.status", json!({})),
        },
        Command::Install { no_autostart } => install_cli(!no_autostart),
        Command::Uninstall => {
            crate::install::uninstall()?;
            println!("mistl uninstalled (per-user install, shortcut, and autostart removed)");
            Ok(())
        }
        Command::Autostart { action } => match action {
            AutostartAction::Enable => {
                crate::install::set_autostart(true)?;
                println!("autostart enabled: the mistl daemon will start at login");
                Ok(())
            }
            AutostartAction::Disable => {
                crate::install::set_autostart(false)?;
                println!("autostart disabled");
                Ok(())
            }
            AutostartAction::Status => {
                let on = crate::install::autostart_enabled();
                println!("autostart: {}", if on { "enabled" } else { "disabled" });
                Ok(())
            }
        },
    }
}

/// `mistl install`: copy this exe into the fixed per-user location, add a
/// Start Menu shortcut, and (unless opted out) enable login autostart. Runs
/// in the client process directly -- it must work from a freshly-downloaded
/// exe with no daemon running yet.
fn install_cli(enable_autostart: bool) -> Result<()> {
    let exe = crate::install::install(enable_autostart)?;
    println!("installed: {}", exe.display());
    if enable_autostart {
        println!("autostart: enabled (the mistl daemon starts at login)");
    }
    println!();
    println!("  Run `mistl` or use the Start Menu shortcut to open the dashboard.");
    println!("  You can delete the copy you just ran; the installed one is used from now on.");
    println!();
    Ok(())
}

/// One combined, human-scannable status snapshot.
fn status_overview() -> Result<()> {
    // The stream call goes first so its auto-start covers daemon.status
    // (which is exempt from auto-start by design).
    let stream = request("stream.status", json!({}))?;
    let overview = json!({
        "daemon": daemon::ipc::client_request("daemon.status", json!({}))?,
        "stream": stream,
        "ai": request("ai.status", json!({}))?,
    });
    println!("{}", serde_json::to_string_pretty(&overview)?);
    Ok(())
}

/// Ensure the daemon is up, then open the dashboard URL in the default
/// browser.
fn open_dashboard() -> Result<()> {
    let config = crate::config::Config::load()?;
    if !config.ui.enabled {
        bail!("the web dashboard is disabled ([ui] enabled = false in config.toml)");
    }
    if daemon::ipc::client_request("daemon.status", json!({})).is_err() {
        daemon::start_background()?;
    }
    let url = format!("http://{}/", config.ui.listen);

    #[cfg(windows)]
    let opened = std::process::Command::new("cmd")
        .args(["/c", "start", "", &url])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    #[cfg(target_os = "macos")]
    let opened = std::process::Command::new("open")
        .arg(&url)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    #[cfg(all(unix, not(target_os = "macos")))]
    let opened = std::process::Command::new("xdg-open")
        .arg(&url)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if opened {
        println!("dashboard: {url}");
    } else {
        println!("open {url} in your browser");
    }
    if !crate::install::is_installed() {
        println!("tip: run `mistl install` to install mistl for your user and start it at login");
    }
    Ok(())
}

/// Send one request to the daemon, transparently starting it first if it
/// isn't running (`daemon.*` commands are exempt so `daemon stop`/`status`
/// never boot a daemon just to talk to it).
fn request(cmd: &str, args: Value) -> Result<Value> {
    match daemon::ipc::client_request(cmd, args.clone()) {
        Err(error)
            if !cmd.starts_with("daemon.")
                && error.to_string().contains("daemon is not running") =>
        {
            eprintln!("mistl: starting daemon...");
            daemon::start_background_quiet()?;
            daemon::ipc::client_request(cmd, args)
        }
        other => other,
    }
}

/// Send one request to the daemon and pretty-print the JSON response.
fn client_call(cmd: &str, args: Value) -> Result<()> {
    let response = request(cmd, args)?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}
