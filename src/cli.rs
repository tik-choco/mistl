use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use crate::daemon;

#[derive(Parser)]
#[command(name = "mistl", version, about = "Unified P2P daemon: identity, storage, RTSP screen share, offline mailbox")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
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
}

#[derive(Subcommand)]
pub enum DaemonAction {
    /// Run the daemon in the foreground
    Run,
    /// Start the daemon in the background
    Start,
    /// Stop the running daemon
    Stop,
    /// Show daemon status
    Status,
}

#[derive(Subcommand)]
pub enum ProfileAction {
    /// Show the current profile
    Show,
    /// Set a profile field
    Set {
        /// Field name (e.g. display_name, bio, avatar)
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
    match cli.command {
        Command::Daemon { action } => match action {
            DaemonAction::Run => daemon::run_foreground(),
            DaemonAction::Start => daemon::start_background(),
            DaemonAction::Stop => client_call("daemon.stop", json!({})),
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
                let response = daemon::ipc::client_request("stream.relay.start", json!({ "room": room }))?;
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
    }
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
    Ok(())
}

/// Send one request to the daemon and pretty-print the JSON response.
fn client_call(cmd: &str, args: Value) -> Result<()> {
    let response = daemon::ipc::client_request(cmd, args)?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}
