use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use crate::daemon;
use crate::tunnel::forward_args;

#[derive(Parser)]
#[command(
    name = "mistl",
    version,
    about = "MISTL - unified P2P daemon: identity, storage, VRChat screen share, offline mailbox/relay, AI network",
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
        /// Print machine-readable JSON instead of tc-storage-cli-style
        /// human-readable text
        #[arg(long, global = true)]
        json: bool,
        #[command(subcommand)]
        action: StoreAction,
    },
    /// RTSP screen sharing (VRChat video player compatible)
    Stream {
        #[command(subcommand)]
        action: StreamAction,
    },
    /// P2P mail relay: store-and-forward messages/data for offline peers,
    /// plus (when configured) a standing tc-chat room relay/bot so chat
    /// messages keep arriving even without a tc-chat browser tab open.
    /// Formerly named `mailbox`; that name still works (`mistl mailbox ...`).
    #[command(visible_alias = "mailbox")]
    Relay {
        #[command(subcommand)]
        action: RelayAction,
    },
    /// P2P AI network: consume or provide LLM inference (mistai compatible)
    Ai {
        #[command(subcommand)]
        action: AiAction,
    },
    /// Job scheduler: run commands on a cron or interval schedule
    Sched {
        #[command(subcommand)]
        action: SchedAction,
    },
    /// Bot pipeline automation: source -> transform(s) -> sink(s) runs on a
    /// schedule (e.g. fetch tc-news global articles, summarize, synthesize
    /// speech, and post to tc-chat / a webhook). Pipelines are defined in
    /// `config.toml`'s `[bot]` section (`mistl config set bot.pipelines
    /// <json>`) -- there is no add/edit subcommand in v1.
    Bot {
        #[command(subcommand)]
        action: BotAction,
    },
    /// WebRTC P2P tunnel: TCP/UDP port forwarding and a stdio-command bridge
    /// to a peer over a mistlib room (ported from the standalone `p2p`
    /// tool). `serve`/`connect` add forwards directly (you already know you
    /// want them); `approve`/`deny` and `accept`/`reject` answer the two
    /// independent prompts a session can raise (an incoming connection
    /// through a forward you're serving, and a peer asking *you* to forward
    /// one of your own targets for them).
    Tunnel {
        #[command(subcommand)]
        action: TunnelAction,
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
    /// Set one field, e.g. `mistl config set ai.default_preset_id default`
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
    Run {
        /// Override the dashboard bind host for this run only, e.g. `0.0.0.0`
        /// to reach it from another device on the LAN (keeps the port from
        /// `ui.listen`). Not persisted -- omit to use the configured value.
        #[arg(long)]
        host: Option<String>,
    },
    /// Start the daemon in the background
    Start {
        /// Same as `daemon run --host`, forwarded to the background process.
        #[arg(long)]
        host: Option<String>,
    },
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
    /// Sign a root -> leaf DID delegation (this identity becomes `root`),
    /// printed as JSON -- the manual-transfer path ("経路B") from
    /// did-delegation.md: paste the output into a browser app that
    /// supports importing a delegation. For the no-copy-paste path, see
    /// `mistl key pair`.
    Delegate {
        /// The leaf DID to delegate to (a `did:key:z...`, typically a
        /// browser app's existing per-origin identity)
        #[arg(long)]
        leaf: String,
        /// Delegation lifetime: a number of days, or suffixed s/m/h/d
        /// (e.g. `90d`, `12h`). Range 1-365 days.
        #[arg(long, default_value = "60d")]
        ttl: String,
    },
    /// List delegations this identity has issued as root (most recent
    /// last), including expired ones
    Delegations,
    /// Pair with a browser app over a short-lived mistlib room ("経路A"
    /// from did-delegation.md): prints a one-time code to read aloud/type
    /// into the browser, then waits for it to be claimed and a delegation
    /// issued
    Pair {
        /// Delegation lifetime once a pairing request is accepted (same
        /// format as `key delegate --ttl`)
        #[arg(long, default_value = "60d")]
        ttl: String,
        /// How long to wait for the code to be claimed before giving up
        /// (suffixed s/m/h/d, default 5 minutes)
        #[arg(long, default_value = "5m")]
        timeout: String,
    },
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
    /// Store a file as an encrypted tc-storage file bundle (web-app
    /// compatible: AES-256-GCM + PBKDF2), returns its content id
    PutFile {
        /// File to encrypt and store
        path: String,
        /// Passphrase used to derive the encryption key
        passphrase: String,
    },
    /// Retrieve and decrypt an encrypted file bundle by content id
    GetFile {
        id: String,
        /// Passphrase the bundle was encrypted with
        passphrase: String,
        /// Output file path (defaults to the downloads dir)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Parse a `tc-share` link and print its fields (no network)
    ParseLink { url: String },
    /// Resolve and decrypt a `tc-share` *file* link from the local store
    FetchShare {
        /// The `tc-share=...` URL or token
        url: String,
        /// Output file path (defaults to the downloads dir)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Content sandbox: import external files, list contents
    Sandbox {
        #[command(subcommand)]
        action: StoreSandboxAction,
    },
    /// Join the storage p2p room(s) (if `storage.room_ids` is configured)
    /// and report connected peers (tc-storage-cli `connect` compatible);
    /// exits non-zero if no peers are connected
    Connect {
        /// Join only this room instead of all of `storage.room_ids`
        /// (additive: the store can hold several rooms at once)
        #[arg(long)]
        room: Option<String>,
    },
    /// Fetch a folder share over the network: request access from the
    /// owner, wait for their approval, and download the folder's files into
    /// the sandbox (tc-storage-cli `folder-get` compatible)
    FolderGet {
        /// The `tc-share=...` folder share URL or token
        url: String,
    },
    /// Import an external file into the sandbox (alias of `sandbox import`;
    /// tc-storage-cli `sandbox-import` compatible)
    SandboxImport { path: String },
    /// List the files currently in the sandbox (alias of `sandbox ls`;
    /// tc-storage-cli `sandbox-list` compatible)
    SandboxList,
    /// Sync a folder share continuously: registers immediately and starts
    /// fetching it into a local directory in the background (retried
    /// automatically if the owner is offline), then keeps that directory
    /// mirrored as the owner announces changes (survives daemon restarts;
    /// `folder-sync ls`/`stop` to manage)
    FolderSync {
        #[command(subcommand)]
        action: StoreFolderSyncAction,
    },
    /// Share a local directory as a tc-storage folder: publish it encrypted
    /// to the room and print a `#tc-share=` link others can sync from (the
    /// daemon keeps serving access grants and announcing changes;
    /// `folder-share ls`/`stop` to manage)
    FolderShare {
        #[command(subcommand)]
        action: StoreFolderShareAction,
    },
}

#[derive(Subcommand)]
pub enum StoreFolderSyncAction {
    /// Start syncing a folder-share link into a local directory
    Start {
        /// The `tc-share=...` folder share URL or token
        url: String,
        /// Local directory to mirror the folder into. Advanced/optional: by
        /// default (omitted) the sync materializes into a managed
        /// subdirectory of the content sandbox instead -- extract individual
        /// files out with `store sandbox export`
        #[arg(long)]
        dir: Option<String>,
    },
    /// List active folder syncs
    Ls,
    /// Stop (and forget) the sync for a folder id; synced files are kept
    Stop { folder_id: String },
}

#[derive(Subcommand)]
pub enum StoreFolderShareAction {
    /// Publish a local directory as a shared folder and print its link
    Start {
        /// Local directory to share
        path: String,
        /// Passphrase protecting the folder's encryption key
        #[arg(long)]
        passphrase: String,
        /// Folder display name (defaults to the directory name)
        #[arg(long)]
        name: Option<String>,
        /// Room to announce the share in (defaults to the first
        /// `storage.room_ids` entry)
        #[arg(long)]
        room: Option<String>,
    },
    /// List folders currently shared from this node
    Ls,
    /// Stop sharing a folder id
    Stop { folder_id: String },
}

#[derive(Subcommand)]
pub enum StoreSandboxAction {
    /// Import an external file into the sandbox
    Import { path: String },
    /// List the files currently in the sandbox
    Ls,
    /// Remove a file (sandbox-relative path) from the sandbox
    Rm { path: String },
    /// Copy a sandbox file out to a real path (defaults to `storage.export_dir`
    /// if configured -- see `mistl config set storage.export_dir <dir>` --
    /// else the downloads dir)
    Export {
        /// Sandbox-relative path of the file to export
        path: String,
        /// Output file path (defaults to the downloads dir)
        #[arg(short, long)]
        output: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum StreamAction {
    /// Start the RTSP screen-share server (local screen capture). Stays
    /// purely local unless --room is given, in which case it also publishes
    /// into that mistlib room -- the same pipeline `stream share` (below)
    /// uses, just with the room optional. Unlike `stream share`, no
    /// stream.room config default applies here -- omit --room entirely to
    /// stay local-only regardless of what's configured.
    Start {
        /// mistlib room id to also publish into; omit to stay local-only.
        /// Native capture only (Windows).
        #[arg(short, long)]
        room: Option<String>,
    },
    /// Relay a tc-chat screen share to VRChat (p2p -> RTSP, video + audio)
    Relay {
        /// tc-chat room id of the share (default: stream.room)
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
    /// Publish this machine's own screen capture into a mistlib room (p2p),
    /// so any consensus-elected relay in that room -- or a direct viewer --
    /// picks it up like a tc-chat share. Also feeds the local RTSP server
    /// directly, so this node's own VRChat output works even alone in the
    /// room. Windows only; audio is not shared yet. Equivalent to `stream
    /// start --room <id>`, except it errors out instead of falling back to
    /// a local-only start when no room is given (explicitly or via
    /// stream.room) -- kept as a separate command for that stricter check.
    Share {
        /// mistlib room id to publish into (default: stream.room)
        #[arg(short, long)]
        room: Option<String>,
    },
    /// Stop the RTSP server
    Stop,
    /// Show stream status and URL
    Status,
}

#[derive(Subcommand)]
pub enum RelayAction {
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
    /// tc-chat rooms configured for relay (`mailbox.chat_rooms`), and
    /// whether each is currently joined
    Rooms,
    /// Recently relayed tc-chat messages for one room
    ChatLog {
        /// tc-chat room id (see `mailbox.chat_rooms` in settings)
        room: String,
        /// Max number of entries to return (default 50)
        #[arg(short, long)]
        limit: Option<usize>,
    },
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

#[derive(Subcommand)]
pub enum SchedAction {
    /// Create a new scheduled job
    Add {
        /// Job name
        #[arg(long)]
        name: String,
        /// Schedule expression: 5- or 6-field cron (seconds field
        /// optional), an `@every` interval (`@every 90m`, `@every 2d`,
        /// `@every 1w Sun 21:00`, `@every 3d 09:30`), or a descriptor
        /// (`@daily`, `@hourly`, `@weekly`, ...)
        #[arg(long)]
        schedule: String,
        /// Shell command to run
        #[arg(long)]
        command: String,
        /// Create the job disabled (jobs are enabled by default)
        #[arg(long)]
        disabled: bool,
    },
    /// List all scheduled jobs
    Ls,
    /// Update one or more fields of an existing job
    Set {
        /// Job id (e.g. job-042117)
        id: String,
        /// New name
        #[arg(long)]
        name: Option<String>,
        /// New schedule expression (see `sched add --help` for the syntax)
        #[arg(long)]
        schedule: Option<String>,
        /// New command
        #[arg(long)]
        command: Option<String>,
    },
    /// Remove a job
    Rm {
        /// Job id
        id: String,
    },
    /// Enable a job so it runs on its schedule again
    Enable {
        /// Job id
        id: String,
    },
    /// Disable a job without removing it
    Disable {
        /// Job id
        id: String,
    },
    /// Run a job immediately, outside its schedule
    Run {
        /// Job id
        id: String,
    },
    /// Show recent run history
    Logs {
        /// Only show runs for this job id
        #[arg(long)]
        id: Option<String>,
        /// Max number of runs to show (default 20)
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Compute upcoming run times for a schedule expression, without
    /// creating a job
    Next {
        /// Schedule expression (see `sched add --help` for the syntax)
        expr: String,
        /// Number of upcoming times to show (default 5)
        #[arg(short, long)]
        n: Option<usize>,
    },
}

#[derive(Subcommand)]
pub enum BotAction {
    /// List configured bot pipelines
    List,
    /// Run a pipeline immediately, outside its schedule
    Run {
        /// Pipeline id
        id: String,
    },
    /// Enable a pipeline so it runs on its schedule again
    Enable {
        /// Pipeline id
        id: String,
    },
    /// Disable a pipeline without removing it
    Disable {
        /// Pipeline id
        id: String,
    },
    /// Show recent pipeline run history
    Logs {
        /// Only show runs for this pipeline id
        #[arg(long)]
        id: Option<String>,
        /// Max number of runs to show (default 20)
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show recently delivered items (articles turned into posts/webhooks)
    Items {
        /// Only show items for this pipeline id
        #[arg(long)]
        id: Option<String>,
        /// Max number of items to show (default 20)
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show bot engine status (enabled, pipeline count, source rooms)
    Status,
}

#[derive(Subcommand)]
pub enum TunnelAction {
    /// Show tunnel status: room, self id, peers, forwards, pending prompts,
    /// trust list, and recent chat/notices
    Status,
    /// Start the tunnel session (joins/generates the configured room and
    /// restores any persisted forwards). A no-op if already running.
    Start,
    /// Stop the tunnel session (leaves the room; forwards stay persisted
    /// for the next `tunnel start`)
    Stop,
    /// Show or switch the tunnel's room
    Room {
        /// Room id to switch to (omit to show the current/configured room;
        /// a fresh id is generated if this is blank and none is configured
        /// yet)
        id: Option<String>,
        /// List recently used rooms instead of switching
        #[arg(long)]
        list: bool,
        /// Always mint and switch to a brand-new room id, ignoring any
        /// currently configured one (the CLI equivalent of the dashboard's
        /// "new room" button, `tunnel.room.new`) rather than only
        /// generating one if none is configured yet
        #[arg(long)]
        new: bool,
    },
    /// Serve one or more forward targets to peers in the tunnel's room
    /// (starts the session first if it isn't already running). Each target
    /// is `[tcp://|udp://]port` (listen and reach the same port, e.g. `22`)
    /// or `[tcp://|udp://]listen:remote` (e.g. `10022:22`); default
    /// protocol is tcp.
    Serve {
        /// Forward targets, e.g. `22` or `udp://5000` or `8080:80`
        forwards: Vec<String>,
    },
    /// Connect into `room`, requesting one or more forwards from whichever
    /// peer is serving them (starts/switches the session to `room` first).
    /// Each target is `listen:remote` (e.g. `10022:22`, listen locally on
    /// 10022, reach the peer's port 22), optionally suffixed `@peer-id` to
    /// pin it to one specific peer when more than one is connected.
    Connect {
        /// Room id to join
        room: String,
        /// Forward targets, e.g. `10022:22` or `udp://19000:9000@peer-id`
        forwards: Vec<String>,
    },
    /// List configured forwards and their state
    Ls,
    /// Remove a forward by its target (see `tunnel ls`)
    Rm {
        /// Forward target, e.g. `tcp:127.0.0.1:22`
        target: String,
    },
    /// Approve a pending connection authorization (see `tunnel status`'s
    /// `pending_auth` list)
    Approve {
        /// Pending authorization id
        id: u64,
        /// Also remember this decision, so future connections from the
        /// same peer for the same forward are approved automatically
        #[arg(long)]
        remember: bool,
    },
    /// Deny a pending connection authorization
    Deny {
        /// Pending authorization id
        id: u64,
        /// Also remember this decision (deny automatically from now on)
        #[arg(long)]
        remember: bool,
    },
    /// Accept a peer's forward proposal (see `tunnel status`'s
    /// `pending_forwards` list) -- serves it locally and notifies the peer
    Accept {
        /// Pending forward request id
        req_id: u64,
    },
    /// Reject a peer's forward proposal
    Reject {
        /// Pending forward request id
        req_id: u64,
    },
    /// Ask a peer to open a forward on *its* side (it becomes the server),
    /// the mirror image of `tunnel serve`. The proposal lands in that peer's
    /// pending list for its operator to accept or reject; until they answer
    /// it shows under `tunnel status`'s `pending_outgoing`, and the local
    /// connect-side forward is established automatically if they accept.
    Propose {
        /// Peer node id to ask (see `tunnel status`'s peer list)
        peer_id: String,
        /// Forward to request, e.g. `10022:22` or `udp://19000:9000`
        forward: String,
    },
    /// Show the trust list, or revoke one entry
    Trust {
        /// Revoke one trust entry: `<peer_id>@<forward_key>` (see the PEER
        /// and TARGET columns `tunnel trust` itself prints)
        #[arg(long)]
        revoke: Option<String>,
    },
    /// Send a chat message to every connected peer in the tunnel's room
    Chat {
        /// Message text
        text: String,
    },
    /// Open the interactive terminal UI. Talks to the daemon over the same
    /// IPC commands as every other `tunnel` subcommand (starts the daemon
    /// first if it isn't running yet, same as the rest of this CLI) rather
    /// than running an in-process session.
    Tui,
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
            DaemonAction::Run { host } => daemon::run_foreground(host),
            DaemonAction::Start { host } => daemon::start_background(host),
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
            KeyAction::Delegate { leaf, ttl } => {
                client_call("key.delegate", json!({ "leaf": leaf, "ttl": ttl }))
            }
            KeyAction::Delegations => {
                let response = request("key.delegations", json!({}))?;
                println!("{}", render_delegations(&response));
                Ok(())
            }
            KeyAction::Pair { ttl, timeout } => key_pair(ttl, timeout),
        },
        Command::Store { json, action } => match action {
            StoreAction::Put { path } => {
                let abs = canonicalize_or_die(&path);
                store_call(
                    "store.put",
                    json!({ "path": abs.to_string_lossy() }),
                    json,
                    render_cid,
                )
            }
            StoreAction::Get { id, output } => store_call(
                "store.get",
                json!({ "id": id, "output": output }),
                json,
                render_get,
            ),
            StoreAction::Ls => store_call("store.ls", json!({}), json, render_ls),
            StoreAction::PutFile { path, passphrase } => {
                let abs = canonicalize_or_die(&path);
                store_call(
                    "store.put-file",
                    json!({ "path": abs.to_string_lossy(), "passphrase": passphrase }),
                    json,
                    render_cid,
                )
            }
            StoreAction::GetFile {
                id,
                passphrase,
                output,
            } => store_call(
                "store.get-file",
                json!({ "cid": id, "passphrase": passphrase, "output": output }),
                json,
                render_get_file,
            ),
            StoreAction::ParseLink { url } => store_call(
                "store.parse-link",
                json!({ "url": url }),
                json,
                render_parse_link,
            ),
            StoreAction::FetchShare { url, output } => store_call(
                "store.fetch-share",
                json!({ "url": url, "output": output }),
                json,
                render_get,
            ),
            StoreAction::Sandbox { action } => match action {
                StoreSandboxAction::Import { path } => sandbox_import(path, json),
                StoreSandboxAction::Ls => sandbox_list(json),
                StoreSandboxAction::Rm { path } => store_call(
                    "store.sandbox.rm",
                    json!({ "path": path }),
                    json,
                    render_sandbox_rm,
                ),
                StoreSandboxAction::Export { path, output } => store_call(
                    "store.sandbox.export",
                    json!({ "path": path, "output": output }),
                    json,
                    render_sandbox_export,
                ),
            },
            StoreAction::Connect { room } => store_connect(room, json),
            StoreAction::FolderGet { url } => store_folder_get(url, json),
            StoreAction::SandboxImport { path } => sandbox_import(path, json),
            StoreAction::SandboxList => sandbox_list(json),
            StoreAction::FolderSync { action } => match action {
                StoreFolderSyncAction::Start { url, dir } => store_call(
                    "store.folder-sync",
                    json!({ "url": url, "dir": dir }),
                    json,
                    render_folder_sync_start,
                ),
                StoreFolderSyncAction::Ls => store_call(
                    "store.folder-sync.ls",
                    json!({}),
                    json,
                    render_folder_sync_ls,
                ),
                StoreFolderSyncAction::Stop { folder_id } => store_call(
                    "store.folder-sync.stop",
                    json!({ "folder_id": folder_id }),
                    json,
                    render_stopped,
                ),
            },
            StoreAction::FolderShare { action } => match action {
                StoreFolderShareAction::Start {
                    path,
                    passphrase,
                    name,
                    room,
                } => store_folder_share_start(path, passphrase, name, room, json),
                StoreFolderShareAction::Ls => store_call(
                    "store.folder-share.ls",
                    json!({}),
                    json,
                    render_folder_share_ls,
                ),
                StoreFolderShareAction::Stop { folder_id } => store_call(
                    "store.folder-share.stop",
                    json!({ "folder_id": folder_id }),
                    json,
                    render_stopped,
                ),
            },
        },
        Command::Stream { action } => match action {
            StreamAction::Start { room } => {
                let response = request("stream.start", json!({ "room": room }))?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                if let Some(url) = response.get("rtsp_url").and_then(Value::as_str) {
                    println!();
                    if let Some(room) = response.get("room").and_then(Value::as_str) {
                        println!("  Sharing into room {room}. Your own VRChat video player URL:");
                    } else {
                        println!("  Paste this URL into the VRChat video player:");
                    }
                    println!();
                    println!("      {url}");
                    println!();
                }
                Ok(())
            }
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
            StreamAction::Selftest {
                audio,
                width,
                height,
                fps,
                seconds,
            } => {
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
            StreamAction::Share { room } => {
                let response = request("stream.share.start", json!({ "room": room }))?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                if let Some(url) = response.get("rtsp_url").and_then(Value::as_str) {
                    println!();
                    println!("  Sharing into the room. Your own VRChat video player URL:");
                    println!();
                    println!("      {url}");
                    println!();
                }
                Ok(())
            }
            StreamAction::Stop => client_call("stream.stop", json!({})),
            StreamAction::Status => client_call("stream.status", json!({})),
        },
        Command::Relay { action } => match action {
            RelayAction::Send { to, file, message } => {
                if file.is_none() && message.is_none() {
                    bail!("provide --file or --message");
                }
                client_call(
                    "mailbox.send",
                    json!({ "to": to, "file": file, "message": message }),
                )
            }
            RelayAction::Ls => client_call("mailbox.ls", json!({})),
            RelayAction::Fetch => client_call("mailbox.fetch", json!({})),
            RelayAction::Rooms => client_call("mailbox.chat.rooms", json!({})),
            RelayAction::ChatLog { room, limit } => {
                client_call("mailbox.chat.log", json!({ "room": room, "limit": limit }))
            }
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
        Command::Sched { action } => match action {
            SchedAction::Add {
                name,
                schedule,
                command,
                disabled,
            } => client_call(
                "sched.add",
                json!({ "name": name, "schedule": schedule, "command": command, "enabled": !disabled }),
            ),
            SchedAction::Ls => {
                let response = request("sched.ls", json!({}))?;
                println!("{}", render_sched_ls(&response));
                Ok(())
            }
            SchedAction::Set {
                id,
                name,
                schedule,
                command,
            } => client_call(
                "sched.set",
                json!({ "id": id, "name": name, "schedule": schedule, "command": command }),
            ),
            SchedAction::Rm { id } => client_call("sched.rm", json!({ "id": id })),
            SchedAction::Enable { id } => {
                client_call("sched.enable", json!({ "id": id, "enabled": true }))
            }
            SchedAction::Disable { id } => {
                client_call("sched.enable", json!({ "id": id, "enabled": false }))
            }
            SchedAction::Run { id } => client_call("sched.run", json!({ "id": id })),
            SchedAction::Logs { id, limit } => {
                let response = request(
                    "sched.logs",
                    json!({ "id": id, "limit": limit.unwrap_or(20) }),
                )?;
                println!("{}", render_sched_logs(&response));
                Ok(())
            }
            SchedAction::Next { expr, n } => {
                let response = request(
                    "sched.next",
                    json!({ "schedule": expr, "n": n.unwrap_or(5) }),
                )?;
                println!("{}", render_sched_next(&response));
                Ok(())
            }
        },
        Command::Bot { action } => match action {
            BotAction::List => {
                let response = request("bot.list", json!({}))?;
                println!("{}", render_bot_ls(&response));
                Ok(())
            }
            BotAction::Run { id } => client_call("bot.run", json!({ "id": id })),
            BotAction::Enable { id } => client_call("bot.enable", json!({ "id": id })),
            BotAction::Disable { id } => client_call("bot.disable", json!({ "id": id })),
            BotAction::Logs { id, limit } => {
                let response = request(
                    "bot.logs",
                    json!({ "id": id, "limit": limit.unwrap_or(20) }),
                )?;
                println!("{}", render_bot_logs(&response));
                Ok(())
            }
            BotAction::Items { id, limit } => client_call(
                "bot.items",
                json!({ "id": id, "limit": limit.unwrap_or(20) }),
            ),
            BotAction::Status => client_call("bot.status", json!({})),
        },
        Command::Tunnel { action } => match action {
            TunnelAction::Status => client_call("tunnel.status", json!({})),
            TunnelAction::Start => client_call("tunnel.start", json!({})),
            TunnelAction::Stop => client_call("tunnel.stop", json!({})),
            TunnelAction::Room { id, list, new } => {
                if list {
                    client_call("tunnel.room.list", json!({}))
                } else if new {
                    let response = request("tunnel.room.new", json!({}))?;
                    let room = response
                        .get("room")
                        .and_then(Value::as_str)
                        .unwrap_or("(none)");
                    println!("room: {room}");
                    Ok(())
                } else if let Some(id) = id {
                    client_call("tunnel.room.set", json!({ "room": id }))
                } else {
                    let response = request("tunnel.status", json!({}))?;
                    let room = response
                        .get("room")
                        .and_then(Value::as_str)
                        .unwrap_or("(none)");
                    println!("room: {room}");
                    Ok(())
                }
            }
            TunnelAction::Serve { forwards } => {
                if forwards.is_empty() {
                    bail!("provide at least one forward, e.g. `mistl tunnel serve 22`");
                }
                let started = request("tunnel.start", json!({}))?;
                if let Some(room) = started.get("room").and_then(Value::as_str) {
                    println!("room: {room}");
                }
                for f in &forwards {
                    let (proto, addr, _fallback_port) = forward_args::parse_forward(f);
                    let target = forward_args::forward_key(proto, addr);
                    request(
                        "tunnel.forward.add",
                        json!({
                            "direction": "serve",
                            "proto": proto,
                            "addr": addr,
                            "listen_port": -1,
                            "target": target,
                        }),
                    )?;
                    println!("serving {target}");
                }
                Ok(())
            }
            TunnelAction::Connect { room, forwards } => {
                if forwards.is_empty() {
                    bail!(
                        "provide at least one forward, e.g. `mistl tunnel connect <room> 10022:22`"
                    );
                }
                request("tunnel.room.set", json!({ "room": room }))?;
                request("tunnel.start", json!({}))?;
                for f in &forwards {
                    let (proto, listen_port, target) = forward_args::parse_connect_forward(f);
                    request(
                        "tunnel.forward.add",
                        json!({
                            "direction": "connect",
                            "proto": proto,
                            "addr": "",
                            "listen_port": listen_port,
                            "target": target,
                        }),
                    )?;
                    println!("connecting {target} (local port {listen_port})");
                }
                Ok(())
            }
            TunnelAction::Ls => {
                let response = request("tunnel.status", json!({}))?;
                println!("{}", render_tunnel_forwards(&response));
                Ok(())
            }
            TunnelAction::Rm { target } => {
                client_call("tunnel.forward.remove", json!({ "target": target }))
            }
            TunnelAction::Propose { peer_id, forward } => {
                // Parsed with the same connect-side grammar `tunnel connect`
                // uses, so `10022:22` means the same thing in both.
                let (proto, listen_port, target) = forward_args::parse_connect_forward(&forward);
                client_call(
                    "tunnel.forward.propose",
                    json!({
                        "peer_id": peer_id,
                        "proto": proto,
                        "listen_port": listen_port,
                        "target": target,
                    }),
                )
            }
            TunnelAction::Approve { id, remember } => client_call(
                "tunnel.auth.approve",
                json!({ "id": id, "remember": remember }),
            ),
            TunnelAction::Deny { id, remember } => client_call(
                "tunnel.auth.deny",
                json!({ "id": id, "remember": remember }),
            ),
            TunnelAction::Accept { req_id } => {
                client_call("tunnel.forward.accept", json!({ "req_id": req_id }))
            }
            TunnelAction::Reject { req_id } => {
                client_call("tunnel.forward.reject", json!({ "req_id": req_id }))
            }
            TunnelAction::Trust { revoke } => match revoke {
                Some(key) => {
                    let (peer_id, forward_key) = key.split_once('@').with_context(|| {
                        format!(
                            "--revoke expects <peer_id>@<forward_key> (got {key:?}); \
                             see the PEER and TARGET columns `mistl tunnel trust` prints"
                        )
                    })?;
                    client_call(
                        "tunnel.trust.revoke",
                        json!({ "key": { "peer_id": peer_id, "forward_key": forward_key } }),
                    )
                }
                None => {
                    let response = request("tunnel.status", json!({}))?;
                    println!("{}", render_tunnel_trust(&response));
                    Ok(())
                }
            },
            TunnelAction::Chat { text } => client_call("tunnel.chat.send", json!({ "text": text })),
            TunnelAction::Tui => crate::tunnel::tui::run(),
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
                if let Some(applies) = response.get("applies").and_then(Value::as_str)
                    && applies != "next service start"
                {
                    println!("note: this change takes effect after a {applies}");
                }
                Ok(())
            }
        },
        Command::Update { action } => match action.unwrap_or(UpdateAction::Check) {
            UpdateAction::Check => {
                let response = request("update.check", json!({}))?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                if response.get("update_available").and_then(Value::as_bool) == Some(true) {
                    let latest = response
                        .get("latest")
                        .and_then(Value::as_str)
                        .unwrap_or("?");
                    println!();
                    if response.get("asset_available").and_then(Value::as_bool) == Some(false) {
                        let notes = response
                            .get("notes_url")
                            .and_then(Value::as_str)
                            .unwrap_or("");
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

// -- `mistl key delegate`/`delegations`/`pair` --------------------------

/// `mistl key delegations`: one row per issued delegation (as returned by
/// `key.delegations`, which already tags each entry with `expired`).
fn render_delegations(response: &Value) -> String {
    let entries = response.as_array().cloned().unwrap_or_default();
    if entries.is_empty() {
        return "no delegations issued".to_string();
    }
    let mut lines = vec!["LEAF\tISSUED\tEXPIRES\tSTATUS".to_string()];
    lines.extend(entries.iter().map(|d| {
        let leaf = d.get("leaf").and_then(Value::as_str).unwrap_or_default();
        let iat = d.get("iat").and_then(Value::as_str).unwrap_or_default();
        let exp = d.get("exp").and_then(Value::as_str).unwrap_or_default();
        let status = if d.get("expired").and_then(Value::as_bool).unwrap_or(false) {
            "expired"
        } else {
            "active"
        };
        format!("{leaf}\t{iat}\t{exp}\t{status}")
    }));
    lines.join("\n")
}

/// `mistl key pair`: start a pairing session, print the code, then poll
/// `key.pair.status` every 2 seconds (matching did-delegation.md's
/// suggested polling cadence for this side) until the browser claims the
/// code (or it expires/is cancelled).
fn key_pair(ttl: String, timeout: String) -> Result<()> {
    let start = request("key.pair.start", json!({ "ttl": ttl, "timeout": timeout }))?;
    let formatted = start
        .get("formatted_code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let room = start
        .get("room")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let expires_at = start
        .get("expires_at")
        .and_then(Value::as_str)
        .unwrap_or_default();

    println!("pairing code: {formatted}");
    println!();
    println!("  Enter this code in the browser app's \"pair with mistl\" flow.");
    println!("  Waiting (room {room}) until {expires_at}...");
    println!();

    loop {
        std::thread::sleep(std::time::Duration::from_secs(2));
        let status = request("key.pair.status", json!({}))?;
        match status
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("none")
        {
            "waiting" => continue,
            "issued" => {
                let leaf = status
                    .get("leaf")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let app = status
                    .get("app")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                println!("paired: delegated to {leaf} (app: {app})");
                return Ok(());
            }
            "expired" => bail!("pairing code expired before it was claimed"),
            "cancelled" => bail!("pairing was cancelled"),
            other => bail!("unexpected pairing status: {other}"),
        }
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
        daemon::start_background(None)?;
    }
    let url = crate::web::dashboard_url(&config.ui.listen);

    let opened = crate::web::browser::open_in_browser(&url);

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

// -- `mistl store`: tc-storage-cli-style human output --------------------
//
// tc-storage's `storage-cli` prints plain human text by default and never
// JSON (see its `cmd/tc-storage/main.go`): `put-file` prints just the CID,
// `get-file` prints "name\t<size> bytes\t<checksum>", `parse-link` prints
// "type=... room=... folder=... file=... cid=...", `sandbox-list` prints
// one entry per line, and every command reports failure as "error: <msg>"
// on stderr with a non-zero exit. `mistl store` matches that by default
// (formatting client-side from the daemon's unchanged JSON response) and
// falls back to the previous pretty-JSON behavior under `--json`.

/// Print "error: <msg>" to stderr and exit 1, tc-storage-cli's error
/// convention -- used for every `mistl store` command instead of the rest
/// of the CLI's default anyhow-chain formatting.
fn store_error(error: anyhow::Error) -> ! {
    eprintln!("error: {error}");
    std::process::exit(1);
}

/// Resolve `path` to an absolute path, or exit via [`store_error`] with the
/// same "error: <msg>" convention as every other `store` failure (rather
/// than propagating a differently-formatted error through the rest of the
/// CLI's anyhow chain).
fn canonicalize_or_die(path: &str) -> std::path::PathBuf {
    match std::fs::canonicalize(path) {
        Ok(abs) => abs,
        Err(_) => store_error(anyhow::anyhow!("file not found: {path}")),
    }
}

/// Send a `store.*` request and print either pretty JSON (`--json`) or a
/// human-rendered line built by `render` from the response. Errors (from
/// the request itself, or from JSON formatting) go through [`store_error`].
fn store_call(
    cmd: &str,
    args: Value,
    json: bool,
    render: impl FnOnce(&Value) -> String,
) -> Result<()> {
    let response = match request(cmd, args) {
        Ok(response) => response,
        Err(error) => store_error(error),
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!("{}", render(&response));
    }
    Ok(())
}

fn sandbox_import(path: String, json: bool) -> Result<()> {
    let abs = canonicalize_or_die(&path);
    store_call(
        "store.sandbox.import",
        json!({ "path": abs.to_string_lossy() }),
        json,
        render_sandbox_import,
    )
}

fn sandbox_list(json: bool) -> Result<()> {
    store_call("store.sandbox.ls", json!({}), json, render_sandbox_ls)
}

/// `mistl store connect`: like [`store_call`], but the peer count also
/// decides the exit code (tc-storage-cli's `connect` prints its
/// node/room/peer summary either way, then fails if zero peers connected).
fn store_connect(room: Option<String>, json: bool) -> Result<()> {
    let response = match request("store.connect", json!({ "room": room })) {
        Ok(response) => response,
        Err(error) => store_error(error),
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!("{}", render_connect(&response));
    }
    let peer_count = response
        .get("peers")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if peer_count == 0 {
        store_error(anyhow::anyhow!("no peers connected"));
    }
    Ok(())
}

/// `mistl store folder-get`: like [`store_call`], but also flushes the
/// response's batched `progress` lines to stderr (prefixed "… ", matching
/// tc-storage-cli) before the primary output, and any `skipped` files after
/// it -- see `storage::folder_share`'s module doc for why progress is
/// batched rather than streamed.
fn store_folder_get(url: String, json: bool) -> Result<()> {
    let response = match request("store.folder-get", json!({ "url": url })) {
        Ok(response) => response,
        Err(error) => store_error(error),
    };
    if let Some(lines) = response.get("progress").and_then(Value::as_array) {
        for line in lines.iter().filter_map(Value::as_str) {
            eprintln!("… {line}");
        }
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!("{}", render_folder_get(&response));
    }
    if let Some(skipped) = response.get("skipped").and_then(Value::as_array) {
        for entry in skipped.iter().filter_map(Value::as_str) {
            eprintln!("  skipped: {entry}");
        }
    }
    Ok(())
}

fn render_cid(response: &Value) -> String {
    response
        .get("cid")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn render_get(response: &Value) -> String {
    let name = response
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let size = response
        .get("size")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let output = response
        .get("output")
        .and_then(Value::as_str)
        .unwrap_or_default();
    format!("saved {name} ({size} bytes) to {output}")
}

fn render_ls(response: &Value) -> String {
    let entries = response.as_array().cloned().unwrap_or_default();
    if entries.is_empty() {
        return "(empty)".to_string();
    }
    entries
        .iter()
        .map(|entry| {
            let cid = entry.get("cid").and_then(Value::as_str).unwrap_or_default();
            let name = entry
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let size = entry
                .get("size")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let stored_at = entry
                .get("stored_at")
                .and_then(Value::as_str)
                .unwrap_or_default();
            format!("{cid}\t{name}\t{size} bytes\t{stored_at}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Matches tc-storage-cli's `get-file` line exactly
/// (`"%s\t%d bytes\t%s\n", file.Name, file.Size, file.Checksum`). Unlike
/// tc-storage's version (metadata only), mistl's `store.get-file` also
/// materializes the file -- noted separately on stderr so the primary
/// stdout line stays script-compatible with tc-storage's.
fn render_get_file(response: &Value) -> String {
    let name = response
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let size = response
        .get("size")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let checksum = response
        .get("checksum")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if let Some(output) = response.get("output").and_then(Value::as_str) {
        eprintln!("saved to {output}");
    }
    if response.get("checksum_ok").and_then(Value::as_bool) == Some(false) {
        eprintln!("warning: checksum mismatch");
    }
    if let Some(note) = response.get("note").and_then(Value::as_str) {
        eprintln!("note: {note}");
    }
    format!("{name}\t{size} bytes\t{checksum}")
}

/// Matches tc-storage-cli's `parse-link` line exactly
/// (`"type=%s room=%s folder=%s file=%s cid=%s\n"`).
fn render_parse_link(response: &Value) -> String {
    let field = |key: &str| {
        response
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
    };
    format!(
        "type={} room={} folder={} file={} cid={}",
        field("type"),
        field("room_id"),
        field("folder_id"),
        field("file_id"),
        field("cid"),
    )
}

fn render_sandbox_import(response: &Value) -> String {
    response
        .get("imported")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn render_sandbox_ls(response: &Value) -> String {
    response
        .get("entries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_sandbox_rm(response: &Value) -> String {
    response
        .get("removed")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn render_sandbox_export(response: &Value) -> String {
    let name = response
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let size = response
        .get("size")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let output = response
        .get("output")
        .and_then(Value::as_str)
        .unwrap_or_default();
    format!("exported {name} ({size} bytes) to {output}")
}

/// Matches tc-storage-cli's `connect` output
/// (`"node=%s room=%s peers=%d\n"` + one `"peer %s"` line each) when the
/// response carries a single `room` -- i.e. an explicit `--room` was given,
/// or exactly one room was involved (one `storage.room_ids` entry
/// configured, or none). This single-room shape is frozen: it is the
/// contract tc-storage-cli's own `connect` output must keep matching byte
/// for byte, so existing scripts parsing it don't break now that the store
/// can join several rooms at once.
///
/// When there is no single `room` (zero or several rooms joined, none of
/// them singled out), the response instead carries a `rooms` array and this
/// renders `"node=%s rooms=%s,%s,... peers=%d\n"` (comma-joined, in the
/// order the daemon returned them -- sorted), followed by the same `peer
/// %s` lines.
fn render_connect(response: &Value) -> String {
    let node_id = response
        .get("node_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let peers = response
        .get("peers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let header = if let Some(room) = response.get("room").and_then(Value::as_str) {
        format!("node={node_id} room={room} peers={}", peers.len())
    } else if let Some(rooms) = response.get("rooms").and_then(Value::as_array) {
        let rooms = rooms
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(",");
        format!("node={node_id} rooms={rooms} peers={}", peers.len())
    } else {
        format!("node={node_id} room= peers={}", peers.len())
    };
    let mut lines = vec![header];
    lines.extend(
        peers
            .iter()
            .filter_map(Value::as_str)
            .map(|id| format!("peer {id}")),
    );
    lines.join("\n")
}

/// Matches tc-storage-cli's `folder-get` output
/// (`"folder: %s\nsaved %d file(s):\n"` + one `"  %s"` line each).
fn render_folder_get(response: &Value) -> String {
    let folder_name = response
        .get("folder_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let files = response
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut lines = vec![
        format!("folder: {folder_name}"),
        format!("saved {} file(s):", files.len()),
    ];
    lines.extend(
        files
            .iter()
            .filter_map(Value::as_str)
            .map(|path| format!("  {path}")),
    );
    lines.join("\n")
}

/// `mistl store folder-share start`: publish a directory, print its link.
fn store_folder_share_start(
    path: String,
    passphrase: String,
    name: Option<String>,
    room: Option<String>,
    json: bool,
) -> Result<()> {
    let abs = canonicalize_or_die(&path);
    let response = match request(
        "store.folder-share",
        json!({
            "path": abs.to_string_lossy(),
            "passphrase": passphrase,
            "name": name,
            "room": room,
        }),
    ) {
        Ok(response) => response,
        Err(error) => store_error(error),
    };
    flush_progress(&response);
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!("{}", render_folder_share_start(&response));
    }
    Ok(())
}

/// Flush a response's batched `progress` lines to stderr ("… " prefixed,
/// matching tc-storage-cli), shared by the folder-get/share commands
/// (folder-sync registers instantly and has no synchronous progress to
/// flush -- see [`render_folder_sync_start`]).
fn flush_progress(response: &Value) {
    if let Some(lines) = response.get("progress").and_then(Value::as_array) {
        for line in lines.iter().filter_map(Value::as_str) {
            eprintln!("… {line}");
        }
    }
}

/// A folder sync's display destination: its managed sandbox subdirectory
/// (the normal case) if `sandbox_dir` is set, else its raw `local_dir` (a
/// legacy entry, or one registered with an explicit `--dir` override).
fn folder_sync_destination(sync: &Value) -> String {
    match sync.get("sandbox_dir").and_then(Value::as_str) {
        Some(sandbox_dir) if !sandbox_dir.is_empty() => format!("sandbox/{sandbox_dir}"),
        _ => sync
            .get("local_dir")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    }
}

/// `mistl store folder-sync start`: the daemon persists a `"connecting"`
/// entry and returns immediately -- the access-grant handshake and first
/// fetch run in the background (retried automatically if the owner is
/// offline), so this only ever prints a registration confirmation, never
/// sync progress.
fn render_folder_sync_start(response: &Value) -> String {
    let sync = response.get("sync").cloned().unwrap_or_default();
    format!(
        "registered sync for folder {:?} ({}) into {}\nconnecting and fetching in the background \
         -- run `mistl store folder-sync ls` for status, `mistl store sandbox export` to extract files",
        sync.get("folder_name")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        sync.get("folder_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        folder_sync_destination(&sync),
    )
}

fn render_folder_sync_ls(response: &Value) -> String {
    let syncs = response
        .get("syncs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if syncs.is_empty() {
        return "no active folder syncs".to_string();
    }
    let mut lines = vec![format!("{} folder sync(s):", syncs.len())];
    lines.extend(syncs.iter().map(|sync| {
        let status = sync
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("synced");
        let mut line = format!(
            "  {} {:?} -> {} (room {}, status {status}, last sync {})",
            sync.get("folder_id")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            sync.get("folder_name")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            folder_sync_destination(sync),
            sync.get("room_id")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            sync.get("last_synced_at")
                .and_then(Value::as_str)
                .unwrap_or("never"),
        );
        if let Some(error) = sync.get("last_error").and_then(Value::as_str) {
            line.push_str(&format!(" [error: {error}]"));
        }
        line
    }));
    lines.join("\n")
}

fn render_folder_share_start(response: &Value) -> String {
    format!(
        "sharing folder {} in room {} ({} file(s) published)\nshare link:\n{}",
        response
            .get("folder_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        response
            .get("room_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        response
            .get("files_published")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        response
            .get("share_url")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    )
}

fn render_folder_share_ls(response: &Value) -> String {
    let shares = response
        .get("shares")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if shares.is_empty() {
        return "no shared folders".to_string();
    }
    let mut lines = vec![format!("{} shared folder(s):", shares.len())];
    lines.extend(shares.iter().map(|share| {
        format!(
            "  {} {:?} from {} (room {}, last published {})",
            share
                .get("folder_id")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            share
                .get("folder_name")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            share
                .get("local_dir")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            share
                .get("room_id")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            share
                .get("last_published_at")
                .and_then(Value::as_str)
                .unwrap_or("never"),
        )
    }));
    lines.join("\n")
}

fn render_stopped(response: &Value) -> String {
    if response
        .get("stopped")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "stopped".to_string()
    } else {
        "not found".to_string()
    }
}

// -- `mistl sched`: human-readable job/run rendering ----------------------

/// `mistl sched ls`: one row per job, tab-separated, sorted by name (as
/// returned by `sched.ls`). `next_run` prints "-" when null (job disabled or
/// otherwise not scheduled).
fn render_sched_ls(response: &Value) -> String {
    let jobs = response
        .get("jobs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if jobs.is_empty() {
        return "no scheduled jobs".to_string();
    }
    let mut lines = vec!["ID\tNAME\tENABLED\tSCHEDULE\tNEXT RUN".to_string()];
    lines.extend(jobs.iter().map(|job| {
        let id = job.get("id").and_then(Value::as_str).unwrap_or_default();
        let name = job.get("name").and_then(Value::as_str).unwrap_or_default();
        let enabled = job.get("enabled").and_then(Value::as_bool).unwrap_or(false);
        let schedule = job
            .get("schedule")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let next_run = job.get("next_run").and_then(Value::as_str).unwrap_or("-");
        format!("{id}\t{name}\t{enabled}\t{schedule}\t{next_run}")
    }));
    lines.join("\n")
}

/// `mistl sched logs`: one block per run (newest first, as returned by
/// `sched.logs`), separated by a blank line.
fn render_sched_logs(response: &Value) -> String {
    let runs = response
        .get("runs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if runs.is_empty() {
        return "no runs recorded".to_string();
    }
    runs.iter()
        .map(render_sched_run)
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Render one run record: a header line (start time, job name, OK/FAIL plus
/// exit code, duration if both timestamps parse) followed by its output,
/// indented and capped at [`SCHED_LOG_OUTPUT_LINES`] lines.
fn render_sched_run(run: &Value) -> String {
    let job_name = run
        .get("job_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let started_at = run
        .get("started_at")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let ended_at = run.get("ended_at").and_then(Value::as_str);
    let ok = run.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let exit_code = run.get("exit_code").and_then(Value::as_i64);
    let outcome = if ok {
        "OK".to_string()
    } else {
        match exit_code {
            Some(code) => format!("FAIL({code})"),
            None => "FAIL".to_string(),
        }
    };
    let mut header = format!("{started_at}  {job_name}  {outcome}");
    if let Some(ended_at) = ended_at
        && let Some(duration) = sched_duration(started_at, ended_at)
    {
        header.push_str(&format!("  ({duration})"));
    }
    let output = run
        .get("output")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut lines = vec![header];
    lines.extend(
        truncate_sched_output(output)
            .into_iter()
            .map(|line| format!("    {line}")),
    );
    lines.join("\n")
}

/// Elapsed wall-clock time between two RFC3339 timestamps, formatted to one
/// decimal place of seconds, or `None` if either fails to parse.
fn sched_duration(started_at: &str, ended_at: &str) -> Option<String> {
    let started = chrono::DateTime::parse_from_rfc3339(started_at).ok()?;
    let ended = chrono::DateTime::parse_from_rfc3339(ended_at).ok()?;
    let seconds = (ended - started).num_milliseconds().max(0) as f64 / 1000.0;
    Some(format!("{seconds:.1}s"))
}

/// Max output lines shown per run in `sched logs` before truncating.
const SCHED_LOG_OUTPUT_LINES: usize = 10;

/// Split `output` into lines, capping at [`SCHED_LOG_OUTPUT_LINES`] and
/// appending a "… (truncated)" marker line if more remain.
fn truncate_sched_output(output: &str) -> Vec<String> {
    if output.is_empty() {
        return Vec::new();
    }
    let lines: Vec<&str> = output.lines().collect();
    if lines.len() <= SCHED_LOG_OUTPUT_LINES {
        return lines.into_iter().map(str::to_string).collect();
    }
    let mut truncated: Vec<String> = lines[..SCHED_LOG_OUTPUT_LINES]
        .iter()
        .map(|line| line.to_string())
        .collect();
    truncated.push("… (truncated)".to_string());
    truncated
}

/// `mistl sched next`: one upcoming RFC3339 time per line.
fn render_sched_next(response: &Value) -> String {
    response
        .get("times")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n")
}

// -- `mistl bot`: human-readable pipeline/run rendering --------------------
//
// Same shape as the `sched` renderers above: `bot list` is a
// `render_sched_ls`-style tab-separated table, `bot logs` is a
// `render_sched_run`-style one-block-per-run listing. `bot run`/`status`/
// `enable`/`disable`/`items` stay pretty-JSON via `client_call`, matching
// the Wave 2 brief's CLI UX spec.

/// `mistl bot list`: one row per pipeline, tab-separated
/// (`ID ENABLED SCHEDULE NEXT RUN LAST RUN LAST STATUS`). `LAST STATUS` is
/// `"OK (N delivered)"`, `"FAIL: <error>"`, or `"-"` when the pipeline has
/// never run.
fn render_bot_ls(response: &Value) -> String {
    let pipelines = response
        .get("pipelines")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if pipelines.is_empty() {
        return "no bot pipelines configured".to_string();
    }
    let mut lines = vec!["ID\tENABLED\tSCHEDULE\tNEXT RUN\tLAST RUN\tLAST STATUS".to_string()];
    lines.extend(pipelines.iter().map(|pipeline| {
        let id = pipeline
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let enabled = pipeline
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let schedule = pipeline
            .get("schedule")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let next_run = pipeline
            .get("next_run")
            .and_then(Value::as_str)
            .unwrap_or("-");
        let (last_run_at, last_status) = bot_last_run_columns(pipeline.get("last_run"));
        format!("{id}\t{enabled}\t{schedule}\t{next_run}\t{last_run_at}\t{last_status}")
    }));
    lines.join("\n")
}

/// `(LAST RUN, LAST STATUS)` for one `bot.list` pipeline entry's `last_run`
/// (a `RunRecord` or `null`).
fn bot_last_run_columns(last_run: Option<&Value>) -> (String, String) {
    let Some(run) = last_run.filter(|v| !v.is_null()) else {
        return ("-".to_string(), "-".to_string());
    };
    let started_at = run
        .get("started_at")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let status = if run.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        let delivered = run
            .get("delivered_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        format!("OK ({delivered} delivered)")
    } else {
        let error = run
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        format!("FAIL: {error}")
    };
    (started_at, status)
}

/// `mistl bot logs`: one block per run (newest first, as returned by
/// `bot.logs`), separated by a blank line.
fn render_bot_logs(response: &Value) -> String {
    let runs = response
        .get("runs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if runs.is_empty() {
        return "no bot runs recorded".to_string();
    }
    runs.iter()
        .map(render_bot_run)
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Render one `RunRecord`: a header line (start time, pipeline id, OK/FAIL,
/// duration if both timestamps parse) followed by fetched/delivered counts
/// and, on failure, the error.
fn render_bot_run(run: &Value) -> String {
    let pipeline_id = run
        .get("pipeline_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let started_at = run
        .get("started_at")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let ended_at = run.get("ended_at").and_then(Value::as_str);
    let ok = run.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let fetched = run
        .get("fetched_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let delivered = run
        .get("delivered_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let mut header = format!(
        "{started_at}  {pipeline_id}  {}",
        if ok { "OK" } else { "FAIL" }
    );
    if let Some(ended_at) = ended_at
        && let Some(duration) = sched_duration(started_at, ended_at)
    {
        header.push_str(&format!("  ({duration})"));
    }
    let mut lines = vec![
        header,
        format!("    fetched={fetched} delivered={delivered}"),
    ];
    if let Some(error) = run.get("error").and_then(Value::as_str) {
        lines.push(format!("    error: {error}"));
    }
    lines.join("\n")
}

// -- `mistl tunnel`: human-readable forward/trust rendering ---------------
//
// Same `render_sched_ls`-style tab-separated table shape as the renderers
// above. Both read `tunnel.status`'s `Snapshot::to_json()` fields
// defensively (`unwrap_or_default`) rather than asserting they're present,
// since the exact snapshot shape is produced by `crate::tunnel::session`
// (see its module doc for the authoritative field list); a missing field
// here just renders as an empty column instead of a panic.

/// `mistl tunnel ls`: one row per entry in `tunnel.status`'s `forwards` list.
fn render_tunnel_forwards(response: &Value) -> String {
    let forwards = response
        .get("forwards")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if forwards.is_empty() {
        return "no forwards configured".to_string();
    }
    let mut lines =
        vec!["DIRECTION\tPROTO\tADDR\tLISTEN\tTARGET\tSTATE\tCONNS\tTX\tRX".to_string()];
    lines.extend(forwards.iter().map(|f| {
        let direction = f
            .get("direction")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let proto = f.get("proto").and_then(Value::as_str).unwrap_or_default();
        let addr = f.get("addr").and_then(Value::as_str).unwrap_or_default();
        let listen_port = f
            .get("listen_port")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        let target = f.get("target").and_then(Value::as_str).unwrap_or_default();
        let state = f.get("state").and_then(Value::as_str).unwrap_or_default();
        let conns = f
            .get("active_conns")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let bytes_tx = f
            .get("bytes_tx")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let bytes_rx = f
            .get("bytes_rx")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        format!(
            "{direction}\t{proto}\t{addr}\t{listen_port}\t{target}\t{state}\t{conns}\t{bytes_tx}\t{bytes_rx}"
        )
    }));
    lines.join("\n")
}

/// `mistl tunnel trust` (no `--revoke`): one row per entry in
/// `tunnel.status`'s `trust` list, plus a reminder of the `--revoke` syntax
/// (`<peer_id>@<forward_key>`, matched against the PEER/TARGET columns
/// printed here -- see `TunnelAction::Trust`).
fn render_tunnel_trust(response: &Value) -> String {
    let trust = response
        .get("trust")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if trust.is_empty() {
        return "no trust entries".to_string();
    }
    let mut lines = vec!["PEER\tTARGET\tDECISION".to_string()];
    lines.extend(trust.iter().map(|entry| {
        let peer_id = entry
            .get("peer_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let target = entry
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let decision = entry
            .get("decision")
            .and_then(Value::as_str)
            .unwrap_or_default();
        format!("{peer_id}\t{target}\t{decision}")
    }));
    lines.push(String::new());
    lines.push("revoke with: mistl tunnel trust --revoke <peer_id>@<target>".to_string());
    lines.join("\n")
}

#[cfg(test)]
mod bot_render_tests {
    use super::*;

    #[test]
    fn render_bot_ls_reports_no_pipelines() {
        assert_eq!(
            render_bot_ls(&json!({ "pipelines": [] })),
            "no bot pipelines configured"
        );
    }

    #[test]
    fn render_bot_ls_shows_dash_columns_for_a_never_run_pipeline() {
        let response = json!({
            "pipelines": [{
                "id": "news-audio",
                "enabled": true,
                "schedule": "@every 30m",
                "next_run": "2026-07-12T01:00:00Z",
                "last_run": null,
                "warnings": [],
            }]
        });
        let rendered = render_bot_ls(&response);
        assert_eq!(
            rendered,
            "ID\tENABLED\tSCHEDULE\tNEXT RUN\tLAST RUN\tLAST STATUS\n\
             news-audio\ttrue\t@every 30m\t2026-07-12T01:00:00Z\t-\t-"
        );
    }

    #[test]
    fn render_bot_ls_formats_a_successful_last_run() {
        let response = json!({
            "pipelines": [{
                "id": "news-audio",
                "enabled": true,
                "schedule": "@every 30m",
                "next_run": null,
                "last_run": {
                    "pipeline_id": "news-audio",
                    "started_at": "2026-07-12T00:00:00Z",
                    "ended_at": "2026-07-12T00:00:05Z",
                    "ok": true,
                    "error": null,
                    "fetched_count": 3,
                    "delivered_count": 2,
                },
                "warnings": [],
            }]
        });
        let rendered = render_bot_ls(&response);
        assert!(
            rendered.contains("2026-07-12T00:00:00Z\tOK (2 delivered)"),
            "{rendered}"
        );
    }

    #[test]
    fn render_bot_ls_formats_a_failed_last_run() {
        let response = json!({
            "pipelines": [{
                "id": "news-audio",
                "enabled": false,
                "schedule": "@every 30m",
                "next_run": null,
                "last_run": {
                    "pipeline_id": "news-audio",
                    "started_at": "2026-07-12T00:00:00Z",
                    "ended_at": "2026-07-12T00:00:01Z",
                    "ok": false,
                    "error": "preset \"worker\" not found in ai.presets",
                    "fetched_count": 0,
                    "delivered_count": 0,
                },
                "warnings": ["preset \"worker\" not found in ai.presets"],
            }]
        });
        let rendered = render_bot_ls(&response);
        assert!(
            rendered.contains("FAIL: preset \"worker\" not found in ai.presets"),
            "{rendered}"
        );
    }

    #[test]
    fn render_bot_logs_reports_no_runs() {
        assert_eq!(
            render_bot_logs(&json!({ "runs": [] })),
            "no bot runs recorded"
        );
    }

    #[test]
    fn render_bot_logs_formats_a_successful_run_with_duration_and_counts() {
        let response = json!({
            "runs": [{
                "pipeline_id": "news-audio",
                "started_at": "2026-07-12T00:00:00Z",
                "ended_at": "2026-07-12T00:00:02Z",
                "ok": true,
                "error": null,
                "fetched_count": 2,
                "delivered_count": 2,
            }]
        });
        let rendered = render_bot_logs(&response);
        assert!(
            rendered.starts_with("2026-07-12T00:00:00Z  news-audio  OK  (2.0s)"),
            "{rendered}"
        );
        assert!(rendered.contains("fetched=2 delivered=2"), "{rendered}");
    }

    #[test]
    fn render_bot_logs_shows_the_error_line_for_a_failed_run() {
        let response = json!({
            "runs": [{
                "pipeline_id": "news-audio",
                "started_at": "2026-07-12T00:00:00Z",
                "ended_at": "2026-07-12T00:00:01Z",
                "ok": false,
                "error": "source failed: could not join room",
                "fetched_count": 0,
                "delivered_count": 0,
            }]
        });
        let rendered = render_bot_logs(&response);
        assert!(rendered.contains("FAIL"), "{rendered}");
        assert!(
            rendered.contains("error: source failed: could not join room"),
            "{rendered}"
        );
    }

    #[test]
    fn render_bot_logs_separates_multiple_runs_with_a_blank_line() {
        let response = json!({
            "runs": [
                {
                    "pipeline_id": "news-audio",
                    "started_at": "2026-07-12T00:00:00Z",
                    "ended_at": "2026-07-12T00:00:01Z",
                    "ok": true,
                    "error": null,
                    "fetched_count": 1,
                    "delivered_count": 1,
                },
                {
                    "pipeline_id": "news-audio",
                    "started_at": "2026-07-11T00:00:00Z",
                    "ended_at": "2026-07-11T00:00:01Z",
                    "ok": true,
                    "error": null,
                    "fetched_count": 1,
                    "delivered_count": 1,
                },
            ]
        });
        let rendered = render_bot_logs(&response);
        assert_eq!(rendered.matches("\n\n").count(), 1);
    }
}

#[cfg(test)]
mod store_render_tests {
    use super::*;

    #[test]
    fn render_cid_extracts_the_cid_field() {
        assert_eq!(
            render_cid(&json!({ "cid": "bafyabc", "name": "x", "size": 1 })),
            "bafyabc"
        );
    }

    #[test]
    fn render_get_formats_saved_line() {
        let response = json!({ "name": "notes.txt", "size": 42, "output": "/tmp/notes.txt" });
        assert_eq!(
            render_get(&response),
            "saved notes.txt (42 bytes) to /tmp/notes.txt"
        );
    }

    #[test]
    fn render_ls_lists_entries_tab_separated() {
        let response = json!([
            { "cid": "bafy1", "name": "a.txt", "size": 10, "stored_at": "2026-01-01T00:00:00Z" },
            { "cid": "bafy2", "name": "b.txt", "size": 20, "stored_at": "2026-01-02T00:00:00Z" },
        ]);
        let rendered = render_ls(&response);
        assert_eq!(
            rendered,
            "bafy1\ta.txt\t10 bytes\t2026-01-01T00:00:00Z\nbafy2\tb.txt\t20 bytes\t2026-01-02T00:00:00Z"
        );
    }

    #[test]
    fn render_ls_reports_empty_store() {
        assert_eq!(render_ls(&json!([])), "(empty)");
    }

    #[test]
    fn render_get_file_matches_tc_storage_format() {
        let response = json!({
            "name": "notes.txt",
            "size": 42,
            "checksum": "deadbeef",
            "output": "/tmp/notes.txt",
        });
        // Primary stdout line matches tc-storage-cli's
        // "%s\t%d bytes\t%s\n" exactly; the `output` field is only ever
        // surfaced via a separate stderr note (not asserted here).
        assert_eq!(render_get_file(&response), "notes.txt\t42 bytes\tdeadbeef");
    }

    #[test]
    fn render_parse_link_matches_tc_storage_format() {
        let response = json!({
            "type": "file-share",
            "room_id": "r",
            "folder_id": "folder-1",
            "file_id": "file-1",
            "cid": "bafyabc",
        });
        assert_eq!(
            render_parse_link(&response),
            "type=file-share room=r folder=folder-1 file=file-1 cid=bafyabc"
        );
    }

    #[test]
    fn render_parse_link_handles_missing_fields_as_empty() {
        let response = json!({ "type": "folder-share", "room_id": "r" });
        assert_eq!(
            render_parse_link(&response),
            "type=folder-share room=r folder= file= cid="
        );
    }

    #[test]
    fn render_sandbox_import_prints_bare_name() {
        assert_eq!(
            render_sandbox_import(&json!({ "imported": "sub/name.txt" })),
            "sub/name.txt"
        );
    }

    #[test]
    fn render_sandbox_ls_lists_one_per_line() {
        let response = json!({ "entries": ["a.txt", "sub/b.txt"] });
        assert_eq!(render_sandbox_ls(&response), "a.txt\nsub/b.txt");
    }

    #[test]
    fn render_sandbox_rm_prints_removed_path() {
        assert_eq!(
            render_sandbox_rm(&json!({ "removed": "sub/name.txt" })),
            "sub/name.txt"
        );
    }

    #[test]
    fn render_sandbox_export_formats_exported_line() {
        let response = json!({ "name": "a.txt", "size": 5, "output": "/tmp/a.txt" });
        assert_eq!(
            render_sandbox_export(&response),
            "exported a.txt (5 bytes) to /tmp/a.txt"
        );
    }

    #[test]
    fn render_connect_matches_tc_storage_format() {
        let response = json!({
            "node_id": "abc123",
            "room": "tc-storage-cli",
            "peers": ["peer1", "peer2"],
        });
        assert_eq!(
            render_connect(&response),
            "node=abc123 room=tc-storage-cli peers=2\npeer peer1\npeer peer2"
        );
    }

    #[test]
    fn render_connect_handles_zero_peers() {
        let response = json!({ "node_id": "abc123", "room": "r", "peers": [] });
        assert_eq!(render_connect(&response), "node=abc123 room=r peers=0");
    }

    #[test]
    fn render_connect_renders_multiple_rooms_comma_joined() {
        // No `room` field: several `storage.room_ids` joined at once, none
        // singled out by an explicit `--room`.
        let response = json!({
            "node_id": "abc123",
            "rooms": ["a", "b"],
            "peers": ["peer1"],
        });
        assert_eq!(
            render_connect(&response),
            "node=abc123 rooms=a,b peers=1\npeer peer1"
        );
    }

    #[test]
    fn render_connect_renders_multiple_rooms_with_zero_peers() {
        let response = json!({ "node_id": "abc123", "rooms": ["a", "b"], "peers": [] });
        assert_eq!(render_connect(&response), "node=abc123 rooms=a,b peers=0");
    }

    #[test]
    fn render_folder_get_matches_tc_storage_format() {
        let response = json!({
            "folder_name": "Fixture Folder",
            "files": ["Fixture Folder/a.txt", "Fixture Folder/b.txt"],
        });
        assert_eq!(
            render_folder_get(&response),
            "folder: Fixture Folder\nsaved 2 file(s):\n  Fixture Folder/a.txt\n  Fixture Folder/b.txt"
        );
    }

    #[test]
    fn render_folder_sync_start_shows_the_sandbox_destination() {
        let response = json!({
            "sync": {
                "folder_id": "folder-1",
                "folder_name": "Fixture Folder",
                "local_dir": "C:\\Users\\testuser\\AppData\\Local\\mistl\\sandbox\\Fixture Folder",
                "sandbox_dir": "Fixture Folder",
            }
        });
        let rendered = render_folder_sync_start(&response);
        assert!(
            rendered.contains("into sandbox/Fixture Folder"),
            "{rendered}"
        );
    }

    #[test]
    fn render_folder_sync_start_falls_back_to_local_dir_for_a_dir_override() {
        let response = json!({
            "sync": {
                "folder_id": "folder-1",
                "folder_name": "Fixture Folder",
                "local_dir": "D:\\my-sync-target",
                "sandbox_dir": "",
            }
        });
        let rendered = render_folder_sync_start(&response);
        assert!(rendered.contains("into D:\\my-sync-target"), "{rendered}");
    }

    #[test]
    fn render_folder_sync_ls_shows_sandbox_destination_and_status() {
        let response = json!({
            "syncs": [{
                "folder_id": "folder-1",
                "folder_name": "Fixture Folder",
                "local_dir": "C:\\data\\sandbox\\Fixture Folder",
                "sandbox_dir": "Fixture Folder",
                "room_id": "room-1",
                "status": "synced",
                "last_synced_at": "2026-07-09T00:00:00Z",
            }]
        });
        let rendered = render_folder_sync_ls(&response);
        assert!(rendered.contains("sandbox/Fixture Folder"), "{rendered}");
        assert!(rendered.contains("status synced"), "{rendered}");
    }

    #[test]
    fn render_folder_sync_ls_reports_errors() {
        let response = json!({
            "syncs": [{
                "folder_id": "folder-1",
                "folder_name": "Fixture Folder",
                "local_dir": "C:\\data\\sandbox\\Fixture Folder",
                "sandbox_dir": "Fixture Folder",
                "room_id": "room-1",
                "status": "error",
                "last_synced_at": null,
                "last_error": "owner offline",
            }]
        });
        let rendered = render_folder_sync_ls(&response);
        assert!(rendered.contains("[error: owner offline]"), "{rendered}");
    }
}

#[cfg(test)]
mod sched_render_tests {
    use super::*;

    #[test]
    fn render_sched_ls_reports_no_jobs() {
        assert_eq!(render_sched_ls(&json!({ "jobs": [] })), "no scheduled jobs");
    }

    #[test]
    fn render_sched_ls_lists_jobs_with_next_run() {
        let response = json!({
            "jobs": [{
                "id": "job-042117",
                "name": "backup",
                "schedule": "@daily",
                "command": "backup.sh",
                "enabled": true,
                "created_at": "2026-07-01T00:00:00Z",
                "updated_at": "2026-07-01T00:00:00Z",
                "next_run": "2026-07-11T00:00:00Z",
            }]
        });
        let rendered = render_sched_ls(&response);
        assert_eq!(
            rendered,
            "ID\tNAME\tENABLED\tSCHEDULE\tNEXT RUN\n\
             job-042117\tbackup\ttrue\t@daily\t2026-07-11T00:00:00Z"
        );
    }

    #[test]
    fn render_sched_ls_shows_dash_for_null_next_run() {
        let response = json!({
            "jobs": [{
                "id": "job-1",
                "name": "disabled-job",
                "schedule": "@daily",
                "command": "x",
                "enabled": false,
                "created_at": "2026-07-01T00:00:00Z",
                "updated_at": "2026-07-01T00:00:00Z",
                "next_run": null,
            }]
        });
        let rendered = render_sched_ls(&response);
        assert!(
            rendered.ends_with("job-1\tdisabled-job\tfalse\t@daily\t-"),
            "{rendered}"
        );
    }

    #[test]
    fn render_sched_logs_reports_no_runs() {
        assert_eq!(
            render_sched_logs(&json!({ "runs": [] })),
            "no runs recorded"
        );
    }

    #[test]
    fn render_sched_logs_formats_a_successful_run_with_duration() {
        let response = json!({
            "runs": [{
                "job_id": "job-1",
                "job_name": "backup",
                "started_at": "2026-07-10T00:00:00Z",
                "ended_at": "2026-07-10T00:00:02Z",
                "exit_code": 0,
                "ok": true,
                "output": "done\n",
            }]
        });
        let rendered = render_sched_logs(&response);
        assert!(
            rendered.starts_with("2026-07-10T00:00:00Z  backup  OK  (2.0s)"),
            "{rendered}"
        );
        assert!(rendered.contains("    done"), "{rendered}");
    }

    #[test]
    fn render_sched_logs_formats_a_failed_run_with_exit_code() {
        let response = json!({
            "runs": [{
                "job_id": "job-1",
                "job_name": "backup",
                "started_at": "2026-07-10T00:00:00Z",
                "ended_at": "2026-07-10T00:00:01Z",
                "exit_code": 1,
                "ok": false,
                "output": "error: disk full\n",
            }]
        });
        let rendered = render_sched_logs(&response);
        assert!(rendered.contains("FAIL(1)"), "{rendered}");
    }

    #[test]
    fn render_sched_logs_truncates_long_output() {
        let output = (1..=15)
            .map(|n| format!("line{n}"))
            .collect::<Vec<_>>()
            .join("\n");
        let response = json!({
            "runs": [{
                "job_id": "job-1",
                "job_name": "chatty",
                "started_at": "2026-07-10T00:00:00Z",
                "ended_at": "2026-07-10T00:00:01Z",
                "exit_code": 0,
                "ok": true,
                "output": output,
            }]
        });
        let rendered = render_sched_logs(&response);
        assert!(rendered.contains("line10"), "{rendered}");
        assert!(!rendered.contains("line11"), "{rendered}");
        assert!(rendered.contains("… (truncated)"), "{rendered}");
    }

    #[test]
    fn render_sched_logs_handles_missing_ended_at_without_a_duration() {
        let response = json!({
            "runs": [{
                "job_id": "job-1",
                "job_name": "still-running",
                "started_at": "2026-07-10T00:00:00Z",
                "ended_at": null,
                "exit_code": null,
                "ok": false,
                "output": "",
            }]
        });
        let rendered = render_sched_logs(&response);
        assert_eq!(rendered, "2026-07-10T00:00:00Z  still-running  FAIL");
    }

    #[test]
    fn render_sched_next_lists_one_time_per_line() {
        let response = json!({ "times": ["2026-07-11T00:00:00Z", "2026-07-12T00:00:00Z"] });
        assert_eq!(
            render_sched_next(&response),
            "2026-07-11T00:00:00Z\n2026-07-12T00:00:00Z"
        );
    }

    #[test]
    fn render_sched_next_handles_empty_times() {
        assert_eq!(render_sched_next(&json!({ "times": [] })), "");
    }
}
