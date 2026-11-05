use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use crate::daemon;
use crate::tunnel::forward_args;

#[derive(Parser)]
#[command(
    name = "mistl",
    version,
    long_version = concat!(env!("CARGO_PKG_VERSION"), " ", env!("MISTL_CHANNEL"), "\nbuild ", env!("MISTL_BUILD_ID")),
    about = "MISTL - unified P2P daemon: identity, storage, VRChat screen share, tc-chat relay, AI network",
    after_help = "Running `mistl` with no arguments opens the web dashboard \
                  (starts the daemon if needed) -- double-clicking mistl.exe does the same."
)]
pub struct Cli {
    /// Isolated instance name (development builds default to this worktree).
    #[arg(long, global = true)]
    pub instance: Option<String>,
    /// Alternate state root; channel and instance are always appended.
    #[arg(long, global = true)]
    pub state_dir: Option<std::path::PathBuf>,
    /// Do not create a Windows notification-area icon.
    #[arg(long, global = true)]
    pub no_tray: bool,
    /// Omitted -> open the web dashboard.
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Allow/block external connections, persistently across PC restarts.
    Network {
        #[command(subcommand)]
        action: NetworkAction,
    },
    /// Show the identity of this binary without starting a daemon.
    BuildInfo,
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
    /// tc-chat relay: a standing relay/bot for configured tc-chat rooms, so
    /// chat messages keep arriving even without a tc-chat browser tab open.
    Chat {
        #[command(subcommand)]
        action: ChatAction,
    },
    /// Durable encrypted checkpoint escrow
    Escrow {
        #[command(subcommand)]
        action: EscrowAction,
    },
    /// Manage peer access, API tokens, capabilities, and usage limits
    Authz {
        /// Print JSON (token and capability creation print their values alone)
        #[arg(long, global = true)]
        json: bool,
        #[command(subcommand)]
        action: AuthzAction,
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
    /// Nostr relay: manage the listener and query stored events
    Nostr {
        #[command(subcommand)]
        action: NostrAction,
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
    /// Natural-language agent (TUI) that runs mistl commands and workspace
    /// tools, using the AI network through the local API
    Agent(crate::agent::AgentArgs),
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
        /// Do not add the install dir to the user's PATH
        #[arg(long)]
        no_path: bool,
    },
    /// Remove the per-user install, Start Menu shortcut, and autostart entry
    Uninstall,
    /// Manage start-on-login (launches the daemon headless at login)
    Autostart {
        #[command(subcommand)]
        action: AutostartAction,
    },
    /// Manage the user PATH entry that makes `mistl` available in every
    /// new terminal (Windows; on Unix only checks ~/.local/bin)
    Path {
        #[command(subcommand)]
        action: PathAction,
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
pub enum PathAction {
    /// Add the install dir to the user's PATH
    Enable,
    /// Remove the install dir from the user's PATH
    Disable,
    /// Show whether the install dir is on the user's PATH
    Status,
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Show the current configuration (secrets masked)
    Show,
    #[command(about = "Get one setting / 設定値を取得 / 读取配置项")]
    Get { path: String },
    /// Set one field, e.g. `mistl config set ai.default_ref <json>`
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
    /// Export the identity as an encrypted tc-storage envelope
    Export {
        /// Output file (stdout if omitted)
        #[arg(long)]
        out: Option<std::path::PathBuf>,
        /// Encryption passphrase (or MISTL_KEY_PASSPHRASE)
        #[arg(long)]
        passphrase: Option<String>,
    },
    /// Import a plain or encrypted tc-storage identity
    Import {
        /// Identity JSON file, or - for stdin
        file: String,
        /// Decryption passphrase (or MISTL_KEY_PASSPHRASE)
        #[arg(long)]
        passphrase: Option<String>,
        /// Replace a different identity, backing up the old key and metadata
        #[arg(long)]
        force: bool,
        /// Validate and report without writing any files
        #[arg(long)]
        dry_run: bool,
    },
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
    /// Review requests for a shared folder's key: a share link alone no
    /// longer grants access, the owner approves each requester (or keeps
    /// a per-folder allowlist)
    FolderAccess {
        #[command(subcommand)]
        action: StoreFolderAccessAction,
    },
}

#[derive(Subcommand)]
pub enum StoreFolderAccessAction {
    /// List pending access requests and per-folder allowlists
    Ls,
    /// Send the folder key to a pending requester
    Approve {
        /// Pending request id (from `folder-access ls`)
        id: String,
        /// Also add the requester to the folder's allowlist
        #[arg(long)]
        remember: bool,
    },
    /// Refuse a pending request
    Deny { id: String },
    /// Add a requester (DID or 16-hex node id) to a folder's allowlist
    Allow {
        folder_id: String,
        requester: String,
    },
    /// Remove a requester from a folder's allowlist
    Revoke {
        folder_id: String,
        requester: String,
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
pub enum EscrowAction {
    /// Show escrow status
    Status,
    /// Show machine-readable escrow status
    StatusJson,
    /// Show automatic replication status for a room
    Slots { room: String },
    /// Export encrypted escrow slots to a portable file
    Export {
        file: String,
        #[arg(long = "room")]
        rooms: Vec<String>,
        #[arg(long)]
        force: bool,
    },
    /// Import slots from an escrow export file (validated before anything is written)
    Import {
        file: String,
        #[arg(long = "room")]
        rooms: Vec<String>,
    },
    /// Change a cluster's voters after learners have caught up (local IPC only)
    Membership {
        room: String,
        #[arg(long, value_delimiter = ',', required = true)]
        voters: Vec<String>,
        #[arg(long, value_delimiter = ',')]
        learners: Vec<String>,
    },
    Rooms {
        #[command(subcommand)]
        action: EscrowRoomsAction,
    },
}

#[derive(Subcommand)]
pub enum EscrowRoomsAction {
    Add { room: String },
    Remove { room: String },
    List,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum AuthzService {
    Ai,
    Escrow,
}

impl AuthzService {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ai => "ai",
            Self::Escrow => "escrow",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum AuthzMode {
    Open,
    Allowlist,
}

impl AuthzMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Allowlist => "allowlist",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum AuthzPeriod {
    Day,
    Month,
    Total,
}

impl AuthzPeriod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Month => "month",
            Self::Total => "total",
        }
    }
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum AuthzSwitch {
    On,
    Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum AuthzCapabilityAction {
    Infer,
    Read,
    Write,
}

impl AuthzCapabilityAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Infer => "infer",
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

#[derive(clap::Args)]
pub struct AuthzLimitArgs {
    /// Counter period (UTC day, UTC month, or total)
    #[arg(long)]
    period: Option<AuthzPeriod>,
    /// Maximum requests per period
    #[arg(long)]
    max_requests: Option<u64>,
    /// Maximum generated characters per period
    #[arg(long)]
    max_output_chars: Option<u64>,
}

#[derive(Subcommand)]
pub enum AuthzAction {
    /// Show policies and token metadata
    Show,
    /// Change a service's access mode
    Mode {
        service: AuthzService,
        mode: AuthzMode,
    },
    /// Manage allowed peers
    Allow {
        #[command(subcommand)]
        action: AuthzRuleAction,
    },
    /// Manage denied peers
    Deny {
        #[command(subcommand)]
        action: AuthzRuleAction,
    },
    /// Set default limits or a per-subject override
    Limits {
        service: AuthzService,
        #[arg(long)]
        subject: Option<String>,
        #[command(flatten)]
        limits: AuthzLimitArgs,
        /// Maximum stored bytes (e.g. 1GiB)
        #[arg(long, value_parser = parse_authz_size)]
        max_stored: Option<u64>,
        /// Reset defaults or remove a subject's override
        #[arg(long, conflicts_with_all = ["period", "max_requests", "max_output_chars", "max_stored"])]
        clear: bool,
    },
    /// Show or reset cumulative usage
    #[command(args_conflicts_with_subcommands = true)]
    Usage {
        service: Option<AuthzService>,
        #[command(subcommand)]
        action: Option<AuthzUsageAction>,
    },
    /// Manage local API authorization
    Api {
        #[command(subcommand)]
        action: AuthzApiAction,
    },
    /// Manage API tokens
    Token {
        #[command(subcommand)]
        action: AuthzTokenAction,
    },
    /// Manage signed capabilities
    Cap {
        #[command(subcommand)]
        action: AuthzCapAction,
    },
    /// Manage trusted capability issuers
    Issuer {
        #[command(subcommand)]
        action: AuthzIssuerAction,
    },
    /// List verified peers
    Peers,
}

#[derive(Subcommand)]
pub enum AuthzRuleAction {
    /// Add or replace a rule
    Add {
        service: AuthzService,
        subject: String,
        /// Lifetime, e.g. 10m, 12h, or 7d (daemon minimum: 60s)
        #[arg(long, value_parser = parse_authz_ttl, conflicts_with = "expires")]
        ttl: Option<u64>,
        /// Expiry as an RFC3339 timestamp
        #[arg(long, value_parser = parse_authz_expiry)]
        expires: Option<String>,
        #[arg(long)]
        note: Option<String>,
    },
    /// Remove a rule
    Rm {
        service: AuthzService,
        subject: String,
    },
}

#[derive(Subcommand)]
pub enum AuthzUsageAction {
    /// Reset usage counters for a service or account
    Reset {
        service: AuthzService,
        #[arg(long)]
        account: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum AuthzApiAction {
    /// Require an API token for local API requests
    RequireToken { value: AuthzSwitch },
}

#[derive(Subcommand)]
pub enum AuthzTokenAction {
    /// Create a token; its secret is printed only once
    Create {
        name: String,
        #[arg(long = "model")]
        models: Vec<String>,
        #[command(flatten)]
        limits: AuthzLimitArgs,
        /// Lifetime, e.g. 10m, 12h, or 7d (daemon minimum: 60s)
        #[arg(long, value_parser = parse_authz_ttl, conflicts_with = "expires")]
        ttl: Option<u64>,
        /// Expiry as an RFC3339 timestamp
        #[arg(long, value_parser = parse_authz_expiry)]
        expires: Option<String>,
    },
    /// List token metadata
    List,
    /// Update token metadata and limits
    Update {
        id: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long = "model")]
        models: Vec<String>,
        /// Remove all model restrictions
        #[arg(long, conflicts_with = "models")]
        clear_models: bool,
        #[command(flatten)]
        limits: AuthzLimitArgs,
        /// Set an RFC3339 expiry
        #[arg(long, value_parser = parse_authz_expiry, conflicts_with = "clear_expiry")]
        expires: Option<String>,
        /// Remove the token's expiry
        #[arg(long)]
        clear_expiry: bool,
    },
    /// Revoke a token
    Revoke { id: String },
    /// Delete a token
    Delete { id: String },
}

#[derive(Subcommand)]
pub enum AuthzCapAction {
    /// Issue a capability; its JSON is printed on stdout
    Issue {
        aud_did: String,
        service: AuthzService,
        /// Permitted action; may be repeated (default: infer for AI, read for escrow)
        #[arg(long = "action")]
        actions: Vec<AuthzCapabilityAction>,
        /// Resource: *, ROOM/*, or ROOM/SLOT (required for escrow)
        #[arg(long, required_if_eq("service", "escrow"))]
        resource: Option<String>,
        /// Lifetime, e.g. 10m, 12h, or 7d (default: 7d)
        #[arg(long, value_parser = parse_authz_ttl, conflicts_with = "expires")]
        ttl: Option<u64>,
        /// Expiry as an RFC3339 timestamp
        #[arg(long, value_parser = parse_authz_expiry)]
        expires: Option<String>,
        /// DID of an allowed verifier; may be repeated
        #[arg(long)]
        to: Vec<String>,
        #[command(flatten)]
        limits: AuthzLimitArgs,
        /// Held capability to delegate
        #[arg(long)]
        parent: Option<String>,
        /// Try to deliver the capability to its holder
        #[arg(long)]
        deliver: bool,
    },
    /// Import a capability from a file, stdin (-), or JSON
    Import { source: String },
    /// List issued, held, presented, and revoked capabilities
    List,
    /// Revoke an issued capability, or revoke any capability locally
    Revoke {
        id: String,
        #[arg(long)]
        local: bool,
    },
    /// Remove a held capability
    Remove { id: String },
    /// Verify a capability from a file, stdin (-), or JSON
    Verify {
        source: String,
        #[arg(long)]
        service: Option<AuthzService>,
        #[arg(long)]
        action: Option<AuthzCapabilityAction>,
        #[arg(long)]
        resource: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum AuthzIssuerAction {
    /// Trust an issuer for a service
    Add { service: AuthzService, did: String },
    /// Stop trusting an issuer for a service
    Rm { service: AuthzService, did: String },
}

#[derive(Subcommand)]
pub enum ChatAction {
    /// tc-chat rooms configured for relay (`chat_relay.rooms`), and
    /// whether each is currently joined
    Rooms,
    /// Recently relayed tc-chat messages for one room
    Log {
        /// tc-chat room id (see `chat_relay.rooms` in settings)
        room: String,
        /// Max number of entries to return (default 50)
        #[arg(short, long)]
        limit: Option<usize>,
    },
}

#[derive(Subcommand)]
pub enum AiAction {
    /// Register AI connections and rooms for another local application
    External {
        #[command(subcommand)]
        action: AiExternalAction,
    },
    /// One-shot chat completion (local provider or first p2p provider)
    Chat {
        /// The user prompt
        prompt: String,
        /// Model to request (default: provider's choice)
        #[arg(short, long)]
        model: Option<String>,
        #[arg(
            long,
            help = "Reasoning effort override / 推論労力の上書き / 覆盖推理强度"
        )]
        reasoning_effort: Option<String>,
    },
    /// Show AI network status (room, provider, API server)
    Status,
    /// List models advertised by the reachable provider
    Models,
    /// Set room sharing flags: start enables enabled rooms with shared models;
    /// stop clears every room's provide flag
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
pub enum AiExternalAction {
    /// Replace this owner's registration with JSON read from stdin
    Apply {
        #[arg(long)]
        owner: String,
    },
    /// Remove this owner's registration
    Remove {
        #[arg(long)]
        owner: String,
    },
    /// Get masked registrations and live room status as JSON
    Get {
        #[arg(long)]
        owner: Option<String>,
    },
}

#[cfg(test)]
mod external_cli_tests {
    use super::*;

    #[test]
    fn external_commands_preserve_global_instance_and_state_dir_options() {
        for action in ["apply", "remove", "get"] {
            let parsed = Cli::try_parse_from([
                "mistl",
                "--instance",
                "npc",
                "--state-dir",
                "state",
                "ai",
                "external",
                action,
                "--owner",
                "tc-npc",
            ])
            .unwrap();
            assert_eq!(parsed.instance.as_deref(), Some("npc"));
            assert_eq!(parsed.state_dir, Some(std::path::PathBuf::from("state")));
            let Some(Command::Ai {
                action:
                    AiAction::External {
                        action: parsed_action,
                    },
            }) = parsed.command
            else {
                panic!("expected external command");
            };
            let owner = match parsed_action {
                AiExternalAction::Apply { owner } | AiExternalAction::Remove { owner } => {
                    Some(owner)
                }
                AiExternalAction::Get { owner } => owner,
            };
            assert_eq!(owner.as_deref(), Some("tc-npc"));
        }
        let parsed = Cli::try_parse_from(["mistl", "ai", "external", "get"]).unwrap();
        assert!(matches!(
            parsed.command,
            Some(Command::Ai {
                action: AiAction::External {
                    action: AiExternalAction::Get { owner: None }
                }
            })
        ));
    }
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
pub enum NostrAction {
    /// Show the relay status
    Status {
        /// Print the full JSON response
        #[arg(long)]
        json: bool,
    },
    /// Start the relay
    Start {
        /// Print the full JSON response
        #[arg(long)]
        json: bool,
    },
    /// Stop the relay
    Stop {
        /// Print the full JSON response
        #[arg(long)]
        json: bool,
    },
    /// Query stored events using one or more Nostr filters
    Query {
        /// Nostr filter as a JSON object (repeatable)
        #[arg(long = "filter")]
        filters: Vec<String>,
        /// Event kind (repeatable)
        #[arg(long = "kind")]
        kinds: Vec<u32>,
        /// Author public key in hexadecimal (repeatable)
        #[arg(long = "author")]
        authors: Vec<String>,
        /// Maximum number of events across all filters
        #[arg(long)]
        limit: Option<usize>,
        /// Print the full JSON response
        #[arg(long)]
        json: bool,
    },
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
    /// Manage automatic decisions for incoming forward proposals
    Rule {
        #[command(subcommand)]
        action: TunnelRuleAction,
    },
    /// Open the interactive terminal UI. Talks to the daemon over the same
    /// IPC commands as every other `tunnel` subcommand (starts the daemon
    /// first if it isn't running yet, same as the rest of this CLI) rather
    /// than running an in-process session.
    Tui,
}

#[derive(Subcommand)]
pub enum TunnelRuleAction {
    /// List proposal rules
    Ls,
    /// Add an auto-allow or auto-deny proposal rule
    Add {
        /// Decision to apply to matching proposals
        #[arg(value_parser = ["allow", "deny"])]
        action: String,
        /// Node id, verified did:key, or * (default: *)
        #[arg(long)]
        peer: Option<String>,
        /// tcp, udp, or * (default: *)
        #[arg(long)]
        proto: Option<String>,
        /// Port, range, comma-separated list, or * (default: *)
        #[arg(long)]
        ports: Option<String>,
        /// loopback, comma-separated host list, or * (default: loopback)
        #[arg(long)]
        hosts: Option<String>,
        /// Optional description
        #[arg(long)]
        note: Option<String>,
    },
    /// Remove a proposal rule by its id
    Rm {
        /// Rule id (see `tunnel rule ls`)
        id: String,
    },
}

/// Dispatch a parsed CLI invocation: either run the daemon, or act as a
/// client sending one request to the running daemon over local IPC.
#[derive(Subcommand)]
pub enum NetworkAction {
    On,
    Off,
    Status,
}

pub fn dispatch(cli: Cli) -> Result<()> {
    let Some(command) = cli.command else {
        // Bare `mistl` (including an Explorer double-click on the exe):
        // bring the whole thing up and land the user in the dashboard.
        return open_dashboard();
    };
    match command {
        Command::BuildInfo => {
            println!(
                "{}",
                serde_json::to_string_pretty(&crate::runtime::build_info())?
            );
            Ok(())
        }
        Command::Network { action } => match action {
            NetworkAction::On => client_call("network.set", json!({"enabled":true})),
            NetworkAction::Off => client_call("network.set", json!({"enabled":false})),
            NetworkAction::Status => client_call("network.status", json!({})),
        },
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
            KeyAction::Export { out, passphrase } => key_export(out, passphrase),
            KeyAction::Import {
                file,
                passphrase,
                force,
                dry_run,
            } => key_import(file, passphrase, force, dry_run),
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
            StoreAction::FolderAccess { action } => {
                let (cmd, args) = match action {
                    StoreFolderAccessAction::Ls => ("store.folder-access.ls", json!({})),
                    StoreFolderAccessAction::Approve { id, remember } => (
                        "store.folder-access.approve",
                        json!({ "id": id, "remember": remember }),
                    ),
                    StoreFolderAccessAction::Deny { id } => {
                        ("store.folder-access.deny", json!({ "id": id }))
                    }
                    StoreFolderAccessAction::Allow {
                        folder_id,
                        requester,
                    } => (
                        "store.folder-access.allow",
                        json!({ "folder_id": folder_id, "requester": requester }),
                    ),
                    StoreFolderAccessAction::Revoke {
                        folder_id,
                        requester,
                    } => (
                        "store.folder-access.revoke",
                        json!({ "folder_id": folder_id, "requester": requester }),
                    ),
                };
                store_call(cmd, args, json, |response| {
                    serde_json::to_string_pretty(response).unwrap_or_default()
                })
            }
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
        Command::Escrow { action } => match action {
            EscrowAction::Status => {
                let response = request("escrow.status", json!({}))?;
                println!("{}", render_escrow_status(&response, false)?);
                Ok(())
            }
            EscrowAction::StatusJson => client_call("escrow.status", json!({})),
            EscrowAction::Slots { room } => {
                let (cmd, args) = escrow_slots_request(room);
                let response = request(cmd, args)?;
                println!("{}", render_escrow_slots(&response));
                Ok(())
            }
            EscrowAction::Export { file, rooms, force } => {
                let path = escrow_export_path(&file)?;
                let rooms = (!rooms.is_empty()).then_some(rooms);
                client_call(
                    "escrow.export",
                    json!({"path": path, "rooms": rooms, "overwrite": force}),
                )
            }
            EscrowAction::Import { file, rooms } => {
                let path = canonicalize_or_die(&file);
                let rooms = (!rooms.is_empty()).then_some(rooms);
                client_call("escrow.import", json!({"path": path, "rooms": rooms}))
            }
            EscrowAction::Membership {
                room,
                voters,
                learners,
            } => client_call(
                "escrow.replication.membership",
                json!({"room":room,"voters":voters,"learners":learners}),
            ),
            EscrowAction::Rooms { action } => match action {
                EscrowRoomsAction::Add { room } => {
                    client_call("escrow.rooms.add", json!({"room":room}))
                }
                EscrowRoomsAction::Remove { room } => {
                    client_call("escrow.rooms.remove", json!({"room":room}))
                }
                EscrowRoomsAction::List => client_call("escrow.rooms.list", json!({})),
            },
        },
        Command::Authz { json, action } => authz_call(action, json),
        Command::Chat { action } => match action {
            ChatAction::Rooms => client_call("chat.rooms", json!({})),
            ChatAction::Log { room, limit } => {
                client_call("chat.log", json!({ "room": room, "limit": limit }))
            }
        },
        Command::Ai { action } => match action {
            AiAction::External { action } => match action {
                AiExternalAction::Apply { owner } => {
                    let value: Value = serde_json::from_reader(std::io::stdin().lock())
                        .context("reading external apply JSON from stdin")?;
                    client_call(
                        "ai.external.apply",
                        crate::ai::external::stdin_payload(&owner, value)?,
                    )
                }
                AiExternalAction::Remove { owner } => {
                    client_call("ai.external.remove", json!({"owner": owner}))
                }
                AiExternalAction::Get { owner } => {
                    client_call("ai.external.get", json!({"owner": owner}))
                }
            },
            AiAction::Chat {
                prompt,
                model,
                reasoning_effort,
            } => client_call(
                "ai.chat",
                json!({ "prompt": prompt, "model": model, "reasoning_effort": reasoning_effort }),
            ),
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
        Command::Nostr { action } => match action {
            NostrAction::Status { json } => {
                let response = request("nostr.status", json!({}))?;
                println!("{}", render_nostr_status(&response, json)?);
                Ok(())
            }
            NostrAction::Start { json } => {
                let response = request("nostr.start", json!({}))?;
                println!("{}", render_nostr_status(&response, json)?);
                Ok(())
            }
            NostrAction::Stop { json } => {
                let response = request("nostr.stop", json!({}))?;
                println!("{}", render_nostr_status(&response, json)?);
                Ok(())
            }
            NostrAction::Query {
                filters,
                kinds,
                authors,
                limit,
                json,
            } => {
                let mut parsed = Vec::new();
                for filter in filters {
                    let value: Value =
                        serde_json::from_str(&filter).context("invalid --filter JSON")?;
                    if !value.is_object() {
                        bail!("--filter must be a JSON object");
                    }
                    parsed.push(value);
                }
                if !kinds.is_empty() || !authors.is_empty() {
                    let mut filter = serde_json::Map::new();
                    if !kinds.is_empty() {
                        filter.insert("kinds".into(), json!(kinds));
                    }
                    if !authors.is_empty() {
                        filter.insert("authors".into(), json!(authors));
                    }
                    parsed.push(Value::Object(filter));
                }
                let mut args = serde_json::Map::new();
                if !parsed.is_empty() {
                    args.insert("filters".into(), Value::Array(parsed));
                }
                if let Some(limit) = limit {
                    args.insert("limit".into(), json!(limit));
                }
                let response = request("nostr.query", Value::Object(args))?;
                let rendered = render_nostr_events(&response, json)?;
                if !rendered.is_empty() {
                    println!("{rendered}");
                }
                Ok(())
            }
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
            TunnelAction::Rule { action } => {
                let is_list = matches!(&action, TunnelRuleAction::Ls);
                let (cmd, args) = tunnel_rule_request(action);
                if is_list {
                    let response = request(cmd, args)?;
                    println!("{}", render_tunnel_rules(&response));
                    Ok(())
                } else {
                    client_call(cmd, args)
                }
            }
            TunnelAction::Tui => crate::tunnel::tui::run(),
        },
        Command::Agent(args) => crate::agent::run(args),
        Command::Ui => open_dashboard(),
        Command::Status => status_overview(),
        Command::Config { action } => match action {
            ConfigAction::Show => client_call("config.show", json!({})),
            ConfigAction::Get { path } => client_call("config.get", json!({"path": path})),
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
                    && applies != "applied immediately"
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
        Command::Install {
            no_autostart,
            no_path,
        } => install_cli(!no_autostart, !no_path),
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
        Command::Path { action } => match action {
            PathAction::Enable => {
                crate::install::set_cli_path(true)?;
                println!("PATH: enabled (open a new terminal to use `mistl` anywhere)");
                Ok(())
            }
            PathAction::Disable => {
                crate::install::set_cli_path(false)?;
                println!("PATH: disabled (takes effect in new terminals)");
                Ok(())
            }
            PathAction::Status => {
                let on = crate::install::cli_path_enabled();
                println!("PATH: {}", if on { "enabled" } else { "disabled" });
                Ok(())
            }
        },
    }
}

/// `mistl install`: copy this exe into the fixed per-user location, add a
/// Start Menu shortcut, and (unless opted out) enable login autostart. Runs
/// in the client process directly -- it must work from a freshly-downloaded
/// exe with no daemon running yet.
fn install_cli(enable_autostart: bool, add_to_path: bool) -> Result<()> {
    let exe = crate::install::install(enable_autostart, add_to_path)?;
    println!("installed: {}", exe.display());
    if enable_autostart {
        println!("autostart: enabled (the mistl daemon starts at login)");
    }
    if crate::install::cli_path_enabled() {
        println!("PATH: enabled (open a new terminal to run `mistl` from anywhere)");
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

fn key_export(out: Option<std::path::PathBuf>, passphrase: Option<String>) -> Result<()> {
    let passphrase = passphrase
        .or_else(|| std::env::var("MISTL_KEY_PASSPHRASE").ok())
        .context("provide --passphrase or set MISTL_KEY_PASSPHRASE")?;
    let response = request("key.export", json!({"passphrase": passphrase}))?;
    let envelope = response
        .get("envelope")
        .context("missing export envelope")?;
    let text = format!("{}\n", serde_json::to_string_pretty(envelope)?);
    if let Some(path) = out {
        let path = std::path::absolute(path).context("resolving export output path")?;
        crate::statefile::write_private(&path, text.as_bytes())
            .with_context(|| format!("writing {}", path.display()))?;
    } else {
        print!("{text}");
    }
    eprintln!(
        "DID: {}",
        response
            .get("did")
            .and_then(Value::as_str)
            .unwrap_or_default()
    );
    Ok(())
}

fn key_import(file: String, passphrase: Option<String>, force: bool, dry_run: bool) -> Result<()> {
    let data = if file == "-" {
        std::io::read_to_string(std::io::stdin()).context("reading identity from stdin")?
    } else {
        std::fs::read_to_string(&file).with_context(|| format!("reading {file}"))?
    };
    let passphrase = passphrase.or_else(|| std::env::var("MISTL_KEY_PASSPHRASE").ok());
    let response = request(
        "key.import",
        json!({
            "data": data, "passphrase": passphrase, "force": force, "dry_run": dry_run,
        }),
    )?;
    println!("{}", render_key_import(&response, dry_run));
    Ok(())
}

fn render_key_import(response: &Value, dry_run: bool) -> String {
    let did = response
        .get("did")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let previous = response.get("previous_did").and_then(Value::as_str);
    let changed = response
        .get("changed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut lines = vec![if !changed {
        format!("Identity unchanged: {did}")
    } else if dry_run {
        format!(
            "Dry run: would import {did} (previous: {}). No files written.",
            previous.unwrap_or("none")
        )
    } else {
        format!(
            "Imported identity: {did} (previous: {})",
            previous.unwrap_or("none")
        )
    }];
    if let Some(backup) = response.get("backup_dir").and_then(Value::as_str) {
        lines.push(format!("Backup: {backup}"));
    }
    if response
        .get("restart_required")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        lines.push("Run `mistl daemon restart` to use the imported identity and node ID.".into());
    }
    lines.join("\n")
}

#[cfg(test)]
mod identity_transfer_cli_tests {
    use super::*;

    #[test]
    fn parses_identity_export_and_import_options() {
        let parsed = Cli::try_parse_from([
            "mistl",
            "key",
            "export",
            "--out",
            "identity.json",
            "--passphrase",
            "test passphrase",
        ])
        .unwrap();
        let Some(Command::Key {
            action: KeyAction::Export { out, passphrase },
        }) = parsed.command
        else {
            panic!("expected key export");
        };
        assert_eq!(out, Some(std::path::PathBuf::from("identity.json")));
        assert_eq!(passphrase.as_deref(), Some("test passphrase"));
        let parsed = Cli::try_parse_from([
            "mistl",
            "key",
            "import",
            "-",
            "--force",
            "--dry-run",
            "--passphrase",
            "test passphrase",
        ])
        .unwrap();
        let Some(Command::Key {
            action:
                KeyAction::Import {
                    file,
                    passphrase,
                    force,
                    dry_run,
                },
        }) = parsed.command
        else {
            panic!("expected key import");
        };
        assert_eq!(file, "-");
        assert_eq!(passphrase.as_deref(), Some("test passphrase"));
        assert!(force && dry_run);
        assert!(Cli::try_parse_from(["mistl", "key", "import"]).is_err());
    }

    #[test]
    fn identity_import_output_reports_backup_restart_and_dry_run() {
        let result = json!({
            "did": "new DID", "previous_did": "old DID", "changed": true,
            "restart_required": true, "backup_dir": "identity/backup/example",
        });
        let output = render_key_import(&result, false);
        assert!(output.contains("new DID") && output.contains("old DID"));
        assert!(output.contains("identity/backup/example"));
        assert!(output.contains("mistl daemon restart"));
        let result = json!({"did": "new DID", "changed": true, "restart_required": false});
        let output = render_key_import(&result, true);
        assert!(output.contains("Dry run") && output.contains("No files written"));
        assert!(!output.contains("restart"));
        let result = json!({"did": "same DID", "changed": false, "restart_required": false});
        assert_eq!(
            render_key_import(&result, false),
            "Identity unchanged: same DID"
        );
    }
}

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

fn parse_authz_ttl(value: &str) -> std::result::Result<u64, String> {
    let (number, multiplier) = match value.as_bytes().last() {
        Some(b's') => (&value[..value.len() - 1], 1),
        Some(b'm') => (&value[..value.len() - 1], 60),
        Some(b'h') => (&value[..value.len() - 1], 60 * 60),
        Some(b'd') => (&value[..value.len() - 1], 24 * 60 * 60),
        _ => return Err("expected a duration such as 30s, 10m, 12h, or 7d".into()),
    };
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err("duration must be a whole number followed by s, m, h, or d".into());
    }
    number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or_else(|| "duration is too large".into())
}

fn parse_authz_size(value: &str) -> std::result::Result<u64, String> {
    let (number, multiplier) = if let Some(n) = value.strip_suffix("KiB") {
        (n, 1024_u64)
    } else if let Some(n) = value.strip_suffix("MiB") {
        (n, 1024_u64.pow(2))
    } else if let Some(n) = value.strip_suffix("GiB") {
        (n, 1024_u64.pow(3))
    } else {
        (value, 1)
    };
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err("expected whole bytes or a KiB, MiB, or GiB suffix".into());
    }
    number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .ok_or_else(|| "size is too large".into())
}

fn parse_authz_expiry(value: &str) -> std::result::Result<String, String> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|_| value.to_owned())
        .map_err(|_| "expected an RFC3339 timestamp".into())
}

impl AuthzLimitArgs {
    fn any(&self) -> bool {
        self.period.is_some() || self.max_requests.is_some() || self.max_output_chars.is_some()
    }

    fn apply(&self, limits: &mut Value) -> Result<()> {
        let fields = limits
            .as_object_mut()
            .context("invalid limits in daemon response")?;
        if let Some(period) = self.period {
            fields.insert("period".into(), json!(period.as_str()));
        }
        if let Some(max) = self.max_requests {
            fields.insert("max_requests".into(), json!(max));
        }
        if let Some(max) = self.max_output_chars {
            fields.insert("max_output_chars".into(), json!(max));
        }
        Ok(())
    }
}

enum AuthzOutput {
    Show,
    Policy,
    Usage(Option<AuthzService>),
    Reset,
    Tokens,
    Token,
    Deleted,
    Secret,
    Peers,
    CapIssued,
    CapImported,
    Caps,
    CapRevoked,
    CapRemoved,
    CapVerified,
}

fn read_authz_cap(source: &str, stdin: &mut impl std::io::Read) -> Result<Value> {
    let text = if source == "-" {
        std::io::read_to_string(stdin).context("reading capability from stdin")?
    } else if source.trim_start().starts_with('{') {
        source.to_owned()
    } else {
        std::fs::read_to_string(source).with_context(|| format!("reading {source}"))?
    };
    serde_json::from_str(&text).context("invalid capability JSON")
}

fn authz_rule_request(allow: bool, action: AuthzRuleAction) -> (&'static str, Value, AuthzOutput) {
    match action {
        AuthzRuleAction::Add {
            service,
            subject,
            ttl,
            expires,
            note,
        } => (
            if allow {
                "authz.allow.add"
            } else {
                "authz.deny.add"
            },
            json!({"service": service.as_str(), "subject": subject, "ttl_secs": ttl,
                   "expires_at": expires, "note": note}),
            AuthzOutput::Policy,
        ),
        AuthzRuleAction::Rm { service, subject } => (
            if allow {
                "authz.allow.remove"
            } else {
                "authz.deny.remove"
            },
            json!({"service": service.as_str(), "subject": subject}),
            AuthzOutput::Policy,
        ),
    }
}

fn authz_request(
    action: AuthzAction,
    get: impl FnMut(&str, Value) -> Result<Value>,
) -> Result<(&'static str, Value, AuthzOutput)> {
    authz_request_with_input(action, get, std::io::stdin().lock())
}

fn authz_request_with_input(
    action: AuthzAction,
    mut get: impl FnMut(&str, Value) -> Result<Value>,
    mut stdin: impl std::io::Read,
) -> Result<(&'static str, Value, AuthzOutput)> {
    Ok(match action {
        AuthzAction::Show => ("authz.show", json!({}), AuthzOutput::Show),
        AuthzAction::Mode { service, mode } => (
            "authz.mode.set",
            json!({"service": service.as_str(), "mode": mode.as_str()}),
            AuthzOutput::Policy,
        ),
        AuthzAction::Allow { action } => authz_rule_request(true, action),
        AuthzAction::Deny { action } => authz_rule_request(false, action),
        AuthzAction::Limits {
            service,
            subject,
            limits,
            max_stored,
            clear,
        } => {
            let value = if clear {
                if subject.is_some() {
                    Value::Null
                } else {
                    json!({})
                }
            } else {
                let current = get("authz.show", json!({}))?;
                let policy = current
                    .get("policy")
                    .and_then(|p| p.get(service.as_str()))
                    .context("missing service policy in daemon response")?;
                let default = policy
                    .get("limits")
                    .context("missing default limits in daemon response")?;
                let mut value = subject
                    .as_ref()
                    .and_then(|subject| {
                        policy
                            .get("overrides")
                            .and_then(Value::as_array)
                            .and_then(|entries| {
                                entries.iter().find(|entry| {
                                    entry.get("subject").and_then(Value::as_str)
                                        == Some(subject.as_str())
                                })
                            })
                            .and_then(|entry| entry.get("limits"))
                    })
                    .unwrap_or(default)
                    .clone();
                limits.apply(&mut value)?;
                if let Some(max) = max_stored {
                    value["max_stored_bytes"] = json!(max);
                }
                value
            };
            (
                "authz.limits.set",
                json!({"service": service.as_str(), "subject": subject, "limits": value}),
                AuthzOutput::Policy,
            )
        }
        AuthzAction::Usage {
            service,
            action: None,
        } => (
            "authz.usage",
            json!({"service": service.map(AuthzService::as_str)}),
            AuthzOutput::Usage(service),
        ),
        AuthzAction::Usage {
            action: Some(AuthzUsageAction::Reset { service, account }),
            ..
        } => (
            "authz.usage.reset",
            json!({"service": service.as_str(), "account": account}),
            AuthzOutput::Reset,
        ),
        AuthzAction::Api {
            action: AuthzApiAction::RequireToken { value },
        } => (
            "authz.api.set",
            json!({"require_token": matches!(value, AuthzSwitch::On)}),
            AuthzOutput::Policy,
        ),
        AuthzAction::Token { action } => match action {
            AuthzTokenAction::Create {
                name,
                models,
                limits,
                ttl,
                expires,
            } => {
                let mut value = json!({});
                limits.apply(&mut value)?;
                (
                    "authz.token.create",
                    json!({"name": name, "models": models, "limits": value,
                    "ttl_secs": ttl, "expires_at": expires}),
                    AuthzOutput::Secret,
                )
            }
            AuthzTokenAction::List => ("authz.token.list", json!({}), AuthzOutput::Tokens),
            AuthzTokenAction::Update {
                id,
                name,
                models,
                clear_models,
                limits,
                expires,
                clear_expiry,
            } => {
                let mut args = json!({"id": id});
                if let Some(name) = name {
                    args["name"] = json!(name);
                }
                if !models.is_empty() || clear_models {
                    args["models"] = json!(models);
                }
                if limits.any() {
                    let tokens = get("authz.token.list", json!({}))?;
                    let token = tokens
                        .as_array()
                        .context("invalid token list in daemon response")?
                        .iter()
                        .find(|token| token.get("id").and_then(Value::as_str) == Some(id.as_str()))
                        .context("unknown API token id")?;
                    let mut value = token
                        .get("limits")
                        .context("missing token limits in daemon response")?
                        .clone();
                    limits.apply(&mut value)?;
                    args["limits"] = value;
                }
                if let Some(expires) = expires {
                    args["expires_at"] = json!(expires);
                }
                if clear_expiry {
                    args["expires_at"] = Value::Null;
                }
                ("authz.token.update", args, AuthzOutput::Token)
            }
            AuthzTokenAction::Revoke { id } => {
                ("authz.token.revoke", json!({"id": id}), AuthzOutput::Token)
            }
            AuthzTokenAction::Delete { id } => (
                "authz.token.delete",
                json!({"id": id}),
                AuthzOutput::Deleted,
            ),
        },
        AuthzAction::Cap { action } => match action {
            AuthzCapAction::Issue {
                aud_did,
                service,
                mut actions,
                resource,
                ttl,
                expires,
                to,
                limits,
                parent,
                deliver,
            } => {
                if actions.is_empty() {
                    actions.push(match service {
                        AuthzService::Ai => AuthzCapabilityAction::Infer,
                        AuthzService::Escrow => AuthzCapabilityAction::Read,
                    });
                }
                let resource = match (service, resource) {
                    (_, Some(resource)) => resource,
                    (AuthzService::Ai, None) => "*".into(),
                    (AuthzService::Escrow, None) => bail!("escrow requires --resource"),
                };
                let mut args = json!({
                    "aud": aud_did,
                    "service": service.as_str(),
                    "actions": actions.iter().map(|action| action.as_str()).collect::<Vec<_>>(),
                    "resource": resource,
                    "deliver": deliver,
                });
                if let Some(ttl) = ttl {
                    args["ttl_secs"] = json!(ttl);
                }
                if let Some(expires) = expires {
                    args["expires_at"] = json!(expires);
                }
                if !to.is_empty() {
                    args["to"] = json!(to);
                }
                if limits.any() {
                    let mut value = json!({});
                    limits.apply(&mut value)?;
                    args["limits"] = value;
                }
                if let Some(parent) = parent {
                    args["parent"] = json!(parent);
                }
                ("authz.cap.issue", args, AuthzOutput::CapIssued)
            }
            AuthzCapAction::Import { source } => (
                "authz.cap.import",
                json!({"cap": read_authz_cap(&source, &mut stdin)?}),
                AuthzOutput::CapImported,
            ),
            AuthzCapAction::List => ("authz.cap.list", json!({}), AuthzOutput::Caps),
            AuthzCapAction::Revoke { id, local } => (
                "authz.cap.revoke",
                json!({"id": id, "local": local}),
                AuthzOutput::CapRevoked,
            ),
            AuthzCapAction::Remove { id } => (
                "authz.cap.remove",
                json!({"id": id}),
                AuthzOutput::CapRemoved,
            ),
            AuthzCapAction::Verify {
                source,
                service,
                action,
                resource,
            } => (
                "authz.cap.verify",
                json!({"cap": read_authz_cap(&source, &mut stdin)?,
                       "service": service.map(AuthzService::as_str),
                       "action": action.map(AuthzCapabilityAction::as_str),
                       "resource": resource}),
                AuthzOutput::CapVerified,
            ),
        },
        AuthzAction::Issuer { action } => match action {
            AuthzIssuerAction::Add { service, did } => (
                "authz.issuer.add",
                json!({"service": service.as_str(), "did": did}),
                AuthzOutput::Policy,
            ),
            AuthzIssuerAction::Rm { service, did } => (
                "authz.issuer.remove",
                json!({"service": service.as_str(), "did": did}),
                AuthzOutput::Policy,
            ),
        },
        AuthzAction::Peers => ("authz.peers", json!({}), AuthzOutput::Peers),
    })
}

fn authz_call(action: AuthzAction, json_output: bool) -> Result<()> {
    let (cmd, args, output) = authz_request(action, request)?;
    let response = request(cmd, args)?;
    if matches!(output, AuthzOutput::Secret) {
        let token = response
            .get("token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .context("token missing from daemon response")?;
        eprintln!("Store this token now; it will not be shown again.");
        println!("{token}");
    } else if matches!(output, AuthzOutput::CapIssued) {
        let cap = response
            .get("cap")
            .filter(|cap| cap.is_object())
            .context("capability missing from daemon response")?;
        let delivered = response["delivered"]
            .as_bool()
            .context("delivery status missing from daemon response")?;
        eprintln!("Issued: {}", authz_cap_summary(cap));
        eprintln!("delivered: {}", if delivered { "yes" } else { "no" });
        println!("{}", serde_json::to_string(cap)?);
    } else if json_output {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        let rendered = match output {
            AuthzOutput::Show => render_authz_show(&response),
            AuthzOutput::Policy => render_authz_policy(&response),
            AuthzOutput::Usage(service) => render_authz_usage(&response, service),
            AuthzOutput::Reset => format!(
                "Reset {} usage account(s).",
                response["reset"].as_u64().unwrap_or(0)
            ),
            AuthzOutput::Tokens => render_authz_tokens(&response),
            AuthzOutput::Token => render_authz_tokens(&json!([response])),
            AuthzOutput::Deleted => "Token deleted.".to_string(),
            AuthzOutput::Peers => render_authz_peers(&response),
            AuthzOutput::CapImported => format!("Imported: {}", authz_cap_summary(&response)),
            AuthzOutput::Caps => render_authz_caps(&response),
            AuthzOutput::CapRevoked => format!(
                "Revoked: {}\nBroadcast: {}",
                authz_yes_no(&response["revoked"]),
                authz_yes_no(&response["broadcast"])
            ),
            AuthzOutput::CapRemoved => {
                format!("Removed: {}", authz_yes_no(&response["removed"]))
            }
            AuthzOutput::CapVerified => render_authz_cap_verification(&response),
            AuthzOutput::Secret | AuthzOutput::CapIssued => unreachable!(),
        };
        println!("{rendered}");
    }
    Ok(())
}

fn authz_text(value: &Value, key: &str) -> String {
    let mut result = String::new();
    for c in value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("-")
        .chars()
    {
        if c.is_control() {
            result.extend(c.escape_debug());
        } else {
            result.push(c);
        }
    }
    result
}

fn authz_subject(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    if value.starts_with("did:key:") && chars.len() > 20 {
        format!(
            "{}…{}",
            chars[..12].iter().collect::<String>(),
            chars[chars.len() - 4..].iter().collect::<String>()
        )
    } else {
        value.to_string()
    }
}

fn authz_number(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_u64)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "unlimited".into())
}

fn authz_size(bytes: u64) -> String {
    for (unit, scale) in [
        ("GiB", 1024_u64.pow(3)),
        ("MiB", 1024_u64.pow(2)),
        ("KiB", 1024_u64),
    ] {
        if bytes >= scale {
            return if bytes.is_multiple_of(scale) {
                format!("{} {unit}", bytes / scale)
            } else {
                format!("{:.1} {unit}", bytes as f64 / scale as f64)
            };
        }
    }
    format!("{bytes} B")
}

fn authz_size_limit(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_u64)
        .map(authz_size)
        .unwrap_or_else(|| "unlimited".into())
}

fn authz_limits_text(limits: &Value) -> String {
    format!(
        "period={} requests={} chars={} stored={}",
        authz_text(limits, "period"),
        authz_number(limits.get("max_requests")),
        authz_number(limits.get("max_output_chars")),
        authz_size_limit(limits.get("max_stored_bytes"))
    )
}

fn render_authz_policy(policy: &Value) -> String {
    let mut lines = vec![format!(
        "API token required: {}",
        if policy["api"]["require_token"].as_bool().unwrap_or(false) {
            "on"
        } else {
            "off"
        }
    )];
    for service in ["ai", "escrow"] {
        let section = &policy[service];
        lines.push(format!(
            "\n{} mode: {}",
            service.to_ascii_uppercase(),
            authz_text(section, "mode")
        ));
        lines.push(format!(
            "  default: {}",
            authz_limits_text(&section["limits"])
        ));
        lines.push(format!(
            "  {:<23}  {:<6}  {:<25}  {}",
            "SUBJECT", "RULE", "EXPIRES", "NOTE"
        ));
        let mut count = 0;
        for kind in ["allow", "deny"] {
            if let Some(rules) = section[kind].as_array() {
                for rule in rules {
                    lines.push(format!(
                        "  {:<23}  {:<6}  {:<25}  {}",
                        authz_subject(&authz_text(rule, "subject")),
                        kind,
                        authz_text(rule, "expires_at"),
                        authz_text(rule, "note")
                    ));
                    count += 1;
                }
            }
        }
        if count == 0 {
            lines.push("  (no rules)".into());
        }
        if let Some(overrides) = section["overrides"]
            .as_array()
            .filter(|items| !items.is_empty())
        {
            lines.push("  limit overrides:".into());
            for entry in overrides {
                lines.push(format!(
                    "    {:<23}  {}",
                    authz_subject(&authz_text(entry, "subject")),
                    authz_limits_text(&entry["limits"])
                ));
            }
        }
        if let Some(issuers) = section["issuers"]
            .as_array()
            .filter(|items| !items.is_empty())
        {
            lines.push("  trusted issuers:".into());
            for issuer in issuers {
                if let Some(did) = issuer.as_str() {
                    lines.push(format!("    {}", did.escape_debug()));
                }
            }
        }
    }
    lines.join("\n")
}

fn render_authz_show(response: &Value) -> String {
    let mut lines = Vec::new();
    if let Some(local) = response.get("local") {
        lines.push(format!(
            "Local: {}  {}",
            authz_text(local, "node"),
            authz_subject(&authz_text(local, "did"))
        ));
    }
    lines.push(render_authz_policy(&response["policy"]));
    lines.push(format!("\n{}", render_authz_tokens(&response["tokens"])));
    lines.join("\n")
}

fn render_authz_usage(response: &Value, service: Option<AuthzService>) -> String {
    let mut lines = Vec::new();
    if service != Some(AuthzService::Escrow) {
        lines.push(format!(
            "AI usage\n{:<23}  {:<23}  {:<6}  {:<21}  {}",
            "ACCOUNT", "DID", "PERIOD", "REQUESTS/LIMIT", "CHARS/LIMIT"
        ));
        if let Some(rows) = response["ai"].as_array() {
            for row in rows {
                let limits = &row["limits"];
                let requests = format!(
                    "{}/{}",
                    row["requests"].as_u64().unwrap_or(0),
                    authz_number(limits.get("max_requests"))
                );
                let chars = format!(
                    "{}/{}",
                    row["output_chars"].as_u64().unwrap_or(0),
                    authz_number(limits.get("max_output_chars"))
                );
                lines.push(format!(
                    "{:<23}  {:<23}  {:<6}  {:<21}  {}",
                    authz_text(row, "account"),
                    authz_subject(&authz_text(row, "did")),
                    authz_text(row, "period"),
                    requests,
                    chars
                ));
            }
        }
    }
    if service != Some(AuthzService::Ai) {
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(format!(
            "Escrow usage\n{:<16}  {:<23}  {}",
            "NODE", "DID", "STORED/LIMIT"
        ));
        if let Some(rows) = response["escrow"].as_array() {
            for row in rows {
                lines.push(format!(
                    "{:<16}  {:<23}  {}/{}",
                    authz_text(row, "node"),
                    authz_subject(&authz_text(row, "did")),
                    authz_size(row["stored_bytes"].as_u64().unwrap_or(0)),
                    authz_size_limit(row.get("limit"))
                ));
            }
        }
    }
    lines.join("\n")
}

fn render_authz_tokens(response: &Value) -> String {
    let mut lines = vec![format!(
        "Tokens\n{:<12}  {:<16}  {:<10}  {:<25}  {}",
        "ID", "NAME", "STATUS", "EXPIRES", "MODELS"
    )];
    if let Some(tokens) = response.as_array() {
        for token in tokens {
            let status = if !token["revoked_at"].is_null() {
                "revoked"
            } else if token["expires_at"]
                .as_str()
                .and_then(|date| chrono::DateTime::parse_from_rfc3339(date).ok())
                .is_some_and(|date| date <= chrono::Utc::now())
            {
                "expired"
            } else {
                "active"
            };
            let models = token["models"]
                .as_array()
                .map(|models| {
                    models
                        .iter()
                        .filter_map(Value::as_str)
                        .map(|model| {
                            model
                                .chars()
                                .flat_map(char::escape_debug)
                                .collect::<String>()
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .filter(|models| !models.is_empty())
                .unwrap_or_else(|| "all".into());
            lines.push(format!(
                "{:<12}  {:<16}  {:<10}  {:<25}  {}",
                authz_text(token, "id"),
                authz_text(token, "name"),
                status,
                authz_text(token, "expires_at"),
                models
            ));
            lines.push(format!(
                "  {}  {}",
                authz_text(token, "hint"),
                authz_limits_text(&token["limits"])
            ));
        }
    }
    lines.join("\n")
}

fn render_authz_peers(response: &Value) -> String {
    let mut lines = vec![format!("{:<16}  {:<23}  {}", "NODE", "DID", "ROOM")];
    if let Some(peers) = response.as_array() {
        for peer in peers {
            lines.push(format!(
                "{:<16}  {:<23}  {}",
                authz_text(peer, "node"),
                authz_subject(&authz_text(peer, "did")),
                authz_text(peer, "room")
            ));
        }
    }
    lines.join("\n")
}

fn authz_yes_no(value: &Value) -> &'static str {
    match value.as_bool() {
        Some(true) => "yes",
        Some(false) => "no",
        None => "unknown",
    }
}

fn authz_cap_time(value: &Value, key: &str) -> String {
    value[key]
        .as_i64()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|date| date.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "-".into())
}

fn authz_cap_actions(cap: &Value) -> String {
    cap["act"]
        .as_array()
        .map(|actions| {
            actions
                .iter()
                .filter_map(Value::as_str)
                .map(|action| action.escape_debug().to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|actions| !actions.is_empty())
        .unwrap_or_else(|| "-".into())
}

fn authz_cap_summary(cap: &Value) -> String {
    format!(
        "id={} audience={} service={} action={} resource={} expires={}",
        authz_text(cap, "id"),
        authz_text(cap, "aud"),
        authz_text(cap, "svc"),
        authz_cap_actions(cap),
        authz_text(cap, "res"),
        authz_cap_time(cap, "exp")
    )
}

fn render_authz_caps(response: &Value) -> String {
    let mut lines = Vec::new();
    for (key, title) in [
        ("issued", "Issued"),
        ("held", "Held"),
        ("presented", "Presented"),
    ] {
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(title.into());
        lines.push(format!(
            "{:<32}  {:<6}  {:<10}  {:<16}  {:<23}  {:<23}  {:<20}  {}",
            "ID", "SERVICE", "ACTIONS", "RESOURCE", "HOLDER", "ISSUER", "EXPIRES", "STATUS/NODE"
        ));
        let rows = response[key].as_array();
        if rows.is_none_or(|rows| rows.is_empty()) {
            lines.push("(none)".into());
        }
        for row in rows.into_iter().flatten() {
            let cap = if key == "held" { row } else { &row["cap"] };
            let status = match key {
                "issued" if row["revoked"] == true => "revoked".to_string(),
                "issued" => "active".to_string(),
                "presented" => authz_text(row, "node"),
                _ => "-".to_string(),
            };
            lines.push(format!(
                "{:<32}  {:<6}  {:<10}  {:<16}  {:<23}  {:<23}  {:<20}  {}",
                authz_text(cap, "id"),
                authz_text(cap, "svc"),
                authz_cap_actions(cap),
                authz_text(cap, "res"),
                authz_subject(&authz_text(cap, "aud")),
                authz_subject(&authz_text(cap, "fromId")),
                authz_cap_time(cap, "exp"),
                status
            ));
        }
    }
    lines.push(String::new());
    lines.push("Revoked".into());
    lines.push(format!("{:<32}  {:<20}  {}", "ID", "REVOKED AT", "EXPIRES"));
    let revoked = response["revoked"].as_array();
    if revoked.is_none_or(|rows| rows.is_empty()) {
        lines.push("(none)".into());
    }
    for row in revoked.into_iter().flatten() {
        lines.push(format!(
            "{:<32}  {:<20}  {}",
            authz_text(row, "id"),
            authz_cap_time(row, "at"),
            authz_cap_time(row, "exp")
        ));
    }
    lines.join("\n")
}

fn render_authz_cap_verification(response: &Value) -> String {
    let mut lines = vec![format!("Valid: {}", authz_yes_no(&response["ok"]))];
    if !response["reason"].is_null() {
        lines.push(format!("Reason: {}", authz_text(response, "reason")));
    }
    lines.push(format!("Root: {}", authz_text(response, "root")));
    let chain = response["chain"]
        .as_array()
        .map(|chain| {
            chain
                .iter()
                .filter_map(Value::as_str)
                .map(|id| id.escape_debug().to_string())
                .collect::<Vec<_>>()
                .join(" -> ")
        })
        .filter(|chain| !chain.is_empty())
        .unwrap_or_else(|| "-".into());
    lines.push(format!("Chain: {chain}"));
    lines.join("\n")
}

#[cfg(test)]
mod authz_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> (bool, AuthzAction) {
        let mut argv = vec!["mistl", "authz"];
        argv.extend_from_slice(args);
        match Cli::try_parse_from(argv).unwrap().command.unwrap() {
            Command::Authz { json, action } => (json, action),
            _ => panic!("expected authz command"),
        }
    }

    fn request_without_read(action: AuthzAction) -> (&'static str, Value, AuthzOutput) {
        authz_request(action, |_, _| panic!("unexpected read")).unwrap()
    }

    #[test]
    fn parses_cap_issue_with_limits_verifiers_and_delivery() {
        let (_, action) = parse(&[
            "cap",
            "issue",
            "did:key:holder",
            "escrow",
            "--action",
            "read",
            "--action",
            "write",
            "--resource",
            "room/*",
            "--ttl",
            "7d",
            "--to",
            "did:key:first",
            "--to",
            "did:key:second",
            "--period",
            "month",
            "--max-requests",
            "100",
            "--max-output-chars",
            "20000",
            "--parent",
            "parent-id",
            "--deliver",
        ]);
        let (cmd, args, output) = request_without_read(action);
        assert_eq!(cmd, "authz.cap.issue");
        assert!(matches!(output, AuthzOutput::CapIssued));
        assert_eq!(
            args,
            json!({"aud":"did:key:holder", "service":"escrow", "actions":["read","write"],
                "resource":"room/*", "ttl_secs":604800, "to":["did:key:first","did:key:second"],
                "limits":{"period":"month","max_requests":100,"max_output_chars":20000},
                "parent":"parent-id", "deliver":true})
        );

        let (_, action) = parse(&["cap", "issue", "did:key:holder", "ai"]);
        assert_eq!(
            request_without_read(action).1,
            json!({"aud":"did:key:holder", "service":"ai", "actions":["infer"],
                "resource":"*", "deliver":false})
        );
        let (_, action) = parse(&[
            "cap",
            "issue",
            "did:key:holder",
            "escrow",
            "--resource",
            "*",
        ]);
        assert_eq!(request_without_read(action).1["actions"], json!(["read"]));
        assert!(
            Cli::try_parse_from(["mistl", "authz", "cap", "issue", "did:key:holder", "escrow"])
                .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "mistl",
                "authz",
                "cap",
                "issue",
                "did:key:holder",
                "ai",
                "--ttl",
                "7d",
                "--expires",
                "2030-01-01T00:00:00Z"
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_cap_import_from_stdin_and_verify_json() {
        let (_, action) = parse(&["cap", "import", "-"]);
        let (cmd, args, output) = authz_request_with_input(
            action,
            |_, _| panic!("unexpected daemon read"),
            std::io::Cursor::new(b"{\"id\":\"from-stdin\"}"),
        )
        .unwrap();
        assert_eq!(cmd, "authz.cap.import");
        assert!(matches!(output, AuthzOutput::CapImported));
        assert_eq!(args, json!({"cap":{"id":"from-stdin"}}));

        let (_, action) = parse(&[
            "cap",
            "verify",
            " {\"id\":\"inline\"}",
            "--service",
            "escrow",
            "--action",
            "read",
            "--resource",
            "room/slot",
        ]);
        let (cmd, args, output) = request_without_read(action);
        assert_eq!(cmd, "authz.cap.verify");
        assert!(matches!(output, AuthzOutput::CapVerified));
        assert_eq!(
            args,
            json!({"cap":{"id":"inline"}, "service":"escrow", "action":"read", "resource":"room/slot"})
        );
    }

    #[test]
    fn parses_cap_management_and_issuer_commands() {
        for (argv, expected, args) in [
            (vec!["cap", "list"], "authz.cap.list", json!({})),
            (
                vec!["cap", "remove", "id"],
                "authz.cap.remove",
                json!({"id":"id"}),
            ),
            (
                vec!["cap", "revoke", "id", "--local"],
                "authz.cap.revoke",
                json!({"id":"id", "local":true}),
            ),
            (
                vec!["issuer", "add", "ai", "did:key:issuer"],
                "authz.issuer.add",
                json!({"service":"ai", "did":"did:key:issuer"}),
            ),
            (
                vec!["issuer", "rm", "escrow", "did:key:issuer"],
                "authz.issuer.remove",
                json!({"service":"escrow", "did":"did:key:issuer"}),
            ),
        ] {
            let (_, action) = parse(&argv);
            let (cmd, actual, _) = request_without_read(action);
            assert_eq!(cmd, expected);
            assert_eq!(actual, args);
        }
    }

    #[test]
    fn renders_capability_lists_and_verification() {
        let cap = json!({"id":"0123456789abcdef0123456789abcdef", "aud":"did:key:holder",
            "fromId":"did:key:issuer", "svc":"ai", "act":["infer"], "res":"*", "exp":1893456000000_i64});
        let output = render_authz_caps(&json!({"issued":[{"cap":cap, "revoked":true}],
            "held":[cap], "presented":[{"node":"1234567890abcdef", "cap":cap}],
            "revoked":[{"id":cap["id"], "at":1893450000000_i64, "exp":1893456000000_i64}]}));
        for text in [
            "Issued",
            "Held",
            "Presented",
            "Revoked",
            "1234567890abcdef",
            "revoked",
            "2030-01-01T00:00:00Z",
        ] {
            assert!(output.contains(text), "missing {text} in {output}");
        }
        let output = render_authz_cap_verification(
            &json!({"ok":false, "reason":"untrusted capability root", "root":"did:key:issuer", "chain":["child", "root"]}),
        );
        assert!(output.contains("Valid: no"));
        assert!(output.contains("untrusted capability root"));
        assert!(output.contains("child -> root"));
    }

    #[test]
    fn parses_allow_add_ttl_and_rule_commands() {
        let (_, action) = parse(&[
            "allow",
            "add",
            "ai",
            "0123456789abcdef",
            "--ttl",
            "7d",
            "--note",
            "friend",
        ]);
        let (cmd, args, _) = request_without_read(action);
        assert_eq!(cmd, "authz.allow.add");
        assert_eq!(args["ttl_secs"], 604800);
        assert_eq!(args["service"], "ai");
        assert_eq!(args["subject"], "0123456789abcdef");
        assert_eq!(args["note"], "friend");
        let (_, action) = parse(&["deny", "rm", "escrow", "0123456789abcdef"]);
        assert_eq!(request_without_read(action).0, "authz.deny.remove");
        assert!(
            Cli::try_parse_from([
                "mistl",
                "authz",
                "allow",
                "add",
                "ai",
                "a",
                "--ttl",
                "7d",
                "--expires",
                "2030-01-01T00:00:00Z"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "mistl",
                "authz",
                "allow",
                "add",
                "ai",
                "a",
                "--expires",
                "tomorrow"
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_sizes_and_merges_limits_without_dropping_other_fields() {
        let (_, action) = parse(&[
            "limits",
            "escrow",
            "--subject",
            "0123456789abcdef",
            "--period",
            "month",
            "--max-stored",
            "1GiB",
        ]);
        let (cmd, args, _) = authz_request(action, |cmd, _| {
            assert_eq!(cmd, "authz.show");
            Ok(json!({"policy":{"escrow":{"limits":{"period":"day", "max_requests":12, "max_output_chars":42, "max_stored_bytes":null},"overrides":[]}}}))
        }).unwrap();
        assert_eq!(cmd, "authz.limits.set");
        assert_eq!(
            args["limits"],
            json!({"period":"month", "max_requests":12, "max_output_chars":42, "max_stored_bytes":1073741824_u64})
        );
        let (_, action) = parse(&["limits", "ai", "--subject", "0123456789abcdef", "--clear"]);
        let (_, args, _) = request_without_read(action);
        assert!(args["limits"].is_null());
        let (_, action) = parse(&["limits", "ai", "--clear"]);
        assert_eq!(request_without_read(action).1["limits"], json!({}));
        assert!(
            Cli::try_parse_from([
                "mistl",
                "authz",
                "limits",
                "ai",
                "--clear",
                "--max-requests",
                "1"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "mistl",
                "authz",
                "limits",
                "escrow",
                "--max-stored",
                "18446744073709551615GiB"
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_token_create_models_and_update_preserves_limits() {
        let (json, action) = parse(&[
            "token",
            "create",
            "app",
            "--model",
            "first",
            "--model",
            "second",
            "--period",
            "day",
            "--max-requests",
            "100",
            "--ttl",
            "30d",
            "--json",
        ]);
        assert!(json);
        let (cmd, args, output) = request_without_read(action);
        assert_eq!(cmd, "authz.token.create");
        assert!(matches!(output, AuthzOutput::Secret));
        assert_eq!(args["models"], json!(["first", "second"]));
        assert_eq!(args["limits"], json!({"period":"day", "max_requests":100}));
        assert_eq!(args["ttl_secs"], 2_592_000);
        let (_, action) = parse(&[
            "token",
            "update",
            "abc",
            "--max-requests",
            "9",
            "--clear-models",
            "--clear-expiry",
        ]);
        let (cmd, args, _) = authz_request(action, |cmd, _| {
            assert_eq!(cmd, "authz.token.list");
            Ok(json!([{"id":"abc", "limits":{"period":"month", "max_requests":3, "max_output_chars":45, "max_stored_bytes":null}}]))
        }).unwrap();
        assert_eq!(cmd, "authz.token.update");
        assert_eq!(args["models"], json!([]));
        assert_eq!(args["expires_at"], Value::Null);
        assert_eq!(
            args["limits"],
            json!({"period":"month", "max_requests":9, "max_output_chars":45, "max_stored_bytes":null})
        );
        let (_, action) = parse(&["token", "update", "abc", "--name", "new"]);
        let (_, args, _) = request_without_read(action);
        assert_eq!(args, json!({"id":"abc", "name":"new"}));
        assert!(
            Cli::try_parse_from([
                "mistl",
                "authz",
                "token",
                "update",
                "abc",
                "--model",
                "m",
                "--clear-models"
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_usage_reset_modes_tokens_and_peers() {
        for (argv, expected) in [
            (vec!["show"], "authz.show"),
            (vec!["mode", "ai", "allowlist"], "authz.mode.set"),
            (vec!["usage"], "authz.usage"),
            (vec!["usage", "ai"], "authz.usage"),
            (
                vec!["usage", "reset", "ai", "--account", "node:123"],
                "authz.usage.reset",
            ),
            (vec!["api", "require-token", "on"], "authz.api.set"),
            (vec!["token", "list"], "authz.token.list"),
            (vec!["token", "revoke", "id"], "authz.token.revoke"),
            (vec!["token", "delete", "id"], "authz.token.delete"),
            (vec!["peers"], "authz.peers"),
        ] {
            let (_, action) = parse(&argv);
            assert_eq!(request_without_read(action).0, expected);
        }
        let (_, action) = parse(&["usage", "reset", "escrow", "--account", "node:123"]);
        assert_eq!(
            request_without_read(action).1,
            json!({"service":"escrow", "account":"node:123"})
        );
        let (json, action) = parse(&["usage", "reset", "ai", "--json"]);
        assert!(json);
        assert_eq!(request_without_read(action).0, "authz.usage.reset");
        let (_, action) = parse(&["api", "require-token", "off"]);
        assert_eq!(
            request_without_read(action).1,
            json!({"require_token":false})
        );
        assert!(Cli::try_parse_from(["mistl", "authz", "usage", "ai", "reset", "escrow"]).is_err());
    }

    #[test]
    fn durations_and_sizes_are_checked_and_usage_is_readable() {
        assert_eq!(parse_authz_ttl("30s").unwrap(), 30);
        assert_eq!(parse_authz_ttl("10m").unwrap(), 600);
        assert_eq!(parse_authz_ttl("12h").unwrap(), 43200);
        assert!(parse_authz_ttl("2w").is_err());
        assert!(parse_authz_ttl("18446744073709551615d").is_err());
        assert_eq!(parse_authz_size("42").unwrap(), 42);
        assert_eq!(parse_authz_size("2KiB").unwrap(), 2048);
        assert_eq!(parse_authz_size("3MiB").unwrap(), 3 * 1024 * 1024);
        assert!(parse_authz_size("-1").is_err());
        let did = "did:key:z6Mkxxxxxxxxxxxxxxxxxxxxabcd";
        let output = render_authz_usage(
            &json!({"ai":[{"account":"node:123", "did":did, "period":"day", "requests":2, "output_chars":50, "limits":{"max_requests":10,"max_output_chars":100}}], "escrow":[{"node":"123", "did":did, "stored_bytes":1024, "limit":2147483648_u64}]}),
            None,
        );
        assert!(output.contains("did:key:z6Mk…abcd"));
        assert!(output.contains("2/10") && output.contains("50/100"));
        assert!(output.contains("1 KiB/2 GiB"));
        let policy = json!({"api":{"require_token":true},"ai":{"mode":"allowlist", "limits":{"period":"day"},"allow":[{"subject":did,"expires_at":"2030-01-01T00:00:00Z"}],"deny":[],"overrides":[]},"escrow":{"mode":"open", "limits":{"period":"day"},"allow":[],"deny":[],"overrides":[]}});
        let output = render_authz_policy(&policy);
        assert!(output.contains("did:key:z6Mk…abcd") && output.contains("2030-01-01T00:00:00Z"));
        assert!(output.contains("API token required: on") && output.contains("allowlist"));
    }
}

fn render_nostr_status(response: &Value, json_only: bool) -> Result<String> {
    if json_only {
        return Ok(serde_json::to_string_pretty(response)?);
    }
    if !response
        .get("running")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok("nostr relay: stopped".to_string());
    }
    let mut lines = vec!["nostr relay: running".to_string()];
    if let Some(url) = response.get("url").and_then(Value::as_str) {
        lines.push(format!("url: {url}"));
    }
    if let Some(tls) = response.get("tls").filter(|value| value.is_object()) {
        let source = tls
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let fingerprint = tls
            .get("sha256")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let path = tls
            .get("cert_path")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        lines.push(format!(
            "tls: {source}\n  sha256: {fingerprint}\n  certificate: {path}"
        ));
        if source == "self-signed" {
            lines.push("  Clients must trust or pin this certificate; public clients need cert_path and key_path.".to_string());
        }
    } else {
        lines.push("tls: disabled".to_string());
    }
    let number = |name| response.get(name).and_then(Value::as_u64).unwrap_or(0);
    lines.push(format!(
        "events: {}  connections: {}  subscriptions: {}  accepted: {}  rejected: {}",
        number("events"),
        number("connections"),
        number("subscriptions"),
        number("accepted"),
        number("rejected")
    ));
    let mesh = response.get("mesh").filter(|value| !value.is_null());
    let rooms = response.get("rooms").and_then(Value::as_array);
    let mesh_rooms = mesh
        .and_then(|mesh| mesh.get("rooms"))
        .and_then(Value::as_array);
    let mut room_names: Vec<&str> = rooms
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    for entry in mesh_rooms.into_iter().flatten() {
        if let Some(room) = entry.get("room").and_then(Value::as_str)
            && !room_names.contains(&room)
        {
            room_names.push(room);
        }
    }
    if rooms.is_some() || mesh_rooms.is_some() {
        if room_names.is_empty() {
            lines.push("mesh rooms: (none)".to_string());
        } else {
            lines.push("mesh rooms:".to_string());
            for room in room_names {
                let entry = mesh_rooms
                    .into_iter()
                    .flatten()
                    .find(|entry| entry.get("room").and_then(Value::as_str) == Some(room));
                if let Some(entry) = entry {
                    let number = |key: &str| {
                        entry
                            .get(key)
                            .and_then(Value::as_u64)
                            .map_or_else(|| "?".to_string(), |n| n.to_string())
                    };
                    let role = entry
                        .get("role")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    let leader = entry.get("leader").and_then(Value::as_str).unwrap_or("-");
                    let epoch = entry.get("epoch").and_then(Value::as_str).unwrap_or("-");
                    lines.push(format!(
                        "  {room}  {} peer(s)  role: {role}  leader: {leader}  members: {}  epoch:seq {epoch}:{}",
                        number("peers"),
                        number("members"),
                        number("seq")
                    ));
                } else {
                    lines.push(format!("  {room}  (not joined)"));
                }
            }
        }
    }
    if let Some(mesh) = mesh {
        let count = |key: &str| mesh.get(key).and_then(Value::as_u64).unwrap_or(0);
        lines.push(format!(
            "mesh: sent {}  received {}  syncs requested {}  served {}",
            count("sent"),
            count("received"),
            count("sync_requests"),
            count("sync_served")
        ));
        if let Some(peers) = mesh.get("interest_peers").and_then(Value::as_u64) {
            lines.push(format!("interest peers: {peers}"));
        }
    }
    let upstreams = response.get("upstreams").filter(|value| value.is_object());
    if mesh.is_some() || upstreams.is_some() {
        let gateway = mesh
            .and_then(|value| value.get("gateway"))
            .and_then(Value::as_bool)
            .or_else(|| {
                upstreams
                    .and_then(|value| value.get("gateway"))
                    .and_then(Value::as_bool)
            });
        lines.push(format!(
            "gateway: {}",
            match gateway {
                Some(true) => "yes",
                Some(false) => "no",
                None => "unknown",
            }
        ));
    }
    if let Some(interest) = upstreams
        .and_then(|value| value.get("interest"))
        .and_then(Value::as_u64)
    {
        lines.push(format!("interest: {interest} filter(s)"));
    }
    if let Some(relays) = upstreams
        .and_then(|value| value.get("relays"))
        .and_then(Value::as_array)
    {
        lines.push("upstream relays:".to_string());
        for relay in relays {
            let url = relay.get("url").and_then(Value::as_str).unwrap_or("?");
            let role = relay
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let state = relay
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let number = |key: &str| {
                relay
                    .get(key)
                    .and_then(Value::as_u64)
                    .map_or_else(|| "?".to_string(), |n| n.to_string())
            };
            let error = relay.get("error").and_then(Value::as_str).unwrap_or("-");
            lines.push(format!(
                "  {url}  role: {role}  {state}  received/published {}/{}  error: {error}",
                number("received"),
                number("published")
            ));
        }
    }
    Ok(lines.join("\n"))
}

fn render_nostr_events(response: &Value, json_only: bool) -> Result<String> {
    if json_only {
        return Ok(serde_json::to_string_pretty(response)?);
    }
    let mut lines = Vec::new();
    if let Some(events) = response.get("events").and_then(Value::as_array) {
        for event in events {
            let timestamp = event.get("created_at").and_then(Value::as_i64);
            let date = timestamp
                .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
                .map(|date| {
                    date.with_timezone(&chrono::Local)
                        .format("%Y-%m-%d %H:%M:%S %z")
                        .to_string()
                })
                .unwrap_or_else(|| timestamp.map_or_else(|| "?".to_string(), |n| n.to_string()));
            let kind = event.get("kind").and_then(Value::as_u64).unwrap_or(0);
            let pubkey: String = event
                .get("pubkey")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .chars()
                .take(12)
                .collect();
            let mut content = String::new();
            for c in event
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .chars()
                .take(80)
            {
                if c.is_control() {
                    content.extend(c.escape_debug());
                } else {
                    content.push(c);
                }
            }
            lines.push(format!("{date}  kind {kind}  {pubkey}  {content}"));
        }
    }
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod nostr_cli_tests {
    use super::*;

    #[test]
    fn parses_nostr_actions_and_repeated_query_options() {
        let cli = Cli::try_parse_from(["mistl", "nostr", "status", "--json"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Nostr {
                action: NostrAction::Status { json: true }
            })
        ));
        let cli = Cli::try_parse_from([
            "mistl",
            "nostr",
            "query",
            "--filter",
            "{}",
            "--filter",
            "{\"since\":1}",
            "--kind",
            "1",
            "--kind",
            "3",
            "--author",
            "abcd",
            "--author",
            "efgh",
            "--limit",
            "5",
            "--json",
        ])
        .unwrap();
        let Some(Command::Nostr {
            action:
                NostrAction::Query {
                    filters,
                    kinds,
                    authors,
                    limit,
                    json,
                },
        }) = cli.command
        else {
            panic!("expected nostr query");
        };
        assert_eq!(filters, ["{}", "{\"since\":1}"]);
        assert_eq!(kinds, [1, 3]);
        assert_eq!(authors, ["abcd", "efgh"]);
        assert_eq!(limit, Some(5));
        assert!(json);
    }

    #[test]
    fn renders_status_with_mesh_rooms_and_counters() {
        let status = json!({"running":true, "url":"wss://127.0.0.1:7447", "tls":{"source":"self-signed","sha256":"AA:BB","cert_path":"cert.pem"}, "events":2, "connections":3, "subscriptions":4, "accepted":5, "rejected":6, "rooms":["room-a","room-b"], "mesh":{"rooms":[{"room":"room-a","peers":3}],"sent":7,"received":8,"sync_requests":9,"sync_served":10}});
        let rendered = render_nostr_status(&status, false).unwrap();
        for part in [
            "running",
            "wss://127.0.0.1:7447",
            "AA:BB",
            "cert.pem",
            "trust or pin",
            "events: 2",
            "connections: 3",
            "subscriptions: 4",
            "accepted: 5",
            "rejected: 6",
            "room-a  3 peer(s)",
            "room-b  (not joined)",
            "mesh: sent 7  received 8  syncs requested 9  served 10",
        ] {
            assert!(rendered.contains(part), "missing {part}: {rendered}");
        }
        assert!(rendered.contains("role: unknown  leader: -  members: ?  epoch:seq -:?"));
        assert!(rendered.contains("gateway: unknown"));
        assert_eq!(
            serde_json::from_str::<Value>(&render_nostr_status(&status, true).unwrap()).unwrap(),
            status
        );
        assert!(
            render_nostr_status(&json!({"running":false}), false)
                .unwrap()
                .contains("stopped")
        );
    }

    #[test]
    fn renders_cluster_and_upstream_status() {
        let status = json!({
            "running": true,
            "tls": null,
            "mesh": {
                "gateway": false,
                "interest_peers": 2,
                "rooms": [
                    {"room":"one", "peers":2, "role":"follower", "leader":"0123456789abcdef", "members":3, "epoch":"abc", "seq":42},
                    {"room":"two", "peers":0, "role":"candidate", "leader":null, "members":1, "epoch":null, "seq":0}
                ]
            },
            "upstreams": {
                "gateway": false,
                "interest": 3,
                "relays": [
                    {"url":"wss://relay.example.com", "role":"upstream", "state":"backoff", "received":12, "published":7, "error":"connection failed"},
                    {"url":"ws://relay.example.net", "role":"bridge", "state":"idle", "received":0, "published":0, "error":null}
                ]
            }
        });
        let rendered = render_nostr_status(&status, false).unwrap();
        for part in [
            "tls: disabled",
            "one  2 peer(s)  role: follower  leader: 0123456789abcdef  members: 3  epoch:seq abc:42",
            "two  0 peer(s)  role: candidate  leader: -  members: 1  epoch:seq -:0",
            "gateway: no",
            "interest peers: 2",
            "interest: 3 filter(s)",
            "wss://relay.example.com  role: upstream  backoff  received/published 12/7  error: connection failed",
            "ws://relay.example.net  role: bridge  idle  received/published 0/0  error: -",
        ] {
            assert!(rendered.contains(part), "missing {part}: {rendered}");
        }
    }

    #[test]
    fn renders_standalone_gateway_and_missing_upstream_fields() {
        let status = json!({"running":true, "upstreams":{"gateway":true, "relays":[{}]}});
        let rendered = render_nostr_status(&status, false).unwrap();
        assert!(rendered.contains("gateway: yes"));
        assert!(rendered.contains("?  role: unknown  unknown  received/published ?/?  error: -"));
        assert!(!rendered.contains("interest:"));
        assert!(!rendered.contains("interest peers:"));

        let zero = json!({"running":true, "mesh":{"interest_peers":0}, "upstreams":{"interest":0}});
        let rendered = render_nostr_status(&zero, false).unwrap();
        assert!(rendered.contains("interest peers: 0"));
        assert!(rendered.contains("interest: 0 filter(s)"));
    }

    #[test]
    fn renders_events_without_splitting_unicode_or_lines() {
        let content = format!("{}\n{}", "猫".repeat(78), "tail");
        let response = json!({"events":[{"created_at":0,"kind":1,"pubkey":"abcdefghijklmnop","content":content}]});
        let rendered = render_nostr_events(&response, false).unwrap();
        assert_eq!(rendered.lines().count(), 1);
        assert!(rendered.contains("kind 1  abcdefghijkl"));
        assert!(rendered.contains("\\nt"));
        assert!(!rendered.contains("tail"));
        assert_eq!(
            serde_json::from_str::<Value>(&render_nostr_events(&response, true).unwrap()).unwrap(),
            response
        );
    }
}

fn escrow_slots_request(room: String) -> (&'static str, Value) {
    ("escrow.auto.slots", json!({"room": room}))
}

fn render_escrow_status(response: &Value, json_only: bool) -> Result<String> {
    let mut text = serde_json::to_string_pretty(response)?;
    if !json_only {
        if let Some(rooms) = response.get("rooms").and_then(Value::as_array) {
            for room in rooms {
                let Some(auto) = room.get("auto").filter(|v| v.is_object()) else {
                    continue;
                };
                let providers = auto.get("providers").and_then(Value::as_array);
                let total = providers.map_or(0, Vec::len);
                let online = providers.map_or(0, |items| {
                    items
                        .iter()
                        .filter(|p| p.get("online").and_then(Value::as_bool) == Some(true))
                        .count()
                });
                let number = |key| auto.get(key).and_then(Value::as_u64).unwrap_or(0);
                let room_name = room.get("room").and_then(Value::as_str).unwrap_or("-");
                text.push_str(&format!(
                    "\n{room_name}:\n  auto: {total} providers ({online} online), target {} - {} healthy, {} replicating, {} degraded, {} conflicts",
                    number("target"), number("healthy"), number("replicating"), number("degraded"), number("conflicts")
                ));
            }
        }
    }
    Ok(text)
}

fn render_escrow_slots(response: &Value) -> String {
    let mut lines = vec![format!(
        "{:<12}  {:>8}  {:<12}  {}",
        "SLOT", "SEQ", "STATE", "CONFIRMED/TARGET"
    )];
    if let Some(slots) = response.get("slots").and_then(Value::as_array) {
        for slot in slots {
            let id: String = slot
                .get("slot")
                .and_then(Value::as_str)
                .unwrap_or("-")
                .chars()
                .take(12)
                .collect();
            let seq = slot.get("seq").and_then(Value::as_u64).unwrap_or(0);
            let state = slot.get("state").and_then(Value::as_str).unwrap_or("-");
            let confirmed = slot.get("confirmed").and_then(Value::as_u64).unwrap_or(0);
            let target = slot.get("target").and_then(Value::as_u64).unwrap_or(0);
            lines.push(format!(
                "{id:<12}  {seq:>8}  {state:<12}  {confirmed}/{target}"
            ));
        }
    }
    lines.join("\n")
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

/// Resolve the existing parent while preserving a not-yet-created export filename.
fn escrow_export_path(path: &str) -> Result<std::path::PathBuf> {
    let path = std::path::Path::new(path);
    let name = path
        .file_name()
        .context("export path requires a file name")?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let parent = std::fs::canonicalize(parent).context("export parent directory must exist")?;
    anyhow::ensure!(parent.is_dir(), "export parent must be a directory");
    Ok(parent.join(name))
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

// -- `mistl tunnel`: human-readable forward/trust/rule rendering ----------
//
// Same `render_sched_ls`-style tab-separated table shape as the renderers
// above. The renderers read IPC response fields defensively rather than
// asserting they're present; a missing field renders as an empty column.

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

fn tunnel_rule_request(action: TunnelRuleAction) -> (&'static str, Value) {
    match action {
        TunnelRuleAction::Ls => ("tunnel.rules.list", json!({})),
        TunnelRuleAction::Add {
            action,
            peer,
            proto,
            ports,
            hosts,
            note,
        } => {
            let mut args = serde_json::Map::new();
            args.insert("action".to_string(), action.into());
            for (key, value) in [
                ("peer", peer),
                ("proto", proto),
                ("ports", ports),
                ("hosts", hosts),
                ("note", note),
            ] {
                if let Some(value) = value {
                    args.insert(key.to_string(), value.into());
                }
            }
            ("tunnel.rules.add", Value::Object(args))
        }
        TunnelRuleAction::Rm { id } => ("tunnel.rules.remove", json!({ "id": id })),
    }
}

/// `mistl tunnel rule ls`: one row per entry in `tunnel.rules.list`.
fn render_tunnel_rules(response: &Value) -> String {
    let rules = response
        .get("rules")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rules.is_empty() {
        return "no proposal rules".to_string();
    }
    let mut lines = vec!["ID\tACTION\tPEER\tPROTO\tPORTS\tHOSTS\tNOTE".to_string()];
    lines.extend(rules.iter().map(|rule| {
        ["id", "action", "peer", "proto", "ports", "hosts", "note"]
            .iter()
            .map(|key| {
                let mut field = String::new();
                for c in rule
                    .get(*key)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .chars()
                {
                    if c.is_control() {
                        field.extend(c.escape_debug());
                    } else {
                        field.push(c);
                    }
                }
                field
            })
            .collect::<Vec<_>>()
            .join("\t")
    }));
    lines.join("\n")
}

#[cfg(test)]
mod tunnel_rule_cli_tests {
    use super::*;

    fn parse_rule(args: &[&str]) -> TunnelRuleAction {
        let parsed = Cli::try_parse_from(args).unwrap();
        let Some(Command::Tunnel {
            action: TunnelAction::Rule { action },
        }) = parsed.command
        else {
            panic!("expected tunnel rule command");
        };
        action
    }

    #[test]
    fn proposal_rule_commands_build_ipc_requests() {
        assert_eq!(
            tunnel_rule_request(parse_rule(&["mistl", "tunnel", "rule", "ls"])),
            ("tunnel.rules.list", json!({}))
        );
        assert_eq!(
            tunnel_rule_request(parse_rule(&["mistl", "tunnel", "rule", "add", "allow"])),
            ("tunnel.rules.add", json!({ "action": "allow" }))
        );
        assert_eq!(
            tunnel_rule_request(parse_rule(&[
                "mistl",
                "tunnel",
                "rule",
                "add",
                "deny",
                "--peer",
                "0123456789abcdef",
                "--proto",
                "udp",
                "--ports",
                "22, 80,8000-8100",
                "--hosts",
                "host1,host2",
                "--note",
                "test rule",
            ])),
            (
                "tunnel.rules.add",
                json!({
                    "action": "deny", "peer": "0123456789abcdef", "proto": "udp",
                    "ports": "22, 80,8000-8100", "hosts": "host1,host2", "note": "test rule"
                })
            )
        );
        assert_eq!(
            tunnel_rule_request(parse_rule(&["mistl", "tunnel", "rule", "rm", "r-1234"])),
            ("tunnel.rules.remove", json!({ "id": "r-1234" }))
        );
        assert!(Cli::try_parse_from(["mistl", "tunnel", "rule", "add"]).is_err());
        assert!(Cli::try_parse_from(["mistl", "tunnel", "rule", "add", "maybe"]).is_err());
        assert!(Cli::try_parse_from(["mistl", "tunnel", "rule", "rm"]).is_err());
    }

    #[test]
    fn proposal_rules_render_empty_and_missing_lists() {
        assert_eq!(
            render_tunnel_rules(&json!({ "rules": [] })),
            "no proposal rules"
        );
        assert_eq!(render_tunnel_rules(&json!({})), "no proposal rules");
    }

    #[test]
    fn proposal_rules_render_all_columns_and_escape_controls() {
        let response = json!({ "rules": [
            {
                "id": "r-1234", "action": "allow", "peer": "0123456789abcdef",
                "proto": "tcp", "ports": "22,80", "hosts": "loopback",
                "note": "SSH\tand\nweb\u{1b}"
            },
            { "id": "r-5678", "action": "deny", "peer": "*", "proto": "*",
              "ports": "*", "hosts": "*", "note": "日本語" }
        ] });
        assert_eq!(
            render_tunnel_rules(&response),
            "ID\tACTION\tPEER\tPROTO\tPORTS\tHOSTS\tNOTE\n\
             r-1234\tallow\t0123456789abcdef\ttcp\t22,80\tloopback\tSSH\\tand\\nweb\\u{1b}\n\
             r-5678\tdeny\t*\t*\t*\t*\t日本語"
        );
    }
}

#[cfg(test)]
mod bot_render_tests {
    use super::*;

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
}

#[cfg(test)]
mod escrow_membership_cli_tests {
    use super::*;

    #[test]
    fn escrow_mesh_cli_status_and_slots() {
        let parsed = Cli::try_parse_from(["mistl", "escrow", "slots", "room-a"]).unwrap();
        let Some(Command::Escrow {
            action: EscrowAction::Slots { room },
        }) = parsed.command
        else {
            panic!("expected slots command")
        };
        assert_eq!(
            escrow_slots_request(room),
            ("escrow.auto.slots", json!({"room":"room-a"}))
        );
        assert!(Cli::try_parse_from(["mistl", "escrow", "slots"]).is_err());
        assert!(matches!(
            Cli::try_parse_from(["mistl", "escrow", "status-json"])
                .unwrap()
                .command,
            Some(Command::Escrow {
                action: EscrowAction::StatusJson
            })
        ));
        let status = json!({"rooms":[{"room":"mesh", "auto": {"target":3,
            "providers":[{"online":true},{"online":true},{"online":false}],
            "healthy":120,"replicating":3,"degraded":1,"conflicts":0}}, {"room":"raft"}]});
        let text = render_escrow_status(&status, false).unwrap();
        assert!(text.contains("mesh:\n  auto: 3 providers (2 online), target 3 - 120 healthy, 3 replicating, 1 degraded, 0 conflicts"));
        assert!(
            !render_escrow_status(&status, true)
                .unwrap()
                .contains("  auto:")
        );
        let slots = render_escrow_slots(
            &json!({"slots":[{"slot":"abcdefghijklmnop","seq":7,"state":"replicating","confirmed":2,"target":3}]}),
        );
        assert!(slots.contains("abcdefghijkl") && !slots.contains("abcdefghijklmnop"));
        assert!(slots.contains("replicating") && slots.contains("2/3"));
        assert!(
            render_escrow_slots(&json!({"slots":[{"slot":"日本語abcdefghi"}]})).contains("日本語")
        );
    }

    #[test]
    fn escrow_transfer_cli_parses_rooms_and_force() {
        let parsed = Cli::try_parse_from([
            "mistl",
            "escrow",
            "export",
            "out.mistl-escrow",
            "--room",
            "a",
            "--room",
            "b",
            "--force",
        ])
        .unwrap();
        let Some(Command::Escrow {
            action: EscrowAction::Export { file, rooms, force },
        }) = parsed.command
        else {
            panic!("expected export command")
        };
        assert_eq!(file, "out.mistl-escrow");
        assert_eq!(rooms, ["a", "b"]);
        assert!(force);
        let parsed = Cli::try_parse_from(["mistl", "escrow", "export", "out"]).unwrap();
        assert!(
            matches!(parsed.command, Some(Command::Escrow { action: EscrowAction::Export { rooms, force: false, .. } }) if rooms.is_empty())
        );
        for rooms_args in [vec![], vec!["--room", "a", "--room", "b"]] {
            let mut args = vec!["mistl", "escrow", "import", "in.mistl-escrow"];
            args.extend(rooms_args);
            let parsed = Cli::try_parse_from(args).unwrap();
            let Some(Command::Escrow {
                action: EscrowAction::Import { file, rooms },
            }) = parsed.command
            else {
                panic!("expected import command")
            };
            assert_eq!(file, "in.mistl-escrow");
            assert!(rooms.is_empty() || rooms == ["a", "b"]);
        }
        assert!(Cli::try_parse_from(["mistl", "escrow", "import", "in", "--force"]).is_err());
        assert!(Cli::try_parse_from(["mistl", "escrow", "export"]).is_err());
    }

    #[test]
    fn escrow_export_cli_resolves_parent_of_new_file() {
        let name = format!("escrow-cli-{:016x}.mistl-escrow", rand::random::<u64>());
        let cwd = std::fs::canonicalize(".").unwrap();
        assert_eq!(escrow_export_path(&name).unwrap(), cwd.join(&name));
        assert_eq!(
            escrow_export_path(cwd.join(&name).to_str().unwrap()).unwrap(),
            cwd.join(&name)
        );
        assert!(!cwd.join(&name).exists());
        assert!(escrow_export_path(&format!("{name}/missing/export")).is_err());
    }

    #[test]
    fn explicit_membership_preserves_isolated_instance_and_list_arguments() {
        let parsed = Cli::try_parse_from([
            "mistl",
            "--instance",
            "backup",
            "--state-dir",
            "state",
            "escrow",
            "membership",
            "room-a",
            "--voters",
            "0000000000000001,0000000000000002,0000000000000003",
            "--learners",
            "0000000000000004,0000000000000005",
        ])
        .unwrap();
        assert_eq!(parsed.instance.as_deref(), Some("backup"));
        assert_eq!(parsed.state_dir, Some(std::path::PathBuf::from("state")));
        let Some(Command::Escrow {
            action:
                EscrowAction::Membership {
                    room,
                    voters,
                    learners,
                },
        }) = parsed.command
        else {
            panic!("expected membership command")
        };
        assert_eq!(room, "room-a");
        assert_eq!(voters.len(), 3);
        assert_eq!(learners.len(), 2);
        assert!(Cli::try_parse_from(["mistl", "escrow", "membership", "room-a"]).is_err());
    }
}
