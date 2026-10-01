//! Direct messages (chat + file attachments) between mistl nodes.
//!
//! Messages travel as signed JSON over `net::send_direct` into a room both
//! nodes share (see [`wire`] for the format and every inbound check).
//! Attachments are not sent inline: the sender encrypts the file with a
//! fresh random passphrase into an ordinary tc-storage bundle in its own
//! content store, and the message carries `{cid, name, size, mime, key}`.
//! The recipient resolves the CID from the rooms where the sender is
//! currently verified and decrypts it into its sandbox on `dm.download`.
//!
//! Trust model: a message is authentic iff its signature verifies for
//! `fromId`, `node_id(fromId)` equals the transport sender, and it is
//! addressed (signed `to`) to our DID. The transport itself (WebRTC) provides
//! confidentiality in transit; there is no extra end-to-end layer for the
//! text. The file key is delivered inside the signed message, so it is only
//! as private as the message path.
//!
//! IPC (`dm.*`):
//! - `dm.send {did, text?, sandbox?}` -> `{message}`
//! - `dm.ls {}` -> `{conversations:[{did, unread, updated_ms, last}]}`
//! - `dm.history {did, limit?}` -> `{messages}` (oldest first, <= 500)
//! - `dm.read {did}` / `dm.clear {did}` -> `{ok}`
//! - `dm.download {did, id}` -> `{sandbox_path, name, size}`

mod history;
mod limits;
mod wire;

use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use serde_json::{Value, json};
use tokio::sync::{OnceCell, Semaphore};
use tracing::{debug, warn};

use crate::daemon::AppState;
use crate::identity::Identity;
use crate::net::peer_auth;

use history::{AddError, History, Status, StoredMsg};
use limits::{Dedupe, RateLimiter};
use wire::{FileMeta, Incoming, Reject};

/// How often dirty conversations are written to disk.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Per-block remote resolution budget for `dm.download`.
const BLOCK_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall budget for fetching one attachment.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(15 * 60);
/// File transfers (encrypt+store on send, fetch+decrypt on download) that may
/// run at once; each holds the whole file in memory several times over.
const MAX_CONCURRENT_TRANSFERS: usize = 2;

/// What happened to one inbound raw message (for the handler and tests).
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Not a DM (or too big to be one): ignored silently.
    Ignored,
    Rejected(Reject),
    NotAdmitted,
    Duplicate,
    RateLimited,
    /// The conversation table is full of conversations we wrote in.
    HistoryFull,
    /// Stored; the caller should send the delivery receipt.
    Accepted {
        from_did: String,
        id: String,
    },
    /// A receipt was processed (`true` = it upgraded a message).
    AckApplied(bool),
}

#[derive(Debug)]
struct Inbox {
    dedupe: Dedupe,
    limiter: RateLimiter,
}

struct Service {
    identity: Arc<Identity>,
    history: Mutex<History>,
    inbox: Mutex<Inbox>,
    /// `<data_dir>/dm`; `None` keeps everything in memory (tests).
    dir: Option<PathBuf>,
    transfers: Semaphore,
}

impl Service {
    fn new(identity: Arc<Identity>, dir: Option<PathBuf>, history: History) -> Self {
        Self {
            identity,
            history: Mutex::new(history),
            inbox: Mutex::new(Inbox {
                dedupe: Dedupe::default(),
                limiter: RateLimiter::new(Instant::now()),
            }),
            dir,
            transfers: Semaphore::new(MAX_CONCURRENT_TRANSFERS),
        }
    }

    fn history(&self) -> std::sync::MutexGuard<'_, History> {
        self.history.lock().expect("dm history poisoned")
    }

    fn inbox(&self) -> std::sync::MutexGuard<'_, Inbox> {
        self.inbox.lock().expect("dm inbox poisoned")
    }

    /// Every inbound check, cheapest first, signature last (see the module
    /// doc of [`wire`]). `from` is the transport sender; nothing in the body
    /// is trusted until it matches it. `admit` is the membership allowlist.
    fn process_inbound(
        &self,
        from: &str,
        data: &[u8],
        now_ms: u64,
        now: Instant,
        admit: &dyn Fn(&str) -> bool,
    ) -> Outcome {
        // Stateless: size, tag, node == from, node_id(fromId) == from,
        // to == us, ts window, field shapes.
        let pre = match wire::precheck(data, from, self.identity.did(), now_ms) {
            Ok(pre) => pre,
            Err(Reject::NotOurs | Reject::Oversized) => return Outcome::Ignored,
            Err(reject) => return Outcome::Rejected(reject),
        };
        if !admit(&pre.from_did) {
            return Outcome::NotAdmitted;
        }
        // Replays are dropped before they cost a signature check or a
        // rate-limit token. The cache only ever holds verified entries.
        if !pre.is_ack() && self.inbox().dedupe.contains(&pre.from_did, &pre.id) {
            return Outcome::Duplicate;
        }
        // Charged per transport sender, before the signature, so a flood of
        // forged messages burns the attacker's own budget and bounded CPU.
        if !self.inbox().limiter.allow(from, now) {
            return Outcome::RateLimited;
        }
        let incoming = match pre.verify() {
            Ok(incoming) => incoming,
            Err(reject) => return Outcome::Rejected(reject),
        };
        match incoming {
            Incoming::Message(msg) => {
                // Race-safe re-check: two copies may both pass `contains`.
                if !self.inbox().dedupe.insert(&msg.from_did, &msg.id, now) {
                    return Outcome::Duplicate;
                }
                let stored = StoredMsg {
                    id: msg.id.clone(),
                    mine: false,
                    ts_ms: msg.ts,
                    text: msg.text,
                    file: msg.file,
                    status: Status::Received,
                };
                match self.history().add(&msg.from_did, stored, now_ms, true) {
                    Ok(()) => Outcome::Accepted {
                        from_did: msg.from_did,
                        id: msg.id,
                    },
                    Err(AddError::Duplicate) => Outcome::Duplicate,
                    Err(AddError::Full) => Outcome::HistoryFull,
                }
            }
            // Bound to the verified sender: only a message *we* sent to
            // `ack.from_did` can be upgraded, never one in another thread.
            Incoming::Ack(ack) => {
                Outcome::AckApplied(self.history().mark_delivered(&ack.from_did, &ack.id))
            }
        }
    }

    /// Writes dirty conversations (and deletes cleared ones).
    async fn flush(self: &Arc<Self>) {
        let Some(dir) = self.dir.clone() else {
            return;
        };
        let pending = self.history().take_pending();
        if pending.is_empty() {
            return;
        }
        let result = tokio::task::spawn_blocking(move || history::apply_pending(&dir, pending))
            .await
            .map_err(|e| anyhow!("join error: {e}"))
            .and_then(|r| r);
        if let Err(error) = result {
            warn!(%error, "dm: persisting history failed; will retry");
            self.history().mark_all_dirty();
        }
    }
}

static SERVICE: OnceCell<Arc<Service>> = OnceCell::const_new();

/// The process-wide DM service: loads history, registers the inbound
/// handler and starts the debounced flusher on first use.
async fn service(state: &Arc<AppState>) -> Result<Arc<Service>> {
    let service = SERVICE
        .get_or_try_init(|| async {
            let identity = crate::identity::current(state)
                .await
                .context("dm: loading identity")?;
            let dir = crate::config::data_dir()
                .context("dm: resolving data dir")?
                .join("dm");
            let history = {
                let dir = dir.clone();
                tokio::task::spawn_blocking(move || History::load(&dir))
                    .await
                    .context("dm: loading history")?
            };
            let service = Arc::new(Service::new(identity, Some(dir), history));
            register_inbound(service.clone(), tokio::runtime::Handle::current());
            let flusher = service.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(FLUSH_INTERVAL);
                loop {
                    tick.tick().await;
                    flusher.flush().await;
                }
            });
            // A node that sent attachments must keep serving their blocks
            // after a restart: opening the store registers the storage wire
            // handler (QUERY/WANT), which otherwise only happens on the first
            // `store.*` command.
            if service.history().has_sent_files() {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(error) = crate::storage::store(&state).await {
                        warn!(%error, "dm: opening the store for attachment serving failed");
                    }
                });
            }
            Ok::<_, anyhow::Error>(service)
        })
        .await?;
    Ok(service.clone())
}

fn register_inbound(service: Arc<Service>, runtime: tokio::runtime::Handle) {
    crate::net::register_room_handler(move |event_type, room, from, data| {
        if event_type != crate::net::EVENT_RAW || !wire::looks_like_dm(data) {
            return;
        }
        let outcome = service.process_inbound(
            from,
            data,
            peer_auth::now_ms(),
            Instant::now(),
            &peer_auth::admitted,
        );
        match outcome {
            Outcome::Accepted { from_did, id } => {
                // Ack only accepted messages, in the room it arrived in.
                let bytes = wire::build_ack(&service.identity, &from_did, &id, peer_auth::now_ms());
                let (room, to) = (room.to_string(), from.to_string());
                runtime.spawn(async move {
                    let _ = crate::net::send_direct(&room, &to, bytes).await;
                });
            }
            Outcome::Ignored | Outcome::AckApplied(_) => {}
            other => debug!(%room, node = %from, ?other, "dm: dropped inbound message"),
        }
    });
}

/// Starts the DM service so messages from verified peers are received (and
/// stored) while nobody is asking.
pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(error) = service(&state).await {
            warn!(%error, "dm: failed to start");
        }
    });
}

fn short_did(did: &str) -> String {
    if did.len() > 20 {
        format!("{}...{}", &did[..12], &did[did.len() - 4..])
    } else {
        did.to_string()
    }
}

fn did_arg(args: &Value, ours: &str) -> Result<String> {
    let did = args
        .get("did")
        .and_then(Value::as_str)
        .context("missing `did`")?;
    if crate::identity::pubkey_from_did(did).is_err() {
        bail!("`did` is not a valid did:key");
    }
    if did == ours {
        bail!("cannot message yourself");
    }
    Ok(did.to_string())
}

/// IPC entry point for `dm.*` commands.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    let svc = service(state).await?;
    match cmd {
        "dm.send" => send(&svc, state, &args).await,
        "dm.ls" => Ok(json!({ "conversations": svc.history().list() })),
        "dm.history" => {
            let did = did_arg(&args, svc.identity.did())?;
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(history::MAX_MESSAGES as u64)
                .min(history::MAX_MESSAGES as u64) as usize;
            Ok(json!({ "messages": svc.history().recent(&did, limit) }))
        }
        "dm.read" => {
            let did = did_arg(&args, svc.identity.did())?;
            svc.history().mark_read(&did);
            Ok(json!({ "ok": true }))
        }
        "dm.clear" => {
            let did = did_arg(&args, svc.identity.did())?;
            svc.history().clear(&did);
            Ok(json!({ "ok": true }))
        }
        "dm.download" => download(&svc, state, &args).await,
        _ => bail!("unknown command: {cmd}"),
    }
}

async fn send(svc: &Arc<Service>, state: &Arc<AppState>, args: &Value) -> Result<Value> {
    let did = did_arg(args, svc.identity.did())?;
    if !peer_auth::admitted(&did) {
        bail!(
            "{} is not allowed by the membership allowlist",
            short_did(&did)
        );
    }
    let text = match args.get("text") {
        None | Some(Value::Null) => None,
        Some(Value::String(t)) if t.trim().is_empty() => None,
        Some(Value::String(t)) => Some(t.as_str()),
        Some(_) => bail!("`text` must be a string"),
    };
    let sandbox = match args.get("sandbox") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.trim().is_empty() => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => bail!("`sandbox` must be a string"),
    };
    if text.is_none() && sandbox.is_none() {
        bail!("nothing to send: provide `text` and/or `sandbox`");
    }
    if let Some(text) = text {
        wire::validate_text(text)?;
    }
    let routes = peer_auth::rooms_for_did(&did);
    if routes.is_empty() {
        bail!("{} is offline: no shared room", short_did(&did));
    }

    let file = match sandbox {
        Some(rel) => Some(prepare_file(svc, state, rel).await?),
        None => None,
    };
    let id = wire::fresh_id();
    let ts = peer_auth::now_ms();
    let bytes = wire::build_message(&svc.identity, &did, &id, ts, text, file.as_ref())?;

    // Same peer may be verified in several rooms: the first route that
    // accepts the message wins (the receiver dedupes anyway).
    let mut sent = false;
    for (room, node) in &routes {
        match crate::net::send_direct(room, node, bytes.clone()).await {
            Ok(()) => {
                sent = true;
                break;
            }
            Err(error) => debug!(%room, %node, %error, "dm: route failed"),
        }
    }
    let stored = StoredMsg {
        id,
        mine: true,
        ts_ms: ts,
        text: text.map(str::to_string),
        // The recipient needs the key, we do not: never persist it here.
        file: file.map(|f| FileMeta {
            key: String::new(),
            ..f
        }),
        status: if sent { Status::Sent } else { Status::Failed },
    };
    let ui = stored.ui();
    match svc.history().add(&did, stored, ts, false) {
        Ok(()) => {}
        Err(AddError::Duplicate) | Err(AddError::Full) => bail!("could not record the message"),
    }
    Ok(json!({ "message": ui }))
}

/// Validates a sandbox-relative path (no traversal) and returns it with the
/// absolute location under the sandbox root.
fn sandbox_file(root: &Path, rel: &str) -> Result<PathBuf> {
    if rel.trim().is_empty() {
        bail!("sandbox path is required");
    }
    let requested = Path::new(rel);
    if requested.is_absolute() {
        bail!("sandbox path must be relative");
    }
    let mut out = root.to_path_buf();
    let mut any = false;
    for component in requested.components() {
        match component {
            Component::Normal(part) => {
                out.push(part);
                any = true;
            }
            Component::CurDir => {}
            _ => bail!("sandbox path escapes the sandbox"),
        }
    }
    if !any {
        bail!("sandbox path is required");
    }
    Ok(out)
}

fn guess_mime(name: &str) -> &'static str {
    let ext = name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("txt" | "log" | "md") => "text/plain",
        Some("html" | "htm") => "text/html",
        Some("json") => "application/json",
        Some("pdf") => "application/pdf",
        Some("zip") => "application/zip",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("ogg") => "audio/ogg",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        _ => "application/octet-stream",
    }
}

/// Encrypts a sandbox file into this node's content store under a fresh
/// random passphrase (the existing tc-storage `store.put-file` path).
async fn prepare_file(svc: &Arc<Service>, state: &Arc<AppState>, rel: &str) -> Result<FileMeta> {
    let root = crate::config::data_dir()?.join("sandbox");
    let path = sandbox_file(&root, rel)?;
    let meta = std::fs::metadata(&path).with_context(|| format!("no such sandbox file: {rel}"))?;
    if !meta.is_file() {
        bail!("not a file: {rel}");
    }
    if meta.len() > wire::MAX_FILE_BYTES {
        bail!(
            "file is larger than {} MiB",
            wire::MAX_FILE_BYTES / (1024 * 1024)
        );
    }
    let _permit = svc
        .transfers
        .try_acquire()
        .map_err(|_| anyhow!("too many file transfers in progress; try again shortly"))?;
    let key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    let put = crate::storage::handle(
        "store.put-file",
        json!({ "sandbox": rel, "passphrase": key }),
        state,
    )
    .await?;
    let cid = put
        .get("cid")
        .and_then(Value::as_str)
        .context("store did not return a cid")?
        .to_string();
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string();
    let mime = guess_mime(&name).to_string();
    Ok(FileMeta {
        cid,
        name,
        size: meta.len(),
        mime,
        key,
    })
}

/// Reduces a peer-supplied file name to one safe path component (no
/// separators, drive or stream syntax, control characters, leading dots or
/// trailing dots/spaces, reserved Windows device names).
fn safe_file_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_control() || "<>:\"/\\|?*".contains(c) {
                '_'
            } else {
                c
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches(|c| c == ' ' || c == '.');
    let mut out: String = trimmed.chars().take(120).collect();
    if out.is_empty() {
        return "file".to_string();
    }
    let stem = out.split('.').next().unwrap_or("").to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && stem.as_bytes()[3].is_ascii_digit());
    if reserved {
        out.insert(0, '_');
    }
    out
}

/// Fetches a received attachment and decrypts it into the sandbox under
/// `dm/<message id>/<safe name>`. Only a file attached to a message we
/// received from `did` can be requested (the UI never supplies a cid), and
/// the CID is only resolved in rooms where that peer is currently verified.
async fn download(svc: &Arc<Service>, state: &Arc<AppState>, args: &Value) -> Result<Value> {
    let did = did_arg(args, svc.identity.did())?;
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .context("missing `id`")?;
    if !wire::is_valid_id(id) {
        bail!("invalid message id");
    }
    if !peer_auth::admitted(&did) {
        bail!(
            "{} is not allowed by the membership allowlist",
            short_did(&did)
        );
    }
    let file = {
        let history = svc.history();
        let msg = history.message(&did, id).context("no such message")?;
        if msg.mine {
            bail!("that is a message you sent");
        }
        msg.file
            .clone()
            .context("that message has no file attached")?
    };
    if file.key.is_empty() || file.size > wire::MAX_FILE_BYTES {
        bail!("that file cannot be downloaded");
    }

    let mut rooms: Vec<String> = peer_auth::rooms_for_did(&did)
        .into_iter()
        .map(|(room, _)| room)
        .collect();
    rooms.sort();
    rooms.dedup();

    let _permit = svc
        .transfers
        .try_acquire()
        .map_err(|_| anyhow!("too many file transfers in progress; try again shortly"))?;
    let store = crate::storage::store(state).await?;
    // Pull the encrypted bundle into the local block store, from the sender's
    // rooms only. A local copy (earlier download) is served without any room.
    match tokio::time::timeout(
        DOWNLOAD_TIMEOUT,
        store.get_remote(&file.cid, &rooms, BLOCK_TIMEOUT),
    )
    .await
    {
        Err(_) => bail!("timed out fetching the file"),
        Ok(Err(error)) if rooms.is_empty() => bail!(
            "{} is offline: no shared room, and the file is not cached locally ({error})",
            short_did(&did)
        ),
        Ok(Err(error)) => return Err(error.context("fetching the file from the sender")),
        Ok(Ok(_blob)) => {}
    }

    let safe = safe_file_name(&file.name);
    let rel = format!("dm/{id}/{safe}");
    let output = sandbox_file(&store.data_dir().join("sandbox"), &rel)?;
    let result = crate::storage::handle(
        "store.get-file",
        json!({
            "cid": file.cid,
            "passphrase": file.key,
            "output": output.to_string_lossy(),
        }),
        state,
    )
    .await
    .context("decrypting the file")?;
    if result.get("output").is_none() {
        bail!("the bundle carries no file content");
    }
    let size = result.get("size").and_then(Value::as_u64).unwrap_or(0);
    if size != file.size {
        let _ = std::fs::remove_file(&output);
        bail!(
            "downloaded file size does not match the message ({size} != {})",
            file.size
        );
    }
    Ok(json!({ "sandbox_path": rel, "name": safe, "size": size }))
}

#[cfg(test)]
mod tests;
