//! tc-chat relay: joins configured tc-chat rooms so signed `tc-chat:*` wires
//! keep arriving -- and get answered/replayed to other late joiners -- even
//! while no tc-chat browser tab is open on this machine. A standing
//! relay/bot for tc-chat's own wire protocol, sharing only the `crate::net`
//! transport with the daemon's other room-scoped protocols (`ai`, `storage`,
//! `tunnel`, the stream relay).
//!
//! IPC surface: `chat.rooms` and `chat.log`, handled by [`handle`].
//!
//! ## Protocol (interop contract with tc-chat's web client)
//!
//! - Every relayed message is a JSON object tagged `type: "tc-chat:post"`
//!   (`tc-chat/src/hooks/usePostStream.ts`), or one of its siblings
//!   `"tc-chat:reaction"` / `"tc-chat:post-edit"` / `"tc-chat:post-delete"`.
//!   Do **not** target the older `"tc-chat:message"` type the tc-chat *CLI*
//!   (`tc-chat/cli/src/wire.rs`) still uses -- the web client (and thus real
//!   users) speaks `tc-chat:post`.
//! - `signature` is an Ed25519 signature, base64url-encoded *without*
//!   padding, over the UTF-8 bytes of [`stable_json::stable_stringify`]
//!   applied to the wire with `signature` itself removed -- byte-identical to
//!   tc-chat's `src/lib/wireSign.ts` `signingPayload`. It is keyed by
//!   `fromId`, a `did:key:z...` string using the *same* did:key derivation as
//!   [`crate::identity`], so [`crate::identity::verify`] verifies these wires
//!   directly.
//! - The room id is the raw, unhashed room string (tc-chat's own room ids
//!   match `^[A-Za-z0-9_-]{1,64}$`); joining the same room string joins the
//!   same mistlib swarm, the same convention `crate::stream::relay` uses.
//! - There are no acks. Catch-up is by history replay: on join, a peer
//!   broadcasts an (unsigned, untargeted) `tc-chat:history-request
//!   {id, roomId}`; any peer already in the room -- a tc-chat tab, or another
//!   mistl relay -- unicasts back every wire in its per-room log, verbatim,
//!   including the original signature. Because the signature covers the
//!   original `fromId` and not the relayer, this is trust-free: the receiver
//!   re-verifies every replayed wire exactly like a live broadcast (see
//!   `tc-chat/src/hooks/useHistorySync.ts`, and the CLI's port of the same
//!   idea in `tc-chat/cli/src/archive.rs`). This module persists and replays
//!   wires **verbatim** (the exact bytes received) rather than
//!   re-serializing them, both because re-serializing through a
//!   `serde_json::Value` would reorder keys (breaking nothing
//!   cryptographically, since verification re-canonicalizes anyway, but
//!   breaking the "byte-identical replay" contract this module aims for) and
//!   because it's simply less code.
//! - Structured post kinds (`text`/`project`/`event`) carry only a `cid`; the
//!   JSON body (`{title?, text?, ...}`) lives in mistlib's own
//!   process-singleton content store (`mistlib::app::storage_get`/`storage_add`,
//!   wired up by `crate::net::start_engine`) -- a **different** store than
//!   [`crate::storage`], which is mistl's own local, independently-configured
//!   block store. `chat.log` best-effort-resolves this for display;
//!   see [`resolve_body_text`].
//! - tc-chat peers' mistlib node ids are random per-session UUIDs unrelated
//!   to their DID -- this module (like tc-chat itself) only ever identifies
//!   senders by `fromId`, never by node id.
//!
//! ## Room disambiguation
//!
//! `crate::net::register_handler`'s raw-event fan-out doesn't tag which of
//! this process's several joined rooms an event arrived from -- fine for
//! the daemon's other room protocols (whose wires are self-contained and
//! room-agnostic), but not fine here: with more than one `chat_relay.rooms` entry configured, a
//! `tc-chat:post` wire carries no room id of its own (it relies entirely on
//! room-scoped transport delivery), so two rooms could not be told apart from
//! payload alone. This module instead uses
//! [`crate::net::register_room_handler`], which mistlib does tag with the
//! originating room.
//!
//! ## mistlib gotchas (see `crate::net`'s module doc for the general version)
//!
//! `join_room` (via `crate::net::ensure_started`) is fire-and-forget with no
//! "room ready" signal, so a send immediately after joining can transiently
//! fail with `"room '<id>' is not joined"` while the session is still being
//! built. [`request_history`] waits [`HISTORY_REQUEST_DELAY`] before its
//! first broadcast (mirroring tc-chat's own `REQUEST_DELAY_MS`) and retries
//! on failure a few times with a short backoff -- the same idiom
//! `crate::stream::relay`'s cascade republish uses.

mod stable_json;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, warn};

use crate::daemon::AppState;

/// Handle `chat.*` IPC commands:
/// - `chat.rooms` `{}` -> `{enabled, rooms: [{room, joined}]}` (relay status)
/// - `chat.log` `{room, limit?}` -> `[{id, type, fromId, fromName, timestamp, ...}]`
///   (relayed tc-chat wires for `room`, oldest first; see [`chat_log`])
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "chat.rooms" => rooms_status(state).await,
        "chat.log" => {
            let args: ChatLogArgs =
                serde_json::from_value(args).context("chat.log: invalid arguments")?;
            chat_log(state, &args.room, args.limit.unwrap_or(50)).await
        }
        _ => bail!("unknown chat command: {cmd}"),
    }
}

#[derive(Deserialize)]
struct ChatLogArgs {
    room: String,
    #[serde(default)]
    limit: Option<usize>,
}

/// Random lowercase-hex wire id (16 bytes -> 32 hex chars), matching the id
/// shape tc-chat's own client generates.
fn random_id() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

const WIRE_POST: &str = "tc-chat:post";
const WIRE_REACTION: &str = "tc-chat:reaction";
const WIRE_POST_EDIT: &str = "tc-chat:post-edit";
const WIRE_POST_DELETE: &str = "tc-chat:post-delete";
const WIRE_HISTORY_REQUEST: &str = "tc-chat:history-request";

/// Most-recent wires kept per room, matching tc-chat's own replay cap
/// (`useHistorySync.ts` persists/replays up to 600).
const WIRELOG_CAP: usize = 600;
/// Minimum gap between two history replays sent to the same requester in the
/// same room, matching tc-chat's `ANSWER_THROTTLE_MS`.
const REPLAY_THROTTLE: Duration = Duration::from_secs(3);
/// Delay before this relay's own catch-up `tc-chat:history-request`, giving
/// the room's session (and other subscribers) time to settle after join --
/// matches tc-chat's own `REQUEST_DELAY_MS`.
const HISTORY_REQUEST_DELAY: Duration = Duration::from_millis(700);
const SEND_RETRY_ATTEMPTS: u32 = 3;
const SEND_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Lazily-initialized relay state, `None` when unconfigured
/// (`chat_relay.enabled = false`, or `chat_relay.rooms` empty).
static SERVICE: OnceCell<Option<Arc<ChatRelayService>>> = OnceCell::const_new();

pub struct ChatRelayService {
    /// This node's mistlib id, used only to recognize (and ignore) our own
    /// broadcast `tc-chat:history-request` echo.
    node_id: String,
    /// Rooms actually joined: the subset of `config.chat_relay.rooms` that
    /// passed [`is_valid_room_id`] and joined successfully.
    rooms: Vec<String>,
    /// `<data_dir>/relay`; each room gets its own `<room>/wirelog.jsonl`.
    data_dir: PathBuf,
    /// Guards wirelog file read-modify-write sequences so concurrent IPC
    /// requests and inbound-wire appends never race on the same file.
    wirelog_lock: Mutex<()>,
    /// Per-`(room, requester)` last-replay time (see [`REPLAY_THROTTLE`]).
    replay_throttle: std::sync::Mutex<HashMap<(String, String), Instant>>,
    /// Handle to the daemon's own tokio runtime, captured so the (plain,
    /// non-async) room-handler callback -- invoked from mistlib's own
    /// dispatch thread, not ours -- can spawn async work back onto it. Same
    /// reason `crate::ai`'s and `crate::tunnel`'s services capture one.
    runtime: tokio::runtime::Handle,
}

/// Get the chat relay service, starting it (room joins, handler
/// registration, catch-up history request) on first call. Returns `None`
/// when unconfigured; callers should treat that as "relay disabled", not an
/// error.
async fn ensure_started(state: &Arc<AppState>) -> Result<Option<Arc<ChatRelayService>>> {
    let service = SERVICE
        .get_or_try_init(|| async { init_service(state).await })
        .await?;
    Ok(service.clone())
}

/// Starts the relay in the background if configured, so it is already
/// running for the whole daemon lifetime instead of only springing to life
/// on the first `chat.*` IPC call -- the point of the feature is
/// receiving tc-chat traffic while nobody is asking. A fast no-op when
/// unconfigured. Mirrors `crate::update::spawn_auto_update`'s "spawn once at
/// daemon start, check my own config" shape.
pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(err) = ensure_started(&state).await {
            warn!(%err, "chat relay: failed to start");
        }
    });
}

async fn init_service(state: &Arc<AppState>) -> Result<Option<Arc<ChatRelayService>>> {
    let cfg = state.config().chat_relay;
    if !cfg.enabled || cfg.rooms.is_empty() {
        return Ok(None);
    }

    let identity = crate::identity::current(state)
        .await
        .context("chat relay: loading identity")?;
    let node_id = identity.node_id();

    let data_dir = crate::config::data_dir()
        .context("chat relay: resolving data dir")?
        .join("relay");
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("chat relay: creating {}", data_dir.display()))?;

    let mut rooms = Vec::new();
    for room in &cfg.rooms {
        if !is_valid_room_id(room) {
            warn!(
                room,
                "chat relay: skipping invalid room id (expected 1-64 chars of [A-Za-z0-9_-])"
            );
            continue;
        }
        match crate::net::ensure_started(state, room.clone()).await {
            Ok(_) => rooms.push(room.clone()),
            Err(err) => warn!(%err, room, "chat relay: failed to join room"),
        }
    }
    if rooms.is_empty() {
        return Ok(None);
    }

    let service = Arc::new(ChatRelayService {
        node_id,
        rooms: rooms.clone(),
        data_dir,
        wirelog_lock: Mutex::new(()),
        replay_throttle: std::sync::Mutex::new(HashMap::new()),
        runtime: tokio::runtime::Handle::current(),
    });

    register_handler(service.clone());

    // Catch up on anything missed while this daemon was offline: broadcast a
    // history-request into each room, mirroring tc-chat's own
    // `useHistorySync` -- any peer present (a tc-chat tab, or another mistl
    // relay) will unicast-replay its stored wires back to us.
    for room in &rooms {
        let service = service.clone();
        let room = room.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HISTORY_REQUEST_DELAY).await;
            request_history(&service, &room).await;
        });
    }

    Ok(Some(service))
}

fn register_handler(service: Arc<ChatRelayService>) {
    let rooms: HashSet<String> = service.rooms.iter().cloned().collect();
    crate::net::register_room_handler(move |event_type, room_id, from_id, data| {
        if event_type != crate::net::EVENT_RAW {
            return;
        }
        if !rooms.contains(room_id) {
            return; // some other joined room (ai/stream/tunnel, or a
            // tc-chat room this daemon isn't configured to relay)
        }
        let Ok(value) = serde_json::from_slice::<Value>(data) else {
            return; // not JSON (or truncated); not ours
        };
        let wire_type = match value.get("type").and_then(Value::as_str) {
            Some(t) if t.starts_with("tc-chat:") => t.to_string(),
            _ => return, // not tc-chat's protocol
        };

        let room = room_id.to_string();
        let from = from_id.to_string();
        let raw = data.to_vec();
        let service = service.clone();
        let rt = service.runtime.clone();
        rt.spawn(async move {
            if let Err(err) = handle_wire(&service, &room, &from, &wire_type, raw, value).await {
                warn!(%err, room, from, wire_type, "chat relay: error handling wire");
            }
        });
    });
}

async fn handle_wire(
    service: &Arc<ChatRelayService>,
    room: &str,
    from_node: &str,
    wire_type: &str,
    raw: Vec<u8>,
    value: Value,
) -> Result<()> {
    let Value::Object(obj) = value else {
        return Ok(());
    };

    if wire_type == WIRE_HISTORY_REQUEST {
        return handle_history_request(service, room, from_node, &obj).await;
    }

    if !matches!(
        wire_type,
        WIRE_POST | WIRE_REACTION | WIRE_POST_EDIT | WIRE_POST_DELETE
    ) {
        return Ok(()); // some other/future tc-chat:* wire type; ignore rather than guess
    }

    let Some(id) = obj.get("id").and_then(Value::as_str) else {
        return Ok(());
    };

    if !verify_wire(&obj) {
        warn!(
            id,
            room,
            from = from_node,
            "chat relay: dropping tc-chat wire with an invalid signature"
        );
        return Ok(());
    }

    persist_wire(service, room, id, &raw).await
}

/// Verifies `obj.signature` against every other field, keyed by
/// `obj.fromId`. Mirrors tc-chat's `verifyWire` -- untrusted peer input, so
/// this must reject anything malformed rather than panic.
fn verify_wire(obj: &Map<String, Value>) -> bool {
    let Some(from_id) = obj.get("fromId").and_then(Value::as_str) else {
        return false;
    };
    let Some(signature) = obj.get("signature").and_then(Value::as_str) else {
        return false;
    };
    let payload = stable_json::signing_payload(obj);
    let Ok(sig_bytes) = URL_SAFE_NO_PAD.decode(signature) else {
        return false;
    };
    crate::identity::verify(from_id, payload.as_bytes(), &sig_bytes).unwrap_or(false)
}

async fn handle_history_request(
    service: &Arc<ChatRelayService>,
    room: &str,
    from_node: &str,
    obj: &Map<String, Value>,
) -> Result<()> {
    if from_node == service.node_id {
        return Ok(()); // our own broadcast echoing back
    }
    let Some(requested_room) = obj.get("roomId").and_then(Value::as_str) else {
        return Ok(());
    };
    if requested_room != room {
        return Ok(()); // a request naming a different room than the swarm it arrived on
    }

    {
        let mut throttle = service
            .replay_throttle
            .lock()
            .expect("chat relay replay throttle poisoned");
        let now = Instant::now();
        let key = (room.to_string(), from_node.to_string());
        if let Some(&last) = throttle.get(&key)
            && now.duration_since(last) < REPLAY_THROTTLE
        {
            return Ok(());
        }
        throttle.insert(key, now);
    }

    let lines = {
        let _guard = service.wirelog_lock.lock().await;
        read_wirelog_lines(&wirelog_path(&service.data_dir, room))?
    };
    for line in lines {
        // Best-effort, matching tc-chat's own fire-and-forget replay: one
        // failed unicast shouldn't abort the rest of the log.
        if let Err(err) = crate::net::send_direct(room, from_node, line).await {
            debug!(%err, room, to = from_node, "chat relay: history replay send failed");
        }
    }
    Ok(())
}

/// Broadcasts this relay's own catch-up `tc-chat:history-request` into
/// `room`, retrying transient failures (see the module doc's mistlib
/// gotchas section) a few times before giving up quietly.
async fn request_history(service: &Arc<ChatRelayService>, room: &str) {
    let _ = service; // reserved for future per-service bookkeeping (e.g. metrics)
    let wire = json!({
        "type": WIRE_HISTORY_REQUEST,
        "id": random_id(),
        "roomId": room,
    });
    let Ok(bytes) = serde_json::to_vec(&wire) else {
        return;
    };
    for attempt in 1..=SEND_RETRY_ATTEMPTS {
        match crate::net::send_broadcast(room, bytes.clone()).await {
            Ok(()) => return,
            Err(err) if attempt < SEND_RETRY_ATTEMPTS => {
                debug!(%err, room, attempt, "chat relay: history request broadcast failed; retrying");
                tokio::time::sleep(SEND_RETRY_DELAY).await;
            }
            Err(err) => warn!(%err, room, "chat relay: history request broadcast failed"),
        }
    }
}

/// True if `room` matches tc-chat's own room id shape
/// (`^[A-Za-z0-9_-]{1,64}$`), which this module also relies on to be safe to
/// use directly as a filesystem directory name.
fn is_valid_room_id(room: &str) -> bool {
    !room.is_empty()
        && room.len() <= 64
        && room
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn wirelog_path(data_dir: &Path, room: &str) -> PathBuf {
    data_dir.join(room).join("wirelog.jsonl")
}

/// Reads `path`'s lines (one raw wire's original JSON bytes each), oldest
/// first. A missing file reads as empty.
fn read_wirelog_lines(path: &Path) -> Result<Vec<Vec<u8>>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(bytes
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| line.to_vec())
        .collect())
}

/// Overwrites `path` with `lines`, one per line. Atomic-ish: temp file in the
/// same directory then rename over the target, so a crash mid-write can never
/// leave a truncated log.
fn write_wirelog_lines(path: &Path, lines: &[Vec<u8>]) -> Result<()> {
    let dir = path
        .parent()
        .context("wirelog path has no parent directory")?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;

    let mut body = Vec::new();
    for line in lines {
        body.extend_from_slice(line);
        body.push(b'\n');
    }
    let tmp = path.with_extension("jsonl.tmp");
    std::fs::write(&tmp, &body).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))?;
    Ok(())
}

fn line_id(line: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(line)
        .ok()?
        .get("id")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
}

/// Appends `raw` (the exact bytes received off the wire) to `room`'s log,
/// deduplicated by wire `id` and capped at [`WIRELOG_CAP`] most-recent
/// entries (oldest dropped first). A no-op if `id` is already present --
/// live delivery and a later replay of the same wire are expected to
/// overlap.
async fn persist_wire(
    service: &Arc<ChatRelayService>,
    room: &str,
    id: &str,
    raw: &[u8],
) -> Result<()> {
    if raw.contains(&b'\n') {
        // Every wire this module relays is compact single-line JSON (as
        // produced by `JSON.stringify`/`serde_json::to_vec`, never
        // pretty-printed), so a literal embedded newline would corrupt the
        // JSONL log; refuse rather than silently splitting a line.
        warn!(
            room,
            id, "chat relay: refusing to persist a wire containing a raw newline byte"
        );
        return Ok(());
    }
    let _guard = service.wirelog_lock.lock().await;
    let path = wirelog_path(&service.data_dir, room);
    let mut lines = read_wirelog_lines(&path)?;
    if lines
        .iter()
        .any(|line| line_id(line).as_deref() == Some(id))
    {
        return Ok(());
    }
    lines.push(raw.to_vec());
    if lines.len() > WIRELOG_CAP {
        let excess = lines.len() - WIRELOG_CAP;
        lines.drain(0..excess);
    }
    write_wirelog_lines(&path, &lines)
}

/// Backs `chat.rooms`: configured rooms plus whether each is
/// currently joined.
async fn rooms_status(state: &Arc<AppState>) -> Result<Value> {
    let cfg = state.config().chat_relay;
    let service = ensure_started(state).await?;
    let joined: Vec<String> = service
        .as_ref()
        .map(|s| s.rooms.clone())
        .unwrap_or_default();
    let items: Vec<Value> = cfg
        .rooms
        .iter()
        .map(|room| json!({ "room": room, "joined": joined.contains(room) }))
        .collect();
    Ok(json!({ "enabled": cfg.enabled, "rooms": items }))
}

/// Backs `chat.log`: recently relayed posts/reactions/edits/deletes
/// for `room`, oldest first (newest last). `limit` caps how many are
/// returned, taken from the tail of the stored log.
async fn chat_log(state: &Arc<AppState>, room: &str, limit: usize) -> Result<Value> {
    let service = ensure_started(state).await?.context(
        "chat relay is not enabled (set chat_relay.enabled = true and chat_relay.rooms, then restart the daemon)",
    )?;
    if !service.rooms.iter().any(|r| r == room) {
        bail!("room {room:?} is not a configured/joined chat relay room");
    }

    let lines = {
        let _guard = service.wirelog_lock.lock().await;
        read_wirelog_lines(&wirelog_path(&service.data_dir, room))?
    };
    let limit = limit.max(1);
    let tail: Vec<&Vec<u8>> = lines.iter().rev().take(limit).collect();

    let mut items = Vec::with_capacity(tail.len());
    for line in tail.into_iter().rev() {
        if let Ok(value) = serde_json::from_slice::<Value>(line) {
            items.push(describe_entry(value).await);
        }
    }
    Ok(json!(items))
}

/// Builds one `chat.log` display item from a stored wire.
async fn describe_entry(value: Value) -> Value {
    let Value::Object(obj) = value else {
        return json!({});
    };
    let str_field = |key: &str| obj.get(key).and_then(Value::as_str).map(|s| s.to_string());
    let wire_type = str_field("type").unwrap_or_default();

    let mut item = json!({
        "type": wire_type,
        "id": str_field("id"),
        "fromId": str_field("fromId"),
        "fromName": str_field("fromName"),
        "timestamp": obj.get("timestamp").cloned().unwrap_or(Value::Null),
        // Only signature-verified wires are ever persisted (see
        // `handle_wire`), so anything read back from the log is verified.
        "verified": true,
    });

    match wire_type.as_str() {
        WIRE_POST => {
            item["surface"] = json!(str_field("surface"));
            item["kind"] = json!(str_field("kind"));
            item["cid"] = json!(str_field("cid"));
            item["mimeType"] = json!(str_field("mimeType"));
            item["fileName"] = json!(str_field("fileName"));
            item["fileSize"] = obj.get("fileSize").cloned().unwrap_or(Value::Null);
            if matches!(
                str_field("kind").as_deref(),
                Some("text") | Some("project") | Some("event")
            ) && let Some(cid) = str_field("cid")
            {
                item["text"] = resolve_body_text(&cid).await;
            }
        }
        WIRE_REACTION => {
            item["targetId"] = json!(str_field("targetId"));
            item["emoji"] = json!(str_field("emoji"));
            item["op"] = json!(str_field("op"));
        }
        WIRE_POST_EDIT | WIRE_POST_DELETE => {
            item["surface"] = json!(str_field("surface"));
            item["targetId"] = json!(str_field("targetId"));
        }
        _ => {}
    }

    item
}

/// Best-effort resolution of a structured post's JSON body (title/text) via
/// mistlib's shared, room-replicated content store
/// (`mistlib::app::storage_get` -- distinct from [`crate::storage`], mistl's
/// own local block store; see the module doc). Returns `Value::Null` on any
/// failure (peer offline, block not yet replicated, malformed body, ...), in
/// which case `chat.log` still returns the wire's metadata, just
/// without resolved text.
async fn resolve_body_text(cid: &str) -> Value {
    let cid = cid.to_string();
    // mistlib's sync storage_get block_ons its internal runtime, which
    // panics on a tokio worker thread -- run it on a blocking thread, the
    // same pattern `crate::net::start_engine` uses for other mistlib calls.
    let bytes = match tokio::task::spawn_blocking(move || mistlib::app::storage_get(&cid)).await {
        Ok(Ok(bytes)) => bytes,
        _ => return Value::Null,
    };
    let Ok(body) = serde_json::from_slice::<Value>(&bytes) else {
        return Value::Null;
    };
    body.get("text")
        .cloned()
        .filter(|v| !v.is_null())
        .or_else(|| body.get("title").cloned())
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::RngCore;
    use rand::rngs::OsRng;

    /// Fresh Ed25519 keypair plus its `did:key:z...` string, using the same
    /// derivation as `crate::identity` (multicodec `0xed 0x01` + base58btc)
    /// so `crate::identity::verify` accepts signatures made with it.
    fn test_did_and_signer() -> (String, SigningKey) {
        let signing_key = SigningKey::generate(&mut OsRng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let mut bytes = vec![0xed, 0x01];
        bytes.extend_from_slice(&pubkey);
        let did = format!("did:key:z{}", bs58::encode(bytes).into_string());
        (did, signing_key)
    }

    /// Signs `fields` (everything except `signature`) the way tc-chat's
    /// `signWireFields` does, returning the exact wire bytes a peer would
    /// send.
    fn sign_wire(signing_key: &SigningKey, mut fields: Map<String, Value>) -> Vec<u8> {
        let payload = super::stable_json::stable_stringify(&Value::Object(fields.clone()));
        let signature = signing_key.sign(payload.as_bytes());
        fields.insert(
            "signature".to_string(),
            json!(URL_SAFE_NO_PAD.encode(signature.to_bytes())),
        );
        serde_json::to_vec(&Value::Object(fields)).expect("wire always serializes")
    }

    fn sample_post_fields(did: &str) -> Map<String, Value> {
        let mut fields = Map::new();
        fields.insert("type".into(), json!(WIRE_POST));
        fields.insert("surface".into(), json!("chat"));
        fields.insert("id".into(), json!("wire-1"));
        fields.insert("parentId".into(), Value::Null);
        fields.insert("fromId".into(), json!(did));
        fields.insert("fromName".into(), json!("tester"));
        fields.insert("timestamp".into(), json!(1_234_567u64));
        fields.insert("kind".into(), json!("text"));
        fields.insert("cid".into(), json!("bafy-test"));
        fields
    }

    fn scratch_dir(name: &str) -> PathBuf {
        let mut suffix_bytes = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut suffix_bytes);
        let suffix: String = suffix_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let dir = std::env::temp_dir().join(format!(
            "mistl-chat-relay-test-{name}-{}-{suffix}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_service(data_dir: PathBuf, rooms: Vec<String>) -> Arc<ChatRelayService> {
        Arc::new(ChatRelayService {
            node_id: "test-local-node".into(),
            rooms,
            data_dir,
            wirelog_lock: Mutex::new(()),
            replay_throttle: std::sync::Mutex::new(HashMap::new()),
            runtime: tokio::runtime::Handle::current(),
        })
    }

    #[test]
    fn verify_wire_accepts_a_validly_signed_post_and_rejects_tampering() {
        let (did, signing_key) = test_did_and_signer();
        let raw = sign_wire(&signing_key, sample_post_fields(&did));
        let Value::Object(signed) = serde_json::from_slice::<Value>(&raw).unwrap() else {
            panic!("wire must decode to an object")
        };
        assert!(verify_wire(&signed));

        let mut tampered = signed.clone();
        tampered.insert("cid".into(), json!("bafy-different"));
        assert!(
            !verify_wire(&tampered),
            "tampering with a signed field must invalidate the signature"
        );

        let mut wrong_signer = signed;
        let (other_did, _other_key) = test_did_and_signer();
        wrong_signer.insert("fromId".into(), json!(other_did));
        assert!(
            !verify_wire(&wrong_signer),
            "signature must not verify against a different DID"
        );
    }

    #[test]
    fn verify_wire_rejects_missing_signature_or_from_id() {
        let mut fields = sample_post_fields("did:key:zNotUsed");
        assert!(
            !verify_wire(&fields),
            "no signature field at all must be rejected"
        );

        fields.insert("signature".into(), json!("not-base64url!!"));
        assert!(
            !verify_wire(&fields),
            "malformed base64url signature must be rejected, not panic"
        );
    }

    #[tokio::test]
    async fn post_wire_round_trips_parse_verify_persist_replay_byte_identical() {
        let (did, signing_key) = test_did_and_signer();
        let raw = sign_wire(&signing_key, sample_post_fields(&did));
        let value: Value = serde_json::from_slice(&raw).unwrap();

        let dir = scratch_dir("roundtrip");
        let service = test_service(dir.clone(), vec!["room-a".into()]);

        handle_wire(
            &service,
            "room-a",
            "peer-node",
            WIRE_POST,
            raw.clone(),
            value,
        )
        .await
        .expect("handling a validly-signed post must succeed");

        let lines = read_wirelog_lines(&wirelog_path(&service.data_dir, "room-a")).unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0], raw,
            "replayed bytes must be byte-identical to the original wire"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn handle_wire_drops_an_invalidly_signed_post_without_persisting_it() {
        let (did, signing_key) = test_did_and_signer();
        let mut raw = sign_wire(&signing_key, sample_post_fields(&did));
        // Flip a byte inside the JSON text body (not the signature field) so
        // the signature no longer matches.
        let idx = raw.windows(5).position(|w| w == b"bafy-").unwrap() + 5;
        raw[idx] = b'X';
        let value: Value = serde_json::from_slice(&raw).unwrap();

        let dir = scratch_dir("invalid-sig");
        let service = test_service(dir.clone(), vec!["room-a".into()]);

        handle_wire(&service, "room-a", "peer-node", WIRE_POST, raw, value)
            .await
            .unwrap();

        let lines = read_wirelog_lines(&wirelog_path(&service.data_dir, "room-a")).unwrap();
        assert!(
            lines.is_empty(),
            "an invalidly-signed wire must never be persisted"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn persist_wire_dedupes_by_id() {
        let dir = scratch_dir("dedupe");
        let service = test_service(dir.clone(), vec!["room-a".into()]);

        persist_wire(&service, "room-a", "same-id", br#"{"id":"same-id","n":1}"#)
            .await
            .unwrap();
        persist_wire(&service, "room-a", "same-id", br#"{"id":"same-id","n":1}"#)
            .await
            .unwrap();
        let lines = read_wirelog_lines(&wirelog_path(&service.data_dir, "room-a")).unwrap();
        assert_eq!(lines.len(), 1, "the same wire id must not be stored twice");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn is_valid_room_id_matches_tc_chats_room_id_shape() {
        assert!(is_valid_room_id("tc-chat-room_1"));
        assert!(is_valid_room_id("a"));
        assert!(!is_valid_room_id(""));
        assert!(!is_valid_room_id(&"a".repeat(65)));
        assert!(!is_valid_room_id("has a space"));
        assert!(!is_valid_room_id("../etc/passwd"));
    }
}
