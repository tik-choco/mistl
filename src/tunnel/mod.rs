//! WebRTC P2P tunnel: TCP/UDP port forwarding and a stdio-command bridge over
//! a mistlib room, ported near-verbatim from the standalone `p2p` binary
//! (`C:\Projects\tik-choco\p2p`) so mistl absorbs it as a first-class daemon
//! service instead of a separate executable a user would have to run and
//! keep track of on the side.
//!
//! ## Wire compatibility is deliberate
//!
//! `wire::TunnelMessage` and `rtc`'s `P2pPayload` envelope are never renamed
//! or restructured here (see their own module docs for the exact shape): a
//! `p2p` binary already deployed somewhere must keep being able to tunnel
//! against a `mistl` daemon, and vice versa. Only the seams around that wire
//! -- how a session is built, where its state lives, how a front end drives
//! it -- are re-seated onto mistl's daemon.
//!
//! ## Module map
//!
//! | module | concern |
//! |---|---|
//! | [`rtc`] | mistlib-room-backed signaling/data-channel manager |
//! | [`wire`] | on-the-wire `TunnelMessage` framing (base64 byte fields) |
//! | [`tcp`], [`udp`], [`proxy`] | the actual forwarded data planes |
//! | [`controller`], [`forward_args`], [`forward_runtime`], [`forward_store`] | forward lifecycle: add/remove/list, CLI argument parsing, disk persistence |
//! | [`negotiation`], [`auth`] | the two independent gates: negotiating a forward with a peer, and authorizing an incoming connection |
//! | [`room_store`] | recently-used room ids, for the dashboard/TUI room picker |
//! | [`session`] | [`session::SessionContext`], the shared state every front end (this module's IPC handler, the dashboard, the TUI) drives |
//! | [`chat`], [`stdio`], [`control_shell`] | side channels riding the same `RTCManager` (chat messages, a stdio command bridge, a line-oriented shell) |
//! | [`tui`] | `mistl tunnel tui`: a daemon IPC client, not an in-process session (see its own module doc) |
//!
//! ## Sharing `crate::net` with ai/chat_relay/stream
//!
//! Every other daemon service that talks p2p (`ai`, `chat_relay`, `stream`)
//! goes through the same [`crate::net`] transport, which fans mistlib's one
//! process-wide raw-message handler out to every joined room. Nothing about
//! that changes here: [`rtc::RTCManagerHandle`] joins its room via
//! `net::ensure_started` like everyone else, and sends/receives through
//! `net::send_direct`/`net::send_broadcast`/a registered room handler. The
//! reason the tunnel's own traffic never collides with ai's or the chat
//! relay's on that shared byte stream is purely a matter of shape, the same
//! way those already coexist: tc-chat wires are tagged by a `type` field
//! naming a `tc-chat:*` kind, ai's by `v` + `type`, and the tunnel's `P2pPayload`/
//! `wire::TunnelMessage` envelope is tagged by `kind` (an internally-tagged
//! enum, `#[serde(tag = "kind")]`, snake_case, matching `p2p` byte-for-byte)
//! -- three disjoint discriminants on the same raw byte stream, so each
//! handler's own parse of someone else's bytes either fails outright or
//! (extremely unlikely) decodes into a shape it silently ignores. This is
//! exactly the contract [`crate::net`]'s own module doc describes, just with
//! a fourth participant added.
//!
//! ## What this module (as opposed to `session`) owns
//!
//! [`session::SessionContext`] is the portable, front-end-agnostic session
//! core (also driven directly by [`tui::run`] via daemon IPC, and by the
//! dashboard through the generic `POST /api/call` route -- see
//! `src/daemon/mod.rs::dispatch`). This top-level module owns the *daemon
//! integration* around it:
//!
//! - the process-wide "is a tunnel session running right now" slot (at most
//!   one at a time, unlike ai/chat_relay/stream which each hold their own
//!   independent long-lived singleton -- a tunnel's room/forwards/trust
//!   state is one coherent thing a user starts and stops as a unit, not
//!   several independent per-command services);
//! - [`spawn_background`], the `[tunnel] enabled` autostart hook called from
//!   `daemon::daemon_main`, mirroring `crate::ai::spawn_provide_autoresume`'s
//!   posture: never fails daemon startup, just logs and leaves the tunnel
//!   not running;
//! - the background outcome-drain loop, which upstream's `p2p web` ran
//!   every 500ms (`p2p/src/web.rs`'s `BROADCAST_INTERVAL` loop) so
//!   forward-negotiation answers from peers get applied even between IPC
//!   calls -- ported here verbatim minus the state-broadcast half (no
//!   WebSocket push channel in this design; dashboard/TUI clients poll
//!   `tunnel.status` on their own schedule instead);
//! - [`handle`], implementing every `tunnel.*` IPC command in the
//!   integration contract verbatim -- see each match arm's doc comment below
//!   for its exact request/response shape. W6's dashboard panel and W7's
//!   `mistl tunnel tui` are both plain IPC clients of this same list, so
//!   changing a shape here is a wire break for both.

pub mod auth;
pub mod chat;
pub mod control_shell;
pub mod controller;
pub mod forward_args;
pub mod forward_runtime;
pub mod forward_store;
pub mod negotiation;
pub mod proxy;
pub mod room_store;
pub mod rtc;
pub mod session;
pub mod stdio;
pub mod tcp;
pub mod tui;
pub mod udp;
pub mod wire;

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::daemon::AppState;
use crate::tunnel::auth::{AuthDecision, TrustKey};
use crate::tunnel::controller::{Direction, ForwardSpec, Proto};
use crate::tunnel::negotiation::OutgoingForward;
use crate::tunnel::room_store::{RoomStore, default_room_store_path};
use crate::tunnel::session::{SessionContext, SessionError};

/// How often the background loop drains forward-negotiation outcomes for
/// the requester side (peers that answered a forward we asked them for --
/// see `SessionContext::drain_and_apply_outcomes`). Matches upstream `p2p
/// web`'s `BROADCAST_INTERVAL` (`p2p/src/web.rs`); there is no
/// state-broadcast half here because every front end (dashboard, TUI) polls
/// `tunnel.status` on its own schedule instead of subscribing to a push
/// channel the way upstream's WebSocket clients did.
const DRAIN_INTERVAL: Duration = Duration::from_millis(500);

/// The one running tunnel session, plus the handle to its background
/// outcome-drain loop so [`stop`] can cleanly tear down both together.
struct RunningSession {
    ctx: SessionContext,
    drain_task: JoinHandle<()>,
    /// The remote-command executor serving inbound stdio sessions, present
    /// only when `[tunnel] stdio_enabled` is set *and* `stdio_command` is
    /// non-empty (see `session::maybe_start_stdio_executor`). Held here so
    /// it lives exactly as long as the session and gets shut down by
    /// [`stop`] rather than lingering as an orphaned task able to run
    /// commands for peers after the tunnel was stopped.
    stdio_executor: Option<Arc<crate::tunnel::proxy::Executor>>,
}

/// Process-wide tunnel session slot: `None` until a `tunnel.start` (manual,
/// or automatic via [`spawn_background`]) succeeds, `None` again after a
/// `tunnel.stop`. A `LazyLock<Mutex<..>>`, matching `crate::net`'s own
/// `ROOMS` bookkeeping pattern. Unlike ai/stream (each lazily
/// started by its own first matching IPC call, and then kept running for
/// the rest of the process), the tunnel deliberately does *not* auto-start
/// on an arbitrary `tunnel.*` command -- starting means joining a mistlib
/// room and potentially opening listen sockets, which should only happen
/// when asked for explicitly (`tunnel.start`) or via `[tunnel] enabled` at
/// daemon startup. See [`running_ctx`] for the error every other mutating
/// command gives while this is `None`.
static SESSION: LazyLock<Mutex<Option<RunningSession>>> = LazyLock::new(|| Mutex::new(None));

// Room ids come from `session::generate_room_id` (4 random bytes, hex-encoded
// -- byte-for-byte upstream `p2p`'s `app::generate_room_id` scheme, so a
// generated id looks the same to a human copying it into a `p2p` client on the
// other end). Imported rather than duplicated here so the CLI, the daemon's
// autostart path, and `tunnel.room.set` all mint ids identically.
use crate::tunnel::session::generate_room_id;

/// Autostart hook for `daemon::daemon_main`: joins `[tunnel] room_id`
/// (generating and persisting one first if blank) and restores persisted
/// forwards when `[tunnel] enabled = true`; a no-op (quiet debug log)
/// otherwise. Never fails daemon startup -- mirrors
/// `crate::ai::spawn_provide_autoresume`'s posture exactly: any problem here
/// (data dir unreadable, network join failure, ...) is logged via `warn!`
/// and simply leaves the tunnel not running, exactly as if `mistl tunnel
/// start` had been run by hand and failed. The daemon keeps running either
/// way.
pub fn spawn_background(state: Arc<AppState>) {
    if !state.config().tunnel.enabled {
        info!("tunnel: disabled via config; not auto-starting (run `mistl tunnel start` manually)");
        return;
    }
    tokio::spawn(async move {
        match start(&state).await {
            Ok(result) => info!(%result, "tunnel: auto-started from [tunnel] enabled = true"),
            Err(error) => warn!(
                %error,
                "tunnel: [tunnel] enabled = true, but auto-start failed; the daemon keeps running \
                 without a tunnel session -- fix the problem and run `mistl tunnel start`"
            ),
        }
    });
}

/// Clones the running session's [`SessionContext`] (cheap: every field is an
/// `Arc`-backed handle, see that struct's own doc comment), or a clear error
/// naming the exact command to run instead. Every mutating `tunnel.*`
/// command below needs a live session; see [`SESSION`]'s doc comment for why
/// this never auto-starts one.
async fn running_ctx() -> Result<SessionContext> {
    SESSION
        .lock()
        .await
        .as_ref()
        .map(|session| session.ctx.clone())
        .context("tunnel session is not running; run `mistl tunnel start` first")
}

/// Persists `room` as `[tunnel] room_id` (only writing/saving the config
/// when it actually changed) and records it in the on-disk room-history
/// store used by `tunnel.room.list`. Called by both [`start`] (first join)
/// and `tunnel.room.set` (explicit switch, live or not), so both paths keep
/// the persisted config and the recent-rooms list in sync the same way.
async fn persist_room(state: &Arc<AppState>, room: &str) -> Result<()> {
    let mut cfg = state.config();
    if cfg.tunnel.room_id != room {
        cfg.tunnel.room_id = room.to_string();
        cfg.save().context("tunnel: persisting room_id")?;
        state.set_config(cfg);
    }
    let store = RoomStore::load(default_room_store_path())
        .await
        .context("tunnel: loading room store")?;
    // Best-effort: a room switch that otherwise succeeded must not fail just
    // because the recent-rooms bookkeeping couldn't write to disk.
    if let Err(error) = store.record_use(room).await {
        warn!(%error, %room, "tunnel: failed to record room in the recent-rooms store");
    }
    Ok(())
}

/// Maps a [`SessionError`] to the `anyhow::Error` shape the integration
/// contract promises callers: `NotFound` messages are prefixed `not found: `
/// so a client can tell "the id/target you asked about is already gone"
/// apart from every other failure without string-matching the rest of the
/// message.
fn session_err(err: SessionError) -> anyhow::Error {
    match err {
        SessionError::NotFound(msg) => anyhow::anyhow!("not found: {msg}"),
        SessionError::Invalid(msg) => anyhow::anyhow!("{msg}"),
    }
}

/// `tunnel.start {} -> {"running":true, "already_running":bool, "room":".."}`
///
/// Starts the tunnel session if one isn't already running: resolves the
/// room (persisted `[tunnel] room_id`, generating and persisting a fresh one
/// if blank), builds a [`SessionContext`] (which itself restores any
/// forwards saved under `<data_dir>/tunnel/`), and spawns the background
/// outcome-drain loop (see [`DRAIN_INTERVAL`]). Idempotent: calling it again
/// while already running just reports the current room instead of erroring.
async fn start(state: &Arc<AppState>) -> Result<Value> {
    let mut guard = SESSION.lock().await;
    if let Some(session) = guard.as_ref() {
        let room = session.ctx.room.lock().await.clone();
        return Ok(json!({ "running": true, "already_running": true, "room": room }));
    }

    let cfg = state.config().tunnel;
    let room = if cfg.room_id.trim().is_empty() {
        generate_room_id()
    } else {
        cfg.room_id.clone()
    };

    // `is_server: true` unconditionally. Upstream picked `is_server` from
    // which short-lived, single-purpose CLI subcommand was running (`p2p
    // serve` -> true, `p2p connect` -> false); this daemon's tunnel session
    // is instead one long-lived thing shared by the dashboard, the TUI, and
    // every `tunnel.*` CLI invocation for its whole lifetime, and can hold
    // both `serve` and `connect` forwards side by side at once (direction
    // lives per-forward on `ForwardSpec`, not on the session). That matches
    // upstream's own always-`true` interactive session (`p2p`'s
    // `app::run_tui`) -- the one upstream front end that was likewise a
    // single long-lived session rather than a one-shot process.
    let ctx = SessionContext::build(state, room.clone(), true)
        .await
        .context("tunnel: starting session")?;

    persist_room(state, &room).await?;

    // Remote command execution for peers, off unless `[tunnel]
    // stdio_enabled` is set and `stdio_command` is configured. Started here
    // (rather than inside `SessionContext::build`) so its lifetime is tied
    // to the session slot below, which `stop` tears down.
    let stdio_executor = crate::tunnel::session::maybe_start_stdio_executor(state, &ctx);
    if stdio_executor.is_some() {
        tracing::info!(
            "tunnel: stdio remote execution is ENABLED; approved peers can run the configured command"
        );
    }

    let drain_ctx = ctx.clone();
    let drain_task = tokio::spawn(async move {
        loop {
            drain_ctx.drain_and_apply_outcomes().await;
            tokio::time::sleep(DRAIN_INTERVAL).await;
        }
    });

    *guard = Some(RunningSession {
        ctx,
        drain_task,
        stdio_executor,
    });
    Ok(json!({ "running": true, "already_running": false, "room": room }))
}

/// `tunnel.stop {} -> {"running":false, "was_running":bool}`
///
/// Leaves the room (`RTCManagerHandle::close`, which only ever calls
/// `net::leave_room` -- see that method's own doc comment on why it must
/// never touch `clear_raw_handler`) and aborts the background drain loop.
/// A no-op (still reports `"running": false`) if nothing was running.
async fn stop() -> Result<Value> {
    let mut guard = SESSION.lock().await;
    match guard.take() {
        Some(session) => {
            session.drain_task.abort();
            // Before dropping the transport, so no peer's stdio session
            // outlives the tunnel that authorized it.
            if let Some(executor) = session.stdio_executor.as_ref() {
                executor.close();
            }
            session.ctx.manager.close().await;
            Ok(json!({ "running": false, "was_running": true }))
        }
        None => Ok(json!({ "running": false, "was_running": false })),
    }
}

/// Placeholder snapshot shape for `tunnel.status` while no session is
/// running: every key `Snapshot::to_json` would emit, empty/defaulted, so
/// dashboard/TUI code can read the same field set either way and only needs
/// to special-case the top-level `running` flag itself, not the presence of
/// every other field.
fn empty_snapshot_json(room_id: &str) -> Value {
    json!({
        "self_id": Value::Null,
        "room": room_id,
        "peers": [],
        "forwards": [],
        "pending_auth": [],
        "pending_forwards": [],
        "pending_outgoing": [],
        "trust": [],
        "events": [],
        "notices": [],
        "chat": [],
    })
}

/// `tunnel.status {} -> Snapshot::to_json() + {"enabled":bool,"running":bool}`
///
/// Never starts a session as a side effect (see [`SESSION`]'s doc comment):
/// while nothing is running this returns [`empty_snapshot_json`] instead of
/// `Snapshot`'s real fields, still carrying `enabled`/`running` so a caller
/// can distinguish "not running" from "running with nothing going on yet".
async fn status(state: &Arc<AppState>) -> Result<Value> {
    let cfg = state.config().tunnel;
    let guard = SESSION.lock().await;
    let mut value = match guard.as_ref() {
        Some(session) => session.ctx.snapshot().await.to_json(),
        None => empty_snapshot_json(&cfg.room_id),
    };
    value["enabled"] = json!(cfg.enabled);
    value["running"] = json!(guard.is_some());
    Ok(value)
}

/// `tunnel.room.set {"room":"abc12345"} -> {"room":".."}`
///
/// Works whether or not a session is currently running: if one is, switches
/// it live (`SessionContext::switch_room`, which purges pending
/// auth/negotiation state scoped to the peers left behind); either way
/// persists the room via [`persist_room`] so it's what the *next*
/// `tunnel.start` joins. A blank/whitespace-only (or omitted) `room`
/// generates a fresh one, matching `tunnel.start`'s own "blank -> generate"
/// rule.
async fn room_set(state: &Arc<AppState>, args: Value) -> Result<Value> {
    let requested = args
        .get("room")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let room = requested.unwrap_or_else(generate_room_id);

    {
        let guard = SESSION.lock().await;
        if let Some(session) = guard.as_ref() {
            // A live session has to actually leave the old room and join the
            // new one; `switch_room` needs `state` because rejoining goes
            // through `net::ensure_started`, which owns the shared engine.
            session
                .ctx
                .switch_room(state, room.clone())
                .await
                .context("tunnel.room.set: switching the live session's room")?;
        }
    }
    persist_room(state, &room).await?;
    Ok(json!({ "room": room }))
}

/// `tunnel.room.new {} -> {"room":"a1b2c3d4"}`
///
/// The dashboard's "new room" button: always mints a fresh room id and
/// switches to it, rather than `tunnel.room.set`'s "blank means generate"
/// convenience which a caller could only reach by omitting `room` --
/// unambiguous for a UI action that must never accidentally re-adopt
/// whatever room happens to already be configured. Ported from p2p's `POST
/// /api/room` with a null `room_id` (`post_room` in `p2p/src/web/routes.rs`).
/// Delegates to [`room_set`] with an empty body so both entry points persist
/// and switch identically.
async fn room_new(state: &Arc<AppState>) -> Result<Value> {
    room_set(state, json!({})).await
}

/// `tunnel.room.list {} -> {"rooms":[{"room":"..","last_used_ms":0}]}`
///
/// Reads the on-disk room-history store directly (no live session needed --
/// mirrors upstream's Web UI `GET /api/rooms`, which read the same store
/// independent of `SessionContext`).
async fn room_list() -> Result<Value> {
    let store = RoomStore::load(default_room_store_path())
        .await
        .context("tunnel: loading room store")?;
    let rooms: Vec<Value> = store
        .list()
        .await
        .into_iter()
        .map(|entry| json!({ "room": entry.room_id, "last_used_ms": entry.last_used_ms }))
        .collect();
    Ok(json!({ "rooms": rooms }))
}

/// `tunnel.forward.add {"direction":"serve|connect","proto":"tcp|udp","addr":"..","listen_port":0,"target":".."} -> {"added":true}`
///
/// Adds the forward directly to the running controller -- this is the
/// "I already know I want this forward" path (mirrors upstream `p2p
/// serve`/`p2p connect`'s own direct `controller.add_forward` calls), not
/// the peer-negotiated "ask a peer to let me forward through them" path
/// (that's `tunnel.forward.accept`/`tunnel.forward.reject`, the other side
/// of `SessionContext::resolve_forward`). Requires a running session (see
/// [`running_ctx`]).
async fn forward_add(args: Value) -> Result<Value> {
    let ctx = running_ctx().await?;

    let direction = match args.get("direction").and_then(Value::as_str) {
        Some("serve") => Direction::Serve,
        Some("connect") => Direction::Connect,
        other => bail!(
            "tunnel.forward.add: `direction` must be \"serve\" or \"connect\" (got {other:?})"
        ),
    };
    let proto_name = args
        .get("proto")
        .and_then(Value::as_str)
        .context("tunnel.forward.add requires `proto`")?;
    let proto = Proto::from_name(proto_name)
        .with_context(|| format!("tunnel.forward.add: {proto_name:?}"))?;
    let addr = args
        .get("addr")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let listen_port = args
        .get("listen_port")
        .and_then(Value::as_i64)
        .unwrap_or(-1) as i32;
    let target = args
        .get("target")
        .and_then(Value::as_str)
        .context("tunnel.forward.add requires `target`")?
        .to_string();

    let spec = ForwardSpec {
        direction,
        proto,
        addr,
        listen_port,
        target,
    };
    ctx.controller
        .add_forward(spec.clone())
        .await
        .with_context(|| format!("tunnel.forward.add: {:?}", spec.target))?;
    // Best-effort persistence: a forward that starts working this session
    // but fails to save to disk (full disk, permissions) still works now --
    // it just won't be restored automatically on the next `tunnel.start`,
    // exactly like `SessionContext::resolve_forward`/`apply_forward_outcome`
    // treat their own `forward_store.add` calls upstream.
    if let Err(error) = ctx.forward_store.add(&spec).await {
        warn!(%error, forward_target = %spec.target, "tunnel: forward added but failed to persist; it will not survive a restart");
    }
    Ok(json!({ "added": true }))
}

/// `tunnel.forward.propose` accepts either of two argument shapes:
///
/// * **friendly** (ported byte-for-byte from p2p's `post_forwards` --
///   `{"proto":"tcp|udp","local":"127.0.0.1:8080","remote":"host:port",
///   "peer_id":".."(optional)}`), the shape the dashboard's "propose a
///   forward" form sends. `local`/`remote` are required and trimmed;
///   `listen_port` is parsed out of `local` via
///   [`crate::tunnel::session::parse_addr_port`] (rejecting anything that
///   isn't `host:port`), and `target` is built as
///   `crate::tunnel::forward_args::node_scoped_target(&format!("{proto}:{remote}"),
///   &peer_id)` so the proposal is pinned to whichever peer ends up
///   handling it. When `peer_id` is omitted or blank it is resolved from
///   [`crate::tunnel::rtc::RTCManager::connected_peers`] instead of making
///   the caller look it up first: exactly one connected peer is used
///   automatically, zero is `"no peers connected"`, and two or more is
///   `"multiple peers connected; peer_id is required"`. This single-peer
///   default is the main thing that makes p2p's UI feel easy (propose a
///   forward without first having to go copy a peer id out of the peer
///   list), so it is ported here verbatim rather than only in the frontend.
/// * **explicit** (the original shape, still used by `mistl tunnel
///   propose`) -- `{"peer_id":"..","proto":"tcp|udp","listen_port":0,
///   "local_addr":"..","remote_addr":"..","target":".."}`, where the caller
///   already built `target` and `listen_port` itself and `peer_id` is
///   mandatory.
///
/// Which shape was sent is detected by the presence of `local`/`remote`
/// (friendly) vs. `target` (explicit); a request with neither is rejected.
/// Both shapes converge on the same [`OutgoingForward`] draft and the same
/// `ctx.send_outgoing_forward` call, so the negotiation itself (landing in
/// the peer's `pending_forwards` queue, `tunnel.status`'s
/// `pending_outgoing`, auto-establishing the connect-side forward on
/// acceptance -- see `SessionContext::apply_forward_outcome`) behaves
/// identically either way.
///
/// `-> {"ok":true, "message":"..", "peer_id":"<the peer actually used>",
/// "target":"<the target actually built>"}` -- `peer_id`/`target` are
/// echoed back in both shapes (not just the friendly one) so a caller that
/// omitted `peer_id` can see which peer was auto-selected.
async fn forward_propose(args: Value) -> Result<Value> {
    let ctx = running_ctx().await?;

    let proto = args
        .get("proto")
        .and_then(Value::as_str)
        .unwrap_or("tcp")
        .trim()
        .to_lowercase();
    // Validated up front so an unknown protocol fails here rather than
    // after the proposal has already been sent to the peer.
    Proto::from_name(&proto).with_context(|| format!("tunnel.forward.propose: {proto:?}"))?;

    let friendly_shape = args.get("local").is_some() || args.get("remote").is_some();

    let draft = if friendly_shape {
        let local = args
            .get("local")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim();
        let remote = args
            .get("remote")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim();
        if local.is_empty() || remote.is_empty() {
            bail!("tunnel.forward.propose requires `local` and `remote`");
        }
        let Some(listen_port) = crate::tunnel::session::parse_addr_port(local) else {
            bail!("tunnel.forward.propose: `local` must be host:port");
        };

        let peer_id = match args
            .get("peer_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(p) => p.to_string(),
            None => {
                let peers = ctx.manager.connected_peers().await;
                match peers.len() {
                    0 => bail!("no peers connected"),
                    1 => peers[0].clone(),
                    _ => bail!("multiple peers connected; peer_id is required"),
                }
            }
        };

        let target =
            crate::tunnel::forward_args::node_scoped_target(&format!("{proto}:{remote}"), &peer_id);
        OutgoingForward {
            peer_id,
            proto,
            listen_port,
            local_addr: local.to_string(),
            remote_addr: remote.to_string(),
            target,
        }
    } else {
        let peer_id = args
            .get("peer_id")
            .and_then(Value::as_str)
            .context("tunnel.forward.propose requires `peer_id`")?
            .to_string();
        let target = args
            .get("target")
            .and_then(Value::as_str)
            .context("tunnel.forward.propose requires `local`/`remote` or `target`")?
            .to_string();
        OutgoingForward {
            peer_id,
            proto,
            listen_port: args
                .get("listen_port")
                .and_then(Value::as_i64)
                .unwrap_or(-1) as i32,
            local_addr: args
                .get("local_addr")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            remote_addr: args
                .get("remote_addr")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            target,
        }
    };

    let peer_id = draft.peer_id.clone();
    let target = draft.target.clone();
    let message = ctx
        .send_outgoing_forward(draft)
        .await
        .map_err(session_err)?;
    Ok(json!({ "ok": true, "message": message, "peer_id": peer_id, "target": target }))
}

/// `tunnel.forward.remove {"target":".."} -> {"removed":true}`
async fn forward_remove(args: Value) -> Result<Value> {
    let ctx = running_ctx().await?;
    let target = args
        .get("target")
        .and_then(Value::as_str)
        .context("tunnel.forward.remove requires `target`")?;
    ctx.remove_forward(target).await.map_err(session_err)?;
    Ok(json!({ "removed": true }))
}

/// `tunnel.auth.approve {"id":"..","remember":bool} -> {"ok":true}` and its
/// `tunnel.auth.deny` sibling (`allow = false`). Resolves one entry from
/// `Snapshot.pending_auth` (the per-connection "a peer wants to reach target
/// X through this node" prompt) -- `remember` upgrades the decision to
/// `AllowAlways`/`DenyAlways` so the trust store remembers it for next time,
/// exactly like the TUI's `Y`/`N` keys (vs. plain `y`/`n`) do.
async fn auth_resolve(args: Value, allow: bool) -> Result<Value> {
    let ctx = running_ctx().await?;
    let id = args
        .get("id")
        .and_then(Value::as_u64)
        .context("tunnel.auth.approve/deny requires a numeric `id`")?;
    let remember = args
        .get("remember")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let decision = match (allow, remember) {
        (true, false) => AuthDecision::Allow,
        (true, true) => AuthDecision::AllowAlways,
        (false, false) => AuthDecision::Deny,
        (false, true) => AuthDecision::DenyAlways,
    };
    if ctx.resolve_pending_auth(id, decision).await {
        Ok(json!({ "ok": true }))
    } else {
        Err(anyhow::anyhow!(
            "not found: pending authorization {id} no longer exists"
        ))
    }
}

/// `tunnel.forward.accept {"req_id":".."} -> {"ok":true}` and its
/// `tunnel.forward.reject` sibling (`allow = false`). Resolves one entry
/// from `Snapshot.pending_forwards` -- a *peer's* proposal that this node
/// forward one of its own targets for them (`SessionContext::resolve_forward`),
/// the mirror image of `tunnel.auth.*` above (which gates connections
/// arriving through a forward *this* node already advertises).
/// Accepts either identifier a `Snapshot.pending_forwards` row carries, so a
/// client can echo back whichever field it happened to render: the wire-level
/// `req_id` (a string, minted by the *proposing* peer) or the local numeric
/// `id` (this node's own queue index). The two are not interchangeable --
/// `req_id` is the one that survives a peer reconnect -- so they route to
/// different resolvers rather than being coerced into one.
async fn forward_resolve(args: Value, allow: bool) -> Result<Value> {
    let ctx = running_ctx().await?;
    match args.get("req_id") {
        Some(Value::String(req_id)) => {
            ctx.resolve_forward_by_req_id(req_id, allow)
                .await
                .map_err(session_err)?;
        }
        _ => {
            let id = args
                .get("id")
                .or_else(|| args.get("req_id"))
                .and_then(Value::as_u64)
                .context(
                    "tunnel.forward.accept/reject requires `req_id` (the string a \
                     `pending_forwards` row carries) or a numeric `id`",
                )?;
            ctx.resolve_forward(id, allow).await.map_err(session_err)?;
        }
    }
    Ok(json!({ "ok": true }))
}

/// `tunnel.trust.revoke {"key":..} -> {"ok":bool}`
///
/// `key` accepts either shape a caller naturally has to hand:
///
/// * the **opaque string** a `trust` entry in `tunnel.status` carries as its
///   own `key` field (`SessionContext::trust_key_id`) -- what the dashboard
///   and the TUI echo straight back from the row they rendered, without
///   needing to know how the id is composed; or
/// * the **object** `TrustKey` serializes as
///   (`{"peer_id":"..","forward_key":".."}`) -- what `mistl tunnel trust
///   --revoke <peer_id>@<forward_key>` builds directly from CLI arguments,
///   where no snapshot row was ever fetched to echo.
///
/// `ok` is `false` (not an error) when the entry was already gone, mirroring
/// `TrustStore::remove`'s own `Result<bool>`.
async fn trust_revoke(args: Value) -> Result<Value> {
    let ctx = running_ctx().await?;
    let key_value = args
        .get("key")
        .cloned()
        .context("tunnel.trust.revoke requires `key`")?;
    let removed = match key_value {
        Value::String(key) => ctx.remove_trust_by_key(&key).await.map_err(session_err)?,
        other => {
            let key: TrustKey = serde_json::from_value(other).context(
                "tunnel.trust.revoke: `key` must be either the opaque string a `trust` entry in \
                 `tunnel.status` carries, or the object {\"peer_id\":\"..\",\"forward_key\":\"..\"}",
            )?;
            ctx.remove_trust(&key.peer_id, &key.forward_key)
                .await
                .map_err(session_err)?
        }
    };
    Ok(json!({ "ok": removed }))
}

/// `tunnel.chat.send {"text":".."} -> {"ok":true}`
async fn chat_send(args: Value) -> Result<Value> {
    let ctx = running_ctx().await?;
    let text = args
        .get("text")
        .and_then(Value::as_str)
        .context("tunnel.chat.send requires `text`")?;
    ctx.send_chat(text).await.map_err(session_err)?;
    Ok(json!({ "ok": true }))
}

/// IPC entry point for every `tunnel.*` command (see the integration
/// contract's frozen `tunnel.*` list -- W6's dashboard panel and W7's `mistl
/// tunnel tui` are both plain clients of exactly this list, so every
/// request/response shape here is load-bearing for both).
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "tunnel.status" => status(state).await,
        "tunnel.start" => start(state).await,
        "tunnel.stop" => stop().await,
        "tunnel.room.set" => room_set(state, args).await,
        "tunnel.room.new" => room_new(state).await,
        "tunnel.room.list" => room_list().await,
        "tunnel.forward.add" => forward_add(args).await,
        "tunnel.forward.propose" => forward_propose(args).await,
        "tunnel.forward.remove" => forward_remove(args).await,
        "tunnel.auth.approve" => auth_resolve(args, true).await,
        "tunnel.auth.deny" => auth_resolve(args, false).await,
        "tunnel.forward.accept" => forward_resolve(args, true).await,
        "tunnel.forward.reject" => forward_resolve(args, false).await,
        "tunnel.trust.revoke" => trust_revoke(args).await,
        "tunnel.chat.send" => chat_send(args).await,
        _ => bail!("unknown command: {cmd}"),
    }
}
