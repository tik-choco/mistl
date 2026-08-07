//! Shared session core driving mistl's P2P tunnel: the RTC manager, forward
//! controller, auth/trust bookkeeping, and forward-negotiation queue, plus
//! the async operations that mutate them. Ported from the standalone `p2p`
//! crate's `src/app/session.rs`, which this stayed close to line-for-line;
//! see below for what actually changed and why.
//!
//! ## What changed from upstream `p2p`
//!
//! - **Ownership.** Upstream, this was shared by a TUI and a small axum web
//!   server, both processes the user ran directly and killed with Ctrl-C.
//!   In mistl exactly one `SessionContext` lives inside the daemon (driven
//!   by `crate::tunnel`'s `handle`/`spawn_background`, owned by W5) and is
//!   mutated by IPC commands (`tunnel.*`) from the CLI, the dashboard
//!   (`src/web/assets/index.html`), and `mistl tunnel tui`. There is no
//!   Ctrl-C wait and no interactive stdin loop here anymore: upstream's
//!   `app/{chat,connect,serve,shell}.rs` were CLI entry points that built a
//!   session and then blocked in a foreground loop until the user quit; the
//!   only *logic* worth keeping from them (build a session, decide who gets
//!   auto-approved, run a peer's stdio command) is folded into this file
//!   (`build`, `ConfiguredAuthorizer`, `maybe_start_stdio_executor`) and
//!   `chat.rs`, with the terminal/process-lifecycle plumbing dropped.
//! - **Identity.** `self_id` used to be a per-install UUID minted once and
//!   cached at `$P2P_CONFIG_DIR/node_id` by upstream's `app::
//!   load_or_create_node_id` (see `p2p/src/app.rs`). mistl already has a
//!   stable per-node identity -- the DID-derived node id baked into
//!   `crate::net`'s `Transport` -- so that file, `load_or_create_node_id`,
//!   and its `P2P_CONFIG_DIR`/`%APPDATA%` resolution are **not ported at
//!   all**: `RTCManager::new` now takes `&Arc<AppState>` and derives
//!   `self_id` from the `Transport` that `crate::net::ensure_started`
//!   returns (see `crate::tunnel::rtc`, owned by W1). The wire format is
//!   unaffected -- `self_id` was always just an opaque string peers never
//!   parse, so swapping how it's minted doesn't touch interop with a
//!   deployed `p2p` binary.
//! - **Storage paths.** `trust.json`/`forwards.json` move from
//!   `%APPDATA%\p2p` to `crate::config::data_dir()/tunnel/` (see
//!   `crate::tunnel::auth::default_trust_store_path` and
//!   `crate::tunnel::forward_store::default_forward_store_path`).
//! - **`Snapshot::to_json`**: new. The single source of truth for the
//!   `tunnel.status` IPC payload the dashboard and TUI both read -- see its
//!   doc comment below for the exact shape emitted.
//! - **Config-driven auto-approval**: new. `ConfiguredAuthorizer` (below)
//!   layers `TunnelConfig::auto_accept`/`allow_peers` in front of upstream's
//!   `PendingAuthorizer` queue, since a headless daemon (unlike the
//!   foreground TUI/web UI upstream always had a human watching) may want
//!   some peers auto-approved without ever showing up in `pending_auth`.
//! - **stdio is opt-in.** See `maybe_start_stdio_executor`'s doc comment for
//!   the two-gate model gating whether this node will ever run
//!   `tunnel.stdio_command` locally on a peer's behalf, and the "KNOWN GAP"
//!   note there about gate 2's enforcement point.
//!
//! ## `Snapshot::to_json`'s wire shape (read this before consuming it)
//!
//! This is the *actual* shape emitted by [`Snapshot::to_json`] as of this
//! port -- the integration contract's own sketch was illustrative and drawn
//! up before this file existed; see that method's doc comment for the
//! field-by-field reasoning and the handful of places this diverges from
//! that sketch (mostly: real upstream field names where the sketch used a
//! placeholder, and no invented fields upstream has no data for).
//! ```json
//! {
//!   "self_id": "..", "room": "..", "peers": [".."],
//!   "forwards": [ /* crate::tunnel::controller::ForwardStatus::to_json */ ],
//!   "pending_auth": [{"id":0,"peer_id":"..","forward_key":"..","target_addr":"..","proto":".."}],
//!   "pending_forwards": [{"id":0,"req_id":"..","peer_id":"..","proto":"..","remote_addr":"..","target":".."}],
//!   "pending_outgoing": [{"peer_id":"..","proto":"..","listen_port":0,"local_addr":"..","remote_addr":"..","target":".."}],
//!   "trust": [{"key":"..","peer_id":"..","forward_key":"..","decision":"allow|deny"}],
//!   "events": [{"sequence":0,"timestamp_ms":0,"peer_id":"..","forward_key":"..","target_addr":"..","proto":"..","decision":"allow|deny|allow_always|deny_always","source":"policy|trust_store|pending"}],
//!   "notices": [{"timestamp_ms":0,"kind":"info|error","text":".."}],
//!   "chat": [{"timestamp_ms":0,"peer_id":"..","mine":false,"text":".."}]
//! }
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::config::TunnelConfig;
use crate::daemon::AppState;
use crate::tunnel::auth::{
    AuthAuditLog, AuthDecision, AuthEvent, AuthEventSource, AuthFuture, AuthRequest,
    ConnectionAuthorizer, PendingAuthorization, PendingAuthorizations, PendingAuthorizer,
    SharedAuthorizer, TrustDecision, TrustEntry, TrustKey, TrustStore, default_trust_store_path,
};
use crate::tunnel::controller::{Direction, ForwardController, ForwardSpec, ForwardStatus, Proto};
use crate::tunnel::forward_store::{ForwardStore, default_forward_store_path};
use crate::tunnel::negotiation::{
    ForwardNegotiator, ForwardOutcome, IncomingForward, OutgoingForward,
};
use crate::tunnel::rtc::RTCManager;

/// How many notices [`SessionContext::push_notice`] retains before dropping
/// the oldest -- mirrors `AuthAuditLog`'s capacity-trim pattern, just with a
/// smaller cap since notices are meant to be a short "what just happened"
/// strip, not a full audit trail (that's what the audit log/events pane is
/// for).
const MAX_NOTICES: usize = 50;

/// A user-facing "something happened" message surfaced by every front end
/// (dashboard, TUI) -- e.g. a forward negotiation outcome that would
/// otherwise only be visible via `tracing` logs (invisible at the daemon's
/// default log level in normal use; see [`SessionContext::apply_forward_outcome`]).
#[derive(Debug, Clone, PartialEq)]
pub struct SessionNotice {
    pub timestamp_ms: u128,
    pub kind: NoticeKind,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeKind {
    Info,
    Error,
}

/// How many chat messages [`SessionContext::chat_log`] retains before
/// dropping the oldest -- mirrors [`MAX_NOTICES`]'s trim pattern.
const MAX_CHAT_MESSAGES: usize = 200;

/// A single chat message, either sent by this node (`mine: true`) or
/// received from a peer (`mine: false`). Surfaced to every front end via
/// `Snapshot::chat` / `Snapshot::to_json`'s `"chat"` array.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatMessage {
    pub timestamp_ms: u128,
    pub peer_id: String,
    pub mine: bool,
    pub text: String,
}

/// How long to wait after a peer's `EVENT_LEAVE` before treating pending
/// auth requests / forward negotiations addressed to it as abandoned.
/// Mirrors the grace window `crate::tunnel::tcp::PEER_LEAVE_GRACE` uses for
/// tunnel-close cleanup (duplicated here since that constant isn't `pub`).
const PEER_LEAVE_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// The shared state backing a running tunnel session: the RTC manager, the
/// forward controller, auth/trust bookkeeping, and the forward-negotiation
/// queue. Built once per daemon (see [`SessionContext::build`]) and driven
/// by whichever front end (CLI, dashboard, TUI) issued the last `tunnel.*`
/// IPC command. All fields are `Clone`-cheap (`Arc`-backed handles), so the
/// context itself derives `Clone` for sharing across daemon tasks.
#[derive(Clone)]
pub struct SessionContext {
    pub room: Arc<Mutex<String>>,
    pub manager: RTCManager,
    pub controller: ForwardController,
    pub trust_store: TrustStore,
    pub audit_log: AuthAuditLog,
    pub pending_auth: PendingAuthorizations,
    pub negotiator: ForwardNegotiator,
    pub forward_store: ForwardStore,
    /// Recent user-facing notices (forward-negotiation outcomes, etc), newest
    /// last, capped at [`MAX_NOTICES`]. Shared (not per-clone) so every handle
    /// to this `SessionContext` sees the same feed.
    pub notices: Arc<Mutex<Vec<SessionNotice>>>,
    /// Recent chat messages (sent and received), oldest first, capped at
    /// [`MAX_CHAT_MESSAGES`]. Shared (not per-clone) so every handle to this
    /// `SessionContext` sees the same feed.
    pub chat_log: Arc<Mutex<Vec<ChatMessage>>>,
}

/// A recoverable failure from a session operation. Carries a human-readable
/// message (the same text every front end has always shown for the
/// equivalent failure) plus enough structure for the IPC layer (W5) to pick
/// an error shape -- `NotFound` messages get the `"not found: "` prefix per
/// the integration contract's IPC error convention.
#[derive(Debug, Clone)]
pub enum SessionError {
    /// The referenced pending item / forward no longer exists.
    NotFound(String),
    /// The request was rejected for some other reason (bad input, send
    /// failure, etc).
    Invalid(String),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::NotFound(m) | SessionError::Invalid(m) => write!(f, "{}", m),
        }
    }
}

/// A point-in-time snapshot of everything the front ends display. Plain
/// domain types; [`Snapshot::to_json`] is the one place that adapts this to
/// the wire (`tunnel.status`) shape -- see its doc comment.
pub struct Snapshot {
    pub self_id: String,
    pub room: String,
    pub peers: Vec<String>,
    pub forwards: Vec<ForwardStatus>,
    pub pending_auth: Vec<PendingAuthorization>,
    pub pending_forwards: Vec<IncomingForward>,
    /// Forward requests this node has sent and is still waiting on a peer
    /// response for.
    pub pending_outgoing: Vec<OutgoingForward>,
    pub trust: Vec<TrustEntry>,
    pub events: Vec<AuthEvent>,
    /// Recent user-facing notices, oldest first (see [`SessionContext::notices`]).
    pub notices: Vec<SessionNotice>,
    /// Recent chat messages, oldest first (see [`SessionContext::chat_log`]).
    pub chat: Vec<ChatMessage>,
}

/// Generates an 8-hex-character room id (4 random bytes, hex-encoded).
/// Ported unchanged from upstream `app::generate_room_id`; kept here rather
/// than in a (deleted) `app.rs` since this is the natural shared home for
/// something both the CLI (`mistl tunnel room`) and the daemon's autostart
/// path (`crate::tunnel::spawn_background`, when `tunnel.room_id` is blank)
/// need.
pub fn generate_room_id() -> String {
    let mut buf = [0u8; 4];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut buf);
    hex::encode(buf)
}

/// Builds the layered connection authorizer shared by ordinary forwards
/// (installed on [`ForwardController`] by [`SessionContext::build`]) and
/// stdio gate 2 (re-derived per-call by
/// [`SessionContext::authorize_stdio_peer`]): a trust-store decision wins if
/// one already exists for the (peer, forward-key) pair; otherwise, when
/// `config.auto_accept` is set or `config.allow_peers` names the peer, the
/// request is allowed immediately (recorded to `audit_log` with
/// `AuthEventSource::Policy`, mirroring upstream's CLI-only
/// `PolicyAuthorizer`) without ever reaching a human; anything left falls
/// through to [`PendingAuthorizer`]'s queue, which is what populates
/// `tunnel.status`'s `pending_auth` for the dashboard/TUI's approve/deny
/// actions.
///
/// This composition is new to mistl: upstream's CLI either ran with
/// `PolicyAuthorizer` alone (`p2p serve`, no queue, no possibility of a
/// human being asked) or `PendingAuthorizer` alone (the TUI/web session,
/// always asks). A long-running daemon plausibly wants both at once --
/// most peers auto-approved by policy, occasional unknown ones still
/// surfaced for a human -- so this layers them instead of picking one.
fn build_authorizer(
    config: &TunnelConfig,
    trust_store: TrustStore,
    pending_auth: PendingAuthorizations,
    audit_log: AuthAuditLog,
) -> SharedAuthorizer {
    let trust_store_for_policy = trust_store.clone();
    let pending =
        PendingAuthorizer::shared_with_audit_log(trust_store, pending_auth, audit_log.clone());
    if config.auto_accept || !config.allow_peers.is_empty() {
        Arc::new(ConfiguredAuthorizer {
            auto_accept: config.auto_accept,
            allow_peers: config.allow_peers.iter().cloned().collect(),
            trust_store: trust_store_for_policy,
            audit_log,
            inner: pending,
        })
    } else {
        pending
    }
}

/// The config-driven auto-approve layer `build_authorizer` installs in
/// front of a [`PendingAuthorizer`]. See that function's doc comment for
/// why this composition exists.
struct ConfiguredAuthorizer {
    auto_accept: bool,
    allow_peers: HashSet<String>,
    /// Consulted *before* the auto-approve policy, so an explicitly
    /// remembered decision always wins -- see [`ConfiguredAuthorizer::authorize`].
    trust_store: TrustStore,
    audit_log: AuthAuditLog,
    /// Always a `PendingAuthorizer` in practice (constructed by
    /// `build_authorizer`), but kept as the trait object so this struct
    /// doesn't need to know that concretely.
    inner: SharedAuthorizer,
}

impl ConnectionAuthorizer for ConfiguredAuthorizer {
    /// Trust store first, policy second -- the same precedence upstream's
    /// `PolicyAuthorizer::decide` uses, and the security-relevant ordering:
    /// checking `auto_accept` first would let a blanket "accept everything"
    /// setting silently override a *deny* the user had explicitly chosen to
    /// remember for this (peer, forward-key) pair. When a remembered
    /// decision exists we delegate to `inner`, whose own trust-store lookup
    /// finds the same entry and records it as
    /// [`AuthEventSource::TrustStore`], so the audit trail attributes the
    /// decision to the store rather than to policy.
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async move {
            let remembered = self
                .trust_store
                .get(&TrustKey {
                    peer_id: req.peer_id.clone(),
                    forward_key: req.forward_key.clone(),
                })
                .await
                .is_some();
            if !remembered && (self.auto_accept || self.allow_peers.contains(&req.peer_id)) {
                self.audit_log
                    .record(req, AuthDecision::Allow, AuthEventSource::Policy)
                    .await;
                return AuthDecision::Allow;
            }
            self.inner.authorize(req).await
        })
    }
}

impl SessionContext {
    /// Builds the shared session context: loads the trust/forward stores,
    /// wires the RTC manager's forward-request/response handlers into the
    /// negotiator queue, and re-establishes previously approved forwards.
    /// The single entry point every front end (CLI `tunnel.start`, the
    /// dashboard, `mistl tunnel tui`) goes through, via the daemon's
    /// `crate::tunnel::handle`/`spawn_background` (W5).
    pub async fn build(state: &Arc<AppState>, room: String, is_server: bool) -> Result<Self> {
        let manager = RTCManager::new(state, room.clone(), is_server).await?;
        let trust_store = TrustStore::load(default_trust_store_path()).await?;
        let audit_log = AuthAuditLog::default();
        let pending_auth = PendingAuthorizations::new();
        let negotiator = ForwardNegotiator::new();
        let forward_store = ForwardStore::load(default_forward_store_path()).await?;

        let tunnel_config = state.config().tunnel;
        let authorizer = build_authorizer(
            &tunnel_config,
            trust_store.clone(),
            pending_auth.clone(),
            audit_log.clone(),
        );
        let controller = ForwardController::with_authorizer(manager.clone(), authorizer);

        // Re-establish previously approved forwards.
        for entry in forward_store.list().await {
            match entry.to_spec() {
                Ok(spec) => {
                    if let Err(e) = controller.add_forward(spec).await {
                        warn!("failed to restore forward {}: {}", entry.target, e);
                    }
                }
                Err(e) => warn!("skipping invalid persisted forward: {}", e),
            }
        }

        // Surface incoming forward proposals into the negotiator queue.
        {
            let neg = negotiator.clone();
            manager
                .on_forward_request(move |peer_id, ev| {
                    let neg = neg.clone();
                    tokio::spawn(async move {
                        neg.record_incoming(
                            ev.req_id,
                            peer_id,
                            ev.proto,
                            ev.remote_addr,
                            ev.target,
                        )
                        .await;
                    });
                })
                .await;
        }
        {
            let neg = negotiator.clone();
            manager
                .on_forward_response(move |peer_id, ev| {
                    let neg = neg.clone();
                    tokio::spawn(async move {
                        neg.record_response(&ev.req_id, &peer_id, ev.accepted).await;
                    });
                })
                .await;
        }

        register_peer_leave_purge(
            manager.clone(),
            pending_auth.clone(),
            Some(negotiator.clone()),
        )
        .await;

        let chat_log = Arc::new(Mutex::new(Vec::new()));
        {
            let chat_log = chat_log.clone();
            manager
                .on_chat_message(move |peer_id, text| {
                    let chat_log = chat_log.clone();
                    tokio::spawn(async move {
                        push_chat(&chat_log, peer_id, false, text).await;
                    });
                })
                .await;
        }

        Ok(Self {
            room: Arc::new(Mutex::new(room)),
            manager,
            controller,
            trust_store,
            audit_log,
            pending_auth,
            negotiator,
            forward_store,
            notices: Arc::new(Mutex::new(Vec::new())),
            chat_log,
        })
    }

    /// Appends a notice, trimming the oldest entries past [`MAX_NOTICES`].
    /// The single choke point for user-facing notices so every front end
    /// picks them up via `snapshot()` regardless of which one happened to
    /// trigger the underlying event.
    async fn push_notice(&self, kind: NoticeKind, text: String) {
        let mut notices = self.notices.lock().await;
        notices.push(SessionNotice {
            timestamp_ms: now_ms(),
            kind,
            text,
        });
        if notices.len() > MAX_NOTICES {
            let drop = notices.len() - MAX_NOTICES;
            notices.drain(0..drop);
        }
    }

    /// Gathers current state for display. Cheap-ish (a handful of lock
    /// acquisitions); safe to call frequently (e.g. from a dashboard/TUI
    /// polling loop hitting `tunnel.status`).
    pub async fn snapshot(&self) -> Snapshot {
        let mut events = self.audit_log.list().await;
        // audit_log.list() is chronological (oldest first); keep only the
        // most recent 100.
        if events.len() > 100 {
            let drop = events.len() - 100;
            events.drain(0..drop);
        }
        Snapshot {
            self_id: self.manager.self_id().to_string(),
            room: self.room.lock().await.clone(),
            peers: self.manager.connected_peers().await,
            forwards: self.controller.list_forwards().await,
            pending_auth: self.pending_auth.list().await,
            pending_forwards: self.negotiator.list_incoming().await,
            pending_outgoing: self.negotiator.list_outgoing().await,
            trust: self.trust_store.list().await,
            events,
            notices: self.notices.lock().await.clone(),
            chat: self.chat_log.lock().await.clone(),
        }
    }

    /// Resolves a pending connection authorization (the auth-request pane;
    /// `tunnel.auth.approve`/`tunnel.auth.deny`). Returns `false` if `id` is
    /// stale.
    pub async fn resolve_pending_auth(&self, id: u64, decision: AuthDecision) -> bool {
        self.pending_auth.resolve(id, decision).await
    }

    /// Approves or denies an incoming forward proposal: on approval, creates
    /// a matching serve forward, remembers trust, persists it, and answers
    /// the peer. `id` is the negotiator's local sequence number (see
    /// [`Snapshot::pending_forwards`]'s `IncomingForward::id`); use
    /// [`SessionContext::resolve_forward_by_req_id`] if only the
    /// protocol-level `req_id` is on hand.
    pub async fn resolve_forward(&self, id: u64, allow: bool) -> Result<String, SessionError> {
        let Some(req) = self.negotiator.take_incoming(id).await else {
            return Err(SessionError::NotFound(
                "forward request no longer exists".into(),
            ));
        };

        if !allow {
            self.manager
                .send_forward_response(&req.peer_id, forward_response(&req, false))
                .await
                .map_err(|e| {
                    SessionError::Invalid(format!(
                        "denied locally, but failed to notify peer: {}",
                        e
                    ))
                })?;
            return Ok(format!("denied forward {}", req.target));
        }

        let proto = Proto::from_name(&req.proto)
            .map_err(|e| SessionError::Invalid(format!("invalid proto: {}", e)))?;
        let spec = ForwardSpec {
            direction: Direction::Serve,
            proto,
            addr: req.remote_addr.clone(),
            listen_port: -1,
            target: req.target.clone(),
        };
        self.controller
            .add_forward(spec.clone())
            .await
            .map_err(|e| SessionError::Invalid(format!("add failed: {}", e)))?;
        let _ = self.forward_store.add(&spec).await;
        // Approving the forward also trusts subsequent connections for it.
        let _ = self
            .trust_store
            .remember(
                TrustKey {
                    peer_id: req.peer_id.clone(),
                    forward_key: req.target.clone(),
                },
                TrustDecision::Allow,
            )
            .await;
        // The local forward/trust bookkeeping above already succeeded and is
        // intentionally not rolled back here: if notifying the peer fails,
        // the forward still works locally, and the caller learns via the
        // returned error that the requester wasn't told.
        self.manager
            .send_forward_response(&req.peer_id, forward_response(&req, true))
            .await
            .map_err(|e| {
                SessionError::Invalid(format!(
                    "accepted locally, but failed to notify peer: {}",
                    e
                ))
            })?;
        Ok(format!("accepted forward {}", req.target))
    }

    /// Convenience wrapper around [`SessionContext::resolve_forward`] that
    /// looks a pending proposal up by its protocol-level `req_id` instead of
    /// the negotiator's local `id`. Added for the `tunnel.forward.accept`/
    /// `tunnel.forward.reject` IPC commands (see the integration contract),
    /// which identify a proposal by `req_id` -- the value shown in
    /// `Snapshot::to_json`'s `pending_forwards[].req_id` -- since that's the
    /// stable, protocol-level identifier; the negotiator's `id` is really
    /// only meant for the local pending-list display.
    pub async fn resolve_forward_by_req_id(
        &self,
        req_id: &str,
        allow: bool,
    ) -> Result<String, SessionError> {
        let id = self
            .negotiator
            .list_incoming()
            .await
            .into_iter()
            .find(|f| f.req_id == req_id)
            .map(|f| f.id)
            .ok_or_else(|| SessionError::NotFound("forward request no longer exists".into()))?;
        self.resolve_forward(id, allow).await
    }

    /// Sends an outgoing forward request to a peer and records it so the
    /// matching response can be matched up later.
    ///
    /// Records the outgoing request *before* sending it, not after: if the
    /// peer's response raced in ahead of a post-send `record_outgoing`, it
    /// would arrive as an unknown req id and be silently dropped by
    /// `ForwardNegotiator::record_response`, leaving the requester waiting
    /// forever for an answer that already came back. Recording first closes
    /// that window; if the send itself then fails, the just-recorded entry
    /// is rolled back with `remove_outgoing`.
    pub async fn send_outgoing_forward(
        &self,
        draft: OutgoingForward,
    ) -> Result<String, SessionError> {
        let req_id = uuid::Uuid::new_v4().to_string();
        let ev = crate::tunnel::rtc::ForwardRequestEvent {
            req_id: req_id.clone(),
            proto: draft.proto.clone(),
            remote_addr: draft.remote_addr.clone(),
            target: draft.target.clone(),
        };
        self.negotiator
            .record_outgoing(req_id.clone(), draft.clone())
            .await;
        match self.manager.send_forward_request(&draft.peer_id, ev).await {
            Ok(()) => Ok(format!("request sent: {}", draft.target)),
            Err(e) => {
                self.negotiator.remove_outgoing(&req_id).await;
                Err(SessionError::Invalid(format!("send failed: {}", e)))
            }
        }
    }

    /// Requester-side handling of a peer's answer to a forward we sent:
    /// establishes the local connect-forward on acceptance. The single
    /// choke point through which every forward negotiation outcome passes
    /// (see [`SessionContext::drain_and_apply_outcomes`]), so it's also
    /// where [`SessionNotice`]s are pushed: otherwise a denied/failed/
    /// duplicate outcome would only ever reach `tracing::warn!`, invisible
    /// at the daemon's default log level and leaving a user with no
    /// explanation for a `pending_outgoing` entry that just vanished.
    pub async fn apply_forward_outcome(
        &self,
        outcome: &ForwardOutcome,
    ) -> Result<String, SessionError> {
        let out = &outcome.outgoing;
        if !outcome.accepted {
            let text = match &outcome.reason {
                Some(reason) => format!("forward {} failed: {}", out.target, reason),
                None => format!("peer denied {}", out.target),
            };
            self.push_notice(NoticeKind::Error, text.clone()).await;
            return Ok(text);
        }
        let proto = Proto::from_name(&out.proto)
            .map_err(|e| SessionError::Invalid(format!("invalid proto: {}", e)))?;
        let spec = ForwardSpec {
            direction: Direction::Connect,
            proto,
            addr: out.local_addr.clone(),
            listen_port: out.listen_port,
            target: out.target.clone(),
        };
        if let Err(first_err) = self.controller.add_forward(spec.clone()).await {
            // A forward restored from `forward_store` at startup (see
            // `SessionContext::build`) can occupy the same key (`target`) a
            // freshly negotiated one now wants. A peer's live approval
            // should win over that stale/persisted entry, so replace it and
            // retry once rather than surfacing "forward already exists" for
            // a forward the user just approved.
            let key_taken = self
                .controller
                .list_forwards()
                .await
                .iter()
                .any(|f| f.key == spec.target);
            let retry = if key_taken {
                let _ = self.controller.remove_forward(&spec.target).await;
                self.controller.add_forward(spec.clone()).await
            } else {
                Err(first_err)
            };
            if let Err(e) = retry {
                let text = format!("add failed: {}", e);
                self.push_notice(NoticeKind::Error, text.clone()).await;
                return Err(SessionError::Invalid(text));
            }
        }
        let _ = self.forward_store.add(&spec).await;
        let text = format!("forward established: {}", out.target);
        self.push_notice(NoticeKind::Info, text.clone()).await;
        Ok(text)
    }

    /// Drains any outcomes of forwards this node requested and applies them.
    /// Meant to be driven by a periodic background tick in the daemon (W5)
    /// in place of upstream's per-TUI-frame `refresh()`. User-facing
    /// surfacing happens inside `apply_forward_outcome` itself (via
    /// `SessionNotice`s, visible to every front end); this loop only
    /// additionally logs at the `tracing` level for anyone tailing logs.
    pub async fn drain_and_apply_outcomes(&self) {
        for outcome in self.negotiator.drain_outcomes().await {
            match self.apply_forward_outcome(&outcome).await {
                Ok(msg) => tracing::info!("{}", msg),
                Err(e) => warn!("{}", e),
            }
        }
    }

    /// Removes a forward by its key (the target string the controller uses
    /// as the forward's id) and drops it from the persisted forward store.
    pub async fn remove_forward(&self, key: &str) -> Result<(), SessionError> {
        self.controller
            .remove_forward(key)
            .await
            .map_err(|e| SessionError::Invalid(format!("remove failed: {}", e)))?;
        let _ = self.forward_store.remove(key).await;
        Ok(())
    }

    /// Removes a trust entry by its two-part key. Returns `Ok(true)` if an
    /// entry was removed, `Ok(false)` if no matching entry existed.
    pub async fn remove_trust(
        &self,
        peer_id: &str,
        forward_key: &str,
    ) -> Result<bool, SessionError> {
        self.trust_store
            .remove(&TrustKey {
                peer_id: peer_id.to_string(),
                forward_key: forward_key.to_string(),
            })
            .await
            .map_err(|e| SessionError::Invalid(format!("remove failed: {}", e)))
    }

    /// Removes a trust entry by the single opaque key
    /// [`SessionContext::trust_key_id`] produces -- what `Snapshot::to_json`
    /// embeds under `trust[].key` and what `tunnel.trust.revoke {"key":".."}`
    /// (see the integration contract) identifies an entry by. `TrustKey`
    /// itself has no single-string form upstream (the CLI's `trust remove
    /// <peer-id> <target>` always took two separate words), so this is new,
    /// added purely so the IPC layer has one opaque id to round-trip rather
    /// than needing to know the two-part key's composition.
    pub async fn remove_trust_by_key(&self, key: &str) -> Result<bool, SessionError> {
        let (peer_id, forward_key) = split_trust_key_id(key)
            .ok_or_else(|| SessionError::Invalid("malformed trust key".into()))?;
        self.remove_trust(peer_id, forward_key).await
    }

    /// Combines a `TrustKey`'s two fields into the single opaque string
    /// `remove_trust_by_key` and `Snapshot::to_json`'s `trust[].key` use. A
    /// `\u{1}` separator (a control character that can't appear in a peer id
    /// or forward key in practice) keeps a forward key containing `:`/`@`
    /// from being ambiguous with the separator.
    pub fn trust_key_id(peer_id: &str, forward_key: &str) -> String {
        format!("{peer_id}\u{1}{forward_key}")
    }

    /// Sends a chat message to every connected peer and immediately records
    /// it in the local chat log (own sends never loop back through
    /// `on_chat_message`, unlike a received message).
    pub async fn send_chat(&self, text: &str) -> Result<(), SessionError> {
        let text = text.trim();
        if text.is_empty() {
            return Err(SessionError::Invalid("message must not be empty".into()));
        }
        push_chat(
            &self.chat_log,
            self.manager.self_id().to_string(),
            true,
            text.to_string(),
        )
        .await;
        self.manager.send_chat_to_all(text).await;
        Ok(())
    }

    /// Switches to a different room. Snapshots currently-connected peers
    /// first and purges pending auth/forward-negotiation state scoped to
    /// them: they are about to become unreachable, and
    /// `RTCManager::switch_room` won't emit real per-peer leave
    /// notifications for them (see its doc comment in
    /// `crate::tunnel::rtc`).
    ///
    /// Takes `state` (unlike upstream's `switch_room(new_room)`) purely to
    /// pass through to `RTCManager::switch_room`, whose new signature needs
    /// it to re-join via `crate::net::ensure_started` -- see the
    /// integration contract's frozen `crate::tunnel::rtc` surface.
    pub async fn switch_room(&self, state: &Arc<AppState>, new_room: String) -> Result<()> {
        let old_peers = self.manager.connected_peers().await;
        self.manager.switch_room(state, new_room.clone()).await?;
        *self.room.lock().await = new_room.clone();
        for peer_id in old_peers {
            self.pending_auth.purge_peer(&peer_id).await;
            self.negotiator.purge_peer(&peer_id).await;
        }
        self.push_notice(NoticeKind::Info, format!("switched to room {}", new_room))
            .await;
        Ok(())
    }

    /// Gate 2 of the stdio two-gate model (see
    /// [`maybe_start_stdio_executor`]'s doc comment): runs the *same*
    /// layered connection-authorization decision an ordinary forward from
    /// `peer_id` would get -- trust store, then `auto_accept`/
    /// `allow_peers`, falling back to the pending-approval queue a human
    /// resolves via `tunnel.auth.approve`/`deny` -- against a synthetic
    /// `AuthRequest` for the reserved `"stdio"` forward key, rather than
    /// silently trusting "the peer is in the room" the way upstream's
    /// `proxy::Executor` always did. A first-time unknown peer therefore
    /// shows up in `tunnel.status`'s `pending_auth` exactly like a forward
    /// connection attempt would.
    ///
    /// Re-derives an equivalent authorizer on every call (via
    /// `build_authorizer`) rather than caching one on `self`: doing so keeps
    /// `SessionContext`'s field set exactly as the integration contract
    /// freezes it (`ForwardController` doesn't expose its private
    /// `authorizer` field, and isn't asked to), and it's cheap -- every
    /// input is an `Arc`-backed clone already sitting on `self`, and this
    /// re-reads `state.config().tunnel` fresh so a live `auto_accept`/
    /// `allow_peers` edit takes effect on the very next stdio-open, matching
    /// the rest of mistl's "config changes apply live" convention (see
    /// `crate::daemon::AppState::config`'s doc comment).
    pub async fn authorize_stdio_peer(&self, state: &Arc<AppState>, peer_id: &str) -> bool {
        let tunnel_config = state.config().tunnel;
        let authorizer = build_authorizer(
            &tunnel_config,
            self.trust_store.clone(),
            self.pending_auth.clone(),
            self.audit_log.clone(),
        );
        let req = AuthRequest {
            peer_id: peer_id.to_string(),
            forward_key: STDIO_FORWARD_KEY.to_string(),
            target_addr: tunnel_config.stdio_command.join(" "),
            proto: "stdio".to_string(),
        };
        authorizer.authorize(&req).await.is_allowed()
    }
}

/// Reserved forward-key namespace used for gate 2 of the stdio two-gate
/// model (see [`SessionContext::authorize_stdio_peer`]). Not a real forward
/// -- `ForwardController` never sees it -- it just reuses the same
/// `AuthRequest`/trust-store shape ordinary forwards use so a per-peer
/// "may this peer make me run `stdio_command`?" decision shows up in the
/// same trust list / audit log / pending-approval queue instead of a
/// bespoke mechanism.
pub const STDIO_FORWARD_KEY: &str = "stdio";

/// Combines a `TrustKey`'s two fields back out of
/// [`SessionContext::trust_key_id`]'s opaque string.
fn split_trust_key_id(key: &str) -> Option<(&str, &str)> {
    key.split_once('\u{1}')
}

/// Maps the `tunnel.auth.approve`/`tunnel.auth.deny` IPC commands' `remember`
/// flag onto the four-way `AuthDecision` [`SessionContext::resolve_pending_auth`]
/// expects -- the same mapping `control_shell::pending::parse_resolve` uses
/// for the text-shell's `approve <id> [always]`/`deny <id> [always]`
/// commands, exposed here too since the IPC layer (W5) has the same need
/// and shouldn't have to duplicate it.
// Not currently called from the IPC layer (W5 hasn't wired it up yet); kept
// public and available since it exists precisely for that future caller, and
// covered by its own test below in the meantime.
#[allow(dead_code)]
pub fn auth_decision_from(allow: bool, remember: bool) -> AuthDecision {
    match (allow, remember) {
        (true, true) => AuthDecision::AllowAlways,
        (true, false) => AuthDecision::Allow,
        (false, true) => AuthDecision::DenyAlways,
        (false, false) => AuthDecision::Deny,
    }
}

/// Folds `app::serve::run_serve`'s command-execution branch (the upstream
/// `p2p serve <room> -- <command>` form, which piped a local process's
/// stdio to whichever peer's session opened): starts
/// `crate::tunnel::proxy::Executor` so `state.config().tunnel.stdio_command`
/// runs locally for a peer's stdio session. Returns `None` (and starts
/// nothing) when stdio serving isn't configured.
///
/// Returns the `Arc<Executor>` (rather than just spawning and discarding a
/// handle) so the caller can keep it alongside its `SessionContext` and call
/// `.close()` on `tunnel.stop`/room switch -- `Executor` isn't one of
/// `SessionContext`'s frozen fields, so the daemon (W5) owns tracking this
/// alongside the context, the same way it must for
/// `crate::tunnel::stdio::Bridge` (connect-mode's stdio piping; see
/// `Bridge::spawn`).
///
/// ## The two-gate model (why this isn't just "start it if configured")
///
/// Upstream, running a local command for a remote peer was something a
/// human explicitly opted into every single time, in the foreground, by
/// typing `p2p serve ... -- <command>` and watching the terminal -- there
/// was nothing to gate beyond that one-time act, and *any* peer already in
/// the room could open a stdio session and get piped output once it
/// started. mistl's tunnel is a long-running daemon service instead
/// (`tunnel.enabled` can autostart it at boot), so an equivalent explicit,
/// standing opt-in has to live in config rather than in a keystroke, and
/// per-peer trust has to be checked explicitly rather than "being in the
/// room" implicitly meaning "trusted":
///
/// 1. **`state.config().tunnel.stdio_enabled`** (default `false`): a global
///    switch the operator sets once. Without it, this function returns
///    `None` and `crate::tunnel::proxy::Executor` is never even
///    constructed -- no `on_stdio_*` hooks are installed on `manager` at
///    all, so no peer in the room can trigger process execution regardless
///    of trust state. This gate is fully enforced by this function.
/// 2. **Per-peer trust** ([`SessionContext::authorize_stdio_peer`]): even
///    with the switch on, a specific peer's stdio-open still runs through
///    the same trust-store/`auto_accept`/`allow_peers`/pending-approval
///    decision an ordinary forward would get. This function supplies that
///    check to the executor as a [`crate::tunnel::proxy::StdioAuthorizer`]
///    closure, which `Executor::handle_open` consults *before* claiming the
///    session slot or spawning anything -- so a first-time unknown peer
///    lands in `tunnel.status`'s `pending_auth` for a human to resolve
///    instead of getting a shell, and a denied peer leaves no trace.
///
/// Both gates are enforced. Upstream `p2p` had neither: it ran the command
/// for whichever peer opened first.
pub fn maybe_start_stdio_executor(
    state: &Arc<AppState>,
    ctx: &SessionContext,
) -> Option<Arc<crate::tunnel::proxy::Executor>> {
    let config = state.config().tunnel;
    if !config.stdio_enabled || config.stdio_command.is_empty() {
        return None;
    }
    // Gate 2: hand the executor a per-peer authorization closure so every
    // stdio-open goes through `authorize_stdio_peer` -- the same trust
    // store, `auto_accept`/`allow_peers` policy, and human-resolved pending
    // queue an ordinary forward connection faces. `state` is re-read inside
    // the closure on every call (not captured as a config snapshot) so a
    // live settings edit applies to the very next stdio-open.
    let auth_ctx = ctx.clone();
    let auth_state = state.clone();
    let authorizer: crate::tunnel::proxy::StdioAuthorizer = Arc::new(move |peer_id: String| {
        let ctx = auth_ctx.clone();
        let state = auth_state.clone();
        Box::pin(async move { ctx.authorize_stdio_peer(&state, &peer_id).await })
    });
    let executor = Arc::new(crate::tunnel::proxy::Executor::with_authorizer(
        ctx.manager.clone(),
        config.stdio_command.clone(),
        authorizer,
    ));
    let exec = executor.clone();
    tokio::spawn(async move {
        if let Err(e) = exec.run().await {
            warn!("stdio proxy executor error: {}", e);
        }
    });
    Some(executor)
}

/// Registers a hook that ties a peer's departure to cleanup of state keyed
/// to it: pending connection authorizations always, and (when a negotiator
/// is supplied) in-flight forward proposals/requests.
pub async fn register_peer_leave_purge(
    manager: RTCManager,
    pending_auth: PendingAuthorizations,
    negotiator: Option<ForwardNegotiator>,
) {
    let hook_manager = manager.clone();
    manager
        .on_peer_leave(move |peer_id, epoch| {
            let manager = hook_manager.clone();
            let pending_auth = pending_auth.clone();
            let negotiator = negotiator.clone();
            tokio::spawn(purge_after_grace(
                peer_id,
                epoch,
                manager,
                pending_auth,
                negotiator,
            ));
        })
        .await;
}

/// Waits out `PEER_LEAVE_GRACE`, then -- unless `peer_id` rejoined in the
/// meantime (its session epoch has moved on from `epoch_at_leave`) --
/// denies/removes anything still keyed to it. Split out from
/// `register_peer_leave_purge` so it can be driven directly in tests
/// without depending on a real `EVENT_LEAVE`/`EVENT_JOIN` round trip.
async fn purge_after_grace(
    peer_id: String,
    epoch_at_leave: u64,
    manager: RTCManager,
    pending_auth: PendingAuthorizations,
    negotiator: Option<ForwardNegotiator>,
) {
    tokio::time::sleep(PEER_LEAVE_GRACE).await;
    if manager.peer_epoch(&peer_id).await != epoch_at_leave {
        // The peer rejoined within the grace window; nothing to purge.
        return;
    }

    let auth_count = pending_auth.purge_peer(&peer_id).await;
    let (incoming, outgoing) = match &negotiator {
        Some(neg) => neg.purge_peer(&peer_id).await,
        None => (0, 0),
    };
    if auth_count > 0 || incoming > 0 || outgoing > 0 {
        info!(
            "purged state for departed peer {}: {} pending auth, {} incoming forwards, {} outgoing forwards",
            peer_id, auth_count, incoming, outgoing
        );
    }
}

/// Appends a chat message, trimming the oldest entries past
/// [`MAX_CHAT_MESSAGES`]. Called both from `SessionContext::send_chat` (our
/// own outbound messages, `mine: true`) and from the `on_chat_message` hook
/// wired in `SessionContext::build` (peer-received messages, `mine: false`)
/// -- a free function rather than a method since the hook closure doesn't
/// have a `self`.
async fn push_chat(log: &Arc<Mutex<Vec<ChatMessage>>>, peer_id: String, mine: bool, text: String) {
    let mut chat = log.lock().await;
    chat.push(ChatMessage {
        timestamp_ms: now_ms(),
        peer_id,
        mine,
        text,
    });
    if chat.len() > MAX_CHAT_MESSAGES {
        let drop = chat.len() - MAX_CHAT_MESSAGES;
        chat.drain(0..drop);
    }
}

fn forward_response(
    req: &IncomingForward,
    accepted: bool,
) -> crate::tunnel::rtc::ForwardResponseEvent {
    crate::tunnel::rtc::ForwardResponseEvent {
        req_id: req.req_id.clone(),
        target: req.target.clone(),
        accepted,
    }
}

/// Parses the port from an `ip:port` string. Shared by every front end's
/// add-forward form.
// Ported shared helper. `tui::app::parse_port` grew its own self-contained
// copy instead of depending on this cross-worker API (see that function's
// doc comment), but `tunnel::forward_propose`'s friendly argument shape
// (the `local`/`remote` p2p-style form) now calls this one directly to pull
// `listen_port` out of `local`, so it is no longer dead code.
pub fn parse_addr_port(addr: &str) -> Option<i32> {
    addr.rsplit(':').next()?.parse::<i32>().ok()
}

/// Current Unix time in milliseconds.
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

impl Snapshot {
    /// Renders this snapshot as the `tunnel.status` IPC payload -- the
    /// single source of truth for the dashboard (`src/web/assets/index.html`,
    /// W6) and the TUI (`src/tunnel/tui.rs`, W7). Both are expected to read
    /// this shape exclusively through the IPC command, never by depending
    /// on `SessionContext`/`Snapshot` directly, so this is intentionally
    /// hand-written (not a `Serialize` derive on `Snapshot`) to keep the
    /// wire shape documented in exactly one place regardless of how the
    /// backing structs evolve. `state.rs`'s `AppState::config`/`enabled`/
    /// `running` wrapper fields (per the integration contract's
    /// `tunnel.status` response shape) are added by the caller (W5), not
    /// here -- this is just the session's own data.
    ///
    /// See the module-level doc comment for the exact JSON emitted. Notes
    /// on where this diverges from the integration contract's illustrative
    /// sketch (written before this method existed):
    /// - `forwards` is exactly `crate::tunnel::controller::ForwardStatus::
    ///   to_json`'s shape (see that method's own doc comment) rather than
    ///   the sketch's flattened `{direction,proto,addr,...}` -- reusing it
    ///   avoids a second, drifting definition of the same data.
    /// - `pending_auth`/`pending_forwards` use the real upstream `id` (a
    ///   `u64` local sequence number) alongside `req_id` where one exists,
    ///   rather than only `req_id`; `resolve_forward_by_req_id` lets a
    ///   caller resolve by whichever it has.
    /// - `pending_outgoing` has no `req_id`/`sent_ms`: upstream's
    ///   `ForwardNegotiator::list_outgoing` never surfaces the req id (it's
    ///   only the negotiator's internal map key) or a sent timestamp, and
    ///   adding either would mean changing `negotiation.rs` (owned by W3).
    /// - `trust` entries carry a synthesized `key` (see
    ///   `SessionContext::trust_key_id`) alongside the real `peer_id`/
    ///   `forward_key` pair, so `tunnel.trust.revoke {"key":".."}` has a
    ///   single opaque identifier to round-trip.
    pub fn to_json(&self) -> Value {
        json!({
            "self_id": self.self_id,
            "room": self.room,
            "peers": self.peers,
            "forwards": self.forwards.iter().map(ForwardStatus::to_json).collect::<Vec<_>>(),
            "pending_auth": self.pending_auth.iter().map(pending_auth_json).collect::<Vec<_>>(),
            "pending_forwards": self.pending_forwards.iter().map(incoming_forward_json).collect::<Vec<_>>(),
            "pending_outgoing": self.pending_outgoing.iter().map(outgoing_forward_json).collect::<Vec<_>>(),
            "trust": self.trust.iter().map(trust_entry_json).collect::<Vec<_>>(),
            "events": self.events.iter().map(auth_event_json).collect::<Vec<_>>(),
            "notices": self.notices.iter().map(notice_json).collect::<Vec<_>>(),
            "chat": self.chat.iter().map(chat_message_json).collect::<Vec<_>>(),
        })
    }
}

fn pending_auth_json(item: &PendingAuthorization) -> Value {
    json!({
        "id": item.id,
        "peer_id": item.request.peer_id,
        "forward_key": item.request.forward_key,
        "target_addr": item.request.target_addr,
        "proto": item.request.proto,
    })
}

fn incoming_forward_json(item: &IncomingForward) -> Value {
    json!({
        "id": item.id,
        "req_id": item.req_id,
        "peer_id": item.peer_id,
        "proto": item.proto,
        "remote_addr": item.remote_addr,
        "target": item.target,
    })
}

fn outgoing_forward_json(item: &OutgoingForward) -> Value {
    json!({
        "peer_id": item.peer_id,
        "proto": item.proto,
        "listen_port": item.listen_port,
        "local_addr": item.local_addr,
        "remote_addr": item.remote_addr,
        "target": item.target,
    })
}

fn trust_entry_json(entry: &TrustEntry) -> Value {
    json!({
        "key": SessionContext::trust_key_id(&entry.key.peer_id, &entry.key.forward_key),
        "peer_id": entry.key.peer_id,
        "forward_key": entry.key.forward_key,
        "decision": match entry.decision {
            TrustDecision::Allow => "allow",
            TrustDecision::Deny => "deny",
        },
    })
}

fn auth_event_json(ev: &AuthEvent) -> Value {
    json!({
        "sequence": ev.sequence,
        "timestamp_ms": ev.timestamp_ms as u64,
        "peer_id": ev.peer_id,
        "forward_key": ev.forward_key,
        "target_addr": ev.target_addr,
        "proto": ev.proto,
        "decision": match ev.decision {
            AuthDecision::Allow => "allow",
            AuthDecision::Deny => "deny",
            AuthDecision::AllowAlways => "allow_always",
            AuthDecision::DenyAlways => "deny_always",
        },
        "source": match ev.source {
            AuthEventSource::Policy => "policy",
            AuthEventSource::TrustStore => "trust_store",
            AuthEventSource::Pending => "pending",
        },
    })
}

fn notice_json(n: &SessionNotice) -> Value {
    json!({
        "timestamp_ms": n.timestamp_ms as u64,
        "kind": match n.kind {
            NoticeKind::Info => "info",
            NoticeKind::Error => "error",
        },
        "text": n.text,
    })
}

fn chat_message_json(m: &ChatMessage) -> Value {
    json!({
        "timestamp_ms": m.timestamp_ms as u64,
        "peer_id": m.peer_id,
        "mine": m.mine,
        "text": m.text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tunnel::auth::AuthRequest;

    #[test]
    fn parses_port_from_addr() {
        assert_eq!(parse_addr_port("127.0.0.1:8080"), Some(8080));
        assert_eq!(parse_addr_port("8080"), Some(8080));
        assert_eq!(parse_addr_port(""), None);
        assert_eq!(parse_addr_port("127.0.0.1:abc"), None);
    }

    #[test]
    fn session_error_display_matches_inner_message() {
        let e = SessionError::NotFound("forward request no longer exists".into());
        assert_eq!(e.to_string(), "forward request no longer exists");
        let e = SessionError::Invalid("invalid proto: nope".into());
        assert_eq!(e.to_string(), "invalid proto: nope");
    }

    #[test]
    fn auth_decision_from_maps_allow_deny_and_remember() {
        assert_eq!(auth_decision_from(true, false), AuthDecision::Allow);
        assert_eq!(auth_decision_from(true, true), AuthDecision::AllowAlways);
        assert_eq!(auth_decision_from(false, false), AuthDecision::Deny);
        assert_eq!(auth_decision_from(false, true), AuthDecision::DenyAlways);
    }

    #[test]
    fn trust_key_id_round_trips() {
        let key = SessionContext::trust_key_id("peer-1", "tcp:80");
        assert_eq!(split_trust_key_id(&key), Some(("peer-1", "tcp:80")));
    }

    fn sample_request(peer_id: &str) -> AuthRequest {
        AuthRequest {
            peer_id: peer_id.to_string(),
            forward_key: "tcp:80".to_string(),
            target_addr: "127.0.0.1:80".to_string(),
            proto: "tcp".to_string(),
        }
    }

    /// Builds a `SessionContext` backed by inert/for-test components (no
    /// real network, disk state scoped to a throwaway temp dir) so
    /// `apply_forward_outcome` and friends can be exercised directly,
    /// without going through `SessionContext::build` (which needs a real
    /// `Arc<AppState>` -- see `test_app_state` below for the one test that
    /// does).
    async fn test_session_context() -> SessionContext {
        let dir = std::env::temp_dir().join(format!(
            "mistl-tunnel-session-test-{}",
            uuid::Uuid::new_v4()
        ));
        SessionContext {
            room: Arc::new(Mutex::new("room".to_string())),
            manager: RTCManager::for_test("self"),
            controller: ForwardController::new_inert(),
            trust_store: TrustStore::load(dir.join("trust.json")).await.unwrap(),
            audit_log: AuthAuditLog::default(),
            pending_auth: PendingAuthorizations::new(),
            negotiator: ForwardNegotiator::new(),
            forward_store: ForwardStore::load(dir.join("forwards.json")).await.unwrap(),
            notices: Arc::new(Mutex::new(Vec::new())),
            chat_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sample_outgoing_forward(target: &str) -> OutgoingForward {
        OutgoingForward {
            peer_id: "peer-1".to_string(),
            proto: "tcp".to_string(),
            listen_port: 8080,
            local_addr: "127.0.0.1:8080".to_string(),
            remote_addr: "127.0.0.1:80".to_string(),
            target: target.to_string(),
        }
    }

    #[tokio::test]
    async fn apply_forward_outcome_pushes_info_notice_on_success() {
        let ctx = test_session_context().await;
        let outcome = ForwardOutcome {
            outgoing: sample_outgoing_forward("tcp:127.0.0.1:80"),
            accepted: true,
            reason: None,
        };

        let msg = ctx.apply_forward_outcome(&outcome).await.unwrap();
        assert_eq!(msg, "forward established: tcp:127.0.0.1:80");

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].kind, NoticeKind::Info);
        assert_eq!(notices[0].text, "forward established: tcp:127.0.0.1:80");
    }

    #[tokio::test]
    async fn apply_forward_outcome_pushes_error_notice_on_add_failure() {
        let ctx = test_session_context().await;
        // An empty target is rejected by `ForwardController::add_forward`
        // ("forward target must not be empty") and isn't a duplicate-key
        // situation, so this exercises the plain add-failure path.
        let outcome = ForwardOutcome {
            outgoing: sample_outgoing_forward(""),
            accepted: true,
            reason: None,
        };

        let err = ctx.apply_forward_outcome(&outcome).await.unwrap_err();
        assert!(err.to_string().contains("add failed"));

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].kind, NoticeKind::Error);
        assert!(notices[0].text.contains("add failed"));
    }

    #[tokio::test]
    async fn apply_forward_outcome_pushes_error_notice_on_denied_and_failed() {
        let ctx = test_session_context().await;

        let denied = ForwardOutcome {
            outgoing: sample_outgoing_forward("tcp:127.0.0.1:80"),
            accepted: false,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&denied).await.unwrap();
        assert_eq!(msg, "peer denied tcp:127.0.0.1:80");

        let failed = ForwardOutcome {
            outgoing: sample_outgoing_forward("tcp:127.0.0.1:81"),
            accepted: false,
            reason: Some("peer disconnected".to_string()),
        };
        let msg = ctx.apply_forward_outcome(&failed).await.unwrap();
        assert_eq!(msg, "forward tcp:127.0.0.1:81 failed: peer disconnected");

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 2);
        assert!(notices.iter().all(|n| n.kind == NoticeKind::Error));
    }

    #[tokio::test]
    async fn apply_forward_outcome_replaces_stale_duplicate_key() {
        let ctx = test_session_context().await;
        let target = "tcp:127.0.0.1:80";

        // Stands in for a forward restored from `forward_store` at startup
        // (see `SessionContext::build`) that happens to share a key with a
        // forward now being negotiated fresh.
        ctx.controller
            .add_forward(ForwardSpec {
                direction: Direction::Serve,
                proto: Proto::Tcp,
                addr: "stale:1".to_string(),
                listen_port: -1,
                target: target.to_string(),
            })
            .await
            .unwrap();

        let outcome = ForwardOutcome {
            outgoing: sample_outgoing_forward(target),
            accepted: true,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&outcome).await.unwrap();
        assert_eq!(msg, format!("forward established: {}", target));

        let statuses = ctx.controller.list_forwards().await;
        assert_eq!(
            statuses.len(),
            1,
            "the stale entry must be replaced, not duplicated"
        );
        assert_eq!(statuses[0].spec.direction, Direction::Connect);
        assert_eq!(statuses[0].spec.addr, "127.0.0.1:8080");

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].kind, NoticeKind::Info);
    }

    #[tokio::test]
    async fn apply_forward_outcome_scoped_targets_on_different_peers_coexist() {
        let ctx = test_session_context().await;

        // Regression test for the original node-scoping bug: two forwards to
        // the *same* remote addr but on different peer nodes previously
        // collided on the unscoped target key (second replaced the first).
        // With `@node` scoping baked into `target`, they must coexist.
        let mut on_a = sample_outgoing_forward("tcp:127.0.0.1:22@node-a");
        on_a.listen_port = 10022;
        let outcome_a = ForwardOutcome {
            outgoing: on_a,
            accepted: true,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&outcome_a).await.unwrap();
        assert_eq!(msg, "forward established: tcp:127.0.0.1:22@node-a");

        let mut on_b = sample_outgoing_forward("tcp:127.0.0.1:22@node-b");
        on_b.listen_port = 10023;
        let outcome_b = ForwardOutcome {
            outgoing: on_b,
            accepted: true,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&outcome_b).await.unwrap();
        assert_eq!(msg, "forward established: tcp:127.0.0.1:22@node-b");

        let statuses = ctx.controller.list_forwards().await;
        assert_eq!(
            statuses.len(),
            2,
            "forwards to the same remote addr but scoped to different peers must coexist"
        );
    }

    #[tokio::test]
    async fn apply_forward_outcome_replaces_same_scoped_target() {
        let ctx = test_session_context().await;
        let target = "tcp:127.0.0.1:22@node-a";

        // The existing replace-on-duplicate-key behavior, now exercised
        // per-node: a second accepted outcome for the *same* scoped target
        // still replaces the first rather than duplicating it.
        let mut first = sample_outgoing_forward(target);
        first.listen_port = 10022;
        let outcome_first = ForwardOutcome {
            outgoing: first,
            accepted: true,
            reason: None,
        };
        ctx.apply_forward_outcome(&outcome_first).await.unwrap();

        let mut second = sample_outgoing_forward(target);
        second.listen_port = 10099;
        let outcome_second = ForwardOutcome {
            outgoing: second,
            accepted: true,
            reason: None,
        };
        ctx.apply_forward_outcome(&outcome_second).await.unwrap();

        let statuses = ctx.controller.list_forwards().await;
        assert_eq!(
            statuses.len(),
            1,
            "second outcome for the same scoped target must replace, not duplicate"
        );
        assert_eq!(statuses[0].spec.listen_port, 10099);
    }

    // --- purge_after_grace: the epoch-changed => no-purge wiring rule -------
    //
    // These drive `purge_after_grace` directly rather than through a real
    // `EVENT_LEAVE`/`EVENT_JOIN` round trip: simulating an actual mistlib
    // rejoin (which would bump `RTCManagerHandle::peer_epoch`) requires
    // internals not exposed outside `crate::tunnel::rtc`. Passing
    // `epoch_at_leave` directly exercises the same comparison
    // `register_peer_leave_purge`'s real hook relies on:
    // `manager.peer_epoch(peer_id) != epoch_at_leave` => treat as rejoined.

    #[tokio::test(start_paused = true)]
    async fn purge_after_grace_skips_when_epoch_moved_on() {
        let manager = RTCManager::for_test("self");
        let pending_auth = PendingAuthorizations::new();
        let receiver = pending_auth
            .enqueue_for_test(sample_request("peer-1"))
            .await;
        let negotiator = ForwardNegotiator::new();
        negotiator
            .record_incoming(
                "req-1".into(),
                "peer-1".into(),
                "tcp".into(),
                "127.0.0.1:80".into(),
                "tcp:127.0.0.1:80".into(),
            )
            .await;

        // `manager` never actually joined peer-1 (its epoch stays 0); a
        // mismatched `epoch_at_leave` stands in for "the peer rejoined
        // (bumping its epoch) before the grace window elapsed".
        purge_after_grace(
            "peer-1".to_string(),
            1,
            manager,
            pending_auth.clone(),
            Some(negotiator.clone()),
        )
        .await;

        assert_eq!(
            pending_auth.list().await.len(),
            1,
            "a peer whose epoch moved on must not have its pending auth purged"
        );
        assert_eq!(
            negotiator.list_incoming().await.len(),
            1,
            "a peer whose epoch moved on must not have its forward proposals purged"
        );
        drop(receiver);
    }

    #[tokio::test(start_paused = true)]
    async fn purge_after_grace_purges_when_epoch_unchanged() {
        let manager = RTCManager::for_test("self");
        let pending_auth = PendingAuthorizations::new();
        let receiver = pending_auth
            .enqueue_for_test(sample_request("peer-1"))
            .await;
        let negotiator = ForwardNegotiator::new();
        negotiator
            .record_incoming(
                "req-1".into(),
                "peer-1".into(),
                "tcp".into(),
                "127.0.0.1:80".into(),
                "tcp:127.0.0.1:80".into(),
            )
            .await;

        // `epoch_at_leave` (0) matches the never-joined manager's current
        // epoch for peer-1 (also 0): no rejoin happened.
        purge_after_grace(
            "peer-1".to_string(),
            0,
            manager,
            pending_auth.clone(),
            Some(negotiator.clone()),
        )
        .await;

        assert!(pending_auth.list().await.is_empty());
        assert_eq!(receiver.await.unwrap(), AuthDecision::Deny);
        assert!(negotiator.list_incoming().await.is_empty());
    }

    // --- chat -----------------------------------------------------------

    #[tokio::test]
    async fn send_chat_pushes_mine_entry_with_own_id() {
        let ctx = test_session_context().await;
        ctx.send_chat("hello").await.unwrap();

        let chat = ctx.chat_log.lock().await.clone();
        assert_eq!(chat.len(), 1);
        assert!(chat[0].mine);
        assert_eq!(chat[0].peer_id, ctx.manager.self_id());
        assert_eq!(chat[0].text, "hello");
    }

    #[tokio::test]
    async fn send_chat_rejects_empty_message() {
        let ctx = test_session_context().await;
        let err = ctx.send_chat("   ").await.unwrap_err();
        assert!(matches!(err, SessionError::Invalid(_)));
        assert!(ctx.chat_log.lock().await.is_empty());
    }

    // --- room switching ---------------------------------------------------
    //
    // NOTE for integration (flagged in this worker's report): unlike every
    // other test in this module, `switch_room` now needs a real
    // `&Arc<crate::daemon::AppState>` (see its doc comment -- it threads
    // `state` through to `RTCManagerHandle::switch_room`). `AppState`'s
    // fields are all private and it has no public/test constructor (see
    // `src/daemon/mod.rs`, owned by W5), so this test assumes one exists;
    // until W5 adds `#[cfg(test)] pub fn for_test() -> Arc<AppState>` (or
    // widens field visibility), this one test (and only this one -- nothing
    // else in this module needs `AppState`) won't compile.
    #[tokio::test]
    async fn switch_room_updates_room_and_pushes_notice() {
        let ctx = test_session_context().await;
        let state = crate::daemon::AppState::for_test();
        ctx.switch_room(&state, "new-room".to_string())
            .await
            .unwrap();

        assert_eq!(ctx.room.lock().await.clone(), "new-room");

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].kind, NoticeKind::Info);
        assert!(notices[0].text.contains("new-room"));
    }
}
