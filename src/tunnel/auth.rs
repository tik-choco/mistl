//! Connection authorization: decides whether an incoming forward request
//! (someone dialing in through a `serve` forward this node published) may
//! proceed, via a pluggable [`ConnectionAuthorizer`]. Three implementations
//! exist: [`allow_all`] (tests / trivial setups), [`PolicyAuthorizer`]
//! (config-driven: auto-accept / allowlist / deny-unknown, consulting
//! [`TrustStore`] first), and `pending::PendingAuthorizer` (queues the
//! request for a human decision via the dashboard/TUI/control-shell,
//! surfaced through [`PendingAuthorizations`]).
//!
//! Ported verbatim from `p2p/src/auth.rs` (and its `audit`/`pending`/`trust`
//! submodules) -- no `crate::X` imports needed rewriting here since auth's
//! only cross-module dependency is on itself. [`AuthRequest`] gains a
//! `Serialize` derive (not present upstream): it's a plain struct of
//! strings, referenced from `pending::PendingAuthorization`, which needs to
//! serialize for the `tunnel.status` IPC payload (see that module's doc
//! comment). [`AuthDecision`] also gains `Serialize` (`#[serde(rename_all =
//! "snake_case")]`: `Allow`->`"allow"`, `Deny`->`"deny"`,
//! `AllowAlways`->`"allow_always"`, `DenyAlways`->`"deny_always"`) since
//! `audit::AuthEvent` embeds it and needs the same treatment.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Serialize;

pub use audit::{AuthAuditLog, AuthEvent, AuthEventSource};
#[allow(unused_imports)]
pub use pending::PendingAuthorization;
pub use pending::{PendingAuthorizations, PendingAuthorizer, PendingEvent};
pub use trust::{TrustDecision, TrustEntry, TrustKey, TrustStore, default_trust_store_path};

pub mod audit;
pub mod pending;
pub mod trust;

pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = AuthDecision> + Send + 'a>>;
pub type SharedAuthorizer = Arc<dyn ConnectionAuthorizer>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthRequest {
    pub peer_id: String,
    pub forward_key: String,
    pub target_addr: String,
    pub proto: String,
    /// The `did:key` that `crate::net::peer_auth` verified for `peer_id`, or
    /// `None` for an unverified peer (older mistl, no hello yet). Use
    /// [`AuthRequest::verified_did`] rather than reading this directly.
    pub did: Option<String>,
}

impl AuthRequest {
    /// The peer's verified DID, only if it really hashes to `peer_id`.
    /// `None` means "unverified": such a peer is never auto-approved and no
    /// decision about it is ever remembered (see `TrustStore::lookup`).
    pub fn verified_did(&self) -> Option<&str> {
        self.did
            .as_deref()
            .filter(|did| crate::identity::node_id_for_did(did) == self.peer_id)
    }
}

/// The DID `peer_auth` verified for transport node `peer_id`: in `room` when
/// known, otherwise (or failing that) in any joined room. Always re-checked
/// against `node_id_for_did`, so the result provably belongs to `peer_id`.
///
/// This only says "the holder of this DID key answered a signed hello as this
/// node id".
pub fn verified_did_for_peer(room: Option<&str>, peer_id: &str) -> Option<String> {
    use crate::net::peer_auth::{verified_did, verified_did_any_room};
    room.and_then(|room| verified_did(room, peer_id))
        .or_else(|| verified_did_any_room(peer_id))
        .filter(|did| crate::identity::node_id_for_did(did) == peer_id)
}

/// Mid-session mis-delivery check: a session approved for `bound_did` only
/// accepts payloads whose sender still maps to that DID. A session approved
/// for an unverified peer (`None`) has nothing to check.
pub fn binding_holds(bound_did: Option<&str>, room: Option<&str>, from: &str) -> bool {
    match bound_did {
        None => true,
        Some(bound) => verified_did_for_peer(room, from).as_deref() == Some(bound),
    }
}

/// Whether config policy auto-approves `req`. Never for an unverified peer.
/// `allow_peers` holds DIDs and/or legacy node ids; a node id only matches
/// when the peer's DID is verified (and therefore hashes to it).
pub(crate) fn policy_allows(
    auto_accept: bool,
    allow_peers: &HashSet<String>,
    req: &AuthRequest,
) -> bool {
    let Some(did) = req.verified_did() else {
        return false;
    };
    auto_accept || allow_peers.contains(did) || allow_peers.contains(&req.peer_id)
}

/// Turns a remember-request into a one-off decision when it cannot be
/// remembered (unverified peer), so the audit trail matches what was stored.
pub(crate) fn downgrade_unremembered(decision: AuthDecision) -> AuthDecision {
    match decision {
        AuthDecision::AllowAlways => AuthDecision::Allow,
        AuthDecision::DenyAlways => AuthDecision::Deny,
        other => other,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthDecision {
    Allow,
    Deny,
    #[allow(dead_code)]
    AllowAlways,
    #[allow(dead_code)]
    DenyAlways,
}

impl AuthDecision {
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::Allow | Self::AllowAlways)
    }
}

pub trait ConnectionAuthorizer: Send + Sync {
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a>;
}

pub fn allow_all() -> SharedAuthorizer {
    Arc::new(AllowAllAuthorizer)
}

#[derive(Debug, Default)]
struct AllowAllAuthorizer;

impl ConnectionAuthorizer for AllowAllAuthorizer {
    fn authorize<'a>(&'a self, _req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async { AuthDecision::Allow })
    }
}

// `AuthPolicy` and `PolicyAuthorizer` are superseded by
// `session::ConfiguredAuthorizer`, which is what's actually wired up today.
// Kept as the upstream-parity policy implementation (ported verbatim from
// `p2p/src/auth.rs`), hence the blanket dead-code allowances below.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AuthPolicy {
    AutoAccept,
    AllowPeers(HashSet<String>),
    DenyUnknown,
}

impl AuthPolicy {
    #[allow(dead_code)]
    pub fn allow_peers<I, S>(peers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::AllowPeers(peers.into_iter().map(Into::into).collect())
    }
}

#[derive(Debug, Clone)]
pub struct PolicyAuthorizer {
    policy: AuthPolicy,
    store: TrustStore,
    audit_log: Option<AuthAuditLog>,
}

impl PolicyAuthorizer {
    #[allow(dead_code)]
    pub fn new(policy: AuthPolicy, store: TrustStore) -> Self {
        Self {
            policy,
            store,
            audit_log: None,
        }
    }

    #[allow(dead_code)]
    pub fn shared(policy: AuthPolicy, store: TrustStore) -> SharedAuthorizer {
        Arc::new(Self::new(policy, store))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_audit_log(policy: AuthPolicy, store: TrustStore, audit_log: AuthAuditLog) -> Self {
        Self {
            policy,
            store,
            audit_log: Some(audit_log),
        }
    }

    #[allow(dead_code)]
    pub fn shared_with_audit_log(
        policy: AuthPolicy,
        store: TrustStore,
        audit_log: AuthAuditLog,
    ) -> SharedAuthorizer {
        Arc::new(Self::with_audit_log(policy, store, audit_log))
    }

    async fn decide(&self, req: &AuthRequest) -> AuthDecision {
        if let Some(decision) = self
            .store
            .lookup(&req.peer_id, req.verified_did(), &req.forward_key)
            .await
        {
            let auth_decision = match decision {
                TrustDecision::Allow => AuthDecision::Allow,
                TrustDecision::Deny => AuthDecision::Deny,
            };
            self.record(req, auth_decision, AuthEventSource::TrustStore)
                .await;
            return auth_decision;
        }

        // Policy never approves an unverified peer (see `policy_allows`).
        let decision = match &self.policy {
            AuthPolicy::AutoAccept if policy_allows(true, &HashSet::new(), req) => {
                AuthDecision::Allow
            }
            AuthPolicy::AllowPeers(peers) if policy_allows(false, peers, req) => {
                AuthDecision::Allow
            }
            _ => AuthDecision::Deny,
        };
        // Policy decisions are Allow/Deny (never *Always), so nothing is
        // remembered here.
        self.record(req, decision, AuthEventSource::Policy).await;
        decision
    }

    async fn record(&self, req: &AuthRequest, decision: AuthDecision, source: AuthEventSource) {
        if let Some(log) = &self.audit_log {
            log.record(req, decision, source).await;
        }
    }
}

impl ConnectionAuthorizer for PolicyAuthorizer {
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async move { self.decide(req).await })
    }
}

#[cfg(test)]
mod tests;
