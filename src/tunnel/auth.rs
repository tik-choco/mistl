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
pub use pending::{PendingAuthorizations, PendingAuthorizer};
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

    fn trust_decision(self) -> Option<TrustDecision> {
        match self {
            Self::AllowAlways => Some(TrustDecision::Allow),
            Self::DenyAlways => Some(TrustDecision::Deny),
            Self::Allow | Self::Deny => None,
        }
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
        let key = TrustKey {
            peer_id: req.peer_id.clone(),
            forward_key: req.forward_key.clone(),
        };
        if let Some(decision) = self.store.get(&key).await {
            let auth_decision = match decision {
                TrustDecision::Allow => AuthDecision::Allow,
                TrustDecision::Deny => AuthDecision::Deny,
            };
            self.record(req, auth_decision, AuthEventSource::TrustStore)
                .await;
            return auth_decision;
        }

        let decision = match &self.policy {
            AuthPolicy::AutoAccept => AuthDecision::Allow,
            AuthPolicy::AllowPeers(peers) if peers.contains(&req.peer_id) => AuthDecision::Allow,
            AuthPolicy::AllowPeers(_) | AuthPolicy::DenyUnknown => AuthDecision::Deny,
        };
        if let Some(trust) = decision.trust_decision() {
            let _ = self.store.remember(key, trust).await;
        }
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
