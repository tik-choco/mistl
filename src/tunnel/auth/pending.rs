//! Queues incoming authorization requests that neither the trust store nor
//! the configured policy could resolve, for a human decision via the
//! dashboard/TUI/control-shell. Ported from `p2p/src/auth/pending.rs`; the
//! only import rewrite is `crate::auth::X` -> `crate::tunnel::auth::X`.
//!
//! [`PendingAuthorization`] gains a `Serialize` derive (not present
//! upstream) so `tunnel.status` can embed the pending-approval list
//! directly (see the integration contract's `pending_auth` field); this
//! requires [`crate::tunnel::auth::AuthRequest`] to also derive `Serialize`,
//! which it now does (see `crate::tunnel::auth`'s module doc comment).
//!
//! JSON shape: `{"id":0,"request":{"peer_id":"..","forward_key":"..","target_addr":"..","proto":".."}}`.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{Mutex, oneshot};

use crate::tunnel::auth::{
    AuthAuditLog, AuthDecision, AuthEventSource, AuthFuture, AuthRequest, ConnectionAuthorizer,
    SharedAuthorizer, TrustDecision, TrustKey, TrustStore,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingAuthorization {
    pub id: u64,
    pub request: AuthRequest,
}

#[derive(Debug, Clone)]
pub struct PendingAuthorizations {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    next_id: u64,
    pending: BTreeMap<u64, PendingItem>,
}

#[derive(Debug)]
struct PendingItem {
    request: AuthRequest,
    responder: oneshot::Sender<AuthDecision>,
}

impl Default for PendingAuthorizations {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingAuthorizations {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                next_id: 1,
                pending: BTreeMap::new(),
            })),
        }
    }

    pub async fn list(&self) -> Vec<PendingAuthorization> {
        self.inner
            .lock()
            .await
            .pending
            .iter()
            .map(|(id, item)| PendingAuthorization {
                id: *id,
                request: item.request.clone(),
            })
            .collect()
    }

    pub async fn resolve(&self, id: u64, decision: AuthDecision) -> bool {
        let item = self.inner.lock().await.pending.remove(&id);
        match item {
            Some(item) => {
                let _ = item.responder.send(decision);
                true
            }
            None => false,
        }
    }

    /// Denies and removes every pending entry whose request came from
    /// `peer_id`. Sending `Deny` into each oneshot unblocks the tcp layer's
    /// waiting `authorize()` call for that entry, which then cleans up its
    /// own pending conn. Does not persist anything to the trust store --
    /// same as any other plain `Deny`, only an explicit `DenyAlways` does
    /// that (see `PendingAuthorizer::decide`). Returns the number purged.
    pub async fn purge_peer(&self, peer_id: &str) -> usize {
        let mut inner = self.inner.lock().await;
        let stale: Vec<u64> = inner
            .pending
            .iter()
            .filter(|(_, item)| item.request.peer_id == peer_id)
            .map(|(id, _)| *id)
            .collect();
        let count = stale.len();
        for id in stale {
            if let Some(item) = inner.pending.remove(&id) {
                let _ = item.responder.send(AuthDecision::Deny);
            }
        }
        count
    }

    async fn enqueue(&self, request: AuthRequest) -> (u64, oneshot::Receiver<AuthDecision>) {
        let (sender, receiver) = oneshot::channel();
        let mut inner = self.inner.lock().await;
        let id = inner.next_id;
        inner.next_id += 1;
        inner.pending.insert(
            id,
            PendingItem {
                request,
                responder: sender,
            },
        );
        (id, receiver)
    }

    /// Removes a pending entry without resolving it -- used when
    /// `PendingAuthorizer::decide`'s wait times out with no responder ever
    /// having claimed it, so no ghost row lingers in `list()`.
    async fn remove_unresolved(&self, id: u64) {
        self.inner.lock().await.pending.remove(&id);
    }

    #[cfg(test)]
    pub(crate) async fn enqueue_for_test(
        &self,
        request: AuthRequest,
    ) -> oneshot::Receiver<AuthDecision> {
        self.enqueue(request).await.1
    }
}

/// How long a queued authorization request waits for a human decision
/// (TUI/web/control-shell approve/deny) before it's treated as denied and
/// dropped from the pending list. Bounds how long a peer can pin a
/// connection open by simply never being answered (the operator stepped
/// away, the UI was closed, etc); `purge_peer` handles the "peer itself
/// disconnected" case separately and sooner. `pub(crate)` so tests can
/// drive the clock past it deterministically.
pub(crate) const DECISION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

#[derive(Debug, Clone)]
pub struct PendingAuthorizer {
    store: TrustStore,
    pending: PendingAuthorizations,
    audit_log: Option<AuthAuditLog>,
}

impl PendingAuthorizer {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(store: TrustStore, pending: PendingAuthorizations) -> Self {
        Self {
            store,
            pending,
            audit_log: None,
        }
    }

    #[allow(dead_code)]
    pub fn shared(store: TrustStore, pending: PendingAuthorizations) -> SharedAuthorizer {
        Arc::new(Self::new(store, pending))
    }

    pub fn with_audit_log(
        store: TrustStore,
        pending: PendingAuthorizations,
        audit_log: AuthAuditLog,
    ) -> Self {
        Self {
            store,
            pending,
            audit_log: Some(audit_log),
        }
    }

    pub fn shared_with_audit_log(
        store: TrustStore,
        pending: PendingAuthorizations,
        audit_log: AuthAuditLog,
    ) -> SharedAuthorizer {
        Arc::new(Self::with_audit_log(store, pending, audit_log))
    }

    async fn decide(&self, req: &AuthRequest) -> AuthDecision {
        let key = TrustKey {
            peer_id: req.peer_id.clone(),
            forward_key: req.forward_key.clone(),
        };
        if let Some(decision) = self.store.get(&key).await {
            let decision = match decision {
                TrustDecision::Allow => AuthDecision::Allow,
                TrustDecision::Deny => AuthDecision::Deny,
            };
            self.record(req, decision, AuthEventSource::TrustStore)
                .await;
            return decision;
        }

        let (id, receiver) = self.pending.enqueue(req.clone()).await;
        let decision = match tokio::time::timeout(DECISION_TIMEOUT, receiver).await {
            Ok(result) => result.unwrap_or(AuthDecision::Deny),
            Err(_elapsed) => {
                // No decision within the timeout: drop the ghost entry so it
                // doesn't linger in `list()` forever, and deny.
                self.pending.remove_unresolved(id).await;
                AuthDecision::Deny
            }
        };
        if let Some(trust) = trust_decision(decision) {
            let _ = self.store.remember(key, trust).await;
        }
        self.record(req, decision, AuthEventSource::Pending).await;
        decision
    }

    async fn record(&self, req: &AuthRequest, decision: AuthDecision, source: AuthEventSource) {
        if let Some(log) = &self.audit_log {
            log.record(req, decision, source).await;
        }
    }
}

impl ConnectionAuthorizer for PendingAuthorizer {
    fn authorize<'a>(&'a self, req: &'a AuthRequest) -> AuthFuture<'a> {
        Box::pin(async move { self.decide(req).await })
    }
}

fn trust_decision(decision: AuthDecision) -> Option<TrustDecision> {
    match decision {
        AuthDecision::AllowAlways => Some(TrustDecision::Allow),
        AuthDecision::DenyAlways => Some(TrustDecision::Deny),
        AuthDecision::Allow | AuthDecision::Deny => None,
    }
}
