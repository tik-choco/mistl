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
    SharedAuthorizer, TrustDecision, TrustStore, downgrade_unremembered,
};

/// Caps on parked approval rows: per peer and in total. Beyond them a request
/// is denied outright instead of queued, so a peer cannot flood the pending
/// pane (or pin memory) just by opening connections.
pub const MAX_PENDING_PER_PEER: usize = 8;
pub const MAX_PENDING_TOTAL: usize = 64;
/// Connections waiting on one coalesced row (see [`PendingAuthorizations`]'s
/// `enqueue`). Bounds the waiters a peer can attach by opening many
/// connections to the same target.
pub const MAX_WAITERS_PER_REQUEST: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingAuthorization {
    pub id: u64,
    pub request: AuthRequest,
}

/// What a [`PendingAuthorizations`] notifier is told about a request: it was
/// parked for a human, or it was resolved. Lets the session tell the
/// connecting peer (see `crate::tunnel::notice`); the queue itself stays
/// transport-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingEvent {
    Parked,
    Allowed,
    Denied,
}

/// Called with the request a [`PendingEvent`] is about. Must not block.
pub type PendingNotifier = Arc<dyn Fn(&AuthRequest, PendingEvent) + Send + Sync>;

#[derive(Clone)]
pub struct PendingAuthorizations {
    inner: Arc<Mutex<Inner>>,
    notifier: Arc<std::sync::Mutex<Option<PendingNotifier>>>,
}

impl std::fmt::Debug for PendingAuthorizations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAuthorizations")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Inner {
    next_id: u64,
    pending: BTreeMap<u64, PendingItem>,
}

/// One approval row. Identical requests (same peer, DID, target) share a row:
/// a client such as a browser opens several connections at once, and each
/// would otherwise park its own row. The decision answers every waiter.
#[derive(Debug)]
struct PendingItem {
    request: AuthRequest,
    responders: Vec<oneshot::Sender<AuthDecision>>,
}

impl PendingItem {
    fn answer(self, decision: AuthDecision) {
        for responder in self.responders {
            let _ = responder.send(decision);
        }
    }
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
            notifier: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Installs the callback told when a request is parked and when it is
    /// resolved (once per coalesced row, not per waiting connection).
    pub fn set_notifier(&self, notifier: PendingNotifier) {
        *self.notifier.lock().unwrap_or_else(|e| e.into_inner()) = Some(notifier);
    }

    fn notify(&self, request: &AuthRequest, event: PendingEvent) {
        let notifier = self
            .notifier
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(notifier) = notifier {
            notifier(request, event);
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
                self.notify(
                    &item.request,
                    if decision.is_allowed() {
                        PendingEvent::Allowed
                    } else {
                        PendingEvent::Denied
                    },
                );
                item.answer(decision);
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
                item.answer(AuthDecision::Deny);
            }
        }
        count
    }

    /// Parks `request`. The flag is `true` when it opened a new row (as
    /// opposed to joining an identical one that is already waiting).
    async fn enqueue(
        &self,
        request: AuthRequest,
    ) -> Option<(u64, oneshot::Receiver<AuthDecision>, bool)> {
        let (sender, receiver) = oneshot::channel();
        let mut inner = self.inner.lock().await;
        if let Some((id, item)) = inner
            .pending
            .iter_mut()
            .find(|(_, item)| item.request == request)
        {
            item.responders.retain(|r| !r.is_closed());
            if item.responders.len() >= MAX_WAITERS_PER_REQUEST {
                return None;
            }
            item.responders.push(sender);
            return Some((*id, receiver, false));
        }
        let from_peer = inner
            .pending
            .values()
            .filter(|item| item.request.peer_id == request.peer_id)
            .count();
        if from_peer >= MAX_PENDING_PER_PEER || inner.pending.len() >= MAX_PENDING_TOTAL {
            return None;
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner.pending.insert(
            id,
            PendingItem {
                request,
                responders: vec![sender],
            },
        );
        Some((id, receiver, true))
    }

    /// Drops waiters whose wait ended without a decision -- used when
    /// `PendingAuthorizer::decide`'s wait times out (its receiver is already
    /// dropped, so its sender reads as closed) -- and removes the row once no
    /// waiter is left, so no ghost row lingers in `list()`. Later waiters on a
    /// coalesced row keep it until their own timeout.
    async fn remove_unresolved(&self, id: u64) {
        let mut inner = self.inner.lock().await;
        let mut expired = None;
        if let Some(item) = inner.pending.get_mut(&id) {
            item.responders.retain(|r| !r.is_closed());
            if item.responders.is_empty() {
                expired = inner.pending.remove(&id);
            }
        }
        drop(inner);
        if let Some(item) = expired {
            self.notify(&item.request, PendingEvent::Denied);
        }
    }

    #[cfg(test)]
    pub(crate) async fn enqueue_for_test(
        &self,
        request: AuthRequest,
    ) -> oneshot::Receiver<AuthDecision> {
        self.enqueue(request)
            .await
            .expect("pending caps not reached in test")
            .1
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
        let did = req.verified_did();
        if let Some(decision) = self.store.lookup(&req.peer_id, did, &req.forward_key).await {
            let decision = match decision {
                TrustDecision::Allow => AuthDecision::Allow,
                TrustDecision::Deny => AuthDecision::Deny,
            };
            self.record(req, decision, AuthEventSource::TrustStore)
                .await;
            // A remembered deny is final: tell the peer why its connection
            // dies (a remembered allow just works, nothing to say).
            if decision == AuthDecision::Deny {
                self.pending.notify(req, PendingEvent::Denied);
            }
            return decision;
        }

        let Some((id, receiver, parked)) = self.pending.enqueue(req.clone()).await else {
            // Too many unanswered requests from this peer (or overall): deny
            // without parking another row or remembering anything.
            self.record(req, AuthDecision::Deny, AuthEventSource::Pending)
                .await;
            self.pending.notify(req, PendingEvent::Denied);
            return AuthDecision::Deny;
        };
        if parked {
            self.pending.notify(req, PendingEvent::Parked);
        }
        let decision = match tokio::time::timeout(DECISION_TIMEOUT, receiver).await {
            Ok(result) => result.unwrap_or(AuthDecision::Deny),
            // `timeout` consumed and dropped the receiver, which is what lets
            // `remove_unresolved` recognise this waiter as gone.
            Err(_elapsed) => {
                // No decision within the timeout: drop the ghost entry so it
                // doesn't linger in `list()` forever, and deny.
                self.pending.remove_unresolved(id).await;
                AuthDecision::Deny
            }
        };
        // "Always" is only honored for a DID-verified peer; an unverified
        // peer gets a one-off decision and nothing is stored.
        let mut decision = decision;
        if let Some(trust) = trust_decision(decision) {
            let remembered = self
                .store
                .remember_for(&req.peer_id, did, &req.forward_key, trust)
                .await
                .unwrap_or(false);
            if !remembered {
                decision = downgrade_unremembered(decision);
            }
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
