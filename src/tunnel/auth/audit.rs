//! Bounded ring buffer of authorization decisions (who was allowed/denied to
//! reach what, and why -- policy, trust store, or a pending human decision),
//! surfaced as the tunnel's audit trail. Ported from `p2p/src/auth/audit.rs`;
//! the only import rewrite is `crate::auth::X` -> `crate::tunnel::auth::X`.
//!
//! [`AuthEvent`] and [`AuthEventSource`] gain `Serialize` derives (not
//! present upstream) so this type can go straight into the `tunnel.status`
//! IPC payload (see the integration contract's `events` field). `timestamp_ms`
//! is already stored as epoch milliseconds, matching the wire convention for
//! every other timestamp this worker's types expose. `AuthEventSource` uses
//! `#[serde(rename_all = "snake_case")]` (`Policy`->`"policy"`,
//! `TrustStore`->`"trust_store"`, `Pending`->`"pending"`), matching
//! `AuthDecision`'s casing in `crate::tunnel::auth`.
//!
//! JSON shape of `AuthEvent`:
//! `{"sequence":0,"timestamp_ms":0,"peer_id":"..","forward_key":"..","target_addr":"..","proto":"..","decision":"allow","source":"policy"}`

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::RwLock;

use crate::tunnel::auth::{AuthDecision, AuthRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthEventSource {
    Policy,
    TrustStore,
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthEvent {
    pub sequence: u64,
    pub timestamp_ms: u128,
    pub peer_id: String,
    pub forward_key: String,
    pub target_addr: String,
    pub proto: String,
    pub decision: AuthDecision,
    pub source: AuthEventSource,
}

#[derive(Debug, Clone)]
pub struct AuthAuditLog {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug)]
struct Inner {
    next_sequence: u64,
    capacity: usize,
    events: VecDeque<AuthEvent>,
}

impl Default for AuthAuditLog {
    fn default() -> Self {
        Self::with_capacity(256)
    }
}

impl AuthAuditLog {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                next_sequence: 1,
                capacity,
                events: VecDeque::new(),
            })),
        }
    }

    pub async fn record(&self, req: &AuthRequest, decision: AuthDecision, source: AuthEventSource) {
        let mut inner = self.inner.write().await;
        if inner.capacity == 0 {
            return;
        }
        let event = AuthEvent {
            sequence: inner.next_sequence,
            timestamp_ms: now_ms(),
            peer_id: req.peer_id.clone(),
            forward_key: req.forward_key.clone(),
            target_addr: req.target_addr.clone(),
            proto: req.proto.clone(),
            decision,
            source,
        };
        inner.next_sequence += 1;
        inner.events.push_back(event);
        while inner.events.len() > inner.capacity {
            inner.events.pop_front();
        }
    }

    pub async fn list(&self) -> Vec<AuthEvent> {
        self.inner.read().await.events.iter().cloned().collect()
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
