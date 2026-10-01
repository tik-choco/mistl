//! Tunnel side messages: small `t`-tagged JSON objects exchanged in the
//! tunnel room next to the `kind`-tagged [`crate::tunnel::rtc::P2pPayload`]
//! envelope. They carry UI state only, never tunneled data, and have no
//! `kind`, `type` or `v` key, so the `P2pPayload` parser, the tc-chat / ai
//! handlers and the consensus `t` demux all ignore them (and older mistl /
//! p2p builds ignore them in turn).
//!
//! * [`AUTH_TAG`] `{"t":"mistl-tunnel-auth-v1","state":"pending|allowed|denied",
//!   "forward_key":"..","target":"..","proto":"..","ts":0}` -- sent by the
//!   owner of a served port to a connecting peer when the connection is parked
//!   for a human decision (`pending`) and when it is resolved. Purely
//!   informational (no signature): the requester only uses it to show
//!   "waiting for <peer> to approve". It is accepted only from the transport
//!   sender that one of our own connect forwards targets, and only for that
//!   forward's key (see [`ScopeMatch`]); state is held in a bounded, TTL'd
//!   [`ApprovalBook`].
//! * [`CANCEL_TAG`] `{"t":"mistl-tunnel-propose-cancel-v1","req_id":".."}` --
//!   the proposer withdrew a forward proposal; the receiver drops the pending
//!   row if (and only if) the transport sender is the peer that proposed it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::time::Instant;

use crate::tunnel::forward_args::split_node_scope;

/// Every tunnel side message's `t` starts with this.
pub const TAG_PREFIX: &str = "mistl-tunnel-";
pub const AUTH_TAG: &str = "mistl-tunnel-auth-v1";
pub const CANCEL_TAG: &str = "mistl-tunnel-propose-cancel-v1";

/// Longest string field we accept from the wire.
const MAX_FIELD_LEN: usize = 512;

/// How long a `pending` mark may stand without a resolution notice: the
/// owner's decision timeout plus slack for the notice to arrive.
pub const AWAITING_TTL: Duration =
    Duration::from_secs(crate::tunnel::auth::pending::DECISION_TIMEOUT.as_secs() + 15);
/// How long a `denied` mark stays visible on the forward.
pub const DENIED_TTL: Duration = Duration::from_secs(120);
/// Cap on tracked marks (one per our-forward x owner peer, so this is only a
/// backstop).
pub const MAX_MARKS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalState {
    Pending,
    Allowed,
    Denied,
}

impl ApprovalState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Allowed => "allowed",
            Self::Denied => "denied",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "pending" => Some(Self::Pending),
            "allowed" => Some(Self::Allowed),
            "denied" => Some(Self::Denied),
            _ => None,
        }
    }
}

/// Builds the owner -> requester approval notice.
pub fn auth_notice(state: ApprovalState, forward_key: &str, target: &str, proto: &str) -> Value {
    json!({
        "t": AUTH_TAG,
        "state": state.as_str(),
        "forward_key": forward_key,
        "target": target,
        "proto": proto,
        "ts": now_ms(),
    })
}

/// Builds the proposer -> proposee withdrawal notice.
pub fn cancel_notice(req_id: &str) -> Value {
    json!({ "t": CANCEL_TAG, "req_id": req_id })
}

/// A syntactically valid approval notice (not yet matched to a forward).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthNotice {
    pub state: ApprovalState,
    pub forward_key: String,
    pub target: String,
    pub proto: String,
}

/// Parses an approval notice; `None` when the tag, state or fields are wrong
/// or oversized.
pub fn parse_auth_notice(value: &Value) -> Option<AuthNotice> {
    if value.get("t")?.as_str()? != AUTH_TAG {
        return None;
    }
    let field = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .filter(|s| s.len() <= MAX_FIELD_LEN)
            .map(str::to_string)
    };
    let forward_key = field("forward_key").filter(|s| !s.is_empty())?;
    Some(AuthNotice {
        state: ApprovalState::from_name(value.get("state")?.as_str()?)?,
        forward_key,
        target: field("target").unwrap_or_default(),
        proto: field("proto").unwrap_or_default(),
    })
}

/// Parses a proposal withdrawal; returns the `req_id`.
pub fn parse_cancel_notice(value: &Value) -> Option<String> {
    if value.get("t")?.as_str()? != CANCEL_TAG {
        return None;
    }
    value
        .get("req_id")?
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= MAX_FIELD_LEN)
        .map(str::to_string)
}

/// How an owner's `forward_key` relates to one of our connect forwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeMatch {
    /// Not this forward.
    No,
    /// Our target is scoped to the sender (`..@<from>`): a match by itself.
    Scoped,
    /// Our target is unscoped: it only matches when the sender is one of the
    /// peers that forward would actually dial (the caller checks that).
    Unscoped,
}

/// Whether a notice for `forward_key` from transport sender `from` can be
/// about our connect forward `our_target`. The forward keys must agree
/// ignoring node scope, and the scope (when present on either side) must name
/// the sender.
pub fn scope_match(our_target: &str, forward_key: &str, from: &str) -> ScopeMatch {
    let (our_base, our_scope) = split_node_scope(our_target);
    let (key_base, key_scope) = split_node_scope(forward_key);
    if our_base != key_base {
        return ScopeMatch::No;
    }
    if key_scope.is_some_and(|scope| scope != from) {
        return ScopeMatch::No;
    }
    match our_scope {
        Some(scope) if scope == from => ScopeMatch::Scoped,
        Some(_) => ScopeMatch::No,
        None => ScopeMatch::Unscoped,
    }
}

/// One connect forward that is waiting on, or was refused by, the owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalMark {
    /// Key of OUR connect forward.
    pub forward_key: String,
    pub peer_id: String,
    pub state: ApprovalState,
    pub since_ms: u64,
    pub target: String,
    pub proto: String,
}

struct Entry {
    mark: ApprovalMark,
    at: Instant,
}

/// Requester-side record of owner approval notices. Cheap to clone; shared by
/// the inbound handler and `tunnel.status`.
#[derive(Clone, Default)]
pub struct ApprovalBook {
    inner: Arc<Mutex<HashMap<(String, String), Entry>>>,
}

impl ApprovalBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a notice already matched to our forward `forward_key` and the
    /// transport sender `peer_id`. `allowed` clears the mark; `pending` and
    /// `denied` set it.
    pub fn apply(&self, peer_id: &str, forward_key: &str, notice: &AuthNotice) {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        prune(&mut map);
        let key = (forward_key.to_string(), peer_id.to_string());
        if notice.state == ApprovalState::Allowed {
            map.remove(&key);
            return;
        }
        if !map.contains_key(&key) && map.len() >= MAX_MARKS {
            return;
        }
        map.insert(
            key,
            Entry {
                mark: ApprovalMark {
                    forward_key: forward_key.to_string(),
                    peer_id: peer_id.to_string(),
                    state: notice.state,
                    since_ms: now_ms(),
                    target: notice.target.clone(),
                    proto: notice.proto.clone(),
                },
                at: Instant::now(),
            },
        );
    }

    /// Live marks (expired ones are dropped first).
    pub fn list(&self) -> Vec<ApprovalMark> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        prune(&mut map);
        let mut marks: Vec<ApprovalMark> = map.values().map(|e| e.mark.clone()).collect();
        marks.sort_by(|a, b| (&a.forward_key, &a.peer_id).cmp(&(&b.forward_key, &b.peer_id)));
        marks
    }

    /// Forgets every mark for a forward (it was removed).
    pub fn forget_forward(&self, forward_key: &str) {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|(key, _), _| key != forward_key);
    }

    /// Forgets every mark about one owner peer (it left the room).
    pub fn forget_peer(&self, peer_id: &str) {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|(_, peer), _| peer != peer_id);
    }
}

fn prune(map: &mut HashMap<(String, String), Entry>) {
    let now = Instant::now();
    map.retain(|_, e| {
        let ttl = match e.mark.state {
            ApprovalState::Denied => DENIED_TTL,
            _ => AWAITING_TTL,
        };
        now.duration_since(e.at) <= ttl
    });
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notice(state: ApprovalState) -> AuthNotice {
        AuthNotice {
            state,
            forward_key: "tcp:127.0.0.1:22@owner".into(),
            target: "127.0.0.1:22".into(),
            proto: "tcp".into(),
        }
    }

    #[test]
    fn auth_notice_roundtrips_and_has_no_envelope_keys() {
        let v = auth_notice(ApprovalState::Pending, "k@owner", "127.0.0.1:22", "tcp");
        assert_eq!(v["t"], AUTH_TAG);
        for key in ["kind", "type", "v"] {
            assert!(
                v.get(key).is_none(),
                "{key} would be claimed by another module"
            );
        }
        // It must not parse as the tunnel's own envelope.
        assert!(
            serde_json::from_value::<crate::tunnel::rtc::payload::P2pPayload>(v.clone()).is_err()
        );
        let parsed = parse_auth_notice(&v).unwrap();
        assert_eq!(parsed.state, ApprovalState::Pending);
        assert_eq!(parsed.forward_key, "k@owner");
        assert_eq!(parsed.target, "127.0.0.1:22");
    }

    #[test]
    fn parse_rejects_bad_state_tag_and_oversized_fields() {
        let mut v = auth_notice(ApprovalState::Denied, "k", "t", "tcp");
        v["state"] = json!("maybe");
        assert!(parse_auth_notice(&v).is_none());
        let mut v = auth_notice(ApprovalState::Denied, "k", "t", "tcp");
        v["t"] = json!("mistl-tunnel-other-v1");
        assert!(parse_auth_notice(&v).is_none());
        let v = auth_notice(
            ApprovalState::Denied,
            &"k".repeat(MAX_FIELD_LEN + 1),
            "t",
            "tcp",
        );
        assert!(parse_auth_notice(&v).is_none());
        let v = auth_notice(ApprovalState::Denied, "", "t", "tcp");
        assert!(parse_auth_notice(&v).is_none());
    }

    #[test]
    fn cancel_notice_roundtrips() {
        assert_eq!(
            parse_cancel_notice(&cancel_notice("r1")).as_deref(),
            Some("r1")
        );
        assert!(parse_cancel_notice(&cancel_notice("")).is_none());
        assert!(parse_cancel_notice(&json!({"t": AUTH_TAG})).is_none());
    }

    #[test]
    fn scope_match_binds_to_the_sender() {
        // Scoped to the sender.
        assert_eq!(
            scope_match("tcp:127.0.0.1:22@owner", "tcp:127.0.0.1:22@owner", "owner"),
            ScopeMatch::Scoped
        );
        // The owner's own key is unscoped (graph / hand-added serve forward).
        assert_eq!(
            scope_match("tcp:127.0.0.1:22@owner", "tcp:127.0.0.1:22", "owner"),
            ScopeMatch::Scoped
        );
        // Our forward targets someone else: the sender cannot speak for it.
        assert_eq!(
            scope_match("tcp:127.0.0.1:22@owner", "tcp:127.0.0.1:22", "mallory"),
            ScopeMatch::No
        );
        // A claimed scope that is not the sender.
        assert_eq!(
            scope_match("tcp:22", "tcp:22@someone-else", "mallory"),
            ScopeMatch::No
        );
        // Different service.
        assert_eq!(
            scope_match("tcp:22@owner", "tcp:80@owner", "owner"),
            ScopeMatch::No
        );
        // Unscoped connect forward: the caller must check the sender serves it.
        assert_eq!(
            scope_match("tcp:22", "tcp:22", "owner"),
            ScopeMatch::Unscoped
        );
    }

    #[test]
    fn book_tracks_pending_then_clears_on_allowed() {
        let book = ApprovalBook::new();
        book.apply("owner", "fwd", &notice(ApprovalState::Pending));
        let marks = book.list();
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[0].state, ApprovalState::Pending);
        assert_eq!(marks[0].peer_id, "owner");
        // Another pending notice for the same pair does not duplicate.
        book.apply("owner", "fwd", &notice(ApprovalState::Pending));
        assert_eq!(book.list().len(), 1);
        book.apply("owner", "fwd", &notice(ApprovalState::Allowed));
        assert!(book.list().is_empty());
    }

    #[test]
    fn book_keeps_a_denied_marker_and_forgets_by_forward_or_peer() {
        let book = ApprovalBook::new();
        book.apply("owner", "fwd", &notice(ApprovalState::Pending));
        book.apply("owner", "fwd", &notice(ApprovalState::Denied));
        assert_eq!(book.list()[0].state, ApprovalState::Denied);
        book.apply("owner", "fwd2", &notice(ApprovalState::Pending));
        book.forget_forward("fwd");
        assert_eq!(book.list().len(), 1);
        book.forget_peer("owner");
        assert!(book.list().is_empty());
    }

    #[test]
    fn book_is_bounded() {
        let book = ApprovalBook::new();
        for i in 0..MAX_MARKS + 20 {
            book.apply(
                "owner",
                &format!("fwd-{i}"),
                &notice(ApprovalState::Pending),
            );
        }
        assert_eq!(book.list().len(), MAX_MARKS);
    }

    #[tokio::test(start_paused = true)]
    async fn book_expires_pending_and_denied_marks() {
        let book = ApprovalBook::new();
        book.apply("owner", "a", &notice(ApprovalState::Pending));
        book.apply("owner", "b", &notice(ApprovalState::Denied));
        tokio::time::advance(DENIED_TTL + Duration::from_secs(1)).await;
        let marks = book.list();
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[0].forward_key, "a");
        tokio::time::advance(AWAITING_TTL).await;
        assert!(book.list().is_empty());
    }
}
