//! DID-bound peer authentication ("DID hello").
//!
//! mistl treats the transport-level sender id (`from` in every raw event) as
//! a routing hint, not as an identity: nothing ties a node id to a `did:key`
//! at the transport layer.
//!
//! This module adds that binding. Every mistl node periodically
//! broadcasts -- and answers every `EVENT_JOIN` with -- a small hello signed
//! with its `did:key` via [`crate::wiresign`]:
//!
//! ```json
//! {"t":"mistl-did-hello-v1","room":"...","node":"<16 hex>","fromId":"did:key:z6Mk...",
//!  "ts":1700000000000,"nonce":"<32 hex>","re":false,"signature":"..."}
//! ```
//!
//! A receiver accepts it only when the signature verifies, `node` equals the
//! transport sender, `sha256(fromId)[..16 hex] == from` (the node id rule),
//! `room` is the room the event actually arrived in, `ts` is within
//! [`MAX_CLOCK_SKEW_MS`] of the local clock, and the nonce hasn't been seen
//! before. Verified `(room, node_id) -> did` pairs are kept in a bounded,
//! TTL'd registry ([`verified_did`]) that other modules consult.
//!
//! What a verified hello does and does not prove: it proves the holder of
//! `fromId`'s private key recently announced itself in this room under the
//! node id derived from that DID. It says nothing about the authenticity of
//! later unsigned messages. Control-plane protocols that need per-message
//! authenticity (e.g.
//! `crate::consensus`) must sign each message themselves; the registry is the
//! membership/identity side of that, and the gate for trust decisions that
//! only need "is this node id held by that DID and present here".
//!
//! Wire shape: a JSON object tagged by `t` (like the consensus envelopes),
//! with no `type`, `v` or `kind` key, so the tc-chat / pairing / bot / storage
//! (`type`), mistai (`v` + `type`), and tunnel (`kind`) handlers all ignore
//! it, and the consensus `t` demux ignores the unknown tag value.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tracing::debug;

use crate::identity::Identity;

/// `t` tag of the network-wide DID hello.
pub const HELLO_TAG: &str = "mistl-did-hello-v1";
/// How often each joined room gets a fresh hello broadcast.
pub const HELLO_INTERVAL: Duration = Duration::from_secs(30);
/// A verified entry not refreshed by a new hello within this long is dropped.
pub const VERIFIED_TTL: Duration = Duration::from_secs(120);
/// Maximum accepted distance between a proof's `ts` and the local clock, in
/// either direction. Also bounds how long a captured proof stays replayable
/// if it ever falls out of the nonce cache.
pub const MAX_CLOCK_SKEW_MS: u64 = 120_000;
/// Global cap on verified `(room, node)` entries.
pub const MAX_VERIFIED: usize = 1024;
/// Per-room cap, so one busy (or hostile) room can't exhaust the global cap.
pub const MAX_VERIFIED_PER_ROOM: usize = 256;
/// Cap on remembered nonces (replay cache). Oldest are evicted first; an
/// evicted nonce is still bounded by the [`MAX_CLOCK_SKEW_MS`] freshness check.
const MAX_NONCES: usize = 4096;
/// Largest proof message worth parsing; real ones are ~400 bytes.
pub const MAX_PROOF_BYTES: usize = 2048;
/// Minimum spacing between direct hello replies to the same (room, peer).
const REPLY_THROTTLE: Duration = Duration::from_secs(10);
/// Cap on the reply-throttle table (entries older than the throttle window
/// are pruned first).
const MAX_REPLY_ENTRIES: usize = 1024;

/// A DID proof that passed every stateless check in [`check_proof`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DidProof {
    pub did: String,
    pub node: String,
    pub ts_ms: u64,
    pub nonce: String,
    /// `true` for a direct reply to someone else's hello (never answered,
    /// so two nodes can't ping-pong).
    pub reply: bool,
}

/// Why a proof was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofError {
    /// Not a proof with the expected tag (someone else's message).
    NotOurs,
    Oversized,
    Malformed,
    /// Signed for a different room than the one it arrived in.
    WrongRoom,
    /// `node` differs from the transport sender.
    NodeMismatch,
    /// `sha256(fromId)[..16 hex]` differs from the transport sender.
    DidMismatch,
    /// `ts` too far from the local clock.
    Stale,
    BadSignature,
    /// Nonce already seen.
    Replay,
    /// Registry is full for this room (or globally).
    Full,
}

/// Milliseconds since the Unix epoch, per the local clock.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A fresh random nonce (128 bits, hex).
pub fn fresh_nonce() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// Builds a signed proof message tagged `tag` for `room`. Shared by the
/// network-wide hello ([`HELLO_TAG`]) and protocol-specific hellos (the
/// consensus relay hello) so both carry the same verifiable shape.
pub fn build_proof(
    tag: &str,
    room: &str,
    identity: &Identity,
    ts_ms: u64,
    nonce: &str,
    reply: bool,
) -> Vec<u8> {
    let mut wire = json!({
        "t": tag,
        "room": room,
        "node": identity.node_id(),
        "fromId": identity.did(),
        "ts": ts_ms,
        "nonce": nonce,
        "re": reply,
    });
    crate::wiresign::sign_wire(&mut wire, identity)
        .expect("fromId is the signing identity's own DID");
    serde_json::to_vec(&wire).expect("proof serialization cannot fail")
}

/// Stateless verification of a raw proof message (see [`check_proof_value`]).
pub fn check_proof(
    data: &[u8],
    tag: &str,
    room: &str,
    from: &str,
    now_ms: u64,
) -> Result<DidProof, ProofError> {
    if data.len() > MAX_PROOF_BYTES {
        return Err(ProofError::Oversized);
    }
    let value: Value = serde_json::from_slice(data).map_err(|_| ProofError::NotOurs)?;
    check_proof_value(&value, tag, room, from, now_ms)
}

/// Stateless verification of an already-parsed proof: tag, room binding,
/// `node == from`, `node_id(fromId) == from`, freshness, and signature (in
/// that order, so the expensive signature check runs last). Replay is the
/// caller's job (see [`accept_proof`]).
pub fn check_proof_value(
    value: &Value,
    tag: &str,
    room: &str,
    from: &str,
    now_ms: u64,
) -> Result<DidProof, ProofError> {
    let obj = value.as_object().ok_or(ProofError::NotOurs)?;
    if obj.get("t").and_then(Value::as_str) != Some(tag) {
        return Err(ProofError::NotOurs);
    }
    let msg_room = obj
        .get("room")
        .and_then(Value::as_str)
        .ok_or(ProofError::Malformed)?;
    if msg_room != room {
        return Err(ProofError::WrongRoom);
    }
    let node = obj
        .get("node")
        .and_then(Value::as_str)
        .ok_or(ProofError::Malformed)?;
    if node != from {
        return Err(ProofError::NodeMismatch);
    }
    let did = obj
        .get("fromId")
        .and_then(Value::as_str)
        .ok_or(ProofError::Malformed)?;
    if did.len() != crate::identity::ED25519_DID_KEY_LEN
        || crate::identity::node_id_for_did(did) != from
    {
        return Err(ProofError::DidMismatch);
    }
    let ts_ms = obj
        .get("ts")
        .and_then(Value::as_u64)
        .ok_or(ProofError::Malformed)?;
    if ts_ms.abs_diff(now_ms) > MAX_CLOCK_SKEW_MS {
        return Err(ProofError::Stale);
    }
    let nonce = obj
        .get("nonce")
        .and_then(Value::as_str)
        .filter(|n| !n.is_empty() && n.len() <= 64)
        .ok_or(ProofError::Malformed)?;
    let reply = obj.get("re").and_then(Value::as_bool).unwrap_or(false);
    if !crate::wiresign::verify_wire(value).unwrap_or(false) {
        return Err(ProofError::BadSignature);
    }
    Ok(DidProof {
        did: did.to_string(),
        node: node.to_string(),
        ts_ms,
        nonce: nonce.to_string(),
        reply,
    })
}

#[derive(Debug, Clone)]
struct Entry {
    did: String,
    last_seen: Instant,
}

/// The verified `(room, node) -> did` table plus its replay cache. Pure
/// (takes `now` explicitly) so the caps/TTL/replay policy is unit-testable;
/// the process-wide instance lives behind [`REGISTRY`].
#[derive(Debug, Default)]
pub struct Registry {
    entries: HashMap<(String, String), Entry>,
    nonces: VecDeque<(Instant, String)>,
    nonce_set: HashSet<String>,
}

impl Registry {
    /// Records a proof that already passed [`check_proof`] for `room`.
    /// Returns `Ok(true)` when `(room, node)` was newly verified, `Ok(false)`
    /// for a refresh.
    pub fn accept(
        &mut self,
        room: &str,
        proof: &DidProof,
        now: Instant,
    ) -> Result<bool, ProofError> {
        self.prune(now);
        let nonce_key = format!("{}:{}", proof.node, proof.nonce);
        if self.nonce_set.contains(&nonce_key) {
            return Err(ProofError::Replay);
        }
        let key = (room.to_string(), proof.node.clone());
        let is_new = match self.entries.get(&key) {
            // node ids are a 64-bit DID hash prefix: keep the first binding
            // rather than letting a colliding DID replace it.
            Some(existing) if existing.did != proof.did => return Err(ProofError::DidMismatch),
            Some(_) => false,
            None => {
                let in_room = self.entries.keys().filter(|(r, _)| r == room).count();
                if self.entries.len() >= MAX_VERIFIED || in_room >= MAX_VERIFIED_PER_ROOM {
                    return Err(ProofError::Full);
                }
                true
            }
        };
        self.entries.insert(
            key,
            Entry {
                did: proof.did.clone(),
                last_seen: now,
            },
        );
        self.remember_nonce(nonce_key, now);
        Ok(is_new)
    }

    fn remember_nonce(&mut self, nonce_key: String, now: Instant) {
        while self.nonces.len() >= MAX_NONCES {
            if let Some((_, old)) = self.nonces.pop_front() {
                self.nonce_set.remove(&old);
            }
        }
        self.nonce_set.insert(nonce_key.clone());
        self.nonces.push_back((now, nonce_key));
    }

    /// Drops expired entries and nonces old enough that the freshness check
    /// alone rejects any replay of them.
    fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|_, entry| now.saturating_duration_since(entry.last_seen) <= VERIFIED_TTL);
        let nonce_window = Duration::from_millis(2 * MAX_CLOCK_SKEW_MS);
        while let Some((seen, _)) = self.nonces.front() {
            if now.saturating_duration_since(*seen) <= nonce_window {
                break;
            }
            let (_, old) = self.nonces.pop_front().expect("front exists");
            self.nonce_set.remove(&old);
        }
    }

    /// The DID verified for `node` in `room`, if still within the TTL.
    #[allow(dead_code)]
    pub fn did_for(&self, room: &str, node: &str, now: Instant) -> Option<String> {
        self.entries
            .get(&(room.to_string(), node.to_string()))
            .filter(|entry| now.saturating_duration_since(entry.last_seen) <= VERIFIED_TTL)
            .map(|entry| entry.did.clone())
    }

    /// The DID verified for `node` in any room, if still within the TTL.
    #[allow(dead_code)]
    pub fn did_for_any_room(&self, node: &str, now: Instant) -> Option<String> {
        self.entries
            .iter()
            .find(|((_, n), entry)| {
                n == node && now.saturating_duration_since(entry.last_seen) <= VERIFIED_TTL
            })
            .map(|(_, entry)| entry.did.clone())
    }

    pub fn remove(&mut self, room: &str, node: &str) -> bool {
        self.entries
            .remove(&(room.to_string(), node.to_string()))
            .is_some()
    }

    pub fn remove_room(&mut self, room: &str) {
        self.entries.retain(|(r, _), _| r != room);
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(Registry::default()));

/// Optional DID allowlist for control-plane membership (relay consensus,
/// and anything else that opts in via [`admitted_did`]). `None` = open: any
/// DID-verified peer is admitted (the default; keeps today's UX).
static ALLOWLIST: RwLock<Option<Vec<String>>> = RwLock::new(None);

/// Records an already-checked proof in the shared registry (see
/// [`Registry::accept`]).
pub fn accept_proof(room: &str, proof: &DidProof) -> Result<bool, ProofError> {
    REGISTRY
        .lock()
        .expect("peer auth registry poisoned")
        .accept(room, proof, Instant::now())
}

/// The DID verified for `node` in `room`, if any.
// Consumer (tunnel trust keyed on DIDs, config allowlist) lands separately.
#[allow(dead_code)]
pub fn verified_did(room: &str, node: &str) -> Option<String> {
    REGISTRY
        .lock()
        .expect("peer auth registry poisoned")
        .did_for(room, node, Instant::now())
}

/// The DID verified for `node` in any joined room, if any. For callers that
/// don't know which room a peer was seen in.
// Consumer (tunnel trust keyed on DIDs, config allowlist) lands separately.
#[allow(dead_code)]
pub fn verified_did_any_room(node: &str) -> Option<String> {
    REGISTRY
        .lock()
        .expect("peer auth registry poisoned")
        .did_for_any_room(node, Instant::now())
}

/// Forget `node`'s verification in `room` (it left).
pub fn forget_peer(room: &str, node: &str) {
    REGISTRY
        .lock()
        .expect("peer auth registry poisoned")
        .remove(room, node);
}

/// Forget every verification in `room` (we left it).
pub fn forget_room(room: &str) {
    REGISTRY
        .lock()
        .expect("peer auth registry poisoned")
        .remove_room(room);
}

/// Membership allowlist hook: `None` (the default) admits every verified
/// DID; `Some(list)` admits only DIDs in `list` (an empty list admits
/// nobody). Intended to be set from config at startup / on reload.
// Consumer (tunnel trust keyed on DIDs, config allowlist) lands separately.
#[allow(dead_code)]
pub fn set_membership_allowlist(allowlist: Option<Vec<String>>) {
    *ALLOWLIST.write().expect("peer auth allowlist poisoned") = allowlist;
}

/// The current membership allowlist (see [`set_membership_allowlist`]).
// Consumer (tunnel trust keyed on DIDs, config allowlist) lands separately.
#[allow(dead_code)]
pub fn membership_allowlist() -> Option<Vec<String>> {
    ALLOWLIST
        .read()
        .expect("peer auth allowlist poisoned")
        .clone()
}

/// Whether `did` passes `allowlist` (`None` = open).
pub fn admits(allowlist: Option<&[String]>, did: &str) -> bool {
    allowlist.is_none_or(|list| list.iter().any(|allowed| allowed == did))
}

/// Whether `did` passes the current process-wide membership allowlist.
pub fn admitted(did: &str) -> bool {
    admits(
        ALLOWLIST
            .read()
            .expect("peer auth allowlist poisoned")
            .as_deref(),
        did,
    )
}

/// The DID of `node` in `room` when it is both verified and admitted by the
/// membership allowlist.
// Consumer (tunnel trust keyed on DIDs, config allowlist) lands separately.
#[allow(dead_code)]
pub fn admitted_did(room: &str, node: &str) -> Option<String> {
    verified_did(room, node).filter(|did| admitted(did))
}

/// Per-(room, peer) reply throttle so a flood of hellos can't turn this node
/// into a reply amplifier.
static LAST_REPLY: LazyLock<Mutex<HashMap<(String, String), Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn reply_allowed(room: &str, node: &str, now: Instant) -> bool {
    let mut table = LAST_REPLY.lock().expect("peer auth reply table poisoned");
    let key = (room.to_string(), node.to_string());
    if table
        .get(&key)
        .is_some_and(|last| now.saturating_duration_since(*last) < REPLY_THROTTLE)
    {
        return false;
    }
    if table.len() >= MAX_REPLY_ENTRIES {
        table.retain(|_, last| now.saturating_duration_since(*last) < REPLY_THROTTLE);
        if table.len() >= MAX_REPLY_ENTRIES {
            return false;
        }
    }
    table.insert(key, now);
    true
}

fn hello_bytes(room: &str, identity: &Identity, reply: bool) -> Vec<u8> {
    build_proof(HELLO_TAG, room, identity, now_ms(), &fresh_nonce(), reply)
}

/// Starts the DID hello service: a room-aware handler (verify inbound
/// hellos, reply to non-reply hellos, answer joins, forget on leave) and a
/// periodic broadcast into every joined room. Called once from
/// `net::start_engine`.
pub(super) fn start(identity: Arc<Identity>, runtime: tokio::runtime::Handle) {
    let handler_identity = identity.clone();
    let handler_runtime = runtime.clone();
    super::register_room_handler(move |event_type, room, from, data| match event_type {
        super::EVENT_RAW => {
            // Cheap prefilter before any JSON parse: size, then tag bytes.
            if data.len() > MAX_PROOF_BYTES
                || !data
                    .windows(HELLO_TAG.len())
                    .any(|w| w == HELLO_TAG.as_bytes())
            {
                return;
            }
            match check_proof(data, HELLO_TAG, room, from, now_ms()) {
                Ok(proof) => match accept_proof(room, &proof) {
                    Ok(is_new) => {
                        if is_new {
                            debug!(%room, node = %from, did = %proof.did, "peer auth: node verified");
                        }
                        if !proof.reply && reply_allowed(room, from, Instant::now()) {
                            let bytes = hello_bytes(room, &handler_identity, true);
                            let room = room.to_string();
                            let to = from.to_string();
                            handler_runtime.spawn(async move {
                                let _ = super::send_direct(&room, &to, bytes).await;
                            });
                        }
                    }
                    Err(error) => {
                        debug!(%room, node = %from, ?error, "peer auth: hello not recorded");
                    }
                },
                Err(ProofError::NotOurs) => {}
                Err(error) => debug!(%room, node = %from, ?error, "peer auth: rejected hello"),
            }
        }
        super::EVENT_JOIN => {
            // Answer every join directly, so a newcomer learns our DID
            // without waiting for the next periodic broadcast.
            let bytes = hello_bytes(room, &handler_identity, false);
            let room = room.to_string();
            let to = from.to_string();
            handler_runtime.spawn(async move {
                let _ = super::send_direct(&room, &to, bytes).await;
            });
        }
        super::EVENT_LEAVE => forget_peer(room, from),
        _ => {}
    });

    runtime.spawn(async move {
        let mut interval = tokio::time::interval(HELLO_INTERVAL);
        loop {
            interval.tick().await;
            for room in super::joined_rooms().await {
                let bytes = hello_bytes(&room, &identity, false);
                // Fails harmlessly while mistlib's async join is still in
                // flight; the next tick (or a peer's join) covers it.
                let _ = super::send_broadcast(&room, bytes).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proof_for(identity: &Identity, room: &str, ts: u64, nonce: &str) -> Vec<u8> {
        build_proof(HELLO_TAG, room, identity, ts, nonce, false)
    }

    fn fake_proof(node: &str, did: &str, nonce: &str) -> DidProof {
        DidProof {
            did: did.to_string(),
            node: node.to_string(),
            ts_ms: 0,
            nonce: nonce.to_string(),
            reply: false,
        }
    }

    #[test]
    fn signed_hello_verifies_for_its_sender() {
        let id = crate::identity::for_test();
        let now = now_ms();
        let bytes = proof_for(&id, "room-a", now, "n1");
        let proof = check_proof(&bytes, HELLO_TAG, "room-a", &id.node_id(), now).unwrap();
        assert_eq!(proof.did, id.did());
        assert_eq!(proof.node, id.node_id());
        assert!(!proof.reply);
    }

    #[test]
    fn hello_shape_does_not_collide_with_other_demux_tags() {
        let id = crate::identity::for_test();
        let value: Value = serde_json::from_slice(&proof_for(&id, "r", now_ms(), "n")).unwrap();
        for key in ["type", "v", "kind"] {
            assert!(value.get(key).is_none(), "hello must not carry `{key}`");
        }
        // The consensus demux sees an unknown `t` value.
        assert_eq!(value["t"], json!(HELLO_TAG));
    }

    #[test]
    fn hello_from_a_spoofed_sender_is_rejected() {
        let id = crate::identity::for_test();
        let victim = crate::identity::for_test();
        let now = now_ms();
        let bytes = proof_for(&id, "room-a", now, "n1");
        // Delivered with someone else's node id as the `from`.
        assert_eq!(
            check_proof(&bytes, HELLO_TAG, "room-a", &victim.node_id(), now),
            Err(ProofError::NodeMismatch)
        );
    }

    #[test]
    fn hello_whose_did_does_not_hash_to_the_node_id_is_rejected() {
        // Claims victim's node id in both `node` and `from`, but can only
        // sign with its own DID.
        let attacker = crate::identity::for_test();
        let victim = crate::identity::for_test();
        let now = now_ms();
        let mut wire = json!({
            "t": HELLO_TAG, "room": "room-a", "node": victim.node_id(),
            "fromId": attacker.did(), "ts": now, "nonce": "n1", "re": false,
        });
        crate::wiresign::sign_wire(&mut wire, &attacker).unwrap();
        let bytes = serde_json::to_vec(&wire).unwrap();
        assert_eq!(
            check_proof(&bytes, HELLO_TAG, "room-a", &victim.node_id(), now),
            Err(ProofError::DidMismatch)
        );

        // And naming the victim's DID without its key fails the signature.
        wire["fromId"] = json!(victim.did());
        let bytes = serde_json::to_vec(&wire).unwrap();
        assert_eq!(
            check_proof(&bytes, HELLO_TAG, "room-a", &victim.node_id(), now),
            Err(ProofError::BadSignature)
        );
    }

    #[test]
    fn tampered_hello_fails_the_signature() {
        let id = crate::identity::for_test();
        let now = now_ms();
        let mut value: Value =
            serde_json::from_slice(&proof_for(&id, "room-a", now, "n1")).unwrap();
        value["re"] = json!(true);
        let bytes = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            check_proof(&bytes, HELLO_TAG, "room-a", &id.node_id(), now),
            Err(ProofError::BadSignature)
        );
    }

    #[test]
    fn hello_for_another_room_is_rejected() {
        let id = crate::identity::for_test();
        let now = now_ms();
        let bytes = proof_for(&id, "room-a", now, "n1");
        assert_eq!(
            check_proof(&bytes, HELLO_TAG, "room-b", &id.node_id(), now),
            Err(ProofError::WrongRoom)
        );
    }

    #[test]
    fn stale_or_future_hello_is_rejected() {
        let id = crate::identity::for_test();
        let now = now_ms();
        let old = proof_for(&id, "room-a", now - MAX_CLOCK_SKEW_MS - 1, "n1");
        assert_eq!(
            check_proof(&old, HELLO_TAG, "room-a", &id.node_id(), now),
            Err(ProofError::Stale)
        );
        let future = proof_for(&id, "room-a", now + MAX_CLOCK_SKEW_MS + 1, "n2");
        assert_eq!(
            check_proof(&future, HELLO_TAG, "room-a", &id.node_id(), now),
            Err(ProofError::Stale)
        );
    }

    #[test]
    fn foreign_and_oversized_messages_are_ignored() {
        let id = crate::identity::for_test();
        let ai = serde_json::to_vec(&json!({"v": 1, "type": "consumer_hello"})).unwrap();
        assert_eq!(
            check_proof(&ai, HELLO_TAG, "r", &id.node_id(), now_ms()),
            Err(ProofError::NotOurs)
        );
        let raft = serde_json::to_vec(&json!({"t": "mistl-raft-v2", "room": "r"})).unwrap();
        assert_eq!(
            check_proof(&raft, HELLO_TAG, "r", &id.node_id(), now_ms()),
            Err(ProofError::NotOurs)
        );
        let big = vec![b' '; MAX_PROOF_BYTES + 1];
        assert_eq!(
            check_proof(&big, HELLO_TAG, "r", &id.node_id(), now_ms()),
            Err(ProofError::Oversized)
        );
    }

    #[test]
    fn replayed_nonce_is_rejected() {
        let id = crate::identity::for_test();
        let now_wall = now_ms();
        let bytes = proof_for(&id, "room-a", now_wall, "n1");
        let proof = check_proof(&bytes, HELLO_TAG, "room-a", &id.node_id(), now_wall).unwrap();
        let mut reg = Registry::default();
        let now = Instant::now();
        assert_eq!(reg.accept("room-a", &proof, now), Ok(true));
        assert_eq!(
            reg.accept("room-a", &proof, now + Duration::from_secs(1)),
            Err(ProofError::Replay)
        );
        // A fresh hello (new nonce) from the same node is a refresh.
        let again = check_proof(
            &proof_for(&id, "room-a", now_wall, "n2"),
            HELLO_TAG,
            "room-a",
            &id.node_id(),
            now_wall,
        )
        .unwrap();
        assert_eq!(reg.accept("room-a", &again, now), Ok(false));
    }

    #[test]
    fn registry_expires_entries_and_forgets_on_leave() {
        let mut reg = Registry::default();
        let t0 = Instant::now();
        reg.accept("room-a", &fake_proof("n1", "did-1", "a"), t0)
            .unwrap();
        reg.accept("room-b", &fake_proof("n1", "did-1", "b"), t0)
            .unwrap();
        assert_eq!(reg.did_for("room-a", "n1", t0).as_deref(), Some("did-1"));
        assert_eq!(reg.did_for("room-c", "n1", t0), None);
        assert!(reg.remove("room-a", "n1"));
        assert_eq!(reg.did_for("room-a", "n1", t0), None);
        assert_eq!(reg.did_for_any_room("n1", t0).as_deref(), Some("did-1"));
        let later = t0 + VERIFIED_TTL + Duration::from_secs(1);
        assert_eq!(reg.did_for("room-b", "n1", later), None);
        reg.remove_room("room-b");
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn registry_caps_per_room_and_globally() {
        let mut reg = Registry::default();
        let now = Instant::now();
        for i in 0..MAX_VERIFIED_PER_ROOM {
            let p = fake_proof(&format!("n{i}"), &format!("d{i}"), "x");
            assert_eq!(reg.accept("room-a", &p, now), Ok(true));
        }
        let overflow = fake_proof("overflow", "d-over", "x");
        assert_eq!(reg.accept("room-a", &overflow, now), Err(ProofError::Full));
        // Other rooms still have room, up to the global cap.
        let rooms_needed = MAX_VERIFIED / MAX_VERIFIED_PER_ROOM;
        for r in 1..rooms_needed {
            for i in 0..MAX_VERIFIED_PER_ROOM {
                let p = fake_proof(&format!("n{i}"), &format!("d{i}"), &format!("r{r}"));
                assert_eq!(reg.accept(&format!("room-{r}"), &p, now), Ok(true));
            }
        }
        assert_eq!(reg.len(), MAX_VERIFIED);
        assert_eq!(
            reg.accept("room-new", &fake_proof("m", "dm", "y"), now),
            Err(ProofError::Full)
        );
        // Known entries still refresh at the cap.
        assert_eq!(
            reg.accept("room-a", &fake_proof("n0", "d0", "fresh"), now),
            Ok(false)
        );
    }

    #[test]
    fn registry_keeps_the_first_did_for_a_node_id() {
        let mut reg = Registry::default();
        let now = Instant::now();
        reg.accept("r", &fake_proof("n", "did-first", "a"), now)
            .unwrap();
        assert_eq!(
            reg.accept("r", &fake_proof("n", "did-colliding", "b"), now),
            Err(ProofError::DidMismatch)
        );
        assert_eq!(reg.did_for("r", "n", now).as_deref(), Some("did-first"));
    }

    #[test]
    fn nonce_cache_is_bounded() {
        let mut reg = Registry::default();
        let now = Instant::now();
        for i in 0..(MAX_NONCES + 10) {
            reg.accept("r", &fake_proof("n", "d", &format!("{i}")), now)
                .unwrap();
        }
        assert_eq!(reg.nonces.len(), MAX_NONCES);
        assert_eq!(reg.nonce_set.len(), MAX_NONCES);
    }

    #[test]
    fn allowlist_hook_defaults_open() {
        assert!(admits(None, "did:key:zAnyone"));
        let list = vec!["did:key:zAlice".to_string()];
        assert!(admits(Some(&list), "did:key:zAlice"));
        assert!(!admits(Some(&list), "did:key:zMallory"));
        assert!(!admits(Some(&[]), "did:key:zAlice"));
    }
}
