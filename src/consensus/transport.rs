//! Wire envelopes for the consensus control plane, and the [`RaftTransport`]
//! adapter that sends/receives them over `crate::net`'s shared transport.
//!
//! Two message shapes coexist on the wire with ai/tunnel traffic (and with
//! tc-chat's own JSON messages, when this room is shared with browser peers
//! or the chat relay), demuxed the same way every other `crate::net` handler
//! does: a JSON `t` tag, silently ignored when it doesn't match.
//!
//! Both are DID-signed (v2). Identity comes from DIDs, not transport sender
//! ids (see `crate::net::peer_auth`'s module doc), so each message carries its
//! signer's `did:key` (`fromId`) and a [`crate::wiresign`] signature, and is
//! only accepted when `sha256(fromId)[..16 hex] == from`:
//!
//! - `mistl-raft-v2`: a bincode-serialized `RaftMessage`, base64-encoded,
//!   plus `room`, `node`, `boot`/`seq` (per-process random boot id and
//!   message counter, for replay rejection) and `ts` (freshness). The node id
//!   embedded in the Raft message itself (`candidate_id`/`voter_id`/
//!   `leader_id`/`follower_id`) must also equal the sender.
//! - `mistl-relay-hello-v2`: the periodic membership announcement, a
//!   `crate::net::peer_auth` DID proof under this tag.
//!
//! The unsigned v1 tags (`mistl-raft-v1`, `mistl-relay-hello-v1`) of older
//! mistl builds are deliberately ignored: an unverifiable peer never votes or
//! leads. See the module doc of `super` for the mixed-version behaviour.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use mistlib_consensus_core::error::{RaftError, Result as RaftResult};
use mistlib_consensus_core::{NodeId, RaftMessage, RaftTransport};
use serde_json::{Value, json};

use crate::identity::Identity;
use crate::net::peer_auth::{self, DidProof, MAX_CLOCK_SKEW_MS};

/// Tag for the signed Raft RPC envelope.
pub const RAFT_TAG: &str = "mistl-raft-v2";
/// Tag for the signed periodic relay-membership hello.
pub const HELLO_TAG: &str = "mistl-relay-hello-v2";
/// Unsigned tags sent by older builds; recognised only to be ignored.
pub const LEGACY_RAFT_TAG: &str = "mistl-raft-v1";
pub const LEGACY_HELLO_TAG: &str = "mistl-relay-hello-v1";

/// Largest Raft envelope worth parsing. v1 never replicates log entries, so
/// real envelopes are a few hundred bytes.
pub const MAX_RAFT_BYTES: usize = 16 * 1024;

/// A signature-verified Raft message, ready for the membership/replay gate.
#[derive(Debug)]
pub struct SignedRaft {
    pub msg: RaftMessage,
    pub did: String,
    pub boot: u64,
    pub seq: u64,
    pub ts_ms: u64,
}

/// A decoded inbound message, demuxed, room-checked and signature-verified
/// by [`decode`].
#[derive(Debug)]
pub enum WireMessage {
    Raft(SignedRaft),
    Hello(DidProof),
}

/// The node id a Raft message claims to come from, which must equal the
/// transport sender -- otherwise a verified member could cast votes or
/// heartbeats in another member's name.
pub fn embedded_sender(msg: &RaftMessage) -> &NodeId {
    match msg {
        RaftMessage::RequestVote { candidate_id, .. } => candidate_id,
        RaftMessage::RequestVoteResponse { voter_id, .. } => voter_id,
        RaftMessage::AppendEntries { leader_id, .. } => leader_id,
        RaftMessage::AppendEntriesResponse { follower_id, .. } => follower_id,
    }
}

/// Serializes `msg` with bincode and wraps it in a signed JSON envelope
/// other `crate::net` handlers can silently ignore.
pub fn encode_raft_message(
    room: &str,
    identity: &Identity,
    boot: u64,
    seq: u64,
    ts_ms: u64,
    msg: &RaftMessage,
) -> Result<Vec<u8>> {
    let payload = bincode::serialize(msg).context("consensus: encoding RaftMessage")?;
    let mut envelope = json!({
        "t": RAFT_TAG,
        "room": room,
        "node": identity.node_id(),
        "fromId": identity.did(),
        // u64s as strings: JSON numbers above 2^53 don't survive JS peers,
        // and the stable signing payload must be exact.
        "boot": boot.to_string(),
        "seq": seq.to_string(),
        "ts": ts_ms,
        "payload": BASE64.encode(payload),
    });
    crate::wiresign::sign_wire(&mut envelope, identity)?;
    serde_json::to_vec(&envelope).context("consensus: encoding raft envelope")
}

/// Builds the periodic signed relay-hello announcement.
pub fn encode_hello(room: &str, identity: &Identity) -> Vec<u8> {
    peer_auth::build_proof(
        HELLO_TAG,
        room,
        identity,
        peer_auth::now_ms(),
        &peer_auth::fresh_nonce(),
        false,
    )
}

/// Decodes and verifies one inbound raw message that arrived in `room` from
/// transport sender `from`. Returns `None` for anything that isn't one of
/// our two envelope shapes, names a different room, or fails verification
/// (signature, DID/node binding, freshness, embedded sender). Replay and
/// membership are checked by the caller.
pub fn decode(data: &[u8], room: &str, from: &str, now_ms: u64) -> Option<WireMessage> {
    if data.len() > MAX_RAFT_BYTES {
        return None;
    }
    let value: Value = serde_json::from_slice(data).ok()?;
    let tag = value.get("t")?.as_str()?;
    match tag {
        RAFT_TAG => decode_raft(&value, room, from, now_ms).map(WireMessage::Raft),
        HELLO_TAG => peer_auth::check_proof_value(&value, HELLO_TAG, room, from, now_ms)
            .ok()
            .map(WireMessage::Hello),
        _ => None,
    }
}

fn decode_raft(value: &Value, room: &str, from: &str, now_ms: u64) -> Option<SignedRaft> {
    if value.get("room")?.as_str()? != room || value.get("node")?.as_str()? != from {
        return None;
    }
    let did = value.get("fromId")?.as_str()?;
    if did.len() != crate::identity::ED25519_DID_KEY_LEN
        || crate::identity::node_id_for_did(did) != from
    {
        return None;
    }
    let ts_ms = value.get("ts")?.as_u64()?;
    if ts_ms.abs_diff(now_ms) > MAX_CLOCK_SKEW_MS {
        return None;
    }
    let boot: u64 = value.get("boot")?.as_str()?.parse().ok()?;
    let seq: u64 = value.get("seq")?.as_str()?.parse().ok()?;
    let payload_b64 = value.get("payload")?.as_str()?;
    if !crate::wiresign::verify_wire(value).unwrap_or(false) {
        return None;
    }
    let payload = BASE64.decode(payload_b64).ok()?;
    let msg: RaftMessage = bincode::deserialize(&payload).ok()?;
    if embedded_sender(&msg).0 != from {
        return None;
    }
    Some(SignedRaft {
        msg,
        did: did.to_string(),
        boot,
        seq,
        ts_ms,
    })
}

/// [`RaftTransport`] backed by `crate::net`'s room-scoped direct send.
/// `RaftTransport::send` already addresses one peer at a time -- Raft has
/// no broadcast primitive -- which matches mistl's transport shape
/// exactly, so no extra fan-out logic is needed here. Every message is
/// signed with this node's DID and numbered under a per-process boot id.
pub struct MistlRaftTransport {
    room: String,
    identity: Arc<Identity>,
    boot: u64,
    seq: AtomicU64,
}

impl MistlRaftTransport {
    pub fn new(room: String, identity: Arc<Identity>) -> Self {
        Self {
            room,
            identity,
            boot: rand::random(),
            seq: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl RaftTransport for MistlRaftTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) -> RaftResult<()> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let bytes = encode_raft_message(
            &self.room,
            &self.identity,
            self.boot,
            seq,
            peer_auth::now_ms(),
            &msg,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?;
        crate::net::send_direct(&self.room, &to.0, bytes)
            .await
            .map_err(|err| RaftError::Transport(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vote_from(id: &Identity) -> RaftMessage {
        RaftMessage::RequestVote {
            term: 7,
            candidate_id: NodeId(id.node_id()),
            last_log_index: 3,
            last_log_term: 2,
        }
    }

    #[test]
    fn signed_raft_envelope_roundtrips() {
        let id = crate::identity::for_test();
        let now = peer_auth::now_ms();
        let bytes = encode_raft_message("room-a", &id, 9, 1, now, &vote_from(&id)).unwrap();
        match decode(&bytes, "room-a", &id.node_id(), now) {
            Some(WireMessage::Raft(signed)) => {
                assert_eq!(signed.did, id.did());
                assert_eq!((signed.boot, signed.seq), (9, 1));
                match signed.msg {
                    RaftMessage::RequestVote { term, .. } => assert_eq!(term, 7),
                    other => panic!("unexpected {other:?}"),
                }
            }
            other => panic!("expected a decoded RequestVote, got {other:?}"),
        }
    }

    #[test]
    fn raft_envelope_ignored_for_a_different_room() {
        let id = crate::identity::for_test();
        let now = peer_auth::now_ms();
        let bytes = encode_raft_message("room-a", &id, 1, 1, now, &vote_from(&id)).unwrap();
        assert!(decode(&bytes, "room-b", &id.node_id(), now).is_none());
    }

    #[test]
    fn raft_envelope_with_a_spoofed_sender_is_rejected() {
        let id = crate::identity::for_test();
        let victim = crate::identity::for_test();
        let now = peer_auth::now_ms();
        let bytes = encode_raft_message("room-a", &id, 1, 1, now, &vote_from(&id)).unwrap();
        // Delivered under the victim's (forged) transport id.
        assert!(decode(&bytes, "room-a", &victim.node_id(), now).is_none());
    }

    #[test]
    fn raft_message_naming_another_node_inside_is_rejected() {
        let id = crate::identity::for_test();
        let victim = crate::identity::for_test();
        let now = peer_auth::now_ms();
        let forged_vote = RaftMessage::RequestVoteResponse {
            term: 3,
            vote_granted: true,
            voter_id: NodeId(victim.node_id()),
        };
        let bytes = encode_raft_message("room-a", &id, 1, 1, now, &forged_vote).unwrap();
        assert!(decode(&bytes, "room-a", &id.node_id(), now).is_none());
    }

    #[test]
    fn tampered_or_stale_raft_envelope_is_rejected() {
        let id = crate::identity::for_test();
        let now = peer_auth::now_ms();
        let bytes = encode_raft_message("room-a", &id, 1, 1, now, &vote_from(&id)).unwrap();
        let mut value: Value = serde_json::from_slice(&bytes).unwrap();
        value["seq"] = json!("2");
        let tampered = serde_json::to_vec(&value).unwrap();
        assert!(decode(&tampered, "room-a", &id.node_id(), now).is_none());

        let old = encode_raft_message(
            "room-a",
            &id,
            1,
            1,
            now - MAX_CLOCK_SKEW_MS - 1,
            &vote_from(&id),
        )
        .unwrap();
        assert!(decode(&old, "room-a", &id.node_id(), now).is_none());
    }

    #[test]
    fn signed_hello_roundtrips_and_is_room_bound() {
        let id = crate::identity::for_test();
        let bytes = encode_hello("room-a", &id);
        let now = peer_auth::now_ms();
        match decode(&bytes, "room-a", &id.node_id(), now) {
            Some(WireMessage::Hello(proof)) => assert_eq!(proof.did, id.did()),
            other => panic!("expected a decoded Hello, got {other:?}"),
        }
        assert!(decode(&bytes, "room-b", &id.node_id(), now).is_none());
        let other = crate::identity::for_test();
        assert!(decode(&bytes, "room-a", &other.node_id(), now).is_none());
    }

    #[test]
    fn unsigned_legacy_envelopes_are_ignored() {
        let id = crate::identity::for_test();
        let now = peer_auth::now_ms();
        let legacy_hello = serde_json::to_vec(&json!({
            "t": LEGACY_HELLO_TAG, "room": "room-a", "node": id.node_id(),
        }))
        .unwrap();
        assert!(decode(&legacy_hello, "room-a", &id.node_id(), now).is_none());

        let payload = bincode::serialize(&vote_from(&id)).unwrap();
        let legacy_raft = serde_json::to_vec(&json!({
            "t": LEGACY_RAFT_TAG, "room": "room-a", "payload": BASE64.encode(payload),
        }))
        .unwrap();
        assert!(decode(&legacy_raft, "room-a", &id.node_id(), now).is_none());
    }

    #[test]
    fn decode_ignores_foreign_message_shapes() {
        let now = peer_auth::now_ms();
        let foreign_tagged = serde_json::to_vec(&json!({
            "t": "something-else",
            "room": "room-a",
            "body": {},
        }))
        .unwrap();
        assert!(decode(&foreign_tagged, "room-a", "n", now).is_none());

        // The network-wide DID hello is a different tag.
        let id = crate::identity::for_test();
        let did_hello =
            peer_auth::build_proof(peer_auth::HELLO_TAG, "room-a", &id, now, "nonce", false);
        assert!(decode(&did_hello, "room-a", &id.node_id(), now).is_none());

        let ai_like = serde_json::to_vec(&json!({ "v": 1, "type": "consumer_hello" })).unwrap();
        assert!(decode(&ai_like, "room-a", "n", now).is_none());
        assert!(decode(b"\x00\x01\x02not json", "room-a", "n", now).is_none());
        assert!(decode(b"[1,2,3]", "room-a", "n", now).is_none());
    }
}
