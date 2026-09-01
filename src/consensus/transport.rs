//! Wire envelopes for the consensus control plane, and the [`RaftTransport`]
//! adapter that sends/receives them over `crate::net`'s shared transport.
//!
//! Two message shapes coexist on the wire with ai/tunnel traffic (and with
//! tc-chat's own JSON messages, when this room is shared with browser peers
//! or the chat relay), demuxed the same way every other `crate::net` handler
//! does: a JSON `t` tag, silently ignored when it doesn't match.
//!
//! - `mistl-raft-v1`: a bincode-serialized `RaftMessage`, base64-encoded,
//!   further scoped by `room` (the raw handler sees every joined room's
//!   traffic, so a foreign room's envelope of the same shape must also be
//!   ignored).
//! - `mistl-relay-hello-v1`: the lightweight periodic membership
//!   announcement consumed by [`super::membership::Membership`].

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use mistlib_consensus_core::error::{RaftError, Result as RaftResult};
use mistlib_consensus_core::{NodeId, RaftMessage, RaftTransport};
use serde_json::{Value, json};

/// Tag for the Raft RPC envelope.
pub const RAFT_TAG: &str = "mistl-raft-v1";
/// Tag for the periodic relay-membership hello.
pub const HELLO_TAG: &str = "mistl-relay-hello-v1";

/// A decoded inbound message, demuxed and room-checked by [`decode`].
pub enum WireMessage {
    Raft(RaftMessage),
    Hello { node: String },
}

/// Serializes `msg` with bincode and wraps it in the JSON envelope other
/// `crate::net` handlers can silently ignore.
pub fn encode_raft_message(room: &str, msg: &RaftMessage) -> Result<Vec<u8>> {
    let payload = bincode::serialize(msg).context("consensus: encoding RaftMessage")?;
    let envelope = json!({
        "t": RAFT_TAG,
        "room": room,
        "payload": BASE64.encode(payload),
    });
    serde_json::to_vec(&envelope).context("consensus: encoding raft envelope")
}

/// Builds the periodic relay-hello announcement.
pub fn encode_hello(room: &str, node_id: &str) -> Vec<u8> {
    let envelope = json!({
        "t": HELLO_TAG,
        "room": room,
        "node": node_id,
    });
    serde_json::to_vec(&envelope).expect("hello envelope serialization cannot fail")
}

/// Decodes one inbound raw message addressed to `room`. Returns `None` for
/// anything that isn't one of our two envelope shapes, or that names a
/// different room (the raw handler fans out every joined room's traffic,
/// so a foreign room's otherwise-identical envelope must be ignored too).
pub fn decode(data: &[u8], room: &str) -> Option<WireMessage> {
    let value: Value = serde_json::from_slice(data).ok()?;
    let tag = value.get("t")?.as_str()?;
    let msg_room = value.get("room")?.as_str()?;
    if msg_room != room {
        return None;
    }
    match tag {
        RAFT_TAG => {
            let payload_b64 = value.get("payload")?.as_str()?;
            let payload = BASE64.decode(payload_b64).ok()?;
            let msg = bincode::deserialize(&payload).ok()?;
            Some(WireMessage::Raft(msg))
        }
        HELLO_TAG => {
            let node = value.get("node")?.as_str()?.to_string();
            Some(WireMessage::Hello { node })
        }
        _ => None,
    }
}

/// [`RaftTransport`] backed by `crate::net`'s room-scoped direct send.
/// `RaftTransport::send` already addresses one peer at a time -- Raft has
/// no broadcast primitive -- which matches mistl's transport shape
/// exactly, so no extra fan-out logic is needed here.
pub struct MistlRaftTransport {
    room: String,
}

impl MistlRaftTransport {
    pub fn new(room: String) -> Self {
        Self { room }
    }
}

#[async_trait]
impl RaftTransport for MistlRaftTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) -> RaftResult<()> {
        let bytes = encode_raft_message(&self.room, &msg)
            .map_err(|err| RaftError::Transport(err.to_string()))?;
        crate::net::send_direct(&self.room, &to.0, bytes)
            .await
            .map_err(|err| RaftError::Transport(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_message() -> RaftMessage {
        RaftMessage::RequestVote {
            term: 7,
            candidate_id: NodeId("cand".into()),
            last_log_index: 3,
            last_log_term: 2,
        }
    }

    #[test]
    fn raft_envelope_roundtrips() {
        let bytes = encode_raft_message("room-a", &sample_message()).unwrap();
        match decode(&bytes, "room-a") {
            Some(WireMessage::Raft(RaftMessage::RequestVote {
                term, candidate_id, ..
            })) => {
                assert_eq!(term, 7);
                assert_eq!(candidate_id, NodeId("cand".into()));
            }
            _ => panic!("expected a decoded RequestVote"),
        }
    }

    #[test]
    fn raft_envelope_ignored_for_a_different_room() {
        let bytes = encode_raft_message("room-a", &sample_message()).unwrap();
        assert!(decode(&bytes, "room-b").is_none());
    }

    #[test]
    fn hello_envelope_roundtrips() {
        let bytes = encode_hello("room-a", "node-123");
        match decode(&bytes, "room-a") {
            Some(WireMessage::Hello { node }) => assert_eq!(node, "node-123"),
            _ => panic!("expected a decoded Hello"),
        }
    }

    #[test]
    fn hello_envelope_ignored_for_a_different_room() {
        let bytes = encode_hello("room-a", "node-123");
        assert!(decode(&bytes, "room-b").is_none());
    }

    #[test]
    fn decode_ignores_foreign_message_shapes() {
        // Foreign JSON that happens to carry a `t` tag that isn't ours.
        let foreign_tagged = serde_json::to_vec(&json!({
            "t": "something-else",
            "room": "room-a",
            "body": {},
        }))
        .unwrap();
        assert!(decode(&foreign_tagged, "room-a").is_none());

        // ai-protocol-shaped JSON (no `t` field at all, uses `v`/`type`).
        let ai_like = serde_json::to_vec(&json!({ "v": 1, "type": "consumer_hello" })).unwrap();
        assert!(decode(&ai_like, "room-a").is_none());

        // Not JSON at all.
        assert!(decode(b"\x00\x01\x02not json", "room-a").is_none());

        // Valid JSON, but not an object (no `t` to read).
        assert!(decode(b"[1,2,3]", "room-a").is_none());
    }

    #[test]
    fn decode_ignores_a_raft_tag_with_corrupt_payload() {
        let bad = serde_json::to_vec(&json!({
            "t": RAFT_TAG,
            "room": "room-a",
            "payload": "not-valid-base64!!",
        }))
        .unwrap();
        assert!(decode(&bad, "room-a").is_none());
    }
}
