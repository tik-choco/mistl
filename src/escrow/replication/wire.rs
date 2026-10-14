//! Signed, room-scoped RPCs for a fixed escrow replication cluster.
use std::collections::{HashMap, HashSet};

use super::super::protocol::Head;
use super::snapshots::{Chunk, Offer, Request};
use anyhow::{Result, bail};
use mistlib_consensus_core::{NodeId, RaftMembership, RaftMessage};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::identity::Identity;
use crate::net::peer_auth::{MAX_CLOCK_SKEW_MS, now_ms};

pub const TAG: &str = "mistl-escrow-raft-v1";
pub const MAX_RPC_BYTES: usize = 16 * 1024;
pub const MAX_VOTERS: usize = 64;
const MAX_BOOTS: usize = 8;
const REPLAY_WINDOW: u64 = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Body {
    Raft(RaftMessage),
    SnapshotOffer(Offer),
    SnapshotRequest(Request),
    SnapshotChunk(Chunk),
    Hello {
        membership: RaftMembership,
    },
    MigrationRequest {
        ticket: String,
        offset: u32,
    },
    MigrationReply {
        ticket: String,
        offset: u32,
        heads: Vec<Head>,
        done: bool,
    },
    Forward {
        request: Value,
        source: String,
        ticket: String,
    },
    Reply {
        ticket: String,
        response: Value,
    },
}

#[derive(Debug)]
pub struct Decoded {
    pub body: Body,
    pub from: String,
    pub did: String,
    pub boot: u64,
    pub seq: u64,
    pub ts_ms: u64,
}

/// Includes the room and sorted voter identities, independent of configuration order.
pub fn cluster_hash(room: &str, voters: &[String]) -> String {
    let mut ids: Vec<&str> = voters.iter().map(String::as_str).collect();
    ids.sort_unstable();
    ids.dedup();
    let scope = serde_json::to_vec(&(TAG, room, ids)).expect("cluster scope serialization");
    hex::encode(Sha256::digest(scope))
}

fn embedded_sender(msg: &RaftMessage) -> &NodeId {
    match msg {
        RaftMessage::RequestVote { candidate_id, .. } => candidate_id,
        RaftMessage::RequestVoteResponse { voter_id, .. } => voter_id,
        RaftMessage::AppendEntries { leader_id, .. }
        | RaftMessage::InstallSnapshot { leader_id, .. } => leader_id,
        RaftMessage::AppendEntriesResponse { follower_id, .. }
        | RaftMessage::InstallSnapshotResponse { follower_id, .. }
        | RaftMessage::AppendEntriesConflict { follower_id, .. } => follower_id,
    }
}

fn identifier(id: &str, limit: usize) -> bool {
    !id.is_empty()
        && id.len() <= limit
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_./:-".contains(&b))
}

fn bounded_json(value: &Value) -> bool {
    serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= super::super::protocol::MAX_MESSAGE)
}

fn valid_members(members: &RaftMembership) -> bool {
    mistlib_consensus_core::state::valid_membership(members)
        && super::membership::peers(members).len() <= 18
        && members
            .voters
            .iter()
            .chain(members.joint_voters.iter().flatten())
            .chain(members.learners.iter())
            .all(|id| super::super::valid_voter(&id.0))
}

fn valid_body(body: &Body, from: &str) -> bool {
    match body {
        Body::Raft(msg) => {
            !matches!(msg, RaftMessage::InstallSnapshot { .. }) && embedded_sender(msg).0 == from
        }
        Body::SnapshotOffer(offer) => {
            identifier(&offer.ticket, 64)
                && super::super::protocol::hex_string(&offer.sha, 64)
                && offer.size > 0
                && offer.size <= 64 * 1024 * 1024
                && offer.index > 0
                && offer.snapshot_term > 0
                && offer.snapshot_term <= offer.term
                && offer.membership.as_ref().is_none_or(valid_members)
        }
        Body::SnapshotRequest(request) => {
            identifier(&request.ticket, 64) && request.offset <= 64 * 1024 * 1024
        }
        Body::SnapshotChunk(chunk) => {
            identifier(&chunk.ticket, 64)
                && chunk.offset <= 64 * 1024 * 1024
                && !chunk.data.is_empty()
                && chunk.data.len() <= 12 * 1024
        }
        Body::Hello { membership } => valid_members(membership),
        Body::MigrationRequest { ticket, offset } => identifier(ticket, 64) && *offset <= 65536,
        Body::MigrationReply {
            ticket,
            offset,
            heads,
            ..
        } => {
            identifier(ticket, 64)
                && *offset <= 65536
                && heads.len() <= 4
                && heads
                    .iter()
                    .all(|head| head.validate(1024 * 1024 * 1024).is_ok() && head.verify().is_ok())
        }
        Body::Forward {
            request,
            source,
            ticket,
        } => {
            identifier(source, 128)
                && identifier(ticket, 64)
                && bounded_json(request)
                && request["v"] == 1
                && matches!(
                    request["type"].as_str(),
                    Some("escrow_get" | "escrow_put" | "escrow_delete")
                )
        }
        Body::Reply { ticket, response } => identifier(ticket, 64) && bounded_json(response),
    }
}

pub fn encode(
    room: &str,
    cluster: &str,
    identity: &Identity,
    boot: u64,
    seq: u64,
    body: &Body,
) -> Result<Vec<u8>> {
    encode_at(room, cluster, identity, boot, seq, body, now_ms())
}

fn encode_at(
    room: &str,
    cluster: &str,
    identity: &Identity,
    boot: u64,
    seq: u64,
    body: &Body,
    ts_ms: u64,
) -> Result<Vec<u8>> {
    if !super::super::valid_room(room)
        || !super::super::protocol::hex_string(cluster, 64)
        || seq == 0
        || !valid_body(body, &identity.node_id())
    {
        bail!("invalid escrow replication envelope");
    }
    let mut envelope = json!({
        "t": TAG, "room": room, "cluster": cluster,
        "node": identity.node_id(), "fromId": identity.did(),
        "boot": boot.to_string(), "seq": seq.to_string(), "ts": ts_ms, "body": body,
    });
    // Gate before signing too: a catch-up batch must be trimmed by the driver.
    if serde_json::to_vec(&envelope)?.len() > MAX_RPC_BYTES - 128 {
        bail!("escrow replication RPC exceeds size limit");
    }
    crate::wiresign::sign_wire(&mut envelope, identity)?;
    let bytes = serde_json::to_vec(&envelope)?;
    if bytes.len() > MAX_RPC_BYTES {
        bail!("escrow replication RPC exceeds size limit");
    }
    Ok(bytes)
}

/// Stateless verification; callers must also pass the result through Replay.
pub fn decode(
    raw: &[u8],
    room: &str,
    cluster: &str,
    from: &str,
    voters: &[String],
    now_ms: u64,
) -> Option<Decoded> {
    if raw.len() > MAX_RPC_BYTES
        || voters.is_empty()
        || voters.len() > MAX_VOTERS
        || voters.iter().collect::<HashSet<_>>().len() != voters.len()
        || !super::super::valid_room(room)
        || !voters
            .iter()
            .all(|id| super::super::protocol::hex_string(id, 16))
        || !voters.iter().any(|id| id == from)
        || !super::super::protocol::hex_string(cluster, 64)
    {
        return None;
    }
    let value: Value = serde_json::from_slice(raw).ok()?;
    if value.get("t")?.as_str()? != TAG
        || value.get("room")?.as_str()? != room
        || value.get("cluster")?.as_str()? != cluster
        || value.get("node")?.as_str()? != from
    {
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
    let counter = |key: &str| -> Option<u64> {
        let s = value.get(key)?.as_str()?;
        if s.len() > 20 || s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        s.parse().ok()
    };
    let boot = counter("boot")?;
    let seq = counter("seq")?;
    if seq == 0 || !crate::wiresign::verify_wire(&value).unwrap_or(false) {
        return None;
    }
    let body: Body = serde_json::from_value(value.get("body")?.clone()).ok()?;
    if !valid_body(&body, from) {
        return None;
    }
    Some(Decoded {
        body,
        from: from.into(),
        did: did.into(),
        boot,
        seq,
        ts_ms,
    })
}

#[derive(Debug)]
struct Window {
    max_seq: u64,
    bitmap: u64,
    last_ts_ms: u64,
}

impl Window {
    fn new(seq: u64, ts_ms: u64) -> Self {
        Self {
            max_seq: seq,
            bitmap: 1,
            last_ts_ms: ts_ms,
        }
    }

    fn accept(&mut self, seq: u64, ts_ms: u64) -> bool {
        if seq > self.max_seq {
            let shift = seq - self.max_seq;
            self.bitmap = if shift >= REPLAY_WINDOW {
                1
            } else {
                (self.bitmap << shift) | 1
            };
            self.max_seq = seq;
        } else {
            let offset = self.max_seq - seq;
            if offset >= REPLAY_WINDOW || self.bitmap & (1 << offset) != 0 {
                return false;
            }
            self.bitmap |= 1 << offset;
        }
        self.last_ts_ms = self.last_ts_ms.max(ts_ms);
        true
    }
}

#[derive(Debug)]
struct Peer {
    did: String,
    boots: HashMap<u64, Window>,
}

/// Fresh windows are never evicted to make space: that would permit captured replays.
#[derive(Debug, Default)]
pub struct Replay {
    peers: HashMap<String, Peer>,
}

impl Replay {
    pub fn accept(&mut self, msg: &Decoded, now_ms: u64) -> bool {
        if msg.seq == 0 || msg.ts_ms.abs_diff(now_ms) > MAX_CLOCK_SKEW_MS {
            return false;
        }
        self.peers.retain(|_, peer| {
            peer.boots
                .retain(|_, window| now_ms <= window.last_ts_ms.saturating_add(MAX_CLOCK_SKEW_MS));
            !peer.boots.is_empty()
        });
        if !self.peers.contains_key(&msg.from) {
            if self.peers.len() >= MAX_VOTERS {
                return false;
            }
            self.peers.insert(
                msg.from.clone(),
                Peer {
                    did: msg.did.clone(),
                    boots: HashMap::new(),
                },
            );
        }
        let peer = self.peers.get_mut(&msg.from).unwrap();
        if peer.did != msg.did {
            return false;
        }
        if let Some(window) = peer.boots.get_mut(&msg.boot) {
            return window.accept(msg.seq, msg.ts_ms);
        }
        if peer.boots.len() >= MAX_BOOTS {
            return false;
        }
        peer.boots.insert(msg.boot, Window::new(msg.seq, msg.ts_ms));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vote(id: &Identity) -> Body {
        Body::Raft(RaftMessage::RequestVote {
            term: 7,
            candidate_id: NodeId(id.node_id()),
            last_log_index: 3,
            last_log_term: 2,
        })
    }

    fn fixture() -> (Identity, Vec<String>, String) {
        let id = crate::identity::for_test();
        let voters = vec![id.node_id()];
        let cluster = cluster_hash("room-a", &voters);
        (id, voters, cluster)
    }

    #[test]
    fn signed_rpc_roundtrips_and_scopes_to_room_cluster_and_transport() {
        let (id, voters, cluster) = fixture();
        let raw = encode_at("room-a", &cluster, &id, 9, 1, &vote(&id), 1000).unwrap();
        let decoded = decode(&raw, "room-a", &cluster, &id.node_id(), &voters, 1000).unwrap();
        assert_eq!((decoded.boot, decoded.seq), (9, 1));
        assert_eq!(decoded.did, id.did());
        assert!(matches!(
            decoded.body,
            Body::Raft(RaftMessage::RequestVote { term: 7, .. })
        ));
        assert!(decode(&raw, "room-b", &cluster, &id.node_id(), &voters, 1000).is_none());
        assert!(
            decode(
                &raw,
                "room-a",
                &"0".repeat(64),
                &id.node_id(),
                &voters,
                1000
            )
            .is_none()
        );
        assert!(decode(&raw, "room-a", &cluster, "0000000000000000", &voters, 1000).is_none());
        assert!(decode(&raw, "room-a", &cluster, &id.node_id(), &[], 1000).is_none());
        assert!(
            decode(
                &raw,
                "room-a",
                &cluster,
                &id.node_id(),
                &voters,
                1001 + MAX_CLOCK_SKEW_MS
            )
            .is_none()
        );
    }

    #[test]
    fn scope_hash_is_order_independent_but_room_and_members_change_it() {
        let a = vec!["a".repeat(16), "b".repeat(16)];
        let b = vec![a[1].clone(), a[0].clone()];
        assert_eq!(cluster_hash("r", &a), cluster_hash("r", &b));
        assert_ne!(cluster_hash("r", &a), cluster_hash("s", &a));
        assert_ne!(cluster_hash("r", &a), cluster_hash("r", &a[..1]));
    }

    #[test]
    fn tampering_and_signed_embedded_sender_spoofing_are_rejected() {
        let (id, voters, cluster) = fixture();
        let raw = encode_at("room-a", &cluster, &id, 9, 1, &vote(&id), 1000).unwrap();
        let mut value: Value = serde_json::from_slice(&raw).unwrap();
        value["seq"] = json!("2");
        assert!(
            decode(
                &serde_json::to_vec(&value).unwrap(),
                "room-a",
                &cluster,
                &id.node_id(),
                &voters,
                1000
            )
            .is_none()
        );
        value["body"]["data"]["RequestVote"]["candidate_id"] = json!("0000000000000000");
        crate::wiresign::sign_wire(&mut value, &id).unwrap();
        assert!(
            decode(
                &serde_json::to_vec(&value).unwrap(),
                "room-a",
                &cluster,
                &id.node_id(),
                &voters,
                1000
            )
            .is_none()
        );
    }

    #[test]
    fn forwards_allow_only_bounded_client_requests_and_identifiers() {
        let (id, voters, cluster) = fixture();
        let mut body = Body::Forward {
            request: json!({"v":1,"type":"escrow_get","slot":"a".repeat(32),"id":"req"}),
            source: "browser-peer".into(),
            ticket: "ticket-1".into(),
        };
        let raw = encode_at("room-a", &cluster, &id, 1, 1, &body, 1000).unwrap();
        assert!(decode(&raw, "room-a", &cluster, &id.node_id(), &voters, 1000).is_some());
        if let Body::Forward {
            request,
            source,
            ticket,
        } = &mut body
        {
            *source = "x".repeat(129);
            assert!(!identifier(source, 128));
            *source = "source".into();
            *ticket = "x".repeat(65);
            assert!(!identifier(ticket, 64));
            *ticket = "ticket".into();
            request["type"] = json!("storage_delete");
        }
        assert!(encode_at("room-a", &cluster, &id, 1, 1, &body, 1000).is_err());
        if let Body::Forward { request, .. } = &mut body {
            request["type"] = json!("escrow_put");
            request["large"] = json!("x".repeat(super::super::super::protocol::MAX_MESSAGE));
        }
        assert!(encode_at("room-a", &cluster, &id, 1, 1, &body, 1000).is_err());
        let reply = Body::Reply {
            ticket: "ticket".into(),
            response: json!("x".repeat(16 * 1024)),
        };
        assert!(encode_at("room-a", &cluster, &id, 1, 1, &reply, 1000).is_err());
        assert!(
            decode(
                &vec![b' '; MAX_RPC_BYTES + 1],
                "room-a",
                &cluster,
                &id.node_id(),
                &voters,
                1000
            )
            .is_none()
        );
    }

    fn message(from: &str, boot: u64, seq: u64, ts_ms: u64) -> Decoded {
        Decoded {
            body: Body::Reply {
                ticket: "ticket".into(),
                response: Value::Null,
            },
            from: from.into(),
            did: format!("did-{from}"),
            boot,
            seq,
            ts_ms,
        }
    }

    #[test]
    fn replay_tracks_boots_and_reordering_without_evicting_fresh_windows() {
        let mut replay = Replay::default();
        assert!(replay.accept(&message("peer", 1, 1, 1000), 1000));
        assert!(replay.accept(&message("peer", 1, 3, 1000), 1000));
        assert!(replay.accept(&message("peer", 1, 2, 1000), 1000));
        assert!(!replay.accept(&message("peer", 1, 2, 1000), 1000));
        assert!(replay.accept(&message("peer", 2, 1, 1000), 1000));
        assert!(!replay.accept(&message("peer", 1, 3, 1000), 1000));
        assert!(replay.accept(&message("peer", 1, 200, 1000), 1000));
        assert!(!replay.accept(&message("peer", 1, 100, 1000), 1000));
        for boot in 3..=MAX_BOOTS as u64 {
            assert!(replay.accept(&message("peer", boot, 1, 1000), 1000));
        }
        assert!(!replay.accept(&message("peer", 99, 1, 1000), 1000));
        assert_eq!(replay.peers["peer"].boots.len(), MAX_BOOTS);
        assert!(replay.accept(
            &message("peer", 99, 1, 1001 + MAX_CLOCK_SKEW_MS),
            1001 + MAX_CLOCK_SKEW_MS
        ));
        assert!(!replay.accept(&message("peer", 1, 1, 1000), 1001 + MAX_CLOCK_SKEW_MS));
    }

    #[test]
    fn replay_caps_peers_and_preserves_first_did_binding() {
        let mut replay = Replay::default();
        for n in 0..MAX_VOTERS {
            assert!(replay.accept(&message(&format!("peer-{n}"), 1, 1, 1000), 1000));
        }
        assert!(!replay.accept(&message("overflow", 1, 1, 1000), 1000));
        let mut collision = message("peer-0", 1, 2, 1000);
        collision.did = "other-did".into();
        assert!(!replay.accept(&collision, 1000));
        assert_eq!(replay.peers.len(), MAX_VOTERS);
    }
}
