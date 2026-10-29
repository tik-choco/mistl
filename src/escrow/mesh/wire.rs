//! Signed control envelopes on the escrow mesh room (`t` = "mistl-escrow-mesh-v1").
use crate::identity::{Identity, node_id_for_did, pubkey_from_did};
use crate::wiresign::{sign_wire, verify_wire};
use anyhow::{Context, Result, ensure};
use rand::RngCore;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};

pub const TAG: &str = "mistl-escrow-mesh-v1";
pub const MAX_MESSAGE: usize = 16 * 1024;
pub const MAX_SKEW_MS: u64 = 120_000;

/// Build and sign an envelope: adds t/type/fromId/room/ts/nonce to `body`
/// (a JSON object). Fails if the result exceeds MAX_MESSAGE.
pub fn seal(me: &Identity, room: &str, kind: &str, body: Value, now_ms: u64) -> Result<Vec<u8>> {
    ensure!(crate::escrow::valid_room(room), "invalid escrow mesh room");
    ensure!(known_kind(kind), "unknown escrow mesh message type");
    let mut map = body
        .as_object()
        .context("escrow mesh body must be an object")?
        .clone();
    for key in ["t", "type", "fromId", "room", "ts", "nonce", "signature"] {
        ensure!(!map.contains_key(key), "reserved envelope field: {key}");
    }
    let mut nonce = [0u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    map.insert("t".into(), TAG.into());
    map.insert("type".into(), kind.into());
    map.insert("fromId".into(), me.did().into());
    map.insert("room".into(), room.into());
    map.insert("ts".into(), now_ms.into());
    map.insert("nonce".into(), hex::encode(nonce).into());
    let mut raw = Value::Object(map);
    ensure!(
        serde_json::to_vec(&raw)?.len() <= MAX_MESSAGE,
        "escrow mesh message too large"
    );
    sign_wire(&mut raw, me)?;
    let bytes = serde_json::to_vec(&raw)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "escrow mesh message too large");
    Ok(bytes)
}

/// Cheap shape test used by the room handler before any work.
pub fn is_mesh_message(bytes: &[u8]) -> bool {
    bytes.len() <= MAX_MESSAGE
        && serde_json::from_slice::<Value>(bytes).is_ok_and(|v| v["t"] == TAG)
}

/// A verified inbound envelope; `body` is the whole envelope object.
#[derive(Debug, Clone)]
pub struct Inbound {
    pub kind: String,
    pub from_did: String,
    pub body: Value,
}

/// Replay/rate state for inbound verification (bounded).
#[derive(Default)]
pub struct Guard {
    nonces: HashSet<(String, String)>,
    nonce_order: VecDeque<(String, String)>,
    peers: HashMap<String, PeerRate>,
}

#[derive(Default)]
struct PeerRate {
    start: u64,
    count: u32,
}

const MAX_NONCES: usize = 4096;
const MAX_PEERS: usize = 256;
const RATE_WINDOW_MS: u64 = 10_000;
const RATE_MESSAGES: u32 = 50;

impl Guard {
    /// Checks size, tag, signature, room, skew, nonce replay, that
    /// node_id_for_did(fromId) == `transport_from`, and a per-peer rate cap.
    /// Membership authorization is the caller's job.
    pub fn open(
        &mut self,
        bytes: &[u8],
        room: &str,
        transport_from: &str,
        now_ms: u64,
    ) -> Result<Inbound> {
        ensure!(bytes.len() <= MAX_MESSAGE, "escrow mesh message too large");
        let raw: Value = serde_json::from_slice(bytes)?;
        ensure!(raw["t"] == TAG, "invalid escrow mesh tag");
        self.record_message(transport_from, now_ms)?;
        ensure!(verify_wire(&raw)?, "invalid escrow mesh signature");
        ensure!(
            crate::escrow::valid_room(room) && raw["room"] == room,
            "wrong escrow mesh room"
        );
        let ts = raw["ts"]
            .as_u64()
            .context("invalid escrow mesh timestamp")?;
        ensure!(
            now_ms.abs_diff(ts) <= MAX_SKEW_MS,
            "escrow mesh message outside time window"
        );
        let nonce = raw["nonce"].as_str().context("missing escrow mesh nonce")?;
        ensure!(valid_hex(nonce, 16), "invalid escrow mesh nonce");
        let did = raw["fromId"]
            .as_str()
            .context("missing escrow mesh signer")?;
        pubkey_from_did(did)?;
        let replay_key = (did.to_owned(), nonce.to_owned());
        ensure!(
            !self.nonces.contains(&replay_key),
            "replayed escrow mesh message"
        );
        ensure!(
            node_id_for_did(did) == transport_from,
            "escrow mesh sender differs from transport"
        );
        let kind = raw["type"].as_str().context("missing escrow mesh type")?;
        validate_body(kind, &raw)?;

        if self.nonce_order.len() == MAX_NONCES {
            if let Some(old) = self.nonce_order.pop_front() {
                self.nonces.remove(&old);
            }
        }
        self.nonces.insert(replay_key.clone());
        self.nonce_order.push_back(replay_key);
        Ok(Inbound {
            kind: kind.to_owned(),
            from_did: did.to_owned(),
            body: raw,
        })
    }

    fn record_message(&mut self, transport_from: &str, now_ms: u64) -> Result<()> {
        ensure!(valid_hex(transport_from, 16), "invalid transport node id");
        self.peers
            .retain(|_, rate| now_ms >= rate.start && now_ms - rate.start < RATE_WINDOW_MS);
        ensure!(
            self.peers.contains_key(transport_from) || self.peers.len() < MAX_PEERS,
            "escrow mesh peer rate table full"
        );
        let rate = self
            .peers
            .entry(transport_from.to_owned())
            .or_insert(PeerRate {
                start: now_ms,
                count: 0,
            });
        ensure!(rate.count < RATE_MESSAGES, "escrow mesh peer rate exceeded");
        rate.count += 1;
        Ok(())
    }
}

fn valid_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn known_kind(kind: &str) -> bool {
    matches!(
        kind,
        "hello" | "index_req" | "index" | "head_req" | "head" | "tombstone"
    )
}

fn validate_body(kind: &str, raw: &Value) -> Result<()> {
    ensure!(
        raw.get("v").is_none(),
        "mesh envelope must not be a public request"
    );
    match kind {
        "hello" => {
            for key in ["free", "capacity", "since", "count"] {
                ensure!(raw[key].as_u64().is_some(), "invalid hello {key}");
            }
            ensure!(
                raw["count"].as_u64().unwrap_or(u64::MAX) <= 65536,
                "too many slots"
            );
            ensure!(
                raw["free"].as_u64() <= raw["capacity"].as_u64(),
                "invalid free capacity"
            );
            for key in ["digest", "held_digest"] {
                ensure!(
                    raw[key].as_str().is_some_and(|s| valid_hex(s, 64)),
                    "invalid digest"
                );
            }
        }
        "index_req" => ensure!(
            raw["after"].is_null() || raw["after"].as_str().is_some_and(|s| valid_hex(s, 32)),
            "invalid cursor"
        ),
        "index" => {
            let entries = raw["entries"].as_array().context("invalid index")?;
            ensure!(entries.len() <= 128, "too many index entries");
            let mut previous = "";
            for e in entries {
                let slot = e["slot"].as_str().context("invalid slot")?;
                ensure!(valid_hex(slot, 32) && slot > previous, "index order");
                previous = slot;
                ensure!(
                    e["seq"]
                        .as_u64()
                        .is_some_and(|n| (1..=9_007_199_254_740_991).contains(&n)),
                    "invalid sequence"
                );
                ensure!(
                    e["hash"].as_str().is_some_and(|s| valid_hex(s, 64)),
                    "invalid hash"
                );
                ensure!(
                    matches!(e["kind"].as_str(), Some("head" | "tombstone"))
                        && e["held"].is_boolean(),
                    "invalid entry"
                );
                ensure!(
                    e["kind"] != "tombstone" || e["held"] == false,
                    "held tombstone"
                );
            }
            ensure!(
                raw["next"].is_null()
                    || (!entries.is_empty() && raw["next"].as_str() == Some(previous)),
                "invalid next cursor"
            );
        }
        "head_req" => ensure!(
            raw["slot"].as_str().is_some_and(|s| valid_hex(s, 32)),
            "invalid slot"
        ),
        "head" => {
            ensure!(raw["head"].is_object(), "invalid head");
            if let Some(home) = raw["home"].as_str() {
                pubkey_from_did(home)?;
            }
            ensure!(
                raw["home"].is_null() || raw["home"].is_string(),
                "invalid home"
            );
        }
        "tombstone" => ensure!(
            raw["delete"].is_object()
                && raw["deleted_head"].is_object()
                && raw["writer"].as_str().is_some_and(|s| s.len() == 43),
            "invalid tombstone"
        ),
        _ => anyhow::bail!("unknown escrow mesh message type"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn request(me: &Identity, now: u64) -> Vec<u8> {
        seal(
            me,
            "test-room",
            "head_req",
            json!({"slot":"a".repeat(32)}),
            now,
        )
        .unwrap()
    }
    #[test]
    fn binding_replay_skew_and_rate() {
        let me = crate::identity::for_test();
        let bytes = request(&me, 1);
        assert!(is_mesh_message(&bytes));
        let mut guard = Guard::default();
        assert!(guard.open(&bytes, "other-room", &me.node_id(), 1).is_err());
        assert!(guard.open(&bytes, "test-room", &"b".repeat(16), 1).is_err());
        assert!(
            guard
                .open(&bytes, "test-room", &me.node_id(), MAX_SKEW_MS + 2)
                .is_err()
        );
        let mut guard = Guard::default();
        assert!(guard.open(&bytes, "test-room", &me.node_id(), 1).is_ok());
        assert!(guard.open(&bytes, "test-room", &me.node_id(), 1).is_err());
        for _ in 2..RATE_MESSAGES {
            assert!(
                guard
                    .open(&request(&me, 1), "test-room", &me.node_id(), 1)
                    .is_ok()
            );
        }
        assert!(
            guard
                .open(&request(&me, 1), "test-room", &me.node_id(), 1)
                .is_err()
        );
        assert!(
            guard
                .open(&request(&me, 10_001), "test-room", &me.node_id(), 10_001)
                .is_ok()
        );
    }
    #[test]
    fn pages_and_envelope_bounds() {
        let e =
            json!({"slot":"a".repeat(32),"seq":1,"hash":"b".repeat(64),"kind":"head","held":true});
        assert!(
            validate_body(
                "index",
                &json!({"entries":[e.clone()],"next":"a".repeat(32)})
            )
            .is_ok()
        );
        assert!(validate_body("index", &json!({"entries":vec![e.clone();129]})).is_err());
        assert!(validate_body("index", &json!({"entries":[e.clone(),e]})).is_err());
        assert!(validate_body("index", &json!({"entries":[],"next":"a".repeat(32)})).is_err());
        assert!(!is_mesh_message(&vec![b' '; MAX_MESSAGE + 1]));
        let me = crate::identity::for_test();
        assert!(seal(&me, "test-room", "join", json!({}), 0).is_err());
        assert!(seal(&me, "test-room", "head_req", json!({"room":"other"}), 0).is_err());
        let mut v: Value = serde_json::from_slice(&request(&me, 0)).unwrap();
        v["slot"] = json!("c".repeat(32));
        assert!(
            Guard::default()
                .open(
                    &serde_json::to_vec(&v).unwrap(),
                    "test-room",
                    &me.node_id(),
                    0
                )
                .is_err()
        );
    }
}
