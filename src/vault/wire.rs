//! Signed control envelopes on the vault room (`t` = "mistl-vault-v1").
use crate::identity::{Identity, node_id_for_did, pubkey_from_did};
use crate::wiresign::{sign_wire, verify_wire};
use anyhow::{Context, Result, ensure};
use rand::RngCore;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};

pub const TAG: &str = "mistl-vault-v1";
pub const MAX_MESSAGE: usize = 16 * 1024;
pub const MAX_SKEW_MS: u64 = 120_000;

/// Build and sign an envelope: adds t/type/fromId/group/ts/nonce to `body`
/// (a JSON object). Fails if the result exceeds MAX_MESSAGE.
pub fn seal(me: &Identity, group: &str, kind: &str, body: Value, now_ms: u64) -> Result<Vec<u8>> {
    ensure!(valid_hex(group, 32), "invalid vault group");
    ensure!(known_kind(kind), "unknown vault message type");
    let mut map = body
        .as_object()
        .context("vault body must be an object")?
        .clone();
    for key in ["t", "type", "fromId", "group", "ts", "nonce", "signature"] {
        ensure!(!map.contains_key(key), "reserved envelope field: {key}");
    }
    let mut nonce = [0u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    map.insert("t".into(), TAG.into());
    map.insert("type".into(), kind.into());
    map.insert("fromId".into(), me.did().into());
    map.insert("group".into(), group.into());
    map.insert("ts".into(), now_ms.into());
    map.insert("nonce".into(), hex::encode(nonce).into());
    let mut raw = Value::Object(map);
    ensure!(
        serde_json::to_vec(&raw)?.len() <= MAX_MESSAGE,
        "vault message too large"
    );
    sign_wire(&mut raw, me)?;
    let bytes = serde_json::to_vec(&raw)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "vault message too large");
    Ok(bytes)
}

/// Cheap shape test used by the room handler before any work.
pub fn is_vault_message(bytes: &[u8]) -> bool {
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
    /// Checks size, tag, signature, group, skew, nonce replay, that
    /// node_id_for_did(fromId) == `transport_from`, and a per-peer rate cap.
    /// Membership authorization is the caller's job.
    pub fn open(
        &mut self,
        bytes: &[u8],
        group: &str,
        transport_from: &str,
        now_ms: u64,
    ) -> Result<Inbound> {
        ensure!(bytes.len() <= MAX_MESSAGE, "vault message too large");
        let raw: Value = serde_json::from_slice(bytes)?;
        ensure!(raw["t"] == TAG, "invalid vault tag");
        self.record_message(transport_from, now_ms)?;
        ensure!(verify_wire(&raw)?, "invalid vault signature");
        ensure!(
            valid_hex(group, 32) && raw["group"] == group,
            "wrong vault group"
        );
        let ts = raw["ts"].as_u64().context("invalid vault timestamp")?;
        ensure!(
            now_ms.abs_diff(ts) <= MAX_SKEW_MS,
            "vault message outside time window"
        );
        let nonce = raw["nonce"].as_str().context("missing vault nonce")?;
        ensure!(valid_hex(nonce, 16), "invalid vault nonce");
        let did = raw["fromId"].as_str().context("missing vault signer")?;
        pubkey_from_did(did)?;
        let replay_key = (did.to_owned(), nonce.to_owned());
        ensure!(!self.nonces.contains(&replay_key), "replayed vault message");
        ensure!(
            node_id_for_did(did) == transport_from,
            "vault sender differs from transport"
        );
        let kind = raw["type"].as_str().context("missing vault type")?;
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
            "vault peer rate table full"
        );
        let rate = self
            .peers
            .entry(transport_from.to_owned())
            .or_insert(PeerRate {
                start: now_ms,
                count: 0,
            });
        ensure!(rate.count < RATE_MESSAGES, "vault peer rate exceeded");
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
    matches!(kind, "hello" | "head" | "head_req" | "members" | "join")
}

fn validate_body(kind: &str, raw: &Value) -> Result<()> {
    match kind {
        "hello" => {
            for key in ["free", "capacity", "since"] {
                ensure!(raw[key].as_u64().is_some(), "invalid hello {key}");
            }
            ensure!(
                raw["members_digest"]
                    .as_str()
                    .is_some_and(|s| valid_hex(s, 64)),
                "invalid membership digest"
            );
            for key in ["heads", "holds"] {
                let items = raw[key].as_array().context("invalid hello summary")?;
                ensure!(items.len() <= 64, "too many hello summaries");
                for item in items {
                    pubkey_from_did(item["owner"].as_str().context("invalid summary owner")?)?;
                    ensure!(
                        item["seq"].as_u64().is_some_and(|n| n >= 1),
                        "invalid summary seq"
                    );
                    ensure!(
                        item["hash"].as_str().is_some_and(|s| valid_hex(s, 64)),
                        "invalid summary hash"
                    );
                    if key == "holds" {
                        ensure!(
                            item["complete"].is_boolean() && item["bytes"].as_u64().is_some(),
                            "invalid holding summary"
                        );
                    }
                }
            }
        }
        "members" => {
            ensure!(
                raw["entries"].as_array().is_some_and(|a| a.len() <= 64),
                "too many or invalid membership entries"
            );
        }
        "head" => ensure!(raw["head"].is_object(), "invalid head body"),
        "head_req" => {
            pubkey_from_did(raw["owner"].as_str().context("invalid requested owner")?)?;
        }
        "join" => {
            ensure!(
                raw["invite"].as_str().is_some_and(|s| valid_hex(s, 16)),
                "invalid invite id"
            );
            ensure!(
                raw["mac"].as_str().is_some_and(|s| valid_hex(s, 64)),
                "invalid join MAC"
            );
        }
        _ => anyhow::bail!("unknown vault message type"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    const GROUP: &str = "0123456789abcdef0123456789abcdef";

    fn message(me: &Identity, now: u64) -> Vec<u8> {
        seal(me, GROUP, "head_req", json!({"owner": me.did()}), now).unwrap()
    }

    #[test]
    fn roundtrip_tampering_and_sender_binding() {
        let me = crate::identity::for_test();
        let bytes = message(&me, 1234);
        let mut guard = Guard::default();
        assert!(is_vault_message(&bytes));
        assert!(!is_vault_message(b"[]"));
        assert!(!is_vault_message(b"not json"));
        assert!(guard.open(&bytes, GROUP, "wrong", 1234).is_err());
        let inbound = guard.open(&bytes, GROUP, &me.node_id(), 1234).unwrap();
        assert_eq!(inbound.kind, "head_req");
        assert_eq!(inbound.from_did, me.did());
        assert!(guard.open(&bytes, GROUP, &me.node_id(), 1234).is_err());
        let mut raw: Value = serde_json::from_slice(&message(&me, 1234)).unwrap();
        raw["ts"] = json!(1235);
        assert!(
            guard
                .open(
                    &serde_json::to_vec(&raw).unwrap(),
                    GROUP,
                    &me.node_id(),
                    1234
                )
                .is_err()
        );
        assert!(
            Guard::default()
                .open(&bytes, &"f".repeat(32), &me.node_id(), 1234)
                .is_err()
        );
    }

    #[test]
    fn time_size_and_field_bounds() {
        let me = crate::identity::for_test();
        let bytes = message(&me, MAX_SKEW_MS);
        assert!(
            Guard::default()
                .open(&bytes, GROUP, &me.node_id(), 0)
                .is_ok()
        );
        let bytes = message(&me, MAX_SKEW_MS + 1);
        assert!(
            Guard::default()
                .open(&bytes, GROUP, &me.node_id(), 0)
                .is_err()
        );
        assert!(
            Guard::default()
                .open(&message(&me, 0), GROUP, &me.node_id(), MAX_SKEW_MS + 1)
                .is_err()
        );
        assert!(
            seal(
                &me,
                GROUP,
                "hello",
                json!({"padding": "x".repeat(MAX_MESSAGE)}),
                0
            )
            .is_err()
        );
        assert!(seal(&me, GROUP, "hello", json!([]), 0).is_err());
        assert!(seal(&me, GROUP, "hello", json!({"fromId": "spoof"}), 0).is_err());
        assert!(!is_vault_message(&vec![b' '; MAX_MESSAGE + 1]));
        for (field, value) in [
            ("nonce", json!("g".repeat(16))),
            ("ts", json!(-1)),
            ("type", json!("unknown")),
            ("fromId", json!("did:key:bad")),
        ] {
            let mut raw: Value = serde_json::from_slice(&message(&me, 1)).unwrap();
            raw[field] = value;
            if field != "fromId" {
                sign_wire(&mut raw, &me).unwrap();
            }
            assert!(
                Guard::default()
                    .open(&serde_json::to_vec(&raw).unwrap(), GROUP, &me.node_id(), 1)
                    .is_err()
            );
        }
        let bytes = seal(
            &me,
            GROUP,
            "members",
            json!({"entries": vec![Value::Null; 65]}),
            1,
        )
        .unwrap();
        assert!(
            Guard::default()
                .open(&bytes, GROUP, &me.node_id(), 1)
                .is_err()
        );
    }

    #[test]
    fn rate_limit_and_bounded_peer_table() {
        let me = crate::identity::for_test();
        let mut guard = Guard::default();
        for _ in 0..50 {
            assert!(
                guard
                    .open(&message(&me, 1), GROUP, &me.node_id(), 1)
                    .is_ok()
            );
        }
        assert!(
            guard
                .open(&message(&me, 1), GROUP, &me.node_id(), 1)
                .is_err()
        );
        assert!(
            guard
                .open(&message(&me, 10_001), GROUP, &me.node_id(), 10_001)
                .is_ok()
        );
        guard.peers.clear();
        for n in 0..MAX_PEERS {
            guard
                .peers
                .insert(format!("peer{n}"), PeerRate { start: 1, count: 1 });
        }
        assert!(
            guard
                .open(&message(&me, 1), GROUP, &me.node_id(), 1)
                .is_err()
        );
        assert_eq!(guard.peers.len(), MAX_PEERS);
        assert!(
            guard
                .open(&message(&me, 10_001), GROUP, &me.node_id(), 10_001)
                .is_ok()
        );
        assert_eq!(guard.peers.len(), 1);
    }

    #[test]
    fn rejected_messages_also_consume_rate_budget() {
        let me = crate::identity::for_test();
        let mut guard = Guard::default();
        let bytes = message(&me, 0);
        guard.open(&bytes, GROUP, &me.node_id(), 0).unwrap();
        for _ in 1..RATE_MESSAGES {
            assert!(guard.open(&bytes, GROUP, &me.node_id(), 0).is_err());
        }
        assert_eq!(guard.peers[&me.node_id()].count, RATE_MESSAGES);
        let err = guard
            .open(&message(&me, 0), GROUP, &me.node_id(), 0)
            .unwrap_err();
        assert!(err.to_string().contains("rate exceeded"));
        assert!(
            guard
                .open(
                    &message(&me, RATE_WINDOW_MS),
                    GROUP,
                    &me.node_id(),
                    RATE_WINDOW_MS
                )
                .is_ok()
        );
        let mut guard = Guard::default();
        let mut raw: Value = serde_json::from_slice(&bytes).unwrap();
        raw["ts"] = json!(1);
        let invalid = serde_json::to_vec(&raw).unwrap();
        for _ in 0..RATE_MESSAGES {
            assert!(guard.open(&invalid, GROUP, &me.node_id(), 0).is_err());
        }
        assert!(
            guard
                .open(&bytes, GROUP, &me.node_id(), 0)
                .unwrap_err()
                .to_string()
                .contains("rate exceeded")
        );
    }

    #[test]
    fn hello_and_join_body_bounds() {
        let me = crate::identity::for_test();
        let summary = json!({"owner": me.did(), "seq": 1, "hash": "a".repeat(64)});
        let hello = json!({"free": u64::MAX, "capacity": u64::MAX, "since": 0,
            "members_digest": "b".repeat(64), "heads": [summary.clone()], "holds": []});
        let mut guard = Guard::default();
        let bytes = seal(&me, GROUP, "hello", hello.clone(), 0).unwrap();
        assert!(guard.open(&bytes, GROUP, &me.node_id(), 0).is_ok());
        let mut too_many = hello.clone();
        too_many["heads"] = json!(vec![summary; 65]);
        let bytes = seal(&me, GROUP, "hello", too_many, 0).unwrap();
        assert!(guard.open(&bytes, GROUP, &me.node_id(), 0).is_err());
        let mut bad = hello;
        bad["holds"] = json!([{"owner": me.did(), "seq": 1, "hash": "c".repeat(64), "bytes": 0, "complete": "yes"}]);
        let bytes = seal(&me, GROUP, "hello", bad, 0).unwrap();
        assert!(guard.open(&bytes, GROUP, &me.node_id(), 0).is_err());
        let bytes = seal(
            &me,
            GROUP,
            "join",
            json!({"invite": "a".repeat(16), "mac": "b".repeat(64)}),
            0,
        )
        .unwrap();
        assert!(guard.open(&bytes, GROUP, &me.node_id(), 0).is_ok());
        let bytes = seal(
            &me,
            GROUP,
            "join",
            json!({"invite": "a".repeat(15), "mac": "b".repeat(64)}),
            0,
        )
        .unwrap();
        assert!(guard.open(&bytes, GROUP, &me.node_id(), 0).is_err());
    }

    #[test]
    fn replay_cache_evicts_oldest_and_is_scoped_to_signer() {
        let me = crate::identity::for_test();
        let mut guard = Guard::default();
        let first = message(&me, 0);
        guard.open(&first, GROUP, &me.node_id(), 0).unwrap();
        for n in 1..MAX_NONCES {
            let key = (me.did().to_owned(), format!("{n:016x}"));
            guard.nonces.insert(key.clone());
            guard.nonce_order.push_back(key);
        }
        guard
            .open(&message(&me, 0), GROUP, &me.node_id(), 0)
            .unwrap();
        assert_eq!(guard.nonces.len(), MAX_NONCES);
        assert_eq!(guard.nonce_order.len(), MAX_NONCES);
        let raw: Value = serde_json::from_slice(&first).unwrap();
        assert!(!guard.nonces.contains(&(
            me.did().to_owned(),
            raw["nonce"].as_str().unwrap().to_owned()
        )));
        let other = crate::identity::for_test();
        let mut raw: Value = serde_json::from_slice(&message(&other, 0)).unwrap();
        let first_raw: Value = serde_json::from_slice(&first).unwrap();
        raw["nonce"] = first_raw["nonce"].clone();
        sign_wire(&mut raw, &other).unwrap();
        let mut fresh = Guard::default();
        fresh.open(&first, GROUP, &me.node_id(), 0).unwrap();
        assert!(
            fresh
                .open(
                    &serde_json::to_vec(&raw).unwrap(),
                    GROUP,
                    &other.node_id(),
                    0
                )
                .is_ok()
        );
    }
}
