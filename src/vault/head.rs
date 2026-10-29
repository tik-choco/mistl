//! Owner-signed, hash-chained dataset heads.
use super::keys::OwnerKey;
use crate::identity::Identity;
use crate::wiresign::{sign_wire, stable_stringify, verify_wire};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const HEAD_TAG: &str = "mistl-vault-head-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    pub owner: String,
    pub group: String,
    pub seq: u64,
    pub parent: Option<String>,
    pub pinset: String,
    pub bytes: u64,
    pub files: u64,
    pub at: u64,
    /// The full signed JSON as received/produced.
    pub raw: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accept {
    /// New current head; `fork` = seq is cur+1 but parent is not hash(cur).
    New {
        fork: bool,
    },
    Duplicate,
    /// Same seq, different content: keep current.
    Conflict,
    /// Older than current.
    Stale,
}

impl Head {
    /// Build and sign the next head after `cur` (seq = cur.seq + 1 or 1).
    pub fn sign(
        key: &OwnerKey,
        group: &str,
        cur: Option<&Head>,
        pinset: &str,
        bytes: u64,
        files: u64,
        now_ms: u64,
    ) -> Result<Head> {
        sign_with(&key.identity(), group, cur, pinset, bytes, files, now_ms)
    }

    /// Parse + verify signature, tag, field shapes and `group`.
    pub fn verify(raw: &Value, group: &str) -> Result<Head> {
        ensure!(raw["t"] == HEAD_TAG, "invalid vault head tag");
        ensure!(
            valid_hex(group, 32) && raw["group"] == group,
            "wrong vault group"
        );
        ensure!(
            serde_json::to_vec(raw)?.len() <= super::wire::MAX_MESSAGE,
            "head too large"
        );
        ensure!(verify_wire(raw)?, "invalid head signature");
        let owner = raw["fromId"].as_str().context("missing head signer")?;
        if let Some(claimed) = raw.get("owner") {
            ensure!(claimed.as_str() == Some(owner), "owner differs from signer");
        }
        let seq = raw["seq"].as_u64().context("invalid head seq")?;
        ensure!(seq >= 1, "head seq must be positive");
        let parent = match raw.get("parent") {
            Some(Value::Null) => None,
            Some(Value::String(s)) if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
                Some(s.clone())
            }
            _ => anyhow::bail!("invalid head parent"),
        };
        let pinset = raw["pinset"].as_str().context("invalid pinset")?;
        ensure!(
            !pinset.is_empty() && pinset.chars().count() <= 128,
            "invalid pinset length"
        );
        Ok(Head {
            owner: owner.to_owned(),
            group: group.to_owned(),
            seq,
            parent,
            pinset: pinset.to_owned(),
            bytes: raw["bytes"].as_u64().context("invalid head bytes")?,
            files: raw["files"].as_u64().context("invalid head files")?,
            at: raw["at"].as_u64().context("invalid head timestamp")?,
            raw: raw.clone(),
        })
    }

    /// hex sha256 of the stable JSON (including signature).
    pub fn hash(&self) -> String {
        hex::encode(Sha256::digest(stable_stringify(&self.raw).as_bytes()))
    }

    /// Classify `self` as a candidate replacement for `cur` (same owner).
    pub fn accept_over(&self, cur: Option<&Head>) -> Accept {
        let Some(cur) = cur else {
            return Accept::New { fork: false };
        };
        if self.owner != cur.owner || self.group != cur.group {
            return Accept::Conflict;
        }
        match self.seq.cmp(&cur.seq) {
            std::cmp::Ordering::Less => Accept::Stale,
            std::cmp::Ordering::Equal if self.hash() == cur.hash() => Accept::Duplicate,
            std::cmp::Ordering::Equal => Accept::Conflict,
            std::cmp::Ordering::Greater => Accept::New {
                fork: cur.seq.checked_add(1) == Some(self.seq)
                    && self.parent.as_deref() != Some(cur.hash().as_str()),
            },
        }
    }
}

fn valid_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn sign_with(
    identity: &Identity,
    group: &str,
    cur: Option<&Head>,
    pinset: &str,
    bytes: u64,
    files: u64,
    now_ms: u64,
) -> Result<Head> {
    if let Some(cur) = cur {
        ensure!(
            cur.owner == identity.did() && cur.group == group,
            "head owner/group mismatch"
        );
    }
    let seq = match cur {
        Some(cur) => cur.seq.checked_add(1).context("head sequence exhausted")?,
        None => 1,
    };
    let mut raw = json!({
        "t": HEAD_TAG, "fromId": identity.did(), "group": group, "seq": seq,
        "parent": cur.map(Head::hash), "pinset": pinset, "bytes": bytes,
        "files": files, "at": now_ms,
    });
    sign_wire(&mut raw, identity)?;
    Head::verify(&raw, group)
}

#[cfg(test)]
mod tests {
    use super::*;
    const GROUP: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn chain_acceptance_and_forks() {
        let me = crate::identity::for_test();
        let first = sign_with(&me, GROUP, None, "catalog", 10, 1, 1).unwrap();
        assert_eq!(Head::verify(&first.raw, GROUP).unwrap(), first);
        assert_eq!(first.accept_over(None), Accept::New { fork: false });
        assert_eq!(first.accept_over(Some(&first)), Accept::Duplicate);
        let next = sign_with(&me, GROUP, Some(&first), "next", 11, 2, 2).unwrap();
        assert_eq!(next.parent, Some(first.hash()));
        assert_eq!(next.accept_over(Some(&first)), Accept::New { fork: false });
        assert_eq!(first.accept_over(Some(&next)), Accept::Stale);
        let other = sign_with(&me, GROUP, None, "different", 1, 0, 3).unwrap();
        assert_eq!(other.accept_over(Some(&first)), Accept::Conflict);
        let mut raw = next.raw.clone();
        raw["parent"] = Value::Null;
        sign_wire(&mut raw, &me).unwrap();
        let fork = Head::verify(&raw, GROUP).unwrap();
        assert_eq!(fork.accept_over(Some(&first)), Accept::New { fork: true });
        raw["seq"] = json!(9);
        sign_wire(&mut raw, &me).unwrap();
        assert_eq!(
            Head::verify(&raw, GROUP).unwrap().accept_over(Some(&first)),
            Accept::New { fork: false }
        );
    }

    #[test]
    fn malformed_signed_heads_and_tampering() {
        let me = crate::identity::for_test();
        let head = sign_with(&me, GROUP, None, "p", u64::MAX, u64::MAX, u64::MAX).unwrap();
        for (field, value) in [
            ("t", json!("wrong")),
            ("seq", json!(0)),
            ("seq", json!(-1)),
            ("seq", json!(1.5)),
            ("bytes", json!(-1)),
            ("files", json!("1")),
            ("at", Value::Null),
            ("parent", json!("a".repeat(63))),
            ("parent", json!("g".repeat(64))),
            ("pinset", json!("")),
            ("pinset", json!("p".repeat(129))),
            ("owner", json!("other")),
        ] {
            let mut raw = head.raw.clone();
            raw[field] = value;
            sign_wire(&mut raw, &me).unwrap();
            assert!(Head::verify(&raw, GROUP).is_err(), "{field}");
        }
        let mut raw = head.raw.clone();
        raw["bytes"] = json!(0);
        assert!(Head::verify(&raw, GROUP).is_err());
        assert!(Head::verify(&head.raw, "f".repeat(32).as_str()).is_err());
        let mut raw = head.raw.clone();
        raw.as_object_mut().unwrap().remove("parent");
        sign_wire(&mut raw, &me).unwrap();
        assert!(Head::verify(&raw, GROUP).is_err());
        assert!(sign_with(&me, GROUP, Some(&head), "p", 0, 0, 0).is_ok());
        raw = head.raw.clone();
        raw["seq"] = json!(u64::MAX);
        sign_wire(&mut raw, &me).unwrap();
        let last = Head::verify(&raw, GROUP).unwrap();
        assert!(sign_with(&me, GROUP, Some(&last), "p", 0, 0, 0).is_err());
        let stranger = crate::identity::for_test();
        assert!(sign_with(&stranger, GROUP, Some(&head), "p", 0, 0, 0).is_err());
    }
}
