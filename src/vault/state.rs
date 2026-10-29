//! Bounded replication bookkeeping and network-independent decisions.
use super::head::{Accept, Head};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

pub(super) const MAX_OWNERS: usize = 64;
pub(super) const GC_GRACE: u64 = 24 * 60 * 60 * 1000;
pub(super) const ONLINE_MS: u64 = 90_000;
pub(super) const MAX_REFERENCES: usize = 262_144;

#[derive(Clone)]
pub(super) struct Owner {
    pub cur: Head,
    pub prev: Option<Head>,
    pub accepted_at: u64,
    pub fork: bool,
    pub home: String,
    pub complete: bool,
    pub held_bytes: u64,
    pub missing: usize,
    pub last_error: Option<String>,
    pub holding: bool,
    pub release_at: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct SavedOwner {
    cur: Value,
    prev: Option<Value>,
    accepted_at: u64,
    fork: bool,
    home: String,
    complete: bool,
    missing: usize,
    last_error: Option<String>,
    #[serde(default = "yes")]
    holding: bool,
    #[serde(default)]
    release_at: Option<u64>,
}

fn yes() -> bool {
    true
}

impl Owner {
    pub fn saved(&self) -> SavedOwner {
        SavedOwner {
            cur: self.cur.raw.clone(),
            prev: self.prev.as_ref().map(|h| h.raw.clone()),
            accepted_at: self.accepted_at,
            fork: self.fork,
            home: self.home.clone(),
            complete: self.complete,
            missing: self.missing,
            last_error: self.last_error.clone(),
            holding: self.holding,
            release_at: self.release_at,
        }
    }
    pub fn load(saved: SavedOwner, group: &str) -> Result<Self> {
        let cur = Head::verify(&saved.cur, group)?;
        let prev = saved
            .prev
            .as_ref()
            .map(|v| Head::verify(v, group))
            .transpose()?;
        ensure!(
            prev.as_ref()
                .is_none_or(|p| p.owner == cur.owner && p.seq < cur.seq),
            "invalid previous head"
        );
        crate::identity::pubkey_from_did(&saved.home)?;
        Ok(Self {
            cur,
            prev,
            accepted_at: saved.accepted_at,
            fork: saved.fork,
            home: saved.home,
            complete: false,
            held_bytes: 0,
            missing: saved.missing.min(MAX_REFERENCES),
            last_error: saved.last_error,
            holding: saved.holding,
            release_at: saved.release_at,
        })
    }
}

pub(super) fn accept_head(
    owners: &mut BTreeMap<String, Owner>,
    head: Head,
    home: &str,
    now: u64,
) -> Result<Accept> {
    let decision = head.accept_over(owners.get(&head.owner).map(|o| &o.cur));
    match decision {
        Accept::New { fork } => {
            ensure!(
                owners.contains_key(&head.owner) || owners.len() < MAX_OWNERS,
                "vault owner limit reached"
            );
            let old = owners.remove(&head.owner);
            let record = Owner {
                cur: head,
                prev: old.as_ref().map(|o| o.cur.clone()),
                accepted_at: now,
                fork: fork || old.as_ref().is_some_and(|o| o.fork),
                home: home.into(),
                complete: false,
                held_bytes: 0,
                missing: 1,
                last_error: None,
                // Placement decides on the next tick; only an existing holder
                // keeps serving as a repair source meanwhile.
                holding: old.as_ref().is_some_and(|o| o.holding),
                release_at: None,
            };
            owners.insert(record.cur.owner.clone(), record);
        }
        Accept::Conflict => {
            if let Some(o) = owners.get_mut(&head.owner) {
                o.fork = true;
            }
        }
        Accept::Duplicate | Accept::Stale => {}
    }
    Ok(decision)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Summary {
    pub owner: String,
    pub seq: u64,
    pub hash: String,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub bytes: u64,
}

#[derive(Clone, Debug)]
pub(super) struct Peer {
    pub first_seen: u64,
    pub last_seen: u64,
    pub free: u64,
    pub capacity: u64,
    pub heads: Vec<Summary>,
    pub holds: Vec<Summary>,
}

pub(super) fn summaries(value: &Value) -> Result<Vec<Summary>> {
    let array = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected summaries"))?;
    ensure!(array.len() <= MAX_OWNERS, "too many summaries");
    let mut seen = HashSet::new();
    let entries: Vec<Summary> = serde_json::from_value(value.clone())?;
    for entry in &entries {
        crate::identity::pubkey_from_did(&entry.owner)?;
        ensure!(
            entry.seq > 0
                && entry.hash.len() == 64
                && entry.hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid summary"
        );
        ensure!(seen.insert(&entry.owner), "duplicate owner summary");
    }
    Ok(entries)
}

pub(super) fn confirms(summary: &Summary, head: &Head) -> bool {
    summary.complete
        && summary.owner == head.owner
        && summary.seq == head.seq
        && summary.hash == head.hash()
}

/// Hold when owned or placed here. A node that is no longer placed keeps an
/// existing copy until every replacement is complete, then for a grace
/// period; a node that never held does not start holding.
pub(super) fn holding_decision(
    owned: bool,
    desired: bool,
    replacements_complete: bool,
    was_holding: bool,
    release_at: Option<u64>,
    now: u64,
) -> (bool, Option<u64>) {
    if owned || desired {
        return (true, None);
    }
    if !was_holding {
        return (false, release_at);
    }
    if !replacements_complete {
        return (true, None);
    }
    let release = release_at.unwrap_or(now.saturating_add(GC_GRACE));
    (now < release, Some(release))
}

pub(super) fn classification(
    fork: bool,
    insufficient: bool,
    complete: bool,
    confirmed: usize,
    target: usize,
    members: usize,
) -> &'static str {
    if fork {
        "conflict"
    } else if insufficient {
        "insufficient_capacity"
    } else if members <= 1 && complete {
        "local_only"
    } else if confirmed >= target && target > 0 {
        "healthy"
    } else if !complete {
        "replicating"
    } else {
        "degraded"
    }
}

pub(super) fn retained_heads(owner: &Owner, owned: bool, now: u64) -> Vec<&Head> {
    if !owner.holding && !owned {
        return Vec::new();
    }
    let mut heads = vec![&owner.cur];
    if now.saturating_sub(owner.accepted_at) < GC_GRACE {
        if let Some(previous) = &owner.prev {
            heads.push(previous);
        }
    }
    heads
}

#[derive(Default)]
pub(super) struct Graph {
    pub keep: HashSet<String>,
    /// Lengths of chunks already found, so repeated references are not re-read.
    pub sizes: std::collections::HashMap<String, u64>,
    pub missing: Vec<String>,
    pub bytes: u64,
    pub expanded: bool,
}

impl Graph {
    pub fn complete(&self, expected: u64) -> bool {
        self.expanded && self.missing.is_empty() && self.bytes == expected
    }
}

pub(super) fn gc_keep(graphs: &[Graph], inflight: &HashSet<String>) -> Option<HashSet<String>> {
    if graphs.iter().any(|g| !g.expanded) {
        return None;
    }
    let mut keep = inflight.clone();
    for graph in graphs {
        keep.extend(graph.keep.iter().cloned());
    }
    Some(keep)
}

#[cfg(test)]
mod tests {
    use super::super::keys::OwnerKey;
    use super::*;
    fn head(key: &OwnerKey, prev: Option<&Head>, now: u64) -> Head {
        Head::sign(
            key,
            "0123456789abcdef0123456789abcdef",
            prev,
            &super::super::object::raw_cid(b"pin"),
            3,
            0,
            now,
        )
        .unwrap()
    }
    #[test]
    fn head_bookkeeping_keeps_previous_and_flags_conflicts() {
        let key = OwnerKey::generate();
        let mut owners = BTreeMap::new();
        let first = head(&key, None, 1);
        accept_head(&mut owners, first.clone(), &key.owner_did(), 10).unwrap();
        let conflicting = head(&key, None, 2);
        assert_eq!(
            accept_head(&mut owners, conflicting, &key.owner_did(), 20).unwrap(),
            Accept::Conflict
        );
        assert_eq!(owners[&key.owner_did()].cur, first);
        let second = head(&key, Some(&first), 3);
        accept_head(&mut owners, second.clone(), &key.owner_did(), 30).unwrap();
        let owner = &owners[&key.owner_did()];
        assert_eq!(owner.prev.as_ref(), Some(&first));
        assert!(owner.fork);
        assert!(!owner.complete);
        assert_eq!(
            accept_head(&mut owners, first, &key.owner_did(), 40).unwrap(),
            Accept::Stale
        );
        assert_eq!(
            accept_head(&mut owners, second, &key.owner_did(), 40).unwrap(),
            Accept::Duplicate
        );
    }
    #[test]
    fn holding_requires_confirmations_and_grace() {
        // A node outside placement never starts holding, even before the
        // placed holders are complete.
        assert_eq!(
            holding_decision(false, false, false, false, None, 1),
            (false, None)
        );
        assert_eq!(
            holding_decision(false, false, false, true, None, 1),
            (true, None)
        );
        assert_eq!(
            holding_decision(false, false, true, true, None, 1),
            (true, Some(1 + GC_GRACE))
        );
        assert_eq!(
            holding_decision(false, false, true, true, Some(1), 1),
            (false, Some(1))
        );
        assert_eq!(
            holding_decision(true, false, true, false, Some(1), 2),
            (true, None)
        );
    }

    #[test]
    fn previous_head_retention_expires_at_grace_boundary() {
        let key = OwnerKey::generate();
        let first = head(&key, None, 1);
        let second = head(&key, Some(&first), 2);
        let mut owners = BTreeMap::new();
        accept_head(&mut owners, first, &key.owner_did(), 1).unwrap();
        accept_head(&mut owners, second, &key.owner_did(), 2).unwrap();
        let owner = owners.get_mut(&key.owner_did()).unwrap();
        owner.holding = true;
        assert_eq!(retained_heads(owner, false, GC_GRACE + 1).len(), 2);
        assert_eq!(retained_heads(owner, false, GC_GRACE + 2).len(), 1);
        owner.holding = false;
        assert!(retained_heads(owner, false, 3).is_empty());
        assert_eq!(retained_heads(owner, true, 3).len(), 2);
    }
    #[test]
    fn status_precedence_and_counts() {
        assert_eq!(classification(true, true, false, 0, 3, 3), "conflict");
        assert_eq!(
            classification(false, true, false, 0, 3, 3),
            "insufficient_capacity"
        );
        assert_eq!(classification(false, false, true, 1, 1, 1), "local_only");
        assert_eq!(classification(false, false, true, 3, 3, 4), "healthy");
        assert_eq!(classification(false, false, false, 0, 3, 4), "replicating");
        assert_eq!(classification(false, false, true, 1, 3, 4), "degraded");
    }
    #[test]
    fn completeness_and_gc_fail_closed() {
        let mut graph = Graph::default();
        assert!(!graph.complete(0));
        assert!(gc_keep(&[graph], &HashSet::new()).is_none());
        graph = Graph {
            expanded: true,
            bytes: 5,
            ..Graph::default()
        };
        graph.keep.insert("block".into());
        assert!(graph.complete(5));
        assert!(!graph.complete(4));
        graph.missing.push("missing".into());
        assert!(!graph.complete(5));
        let keep = gc_keep(&[graph], &HashSet::from(["fetch".into()])).unwrap();
        assert!(keep.contains("block") && keep.contains("fetch"));
    }
}
