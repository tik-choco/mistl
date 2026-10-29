//! Pure convergence, retention and status decisions.
use super::placement::Candidate;
use crate::escrow::protocol::{Error, Head};
use serde::{Deserialize, Serialize};

pub struct ProviderInput<'a> {
    pub did: &'a str,
    pub free: u64,
    pub last_seen: u64,
    pub held: bool,
}

pub fn placement_inputs(
    providers: &[ProviderInput<'_>],
    now: u64,
    dead_after: u64,
    size: u64,
    replicas: usize,
    home: Option<&str>,
) -> (usize, Vec<Candidate>, Option<String>) {
    let candidates: Vec<_> = providers
        .iter()
        .filter(|p| now.saturating_sub(p.last_seen) <= dead_after)
        .map(|p| Candidate {
            did: p.did.into(),
            available: p.free.saturating_add(if p.held { size } else { 0 }),
        })
        .collect();
    let target = replicas.min(candidates.len());
    let home = home
        .filter(|home| candidates.iter().any(|p| p.did == *home))
        .map(str::to_owned);
    (target, candidates, home)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Acceptance {
    New,
    Same,
    Stale,
    Conflict,
    WriterMismatch,
}

pub fn acceptance(current: Option<&Head>, incoming: &Head) -> Acceptance {
    let Some(current) = current else {
        return Acceptance::New;
    };
    if current.writer != incoming.writer {
        Acceptance::WriterMismatch
    } else if current == incoming {
        Acceptance::Same
    } else if incoming.seq < current.seq {
        Acceptance::Stale
    } else if incoming.seq == current.seq {
        Acceptance::Conflict
    } else {
        Acceptance::New
    }
}

pub fn successor(head: &Head, deleted: &Head) -> Result<(), Error> {
    if head.writer != deleted.writer {
        return Err(Error::new("writer_mismatch"));
    }
    if deleted.seq.checked_add(1) != Some(head.seq)
        || head.parent.as_deref() != Some(deleted.hash().as_str())
    {
        let mut error = Error::conflict(deleted);
        error.code = "deleted";
        return Err(error);
    }
    Ok(())
}

pub const GRACE_MS: u64 = 24 * 60 * 60 * 1000;
pub fn informed(since: u64, now: u64, known: usize, hello: usize) -> bool {
    now.saturating_sub(since) >= 60_000 || (known > 0 && known == hello)
}

/// A never-holder cannot acquire an unplaced slot. Grace restarts if confirmation is lost.
pub fn holding(
    placed: bool,
    held: bool,
    informed: bool,
    all_confirmed: bool,
    release: Option<u64>,
    now: u64,
) -> (bool, Option<u64>) {
    if placed {
        return (held || informed, None);
    }
    if !held {
        return (false, None);
    }
    if !all_confirmed {
        return (true, None);
    }
    let release = release.unwrap_or(now.saturating_add(GRACE_MS));
    (now < release, Some(release))
}

pub fn classify(conflict: bool, confirmed: usize, target: usize, fetching: bool) -> &'static str {
    if conflict {
        "conflict"
    } else if confirmed >= target {
        "healthy"
    } else if fetching {
        "replicating"
    } else {
        "degraded"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub slot: String,
    pub seq: u64,
    pub hash: String,
    pub kind: String,
    pub held: bool,
}

pub fn needs_head(local: Option<&Entry>, remote: &Entry) -> bool {
    local.is_none_or(|local| {
        remote.seq > local.seq
            || (remote.seq == local.seq && (remote.hash != local.hash || remote.kind != local.kind))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn grace_and_partial_views() {
        assert!(!informed(0, 59_999, 2, 1));
        assert!(informed(0, 60_000, 2, 1));
        assert!(informed(0, 1, 2, 2));
        assert_eq!(holding(false, false, true, true, None, 0), (false, None));
        assert_eq!(holding(true, false, false, true, None, 0), (false, None));
        assert_eq!(
            holding(false, true, true, true, None, 1),
            (true, Some(GRACE_MS + 1))
        );
        assert_eq!(holding(false, true, true, false, Some(2), 3), (true, None));
        assert_eq!(
            holding(false, true, true, true, Some(2), 3),
            (false, Some(2))
        );
        assert_eq!(holding(true, true, false, true, Some(2), 3), (true, None));
    }
    #[test]
    fn index_and_status() {
        let a = Entry {
            slot: "a".repeat(32),
            seq: 2,
            hash: "b".repeat(64),
            kind: "head".into(),
            held: true,
        };
        let mut b = a.clone();
        b.held = false;
        assert!(!needs_head(Some(&a), &b));
        b.kind = "tombstone".into();
        assert!(needs_head(Some(&a), &b));
        b.seq = 1;
        assert!(!needs_head(Some(&a), &b));
        assert!(needs_head(None, &b));
        assert_eq!(classify(true, 3, 3, false), "conflict");
        assert_eq!(classify(false, 3, 3, false), "healthy");
        assert_eq!(classify(false, 2, 3, true), "replicating");
        assert_eq!(classify(false, 2, 3, false), "degraded");
    }
    #[test]
    fn placement_excludes_dead_home_and_credits_existing_copy() {
        let providers = [
            ProviderInput {
                did: "a",
                free: 0,
                last_seen: 100,
                held: true,
            },
            ProviderInput {
                did: "b",
                free: 100,
                last_seen: 1,
                held: false,
            },
        ];
        let (target, candidates, home) = placement_inputs(&providers, 100, 60, 25, 3, Some("b"));
        assert_eq!(target, 1);
        assert!(home.is_none());
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].available, 25);
        let (target, _, home) = placement_inputs(&providers, 61, 60, 25, 3, Some("b"));
        assert_eq!(target, 2);
        assert_eq!(home.as_deref(), Some("b"));
    }
}
