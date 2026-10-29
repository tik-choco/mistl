//! Weighted rendezvous (HRW) placement of an owner's dataset onto members.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub struct Candidate {
    pub did: String,
    /// Advertised free bytes plus bytes already held for this owner.
    pub available: u64,
}

fn negative_log_uniform(value: u64) -> f64 {
    const TWO_TO_64: f64 = 18_446_744_073_709_551_616.0;
    if value < (1u64 << 63) {
        -((value as f64 + 0.5) / TWO_TO_64).ln()
    } else {
        // Calculate ln(1 - x) directly so u close to 1 does not round to 1.
        let complement = ((u64::MAX - value) as f64 + 0.5) / TWO_TO_64;
        -(-complement).ln_1p()
    }
}

/// Pick up to `replicas` holders for `owner` among candidates whose
/// `available >= bytes`. `home`, when Some, is always included first (even if
/// absent from `candidates`) and counts toward `replicas`. Deterministic for
/// equal inputs; order = home, then best score.
pub fn place(
    owner: &str,
    bytes: u64,
    replicas: usize,
    home: Option<&str>,
    candidates: &[Candidate],
) -> Vec<String> {
    if replicas == 0 {
        return Vec::new();
    }

    // A repeated DID represents one member. Use the largest advertised
    // capacity so input order cannot change its eligibility or score.
    let mut members = BTreeMap::<&str, u64>::new();
    for candidate in candidates {
        if candidate.available < bytes || home == Some(candidate.did.as_str()) {
            continue;
        }
        members
            .entry(candidate.did.as_str())
            .and_modify(|available| *available = (*available).max(candidate.available))
            .or_insert(candidate.available);
    }

    let mut ranked: Vec<_> = members
        .into_iter()
        .map(|(did, available)| {
            let mut hash = Sha256::new();
            hash.update(owner.as_bytes());
            hash.update(b"|");
            hash.update(did.as_bytes());
            let digest = hash.finalize();
            let value = u64::from_be_bytes([
                digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6],
                digest[7],
            ]);
            let weight = (available as f64 / 1_073_741_824.0).clamp(1.0, 64.0);
            (did, negative_log_uniform(value) / weight)
        })
        .collect();
    ranked.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(b.0)));

    let mut result = Vec::new();
    if let Some(home) = home {
        result.push(home.to_owned());
    }
    let remaining = replicas.saturating_sub(result.len());
    result.extend(
        ranked
            .into_iter()
            .take(remaining)
            .map(|(did, _)| did.to_owned()),
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn candidate(did: &str, available: u64) -> Candidate {
        Candidate {
            did: did.to_owned(),
            available,
        }
    }

    #[test]
    fn deterministic_and_independent_of_candidate_order_and_duplicates() {
        let candidates = vec![
            candidate("a", GIB),
            candidate("b", 3 * GIB),
            candidate("c", 2 * GIB),
            candidate("b", GIB),
        ];
        let reversed: Vec<_> = candidates
            .iter()
            .rev()
            .map(|c| candidate(&c.did, c.available))
            .collect();
        let first = place("owner", 1, 5, None, &candidates);
        assert_eq!(first, ["c", "b", "a"]);
        assert_eq!(first, place("owner", 1, 5, None, &candidates));
        assert_eq!(first, place("owner", 1, 5, None, &reversed));
        assert_eq!(first.len(), 3);
    }

    #[test]
    fn home_is_first_even_if_absent_or_below_capacity() {
        let candidates = vec![candidate("home", 0), candidate("a", 10), candidate("b", 1)];
        assert_eq!(place("owner", 5, 1, Some("home"), &candidates), ["home"]);
        assert_eq!(
            place("owner", 5, 100, Some("elsewhere"), &candidates),
            ["elsewhere", "a"]
        );
        assert_eq!(
            place("owner", 5, 100, Some("home"), &candidates),
            ["home", "a"]
        );
        assert!(place("owner", 5, 0, Some("home"), &candidates).is_empty());
        assert_eq!(place("owner", 5, 2, Some("home"), &[]), ["home"]);
    }

    #[test]
    fn excludes_insufficient_capacity_and_limits_replicas() {
        let candidates = vec![
            candidate("below", 99),
            candidate("exact", 100),
            candidate("above", 101),
            candidate("exact", 100),
        ];
        let holders = place("owner", 100, usize::MAX, None, &candidates);
        assert_eq!(holders.len(), 2);
        assert!(holders.contains(&"exact".to_owned()));
        assert!(holders.contains(&"above".to_owned()));
        assert_eq!(place("owner", 100, 1, None, &candidates).len(), 1);
        assert!(place("owner", 102, 3, None, &candidates).is_empty());
    }

    #[test]
    fn extreme_hash_values_keep_positive_distinct_scores() {
        let low = negative_log_uniform(0);
        let high = negative_log_uniform(u64::MAX);
        assert!(low.is_finite() && low > 0.0);
        assert!(high.is_finite() && high > 0.0);
        assert!(negative_log_uniform(u64::MAX - 1) > high);
    }

    #[test]
    fn adding_a_member_moves_only_the_needed_holders() {
        let mut candidates: Vec<_> = (0..6)
            .map(|i| candidate(&format!("member-{i}"), GIB))
            .collect();
        let before: Vec<_> = (0..2000)
            .map(|i| place(&format!("owner-{i}"), 0, 2, None, &candidates))
            .collect();
        candidates.push(candidate("new-member", GIB));
        let mut unchanged = 0;
        for (i, original) in before.iter().enumerate() {
            let after = place(&format!("owner-{i}"), 0, 2, None, &candidates);
            if after == *original {
                unchanged += 1;
            } else {
                assert!(after.contains(&"new-member".to_owned()));
                assert!(after.iter().any(|holder| original.contains(holder)));
            }
        }
        assert!(
            unchanged > 1200,
            "only {unchanged} owners kept their holders"
        );
    }

    #[test]
    fn weights_are_roughly_proportional_and_capped() {
        let candidates = vec![candidate("small", GIB), candidate("large", 4 * GIB)];
        let large_wins = (0..10_000)
            .filter(|i| place(&format!("owner-{i}"), 0, 1, None, &candidates)[0] == "large")
            .count();
        assert!((7600..8400).contains(&large_wins), "{large_wins}");

        let capped = vec![candidate("a", 64 * GIB), candidate("b", 64 * GIB)];
        let over_cap = vec![candidate("a", u64::MAX), candidate("b", 64 * GIB)];
        for i in 0..100 {
            let owner = format!("owner-{i}");
            assert_eq!(
                place(&owner, 0, 2, None, &capped),
                place(&owner, 0, 2, None, &over_cap)
            );
        }
    }
}
