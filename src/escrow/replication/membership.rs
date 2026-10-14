//! Admission and operator-facing bounds; voting changes remain Raft log operations.
use anyhow::{Result, ensure};
use mistlib_consensus_core::{NodeId, storage::RaftMembership};
use std::collections::{BTreeSet, HashMap};

const MAX_VOTERS: usize = 9;
const MAX_LEARNERS: usize = 9;

pub(super) fn configuration(voters: &[String], learners: &[String]) -> Result<RaftMembership> {
    ensure!(
        (1..=MAX_VOTERS).contains(&voters.len()),
        "expected 1..9 voters"
    );
    ensure!(
        learners.len() <= MAX_LEARNERS,
        "expected at most 9 learners"
    );
    let mut ids = BTreeSet::new();
    for id in voters.iter().chain(learners) {
        ensure!(
            super::super::valid_voter(id) && ids.insert(id),
            "invalid, duplicate or overlapping member id"
        );
    }
    let mut voters: Vec<_> = voters.iter().cloned().map(NodeId).collect();
    let mut learners: Vec<_> = learners.iter().cloned().map(NodeId).collect();
    voters.sort_by(|a, b| a.0.cmp(&b.0));
    learners.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(RaftMembership {
        voters,
        joint_voters: None,
        learners,
    })
}

pub(super) fn peers(membership: &RaftMembership) -> Vec<String> {
    membership
        .voters
        .iter()
        .chain(membership.joint_voters.iter().flatten())
        .chain(&membership.learners)
        .map(|id| id.0.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn change(
    current: &RaftMembership,
    voters: &[String],
    learners: &[String],
    committed: u64,
    matched: &HashMap<String, u64>,
) -> Result<RaftMembership> {
    ensure!(
        current.joint_voters.is_none(),
        "membership change already in progress"
    );
    let next = configuration(voters, learners)?;
    let simultaneous: BTreeSet<_> = current
        .voters
        .iter()
        .chain(&next.voters)
        .chain(&next.learners)
        .map(|id| &id.0)
        .collect();
    ensure!(
        simultaneous.len() <= MAX_VOTERS + MAX_LEARNERS,
        "joint membership exceeds peer capacity"
    );
    for voter in &next.voters {
        if !current.voters.contains(voter) {
            ensure!(
                current.learners.contains(voter),
                "new voters must first join as learners"
            );
            ensure!(
                matched
                    .get(&voter.0)
                    .is_some_and(|index| *index >= committed),
                "learner has not durably caught up"
            );
        }
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(number: u8) -> String {
        format!("{number:016x}")
    }
    fn config(voters: &[u8], learners: &[u8]) -> RaftMembership {
        configuration(
            &voters.iter().map(|n| id(*n)).collect::<Vec<_>>(),
            &learners.iter().map(|n| id(*n)).collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn initial_learner_is_admitted_without_voting_or_bootstrapping() {
        let current = config(&[1, 2, 3], &[4]);
        assert_eq!(peers(&current), vec![id(1), id(2), id(3), id(4)]);
        assert!(!current.voters.contains(&NodeId(id(4))));
        assert_eq!(current.voters.len() / 2 + 1, 2);
    }

    #[test]
    fn promotion_requires_durable_match_including_empty_snapshot() {
        let current = config(&[1, 2, 3], &[4]);
        let voters = vec![id(1), id(2), id(3), id(4)];
        assert!(change(&current, &voters, &[], 0, &HashMap::new()).is_err());
        assert!(change(&current, &voters, &[], 0, &HashMap::from([(id(4), 0)])).is_ok());
        assert!(change(&current, &voters, &[], 12, &HashMap::from([(id(4), 11)])).is_err());
        assert!(change(&current, &voters, &[], 12, &HashMap::from([(id(4), 12)])).is_ok());
        assert!(
            change(
                &current,
                &[id(1), id(2), id(5)],
                &[],
                12,
                &HashMap::from([(id(5), 12)])
            )
            .is_err()
        );
    }

    #[test]
    fn disconnect_does_not_change_voters_or_during_joint_admission() {
        let mut current = config(&[1, 2, 3], &[4]);
        let before = current.voters.clone();
        assert!(
            change(
                &current,
                &[id(1), id(2), id(4)],
                &[],
                15,
                &HashMap::from([(id(4), 14)])
            )
            .is_err()
        );
        assert_eq!(current.voters, before);
        current.joint_voters = Some(vec![NodeId(id(1)), NodeId(id(2)), NodeId(id(4))]);
        assert_eq!(peers(&current), vec![id(1), id(2), id(3), id(4)]);
        assert!(change(&current, &[id(1), id(2)], &[], 15, &HashMap::new()).is_err());
    }

    #[test]
    fn membership_bounds_are_validated_before_raft() {
        assert!(configuration(&[], &[]).is_err());
        assert!(configuration(&(0..10).map(id).collect::<Vec<_>>(), &[]).is_err());
        assert!(configuration(&[id(0)], &(1..11).map(id).collect::<Vec<_>>()).is_err());
        for (voters, learners) in [
            (vec![id(1), id(1)], vec![]),
            (vec![id(1)], vec![id(1)]),
            (vec!["00000000000000AA".into()], vec![]),
            (vec![id(1)], vec!["invalid".into()]),
        ] {
            assert!(configuration(&voters, &learners).is_err());
        }
        let current = config(&[3, 1, 2], &[5, 4]);
        assert_eq!(
            current.voters,
            vec![NodeId(id(1)), NodeId(id(2)), NodeId(id(3))]
        );
    }
}
