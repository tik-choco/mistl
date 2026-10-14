//! Disk-backed Raft state and compacted application snapshots, durable before RPCs.
use anyhow::{Context, Result, ensure};
use mistlib_consensus_core::storage::{RaftMembership, RaftSnapshot};
use mistlib_consensus_core::{
    Event, LogEntry, NodeId, Output, RaftMessage, RaftState, RaftStorage, Role,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

const MAX_STATE_BYTES: usize = 80 * 1024 * 1024;
const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const MAX_COMMAND_BYTES: usize = 16 * 1024;
const MAX_ENTRIES: usize = 1024;

struct StateBytes(Vec<u8>);
impl std::io::Write for StateBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_STATE_BYTES {
            return Err(std::io::Error::other("Raft log full; compaction required"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Durable {
    v: u8,
    node: NodeId,
    voters: Vec<NodeId>,
    #[serde(default)]
    learners: Vec<NodeId>,
    term: u64,
    vote: Option<NodeId>,
    log: Vec<LogEntry>,
    commit: u64,
    applied: u64,
    #[serde(default)]
    snapshot: Option<SnapshotFile>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotFile {
    last_included_index: u64,
    last_included_term: u64,
    sha256: String,
    size: usize,
    membership: Option<RaftMembership>,
}

impl SnapshotFile {
    fn from_snapshot(snapshot: &RaftSnapshot) -> Self {
        Self {
            last_included_index: snapshot.last_included_index,
            last_included_term: snapshot.last_included_term,
            sha256: crate::escrow::protocol::digest(&snapshot.data),
            size: snapshot.data.len(),
            membership: snapshot.membership.clone(),
        }
    }
    fn path(&self, raft_path: &std::path::Path) -> PathBuf {
        raft_path.with_file_name(format!(
            "{}.snapshot-{}.bin",
            raft_path.file_stem().unwrap().to_string_lossy(),
            self.sha256
        ))
    }
    fn load(&self, raft_path: &std::path::Path, term: u64) -> Result<RaftSnapshot> {
        ensure!(
            crate::escrow::protocol::hex_string(&self.sha256, 64)
                && self.size <= MAX_SNAPSHOT_BYTES,
            "invalid snapshot file reference"
        );
        let file = std::fs::File::open(self.path(raft_path)).context("missing Raft snapshot")?;
        ensure!(
            file.metadata()?.len() == self.size as u64,
            "invalid snapshot file size"
        );
        let mut bytes = Vec::with_capacity(self.size);
        file.take(self.size as u64 + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() == self.size && crate::escrow::protocol::digest(&bytes) == self.sha256,
            "corrupt Raft snapshot"
        );
        let snapshot = RaftSnapshot {
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
            data: Arc::new(bytes),
            membership: self.membership.clone(),
        };
        validate_snapshot(&snapshot, term)?;
        Ok(snapshot)
    }
}

struct Storage {
    term: u64,
    vote: Option<NodeId>,
    log: Vec<LogEntry>,
    committed: u64,
    snapshot: Option<RaftSnapshot>,
    dirty: bool,
}

impl Storage {
    fn base(&self) -> u64 {
        self.snapshot.as_ref().map_or(0, |s| s.last_included_index)
    }
    fn entry(&self, index: u64) -> Option<&LogEntry> {
        index
            .checked_sub(self.base() + 1)
            .and_then(|n| usize::try_from(n).ok())
            .and_then(|n| self.log.get(n))
    }
}
impl RaftStorage for Storage {
    fn current_term(&self) -> u64 {
        self.term
    }
    fn voted_for(&self) -> Option<NodeId> {
        self.vote.clone()
    }
    fn set_term_and_vote(&mut self, term: u64, vote: Option<NodeId>) {
        if self.term != term || self.vote != vote {
            self.term = term;
            self.vote = vote;
            self.dirty = true;
        }
    }
    fn log(&self) -> &[LogEntry] {
        &self.log
    }
    fn append_entries(&mut self, start: u64, entries: Vec<LogEntry>) {
        // Matching entries do not replace their suffix: delayed RPCs may be shorter.
        for (offset, entry) in entries.iter().enumerate() {
            let index = (start - self.base() - 1) as usize + offset;
            if self.log.get(index) == Some(entry) {
                continue;
            }
            assert!(entry.index > self.committed, "committed log conflict");
            self.log.truncate(index);
            self.log.extend_from_slice(&entries[offset..]);
            self.dirty = true;
            break;
        }
    }
    fn last_log_index(&self) -> u64 {
        self.log.last().map_or(self.base(), |entry| entry.index)
    }
    fn last_log_term(&self) -> u64 {
        self.log.last().map_or_else(
            || self.snapshot.as_ref().map_or(0, |s| s.last_included_term),
            |entry| entry.term,
        )
    }
    fn term_at(&self, index: u64) -> Option<u64> {
        if let Some(snapshot) = &self.snapshot {
            if index == snapshot.last_included_index {
                return Some(snapshot.last_included_term);
            }
            if index < snapshot.last_included_index {
                return None;
            }
        }
        if index == 0 {
            Some(0)
        } else {
            self.entry(index).map(|e| e.term)
        }
    }
    fn entries_from(&self, from: u64) -> Vec<LogEntry> {
        self.log
            .iter()
            .filter(|entry| entry.index >= from)
            .cloned()
            .collect()
    }
    fn truncate_from(&mut self, index: u64) {
        assert!(index > self.committed, "committed log truncation");
        if index <= self.last_log_index() {
            self.log.retain(|entry| entry.index < index);
            self.dirty = true;
        }
    }
    fn snapshot(&self) -> Option<RaftSnapshot> {
        self.snapshot.clone()
    }
    fn install_snapshot(&mut self, snapshot: RaftSnapshot) -> bool {
        if snapshot.last_included_index <= self.base() {
            return false;
        }
        if self.term_at(snapshot.last_included_index) == Some(snapshot.last_included_term) {
            self.log
                .retain(|entry| entry.index > snapshot.last_included_index);
        } else {
            if snapshot.last_included_index < self.committed {
                return false;
            }
            self.log.clear();
        }
        self.snapshot = Some(snapshot);
        self.dirty = true;
        true
    }
}

pub(super) struct Engine {
    path: PathBuf,
    voters: Vec<NodeId>,
    state: RaftState<Storage>,
    genesis_learners: Vec<NodeId>,
    applied: u64,
    saved_commit: u64,
    stopped: bool,
    acknowledged: BTreeMap<String, u64>,
    saved_snapshot: Option<SnapshotFile>,
}

impl Engine {
    #[cfg(test)]
    pub(super) fn open(path: PathBuf, node: NodeId, voters: Vec<NodeId>) -> Result<Self> {
        Self::open_with_membership(path, node, voters, Vec::new())
    }
    pub(super) fn open_with_membership(
        path: PathBuf,
        node: NodeId,
        mut voters: Vec<NodeId>,
        mut learners: Vec<NodeId>,
    ) -> Result<Self> {
        ensure!(
            !voters.is_empty() && voters.len() <= 64,
            "invalid Raft voter count"
        );
        voters.sort_by(|a, b| a.0.cmp(&b.0));
        let unique: BTreeSet<_> = voters.iter().map(|id| id.0.as_str()).collect();
        ensure!(
            unique.len() == voters.len() && !node.0.is_empty() && node.0.len() <= 128,
            "invalid Raft voters"
        );
        ensure!(
            voters
                .iter()
                .all(|id| !id.0.is_empty() && id.0.len() <= 128),
            "invalid Raft voter id"
        );
        learners.sort_by(|a, b| a.0.cmp(&b.0));
        ensure!(
            learners.len() <= 64
                && learners
                    .iter()
                    .all(|id| !id.0.is_empty() && id.0.len() <= 128 && !voters.contains(id))
                && learners.windows(2).all(|w| w[0] != w[1]),
            "invalid Raft learners"
        );
        let durable = match std::fs::metadata(&path) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_file() && metadata.len() <= MAX_STATE_BYTES as u64,
                    "oversized Raft state"
                );
                let bytes = std::fs::read(&path)?;
                ensure!(bytes.len() <= MAX_STATE_BYTES, "oversized Raft state");
                let durable: Durable =
                    serde_json::from_slice(&bytes).context("invalid Raft state")?;
                ensure!(
                    durable.v == 1
                        && durable.node == node
                        && durable.voters == voters
                        && durable.learners == learners,
                    "Raft configuration changed"
                );
                durable
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Durable {
                v: 1,
                node: node.clone(),
                voters: voters.clone(),
                learners: learners.clone(),
                term: 0,
                vote: None,
                log: Vec::new(),
                commit: 0,
                applied: 0,
                snapshot: None,
            },
            Err(error) => return Err(error.into()),
        };
        ensure!(durable.term < u64::MAX, "Raft term exhausted");
        ensure!(
            durable
                .vote
                .as_ref()
                .is_none_or(|id| !id.0.is_empty() && id.0.len() <= 128),
            "invalid persisted vote"
        );
        let snapshot = durable
            .snapshot
            .as_ref()
            .map(|s| s.load(&path, durable.term))
            .transpose()?;
        let base = snapshot.as_ref().map_or(0, |s| s.last_included_index);
        validate_entries(&durable.log, base, durable.term)?;
        ensure!(
            snapshot.as_ref().is_none_or(|s| durable
                .log
                .first()
                .is_none_or(|e| e.term >= s.last_included_term)),
            "invalid snapshot tail term order"
        );
        ensure!(
            base <= durable.applied
                && durable.applied <= durable.commit
                && durable.commit <= base + durable.log.len() as u64,
            "invalid persisted Raft cursors"
        );
        let genesis = RaftMembership {
            voters: voters.clone(),
            joint_voters: None,
            learners: learners.clone(),
        };
        let mut state = RaftState::with_membership(
            node,
            genesis,
            Storage {
                term: durable.term,
                vote: durable.vote,
                log: durable.log,
                committed: durable.commit,
                snapshot,
                dirty: false,
            },
        )
        .context("invalid persisted Raft membership")?;
        state.commit_index = durable.commit;
        // Undelivered application work is recovered from log()/last_applied().
        state.last_applied = durable.commit;
        let mut engine = Self {
            path,
            voters,
            state,
            genesis_learners: learners,
            applied: durable.applied,
            saved_commit: durable.commit,
            stopped: false,
            acknowledged: BTreeMap::new(),
            saved_snapshot: durable.snapshot,
        };
        engine.persist()?;
        Ok(engine)
    }

    pub(super) fn step(&mut self, mut event: Event) -> Result<Vec<Output>> {
        ensure!(!self.stopped, "Raft storage unavailable; restart required");
        let mut matched = None;
        match &mut event {
            Event::AddPeer { .. }
            | Event::RemovePeer { .. }
            | Event::AddLearner { .. }
            | Event::ChangeMembership { .. } => {
                ensure!(
                    self.log().len() < MAX_ENTRIES - 1,
                    "Raft log full; compaction required"
                );
            }
            Event::ClientPropose { command } => {
                ensure!(command.len() <= MAX_COMMAND_BYTES, "oversized Raft command");
                ensure!(
                    self.state.storage.log.len() < MAX_ENTRIES,
                    "Raft log full; compaction required"
                );
            }
            Event::ElectionTimeout => {
                ensure!(self.current_term() < u64::MAX - 1, "Raft term exhausted")
            }
            Event::Message { from, msg } => {
                ensure!(
                    *from != self.state.id && self.state.accepts_peer(from),
                    "unknown Raft voter"
                );
                ensure!(message_sender(msg) == from, "Raft sender mismatch");
                ensure!(msg.term() < u64::MAX, "Raft term exhausted");
                match msg {
                    RaftMessage::AppendEntries {
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                        term,
                        ..
                    } => {
                        ensure!(
                            *prev_log_index < u64::MAX && *prev_log_term <= *term,
                            "invalid Raft prefix"
                        );
                        validate_entries(entries, *prev_log_index, *term)?;
                        ensure!(
                            entries
                                .first()
                                .is_none_or(|entry| entry.term >= *prev_log_term),
                            "invalid Raft entry term order"
                        );
                        let end = *prev_log_index + entries.len() as u64;
                        ensure!(
                            end.saturating_sub(self.state.storage.base()) <= MAX_ENTRIES as u64
                                || *prev_log_index < self.state.storage.base(),
                            "Raft log full; compaction required"
                        );
                        // Reject corruption before calling a trait without an error return.
                        if *term >= self.current_term()
                            && self.state.storage.term_at(*prev_log_index) == Some(*prev_log_term)
                        {
                            for entry in entries
                                .iter()
                                .filter(|entry| entry.index <= self.state.commit_index)
                            {
                                ensure!(
                                    self.state.storage.entry(entry.index) == Some(entry),
                                    "committed Raft log conflict"
                                );
                            }
                        }
                        *leader_commit = (*leader_commit).min(end);
                        matched = Some(end);
                    }
                    RaftMessage::AppendEntriesResponse {
                        success: true,
                        match_index,
                        term,
                        ..
                    } => {
                        ensure!(
                            *match_index <= self.state.storage.last_log_index(),
                            "invalid Raft acknowledgement"
                        );
                        if *term == self.current_term() && self.role() == Role::Leader {
                            let acknowledged = self.acknowledged.entry(from.0.clone()).or_default();
                            *acknowledged = (*acknowledged).max(*match_index);
                            *match_index = *acknowledged;
                        }
                    }
                    RaftMessage::RequestVote {
                        last_log_index,
                        last_log_term,
                        term,
                        ..
                    } => {
                        ensure!(
                            *last_log_index < u64::MAX && *last_log_term <= *term,
                            "invalid Raft candidate log"
                        );
                    }
                    RaftMessage::InstallSnapshot { snapshot, term, .. } => {
                        validate_snapshot(snapshot, *term)?;
                    }
                    RaftMessage::InstallSnapshotResponse {
                        success: true,
                        last_included_index,
                        ..
                    } => {
                        ensure!(
                            *last_included_index <= self.state.storage.last_log_index(),
                            "invalid snapshot acknowledgement"
                        );
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        let before_term = self.current_term();
        let before_role = self.role();
        self.state.storage.committed = self.state.commit_index;
        let mut outputs = self.state.step(event);
        if before_term != self.current_term() || before_role != self.role() {
            self.acknowledged.clear();
        }
        if let Some(end) = matched {
            for output in &mut outputs {
                if let Output::SendMessage {
                    msg:
                        RaftMessage::AppendEntriesResponse {
                            success: true,
                            match_index,
                            ..
                        },
                    ..
                } = output
                {
                    *match_index = end;
                }
            }
        }
        for output in &outputs {
            if let Output::InstallSnapshot { snapshot } = output {
                self.applied = self.applied.max(snapshot.last_included_index);
            }
        }
        self.state.storage.committed = self.state.commit_index;
        if self.state.storage.dirty || self.saved_commit != self.state.commit_index {
            self.persist()?;
        }
        Ok(outputs)
    }

    fn persist(&mut self) -> Result<()> {
        let result: Result<()> = (|| {
            let snapshot = self.state.storage.snapshot.as_ref().map(|s| {
                self.saved_snapshot
                    .as_ref()
                    .filter(|saved| {
                        saved.last_included_index == s.last_included_index
                            && saved.last_included_term == s.last_included_term
                    })
                    .cloned()
                    .unwrap_or_else(|| SnapshotFile::from_snapshot(s))
            });
            if let (Some(snapshot), Some(data)) = (&snapshot, &self.state.storage.snapshot) {
                if self.saved_snapshot.as_ref().map(|s| &s.sha256) != Some(&snapshot.sha256) {
                    crate::statefile::write_private(&snapshot.path(&self.path), &data.data)?;
                }
            }
            let durable = Durable {
                v: 1,
                node: self.state.id.clone(),
                voters: self.voters.clone(),
                learners: self.genesis_learners.clone(),
                term: self.current_term(),
                vote: self.state.storage.vote.clone(),
                log: self.state.storage.log.clone(),
                commit: self.state.commit_index,
                applied: self.applied,
                snapshot: snapshot.clone(),
            };
            let mut bytes = StateBytes(Vec::new());
            serde_json::to_writer(&mut bytes, &durable)?;
            crate::statefile::write_private(&self.path, &bytes.0)?;
            self.saved_snapshot = snapshot;
            // Metadata has durably moved to the new snapshot; old binary files
            // are now orphans, including snapshots from interrupted installs.
            for item in std::fs::read_dir(self.path.parent().context("Raft parent")?)? {
                let item = item?;
                let name = item.file_name().to_string_lossy().into_owned();
                let prefix = format!(
                    "{}.snapshot-",
                    self.path.file_stem().unwrap().to_string_lossy()
                );
                if let Some(hash) = name
                    .strip_prefix(&prefix)
                    .and_then(|n| n.strip_suffix(".bin"))
                {
                    if crate::escrow::protocol::hex_string(hash, 64)
                        && item.file_type()?.is_file()
                        && self
                            .saved_snapshot
                            .as_ref()
                            .is_none_or(|s| s.sha256 != hash)
                    {
                        std::fs::remove_file(item.path())?;
                    }
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.stopped = true;
        }
        result?;
        self.state.storage.dirty = false;
        self.saved_commit = self.state.commit_index;
        Ok(())
    }

    pub(super) fn mark_applied(&mut self, index: u64) -> Result<()> {
        ensure!(!self.stopped, "Raft storage unavailable; restart required");
        if index == self.applied {
            return Ok(());
        }
        ensure!(
            index == self.applied + 1 && index <= self.state.commit_index,
            "invalid Raft application cursor"
        );
        self.applied = index;
        self.persist()
    }
    pub(super) fn snapshot(&self) -> Option<RaftSnapshot> {
        if self.stopped {
            None
        } else {
            self.state.storage.snapshot()
        }
    }
    pub(super) fn needs_compaction(&self) -> bool {
        self.log().len() >= 128 && self.applied > self.state.storage.base()
    }
    pub(super) fn compact(&mut self, data: Vec<u8>) -> Result<()> {
        ensure!(!self.stopped, "Raft storage unavailable; restart required");
        ensure!(
            data.len() <= MAX_SNAPSHOT_BYTES,
            "oversized escrow snapshot"
        );
        ensure!(
            self.applied == self.commit_index(),
            "snapshot requires fully applied committed state"
        );
        if self.applied == self.state.storage.base() {
            return Ok(());
        }
        let outputs = self.step(Event::CompactSnapshot {
            index: self.applied,
            data,
        })?;
        ensure!(
            outputs
                .iter()
                .any(|output| matches!(output, Output::SnapshotCompacted { .. })),
            "Raft snapshot rejected"
        );
        Ok(())
    }
    pub(super) fn stopped(&self) -> bool {
        self.stopped
    }
    pub(super) fn role(&self) -> Role {
        if self.stopped {
            Role::Follower
        } else {
            self.state.role
        }
    }
    pub(super) fn leader(&self) -> Option<NodeId> {
        if self.stopped {
            return None;
        }
        if self.role() == Role::Leader {
            Some(self.state.id.clone())
        } else {
            self.state
                .leader_id
                .clone()
                .filter(|id| *id != self.state.id)
        }
    }
    pub(super) fn current_term(&self) -> u64 {
        self.state.storage.term
    }
    pub(super) fn log(&self) -> &[LogEntry] {
        &self.state.storage.log
    }
    pub(super) fn commit_index(&self) -> u64 {
        self.state.commit_index
    }
    pub(super) fn last_applied(&self) -> u64 {
        self.applied
    }
    pub(super) fn quorum(&self) -> usize {
        self.state.membership.voters.len() / 2 + 1
    }
    pub(super) fn voters(&self) -> &[NodeId] {
        &self.state.membership.voters
    }
    pub(super) fn membership(&self) -> RaftMembership {
        self.state.membership.clone()
    }
    pub(super) fn committed_membership(&self) -> RaftMembership {
        self.state.committed_membership()
    }
    pub(super) fn peers(&self) -> Vec<NodeId> {
        self.state.peers.iter().cloned().collect()
    }
    pub(super) fn matched_index(&self, peer: &NodeId) -> u64 {
        self.state.matched_index(peer)
    }
}

fn message_sender(msg: &RaftMessage) -> &NodeId {
    match msg {
        RaftMessage::RequestVote { candidate_id, .. } => candidate_id,
        RaftMessage::RequestVoteResponse { voter_id, .. } => voter_id,
        RaftMessage::AppendEntries { leader_id, .. } => leader_id,
        RaftMessage::AppendEntriesResponse { follower_id, .. }
        | RaftMessage::InstallSnapshotResponse { follower_id, .. }
        | RaftMessage::AppendEntriesConflict { follower_id, .. } => follower_id,
        RaftMessage::InstallSnapshot { leader_id, .. } => leader_id,
    }
}

fn validate_entries(entries: &[LogEntry], previous: u64, term: u64) -> Result<()> {
    ensure!(
        entries.len() <= MAX_ENTRIES && previous < u64::MAX - entries.len() as u64,
        "oversized Raft log"
    );
    let mut prior_term = 0;
    for (offset, entry) in entries.iter().enumerate() {
        ensure!(
            entry.index == previous + offset as u64 + 1
                && entry.term > 0
                && entry.term <= term
                && entry.term >= prior_term
                && entry.command.len() <= MAX_COMMAND_BYTES,
            "invalid Raft log entry"
        );
        prior_term = entry.term;
    }
    Ok(())
}

fn validate_snapshot(snapshot: &RaftSnapshot, term: u64) -> Result<()> {
    ensure!(
        snapshot.last_included_index > 0
            && snapshot.last_included_index < u64::MAX
            && snapshot.last_included_term > 0
            && snapshot.last_included_term <= term
            && snapshot.data.len() <= MAX_SNAPSHOT_BYTES,
        "invalid Raft snapshot"
    );
    if let Some(membership) = &snapshot.membership {
        for ids in [&membership.voters, &membership.learners] {
            ensure!(
                ids.len() <= 64
                    && ids.iter().all(|id| !id.0.is_empty() && id.0.len() <= 128)
                    && ids.iter().map(|id| &id.0).collect::<BTreeSet<_>>().len() == ids.len(),
                "invalid snapshot membership"
            );
        }
        ensure!(!membership.voters.is_empty(), "empty snapshot voters");
        if let Some(ids) = &membership.joint_voters {
            ensure!(
                !ids.is_empty()
                    && ids.len() <= 64
                    && ids.iter().all(|id| !id.0.is_empty() && id.0.len() <= 128)
                    && ids.iter().map(|id| &id.0).collect::<BTreeSet<_>>().len() == ids.len(),
                "invalid joint membership"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Dir(PathBuf);
    impl Dir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("mistl-raft-{:016x}", rand::random::<u64>()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn id(value: &str) -> NodeId {
        NodeId(value.into())
    }
    fn engine(dir: &Dir, node: &str) -> Engine {
        Engine::open(
            dir.0.join(format!("{node}.json")),
            id(node),
            vec![id("a"), id("b"), id("c")],
        )
        .unwrap()
    }
    fn deliver(nodes: &mut [Engine], source: usize, outputs: Vec<Output>) {
        deliver_where(nodes, source, outputs, |_, _| true);
    }
    fn deliver_where(
        nodes: &mut [Engine],
        source: usize,
        outputs: Vec<Output>,
        connected: impl Fn(usize, usize) -> bool,
    ) {
        let mut pending = VecDeque::from([(source, outputs)]);
        let mut count = 0;
        while let Some((source, outputs)) = pending.pop_front() {
            for output in outputs {
                if let Output::SendMessage { to, msg } = output {
                    count += 1;
                    assert!(count < 1000);
                    let target = nodes.iter().position(|node| node.state.id == to).unwrap();
                    if !connected(source, target) {
                        continue;
                    }
                    let from = nodes[source].state.id.clone();
                    let outputs = nodes[target].step(Event::Message { from, msg }).unwrap();
                    pending.push_back((target, outputs));
                }
            }
        }
    }
    fn append(term: u64, previous: u64, entries: Vec<LogEntry>, commit: u64) -> Event {
        Event::Message {
            from: id("a"),
            msg: RaftMessage::AppendEntries {
                term,
                leader_id: id("a"),
                prev_log_index: previous,
                prev_log_term: if previous == 0 { 0 } else { term },
                entries,
                leader_commit: commit,
            },
        }
    }
    fn entry(index: u64, term: u64, command: u8) -> LogEntry {
        LogEntry {
            index,
            term,
            command: vec![command],
        }
    }

    #[test]
    fn majority_commit_and_application_survive_restart() {
        let dir = Dir::new();
        let mut nodes = [engine(&dir, "a"), engine(&dir, "b"), engine(&dir, "c")];
        let out = nodes[0].step(Event::ElectionTimeout).unwrap();
        deliver(&mut nodes, 0, out);
        assert_eq!(nodes[0].role(), Role::Leader);
        let out = nodes[0]
            .step(Event::ClientPropose {
                command: b"checkpoint".to_vec(),
            })
            .unwrap();
        assert_eq!(nodes[0].commit_index(), 0);
        deliver(&mut nodes, 0, out);
        assert_eq!(nodes[0].commit_index(), 1);
        let out = nodes[0].step(Event::HeartbeatTimeout).unwrap();
        deliver(&mut nodes, 0, out);
        assert!(nodes.iter().all(|node| node.commit_index() == 1));
        nodes[0].mark_applied(1).unwrap();
        let restored = engine(&dir, "a");
        assert_eq!(restored.commit_index(), 1);
        assert_eq!(restored.last_applied(), 1);
        assert_eq!(restored.log()[0].command, b"checkpoint");
    }

    #[test]
    fn isolated_voter_never_commits_even_during_joint_membership_change() {
        let dir = Dir::new();
        let mut a = engine(&dir, "a");
        a.step(Event::ElectionTimeout).unwrap();
        assert_eq!(a.role(), Role::Candidate);
        a.step(Event::Message {
            from: id("b"),
            msg: RaftMessage::RequestVoteResponse {
                term: a.current_term(),
                vote_granted: true,
                voter_id: id("b"),
            },
        })
        .unwrap();
        a.step(Event::ClientPropose { command: vec![1] }).unwrap();
        for _ in 0..3 {
            a.step(Event::HeartbeatTimeout).unwrap();
        }
        assert_eq!(a.commit_index(), 0);
        assert_eq!(a.quorum(), 2);
        let outputs = a.step(Event::RemovePeer { peer: id("b") }).unwrap();
        assert!(outputs.iter().any(|o| matches!(o, Output::Proposed { .. })));
        assert!(a.membership().joint_voters.is_some());
        assert_eq!(a.commit_index(), 0);
        assert_eq!(a.quorum(), 2);
    }

    #[test]
    fn restart_remembers_vote_and_uncommitted_log() {
        let dir = Dir::new();
        let mut b = engine(&dir, "b");
        b.step(Event::Message {
            from: id("a"),
            msg: RaftMessage::RequestVote {
                term: 4,
                candidate_id: id("a"),
                last_log_index: 0,
                last_log_term: 0,
            },
        })
        .unwrap();
        b.step(append(4, 0, vec![entry(1, 4, 7)], 0)).unwrap();
        let mut restored = engine(&dir, "b");
        assert_eq!(restored.log(), &[entry(1, 4, 7)]);
        let out = restored
            .step(Event::Message {
                from: id("c"),
                msg: RaftMessage::RequestVote {
                    term: 4,
                    candidate_id: id("c"),
                    last_log_index: 1,
                    last_log_term: 4,
                },
            })
            .unwrap();
        assert!(out.iter().any(|output| matches!(
            output,
            Output::SendMessage {
                msg: RaftMessage::RequestVoteResponse {
                    vote_granted: false,
                    ..
                },
                ..
            }
        )));
    }

    #[test]
    fn delayed_short_append_preserves_committed_suffix() {
        let dir = Dir::new();
        let mut b = engine(&dir, "b");
        b.step(append(1, 0, vec![entry(1, 1, 1), entry(2, 1, 2)], 2))
            .unwrap();
        let out = b.step(append(1, 0, vec![entry(1, 1, 1)], 1)).unwrap();
        assert_eq!(b.log(), &[entry(1, 1, 1), entry(2, 1, 2)]);
        assert_eq!(b.commit_index(), 2);
        assert!(out.iter().any(|output| matches!(
            output,
            Output::SendMessage {
                msg: RaftMessage::AppendEntriesResponse {
                    success: true,
                    match_index: 1,
                    ..
                },
                ..
            }
        )));
        assert!(b.step(append(2, 0, vec![entry(1, 2, 9)], 0)).is_err());
        assert_eq!(b.log().len(), 2);
    }

    #[test]
    fn heartbeat_never_acknowledges_or_commits_unmatched_tail() {
        let dir = Dir::new();
        let mut b = engine(&dir, "b");
        b.step(append(1, 0, vec![entry(1, 1, 1), entry(2, 1, 2)], 0))
            .unwrap();
        let out = b.step(append(1, 1, vec![], 2)).unwrap();
        assert_eq!(b.commit_index(), 1);
        assert!(out.iter().any(|output| matches!(
            output,
            Output::SendMessage {
                msg: RaftMessage::AppendEntriesResponse {
                    success: true,
                    match_index: 1,
                    ..
                },
                ..
            }
        )));
    }

    #[test]
    fn persistence_failure_emits_no_reply_and_stops_driver() {
        let dir = Dir::new();
        let mut b = engine(&dir, "b");
        std::fs::remove_file(&b.path).unwrap();
        std::fs::create_dir(&b.path).unwrap();
        assert!(b.step(append(1, 0, vec![entry(1, 1, 1)], 1)).is_err());
        assert!(b.step(Event::HeartbeatTimeout).is_err());
    }

    #[test]
    fn unknown_voter_sender_spoof_and_noncontiguous_log_are_rejected() {
        let dir = Dir::new();
        let mut b = engine(&dir, "b");
        assert!(
            b.step(Event::Message {
                from: id("outsider"),
                msg: RaftMessage::RequestVote {
                    term: 1,
                    candidate_id: id("outsider"),
                    last_log_index: 0,
                    last_log_term: 0,
                }
            })
            .is_err()
        );
        assert!(
            b.step(Event::Message {
                from: id("a"),
                msg: RaftMessage::RequestVote {
                    term: 1,
                    candidate_id: id("c"),
                    last_log_index: 0,
                    last_log_term: 0,
                }
            })
            .is_err()
        );
        assert!(b.step(append(1, 0, vec![entry(2, 1, 1)], 0)).is_err());
        assert_eq!(b.current_term(), 0);
    }

    #[test]
    fn partition_new_leader_replaces_only_uncommitted_suffix_then_rejoins() {
        let dir = Dir::new();
        let mut nodes = [engine(&dir, "a"), engine(&dir, "b"), engine(&dir, "c")];
        let out = nodes[0].step(Event::ElectionTimeout).unwrap();
        deliver(&mut nodes, 0, out);
        let out = nodes[0]
            .step(Event::ClientPropose {
                command: b"committed".to_vec(),
            })
            .unwrap();
        deliver(&mut nodes, 0, out);
        let out = nodes[0].step(Event::HeartbeatTimeout).unwrap();
        deliver(&mut nodes, 0, out);
        nodes[0]
            .step(Event::ClientPropose {
                command: b"isolated".to_vec(),
            })
            .unwrap();
        assert_eq!(nodes[0].commit_index(), 1);
        let out = nodes[1].step(Event::ElectionTimeout).unwrap();
        deliver_where(&mut nodes, 1, out, |source, target| {
            source != 0 && target != 0
        });
        assert_eq!(nodes[1].role(), Role::Leader);
        let out = nodes[1]
            .step(Event::ClientPropose {
                command: b"replacement".to_vec(),
            })
            .unwrap();
        deliver_where(&mut nodes, 1, out, |source, target| {
            source != 0 && target != 0
        });
        assert_eq!(nodes[1].commit_index(), 2);
        let out = nodes[1].step(Event::HeartbeatTimeout).unwrap();
        deliver(&mut nodes, 1, out);
        assert_eq!(nodes[0].role(), Role::Follower);
        assert_eq!(nodes[0].log(), nodes[1].log());
        // Propagate the final commit after a catch-up AppendEntriesResponse.
        let out = nodes[1].step(Event::HeartbeatTimeout).unwrap();
        deliver(&mut nodes, 1, out);
        assert!(nodes.iter().all(|node| node.commit_index() == 2));
        let restored = engine(&dir, "c");
        assert_eq!(restored.log()[0].command, b"committed");
        assert_eq!(restored.log()[1].command, b"replacement");
        assert_eq!(restored.commit_index(), 2);
        assert_eq!(restored.last_applied(), 0);
    }

    #[test]
    fn reordered_dropped_and_replayed_rpcs_never_diverge_committed_prefixes() {
        use rand::{Rng, SeedableRng};
        let dir = Dir::new();
        let mut nodes = [engine(&dir, "a"), engine(&dir, "b"), engine(&dir, "c")];
        let mut random = rand::rngs::StdRng::seed_from_u64(91237);
        let mut pending: Vec<(usize, usize, RaftMessage)> = Vec::new();
        let mut committed = BTreeMap::new();
        for tick in 0..1500 {
            let node = random.gen_range(0..3);
            let action = random.gen_range(0..30);
            let outputs = match action {
                0 => {
                    nodes[node] = engine(&dir, ["a", "b", "c"][node]);
                    Vec::new()
                }
                1 => nodes[node].step(Event::ElectionTimeout).unwrap(),
                2..=5 => nodes[node].step(Event::HeartbeatTimeout).unwrap(),
                6..=8 if nodes[node].role() == Role::Leader => nodes[node]
                    .step(Event::ClientPropose {
                        command: tick.to_string().into_bytes(),
                    })
                    .unwrap(),
                _ if !pending.is_empty() => {
                    let index = random.gen_range(0..pending.len());
                    let (source, target, msg) = pending.swap_remove(index);
                    if action == 9 {
                        continue;
                    }
                    if action == 10 {
                        pending.push((source, target, msg.clone()));
                    }
                    let from = nodes[source].state.id.clone();
                    let outputs = nodes[target].step(Event::Message { from, msg });
                    assert!(!nodes[target].stopped());
                    // Stale ACKs may reference a suffix replaced by a newer leader.
                    let outputs = outputs.unwrap_or_default();
                    for output in outputs {
                        if let Output::SendMessage { to, msg } = output {
                            let destination = nodes
                                .iter()
                                .position(|engine| engine.state.id == to)
                                .unwrap();
                            pending.push((target, destination, msg));
                        }
                    }
                    Vec::new()
                }
                _ => Vec::new(),
            };
            for output in outputs {
                if let Output::SendMessage { to, msg } = output {
                    let target = nodes
                        .iter()
                        .position(|engine| engine.state.id == to)
                        .unwrap();
                    pending.push((node, target, msg));
                }
            }
            for engine in &nodes {
                for entry in engine.log().iter().take(engine.commit_index() as usize) {
                    if let Some(previous) = committed.insert(entry.index, entry.clone()) {
                        assert_eq!(previous, *entry, "divergent committed entry at tick {tick}");
                    }
                }
            }
        }
        assert!(!committed.is_empty(), "schedule must exercise real commits");
    }
}

#[cfg(test)]
#[path = "engine_storage_tests.rs"]
mod production_tests;
