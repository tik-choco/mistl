//! Deterministic visible heads reconstructed from the committed room log.
use crate::escrow::protocol::{Delete, Error, Head, check_cas};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(super) enum Command {
    ReadBarrier { ticket: String },
    Put { ticket: String, head: Head },
    Delete { ticket: String, request: Delete },
    Bootstrap { ticket: String, heads: Vec<Head> },
    BootstrapBegin { ticket: String },
    BootstrapComplete { ticket: String },
}

impl Command {
    pub fn ticket(&self) -> &str {
        match self {
            Self::ReadBarrier { ticket }
            | Self::Put { ticket, .. }
            | Self::Delete { ticket, .. }
            | Self::Bootstrap { ticket, .. }
            | Self::BootstrapBegin { ticket }
            | Self::BootstrapComplete { ticket } => ticket,
        }
    }
}

pub(super) fn validate(command: &Command, max_blob: u64) -> Result<(), Error> {
    if command.ticket().is_empty() || command.ticket().len() > 64 {
        return Err(Error::new("bad_request"));
    }
    match command {
        Command::ReadBarrier { .. }
        | Command::BootstrapBegin { .. }
        | Command::BootstrapComplete { .. } => Ok(()),
        Command::Put { head, .. } => {
            head.validate(max_blob)?;
            head.verify()
        }
        Command::Delete { request, .. } => request.validate(),
        Command::Bootstrap { heads, .. } => {
            if heads.len() > 65536 {
                return Err(Error::new("quota"));
            }
            let mut slots = std::collections::HashSet::new();
            for head in heads {
                head.validate(max_blob)?;
                head.verify()?;
                if !slots.insert(&head.slot) {
                    return Err(Error::new("bad_request"));
                }
            }
            Ok(())
        }
    }
}

const SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

fn head_bytes(head: &Head) -> u64 {
    serde_json::to_vec(head).expect("head serialization").len() as u64 + 36
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    #[serde(skip)]
    metadata_bytes: u64,
    heads: HashMap<String, Head>,
    previous: HashMap<String, Head>,
    deleted: HashMap<String, Head>,
    #[serde(default)]
    migration_started: bool,
    #[serde(default)]
    migration_finished: bool,
}

impl State {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)?;
        anyhow::ensure!(
            bytes.len() as u64 <= SNAPSHOT_BYTES,
            "oversized escrow snapshot"
        );
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8], max_slots: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(bytes.len() <= 64 * 1024 * 1024, "oversized escrow snapshot");
        let mut state: Self = serde_json::from_slice(bytes)?;
        state.metadata_bytes = state
            .heads
            .values()
            .chain(state.previous.values())
            .chain(state.deleted.values())
            .map(head_bytes)
            .sum();
        anyhow::ensure!(
            state.metadata_bytes + 128 <= SNAPSHOT_BYTES,
            "escrow snapshot metadata quota"
        );
        anyhow::ensure!(
            !state.migration_finished || state.migration_started,
            "invalid migration flags"
        );
        anyhow::ensure!(
            state.heads.len().saturating_add(state.deleted.len()) <= max_slots
                && state.previous.len() <= state.heads.len(),
            "escrow snapshot slot quota"
        );
        for map in [&state.heads, &state.previous, &state.deleted] {
            for (slot, head) in map {
                anyhow::ensure!(*slot == head.slot, "escrow snapshot slot mismatch");
                head.validate(1024 * 1024 * 1024)
                    .map_err(|e| anyhow::anyhow!(e.code))?;
                head.verify().map_err(|e| anyhow::anyhow!(e.code))?;
            }
        }
        for (slot, head) in &state.previous {
            let current = state
                .heads
                .get(slot)
                .ok_or_else(|| anyhow::anyhow!("orphan previous head"))?;
            anyhow::ensure!(
                current.writer == head.writer
                    && head.seq.checked_add(1) == Some(current.seq)
                    && current.parent.as_deref() == Some(head.hash().as_str()),
                "invalid previous head"
            );
        }
        anyhow::ensure!(
            state
                .deleted
                .keys()
                .all(|slot| !state.heads.contains_key(slot) && !state.previous.contains_key(slot)),
            "conflicting tombstone"
        );
        Ok(state)
    }

    pub fn retained_heads(&self) -> Vec<Head> {
        self.heads
            .values()
            .chain(self.previous.values())
            .cloned()
            .collect()
    }

    pub fn migrating(&self) -> bool {
        self.migration_started && !self.migration_finished
    }
    pub fn migration_finished(&self) -> bool {
        self.migration_finished
    }

    pub fn is_empty(&self) -> bool {
        self.heads.is_empty() && self.deleted.is_empty()
    }

    pub fn head(&self, slot: &str) -> Option<Head> {
        self.heads.get(slot).cloned()
    }

    pub fn slots(&self) -> usize {
        self.heads.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.heads
            .values()
            .chain(self.previous.values())
            .map(|head| head.size)
            .sum()
    }

    pub fn apply(&mut self, command: &Command, id: &str, max_slots: usize) -> Value {
        let result = validate(command, 1024 * 1024 * 1024).and_then(|()| {
            if self.migration_started
                && !self.migration_finished
                && matches!(
                    command,
                    Command::ReadBarrier { .. } | Command::Put { .. } | Command::Delete { .. }
                )
            {
                return Err(Error::new("migrating"));
            }
            match command {
                Command::ReadBarrier { .. } => {
                    Ok(json!({"v":1,"type":"escrow_read_ready","id":id}))
                }
                Command::Put { head, .. } => self.put(head, id, max_slots),
                Command::Delete { request, .. } => self.delete(request, id),
                Command::Bootstrap { heads, .. } => {
                    if !self.migration_started || self.migration_finished {
                        return Err(Error::new("conflict"));
                    }
                    for head in heads {
                        if self
                            .heads
                            .get(&head.slot)
                            .is_some_and(|existing| existing != head)
                        {
                            return Err(Error::new("conflict"));
                        }
                    }
                    let additional = heads
                        .iter()
                        .filter(|head| !self.heads.contains_key(&head.slot))
                        .count();
                    if self.heads.len().saturating_add(additional) > max_slots {
                        return Err(Error::new("quota"));
                    }
                    let added_bytes: u64 = heads
                        .iter()
                        .filter(|head| !self.heads.contains_key(&head.slot))
                        .map(head_bytes)
                        .sum();
                    if self.metadata_bytes + added_bytes + 128 > SNAPSHOT_BYTES {
                        return Err(Error::new("quota"));
                    }
                    self.metadata_bytes += added_bytes;
                    for head in heads {
                        self.heads.insert(head.slot.clone(), head.clone());
                    }
                    Ok(json!({"v":1,"type":"escrow_bootstrapped","id":id}))
                }
                Command::BootstrapBegin { .. } => {
                    if self.migration_finished || (!self.migration_started && !self.is_empty()) {
                        return Err(Error::new("conflict"));
                    }
                    self.migration_started = true;
                    Ok(json!({"v":1,"type":"escrow_bootstrapped","id":id}))
                }
                Command::BootstrapComplete { .. } => {
                    if !self.migration_started {
                        return Err(Error::new("conflict"));
                    }
                    self.migration_finished = true;
                    Ok(json!({"v":1,"type":"escrow_bootstrapped","id":id}))
                }
            }
        });
        result.unwrap_or_else(|error| error.reply(id))
    }

    pub(super) fn put(&mut self, head: &Head, id: &str, max_slots: usize) -> Result<Value, Error> {
        if let Some(deleted) = self.deleted.get(&head.slot) {
            if head.writer != deleted.writer {
                return Err(Error::new("writer_mismatch"));
            }
            if head.seq != deleted.seq.saturating_add(1)
                || head.parent.as_deref() != Some(deleted.hash().as_str())
            {
                let mut error = Error::conflict(deleted);
                error.code = "deleted";
                return Err(error);
            }
            let next_bytes = self.metadata_bytes - head_bytes(deleted) + head_bytes(head);
            if next_bytes + 128 > SNAPSHOT_BYTES {
                return Err(Error::new("quota"));
            }
            self.metadata_bytes = next_bytes;
            self.deleted.remove(&head.slot);
            self.heads.insert(head.slot.clone(), head.clone());
            return Ok(head.ack(id));
        }
        let current = self.heads.get(&head.slot);
        if self.previous.get(&head.slot) == Some(head) || check_cas(head, current)? {
            return Ok(head.ack(id));
        }
        if current.is_none() && self.heads.len() + self.deleted.len() >= max_slots {
            return Err(Error::new("quota"));
        }
        let next_bytes = self.metadata_bytes + head_bytes(head)
            - self.previous.get(&head.slot).map_or(0, head_bytes);
        if next_bytes + 128 > SNAPSHOT_BYTES {
            return Err(Error::new("quota"));
        }
        self.metadata_bytes = next_bytes;
        if let Some(previous) = self.heads.insert(head.slot.clone(), head.clone()) {
            self.previous.insert(head.slot.clone(), previous);
        }
        Ok(head.ack(id))
    }

    pub(super) fn delete(&mut self, request: &Delete, id: &str) -> Result<Value, Error> {
        let current = self
            .heads
            .get(&request.slot)
            .or_else(|| self.deleted.get(&request.slot));
        let Some(current) = current else {
            return Ok(request.reply(id));
        };
        if request.seq != current.seq || request.head != current.hash() {
            return Err(Error::conflict(current));
        }
        request.verify(&current.writer)?;
        if let Some(head) = self.heads.remove(&request.slot) {
            self.deleted.insert(request.slot.clone(), head);
            if let Some(previous) = self.previous.remove(&request.slot) {
                self.metadata_bytes -= head_bytes(&previous);
            }
        }
        Ok(request.reply(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use ed25519_dalek::{Signer, SigningKey};
    use mistlib_core::storage::cid::{MULTICODEC_RAW, compute_cid};

    pub(super) fn fixture(seq: u64, parent: Option<String>, content: &[u8]) -> Head {
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut head = Head {
            v: 1,
            kind: "tc-escrow-head".into(),
            slot: "ab".repeat(16),
            seq,
            parent,
            cid: compute_cid(content, MULTICODEC_RAW),
            size: content.len() as u64,
            sha256: crate::escrow::protocol::digest(content),
            comp: "none".into(),
            schema: "test/1".into(),
            writer: URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
            at: 1,
            sig: String::new(),
        };
        head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
        head
    }

    pub(super) fn put(ticket: &str, head: &Head) -> Command {
        Command::Put {
            ticket: ticket.into(),
            head: head.clone(),
        }
    }

    pub(super) fn delete(head: &Head) -> Command {
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut request = Delete {
            slot: head.slot.clone(),
            seq: head.seq,
            head: head.hash(),
            sig: String::new(),
        };
        request.sig = URL_SAFE_NO_PAD.encode(key.sign(&request.signing_payload()).to_bytes());
        Command::Delete {
            ticket: "delete".into(),
            request,
        }
    }

    #[test]
    fn replay_reconstructs_heads_and_retained_retry_results() {
        let first = fixture(1, None, b"first");
        let second = fixture(2, Some(first.hash()), b"second");
        let commands = [
            put("first", &first),
            put("second", &second),
            put("retry", &first),
            Command::ReadBarrier {
                ticket: "read".into(),
            },
        ];
        let mut a = State::default();
        let mut b = State::default();
        for command in &commands {
            let wire = serde_json::to_vec(command).unwrap();
            let decoded: Command = serde_json::from_slice(&wire).unwrap();
            assert_eq!(
                a.apply(command, command.ticket(), 8),
                b.apply(&decoded, decoded.ticket(), 8)
            );
        }
        assert_eq!(a.head(&first.slot), Some(second.clone()));
        assert_eq!(b.head(&first.slot), Some(second.clone()));
        assert_eq!(a.slots(), 1);
        assert_eq!(a.total_bytes(), first.size + second.size);
        assert_eq!(
            a.apply(&put("retry", &first), "retry", 8),
            first.ack("retry")
        );
    }

    #[test]
    fn committed_order_picks_one_of_concurrent_same_parent_writes() {
        let first = fixture(1, None, b"first");
        let a = fixture(2, Some(first.hash()), b"a");
        let b = fixture(2, Some(first.hash()), b"b");
        let mut state = State::default();
        state.apply(&put("first", &first), "first", 8);
        assert_eq!(state.apply(&put("a", &a), "a", 8)["type"], "escrow_ack");
        let result = state.apply(&put("b", &b), "b", 8);
        assert_eq!(result["code"], "conflict");
        assert_eq!(result["head"], json!(a));
        assert_eq!(state.head(&first.slot), Some(a));
    }

    #[test]
    fn delete_checks_current_writer_and_is_idempotent_without_resurrection() {
        let first = fixture(1, None, b"first");
        let second = fixture(2, Some(first.hash()), b"second");
        let mut state = State::default();
        state.apply(&put("first", &first), "first", 8);
        state.apply(&put("second", &second), "second", 8);
        assert_eq!(state.apply(&delete(&first), "stale", 8)["code"], "conflict");
        let mut forged = delete(&second);
        if let Command::Delete { request, .. } = &mut forged {
            request.sig = URL_SAFE_NO_PAD.encode([0; 64]);
        }
        assert_eq!(state.apply(&forged, "forged", 8)["code"], "bad_signature");
        let command = delete(&second);
        assert_eq!(state.apply(&command, "delete", 8)["type"], "escrow_deleted");
        assert_eq!(state.apply(&command, "repeat", 8)["type"], "escrow_deleted");
        assert!(state.head(&first.slot).is_none());
        assert_eq!(state.total_bytes(), 0);
        assert_eq!(
            state.apply(&put("old", &first), "old", 8)["code"],
            "deleted"
        );
        let fresh = fixture(1, None, b"fresh");
        assert_eq!(
            state.apply(&put("fresh", &fresh), "fresh", 8)["code"],
            "deleted"
        );
        let mut replay = State::default();
        for cmd in [
            put("first", &first),
            put("second", &second),
            command,
            put("old", &first),
        ] {
            replay.apply(&cmd, cmd.ticket(), 8);
        }
        assert!(replay.head(&first.slot).is_none());
    }

    #[test]
    fn invalid_signed_writes_and_tombstone_quota_do_not_mutate_state() {
        let first = fixture(1, None, b"first");
        let mut state = State::default();
        let mut bad = first.clone();
        bad.seq += 1;
        assert_eq!(state.apply(&put("bad", &bad), "bad", 1)["code"], "bad_sig");
        assert!(state.head(&first.slot).is_none());
        state.apply(&put("first", &first), "first", 1);
        state.apply(&delete(&first), "delete", 1);
        let mut another = fixture(1, None, b"another");
        another.slot = "cd".repeat(16);
        let key = SigningKey::from_bytes(&[7; 32]);
        another.sig = URL_SAFE_NO_PAD.encode(key.sign(&another.signing_payload()).to_bytes());
        assert_eq!(
            state.apply(&put("another", &another), "another", 1)["code"],
            "quota"
        );
        assert_eq!(
            validate(
                &Command::ReadBarrier {
                    ticket: "a".repeat(65)
                },
                100
            )
            .unwrap_err()
            .code,
            "bad_request"
        );
    }
}

#[cfg(test)]
#[path = "state_storage_tests.rs"]
mod production_tests;
