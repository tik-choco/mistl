//! Metadata-only records share the standalone repository's durable publication and quotas.
use super::*;
use crate::escrow::mesh::decision::{Acceptance, Entry, acceptance, successor};

#[derive(Clone)]
pub(in crate::escrow) struct MeshRecord {
    pub head: Head,
    pub deleted: Option<Delete>,
    pub home: Option<String>,
    pub held: bool,
}

impl MeshRecord {
    pub fn entry(&self) -> Entry {
        Entry {
            slot: self.head.slot.clone(),
            seq: self.head.seq,
            hash: self.deleted.as_ref().map_or_else(
                || self.head.hash(),
                |d| {
                    digest(
                        crate::wiresign::stable_stringify(&serde_json::to_value(d).unwrap())
                            .as_bytes(),
                    )
                },
            ),
            kind: if self.deleted.is_some() {
                "tombstone"
            } else {
                "head"
            }
            .into(),
            held: self.held && self.deleted.is_none(),
        }
    }
}

pub(super) fn validate_delete(delete: &Delete, head: &Head) -> Result<(), Error> {
    delete.validate()?;
    head.validate(1024 * 1024 * 1024)?;
    head.verify()?;
    if delete.slot != head.slot || delete.seq != head.seq || delete.head != head.hash() {
        return Err(Error::conflict(head));
    }
    delete.verify(&head.writer)
}

impl Repository {
    pub(in crate::escrow) fn mesh_records(&self, room: &str) -> Vec<MeshRecord> {
        let index = self.index.lock().unwrap();
        let mut records: Vec<_> = index
            .records
            .values()
            .filter(|r| r.room == room)
            .map(|r| MeshRecord {
                head: r.current.clone(),
                deleted: r.deleted.clone(),
                home: r.home.clone(),
                held: r.held,
            })
            .collect();
        records.sort_by(|a, b| a.head.slot.cmp(&b.head.slot));
        records
    }

    pub(in crate::escrow) fn free_bytes(&self) -> u64 {
        let index = self.index.lock().unwrap();
        self.cfg
            .max_total_bytes
            .saturating_sub(index.bytes.saturating_add(index.reserved))
    }

    pub(in crate::escrow) fn held(&self, room: &str, head: &Head) -> bool {
        let index = self.index.lock().unwrap();
        index
            .records
            .get(&key(room, &head.slot))
            .is_some_and(|r| r.held && r.deleted.is_none() && r.current == *head)
    }

    /// Caller holds the slot stripe; hashing does not hold the repository index.
    pub(in crate::escrow) fn verify_holding(&self, room: &str, head: &Head) -> Result<(), Error> {
        let snapshot = {
            let index = self.index.lock().unwrap();
            index
                .records
                .get(&key(room, &head.slot))
                .filter(|r| r.deleted.is_none() && r.current == *head)
                .cloned()
        };
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        let corrupt_current = snapshot.held && self.read_generation(room, head).is_err();
        let corrupt_previous = snapshot.previous_held
            && snapshot
                .previous
                .as_ref()
                .is_some_and(|h| self.read_generation(room, h).is_err());
        if !corrupt_current && !corrupt_previous {
            return Ok(());
        }
        let mut index = self.index.lock().unwrap();
        let k = key(room, &head.slot);
        let Some(mut record) = index.records.get(&k).cloned() else {
            return Ok(());
        };
        if record.deleted.is_some()
            || record.current != snapshot.current
            || record.previous != snapshot.previous
            || record.held != snapshot.held
            || record.previous_held != snapshot.previous_held
        {
            return Ok(());
        }
        if corrupt_current {
            record.held = false;
        }
        if corrupt_previous {
            record.previous = None;
            record.previous_held = false;
        }
        self.publish_metadata(&mut index, k, record)?;
        tracing::info!(room, slot=%head.slot, corrupt_current, corrupt_previous, "escrow mesh: corrupt generations removed");
        Ok(())
    }

    fn publish_metadata(&self, index: &mut Index, k: String, record: Record) -> Result<(), Error> {
        if index.write_failed {
            return Err(Error::new("quota"));
        }
        let result = (|| -> Result<()> {
            crate::statefile::write_private(
                &self.root.join("heads").join(format!("{k}.json")),
                &serde_json::to_vec(&record)?,
            )?;
            sync_dir(&self.root.join("heads"))?;
            index.records.insert(k, record);
            self.rebuild_index(index);
            self.cleanup(index)?;
            Ok(())
        })();
        result.map_err(|error| {
            index.write_failed = true;
            tracing::warn!(%error, "escrow mesh: metadata persistence failed");
            Error::new("fetch_failed")
        })
    }

    pub(in crate::escrow) fn set_home(
        &self,
        room: &str,
        head: &Head,
        home: &str,
    ) -> Result<(), Error> {
        crate::identity::pubkey_from_did(home).map_err(|_| Error::new("bad_request"))?;
        let mut index = self.index.lock().unwrap();
        let k = key(room, &head.slot);
        let mut record = index
            .records
            .get(&k)
            .cloned()
            .ok_or_else(|| Error::new("conflict"))?;
        if record.current != *head || record.deleted.is_some() {
            return Err(Error::new("conflict"));
        }
        if record.home.as_deref() == Some(home) {
            return Ok(());
        }
        record.home = Some(home.into());
        self.publish_metadata(&mut index, k, record)
    }

    /// Monotonic metadata admission; does not require local blob capacity.
    pub(in crate::escrow) fn learn(
        &self,
        room: &str,
        head: &Head,
        home: Option<&str>,
        deleted: Option<&Delete>,
    ) -> Result<Acceptance, Error> {
        if !crate::escrow::valid_room(room) {
            return Err(Error::new("bad_request"));
        }
        head.validate(1024 * 1024 * 1024)?;
        head.verify()?;
        if let Some(home) = home {
            crate::identity::pubkey_from_did(home).map_err(|_| Error::new("bad_request"))?;
        }
        if let Some(delete) = deleted {
            validate_delete(delete, head)?;
        }
        let mut index = self.index.lock().unwrap();
        if index.write_failed {
            return Err(Error::new("quota"));
        }
        let k = key(room, &head.slot);
        let old = index.records.get(&k);
        let outcome = acceptance(old.map(|r| &r.current), head);
        if matches!(
            outcome,
            Acceptance::Stale | Acceptance::Conflict | Acceptance::WriterMismatch
        ) {
            return Ok(outcome);
        }
        if let Some(old) = old {
            if old.deleted.is_some() {
                if deleted.is_some() && old.current == *head {
                    return Ok(Acceptance::Same);
                }
                // Higher signed sequences may omit an offline provider's history.
                // A direct recreation still proves the deleted parent explicitly.
                if deleted.is_none() && head.seq <= old.current.seq.saturating_add(1) {
                    successor(head, &old.current)?;
                }
            } else if outcome == Acceptance::Same && deleted.is_none() {
                return Ok(outcome);
            }
        } else if index.records.len() + index.pending_slots.len() >= 65536
            || index.records.values().filter(|r| r.room == room).count()
                + index.pending_slots.values().filter(|r| *r == room).count()
                >= self.cfg.max_slots_per_room
        {
            return Err(Error::new("quota"));
        }
        let previous = old.filter(|r| {
            r.deleted.is_none()
                && deleted.is_none()
                && head.seq == r.current.seq.saturating_add(1)
                && head.parent.as_deref() == Some(r.current.hash().as_str())
        });
        let record = Record {
            room: room.into(),
            current: head.clone(),
            previous: previous.map(|r| r.current.clone()),
            previous_held: previous.is_some_and(|r| r.held),
            home: home
                .map(str::to_owned)
                .or_else(|| old.and_then(|r| r.home.clone())),
            deleted: deleted.cloned(),
            held: false,
        };
        self.publish_metadata(&mut index, k, record)?;
        Ok(Acceptance::New)
    }

    pub(in crate::escrow) fn mesh_delete(&self, room: &str, delete: &Delete) -> Result<(), Error> {
        let record = self
            .mesh_records(room)
            .into_iter()
            .find(|r| r.head.slot == delete.slot);
        let Some(record) = record else {
            return Ok(());
        };
        validate_delete(delete, &record.head)?;
        self.learn(room, &record.head, record.home.as_deref(), Some(delete))?;
        Ok(())
    }

    pub(in crate::escrow) fn drop_holding(&self, room: &str, head: &Head) -> Result<(), Error> {
        let mut index = self.index.lock().unwrap();
        let k = key(room, &head.slot);
        let Some(mut record) = index.records.get(&k).cloned() else {
            return Ok(());
        };
        if record.current != *head || !record.held {
            return Ok(());
        }
        record.held = false;
        record.previous_held = false;
        self.publish_metadata(&mut index, k, record)
    }
}
