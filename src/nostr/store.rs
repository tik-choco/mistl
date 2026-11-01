//! Bounded NIP-01/09/40 event storage with chronological and address indexes.
//!
//! Mutations only update memory. Pending log records share event allocations;
//! serialization and filesystem work happen after releasing the relay lock.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::hash::Hash;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use super::event::{Event, KindClass};
use super::filter::Filter;

const LOG_FILE: &str = "events.jsonl";
type Address = (u32, String, String);

/// Storage and tombstone capacity (each table has this independent cap).
#[derive(Debug, Clone, Copy)]
pub struct StoreLimits {
    /// Maximum stored events, including deletion requests.
    pub max_events: usize,
}

/// Result of applying an already verified event to the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    /// Accepted; capacity eviction may immediately remove it again.
    Stored,
    /// Accepted without storage.
    Ephemeral,
    /// This event id is already stored.
    Duplicate,
    /// A newer version, or a lower id at the same timestamp, is stored.
    Obsolete,
    /// The author previously requested deletion.
    Deleted,
}

/// Bounded in-memory store. Only `open` performs filesystem work.
pub struct Store {
    events: BTreeMap<String, Arc<Event>>,
    by_time: BTreeMap<u64, BTreeSet<String>>,
    addresses: HashMap<Address, String>,
    deleted_ids: Fifo<(String, String), ()>,
    deleted_addresses: Fifo<Address, u64>,
    limits: StoreLimits,
    persistent: bool,
    pending: Vec<Line>,
    log_lines: usize,
    rewrite: bool,
}

struct Fifo<K, V> {
    values: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K: Clone + Eq + Hash, V> Fifo<K, V> {
    fn new() -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn insert(&mut self, key: K, value: V, cap: usize) {
        if let Some(old) = self.values.get_mut(&key) {
            *old = value;
            return;
        }
        if cap == 0 {
            return;
        }
        if self.values.len() >= cap
            && let Some(old) = self.order.pop_front()
        {
            self.values.remove(&old);
        }
        self.order.push_back(key.clone());
        self.values.insert(key, value);
    }
}

enum Line {
    Ev(Arc<Event>),
    Del(String),
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum DiskLine<'a> {
    Ev(&'a Event),
    Del(&'a str),
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ReadLine {
    Ev(Value),
    Del(String),
}

/// Disk work taken out of the store under its lock, applied without it.
pub struct Pending {
    rewrite: bool,
    lines: Vec<Line>,
}

impl Pending {
    /// A rewrite of an empty store still requires applying to disk.
    pub fn is_empty(&self) -> bool {
        !self.rewrite && self.lines.is_empty()
    }

    /// Appends log records, or atomically replaces the log with a snapshot.
    pub fn apply(self, dir: &Path) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(dir).context("creating nostr store directory")?;
        let path = dir.join(LOG_FILE);
        if self.rewrite {
            let mut bytes = Vec::new();
            self.write_to(&mut bytes)?;
            crate::statefile::write_bytes(&path, &bytes)
        } else {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .context("opening nostr event log")?;
            let mut writer = BufWriter::new(file);
            self.write_to(&mut writer)?;
            writer.flush().context("flushing nostr event log")
        }
    }

    fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        for line in &self.lines {
            let line = match line {
                Line::Ev(ev) => DiskLine::Ev(ev),
                Line::Del(id) => DiskLine::Del(id),
            };
            serde_json::to_writer(&mut *writer, &line)?;
            writer.write_all(b"\n")?;
        }
        Ok(())
    }
}

impl Store {
    /// Replays our own log without signature verification. Corrupt lines and
    /// expired events are discarded and repaired by the next flush.
    pub fn open(dir: Option<&Path>, limits: StoreLimits, now: u64) -> Result<Store> {
        let mut store = Self {
            events: BTreeMap::new(),
            by_time: BTreeMap::new(),
            addresses: HashMap::new(),
            deleted_ids: Fifo::new(),
            deleted_addresses: Fifo::new(),
            limits,
            persistent: false,
            pending: Vec::new(),
            log_lines: 0,
            rewrite: false,
        };
        let Some(dir) = dir else { return Ok(store) };
        let file = match std::fs::File::open(dir.join(LOG_FILE)) {
            Ok(file) => Some(file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("opening nostr event log"),
        };
        if let Some(file) = file {
            let mut reader = BufReader::new(file);
            let mut line = Vec::new();
            let mut index = 0usize;
            loop {
                line.clear();
                if reader
                    .read_until(b'\n', &mut line)
                    .context("reading nostr event log")?
                    == 0
                {
                    break;
                }
                index = index.saturating_add(1);
                store.log_lines = store.log_lines.saturating_add(1);
                // A complete JSON value without a final newline is readable,
                // but must be rewritten before a later append can concatenate it.
                if line.last() != Some(&b'\n') {
                    store.rewrite = true;
                }
                let parsed = serde_json::from_slice::<ReadLine>(&line)
                    .map_err(anyhow::Error::from)
                    .and_then(|line| match line {
                        ReadLine::Ev(value) => Ok(Line::Ev(Arc::new(Event::parse(&value)?))),
                        ReadLine::Del(id) => Ok(Line::Del(id)),
                    });
                match parsed {
                    Ok(Line::Ev(ev)) => {
                        if expired(&ev, now) {
                            store.rewrite = true;
                            continue;
                        }
                        let before = store.len();
                        let outcome = store.insert((*ev).clone(), now);
                        if outcome != Insert::Stored || store.len() != before.saturating_add(1) {
                            store.rewrite = true;
                        }
                    }
                    Ok(Line::Del(id)) => {
                        store.remove(&id);
                        store.rewrite = true;
                    }
                    Err(error) => {
                        warn!(line = index, %error, "nostr: skipping corrupt event log line");
                        store.rewrite = true;
                    }
                }
            }
        }
        store.persistent = true;
        Ok(store)
    }

    /// Applies replacement, deletion and capacity rules without disk I/O.
    /// Signature, policy and expiration checks are the caller's responsibility.
    pub fn insert(&mut self, ev: Event, _now: u64) -> Insert {
        if self.contains(&ev.id) {
            return Insert::Duplicate;
        }
        if ev.kind_class() == KindClass::Ephemeral {
            return Insert::Ephemeral;
        }
        // NIP-09 deletion requests cannot themselves be deleted.
        if ev.kind != 5
            && self
                .deleted_ids
                .values
                .contains_key(&(ev.id.clone(), ev.pubkey.clone()))
        {
            return Insert::Deleted;
        }
        let address = address_of(&ev);
        if let Some(address) = &address {
            if self
                .deleted_addresses
                .values
                .get(address)
                .is_some_and(|&time| time >= ev.created_at)
            {
                return Insert::Deleted;
            }
            if let Some(old) = self
                .addresses
                .get(address)
                .and_then(|id| self.events.get(id))
            {
                if old.created_at > ev.created_at
                    || (old.created_at == ev.created_at && old.id < ev.id)
                {
                    return Insert::Obsolete;
                }
                let id = old.id.clone();
                self.remove(&id);
            }
        }
        if ev.kind == 5 {
            self.delete_requested(&ev);
        }
        let ev = Arc::new(ev);
        self.by_time
            .entry(ev.created_at)
            .or_default()
            .insert(ev.id.clone());
        if let Some(address) = address {
            self.addresses.insert(address, ev.id.clone());
        }
        self.events.insert(ev.id.clone(), ev.clone());
        self.record(Line::Ev(ev));
        while self.len() > self.limits.max_events {
            let oldest = self
                .by_time
                .first_key_value()
                .and_then(|(_, ids)| ids.first())
                .cloned();
            let Some(id) = oldest else { break };
            self.remove(&id);
        }
        Insert::Stored
    }

    fn delete_requested(&mut self, ev: &Event) {
        for id in ev.tag_values("e") {
            if !is_hex_key(id) {
                continue;
            }
            if self
                .events
                .get(id)
                .is_some_and(|old| old.pubkey == ev.pubkey && old.kind != 5)
            {
                self.remove(id);
            }
            self.deleted_ids.insert(
                (id.to_string(), ev.pubkey.clone()),
                (),
                self.limits.max_events,
            );
        }
        for value in ev.tag_values("a") {
            let Some(address) = parse_address(value) else {
                continue;
            };
            if address.1 != ev.pubkey {
                continue;
            }
            if let Some(old) = self
                .addresses
                .get(&address)
                .and_then(|id| self.events.get(id))
                && old.created_at <= ev.created_at
            {
                let id = old.id.clone();
                self.remove(&id);
            }
            let time = self
                .deleted_addresses
                .values
                .get(&address)
                .copied()
                .unwrap_or(0)
                .max(ev.created_at);
            self.deleted_addresses
                .insert(address, time, self.limits.max_events);
        }
    }

    fn remove(&mut self, id: &str) {
        let Some(ev) = self.events.remove(id) else {
            return;
        };
        if let Some(ids) = self.by_time.get_mut(&ev.created_at) {
            ids.remove(id);
            if ids.is_empty() {
                self.by_time.remove(&ev.created_at);
            }
        }
        if let Some(address) = address_of(&ev) {
            self.addresses.remove(&address);
        }
        self.record(Line::Del(ev.id.clone()));
    }

    fn record(&mut self, line: Line) {
        if !self.persistent {
            return;
        }
        self.log_lines = self.log_lines.saturating_add(1);
        // Once compaction is needed, only the bounded snapshot matters.
        if self.log_lines > self.compaction_threshold() {
            self.mark_all_dirty();
        } else if !self.rewrite {
            self.pending.push(line);
        }
    }

    fn compaction_threshold(&self) -> usize {
        self.len().saturating_mul(2).saturating_add(1000)
    }

    /// Whether an id is currently stored (including expired events).
    pub fn contains(&self, id: &str) -> bool {
        self.events.contains_key(id)
    }

    /// Union of each filter's newest matches, capped and ordered by descending
    /// timestamp and ascending id. Expiration is evaluated at query time.
    pub fn query(&self, filters: &[Filter], cap: usize, now: u64) -> Vec<Event> {
        let Some(newest) = self.newest_created_at().filter(|_| cap != 0) else {
            return Vec::new();
        };
        let mut found = BTreeMap::new();
        for filter in filters {
            let limit = filter.limit.unwrap_or(cap).min(cap);
            if limit == 0 {
                continue;
            }
            if let Some(ids) = &filter.ids {
                let mut candidates = BTreeMap::new();
                for id in ids {
                    if let Some(ev) = self.events.get(id)
                        && !expired(ev, now)
                        && filter.matches(ev)
                    {
                        candidates.insert((std::cmp::Reverse(ev.created_at), id), ev);
                    }
                }
                for (_, ev) in candidates.into_iter().take(limit) {
                    found.insert((std::cmp::Reverse(ev.created_at), ev.id.clone()), ev);
                }
                continue;
            }
            let since = filter.since.unwrap_or(0);
            let until = filter.until.unwrap_or(newest).min(newest);
            if since > until {
                continue;
            }
            let mut contributed = 0;
            'times: for (_, ids) in self.by_time.range(since..=until).rev() {
                for id in ids {
                    let Some(ev) = self.events.get(id) else {
                        continue;
                    };
                    if !expired(ev, now) && filter.matches(ev) {
                        found.insert((std::cmp::Reverse(ev.created_at), id.clone()), ev);
                        contributed += 1;
                        if contributed == limit {
                            break 'times;
                        }
                    }
                }
            }
        }
        found
            .into_values()
            .take(cap)
            .map(|ev| (**ev).clone())
            .collect()
    }

    /// Distinct matching events, ignoring filter limits and expired events.
    pub fn count(&self, filters: &[Filter], now: u64) -> usize {
        self.events
            .values()
            .filter(|ev| !expired(ev, now) && filters.iter().any(|filter| filter.matches(ev)))
            .count()
    }

    /// Stored events, including those that expired after insertion.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Latest stored timestamp, if any (including expired events).
    pub fn newest_created_at(&self) -> Option<u64> {
        self.by_time.last_key_value().map(|(&time, _)| time)
    }

    /// Moves queued records out, or shares a full snapshot for compaction.
    pub fn take_pending(&mut self) -> Pending {
        if !self.persistent {
            return Pending {
                rewrite: false,
                lines: Vec::new(),
            };
        }
        let rewrite = self.rewrite || self.log_lines > self.compaction_threshold();
        self.rewrite = false;
        let lines = if rewrite {
            self.pending.clear();
            self.log_lines = self.len();
            // Chronological replay avoids temporary eviction of newer entries.
            self.by_time
                .values()
                .flat_map(|ids| ids.iter())
                .filter_map(|id| self.events.get(id).cloned())
                .map(Line::Ev)
                .collect()
        } else {
            std::mem::take(&mut self.pending)
        };
        Pending { rewrite, lines }
    }

    /// Schedules a snapshot, for example after a failed append.
    pub fn mark_all_dirty(&mut self) {
        if self.persistent {
            self.rewrite = true;
            self.pending.clear();
        }
    }
}

fn expired(ev: &Event, now: u64) -> bool {
    ev.expiration().is_some_and(|time| time <= now)
}

fn address_of(ev: &Event) -> Option<Address> {
    match ev.kind_class() {
        KindClass::Replaceable => Some((ev.kind, ev.pubkey.clone(), String::new())),
        KindClass::Addressable => Some((ev.kind, ev.pubkey.clone(), ev.d_tag().to_string())),
        _ => None,
    }
}

fn is_hex_key(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn parse_address(value: &str) -> Option<Address> {
    let mut parts = value.splitn(3, ':');
    let kind = parts.next()?.parse().ok()?;
    let pubkey = parts.next()?;
    let d = parts.next()?;
    if !is_hex_key(pubkey) {
        return None;
    }
    Some((kind, pubkey.to_string(), d.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::{test_event, test_pubkey};
    use serde_json::json;
    use std::path::PathBuf;

    const ALICE: [u8; 32] = [1; 32];
    const BOB: [u8; 32] = [2; 32];

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            Self(
                std::env::temp_dir()
                    .join(format!("mistl-nostr-store-{:016x}", rand::random::<u64>())),
            )
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn memory(cap: usize) -> Store {
        Store::open(None, StoreLimits { max_events: cap }, 0).unwrap()
    }

    fn disk(dir: &TempDir, cap: usize, now: u64) -> Store {
        Store::open(Some(&dir.0), StoreLimits { max_events: cap }, now).unwrap()
    }

    fn event(time: u64, kind: u32, tags: Vec<Vec<String>>, content: &str) -> Event {
        test_event(&ALICE, time, kind, tags, content)
    }

    fn tag(name: &str, value: impl Into<String>) -> Vec<String> {
        vec![name.into(), value.into()]
    }

    fn all(store: &Store, now: u64) -> Vec<Event> {
        store.query(&[Filter::default()], usize::MAX, now)
    }

    #[test]
    fn duplicate_ephemeral_and_memory_only() {
        let mut store = memory(10);
        let ev = event(10, 1, vec![], "one");
        assert_eq!(store.newest_created_at(), None);
        assert_eq!(store.insert(ev.clone(), 10), Insert::Stored);
        assert_eq!(store.insert(ev.clone(), 10), Insert::Duplicate);
        assert!(store.contains(&ev.id));
        assert_eq!(
            store.insert(event(11, 20_000, vec![], "live"), 11),
            Insert::Ephemeral
        );
        assert_eq!(store.len(), 1);
        assert_eq!(store.newest_created_at(), Some(10));
        store.mark_all_dirty();
        assert!(store.take_pending().is_empty());
    }

    #[test]
    fn replaceable_newer_older_and_lowest_id_tie() {
        let mut store = memory(10);
        let old = event(10, 0, vec![], "old");
        let newer = event(11, 0, vec![], "new");
        assert_eq!(store.insert(old.clone(), 11), Insert::Stored);
        assert_eq!(store.insert(newer.clone(), 11), Insert::Stored);
        assert!(!store.contains(&old.id));
        assert_eq!(store.insert(old, 11), Insert::Obsolete);
        let tie = event(11, 0, vec![], "tie");
        let (low, high) = if tie.id < newer.id {
            (tie, newer)
        } else {
            (newer, tie)
        };
        assert_eq!(
            store.insert(high.clone(), 11),
            if store.contains(&high.id) {
                Insert::Duplicate
            } else {
                Insert::Obsolete
            }
        );
        if !store.contains(&low.id) {
            assert_eq!(store.insert(low.clone(), 11), Insert::Stored);
        }
        assert_eq!(store.insert(high, 11), Insert::Obsolete);
        assert_eq!(all(&store, 11), vec![low]);
    }

    #[test]
    fn addressable_versions_are_per_author_kind_and_d_tag() {
        let mut store = memory(10);
        let a = event(10, 30_001, vec![tag("d", "a")], "old");
        let b = event(11, 30_001, vec![tag("d", "b")], "other address");
        let new_a = event(12, 30_001, vec![tag("d", "a")], "new");
        let bob = test_event(&BOB, 13, 30_001, vec![tag("d", "a")], "other author");
        let other_kind = event(14, 30_002, vec![tag("d", "a")], "other kind");
        for ev in [
            a.clone(),
            b.clone(),
            new_a.clone(),
            bob.clone(),
            other_kind.clone(),
        ] {
            assert_eq!(store.insert(ev, 20), Insert::Stored);
        }
        assert_eq!(all(&store, 20), vec![other_kind, bob, new_a, b]);
        assert!(!store.contains(&a.id));
    }

    #[test]
    fn e_deletion_is_author_bound_and_blocks_late_arrivals() {
        let mut store = memory(20);
        let owned = event(10, 1, vec![], "owned");
        let late = event(11, 1, vec![], "late");
        let other = test_event(&BOB, 12, 1, vec![], "other");
        store.insert(owned.clone(), 20);
        store.insert(other.clone(), 20);
        let deletion = event(
            20,
            5,
            vec![tag("e", &owned.id), tag("e", &late.id), tag("e", &other.id)],
            "",
        );
        assert_eq!(store.insert(deletion.clone(), 20), Insert::Stored);
        assert!(!store.contains(&owned.id));
        assert!(store.contains(&other.id));
        assert!(store.contains(&deletion.id));
        assert_eq!(store.insert(owned, 20), Insert::Deleted);
        assert_eq!(store.insert(late, 20), Insert::Deleted);
        // The wrong author's tombstone must not block a later arrival either.
        let bob_late = test_event(&BOB, 13, 1, vec![], "late from bob");
        store.insert(event(21, 5, vec![tag("e", &bob_late.id)], ""), 21);
        assert_eq!(store.insert(bob_late, 21), Insert::Stored);
    }

    #[test]
    fn deletion_requests_cannot_be_deleted_even_before_arrival() {
        let mut store = memory(10);
        let first = event(10, 5, vec![], "first");
        let late = event(11, 5, vec![], "late");
        store.insert(first.clone(), 20);
        store.insert(
            event(20, 5, vec![tag("e", &first.id), tag("e", &late.id)], ""),
            20,
        );
        assert!(store.contains(&first.id));
        assert_eq!(store.insert(late, 20), Insert::Stored);
    }

    #[test]
    fn a_deletion_has_timestamp_author_and_address_scope() {
        let mut store = memory(20);
        let address = format!("30001:{}:entry:with:colons", test_pubkey(&ALICE));
        let old = event(10, 30_001, vec![tag("d", "entry:with:colons")], "old");
        store.insert(old.clone(), 20);
        let wrong_author = test_event(&BOB, 20, 5, vec![tag("a", &address)], "");
        store.insert(wrong_author, 20);
        assert!(store.contains(&old.id));
        store.insert(
            event(20, 5, vec![tag("a", &address), tag("a", "bad address")], ""),
            20,
        );
        assert!(!store.contains(&old.id));
        assert_eq!(store.insert(old, 20), Insert::Deleted);
        assert_eq!(
            store.insert(
                event(20, 30_001, vec![tag("d", "entry:with:colons")], "equal"),
                20
            ),
            Insert::Deleted
        );
        let new = event(21, 30_001, vec![tag("d", "entry:with:colons")], "new");
        assert_eq!(store.insert(new.clone(), 21), Insert::Stored);
        store.insert(event(19, 5, vec![tag("a", &address)], "older deletion"), 21);
        assert!(store.contains(&new.id));
        assert_eq!(
            store.deleted_addresses.values[&parse_address(&address).unwrap()],
            20
        );
        assert_eq!(
            store.insert(event(18, 30_001, vec![tag("d", "other")], "other"), 21),
            Insert::Stored
        );
    }

    #[test]
    fn tombstone_tables_are_bounded_fifo_and_updates_do_not_grow_queues() {
        let mut store = memory(2);
        let targets: Vec<_> = (1..=3)
            .map(|time| event(time, 1, vec![], "target"))
            .collect();
        for (index, target) in targets.iter().enumerate() {
            let address = format!("30001:{}:{index}", target.pubkey);
            store.insert(
                event(
                    10 + index as u64,
                    5,
                    vec![tag("e", &target.id), tag("a", address)],
                    "",
                ),
                20,
            );
        }
        assert_eq!(store.deleted_ids.values.len(), 2);
        assert_eq!(store.deleted_ids.order.len(), 2);
        assert_eq!(store.deleted_addresses.values.len(), 2);
        assert_eq!(store.deleted_addresses.order.len(), 2);
        assert!(
            !store
                .deleted_ids
                .values
                .contains_key(&(targets[0].id.clone(), targets[0].pubkey.clone()))
        );
        assert_eq!(store.insert(targets[2].clone(), 20), Insert::Deleted);
        assert_eq!(store.insert(targets[0].clone(), 20), Insert::Stored);
        let address = format!("30001:{}:2", targets[2].pubkey);
        store.insert(
            event(
                20,
                5,
                vec![tag("e", &targets[2].id), tag("a", address)],
                "update",
            ),
            20,
        );
        assert_eq!(store.deleted_ids.order.len(), 2);
        assert_eq!(store.deleted_addresses.order.len(), 2);
    }

    #[test]
    fn eviction_uses_oldest_timestamp_then_lowest_id_and_cleans_indexes() {
        let mut store = memory(2);
        let mut tied = [event(10, 1, vec![], "a"), event(10, 1, vec![], "b")];
        tied.sort_by(|a, b| a.id.cmp(&b.id));
        for ev in &tied {
            store.insert(ev.clone(), 20);
        }
        let newest = event(20, 0, vec![], "new");
        store.insert(newest.clone(), 20);
        assert!(!store.contains(&tied[0].id));
        assert!(store.contains(&tied[1].id));
        let oldest = event(1, 1, vec![], "immediately evicted");
        assert_eq!(store.insert(oldest.clone(), 20), Insert::Stored);
        assert!(!store.contains(&oldest.id));
        assert_eq!(all(&store, 20), vec![newest, tied[1].clone()]);
        let mut zero = memory(0);
        assert_eq!(zero.insert(event(1, 0, vec![], ""), 1), Insert::Stored);
        assert_eq!(zero.len(), 0);
        assert!(zero.by_time.is_empty());
        assert!(zero.addresses.is_empty());
    }

    #[test]
    fn expired_events_are_hidden_at_boundary() {
        let mut store = memory(10);
        let ev = event(10, 1, vec![tag("expiration", "20")], "expires");
        store.insert(ev.clone(), 10);
        assert_eq!(all(&store, 19), vec![ev]);
        assert!(all(&store, 20).is_empty());
        assert_eq!(store.count(&[Filter::default()], 20), 0);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn query_limits_union_deduplication_and_tie_order() {
        let mut store = memory(20);
        let mut events = vec![
            event(10, 1, vec![], "a"),
            event(20, 1, vec![], "b"),
            event(20, 1, vec![], "c"),
            event(30, 2, vec![], "d"),
        ];
        for ev in &events {
            store.insert(ev.clone(), 30);
        }
        events.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
        assert_eq!(all(&store, 30), events);
        let first = Filter {
            limit: Some(1),
            ..Filter::default()
        };
        let notes = Filter {
            kinds: Some(vec![1]),
            limit: Some(2),
            ..Filter::default()
        };
        assert_eq!(
            store.query(&[first.clone(), notes, first], 10, 30),
            events[..3]
        );
        assert_eq!(store.query(&[Filter::default()], 2, 30), events[..2]);
        assert!(store.query(&[Filter::default()], 0, 30).is_empty());
        assert!(
            store
                .query(
                    &[Filter {
                        limit: Some(0),
                        ..Filter::default()
                    }],
                    10,
                    30
                )
                .is_empty()
        );
        assert!(store.query(&[], 10, 30).is_empty());
    }

    #[test]
    fn query_time_bounds_and_id_lookups_apply_all_conditions() {
        let mut store = memory(10);
        let events: Vec<_> = (1..=4).map(|time| event(time, 1, vec![], "")).collect();
        for ev in &events {
            store.insert(ev.clone(), 4);
        }
        let range = Filter {
            since: Some(2),
            until: Some(3),
            ..Filter::default()
        };
        assert_eq!(
            store.query(&[range], 10, 4),
            vec![events[2].clone(), events[1].clone()]
        );
        assert!(
            store
                .query(
                    &[Filter {
                        since: Some(3),
                        until: Some(2),
                        ..Filter::default()
                    }],
                    10,
                    4
                )
                .is_empty()
        );
        let ids = Filter {
            ids: Some(vec![
                events[0].id.clone(),
                events[2].id.clone(),
                events[3].id.clone(),
            ]),
            until: Some(3),
            limit: Some(1),
            ..Filter::default()
        };
        assert_eq!(store.query(&[ids], 10, 4), vec![events[2].clone()]);
        let repeated = Filter {
            ids: Some(vec![events[2].id.clone(), events[2].id.clone()]),
            ..Filter::default()
        };
        assert_eq!(store.query(&[repeated], 10, 4), vec![events[2].clone()]);
        assert!(
            store
                .query(
                    &[Filter {
                        ids: Some(vec![]),
                        ..Filter::default()
                    }],
                    10,
                    4
                )
                .is_empty()
        );
    }

    #[test]
    fn count_is_distinct_and_ignores_query_limits() {
        let mut store = memory(10);
        for time in 1..=3 {
            store.insert(event(time, 1, vec![], ""), 3);
        }
        let filter = Filter {
            limit: Some(1),
            ..Filter::default()
        };
        assert_eq!(store.query(&[filter.clone()], 10, 3).len(), 1);
        assert_eq!(store.count(&[filter.clone(), filter], 3), 3);
        assert_eq!(store.count(&[], 3), 0);
    }

    #[test]
    fn persistence_round_trip_replacement_deletion_and_append_records() {
        let dir = TempDir::new();
        let mut store = disk(&dir, 20, 0);
        let old = event(1, 0, vec![], "old");
        let note = event(2, 1, vec![], "note");
        store.insert(old.clone(), 10);
        store.insert(note.clone(), 10);
        let pending = store.take_pending();
        assert!(!pending.rewrite);
        assert!(!pending.is_empty());
        // Neither open nor insert creates the directory until apply runs.
        assert!(!dir.0.exists());
        pending.apply(&dir.0).unwrap();
        assert!(store.take_pending().is_empty());
        let new = event(3, 0, vec![], "new");
        store.insert(new.clone(), 10);
        let deletion = event(4, 5, vec![tag("e", &note.id)], "");
        store.insert(deletion.clone(), 10);
        store.take_pending().apply(&dir.0).unwrap();
        let log = std::fs::read_to_string(dir.0.join(LOG_FILE)).unwrap();
        assert!(log.contains(&format!("{{\"del\":\"{}\"}}", old.id)));
        assert!(log.contains(&format!("{{\"del\":\"{}\"}}", note.id)));
        let mut reopened = disk(&dir, 20, 10);
        assert_eq!(all(&reopened, 10), vec![deletion, new]);
        assert_eq!(all(&reopened, 10), all(&store, 10));
        assert_eq!(reopened.insert(note.clone(), 10), Insert::Deleted);
        reopened.take_pending().apply(&dir.0).unwrap();
        let mut compacted = disk(&dir, 20, 10);
        assert_eq!(all(&compacted, 10), all(&store, 10));
        assert_eq!(compacted.insert(note, 10), Insert::Deleted);
        assert!(compacted.take_pending().is_empty());
    }

    #[test]
    fn open_tolerates_corrupt_truncated_and_invalid_event_lines() {
        let dir = TempDir::new();
        std::fs::create_dir_all(&dir.0).unwrap();
        let ev = event(1, 1, vec![], "valid");
        let bytes = format!("garbage\n{}\n{{\"ev\":{{}}}}\n{{\"ev\":", json!({"ev": ev}));
        std::fs::write(dir.0.join(LOG_FILE), bytes).unwrap();
        let mut store = disk(&dir, 10, 2);
        assert_eq!(all(&store, 2), vec![ev]);
        let repair = store.take_pending();
        assert!(repair.rewrite);
        repair.apply(&dir.0).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.0.join(LOG_FILE))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(all(&disk(&dir, 10, 2), 2), all(&store, 2));
    }

    #[test]
    fn open_drops_expiration_and_enforces_smaller_capacity() {
        let dir = TempDir::new();
        let mut store = disk(&dir, 10, 0);
        store.insert(event(1, 1, vec![tag("expiration", "10")], "expires"), 1);
        store.insert(event(2, 1, vec![], "older"), 2);
        let newest = event(3, 1, vec![], "newest");
        store.insert(newest.clone(), 3);
        store.take_pending().apply(&dir.0).unwrap();
        let mut reopened = disk(&dir, 1, 10);
        assert_eq!(all(&reopened, 10), vec![newest]);
        assert!(reopened.take_pending().rewrite);
    }

    #[test]
    fn compaction_and_explicit_dirty_rewrite_only_current_events() {
        let dir = TempDir::new();
        let mut store = disk(&dir, 1, 0);
        // Alternating valid regular events exercises append/removal pressure
        // without spending the test signing a thousand independent events.
        let a = event(1, 1, vec![], "a");
        let b = event(2, 1, vec![], "b");
        for _ in 0..510 {
            store.insert(a.clone(), 2);
            store.insert(b.clone(), 2);
        }
        assert!(store.rewrite);
        assert!(store.pending.is_empty());
        let pending = store.take_pending();
        assert!(pending.rewrite);
        assert_eq!(pending.lines.len(), 1);
        pending.apply(&dir.0).unwrap();
        assert_eq!(store.log_lines, 1);
        assert_eq!(all(&disk(&dir, 1, 2), 2), vec![b]);
        store.mark_all_dirty();
        assert!(store.take_pending().rewrite);
        assert!(store.take_pending().is_empty());
    }

    #[test]
    fn empty_snapshot_replaces_existing_log() {
        let dir = TempDir::new();
        let mut store = disk(&dir, 1, 0);
        store.insert(event(1, 1, vec![], ""), 1);
        store.take_pending().apply(&dir.0).unwrap();
        let mut empty = disk(&dir, 0, 2);
        let pending = empty.take_pending();
        assert!(pending.rewrite);
        assert!(!pending.is_empty());
        pending.apply(&dir.0).unwrap();
        assert_eq!(std::fs::read(dir.0.join(LOG_FILE)).unwrap(), b"");
    }

    #[test]
    fn unterminated_valid_line_is_repaired_before_append() {
        let dir = TempDir::new();
        std::fs::create_dir_all(&dir.0).unwrap();
        let first = event(1, 1, vec![], "first");
        std::fs::write(dir.0.join(LOG_FILE), json!({"ev": first}).to_string()).unwrap();
        let mut store = disk(&dir, 10, 2);
        let second = event(2, 1, vec![], "second");
        store.insert(second.clone(), 2);
        let pending = store.take_pending();
        assert!(pending.rewrite);
        pending.apply(&dir.0).unwrap();
        assert_eq!(all(&disk(&dir, 10, 2), 2), vec![second, first]);
    }

    #[test]
    fn address_deletion_tombstone_survives_snapshot_replay() {
        let dir = TempDir::new();
        let mut store = disk(&dir, 10, 0);
        let old = event(1, 30_001, vec![tag("d", "entry")], "old");
        let address = format!("30001:{}:entry", old.pubkey);
        store.insert(old.clone(), 3);
        store.insert(event(3, 5, vec![tag("a", address)], ""), 3);
        store.mark_all_dirty();
        store.take_pending().apply(&dir.0).unwrap();
        let mut reopened = disk(&dir, 10, 3);
        assert_eq!(reopened.insert(old, 3), Insert::Deleted);
        assert_eq!(
            reopened.insert(event(4, 30_001, vec![tag("d", "entry")], "new"), 4),
            Insert::Stored
        );
    }

    #[test]
    fn detached_snapshot_and_subsequent_append_replay_consistently() {
        let dir = TempDir::new();
        let mut store = disk(&dir, 10, 0);
        store.insert(event(1, 0, vec![], "old"), 3);
        store.mark_all_dirty();
        let snapshot = store.take_pending();
        let new = event(2, 0, vec![], "new");
        store.insert(new.clone(), 3);
        let append = store.take_pending();
        assert!(snapshot.rewrite);
        assert!(!append.rewrite);
        snapshot.apply(&dir.0).unwrap();
        append.apply(&dir.0).unwrap();
        assert_eq!(all(&disk(&dir, 10, 3), 3), vec![new]);
    }

    #[test]
    fn open_parses_shape_without_reverifying_signatures() {
        let dir = TempDir::new();
        std::fs::create_dir_all(&dir.0).unwrap();
        let mut ev = event(1, 1, vec![], "trusted local log");
        ev.sig = "0".repeat(128);
        std::fs::write(dir.0.join(LOG_FILE), format!("{}\n", json!({"ev": ev}))).unwrap();
        assert_eq!(all(&disk(&dir, 10, 2), 2), vec![ev]);
    }
}
