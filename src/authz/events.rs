//! Opt-in usage feed, separate from the cumulative limit meters.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{Action, Authorized, Service};

const CURRENT: &str = "events.jsonl";
const PREVIOUS: &str = "events.1.jsonl";
const ROTATION_BYTES: u64 = 8 * 1024 * 1024;
const RING_CAPACITY: usize = 4096;
const MAX_LINE_BYTES: usize = 16 * 1024;

/// Held by readers for a whole scan and by writers only while rotating, so a
/// query never sees one file renamed between its two reads. Lock order:
/// authz store, then this.
static ROTATION: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Event {
    seq: u64,
    at: String,
    service: Service,
    action: Action,
    #[serde(skip_serializing_if = "Option::is_none")]
    op: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    did: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    caps: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    requests: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_chars: Option<u64>,
}

#[derive(Clone)]
pub(super) struct EventLog {
    last_seq: u64,
    recovered: bool,
    ring: VecDeque<Event>,
    rotation_bytes: u64,
    last_warning: Option<DateTime<Utc>>,
}

impl Default for EventLog {
    fn default() -> Self {
        Self {
            last_seq: 0,
            recovered: true,
            ring: VecDeque::new(),
            rotation_bytes: ROTATION_BYTES,
            last_warning: None,
        }
    }
}

impl EventLog {
    pub(super) fn load(dir: &Path) -> Self {
        let mut log = Self::default();
        if let Err(error) = log.recover(dir) {
            tracing::warn!(%error, "failed to recover usage event log");
        }
        log
    }

    // Scan without retaining the events. Both files matter if a restart
    // occurred after rotation but before the first append to the new file.
    fn recover(&mut self, dir: &Path) -> Result<()> {
        self.recovered = false;
        let previous_last = last_seq(&dir.join(PREVIOUS))?;
        let current_last = last_seq(&dir.join(CURRENT))?;
        self.last_seq = self.last_seq.max(previous_last).max(current_last);
        self.recovered = true;
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        dir: Option<&Path>,
        authorized: &Authorized,
        requests: u64,
        output_chars: u64,
        now: DateTime<Utc>,
    ) {
        let result = (|| -> Result<()> {
            if let Some(dir) = dir
                && !self.recovered
            {
                self.recover(dir)?;
            }
            let seq = self
                .last_seq
                .checked_add(1)
                .context("event sequence exhausted")?;
            let event = Event {
                seq,
                at: now.to_rfc3339_opts(SecondsFormat::Millis, true),
                service: authorized.service,
                action: authorized.action,
                op: authorized.op.map(str::to_owned),
                node: authorized.principal.as_ref().map(|p| p.node.clone()),
                did: authorized.principal.as_ref().and_then(|p| p.did.clone()),
                token: authorized.token.clone(),
                caps: authorized.cap_ids.clone(),
                model: authorized.model.clone(),
                requests,
                // Escrow sizes are billed from stored bytes, not per request.
                output_chars: (authorized.service == Service::Ai).then_some(output_chars),
            };
            if let Some(dir) = dir {
                self.append(dir, &event)?;
            } else {
                if self.ring.len() == RING_CAPACITY {
                    self.ring.pop_front();
                }
                self.ring.push_back(event);
            }
            self.last_seq = seq;
            Ok(())
        })();
        if let Err(error) = result {
            // An ambiguous append or rotation failure must be recovered
            // before assigning another sequence number.
            self.recovered = false;
            if self
                .last_warning
                .is_none_or(|at| now - at >= Duration::minutes(1))
            {
                tracing::warn!(%error, "failed to record usage event");
                self.last_warning = Some(now);
            }
        }
    }

    fn append(&mut self, dir: &Path, event: &Event) -> Result<()> {
        let mut line = serde_json::to_vec(event)?;
        line.push(b'\n');
        ensure!(
            line.len() <= MAX_LINE_BYTES,
            "usage event exceeds line limit"
        );
        let path = dir.join(CURRENT);
        let size = match fs::metadata(&path) {
            Ok(meta) => meta.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        if size > 0 && size.saturating_add(line.len() as u64) > self.rotation_bytes {
            let _io = ROTATION.lock().expect("event rotation lock");
            fs::rename(&path, dir.join(PREVIOUS))?;
        }
        crate::statefile::create_private_dir(dir)?;
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path)?;
        crate::statefile::restrict_existing(&path);
        let size = file.metadata()?.len();
        // A crash may have left a torn last line. Keep the next event on
        // its own line; readers ignore invalid JSON lines.
        if size > 0 {
            file.seek(SeekFrom::End(-1))?;
            let mut last = [0];
            file.read_exact(&mut last)?;
            if last[0] != b'\n' {
                line.insert(0, b'\n');
            }
        }
        if let Err(error) = file.write_all(&line) {
            let _ = file.set_len(size);
            return Err(error.into());
        }
        Ok(())
    }

    /// Captures what a query needs under the store lock; the file scan in
    /// [`Query::run`] happens after the lock is released.
    pub(super) fn plan(&self, dir: Option<&Path>, after: u64, limit: usize) -> Query {
        let ring = || {
            self.ring
                .iter()
                .filter(|e| e.seq > after)
                .take(limit)
                .cloned()
                .collect()
        };
        Query {
            after,
            limit,
            dir: dir.map(Path::to_path_buf),
            ring_oldest: self.ring.front().map(|e| e.seq),
            events: if dir.is_none() { ring() } else { Vec::new() },
        }
    }

    #[cfg(test)]
    pub(super) fn set_rotation_bytes(&mut self, bytes: u64) {
        self.rotation_bytes = bytes;
    }
}

pub(super) struct Query {
    after: u64,
    limit: usize,
    dir: Option<PathBuf>,
    ring_oldest: Option<u64>,
    events: Vec<Event>,
}

impl Query {
    pub(super) fn run(mut self) -> Result<Value> {
        let (after, limit) = (self.after, self.limit);
        let oldest = if let Some(dir) = &self.dir {
            // Rotation waits for this scan, so the two files stay consistent.
            let _io = ROTATION.lock().expect("event rotation lock");
            let previous_first = first_seq(&dir.join(PREVIOUS))?;
            let current_first = first_seq(&dir.join(CURRENT))?;
            // The previous file holds only seqs below the current file's first.
            if current_first.is_none_or(|seq| after.saturating_add(1) < seq) {
                read_page(&dir.join(PREVIOUS), after, limit, &mut self.events)?;
            }
            if self.events.len() < limit {
                read_page(&dir.join(CURRENT), after, limit, &mut self.events)?;
            }
            previous_first.or(current_first)
        } else {
            self.ring_oldest
        };
        let next = self.events.last().map_or(after, |e| e.seq);
        let gap = oldest.is_some_and(|seq| after.saturating_add(1) < seq);
        Ok(json!({"events": self.events, "next": next, "oldest": oldest, "gap": gap}))
    }
}

fn first_seq(path: &Path) -> Result<Option<u64>> {
    let mut first = None;
    scan(path, |event| {
        first = Some(event.seq);
        false
    })?;
    Ok(first)
}

// Visit at most one JSON line at a time, stopping when the visitor has
// enough events. Missing files are normal before the first append/rotation.
fn scan(path: &Path, mut visit: impl FnMut(Event) -> bool) -> Result<()> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    loop {
        line.clear();
        let len = reader
            .by_ref()
            .take(MAX_LINE_BYTES as u64 + 1)
            .read_until(b'\n', &mut line)?;
        if len == 0 {
            break;
        }
        if len > MAX_LINE_BYTES {
            if line.last() != Some(&b'\n') {
                reader.skip_until(b'\n')?;
            }
            continue;
        }
        if let Ok(event) = serde_json::from_slice::<Event>(&line)
            && !visit(event)
        {
            break;
        }
    }
    Ok(())
}

fn last_seq(path: &Path) -> Result<u64> {
    let mut last = 0;
    scan(path, |event| {
        last = last.max(event.seq);
        true
    })?;
    Ok(last)
}

fn read_page(path: &Path, after: u64, limit: usize, events: &mut Vec<Event>) -> Result<()> {
    scan(path, |event| {
        if event.seq > after {
            events.push(event);
        }
        events.len() < limit
    })
}
