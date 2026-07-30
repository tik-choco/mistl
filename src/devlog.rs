//! In-memory ring buffer of recent log lines, for the dashboard's developer
//! console (`logs.tail`/`logs.clear`). A dedicated `tracing` layer captures at
//! debug level regardless of the stderr/file log's own verbosity (default
//! `mistl=info`), so flipping on "developer mode" in the dashboard shows
//! detail immediately -- no daemon restart, no RUST_LOG change required.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

const CAPACITY: usize = 1000;

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub id: u64,
    pub ts_ms: i64,
    pub level: &'static str,
    pub target: String,
    pub message: String,
}

struct RingBuffer {
    entries: Mutex<VecDeque<LogEntry>>,
    next_id: AtomicU64,
}

impl RingBuffer {
    fn push(&self, level: &'static str, target: String, message: String) {
        let entry = LogEntry {
            id: self.next_id.fetch_add(1, Ordering::SeqCst),
            ts_ms: chrono::Utc::now().timestamp_millis(),
            level,
            target,
            message,
        };
        let mut entries = self.entries.lock().expect("devlog buffer lock poisoned");
        entries.push_back(entry);
        if entries.len() > CAPACITY {
            entries.pop_front();
        }
    }

    fn tail(&self, since: u64) -> Vec<LogEntry> {
        let entries = self.entries.lock().expect("devlog buffer lock poisoned");
        entries.iter().filter(|e| e.id >= since).cloned().collect()
    }

    fn clear(&self) {
        self.entries
            .lock()
            .expect("devlog buffer lock poisoned")
            .clear();
    }
}

static BUFFER: OnceLock<RingBuffer> = OnceLock::new();

fn buffer() -> &'static RingBuffer {
    BUFFER.get_or_init(|| RingBuffer {
        entries: Mutex::new(VecDeque::with_capacity(CAPACITY)),
        next_id: AtomicU64::new(1),
    })
}

/// Entries with `id >= since` (pass 0 for the full buffer), oldest first.
pub fn tail(since: u64) -> Vec<LogEntry> {
    buffer().tail(since)
}

pub fn clear() {
    buffer().clear();
}

/// Extracts just the formatted `message` field text (mirrors what the default
/// `fmt` layer shows as the event's message), ignoring other structured
/// fields -- enough detail for a human skimming the dashboard, without
/// reimplementing a full formatter.
#[derive(Default)]
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// `tracing_subscriber::Layer` that mirrors every event into the ring buffer.
/// Composed with its own `EnvFilter` (see `layer()`) so it can capture more
/// detail than the stderr/file log without touching that log's verbosity.
pub struct DevLogLayer;

impl<S> Layer<S> for DevLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        buffer().push(
            event.metadata().level().as_str(),
            event.metadata().target().to_string(),
            visitor.0,
        );
    }
}
