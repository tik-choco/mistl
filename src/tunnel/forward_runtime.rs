//! Per-forward traffic counters (active connection count, bytes in/out) and
//! a shutdown-broadcast `watch` channel used to cancel a forward's listener
//! task. Ported verbatim from `p2p/src/forward_runtime.rs` -- this module
//! has no `crate::X` imports to rewrite and no storage-path seam.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardMetrics {
    pub active_conns: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

/// Per-peer breakdown of a single forward's traffic, surfaced to the TUI for
/// the expanded connection-details view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerMetrics {
    pub peer_id: String,
    pub active_conns: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl PeerMetrics {
    /// `{"peer_id": "..", "active_conns": 0, "bytes_in": 0, "bytes_out": 0}`.
    /// A hand-written method (not a `Serialize` derive) to match
    /// [`crate::tunnel::controller::ForwardStatus::to_json`], which embeds
    /// a `Vec<PeerMetrics>` and calls this per entry.
    pub fn to_json(&self) -> Value {
        json!({
            "peer_id": self.peer_id,
            "active_conns": self.active_conns,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        })
    }
}

#[derive(Debug, Default)]
struct PeerCounters {
    active_conns: AtomicUsize,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

#[derive(Debug, Default)]
struct ForwardCounters {
    active_conns: AtomicUsize,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    per_peer: Mutex<HashMap<String, Arc<PeerCounters>>>,
}

#[derive(Debug, Clone)]
pub struct ForwardRuntime {
    counters: Arc<ForwardCounters>,
    shutdown_tx: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
pub struct ForwardPeerRuntime {
    counters: Arc<ForwardCounters>,
    peer: Arc<PeerCounters>,
}

impl ForwardRuntime {
    pub fn new() -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            counters: Arc::new(ForwardCounters::default()),
            shutdown_tx,
        }
    }

    pub fn metrics(&self) -> ForwardMetrics {
        ForwardMetrics {
            active_conns: self.counters.active_conns.load(Ordering::Relaxed),
            bytes_in: self.counters.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.counters.bytes_out.load(Ordering::Relaxed),
        }
    }

    /// Per-peer metrics, sorted by peer id. Entries persist after a peer's
    /// connections close so cumulative byte totals remain visible.
    pub fn peer_metrics(&self) -> Vec<PeerMetrics> {
        let per_peer = self.counters.per_peer.lock().unwrap();
        let mut metrics = per_peer
            .iter()
            .map(|(peer_id, c)| PeerMetrics {
                peer_id: peer_id.clone(),
                active_conns: c.active_conns.load(Ordering::Relaxed),
                bytes_in: c.bytes_in.load(Ordering::Relaxed),
                bytes_out: c.bytes_out.load(Ordering::Relaxed),
            })
            .collect::<Vec<_>>();
        metrics.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        metrics
    }

    pub fn peer(&self, peer_id: &str) -> ForwardPeerRuntime {
        let mut per_peer = self.counters.per_peer.lock().unwrap();
        let peer = per_peer
            .entry(peer_id.to_string())
            .or_insert_with(|| Arc::new(PeerCounters::default()))
            .clone();
        ForwardPeerRuntime {
            counters: self.counters.clone(),
            peer,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub fn cancel(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    #[allow(dead_code)]
    pub fn record_conn_open(&self) {
        self.counters.active_conns.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_conn_close(&self) {
        let _ = self.counters.active_conns.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| current.checked_sub(1),
        );
    }

    #[allow(dead_code)]
    pub fn record_bytes_in(&self, bytes: usize) {
        self.counters
            .bytes_in
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_bytes_out(&self, bytes: usize) {
        self.counters
            .bytes_out
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn record_conn_open_for(&self, peer_id: &str) {
        self.peer(peer_id).record_conn_open();
    }

    #[allow(dead_code)]
    pub fn record_conn_close_for(&self, peer_id: &str) {
        self.peer(peer_id).record_conn_close();
    }

    #[allow(dead_code)]
    pub fn record_bytes_in_for(&self, peer_id: &str, bytes: usize) {
        self.peer(peer_id).record_bytes_in(bytes);
    }

    #[allow(dead_code)]
    pub fn record_bytes_out_for(&self, peer_id: &str, bytes: usize) {
        self.peer(peer_id).record_bytes_out(bytes);
    }
}

impl ForwardPeerRuntime {
    pub fn record_conn_open(&self) {
        self.counters.active_conns.fetch_add(1, Ordering::Relaxed);
        self.peer.active_conns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_conn_close(&self) {
        let _ = self.counters.active_conns.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| current.checked_sub(1),
        );
        let _ =
            self.peer
                .active_conns
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_sub(1)
                });
    }

    pub fn record_bytes_in(&self, bytes: usize) {
        let bytes = bytes as u64;
        self.counters.bytes_in.fetch_add(bytes, Ordering::Relaxed);
        self.peer.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_bytes_out(&self, bytes: usize) {
        let bytes = bytes as u64;
        self.counters.bytes_out.fetch_add(bytes, Ordering::Relaxed);
        self.peer.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl Default for ForwardRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_start_at_zero() {
        let runtime = ForwardRuntime::new();

        assert_eq!(
            runtime.metrics(),
            ForwardMetrics {
                active_conns: 0,
                bytes_in: 0,
                bytes_out: 0
            }
        );
    }

    #[test]
    fn records_connections_and_bytes() {
        let runtime = ForwardRuntime::new();

        runtime.record_conn_open();
        runtime.record_conn_open();
        runtime.record_conn_close();
        runtime.record_bytes_in(10);
        runtime.record_bytes_in(5);
        runtime.record_bytes_out(7);

        assert_eq!(
            runtime.metrics(),
            ForwardMetrics {
                active_conns: 1,
                bytes_in: 15,
                bytes_out: 7
            }
        );
    }

    #[test]
    fn per_peer_metrics_are_tracked_and_aggregated() {
        let runtime = ForwardRuntime::new();

        runtime.record_conn_open_for("peer-b");
        runtime.record_bytes_in_for("peer-b", 100);
        runtime.record_conn_open_for("peer-a");
        runtime.record_bytes_out_for("peer-a", 50);
        runtime.record_conn_close_for("peer-b");

        // Aggregate reflects the sum across peers.
        assert_eq!(
            runtime.metrics(),
            ForwardMetrics {
                active_conns: 1,
                bytes_in: 100,
                bytes_out: 50,
            }
        );

        // Per-peer breakdown is sorted by peer id and persists byte totals.
        assert_eq!(
            runtime.peer_metrics(),
            vec![
                PeerMetrics {
                    peer_id: "peer-a".to_string(),
                    active_conns: 1,
                    bytes_in: 0,
                    bytes_out: 50,
                },
                PeerMetrics {
                    peer_id: "peer-b".to_string(),
                    active_conns: 0,
                    bytes_in: 100,
                    bytes_out: 0,
                },
            ]
        );
    }

    #[test]
    fn close_connection_saturates_at_zero() {
        let runtime = ForwardRuntime::new();

        runtime.record_conn_close();

        assert_eq!(runtime.metrics().active_conns, 0);
    }

    #[tokio::test]
    async fn cancel_notifies_subscribers() {
        let runtime = ForwardRuntime::new();
        let mut shutdown = runtime.subscribe();

        runtime.cancel();
        shutdown.changed().await.unwrap();

        assert!(*shutdown.borrow());
        assert!(runtime.is_cancelled());
    }
}
