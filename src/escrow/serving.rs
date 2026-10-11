//! Bounded storage serving: one active request per transport peer, round-robin across peers.
use super::Service;
use anyhow::Result;
use mistlib_core::storage::protocol::{
    MSG_QUERY, build_have_payload, build_have_status_message, chunk_size_for_limit,
    have_chunk_count, parse_query_message, parse_want_message,
};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;

pub(super) const MAX_SERVING: usize = 4;
pub(super) const MAX_REQUESTS: usize = 256;
pub(super) const MAX_PEER_REQUESTS: usize = 64;
// Discovery must leave room for WANTs whose clients already found this provider.
const MAX_QUERIES: usize = MAX_REQUESTS - 64;
const MAX_PEER_QUERIES: usize = MAX_PEER_REQUESTS - 16;
// net::send_direct only acknowledges app-queue admission. No downstream capacity
// or delivery ACK is exposed. One frame per 10 ms limits a peer to 100 frames/s;
// keep the delay after the last frame too, so consecutive WANTs cannot burst.
pub(super) const FRAME_SPACING: Duration = Duration::from_millis(10);
const SEND_DEADLINE: Duration = Duration::from_secs(5);

type SendFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub(super) type Sender = Arc<dyn Fn(String, String, Vec<u8>) -> SendFuture + Send + Sync>;

pub(super) struct Request {
    pub room: String,
    pub from: String,
    pub cid: String,
    pub query: bool,
}

#[derive(Default)]
struct Peer {
    pending: VecDeque<Request>,
    active: bool,
}

#[derive(Default)]
pub(super) struct Queue {
    peers: HashMap<String, Peer>,
    ready: VecDeque<String>,
    count: usize,
    active: usize,
}

impl Queue {
    // Counts include active requests, so neither the task nor peer map can grow beyond the cap.
    pub fn push(&mut self, request: Request) -> bool {
        let peer_count = self
            .peers
            .get(&request.from)
            .map_or(0, |peer| peer.pending.len() + usize::from(peer.active));
        let (global_cap, peer_cap) = if request.query {
            (MAX_QUERIES, MAX_PEER_QUERIES)
        } else {
            (MAX_REQUESTS, MAX_PEER_REQUESTS)
        };
        if self.count >= global_cap || peer_count >= peer_cap {
            // No storage negative-reply frame exists: a refused QUERY advertises nothing.
            // Never evict accepted WANTs to accommodate new discovery traffic.
            return false;
        }
        let peer = self.peers.entry(request.from.clone()).or_default();
        if !peer.active && peer.pending.is_empty() {
            self.ready.push_back(request.from.clone());
        }
        peer.pending.push_back(request);
        self.count += 1;
        true
    }

    pub fn next(&mut self) -> Option<Request> {
        if self.active >= MAX_SERVING {
            return None;
        }
        let from = self.ready.pop_front()?;
        let peer = self.peers.get_mut(&from).unwrap();
        peer.active = true;
        self.active += 1;
        peer.pending.pop_front()
    }

    pub fn finish(&mut self, from: &str) {
        let peer = self.peers.get_mut(from).unwrap();
        peer.active = false;
        self.active -= 1;
        self.count -= 1;
        if peer.pending.is_empty() {
            self.peers.remove(from);
        } else {
            self.ready.push_back(from.into());
        }
    }
}

pub(super) struct Serving {
    pub queue: Mutex<Queue>,
    pub send: Sender,
}

impl Default for Serving {
    fn default() -> Self {
        Self {
            queue: Mutex::new(Queue::default()),
            send: Arc::new(|room, from, bytes| {
                Box::pin(async move { crate::net::send_direct(&room, &from, bytes).await })
            }),
        }
    }
}

impl Service {
    /// Returns true for a storage request, including an over-cap request that cannot be queued.
    pub(super) fn enqueue_storage(
        self: &Arc<Self>,
        rt: &Handle,
        room: &str,
        from: &str,
        raw: &[u8],
    ) -> bool {
        if raw.len() > 129 {
            return false;
        }
        let cid = parse_want_message(raw).or_else(|| parse_query_message(raw));
        let Some(cid) = cid.filter(|cid| mistlib_core::storage::is_valid_cid(cid)) else {
            return false;
        };
        let accepted = self.serving.queue.lock().unwrap().push(Request {
            room: room.into(),
            from: from.into(),
            cid,
            query: raw[0] == MSG_QUERY,
        });
        if accepted {
            self.drain_storage(rt);
        }
        true
    }

    fn drain_storage(self: &Arc<Self>, rt: &Handle) {
        loop {
            let Some(request) = self.serving.queue.lock().unwrap().next() else {
                return;
            };
            let service = self.clone();
            let rt = rt.clone();
            rt.clone().spawn(async move {
                service.serve_storage(&request).await;
                service.serving.queue.lock().unwrap().finish(&request.from);
                service.drain_storage(&rt);
            });
        }
    }

    async fn send_storage_frame(&self, request: &Request, bytes: Vec<u8>) -> Result<()> {
        let deadline = tokio::time::Instant::now() + SEND_DEADLINE;
        loop {
            let result = tokio::time::timeout_at(
                deadline,
                (self.serving.send)(request.room.clone(), request.from.clone(), bytes.clone()),
            )
            .await;
            // The queue admits just one active request per peer, across rooms. Keeping
            // its worker occupied through this sleep also paces the next queued WANT.
            tokio::time::sleep(FRAME_SPACING).await;
            match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(_)) if tokio::time::Instant::now() < deadline => continue,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "escrow: storage enqueue failed after retries");
                    return Err(error);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn serve_storage(&self, request: &Request) {
        // Read the block only after admission to the four active tasks, never while queuing.
        let Some(data) = self.repo.block(&request.room, &request.cid) else {
            return;
        };
        if request.query {
            let _ = self
                .send_storage_frame(request, build_have_status_message(&request.cid))
                .await;
            return;
        }
        // The app API does not expose the negotiated limit. Apply the core helper to the
        // conservative browser ceiling explicitly: None would omit its envelope margin.
        let chunk_size = chunk_size_for_limit(&request.cid, Some(16 * 1024));
        let Some(total) = have_chunk_count(data.len(), chunk_size) else {
            return;
        };
        for i in 0..total {
            if self
                .send_storage_frame(
                    request,
                    build_have_payload(&request.cid, &data, i, total, chunk_size),
                )
                .await
                .is_err()
            {
                break;
            }
        }
    }
}
