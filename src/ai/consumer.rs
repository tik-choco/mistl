//! Consumer side of the AI network, ported from mistai's `consumer.ts` +
//! `client.ts` discovery behavior.
//!
//! ## Discovery (first provider wins)
//!
//! The service broadcasts `consumer_hello` after joining; providers answer
//! with `provider_hello`. The first `provider_hello` sender becomes the
//! locked-in provider; later hellos from the *same* id refresh its model
//! list, hellos from other ids are ignored. When the locked-in provider
//! disconnects ([`Consumer::on_peer_disconnected`]), every in-flight
//! request is rejected and the lock is cleared so a new provider can be
//! discovered.
//!
//! ## Request lifecycle (mirrors consumer.ts)
//!
//! [`Consumer::request`] generates `id = protocol::random_id()`, registers
//! a pending entry, sends `llm_request` (include `model` only if `Some`),
//! then consumes events until done/error:
//!
//! - Chunks WITHOUT `seq` (legacy senders): apply immediately in arrival
//!   order (append delta to content, forward to `delta_tx`) -- no
//!   buffering.
//! - Chunks WITH `seq`: `seq < next_seq` -> drop (stale duplicate);
//!   `seq > next_seq` -> buffer in a map; `seq == next_seq` -> apply,
//!   `next_seq += 1`, then drain now-contiguous buffered entries.
//! - `llm_response_done`: final content = the message's `content` if
//!   `Some` (server copy wins), else the accumulated deltas. Resolve.
//! - `llm_error`: reject with the remote message.
//! - Timeout is an INACTIVITY timeout: it resets on every received chunk,
//!   not a total deadline. On expiry, reject with a timeout error.
//!
//! Implementation shape: `handle_message` is synchronous (called from a
//! spawned dispatch task) and pushes events into the pending request's
//! `tokio::sync::mpsc` unbounded channel keyed by `id`; the `request()`
//! future owns reordering/accumulation and applies the inactivity timeout
//! as `tokio::time::timeout` around each channel `recv()`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, watch};

use super::SendFn;
use super::protocol::{self, ChatMessage, ProtocolMessage};

/// The provider this consumer has locked onto.
#[derive(Debug, Clone)]
pub struct ProviderInfo {
    pub node_id: String,
    /// Models from the provider's latest `provider_hello` (may be empty).
    pub models: Vec<String>,
}

/// Internal events routed to an in-flight [`Consumer::request`] call.
#[derive(Debug, Clone)]
enum Event {
    Chunk { delta: String, seq: Option<u64> },
    Done { content: Option<String> },
    Error { message: String },
    Rejected { reason: String },
}

/// Consumer state: provider lock + in-flight requests.
pub struct Consumer {
    send: SendFn,
    pending: Mutex<HashMap<String, mpsc::UnboundedSender<Event>>>,
    /// Locked-in provider, `None` until the first `provider_hello`.
    /// A `watch` channel lets `wait_for_provider` await lock-in without
    /// polling.
    provider: watch::Sender<Option<ProviderInfo>>,
}

impl Consumer {
    pub fn new(send: SendFn) -> std::sync::Arc<Self> {
        let (provider, _rx) = watch::channel(None);
        std::sync::Arc::new(Self {
            send,
            pending: Mutex::new(HashMap::new()),
            provider,
        })
    }

    /// Feed one decoded inbound message. Only `provider_hello`,
    /// `llm_response_chunk`, `llm_response_done`, and `llm_error` are
    /// meaningful; everything else is ignored. On first `provider_hello`
    /// (lock-in), reply `consumer_hello` directly to that provider via
    /// `self.send` (mistai does this to let the provider classify us).
    pub fn handle_message(&self, from: &str, msg: &ProtocolMessage) {
        match msg {
            ProtocolMessage::ProviderHello { models } => {
                let mut locked_in = false;
                self.provider.send_if_modified(|current| match current {
                    None => {
                        *current = Some(ProviderInfo {
                            node_id: from.to_string(),
                            models: models.clone().unwrap_or_default(),
                        });
                        locked_in = true;
                        true
                    }
                    Some(info) if info.node_id == from => {
                        info.models = models.clone().unwrap_or_default();
                        true
                    }
                    Some(_) => false,
                });
                if locked_in {
                    (self.send)(from, ProtocolMessage::ConsumerHello);
                }
            }
            ProtocolMessage::LlmResponseChunk { id, delta, seq } => {
                self.send_event(
                    id,
                    Event::Chunk {
                        delta: delta.clone(),
                        seq: *seq,
                    },
                );
            }
            ProtocolMessage::LlmResponseDone { id, content } => {
                self.send_event(
                    id,
                    Event::Done {
                        content: content.clone(),
                    },
                );
            }
            ProtocolMessage::LlmError { id, message } => {
                self.send_event(
                    id,
                    Event::Error {
                        message: message.clone(),
                    },
                );
            }
            _ => {}
        }
    }

    fn send_event(&self, id: &str, event: Event) {
        let pending = self.pending.lock().expect("consumer pending lock");
        if let Some(tx) = pending.get(id) {
            let _ = tx.send(event);
        }
    }

    /// Peer left: if it was the locked-in provider, reject all in-flight
    /// requests ("provider disconnected") and clear the lock.
    pub fn on_peer_disconnected(&self, node_id: &str) {
        let mut cleared = false;
        self.provider.send_if_modified(|current| {
            if current.as_ref().map(|info| info.node_id.as_str()) == Some(node_id) {
                *current = None;
                cleared = true;
                true
            } else {
                false
            }
        });
        if cleared {
            self.reject_all("provider disconnected");
        }
    }

    /// The currently locked-in provider, if any.
    pub fn provider(&self) -> Option<ProviderInfo> {
        self.provider.borrow().clone()
    }

    /// Wait until a provider is locked in (or `timeout` elapses -> error
    /// "no provider found"). Returns immediately when already locked.
    pub async fn wait_for_provider(&self, timeout: Duration) -> Result<ProviderInfo> {
        let mut rx = self.provider.subscribe();
        let wait = async {
            loop {
                if let Some(info) = rx.borrow().clone() {
                    return info;
                }
                if rx.changed().await.is_err() {
                    // Sender dropped; Consumer is gone. Park forever so the
                    // outer timeout fires instead of returning a bogus value.
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| anyhow::anyhow!("no provider found"))
    }

    /// Run one chat request against `provider_id` per the module doc.
    /// `inactivity_timeout` resets on every chunk.
    pub async fn request(
        &self,
        provider_id: &str,
        messages: Vec<ChatMessage>,
        model: Option<String>,
        inactivity_timeout: Duration,
        delta_tx: Option<UnboundedSender<String>>,
    ) -> Result<String> {
        let id = protocol::random_id();
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        self.pending
            .lock()
            .expect("consumer pending lock")
            .insert(id.clone(), tx);

        // Always remove the pending entry on every exit path.
        struct RemoveOnDrop<'a> {
            consumer: &'a Consumer,
            id: &'a str,
        }
        impl Drop for RemoveOnDrop<'_> {
            fn drop(&mut self) {
                self.consumer
                    .pending
                    .lock()
                    .expect("consumer pending lock")
                    .remove(self.id);
            }
        }
        let _guard = RemoveOnDrop {
            consumer: self,
            id: &id,
        };

        (self.send)(
            provider_id,
            ProtocolMessage::LlmRequest {
                id: id.clone(),
                messages,
                model,
            },
        );

        let mut content = String::new();
        let mut next_seq: u64 = 0;
        let mut buffered: BTreeMap<u64, String> = BTreeMap::new();

        loop {
            let event = match tokio::time::timeout(inactivity_timeout, rx.recv()).await {
                Ok(Some(event)) => event,
                Ok(None) => bail!("request channel closed unexpectedly"),
                Err(_) => bail!("request timed out"),
            };

            match event {
                Event::Chunk { delta, seq } => match seq {
                    None => {
                        content.push_str(&delta);
                        if let Some(tx) = &delta_tx {
                            let _ = tx.send(delta);
                        }
                    }
                    Some(seq) if seq < next_seq => {
                        // Stale duplicate; drop.
                    }
                    Some(seq) if seq > next_seq => {
                        buffered.insert(seq, delta);
                    }
                    Some(_) => {
                        content.push_str(&delta);
                        if let Some(tx) = &delta_tx {
                            let _ = tx.send(delta);
                        }
                        next_seq += 1;
                        while let Some(next) = buffered.remove(&next_seq) {
                            content.push_str(&next);
                            if let Some(tx) = &delta_tx {
                                let _ = tx.send(next);
                            }
                            next_seq += 1;
                        }
                    }
                },
                Event::Done { content: final_content } => {
                    return Ok(final_content.unwrap_or(content));
                }
                Event::Error { message } => {
                    bail!(message);
                }
                Event::Rejected { reason } => {
                    bail!(reason);
                }
            }
        }
    }

    /// Reject every in-flight request with `reason`.
    pub fn reject_all(&self, reason: &str) {
        let senders: Vec<_> = {
            let pending = self.pending.lock().expect("consumer pending lock");
            pending.values().cloned().collect()
        };
        for tx in senders {
            let _ = tx.send(Event::Rejected {
                reason: reason.to_string(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::sleep;

    type Sent = Arc<Mutex<Vec<(String, ProtocolMessage)>>>;

    fn fake_send() -> (SendFn, Sent) {
        let sent: Sent = Arc::new(Mutex::new(Vec::new()));
        let sent2 = sent.clone();
        let send: SendFn = Arc::new(move |to: &str, msg: ProtocolMessage| {
            sent2.lock().unwrap().push((to.to_string(), msg));
        });
        (send, sent)
    }

    fn last_request_id(sent: &Sent) -> String {
        let sent = sent.lock().unwrap();
        for (_, msg) in sent.iter().rev() {
            if let ProtocolMessage::LlmRequest { id, .. } = msg {
                return id.clone();
            }
        }
        panic!("no llm_request found in sent messages");
    }

    fn chat(content: &str) -> ChatMessage {
        ChatMessage {
            role: "user".into(),
            content: content.into(),
        }
    }

    #[tokio::test]
    async fn seq_reorder_delivers_in_order() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let (delta_tx, mut delta_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(500),
                Some(delta_tx),
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "b".into(),
                seq: Some(1),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "a".into(),
                seq: Some(0),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "c".into(),
                seq: Some(2),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id: id.clone(),
                content: None,
            },
        );

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, "abc");

        let mut deltas = Vec::new();
        while let Ok(d) = delta_rx.try_recv() {
            deltas.push(d);
        }
        assert_eq!(deltas, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn stale_duplicate_is_dropped() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_millis(500), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "a".into(),
                seq: Some(0),
            },
        );
        // Stale duplicate of seq 0, should be dropped.
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "X".into(),
                seq: Some(0),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "b".into(),
                seq: Some(1),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id: id.clone(),
                content: None,
            },
        );

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, "ab");
    }

    #[tokio::test]
    async fn legacy_no_seq_arrival_order() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_millis(500), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        for d in ["a", "b", "c"] {
            consumer.handle_message(
                "provider1",
                &ProtocolMessage::LlmResponseChunk {
                    id: id.clone(),
                    delta: d.into(),
                    seq: None,
                },
            );
        }
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id: id.clone(),
                content: None,
            },
        );

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, "abc");
    }

    #[tokio::test]
    async fn done_content_overrides_accumulated() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_millis(500), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "partial".into(),
                seq: Some(0),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id: id.clone(),
                content: Some("server copy wins".into()),
            },
        );

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, "server copy wins");
    }

    #[tokio::test]
    async fn done_falls_back_to_accumulated_when_no_content() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_millis(500), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "accumulated".into(),
                seq: Some(0),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id: id.clone(),
                content: None,
            },
        );

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, "accumulated");
    }

    #[tokio::test]
    async fn inactivity_timeout_fires_with_no_activity() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        let result = consumer
            .request("provider1", vec![chat("hi")], None, Duration::from_millis(60), None)
            .await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("timed out"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn inactivity_timeout_resets_on_chunk_activity() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_millis(150), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        // Each gap is well under the 150ms inactivity timeout, so chunk
        // activity should keep resetting the timer.
        sleep(Duration::from_millis(60)).await;
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "a".into(),
                seq: Some(0),
            },
        );
        sleep(Duration::from_millis(60)).await;
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "b".into(),
                seq: Some(1),
            },
        );
        sleep(Duration::from_millis(60)).await;
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id: id.clone(),
                content: None,
            },
        );

        let result = handle.await.unwrap().unwrap();
        assert_eq!(result, "ab");
    }

    #[tokio::test]
    async fn inactivity_timeout_fires_despite_earlier_activity() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_millis(100), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseChunk {
                id: id.clone(),
                delta: "a".into(),
                seq: Some(0),
            },
        );
        // No further activity for longer than the inactivity timeout.
        let result = handle.await.unwrap();
        let err = result.unwrap_err();
        assert!(err.to_string().contains("timed out"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn llm_error_rejects_request() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_millis(500), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmError {
                id: id.clone(),
                message: "upstream exploded".into(),
            },
        );

        let err = handle.await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "upstream exploded");
    }

    #[tokio::test]
    async fn reject_all_rejects_every_pending_request() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let c3 = consumer.clone();
        let h1 = tokio::spawn(async move {
            c2.request("provider1", vec![chat("hi")], None, Duration::from_secs(5), None)
                .await
        });
        let h2 = tokio::spawn(async move {
            c3.request("provider1", vec![chat("yo")], None, Duration::from_secs(5), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        assert_eq!(consumer.pending.lock().unwrap().len(), 2);

        consumer.reject_all("shutting down");

        let e1 = h1.await.unwrap().unwrap_err();
        let e2 = h2.await.unwrap().unwrap_err();
        assert_eq!(e1.to_string(), "shutting down");
        assert_eq!(e2.to_string(), "shutting down");
        assert_eq!(consumer.pending.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn provider_lock_in_first_wins_and_replies_consumer_hello() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into()]),
            },
        );

        let info = consumer.provider().expect("should be locked in");
        assert_eq!(info.node_id, "p1");
        assert_eq!(info.models, vec!["gpt-4o".to_string()]);

        let sent = sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|(to, msg)| to == "p1" && matches!(msg, ProtocolMessage::ConsumerHello)),
            "expected a consumer_hello reply to p1, got: {sent:?}"
        );
    }

    #[tokio::test]
    async fn provider_lock_in_same_id_refreshes_models() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into()]),
            },
        );
        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into(), "gpt-4o-mini".into()]),
            },
        );

        let info = consumer.provider().unwrap();
        assert_eq!(info.node_id, "p1");
        assert_eq!(info.models, vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()]);

        // Only the first hello should have triggered a consumer_hello reply.
        let consumer_hellos = sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, msg)| to == "p1" && matches!(msg, ProtocolMessage::ConsumerHello))
            .count();
        assert_eq!(consumer_hellos, 1);
    }

    #[tokio::test]
    async fn provider_lock_in_ignores_other_ids() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message("p1", &ProtocolMessage::ProviderHello { models: None });
        consumer.handle_message(
            "p2",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["other-model".into()]),
            },
        );

        let info = consumer.provider().unwrap();
        assert_eq!(info.node_id, "p1");

        let sent_to_p2 = sent.lock().unwrap().iter().any(|(to, _)| to == "p2");
        assert!(!sent_to_p2, "should not reply to a non-locked-in provider");
    }

    #[tokio::test]
    async fn disconnect_of_locked_provider_clears_lock_and_rejects_pending() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        consumer.handle_message("p1", &ProtocolMessage::ProviderHello { models: None });
        assert!(consumer.provider().is_some());

        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request("p1", vec![chat("hi")], None, Duration::from_secs(5), None)
                .await
        });
        sleep(Duration::from_millis(20)).await;
        let _ = last_request_id(&sent);

        consumer.on_peer_disconnected("p1");

        assert!(consumer.provider().is_none());
        let err = handle.await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "provider disconnected");
    }

    #[tokio::test]
    async fn disconnect_of_unrelated_peer_keeps_lock() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        consumer.handle_message("p1", &ProtocolMessage::ProviderHello { models: None });

        consumer.on_peer_disconnected("someone-else");

        assert_eq!(consumer.provider().unwrap().node_id, "p1");
    }

    #[tokio::test]
    async fn wait_for_provider_before_and_after_lock_in() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();

        let handle = tokio::spawn(async move { c2.wait_for_provider(Duration::from_secs(5)).await });
        sleep(Duration::from_millis(30)).await;
        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["m1".into()]),
            },
        );

        let info = handle.await.unwrap().unwrap();
        assert_eq!(info.node_id, "p1");

        // Already locked in: returns immediately, well within a tiny timeout.
        let info2 = consumer.wait_for_provider(Duration::from_millis(10)).await.unwrap();
        assert_eq!(info2.node_id, "p1");
    }

    #[tokio::test]
    async fn wait_for_provider_times_out_when_none_found() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        let err = consumer
            .wait_for_provider(Duration::from_millis(40))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no provider found"));
    }
}
