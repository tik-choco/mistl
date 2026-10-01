//! Consumer side of the AI network, ported from mistai's `consumer.ts` +
//! `client.ts` discovery behavior.
//!
//! ## Discovery (first *chat* provider wins)
//!
//! The service broadcasts `consumer_hello` after joining; providers answer
//! with `provider_hello`. This consumer only ever issues `llm_request`s
//! (chat), so a `provider_hello` that does not advertise the `"chat"`
//! service (per `protocol::advertises_service` -- a missing `services`
//! field defaults to `["chat"]` per the wire spec) is not a candidate at
//! all: it is never locked onto and never triggers a `consumer_hello`
//! reply, even if no provider is currently locked in. Among hellos that do
//! advertise chat, the first sender becomes the locked-in provider; later
//! hellos from the *same* id refresh its model/service list, hellos from
//! other ids are ignored. When the locked-in provider disconnects
//! ([`Consumer::on_peer_disconnected`]), every in-flight request is
//! rejected and the lock is cleared so a new (chat) provider can be
//! discovered.
//!
//! This is intentionally a narrow, additive change to the existing
//! first-wins rule (filter candidates to chat providers, otherwise
//! unchanged) rather than the fuller per-service provider table the wire
//! spec's "consumer 側の provider 選択手順" section describes (service-
//! scoped candidate pools, model-aware ranking, failover) -- this consumer
//! only ever speaks `llm_request`/chat, so that generality isn't needed
//! here.
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
//! - `llm_error`: reject with the remote message (and, when present, the
//!   `code` -- e.g. `"unsupported_service"` -- appended for diagnosability;
//!   see `Event::Error`).
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
use tracing::{debug, warn};

use super::openai::{ChatOutput, ToolOptions};
use super::protocol::{self, ChatMessage, ProtocolMessage};
use super::{SendFn, ToolsUnsupported};

/// The provider this consumer has locked onto.
#[derive(Debug, Clone)]
pub struct ProviderInfo {
    pub node_id: String,
    /// Models from the provider's latest `provider_hello` (may be empty).
    pub models: Vec<String>,
    /// Raw `services` from the provider's latest `provider_hello`, as
    /// decoded (`None` if the field was absent/invalid on the wire). This
    /// consumer only locks onto providers that advertise chat (see the
    /// module doc), so every `ProviderInfo` here is a chat provider by
    /// construction; `services` is kept as-received (not defaulted) so
    /// callers can distinguish an explicit `["chat"]` advertisement from a
    /// pre-`services` peer that omitted the field entirely.
    pub services: Option<Vec<String>>,
    /// Raw `voices` from the provider's latest `provider_hello`, as decoded
    /// (`None` if absent/invalid). This consumer never issues
    /// `tts_request`s itself (see the module doc's Discovery section, which
    /// only covers `llm_request`/chat) -- kept as-received purely so
    /// callers inspecting `ProviderInfo` (e.g. a status/dashboard surface)
    /// can see what the locked-in provider advertises, same as `services`.
    pub voices: Option<Vec<String>>,
    /// DID verified for this provider's node (peer-auth hello), if any.
    pub did: Option<String>,
    /// Whether the provider matched `ai.trusted_providers`. `false` when the
    /// allowlist is empty (legacy first-provider-wins pinning).
    pub trusted: bool,
}

/// DID lookup for a transport sender (`net::peer_auth` registry by default).
type DidLookup = fn(&str) -> Option<String>;

/// Whether the verified `did` matches one `trusted` entry: a full did:key, or
/// a 16-hex node id that the verified DID hashes to. An unverified peer
/// (`did == None`) never matches.
fn provider_matches(trusted: &[String], did: Option<&str>) -> bool {
    let Some(did) = did else { return false };
    trusted
        .iter()
        .any(|t| t == did || t.eq_ignore_ascii_case(&crate::identity::node_id_for_did(did)))
}

/// Internal events routed to an in-flight [`Consumer::request`] call.
#[derive(Debug, Clone)]
enum Event {
    Chunk {
        delta: String,
        seq: Option<u64>,
    },
    Done {
        content: Option<String>,
        tool_calls: Option<serde_json::Value>,
    },
    /// `code` is the wire `llm_error.code` (e.g. `"unsupported_service"`),
    /// when present.
    Error {
        message: String,
        code: Option<String>,
    },
    Rejected {
        reason: String,
    },
}

/// Cap on accumulated response content per request.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Cap on out-of-order chunks held back waiting for a missing `seq`.
const MAX_BUFFERED_CHUNKS: usize = 1024;
/// Largest accepted distance between a chunk's `seq` and the next expected.
const MAX_SEQ_GAP: u64 = 1024;
/// Total wall-clock deadline per request, on top of the inactivity timeout.
const MAX_REQUEST_DURATION: Duration = Duration::from_secs(600);

/// Consumer state: provider lock + in-flight requests.
pub struct Consumer {
    send: SendFn,
    /// Request id -> (provider node id the request was sent to, event
    /// sink). Events whose sender differs from the recorded provider are
    /// dropped so another peer cannot inject into someone else's request.
    pending: Mutex<HashMap<String, (String, mpsc::UnboundedSender<Event>)>>,
    /// Locked-in provider, `None` until the first `provider_hello`.
    /// A `watch` channel lets `wait_for_provider` await lock-in without
    /// polling.
    provider: watch::Sender<Option<ProviderInfo>>,
    /// `ai.trusted_providers`; empty = legacy first-wins (with a warning).
    trusted: Mutex<Vec<String>>,
    did_lookup: Mutex<DidLookup>,
}

impl Consumer {
    pub fn new(send: SendFn) -> std::sync::Arc<Self> {
        let (provider, _rx) = watch::channel(None);
        std::sync::Arc::new(Self {
            send,
            pending: Mutex::new(HashMap::new()),
            provider,
            trusted: Mutex::new(Vec::new()),
            did_lookup: Mutex::new(crate::net::peer_auth::verified_did_any_room),
        })
    }

    pub fn trusted_providers(&self) -> Vec<String> {
        self.trusted.lock().expect("trusted lock").clone()
    }

    /// Installs `ai.trusted_providers`. A currently pinned provider that no
    /// longer qualifies is dropped so a trusted one can take over.
    pub fn set_trusted_providers(&self, list: Vec<String>) {
        *self.trusted.lock().expect("trusted lock") = list.clone();
        if list.is_empty() {
            return;
        }
        self.provider.send_if_modified(|current| {
            if current
                .as_ref()
                .is_some_and(|p| !provider_matches(&list, p.did.as_deref()))
            {
                *current = None;
                true
            } else {
                false
            }
        });
    }

    /// Feed one decoded inbound message. Only `provider_hello`,
    /// `llm_response_chunk`, `llm_response_done`, and `llm_error` are
    /// meaningful; everything else is ignored. On first `provider_hello`
    /// from a chat-capable provider (lock-in), reply `consumer_hello`
    /// directly to that provider via `self.send` (mistai does this to let
    /// the provider classify us). `provider_hello`s that don't advertise
    /// `"chat"` (see [`protocol::advertises_service`]) are not candidates
    /// at all -- see the module doc's "Discovery" section.
    pub fn handle_message(&self, from: &str, msg: &ProtocolMessage) {
        match msg {
            ProtocolMessage::ProviderHello {
                models,
                services,
                voices,
            } => {
                if !protocol::advertises_service(services, protocol::SERVICE_CHAT) {
                    debug!(
                        %from,
                        ?services,
                        "ai: ignoring provider_hello that does not advertise chat"
                    );
                    return;
                }
                let trusted_list = self.trusted.lock().expect("trusted lock").clone();
                let did = (*self.did_lookup.lock().expect("did lookup lock"))(from);
                let trusted = provider_matches(&trusted_list, did.as_deref());
                if !trusted_list.is_empty() && !trusted {
                    debug!(
                        %from,
                        "ai: ignoring provider_hello from a peer not in ai.trusted_providers"
                    );
                    return;
                }
                let mut locked_in = false;
                self.provider.send_if_modified(|current| match current {
                    None => {
                        *current = Some(ProviderInfo {
                            did: did.clone(),
                            trusted,
                            node_id: from.to_string(),
                            models: models.clone().unwrap_or_default(),
                            services: services.clone(),
                            voices: voices.clone(),
                        });
                        locked_in = true;
                        true
                    }
                    Some(info) if info.node_id == from => {
                        info.models = models.clone().unwrap_or_default();
                        info.services = services.clone();
                        info.voices = voices.clone();
                        info.did = did.clone();
                        info.trusted = trusted;
                        true
                    }
                    Some(_) => false,
                });
                if locked_in {
                    if !trusted {
                        warn!(
                            %from,
                            did = ?did,
                            "ai: pinned an UNVERIFIED remote provider (first provider_hello wins); set ai.trusted_providers so strangers in the room cannot read prompts"
                        );
                    }
                    (self.send)(from, ProtocolMessage::ConsumerHello);
                }
            }
            ProtocolMessage::LlmResponseChunk { id, delta, seq } => {
                self.send_event(
                    from,
                    id,
                    Event::Chunk {
                        delta: delta.clone(),
                        seq: *seq,
                    },
                );
            }
            ProtocolMessage::LlmResponseDone {
                id,
                content,
                tool_calls,
            } => {
                self.send_event(
                    from,
                    id,
                    Event::Done {
                        content: content.clone(),
                        tool_calls: tool_calls.clone(),
                    },
                );
            }
            ProtocolMessage::LlmError { id, message, code } => {
                self.send_event(
                    from,
                    id,
                    Event::Error {
                        message: message.clone(),
                        code: code.clone(),
                    },
                );
            }
            _ => {}
        }
    }

    fn send_event(&self, from: &str, id: &str, event: Event) {
        let pending = self.pending.lock().expect("consumer pending lock");
        if let Some((provider, tx)) = pending.get(id) {
            if provider == from {
                let _ = tx.send(event);
            } else {
                debug!(%from, %id, "ai: dropping response event from a peer that is not the request's provider");
            }
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
    /// Text-only convenience over [`Consumer::request_tools`] (test helper).
    #[cfg(test)]
    pub async fn request(
        &self,
        provider_id: &str,
        messages: Vec<ChatMessage>,
        model: Option<String>,
        inactivity_timeout: Duration,
        delta_tx: Option<UnboundedSender<String>>,
    ) -> Result<String> {
        Ok(self
            .request_tools(
                provider_id,
                messages,
                ToolOptions::default(),
                model,
                inactivity_timeout,
                delta_tx,
            )
            .await?
            .content)
    }

    /// [`Consumer::request`] with the optional tool-calling fields. When the
    /// request uses any tool field (`tools`/`tool_choice`, or messages with
    /// the `tool` role / `tool_calls` / `tool_call_id`) the provider must
    /// currently advertise `"tools"`; otherwise this fails with
    /// [`ToolsUnsupported`] **before anything is sent**, so peers that
    /// predate the extension never see the new fields.
    pub async fn request_tools(
        &self,
        provider_id: &str,
        messages: Vec<ChatMessage>,
        tools: ToolOptions,
        model: Option<String>,
        inactivity_timeout: Duration,
        delta_tx: Option<UnboundedSender<String>>,
    ) -> Result<ChatOutput> {
        if tools.request_uses_tools(&messages) {
            let supported = self.provider().is_some_and(|info| {
                info.node_id == provider_id
                    && protocol::advertises_service(&info.services, protocol::SERVICE_TOOLS)
            });
            if !supported {
                return Err(anyhow::Error::new(ToolsUnsupported));
            }
        }
        let ToolOptions { tools, tool_choice } = tools;
        let id = protocol::random_id();
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        self.pending
            .lock()
            .expect("consumer pending lock")
            .insert(id.clone(), (provider_id.to_string(), tx));

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
                tools,
                tool_choice,
            },
        );

        let mut content = String::new();
        let mut next_seq: u64 = 0;
        let mut buffered: BTreeMap<u64, String> = BTreeMap::new();
        let deadline = tokio::time::Instant::now() + MAX_REQUEST_DURATION;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                bail!("request timed out");
            }
            let wait = inactivity_timeout.min(remaining);
            let event = match tokio::time::timeout(wait, rx.recv()).await {
                Ok(Some(event)) => event,
                Ok(None) => bail!("request channel closed unexpectedly"),
                Err(_) => bail!("request timed out"),
            };

            match event {
                Event::Chunk { delta, seq } => match seq {
                    None => {
                        if content.len() + delta.len() > MAX_RESPONSE_BYTES {
                            bail!("provider response exceeded the maximum size");
                        }
                        content.push_str(&delta);
                        if let Some(tx) = &delta_tx {
                            let _ = tx.send(delta);
                        }
                    }
                    Some(seq) if seq < next_seq => {
                        // Stale duplicate; drop.
                    }
                    Some(seq) if seq > next_seq => {
                        if seq - next_seq > MAX_SEQ_GAP
                            || (buffered.len() >= MAX_BUFFERED_CHUNKS
                                && !buffered.contains_key(&seq))
                        {
                            bail!("provider sent too many out-of-order chunks");
                        }
                        buffered.insert(seq, delta);
                        let buffered_bytes: usize = buffered.values().map(String::len).sum();
                        if content.len() + buffered_bytes > MAX_RESPONSE_BYTES {
                            bail!("provider response exceeded the maximum size");
                        }
                    }
                    Some(_) => {
                        if content.len() + delta.len() > MAX_RESPONSE_BYTES {
                            bail!("provider response exceeded the maximum size");
                        }
                        content.push_str(&delta);
                        if let Some(tx) = &delta_tx {
                            let _ = tx.send(delta);
                        }
                        next_seq += 1;
                        while let Some(next) = buffered.remove(&next_seq) {
                            if content.len() + next.len() > MAX_RESPONSE_BYTES {
                                bail!("provider response exceeded the maximum size");
                            }
                            content.push_str(&next);
                            if let Some(tx) = &delta_tx {
                                let _ = tx.send(next);
                            }
                            next_seq += 1;
                        }
                    }
                },
                Event::Done {
                    content: final_content,
                    tool_calls,
                } => {
                    if let Some(calls) = &tool_calls {
                        let too_many = calls
                            .as_array()
                            .is_none_or(|a| a.len() > protocol::MAX_TOOL_CALLS);
                        if too_many || calls.to_string().len() > MAX_RESPONSE_BYTES {
                            bail!("provider response exceeded the maximum size");
                        }
                    }
                    // An empty array means "no calls".
                    let tool_calls =
                        tool_calls.filter(|c| c.as_array().is_some_and(|a| !a.is_empty()));
                    if let Some(final_content) = final_content {
                        if final_content.len() > MAX_RESPONSE_BYTES {
                            bail!("provider response exceeded the maximum size");
                        }
                        return Ok(ChatOutput {
                            content: final_content,
                            tool_calls,
                        });
                    }
                    return Ok(ChatOutput {
                        content,
                        tool_calls,
                    });
                }
                Event::Error { message, code } => {
                    // `code` (e.g. "unsupported_service") is appended for
                    // diagnosability -- this consumer doesn't act on it
                    // (no automatic failover; see the module doc), it just
                    // surfaces it to the caller/logs.
                    match code {
                        Some(code) => bail!("{message} (code: {code})"),
                        None => bail!(message),
                    }
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
            pending.values().map(|(_, tx)| tx.clone()).collect()
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
            tool_calls: None,
            tool_call_id: None,
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
                tool_calls: None,
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
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(500),
                None,
            )
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
                tool_calls: None,
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
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(500),
                None,
            )
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
                tool_calls: None,
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
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(500),
                None,
            )
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
                tool_calls: None,
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
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(500),
                None,
            )
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
                tool_calls: None,
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
            .request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(60),
                None,
            )
            .await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn inactivity_timeout_resets_on_chunk_activity() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(150),
                None,
            )
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
                tool_calls: None,
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
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(100),
                None,
            )
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
        assert!(
            err.to_string().contains("timed out"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn llm_error_rejects_request() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(500),
                None,
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmError {
                id: id.clone(),
                message: "upstream exploded".into(),
                code: None,
            },
        );

        let err = handle.await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "upstream exploded");
    }

    #[tokio::test]
    async fn llm_error_with_code_appends_code_to_error_message() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(500),
                None,
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);

        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmError {
                id: id.clone(),
                message: "chat not supported".into(),
                code: Some("unsupported_service".into()),
            },
        );

        let err = handle.await.unwrap().unwrap_err();
        assert_eq!(
            err.to_string(),
            "chat not supported (code: unsupported_service)"
        );
    }

    #[tokio::test]
    async fn reject_all_rejects_every_pending_request() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let c3 = consumer.clone();
        let h1 = tokio::spawn(async move {
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_secs(5),
                None,
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            c3.request(
                "provider1",
                vec![chat("yo")],
                None,
                Duration::from_secs(5),
                None,
            )
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
                services: None,
                voices: None,
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
                services: None,
                voices: None,
            },
        );
        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into(), "gpt-4o-mini".into()]),
                services: None,
                voices: None,
            },
        );

        let info = consumer.provider().unwrap();
        assert_eq!(info.node_id, "p1");
        assert_eq!(
            info.models,
            vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()]
        );

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
    async fn provider_lock_in_stores_services() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into()]),
                services: Some(vec!["chat".into(), "tts".into()]),
                voices: Some(vec!["alloy".into()]),
            },
        );

        let info = consumer.provider().expect("should be locked in");
        assert_eq!(
            info.services,
            Some(vec!["chat".to_string(), "tts".to_string()])
        );
        assert_eq!(info.voices, Some(vec!["alloy".to_string()]));
    }

    #[tokio::test]
    async fn provider_hello_without_chat_service_is_not_a_lock_in_candidate() {
        // A voice/embedding-only provider (services present, but no
        // "chat") must not be locked onto: this consumer only issues
        // llm_request/chat, so it must keep waiting for a chat-capable
        // provider instead.
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message(
            "voice-only",
            &ProtocolMessage::ProviderHello {
                models: None,
                services: Some(vec!["tts".into(), "stt".into()]),
                voices: None,
            },
        );

        assert!(
            consumer.provider().is_none(),
            "must not lock onto a non-chat provider"
        );
        let sent_to_voice_only = sent
            .lock()
            .unwrap()
            .iter()
            .any(|(to, _)| to == "voice-only");
        assert!(
            !sent_to_voice_only,
            "must not reply consumer_hello to a non-chat provider"
        );
    }

    #[tokio::test]
    async fn chat_provider_hello_after_non_chat_hello_still_locks_in() {
        // A non-chat hello arriving first must not "use up" the first-wins
        // slot -- the first genuinely chat-capable hello should still win.
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message(
            "voice-only",
            &ProtocolMessage::ProviderHello {
                models: None,
                services: Some(vec!["tts".into()]),
                voices: None,
            },
        );
        consumer.handle_message(
            "chat-provider",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into()]),
                services: Some(vec!["chat".into()]),
                voices: None,
            },
        );

        let info = consumer.provider().expect("should be locked in");
        assert_eq!(info.node_id, "chat-provider");
        let sent = sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|(to, msg)| to == "chat-provider"
                    && matches!(msg, ProtocolMessage::ConsumerHello)),
            "expected a consumer_hello reply to the chat provider, got: {sent:?}"
        );
    }

    #[tokio::test]
    async fn provider_hello_missing_services_defaults_to_chat_and_locks_in() {
        // The wire spec's backward-compat default: a `provider_hello` with
        // no `services` field at all (pre-services-extension peer, e.g.
        // tc-mistllm core / older mistl) is treated as chat-only, so it
        // must still be a valid lock-in candidate.
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message(
            "legacy-provider",
            &ProtocolMessage::ProviderHello {
                models: None,
                services: None,
                voices: None,
            },
        );

        let info = consumer.provider().expect("should be locked in");
        assert_eq!(info.node_id, "legacy-provider");
    }

    #[tokio::test]
    async fn provider_lock_in_ignores_other_ids() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);

        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: None,
                services: None,
                voices: None,
            },
        );
        consumer.handle_message(
            "p2",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["other-model".into()]),
                services: None,
                voices: None,
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
        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: None,
                services: None,
                voices: None,
            },
        );
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
        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: None,
                services: None,
                voices: None,
            },
        );

        consumer.on_peer_disconnected("someone-else");

        assert_eq!(consumer.provider().unwrap().node_id, "p1");
    }

    #[tokio::test]
    async fn wait_for_provider_before_and_after_lock_in() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();

        let handle =
            tokio::spawn(async move { c2.wait_for_provider(Duration::from_secs(5)).await });
        sleep(Duration::from_millis(30)).await;
        consumer.handle_message(
            "p1",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["m1".into()]),
                services: None,
                voices: None,
            },
        );

        let info = handle.await.unwrap().unwrap();
        assert_eq!(info.node_id, "p1");

        // Already locked in: returns immediately, well within a tiny timeout.
        let info2 = consumer
            .wait_for_provider(Duration::from_millis(10))
            .await
            .unwrap();
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

    #[tokio::test]
    async fn events_from_a_non_provider_peer_are_dropped() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_millis(300),
                None,
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);
        consumer.handle_message(
            "attacker",
            &ProtocolMessage::LlmResponseDone {
                tool_calls: None,
                id: id.clone(),
                content: Some("forged".into()),
            },
        );
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                tool_calls: None,
                id,
                content: Some("real".into()),
            },
        );
        assert_eq!(handle.await.unwrap().unwrap(), "real");
    }

    fn hello() -> ProtocolMessage {
        ProtocolMessage::ProviderHello {
            models: None,
            services: Some(vec!["chat".into()]),
            voices: None,
        }
    }

    fn did_a() -> String {
        crate::identity::did_for_seed(1)
    }

    fn lookup(from: &str) -> Option<String> {
        (from == crate::identity::node_id_for_did(&did_a())).then(did_a)
    }

    #[test]
    fn empty_allowlist_pins_first_provider_but_flags_untrusted() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        consumer.handle_message("stranger", &hello());
        let info = consumer.provider().unwrap();
        assert_eq!(info.node_id, "stranger");
        assert!(!info.trusted);
        assert!(info.did.is_none());
    }

    #[test]
    fn allowlist_ignores_unverified_provider() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        *consumer.did_lookup.lock().unwrap() = lookup;
        consumer.set_trusted_providers(vec![did_a()]);
        consumer.handle_message("stranger", &hello());
        assert!(consumer.provider().is_none());
        assert!(sent.lock().unwrap().is_empty());
    }

    #[test]
    fn allowlist_accepts_verified_did_and_node_id_entries() {
        for entry in [did_a(), crate::identity::node_id_for_did(&did_a())] {
            let (send, _sent) = fake_send();
            let consumer = Consumer::new(send);
            *consumer.did_lookup.lock().unwrap() = lookup;
            consumer.set_trusted_providers(vec![entry]);
            let node = crate::identity::node_id_for_did(&did_a());
            // A stranger first must not take the slot.
            consumer.handle_message("stranger", &hello());
            consumer.handle_message(&node, &hello());
            let info = consumer.provider().unwrap();
            assert_eq!(info.node_id, node);
            assert!(info.trusted);
            assert_eq!(info.did.as_deref(), Some(did_a().as_str()));
        }
    }

    #[test]
    fn node_id_entry_does_not_match_unverified_claim() {
        // A node id entry matches only through a verified DID.
        assert!(!provider_matches(&["d1a9220e96a8afd0".into()], None));
    }

    #[test]
    fn tightening_allowlist_drops_untrusted_pinned_provider() {
        let (send, _sent) = fake_send();
        let consumer = Consumer::new(send);
        consumer.handle_message("stranger", &hello());
        assert!(consumer.provider().is_some());
        consumer.set_trusted_providers(vec![did_a()]);
        assert!(consumer.provider().is_none());
    }

    #[tokio::test]
    async fn oversized_done_content_is_rejected() {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request(
                "provider1",
                vec![chat("hi")],
                None,
                Duration::from_secs(5),
                None,
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                tool_calls: None,
                id,
                content: Some("x".repeat(MAX_RESPONSE_BYTES + 1)),
            },
        );
        let err = handle.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("maximum size"), "{err}");
    }

    // ---- tool calling -------------------------------------------------

    fn tool_opts() -> ToolOptions {
        ToolOptions {
            tools: Some(serde_json::json!([{"type":"function","function":{"name":"f"}}])),
            tool_choice: None,
        }
    }

    fn locked(services: Option<Vec<String>>) -> (Arc<Consumer>, Sent) {
        let (send, sent) = fake_send();
        let consumer = Consumer::new(send);
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::ProviderHello {
                models: None,
                services,
                voices: None,
            },
        );
        sent.lock().unwrap().clear();
        (consumer, sent)
    }

    #[tokio::test]
    async fn tools_are_refused_before_sending_to_a_provider_without_the_capability() {
        for services in [None, Some(vec!["chat".to_string()])] {
            let (consumer, sent) = locked(services);
            let err = consumer
                .request_tools(
                    "provider1",
                    vec![chat("hi")],
                    tool_opts(),
                    None,
                    Duration::from_millis(200),
                    None,
                )
                .await
                .expect_err("must refuse");
            assert!(err.downcast_ref::<ToolsUnsupported>().is_some(), "{err:#}");
            assert!(sent.lock().unwrap().is_empty(), "nothing may be sent");
        }
        // Tool-role / tool_calls messages alone also count as tool use.
        let (consumer, sent) = locked(Some(vec!["chat".to_string()]));
        let mut m = chat("r");
        m.role = "tool".into();
        m.tool_call_id = Some("c".into());
        let err = consumer
            .request_tools(
                "provider1",
                vec![m],
                ToolOptions::default(),
                None,
                Duration::from_millis(200),
                None,
            )
            .await
            .expect_err("must refuse");
        assert!(err.downcast_ref::<ToolsUnsupported>().is_some());
        assert!(sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn plain_chat_still_works_against_a_provider_without_tools() {
        let (consumer, sent) = locked(Some(vec!["chat".to_string()]));
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request_tools(
                "provider1",
                vec![chat("hi")],
                ToolOptions::default(),
                None,
                Duration::from_millis(500),
                None,
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id,
                content: Some("ok".into()),
                tool_calls: None,
            },
        );
        assert_eq!(handle.await.unwrap().unwrap().content, "ok");
    }

    #[tokio::test]
    async fn tools_are_sent_and_tool_calls_returned_with_the_capability() {
        let (consumer, sent) = locked(Some(vec!["chat".to_string(), "tools".to_string()]));
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request_tools(
                "provider1",
                vec![chat("hi")],
                tool_opts(),
                None,
                Duration::from_millis(500),
                None,
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);
        {
            let sent = sent.lock().unwrap();
            match &sent.last().unwrap().1 {
                ProtocolMessage::LlmRequest { tools, .. } => assert_eq!(*tools, tool_opts().tools),
                other => panic!("unexpected {other:?}"),
            }
        }
        let calls = serde_json::json!([{"id":"c1","type":"function",
            "function":{"name":"f","arguments":"{}"}}]);
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id,
                content: Some(String::new()),
                tool_calls: Some(calls.clone()),
            },
        );
        let out = handle.await.unwrap().unwrap();
        assert_eq!(out.content, "");
        assert_eq!(out.tool_calls, Some(calls));
    }

    #[tokio::test]
    async fn oversized_tool_calls_in_done_are_rejected() {
        let (consumer, sent) = locked(Some(vec!["chat".to_string(), "tools".to_string()]));
        let c2 = consumer.clone();
        let handle = tokio::spawn(async move {
            c2.request_tools(
                "provider1",
                vec![chat("hi")],
                tool_opts(),
                None,
                Duration::from_millis(500),
                None,
            )
            .await
        });
        sleep(Duration::from_millis(20)).await;
        let id = last_request_id(&sent);
        let many: Vec<_> = (0..=protocol::MAX_TOOL_CALLS)
            .map(|i| serde_json::json!({"id": format!("c{i}")}))
            .collect();
        consumer.handle_message(
            "provider1",
            &ProtocolMessage::LlmResponseDone {
                id,
                content: None,
                tool_calls: Some(serde_json::Value::Array(many)),
            },
        );
        assert!(handle.await.unwrap().is_err());
    }
}
