//! Provider side of the AI network, ported from mistai's `provider.ts`.
//!
//! This provider serves **LLM chat only**. Unlike mistai's reference
//! implementation (which pairs `ProviderService` with a separate
//! `VoiceProviderService` for `tts_request`/`stt_request`), this port never
//! calls out to a TTS/STT upstream: voice requests are rejected immediately
//! with `voice_error` (see below) rather than silently ignored, so a peer's
//! `ConsumerClient.requestTts`/`requestStt` gets a clear, prompt error
//! instead of hanging until its own client-side timeout.
//!
//! Handles inbound `llm_request`s by forwarding them to the injected
//! upstream call ([`super::LlmCallFn`], normally
//! `openai::stream_chat_completion`) and streaming the result back:
//!
//! - Each upstream delta is sent immediately as
//!   `llm_response_chunk { id, delta, seq }` with a per-request `seq`
//!   counter starting at 0 (one chunk per delta, no batching).
//! - On success: `llm_response_done { id, content: Some(full) }`.
//! - On failure: `llm_error { id, message }`.
//! - `consumer_hello` -> reply [`Provider::hello`] directly to the sender.
//! - `tts_request` / `stt_request` -> reply `voice_error { id, message }`
//!   directly to the sender; this provider has no voice upstream.
//!
//! Keeps a ring buffer of request logs (default cap 50, oldest dropped;
//! re-logging an id replaces the previous entry): status progresses
//! `started` -> `streaming` (with cumulative char count) -> `done` /
//! `error` (with detail).

use std::sync::Mutex;

use serde::Serialize;

use super::protocol::ProtocolMessage;
use super::{LlmCallFn, SendFn};

/// Maximum number of log entries retained; oldest are dropped first.
const DEFAULT_MAX_LOG_ENTRIES: usize = 50;

/// One entry in the provider's request log (newest first from [`Provider::logs`]).
#[derive(Debug, Clone, Serialize)]
pub struct RequestLog {
    pub id: String,
    pub from: String,
    pub model: Option<String>,
    /// "started" | "streaming" | "done" | "error"
    pub status: String,
    /// RFC 3339.
    pub started_at: String,
    /// Cumulative characters streamed so far.
    pub char_count: usize,
    /// Error detail when status == "error".
    pub detail: Option<String>,
}

/// Provider state: send fn, upstream call, advertised models, logs.
pub struct Provider {
    send: SendFn,
    call: LlmCallFn,
    models: Vec<String>,
    logs: Mutex<Vec<RequestLog>>,
}

impl Provider {
    pub fn new(send: SendFn, call: LlmCallFn, models: Vec<String>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            send,
            call,
            models,
            logs: Mutex::new(Vec::new()),
        })
    }

    /// Models advertised in `provider_hello`.
    pub fn models(&self) -> Vec<String> {
        self.models.clone()
    }

    /// The `provider_hello` announcement: `models` field included only
    /// when the list is non-empty (mistai omits it otherwise). `services`
    /// always advertises `["chat"]` -- this port is a chat-only provider
    /// (see the module doc), so it always sets `services` explicitly
    /// rather than relying on the wire spec's "missing == chat only"
    /// default; this makes the advertisement self-describing to
    /// `services`-aware peers even though the two are equivalent today.
    pub fn hello(&self) -> ProtocolMessage {
        let models = if self.models.is_empty() {
            None
        } else {
            Some(self.models.clone())
        };
        ProtocolMessage::ProviderHello {
            models,
            services: Some(vec![super::protocol::SERVICE_CHAT.to_string()]),
        }
    }

    /// Handle one decoded inbound message (`llm_request` / `consumer_hello`
    /// / `tts_request` / `stt_request`; everything else ignored). Runs the
    /// upstream call inline (callers spawn this future per message, so
    /// concurrent requests don't block each other).
    pub async fn handle_message(self: std::sync::Arc<Self>, from: String, msg: ProtocolMessage) {
        match msg {
            ProtocolMessage::ConsumerHello => {
                (self.send)(&from, self.hello());
            }
            ProtocolMessage::LlmRequest { id, messages, model } => {
                self.handle_llm_request(from, id, messages, model).await;
            }
            ProtocolMessage::TtsRequest { id, .. } | ProtocolMessage::SttRequest { id, .. } => {
                self.reject_voice_request(&from, id);
            }
            _ => {}
        }
    }

    /// This provider has no TTS/STT upstream, so voice requests are
    /// answered with an immediate `voice_error` instead of being dropped
    /// (which would otherwise leave the requester's `ConsumerClient` waiting
    /// until its own client-side voice timeout, e.g. mistai's 120s default).
    /// Sent through the same `send` fn (and thus the same ordered queue) as
    /// every other reply.
    fn reject_voice_request(&self, from: &str, id: String) {
        (self.send)(
            from,
            ProtocolMessage::VoiceError {
                id,
                message: "this provider does not support voice (tts/stt)".to_string(),
                code: Some(super::protocol::CODE_UNSUPPORTED_SERVICE.to_string()),
            },
        );
    }

    async fn handle_llm_request(
        &self,
        from: String,
        id: String,
        messages: Vec<super::protocol::ChatMessage>,
        model: Option<String>,
    ) {
        let started_at = chrono::Utc::now().to_rfc3339();
        self.push_log(RequestLog {
            id: id.clone(),
            from: from.clone(),
            model: model.clone(),
            status: "started".into(),
            started_at: started_at.clone(),
            char_count: 0,
            detail: None,
        });

        let (delta_tx, mut delta_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let call_fut = (self.call)(messages, model.clone(), Some(delta_tx));
        tokio::pin!(call_fut);

        let mut seq: u64 = 0;
        let mut char_count: usize = 0;
        let mut delta_open = true;

        let result = loop {
            tokio::select! {
                biased;
                maybe_delta = delta_rx.recv(), if delta_open => {
                    match maybe_delta {
                        Some(delta) => {
                            self.record_chunk(&from, &id, &model, &started_at, delta, &mut seq, &mut char_count);
                        }
                        None => {
                            delta_open = false;
                        }
                    }
                }
                res = &mut call_fut => {
                    break res;
                }
            }
        };

        // `call_fut` may resolve in the very same poll that also enqueued its
        // last delta(s) (e.g. an upstream call with no await points between
        // its final send and return). The select above can observe the call
        // future as Ready before ever seeing those queued items, so drain
        // whatever is left in the channel now -- the sender is guaranteed to
        // already be dropped once `call_fut` has resolved, so this cannot
        // block forever.
        while delta_open {
            match delta_rx.recv().await {
                Some(delta) => {
                    self.record_chunk(&from, &id, &model, &started_at, delta, &mut seq, &mut char_count);
                }
                None => delta_open = false,
            }
        }

        match result {
            Ok(content) => {
                (self.send)(
                    &from,
                    ProtocolMessage::LlmResponseDone {
                        id: id.clone(),
                        content: Some(content.clone()),
                    },
                );
                self.push_log(RequestLog {
                    id,
                    from,
                    model,
                    status: "done".into(),
                    started_at,
                    char_count: content.chars().count(),
                    detail: None,
                });
            }
            Err(err) => {
                let message = err.to_string();
                (self.send)(
                    &from,
                    ProtocolMessage::LlmError {
                        id: id.clone(),
                        message: message.clone(),
                        // No `code`: this is a generic upstream-call
                        // failure, not a capability mismatch (this
                        // provider does support chat) -- distinct from
                        // `"unsupported_service"` per the wire spec.
                        code: None,
                    },
                );
                self.push_log(RequestLog {
                    id,
                    from,
                    model,
                    status: "error".into(),
                    started_at,
                    char_count,
                    detail: Some(message),
                });
            }
        }
    }

    /// Send one delta as a chunk and record the cumulative streaming log
    /// entry. Shared by the concurrent drain loop and the post-completion
    /// final drain.
    fn record_chunk(
        &self,
        from: &str,
        id: &str,
        model: &Option<String>,
        started_at: &str,
        delta: String,
        seq: &mut u64,
        char_count: &mut usize,
    ) {
        *char_count += delta.chars().count();
        (self.send)(
            from,
            ProtocolMessage::LlmResponseChunk {
                id: id.to_string(),
                delta,
                seq: Some(*seq),
            },
        );
        *seq += 1;
        self.push_log(RequestLog {
            id: id.to_string(),
            from: from.to_string(),
            model: model.clone(),
            status: "streaming".into(),
            started_at: started_at.to_string(),
            char_count: *char_count,
            detail: None,
        });
    }

    /// Call the upstream directly, bypassing the network (used by the
    /// local API server / `ai chat` when this node provides).
    pub async fn call_upstream(
        &self,
        messages: Vec<super::protocol::ChatMessage>,
        model: Option<String>,
        delta_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    ) -> anyhow::Result<String> {
        (self.call)(messages, model, delta_tx).await
    }

    /// Request log, newest first.
    pub fn logs(&self) -> Vec<RequestLog> {
        self.logs.lock().expect("provider logs lock").clone()
    }

    /// Insert/replace a log entry by id, keeping newest-first order and
    /// enforcing the cap.
    fn push_log(&self, entry: RequestLog) {
        let mut logs = self.logs.lock().expect("provider logs lock");
        if let Some(existing) = logs.iter_mut().find(|e| e.id == entry.id) {
            *existing = entry;
        } else {
            logs.insert(0, entry);
            if logs.len() > DEFAULT_MAX_LOG_ENTRIES {
                logs.truncate(DEFAULT_MAX_LOG_ENTRIES);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::protocol::ChatMessage;
    use std::sync::Arc;

    type Sent = Arc<Mutex<Vec<(String, ProtocolMessage)>>>;

    fn fake_send() -> (SendFn, Sent) {
        let sent: Sent = Arc::new(Mutex::new(Vec::new()));
        let sent2 = sent.clone();
        let send: SendFn = Arc::new(move |to: &str, msg: ProtocolMessage| {
            sent2.lock().unwrap().push((to.to_string(), msg));
        });
        (send, sent)
    }

    fn fake_call_success(deltas: Vec<&'static str>, content: &'static str) -> LlmCallFn {
        let deltas: Vec<String> = deltas.into_iter().map(String::from).collect();
        let content = content.to_string();
        Arc::new(move |_messages, _model, delta_tx| {
            let deltas = deltas.clone();
            let content = content.clone();
            Box::pin(async move {
                if let Some(tx) = &delta_tx {
                    for d in &deltas {
                        let _ = tx.send(d.clone());
                    }
                }
                Ok(content)
            })
        })
    }

    fn fake_call_error(message: &'static str) -> LlmCallFn {
        Arc::new(move |_messages, _model, _delta_tx| Box::pin(async move { anyhow::bail!(message) }))
    }

    fn messages() -> Vec<ChatMessage> {
        vec![ChatMessage {
            role: "user".into(),
            content: "hi".into(),
        }]
    }

    #[tokio::test]
    async fn happy_path_emits_chunks_then_done() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec!["Hel", "lo"], "Hello");
        let provider = Provider::new(send, call, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: messages(),
                    model: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 3);
        match &sent[0] {
            (to, ProtocolMessage::LlmResponseChunk { id, delta, seq }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert_eq!(delta, "Hel");
                assert_eq!(*seq, Some(0));
            }
            other => panic!("unexpected first message: {other:?}"),
        }
        match &sent[1] {
            (to, ProtocolMessage::LlmResponseChunk { id, delta, seq }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert_eq!(delta, "lo");
                assert_eq!(*seq, Some(1));
            }
            other => panic!("unexpected second message: {other:?}"),
        }
        match &sent[2] {
            (to, ProtocolMessage::LlmResponseDone { id, content }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert_eq!(content.as_deref(), Some("Hello"));
            }
            other => panic!("unexpected third message: {other:?}"),
        }

        let logs = provider.logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].status, "done");
        assert_eq!(logs[0].char_count, 5);
    }

    #[tokio::test]
    async fn error_path_emits_llm_error() {
        let (send, sent) = fake_send();
        let call = fake_call_error("upstream boom");
        let provider = Provider::new(send, call, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: messages(),
                    model: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            (to, ProtocolMessage::LlmError { id, message, code }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert_eq!(message, "upstream boom");
                assert_eq!(*code, None, "generic upstream failure should not carry a code");
            }
            other => panic!("unexpected message: {other:?}"),
        }

        let logs = provider.logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].status, "error");
        assert_eq!(logs[0].detail.as_deref(), Some("upstream boom"));
    }

    #[test]
    fn hello_omits_models_when_empty() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new(send, call, vec![]);
        assert_eq!(
            provider.hello(),
            ProtocolMessage::ProviderHello {
                models: None,
                services: Some(vec!["chat".into()]),
            }
        );
    }

    #[test]
    fn hello_includes_models_when_present() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new(send, call, vec!["gpt-4o".into(), "gpt-4o-mini".into()]);
        assert_eq!(
            provider.hello(),
            ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into(), "gpt-4o-mini".into()]),
                services: Some(vec!["chat".into()]),
            }
        );
    }

    #[test]
    fn hello_always_advertises_chat_service() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new(send, call, vec![]);
        match provider.hello() {
            ProtocolMessage::ProviderHello { services, .. } => {
                assert_eq!(services, Some(vec!["chat".to_string()]));
            }
            other => panic!("expected ProviderHello, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn consumer_hello_gets_hello_reply() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new(send, call, vec!["m1".into()]);

        provider
            .clone()
            .handle_message("consumer1".into(), ProtocolMessage::ConsumerHello)
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "consumer1");
        assert_eq!(sent[0].1, provider.hello());
    }

    #[tokio::test]
    async fn log_status_transitions() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec!["a", "b"], "ab");
        let provider = Provider::new(send, call, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: messages(),
                    model: Some("gpt-4o".into()),
                },
            )
            .await;

        let logs = provider.logs();
        assert_eq!(logs.len(), 1);
        let entry = &logs[0];
        assert_eq!(entry.id, "req1");
        assert_eq!(entry.from, "consumer1");
        assert_eq!(entry.model.as_deref(), Some("gpt-4o"));
        assert_eq!(entry.status, "done");
        assert_eq!(entry.char_count, 2);
        assert!(entry.detail.is_none());
        // RFC3339 timestamps contain a 'T' separator between date and time.
        assert!(entry.started_at.contains('T'));
    }

    #[tokio::test]
    async fn log_ring_buffer_caps_and_orders_newest_first() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "ok");
        let provider = Provider::new(send, call, vec![]);

        for i in 0..(DEFAULT_MAX_LOG_ENTRIES + 5) {
            provider
                .clone()
                .handle_message(
                    format!("consumer{i}"),
                    ProtocolMessage::LlmRequest {
                        id: format!("req{i}"),
                        messages: messages(),
                        model: None,
                    },
                )
                .await;
        }

        let logs = provider.logs();
        assert_eq!(logs.len(), DEFAULT_MAX_LOG_ENTRIES);
        // Newest request is first.
        let last_index = DEFAULT_MAX_LOG_ENTRIES + 5 - 1;
        assert_eq!(logs[0].id, format!("req{last_index}"));
        // The oldest 5 requests should have been evicted.
        assert!(!logs.iter().any(|l| l.id == "req0"));
    }

    #[tokio::test]
    async fn re_logging_same_id_replaces_in_place() {
        let (send, _sent) = fake_send();
        // Multiple deltas trigger multiple push_log calls (started ->
        // streaming x N -> done) for the *same* request id; the log length
        // must stay at 1.
        let call = fake_call_success(vec!["a", "b", "c"], "abc");
        let provider = Provider::new(send, call, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: messages(),
                    model: None,
                },
            )
            .await;

        assert_eq!(provider.logs().len(), 1);
        assert_eq!(provider.logs()[0].status, "done");
    }

    #[tokio::test]
    async fn tts_request_gets_voice_error_reply() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new(send, call, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::TtsRequest {
                    id: "req1".into(),
                    text: "hello there".into(),
                    model: None,
                    voice: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "expected exactly one reply, got: {sent:?}");
        match &sent[0] {
            (to, ProtocolMessage::VoiceError { id, message, code }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert!(!message.is_empty());
                assert_eq!(code.as_deref(), Some("unsupported_service"));
            }
            other => panic!("expected a voice_error reply, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn stt_request_gets_voice_error_reply() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new(send, call, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::SttRequest {
                    id: "req2".into(),
                    seq: 0,
                    data: "AAAA".into(),
                    last: true,
                    mime: "audio/wav".into(),
                    model: None,
                    file_name: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "expected exactly one reply, got: {sent:?}");
        match &sent[0] {
            (to, ProtocolMessage::VoiceError { id, message, code }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req2");
                assert!(!message.is_empty());
                assert_eq!(code.as_deref(), Some("unsupported_service"));
            }
            other => panic!("expected a voice_error reply, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn voice_requests_do_not_affect_unrelated_llm_requests() {
        // Guards against a shared-state regression: handling a tts_request
        // must not touch the provider's llm_request logging/response path.
        let (send, sent) = fake_send();
        let call = fake_call_success(vec!["Hel", "lo"], "Hello");
        let provider = Provider::new(send, call, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::TtsRequest {
                    id: "voice-req".into(),
                    text: "hi".into(),
                    model: None,
                    voice: None,
                },
            )
            .await;
        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "llm-req".into(),
                    messages: messages(),
                    model: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        // 1 voice_error + 2 llm chunks + 1 done.
        assert_eq!(sent.len(), 4);
        assert!(matches!(&sent[0].1, ProtocolMessage::VoiceError { id, .. } if id == "voice-req"));
        assert!(matches!(&sent[1].1, ProtocolMessage::LlmResponseChunk { id, .. } if id == "llm-req"));
        assert!(matches!(&sent[2].1, ProtocolMessage::LlmResponseChunk { id, .. } if id == "llm-req"));
        assert!(matches!(&sent[3].1, ProtocolMessage::LlmResponseDone { id, .. } if id == "llm-req"));

        // The voice request left no log entry; only the llm_request did.
        let logs = provider.logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].id, "llm-req");
    }

    #[tokio::test]
    async fn call_upstream_bypasses_network() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec!["x"], "x");
        let provider = Provider::new(send, call, vec![]);

        let (delta_tx, mut delta_rx) = tokio::sync::mpsc::unbounded_channel();
        let content = provider
            .call_upstream(messages(), None, Some(delta_tx))
            .await
            .unwrap();

        assert_eq!(content, "x");
        assert_eq!(delta_rx.try_recv().unwrap(), "x");
        assert!(sent.lock().unwrap().is_empty(), "call_upstream must not touch the network");
    }
}
