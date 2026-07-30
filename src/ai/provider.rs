//! Provider side of the AI network, ported from mistai's `provider.ts`.
//!
//! Chat (`llm_request`) is always served once this `Provider` exists (see
//! `super::provide_start`). TTS/STT are served only when a `tts`/`stt`
//! closure was supplied at construction time ([`Provider::new_with_voice`],
//! wired from `ai.tts_preset_id`/`ai.stt_preset_id` in `super::provide_start`);
//! when the corresponding closure is absent, voice requests get an
//! immediate `voice_error` reply instead of silently going unanswered, so a
//! peer's `ConsumerClient.requestTts`/`requestStt` gets a clear, prompt
//! error instead of hanging until its own client-side timeout.
//!
//! Handles inbound messages:
//!
//! - `llm_request`: forwarded to the injected upstream call
//!   ([`super::LlmCallFn`], normally `openai::stream_chat_completion`),
//!   streaming the result back:
//!   - Each upstream delta is sent immediately as
//!     `llm_response_chunk { id, delta, seq }` with a per-request `seq`
//!     counter starting at 0 (one chunk per delta, no batching).
//!   - On success: `llm_response_done { id, content: Some(full) }`.
//!   - On failure: `llm_error { id, message }`.
//! - `consumer_hello` -> reply [`Provider::hello`] directly to the sender.
//! - `tts_request` (single message: `id, text, model?, voice?, lang?`) ->
//!   when a TTS closure is configured, synthesize and reply with one or more
//!   `tts_response { id, seq, data (base64), last, mime }` chunks
//!   ([`TTS_CHUNK_RAW_BYTES`] raw bytes per chunk before base64, comfortably
//!   under mist's message size ceiling once inflated); on upstream failure,
//!   `voice_error { id, message }` (no `code`: this provider does support
//!   TTS, the upstream call itself just failed). Otherwise, immediate
//!   `voice_error { code: "unsupported_service" }`.
//! - `stt_request` (chunked: `id, seq, data (base64), last, mime, model?,
//!   fileName?`, one or more messages sharing `id`) -> when an STT closure
//!   is configured, chunks are reassembled per `id` (capped at
//!   [`STT_MAX_BUFFERED_BYTES`], beyond which the buffer is dropped and a
//!   `voice_error` sent) until `last == true`, then transcribed and replied
//!   as `stt_response { id, text }` (or `voice_error` on upstream failure).
//!   Otherwise, immediate `voice_error { code: "unsupported_service" }`,
//!   without ever buffering.
//!
//! `services` advertised in [`Provider::hello`] always includes `"chat"`
//! and additionally `"tts"`/`"stt"` exactly when the corresponding closure
//! is configured.
//!
//! Keeps a ring buffer of request logs (default cap 50, oldest dropped;
//! re-logging an id replaces the previous entry): status progresses
//! `started` -> `streaming` (with cumulative char count) -> `done` /
//! `error` (with detail). Voice requests are not logged (the `RequestLog`
//! shape is chat-specific -- `model` there means the *chat* model).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::config::ResolvedAiPreset;

use super::openai::{self, UpstreamConfig};
use super::protocol::ProtocolMessage;
use super::tts::TtsAudio;
use super::{LlmCallFn, LlmCallFuture, SendFn};

/// Maximum number of log entries retained; oldest are dropped first.
const DEFAULT_MAX_LOG_ENTRIES: usize = 50;

/// Raw (pre-base64) bytes per `tts_response` chunk. Chosen so the
/// base64-encoded chunk (~4/3 inflation) plus the small JSON envelope stays
/// comfortably under mist's ~16KB per-message safe limit, mirroring
/// tc-translate's oai-tunnel chunk sizing (12KB base64 ~= 9KB raw).
const TTS_CHUNK_RAW_BYTES: usize = 9 * 1024;

/// Maximum total bytes buffered while reassembling one `stt_request`
/// stream (across all its chunks) before giving up and replying
/// `voice_error`. Guards against a misbehaving/malicious peer growing
/// memory unboundedly by never sending `last: true`.
const STT_MAX_BUFFERED_BYTES: usize = 25 * 1024 * 1024;

/// Boxed future returned by a voice call closure.
type VoiceFuture<T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send>>;

/// Synthesizes speech: `(text, model_override, voice_override, lang_hint)`
/// -> audio. Built in `super::provide_start` from the resolved
/// `ai.tts_preset_id` preset (which supplies the default model/voice, and
/// per-language voice overrides, when the request omits them -- see
/// `super::resolve_tts_voice`); `None` on [`Provider`] means this node
/// doesn't offer TTS. `lang_hint` is the `tts_request.lang` BCP-47 tag
/// (mistllm-wire tts-lang-hint-v1), passed through unchanged for the
/// closure to resolve a voice from; it never overrides an explicit
/// `voice_override`.
pub type TtsCallFn = Arc<
    dyn Fn(String, Option<String>, Option<String>, Option<String>) -> VoiceFuture<TtsAudio>
        + Send
        + Sync,
>;

/// Transcribes speech: `(audio_bytes, mime, model_override, file_name)` ->
/// text. Built in `super::provide_start` from the resolved
/// `ai.stt_preset_id` preset; `None` on [`Provider`] means this node
/// doesn't offer STT.
pub type SttCallFn = Arc<
    dyn Fn(Vec<u8>, String, Option<String>, Option<String>) -> VoiceFuture<String> + Send + Sync,
>;

/// In-progress reassembly of one chunked `stt_request` stream, keyed by its
/// `id`. `mime`/`model`/`file_name` are captured from the first chunk only
/// (peers are not required to repeat them on every chunk).
struct SttBuffer {
    mime: String,
    model: Option<String>,
    file_name: Option<String>,
    bytes: Vec<u8>,
}

/// Outcome of [`Provider::resolve_llm_call`]: which upstream (if any)
/// should serve one `llm_request`.
enum LlmCallResolution {
    /// Use the default injected `call` closure, unchanged: either no
    /// `model` was requested, or this provider has no advertised list at
    /// all (legacy pass-through: any `model` is forwarded to `call`
    /// verbatim, unchecked).
    Default,
    /// The requested `model` named a preset in [`Provider::advertised`] --
    /// call that preset's own upstream directly instead of `call`.
    Resolved(ResolvedAiPreset),
    /// A `model` was requested, an advertised list *is* configured, but it
    /// matched none of it.
    Reject,
}

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

/// Maximum number of voice ids advertised in `provider_hello.voices`
/// (tts-voice-selection-v1 §2.1): keeps the hello payload well under mist's
/// ~16KB safe message limit even if an upstream catalog is huge. Extra
/// entries are silently truncated; per the spec this truncation is not
/// surfaced anywhere (a known v1 limitation).
const MAX_ADVERTISED_VOICES: usize = 64;

/// Message returned to a peer whose `llm_request.model` named neither
/// nothing nor one of this provider's advertised names, when an advertised
/// list *is* configured (see [`Provider::resolve_llm_call`]).
const MODEL_NOT_SHARED_MESSAGE: &str = "The requested model is not shared by this provider.";

/// Provider state: send fn, upstream call, advertised models, optional
/// voice calls, logs.
pub struct Provider {
    send: SendFn,
    call: LlmCallFn,
    models: Vec<String>,
    /// Advertised-name -> resolved preset (base_url/api_key/model/
    /// temperature/reasoning_effort), used by [`Provider::resolve_llm_call`]
    /// to route an `llm_request` whose `model` names one of `models` to
    /// *that preset's own* upstream, rather than always going through the
    /// single default `call` closure (whose upstream is fixed to the
    /// default preset's, and can't speak for a different preset that
    /// happens to point at a different provider). Empty means "no
    /// advertised list configured" -- the legacy pass-through mode where
    /// any `model` (or none) always goes through `call` verbatim, unchecked.
    advertised: HashMap<String, ResolvedAiPreset>,
    tts: Option<TtsCallFn>,
    stt: Option<SttCallFn>,
    /// TTS voice catalog to advertise in `provider_hello.voices`
    /// (tts-voice-selection-v1 §2.1/§3.5); only ever surfaced by
    /// [`Provider::hello`] when `tts` is also configured, regardless of
    /// whether this list is non-empty (see [`Provider::hello`]).
    voices: Vec<String>,
    stt_buffers: Mutex<HashMap<String, SttBuffer>>,
    logs: Mutex<Vec<RequestLog>>,
}

impl Provider {
    /// Constructs a provider, optionally wiring TTS/STT upstream calls (see
    /// the module doc and [`TtsCallFn`]/[`SttCallFn`]); pass `None, None`
    /// for a chat-only provider, where `tts_request`/`stt_request` always
    /// get an immediate `voice_error`. `voices` is the TTS voice catalog to
    /// advertise (see [`Provider::hello`]); pass an empty vec when unknown
    /// or when `tts` is `None`.
    #[allow(dead_code)] // retained for this module's own tests (empty advertised-table convenience)
    pub fn new_with_voice(
        send: SendFn,
        call: LlmCallFn,
        models: Vec<String>,
        tts: Option<TtsCallFn>,
        stt: Option<SttCallFn>,
        voices: Vec<String>,
    ) -> Arc<Self> {
        Self::new(send, call, models, HashMap::new(), tts, stt, voices)
    }

    /// Full constructor, additionally taking the advertised-name ->
    /// resolved-preset routing table (see [`Provider::advertised`] and
    /// [`Provider::resolve_llm_call`]). Prefer [`Provider::new_with_voice`]
    /// (an empty table -- legacy pass-through mode) when that routing isn't
    /// needed, as most of this module's own tests do.
    pub fn new(
        send: SendFn,
        call: LlmCallFn,
        models: Vec<String>,
        advertised: HashMap<String, ResolvedAiPreset>,
        tts: Option<TtsCallFn>,
        stt: Option<SttCallFn>,
        voices: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            send,
            call,
            models,
            advertised,
            tts,
            stt,
            voices,
            stt_buffers: Mutex::new(HashMap::new()),
            logs: Mutex::new(Vec::new()),
        })
    }

    /// Models advertised in `provider_hello`.
    pub fn models(&self) -> Vec<String> {
        self.models.clone()
    }

    /// Services this provider actually offers right now: always `"chat"`,
    /// plus `"tts"`/`"stt"` exactly when the corresponding closure is
    /// configured. Backs both [`Provider::hello`] (the wire announcement)
    /// and `super::status`'s `services` field, so the dashboard reflects
    /// the same capability set peers see in `provider_hello`.
    pub fn services(&self) -> Vec<String> {
        let mut services = vec![super::protocol::SERVICE_CHAT.to_string()];
        if self.tts.is_some() {
            services.push(super::protocol::SERVICE_TTS.to_string());
        }
        if self.stt.is_some() {
            services.push(super::protocol::SERVICE_STT.to_string());
        }
        services
    }

    /// The `provider_hello` announcement: `models` field included only
    /// when the list is non-empty (mistai omits it otherwise); `services`
    /// is always present (rather than relying on the wire spec's "missing
    /// == chat only" default), self-describing this provider's actual
    /// capabilities to `services`-aware peers -- see [`Provider::services`].
    /// `voices` is included only when this provider actually offers TTS
    /// (`tts` configured) *and* has a non-empty catalog to advertise
    /// (tts-voice-selection-v1 §2.1: "`services` に `tts` を広告する
    /// provider のみが `voices` を広告してよい"); the list is truncated to
    /// [`MAX_ADVERTISED_VOICES`] entries.
    pub fn hello(&self) -> ProtocolMessage {
        let models = if self.models.is_empty() {
            None
        } else {
            Some(self.models.clone())
        };
        let voices = if self.tts.is_some() && !self.voices.is_empty() {
            Some(
                self.voices
                    .iter()
                    .take(MAX_ADVERTISED_VOICES)
                    .cloned()
                    .collect(),
            )
        } else {
            None
        };
        ProtocolMessage::ProviderHello {
            models,
            services: Some(self.services()),
            voices,
        }
    }

    /// Handle one decoded inbound message (`llm_request` / `consumer_hello`
    /// / `tts_request` / `stt_request`; everything else ignored). Runs the
    /// upstream call inline (callers spawn this future per message, so
    /// concurrent requests don't block each other).
    pub async fn handle_message(self: Arc<Self>, from: String, msg: ProtocolMessage) {
        match msg {
            ProtocolMessage::ConsumerHello => {
                (self.send)(&from, self.hello());
            }
            ProtocolMessage::LlmRequest {
                id,
                messages,
                model,
            } => {
                self.handle_llm_request(from, id, messages, model).await;
            }
            ProtocolMessage::TtsRequest {
                id,
                text,
                model,
                voice,
                lang,
            } => {
                self.handle_tts_request(from, id, text, model, voice, lang)
                    .await;
            }
            ProtocolMessage::SttRequest {
                id,
                seq,
                data,
                last,
                mime,
                model,
                file_name,
            } => {
                self.handle_stt_request(from, id, seq, data, last, mime, model, file_name)
                    .await;
            }
            _ => {}
        }
    }

    /// No TTS/STT upstream configured, so the voice request is answered
    /// with an immediate `voice_error` instead of being dropped (which
    /// would otherwise leave the requester's `ConsumerClient` waiting until
    /// its own client-side voice timeout, e.g. mistai's 120s default). Sent
    /// through the same `send` fn (and thus the same ordered queue) as
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

    /// Synthesizes `text` via the configured TTS closure and replies with
    /// one or more `tts_response` chunks; `voice_error` (no `code`) on an
    /// upstream failure, or the usual `unsupported_service` rejection when
    /// no TTS closure is configured.
    #[allow(clippy::too_many_arguments)]
    async fn handle_tts_request(
        &self,
        from: String,
        id: String,
        text: String,
        model: Option<String>,
        voice: Option<String>,
        lang: Option<String>,
    ) {
        let Some(tts) = &self.tts else {
            self.reject_voice_request(&from, id);
            return;
        };
        match tts(text, model, voice, lang).await {
            Ok(audio) => self.send_tts_response(&from, id, audio),
            Err(err) => {
                (self.send)(
                    &from,
                    ProtocolMessage::VoiceError {
                        id,
                        message: err.to_string(),
                        code: None,
                    },
                );
            }
        }
    }

    /// Splits `audio.bytes` into [`TTS_CHUNK_RAW_BYTES`]-sized chunks and
    /// sends one `tts_response` per chunk (`last: true` on the final one --
    /// a single chunk, possibly empty, if the audio is small).
    fn send_tts_response(&self, from: &str, id: String, audio: TtsAudio) {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;
        let chunks: Vec<&[u8]> = audio.bytes.chunks(TTS_CHUNK_RAW_BYTES).collect();
        if chunks.is_empty() {
            (self.send)(
                from,
                ProtocolMessage::TtsResponse {
                    id,
                    seq: 0,
                    data: String::new(),
                    last: true,
                    mime: audio.mime,
                },
            );
            return;
        }
        let last_idx = chunks.len() - 1;
        for (seq, chunk) in chunks.into_iter().enumerate() {
            (self.send)(
                from,
                ProtocolMessage::TtsResponse {
                    id: id.clone(),
                    seq: seq as u64,
                    data: engine.encode(chunk),
                    last: seq == last_idx,
                    mime: audio.mime.clone(),
                },
            );
        }
    }

    /// Reassembles one chunk of an `stt_request` stream (keyed by `id`)
    /// into `stt_buffers`; once a chunk with `last == true` arrives,
    /// transcribes the full buffer via the configured STT closure and
    /// replies `stt_response` (or `voice_error` on upstream failure /
    /// buffer overflow). `seq` is not used for reordering: chunks for one
    /// `id` arrive from a single sender through mist's ordered per-room
    /// delivery, mirrored by this provider's own single-writer `SendFn`
    /// queue on the reply side.
    #[allow(clippy::too_many_arguments)]
    async fn handle_stt_request(
        &self,
        from: String,
        id: String,
        seq: u64,
        data: String,
        last: bool,
        mime: String,
        model: Option<String>,
        file_name: Option<String>,
    ) {
        let _ = seq;
        let Some(stt) = self.stt.clone() else {
            self.reject_voice_request(&from, id);
            return;
        };

        use base64::Engine as _;
        let decoded = match base64::engine::general_purpose::STANDARD.decode(&data) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.stt_buffers
                    .lock()
                    .expect("ai stt buffer lock")
                    .remove(&id);
                (self.send)(
                    &from,
                    ProtocolMessage::VoiceError {
                        id,
                        message: format!("ai: invalid stt_request chunk encoding: {err}"),
                        code: None,
                    },
                );
                return;
            }
        };

        enum ChunkOutcome {
            Waiting,
            Overflowed,
            Ready(SttBuffer),
        }
        let outcome = {
            let mut buffers = self.stt_buffers.lock().expect("ai stt buffer lock");
            let buffer = buffers.entry(id.clone()).or_insert_with(|| SttBuffer {
                mime,
                model,
                file_name,
                bytes: Vec::new(),
            });
            buffer.bytes.extend_from_slice(&decoded);
            if buffer.bytes.len() > STT_MAX_BUFFERED_BYTES {
                buffers.remove(&id);
                ChunkOutcome::Overflowed
            } else if last {
                ChunkOutcome::Ready(buffers.remove(&id).expect("just inserted above"))
            } else {
                ChunkOutcome::Waiting
            }
        };

        let buffer = match outcome {
            ChunkOutcome::Waiting => return,
            ChunkOutcome::Overflowed => {
                (self.send)(
                    &from,
                    ProtocolMessage::VoiceError {
                        id,
                        message: "ai: stt audio exceeded the maximum buffered size".to_string(),
                        code: None,
                    },
                );
                return;
            }
            ChunkOutcome::Ready(buffer) => buffer,
        };

        match stt(buffer.bytes, buffer.mime, buffer.model, buffer.file_name).await {
            Ok(text) => (self.send)(&from, ProtocolMessage::SttResponse { id, text }),
            Err(err) => {
                (self.send)(
                    &from,
                    ProtocolMessage::VoiceError {
                        id,
                        message: err.to_string(),
                        code: None,
                    },
                );
            }
        }
    }

    /// Which upstream (if any) should serve one `llm_request.model`, per
    /// mistllm-wire's "advertised name = preset label" contract: a bare
    /// `model` mismatch never falls back to the default preset, it's
    /// rejected outright, so a peer can't accidentally get an unrelated
    /// preset's answer under a name it didn't ask for.
    fn resolve_llm_call(&self, model: &Option<String>) -> LlmCallResolution {
        let Some(name) = model else {
            // No model requested: always the default preset's upstream,
            // whether or not an advertised list is configured.
            return LlmCallResolution::Default;
        };
        if self.advertised.is_empty() {
            // Legacy pass-through mode: no advertised list configured at
            // all, so there is nothing to check the name against -- forward
            // it to the default upstream verbatim, unchanged from before
            // this routing table existed.
            return LlmCallResolution::Default;
        }
        match self.advertised.get(name) {
            Some(resolved) => LlmCallResolution::Resolved(resolved.clone()),
            None => LlmCallResolution::Reject,
        }
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

        let resolution = self.resolve_llm_call(&model);
        if matches!(resolution, LlmCallResolution::Reject) {
            let message = MODEL_NOT_SHARED_MESSAGE.to_string();
            (self.send)(
                &from,
                ProtocolMessage::LlmError {
                    id: id.clone(),
                    message: message.clone(),
                    code: Some(super::protocol::CODE_MODEL_NOT_SHARED.to_string()),
                },
            );
            self.push_log(RequestLog {
                id,
                from,
                model,
                status: "error".into(),
                started_at,
                char_count: 0,
                detail: Some(message),
            });
            return;
        }

        let (delta_tx, mut delta_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let call_fut: LlmCallFuture = match resolution {
            LlmCallResolution::Resolved(resolved) => {
                // A named preset may point at a *different* provider than
                // the default preset's `call` closure was built from, so
                // this calls the upstream directly against the resolved
                // preset's own connection info instead of reusing `call`.
                let call_model = resolved.model.clone();
                let upstream = UpstreamConfig {
                    base_url: resolved.base_url,
                    api_key: resolved.api_key,
                    model: Some(resolved.model),
                    temperature: resolved.temperature,
                    reasoning_effort: resolved.reasoning_effort,
                };
                Box::pin(async move {
                    openai::stream_chat_completion(
                        &upstream,
                        &messages,
                        Some(&call_model),
                        Some(delta_tx),
                    )
                    .await
                })
            }
            LlmCallResolution::Default => (self.call)(messages, model.clone(), Some(delta_tx)),
            LlmCallResolution::Reject => unreachable!("handled and returned above"),
        };
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
                    self.record_chunk(
                        &from,
                        &id,
                        &model,
                        &started_at,
                        delta,
                        &mut seq,
                        &mut char_count,
                    );
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
        Arc::new(move |_messages, _model, _delta_tx| {
            Box::pin(async move { anyhow::bail!(message) })
        })
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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

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
                assert_eq!(
                    *code, None,
                    "generic upstream failure should not carry a code"
                );
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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);
        assert_eq!(
            provider.hello(),
            ProtocolMessage::ProviderHello {
                models: None,
                services: Some(vec!["chat".into()]),
                voices: None,
            }
        );
    }

    #[test]
    fn hello_includes_models_when_present() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec!["gpt-4o".into(), "gpt-4o-mini".into()],
            None,
            None,
            vec![],
        );
        assert_eq!(
            provider.hello(),
            ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".into(), "gpt-4o-mini".into()]),
                services: Some(vec!["chat".into()]),
                voices: None,
            }
        );
    }

    #[test]
    fn hello_always_advertises_chat_service() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);
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
        let provider = Provider::new_with_voice(send, call, vec!["m1".into()], None, None, vec![]);

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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::TtsRequest {
                    id: "req1".into(),
                    text: "hello there".into(),
                    model: None,
                    voice: None,
                    lang: None,
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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

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
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::TtsRequest {
                    id: "voice-req".into(),
                    text: "hi".into(),
                    model: None,
                    voice: None,
                    lang: None,
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
        assert!(
            matches!(&sent[1].1, ProtocolMessage::LlmResponseChunk { id, .. } if id == "llm-req")
        );
        assert!(
            matches!(&sent[2].1, ProtocolMessage::LlmResponseChunk { id, .. } if id == "llm-req")
        );
        assert!(
            matches!(&sent[3].1, ProtocolMessage::LlmResponseDone { id, .. } if id == "llm-req")
        );

        // The voice request left no log entry; only the llm_request did.
        let logs = provider.logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].id, "llm-req");
    }

    #[tokio::test]
    async fn call_upstream_bypasses_network() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec!["x"], "x");
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);

        let (delta_tx, mut delta_rx) = tokio::sync::mpsc::unbounded_channel();
        let content = provider
            .call_upstream(messages(), None, Some(delta_tx))
            .await
            .unwrap();

        assert_eq!(content, "x");
        assert_eq!(delta_rx.try_recv().unwrap(), "x");
        assert!(
            sent.lock().unwrap().is_empty(),
            "call_upstream must not touch the network"
        );
    }

    fn sample_resolved_preset() -> ResolvedAiPreset {
        ResolvedAiPreset {
            base_url: "http://upstream.invalid/v1".to_string(),
            api_key: "sk-test".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
        }
    }

    #[test]
    fn resolve_llm_call_is_default_when_no_model_requested() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let mut advertised = HashMap::new();
        advertised.insert("Chat".to_string(), sample_resolved_preset());
        let provider = Provider::new(
            send,
            call,
            vec!["Chat".into()],
            advertised,
            None,
            None,
            vec![],
        );
        assert!(matches!(
            provider.resolve_llm_call(&None),
            LlmCallResolution::Default
        ));
    }

    #[test]
    fn resolve_llm_call_is_default_in_legacy_mode_with_no_advertised_table() {
        // No advertised list configured at all: any model (even one that
        // matches nothing) is passed straight through, unchecked -- the
        // pre-existing legacy pass-through behavior.
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);
        assert!(matches!(
            provider.resolve_llm_call(&Some("anything".to_string())),
            LlmCallResolution::Default
        ));
    }

    #[test]
    fn resolve_llm_call_resolves_a_matching_advertised_name() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let mut advertised = HashMap::new();
        advertised.insert("Chat".to_string(), sample_resolved_preset());
        let provider = Provider::new(
            send,
            call,
            vec!["Chat".into()],
            advertised,
            None,
            None,
            vec![],
        );
        match provider.resolve_llm_call(&Some("Chat".to_string())) {
            LlmCallResolution::Resolved(resolved) => assert_eq!(resolved.model, "gpt-4o"),
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn resolve_llm_call_rejects_an_unmatched_name_when_advertised_table_is_non_empty() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let mut advertised = HashMap::new();
        advertised.insert("Chat".to_string(), sample_resolved_preset());
        let provider = Provider::new(
            send,
            call,
            vec!["Chat".into()],
            advertised,
            None,
            None,
            vec![],
        );
        assert!(matches!(
            provider.resolve_llm_call(&Some("not-advertised".to_string())),
            LlmCallResolution::Reject
        ));
    }

    #[tokio::test]
    async fn llm_request_named_but_unshared_model_is_rejected_without_calling_upstream() {
        let (send, sent) = fake_send();
        // This closure must never run: a rejected request must short-circuit
        // before reaching either the default closure or any upstream call.
        let call = fake_call_error("default closure must not be used for a rejected model");
        let mut advertised = HashMap::new();
        advertised.insert("Chat".to_string(), sample_resolved_preset());
        let provider = Provider::new(
            send,
            call,
            vec!["Chat".into()],
            advertised,
            None,
            None,
            vec![],
        );

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: messages(),
                    model: Some("not-advertised".into()),
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "expected exactly one reply, got: {sent:?}");
        match &sent[0] {
            (to, ProtocolMessage::LlmError { id, message, code }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert_eq!(
                    message,
                    "The requested model is not shared by this provider."
                );
                assert_eq!(code.as_deref(), Some("model_not_shared"));
            }
            other => panic!("expected an llm_error reply, got: {other:?}"),
        }

        let logs = provider.logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].status, "error");
    }

    /// End-to-end: a matching advertised name routes to *that preset's own*
    /// upstream (a real HTTP call against a mock server bound to a
    /// different address than any "default" closure would use), proving
    /// `handle_llm_request` bypasses the injected `call` closure entirely
    /// for the `Resolved` case rather than only ever using its fixed
    /// upstream. Also asserts the *real* model id (not the advertised
    /// label) is what actually reaches the upstream request body.
    #[tokio::test]
    async fn llm_request_with_a_matching_advertised_name_calls_that_presets_own_upstream() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn read_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            loop {
                let n = socket.read(&mut tmp).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(header_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let content_length: usize = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if buf.len() - (header_end + 4) >= content_length {
                        break;
                    }
                }
            }
            buf
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            let body = r#"{"choices":[{"message":{"content":"hi from resolved preset"}}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            let _ = socket.shutdown().await;
            request
        });

        let (send, sent) = fake_send();
        let call =
            fake_call_error("default closure must not be used for a resolved advertised model");
        let mut advertised = HashMap::new();
        advertised.insert(
            "Chat".to_string(),
            ResolvedAiPreset {
                base_url: format!("http://{addr}"),
                api_key: "sk-resolved".to_string(),
                model: "real-upstream-model".to_string(),
                temperature: None,
                reasoning_effort: None,
                voice: None,
                lang_voices: HashMap::new(),
            },
        );
        let provider = Provider::new(
            send,
            call,
            vec!["Chat".into()],
            advertised,
            None,
            None,
            vec![],
        );

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: messages(),
                    model: Some("Chat".into()),
                },
            )
            .await;

        let raw_request = server.await.unwrap();
        let request_text = String::from_utf8_lossy(&raw_request);
        assert!(
            request_text.contains("real-upstream-model"),
            "the resolved preset's own model id must reach the upstream body: {request_text}"
        );
        assert!(
            !request_text.contains("\"Chat\""),
            "the advertised *label* must never reach the upstream body: {request_text}"
        );

        let sent = sent.lock().unwrap();
        match sent.last().unwrap() {
            (to, ProtocolMessage::LlmResponseDone { id, content }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert_eq!(content.as_deref(), Some("hi from resolved preset"));
            }
            other => panic!("expected llm_response_done, got: {other:?}"),
        }
    }

    fn fake_tts_success(bytes: Vec<u8>, mime: &'static str) -> TtsCallFn {
        Arc::new(move |_text, _model, _voice, _lang| {
            let bytes = bytes.clone();
            Box::pin(async move {
                Ok(TtsAudio {
                    bytes,
                    mime: mime.to_string(),
                })
            })
        })
    }

    fn fake_tts_error(message: &'static str) -> TtsCallFn {
        Arc::new(move |_text, _model, _voice, _lang| {
            Box::pin(async move { anyhow::bail!(message) })
        })
    }

    type TtsCallArgs = (String, Option<String>, Option<String>, Option<String>);

    /// Records the exact `(text, model, voice, lang)` tuple `handle_message`
    /// hands to the TTS closure, so dispatch-level plumbing of the new
    /// `lang` hint (voice *resolution* itself lives in `super::mod.rs` and
    /// is tested there against `resolve_tts_voice` directly) can be
    /// asserted end to end through `Provider::handle_message`.
    fn fake_tts_capturing(captured: Arc<Mutex<Option<TtsCallArgs>>>) -> TtsCallFn {
        Arc::new(move |text, model, voice, lang| {
            let captured = captured.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((text, model, voice, lang));
                Ok(TtsAudio {
                    bytes: vec![],
                    mime: "audio/mpeg".to_string(),
                })
            })
        })
    }

    type SttCallArgs = (Vec<u8>, String, Option<String>, Option<String>);

    fn fake_stt_success(
        text: &'static str,
        captured: Arc<Mutex<Option<SttCallArgs>>>,
    ) -> SttCallFn {
        Arc::new(move |audio, mime, model, file_name| {
            let text = text.to_string();
            let captured = captured.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some((audio, mime, model, file_name));
                Ok(text)
            })
        })
    }

    fn fake_stt_error(message: &'static str) -> SttCallFn {
        Arc::new(move |_audio, _mime, _model, _file_name| {
            Box::pin(async move { anyhow::bail!(message) })
        })
    }

    #[test]
    fn hello_advertises_tts_and_stt_when_configured() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            Some(fake_tts_success(vec![1, 2, 3], "audio/mpeg")),
            Some(fake_stt_success("hi", Arc::new(Mutex::new(None)))),
            vec![],
        );
        match provider.hello() {
            ProtocolMessage::ProviderHello { services, .. } => {
                assert_eq!(
                    services,
                    Some(vec![
                        "chat".to_string(),
                        "tts".to_string(),
                        "stt".to_string()
                    ])
                );
            }
            other => panic!("expected ProviderHello, got {other:?}"),
        }
    }

    #[test]
    fn hello_advertises_voices_when_tts_configured_with_a_voice_catalog() {
        // Mirrors mod.rs's build_provider wiring: a tts_preset_id whose
        // resolved preset has a `voice` set produces a one-element
        // `voices` catalog (tts-voice-selection-v1 §2.1/§3.5 minimal
        // implementation).
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            Some(fake_tts_success(vec![1, 2, 3], "audio/mpeg")),
            None,
            vec!["alloy".to_string()],
        );
        assert_eq!(
            provider.hello(),
            ProtocolMessage::ProviderHello {
                models: None,
                services: Some(vec!["chat".to_string(), "tts".to_string()]),
                voices: Some(vec!["alloy".to_string()]),
            }
        );
    }

    #[test]
    fn hello_omits_voices_when_tts_not_configured() {
        // Even if a `voices` list were somehow passed in, it must not be
        // advertised unless this provider actually offers "tts" -- the
        // wire spec's advertising condition (§2.1: "services に tts を広告
        // する provider のみが voices を広告してよい").
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider =
            Provider::new_with_voice(send, call, vec![], None, None, vec!["alloy".to_string()]);
        match provider.hello() {
            ProtocolMessage::ProviderHello { voices, .. } => {
                assert_eq!(voices, None);
            }
            other => panic!("expected ProviderHello, got {other:?}"),
        }
    }

    #[test]
    fn hello_omits_voices_when_catalog_empty() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            Some(fake_tts_success(vec![1, 2, 3], "audio/mpeg")),
            None,
            vec![],
        );
        match provider.hello() {
            ProtocolMessage::ProviderHello { voices, .. } => {
                assert_eq!(voices, None);
            }
            other => panic!("expected ProviderHello, got {other:?}"),
        }
    }

    #[test]
    fn hello_truncates_voices_to_max_advertised() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let many_voices: Vec<String> = (0..100).map(|i| format!("voice-{i}")).collect();
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            Some(fake_tts_success(vec![1, 2, 3], "audio/mpeg")),
            None,
            many_voices.clone(),
        );
        match provider.hello() {
            ProtocolMessage::ProviderHello {
                voices: Some(voices),
                ..
            } => {
                assert_eq!(voices.len(), MAX_ADVERTISED_VOICES);
                assert_eq!(voices, many_voices[..MAX_ADVERTISED_VOICES].to_vec());
            }
            other => panic!("expected ProviderHello with voices, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tts_request_with_configured_tts_synthesizes_and_replies_in_chunks() {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;

        // Spans 3 chunks: 2 full TTS_CHUNK_RAW_BYTES chunks + one partial.
        let audio_bytes: Vec<u8> = (0..(TTS_CHUNK_RAW_BYTES * 2 + 10))
            .map(|i| (i % 256) as u8)
            .collect();
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            Some(fake_tts_success(audio_bytes.clone(), "audio/mpeg")),
            None,
            vec![],
        );

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::TtsRequest {
                    id: "tts1".into(),
                    text: "read this".into(),
                    model: None,
                    voice: None,
                    lang: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 3, "expected 3 chunks, got: {sent:?}");
        let mut reassembled = Vec::new();
        for (i, (to, msg)) in sent.iter().enumerate() {
            assert_eq!(to, "consumer1");
            match msg {
                ProtocolMessage::TtsResponse {
                    id,
                    seq,
                    data,
                    last,
                    mime,
                } => {
                    assert_eq!(id, "tts1");
                    assert_eq!(*seq, i as u64);
                    assert_eq!(mime, "audio/mpeg");
                    assert_eq!(*last, i == 2);
                    reassembled.extend(engine.decode(data).unwrap());
                }
                other => panic!("expected a tts_response chunk, got: {other:?}"),
            }
        }
        assert_eq!(reassembled, audio_bytes);
    }

    #[tokio::test]
    async fn tts_request_lang_hint_is_forwarded_to_the_tts_closure() {
        let captured: Arc<Mutex<Option<TtsCallArgs>>> = Arc::new(Mutex::new(None));
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            Some(fake_tts_capturing(captured.clone())),
            None,
            vec![],
        );

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::TtsRequest {
                    id: "tts1".into(),
                    text: "read this".into(),
                    model: None,
                    voice: None,
                    lang: Some("ja-JP".into()),
                },
            )
            .await;

        let (text, model, voice, lang) = captured
            .lock()
            .unwrap()
            .clone()
            .expect("tts closure was called");
        assert_eq!(text, "read this");
        assert_eq!(model, None);
        assert_eq!(voice, None);
        assert_eq!(lang.as_deref(), Some("ja-JP"));
    }

    #[tokio::test]
    async fn tts_request_upstream_failure_sends_voice_error_without_code() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            Some(fake_tts_error("tts boom")),
            None,
            vec![],
        );

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::TtsRequest {
                    id: "tts1".into(),
                    text: "read this".into(),
                    model: None,
                    voice: None,
                    lang: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            (to, ProtocolMessage::VoiceError { id, message, code }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "tts1");
                assert_eq!(message, "tts boom");
                assert_eq!(*code, None, "an upstream failure should not carry a code");
            }
            other => panic!("expected a voice_error reply, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn stt_request_reassembles_chunks_and_replies_with_text() {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;

        let captured: Arc<Mutex<Option<SttCallArgs>>> = Arc::new(Mutex::new(None));
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            None,
            Some(fake_stt_success("hello world", captured.clone())),
            vec![],
        );

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::SttRequest {
                    id: "stt1".into(),
                    seq: 0,
                    data: engine.encode(b"hello "),
                    last: false,
                    mime: "audio/wav".into(),
                    model: Some("whisper-1".into()),
                    file_name: Some("clip.wav".into()),
                },
            )
            .await;
        // No reply yet -- still waiting for the last chunk.
        assert!(sent.lock().unwrap().is_empty());

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::SttRequest {
                    id: "stt1".into(),
                    seq: 1,
                    data: engine.encode(b"world"),
                    last: true,
                    // A second chunk's model/fileName must not override the
                    // first chunk's -- only the first chunk's fields are
                    // captured (see the module doc).
                    mime: "audio/wav".into(),
                    model: None,
                    file_name: None,
                },
            )
            .await;

        let (audio, mime, model, file_name) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(audio, b"hello world");
        assert_eq!(mime, "audio/wav");
        assert_eq!(model.as_deref(), Some("whisper-1"));
        assert_eq!(file_name.as_deref(), Some("clip.wav"));

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            (to, ProtocolMessage::SttResponse { id, text }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "stt1");
                assert_eq!(text, "hello world");
            }
            other => panic!("expected an stt_response reply, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn stt_request_upstream_failure_sends_voice_error_without_code() {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;

        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            None,
            Some(fake_stt_error("stt boom")),
            vec![],
        );

        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::SttRequest {
                    id: "stt1".into(),
                    seq: 0,
                    data: engine.encode(b"audio"),
                    last: true,
                    mime: "audio/wav".into(),
                    model: None,
                    file_name: None,
                },
            )
            .await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            (to, ProtocolMessage::VoiceError { id, message, code }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "stt1");
                assert_eq!(message, "stt boom");
                assert_eq!(*code, None, "an upstream failure should not carry a code");
            }
            other => panic!("expected a voice_error reply, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn stt_request_buffer_overflow_sends_voice_error_and_skips_upstream_call() {
        let captured: Arc<Mutex<Option<SttCallArgs>>> = Arc::new(Mutex::new(None));
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(
            send,
            call,
            vec![],
            None,
            Some(fake_stt_success("should not be reached", captured.clone())),
            vec![],
        );

        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;
        let oversized = vec![0u8; STT_MAX_BUFFERED_BYTES + 1];
        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::SttRequest {
                    id: "stt1".into(),
                    seq: 0,
                    data: engine.encode(&oversized),
                    last: false,
                    mime: "audio/wav".into(),
                    model: None,
                    file_name: None,
                },
            )
            .await;

        assert!(
            captured.lock().unwrap().is_none(),
            "stt upstream must not be called"
        );
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        match &sent[0] {
            (to, ProtocolMessage::VoiceError { id, message, code }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "stt1");
                assert!(message.contains("maximum buffered size"));
                assert_eq!(*code, None);
            }
            other => panic!("expected a voice_error reply, got: {other:?}"),
        }
    }
}
