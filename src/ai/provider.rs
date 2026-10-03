//! Provider side of the AI network, ported from mistai's `provider.ts`.
//!
//! Chat (`llm_request`) is always served once this `Provider` exists (see
//! `super::provide_start`). TTS/STT are served only when a `tts`/`stt`
//! closure was supplied at construction time ([`Provider::new_with_voice`],
//! wired from `ai.tts`/`ai.stt` in `super::provide_start`);
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
//! `services` advertised in [`Provider::hello`] always includes `"chat"` and
//! `"tools"` (tool calling is executed by the upstream) and additionally
//! `"tts"`/`"stt"` exactly when the corresponding closure is configured.
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
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::warn;

use crate::config::ResolvedModel;

use super::openai::ToolOptions;
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
/// `ai.tts` model reference (which supplies the default model/voice, and
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
/// `ai.stt` model reference; `None` on [`Provider`] means this node
/// doesn't offer STT.
pub type SttCallFn = Arc<
    dyn Fn(Vec<u8>, String, Option<String>, Option<String>) -> VoiceFuture<String> + Send + Sync,
>;

/// Total bytes buffered across *all* in-flight `stt_request` streams.
const STT_MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;
/// Maximum concurrently buffered `stt_request` streams per sending peer.
const STT_MAX_BUFFERS_PER_PEER: usize = 4;
/// Maximum concurrently buffered `stt_request` streams overall.
const STT_MAX_BUFFERS_TOTAL: usize = 32;
/// A buffer with no new chunk for this long is dropped (swept lazily on
/// every insert).
const STT_BUFFER_TTL: Duration = Duration::from_secs(60);

/// Maximum concurrent remote-driven upstream jobs (llm/tts/stt) overall.
const MAX_CONCURRENT_REMOTE_JOBS: usize = 8;
/// Maximum concurrent remote-driven upstream jobs per peer.
const MAX_JOBS_PER_PEER: usize = 2;
/// Maximum `messages` in one remote `llm_request`.
const MAX_LLM_MESSAGES: usize = 256;
/// Maximum total `content` bytes across one remote `llm_request`.
const MAX_LLM_CONTENT_BYTES: usize = 1024 * 1024;

const BUSY_MESSAGE: &str = "The provider is busy; try again later.";
const REQUEST_TOO_LARGE_MESSAGE: &str = "The request is too large for this provider.";

/// Checks the size limits of a remote `llm_request` before any upstream call.
fn validate_llm_messages(
    messages: &[super::protocol::ChatMessage],
    tools: &ToolOptions,
) -> Result<(), &'static str> {
    use super::protocol::{MAX_TOOL_CALLS, MAX_TOOLS, MAX_TOOLS_BYTES};
    if messages.len() > MAX_LLM_MESSAGES {
        return Err(REQUEST_TOO_LARGE_MESSAGE);
    }
    if let Some(t) = &tools.tools {
        let too_many = t.as_array().is_none_or(|a| a.len() > MAX_TOOLS);
        if too_many || t.to_string().len() > MAX_TOOLS_BYTES {
            return Err(REQUEST_TOO_LARGE_MESSAGE);
        }
    }
    let mut total: usize = 0;
    for m in messages {
        total = total.saturating_add(m.content.len());
        if let Some(calls) = &m.tool_calls {
            if calls.as_array().is_none_or(|a| a.len() > MAX_TOOL_CALLS) {
                return Err(REQUEST_TOO_LARGE_MESSAGE);
            }
            total = total.saturating_add(calls.to_string().len());
        }
    }
    if total > MAX_LLM_CONTENT_BYTES {
        return Err(REQUEST_TOO_LARGE_MESSAGE);
    }
    Ok(())
}

/// Turns an upstream failure into text that is safe to send to a remote
/// peer: the raw error can carry the upstream `base_url` and response body.
/// The detail is logged locally; errors that are not upstream transport or
/// HTTP failures (validation messages etc.) pass through unchanged.
fn peer_safe_error(err: &anyhow::Error) -> String {
    if let Some(http) = err
        .chain()
        .find_map(|e| e.downcast_ref::<openai::UpstreamHttpError>())
    {
        warn!(error = %err, "ai: upstream returned an error");
        return format!("upstream error (status {})", http.status);
    }
    if err
        .chain()
        .any(|e| e.downcast_ref::<reqwest::Error>().is_some())
    {
        warn!(error = %err, "ai: upstream unavailable");
        return "upstream unavailable".to_string();
    }
    err.to_string()
}

/// Limits remote-driven work: a global semaphore plus a per-peer in-flight
/// count. Permits are released when the returned [`JobGuard`] drops.
struct JobLimiter {
    global: Arc<tokio::sync::Semaphore>,
    per_peer: Arc<Mutex<HashMap<String, usize>>>,
}

struct JobGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
    per_peer: Arc<Mutex<HashMap<String, usize>>>,
    peer: String,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        let mut map = self.per_peer.lock().expect("ai job limiter lock");
        if let Some(n) = map.get_mut(&self.peer) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                map.remove(&self.peer);
            }
        }
    }
}

impl JobLimiter {
    fn new(global: usize) -> Self {
        Self {
            global: Arc::new(tokio::sync::Semaphore::new(global)),
            per_peer: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn try_acquire(&self, peer: &str, per_peer_max: usize) -> Option<JobGuard> {
        let mut map = self.per_peer.lock().expect("ai job limiter lock");
        if map.get(peer).copied().unwrap_or(0) >= per_peer_max {
            return None;
        }
        let permit = self.global.clone().try_acquire_owned().ok()?;
        *map.entry(peer.to_string()).or_insert(0) += 1;
        Some(JobGuard {
            _permit: permit,
            per_peer: self.per_peer.clone(),
            peer: peer.to_string(),
        })
    }
}

/// In-progress reassembly of one chunked `stt_request` stream, keyed by
/// `(from, id)`. `mime`/`model`/`file_name` are captured from the first
/// chunk only (peers are not required to repeat them on every chunk).
struct SttBuffer {
    mime: String,
    model: Option<String>,
    file_name: Option<String>,
    bytes: Vec<u8>,
    next_seq: u64,
    last_update: Instant,
}

/// Why a chunk was refused by [`SttBuffers::push`].
#[derive(Debug, PartialEq, Eq)]
enum SttReject {
    /// Per-stream size cap exceeded.
    StreamTooLarge,
    /// Global byte / buffer-count / per-peer caps exceeded.
    Busy,
    /// `seq` was not the next expected chunk index.
    OutOfOrder,
}

enum SttPush {
    Waiting,
    Ready(SttBuffer),
    Rejected(SttReject),
}

/// All in-flight `stt_request` reassembly buffers with their global caps.
#[derive(Default)]
struct SttBuffers {
    map: HashMap<(String, String), SttBuffer>,
    total_bytes: usize,
}

impl SttBuffers {
    fn remove(&mut self, from: &str, id: &str) -> Option<SttBuffer> {
        let buf = self.map.remove(&(from.to_string(), id.to_string()))?;
        self.total_bytes = self.total_bytes.saturating_sub(buf.bytes.len());
        Some(buf)
    }

    /// Drops buffers idle for longer than [`STT_BUFFER_TTL`].
    fn sweep(&mut self, now: Instant) {
        let expired: Vec<(String, String)> = self
            .map
            .iter()
            .filter(|(_, b)| now.saturating_duration_since(b.last_update) > STT_BUFFER_TTL)
            .map(|(k, _)| k.clone())
            .collect();
        for (from, id) in expired {
            self.remove(&from, &id);
        }
    }

    /// Drops every buffer belonging to `from` (peer left).
    fn drop_peer(&mut self, from: &str) {
        let ids: Vec<String> = self
            .map
            .keys()
            .filter(|(f, _)| f == from)
            .map(|(_, id)| id.clone())
            .collect();
        for id in ids {
            self.remove(from, &id);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        now: Instant,
        from: &str,
        id: &str,
        seq: u64,
        chunk: &[u8],
        last: bool,
        mime: String,
        model: Option<String>,
        file_name: Option<String>,
    ) -> SttPush {
        self.sweep(now);
        let key = (from.to_string(), id.to_string());
        let existing_len = match self.map.get(&key) {
            Some(buf) => {
                if buf.next_seq != seq {
                    self.remove(from, id);
                    return SttPush::Rejected(SttReject::OutOfOrder);
                }
                buf.bytes.len()
            }
            None => {
                if seq != 0 {
                    return SttPush::Rejected(SttReject::OutOfOrder);
                }
                let peer_count = self.map.keys().filter(|(f, _)| f == from).count();
                if self.map.len() >= STT_MAX_BUFFERS_TOTAL || peer_count >= STT_MAX_BUFFERS_PER_PEER
                {
                    return SttPush::Rejected(SttReject::Busy);
                }
                0
            }
        };
        if existing_len + chunk.len() > STT_MAX_BUFFERED_BYTES {
            self.remove(from, id);
            return SttPush::Rejected(SttReject::StreamTooLarge);
        }
        if self.total_bytes + chunk.len() > STT_MAX_TOTAL_BYTES {
            self.remove(from, id);
            return SttPush::Rejected(SttReject::Busy);
        }
        let buf = self.map.entry(key).or_insert_with(|| SttBuffer {
            mime,
            model,
            file_name,
            bytes: Vec::new(),
            next_seq: 0,
            last_update: now,
        });
        buf.bytes.extend_from_slice(chunk);
        buf.next_seq = seq.saturating_add(1);
        buf.last_update = now;
        self.total_bytes += chunk.len();
        if last {
            SttPush::Ready(self.remove(from, id).expect("just inserted above"))
        } else {
            SttPush::Waiting
        }
    }
}

/// Outcome of [`Provider::resolve_llm_call`]: which upstream (if any)
/// should serve one `llm_request`.
enum LlmCallResolution {
    /// Use the injected default call. It selects the enabled HTTP default
    /// or first usable shared reference without forwarding a peer's unknown id.
    Default,
    /// The requested `model` named a model reference in [`Provider::advertised`] --
    /// call that model reference's own upstream directly instead of `call`.
    Resolved(ResolvedModel),
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
    /// Raw shared model id -> exact enabled HTTP connection. Each room has
    /// its own table; shared_restricted also covers nonempty lists whose
    /// references are currently unavailable.
    advertised: HashMap<String, ResolvedModel>,
    shared_restricted: std::sync::atomic::AtomicBool,
    tts: Option<TtsCallFn>,
    stt: Option<SttCallFn>,
    /// TTS voice catalog to advertise in `provider_hello.voices`
    /// (tts-voice-selection-v1 §2.1/§3.5); only ever surfaced by
    /// [`Provider::hello`] when `tts` is also configured, regardless of
    /// whether this list is non-empty (see [`Provider::hello`]).
    voices: Vec<String>,
    stt_buffers: Mutex<SttBuffers>,
    limiter: JobLimiter,
    logs: Mutex<Vec<RequestLog>>,
    /// Last `consumer_hello` reply per peer, so a peer spamming hellos
    /// cannot fill the shared send queue and starve other consumers' chunks.
    hello_replied: Mutex<HashMap<String, Instant>>,
}

/// Minimum spacing between `provider_hello` replies to the same peer.
const HELLO_REPLY_INTERVAL: Duration = Duration::from_secs(5);
/// Bound on tracked peers in `hello_replied` (stale entries pruned first).
const MAX_HELLO_TRACKED: usize = 1024;

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
    /// resolved-model reference routing table (see [`Provider::advertised`] and
    /// [`Provider::resolve_llm_call`]). Prefer [`Provider::new_with_voice`]
    /// (an empty table -- legacy pass-through mode) when that routing isn't
    /// needed, as most of this module's own tests do.
    pub fn new(
        send: SendFn,
        call: LlmCallFn,
        models: Vec<String>,
        advertised: HashMap<String, ResolvedModel>,
        tts: Option<TtsCallFn>,
        stt: Option<SttCallFn>,
        voices: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            send,
            call,
            models,
            shared_restricted: std::sync::atomic::AtomicBool::new(!advertised.is_empty()),
            advertised,
            tts,
            stt,
            voices,
            stt_buffers: Mutex::new(SttBuffers::default()),
            limiter: JobLimiter::new(MAX_CONCURRENT_REMOTE_JOBS),
            logs: Mutex::new(Vec::new()),
            hello_replied: Mutex::new(HashMap::new()),
        })
    }

    /// Whether a hello reply to `peer` is allowed now (and records it).
    fn hello_reply_allowed(&self, peer: &str) -> bool {
        let now = Instant::now();
        let mut seen = self.hello_replied.lock().expect("hello throttle lock");
        if let Some(last) = seen.get(peer)
            && now.duration_since(*last) < HELLO_REPLY_INTERVAL
        {
            return false;
        }
        if seen.len() >= MAX_HELLO_TRACKED {
            seen.retain(|_, t| now.duration_since(*t) < HELLO_REPLY_INTERVAL);
            if seen.len() >= MAX_HELLO_TRACKED {
                return false;
            }
        }
        seen.insert(peer.to_string(), now);
        true
    }

    /// Models advertised in `provider_hello`.
    pub fn models(&self) -> Vec<String> {
        self.models.clone()
    }

    /// Services this provider actually offers right now: always `"chat"` and `"tools"`,
    /// plus `"tts"`/`"stt"` exactly when the corresponding closure is
    /// configured. Backs both [`Provider::hello`] (the wire announcement)
    /// and `super::status`'s `services` field, so the dashboard reflects
    /// the same capability set peers see in `provider_hello`.
    pub fn services(&self) -> Vec<String> {
        // Tool calling rides along with chat: the upstream executes it.
        let mut services = vec![
            super::protocol::SERVICE_CHAT.to_string(),
            super::protocol::SERVICE_TOOLS.to_string(),
        ];
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
                if self.hello_reply_allowed(&from) {
                    (self.send)(&from, self.hello());
                }
            }
            ProtocolMessage::LlmRequest {
                id,
                messages,
                model,
                tools,
                tool_choice,
            } => {
                let tools = ToolOptions { tools, tool_choice };
                self.handle_llm_request(from, id, messages, tools, model)
                    .await;
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
        let Some(_job) = self.limiter.try_acquire(&from, MAX_JOBS_PER_PEER) else {
            (self.send)(
                &from,
                ProtocolMessage::VoiceError {
                    id,
                    message: BUSY_MESSAGE.to_string(),
                    code: None,
                },
            );
            return;
        };
        match tts(text, model, voice, lang).await {
            Ok(audio) => self.send_tts_response(&from, id, audio),
            Err(err) => {
                (self.send)(
                    &from,
                    ProtocolMessage::VoiceError {
                        id,
                        message: peer_safe_error(&err),
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
                    .remove(&from, &id);
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

        let outcome = self.stt_buffers.lock().expect("ai stt buffer lock").push(
            Instant::now(),
            &from,
            &id,
            seq,
            &decoded,
            last,
            mime,
            model,
            file_name,
        );

        let buffer = match outcome {
            SttPush::Waiting => return,
            SttPush::Rejected(reason) => {
                let message = match reason {
                    SttReject::StreamTooLarge => "ai: stt audio exceeded the maximum buffered size",
                    SttReject::Busy => BUSY_MESSAGE,
                    SttReject::OutOfOrder => "ai: stt_request chunk out of order",
                };
                (self.send)(
                    &from,
                    ProtocolMessage::VoiceError {
                        id,
                        message: message.to_string(),
                        code: None,
                    },
                );
                return;
            }
            SttPush::Ready(buffer) => buffer,
        };

        let Some(_job) = self.limiter.try_acquire(&from, MAX_JOBS_PER_PEER) else {
            (self.send)(
                &from,
                ProtocolMessage::VoiceError {
                    id,
                    message: BUSY_MESSAGE.to_string(),
                    code: None,
                },
            );
            return;
        };
        match stt(buffer.bytes, buffer.mime, buffer.model, buffer.file_name).await {
            Ok(text) => (self.send)(&from, ProtocolMessage::SttResponse { id, text }),
            Err(err) => {
                (self.send)(
                    &from,
                    ProtocolMessage::VoiceError {
                        id,
                        message: peer_safe_error(&err),
                        code: None,
                    },
                );
            }
        }
    }

    /// Drops any partially buffered `stt_request` streams of a peer that
    /// left the room.
    pub fn on_peer_left(&self, from: &str) {
        self.stt_buffers
            .lock()
            .expect("ai stt buffer lock")
            .drop_peer(from);
    }

    /// Preserve the room's explicit sharing boundary even if every ref is disabled.
    pub fn set_shared_restricted(&self, restricted: bool) {
        self.shared_restricted
            .store(restricted, std::sync::atomic::Ordering::Relaxed);
    }

    fn resolve_llm_call(&self, model: &Option<String>) -> LlmCallResolution {
        let Some(name) = model.as_deref().filter(|m| !m.trim().is_empty()) else {
            return LlmCallResolution::Default;
        };
        if let Some(resolved) = self.advertised.get(name) {
            return LlmCallResolution::Resolved(resolved.clone());
        }
        if self
            .shared_restricted
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            LlmCallResolution::Reject
        } else {
            LlmCallResolution::Default
        }
    }

    async fn handle_llm_request(
        &self,
        from: String,
        id: String,
        messages: Vec<super::protocol::ChatMessage>,
        tools: ToolOptions,
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

        let refusal = validate_llm_messages(&messages, &tools).err();
        let job = if refusal.is_none() {
            self.limiter.try_acquire(&from, MAX_JOBS_PER_PEER)
        } else {
            None
        };
        let refusal = refusal.or(if job.is_none() {
            Some(BUSY_MESSAGE)
        } else {
            None
        });
        if let Some(message) = refusal {
            let message = message.to_string();
            (self.send)(
                &from,
                ProtocolMessage::LlmError {
                    id: id.clone(),
                    message: message.clone(),
                    code: None,
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
        let _job = job;

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
                // A named model reference may point at a *different* provider than
                // the default model reference's `call` closure was built from, so
                // this calls the upstream directly against the resolved
                // model reference's own connection info instead of reusing `call`.
                let call_model = resolved.model.clone();
                let upstream = UpstreamConfig {
                    base_url: resolved.base_url,
                    api_key: resolved.api_key,
                    model: Some(resolved.model),
                    reasoning_effort: resolved.reasoning_effort,
                };
                Box::pin(async move {
                    openai::stream_chat_completion_tools(
                        &upstream,
                        &messages,
                        Some(&call_model),
                        &tools,
                        Some(delta_tx),
                    )
                    .await
                })
            }
            LlmCallResolution::Default => (self.call)(messages, tools, None, Some(delta_tx)),
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
            Ok(super::openai::ChatOutput {
                content,
                tool_calls,
            }) => {
                (self.send)(
                    &from,
                    ProtocolMessage::LlmResponseDone {
                        id: id.clone(),
                        content: Some(content.clone()),
                        tool_calls,
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
                let message = peer_safe_error(&err);
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

    /// Resolve a raw model id through the same sharing boundary as the wire path.
    #[cfg(test)]
    pub async fn call_upstream(
        &self,
        messages: Vec<super::protocol::ChatMessage>,
        tools: ToolOptions,
        model: Option<String>,
        delta_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    ) -> anyhow::Result<super::openai::ChatOutput> {
        match self.resolve_llm_call(&model) {
            LlmCallResolution::Resolved(resolved) => {
                // Same as the p2p path: a named model reference may point at a
                // different provider than `call` was built from, so call
                // that model reference's own connection info directly.
                let call_model = resolved.model.clone();
                let upstream = UpstreamConfig {
                    base_url: resolved.base_url,
                    api_key: resolved.api_key,
                    model: Some(resolved.model),
                    reasoning_effort: resolved.reasoning_effort,
                };
                openai::stream_chat_completion_tools(
                    &upstream,
                    &messages,
                    Some(&call_model),
                    &tools,
                    delta_tx,
                )
                .await
            }
            LlmCallResolution::Default => (self.call)(messages, tools, None, delta_tx).await,
            // Named a model that is not in the advertised list. The p2p
            // path answers `model_not_shared`; the local caller gets the
            // same refusal as an error rather than an unrelated model reference's
            // answer or a leaked upstream 404.
            LlmCallResolution::Reject => anyhow::bail!(MODEL_NOT_SHARED_MESSAGE),
        }
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
        Arc::new(move |_messages, _tools, _model, delta_tx| {
            let deltas = deltas.clone();
            let content = content.clone();
            Box::pin(async move {
                if let Some(tx) = &delta_tx {
                    for d in &deltas {
                        let _ = tx.send(d.clone());
                    }
                }
                Ok(content.into())
            })
        })
    }

    fn fake_call_error(message: &'static str) -> LlmCallFn {
        Arc::new(move |_messages, _tools, _model, _delta_tx| {
            Box::pin(async move { anyhow::bail!(message) })
        })
    }

    fn messages() -> Vec<ChatMessage> {
        vec![ChatMessage {
            tool_calls: None,
            tool_call_id: None,
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
                    tools: None,
                    tool_choice: None,
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
            (to, ProtocolMessage::LlmResponseDone { id, content, .. }) => {
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
                    tools: None,
                    tool_choice: None,
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
                services: Some(vec!["chat".into(), "tools".into()]),
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
                services: Some(vec!["chat".into(), "tools".into()]),
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
                assert_eq!(
                    services,
                    Some(vec!["chat".to_string(), "tools".to_string()])
                );
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
    async fn consumer_hello_replies_are_throttled_per_peer() {
        let (send, sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);
        for _ in 0..5 {
            provider
                .clone()
                .handle_message("spammer".into(), ProtocolMessage::ConsumerHello)
                .await;
        }
        provider
            .clone()
            .handle_message("other".into(), ProtocolMessage::ConsumerHello)
            .await;
        let sent = sent.lock().unwrap();
        assert_eq!(sent.iter().filter(|(to, _)| to == "spammer").count(), 1);
        assert_eq!(sent.iter().filter(|(to, _)| to == "other").count(), 1);
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
                    tools: None,
                    tool_choice: None,
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
                        tools: None,
                        tool_choice: None,
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
                    tools: None,
                    tool_choice: None,
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
                    tools: None,
                    tool_choice: None,
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
            .call_upstream(messages(), ToolOptions::default(), None, Some(delta_tx))
            .await
            .unwrap();

        assert_eq!(content.content, "x");
        assert_eq!(delta_rx.try_recv().unwrap(), "x");
        assert!(
            sent.lock().unwrap().is_empty(),
            "call_upstream must not touch the network"
        );
    }

    fn sample_resolved_model() -> ResolvedModel {
        ResolvedModel {
            base_url: "http://upstream.invalid/v1".to_string(),
            api_key: "sk-test".to_string(),
            model: "gpt-4o".to_string(),
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn call_upstream_resolves_an_advertised_name_rather_than_using_the_default_call() {
        // The local API server advertises the same names as
        // `provider_hello`, so one of those names must reach *that
        // model reference's* upstream. Before this was wired up the name went to
        // the injected default `call`, whose success value masked the
        // misrouting (and, against a real upstream, surfaced as a 404 for
        // a model id the upstream had never heard of).
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec!["from-default-call"], "from-default-call");
        let mut advertised = HashMap::new();
        advertised.insert("gpt-4o".to_string(), sample_resolved_model());
        let provider = Provider::new(
            send,
            call,
            vec!["gpt-4o".into()],
            advertised,
            None,
            None,
            vec![],
        );

        let result = provider
            .call_upstream(
                messages(),
                ToolOptions::default(),
                Some("gpt-4o".to_string()),
                None,
            )
            .await;

        // `sample_resolved_model`'s base_url is unroutable, so this errors
        // -- attempting it at all is the assertion: the default `call`
        // would have returned `Ok("from-default-call")`.
        assert!(
            result.is_err(),
            "an advertised name must go to its own model reference's upstream, not the default call"
        );
    }

    #[tokio::test]
    async fn call_upstream_rejects_a_name_that_is_not_advertised() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec!["x"], "x");
        let mut advertised = HashMap::new();
        advertised.insert("gpt-4o".to_string(), sample_resolved_model());
        let provider = Provider::new(
            send,
            call,
            vec!["gpt-4o".into()],
            advertised,
            None,
            None,
            vec![],
        );

        let error = provider
            .call_upstream(
                messages(),
                ToolOptions::default(),
                Some("not-shared".to_string()),
                None,
            )
            .await
            .expect_err("an unadvertised name must be refused, not forwarded upstream");
        assert!(error.to_string().contains(MODEL_NOT_SHARED_MESSAGE));
    }

    #[test]
    fn resolve_llm_call_routes_empty_model_to_default_closure() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let mut advertised = HashMap::new();
        advertised.insert("gpt-4o".to_string(), sample_resolved_model());
        let provider = Provider::new(
            send,
            call,
            vec!["gpt-4o".into()],
            advertised,
            None,
            None,
            vec![],
        );
        assert!(matches!(
            provider.resolve_llm_call(&None),
            LlmCallResolution::Default
        ));
        assert!(matches!(
            provider.resolve_llm_call(&Some(String::new())),
            LlmCallResolution::Default
        ));
    }

    #[test]
    fn resolve_llm_call_is_default_in_legacy_mode_with_no_advertised_table() {
        // No advertised list configured at all: the peer-supplied model is
        // ignored and the default model reference serves the request (see
        // `legacy_mode_ignores_the_peer_supplied_model`).
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
        advertised.insert("gpt-4o".to_string(), sample_resolved_model());
        let provider = Provider::new(
            send,
            call,
            vec!["gpt-4o".into()],
            advertised,
            None,
            None,
            vec![],
        );
        match provider.resolve_llm_call(&Some("gpt-4o".to_string())) {
            LlmCallResolution::Resolved(resolved) => assert_eq!(resolved.model, "gpt-4o"),
            _ => panic!("expected Resolved"),
        }
    }

    #[test]
    fn resolve_llm_call_rejects_an_unmatched_name_when_advertised_table_is_non_empty() {
        let (send, _sent) = fake_send();
        let call = fake_call_success(vec![], "");
        let mut advertised = HashMap::new();
        advertised.insert("gpt-4o".to_string(), sample_resolved_model());
        let provider = Provider::new(
            send,
            call,
            vec!["gpt-4o".into()],
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
        advertised.insert("gpt-4o".to_string(), sample_resolved_model());
        let provider = Provider::new(
            send,
            call,
            vec!["gpt-4o".into()],
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
                    tools: None,
                    tool_choice: None,
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

    /// End-to-end: a matching advertised name routes to *that model reference's own*
    /// upstream (a real HTTP call against a mock server bound to a
    /// different address than any "default" closure would use), proving
    /// `handle_llm_request` bypasses the injected `call` closure entirely
    /// for the `Resolved` case rather than only ever using its fixed
    /// upstream. Also asserts the *real* model id (not the advertised
    /// label) is what actually reaches the upstream request body.
    #[tokio::test]
    async fn llm_request_with_a_matching_raw_id_calls_that_models_own_upstream() {
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
            let body =
                r#"{"choices":[{"message":{"content":"hi from resolved model reference"}}]}"#;
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
            "gpt-4o".to_string(),
            ResolvedModel {
                base_url: format!("http://{addr}"),
                api_key: "sk-resolved".to_string(),
                model: "real-upstream-model".to_string(),
                reasoning_effort: None,
                voice: None,
                lang_voices: HashMap::new(),
            },
        );
        let provider = Provider::new(
            send,
            call,
            vec!["gpt-4o".into()],
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
                    tools: None,
                    tool_choice: None,
                    id: "req1".into(),
                    messages: messages(),
                    model: Some("gpt-4o".into()),
                },
            )
            .await;

        let raw_request = server.await.unwrap();
        let request_text = String::from_utf8_lossy(&raw_request);
        assert!(
            request_text.contains("real-upstream-model"),
            "the resolved model reference's own model id must reach the upstream body: {request_text}"
        );
        assert!(
            !request_text.contains("\"Chat\""),
            "the advertised *label* must never reach the upstream body: {request_text}"
        );

        let sent = sent.lock().unwrap();
        match sent.last().unwrap() {
            (to, ProtocolMessage::LlmResponseDone { id, content, .. }) => {
                assert_eq!(to, "consumer1");
                assert_eq!(id, "req1");
                assert_eq!(content.as_deref(), Some("hi from resolved model reference"));
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
                        "tools".to_string(),
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
        // Mirrors mod.rs's build_provider wiring: an ai.tts reference whose
        // resolved model reference has a `voice` set produces a one-element
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
                services: Some(vec![
                    "chat".to_string(),
                    "tools".to_string(),
                    "tts".to_string()
                ]),
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

    #[tokio::test]
    async fn legacy_mode_ignores_the_peer_supplied_model() {
        let seen: Arc<Mutex<Option<Option<String>>>> = Arc::new(Mutex::new(None));
        let seen2 = seen.clone();
        let call: LlmCallFn = Arc::new(move |_m, _tools, model, _tx| {
            *seen2.lock().unwrap() = Some(model);
            Box::pin(async { Ok("ok".to_string().into()) })
        });
        let (send, _sent) = fake_send();
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);
        provider
            .call_upstream(
                messages(),
                ToolOptions::default(),
                Some("gpt-expensive".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(seen.lock().unwrap().clone(), Some(None));
    }

    #[test]
    fn validate_llm_messages_caps_count_and_bytes() {
        let m = |n: usize| ChatMessage {
            tool_calls: None,
            tool_call_id: None,
            role: "user".into(),
            content: "x".repeat(n),
        };
        assert!(validate_llm_messages(&[m(10)], &ToolOptions::default()).is_ok());
        let many: Vec<_> = (0..=MAX_LLM_MESSAGES).map(|_| m(1)).collect();
        assert!(validate_llm_messages(&many, &ToolOptions::default()).is_err());
        assert!(
            validate_llm_messages(&[m(MAX_LLM_CONTENT_BYTES + 1)], &ToolOptions::default())
                .is_err()
        );
    }

    #[test]
    fn peer_safe_error_hides_upstream_http_detail() {
        let err = anyhow::Error::new(openai::UpstreamHttpError {
            label: "LLM API",
            status: 502,
            detail: "http://10.0.0.5/secret body".into(),
        });
        let msg = peer_safe_error(&err);
        assert_eq!(msg, "upstream error (status 502)");
        assert_eq!(peer_safe_error(&anyhow::anyhow!("plain")), "plain");
    }

    #[test]
    fn job_limiter_enforces_per_peer_and_global_limits() {
        let lim = JobLimiter::new(3);
        let a1 = lim.try_acquire("a", 2).expect("a1");
        let _a2 = lim.try_acquire("a", 2).expect("a2");
        assert!(lim.try_acquire("a", 2).is_none(), "per-peer cap");
        let _b1 = lim.try_acquire("b", 2).expect("b1");
        assert!(lim.try_acquire("c", 2).is_none(), "global cap");
        drop(a1);
        assert!(
            lim.try_acquire("c", 2).is_some(),
            "released permit reusable"
        );
    }

    fn push(
        b: &mut SttBuffers,
        now: Instant,
        from: &str,
        id: &str,
        seq: u64,
        n: usize,
        last: bool,
    ) -> SttPush {
        b.push(
            now,
            from,
            id,
            seq,
            &vec![0u8; n],
            last,
            "audio/wav".into(),
            None,
            None,
        )
    }

    #[test]
    fn stt_buffers_are_keyed_by_peer_and_id() {
        let mut b = SttBuffers::default();
        let now = Instant::now();
        assert!(matches!(
            push(&mut b, now, "a", "x", 0, 3, false),
            SttPush::Waiting
        ));
        // Same id from another peer is an independent stream.
        assert!(matches!(
            push(&mut b, now, "b", "x", 0, 5, false),
            SttPush::Waiting
        ));
        match push(&mut b, now, "a", "x", 1, 2, true) {
            SttPush::Ready(buf) => assert_eq!(buf.bytes.len(), 5),
            _ => panic!("expected Ready"),
        }
        assert_eq!(b.total_bytes, 5);
    }

    #[test]
    fn stt_buffers_reject_out_of_order_seq() {
        let mut b = SttBuffers::default();
        let now = Instant::now();
        assert!(matches!(
            push(&mut b, now, "a", "x", 1, 1, false),
            SttPush::Rejected(SttReject::OutOfOrder)
        ));
        assert!(matches!(
            push(&mut b, now, "a", "x", 0, 1, false),
            SttPush::Waiting
        ));
        assert!(matches!(
            push(&mut b, now, "a", "x", 2, 1, false),
            SttPush::Rejected(SttReject::OutOfOrder)
        ));
        assert_eq!(b.total_bytes, 0, "bad stream is dropped");
    }

    #[test]
    fn stt_buffers_cap_per_peer_and_total_counts() {
        let mut b = SttBuffers::default();
        let now = Instant::now();
        for i in 0..STT_MAX_BUFFERS_PER_PEER {
            assert!(matches!(
                push(&mut b, now, "a", &format!("i{i}"), 0, 1, false),
                SttPush::Waiting
            ));
        }
        assert!(matches!(
            push(&mut b, now, "a", "extra", 0, 1, false),
            SttPush::Rejected(SttReject::Busy)
        ));
        for i in 0..(STT_MAX_BUFFERS_TOTAL - STT_MAX_BUFFERS_PER_PEER) {
            assert!(matches!(
                push(&mut b, now, &format!("p{i}"), "x", 0, 1, false),
                SttPush::Waiting
            ));
        }
        assert!(matches!(
            push(&mut b, now, "late", "x", 0, 1, false),
            SttPush::Rejected(SttReject::Busy)
        ));
    }

    #[test]
    fn stt_buffers_cap_total_bytes() {
        let mut b = SttBuffers::default();
        let now = Instant::now();
        let per = STT_MAX_BUFFERED_BYTES;
        assert!(matches!(
            push(&mut b, now, "a", "x", 0, per, false),
            SttPush::Waiting
        ));
        assert!(matches!(
            push(&mut b, now, "b", "x", 0, per, false),
            SttPush::Waiting
        ));
        assert!(matches!(
            push(&mut b, now, "c", "x", 0, per, false),
            SttPush::Rejected(SttReject::Busy)
        ));
        assert!(matches!(
            push(&mut b, now, "a", "x", 1, 1, false),
            SttPush::Rejected(SttReject::StreamTooLarge)
        ));
    }

    #[test]
    fn stt_buffers_expire_and_drop_on_peer_leave() {
        let mut b = SttBuffers::default();
        let t0 = Instant::now();
        assert!(matches!(
            push(&mut b, t0, "a", "x", 0, 4, false),
            SttPush::Waiting
        ));
        assert!(matches!(
            push(&mut b, t0, "b", "y", 0, 4, false),
            SttPush::Waiting
        ));
        b.drop_peer("b");
        assert_eq!(b.map.len(), 1);
        assert_eq!(b.total_bytes, 4);
        // A later insert sweeps the stale buffer of "a".
        let later = t0 + STT_BUFFER_TTL + Duration::from_secs(1);
        assert!(matches!(
            push(&mut b, later, "c", "z", 0, 1, false),
            SttPush::Waiting
        ));
        assert_eq!(b.map.len(), 1);
        assert_eq!(b.total_bytes, 1);
    }

    // ---- tool calling -------------------------------------------------

    #[test]
    fn validate_llm_messages_enforces_tool_limits() {
        use crate::ai::protocol::{MAX_TOOL_CALLS, MAX_TOOLS, MAX_TOOLS_BYTES};
        let ok_tool = serde_json::json!({"type":"function","function":{"name":"f"}});
        let tools = |n: usize| ToolOptions {
            tools: Some(serde_json::Value::Array(vec![ok_tool.clone(); n])),
            tool_choice: None,
        };
        let msg = [ChatMessage::new("user", "x")];
        assert!(validate_llm_messages(&msg, &tools(MAX_TOOLS)).is_ok());
        assert!(validate_llm_messages(&msg, &tools(MAX_TOOLS + 1)).is_err());
        // Within the entry cap but over the byte cap.
        let fat = ToolOptions {
            tools: Some(serde_json::json!([{"description": "x".repeat(MAX_TOOLS_BYTES)}])),
            tool_choice: None,
        };
        assert!(validate_llm_messages(&msg, &fat).is_err());
        // Not an array at all.
        let bad = ToolOptions {
            tools: Some(serde_json::json!({})),
            tool_choice: None,
        };
        assert!(validate_llm_messages(&msg, &bad).is_err());

        let calls = |n: usize| ChatMessage {
            role: "assistant".into(),
            content: String::new(),
            tool_calls: Some(serde_json::Value::Array(vec![
                serde_json::json!({"id":"c"});
                n
            ])),
            tool_call_id: None,
        };
        assert!(validate_llm_messages(&[calls(MAX_TOOL_CALLS)], &ToolOptions::default()).is_ok());
        assert!(
            validate_llm_messages(&[calls(MAX_TOOL_CALLS + 1)], &ToolOptions::default()).is_err()
        );
    }

    #[test]
    fn hello_advertises_tools_with_chat() {
        let (send, _sent) = fake_send();
        let provider = Provider::new_with_voice(
            send,
            fake_call_success(vec![], ""),
            vec![],
            None,
            None,
            vec![],
        );
        match provider.hello() {
            ProtocolMessage::ProviderHello { services, .. } => {
                assert!(crate::ai::protocol::advertises_service(
                    &services,
                    crate::ai::protocol::SERVICE_TOOLS
                ));
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn tool_request_reaches_the_call_and_tool_calls_come_back_in_done() {
        let seen: Arc<Mutex<Option<(Vec<ChatMessage>, ToolOptions)>>> = Arc::new(Mutex::new(None));
        let seen2 = seen.clone();
        let calls = serde_json::json!([{"id":"c1","type":"function",
            "function":{"name":"get_weather","arguments":"{\"city\":\"Tokyo\"}"}}]);
        let calls2 = calls.clone();
        let call: LlmCallFn = Arc::new(move |m, t, _model, _tx| {
            *seen2.lock().unwrap() = Some((m, t));
            let calls = calls2.clone();
            Box::pin(async move {
                Ok(crate::ai::openai::ChatOutput {
                    content: String::new(),
                    tool_calls: Some(calls),
                })
            })
        });
        let (send, sent) = fake_send();
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);
        let tools = serde_json::json!([{"type":"function","function":{"name":"get_weather"}}]);
        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: vec![
                        ChatMessage::new("user", "weather?"),
                        ChatMessage {
                            role: "tool".into(),
                            content: "sunny".into(),
                            tool_calls: None,
                            tool_call_id: Some("c0".into()),
                        },
                    ],
                    model: None,
                    tools: Some(tools.clone()),
                    tool_choice: Some(serde_json::json!("auto")),
                },
            )
            .await;

        let (msgs, opts) = seen.lock().unwrap().clone().expect("call invoked");
        assert_eq!(msgs[1].tool_call_id.as_deref(), Some("c0"));
        assert_eq!(opts.tools, Some(tools));
        assert_eq!(opts.tool_choice, Some(serde_json::json!("auto")));

        let sent = sent.lock().unwrap();
        match &sent.last().unwrap().1 {
            ProtocolMessage::LlmResponseDone {
                id,
                content,
                tool_calls,
            } => {
                assert_eq!(id, "req1");
                assert_eq!(content.as_deref(), Some(""));
                assert_eq!(*tool_calls, Some(calls));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversized_tools_get_an_llm_error_and_no_upstream_call() {
        let called = Arc::new(Mutex::new(false));
        let called2 = called.clone();
        let call: LlmCallFn = Arc::new(move |_m, _t, _model, _tx| {
            *called2.lock().unwrap() = true;
            Box::pin(async { Ok("x".to_string().into()) })
        });
        let (send, sent) = fake_send();
        let provider = Provider::new_with_voice(send, call, vec![], None, None, vec![]);
        let tools = serde_json::Value::Array(vec![
            serde_json::json!({"type":"function"});
            crate::ai::protocol::MAX_TOOLS + 1
        ]);
        provider
            .clone()
            .handle_message(
                "consumer1".into(),
                ProtocolMessage::LlmRequest {
                    id: "req1".into(),
                    messages: messages(),
                    model: None,
                    tools: Some(tools),
                    tool_choice: None,
                },
            )
            .await;
        assert!(!*called.lock().unwrap());
        let sent = sent.lock().unwrap();
        assert!(matches!(
            &sent.last().unwrap().1,
            ProtocolMessage::LlmError { id, .. } if id == "req1"
        ));
    }
}
