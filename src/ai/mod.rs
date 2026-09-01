//! P2P AI network: consume or provide LLM inference over mistlib rooms,
//! wire-compatible with `@tik-choco/mistai` protocol v1 (tc-mistllm /
//! tc-translate / tc-note peers can share the room).
//!
//! Two roles, both optional and combinable:
//!
//! - **provide** (`ai provide start`): announce `provider_hello` and
//!   forward inbound `llm_request`s to the resolved default preset's
//!   upstream (`[ai] default_preset_id` -> `[[ai.presets]]` ->
//!   `[[ai.providers]]`, see `crate::config::resolve_preset`), streaming
//!   deltas back as chunks. TTS/STT are served the same way when
//!   `[ai] tts_preset_id`/`stt_preset_id` name a preset (independent of the
//!   chat preset, possibly a different provider) -- otherwise inbound
//!   `tts_request`/`stt_request`s get an immediate `voice_error` reply
//!   instead of silently going unanswered (see `provider.rs`).
//! - **serve** (`ai serve start`): run a local OpenAI-compatible HTTP API
//!   server; requests go to the local provider when one is running, else
//!   to the first provider discovered on the network. Point any OpenAI
//!   client at `http://<api_listen>/v1`.
//!
//! `ai chat` is a one-shot version of the same backend selection.
//!
//! The AI room falls back to `net::DEFAULT_ROOM` when `[ai] room_id` is
//! unset. `crate::net` supports multiple simultaneous rooms per process, so
//! `[ai] room_id` may name any room; the ai protocol coexists with the
//! daemon's other room protocols by message shape.

mod api_server;
mod consumer;
/// Opened to `pub(crate)` so `crate::bot`'s `summarize` transform can reuse
/// the upstream chat-completion client directly (`UpstreamConfig` +
/// `stream_chat_completion`) instead of re-implementing an OpenAI-compatible
/// client -- see the bot pipeline draft's summarize step.
pub(crate) mod openai;
/// Opened to `pub(crate)` for the same reason as [`openai`]: `crate::bot`
/// needs `ChatMessage` to build a `stream_chat_completion` request.
pub(crate) mod protocol;
mod provide_state;
mod provider;
mod serve_state;
mod stt;
pub mod tts;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, info, warn};

use crate::daemon::AppState;

use api_server::ApiServer;
use consumer::Consumer;
use openai::UpstreamConfig;
use protocol::{ChatMessage, ProtocolMessage};
use provider::{Provider, SttCallFn, TtsCallFn};

/// Ordered, fire-and-forget wire send: `(to_node_id, message)`. The
/// service backs this with a single queue-draining task, so call order is
/// send order (chunk streams stay ordered without relying on `seq` alone).
pub type SendFn = Arc<dyn Fn(&str, ProtocolMessage) + Send + Sync>;

/// Boxed future returned by [`LlmCallFn`].
pub type LlmCallFuture = Pin<Box<dyn Future<Output = Result<String>> + Send>>;

/// One chat completion: `(messages, model, delta_tx)` -> full content.
/// Deltas are streamed into `delta_tx` (when provided) as they arrive.
pub type LlmCallFn = Arc<
    dyn Fn(Vec<ChatMessage>, Option<String>, Option<UnboundedSender<String>>) -> LlmCallFuture
        + Send
        + Sync,
>;

/// Model list for `GET /v1/models`.
pub type ModelsFn = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// One text-to-speech call for `POST /v1/audio/speech`.
pub type TtsCallFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<tts::TtsAudio>> + Send>>;
/// `(params, lang)` — `lang` is a BCP-47 hint, the same one
/// `tts_request.lang` carries over the wire, feeding the shared voice
/// resolution chain.
pub type TtsFn = Arc<dyn Fn(tts::TtsParams, Option<String>) -> TtsCallFuture + Send + Sync>;

/// One transcription for `POST /v1/audio/transcriptions`.
pub type SttCallFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send>>;
pub type SttFn = Arc<dyn Fn(stt::SttParams) -> SttCallFuture + Send + Sync>;

/// How long discovery waits for a `provider_hello` (mistai default).
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on listing an upstream's voices from inside a synthesis request
/// (see `AiService::synthesize`). Short on purpose: the catalog only feeds
/// two fallback steps of the voice chain, so giving up on it costs a guess,
/// while waiting on an upstream with no voice-listing endpoint would cost
/// the whole request.
const VOICE_CATALOG_TIMEOUT: Duration = Duration::from_secs(5);

struct AiService {
    room: String,
    node_id: String,
    send: SendFn,
    consumer: Arc<Consumer>,
    provider: RwLock<Option<Arc<Provider>>>,
    api_server: Mutex<Option<Arc<ApiServer>>>,
    /// Inactivity timeout for p2p requests (resets per chunk).
    request_timeout: Duration,
}

static SERVICE: OnceCell<Arc<AiService>> = OnceCell::const_new();

async fn ensure_started(state: &Arc<AppState>) -> Result<Arc<AiService>> {
    let service = SERVICE
        .get_or_try_init(|| async { init_service(state).await })
        .await?;
    Ok(service.clone())
}

async fn init_service(state: &Arc<AppState>) -> Result<Arc<AiService>> {
    let config = state.config();
    let room = config
        .ai
        .room_id
        .clone()
        .unwrap_or_else(|| crate::net::DEFAULT_ROOM.to_string());
    let transport = crate::net::ensure_started(state, room)
        .await
        .context("ai: starting p2p transport")?;

    // Single-writer send queue: preserves cross-request send order.
    let (send_tx, mut send_rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, ProtocolMessage)>();
    let ai_room = transport.room.clone();
    tokio::spawn(async move {
        while let Some((to, msg)) = send_rx.recv().await {
            if let Err(err) = crate::net::send_direct(&ai_room, &to, protocol::encode(&msg)).await {
                debug!(%err, to = %to, "ai: send failed");
            }
        }
    });
    let send: SendFn = Arc::new(move |to: &str, msg: ProtocolMessage| {
        let _ = send_tx.send((to.to_string(), msg));
    });

    let service = Arc::new(AiService {
        room: transport.room.clone(),
        node_id: transport.node_id.clone(),
        send: send.clone(),
        consumer: Consumer::new(send),
        provider: RwLock::new(None),
        api_server: Mutex::new(None),
        request_timeout: Duration::from_secs(config.ai.request_timeout_secs.max(1)),
    });

    {
        let service = service.clone();
        let rt = tokio::runtime::Handle::current();
        crate::net::register_handler(move |event_type, from, data| match event_type {
            crate::net::EVENT_RAW => {
                let Some(msg) = protocol::decode(data) else {
                    return; // Not an ai protocol message; ignore.
                };
                let from = from.to_string();
                let service = service.clone();
                rt.spawn(async move {
                    service.consumer.handle_message(&from, &msg);
                    let provider = service.provider.read().expect("ai provider lock").clone();
                    if let Some(provider) = provider {
                        provider.handle_message(from, msg).await;
                    }
                });
            }
            crate::net::EVENT_JOIN => {
                // Announce both roles to the new peer, mirroring mistai's
                // per-peer hello behavior.
                let from = from.to_string();
                let service = service.clone();
                rt.spawn(async move {
                    (service.send)(&from, ProtocolMessage::ConsumerHello);
                    let provider = service.provider.read().expect("ai provider lock").clone();
                    if let Some(provider) = provider {
                        (service.send)(&from, provider.hello());
                    }
                });
            }
            crate::net::EVENT_LEAVE => service.consumer.on_peer_disconnected(from),
            _ => {}
        });
    }

    // Announce ourselves to anyone already connected.
    {
        let service = service.clone();
        tokio::spawn(async move {
            service.broadcast(ProtocolMessage::ConsumerHello).await;
        });
    }

    Ok(service)
}

impl AiService {
    /// Send `msg` to every currently-connected peer (mistlib native has no
    /// room broadcast; peers == room members).
    async fn broadcast(&self, msg: ProtocolMessage) {
        for node in crate::net::connected_nodes().await {
            (self.send)(&node, msg.clone());
        }
    }

    fn local_provider(&self) -> Option<Arc<Provider>> {
        self.provider.read().expect("ai provider lock").clone()
    }

    /// Backend selection shared by `ai chat` and the API server: local
    /// provider first, else the first provider discovered on the network.
    /// Returns `(content, via, remote_provider_id)`.
    async fn chat(
        &self,
        messages: Vec<ChatMessage>,
        model: Option<String>,
        delta_tx: Option<UnboundedSender<String>>,
    ) -> Result<(String, &'static str, Option<String>)> {
        if let Some(provider) = self.local_provider() {
            let content = provider.call_upstream(messages, model, delta_tx).await?;
            return Ok((content, "local", None));
        }

        let info = match self.consumer.provider() {
            Some(info) => info,
            None => {
                // Nudge providers to announce, then wait.
                self.broadcast(ProtocolMessage::ConsumerHello).await;
                self.consumer
                    .wait_for_provider(DISCOVERY_TIMEOUT)
                    .await
                    .context("ai: no provider found on the network")?
            }
        };
        let content = self
            .consumer
            .request(
                &info.node_id,
                messages,
                model,
                self.request_timeout,
                delta_tx,
            )
            .await?;
        Ok((content, "p2p", Some(info.node_id)))
    }

    /// Synthesize speech from this node's configured TTS preset.
    ///
    /// The voice counterpart of [`AiService::chat`], and deliberately only
    /// half of it: this resolves the **local** `ai.tts_preset_id` and calls
    /// its upstream. There is no p2p fallback yet — sending `tts_request`
    /// as a *consumer* is still unimplemented (the provider half has been
    /// there since the voice extension landed), so a node with no TTS
    /// preset of its own has nothing to fall back to and says so rather
    /// than hanging waiting for a peer it can't ask.
    async fn synthesize(
        &self,
        state: &Arc<AppState>,
        req: tts::TtsParams,
        lang: Option<String>,
    ) -> Result<tts::TtsAudio> {
        let cfg = state.config().ai.clone();
        let resolved = voice_preset_provider(&cfg, &cfg.tts_preset_id).context(
            "ai: no TTS preset configured (set `ai.tts_preset_id` to a preset id; \
             see `mistl config show`)",
        )?;
        let (provider, preset) = resolved.clone();

        // The request's own model/voice win when given, exactly as they do
        // over the wire (mistllm-wire's "provider voice/model respect
        // rules"): a caller that names a voice gets that voice, and the
        // preset only fills in what wasn't asked for.
        //
        // Voice specifically goes through `resolve_tts_voice` — the same
        // chain the wire path uses — rather than reading `preset.voice`
        // directly. Both doors advertise the same voices, so they have to
        // mean the same thing by them: a local caller must not get "tts
        // requires a voice" for a request the wire would have answered from
        // `lang_voices` or from the catalog.
        let request_voice = (!req.voice.trim().is_empty()).then(|| req.voice.clone());

        // The catalog is consulted by only two *fallback* steps of that
        // chain (the kokoro-shaped `lang` guess, and the last-resort first
        // entry), and fetching it costs an upstream round trip. The wire
        // path pays that once, when the provider is built; paying it here
        // would mean paying it on every single synthesis — and, worse,
        // hanging the request whenever that endpoint doesn't answer. So it
        // is fetched only when one of those two steps can actually be
        // reached, and never when the answer is already decided.
        let needs_catalog = request_voice.is_none() && (lang.is_some() || preset.voice.is_none());
        let catalog = if needs_catalog {
            // Bounded: being configured against a TTS upstream with no
            // voice-listing endpoint is a normal thing, and "no catalog" is
            // a fine answer — waiting forever is not.
            match tokio::time::timeout(
                VOICE_CATALOG_TIMEOUT,
                resolve_advertised_voices(Some(&resolved)),
            )
            .await
            {
                Ok(voices) => voices,
                Err(_) => {
                    tracing::warn!(
                        timeout_secs = VOICE_CATALOG_TIMEOUT.as_secs(),
                        "ai: timed out listing upstream voices; continuing without a catalog"
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        let voice = resolve_tts_voice(
            request_voice,
            lang.as_deref(),
            &preset.lang_voices,
            &catalog,
            &preset.voice,
        )
        .unwrap_or_default();

        let req = tts::TtsParams {
            model: if req.model.trim().is_empty() {
                preset.model.clone()
            } else {
                req.model
            },
            voice,
            ..req
        };
        tts::synthesize(&provider, req).await
    }

    /// Transcribe audio with this node's configured STT preset. Same
    /// local-only shape, and the same reason, as [`AiService::synthesize`].
    async fn transcribe(&self, state: &Arc<AppState>, req: stt::SttParams) -> Result<String> {
        let cfg = state.config().ai.clone();
        let (provider, preset) = voice_preset_provider(&cfg, &cfg.stt_preset_id).context(
            "ai: no STT preset configured (set `ai.stt_preset_id` to a preset id; \
             see `mistl config show`)",
        )?;
        let req = stt::SttParams {
            model: if req.model.trim().is_empty() {
                preset.model.clone()
            } else {
                req.model
            },
            ..req
        };
        stt::transcribe(&provider, req).await
    }
}

/// Looks up `provider_id` in `ai.providers` and builds an [`UpstreamConfig`]
/// for a one-off request against it directly -- no preset/model resolution
/// involved, unlike `crate::config::resolve_preset`. Returns an actionable
/// error if the provider isn't configured.
fn provider_upstream(ai: &crate::config::AiConfig, provider_id: &str) -> Result<UpstreamConfig> {
    let provider = ai
        .providers
        .iter()
        .find(|p| p.id == provider_id)
        .with_context(|| {
            format!(
                "ai: provider {provider_id:?} not found in ai.providers; add it with \
                 `mistl config set ai.providers <json>` (see `mistl config show`)"
            )
        })?;
    Ok(UpstreamConfig {
        base_url: provider.base_url.clone(),
        api_key: provider.api_key.clone(),
        model: None,
        temperature: None,
        reasoning_effort: None,
    })
}

/// IPC entry point for all `ai.*` commands.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    // Listing a provider's upstream models is a plain HTTP GET against its
    // configured `base_url` -- it doesn't touch the p2p AI network, so this
    // is handled before `ensure_started` (which joins the network room).
    if cmd == "ai.upstream_models" {
        let provider_id = args
            .get("provider_id")
            .and_then(Value::as_str)
            .context("ai.upstream_models requires `provider_id`")?;
        let upstream = provider_upstream(&state.config().ai, provider_id)?;
        let models = openai::fetch_models(&upstream).await?;
        return Ok(json!({ "provider_id": provider_id, "models": models }));
    }

    let service = ensure_started(state).await?;
    match cmd {
        "ai.status" => status(&service).await,
        "ai.chat" => {
            let prompt = args
                .get("prompt")
                .and_then(Value::as_str)
                .context("ai.chat requires `prompt`")?;
            let model = args.get("model").and_then(Value::as_str).map(String::from);
            let messages = vec![ChatMessage {
                role: "user".into(),
                content: prompt.to_string(),
            }];
            let (content, via, provider) = service.chat(messages, model, None).await?;
            Ok(json!({ "content": content, "via": via, "provider": provider }))
        }
        "ai.models" => models(&service).await,
        "ai.provide.start" => provide_start(&service, state).await,
        "ai.provide.stop" => {
            let stopped = service
                .provider
                .write()
                .expect("ai provider lock")
                .take()
                .is_some();
            // Explicit stop always clears the persisted intent, regardless
            // of `stopped` (idempotent: calling stop when already stopped
            // still means "don't auto-resume next time"). See
            // `provide_state`'s module doc for why this lives in its own
            // state file rather than `config.toml`.
            persist_provide_state(false);
            Ok(json!({ "providing": false, "was_running": stopped }))
        }
        "ai.serve.start" => serve_start(&service, state).await,
        "ai.serve.stop" => {
            let mut guard = service.api_server.lock().await;
            let stopped = match guard.take() {
                Some(server) => {
                    server.stop();
                    true
                }
                None => false,
            };
            // An explicit, idempotent stop also clears the intent restored at
            // daemon startup, even if the listener was already absent.
            persist_serve_state(false);
            Ok(json!({ "serving": false, "was_running": stopped }))
        }
        _ => bail!("unknown command: {cmd}"),
    }
}

async fn status(service: &Arc<AiService>) -> Result<Value> {
    let provider = service.local_provider();
    let serving = service
        .api_server
        .lock()
        .await
        .as_ref()
        .map(|server| server.addr().to_string());
    let remote = service.consumer.provider().map(
        |info| json!({ "node_id": info.node_id, "models": info.models, "services": info.services }),
    );
    Ok(json!({
        "room": service.room,
        "node_id": service.node_id,
        "connected_peers": crate::net::connected_nodes().await.len(),
        "providing": provider.is_some(),
        "models": provider.as_ref().map(|p| p.models()),
        "services": provider.as_ref().map(|p| p.services()),
        "recent_requests": provider.as_ref().map(|p| {
            p.logs().into_iter().take(5).collect::<Vec<_>>()
        }),
        "serving": serving,
        "remote_provider": remote,
    }))
}

async fn models(service: &Arc<AiService>) -> Result<Value> {
    if let Some(provider) = service.local_provider() {
        return Ok(json!({ "via": "local", "models": provider.models() }));
    }
    let info = match service.consumer.provider() {
        Some(info) => info,
        None => {
            service.broadcast(ProtocolMessage::ConsumerHello).await;
            service
                .consumer
                .wait_for_provider(DISCOVERY_TIMEOUT)
                .await
                .context("ai: no provider found on the network")?
        }
    };
    Ok(json!({
        "via": "p2p",
        "provider": info.node_id,
        "models": info.models,
        "services": info.services,
    }))
}

async fn provide_start(service: &Arc<AiService>, state: &Arc<AppState>) -> Result<Value> {
    if let Some(provider) = service.local_provider() {
        return Ok(json!({
            "providing": true,
            "already_running": true,
            "models": provider.models(),
        }));
    }

    let cfg = state.config().ai;
    let built = build_provider(service, &cfg).await?;
    *service.provider.write().expect("ai provider lock") = Some(built.provider.clone());
    service.broadcast(built.provider.hello()).await;

    // Only persisted on a successful start -- a failed `?` above (e.g. no
    // default preset configured yet) leaves the previous persisted intent
    // untouched, exactly like the manual command that failed didn't change
    // anything either.
    persist_provide_state(true);

    Ok(json!({
        "providing": true,
        "upstream": built.upstream_base_url,
        "models": built.models,
    }))
}

/// Writes `enabled` to `<data_dir>/ai-provide-state.json` (see
/// `provide_state`'s module doc), logging a `warn!` instead of failing the
/// caller if the write itself fails (e.g. disk full, permissions) -- losing
/// the persisted intent is a real problem (the daemon won't auto-resume/
/// auto-stay-stopped correctly next restart) but it must never turn a
/// successful `ai provide start`/`stop` into a failed IPC call over a
/// bookkeeping write.
fn persist_provide_state(enabled: bool) {
    let result = crate::config::data_dir()
        .context("ai: resolving the data directory")
        .and_then(|dir| provide_state::write_state(&dir, provide_state::ProvideState { enabled }));
    if let Err(err) = result {
        warn!(%err, enabled, "ai: failed to persist the provide-enabled state; a daemon restart will not correctly auto-resume/stay-stopped");
    }
}

/// Auto-resumes network `provide` at daemon startup if it was left enabled
/// on a previous run (persisted by [`persist_provide_state`] from
/// `provide_start`/`ai.provide.stop`, including via the dashboard toggle --
/// both go through the same `ai.provide.start`/`ai.provide.stop` IPC
/// commands, see `provide_state`'s module doc). Spawned eagerly from
/// `daemon::daemon_main`, the same way as `chat_relay`'s and
/// `storage::folder_owner`'s background tasks, rather than waited on lazily
/// like the rest of the `ai` service (which only starts on the first
/// `ai.*` IPC call) -- the whole point is providing coming back up without
/// any client ever having to ask.
///
/// Never fails daemon startup: any problem here (data dir unreadable,
/// corrupt state file, network room join failure, or a config problem such
/// as a dangling preset caught by [`build_provider`]) is logged via `warn!`
/// and simply leaves providing off, exactly as if `ai provide start` had
/// been run by hand and failed -- the daemon keeps running either way.
/// Success is always logged via `info!` so "is it providing after a
/// restart, and why (not)" has a concrete answer in the log without having
/// to poll `ai.status`.
pub fn spawn_provide_autoresume(state: Arc<AppState>) {
    tokio::spawn(async move {
        let data_dir = match crate::config::data_dir() {
            Ok(dir) => dir,
            Err(err) => {
                warn!(%err, "ai: provide auto-resume skipped -- could not resolve the data directory");
                return;
            }
        };
        let persisted = match provide_state::read_state(&data_dir) {
            Ok(state) => state,
            Err(err) => {
                warn!(%err, "ai: provide auto-resume skipped -- could not read the persisted provide state");
                return;
            }
        };
        if !persisted.enabled {
            debug!("ai: provide was not left enabled on the previous run; not auto-resuming");
            return;
        }
        let service = match ensure_started(&state).await {
            Ok(service) => service,
            Err(err) => {
                warn!(%err, "ai: provide was left enabled on the previous run, but the ai network service failed to start; providing is off until `ai provide start` succeeds");
                return;
            }
        };
        match provide_start(&service, &state).await {
            Ok(result) => info!(%result, "ai: provide auto-resumed from persisted state"),
            Err(err) => {
                warn!(%err, "ai: provide was left enabled on the previous run, but auto-resume failed (likely an incomplete ai config, e.g. a dangling preset); providing is off until `ai provide start` succeeds")
            }
        }
    });
}

fn persist_serve_state(enabled: bool) {
    let result = crate::config::data_dir()
        .context("ai: resolving the data directory")
        .and_then(|dir| serve_state::write_state(&dir, serve_state::ServeState { enabled }));
    if let Err(err) = result {
        warn!(%err, enabled, "ai: failed to persist the API-server state; a daemon restart will not restore it correctly");
    }
}

/// Restores the local OpenAI-compatible listener when it was left running
/// before the daemon stopped. Failures are logged without preventing the
/// rest of the daemon from starting.
pub fn spawn_serve_autoresume(state: Arc<AppState>) {
    tokio::spawn(async move {
        let data_dir = match crate::config::data_dir() {
            Ok(dir) => dir,
            Err(err) => {
                warn!(%err, "ai: API-server auto-resume skipped -- could not resolve the data directory");
                return;
            }
        };
        let persisted = match serve_state::read_state(&data_dir) {
            Ok(state) => state,
            Err(err) => {
                warn!(%err, "ai: API-server auto-resume skipped -- could not read its persisted state");
                return;
            }
        };
        if !persisted.enabled {
            debug!("ai: API server was not left enabled; not auto-resuming");
            return;
        }
        let service = match ensure_started(&state).await {
            Ok(service) => service,
            Err(err) => {
                warn!(%err, "ai: API server was left enabled, but the AI service failed to start");
                return;
            }
        };
        match serve_start(&service, &state).await {
            Ok(result) => info!(%result, "ai: API server auto-resumed from persisted state"),
            Err(err) => warn!(%err, "ai: API server was left enabled, but auto-resume failed"),
        }
    });
}

/// Silently rebuilds the running provider from the *current* config and
/// re-broadcasts `provider_hello` (same connections, no leave/rejoin), so
/// dashboard/CLI edits to `ai.providers`/`ai.presets`/`ai.default_preset_id`/
/// `ai.tts_preset_id`/`ai.stt_preset_id`/`ai.advertised_models` take effect
/// live -- the user never needs to stop/start providing, let alone restart
/// the daemon, for a preset/model change to apply. A no-op if the `ai`
/// service was never started, or isn't currently providing (nothing to
/// reload). Called fire-and-forget from `daemon::handle`'s `config.set`
/// after a matching path saves successfully; failures are logged and leave
/// the previous (still-valid) provider running rather than tearing it down.
pub async fn reload_provider_if_running(state: &Arc<AppState>) {
    let Some(service) = SERVICE.get() else {
        return;
    };
    if service.local_provider().is_none() {
        return;
    }
    let cfg = state.config().ai;
    match build_provider(service, &cfg).await {
        Ok(built) => {
            *service.provider.write().expect("ai provider lock") = Some(built.provider.clone());
            service.broadcast(built.provider.hello()).await;
        }
        Err(err) => {
            warn!(%err, "ai: failed to reload provider after a config change; the previous provider keeps running");
        }
    }
}

struct BuiltProvider {
    provider: Arc<Provider>,
    upstream_base_url: String,
    models: Vec<String>,
}

/// Builds `(models, advertised)` for [`build_provider`]/[`Provider::hello`]
/// from `cfg.advertised_models` (a set of **preset ids**, see that field's
/// doc comment on `AiConfig`) and `cfg.presets`:
///
/// - Iterates `cfg.presets` in their *configured* order (not
///   `advertised_models`'s own order, which the dashboard's "what to
///   provide" checklist can reorder independent of preset definition
///   order -- see `renderProvideChecklist` in `web/assets/index.html`),
///   filtering down to those whose `id` appears in `advertised_models`.
/// - Each selected preset's advertised *name* is `label.trim()` when
///   non-empty, else `model` -- mistllm-wire's "advertised name = preset
///   label" contract.
/// - Two selected presets that resolve to the same advertised name: the
///   first configured one wins the name; the rest are dropped entirely,
///   from both `models` and `advertised` (a known v1 limitation -- an
///   `llm_request` naming that model can only ever reach the first
///   preset).
/// - A selected preset whose `provider_id` doesn't resolve to a configured
///   provider is skipped with a `warn!` (dangling reference, same
///   defensive posture as `voice_preset_provider`).
///
/// `cfg.advertised_models` empty -> both returned collections are empty:
/// per mistllm-wire this means `provider_hello` omits `models` entirely
/// (`Provider::hello`) -- the previous fallback of fetching and advertising
/// every upstream `GET /models` result has been removed, since an inbound
/// `model` is no longer name-checked against anything in that mode (see
/// `Provider::resolve_llm_call`) and blanket-advertising an unchecked
/// upstream catalog was never actually meaningful under that contract.
fn resolve_advertised_models(
    cfg: &crate::config::AiConfig,
) -> (
    Vec<String>,
    HashMap<String, crate::config::ResolvedAiPreset>,
) {
    let mut models = Vec::new();
    let mut advertised: HashMap<String, crate::config::ResolvedAiPreset> = HashMap::new();
    if cfg.advertised_models.is_empty() {
        return (models, advertised);
    }
    let wanted: HashSet<&str> = cfg.advertised_models.iter().map(String::as_str).collect();
    for preset in cfg
        .presets
        .iter()
        .filter(|p| wanted.contains(p.id.as_str()))
    {
        let label = preset.label.trim();
        let name = if !label.is_empty() {
            label.to_string()
        } else {
            preset.model.clone()
        };
        if advertised.contains_key(&name) {
            continue;
        }
        let Some(resolved) = crate::config::resolve_preset(cfg, Some(&preset.id)) else {
            warn!(
                preset_id = %preset.id,
                "ai: advertised preset's provider is not configured; skipping it"
            );
            continue;
        };
        models.push(name.clone());
        advertised.insert(name, resolved);
    }
    (models, advertised)
}

/// Resolves `cfg`'s default/tts/stt presets and constructs a fresh
/// [`Provider`] from them -- the shared core of [`provide_start`] (first
/// build) and [`reload_provider_if_running`] (rebuild after a config
/// change). Does not touch `service.provider`; callers install the result.
async fn build_provider(
    service: &Arc<AiService>,
    cfg: &crate::config::AiConfig,
) -> Result<BuiltProvider> {
    let resolved = crate::config::resolve_preset(cfg, None).context(
        "ai: no default LLM preset configured; set it up in the dashboard's \
         Settings panel, or with `mistl config set ai.providers <json>`, \
         `ai.presets <json>`, and `ai.default_preset_id <id>`",
    )?;
    let mut upstream = UpstreamConfig {
        base_url: resolved.base_url,
        api_key: resolved.api_key,
        model: (!resolved.model.is_empty()).then_some(resolved.model),
        temperature: resolved.temperature,
        reasoning_effort: resolved.reasoning_effort,
    };

    let (models, advertised) = resolve_advertised_models(cfg);
    // Requests without an explicit model fall back to the first advertised
    // preset's own resolved model (mirrors the pre-advertised-name-contract
    // "models.first()" fallback, just resolved against the preset table
    // instead of a flat raw-model-id list -- see
    // `resolve_advertised_models`'s doc comment). Empty `advertised_models`
    // (or a default preset with its own non-blank `model`, the common
    // case) leaves this `None`, unchanged from before.
    if upstream.model.is_none() {
        upstream.model = models
            .first()
            .and_then(|name| advertised.get(name))
            .map(|resolved| resolved.model.clone());
    }

    let call: LlmCallFn = {
        let upstream = upstream.clone();
        Arc::new(move |messages, model, delta_tx| {
            let upstream = upstream.clone();
            Box::pin(async move {
                openai::stream_chat_completion(&upstream, &messages, model.as_deref(), delta_tx)
                    .await
            })
        })
    };

    let tts_preset = voice_preset_provider(cfg, &cfg.tts_preset_id);
    let advertised_voices = resolve_advertised_voices(tts_preset.as_ref()).await;
    log_voice_preset_diagnostics(
        cfg,
        "tts",
        &cfg.tts_preset_id,
        tts_preset.as_ref().map(|(_, resolved)| resolved),
    );
    let tts_call = tts_preset.map(|(provider, resolved)| {
        let catalog = advertised_voices.clone();
        let call: TtsCallFn = Arc::new(move |text, model, voice, lang| {
            let provider = provider.clone();
            let effective_voice = resolve_tts_voice(
                voice,
                lang.as_deref(),
                &resolved.lang_voices,
                &catalog,
                &resolved.voice,
            );
            let req = tts::TtsParams {
                model: resolve_voice_call_model(model, &resolved.model, "tts"),
                voice: effective_voice.unwrap_or_default(),
                input: text,
                format: None,
                speed: None,
            };
            Box::pin(async move { tts::synthesize(&provider, req).await })
        });
        call
    });
    let stt_preset = voice_preset_provider(cfg, &cfg.stt_preset_id);
    log_voice_preset_diagnostics(
        cfg,
        "stt",
        &cfg.stt_preset_id,
        stt_preset.as_ref().map(|(_, resolved)| resolved),
    );
    let stt_call = stt_preset.map(|(provider, resolved)| {
        let call: SttCallFn = Arc::new(move |audio, mime, model, file_name| {
            let provider = provider.clone();
            let req = stt::SttParams {
                model: resolve_voice_call_model(model, &resolved.model, "stt"),
                audio,
                mime,
                file_name,
            };
            Box::pin(async move { stt::transcribe(&provider, req).await })
        });
        call
    });

    let provider = Provider::new(
        service.send.clone(),
        call,
        models.clone(),
        advertised,
        tts_call,
        stt_call,
        advertised_voices,
    );
    Ok(BuiltProvider {
        provider,
        upstream_base_url: upstream.base_url,
        models,
    })
}

/// Resolves `preset_id` (an `ai.tts_preset_id`/`ai.stt_preset_id` value)
/// into an ad hoc `AiProviderConfig` (base_url/api_key only -- built from
/// `resolve_preset`, not looked up by provider id) plus the full resolved
/// preset (for its `model`/`voice` defaults). Returns `None` when
/// `preset_id` is blank ("not configured") or when it doesn't resolve to a
/// known preset/provider -- unlike `resolve_preset` itself, a blank id is
/// *not* defaulted to `ai.default_preset_id` here: an explicitly empty
/// tts/stt preset id means "don't offer this service", and silently
/// borrowing the chat preset would opt a node into serving voice it never
/// configured.
fn voice_preset_provider(
    cfg: &crate::config::AiConfig,
    preset_id: &str,
) -> Option<(
    crate::config::AiProviderConfig,
    crate::config::ResolvedAiPreset,
)> {
    let preset_id = preset_id.trim();
    if preset_id.is_empty() {
        return None;
    }
    // `resolve_preset` falls back to `ai.default_preset_id` whenever
    // `preset_id` doesn't name a *known* preset (see its own doc comment --
    // that's the correct, intentional behavior for chat resolution, mirrored
    // from the shared LLM config contract's `resolvePreset`). For tts/stt
    // that fallback would be actively harmful: a dangling `tts_preset_id`
    // (its preset got deleted/renamed after being assigned -- nothing here
    // or in the dashboard clears the reference when a preset is removed)
    // must never silently start serving the room's *chat* default preset as
    // TTS. That's exactly the confusing failure this function's own doc
    // comment above already calls out for the blank-id case; a dangling
    // non-blank id hits the same trap through `resolve_preset`'s fallback,
    // so it's guarded the same way here: require an exact, still-existing
    // preset id before ever calling `resolve_preset`.
    if !cfg.presets.iter().any(|p| p.id == preset_id) {
        return None;
    }
    let resolved = crate::config::resolve_preset(cfg, Some(preset_id))?;
    let provider = crate::config::AiProviderConfig {
        id: String::new(),
        label: String::new(),
        base_url: resolved.base_url.clone(),
        api_key: resolved.api_key.clone(),
    };
    Some((provider, resolved))
}

/// Builds the voice catalog to advertise in `provider_hello.voices`
/// (tts-voice-selection-v1 §2.1/§2.5), given the already-resolved tts
/// preset/provider (or `None` when tts isn't configured at all). Fallback
/// order:
///
/// 1. the tts preset's own upstream catalog, via [`openai::fetch_voices`]
///    (`GET {base_url}/audio/voices` -> `GET {base_url}/voices` -> `[]`,
///    matching mistai's web providers' discovery so mistl advertises the
///    same voices a tc-translate/tc-lingo provider on the same upstream
///    would);
/// 2. the resolved preset's single configured `voice`, only when (1) came
///    back empty (unreachable upstream, no such endpoint, or a genuinely
///    empty catalog);
/// 3. otherwise `[]` -- `hello()` then omits the `voices` field entirely.
///
/// `hello()` truncates the result to `MAX_ADVERTISED_VOICES` regardless of
/// which source populated it. Split out from `build_provider` so this
/// fallback order can be unit-tested against a mock upstream without
/// standing up a full `AiService`.
async fn resolve_advertised_voices(
    tts_preset: Option<&(
        crate::config::AiProviderConfig,
        crate::config::ResolvedAiPreset,
    )>,
) -> Vec<String> {
    let Some((provider, resolved)) = tts_preset else {
        return Vec::new();
    };
    let fetched = openai::fetch_voices(&provider.base_url, &provider.api_key).await;
    if fetched.is_empty() {
        resolved.voice.clone().into_iter().collect()
    } else {
        fetched
    }
}

/// Extracts the BCP-47 *primary* subtag from a language tag, lowercased
/// (e.g. `"en-US"` -> `"en"`, `"JA"` -> `"ja"`, `"en"` -> `"en"`). Used by
/// both [`resolve_tts_voice`]'s `lang_voices` lookup and its kokoro-style
/// catalog heuristic, so `"en-US"` and `"en-GB"` both match an `"en"` entry
/// / prefix without requiring the config or the catalog to enumerate every
/// regional variant.
fn lang_primary_subtag(lang: &str) -> String {
    lang.split('-')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// Case-insensitive `lang_voices` lookup by primary subtag (mistllm-wire
/// tts-lang-hint-v1): config authors are asked to use lowercase keys (see
/// `AiPresetConfig::lang_voices`'s doc comment and the README example), but
/// this looks up case-insensitively anyway so a stray uppercase key in a
/// hand-edited `config.toml` still works rather than silently never
/// matching.
fn lookup_lang_voice(lang_voices: &HashMap<String, String>, primary: &str) -> Option<String> {
    lang_voices
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(primary))
        .map(|(_, voice)| voice.clone())
}

/// Kokoro's single-letter language-prefix convention (e.g. `af_heart` = "a"
/// (American English) + "f" (female) + "_heart"): first letter names the
/// language/locale, second letter names the voice's gender (`f`/`m`), then
/// an underscore-separated name. Maps a BCP-47 primary subtag to the
/// kokoro prefix letter(s) that speak it; `None` for languages kokoro
/// doesn't have a documented prefix for, in which case the heuristic in
/// [`kokoro_style_lang_voice`] never applies regardless of catalog shape.
fn kokoro_prefix_letters(primary: &str) -> Option<&'static [char]> {
    match primary {
        "en" => Some(&['a', 'b']),
        "ja" => Some(&['j']),
        "zh" => Some(&['z']),
        "es" => Some(&['e']),
        "fr" => Some(&['f']),
        "hi" => Some(&['h']),
        "it" => Some(&['i']),
        "pt" => Some(&['p']),
        _ => None,
    }
}

/// Whether `voice` looks like a kokoro-style id: `^[a-z][fm]_` (one lowercase
/// letter naming the language, then `f`/`m` naming gender, then an
/// underscore), e.g. `"af_heart"`, `"jf_alpha"`, `"bm_george"`. Deliberately
/// narrow -- this is a heuristic over an *opaque* upstream voice id (mistl
/// has no other way to know a catalog is kokoro's), so it only fires on ids
/// that unambiguously fit the pattern.
fn is_kokoro_style_voice(voice: &str) -> bool {
    let mut chars = voice.chars();
    let Some(c0) = chars.next() else { return false };
    let Some(c1) = chars.next() else { return false };
    let Some(c2) = chars.next() else { return false };
    c0.is_ascii_lowercase() && (c1 == 'f' || c1 == 'm') && c2 == '_'
}

/// Step 3 of [`resolve_tts_voice`]'s fallback order: when `lang_voices` had
/// no entry for `primary`, and *every* voice in `catalog` fits
/// [`is_kokoro_style_voice`] (a non-empty catalog required -- an empty one
/// can't be "uniformly" anything), picks the first catalog voice whose
/// prefix letter matches `primary` per [`kokoro_prefix_letters`]. Returns
/// `None` (never guesses) when the catalog is empty, isn't uniformly
/// kokoro-shaped, `primary` has no known kokoro prefix, or no catalog entry
/// actually starts with one of that language's prefix letters -- callers
/// then fall through to the preset/catalog-first fallback, same as if no
/// `lang` had been given at all.
fn kokoro_style_lang_voice(primary: &str, catalog: &[String]) -> Option<String> {
    if catalog.is_empty() || !catalog.iter().all(|v| is_kokoro_style_voice(v)) {
        return None;
    }
    let prefixes = kokoro_prefix_letters(primary)?;
    catalog
        .iter()
        .find(|v| v.chars().next().is_some_and(|c| prefixes.contains(&c)))
        .cloned()
}

/// Resolves the effective voice for one `tts_request` (mistllm-wire
/// tts-lang-hint-v1), in fallback order:
///
/// 1. the request's own `voice` -- always wins unconditionally; `lang`
///    never overrides an explicit `voice`.
/// 2. if `lang` is present: [`lookup_lang_voice`] against the tts preset's
///    `lang_voices` map, by primary subtag ([`lang_primary_subtag`],
///    case-insensitive).
/// 3. if `lang` is present and step 2 found nothing:
///    [`kokoro_style_lang_voice`] -- only applies when the advertised
///    catalog is uniformly kokoro-shaped, a deliberately conservative
///    heuristic (see its own doc comment); logged via `info!` when it
///    fires, since it's a guess rather than something the operator
///    configured.
/// 4. the tts preset's configured `voice`.
/// 5. (previously a `tts_request` with neither would error immediately)
///    the first entry of the advertised voice catalog built by
///    [`resolve_advertised_voices`], logged via `info!` since it means
///    synthesis is proceeding with a voice nobody explicitly chose.
///
/// `None` only when all of the above are unavailable; the caller's
/// `tts::synthesize` call then still surfaces its pre-existing `"ai: tts
/// requires a voice"` error, unchanged from before any of this fallback
/// existed. Pure aside from the two `info!` calls, so the fallback order
/// itself can be unit-tested directly.
fn resolve_tts_voice(
    request_voice: Option<String>,
    lang: Option<&str>,
    lang_voices: &HashMap<String, String>,
    catalog: &[String],
    preset_voice: &Option<String>,
) -> Option<String> {
    if let Some(voice) = request_voice {
        return Some(voice);
    }
    if let Some(lang) = lang {
        let primary = lang_primary_subtag(lang);
        if let Some(voice) = lookup_lang_voice(lang_voices, &primary) {
            return Some(voice);
        }
        if let Some(voice) = kokoro_style_lang_voice(&primary, catalog) {
            info!(
                %voice,
                %lang,
                "ai: tts_request lang hint matched a kokoro-style catalog voice by \
                 language prefix (no explicit lang_voices entry configured)"
            );
            return Some(voice);
        }
    }
    if let Some(voice) = preset_voice.clone() {
        return Some(voice);
    }
    let voice = catalog.first().cloned()?;
    info!(
        %voice,
        "ai: tts_request had no voice and the tts preset has none configured; \
         using the first voice from the advertised catalog"
    );
    Some(voice)
}

/// Resolves the effective `model` for one `tts_request`/`stt_request`
/// against `preset_model` (the tts/stt preset's own configured model), per
/// mistllm-wire's "provider の voice/model 尊重規則": unlike `voice`, the
/// request's `model` is only ever honored when it *exactly matches*
/// `preset_model` -- otherwise (mismatch, or omitted entirely) this
/// provider's own configured model is used instead. This is a fallback, not
/// a rejection: a mismatched model never fails the request, it's silently
/// replaced. The mismatch case matters because consumers may echo back an
/// advertised *label* (e.g. a chat preset's display name such as `"TTS"`)
/// as `model` rather than a real upstream model id -- sending that straight
/// to `/audio/speech`/`/audio/transcriptions` would otherwise fail upstream
/// (see `tc-translate`'s `useNetworkProvider.ts`:
/// `model === ownTtsModel ? model : ownTtsModel` for the reference
/// implementation this mirrors). `kind` ("tts"/"stt") is only used to label
/// the mismatch debug log.
fn resolve_voice_call_model(
    request_model: Option<String>,
    preset_model: &str,
    kind: &'static str,
) -> String {
    match request_model {
        Some(model) if model == preset_model => model,
        Some(mismatched) => {
            debug!(
                kind,
                requested = %mismatched,
                configured = %preset_model,
                "ai: voice request model did not match this provider's configured model; \
                 falling back to the configured model instead of forwarding the request's value upstream"
            );
            preset_model.to_string()
        }
        None => preset_model.to_string(),
    }
}

/// Pure classification behind [`log_voice_preset_diagnostics`] -- split out
/// so the dangling-id and kind-mismatch detection (shared byte-for-byte
/// between `ai.tts_preset_id` and `ai.stt_preset_id`, see
/// [`voice_preset_provider`]'s doc comment) is unit-testable directly,
/// rather than only observable via `tracing` log output.
#[derive(Debug, Clone, PartialEq, Eq)]
enum VoicePresetDiagnostic {
    /// The preset id is blank: this service isn't offered at all, which is
    /// expected/quiet, not a warning.
    Unconfigured,
    /// The preset id is set but doesn't resolve to a live preset+provider
    /// (deleted/renamed preset, or its provider was removed).
    Dangling,
    /// Resolves fine, but the preset's own "Provides" `kind` (dashboard
    /// categorization, see `AiPresetConfig::kind`) doesn't match the
    /// service it's wired up as (e.g. a "chat"-kind preset assigned to
    /// `stt_preset_id`) -- a strong signal of an accidental assignment.
    /// Carries the mismatched kind actually found, for the log message.
    KindMismatch { actual: String },
    /// Resolves fine and its `kind` matches (or the preset predates `kind`
    /// entirely, which defaults to `"chat"` -- see that field's doc).
    Ok,
}

/// Classifies `preset_id` (an `ai.tts_preset_id`/`ai.stt_preset_id` value,
/// already resolved by the caller via [`voice_preset_provider`] into
/// `resolved`) against `expected_kind` (`"tts"`|`"stt"`). Pure: no I/O, no
/// logging -- see [`log_voice_preset_diagnostics`] for the logging wrapper
/// callers actually use.
fn diagnose_voice_preset(
    cfg: &crate::config::AiConfig,
    preset_id: &str,
    resolved: Option<&crate::config::ResolvedAiPreset>,
    expected_kind: &str,
) -> VoicePresetDiagnostic {
    let preset_id = preset_id.trim();
    if preset_id.is_empty() {
        return VoicePresetDiagnostic::Unconfigured;
    }
    if resolved.is_none() {
        return VoicePresetDiagnostic::Dangling;
    }
    let kind = cfg
        .presets
        .iter()
        .find(|p| p.id == preset_id)
        .map(|p| {
            if p.kind.is_empty() {
                "chat"
            } else {
                p.kind.as_str()
            }
        })
        .unwrap_or("chat");
    if kind == expected_kind {
        VoicePresetDiagnostic::Ok
    } else {
        VoicePresetDiagnostic::KindMismatch {
            actual: kind.to_string(),
        }
    }
}

/// Logs the resolved state of `ai.tts_preset_id`/`ai.stt_preset_id`
/// (`kind` selects which -- `"tts"` or `"stt"`) at every provider
/// (re)build (`provide start`, and every live config reload -- see
/// `reload_provider_if_running`), so an operator debugging "TTS/STT isn't
/// working"/"no voices advertised" on a real device has something concrete
/// to check in `mistl`'s own logs before ever reproducing a failing
/// request. Built on [`diagnose_voice_preset`]'s three failure-relevant
/// cases (`Unconfigured`/`Dangling`/`KindMismatch`, each logged once and
/// clearly) plus one kind-specific note logged only when it resolves
/// (`Ok` or `KindMismatch` both still resolved to a real preset+provider):
/// for `"tts"`, whether a fallback `voice` is configured (mirrors the
/// pre-refactor TTS-only diagnostics exactly); for `"stt"`, a plain
/// confirmation that inbound `stt_request`s will be forwarded upstream
/// (STT has no `voice` concept to report on).
fn log_voice_preset_diagnostics(
    cfg: &crate::config::AiConfig,
    kind: &'static str,
    preset_id: &str,
    resolved: Option<&crate::config::ResolvedAiPreset>,
) {
    let trimmed = preset_id.trim();
    match diagnose_voice_preset(cfg, preset_id, resolved, kind) {
        VoicePresetDiagnostic::Unconfigured => {
            debug!(
                kind,
                "ai: preset id for this service is unset - this provider will not offer it (request gets an immediate error reply instead of silently going unanswered)"
            );
            return;
        }
        VoicePresetDiagnostic::Dangling => {
            warn!(
                preset_id = trimmed,
                kind,
                "ai: preset id does not resolve to a configured preset+provider (deleted/renamed preset, or its provider was removed) - this provider will NOT offer this service, it does not fall back to the default preset"
            );
            return;
        }
        VoicePresetDiagnostic::KindMismatch { actual } => {
            warn!(
                preset_id = trimmed,
                expected_kind = kind,
                actual_kind = %actual,
                "ai: preset id points at a preset whose dashboard \"Provides\" kind does not match this service - likely assigned by mistake (its model/provider probably don't speak the matching endpoint); double-check the preset's \"Provides\" dropdown, or reassign the preset id"
            );
        }
        VoicePresetDiagnostic::Ok => {}
    }
    let Some(resolved) = resolved else {
        return;
    };
    if kind == "tts" {
        // The preset's own `voice` (if set) is only the *fallback* default
        // now -- the catalog actually advertised in `provider_hello.voices`
        // is built in `build_provider` from the upstream `fetch_voices`
        // result first, this `voice` second (see that function's comment);
        // this just logs whether that fallback exists, not the final
        // advertised list.
        match resolved.voice.as_deref() {
            Some(voice) => {
                debug!(preset_id = trimmed, %voice, "ai: tts preset resolved; falls back to this voice if upstream voice discovery returns nothing")
            }
            None => debug!(
                preset_id = trimmed,
                "ai: tts preset resolved but has no \"voice\" set - if upstream voice discovery also returns nothing, provider_hello will omit the voices catalog entirely"
            ),
        }
    } else {
        debug!(
            preset_id = trimmed,
            "ai: stt preset resolved; inbound stt_requests will be forwarded to its upstream transcription endpoint"
        );
    }
}

async fn serve_start(service: &Arc<AiService>, state: &Arc<AppState>) -> Result<Value> {
    let mut guard = service.api_server.lock().await;
    if let Some(server) = guard.as_ref() {
        return Ok(json!({
            "serving": true,
            "already_running": true,
            "listen": server.addr().to_string(),
        }));
    }

    let call: LlmCallFn = {
        let service = service.clone();
        Arc::new(move |messages, model, delta_tx| {
            let service = service.clone();
            Box::pin(async move {
                service
                    .chat(messages, model, delta_tx)
                    .await
                    .map(|(content, _, _)| content)
            })
        })
    };
    let models_fn: ModelsFn = {
        let service = service.clone();
        Arc::new(move || {
            if let Some(provider) = service.local_provider() {
                provider.models()
            } else {
                service
                    .consumer
                    .provider()
                    .map(|info| info.models)
                    .unwrap_or_default()
            }
        })
    };

    let tts_fn: TtsFn = {
        let service = service.clone();
        let state = state.clone();
        Arc::new(move |params, lang| {
            let service = service.clone();
            let state = state.clone();
            Box::pin(async move { service.synthesize(&state, params, lang).await })
        })
    };
    let stt_fn: SttFn = {
        let service = service.clone();
        let state = state.clone();
        Arc::new(move |params| {
            let service = service.clone();
            let state = state.clone();
            Box::pin(async move { service.transcribe(&state, params).await })
        })
    };

    let api_listen = state.config().ai.api_listen;
    let server = ApiServer::start(&api_listen, call, models_fn, tts_fn, stt_fn)
        .await
        .with_context(|| format!("ai: binding API server on {api_listen}"))?;
    let addr = server.addr();
    *guard = Some(server);

    // Record intent only once bind has succeeded. A failed manual start must
    // not overwrite the state from the last successful start/stop command.
    persist_serve_state(true);

    Ok(json!({
        "serving": true,
        "listen": addr.to_string(),
        "openai_base_url": format!("http://{addr}/v1"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AiConfig, AiPresetConfig, AiProviderConfig, resolve_preset};
    use std::collections::HashMap;

    #[test]
    fn provider_upstream_maps_base_url_and_api_key_for_a_known_provider() {
        let mut ai = AiConfig::default();
        ai.providers.push(AiProviderConfig {
            id: "openai".to_string(),
            label: "OpenAI".to_string(),
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "sk-test".to_string(),
        });

        let upstream = provider_upstream(&ai, "openai").unwrap();
        assert_eq!(upstream.base_url, "https://api.openai.com/v1");
        assert_eq!(upstream.api_key, "sk-test");
        assert_eq!(upstream.model, None);
        assert_eq!(upstream.temperature, None);
        assert_eq!(upstream.reasoning_effort, None);
    }

    #[test]
    fn provider_upstream_errors_for_an_unknown_provider() {
        let ai = AiConfig::default();
        let err = provider_upstream(&ai, "missing").unwrap_err();
        assert!(err.to_string().contains("missing"));
        assert!(err.to_string().contains("ai.providers"));
    }

    fn ai_with_chat_default_and_tts_preset() -> AiConfig {
        let mut ai = AiConfig::default();
        ai.providers.push(AiProviderConfig {
            id: "p1".to_string(),
            label: "Provider".to_string(),
            base_url: "https://chat.example/v1".to_string(),
            api_key: "sk-chat".to_string(),
        });
        ai.providers.push(AiProviderConfig {
            id: "p2".to_string(),
            label: "TTS provider".to_string(),
            base_url: "https://tts.example/v1".to_string(),
            api_key: "sk-tts".to_string(),
        });
        ai.presets.push(AiPresetConfig {
            id: "chat-default".to_string(),
            label: "Chat".to_string(),
            provider_id: "p1".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        ai.presets.push(AiPresetConfig {
            id: "tts-real".to_string(),
            label: "Voice".to_string(),
            provider_id: "p2".to_string(),
            model: "tts-1".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: Some("alloy".to_string()),
            lang_voices: HashMap::new(),
            kind: "tts".to_string(),
        });
        ai.default_preset_id = "chat-default".to_string();
        ai
    }

    #[test]
    fn voice_preset_provider_none_for_blank_id() {
        let ai = ai_with_chat_default_and_tts_preset();
        assert!(voice_preset_provider(&ai, "").is_none());
        assert!(voice_preset_provider(&ai, "   ").is_none());
    }

    #[test]
    fn voice_preset_provider_resolves_a_real_tts_preset() {
        let ai = ai_with_chat_default_and_tts_preset();
        let (provider, resolved) = voice_preset_provider(&ai, "tts-real").unwrap();
        assert_eq!(provider.base_url, "https://tts.example/v1");
        assert_eq!(resolved.model, "tts-1");
        assert_eq!(resolved.voice.as_deref(), Some("alloy"));
    }

    #[test]
    fn resolve_advertised_models_empty_when_no_ids_configured() {
        let ai = ai_with_chat_default_and_tts_preset();
        let (models, advertised) = resolve_advertised_models(&ai);
        assert!(models.is_empty());
        assert!(advertised.is_empty());
    }

    #[test]
    fn resolve_advertised_models_uses_label_when_non_blank_else_model() {
        let mut ai = ai_with_chat_default_and_tts_preset();
        ai.presets.push(AiPresetConfig {
            id: "unlabeled".to_string(),
            label: "   ".to_string(), // blank once trimmed
            provider_id: "p1".to_string(),
            model: "raw-model-id".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        ai.advertised_models = vec!["chat-default".to_string(), "unlabeled".to_string()];

        let (models, advertised) = resolve_advertised_models(&ai);
        assert_eq!(models, vec!["Chat".to_string(), "raw-model-id".to_string()]);
        assert_eq!(advertised.get("Chat").unwrap().model, "gpt-4o");
        assert_eq!(
            advertised.get("raw-model-id").unwrap().model,
            "raw-model-id"
        );
    }

    #[test]
    fn resolve_advertised_models_orders_by_preset_config_order_not_advertised_list_order() {
        let mut ai = ai_with_chat_default_and_tts_preset();
        ai.presets.push(AiPresetConfig {
            id: "chat-second".to_string(),
            label: "Second".to_string(),
            provider_id: "p1".to_string(),
            model: "gpt-4o-mini".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        // Listed in reverse of `ai.presets`'s own order.
        ai.advertised_models = vec!["chat-second".to_string(), "chat-default".to_string()];

        let (models, _advertised) = resolve_advertised_models(&ai);
        assert_eq!(models, vec!["Chat".to_string(), "Second".to_string()]);
    }

    #[test]
    fn resolve_advertised_models_dedups_same_name_first_configured_preset_wins() {
        let mut ai = ai_with_chat_default_and_tts_preset();
        ai.presets.push(AiPresetConfig {
            id: "chat-duplicate-name".to_string(),
            label: "Chat".to_string(), // same advertised name as chat-default
            provider_id: "p2".to_string(),
            model: "other-model".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        ai.advertised_models = vec![
            "chat-default".to_string(),
            "chat-duplicate-name".to_string(),
        ];

        let (models, advertised) = resolve_advertised_models(&ai);
        assert_eq!(
            models,
            vec!["Chat".to_string()],
            "the duplicate name is dropped entirely"
        );
        assert_eq!(
            advertised.get("Chat").unwrap().model,
            "gpt-4o",
            "the first-configured preset wins the shared name"
        );
    }

    #[test]
    fn resolve_advertised_models_skips_a_preset_whose_provider_is_not_configured() {
        let mut ai = ai_with_chat_default_and_tts_preset();
        ai.presets.push(AiPresetConfig {
            id: "dangling".to_string(),
            label: "Dangling".to_string(),
            provider_id: "no-such-provider".to_string(),
            model: "whatever".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        ai.advertised_models = vec!["chat-default".to_string(), "dangling".to_string()];

        let (models, advertised) = resolve_advertised_models(&ai);
        assert_eq!(models, vec!["Chat".to_string()]);
        assert!(!advertised.contains_key("Dangling"));
    }

    /// The critical regression test: a dangling `tts_preset_id` (its preset
    /// was deleted/renamed after being assigned -- nothing clears the
    /// reference automatically) must NOT silently fall back to
    /// `ai.default_preset_id` the way plain `resolve_preset` does for chat.
    /// That fallback would silently start serving the room's *chat* default
    /// preset (here: "chat-default", provider p1, no voice) as "tts",
    /// exactly the confusing real-device failure this module's diagnostics
    /// exist to catch (see `log_voice_preset_diagnostics`).
    #[test]
    fn voice_preset_provider_does_not_fall_back_to_default_preset_for_a_dangling_id() {
        let ai = ai_with_chat_default_and_tts_preset();
        assert!(voice_preset_provider(&ai, "deleted-preset-id").is_none());
    }

    #[test]
    fn voice_preset_provider_none_when_the_resolved_preset_has_no_provider() {
        let mut ai = ai_with_chat_default_and_tts_preset();
        ai.presets.push(AiPresetConfig {
            id: "orphaned".to_string(),
            label: "Orphaned".to_string(),
            provider_id: "no-such-provider".to_string(),
            model: "tts-1".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: Some("alloy".to_string()),
            lang_voices: HashMap::new(),
            kind: "tts".to_string(),
        });
        assert!(voice_preset_provider(&ai, "orphaned").is_none());
    }

    // -- diagnose_voice_preset: pure tts/stt diagnostic classification ----

    #[test]
    fn diagnose_voice_preset_unconfigured_for_a_blank_id() {
        let ai = ai_with_chat_default_and_tts_preset();
        assert_eq!(
            diagnose_voice_preset(&ai, "", None, "tts"),
            VoicePresetDiagnostic::Unconfigured
        );
        assert_eq!(
            diagnose_voice_preset(&ai, "   ", None, "stt"),
            VoicePresetDiagnostic::Unconfigured
        );
    }

    #[test]
    fn diagnose_voice_preset_dangling_for_a_nonblank_id_that_did_not_resolve() {
        // `resolved: None` here stands in for what `voice_preset_provider`
        // returns for a dangling/orphaned id -- `diagnose_voice_preset`
        // itself never re-resolves, it just classifies what the caller
        // already found (or didn't).
        let ai = ai_with_chat_default_and_tts_preset();
        assert_eq!(
            diagnose_voice_preset(&ai, "deleted-preset-id", None, "tts"),
            VoicePresetDiagnostic::Dangling
        );
        assert_eq!(
            diagnose_voice_preset(&ai, "deleted-preset-id", None, "stt"),
            VoicePresetDiagnostic::Dangling
        );
    }

    #[test]
    fn diagnose_voice_preset_ok_when_resolved_and_kind_matches() {
        let ai = ai_with_chat_default_and_tts_preset();
        let resolved = resolve_preset(&ai, Some("tts-real")).unwrap();
        assert_eq!(
            diagnose_voice_preset(&ai, "tts-real", Some(&resolved), "tts"),
            VoicePresetDiagnostic::Ok
        );
    }

    #[test]
    fn diagnose_voice_preset_kind_mismatch_when_resolved_preset_is_labeled_differently() {
        // "chat-default" is kind "chat" but is being checked against "stt" --
        // the exact real-device failure mode `ai.stt_preset_id`/`ai.tts_preset_id`
        // diagnostics exist to flag (see `log_voice_preset_diagnostics`).
        let ai = ai_with_chat_default_and_tts_preset();
        let resolved = resolve_preset(&ai, Some("chat-default")).unwrap();
        assert_eq!(
            diagnose_voice_preset(&ai, "chat-default", Some(&resolved), "stt"),
            VoicePresetDiagnostic::KindMismatch {
                actual: "chat".to_string()
            }
        );
        // Same preset checked against "tts" also mismatches (it's "chat").
        assert_eq!(
            diagnose_voice_preset(&ai, "chat-default", Some(&resolved), "tts"),
            VoicePresetDiagnostic::KindMismatch {
                actual: "chat".to_string()
            }
        );
        // And a "tts"-kind preset checked against "stt" mismatches too.
        let tts_resolved = resolve_preset(&ai, Some("tts-real")).unwrap();
        assert_eq!(
            diagnose_voice_preset(&ai, "tts-real", Some(&tts_resolved), "stt"),
            VoicePresetDiagnostic::KindMismatch {
                actual: "tts".to_string()
            }
        );
    }

    #[test]
    fn diagnose_voice_preset_treats_an_empty_kind_field_as_chat() {
        // A preset predating `AiPresetConfig::kind` (old config.toml) has
        // `kind == ""`, which the doc comment says must behave like "chat".
        let mut ai = ai_with_chat_default_and_tts_preset();
        ai.presets.push(AiPresetConfig {
            id: "legacy".to_string(),
            label: "Legacy".to_string(),
            provider_id: "p1".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: String::new(),
        });
        let resolved = resolve_preset(&ai, Some("legacy")).unwrap();
        assert_eq!(
            diagnose_voice_preset(&ai, "legacy", Some(&resolved), "chat"),
            VoicePresetDiagnostic::Ok
        );
        assert_eq!(
            diagnose_voice_preset(&ai, "legacy", Some(&resolved), "tts"),
            VoicePresetDiagnostic::KindMismatch {
                actual: "chat".to_string()
            }
        );
    }

    // -- resolve_tts_voice: pure fallback-order tests -------------------

    fn empty_lang_voices() -> HashMap<String, String> {
        HashMap::new()
    }

    fn no_catalog() -> Vec<String> {
        Vec::new()
    }

    #[test]
    fn resolve_tts_voice_prefers_the_request_voice() {
        let voice = resolve_tts_voice(
            Some("request-voice".to_string()),
            None,
            &empty_lang_voices(),
            &no_catalog(),
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("request-voice"));
    }

    #[test]
    fn resolve_tts_voice_falls_back_to_the_preset_voice() {
        let voice = resolve_tts_voice(
            None,
            None,
            &empty_lang_voices(),
            &no_catalog(),
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("preset-voice"));
    }

    #[test]
    fn resolve_tts_voice_falls_back_to_the_first_catalog_voice() {
        // No request voice, no preset voice, but a non-empty advertised
        // catalog -- previously this would have gone on to hit
        // `tts::synthesize`'s "ai: tts requires a voice" error.
        let voice = resolve_tts_voice(
            None,
            None,
            &empty_lang_voices(),
            &["catalog-voice".to_string()],
            &None,
        );
        assert_eq!(voice.as_deref(), Some("catalog-voice"));
    }

    #[test]
    fn resolve_tts_voice_none_when_everything_is_absent() {
        assert_eq!(
            resolve_tts_voice(None, None, &empty_lang_voices(), &no_catalog(), &None),
            None
        );
    }

    // -- resolve_tts_voice: `lang` hint (mistllm-wire tts-lang-hint-v1) ----

    #[test]
    fn resolve_tts_voice_request_voice_wins_over_lang() {
        // lang never overrides an explicit request voice, even when
        // lang_voices has its own entry for that exact language.
        let mut lang_voices = empty_lang_voices();
        lang_voices.insert("en".to_string(), "lang-voice".to_string());
        let voice = resolve_tts_voice(
            Some("request-voice".to_string()),
            Some("en"),
            &lang_voices,
            &no_catalog(),
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("request-voice"));
    }

    #[test]
    fn resolve_tts_voice_lang_voices_exact_match() {
        let mut lang_voices = empty_lang_voices();
        lang_voices.insert("en".to_string(), "en-voice".to_string());
        lang_voices.insert("ja".to_string(), "ja-voice".to_string());
        let voice = resolve_tts_voice(
            None,
            Some("ja"),
            &lang_voices,
            &no_catalog(),
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("ja-voice"));
    }

    #[test]
    fn resolve_tts_voice_lang_voices_matches_primary_subtag() {
        // "en-US" must match a lang_voices entry keyed just "en".
        let mut lang_voices = empty_lang_voices();
        lang_voices.insert("en".to_string(), "en-voice".to_string());
        let voice = resolve_tts_voice(None, Some("en-US"), &lang_voices, &no_catalog(), &None);
        assert_eq!(voice.as_deref(), Some("en-voice"));
    }

    #[test]
    fn resolve_tts_voice_lang_voices_lookup_is_case_insensitive() {
        let mut lang_voices = empty_lang_voices();
        lang_voices.insert("EN".to_string(), "en-voice".to_string());
        let voice = resolve_tts_voice(None, Some("en-us"), &lang_voices, &no_catalog(), &None);
        assert_eq!(voice.as_deref(), Some("en-voice"));
    }

    #[test]
    fn resolve_tts_voice_lang_with_no_lang_voices_entry_falls_back_to_preset_voice() {
        let mut lang_voices = empty_lang_voices();
        lang_voices.insert("ja".to_string(), "ja-voice".to_string());
        // Requested "fr" has no entry and the catalog isn't kokoro-shaped
        // (empty), so this falls all the way through to preset_voice.
        let voice = resolve_tts_voice(
            None,
            Some("fr"),
            &lang_voices,
            &no_catalog(),
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("preset-voice"));
    }

    // -- resolve_tts_voice: kokoro-style catalog heuristic -----------------

    fn kokoro_catalog() -> Vec<String> {
        vec![
            "af_heart".to_string(),
            "bm_george".to_string(),
            "jf_alpha".to_string(),
            "zm_yunjian".to_string(),
        ]
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_applies_when_catalog_is_uniformly_kokoro_shaped() {
        let voice = resolve_tts_voice(
            None,
            Some("ja"),
            &empty_lang_voices(),
            &kokoro_catalog(),
            &None,
        );
        assert_eq!(voice.as_deref(), Some("jf_alpha"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_prefers_lang_voices_when_both_could_apply() {
        // lang_voices is checked before the kokoro heuristic, even though
        // the catalog is also kokoro-shaped and could otherwise resolve
        // "ja" itself.
        let mut lang_voices = empty_lang_voices();
        lang_voices.insert("ja".to_string(), "explicit-ja-voice".to_string());
        let voice = resolve_tts_voice(None, Some("ja"), &lang_voices, &kokoro_catalog(), &None);
        assert_eq!(voice.as_deref(), Some("explicit-ja-voice"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_picks_first_matching_prefix_english_has_two_letters() {
        // "en" maps to both "a" and "b" prefixes; the first catalog entry
        // matching either wins (catalog order, not prefix-letter order).
        let voice = resolve_tts_voice(
            None,
            Some("en"),
            &empty_lang_voices(),
            &kokoro_catalog(),
            &None,
        );
        assert_eq!(voice.as_deref(), Some("af_heart"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_does_not_apply_to_a_non_kokoro_catalog() {
        // Safety-first: a catalog with even one non-conforming id must not
        // be treated as kokoro-shaped at all.
        let catalog = vec!["af_heart".to_string(), "alloy".to_string()];
        let voice = resolve_tts_voice(
            None,
            Some("en"),
            &empty_lang_voices(),
            &catalog,
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("preset-voice"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_does_not_apply_to_an_empty_catalog() {
        let voice = resolve_tts_voice(
            None,
            Some("en"),
            &empty_lang_voices(),
            &no_catalog(),
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("preset-voice"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_does_not_apply_for_an_unmapped_language() {
        // "de" (German) has no documented kokoro prefix letter.
        let voice = resolve_tts_voice(
            None,
            Some("de"),
            &empty_lang_voices(),
            &kokoro_catalog(),
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("preset-voice"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_does_not_apply_when_no_prefix_letter_matches() {
        // Catalog is kokoro-shaped but has no "z" (zh) entries -- must not
        // guess a wrong-language voice, falls through instead.
        let catalog = vec!["af_heart".to_string(), "jf_alpha".to_string()];
        let voice = resolve_tts_voice(
            None,
            Some("zh"),
            &empty_lang_voices(),
            &catalog,
            &Some("preset-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("preset-voice"));
    }

    #[test]
    fn lang_primary_subtag_lowercases_and_strips_region() {
        assert_eq!(lang_primary_subtag("en-US"), "en");
        assert_eq!(lang_primary_subtag("JA-JP"), "ja");
        assert_eq!(lang_primary_subtag("en"), "en");
    }

    #[test]
    fn is_kokoro_style_voice_matches_and_rejects() {
        assert!(is_kokoro_style_voice("af_heart"));
        assert!(is_kokoro_style_voice("jm_kumo"));
        assert!(!is_kokoro_style_voice("alloy"));
        assert!(!is_kokoro_style_voice("a_heart")); // missing gender letter
        assert!(!is_kokoro_style_voice("Af_heart")); // uppercase language letter
        assert!(!is_kokoro_style_voice(""));
    }

    // -- resolve_voice_call_model: request model vs. preset model ---------

    #[test]
    fn resolve_voice_call_model_honors_a_matching_request_model_tts() {
        let model = resolve_voice_call_model(Some("irodori-tts".to_string()), "irodori-tts", "tts");
        assert_eq!(model, "irodori-tts");
    }

    #[test]
    fn resolve_voice_call_model_falls_back_on_a_mismatched_request_model_tts() {
        // The regression this guards: a consumer selecting an advertised ad
        // card can echo back its *label* (e.g. "TTS") as `model` rather than
        // a real upstream model id -- that must never be forwarded as-is.
        let model = resolve_voice_call_model(Some("TTS".to_string()), "irodori-tts", "tts");
        assert_eq!(model, "irodori-tts");
    }

    #[test]
    fn resolve_voice_call_model_falls_back_when_omitted_tts() {
        let model = resolve_voice_call_model(None, "irodori-tts", "tts");
        assert_eq!(model, "irodori-tts");
    }

    #[test]
    fn resolve_voice_call_model_honors_a_matching_request_model_stt() {
        let model = resolve_voice_call_model(Some("whisper-1".to_string()), "whisper-1", "stt");
        assert_eq!(model, "whisper-1");
    }

    #[test]
    fn resolve_voice_call_model_falls_back_on_a_mismatched_request_model_stt() {
        let model = resolve_voice_call_model(Some("STT".to_string()), "whisper-1", "stt");
        assert_eq!(model, "whisper-1");
    }

    #[test]
    fn resolve_voice_call_model_falls_back_when_omitted_stt() {
        let model = resolve_voice_call_model(None, "whisper-1", "stt");
        assert_eq!(model, "whisper-1");
    }

    // -- resolve_advertised_voices: fallback order against a mock upstream --

    /// One-shot mock HTTP server: accepts a single connection, replies with
    /// `response`, closes. Mirrors `openai::tests::mock_server` (kept
    /// separate/minimal here rather than exposing that private helper
    /// across modules).
    async fn mock_voices_server(response: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let response = response.to_string();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{addr}")
    }

    fn ok_json(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn not_found() -> String {
        let body = "nope";
        format!(
            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn resolved_preset(base_url: String, voice: Option<&str>) -> crate::config::ResolvedAiPreset {
        crate::config::ResolvedAiPreset {
            base_url,
            api_key: "key".to_string(),
            model: "tts-1".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: voice.map(String::from),
            lang_voices: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn resolve_advertised_voices_empty_when_no_tts_preset_configured() {
        assert_eq!(resolve_advertised_voices(None).await, Vec::<String>::new());
    }

    #[tokio::test]
    async fn resolve_advertised_voices_prefers_the_fetched_upstream_catalog() {
        // Fetch succeeds *and* the preset has its own `voice` set -- the
        // fetched catalog wins (fallback order step 1 beats step 2).
        let base_url = mock_voices_server(&ok_json(r#"{"voices":["nova","shimmer"]}"#)).await;
        let resolved = resolved_preset(base_url.clone(), Some("alloy"));
        let provider = crate::config::AiProviderConfig {
            id: String::new(),
            label: String::new(),
            base_url,
            api_key: "key".to_string(),
        };
        let voices = resolve_advertised_voices(Some(&(provider, resolved))).await;
        assert_eq!(voices, vec!["nova".to_string(), "shimmer".to_string()]);
    }

    #[tokio::test]
    async fn resolve_advertised_voices_falls_back_to_preset_voice_when_fetch_is_empty() {
        // Both /audio/voices and /voices need to be tried and fail for
        // fetch_voices to return []; a single 404 response closes the
        // connection after the first candidate, so the second candidate
        // request hits a closed/refused socket and also fails -- exercising
        // the "upstream has no voices endpoint at all" case end to end.
        let base_url = mock_voices_server(&not_found()).await;
        let resolved = resolved_preset(base_url.clone(), Some("alloy"));
        let provider = crate::config::AiProviderConfig {
            id: String::new(),
            label: String::new(),
            base_url,
            api_key: "key".to_string(),
        };
        let voices = resolve_advertised_voices(Some(&(provider, resolved))).await;
        assert_eq!(voices, vec!["alloy".to_string()]);
    }

    #[tokio::test]
    async fn resolve_advertised_voices_empty_when_fetch_fails_and_preset_has_no_voice() {
        let base_url = mock_voices_server(&not_found()).await;
        let resolved = resolved_preset(base_url.clone(), None);
        let provider = crate::config::AiProviderConfig {
            id: String::new(),
            label: String::new(),
            base_url,
            api_key: "key".to_string(),
        };
        let voices = resolve_advertised_voices(Some(&(provider, resolved))).await;
        assert_eq!(voices, Vec::<String>::new());
    }
}
