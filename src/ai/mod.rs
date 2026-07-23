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
//! The AI room defaults to the mailbox room for backward-compat convenience
//! (so a bare config still puts mailbox and ai in the same room), but this
//! is no longer mandatory: `crate::net` supports multiple simultaneous
//! rooms per process, so `[ai] room_id` may name a distinct room. Either
//! way mailbox and ai protocols coexist by shape.

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
mod provider;
mod stt;
pub mod tts;

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
pub type LlmCallFn =
    Arc<dyn Fn(Vec<ChatMessage>, Option<String>, Option<UnboundedSender<String>>) -> LlmCallFuture + Send + Sync>;

/// Model list for `GET /v1/models`.
pub type ModelsFn = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// How long discovery waits for a `provider_hello` (mistai default).
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

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
        .or_else(|| config.mailbox.room_id.clone())
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
            .request(&info.node_id, messages, model, self.request_timeout, delta_tx)
            .await?;
        Ok((content, "p2p", Some(info.node_id)))
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
    let remote = service.consumer.provider().map(|info| {
        json!({ "node_id": info.node_id, "models": info.models, "services": info.services })
    });
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

    Ok(json!({
        "providing": true,
        "upstream": built.upstream_base_url,
        "models": built.models,
    }))
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

/// Resolves `cfg`'s default/tts/stt presets and constructs a fresh
/// [`Provider`] from them -- the shared core of [`provide_start`] (first
/// build) and [`reload_provider_if_running`] (rebuild after a config
/// change). Does not touch `service.provider`; callers install the result.
async fn build_provider(service: &Arc<AiService>, cfg: &crate::config::AiConfig) -> Result<BuiltProvider> {
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

    let models = if !cfg.advertised_models.is_empty() {
        cfg.advertised_models.clone()
    } else {
        match openai::fetch_models(&upstream).await {
            Ok(models) => models,
            Err(err) => {
                warn!(%err, "ai: could not fetch upstream model list; advertising none");
                Vec::new()
            }
        }
    };
    // Requests without an explicit model fall back to the first known one.
    if upstream.model.is_none() {
        upstream.model = models.first().cloned();
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
    log_tts_preset_diagnostics(cfg, tts_preset.as_ref().map(|(_, resolved)| resolved));
    let tts_call = tts_preset.map(|(provider, resolved)| {
        let fallback_voice = advertised_voices.first().cloned();
        let call: TtsCallFn = Arc::new(move |text, model, voice| {
            let provider = provider.clone();
            let effective_voice = resolve_tts_voice(voice, &resolved.voice, &fallback_voice);
            let req = tts::TtsParams {
                model: model.unwrap_or_else(|| resolved.model.clone()),
                voice: effective_voice.unwrap_or_default(),
                input: text,
                format: None,
                speed: None,
            };
            Box::pin(async move { tts::synthesize(&provider, req).await })
        });
        call
    });
    let stt_call = voice_preset_provider(cfg, &cfg.stt_preset_id).map(|(provider, resolved)| {
        let call: SttCallFn = Arc::new(move |audio, mime, model, file_name| {
            let provider = provider.clone();
            let req = stt::SttParams {
                model: model.unwrap_or_else(|| resolved.model.clone()),
                audio,
                mime,
                file_name,
            };
            Box::pin(async move { stt::transcribe(&provider, req).await })
        });
        call
    });

    let provider = Provider::new_with_voice(
        service.send.clone(),
        call,
        models.clone(),
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
) -> Option<(crate::config::AiProviderConfig, crate::config::ResolvedAiPreset)> {
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
    tts_preset: Option<&(crate::config::AiProviderConfig, crate::config::ResolvedAiPreset)>,
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

/// Resolves the effective voice for one `tts_request`, in fallback order:
/// the request's own `voice`, then the tts preset's configured `voice`,
/// then (new -- previously a `tts_request` with neither would error
/// immediately) the first entry of the advertised voice catalog built by
/// [`resolve_advertised_voices`], logged via `info!` since it means
/// synthesis is proceeding with a voice nobody explicitly chose. `None`
/// only when all three are unavailable; the caller's `tts::synthesize` call
/// then still surfaces its pre-existing `"ai: tts requires a voice"` error,
/// unchanged from before this fallback existed. Pure aside from that one log
/// call, so the fallback order itself can be unit-tested directly.
fn resolve_tts_voice(
    request_voice: Option<String>,
    preset_voice: &Option<String>,
    catalog_first: &Option<String>,
) -> Option<String> {
    if let Some(voice) = request_voice {
        return Some(voice);
    }
    if let Some(voice) = preset_voice.clone() {
        return Some(voice);
    }
    let voice = catalog_first.clone()?;
    info!(
        %voice,
        "ai: tts_request had no voice and the tts preset has none configured; \
         using the first voice from the advertised catalog"
    );
    Some(voice)
}

/// Logs the resolved state of `cfg.tts_preset_id` at every provider
/// (re)build (`provide start`, and every live config reload -- see
/// `reload_provider_if_running`), so an operator debugging "TTS isn't
/// working"/"no voices advertised" on a real device has something concrete
/// to check in `mistl`'s own logs before ever reproducing a failing
/// request. Three cases, each logged once and clearly:
///  - `tts_preset_id` unset: TTS isn't offered at all (expected/quiet, not
///    a warning).
///  - it's set but doesn't resolve to a real preset (dangling id, or that
///    preset's own provider got removed): also not offered, but this is
///    worth a `warn!` since it usually means a preset assignment silently
///    went stale.
///  - it resolves: logs whether a voice catalog will be advertised, and
///    `warn!`s when the resolved preset's own `kind` (the dashboard's
///    "Provides" categorization, see `AiPresetConfig::kind`) isn't `"tts"`
///    -- `kind` has zero effect on routing (see its doc comment), so this
///    can't be *prevented* here, but a preset whose own author labeled it
///    "chat"/"stt" being wired up as the TTS preset is a strong signal of
///    an accidental assignment (e.g. via `mistl config set
///    ai.tts_preset_id` naming the wrong id) worth flagging loudly.
fn log_tts_preset_diagnostics(cfg: &crate::config::AiConfig, resolved: Option<&crate::config::ResolvedAiPreset>) {
    let preset_id = cfg.tts_preset_id.trim();
    if preset_id.is_empty() {
        debug!("ai: ai.tts_preset_id is unset - this provider will not offer \"tts\" (tts_request gets an immediate voice_error)");
        return;
    }
    let Some(resolved) = resolved else {
        warn!(
            %preset_id,
            "ai: ai.tts_preset_id does not resolve to a configured preset+provider (deleted/renamed preset, or its provider was removed) - this provider will NOT offer \"tts\", it does not fall back to the default preset"
        );
        return;
    };
    let kind = cfg
        .presets
        .iter()
        .find(|p| p.id == preset_id)
        .map(|p| if p.kind.is_empty() { "chat" } else { p.kind.as_str() })
        .unwrap_or("chat");
    if kind != "tts" {
        warn!(
            %preset_id,
            kind,
            "ai: ai.tts_preset_id points at a preset whose dashboard \"Provides\" kind is not \"tts\" - likely a chat/stt preset assigned to TTS by mistake (its model/provider probably don't speak /audio/speech); double-check the preset's \"Provides\" dropdown, or reassign ai.tts_preset_id"
        );
    }
    // The preset's own `voice` (if set) is only the *fallback* default now --
    // the catalog actually advertised in `provider_hello.voices` is built in
    // `build_provider` from the upstream `fetch_voices` result first, this
    // `voice` second (see that function's comment); this just logs whether
    // that fallback exists, not the final advertised list.
    match resolved.voice.as_deref() {
        Some(voice) => debug!(%preset_id, %voice, "ai: tts preset resolved; falls back to this voice if upstream voice discovery returns nothing"),
        None => debug!(%preset_id, "ai: tts preset resolved but has no \"voice\" set - if upstream voice discovery also returns nothing, provider_hello will omit the voices catalog entirely"),
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

    let api_listen = state.config().ai.api_listen;
    let server = ApiServer::start(&api_listen, call, models_fn)
        .await
        .with_context(|| format!("ai: binding API server on {api_listen}"))?;
    let addr = server.addr();
    *guard = Some(server);

    Ok(json!({
        "serving": true,
        "listen": addr.to_string(),
        "openai_base_url": format!("http://{addr}/v1"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AiConfig, AiPresetConfig, AiProviderConfig};

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

    /// The critical regression test: a dangling `tts_preset_id` (its preset
    /// was deleted/renamed after being assigned -- nothing clears the
    /// reference automatically) must NOT silently fall back to
    /// `ai.default_preset_id` the way plain `resolve_preset` does for chat.
    /// That fallback would silently start serving the room's *chat* default
    /// preset (here: "chat-default", provider p1, no voice) as "tts",
    /// exactly the confusing real-device failure this module's diagnostics
    /// exist to catch (see `log_tts_preset_diagnostics`).
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
            kind: "tts".to_string(),
        });
        assert!(voice_preset_provider(&ai, "orphaned").is_none());
    }

    // -- resolve_tts_voice: pure fallback-order tests -------------------

    #[test]
    fn resolve_tts_voice_prefers_the_request_voice() {
        let voice = resolve_tts_voice(
            Some("request-voice".to_string()),
            &Some("preset-voice".to_string()),
            &Some("catalog-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("request-voice"));
    }

    #[test]
    fn resolve_tts_voice_falls_back_to_the_preset_voice() {
        let voice = resolve_tts_voice(
            None,
            &Some("preset-voice".to_string()),
            &Some("catalog-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("preset-voice"));
    }

    #[test]
    fn resolve_tts_voice_falls_back_to_the_first_catalog_voice() {
        // The new fallback: no request voice, no preset voice, but a
        // non-empty advertised catalog -- previously this would have gone
        // on to hit `tts::synthesize`'s "ai: tts requires a voice" error.
        let voice = resolve_tts_voice(None, &None, &Some("catalog-voice".to_string()));
        assert_eq!(voice.as_deref(), Some("catalog-voice"));
    }

    #[test]
    fn resolve_tts_voice_none_when_all_three_are_absent() {
        assert_eq!(resolve_tts_voice(None, &None, &None), None);
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
