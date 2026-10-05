//! P2P AI network: consume or provide LLM inference over mistlib rooms,
//! wire-compatible with `@tik-choco/mistai` protocol v1 (tc-mistllm /
//! tc-translate / tc-note peers can share the room).
//!
//! HTTP and Room providers use explicit model references. Network providing
//! advertises raw model ids independently in each enabled room with provide on.
//! The local OpenAI-compatible server resolves ids from provider caches and
//! ai.default_ref, including network routing when the selected provider is a room.

mod api_server;
mod consumer;
pub(crate) mod external;
mod model_discovery;
mod oai_tunnel;
pub(crate) use model_discovery::{ModelDiscovery, spawn_http_refresh};
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
mod voice_consumer;

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
use openai::{ChatOutput, ToolOptions, UpstreamConfig};
use protocol::{ChatMessage, ProtocolMessage};
use provider::{Provider, SttCallFn, TtsCallFn};

/// Ordered, fire-and-forget wire send: `(to_node_id, message)`. The
/// service backs this with a single queue-draining task, so call order is
/// send order (chunk streams stay ordered without relying on `seq` alone).
pub type SendFn = Arc<dyn Fn(&str, ProtocolMessage) + Send + Sync>;

/// Boxed future returned by [`LlmCallFn`].
pub type LlmCallFuture = Pin<Box<dyn Future<Output = Result<ChatOutput>> + Send>>;

/// One chat completion: `(messages, tools, model, delta_tx)` -> full content
/// plus any merged tool calls. Deltas are streamed into `delta_tx` (when
/// provided) as they arrive; tool calls are returned only at the end.
pub type LlmCallFn = Arc<
    dyn Fn(
            Vec<ChatMessage>,
            ToolOptions,
            Option<String>,
            Option<UnboundedSender<String>>,
        ) -> LlmCallFuture
        + Send
        + Sync,
>;

/// The request uses tool calling but the selected backend (a remote
/// provider) does not advertise the `"tools"` service. The API server maps
/// this to the 400 `tools_unsupported` response.
#[derive(Debug)]
pub struct ToolsUnsupported;

impl std::fmt::Display for ToolsUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the AI provider on the network does not support tools")
    }
}

impl std::error::Error for ToolsUnsupported {}

/// A local API model name cannot be resolved from enabled providers.
/// This is distinct from a remote provider refusing an unshared model.
#[derive(Debug)]
pub struct ModelNotFound(pub String);

impl std::fmt::Display for ModelNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The model `{}` does not exist or is not available from enabled providers.",
            self.0
        )
    }
}

impl std::error::Error for ModelNotFound {}

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

/// Capacity of the outbound reply queue (see `init_service`).
const SEND_QUEUE_CAPACITY: usize = 4096;

/// How long discovery waits for a `provider_hello` (mistai default).
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on listing an upstream's voices from inside a synthesis request
/// (see `AiService::synthesize`). Short on purpose: the catalog only feeds
/// two fallback steps of the voice chain, so giving up on it costs a guess,
/// while waiting on an upstream with no voice-listing endpoint would cost
/// the whole request.
const VOICE_CATALOG_TIMEOUT: Duration = Duration::from_secs(5);

struct AiRoom {
    cache_lock: std::sync::Mutex<()>,
    room: String,
    node_id: String,
    send: SendFn,
    consumer: Arc<Consumer>,
    voice: Arc<voice_consumer::VoiceConsumer>,
    oai: Arc<oai_tunnel::TunnelConsumer>,
    provider: RwLock<Option<Arc<Provider>>>,
    handlers: Arc<tokio::sync::Semaphore>,
    dropped_messages: std::sync::atomic::AtomicU64,
}

struct AiService {
    state: Arc<AppState>,
    rooms: RwLock<HashMap<String, Arc<AiRoom>>>,
    on_demand: RwLock<HashSet<String>>,
    sync: Mutex<()>,
    api_server: Mutex<Option<Arc<ApiServer>>>,
}

const MAX_INFLIGHT_HANDLERS: usize = 64;
static SERVICE: OnceCell<Arc<AiService>> = OnceCell::const_new();

async fn ensure_started(state: &Arc<AppState>) -> Result<Arc<AiService>> {
    let service = SERVICE
        .get_or_init(|| async {
            let service = Arc::new(AiService {
                state: state.clone(),
                rooms: RwLock::new(HashMap::new()),
                on_demand: RwLock::new(HashSet::new()),
                sync: Mutex::new(()),
                api_server: Mutex::new(None),
            });
            let weak = Arc::downgrade(&service);
            let rt = tokio::runtime::Handle::current();
            crate::net::register_room_handler(move |event, room, from, data| {
                let Some(service) = weak.upgrade() else {
                    return;
                };
                service.handle_room_event(event, room, from, data, &rt);
            });
            service
        })
        .await
        .clone();
    service.reconcile(false).await?;
    Ok(service)
}

impl AiRoom {
    fn local_provider(&self) -> Option<Arc<Provider>> {
        self.provider.read().expect("ai provider lock").clone()
    }
    async fn broadcast(&self, msg: ProtocolMessage) {
        let _ = crate::net::send_broadcast(&self.room, protocol::encode(&msg)).await;
    }
}

fn providing_room(provider: &crate::config::AiProviderConfig) -> bool {
    provider.enabled && provider.provide && provider.room().is_some()
}

/// CLI commands edit the same flags as the dashboard, preserving all other settings.
pub(crate) fn set_provide_flags(ai: &mut crate::config::AiConfig, start: bool) -> Vec<String> {
    let mut rooms = Vec::new();
    for provider in &mut ai.providers {
        let Some(room) = provider.room().map(String::from) else {
            continue;
        };
        if start {
            if provider.enabled && !provider.shared.is_empty() {
                provider.provide = true;
                rooms.push(room);
            }
        } else {
            if provider.provide {
                rooms.push(room);
            }
            provider.provide = false;
        }
    }
    rooms
}

/// Enabled rooms used by tasks/voice/default or configured to provide stay joined.
fn referenced_rooms(config: &crate::config::Config) -> HashSet<String> {
    let mut ids = HashSet::new();
    if let Some(reference) = &config.ai.default_ref {
        ids.insert(reference.provider_id.clone());
    }
    if let Some(voice) = &config.ai.tts {
        ids.insert(voice.provider_id.clone());
    }
    if let Some(reference) = &config.ai.stt {
        ids.insert(reference.provider_id.clone());
    }
    for pipeline in &config.bot.pipelines {
        for transform in &pipeline.transforms {
            let reference = match transform {
                crate::config::TransformConfig::Summarize { model, .. }
                | crate::config::TransformConfig::Translate { model, .. }
                | crate::config::TransformConfig::Tts { model, .. } => model,
            };
            if let Some(reference) = reference {
                ids.insert(reference.provider_id.clone());
            }
        }
    }
    config
        .ai
        .providers
        .iter()
        .filter(|p| p.enabled && (p.provide || ids.contains(&p.id)))
        .filter_map(|p| p.room().map(String::from))
        .collect()
}

impl AiService {
    fn handle_room_event(
        self: &Arc<Self>,
        event: u32,
        room: &str,
        from: &str,
        data: &[u8],
        rt: &tokio::runtime::Handle,
    ) {
        let service = self.clone();
        let session = service
            .rooms
            .read()
            .expect("ai rooms lock")
            .get(room)
            .cloned();
        let Some(session) = session else {
            return;
        };
        match event {
            crate::net::EVENT_RAW => {
                if data.len() > 2 * 1024 * 1024 {
                    return;
                }
                let Some(msg) = protocol::decode(data) else {
                    return;
                };
                let catalog_changed = session.consumer.handle_message(from, &msg);
                session.voice.handle_message(from, &msg);
                session.oai.handle_message(from, &msg);
                let Ok(permit) = session.handlers.clone().try_acquire_owned() else {
                    session
                        .dropped_messages
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if catalog_changed {
                        service.cache_room_models(&session);
                    }
                    return;
                };
                let from = from.to_string();
                rt.spawn(async move {
                    let _permit = permit;
                    if catalog_changed {
                        service.cache_room_models(&session);
                    }
                    if let Some(provider) = session.local_provider() {
                        provider.handle_message(from, msg).await;
                    }
                });
            }
            crate::net::EVENT_JOIN => {
                (session.send)(from, ProtocolMessage::ConsumerHello);
                if let Some(provider) = session.local_provider() {
                    (session.send)(from, provider.hello());
                }
            }
            crate::net::EVENT_LEAVE => {
                session.consumer.on_peer_disconnected(from);
                session.voice.on_peer_left(from);
                session.oai.drop_peer(from);
                if let Some(provider) = session.local_provider() {
                    provider.on_peer_left(from);
                }
            }
            _ => {}
        }
    }

    async fn api_room(self: Arc<Self>, room: String) -> Result<Option<api_server::RoomRoutes>> {
        if !enabled_api_room(&self.state.effective_config().ai, &room) {
            return Ok(None);
        }
        let session = self.room(&room).await?;
        let timeout =
            Duration::from_secs(self.state.effective_config().ai.request_timeout_secs.max(1));
        let models: ModelsFn = {
            let session = session.clone();
            Arc::new(move || session.consumer.models())
        };
        let call: LlmCallFn = {
            let session = session.clone();
            Arc::new(move |messages, tools, model, delta_tx| {
                let session = session.clone();
                Box::pin(async move {
                    let model = network_model(model);
                    session
                        .consumer
                        .validate_api_model(protocol::SERVICE_CHAT, model.as_deref())?;
                    session.broadcast(ProtocolMessage::ConsumerHello).await;
                    let info = session
                        .consumer
                        .wait_for_model(model.as_deref(), DISCOVERY_TIMEOUT)
                        .await?;
                    session
                        .consumer
                        .request_tools(&info.node_id, messages, tools, model, timeout, delta_tx)
                        .await
                })
            })
        };
        let tts: TtsFn = {
            let session = session.clone();
            Arc::new(move |params, lang| {
                let session = session.clone();
                Box::pin(async move {
                    session
                        .consumer
                        .validate_api_model(protocol::SERVICE_TTS, Some(&params.model))?;
                    session.broadcast(ProtocolMessage::ConsumerHello).await;
                    let info = session
                        .consumer
                        .wait_for_service(protocol::SERVICE_TTS, DISCOVERY_TIMEOUT)
                        .await?;
                    session
                        .voice
                        .synthesize(&info.node_id, params, lang, timeout)
                        .await
                })
            })
        };
        let stt: SttFn = {
            let session = session.clone();
            Arc::new(move |params| {
                let session = session.clone();
                Box::pin(async move {
                    session
                        .consumer
                        .validate_api_model(protocol::SERVICE_STT, Some(&params.model))?;
                    session.broadcast(ProtocolMessage::ConsumerHello).await;
                    let info = session
                        .consumer
                        .wait_for_service(protocol::SERVICE_STT, DISCOVERY_TIMEOUT)
                        .await?;
                    session
                        .voice
                        .transcribe(&info.node_id, params, timeout)
                        .await
                })
            })
        };
        let oai: api_server::OaiFn = Arc::new(move |mut body| {
            let session = session.clone();
            Box::pin(async move {
                let model =
                    network_model(body.get("model").and_then(Value::as_str).map(String::from));
                session
                    .consumer
                    .validate_api_model(protocol::SERVICE_OAI, model.as_deref())?;
                body["model"] = json!(model.unwrap_or_default());
                body["stream"] = json!(false);
                body.as_object_mut()
                    .context("invalid OAI body")?
                    .remove("temperature");
                session.broadcast(ProtocolMessage::ConsumerHello).await;
                let info = session
                    .consumer
                    .wait_for_service_model(
                        protocol::SERVICE_OAI,
                        body.get("model")
                            .and_then(Value::as_str)
                            .filter(|s| !s.is_empty()),
                        DISCOVERY_TIMEOUT,
                    )
                    .await?;
                session
                    .oai
                    .request(
                        &info.node_id,
                        "/chat/completions",
                        &serde_json::to_vec(&body)?,
                        timeout,
                    )
                    .await
            })
        });
        Ok(Some(api_server::RoomRoutes {
            call,
            models,
            tts,
            stt,
            oai,
        }))
    }

    fn cache_room_models(&self, session: &AiRoom) {
        let _guard = session.cache_lock.lock().expect("ai room cache lock");
        let models = session.consumer.models();
        for provider in self
            .state
            .effective_config()
            .ai
            .providers
            .iter()
            .filter(|p| p.enabled && p.room() == Some(session.room.as_str()))
        {
            if let Err(err) = self.state.cache_ai_models(provider, &models) {
                warn!(%err, "ai: failed to persist room model cache");
            }
        }
    }

    async fn join_locked(&self, room: &str) -> Result<Arc<AiRoom>> {
        if let Some(session) = self.rooms.read().expect("ai rooms lock").get(room).cloned() {
            return Ok(session);
        }
        let transport = crate::net::ensure_started(&self.state, room.to_string()).await?;
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<(String, ProtocolMessage)>(SEND_QUEUE_CAPACITY);
        let send_room = room.to_string();
        tokio::spawn(async move {
            while let Some((to, msg)) = rx.recv().await {
                if let Err(err) =
                    crate::net::send_direct(&send_room, &to, protocol::encode(&msg)).await
                {
                    debug!(%err, "ai: send failed");
                }
            }
        });
        let send: SendFn = Arc::new(move |to, msg| {
            if tx.try_send((to.to_string(), msg)).is_err() {
                debug!("ai: send queue full or closed");
            }
        });
        let session = Arc::new(AiRoom {
            cache_lock: std::sync::Mutex::new(()),
            room: transport.room.clone(),
            node_id: transport.node_id.clone(),
            consumer: Consumer::new(send.clone()),
            voice: voice_consumer::VoiceConsumer::new(send.clone()),
            oai: oai_tunnel::TunnelConsumer::new(send.clone()),
            send,
            provider: RwLock::new(None),
            handlers: Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_HANDLERS)),
            dropped_messages: std::sync::atomic::AtomicU64::new(0),
        });
        session
            .consumer
            .set_trusted_providers(self.state.effective_config().ai.trusted_providers);
        session.consumer.set_room(room.to_string());
        self.rooms
            .write()
            .expect("ai rooms lock")
            .insert(room.to_string(), session.clone());
        session.broadcast(ProtocolMessage::ConsumerHello).await;
        Ok(session)
    }

    async fn room(&self, room: &str) -> Result<Arc<AiRoom>> {
        let _guard = self.sync.lock().await;
        self.on_demand
            .write()
            .expect("ai demand lock")
            .insert(room.to_string());
        self.join_locked(room).await
    }

    async fn reconcile(&self, reload: bool) -> Result<()> {
        let _guard = self.sync.lock().await;
        let config = self.state.effective_config();
        let sessions = self.rooms.read().expect("ai rooms lock").clone();
        let mut withdrawn = Vec::new();
        // Withdraw before joins or upstream discovery can fail. Consumer references
        // may keep a room connected after its local provider has stopped.
        for session in sessions.values() {
            let providing = config
                .ai
                .providers
                .iter()
                .any(|p| providing_room(p) && p.room() == Some(session.room.as_str()));
            if (reload || !providing)
                && session
                    .provider
                    .write()
                    .expect("ai provider lock")
                    .take()
                    .is_some()
            {
                if !providing {
                    self.on_demand
                        .write()
                        .expect("ai demand lock")
                        .remove(&session.room);
                    withdrawn.push(session.clone());
                }
            }
        }
        for session in withdrawn {
            session
                .broadcast(ProtocolMessage::ProviderHello {
                    models: Some(Vec::new()),
                    services: Some(Vec::new()),
                    voices: None,
                })
                .await;
        }
        let mut wanted = referenced_rooms(&config);
        wanted.extend(
            self.state
                .ai_external
                .lock()
                .expect("external registrations lock")
                .consuming_rooms(&config.ai),
        );
        self.on_demand
            .write()
            .expect("ai demand lock")
            .retain(|room| {
                config
                    .ai
                    .providers
                    .iter()
                    .any(|p| p.enabled && p.room() == Some(room.as_str()))
            });
        wanted.extend(
            self.on_demand
                .read()
                .expect("ai demand lock")
                .iter()
                .cloned(),
        );
        let removed: Vec<_> = self
            .rooms
            .read()
            .expect("ai rooms lock")
            .keys()
            .filter(|r| !wanted.contains(*r))
            .cloned()
            .collect();
        for room in removed {
            let session = self.rooms.write().expect("ai rooms lock").remove(&room);
            if let Some(session) = session {
                *session.provider.write().expect("ai provider lock") = None;
                session.consumer.reject_all("AI room disabled or removed");
                session.voice.reject_all();
                session.oai.reject_all();
                crate::net::leave_room(&room).await?;
            }
        }
        for room in wanted {
            self.join_locked(&room).await?;
        }
        let sessions = self.rooms.read().expect("ai rooms lock").clone();
        for session in sessions.values() {
            if session.local_provider().is_some() {
                continue;
            }
            if let Some(room_config) = config
                .ai
                .providers
                .iter()
                .find(|p| providing_room(p) && p.room() == Some(session.room.as_str()))
            {
                let built = build_provider(session, &config.ai, room_config).await?;
                *session.provider.write().expect("ai provider lock") = Some(built.clone());
                session.broadcast(built.hello()).await;
            }
        }
        Ok(())
    }

    fn model_list(&self) -> Vec<String> {
        let config = self.state.effective_config();
        let rooms = self.rooms.read().expect("ai rooms lock");
        let mut models = Vec::new();
        for provider in config.ai.providers.iter().filter(|p| p.enabled) {
            let available = provider
                .room()
                .and_then(|room| rooms.get(room))
                .map(|session| session.consumer.models())
                .unwrap_or_else(|| provider.models.clone());
            for model in available {
                if !models.contains(&model) {
                    models.push(model);
                }
            }
        }
        if let Some(reference) = &config.ai.default_ref
            && crate::config::resolve_ref_exact(&config.ai, reference).is_some()
            && reference.model != "network-auto"
            && !models.contains(&reference.model)
        {
            models.push(reference.model.clone());
        }
        models
    }

    async fn chat(
        &self,
        messages: Vec<ChatMessage>,
        tools: ToolOptions,
        model: Option<String>,
        delta_tx: Option<UnboundedSender<String>>,
    ) -> Result<(ChatOutput, &'static str, Option<String>)> {
        let config = self.state.effective_config();
        let reference = resolve_api_ref(&config.ai, model.as_deref(), &self.live_models())?;
        if let Some(model) = network_model(model)
            && let Some(room) = config
                .ai
                .providers
                .iter()
                .find(|p| p.id == reference.provider_id)
                .and_then(|p| p.room())
        {
            self.room(room)
                .await?
                .consumer
                .validate_api_model(protocol::SERVICE_CHAT, Some(&model))?;
        }
        self.chat_ref(
            &reference,
            tools.reasoning_effort.clone(),
            messages,
            tools,
            delta_tx,
        )
        .await
    }

    fn live_models(&self) -> HashMap<String, Vec<String>> {
        self.rooms
            .read()
            .expect("ai rooms lock")
            .iter()
            .map(|(room, session)| (room.clone(), session.consumer.models()))
            .collect()
    }

    async fn chat_ref(
        &self,
        reference: &crate::config::ModelRef,
        effort: Option<String>,
        messages: Vec<ChatMessage>,
        mut tools: ToolOptions,
        delta_tx: Option<UnboundedSender<String>>,
    ) -> Result<(ChatOutput, &'static str, Option<String>)> {
        tools.reasoning_effort = effort.clone().or(tools.reasoning_effort);
        let config = self.state.effective_config();
        let resolved = crate::config::resolve_ref(&config.ai, Some(reference))
            .context("ai: model reference is unavailable; check ai.providers and ai.default_ref")?;
        if let Some(room) = resolved.base_url.strip_prefix("mist-network://") {
            let session = self.room(room).await?;
            let requested = (resolved.model != "network-auto").then_some(resolved.model.clone());
            let info = match session.consumer.provider_for_model(requested.as_deref()) {
                Some(info) => info,
                None => {
                    session.broadcast(ProtocolMessage::ConsumerHello).await;
                    session
                        .consumer
                        .wait_for_model(requested.as_deref(), DISCOVERY_TIMEOUT)
                        .await?
                }
            };
            let output = session
                .consumer
                .request_tools(
                    &info.node_id,
                    messages,
                    tools,
                    requested,
                    Duration::from_secs(config.ai.request_timeout_secs.max(1)),
                    delta_tx,
                )
                .await?;
            return Ok((output, "p2p", Some(info.node_id)));
        }
        let upstream = UpstreamConfig {
            base_url: resolved.base_url,
            api_key: resolved.api_key,
            model: Some(resolved.model),
            reasoning_effort: effort,
        };
        let output =
            openai::stream_chat_completion_tools(&upstream, &messages, None, &tools, delta_tx)
                .await?;
        Ok((output, "local", None))
    }

    async fn synthesize(
        &self,
        state: &Arc<AppState>,
        req: tts::TtsParams,
        lang: Option<String>,
    ) -> Result<tts::TtsAudio> {
        let cfg = state.effective_config().ai;
        let resolved_ref = crate::config::resolve_voice(&cfg, cfg.tts.as_ref())
            .context("ai: no usable ai.tts configured")?;
        if let Some(room) = resolved_ref.base_url.strip_prefix("mist-network://") {
            let session = self.room(room).await?;
            session
                .consumer
                .validate_api_model(protocol::SERVICE_TTS, Some(&req.model))?;
            session.broadcast(ProtocolMessage::ConsumerHello).await;
            let info = session
                .consumer
                .wait_for_service(protocol::SERVICE_TTS, DISCOVERY_TIMEOUT)
                .await?;
            let req = tts::TtsParams {
                model: if req.model.trim().is_empty() {
                    resolved_ref.model
                } else {
                    req.model
                },
                voice: if req.voice.trim().is_empty() {
                    resolved_ref.voice.unwrap_or_default()
                } else {
                    req.voice
                },
                ..req
            };
            return session
                .voice
                .synthesize(
                    &info.node_id,
                    req,
                    lang,
                    Duration::from_secs(cfg.request_timeout_secs.max(1)),
                )
                .await;
        }
        let resolved = voice_ref_provider(&cfg, cfg.tts.as_ref())
            .context("ai: no usable ai.tts configured")?;
        validate_voice_api_model(&cfg, &req.model, &self.live_models())?;
        let (provider, voice_config) = resolved.clone();
        let request_voice = (!req.voice.trim().is_empty()).then(|| req.voice.clone());
        let catalog = if request_voice.is_none() && (lang.is_some() || voice_config.voice.is_none())
        {
            tokio::time::timeout(
                VOICE_CATALOG_TIMEOUT,
                resolve_advertised_voices(Some(&resolved)),
            )
            .await
            .unwrap_or_default()
        } else {
            Vec::new()
        };
        let voice = resolve_tts_voice(
            request_voice,
            lang.as_deref(),
            &voice_config.lang_voices,
            &catalog,
            &voice_config.voice,
        )
        .unwrap_or_default();
        let req = tts::TtsParams {
            model: if req.model.trim().is_empty() {
                voice_config.model
            } else {
                req.model
            },
            voice,
            ..req
        };
        tts::synthesize(&provider, req).await
    }

    async fn transcribe(&self, state: &Arc<AppState>, req: stt::SttParams) -> Result<String> {
        let cfg = state.effective_config().ai;
        let resolved_ref = cfg
            .stt
            .as_ref()
            .and_then(|r| crate::config::resolve_ref(&cfg, Some(r)))
            .context("ai: no usable ai.stt configured")?;
        if let Some(room) = resolved_ref.base_url.strip_prefix("mist-network://") {
            let session = self.room(room).await?;
            session
                .consumer
                .validate_api_model(protocol::SERVICE_STT, Some(&req.model))?;
            session.broadcast(ProtocolMessage::ConsumerHello).await;
            let info = session
                .consumer
                .wait_for_service(protocol::SERVICE_STT, DISCOVERY_TIMEOUT)
                .await?;
            let req = stt::SttParams {
                model: if req.model.trim().is_empty() {
                    resolved_ref.model
                } else {
                    req.model
                },
                ..req
            };
            return session
                .voice
                .transcribe(
                    &info.node_id,
                    req,
                    Duration::from_secs(cfg.request_timeout_secs.max(1)),
                )
                .await;
        }
        let (provider, resolved) = model_ref_provider(&cfg, cfg.stt.as_ref())
            .context("ai: no usable ai.stt configured")?;
        validate_voice_api_model(&cfg, &req.model, &self.live_models())?;
        let req = stt::SttParams {
            model: if req.model.trim().is_empty() {
                resolved.model
            } else {
                req.model
            },
            ..req
        };
        stt::transcribe(&provider, req).await
    }
}

/// Explicit raw ids use the default when it names the id, then enabled caches.
/// A room default can resolve an uncached model through live network discovery.
fn resolve_api_ref(
    ai: &crate::config::AiConfig,
    model: Option<&str>,
    live: &HashMap<String, Vec<String>>,
) -> Result<crate::config::ModelRef> {
    use crate::config::{ModelRef, resolve_ref_exact};
    let default = ai
        .default_ref
        .as_ref()
        .filter(|r| resolve_ref_exact(ai, r).is_some());
    let Some(model) = model.filter(|m| !m.trim().is_empty()) else {
        return default
            .cloned()
            .context("ai: no usable ai.default_ref configured");
    };
    if let Some(reference) = known_api_ref(ai, model, live) {
        return Ok(reference);
    }
    if let Some(default) = default
        && ai
            .providers
            .iter()
            .any(|p| p.id == default.provider_id && p.room().is_some())
    {
        return Ok(ModelRef {
            provider_id: default.provider_id.clone(),
            model: model.to_string(),
        });
    }
    Err(ModelNotFound(model.to_string()).into())
}

fn known_api_ref(
    ai: &crate::config::AiConfig,
    model: &str,
    live: &HashMap<String, Vec<String>>,
) -> Option<crate::config::ModelRef> {
    use crate::config::{ModelRef, resolve_ref_exact};
    if let Some(reference) = ai.default_ref.as_ref()
        && reference.model == model
        && resolve_ref_exact(ai, reference).is_some()
    {
        return Some(reference.clone());
    }
    for provider in ai.providers.iter().filter(|p| p.enabled) {
        let models = provider
            .room()
            .and_then(|room| live.get(room))
            .unwrap_or(&provider.models);
        if models.iter().any(|m| m == model) {
            return Some(ModelRef {
                provider_id: provider.id.clone(),
                model: model.to_string(),
            });
        }
    }
    let tts = ai.tts.as_ref().map(|r| ModelRef {
        provider_id: r.provider_id.clone(),
        model: r.model.clone(),
    });
    for reference in ai.stt.iter().chain(tts.iter()).chain(
        ai.providers
            .iter()
            .filter(|p| p.enabled && p.room().is_some())
            .flat_map(|p| p.shared.iter()),
    ) {
        if reference.model == model && resolve_ref_exact(ai, reference).is_some() {
            return Some(reference.clone());
        }
    }
    None
}

fn validate_voice_api_model(
    ai: &crate::config::AiConfig,
    model: &str,
    live: &HashMap<String, Vec<String>>,
) -> Result<()> {
    if model.trim().is_empty() {
        return Ok(());
    }
    if known_api_ref(ai, model, live).is_some() {
        return Ok(());
    }
    Err(ModelNotFound(model.to_string()).into())
}

fn enabled_api_room(ai: &crate::config::AiConfig, room: &str) -> bool {
    ai.providers
        .iter()
        .any(|p| p.enabled && p.room() == Some(room))
}

fn network_model(model: Option<String>) -> Option<String> {
    model.filter(|m| !m.is_empty() && m != "network-auto")
}

pub(crate) async fn chat_model(
    state: &Arc<AppState>,
    reference: Option<&crate::config::ModelRef>,
    effort: Option<String>,
    messages: Vec<ChatMessage>,
) -> Result<String> {
    let service = ensure_started(state).await?;
    let config = state.effective_config();
    let reference = reference
        .or(config.ai.default_ref.as_ref())
        .context("ai.default_ref is not set")?;
    Ok(service
        .chat_ref(reference, effort, messages, ToolOptions::default(), None)
        .await?
        .0
        .content)
}

pub(crate) async fn synthesize_model(
    state: &Arc<AppState>,
    reference: &crate::config::ModelRef,
    req: tts::TtsParams,
) -> Result<tts::TtsAudio> {
    let service = ensure_started(state).await?;
    let cfg = state.effective_config().ai;
    let resolved = crate::config::resolve_ref(&cfg, Some(reference))
        .context("ai: TTS model reference unavailable")?;
    if let Some(room) = resolved.base_url.strip_prefix("mist-network://") {
        let session = service.room(room).await?;
        session.broadcast(ProtocolMessage::ConsumerHello).await;
        let info = session
            .consumer
            .wait_for_service(protocol::SERVICE_TTS, DISCOVERY_TIMEOUT)
            .await?;
        session
            .voice
            .synthesize(
                &info.node_id,
                req,
                None,
                Duration::from_secs(cfg.request_timeout_secs.max(1)),
            )
            .await
    } else {
        let provider = crate::config::AiProviderConfig {
            base_url: resolved.base_url,
            api_key: resolved.api_key,
            ..Default::default()
        };
        tts::synthesize(&provider, req).await
    }
}

pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    if cmd.starts_with("ai.external.") {
        return handle_external(cmd, args, state).await;
    }
    if cmd == "ai.status" {
        return status(state).await;
    }
    if cmd == "ai.upstream_models" {
        let id = args
            .get("provider_id")
            .and_then(Value::as_str)
            .context("ai.upstream_models requires provider_id")?;
        return state.ai_model_discovery.fetch(state, id).await;
    }
    if matches!(cmd, "ai.provide.start" | "ai.provide.stop") {
        let start = cmd == "ai.provide.start";
        let was_running = status(state).await?["providing"].as_bool().unwrap_or(false);
        let rooms = state.set_ai_provide(start)?;
        if state.network.permitted() {
            reload_provider_if_running(state).await?;
        }
        let providing = status(state).await?["providing"].as_bool().unwrap_or(false);
        return Ok(if start {
            json!({ "providing": providing, "already_running": was_running, "rooms": rooms,
                "models": SERVICE.get().map(|s| s.model_list()).unwrap_or_default() })
        } else {
            json!({ "providing": providing, "was_running": was_running, "rooms": rooms })
        });
    }
    let service = ensure_started(state).await?;
    match cmd {
        "ai.chat" => {
            let prompt = args
                .get("prompt")
                .and_then(Value::as_str)
                .context("ai.chat requires prompt")?;
            let model = args.get("model").and_then(Value::as_str).map(String::from);
            let (output, via, provider) = service
                .chat(
                    vec![ChatMessage::new("user", prompt)],
                    ToolOptions {
                        reasoning_effort: args
                            .get("reasoning_effort")
                            .and_then(Value::as_str)
                            .map(String::from),
                        ..Default::default()
                    },
                    model,
                    None,
                )
                .await?;
            Ok(json!({ "content": output.content, "via": via, "provider": provider }))
        }
        "ai.models" => Ok(json!({ "via": "configured", "models": service.model_list() })),
        "ai.serve.start" => serve_start(&service, state).await,
        "ai.serve.stop" => {
            let stopped = service
                .api_server
                .lock()
                .await
                .take()
                .map(|server| server.stop())
                .is_some();
            persist_serve_state(false);
            Ok(json!({ "serving": false, "was_running": stopped }))
        }
        _ => bail!("unknown command: {cmd}"),
    }
}

async fn discover_models(
    state: &Arc<AppState>,
    provider: &crate::config::AiProviderConfig,
) -> Result<Value> {
    if !provider.enabled {
        return Ok(json!({ "models": provider.models, "live": false }));
    }
    let result = if let Some(room) = provider.room() {
        match ensure_started(state).await {
            Ok(service) => match service.room(room).await {
                Ok(session) => {
                    if session.consumer.wait_for_catalog(DISCOVERY_TIMEOUT).await {
                        Ok(session.consumer.models())
                    } else {
                        Err(anyhow::anyhow!(
                            "No model advertisement received from this room"
                        ))
                    }
                }
                Err(err) => Err(err),
            },
            Err(err) => Err(err),
        }
    } else {
        openai::fetch_models(&UpstreamConfig {
            base_url: provider.base_url.clone(),
            api_key: provider.api_key.clone(),
            model: None,
            reasoning_effort: None,
        })
        .await
    };
    match result {
        Ok(models) => {
            state.cache_ai_models(provider, &models)?;
            Ok(json!({ "models": models, "live": true }))
        }
        Err(err) => {
            Ok(json!({ "models": provider.models, "live": false, "error": format!("{err:#}") }))
        }
    }
}

async fn status(state: &Arc<AppState>) -> Result<Value> {
    status_with_service(state, SERVICE.get()).await
}

async fn handle_external(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    if cmd == "ai.external.get" {
        let owner = match args.get("owner") {
            None | Some(Value::Null) => None,
            Some(Value::String(owner)) => Some(owner.as_str()),
            _ => bail!("owner must be a string"),
        };
        let status = status(state).await?;
        return state
            .ai_external
            .lock()
            .expect("external registrations lock")
            .get(
                owner,
                &state.config().ai,
                status["rooms"].as_array().unwrap(),
            );
    }
    let previous = state.effective_config().ai;
    let owner = args
        .get("owner")
        .and_then(Value::as_str)
        .context("external command requires owner")?
        .to_string();
    let changed = match cmd {
        "ai.external.apply" => state
            .ai_external
            .lock()
            .expect("external registrations lock")
            .apply(serde_json::from_value(args).context("invalid external registration")?)?,
        "ai.external.remove" => state
            .ai_external
            .lock()
            .expect("external registrations lock")
            .remove(&owner)?,
        _ => bail!("unknown command: {cmd}"),
    };
    if changed && state.network.permitted() {
        spawn_http_refresh(state.clone(), Some(&previous));
        reload_provider_if_running(state).await?;
    }
    if cmd == "ai.external.remove" {
        Ok(json!({"owner":owner,"removed":changed}))
    } else {
        let warnings = state
            .ai_external
            .lock()
            .expect("external registrations lock")
            .warnings(&owner, &state.config().ai);
        Ok(json!({"owner":owner,"applied":true,"warnings":warnings}))
    }
}

async fn status_with_service(
    state: &Arc<AppState>,
    service: Option<&Arc<AiService>>,
) -> Result<Value> {
    let config = state.effective_config();
    let sessions = service
        .map(|s| s.rooms.read().expect("ai rooms lock").clone())
        .unwrap_or_default();
    let connections = if sessions.is_empty() {
        Vec::new()
    } else {
        crate::net::room_connections().await
    };
    let mut rooms = Vec::new();
    for provider in config.ai.providers.iter().filter(|p| p.room().is_some()) {
        let room = provider.room().unwrap();
        let session = sessions.get(room);
        let peers = connections
            .iter()
            .find(|(r, _)| r == room)
            .map(|(_, peers)| peers.len())
            .unwrap_or(0);
        rooms.push(json!({ "provider_id": provider.id, "room": room, "enabled": provider.enabled,
            "owners": state.ai_external.lock().expect("external registrations lock").owners(room),
            "models": session.filter(|s| s.consumer.has_catalog()).map(|s| s.consumer.models()),
            "joined": provider.enabled && session.is_some(), "providing": provider.enabled && provider.provide && session.is_some_and(|s| s.local_provider().is_some()), "peers": peers }));
    }
    let first = config
        .ai
        .providers
        .iter()
        .filter_map(|p| p.room())
        .find_map(|room| sessions.get(room));
    let providers: Vec<_> = sessions
        .values()
        .filter_map(|s| s.local_provider())
        .collect();
    let models: std::collections::BTreeSet<_> = providers.iter().flat_map(|p| p.models()).collect();
    let services: std::collections::BTreeSet<_> =
        providers.iter().flat_map(|p| p.services()).collect();
    let mut logs: Vec<_> = providers.iter().flat_map(|p| p.logs()).collect();
    logs.sort_by(|a, b| b.started_at.cmp(&a.started_at));
    logs.truncate(5);
    let remote = first.and_then(|s| s.consumer.provider()).map(|info| json!({ "node_id": info.node_id,
        "models": info.models, "services": info.services, "provider_trusted": info.trusted, "provider_did": info.did }));
    let serving = if let Some(service) = service {
        service
            .api_server
            .lock()
            .await
            .as_ref()
            .map(|s| s.addr().to_string())
    } else {
        None
    };
    Ok(
        json!({ "room": first.map(|s| &s.room), "node_id": first.map(|s| &s.node_id),
        "connected_peers": connections.iter().flat_map(|(_, p)| p.iter().map(|(node, _)| node)).collect::<HashSet<_>>().len(),
        "providing": rooms.iter().any(|r| r["providing"] == true),
        "models": models,
        "services": services,
        "recent_requests": logs,
        "serving": serving, "remote_provider": remote, "rooms": rooms,
        "dropped_messages": sessions.values().map(|s| s.dropped_messages.load(std::sync::atomic::Ordering::Relaxed)).sum::<u64>(),
        "trusted_providers_configured": first.map(|s| !s.consumer.trusted_providers().is_empty()).unwrap_or(!config.ai.trusted_providers.is_empty()) }),
    )
}

/// Apply room flags even when no AI command has initialized the service yet.
pub async fn reload_provider_if_running(state: &Arc<AppState>) -> Result<()> {
    if let Some(service) = SERVICE.get() {
        service.reconcile(true).await
    } else {
        ensure_started(state).await.map(|_| ())
    }
}
pub fn apply_trusted_providers(state: &Arc<AppState>) {
    if let Some(service) = SERVICE.get() {
        for session in service.rooms.read().expect("ai rooms lock").values() {
            session
                .consumer
                .set_trusted_providers(state.effective_config().ai.trusted_providers.clone());
        }
    }
}

pub fn spawn_room_connections(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(err) = ensure_started(&state).await {
            warn!(%err, "ai: room connections failed to start");
        }
    });
}

fn resolve_advertised_models(
    cfg: &crate::config::AiConfig,
    room: &crate::config::AiProviderConfig,
) -> (Vec<String>, HashMap<String, crate::config::ResolvedModel>) {
    let mut models = Vec::new();
    let mut advertised = HashMap::new();
    for reference in &room.shared {
        let Some(resolved) = crate::config::resolve_ref_exact(cfg, reference) else {
            continue;
        };
        if resolved.base_url.starts_with("mist-network://")
            || advertised.contains_key(&reference.model)
        {
            continue;
        }
        models.push(reference.model.clone());
        advertised.insert(reference.model.clone(), resolved);
    }
    (models, advertised)
}

async fn build_provider(
    session: &Arc<AiRoom>,
    cfg: &crate::config::AiConfig,
    room: &crate::config::AiProviderConfig,
) -> Result<Arc<Provider>> {
    let (models, advertised) = resolve_advertised_models(cfg, room);
    let default = cfg
        .default_ref
        .as_ref()
        .and_then(|r| crate::config::resolve_ref_exact(cfg, r))
        .filter(|r| !r.base_url.starts_with("mist-network://"))
        .or_else(|| models.first().and_then(|m| advertised.get(m)).cloned());
    let oai_default = default.clone();
    let call: LlmCallFn = Arc::new(move |messages, tools, _, delta_tx| {
        let default = default.clone();
        Box::pin(async move {
            let default =
                default.context("ai: no enabled HTTP ai.default_ref or usable shared ref")?;
            let upstream = UpstreamConfig {
                base_url: default.base_url,
                api_key: default.api_key,
                model: Some(default.model),
                reasoning_effort: default.reasoning_effort,
            };
            openai::stream_chat_completion_tools(&upstream, &messages, None, &tools, delta_tx).await
        })
    });
    let tts_ref = voice_ref_provider(cfg, cfg.tts.as_ref());
    let voices = tokio::time::timeout(
        VOICE_CATALOG_TIMEOUT,
        resolve_advertised_voices(tts_ref.as_ref()),
    )
    .await
    .unwrap_or_default();
    let tts_call = tts_ref.map(|(provider, resolved)| {
        let catalog = voices.clone();
        let call: TtsCallFn = Arc::new(move |text, model, voice, lang| {
            let provider = provider.clone();
            let voice = resolve_tts_voice(
                voice,
                lang.as_deref(),
                &resolved.lang_voices,
                &catalog,
                &resolved.voice,
            )
            .unwrap_or_default();
            let req = tts::TtsParams {
                model: resolve_voice_call_model(model, &resolved.model, "tts"),
                voice,
                input: text,
                format: None,
                speed: None,
            };
            Box::pin(async move { tts::synthesize(&provider, req).await })
        });
        call
    });
    let stt_call = model_ref_provider(cfg, cfg.stt.as_ref()).map(|(provider, resolved)| {
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
        session.send.clone(),
        call,
        models,
        advertised,
        tts_call,
        stt_call,
        voices,
    );
    provider.set_shared_restricted(!room.shared.is_empty());
    // Providing rooms support the tunnel even if their upstream is unavailable;
    // the resolver rejects those requests without hiding the wire capability.
    let (_, shared) = resolve_advertised_models(cfg, room);
    let restricted = !room.shared.is_empty();
    provider.set_oai(oai_tunnel::TunnelProvider::new(
        session.send.clone(),
        Arc::new(move |_path, body| {
            let model = body.get("model").and_then(Value::as_str).unwrap_or("");
            resolve_oai_model(&shared, &oai_default, restricted, model)
        }),
    ));
    Ok(provider)
}

fn resolve_oai_model(
    shared: &HashMap<String, crate::config::ResolvedModel>,
    default: &Option<crate::config::ResolvedModel>,
    restricted: bool,
    model: &str,
) -> Result<Option<crate::config::ResolvedModel>> {
    if !model.is_empty() {
        if let Some(target) = shared.get(model) {
            return Ok(Some(target.clone()));
        }
        if restricted {
            bail!("model_not_shared");
        }
    }
    Ok(default.clone())
}

fn voice_ref_provider(
    cfg: &crate::config::AiConfig,
    voice: Option<&crate::config::VoiceConfig>,
) -> Option<(
    crate::config::AiProviderConfig,
    crate::config::ResolvedModel,
)> {
    let resolved = crate::config::resolve_voice(cfg, voice)?;
    if resolved.base_url.starts_with("mist-network://") {
        return None;
    }
    let provider = crate::config::AiProviderConfig {
        base_url: resolved.base_url.clone(),
        api_key: resolved.api_key.clone(),
        ..Default::default()
    };
    Some((provider, resolved))
}

fn model_ref_provider(
    cfg: &crate::config::AiConfig,
    reference: Option<&crate::config::ModelRef>,
) -> Option<(
    crate::config::AiProviderConfig,
    crate::config::ResolvedModel,
)> {
    let resolved = crate::config::resolve_ref(cfg, Some(reference?))?;
    if resolved.base_url.starts_with("mist-network://") {
        return None;
    }
    let provider = crate::config::AiProviderConfig {
        base_url: resolved.base_url.clone(),
        api_key: resolved.api_key.clone(),
        ..Default::default()
    };
    Some((provider, resolved))
}

/// Discard the retired global switch regardless of its value or JSON validity.
/// Room configuration is the sole providing intent, including on offline starts.
pub fn migrate_provide_state() {
    let result = crate::config::data_dir().and_then(|dir| provide_state::migrate(&dir));
    if let Err(err) = result {
        warn!(%err, "ai: failed to remove legacy provide state; its flag is ignored");
    }
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

/// Builds the voice catalog to advertise in `provider_hello.voices`
/// (tts-voice-selection-v1 §2.1/§2.5), given the already-resolved tts
/// preset/provider (or `None` when tts isn't configured at all). Fallback
/// order:
///
/// 1. the tts config's own upstream catalog, via [`openai::fetch_voices`]
///    (`GET {base_url}/audio/voices` -> `GET {base_url}/voices` -> `[]`,
///    matching mistai's web providers' discovery so mistl advertises the
///    same voices a tc-translate/tc-lingo provider on the same upstream
///    would);
/// 2. the resolved configuration's single configured `voice`, only when (1) came
///    back empty (unreachable upstream, no such endpoint, or a genuinely
///    empty catalog);
/// 3. otherwise `[]` -- `hello()` then omits the `voices` field entirely.
///
/// `hello()` truncates the result to `MAX_ADVERTISED_VOICES` regardless of
/// which source populated it. Split out from `build_provider` so this
/// fallback order can be unit-tested against a mock upstream without
/// standing up a full `AiService`.
async fn resolve_advertised_voices(
    tts_config: Option<&(
        crate::config::AiProviderConfig,
        crate::config::ResolvedModel,
    )>,
) -> Vec<String> {
    let Some((provider, resolved)) = tts_config else {
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
/// `VoiceConfig::lang_voices`'s doc comment and the README example), but
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
/// then fall through to the configuration/catalog-first fallback, same as if no
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
/// 2. if `lang` is present: [`lookup_lang_voice`] against the tts config's
///    `lang_voices` map, by primary subtag ([`lang_primary_subtag`],
///    case-insensitive).
/// 3. if `lang` is present and step 2 found nothing:
///    [`kokoro_style_lang_voice`] -- only applies when the advertised
///    catalog is uniformly kokoro-shaped, a deliberately conservative
///    heuristic (see its own doc comment); logged via `info!` when it
///    fires, since it's a guess rather than something the operator
///    configured.
/// 4. the tts config's configured `voice`.
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
    config_voice: &Option<String>,
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
    if let Some(voice) = config_voice.clone() {
        return Some(voice);
    }
    let voice = catalog.first().cloned()?;
    info!(
        %voice,
        "ai: tts_request had no voice and the tts config has none configured; \
         using the first voice from the advertised catalog"
    );
    Some(voice)
}

/// Resolves the effective `model` for one `tts_request`/`stt_request`
/// against `preset_model` (the tts/stt configuration's own configured model), per
/// mistllm-wire's "provider の voice/model 尊重規則": unlike `voice`, the
/// request's `model` is only ever honored when it *exactly matches*
/// `preset_model` -- otherwise (mismatch, or omitted entirely) this
/// provider's own configured model is used instead. This is a fallback, not
/// a rejection: a mismatched model never fails the request, it's silently
/// replaced. The mismatch case matters because consumers may echo back an
/// advertised *label* (e.g. a chat configuration's display name such as `"TTS"`)
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
        Arc::new(move |messages, tools, model, delta_tx| {
            let service = service.clone();
            Box::pin(async move {
                service
                    .chat(messages, tools, model, delta_tx)
                    .await
                    .map(|(output, _, _)| output)
            })
        })
    };
    let models_fn: ModelsFn = {
        let service = service.clone();
        Arc::new(move || service.model_list())
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

    let api_listen = state.effective_config().ai.api_listen;
    let rooms: api_server::RoomsFn = {
        let service = service.clone();
        Arc::new(move |room| Box::pin(service.clone().api_room(room)))
    };
    let server =
        ApiServer::start_with_rooms(&api_listen, call, models_fn, tts_fn, stt_fn, Some(rooms))
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
    use crate::config::{AiConfig, AiProviderConfig, ModelRef};

    #[test]
    fn oai_models_use_room_shares_and_same_empty_model_rules_as_chat() {
        let mut ai = sample_config();
        ai.providers.insert(
            0,
            AiProviderConfig {
                id: "other".into(),
                base_url: "http://other/v1".into(),
                ..Default::default()
            },
        );
        ai.providers[2].shared.push(ModelRef {
            provider_id: "other".into(),
            model: "raw".into(),
        });
        let (_, shared) = resolve_advertised_models(&ai, &ai.providers[2]);
        let default = ai
            .default_ref
            .as_ref()
            .and_then(|r| crate::config::resolve_ref_exact(&ai, r));
        assert_eq!(
            resolve_oai_model(&shared, &default, true, "raw")
                .unwrap()
                .unwrap()
                .base_url,
            "http://127.0.0.1/v1"
        );
        assert_eq!(
            resolve_oai_model(&shared, &default, true, "")
                .unwrap()
                .unwrap()
                .model,
            "raw"
        );
        assert_eq!(
            resolve_oai_model(&shared, &default, true, "unshared")
                .unwrap_err()
                .to_string(),
            "model_not_shared"
        );
        assert!(
            resolve_oai_model(&HashMap::new(), &default, true, "raw").is_err(),
            "unavailable shares still restrict named requests"
        );
        assert_eq!(
            resolve_oai_model(&HashMap::new(), &default, false, "unknown")
                .unwrap()
                .unwrap()
                .model,
            "raw"
        );
    }

    #[tokio::test]
    async fn built_room_provider_advertises_oai_even_when_targets_are_unavailable() {
        let ai = sample_config();
        let provider = build_provider(&test_session("test"), &ai, &ai.providers[1])
            .await
            .unwrap();
        assert!(provider.services().iter().any(|s| s == "oai"));
        let mut ai = ai;
        ai.providers[0].enabled = false;
        let provider = build_provider(&test_session("test"), &ai, &ai.providers[1])
            .await
            .unwrap();
        assert!(provider.services().iter().any(|s| s == "oai"));
    }
    #[test]
    fn scoped_api_requires_an_enabled_exact_room_and_normalizes_auto_model() {
        let mut ai = sample_config();
        assert!(enabled_api_room(&ai, "test"));
        assert!(!enabled_api_room(&ai, "missing"));
        ai.providers[1].enabled = false;
        assert!(!enabled_api_room(&ai, "test"));
        assert_eq!(network_model(Some("network-auto".into())), None);
        assert_eq!(network_model(Some("raw".into())), Some("raw".into()));
    }

    fn sample_config() -> AiConfig {
        AiConfig {
            providers: vec![
                AiProviderConfig {
                    id: "http".into(),
                    base_url: "http://127.0.0.1/v1".into(),
                    models: vec!["raw".into()],
                    ..Default::default()
                },
                AiProviderConfig {
                    id: "room".into(),
                    base_url: "mist-network://test".into(),
                    shared: vec![ModelRef {
                        provider_id: "http".into(),
                        model: "raw".into(),
                    }],
                    ..Default::default()
                },
            ],
            default_ref: Some(ModelRef {
                provider_id: "http".into(),
                model: "raw".into(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn advertisements_use_raw_ids_and_skip_disabled_or_room_refs() {
        let mut ai = sample_config();
        ai.providers[1].shared.push(ModelRef {
            provider_id: "room".into(),
            model: "loop".into(),
        });
        ai.providers[1].shared.push(ModelRef {
            provider_id: "http".into(),
            model: "raw".into(),
        });
        let (models, refs) = resolve_advertised_models(&ai, &ai.providers[1]);
        assert_eq!(models, vec!["raw"]);
        assert_eq!(refs.len(), 1);
        ai.providers[0].enabled = false;
        assert!(
            resolve_advertised_models(&ai, &ai.providers[1])
                .0
                .is_empty()
        );
    }

    #[test]
    fn duplicate_raw_ids_keep_shared_list_order() {
        let mut ai = sample_config();
        ai.providers.push(AiProviderConfig {
            id: "second".into(),
            base_url: "http://second/v1".into(),
            ..Default::default()
        });
        ai.providers[1].shared.insert(
            0,
            ModelRef {
                provider_id: "second".into(),
                model: "raw".into(),
            },
        );
        let (_, refs) = resolve_advertised_models(&ai, &ai.providers[1]);
        assert_eq!(refs["raw"].base_url, "http://second/v1");
    }

    #[test]
    fn api_resolution_uses_default_and_enabled_caches_without_guessing() {
        let mut ai = sample_config();
        let live = HashMap::new();
        assert_eq!(
            resolve_api_ref(&ai, Some("raw"), &live)
                .unwrap()
                .provider_id,
            "http"
        );
        assert!(resolve_api_ref(&ai, Some("unknown"), &live).is_err());
        ai.providers[0].enabled = false;
        assert!(resolve_api_ref(&ai, None, &live).is_err());
        ai.default_ref = Some(ModelRef {
            provider_id: "room".into(),
            model: "raw".into(),
        });
        assert_eq!(
            resolve_api_ref(&ai, Some("unknown"), &live)
                .unwrap()
                .provider_id,
            "room"
        );
    }

    #[test]
    fn live_room_catalog_replaces_stale_cache() {
        let mut ai = sample_config();
        ai.providers[1].models = vec!["old".into()];
        let live = HashMap::from([("test".into(), vec!["new".into()])]);
        assert!(resolve_api_ref(&ai, Some("old"), &live).is_err());
        assert_eq!(
            resolve_api_ref(&ai, Some("new"), &live)
                .unwrap()
                .provider_id,
            "room"
        );
    }

    #[test]
    fn api_unknown_models_have_a_clean_error_and_configured_models_remain_usable() {
        let mut ai = sample_config();
        ai.providers[0].models.clear();
        let live = HashMap::new();
        assert_eq!(resolve_api_ref(&ai, None, &live).unwrap().model, "raw");
        assert_eq!(
            resolve_api_ref(&ai, Some("raw"), &live).unwrap().model,
            "raw"
        );
        ai.tts = Some(crate::config::VoiceConfig {
            provider_id: "http".into(),
            model: "speech".into(),
            ..Default::default()
        });
        ai.stt = Some(ModelRef {
            provider_id: "http".into(),
            model: "transcribe".into(),
        });
        for model in ["raw", "speech", "transcribe"] {
            validate_voice_api_model(&ai, model, &live).unwrap();
            assert_eq!(
                resolve_api_ref(&ai, Some(model), &live).unwrap().model,
                model
            );
        }
        validate_voice_api_model(&ai, "", &live).unwrap();
        let error = resolve_api_ref(&ai, Some("unknown"), &live).unwrap_err();
        assert!(error.downcast_ref::<ModelNotFound>().is_some());
        assert_eq!(
            error.to_string(),
            "The model `unknown` does not exist or is not available from enabled providers."
        );
        assert!(validate_voice_api_model(&ai, "unknown", &live).is_err());
        ai.providers[0].enabled = false;
        assert!(resolve_api_ref(&ai, Some("speech"), &live).is_err());
        assert!(validate_voice_api_model(&ai, "transcribe", &live).is_err());
    }

    #[tokio::test]
    async fn local_api_unknown_models_return_404_before_http_or_room_calls() {
        let state = AppState::for_test();
        let mut config = state.config();
        config.ai = sample_config();
        config.ai.tts = Some(crate::config::VoiceConfig {
            provider_id: "http".into(),
            model: "speech".into(),
            voice: Some("speaker".into()),
            ..Default::default()
        });
        config.ai.stt = Some(ModelRef {
            provider_id: "http".into(),
            model: "transcribe".into(),
        });
        state.set_config(config);
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = sent.clone();
        let session = test_session_with_send(
            "test",
            Arc::new(move |_, _| {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }),
        );
        session.consumer.handle_message(
            "peer",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["raw".into(), "speech".into(), "transcribe".into()]),
                services: Some(vec![
                    "chat".into(),
                    "tts".into(),
                    "stt".into(),
                    "oai".into(),
                ]),
                voices: None,
            },
        );
        sent.store(0, std::sync::atomic::Ordering::Relaxed);
        let service = Arc::new(AiService {
            state: state.clone(),
            rooms: RwLock::new(HashMap::from([("test".into(), session)])),
            on_demand: RwLock::new(HashSet::new()),
            sync: Mutex::new(()),
            api_server: Mutex::new(None),
        });
        let chat_service = service.clone();
        let call: LlmCallFn = Arc::new(move |m, t, model, tx| {
            let service = chat_service.clone();
            Box::pin(async move {
                service
                    .chat(m, t, model, tx)
                    .await
                    .map(|(output, _, _)| output)
            })
        });
        let tts_service = service.clone();
        let tts_state = state.clone();
        let tts: TtsFn = Arc::new(move |params, lang| {
            let service = tts_service.clone();
            let state = tts_state.clone();
            Box::pin(async move { service.synthesize(&state, params, lang).await })
        });
        let stt_service = service.clone();
        let stt_state = state.clone();
        let stt: SttFn = Arc::new(move |params| {
            let service = stt_service.clone();
            let state = stt_state.clone();
            Box::pin(async move { service.transcribe(&state, params).await })
        });
        let room_service = service.clone();
        let rooms: api_server::RoomsFn =
            Arc::new(move |room| Box::pin(room_service.clone().api_room(room)));
        let server = ApiServer::start_with_rooms(
            "127.0.0.1:0",
            call,
            Arc::new(Vec::new),
            tts,
            stt,
            Some(rooms),
        )
        .await
        .unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        for prefix in ["/v1", "/v1/rooms/test"] {
            for stream in [false, true] {
                let response = client.post(format!("http://{}{prefix}/chat/completions", server.addr()))
                    .json(&json!({"model":"missing","messages":[{"role":"user","content":"hi"}],"stream":stream}))
                    .send().await.unwrap();
                assert_model_not_found(response).await;
            }
            let response = client
                .post(format!("http://{}{prefix}/audio/speech", server.addr()))
                .json(&json!({"model":"missing","input":"hi","voice":"speaker"}))
                .send()
                .await
                .unwrap();
            assert_model_not_found(response).await;
            let body = "--b\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nmissing\r\n--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.wav\"\r\nContent-Type: audio/wav\r\n\r\naudio\r\n--b--\r\n";
            let response = client
                .post(format!(
                    "http://{}{prefix}/audio/transcriptions",
                    server.addr()
                ))
                .header("content-type", "multipart/form-data; boundary=b")
                .body(body)
                .send()
                .await
                .unwrap();
            assert_model_not_found(response).await;
        }
        for stream in [false, true] {
            let response = client.post(format!("http://{}/v1/rooms/test/chat/completions", server.addr()))
                .json(&json!({"model":"missing","stream":stream,"messages":[{"role":"user","content":[
                    {"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"data:image/png;base64,YQ=="}}
                ]}]})).send().await.unwrap();
            assert_model_not_found(response).await;
        }
        // A default Room reference must also reject from its current catalog.
        let mut config = state.config();
        config.ai.default_ref = Some(ModelRef {
            provider_id: "room".into(),
            model: "raw".into(),
        });
        config.ai.tts.as_mut().unwrap().provider_id = "room".into();
        config.ai.stt.as_mut().unwrap().provider_id = "room".into();
        state.set_config(config);
        let response = client
            .post(format!("http://{}/v1/chat/completions", server.addr()))
            .json(&json!({"model":"missing","messages":[{"role":"user","content":"hi"}]}))
            .send()
            .await
            .unwrap();
        assert_model_not_found(response).await;
        let response = client
            .post(format!("http://{}/v1/audio/speech", server.addr()))
            .json(&json!({"model":"missing","input":"hi"}))
            .send()
            .await
            .unwrap();
        assert_model_not_found(response).await;
        assert_eq!(
            sent.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "unknown requests must not be sent to peers"
        );
        server.stop();
    }

    async fn assert_model_not_found(response: reqwest::Response) {
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(
            response.json::<Value>().await.unwrap(),
            json!({"error": {
                "message":"The model `missing` does not exist or is not available from enabled providers.",
                "type":"invalid_request_error", "param":"model", "code":"model_not_found",
            }})
        );
    }

    #[test]
    fn room_join_policy_uses_references_and_provide_flag() {
        let mut config = crate::config::Config::default();
        config.ai = sample_config();
        assert!(referenced_rooms(&config).is_empty());
        config.ai.providers[1].provide = true;
        assert_eq!(referenced_rooms(&config), HashSet::from(["test".into()]));
        config.ai.providers[1].enabled = false;
        assert!(referenced_rooms(&config).is_empty());
        config.ai.providers[1].enabled = true;
        config.ai.providers[1].provide = false;
        config.ai.default_ref = Some(ModelRef {
            provider_id: "room".into(),
            model: "raw".into(),
        });
        assert!(referenced_rooms(&config).contains("test"));
    }

    #[test]
    fn providing_decision_requires_enabled_room_flag_but_not_shared_models() {
        let mut ai = sample_config();
        assert!(!ai.providers.iter().any(providing_room));
        ai.providers[0].provide = true;
        assert!(
            !ai.providers.iter().any(providing_room),
            "HTTP flags do not provide"
        );
        ai.providers[1].provide = true;
        ai.providers[1].shared.clear();
        assert!(
            ai.providers.iter().any(providing_room),
            "empty sharing still provides"
        );
        ai.providers[1].enabled = false;
        assert!(!ai.providers.iter().any(providing_room));
        ai.providers[1].enabled = true;
        ai.providers[1].provide = false;
        assert!(!ai.providers.iter().any(providing_room));
    }

    #[test]
    fn cli_provide_flags_map_start_and_stop_without_changing_other_settings() {
        let mut ai = sample_config();
        let mut empty = ai.providers[1].clone();
        empty.id = "empty".into();
        empty.base_url = "mist-network://empty".into();
        empty.shared.clear();
        ai.providers.push(empty.clone());
        empty.id = "empty-on".into();
        empty.base_url = "mist-network://empty-on".into();
        empty.provide = true;
        ai.providers.push(empty);
        let mut disabled = ai.providers[1].clone();
        disabled.id = "disabled".into();
        disabled.base_url = "mist-network://disabled".into();
        disabled.enabled = false;
        ai.providers.push(disabled.clone());
        disabled.id = "disabled-on".into();
        disabled.base_url = "mist-network://disabled-on".into();
        disabled.provide = true;
        ai.providers.push(disabled);
        ai.providers[0].provide = true;
        let original = ai.clone();
        assert_eq!(set_provide_flags(&mut ai, true), ["test"]);
        assert_eq!(
            ai.providers.iter().map(|p| p.provide).collect::<Vec<_>>(),
            [true, true, false, true, false, true]
        );
        assert_eq!(
            set_provide_flags(&mut ai, true),
            ["test"],
            "start is idempotent"
        );
        assert_eq!(
            set_provide_flags(&mut ai, false),
            ["test", "empty-on", "disabled-on"]
        );
        assert_eq!(
            ai.providers.iter().map(|p| p.provide).collect::<Vec<_>>(),
            [true, false, false, false, false, false]
        );
        assert!(set_provide_flags(&mut ai, false).is_empty());
        for (provider, old) in ai.providers.iter_mut().zip(&original.providers) {
            provider.provide = old.provide;
        }
        assert_eq!(
            serde_json::to_value(ai).unwrap(),
            serde_json::to_value(original).unwrap()
        );
    }

    #[tokio::test]
    async fn reconcile_starts_from_flags_reloads_and_stops_without_global_state() {
        let state = AppState::for_test();
        let mut config = state.config();
        config.ai = sample_config();
        config.ai.providers[1].provide = true;
        // A consumer reference keeps the mocked transport joined after stopping.
        config.ai.default_ref = Some(ModelRef {
            provider_id: "room".into(),
            model: "raw".into(),
        });
        state.set_config(config);
        let session = test_session("test");
        let service = Arc::new(AiService {
            state: state.clone(),
            rooms: RwLock::new(HashMap::from([("test".into(), session.clone())])),
            on_demand: RwLock::new(HashSet::new()),
            sync: Mutex::new(()),
            api_server: Mutex::new(None),
        });
        service.reconcile(false).await.unwrap();
        let first = session.local_provider().unwrap();
        assert_eq!(first.models(), ["raw"]);
        let status = status_with_service(&state, Some(&service)).await.unwrap();
        assert_eq!(status["providing"], true);
        assert_eq!(status["rooms"][0]["providing"], true);
        service.reconcile(false).await.unwrap();
        assert!(
            Arc::ptr_eq(&first, &session.local_provider().unwrap()),
            "ordinary calls preserve the provider"
        );

        let mut config = state.config();
        config.ai.providers[1].shared.clear();
        state.set_config(config);
        service.reconcile(true).await.unwrap();
        assert!(session.local_provider().unwrap().models().is_empty());
        assert_eq!(
            status_with_service(&state, Some(&service)).await.unwrap()["providing"],
            true
        );

        let mut config = state.config();
        config.ai.providers[1].provide = false;
        state.set_config(config);
        service.reconcile(true).await.unwrap();
        assert!(session.local_provider().is_none());
        let status = status_with_service(&state, Some(&service)).await.unwrap();
        assert_eq!(status["providing"], false);
        assert_eq!(status["rooms"][0]["providing"], false);
        assert_eq!(status["rooms"][0]["joined"], true);

        let mut config = state.config();
        config.ai.providers[1].provide = true;
        state.set_config(config);
        service.reconcile(true).await.unwrap();
        assert!(session.local_provider().is_some());
        let mut config = state.config();
        config.ai.providers[1].enabled = false;
        state.set_config(config);
        service.reconcile(true).await.unwrap();
        assert!(session.local_provider().is_none());
        assert!(service.rooms.read().unwrap().is_empty());
        assert_eq!(
            status_with_service(&state, Some(&service)).await.unwrap()["providing"],
            false
        );
    }

    #[tokio::test]
    async fn external_rooms_reconcile_live_and_are_available_to_room_api_and_status() {
        let state = AppState::for_test();
        let before = serde_json::to_value(state.config()).unwrap();
        let payload = json!({"owner":"app","providers":[{"id":"http","label":"Local","base_url":"http://127.0.0.1:1/v1","api_key":"secret","enabled":true}],
            "rooms":[{"room":"test","consume":true,"provide":true,"shared":[{"provider_id":"http","model":"external-model"}]}]});
        state
            .ai_external
            .lock()
            .unwrap()
            .apply(serde_json::from_value(payload.clone()).unwrap())
            .unwrap();
        let session = test_session("test");
        let service = Arc::new(AiService {
            state: state.clone(),
            rooms: RwLock::new(HashMap::from([("test".into(), session.clone())])),
            on_demand: RwLock::new(HashSet::new()),
            sync: Mutex::new(()),
            api_server: Mutex::new(None),
        });
        service.reconcile(true).await.unwrap();
        assert_eq!(
            session.local_provider().unwrap().models(),
            ["external-model"]
        );
        assert!(enabled_api_room(&state.effective_config().ai, "test"));
        assert!(
            service
                .clone()
                .api_room("test".into())
                .await
                .unwrap()
                .is_some()
        );
        let status = status_with_service(&state, Some(&service)).await.unwrap();
        assert_eq!(status["rooms"][0]["owners"], json!(["app"]));
        assert_eq!(status["rooms"][0]["providing"], true);
        let mut changed = payload;
        changed["rooms"][0]["shared"][0]["model"] = json!("changed-model");
        state
            .ai_external
            .lock()
            .unwrap()
            .apply(serde_json::from_value(changed).unwrap())
            .unwrap();
        service.reconcile(true).await.unwrap();
        assert_eq!(
            session.local_provider().unwrap().models(),
            ["changed-model"]
        );
        state.ai_external.lock().unwrap().remove("app").unwrap();
        service.reconcile(true).await.unwrap();
        assert!(session.local_provider().is_none());
        assert!(service.rooms.read().unwrap().is_empty());
        assert!(
            service
                .clone()
                .api_room("test".into())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(serde_json::to_value(state.config()).unwrap(), before);
    }

    #[test]
    fn unset_or_missing_voice_ref_does_not_adopt_chat_default() {
        let ai = sample_config();
        assert!(voice_ref_provider(&ai, None).is_none());
        let voice = crate::config::VoiceConfig {
            provider_id: "missing".into(),
            model: "speech".into(),
            ..Default::default()
        };
        assert!(voice_ref_provider(&ai, Some(&voice)).is_none());
    }

    #[tokio::test]
    async fn status_rooms_contract_includes_disabled_rooms_without_joining() {
        let state = AppState::for_test();
        let mut config = crate::config::Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "disabled-room".into(),
            base_url: "mist-network://disabled-test".into(),
            enabled: false,
            provide: true,
            ..Default::default()
        });
        state.set_config(config);
        let status = handle("ai.status", json!({}), &state).await.unwrap();
        assert_eq!(
            status["rooms"],
            json!([{ "provider_id": "disabled-room", "room": "disabled-test", "enabled": false,
            "joined": false, "providing": false, "peers": 0, "models": null, "owners": [] }])
        );
    }

    fn test_session(room: &str) -> Arc<AiRoom> {
        test_session_with_send(room, Arc::new(|_, _| {}))
    }

    fn test_session_with_send(room: &str, send: SendFn) -> Arc<AiRoom> {
        Arc::new(AiRoom {
            cache_lock: std::sync::Mutex::new(()),
            room: room.into(),
            node_id: "node".into(),
            consumer: Consumer::new(send.clone()),
            voice: voice_consumer::VoiceConsumer::new(send.clone()),
            oai: oai_tunnel::TunnelConsumer::new(send.clone()),
            send,
            provider: RwLock::new(None),
            handlers: Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_HANDLERS)),
            dropped_messages: std::sync::atomic::AtomicU64::new(0),
        })
    }

    #[tokio::test]
    async fn external_and_config_room_events_reply_with_scoped_models_and_services() {
        for restart in [false, true] {
            let state = AppState::for_test();
            let mut config = state.config();
            config.ai = sample_config();
            config.ai.providers[1].provide = true;
            state.set_config(config);
            let saved = serde_json::to_value(state.config()).unwrap();
            let (base_url, request) = mock_chat_server_capture("external reply").await;
            let payload = json!({"owner":"app","providers":[{"id":"up","label":"Upstream","base_url":base_url,"api_key":"","enabled":true}],
                "rooms":[{"room":"external-only","consume":false,"provide":true,"shared":[{"provider_id":"up","model":"external-model"}]}]});
            let dir = std::env::temp_dir()
                .join(format!("mistl-room-events-{:016x}", rand::random::<u64>()));
            let mut store = external::Store::load(&dir).unwrap();
            store
                .apply(serde_json::from_value(payload.clone()).unwrap())
                .unwrap();
            *state.ai_external.lock().unwrap() = if restart {
                external::Store::load(&dir).unwrap()
            } else {
                store
            };
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut sessions = HashMap::new();
            for room in ["test", "external-only"] {
                let tx = tx.clone();
                let send: SendFn = Arc::new(move |to, msg| {
                    tx.send((room, to.to_string(), protocol::encode(&msg)))
                        .unwrap();
                });
                sessions.insert(room.to_string(), test_session_with_send(room, send));
            }
            let service = Arc::new(AiService {
                state: state.clone(),
                rooms: RwLock::new(sessions),
                on_demand: RwLock::new(HashSet::new()),
                sync: Mutex::new(()),
                api_server: Mutex::new(None),
            });
            service.reconcile(true).await.unwrap();
            let rt = tokio::runtime::Handle::current();
            for (room, model) in [("test", "raw"), ("external-only", "external-model")] {
                service.handle_room_event(
                    crate::net::EVENT_RAW,
                    room,
                    "consumer",
                    &protocol::encode(&ProtocolMessage::ConsumerHello),
                    &rt,
                );
                let (sent_room, to, bytes) =
                    tokio::time::timeout(Duration::from_secs(2), rx.recv())
                        .await
                        .unwrap()
                        .unwrap();
                let hello: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(sent_room, room);
                assert_eq!(to, "consumer");
                assert_eq!(hello["type"], "provider_hello");
                assert_eq!(hello["models"], json!([model]));
                assert_eq!(hello["services"], json!(["chat", "tools", "oai"]));
                let local = service.rooms.read().unwrap()[room]
                    .local_provider()
                    .unwrap();
                assert_eq!(hello["services"], json!(local.services()));
                service.handle_room_event(crate::net::EVENT_JOIN, room, "new-consumer", &[], &rt);
                assert_eq!(
                    protocol::decode(&rx.recv().await.unwrap().2),
                    Some(ProtocolMessage::ConsumerHello)
                );
                let join_hello: Value =
                    serde_json::from_slice(&rx.recv().await.unwrap().2).unwrap();
                assert_eq!(join_hello, hello);
            }
            let chat = ProtocolMessage::LlmRequest {
                id: "request".into(),
                messages: vec![ChatMessage {
                    role: "user".into(),
                    content: "probe".into(),
                    tool_calls: None,
                    tool_call_id: None,
                }],
                model: Some("external-model".into()),
                reasoning_effort: Some("high".into()),
                tools: None,
                tool_choice: None,
            };
            service.handle_room_event(
                crate::net::EVENT_RAW,
                "external-only",
                "consumer",
                &protocol::encode(&chat),
                &rt,
            );
            let request = tokio::time::timeout(Duration::from_secs(2), request)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request["model"], "external-model");
            assert_eq!(request["reasoning_effort"], "high");
            assert!(request.get("temperature").is_none());
            loop {
                let (room, to, bytes) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(room, "external-only");
                assert_eq!(to, "consumer");
                if let Some(ProtocolMessage::LlmResponseDone { content, .. }) =
                    protocol::decode(&bytes)
                {
                    assert_eq!(content.as_deref(), Some("external reply"));
                    break;
                }
            }
            service.handle_room_event(
                crate::net::EVENT_RAW,
                "test",
                "consumer",
                &protocol::encode(&chat),
                &rt,
            );
            let reply = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(protocol::decode(&reply.2), Some(ProtocolMessage::LlmError { code: Some(code), .. }) if code == "model_not_shared")
            );
            service.handle_room_event(
                crate::net::EVENT_RAW,
                "unjoined",
                "consumer",
                &protocol::encode(&ProtocolMessage::ConsumerHello),
                &rt,
            );
            assert!(rx.try_recv().is_err());
            assert_eq!(serde_json::to_value(state.config()).unwrap(), saved);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[tokio::test]
    async fn received_hellos_replace_room_cache_and_withdrawals_clear_it() {
        let state = AppState::for_test();
        let mut config = state.config();
        config.ai = sample_config();
        state.set_config(config);
        let session = test_session("test");
        let service = AiService {
            state: state.clone(),
            rooms: RwLock::new(HashMap::new()),
            on_demand: RwLock::new(HashSet::new()),
            sync: Mutex::new(()),
            api_server: Mutex::new(None),
        };
        let hello = |models| ProtocolMessage::ProviderHello {
            models: Some(models),
            services: None,
            voices: None,
        };
        assert!(
            session
                .consumer
                .handle_message("peer-a", &hello(vec!["first".into()]))
        );
        service.cache_room_models(&session);
        assert_eq!(state.config().ai.providers[1].models, ["first"]);
        assert!(state.config().ai.providers[1].models_fetched_at.is_some());
        assert!(
            session
                .consumer
                .handle_message("peer-a", &hello(vec!["changed".into()]))
        );
        service.cache_room_models(&session);
        assert_eq!(state.config().ai.providers[1].models, ["changed"]);
        assert!(
            session
                .consumer
                .handle_message("peer-b", &hello(vec!["other".into()]))
        );
        service.cache_room_models(&session);
        assert_eq!(state.config().ai.providers[1].models, ["changed", "other"]);
        assert!(
            !session
                .consumer
                .handle_message("oversized", &hello(vec!["x".into(); 257]))
        );
        assert_eq!(state.config().ai.providers[1].models, ["changed", "other"]);
        let withdrawal = ProtocolMessage::ProviderHello {
            models: Some(Vec::new()),
            services: Some(Vec::new()),
            voices: None,
        };
        session.consumer.handle_message("peer-a", &withdrawal);
        session.consumer.handle_message("peer-b", &withdrawal);
        service.cache_room_models(&session);
        assert!(state.config().ai.providers[1].models.is_empty());
        assert!(
            session
                .consumer
                .wait_for_catalog(Duration::from_millis(10))
                .await
        );
    }

    #[tokio::test]
    async fn provider_hello_is_scoped_to_each_rooms_shared_refs() {
        let mut ai = sample_config();
        let second = AiProviderConfig {
            id: "room2".into(),
            base_url: "mist-network://second".into(),
            shared: vec![ModelRef {
                provider_id: "http".into(),
                model: "other".into(),
            }],
            ..Default::default()
        };
        let first = build_provider(&test_session("test"), &ai, &ai.providers[1])
            .await
            .unwrap();
        let second = build_provider(&test_session("second"), &ai, &second)
            .await
            .unwrap();
        assert_eq!(first.models(), vec!["raw"]);
        assert_eq!(second.models(), vec!["other"]);
        let hello: Value = serde_json::from_slice(&protocol::encode(&first.hello())).unwrap();
        assert_eq!(hello["models"], json!(["raw"]));
        ai.providers[0].enabled = false;
        let first = build_provider(&test_session("test"), &ai, &ai.providers[1])
            .await
            .unwrap();
        assert!(first.models().is_empty());
        assert!(
            first
                .call_upstream(vec![], ToolOptions::default(), Some("raw".into()), None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn empty_inbound_model_prefers_enabled_http_default_over_first_shared() {
        let (base_url, requested_model) = mock_chat_server("default").await;
        let mut ai = sample_config();
        ai.providers[0].base_url = base_url;
        ai.providers[1].shared = vec![ModelRef {
            provider_id: "http".into(),
            model: "shared".into(),
        }];
        let provider = build_provider(&test_session("test"), &ai, &ai.providers[1])
            .await
            .unwrap();
        assert_eq!(
            provider
                .call_upstream(
                    vec![ChatMessage::new("user", "test")],
                    ToolOptions::default(),
                    None,
                    None
                )
                .await
                .unwrap()
                .content,
            "default"
        );
        assert_eq!(requested_model.await.unwrap(), "raw");
    }

    #[tokio::test]
    async fn room_default_uses_first_http_shared_ref_for_inbound_requests() {
        let (base_url, requested_model) = mock_chat_server("shared").await;
        let mut ai = sample_config();
        ai.providers[0].base_url = base_url;
        ai.default_ref = Some(ModelRef {
            provider_id: "room".into(),
            model: "remote".into(),
        });
        let provider = build_provider(&test_session("test"), &ai, &ai.providers[1])
            .await
            .unwrap();
        assert_eq!(
            provider
                .call_upstream(
                    vec![ChatMessage::new("user", "test")],
                    ToolOptions::default(),
                    Some(String::new()),
                    None
                )
                .await
                .unwrap()
                .content,
            "shared"
        );
        assert_eq!(requested_model.await.unwrap(), "raw");
    }

    async fn mock_chat_server(content: &str) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let (url, request) = mock_chat_server_capture(content).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let body = request.await.unwrap();
            let _ = tx.send(body["model"].as_str().unwrap().to_string());
        });
        (url, rx)
    }

    async fn mock_chat_server_capture(
        content: &str,
    ) -> (String, tokio::sync::oneshot::Receiver<Value>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = json!({ "choices": [{ "message": { "content": content } }] }).to_string();
        let response = ok_json(&body);
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buf[..n]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&request[..end]);
                    let len: usize = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap();
                    if request.len() >= end + 4 + len {
                        let body: Value =
                            serde_json::from_slice(&request[end + 4..end + 4 + len]).unwrap();
                        tx.send(body).unwrap();
                        break;
                    }
                }
            }
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (format!("http://{addr}"), rx)
    }

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
            &Some("config-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("request-voice"));
    }

    #[test]
    fn resolve_tts_voice_falls_back_to_the_config_voice() {
        let voice = resolve_tts_voice(
            None,
            None,
            &empty_lang_voices(),
            &no_catalog(),
            &Some("config-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("config-voice"));
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
            &Some("config-voice".to_string()),
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
            &Some("config-voice".to_string()),
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
    fn resolve_tts_voice_lang_with_no_lang_voices_entry_falls_back_to_config_voice() {
        let mut lang_voices = empty_lang_voices();
        lang_voices.insert("ja".to_string(), "ja-voice".to_string());
        // Requested "fr" has no entry and the catalog isn't kokoro-shaped
        // (empty), so this falls all the way through to config_voice.
        let voice = resolve_tts_voice(
            None,
            Some("fr"),
            &lang_voices,
            &no_catalog(),
            &Some("config-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("config-voice"));
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
            &Some("config-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("config-voice"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_does_not_apply_to_an_empty_catalog() {
        let voice = resolve_tts_voice(
            None,
            Some("en"),
            &empty_lang_voices(),
            &no_catalog(),
            &Some("config-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("config-voice"));
    }

    #[test]
    fn resolve_tts_voice_kokoro_heuristic_does_not_apply_for_an_unmapped_language() {
        // "de" (German) has no documented kokoro prefix letter.
        let voice = resolve_tts_voice(
            None,
            Some("de"),
            &empty_lang_voices(),
            &kokoro_catalog(),
            &Some("config-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("config-voice"));
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
            &Some("config-voice".to_string()),
        );
        assert_eq!(voice.as_deref(), Some("config-voice"));
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
    fn resolve_voice_call_model_honors_a_matching_request_model() {
        let model = resolve_voice_call_model(Some("irodori-tts".to_string()), "irodori-tts", "tts");
        assert_eq!(model, "irodori-tts");

        let model = resolve_voice_call_model(Some("whisper-1".to_string()), "whisper-1", "stt");
        assert_eq!(model, "whisper-1");
    }

    #[test]
    fn resolve_voice_call_model_falls_back_on_a_mismatched_or_omitted_request_model() {
        // The regression this guards: a consumer selecting an advertised ad
        // card can echo back its *label* (e.g. "TTS") as `model` rather than
        // a real upstream model id -- that must never be forwarded as-is.
        let model = resolve_voice_call_model(Some("TTS".to_string()), "irodori-tts", "tts");
        assert_eq!(model, "irodori-tts");
        let model = resolve_voice_call_model(None, "irodori-tts", "tts");
        assert_eq!(model, "irodori-tts");

        let model = resolve_voice_call_model(Some("STT".to_string()), "whisper-1", "stt");
        assert_eq!(model, "whisper-1");
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

    fn resolved_voice_config(
        base_url: String,
        voice: Option<&str>,
    ) -> crate::config::ResolvedModel {
        crate::config::ResolvedModel {
            base_url,
            api_key: "key".to_string(),
            model: "tts-1".to_string(),
            reasoning_effort: None,
            voice: voice.map(String::from),
            lang_voices: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn resolve_advertised_voices_empty_when_no_tts_config_configured() {
        assert_eq!(resolve_advertised_voices(None).await, Vec::<String>::new());
    }

    #[tokio::test]
    async fn resolve_advertised_voices_prefers_the_fetched_upstream_catalog() {
        // Fetch succeeds *and* the configuration has its own `voice` set -- the
        // fetched catalog wins (fallback order step 1 beats step 2).
        let base_url = mock_voices_server(&ok_json(r#"{"voices":["nova","shimmer"]}"#)).await;
        let resolved = resolved_voice_config(base_url.clone(), Some("alloy"));
        let provider = crate::config::AiProviderConfig {
            id: String::new(),
            label: String::new(),
            base_url,
            api_key: "key".to_string(),
            ..Default::default()
        };
        let voices = resolve_advertised_voices(Some(&(provider, resolved))).await;
        assert_eq!(voices, vec!["nova".to_string(), "shimmer".to_string()]);
    }

    #[tokio::test]
    async fn resolve_advertised_voices_falls_back_to_config_voice_when_fetch_is_empty() {
        // Both /audio/voices and /voices need to be tried and fail for
        // fetch_voices to return []; a single 404 response closes the
        // connection after the first candidate, so the second candidate
        // request hits a closed/refused socket and also fails -- exercising
        // the "upstream has no voices endpoint at all" case end to end.
        let base_url = mock_voices_server(&not_found()).await;
        let resolved = resolved_voice_config(base_url.clone(), Some("alloy"));
        let provider = crate::config::AiProviderConfig {
            id: String::new(),
            label: String::new(),
            base_url,
            api_key: "key".to_string(),
            ..Default::default()
        };
        let voices = resolve_advertised_voices(Some(&(provider, resolved))).await;
        assert_eq!(voices, vec!["alloy".to_string()]);
    }

    #[tokio::test]
    async fn resolve_advertised_voices_empty_when_fetch_fails_and_preset_has_no_voice() {
        let base_url = mock_voices_server(&not_found()).await;
        let resolved = resolved_voice_config(base_url.clone(), None);
        let provider = crate::config::AiProviderConfig {
            id: String::new(),
            label: String::new(),
            base_url,
            api_key: "key".to_string(),
            ..Default::default()
        };
        let voices = resolve_advertised_voices(Some(&(provider, resolved))).await;
        assert_eq!(voices, Vec::<String>::new());
    }
}
