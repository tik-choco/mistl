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
use tracing::{debug, warn};

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
    // Minimal voice catalog advertisement (tts-voice-selection-v1 §2.1/§3.5):
    // the resolved tts preset's single configured `voice`, if any. Upstream
    // `/audio/voices` discovery is a v2 follow-up (see the draft's §2.5).
    let advertised_voices: Vec<String> = tts_preset
        .as_ref()
        .and_then(|(_, resolved)| resolved.voice.clone())
        .into_iter()
        .collect();
    let tts_call = tts_preset.map(|(provider, resolved)| {
        let call: TtsCallFn = Arc::new(move |text, model, voice| {
            let provider = provider.clone();
            let req = tts::TtsParams {
                model: model.unwrap_or_else(|| resolved.model.clone()),
                voice: voice
                    .or_else(|| resolved.voice.clone())
                    .unwrap_or_default(),
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
    if preset_id.trim().is_empty() {
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
    use crate::config::{AiConfig, AiProviderConfig};

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
}
