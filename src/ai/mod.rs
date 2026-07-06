//! P2P AI network: consume or provide LLM inference over mistlib rooms,
//! wire-compatible with `@tik-choco/mistai` protocol v1 (tc-mistllm /
//! tc-translate / tc-note peers can share the room).
//!
//! Two roles, both optional and combinable:
//!
//! - **provide** (`ai provide start`): announce `provider_hello` and
//!   forward inbound `llm_request`s to the configured OpenAI-compatible
//!   upstream (`[ai] upstream_url`), streaming deltas back as chunks.
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
mod openai;
mod protocol;
mod provider;

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
use provider::Provider;

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

/// IPC entry point for all `ai.*` commands.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
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
        json!({ "node_id": info.node_id, "models": info.models })
    });
    Ok(json!({
        "room": service.room,
        "node_id": service.node_id,
        "connected_peers": crate::net::connected_nodes().await.len(),
        "providing": provider.is_some(),
        "models": provider.as_ref().map(|p| p.models()),
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
    Ok(json!({ "via": "p2p", "provider": info.node_id, "models": info.models }))
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
    let base_url = cfg.upstream_url.clone().context(
        "ai: [ai] upstream_url is not configured; set it in the dashboard's \
         Settings panel or with `mistl config set ai.upstream_url <url>` \
         (e.g. \"http://127.0.0.1:11434/v1\" for Ollama)",
    )?;
    let mut upstream = UpstreamConfig {
        base_url,
        api_key: cfg.upstream_api_key.clone().unwrap_or_default(),
        model: cfg.default_model.clone(),
        temperature: cfg.temperature,
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

    let provider = Provider::new(service.send.clone(), call, models.clone());
    *service.provider.write().expect("ai provider lock") = Some(provider.clone());
    service.broadcast(provider.hello()).await;

    Ok(json!({
        "providing": true,
        "upstream": upstream.base_url,
        "models": models,
    }))
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
