//! tc-ai-api routing and local lifecycle aggregation.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::future::join_all;
use serde_json::{Value, json};

use super::api_server::RelayRequest;
use super::oai_tunnel::{self, TunnelResponse};
use super::{AiRoom, AiService, DISCOVERY_TIMEOUT, ProtocolMessage, protocol, resolve_api_ref};
use crate::config::AiProviderConfig;

const JOB_LIMIT: usize = 1024;
const JOB_TTL: Duration = Duration::from_secs(25 * 60 * 60);

pub(super) struct Jobs<T>(Mutex<HashMap<String, (T, Instant)>>);

impl<T> Default for Jobs<T> {
    fn default() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
}

impl<T: Clone> Jobs<T> {
    fn snapshot(&self) -> HashMap<String, T> {
        self.snapshot_at(Instant::now())
    }

    fn snapshot_at(&self, now: Instant) -> HashMap<String, T> {
        let mut jobs = self.0.lock().expect("relay jobs lock");
        jobs.retain(|_, (_, at)| now.saturating_duration_since(*at) < JOB_TTL);
        jobs.iter()
            .map(|(id, (route, _))| (id.clone(), route.clone()))
            .collect()
    }

    fn remember(&self, response: &TunnelResponse, route: T) {
        if !matches!(response.status, 200 | 202) {
            return;
        }
        let Ok(body) = serde_json::from_slice::<Value>(&response.body) else {
            return;
        };
        let Some(id) = body
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| valid_id(id))
        else {
            return;
        };
        let mut jobs = self.0.lock().expect("relay jobs lock");
        jobs.retain(|_, (_, at)| at.elapsed() < JOB_TTL);
        if jobs.len() >= JOB_LIMIT
            && !jobs.contains_key(id)
            && let Some(oldest) = jobs
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(id, _)| id.clone())
        {
            jobs.remove(&oldest);
        }
        jobs.insert(id.to_string(), (route, Instant::now()));
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum Route {
    Http { provider_id: String },
    Room { room: String },
}

fn json_response(status: u16, body: Value) -> TunnelResponse {
    TunnelResponse {
        status,
        content_type: "application/json".into(),
        body: serde_json::to_vec(&body).expect("JSON response"),
    }
}

fn error(status: u16, message: &str, code: &str, param: Option<&str>) -> TunnelResponse {
    let mut body = json!({"error":{"message":message,"type":"invalid_request_error","code":code}});
    if let Some(param) = param {
        body["error"]["param"] = json!(param);
    }
    json_response(status, body)
}

fn not_found() -> TunnelResponse {
    error(404, "generation not found", "not_found", Some("id"))
}

fn valid_id(id: &str) -> bool {
    id.len() == 38
        && id.starts_with("gen3d_")
        && id.as_bytes()[6..]
            .iter()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c))
}

fn job_id(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/3d/generations/")?;
    let id = rest.strip_suffix("/content").unwrap_or(rest);
    valid_id(id).then_some(id)
}

fn allowed(method: &str, path: &str) -> bool {
    match (method, path) {
        ("GET", "/3d/generations" | "/audio/voices" | "/health" | "/api/ps") => true,
        ("GET", path) => job_id(path).is_some() || oai_tunnel::is_motion_catalog(path),
        ("POST", path) => path == "/chat/completions" || oai_tunnel::is_model_relay_post(path),
        ("DELETE", path) => !path.ends_with("/content") && job_id(path).is_some(),
        _ => false,
    }
}

/// Parses a model-routed passthrough body; `Err` is the 400 to return.
/// There is no built-in default model: routing needs an explicit id.
fn generation_body(request: &RelayRequest) -> std::result::Result<Value, TunnelResponse> {
    let body: Value = serde_json::from_slice(request.body.as_deref().unwrap_or_default())
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| error(400, "body must be a JSON object", "invalid_request", None))?;
    match body.get("model") {
        Some(Value::String(model)) if !model.trim().is_empty() => Ok(body),
        None | Some(Value::Null) | Some(Value::String(_)) => Err(oai_tunnel::missing_model()),
        _ => Err(error(
            400,
            "model must be a string",
            "invalid_request",
            Some("model"),
        )),
    }
}

fn relativize(mut response: TunnelResponse) -> TunnelResponse {
    if response.content_type.to_ascii_lowercase().contains("json")
        && let Ok(mut body) = serde_json::from_slice::<Value>(&response.body)
    {
        oai_tunnel::relativize_job_urls(&mut body);
        response.body = serde_json::to_vec(&body).expect("JSON response");
    }
    response
}

fn merge_lists(responses: Vec<TunnelResponse>) -> TunnelResponse {
    let mut seen = HashSet::new();
    let mut data = Vec::new();
    for response in responses
        .into_iter()
        .filter(|r| (200..300).contains(&r.status))
    {
        if let Ok(body) = serde_json::from_slice::<Value>(&response.body)
            && let Some(jobs) = body.get("data").and_then(Value::as_array)
        {
            for job in jobs {
                if let Some(id) = job
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| valid_id(id))
                    && seen.insert(id.to_string())
                {
                    data.push(job.clone());
                }
            }
        }
    }
    data.sort_by_key(|job| {
        std::cmp::Reverse(
            job.get("created")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
        )
    });
    relativize(json_response(200, json!({"object":"list","data":data})))
}

impl AiService {
    fn generation_route(&self, model: Option<&str>) -> Result<(Route, String)> {
        let config = self.state.effective_config();
        let reference = resolve_api_ref(&config.ai, model, &self.live_models())?;
        let resolved = crate::config::resolve_ref(&config.ai, Some(&reference))
            .context("ai: model reference unavailable")?;
        let route = match resolved.base_url.strip_prefix("mist-network://") {
            Some(room) => Route::Room {
                room: room.to_string(),
            },
            None => Route::Http {
                provider_id: reference.provider_id,
            },
        };
        Ok((route, resolved.model))
    }

    async fn forward_route(
        self: Arc<Self>,
        route: Route,
        request: RelayRequest,
    ) -> Result<TunnelResponse> {
        match route {
            Route::Room { room } => {
                let routes = self
                    .api_room(room)
                    .await?
                    .context("ai: room is not enabled")?;
                if request.path == "/chat/completions" {
                    let body = serde_json::from_slice(request.body.as_deref().unwrap_or_default())?;
                    return (routes.oai)(body).await;
                }
                (routes.relay)(request).await
            }
            Route::Http { provider_id } => {
                let config = self.state.effective_config();
                let provider = config
                    .ai
                    .providers
                    .iter()
                    .find(|p| {
                        p.id == provider_id
                            && p.enabled
                            && !p.base_url.starts_with("mist-network://")
                    })
                    .context("ai: provider unavailable")?;
                let (path, range) = oai_tunnel::split_range(&request.path)?;
                let image_edit = request.method == "POST" && path == "/images/edits";
                // Only the 3D deadline depends on the request body.
                let body = request
                    .body
                    .as_deref()
                    .filter(|_| path == "/3d/generations")
                    .and_then(|raw| serde_json::from_slice::<Value>(raw).ok())
                    .unwrap_or(Value::Null);
                let response = oai_tunnel::forward_capped(
                    &provider.base_url,
                    &provider.api_key,
                    &request.method,
                    &path,
                    request.body,
                    range,
                    oai_tunnel::relay_deadline(&request.method, &path, &body),
                    if image_edit {
                        oai_tunnel::MAX_IMAGE_RESPONSE_BYTES
                    } else {
                        oai_tunnel::MAX_BYTES
                    },
                )
                .await?;
                Ok(if path.starts_with("/3d/generations") {
                    relativize(response)
                } else {
                    response
                })
            }
        }
    }

    pub(super) async fn relay_request(
        self: Arc<Self>,
        mut request: RelayRequest,
    ) -> Result<TunnelResponse> {
        let (path, _) = match oai_tunnel::split_range(&request.path) {
            Ok(parts) => parts,
            Err(err) => return Ok(error(400, &err.to_string(), "invalid_request", None)),
        };
        if !allowed(&request.method, &path) {
            return Ok(not_found());
        }
        if matches!(path.as_str(), "/health" | "/api/ps") {
            return lifecycle(&self.state.effective_config().ai.providers, &request).await;
        }
        if path == "/audio/voices" {
            let config = self.state.effective_config();
            let Some(resolved) = crate::config::resolve_voice(&config.ai, config.ai.tts.as_ref())
            else {
                return Ok(error(404, "No TTS provider configured", "not_found", None));
            };
            if let Some(room) = resolved.base_url.strip_prefix("mist-network://") {
                return self
                    .forward_route(
                        Route::Room {
                            room: room.to_string(),
                        },
                        request,
                    )
                    .await;
            }
            let response = oai_tunnel::forward_with(
                &resolved.base_url,
                &resolved.api_key,
                "GET",
                "/audio/voices",
                None,
                None,
                super::VOICE_CATALOG_TIMEOUT,
            )
            .await;
            if let Ok(response) = response
                && (200..300).contains(&response.status)
            {
                return Ok(response);
            }
            return oai_tunnel::forward_with(
                &resolved.base_url,
                &resolved.api_key,
                "GET",
                "/voices",
                None,
                None,
                super::VOICE_CATALOG_TIMEOUT,
            )
            .await;
        }
        if oai_tunnel::is_motion_catalog(&path) {
            let model = match oai_tunnel::motion_catalog_model(&path) {
                Ok(model) => model,
                Err(response) => return Ok(response),
            };
            let (route, model) = self.generation_route(Some(&model))?;
            request.path = oai_tunnel::motion_catalog_path(&path, &model)?;
            return self.forward_route(route, request).await;
        }
        if request.method == "POST" {
            let body = if path == "/chat/completions" {
                // The local API has already validated the image request.
                serde_json::from_slice(request.body.as_deref().unwrap_or_default())?
            } else {
                match generation_body(&request) {
                    Ok(body) => body,
                    Err(response) => return Ok(response),
                }
            };
            let (route, model) = self.generation_route(body["model"].as_str())?;
            let mut set = vec![("model", json!(model))];
            if path == "/chat/completions" && body.get("stream").is_some() {
                set.push(("stream", json!(false)));
            }
            request.body = Some(oai_tunnel::rewrite_object(
                request.body.as_deref().unwrap_or_default(),
                &set,
                &[],
            )?);
            let response = self.clone().forward_route(route.clone(), request).await?;
            if path == "/3d/generations" {
                self.relay_jobs.remember(&response, route);
            }
            return Ok(response);
        }
        let jobs = self.relay_jobs.snapshot();
        if path == "/3d/generations" {
            let mut routes: HashSet<_> = jobs.into_values().collect();
            {
                let rooms = self.rooms.read().expect("ai rooms lock");
                routes.extend(
                    rooms
                        .iter()
                        .filter(|(_, session)| !session.relay_jobs.snapshot().is_empty())
                        .map(|(room, _)| Route::Room { room: room.clone() }),
                );
            }
            let mut routes: Vec<_> = routes.into_iter().collect();
            // Stable order also makes duplicate ids resolve consistently.
            routes.sort_by_key(|route| format!("{route:?}"));
            let replies = join_all(routes.into_iter().map(|route| {
                self.clone().forward_route(
                    route,
                    RelayRequest {
                        method: "GET".into(),
                        path: path.clone(),
                        body: None,
                    },
                )
            }))
            .await;
            return Ok(merge_lists(
                replies.into_iter().filter_map(Result::ok).collect(),
            ));
        }
        let id = job_id(&path).expect("validated job path");
        // Jobs this daemon did not create (or forgot on restart) are unknown.
        let route = jobs.get(id).cloned().or_else(|| {
            let rooms = self.rooms.read().expect("ai rooms lock");
            rooms.iter().find_map(|(room, session)| {
                session
                    .relay_jobs
                    .snapshot()
                    .contains_key(id)
                    .then(|| Route::Room { room: room.clone() })
            })
        });
        let Some(route) = route else {
            return Ok(not_found());
        };
        self.forward_route(route, request).await
    }
}

impl AiRoom {
    // Called only after Consumer accepts the advertisement, preserving its
    // transport identity, trust policy and catalog bounds.
    pub(super) fn record_relay_voices(&self, from: &str, msg: &ProtocolMessage) {
        if let ProtocolMessage::ProviderHello {
            services, voices, ..
        } = msg
        {
            let mut catalog = self.relay_voices.write().expect("relay voices lock");
            if protocol::advertises_service(services, protocol::SERVICE_TTS) {
                if catalog.len() < 256 || catalog.contains_key(from) {
                    catalog.insert(from.to_string(), voices.clone().unwrap_or_default());
                }
            } else {
                catalog.remove(from);
            }
        }
    }

    pub(super) async fn relay(self: Arc<Self>, request: RelayRequest) -> Result<TunnelResponse> {
        let (path, _) = match oai_tunnel::split_range(&request.path) {
            Ok(parts) => parts,
            Err(err) => return Ok(error(400, &err.to_string(), "invalid_request", None)),
        };
        if !allowed(&request.method, &path)
            || matches!(path.as_str(), "/health" | "/api/ps" | "/chat/completions")
        {
            return Ok(not_found());
        }
        if path == "/audio/voices" {
            self.broadcast(ProtocolMessage::ConsumerHello).await;
            let Ok(info) = self
                .consumer
                .wait_for_service(protocol::SERVICE_TTS, DISCOVERY_TIMEOUT)
                .await
            else {
                return Ok(error(404, "No TTS provider found", "not_found", None));
            };
            let mut voices: BTreeSet<String> = self
                .relay_voices
                .read()
                .expect("relay voices lock")
                .values()
                .flatten()
                .cloned()
                .collect();
            voices.extend(info.voices.unwrap_or_default());
            let data: Vec<_> = voices
                .into_iter()
                .map(|v| json!({"id":v,"name":v,"object":"voice"}))
                .collect();
            return Ok(json_response(
                200,
                json!({"object":"list","count":data.len(),"data":data,"voices":data}),
            ));
        }
        let catalog = oai_tunnel::is_motion_catalog(&path);
        if request.method == "POST" || catalog {
            let (body, model) = if catalog {
                let model = match oai_tunnel::motion_catalog_model(&path) {
                    Ok(model) => model,
                    Err(response) => return Ok(response),
                };
                (Value::Null, model)
            } else {
                let body = match generation_body(&request) {
                    Ok(body) => body,
                    Err(response) => return Ok(response),
                };
                let model = body["model"].as_str().expect("validated model").to_string();
                (body, model)
            };
            self.consumer
                .validate_api_model(protocol::SERVICE_OAI, Some(&model))?;
            self.broadcast(ProtocolMessage::ConsumerHello).await;
            let info = self
                .consumer
                .wait_for_service_model(protocol::SERVICE_OAI, Some(&model), DISCOVERY_TIMEOUT)
                .await?;
            let response = self
                .oai
                .request_method(
                    &info.node_id,
                    &request.method,
                    &path,
                    request.body.as_deref().unwrap_or_default(),
                    oai_tunnel::relay_deadline(&request.method, &path, &body),
                )
                .await?;
            if path == "/3d/generations" {
                self.relay_jobs.remember(&response, info.node_id);
            }
            return Ok(response);
        }
        let jobs = self.relay_jobs.snapshot();
        if path == "/3d/generations" {
            let peers: BTreeSet<_> = jobs.into_values().collect();
            let responses = join_all(peers.iter().map(|peer| {
                self.oai.request_method(
                    peer,
                    "GET",
                    &path,
                    &[],
                    oai_tunnel::relay_deadline("GET", &path, &Value::Null),
                )
            }))
            .await;
            return Ok(merge_lists(
                responses.into_iter().filter_map(Result::ok).collect(),
            ));
        }
        let Some(peer) = jobs.get(job_id(&path).expect("validated job path")) else {
            return Ok(not_found());
        };
        self.oai
            .request_method(
                peer,
                &request.method,
                &request.path,
                &[],
                oai_tunnel::relay_deadline(&request.method, &path, &Value::Null),
            )
            .await
    }
}

fn origin(provider: &AiProviderConfig) -> &str {
    let base = provider.base_url.trim_end_matches('/');
    base.strip_suffix("/v1").unwrap_or(base)
}

/// `GET <origin><path>` on one provider's root (lifecycle routes live
/// outside `/v1`).
async fn lifecycle_get(
    provider: &AiProviderConfig,
    path: &str,
    timeout: u64,
) -> Result<TunnelResponse> {
    oai_tunnel::forward_with(
        origin(provider),
        &provider.api_key,
        "GET",
        path,
        None,
        None,
        Duration::from_secs(timeout),
    )
    .await
}

fn successful_json(response: Result<TunnelResponse>) -> Option<Value> {
    let response = response.ok()?;
    if !(200..300).contains(&response.status) {
        return None;
    }
    serde_json::from_slice(&response.body).ok()
}

async fn lifecycle(
    providers: &[AiProviderConfig],
    request: &RelayRequest,
) -> Result<TunnelResponse> {
    let providers: Vec<_> = providers
        .iter()
        .filter(|p| {
            p.enabled && (p.base_url.starts_with("http://") || p.base_url.starts_with("https://"))
        })
        .collect();
    if request.path == "/health" {
        let results = join_all(providers.iter().map(|p| lifecycle_get(p, "/health", 2))).await;
        let mut backends = serde_json::Map::new();
        for (provider, result) in providers.iter().zip(results) {
            backends.insert(
                provider.id.clone(),
                json!(if result.is_ok_and(|r| (200..300).contains(&r.status)) {
                    "ok"
                } else {
                    "unreachable"
                }),
            );
        }
        return Ok(json_response(
            200,
            json!({"status":"ok","backends":backends}),
        ));
    }
    let results = join_all(providers.iter().map(|p| lifecycle_get(p, "/api/ps", 3))).await;
    let mut models = Vec::new();
    for (provider, result) in providers.iter().zip(results) {
        let Some(entries) = successful_json(result)
            .and_then(|mut v| v.get_mut("models").map(Value::take))
            .and_then(|v| match v {
                Value::Array(entries) => Some(entries),
                _ => None,
            })
        else {
            continue;
        };
        for entry in entries {
            if let Value::Object(mut entry) = entry {
                let backend = entry
                    .get("backend")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(|backend| format!("{}/{backend}", provider.id))
                    .unwrap_or_else(|| provider.id.clone());
                entry.insert("backend".into(), json!(backend));
                models.push(Value::Object(entry));
            }
        }
    }
    Ok(json_response(200, json!({"models":models})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::VoiceConfig;
    use crate::daemon::AppState;
    use std::sync::RwLock;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const ID: &str = "gen3d_00000000000000000000000000000001";
    const OTHER: &str = "gen3d_00000000000000000000000000000002";

    struct Mock {
        provider: AiProviderConfig,
        requests: Arc<Mutex<Vec<(String, String, Value, String)>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn mock(
        id: &str,
        handler: impl Fn(&str, &str, &Value) -> (u16, Value) + Send + Sync + 'static,
    ) -> Mock {
        mock_bytes(id, move |method, path, body| {
            let (status, response) = handler(method, path, body);
            (
                status,
                "application/json",
                serde_json::to_vec(&response).unwrap(),
            )
        })
        .await
    }

    async fn mock_bytes(
        id: &str,
        handler: impl Fn(&str, &str, &Value) -> (u16, &'static str, Vec<u8>) + Send + Sync + 'static,
    ) -> Mock {
        mock_raw(id, move |method, path, body| {
            let body = serde_json::from_slice(body).unwrap_or(Value::Null);
            handler(method, path, &body)
        })
        .await
    }

    async fn mock_raw(
        id: &str,
        handler: impl Fn(&str, &str, &[u8]) -> (u16, &'static str, Vec<u8>) + Send + Sync + 'static,
    ) -> Mock {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/v1/", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let headers_end = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0 && bytes.len() < 128 * 1024);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8(bytes[..headers_end].to_vec()).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                while bytes.len() < headers_end + length {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let mut first = headers.lines().next().unwrap().split_whitespace();
                let method = first.next().unwrap().to_string();
                let path = first.next().unwrap().to_string();
                let body = &bytes[headers_end..headers_end + length];
                let (status, content_type, response) = handler(&method, &path, body);
                let parsed = serde_json::from_slice(body).unwrap_or(Value::Null);
                recorded
                    .lock()
                    .unwrap()
                    .push((method, path, parsed, headers));
                let body = response;
                let header = format!(
                    "HTTP/1.1 {status} Mock\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        Mock {
            provider: AiProviderConfig {
                id: id.into(),
                base_url,
                api_key: "mock-key".into(),
                models: vec!["3d-model".into()],
                ..Default::default()
            },
            requests,
            task,
        }
    }

    fn service(providers: Vec<AiProviderConfig>) -> Arc<AiService> {
        let state = AppState::for_test();
        let mut config = state.config();
        config.ai.providers = providers;
        state.set_config(config);
        Arc::new(AiService {
            state,
            rooms: RwLock::new(HashMap::new()),
            on_demand: Default::default(),
            sync: tokio::sync::Mutex::new(()),
            api_server: tokio::sync::Mutex::new(None),
            relay_jobs: Default::default(),
        })
    }

    fn request(method: &str, path: &str, body: Option<Value>) -> RelayRequest {
        RelayRequest {
            method: method.into(),
            path: path.into(),
            body: body.map(|v| serde_json::to_vec(&v).unwrap()),
        }
    }

    fn room(send: super::super::SendFn) -> Arc<AiRoom> {
        Arc::new(AiRoom {
            relay_jobs: Default::default(),
            relay_voices: Default::default(),
            cache_lock: Mutex::new(()),
            room: "test".into(),
            node_id: "node".into(),
            consumer: super::super::Consumer::new(send.clone()),
            voice: super::super::voice_consumer::VoiceConsumer::new(send.clone()),
            oai: oai_tunnel::TunnelConsumer::new(send.clone()),
            send,
            provider: RwLock::new(None),
            handlers: Arc::new(tokio::sync::Semaphore::new(64)),
            dropped_messages: Default::default(),
        })
    }

    fn value(response: TunnelResponse) -> Value {
        serde_json::from_slice(&response.body).unwrap()
    }

    #[tokio::test]
    async fn game_and_motion_select_http_providers_by_model() {
        const GAME: &str = r#"{ "model":"game-model", "image":"AA==", "game":{"z":0,"a":1} }"#;
        const MOTION: &str = r#"{ "model":"motion-model", "start":[0,1], "format":"csv" }"#;
        const CLIPS: &str = "/motion/clips?tag=a+b&model=motion%2Dmodel&tag=%2B";
        const CSV: &[u8] = b"frame,joint\r\n0,1\r\n";
        let game_reply =
            serde_json::to_vec_pretty(&json!({"id":ID,"data":[{"url":"kept"}]})).unwrap();
        let expected = game_reply.clone();
        let mut game = mock_raw("game", move |method, path, body| {
            assert_eq!(
                (method, path, body),
                ("POST", "/v1/game/act", GAME.as_bytes())
            );
            (202, "application/json", game_reply.clone())
        })
        .await;
        game.provider.models = vec!["game-model".into()];
        let mut motion = mock_raw("motion", |method, path, body| {
            match (method, path.strip_prefix("/v1").unwrap()) {
                ("POST", "/motion/inbetween") => assert_eq!(body, MOTION.as_bytes()),
                ("GET", "/motion/skeletons/motion-model" | CLIPS) => assert!(body.is_empty()),
                _ => panic!("unexpected request {method} {path}"),
            }
            (200, "text/csv", CSV.to_vec())
        })
        .await;
        motion.provider.models = vec!["motion-model".into()];
        let service = service(vec![game.provider.clone(), motion.provider.clone()]);
        for (method, path, body, status, mime, bytes) in [
            (
                "POST",
                "/game/act",
                Some(GAME),
                202,
                "application/json",
                expected.as_slice(),
            ),
            (
                "POST",
                "/motion/inbetween",
                Some(MOTION),
                200,
                "text/csv",
                CSV,
            ),
            (
                "GET",
                "/motion/skeletons/motion%2Dmodel",
                None,
                200,
                "text/csv",
                CSV,
            ),
            ("GET", CLIPS, None, 200, "text/csv", CSV),
        ] {
            let mut req = request(method, path, None);
            req.body = body.map(|s| s.as_bytes().to_vec());
            let response = service.clone().relay(req).await.unwrap();
            assert_eq!(response.status, status);
            assert_eq!(response.content_type, mime);
            assert_eq!(response.body, bytes);
        }
        let response = service
            .clone()
            .relay(request("GET", "/motion/clips", None))
            .await
            .unwrap();
        assert_eq!(response.status, 400);
        assert_eq!(
            value(response)["error"]["code"],
            "missing_required_parameter"
        );
        assert_eq!(game.requests.lock().unwrap().len(), 1);
        assert_eq!(motion.requests.lock().unwrap().len(), 3);
        assert!(service.relay_jobs.snapshot().is_empty());
    }

    #[tokio::test]
    async fn unscoped_vision_chat_forwards_raw_http_body_and_adapts_sse() {
        const VISION: &str = r#"{ "model":"game-model", "temperature":0.4, "stream":false, "messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}], "extra":{"z":1,"a":2} }"#;
        const REPLY: &[u8] = br#"{ "choices": [{"message":{"content":"image answer"}}] }"#;
        let _guard = crate::authz::TEST_LOCK.lock().await;
        crate::authz::test_reset();
        let requests = std::sync::atomic::AtomicUsize::new(0);
        let mut upstream = mock_raw("vision", move |method, path, body| {
            assert_eq!((method, path), ("POST", "/v1/chat/completions"));
            if requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                assert_eq!(body, VISION.as_bytes());
            }
            (200, "application/json", REPLY.to_vec())
        })
        .await;
        upstream.provider.models = vec!["game-model".into()];
        let service = service(vec![upstream.provider.clone()]);
        let relay: super::super::api_server::RelayFn =
            Arc::new(move |request| Box::pin(service.clone().relay(request)));
        let server = super::super::api_server::ApiServer::start_with_rooms(
            "127.0.0.1:0",
            Arc::new(|messages, _, model, _| {
                Box::pin(async move {
                    assert_eq!(model.as_deref(), Some("game-model"));
                    assert_eq!(messages[0].content, "text prompt");
                    Ok("text answer".to_string().into())
                })
            }),
            Arc::new(Vec::new),
            Arc::new(|_, _| Box::pin(async { anyhow::bail!("unused") })),
            Arc::new(|_| Box::pin(async { anyhow::bail!("unused") })),
            None,
            Some(relay),
            None,
            None,
        )
        .await
        .unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let url = format!("http://{}/v1/chat/completions", server.addr());
        let post = |body: String| {
            client
                .post(&url)
                .header("content-type", "application/json")
                .body(body)
                .send()
        };
        let json = post(VISION.into()).await.unwrap();
        assert_eq!(json.status(), 200);
        assert_eq!(json.bytes().await.unwrap().as_ref(), REPLY);
        let sse = post(VISION.replace("\"stream\":false", "\"stream\":true"))
            .await
            .unwrap();
        assert_eq!(sse.status(), 200);
        assert!(
            sse.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );
        let sse = sse.text().await.unwrap();
        assert!(sse.contains("image answer"));
        assert!(sse.contains("data: [DONE]"));
        let text = post(
            r#"{"model":"game-model","messages":[{"role":"user","content":"text prompt"}]}"#.into(),
        )
        .await
        .unwrap();
        assert_eq!(text.status(), 200);
        assert_eq!(
            text.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
            "text answer"
        );
        let calls = upstream.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|(_, _, sent, head)| {
            sent["temperature"] == 0.4
                && sent["stream"] == false
                && sent
                    .pointer("/messages/0/content/0/image_url/url")
                    .and_then(Value::as_str)
                    == Some("data:image/png;base64,AA==")
                && head
                    .to_ascii_lowercase()
                    .contains("authorization: bearer mock-key")
        }));
        server.stop();
    }

    #[tokio::test]
    async fn image_edits_route_to_http_preserve_large_binary_and_json_without_jobs() {
        let image = vec![0xa5; oai_tunnel::MAX_BYTES + 1];
        let expected = image.clone();
        let json =
            serde_json::to_vec_pretty(&json!({"id":ID,"data":[{"url":"unchanged"}]})).unwrap();
        let expected_json = json.clone();
        let mut upstream = mock_bytes("images", move |method, path, body| {
            assert_eq!(method, "POST");
            assert_eq!(path, "/v1/images/edits");
            assert_eq!(body["model"], "image-model");
            assert_eq!(body["image"], "data:image/png;base64,AA==");
            assert_eq!(body["temperature"], 0.7);
            if body["response_format"] == "binary" {
                (200, "image/png", image.clone())
            } else {
                (200, "application/json", json.clone())
            }
        })
        .await;
        upstream.provider.models = vec!["image-model".into()];
        let service = service(vec![upstream.provider.clone()]);
        for (format, content_type, expected) in [
            ("binary", "image/png", expected),
            ("b64_json", "application/json", expected_json),
        ] {
            let response = service
                .clone()
                .relay(request(
                    "POST",
                    "/images/edits",
                    Some(json!({
                        "model":"image-model", "image":"data:image/png;base64,AA==",
                        "response_format":format, "temperature":0.7
                    })),
                ))
                .await
                .unwrap();
            assert_eq!(response.status, 200);
            assert_eq!(response.content_type, content_type);
            assert_eq!(response.body, expected);
            assert!(service.relay_jobs.snapshot().is_empty());
        }
        let calls = upstream.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|(_, _, _, headers)| {
            headers
                .to_ascii_lowercase()
                .contains("authorization: bearer mock-key")
        }));
    }

    #[tokio::test]
    async fn post_records_provider_and_unknown_jobs_are_not_found() {
        let a = mock("a", |method, _, body| {
            if method == "POST" {
                assert_eq!(body["model"], "3d-model");
            }
            (
                if method == "POST" { 202 } else { 200 },
                json!({"id":ID,"backend":"a"}),
            )
        })
        .await;
        let b = mock("b", |_, _, _| (200, json!({"backend":"b"}))).await;
        let service = service(vec![a.provider.clone(), b.provider.clone()]);
        // No built-in default model: an omitted id is a 400, never routed.
        for model in [json!(null), json!("")] {
            let response = service
                .clone()
                .relay(request(
                    "POST",
                    "/3d/generations",
                    Some(json!({"image":"a","model":model})),
                ))
                .await
                .unwrap();
            assert_eq!(response.status, 400);
            assert_eq!(value(response)["error"]["param"], "model");
        }
        let response = service
            .clone()
            .relay(request(
                "POST",
                "/3d/generations",
                Some(json!({"image":"a","model":"3d-model"})),
            ))
            .await
            .unwrap();
        assert_eq!(response.status, 202);
        let response = service
            .clone()
            .relay(request("GET", &format!("/3d/generations/{ID}"), None))
            .await
            .unwrap();
        assert_eq!(value(response)["backend"], "a");
        let response = service
            .clone()
            .relay(request("GET", &format!("/3d/generations/{OTHER}"), None))
            .await
            .unwrap();
        assert_eq!(response.status, 404);
        let response = service
            .clone()
            .relay(request(
                "POST",
                "/3d/generations",
                Some(json!({"model":"missing"})),
            ))
            .await;
        assert!(
            response
                .err()
                .unwrap()
                .downcast_ref::<super::super::ModelNotFound>()
                .is_some()
        );
        let mut config = service.state.config();
        config.ai.providers[0].enabled = false;
        service.state.set_config(config);
        assert!(
            service
                .clone()
                .relay(request("GET", &format!("/3d/generations/{ID}"), None))
                .await
                .is_err()
        );
        let calls = a.requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|(_, _, _, headers)| {
            headers
                .to_ascii_lowercase()
                .contains("authorization: bearer mock-key")
        }));
    }

    #[tokio::test]
    async fn list_unions_bound_routes_deduplicates_and_sorts() {
        let a = mock("a", |_, _, _| {
            (
                200,
                json!({"object":"list","data":[{"id":ID,"created":1,"owner":"a"}]}),
            )
        })
        .await;
        let b = mock("b", |_, _, _| {
            (
                200,
                json!({"data":[{"id":ID,"created":9,"owner":"b"},{"id":OTHER,"created":3}]}),
            )
        })
        .await;
        let service = service(vec![a.provider.clone(), b.provider.clone()]);
        service.relay_jobs.remember(
            &json_response(202, json!({"id":ID})),
            Route::Http {
                provider_id: "a".into(),
            },
        );
        service.relay_jobs.remember(
            &json_response(202, json!({"id":OTHER})),
            Route::Http {
                provider_id: "b".into(),
            },
        );
        let response = value(
            service
                .relay(request("GET", "/3d/generations", None))
                .await
                .unwrap(),
        );
        assert_eq!(response["data"].as_array().unwrap().len(), 2);
        assert_eq!(response["data"][0]["id"], OTHER);
        assert_eq!(response["data"][1]["owner"], "a");
        assert_eq!(a.requests.lock().unwrap().len(), 1);
        assert_eq!(b.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn voices_preserve_http_response_and_fall_back_to_alias() {
        let a = mock("a", |_, path, _| match path {
            "/v1/audio/voices" => (404, json!({"error":"missing"})),
            "/v1/voices" => (
                200,
                json!({"voices":[{"id":"custom","backends":["tts"]}],"count":1}),
            ),
            _ => panic!("unexpected path: {path}"),
        })
        .await;
        let service = service(vec![a.provider.clone()]);
        assert_eq!(
            service
                .clone()
                .relay(request("GET", "/audio/voices", None))
                .await
                .unwrap()
                .status,
            404
        );
        let mut config = service.state.config();
        config.ai.tts = Some(VoiceConfig {
            provider_id: "a".into(),
            model: "tts-model".into(),
            ..Default::default()
        });
        service.state.set_config(config);
        let result = service
            .relay(request("GET", "/audio/voices", None))
            .await
            .unwrap();
        assert_eq!(result.status, 200);
        assert_eq!(value(result)["voices"][0]["backends"], json!(["tts"]));
        assert_eq!(a.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn lifecycle_aggregates_health_and_loaded_models_per_origin() {
        let a = mock("a", |method, path, _| match (method, path) {
            ("GET", "/health") => (200, json!({"status":"ok"})),
            ("GET", "/api/ps") => (
                200,
                json!({"models":[{"name":"loaded","backend":"gpu"},{"model":"other"}]}),
            ),
            _ => panic!("unexpected path: {path}"),
        })
        .await;
        let b = mock("ext:app:b", |_, _, _| (404, json!({"error":"missing"}))).await;
        let mut disabled = a.provider.clone();
        disabled.id = "disabled".into();
        disabled.enabled = false;
        let room = AiProviderConfig {
            id: "room".into(),
            base_url: "mist-network://test".into(),
            ..Default::default()
        };
        let providers = vec![a.provider.clone(), b.provider.clone(), disabled, room];
        let health = lifecycle(&providers, &request("GET", "/health", None))
            .await
            .unwrap();
        assert_eq!(health.status, 200);
        assert_eq!(
            value(health)["backends"],
            json!({"a":"ok","ext:app:b":"unreachable"})
        );
        let ps = value(
            lifecycle(&providers, &request("GET", "/api/ps", None))
                .await
                .unwrap(),
        );
        assert_eq!(ps["models"][0]["backend"], "a/gpu");
        assert_eq!(ps["models"][1]["backend"], "a");
        assert_eq!(ps["models"].as_array().unwrap().len(), 2);
        assert!(
            a.requests
                .lock()
                .unwrap()
                .iter()
                .all(|(_, path, _, headers)| !path.starts_with("/v1")
                    && headers
                        .to_ascii_lowercase()
                        .contains("authorization: bearer mock-key"))
        );
    }

    #[tokio::test]
    async fn game_motion_and_unscoped_vision_use_room_oai_model_discovery() {
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD;

        const CLIPS: &str = "/motion/clips?model=motion%2Dmodel&tag=a+b";
        let cases = [
            (
                "game-peer",
                "POST",
                "/game/act",
                r#"{ "model":"game-model","image":"AA==" }"#,
            ),
            (
                "motion-peer",
                "POST",
                "/motion/inbetween",
                r#"{ "model":"motion-model","target":[1] }"#,
            ),
            ("motion-peer", "GET", "/motion/skeletons/motion%2Dmodel", ""),
            ("motion-peer", "GET", CLIPS, ""),
            (
                "game-peer",
                "POST",
                "/chat/completions",
                r#"{"messages":[{"content":[{"image_url":{"url":"data:image/png;base64,AA=="},"type":"image_url"}],"role":"user"}],"model":"game-model","stream":false,"temperature":0.4}"#,
            ),
        ];
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let room = room(Arc::new(move |peer, message| {
            tx.send((peer.to_string(), message)).unwrap();
        }));
        let service = service(vec![AiProviderConfig {
            id: "room".into(),
            base_url: "mist-network://test".into(),
            ..Default::default()
        }]);
        service
            .rooms
            .write()
            .unwrap()
            .insert("test".into(), room.clone());
        for (peer, service, model) in [
            ("chat-peer", protocol::SERVICE_CHAT, "game-model"),
            ("game-peer", protocol::SERVICE_OAI, "game-model"),
            ("motion-peer", protocol::SERVICE_OAI, "motion-model"),
        ] {
            assert!(room.consumer.handle_message(
                peer,
                &ProtocolMessage::ProviderHello {
                    models: Some(vec![model.into()]),
                    services: Some(vec![service.into()]),
                    voices: None,
                }
            ));
        }
        let oai = room.oai.clone();
        let responder = tokio::spawn(async move {
            for (expected_peer, expected_method, expected_path, expected_body) in cases {
                loop {
                    let (peer, message) = rx.recv().await.unwrap();
                    let ProtocolMessage::OaiRequest {
                        id,
                        seq,
                        last,
                        data,
                        method,
                        path,
                        ..
                    } = message
                    else {
                        assert!(matches!(message, ProtocolMessage::ConsumerHello));
                        continue;
                    };
                    assert_eq!(peer, expected_peer);
                    assert_eq!(seq, 0);
                    assert!(last);
                    assert_eq!(method.as_deref(), Some(expected_method));
                    assert_eq!(path.as_deref(), Some(expected_path));
                    assert_eq!(STANDARD.decode(data).unwrap(), expected_body.as_bytes());
                    oai.handle_message(
                        &peer,
                        &ProtocolMessage::OaiResponse {
                            id,
                            seq: 0,
                            last: true,
                            data: STANDARD.encode(b"frame,joint\r\n0,1\r\n"),
                            status: Some(202),
                            content_type: Some("text/csv".into()),
                        },
                    );
                    break;
                }
            }
        });
        for (_, method, path, body) in cases {
            let mut request = request(method, path, None);
            request.body = (!body.is_empty()).then(|| body.as_bytes().to_vec());
            let response = tokio::time::timeout(Duration::from_secs(5), async {
                if path == "/chat/completions" {
                    service.clone().relay(request).await
                } else {
                    room.clone().relay(request).await
                }
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(response.status, 202);
            assert_eq!(response.content_type, "text/csv");
            assert_eq!(response.body, b"frame,joint\r\n0,1\r\n");
        }
        assert!(room.relay_jobs.snapshot().is_empty());
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn image_edits_use_room_model_discovery_without_recording_jobs() {
        use base64::Engine as _;

        let _guard = crate::authz::TEST_LOCK.lock().await;
        let service = service(vec![AiProviderConfig {
            id: "room".into(),
            base_url: "mist-network://test".into(),
            models: vec!["image-model".into()],
            ..Default::default()
        }]);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let room = room(Arc::new(move |peer, message| {
            tx.send((peer.to_string(), message)).unwrap();
        }));
        service
            .rooms
            .write()
            .unwrap()
            .insert("test".into(), room.clone());
        for (peer, service_tag, model) in [
            ("chat-peer", protocol::SERVICE_CHAT, "image-model"),
            ("other-peer", protocol::SERVICE_OAI, "upscale-model"),
            ("image-peer", protocol::SERVICE_OAI, "image-model"),
        ] {
            assert!(room.consumer.handle_message(
                peer,
                &ProtocolMessage::ProviderHello {
                    models: Some(vec![model.into()]),
                    services: Some(vec![service_tag.into()]),
                    voices: None,
                }
            ));
        }
        // A job-shaped fixture catches accidental registration at either hop.
        let bytes =
            serde_json::to_vec_pretty(&json!({"id":ID,"data":[{"url":"unchanged"}]})).unwrap();
        let expected = bytes.clone();
        let oai = room.oai.clone();
        let task = tokio::spawn(async move {
            let mut requests = 0;
            while requests < 2 {
                let (peer, message) = rx.recv().await.unwrap();
                match message {
                    ProtocolMessage::ConsumerHello => {}
                    ProtocolMessage::OaiRequest {
                        id,
                        seq,
                        last,
                        data,
                        method,
                        path,
                        ..
                    } => {
                        assert_eq!(peer, "image-peer");
                        assert_eq!(seq, 0);
                        assert!(last);
                        assert_eq!(method.as_deref(), Some("POST"));
                        assert_eq!(path.as_deref(), Some("/images/edits"));
                        let body: Value = serde_json::from_slice(
                            &base64::engine::general_purpose::STANDARD
                                .decode(data)
                                .unwrap(),
                        )
                        .unwrap();
                        assert_eq!(body, json!({"model":"image-model","scale":2}));
                        oai.handle_message(
                            &peer,
                            &ProtocolMessage::OaiResponse {
                                id,
                                seq: 0,
                                last: true,
                                data: base64::engine::general_purpose::STANDARD.encode(&bytes),
                                status: Some(200),
                                content_type: Some("application/json".into()),
                            },
                        );
                        requests += 1;
                    }
                    _ => panic!("unexpected room message"),
                }
            }
        });
        let make_request = || {
            request(
                "POST",
                "/images/edits",
                Some(json!({"model":"image-model","scale":2})),
            )
        };
        for response in [
            tokio::time::timeout(Duration::from_secs(5), room.clone().relay(make_request()))
                .await
                .unwrap()
                .unwrap(),
            tokio::time::timeout(
                Duration::from_secs(5),
                service.clone().relay(make_request()),
            )
            .await
            .unwrap()
            .unwrap(),
        ] {
            assert_eq!(response.status, 200);
            assert_eq!(response.body, expected);
            assert!(room.relay_jobs.snapshot().is_empty());
            assert!(service.relay_jobs.snapshot().is_empty());
        }
        task.await.unwrap();
        for path in ["/images/edits", "/3d/generations"] {
            let result = room
                .clone()
                .relay(request("POST", path, Some(json!({"model":"missing"}))))
                .await;
            assert!(
                result
                    .err()
                    .unwrap()
                    .downcast_ref::<super::super::ModelNotFound>()
                    .is_some()
            );
        }
        for method in ["GET", "DELETE"] {
            assert_eq!(
                room.clone()
                    .relay(request(method, "/images/edits", None))
                    .await
                    .unwrap()
                    .status,
                404
            );
        }
        for body in [
            json!({}),
            json!({"model":""}),
            json!({"model":23}),
            json!([]),
        ] {
            assert_eq!(
                room.clone()
                    .relay(request("POST", "/images/edits", Some(body)))
                    .await
                    .unwrap()
                    .status,
                400
            );
        }
    }

    #[tokio::test]
    async fn room_voices_use_accepted_advertisements_and_remove_departed_peers() {
        let _guard = crate::authz::TEST_LOCK.lock().await;
        let service = service(vec![]);
        let send: super::super::SendFn = Arc::new(|_, _| {});
        let room = room(send);
        service
            .rooms
            .write()
            .unwrap()
            .insert("test".into(), room.clone());
        let rt = tokio::runtime::Handle::current();
        for (peer, service_tag, voices) in [
            ("a", "tts", vec!["z", "a"]),
            ("b", "tts", vec!["a", "b"]),
            ("c", "chat", vec!["excluded"]),
        ] {
            let hello = ProtocolMessage::ProviderHello {
                models: Some(vec!["tts-model".into()]),
                services: Some(vec![service_tag.into()]),
                voices: Some(voices.into_iter().map(String::from).collect()),
            };
            service.handle_room_event(
                crate::net::EVENT_RAW,
                "test",
                peer,
                &protocol::encode(&hello),
                &rt,
            );
        }
        let voices = value(
            room.clone()
                .relay(request("GET", "/audio/voices", None))
                .await
                .unwrap(),
        );
        assert_eq!(voices["count"], 3);
        assert_eq!(voices["data"], voices["voices"]);
        assert_eq!(
            voices["data"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["a", "b", "z"]
        );
        service.handle_room_event(crate::net::EVENT_PEER_DISCONNECTED, "test", "a", &[], &rt);
        let voices = value(
            room.clone()
                .relay(request("GET", "/audio/voices", None))
                .await
                .unwrap(),
        );
        assert_eq!(voices["count"], 2);
        let withdrawn = ProtocolMessage::ProviderHello {
            models: Some(vec![]),
            services: Some(vec![]),
            voices: None,
        };
        service.handle_room_event(
            crate::net::EVENT_RAW,
            "test",
            "b",
            &protocol::encode(&withdrawn),
            &rt,
        );
        assert!(room.relay_voices.read().unwrap().is_empty());
        assert_eq!(
            room.clone()
                .relay(request("GET", "/health", None))
                .await
                .unwrap()
                .status,
            404
        );
        assert_eq!(
            room.clone()
                .relay(request("GET", &format!("/3d/generations/{ID}"), None))
                .await
                .unwrap()
                .status,
            404
        );
        assert_eq!(
            value(
                room.relay(request("GET", "/3d/generations", None))
                    .await
                    .unwrap()
            )["data"],
            json!([])
        );
    }

    #[tokio::test]
    async fn unscoped_jobs_find_room_creations_and_respect_room_enablement() {
        use base64::Engine as _;

        let _guard = crate::authz::TEST_LOCK.lock().await;
        let service = service(vec![AiProviderConfig {
            id: "room".into(),
            base_url: "mist-network://test".into(),
            ..Default::default()
        }]);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let send: super::super::SendFn = Arc::new(move |peer, message| {
            tx.send((peer.to_string(), message)).unwrap();
        });
        let room = room(send);
        service
            .rooms
            .write()
            .unwrap()
            .insert("test".into(), room.clone());
        assert!(room.consumer.handle_message(
            "provider",
            &ProtocolMessage::ProviderHello {
                models: Some(vec!["3d-model".into()]),
                services: Some(vec![protocol::SERVICE_OAI.into()]),
                voices: None,
            },
        ));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let oai = room.oai.clone();
        let task = tokio::spawn(async move {
            while let Some((peer, message)) = rx.recv().await {
                let ProtocolMessage::OaiRequest {
                    id,
                    method,
                    path,
                    last,
                    ..
                } = message
                else {
                    continue;
                };
                assert_eq!(peer, "provider");
                assert!(last);
                let method = method.unwrap();
                let path = path.unwrap();
                let body = if method == "GET" && path == "/3d/generations" {
                    json!({"data":[{"id":ID,"status":"completed"}]})
                } else {
                    json!({"id":ID,"status":"completed"})
                };
                let status = if method == "POST" { 202 } else { 200 };
                recorded.lock().unwrap().push((method, path));
                oai.handle_message(
                    &peer,
                    &ProtocolMessage::OaiResponse {
                        id,
                        seq: 0,
                        last: true,
                        data: base64::engine::general_purpose::STANDARD
                            .encode(serde_json::to_vec(&body).unwrap()),
                        status: Some(status),
                        content_type: Some("application/json".into()),
                    },
                );
            }
        });
        let routes = service
            .clone()
            .api_room("test".into())
            .await
            .unwrap()
            .unwrap();
        let response = (routes.relay)(request(
            "POST",
            "/3d/generations",
            Some(json!({"model":"3d-model","image":"a"})),
        ))
        .await
        .unwrap();
        assert_eq!(response.status, 202);
        assert!(service.relay_jobs.snapshot().is_empty());
        assert_eq!(room.relay_jobs.snapshot().get(ID).unwrap(), "provider");

        for (method, suffix) in [("GET", ""), ("GET", "/content?range=0-7"), ("DELETE", "")] {
            let path = format!("/3d/generations/{ID}{suffix}");
            let response = service
                .clone()
                .relay(request(method, &path, None))
                .await
                .unwrap();
            assert_eq!(response.status, 200);
            assert_eq!(value(response)["id"], ID);
            assert_eq!(
                requests.lock().unwrap().last().unwrap(),
                &(method.into(), path)
            );
        }
        for _ in 0..2 {
            let response = service
                .clone()
                .relay(request("GET", "/3d/generations", None))
                .await
                .unwrap();
            assert_eq!(value(response)["data"][0]["id"], ID);
            // A route known at both levels must still be queried only once.
            service.relay_jobs.remember(
                &json_response(202, json!({"id":ID})),
                Route::Room {
                    room: "test".into(),
                },
            );
        }
        assert_eq!(requests.lock().unwrap().len(), 6);
        for (method, suffix) in [("GET", ""), ("GET", "/content"), ("DELETE", "")] {
            let response = service
                .clone()
                .relay(request(
                    method,
                    &format!("/3d/generations/{OTHER}{suffix}"),
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(response.status, 404);
        }

        // Disable the API provider while the live room still remembers the job.
        service.relay_jobs.0.lock().unwrap().clear();
        let mut config = service.state.config();
        config.ai.providers[0].enabled = false;
        service.state.set_config(config);
        assert!(
            service
                .clone()
                .relay(request("GET", &format!("/3d/generations/{ID}"), None))
                .await
                .is_err()
        );
        let response = service
            .relay(request("GET", "/3d/generations", None))
            .await
            .unwrap();
        assert_eq!(value(response)["data"], json!([]));
        assert_eq!(requests.lock().unwrap().len(), 6);
        task.abort();
    }

    #[test]
    fn jobs_are_bounded_expire_and_reject_invalid_ids_and_failed_responses() {
        let jobs = Jobs::default();
        for n in 0..=JOB_LIMIT {
            jobs.remember(
                &json_response(202, json!({"id":format!("gen3d_{n:032x}")})),
                n,
            );
        }
        assert_eq!(jobs.snapshot().len(), JOB_LIMIT);
        assert!(
            !jobs
                .snapshot()
                .contains_key("gen3d_00000000000000000000000000000000")
        );
        jobs.remember(&json_response(202, json!({"id":"invalid"})), 0);
        jobs.remember(&json_response(500, json!({"id":ID})), 0);
        assert_eq!(jobs.snapshot().len(), JOB_LIMIT);
        assert!(jobs.snapshot_at(Instant::now() + JOB_TTL).is_empty());
    }

    #[tokio::test]
    async fn invalid_paths_bodies_and_unresolved_default_return_json_errors() {
        let service = service(vec![]);
        for (method, path) in [
            ("GET", "/3d/generations/gen3d_bad"),
            ("POST", "/api/ps"),
            ("DELETE", &format!("/3d/generations/{ID}/content")),
            ("GET", "/unexpected"),
            ("GET", "/images/edits"),
            ("DELETE", "/images/edits"),
        ] {
            let response = service
                .clone()
                .relay(request(method, path, None))
                .await
                .unwrap();
            assert_eq!(response.status, 404);
            assert_eq!(response.content_type, "application/json");
        }
        for path in ["/3d/generations", "/images/edits"] {
            for body in [
                json!([]),
                json!(null),
                json!({"model":23}),
                json!({}),
                json!({"model":""}),
            ] {
                assert_eq!(
                    service
                        .clone()
                        .relay(request("POST", path, Some(body)))
                        .await
                        .unwrap()
                        .status,
                    400
                );
            }
            let result = service
                .clone()
                .relay(request("POST", path, Some(json!({"model":"missing"}))))
                .await;
            assert!(
                result
                    .err()
                    .unwrap()
                    .downcast_ref::<super::super::ModelNotFound>()
                    .is_some()
            );
        }
        assert_eq!(
            service
                .clone()
                .relay(request("GET", &format!("/3d/generations/{ID}"), None))
                .await
                .unwrap()
                .status,
            404
        );
        assert_eq!(
            value(
                service
                    .relay(request("GET", "/3d/generations", None))
                    .await
                    .unwrap()
            )["data"],
            json!([])
        );
    }
}
