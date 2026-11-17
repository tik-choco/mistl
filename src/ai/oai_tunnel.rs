//! mistai oai_* v1: base64 the entire body, then split the encoded string.
//! Metadata lives on seq 0. Reassembly and replies are bound to the sender.
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine;
use serde_json::Value;
use tokio::sync::mpsc;

use super::protocol::{self, ProtocolMessage};
use super::{SendFn, provider::JobLimiter};
use crate::authz::{Action, Authorized, Denied, Service};
use crate::config::ResolvedModel;

pub(super) const CHUNK_SIZE: usize = 12 * 1024;
pub(super) const MAX_BASE64_CHARS: usize = 24 * 1024 * 1024;
/// Decoded body cap for one tunnel request or response (18 MiB).
pub(super) const MAX_BYTES: usize = MAX_BASE64_CHARS / 4 * 3;
/// tc-ai-api passthrough: deadline for a synchronous (`background` not
/// `true`) `POST /3d/generations`, above the upstream's 30-minute JOB_TIMEOUT.
pub(super) const GEN3D_SYNC_TIMEOUT: Duration = Duration::from_secs(35 * 60);
/// Deadline for one `/3d/generations/<id>/content` range window.
pub(super) const CONTENT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Size of one content range window fetched through a relay.
pub(super) const CONTENT_WINDOW: u64 = 8 * 1024 * 1024;
const MAX_BUFFERS: usize = 32;
const MAX_PER_PEER: usize = 4;
const MAX_TOTAL_CHARS: usize = 48 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(120);
const RESPONSE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SEQ: u64 = (MAX_BASE64_CHARS / CHUNK_SIZE) as u64;
const MAX_JOB_BINDINGS: usize = 1024;
const MAX_JOBS_PER_PEER: usize = 64;
const JOB_TTL: Duration = Duration::from_secs(25 * 60 * 60);

/// Rewrites top-level members of a JSON object body without re-serializing
/// the rest: `set` replaces a member in place (or appends it), `remove`
/// drops it. Nested values are copied verbatim, so key order survives --
/// decision upstreams (`/systemone`) present `choice` criteria in the order
/// the client wrote them, while a `Value` round-trip would sort the keys.
pub(super) fn rewrite_object(
    bytes: &[u8],
    set: &[(&str, Value)],
    remove: &[&str],
) -> Result<Vec<u8>> {
    use serde::de::{Deserializer, MapAccess, Visitor};
    use serde_json::value::RawValue;

    struct Members(Vec<(String, Box<RawValue>)>);
    impl<'de> serde::Deserialize<'de> for Members {
        fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Members;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<Members, A::Error> {
                    let mut out = Vec::new();
                    while let Some(entry) = map.next_entry::<String, Box<RawValue>>()? {
                        out.push(entry);
                    }
                    Ok(Members(out))
                }
            }
            d.deserialize_map(V)
        }
    }

    let Members(members) = serde_json::from_slice(bytes).context("request_rejected")?;
    let mut out = String::from("{");
    let mut written: Vec<&str> = Vec::new();
    let push = |out: &mut String, key: &str, raw: &str| {
        if out.len() > 1 {
            out.push(',');
        }
        out.push_str(&Value::String(key.to_string()).to_string());
        out.push(':');
        out.push_str(raw);
    };
    for (key, raw) in &members {
        if remove.contains(&key.as_str()) || written.contains(&key.as_str()) {
            continue;
        }
        match set.iter().find(|(k, _)| k == key) {
            Some((k, value)) => {
                push(&mut out, key, &value.to_string());
                written.push(k);
            }
            None => push(&mut out, key, raw.get()),
        }
    }
    for (key, value) in set {
        if !written.contains(key) {
            push(&mut out, key, &value.to_string());
        }
    }
    out.push('}');
    Ok(out.into_bytes())
}

pub(super) fn chunks(bytes: &[u8]) -> Vec<String> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    if encoded.is_empty() {
        return vec![String::new()];
    }
    encoded
        .as_bytes()
        .chunks(CHUNK_SIZE)
        .map(|s| String::from_utf8(s.to_vec()).expect("base64 ASCII"))
        .collect()
}

#[derive(Default)]
struct Assembly {
    authorized: Option<Authorized>,
    parts: BTreeMap<u64, String>,
    last: Option<u64>,
    chars: usize,
    path: String,
    method: String,
    content_type: String,
}

impl Assembly {
    fn push(&mut self, seq: u64, last: bool, data: String) -> Result<bool> {
        if seq > MAX_SEQ || data.len() > CHUNK_SIZE || !data.is_ascii() {
            bail!("request_too_large");
        }
        if self.parts.contains_key(&seq) {
            return Ok(false);
        }
        if self.chars.saturating_add(data.len()) > MAX_BASE64_CHARS {
            bail!("request_too_large");
        }
        if self
            .last
            .is_some_and(|end| seq > end || (last && seq != end))
        {
            bail!("request_rejected");
        }
        if last {
            if self.parts.keys().any(|n| *n > seq) {
                bail!("request_rejected");
            }
            self.last = Some(seq);
        }
        self.chars += data.len();
        self.parts.insert(seq, data);
        Ok(self.last.is_some_and(|n| self.parts.len() as u64 == n + 1))
    }

    fn decode(self) -> Result<Vec<u8>> {
        let encoded: String = self.parts.into_values().collect();
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .context("Failed to decode request body.")
    }
}

type Resolver = Arc<dyn Fn(&str, &Value) -> Result<Option<ResolvedModel>> + Send + Sync>;

struct JobBinding {
    peer: String,
    target: ResolvedModel,
    at: Instant,
}

#[derive(Default)]
struct JobBindings(HashMap<String, JobBinding>);

impl JobBindings {
    fn prune(&mut self) {
        self.prune_at(Instant::now());
    }

    fn prune_at(&mut self, now: Instant) {
        self.0
            .retain(|_, binding| now.duration_since(binding.at) < JOB_TTL);
    }

    fn bind(&mut self, id: String, peer: &str, target: ResolvedModel) {
        self.prune();
        // A colliding upstream id must never transfer ownership to another peer.
        if self.0.get(&id).is_some_and(|b| b.peer != peer) {
            return;
        }
        self.0.remove(&id);
        if self.0.values().filter(|b| b.peer == peer).count() >= MAX_JOBS_PER_PEER {
            self.evict_oldest(Some(peer));
        }
        if self.0.len() >= MAX_JOB_BINDINGS {
            self.evict_oldest(None);
        }
        self.0.insert(
            id,
            JobBinding {
                peer: peer.to_string(),
                target,
                at: Instant::now(),
            },
        );
    }

    fn evict_oldest(&mut self, peer: Option<&str>) {
        let oldest = self
            .0
            .iter()
            .filter(|(_, b)| peer.is_none_or(|peer| b.peer == peer))
            .min_by_key(|(_, b)| b.at)
            .map(|(id, _)| id.clone());
        if let Some(id) = oldest {
            self.0.remove(&id);
        }
    }

    fn target(&mut self, id: &str, peer: &str) -> Option<ResolvedModel> {
        self.prune();
        self.0
            .get(id)
            .filter(|b| b.peer == peer)
            .map(|b| b.target.clone())
    }

    fn remove(&mut self, id: &str, peer: &str) {
        self.prune();
        if self.0.get(id).is_some_and(|b| b.peer == peer) {
            self.0.remove(id);
        }
    }
}

pub(super) struct TunnelProvider {
    send: SendFn,
    resolve: Resolver,
    pending: Mutex<HashMap<(String, String), (Instant, Assembly)>>,
    jobs: JobLimiter,
    bindings: Mutex<JobBindings>,
}

impl TunnelProvider {
    pub fn new(send: SendFn, resolve: Resolver) -> Arc<Self> {
        Arc::new(Self {
            send,
            resolve,
            pending: Mutex::new(HashMap::new()),
            jobs: JobLimiter::new(8),
            bindings: Mutex::new(JobBindings::default()),
        })
    }

    pub fn drop_peer(&self, from: &str) {
        self.pending
            .lock()
            .expect("oai pending lock")
            .retain(|(peer, _), _| peer != from);
    }

    fn error(&self, from: &str, id: String, message: &str, code: Option<&str>) {
        (self.send)(
            from,
            ProtocolMessage::OaiError {
                id,
                message: message.into(),
                code: code.map(String::from),
            },
        );
    }

    #[cfg(test)]
    pub async fn handle_message(&self, from: &str, msg: ProtocolMessage) {
        let _ = self.handle_message_in_room(None, from, msg).await;
    }

    pub async fn handle_message_in_room(
        &self,
        room: Option<&str>,
        from: &str,
        msg: ProtocolMessage,
    ) -> std::result::Result<(), Denied> {
        let ProtocolMessage::OaiRequest {
            id,
            seq,
            last,
            data,
            path,
            method,
            content_type,
        } = msg
        else {
            return Ok(());
        };
        if id.len() > 256
            || path.as_ref().is_some_and(|p| p.len() > 256)
            || method.as_ref().is_some_and(|m| m.len() > 16)
            || content_type.as_ref().is_some_and(|c| c.len() > 256)
        {
            self.error(
                from,
                id,
                "Request exceeded the maximum allowed size.",
                Some("request_too_large"),
            );
            return Ok(());
        }
        let ready = {
            let mut pending = self.pending.lock().expect("oai pending lock");
            let now = Instant::now();
            pending.retain(|_, (at, _)| now.duration_since(*at) < TIMEOUT);
            let key = (from.to_string(), id.clone());
            let authorized = if pending.contains_key(&key) {
                None
            } else {
                match crate::authz::authorize_p2p(Service::Ai, Action::Infer, room, from, None) {
                    Ok(authorized) => Some(authorized),
                    Err(denied) => {
                        self.error(from, id, &denied.message, Some(denied.code));
                        return Err(denied);
                    }
                }
            };
            let total: usize = pending.values().map(|(_, a)| a.chars).sum();
            if (!pending.contains_key(&key)
                && (pending.len() >= MAX_BUFFERS
                    || pending.keys().filter(|(peer, _)| peer == from).count() >= MAX_PER_PEER))
                || total.saturating_add(data.len()) > MAX_TOTAL_CHARS
            {
                pending.remove(&key);
                self.error(
                    from,
                    id,
                    "Too many pending OAI requests.",
                    Some("request_rejected"),
                );
                return Ok(());
            }
            let (at, assembly) = pending.entry(key.clone()).or_insert_with(|| {
                (
                    now,
                    Assembly {
                        authorized,
                        ..Assembly::default()
                    },
                )
            });
            *at = now;
            // Later chunks never override seq 0 metadata, including duplicates.
            if seq == 0 && !assembly.parts.contains_key(&0) {
                assembly.path = path.unwrap_or_default();
                assembly.method = method.unwrap_or_else(|| "POST".into());
                assembly.content_type = content_type.unwrap_or_else(|| "application/json".into());
            }
            match assembly.push(seq, last, data) {
                Ok(false) => return Ok(()),
                Ok(true) => pending.remove(&key).expect("complete assembly").1,
                Err(err) => {
                    pending.remove(&key);
                    let code = err.to_string();
                    self.error(from, id, "Invalid or oversized OAI request.", Some(&code));
                    return Ok(());
                }
            }
        };
        let Some(_job) = self.jobs.try_acquire(from, 2) else {
            self.error(
                from,
                id,
                "Too many concurrent OAI requests.",
                Some("request_rejected"),
            );
            return Ok(());
        };
        let authorized = ready.authorized.clone().expect("authorized OAI assembly");
        let path = ready.path.clone();
        let method = ready.method.clone();
        let content_type = ready.content_type.clone();
        let result = match ready.decode() {
            Ok(bytes) => {
                let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                let deadline = relay_deadline(&method, &path, &body);
                tokio::time::timeout(
                    deadline,
                    self.dispatch(from, &method, &path, &content_type, &bytes),
                )
                .await
            }
            Err(err) => Ok(Err(err)),
        };
        authorized.record(1, 0);
        match result {
            Ok(Ok(response)) => {
                let parts = chunks(&response.body);
                let count = parts.len();
                for (seq, data) in parts.into_iter().enumerate() {
                    (self.send)(
                        from,
                        ProtocolMessage::OaiResponse {
                            id: id.clone(),
                            seq: seq as u64,
                            last: seq + 1 == count,
                            data,
                            status: (seq == 0).then_some(response.status),
                            content_type: (seq == 0).then(|| response.content_type.clone()),
                        },
                    );
                    tokio::task::yield_now().await;
                }
            }
            Ok(Err(err)) => {
                let code = err.to_string();
                let known = matches!(
                    code.as_str(),
                    "unsupported_path"
                        | "model_not_shared"
                        | "request_rejected"
                        | "request_too_large"
                );
                tracing::debug!(%err, "ai: OAI upstream failed");
                self.error(
                    from,
                    id,
                    if known {
                        &code
                    } else {
                        "OAI upstream request failed."
                    },
                    known.then_some(code.as_str()),
                );
            }
            Err(_) => self.error(from, id, "OAI upstream request timed out.", None),
        }
        Ok(())
    }

    async fn dispatch(
        &self,
        from: &str,
        method: &str,
        path: &str,
        content_type: &str,
        bytes: &[u8],
    ) -> Result<TunnelResponse> {
        let (path, range) = split_range(path)?;
        let job_path = generation_path(&path);
        if !matches!(
            path.as_str(),
            "/chat/completions" | "/models" | "/embeddings" | "/systemone" | "/3d/generations"
        ) && job_path.is_none()
        {
            if path.starts_with("/3d/generations/") {
                return Ok(generation_not_found());
            }
            bail!("unsupported_path");
        }
        let allowed = match (path.as_str(), job_path) {
            ("/models", _) => method == "GET",
            ("/3d/generations", _) => matches!(method, "GET" | "POST"),
            (_, Some((_, true))) => method == "GET",
            (_, Some((_, false))) => matches!(method, "GET" | "DELETE"),
            _ => method == "POST",
        };
        if !allowed {
            bail!("request_rejected");
        }
        if let Some((id, content)) = job_path {
            let target = self
                .bindings
                .lock()
                .expect("oai bindings lock")
                .target(id, from);
            let Some(target) = target else {
                return Ok(generation_not_found());
            };
            let mut response = forward_with(
                &target.base_url,
                &target.api_key,
                method,
                &path,
                None,
                range,
                relay_deadline(method, &path, &Value::Null),
            )
            .await?;
            if response.status == 404
                || (method == "DELETE" && (200..300).contains(&response.status))
            {
                self.bindings
                    .lock()
                    .expect("oai bindings lock")
                    .remove(id, from);
            }
            if !content {
                relativize_response(&mut response)?;
            }
            return Ok(response);
        }
        if path == "/3d/generations" && method == "GET" {
            return self.list_generations(from).await;
        }
        let body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            if !content_type.to_ascii_lowercase().contains("json") {
                bail!("request_rejected");
            }
            serde_json::from_slice(bytes).map_err(|_| anyhow::anyhow!("request_rejected"))?
        };
        let generation = path == "/3d/generations";
        if generation {
            let object = body.as_object().context("request_rejected")?;
            // No built-in default: routing needs an explicit model id.
            if object
                .get("model")
                .and_then(Value::as_str)
                .is_none_or(|m| m.trim().is_empty())
            {
                return Ok(missing_model());
            }
        }
        let target = (self.resolve)(&path, &body)?.context("unsupported_path")?;
        let payload = if method == "POST" {
            let mut set = vec![("model", Value::String(target.model.clone()))];
            if path == "/chat/completions" {
                set.push(("stream", Value::Bool(false)));
            }
            Some(rewrite_object(
                bytes,
                &set,
                if generation { &[] } else { &["temperature"] },
            )?)
        } else {
            None
        };
        if !generation {
            return forward(&target.base_url, &target.api_key, method, &path, payload).await;
        }
        let mut response = forward_with(
            &target.base_url,
            &target.api_key,
            method,
            &path,
            payload,
            None,
            relay_deadline(method, &path, &body),
        )
        .await?;
        if matches!(response.status, 200 | 202)
            && let Ok(job) = serde_json::from_slice::<Value>(&response.body)
            && let Some(id) = job
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| valid_job_id(id))
        {
            self.bindings
                .lock()
                .expect("oai bindings lock")
                .bind(id.to_string(), from, target);
        }
        relativize_response(&mut response)?;
        Ok(response)
    }

    async fn list_generations(&self, from: &str) -> Result<TunnelResponse> {
        let mut targets = Vec::new();
        {
            let mut bindings = self.bindings.lock().expect("oai bindings lock");
            bindings.prune();
            for binding in bindings.0.values().filter(|b| b.peer == from) {
                if !targets
                    .iter()
                    .any(|t: &ResolvedModel| same_target(t, &binding.target))
                {
                    targets.push(binding.target.clone());
                }
            }
        }
        let mut jobs = Vec::new();
        let mut size = 0usize;
        for target in targets {
            let Ok(response) = forward(
                &target.base_url,
                &target.api_key,
                "GET",
                "/3d/generations",
                None,
            )
            .await
            else {
                continue;
            };
            if !(200..300).contains(&response.status) {
                continue;
            }
            let Ok(mut body) = serde_json::from_slice::<Value>(&response.body) else {
                continue;
            };
            relativize_job_urls(&mut body);
            let Some(data) = body.get_mut("data").and_then(Value::as_array_mut) else {
                continue;
            };
            let mut bindings = self.bindings.lock().expect("oai bindings lock");
            bindings.prune();
            for job in data.drain(..) {
                let Some(id) = job.get("id").and_then(Value::as_str) else {
                    continue;
                };
                if !bindings
                    .0
                    .get(id)
                    .is_some_and(|b| b.peer == from && same_target(&b.target, &target))
                    || jobs.iter().any(|j: &Value| j["id"].as_str() == Some(id))
                {
                    continue;
                }
                size = size.saturating_add(serde_json::to_vec(&job)?.len());
                if size > MAX_BYTES {
                    bail!("request_too_large");
                }
                jobs.push(job);
            }
        }
        jobs.sort_by(|a, b| {
            b["created"]
                .as_u64()
                .unwrap_or(0)
                .cmp(&a["created"].as_u64().unwrap_or(0))
        });
        json_response(200, serde_json::json!({"object":"list", "data":jobs}))
    }
}

fn same_target(a: &ResolvedModel, b: &ResolvedModel) -> bool {
    a.base_url == b.base_url && a.api_key == b.api_key
}

fn valid_job_id(id: &str) -> bool {
    id.strip_prefix("gen3d_").is_some_and(|hex| {
        hex.len() == 32
            && hex
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    })
}

fn generation_path(path: &str) -> Option<(&str, bool)> {
    let suffix = path.strip_prefix("/3d/generations/")?;
    let (id, content) = match suffix.strip_suffix("/content") {
        Some(id) => (id, true),
        None => (suffix, false),
    };
    valid_job_id(id).then_some((id, content))
}

fn json_response(status: u16, body: Value) -> Result<TunnelResponse> {
    let body = serde_json::to_vec(&body)?;
    if body.len() > MAX_BYTES {
        bail!("request_too_large");
    }
    Ok(TunnelResponse {
        status,
        content_type: "application/json".into(),
        body,
    })
}

/// 400 for a `POST /3d/generations` without a model id.
pub(super) fn missing_model() -> TunnelResponse {
    json_response(
        400,
        serde_json::json!({"error": {
            "message":"`model` is required", "type":"invalid_request_error",
            "code":"missing_required_parameter", "param":"model"
        }}),
    )
    .expect("small error response")
}

fn generation_not_found() -> TunnelResponse {
    json_response(
        404,
        serde_json::json!({"error": {
            "message":"generation not found", "type":"invalid_request_error",
            "code":"not_found", "param":"id"
        }}),
    )
    .expect("small error response")
}

fn relativize_response(response: &mut TunnelResponse) -> Result<()> {
    if let Ok(mut body) = serde_json::from_slice::<Value>(&response.body) {
        relativize_job_urls(&mut body);
        let bytes = serde_json::to_vec(&body)?;
        if bytes.len() > MAX_BYTES {
            bail!("request_too_large");
        }
        response.body = bytes;
    }
    Ok(())
}

/// Sends one buffered request to an OpenAI-compatible upstream and reads the
/// reply (status, content type, body) under the tunnel's size cap.
pub(super) async fn forward(
    base_url: &str,
    api_key: &str,
    method: &str,
    path: &str,
    payload: Option<Vec<u8>>,
) -> Result<TunnelResponse> {
    forward_with(base_url, api_key, method, path, payload, None, TIMEOUT).await
}

/// [`forward`] with an explicit `Range: bytes=start-end` header and deadline.
/// `path` has no query string.
pub(super) async fn forward_with(
    base_url: &str,
    api_key: &str,
    method: &str,
    path: &str,
    payload: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
    timeout: Duration,
) -> Result<TunnelResponse> {
    // `no_proxy()`: upstreams are usually loopback (e.g. Ollama), and a
    // proxy from the environment would break those requests.
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()?;
    let url = format!("{}{path}", base_url.trim_end_matches('/'));
    let mut request = client.request(reqwest::Method::from_bytes(method.as_bytes())?, url);
    if !api_key.trim().is_empty() {
        request = request.bearer_auth(api_key);
    }
    if let Some((start, end)) = range {
        request = request.header(reqwest::header::RANGE, format!("bytes={start}-{end}"));
    }
    // Request bodies are bounded by their source: tunnel reassembly caps
    // peer bodies at MAX_BYTES, the local API caps its own (3D JSON can
    // legitimately exceed MAX_BYTES on an HTTP-direct route).
    if let Some(payload) = payload.filter(|_| method == "POST") {
        request = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload);
    }
    let mut response = request.send().await?;
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    if response
        .content_length()
        .is_some_and(|n| n > MAX_BYTES as u64)
    {
        bail!("request_too_large");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > MAX_BYTES {
            bail!("request_too_large");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(TunnelResponse {
        status,
        content_type,
        body,
    })
}

/// Deadline for one tc-ai-api passthrough request. `path` may carry the
/// `?range=` query; `body` is the parsed JSON body (`Value::Null` if none).
pub(super) fn relay_deadline(method: &str, path: &str, body: &Value) -> Duration {
    let path = path.split('?').next().unwrap_or(path);
    if method == "POST" && path == "/3d/generations" && body["background"] != true {
        GEN3D_SYNC_TIMEOUT
    } else if generation_path(path).is_some_and(|(_, content)| content) {
        CONTENT_TIMEOUT
    } else {
        TIMEOUT
    }
}

/// Splits `/3d/generations/<id>/content?range=<start>-<end>` into the bare
/// path and the inclusive byte range. Any other query is rejected.
pub(super) fn split_range(path: &str) -> Result<(String, Option<(u64, u64)>)> {
    let Some((path, query)) = path.split_once('?') else {
        return Ok((path.to_string(), None));
    };
    if !generation_path(path).is_some_and(|(_, content)| content) {
        bail!("request_rejected");
    }
    let (start, end) = query
        .strip_prefix("range=")
        .and_then(|range| range.split_once('-'))
        .context("request_rejected")?;
    let decimal = |s: &str| -> Result<u64> {
        if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
            bail!("request_rejected");
        }
        s.parse().context("request_rejected")
    };
    let start = decimal(start)?;
    let end = decimal(end)?;
    if end < start || end - start >= CONTENT_WINDOW {
        bail!("request_rejected");
    }
    Ok((path.to_string(), Some((start, end))))
}

/// Rewrites every 3D job's `data[].url` (in a job, a sync response or a
/// `{"object":"list","data":[job...]}` list) to the host-free relative form
/// `/3d/generations/<id>/content`, so no upstream address leaks.
pub(super) fn relativize_job_urls(body: &mut Value) {
    if body["object"] == "list" {
        if let Some(jobs) = body.get_mut("data").and_then(Value::as_array_mut) {
            for job in jobs {
                relativize_job_urls(job);
            }
        }
        return;
    }
    let Some(id) = body
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_job_id(id))
    else {
        return;
    };
    let url = format!("/3d/generations/{id}/content");
    if let Some(data) = body.get_mut("data").and_then(Value::as_array_mut) {
        for entry in data {
            if let Some(object) = entry.as_object_mut()
                && let Some(value) = object.get_mut("url")
            {
                *value = Value::String(url.clone());
            }
        }
    }
}

pub(super) struct TunnelResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

pub(super) struct TunnelConsumer {
    send: SendFn,
    pending: Mutex<HashMap<String, (String, mpsc::Sender<ProtocolMessage>)>>,
}

struct PendingGuard<'a> {
    consumer: &'a TunnelConsumer,
    id: String,
}
impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.consumer
            .pending
            .lock()
            .expect("oai consumer lock")
            .remove(&self.id);
    }
}

impl TunnelConsumer {
    pub fn new(send: SendFn) -> Arc<Self> {
        Arc::new(Self {
            send,
            pending: Mutex::new(HashMap::new()),
        })
    }
    pub fn reject_all(&self) {
        self.pending.lock().expect("oai consumer lock").clear();
    }
    pub fn drop_peer(&self, from: &str) {
        self.pending
            .lock()
            .expect("oai consumer lock")
            .retain(|_, (peer, _)| peer != from);
    }
    pub fn handle_message(&self, from: &str, msg: &ProtocolMessage) {
        let (id, valid) = match msg {
            ProtocolMessage::OaiResponse {
                id,
                data,
                seq,
                content_type,
                ..
            } => (
                id,
                data.len() <= CHUNK_SIZE
                    && *seq <= MAX_SEQ
                    && content_type.as_ref().is_none_or(|s| s.len() <= 256),
            ),
            ProtocolMessage::OaiError { id, message, code } => (
                id,
                message.len() <= 4096 && code.as_ref().is_none_or(|s| s.len() <= 256),
            ),
            _ => return,
        };
        let mut pending = self.pending.lock().expect("oai consumer lock");
        if let Some((peer, tx)) = pending.get(id) {
            if peer != from {
                return;
            }
            if !valid || tx.try_send(msg.clone()).is_err() {
                pending.remove(id);
            }
        }
    }
    pub async fn request(
        &self,
        peer: &str,
        path: &str,
        body: &[u8],
        timeout: Duration,
    ) -> Result<TunnelResponse> {
        let method = if path == "/models" { "GET" } else { "POST" };
        self.request_method(peer, method, path, body, timeout.min(TIMEOUT))
            .await
    }

    /// Like [`Self::request`] with an explicit method (`GET`/`POST`/`DELETE`)
    /// for the tc-ai-api passthrough paths; GET/DELETE send an empty body.
    pub async fn request_method(
        &self,
        peer: &str,
        method: &str,
        path: &str,
        body: &[u8],
        timeout: Duration,
    ) -> Result<TunnelResponse> {
        if !matches!(method, "GET" | "POST" | "DELETE") {
            bail!("request_rejected");
        }
        let body = if method == "POST" { body } else { &[] };
        if body.len() > MAX_BYTES {
            bail!("OAI request too large");
        }
        let id = protocol::random_id();
        let mut rx = {
            let mut pending = self.pending.lock().expect("oai consumer lock");
            if pending.len() >= 8 {
                bail!("Too many pending OAI requests");
            }
            // Fit every sequence plus an error without requiring the receiver to poll.
            let (tx, rx) = mpsc::channel(MAX_SEQ as usize + 2);
            pending.insert(id.clone(), (peer.to_string(), tx));
            rx
        };
        let _guard = PendingGuard {
            consumer: self,
            id: id.clone(),
        };
        let parts = chunks(body);
        let count = parts.len();
        for (seq, data) in parts.into_iter().enumerate() {
            (self.send)(
                peer,
                ProtocolMessage::OaiRequest {
                    id: id.clone(),
                    seq: seq as u64,
                    last: seq + 1 == count,
                    data,
                    path: (seq == 0).then(|| path.to_string()),
                    method: (seq == 0).then(|| method.to_string()),
                    content_type: (seq == 0).then(|| "application/json".into()),
                },
            );
            tokio::task::yield_now().await;
        }
        tokio::time::timeout(timeout.min(GEN3D_SYNC_TIMEOUT), async {
            let mut assembly = Assembly::default();
            let mut status = 200;
            let mut content_type = "application/json".to_string();
            let mut received_response = false;
            loop {
                // Synchronous generation may wait much longer for its first chunk.
                let message = if received_response {
                    tokio::time::timeout(RESPONSE_IDLE_TIMEOUT, rx.recv())
                        .await
                        .context("OAI response stalled")?
                } else {
                    rx.recv().await
                }
                .context("OAI response closed")?;
                received_response = true;
                match message {
                    ProtocolMessage::OaiError { message, code, .. } => {
                        bail!("{message} ({})", code.unwrap_or_default())
                    }
                    ProtocolMessage::OaiResponse {
                        seq,
                        last,
                        data,
                        status: s,
                        content_type: c,
                        ..
                    } => {
                        if seq == 0 && !assembly.parts.contains_key(&0) {
                            status = s.unwrap_or(200);
                            content_type = c.unwrap_or_else(|| "application/json".into());
                        }
                        if assembly.push(seq, last, data)? {
                            return Ok(TunnelResponse {
                                status,
                                content_type,
                                body: assembly.decode()?,
                            });
                        }
                    }
                    _ => bail!("Unexpected OAI response"),
                }
            }
        })
        .await
        .context("OAI request timed out")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn capture() -> (SendFn, Arc<Mutex<Vec<(String, ProtocolMessage)>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let copy = sent.clone();
        (
            Arc::new(move |peer, msg| copy.lock().unwrap().push((peer.into(), msg))),
            sent,
        )
    }

    fn request(
        id: &str,
        seq: u64,
        last: bool,
        data: String,
        path: Option<&str>,
    ) -> ProtocolMessage {
        ProtocolMessage::OaiRequest {
            id: id.into(),
            seq,
            last,
            data,
            path: path.map(String::from),
            method: None,
            content_type: None,
        }
    }

    const JOB_A: &str = "gen3d_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const JOB_B: &str = "gen3d_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const JOB_C: &str = "gen3d_cccccccccccccccccccccccccccccccc";

    fn fixture_job(id: &str, created: u64) -> Value {
        json!({"id":id, "created":created, "data":[{
            "url":format!("http://localhost:8087/v1/3d/generations/{id}/content"),
            "format":"glb"
        }]})
    }

    fn json_body(response: &TunnelResponse) -> Value {
        serde_json::from_slice(&response.body).unwrap()
    }

    type RecordedRequests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

    async fn mock_upstream(
        replies: Vec<(u16, &'static str, Vec<u8>)>,
    ) -> (ResolvedModel, RecordedRequests, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let copy = recorded.clone();
        let server = tokio::spawn(async move {
            for (status, content_type, body) in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut raw = Vec::new();
                let header_end = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    raw.extend_from_slice(&buf[..n]);
                    if let Some(end) = raw.windows(4).position(|s| s == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8(raw[..header_end].to_vec()).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|s| s.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                while raw.len() < header_end + length {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    raw.extend_from_slice(&buf[..n]);
                }
                copy.lock()
                    .unwrap()
                    .push((headers, raw[header_end..].to_vec()));
                socket.write_all(format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()
                ).as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        (
            ResolvedModel {
                base_url: format!("http://{addr}/v1"),
                api_key: "fixture-key".into(),
                model: "served-3d".into(),
                reasoning_effort: None,
                voice: None,
                lang_voices: HashMap::new(),
            },
            recorded,
            server,
        )
    }

    #[test]
    fn range_queries_are_strict_and_bounded() {
        let path = format!("/3d/generations/{JOB_A}/content");
        assert_eq!(split_range(&path).unwrap(), (path.clone(), None));
        assert_eq!(split_range("/models").unwrap(), ("/models".into(), None));
        for (start, end) in [(0, 9), (0, CONTENT_WINDOW - 1), (u64::MAX, u64::MAX)] {
            assert_eq!(
                split_range(&format!("{path}?range={start}-{end}")).unwrap(),
                (path.clone(), Some((start, end)))
            );
        }
        for query in [
            "",
            "range=",
            "range=0-",
            "range=-9",
            "range=9-0",
            "range=+0-9",
            "range=0-+9",
            "range=0-9&x=y",
            "range=0-9?x=y",
            "range=0-9-10",
            "Range=0-9",
            "range=0-18446744073709551616",
            "range=0-8388608",
            "range= 0-9",
            "range=０-9",
        ] {
            assert_eq!(
                split_range(&format!("{path}?{query}"))
                    .unwrap_err()
                    .to_string(),
                "request_rejected"
            );
        }
        for path in [
            "/models",
            "/3d/generations",
            "/3d/generations/invalid/content",
            "/3d/generations/gen3d_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/content",
        ] {
            assert!(split_range(&format!("{path}?range=0-9")).is_err());
        }
    }

    #[test]
    fn relay_deadlines_distinguish_sync_background_and_content() {
        for body in [
            json!({}),
            json!({"background":false}),
            json!({"background":"true"}),
        ] {
            assert_eq!(
                relay_deadline("POST", "/3d/generations", &body),
                GEN3D_SYNC_TIMEOUT
            );
        }
        assert_eq!(
            relay_deadline("POST", "/3d/generations", &json!({"background":true})),
            TIMEOUT
        );
        assert_eq!(
            relay_deadline(
                "GET",
                &format!("/3d/generations/{JOB_A}/content?range=0-9"),
                &Value::Null
            ),
            CONTENT_TIMEOUT
        );
        assert_eq!(
            relay_deadline("GET", "/3d/generations", &Value::Null),
            TIMEOUT
        );
        assert_eq!(
            relay_deadline("POST", "/chat/completions", &Value::Null),
            TIMEOUT
        );
    }

    #[test]
    fn job_urls_are_relativized_in_jobs_sync_results_and_lists() {
        let relative = format!("/3d/generations/{JOB_A}/content");
        let mut job = fixture_job(JOB_A, 1);
        job["data"]
            .as_array_mut()
            .unwrap()
            .push(json!({"b64_json":"AA=="}));
        relativize_job_urls(&mut job);
        assert_eq!(job["data"][0]["url"], relative);
        assert_eq!(job["data"][1], json!({"b64_json":"AA=="}));
        let mut sync = json!({"id":JOB_A, "data":[{"url":"upstream"},{"url":null}], "seed":42});
        relativize_job_urls(&mut sync);
        assert_eq!(sync["data"][0]["url"], relative);
        assert_eq!(sync["data"][1]["url"], relative);
        assert_eq!(sync["seed"], 42);
        let invalid = fixture_job("gen3d_invalid", 3);
        let mut list = json!({"object":"list", "data":[fixture_job(JOB_A, 1), invalid.clone()]});
        relativize_job_urls(&mut list);
        assert_eq!(list["data"][0]["data"][0]["url"], relative);
        assert_eq!(list["data"][1], invalid);
        for mut shape in [
            Value::Null,
            json!([]),
            json!({"data":[{"url":"unchanged"}]}),
            invalid,
        ] {
            let original = shape.clone();
            relativize_job_urls(&mut shape);
            assert_eq!(shape, original);
        }
    }

    #[tokio::test]
    async fn generation_jobs_bind_to_sender_forward_ranges_filter_lists_and_unbind() {
        let reply = |status, body: Value| {
            (
                status,
                "application/json",
                serde_json::to_vec(&body).unwrap(),
            )
        };
        let (target, recorded, server) = mock_upstream(vec![
            reply(202, fixture_job(JOB_A, 10)),
            reply(200, fixture_job(JOB_B, 30)),
            reply(202, fixture_job(JOB_C, 20)),
            reply(200, fixture_job(JOB_A, 10)),
            (206, "model/gltf-binary", b"0123456789".to_vec()),
            reply(200, json!({"object":"list", "data":[fixture_job(JOB_B, 30),
                fixture_job(JOB_A, 10), fixture_job(JOB_C, 20), fixture_job("gen3d_ffffffffffffffffffffffffffffffff", 40)]})),
            (204, "application/json", Vec::new()),
            reply(404, json!({"error":{"message":"gone"}})),
        ]).await;
        let (send, _) = capture();
        let resolved = Arc::new(Mutex::new(Vec::new()));
        let copy = resolved.clone();
        let provider = TunnelProvider::new(
            send,
            Arc::new(move |path, body| {
                assert_eq!(path, "/3d/generations");
                copy.lock().unwrap().push(body.clone());
                Ok(Some(target.clone()))
            }),
        );
        for (peer, body, id) in [
            (
                "a",
                br#"{"model":"3d-model","temperature":0.7,"parameters":{"z":1,"a":2}}"#.as_slice(),
                JOB_A,
            ),
            (
                "b",
                br#"{"model":"3d-model","background":true}"#.as_slice(),
                JOB_B,
            ),
            (
                "a",
                br#"{"model":"3d-model","image":"data:image/png;base64,AA=="}"#.as_slice(),
                JOB_C,
            ),
        ] {
            let response = provider
                .dispatch(peer, "POST", "/3d/generations", "application/json", body)
                .await
                .unwrap();
            assert_eq!(
                json_body(&response)["data"][0]["url"],
                format!("/3d/generations/{id}/content")
            );
        }
        assert!(
            resolved
                .lock()
                .unwrap()
                .iter()
                .all(|v| v["model"] == "3d-model")
        );
        let path = format!("/3d/generations/{JOB_A}");
        for peer in ["b", "attacker"] {
            let response = provider
                .dispatch(peer, "GET", &path, "", b"")
                .await
                .unwrap();
            assert_eq!(response.status, 404);
            assert_eq!(json_body(&response)["error"]["code"], "not_found");
        }
        assert_eq!(recorded.lock().unwrap().len(), 3);
        provider.drop_peer("a");
        let response = provider.dispatch("a", "GET", &path, "", b"").await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(
            json_body(&response)["data"][0]["url"],
            format!("{path}/content")
        );
        let response = provider
            .dispatch("a", "GET", &format!("{path}/content?range=0-9"), "", b"")
            .await
            .unwrap();
        assert_eq!(response.status, 206);
        assert_eq!(response.content_type, "model/gltf-binary");
        assert_eq!(response.body, b"0123456789");
        let response = provider
            .dispatch("a", "GET", "/3d/generations", "", b"")
            .await
            .unwrap();
        let list = json_body(&response);
        assert_eq!(list["data"].as_array().unwrap().len(), 2);
        assert_eq!(list["data"][0]["id"], JOB_C);
        assert_eq!(list["data"][1]["id"], JOB_A);
        assert_eq!(
            list["data"][0]["data"][0]["url"],
            format!("/3d/generations/{JOB_C}/content")
        );
        assert_eq!(
            provider
                .dispatch("a", "DELETE", &path, "", b"ignored")
                .await
                .unwrap()
                .status,
            204
        );
        assert_eq!(
            provider
                .dispatch("a", "GET", &path, "", b"")
                .await
                .unwrap()
                .status,
            404
        );
        assert_eq!(
            provider
                .dispatch("a", "GET", &format!("/3d/generations/{JOB_C}"), "", b"")
                .await
                .unwrap()
                .status,
            404
        );
        let empty = provider
            .dispatch("a", "GET", "/3d/generations", "", b"")
            .await
            .unwrap();
        assert_eq!(json_body(&empty), json!({"object":"list", "data":[]}));
        server.await.unwrap();
        let requests = recorded.lock().unwrap();
        assert_eq!(requests.len(), 8);
        for (_, body) in &requests[..3] {
            assert_eq!(
                serde_json::from_slice::<Value>(body).unwrap()["model"],
                "served-3d"
            );
        }
        let first = String::from_utf8_lossy(&requests[0].1);
        assert!(first.contains(r#""temperature":0.7"#));
        assert!(first.contains(r#""parameters":{"z":1,"a":2}"#));
        assert!(
            requests[4]
                .0
                .starts_with(&format!("GET /v1{path}/content HTTP/1.1"))
        );
        assert!(
            requests[4]
                .0
                .to_ascii_lowercase()
                .contains("range: bytes=0-9\r\n")
        );
        assert!(
            requests[6]
                .0
                .starts_with(&format!("DELETE /v1{path} HTTP/1.1"))
        );
        assert!(requests[6].1.is_empty());
    }

    #[tokio::test]
    async fn generation_lists_merge_distinct_targets_and_skip_failures() {
        let (target_a, calls_a, server_a) = mock_upstream(vec![(200, "application/json", serde_json::to_vec(
            &json!({"object":"list", "data":[fixture_job(JOB_A, 1), fixture_job(JOB_A, 1), fixture_job(JOB_B, 2)]})
        ).unwrap())]).await;
        let (target_b, calls_b, server_b) = mock_upstream(vec![(
            200,
            "application/json",
            serde_json::to_vec(&json!({"object":"list", "data":[fixture_job(JOB_B, 2)]})).unwrap(),
        )])
        .await;
        let (target_c, _, server_c) =
            mock_upstream(vec![(503, "application/json", b"{}".to_vec())]).await;
        let (target_d, _, server_d) =
            mock_upstream(vec![(200, "text/plain", b"not JSON".to_vec())]).await;
        let (send, _) = capture();
        let provider =
            TunnelProvider::new(send, Arc::new(|_, _| panic!("list must use bound targets")));
        {
            let mut bindings = provider.bindings.lock().unwrap();
            bindings.bind(JOB_A.into(), "a", target_a.clone());
            // Different models at the same upstream still require only one list call.
            let mut another_model = target_a;
            another_model.model = "another-model".into();
            bindings.bind(
                "gen3d_11111111111111111111111111111111".into(),
                "a",
                another_model,
            );
            bindings.bind(JOB_B.into(), "a", target_b);
            bindings.bind(JOB_C.into(), "a", target_c);
            bindings.bind(
                "gen3d_22222222222222222222222222222222".into(),
                "a",
                target_d,
            );
        }
        let response = provider
            .dispatch("a", "GET", "/3d/generations", "", b"")
            .await
            .unwrap();
        let body = json_body(&response);
        assert_eq!(body["data"].as_array().unwrap().len(), 2);
        assert_eq!(body["data"][0]["id"], JOB_B);
        assert_eq!(body["data"][1]["id"], JOB_A);
        for server in [server_a, server_b, server_c, server_d] {
            server.await.unwrap();
        }
        assert_eq!(calls_a.lock().unwrap().len(), 1);
        assert_eq!(calls_b.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn generation_validation_rejects_methods_bodies_and_unknown_jobs() {
        let (send, _) = capture();
        let provider =
            TunnelProvider::new(send, Arc::new(|_, _| panic!("invalid request resolved")));
        for (method, path, content_type, body) in [
            ("DELETE", "/3d/generations".to_string(), "", b"".as_slice()),
            (
                "POST",
                format!("/3d/generations/{JOB_A}"),
                "application/json",
                b"{}".as_slice(),
            ),
            (
                "DELETE",
                format!("/3d/generations/{JOB_A}/content"),
                "",
                b"".as_slice(),
            ),
            (
                "POST",
                "/3d/generations".to_string(),
                "application/json",
                b"[]".as_slice(),
            ),
            (
                "POST",
                "/3d/generations".to_string(),
                "application/json",
                b"null".as_slice(),
            ),
            (
                "POST",
                "/3d/generations".to_string(),
                "application/json",
                b"".as_slice(),
            ),
            (
                "POST",
                "/3d/generations".to_string(),
                "text/plain",
                b"{}".as_slice(),
            ),
        ] {
            let error = provider
                .dispatch("a", method, &path, content_type, body)
                .await
                .err()
                .unwrap();
            assert_eq!(error.to_string(), "request_rejected");
        }
        for path in [
            format!("/3d/generations/{JOB_A}"),
            "/3d/generations/invalid".into(),
        ] {
            assert_eq!(
                provider
                    .dispatch("a", "GET", &path, "", b"")
                    .await
                    .unwrap()
                    .status,
                404
            );
        }
        assert_eq!(
            provider
                .dispatch("a", "GET", "/admin", "", b"")
                .await
                .err()
                .unwrap()
                .to_string(),
            "unsupported_path"
        );
    }

    #[test]
    fn job_bindings_are_bounded_expire_and_cannot_change_owner() {
        let target = ResolvedModel {
            base_url: "http://localhost:8087/v1".into(),
            api_key: String::new(),
            model: "3d-model".into(),
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
        };
        let mut bindings = JobBindings::default();
        for n in 0..MAX_JOBS_PER_PEER {
            bindings.bind(format!("gen3d_{n:032x}"), "a", target.clone());
        }
        let oldest = "gen3d_00000000000000000000000000000000";
        bindings.0.get_mut(oldest).unwrap().at = Instant::now() - Duration::from_secs(10);
        bindings.bind(JOB_A.into(), "a", target.clone());
        assert_eq!(bindings.0.len(), MAX_JOBS_PER_PEER);
        assert!(!bindings.0.contains_key(oldest));
        bindings.bind(JOB_A.into(), "attacker", target.clone());
        assert!(bindings.target(JOB_A, "attacker").is_none());
        assert!(bindings.target(JOB_A, "a").is_some());
        bindings.remove(JOB_A, "attacker");
        assert!(bindings.target(JOB_A, "a").is_some());
        bindings.prune_at(Instant::now() + JOB_TTL);
        assert!(bindings.target(JOB_A, "a").is_none());
        bindings.0.clear();
        for n in 0..MAX_JOB_BINDINGS {
            bindings.bind(
                format!("gen3d_{n:032x}"),
                &format!("peer{n}"),
                target.clone(),
            );
        }
        bindings.0.get_mut(oldest).unwrap().at = Instant::now() - Duration::from_secs(10);
        bindings.bind(JOB_B.into(), "new", target);
        assert_eq!(bindings.0.len(), MAX_JOB_BINDINGS);
        assert!(!bindings.0.contains_key(oldest));
    }

    #[tokio::test]
    async fn consumer_explicit_methods_only_put_metadata_on_seq_zero() {
        for method in ["GET", "POST", "DELETE"] {
            let (send, sent) = capture();
            let consumer = TunnelConsumer::new(send);
            let copy = consumer.clone();
            let body = vec![b'x'; CHUNK_SIZE];
            let expected = if method == "POST" {
                body.clone()
            } else {
                Vec::new()
            };
            let path = format!("/3d/generations/{JOB_A}/content?range=0-9");
            let requested_path = path.clone();
            let task = tokio::spawn(async move {
                copy.request_method(
                    "provider",
                    method,
                    &requested_path,
                    &body,
                    GEN3D_SYNC_TIMEOUT * 2,
                )
                .await
            });
            let count = chunks(&expected).len();
            let messages = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if sent.lock().unwrap().len() == count {
                        break sent.lock().unwrap().clone();
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let mut encoded = String::new();
            let mut request_id = String::new();
            for (n, (peer, message)) in messages.into_iter().enumerate() {
                let ProtocolMessage::OaiRequest {
                    id,
                    seq,
                    last,
                    data,
                    path: p,
                    method: m,
                    content_type,
                } = message
                else {
                    panic!("request");
                };
                assert_eq!(peer, "provider");
                assert_eq!(seq, n as u64);
                assert_eq!(last, n + 1 == count);
                assert_eq!(m.as_deref(), (n == 0).then_some(method));
                assert_eq!(p.as_deref(), (n == 0).then_some(path.as_str()));
                assert_eq!(
                    content_type.as_deref(),
                    (n == 0).then_some("application/json")
                );
                request_id = id;
                encoded.push_str(&data);
            }
            assert_eq!(
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .unwrap(),
                expected
            );
            consumer.handle_message(
                "provider",
                &ProtocolMessage::OaiResponse {
                    id: request_id,
                    seq: 0,
                    last: true,
                    data: String::new(),
                    status: Some(204),
                    content_type: None,
                },
            );
            assert_eq!(task.await.unwrap().unwrap().status, 204);
            assert!(consumer.pending.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn consumer_accepts_synchronous_response_burst_before_polling() {
        let (send, sent) = capture();
        let consumer = TunnelConsumer::new(send);
        let copy = consumer.clone();
        let task = tokio::spawn(async move {
            copy.request_method("provider", "GET", "/models", b"", CONTENT_TIMEOUT)
                .await
        });
        let id = loop {
            if let Some((_, ProtocolMessage::OaiRequest { id, .. })) = sent.lock().unwrap().first()
            {
                break id.clone();
            }
            tokio::task::yield_now().await;
        };
        let body = vec![b'x'; 200 * CHUNK_SIZE / 4 * 3 - 1];
        let parts = chunks(&body);
        assert_eq!(parts.len(), 200);
        // This current-thread test does not yield until the entire burst is queued.
        for (seq, data) in parts.into_iter().enumerate() {
            consumer.handle_message(
                "provider",
                &ProtocolMessage::OaiResponse {
                    id: id.clone(),
                    seq: seq as u64,
                    last: seq == 199,
                    data,
                    status: (seq == 0).then_some(206),
                    content_type: (seq == 0).then(|| "model/gltf-binary".into()),
                },
            );
        }
        let response = task.await.unwrap().unwrap();
        assert_eq!(response.body, body);
        assert_eq!(response.status, 206);
        assert_eq!(response.content_type, "model/gltf-binary");
        assert!(consumer.pending.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn consumer_idle_timeout_starts_after_first_response_and_resets_on_chunks() {
        let (send, sent) = capture();
        let consumer = TunnelConsumer::new(send);
        let copy = consumer.clone();
        let started = tokio::time::Instant::now();
        let task = tokio::spawn(async move {
            copy.request_method(
                "provider",
                "POST",
                "/3d/generations",
                b"{}",
                GEN3D_SYNC_TIMEOUT,
            )
            .await
        });
        let id = loop {
            if let Some((_, ProtocolMessage::OaiRequest { id, .. })) = sent.lock().unwrap().first()
            {
                break id.clone();
            }
            tokio::task::yield_now().await;
        };
        tokio::task::yield_now().await;
        tokio::time::advance(RESPONSE_IDLE_TIMEOUT * 2).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());

        for seq in 0..2 {
            consumer.handle_message(
                "provider",
                &ProtocolMessage::OaiResponse {
                    id: id.clone(),
                    seq,
                    last: false,
                    data: "eHh4".into(),
                    status: None,
                    content_type: None,
                },
            );
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(20)).await;
            tokio::task::yield_now().await;
            assert!(!task.is_finished());
        }
        tokio::time::advance(Duration::from_secs(11)).await;
        let error = task.await.unwrap().err().unwrap();
        assert_eq!(error.to_string(), "OAI response stalled");
        assert!(started.elapsed() < CONTENT_TIMEOUT);
        assert!(consumer.pending.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn consumer_explicit_deadline_allows_long_waits_and_caps_at_sync_limit() {
        let (send, sent) = capture();
        let consumer = TunnelConsumer::new(send);
        let copy = consumer.clone();
        let task = tokio::spawn(async move {
            copy.request_method(
                "provider",
                "POST",
                "/3d/generations",
                b"{}",
                GEN3D_SYNC_TIMEOUT * 2,
            )
            .await
        });
        while sent.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(TIMEOUT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(GEN3D_SYNC_TIMEOUT - TIMEOUT).await;
        assert!(task.await.unwrap().is_err());
        assert!(consumer.pending.lock().unwrap().is_empty());

        sent.lock().unwrap().clear();
        let copy = consumer.clone();
        let task = tokio::spawn(async move {
            copy.request("provider", "/chat/completions", b"{}", GEN3D_SYNC_TIMEOUT)
                .await
        });
        while sent.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(TIMEOUT + Duration::from_secs(1)).await;
        assert!(task.await.unwrap().is_err());
        assert!(consumer.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn mistai_chunk_fixture_matches_bytes_padding_and_empty_body() {
        let fixture: Value =
            serde_json::from_str(include_str!("fixtures/mistai-oai-chunks.json")).unwrap();
        let body = fixture["body"].as_str().unwrap();
        let expected: Vec<_> = fixture["request"]
            .as_array()
            .unwrap()
            .iter()
            .map(|msg| msg["data"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(chunks(body.as_bytes()), expected);
        assert_eq!(chunks(b""), vec![""]);
        assert_eq!(chunks(b"x"), vec!["eA=="]);
        let mut assembly = Assembly::default();
        // Reliable channel order is usual, but reassembly also accepts a last
        // chunk arriving early and ignores duplicate chunks, like mistai.
        for value in fixture["request"].as_array().unwrap().iter().rev() {
            let msg = protocol::decode(&serde_json::to_vec(value).unwrap()).unwrap();
            let ProtocolMessage::OaiRequest {
                seq, last, data, ..
            } = msg
            else {
                panic!("request");
            };
            assembly.push(seq, last, data.clone()).unwrap();
            assert!(!assembly.push(seq, last, data).unwrap());
        }
        assert_eq!(assembly.decode().unwrap(), body.as_bytes());
        let response = ProtocolMessage::OaiResponse {
            id: "fixture".into(),
            seq: 0,
            last: false,
            data: expected[0].clone(),
            status: Some(201),
            content_type: Some("application/json".into()),
        };
        let json: Value = serde_json::from_slice(&protocol::encode(&response)).unwrap();
        assert_eq!(json["contentType"], "application/json");
        assert!(json.get("content_type").is_none());
        assert_eq!(
            protocol::decode(&protocol::encode(&response)),
            Some(response)
        );
    }

    #[test]
    fn assembly_caps_chunks_sequences_and_total_body() {
        let mut assembly = Assembly::default();
        assert!(assembly.push(MAX_SEQ + 1, true, "".into()).is_err());
        assert!(assembly.push(0, false, "x".repeat(CHUNK_SIZE + 1)).is_err());
        assembly.chars = MAX_BASE64_CHARS;
        assert!(assembly.push(0, false, "x".into()).is_err());
        let mut assembly = Assembly::default();
        assert!(!assembly.push(1, true, "AA==".into()).unwrap());
        assert!(assembly.push(2, false, "".into()).is_err());
    }

    #[tokio::test]
    async fn request_buffers_are_peer_bound_capped_expired_and_removed_on_disconnect() {
        let _authz_guard = crate::authz::TEST_LOCK.lock().await;
        let (send, sent) = capture();
        let provider = TunnelProvider::new(send, Arc::new(|_, _| Ok(None)));
        for peer in ["a", "b"] {
            provider
                .handle_message(
                    peer,
                    request("same", 0, false, "eA==".into(), Some("/models")),
                )
                .await;
        }
        assert_eq!(provider.pending.lock().unwrap().len(), 2);
        provider.drop_peer("b");
        assert_eq!(provider.pending.lock().unwrap().len(), 1);
        for n in 0..MAX_PER_PEER {
            provider
                .handle_message(
                    "a",
                    request(&format!("r{n}"), 0, false, "".into(), Some("/models")),
                )
                .await;
        }
        assert_eq!(provider.pending.lock().unwrap().len(), MAX_PER_PEER);
        assert!(
            matches!(&sent.lock().unwrap().last().unwrap().1, ProtocolMessage::OaiError { code: Some(code), .. } if code == "request_rejected")
        );
        for (at, _) in provider.pending.lock().unwrap().values_mut() {
            *at = Instant::now() - TIMEOUT - Duration::from_secs(1);
        }
        provider
            .handle_message("c", request("new", 0, false, "".into(), None))
            .await;
        assert_eq!(provider.pending.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unsupported_path_and_unshared_model_keep_mistai_error_codes() {
        let _authz_guard = crate::authz::TEST_LOCK.lock().await;
        let (send, sent) = capture();
        let provider = TunnelProvider::new(send, Arc::new(|_, _| bail!("model_not_shared")));
        provider
            .handle_message("peer", request("path", 0, true, "".into(), Some("/admin")))
            .await;
        provider
            .handle_message(
                "peer",
                request(
                    "model",
                    0,
                    true,
                    chunks(br#"{"model":"unshared"}"#).remove(0),
                    Some("/chat/completions"),
                ),
            )
            .await;
        let sent = sent.lock().unwrap();
        assert!(
            matches!(&sent[0].1, ProtocolMessage::OaiError { code: Some(code), .. } if code == "unsupported_path")
        );
        assert!(
            matches!(&sent[1].1, ProtocolMessage::OaiError { code: Some(code), .. } if code == "model_not_shared")
        );
    }

    #[tokio::test]
    async fn tunnel_forwards_resolved_key_image_effort_and_http_status_without_temperature() {
        let _authz_guard = crate::authz::TEST_LOCK.lock().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            let header_end = loop {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                raw.extend_from_slice(&buf[..n]);
                if let Some(end) = raw.windows(4).position(|s| s == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = String::from_utf8(raw[..header_end].to_vec()).unwrap();
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse().unwrap())
                })
                .unwrap();
            while raw.len() < header_end + length {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                raw.extend_from_slice(&buf[..n]);
            }
            let body = br#"{"error":{"message":"fixture failure"}}"#;
            socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
            (
                headers,
                serde_json::from_slice::<Value>(&raw[header_end..]).unwrap(),
            )
        });
        let (send, sent) = capture();
        let provider = TunnelProvider::new(
            send,
            Arc::new(move |_, _| {
                Ok(Some(ResolvedModel {
                    base_url: format!("http://{addr}/v1"),
                    api_key: "provider-secret".into(),
                    model: "resolved".into(),
                    reasoning_effort: None,
                    voice: None,
                    lang_voices: HashMap::new(),
                }))
            }),
        );
        let body = json!({"model":"raw","reasoning_effort":"future-effort","temperature":0.8,"stream":true,
            "messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]});
        provider
            .handle_message(
                "consumer",
                request(
                    "r",
                    0,
                    true,
                    chunks(&serde_json::to_vec(&body).unwrap()).remove(0),
                    Some("/chat/completions"),
                ),
            )
            .await;
        let (headers, upstream) = server.await.unwrap();
        assert!(headers.starts_with("POST /v1/chat/completions "));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("authorization: bearer provider-secret")
        );
        assert_eq!(upstream["model"], "resolved");
        assert_eq!(upstream["reasoning_effort"], "future-effort");
        assert_eq!(upstream["messages"], body["messages"]);
        assert_eq!(upstream["stream"], false);
        assert!(upstream.get("temperature").is_none());
        assert!(
            matches!(&sent.lock().unwrap()[0], (peer, ProtocolMessage::OaiResponse { status: Some(429), content_type: Some(ct), last: true, .. }) if peer == "consumer" && ct == "application/json")
        );
    }

    #[tokio::test]
    async fn consumer_ignores_other_senders_and_reassembles_response() {
        let _authz_guard = crate::authz::TEST_LOCK.lock().await;
        let (send, sent) = capture();
        let consumer = TunnelConsumer::new(send);
        let c = consumer.clone();
        let task = tokio::spawn(async move {
            c.request(
                "provider",
                "/chat/completions",
                b"{}",
                Duration::from_secs(1),
            )
            .await
        });
        let id = loop {
            if let Some((_, ProtocolMessage::OaiRequest { id, .. })) = sent.lock().unwrap().first()
            {
                break id.clone();
            }
            tokio::task::yield_now().await;
        };
        consumer.handle_message(
            "attacker",
            &ProtocolMessage::OaiError {
                id: id.clone(),
                message: "bad".into(),
                code: None,
            },
        );
        consumer.handle_message(
            "provider",
            &ProtocolMessage::OaiResponse {
                id,
                seq: 0,
                last: true,
                data: "eA==".into(),
                status: Some(201),
                content_type: Some("text/plain".into()),
            },
        );
        let result = task.await.unwrap().unwrap();
        assert_eq!(result.body, b"x");
        assert_eq!(result.status, 201);
        assert_eq!(result.content_type, "text/plain");
        assert!(consumer.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn rewrite_object_keeps_nested_key_order_and_rewrites_top_level() {
        let input = br#"{"temperature":1,"model":"asked","questions":{"q":{"type":"choice","criteria":{"zebra":"z","apple":null}}},"state":"s"}"#;
        let out = rewrite_object(
            input,
            &[("model", json!("served")), ("stream", json!(false))],
            &["temperature"],
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert_eq!(
            text,
            r#"{"model":"served","questions":{"q":{"type":"choice","criteria":{"zebra":"z","apple":null}}},"state":"s","stream":false}"#
        );
        let dup = rewrite_object(
            br#"{"model":"a","model":"b"}"#,
            &[("model", json!("c"))],
            &[],
        )
        .unwrap();
        assert_eq!(dup, br#"{"model":"c"}"#);
        assert!(rewrite_object(b"[1]", &[], &[]).is_err());
        assert!(rewrite_object(b"", &[], &[]).is_err());
    }

    #[tokio::test]
    async fn models_get_and_embeddings_post_are_forwarded_to_the_selected_upstream() {
        let _authz_guard = crate::authz::TEST_LOCK.lock().await;
        for (path, method, request_body, response_body) in [
            (
                "/models",
                "GET",
                json!(null),
                json!({"data":[{"id":"raw"}]}),
            ),
            (
                "/embeddings",
                "POST",
                json!({"model":"raw","input":"hello"}),
                json!({"data":[{"embedding":[0.5]}]}),
            ),
            (
                "/systemone",
                "POST",
                json!({"model":"raw","state":"hi","questions":{"q":{"type":"noul","instructions":"?"}}}),
                json!({"model":"raw","answers":{"q":{"type":"noul","noul":0.9}}}),
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let expected_line = format!("{method} /v1{path} HTTP/1.1");
            let response_bytes = serde_json::to_vec(&response_body).unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|s| s.trim().parse().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                assert!(String::from_utf8_lossy(&bytes).starts_with(&expected_line));
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response_bytes.len()).as_bytes()).await.unwrap();
                socket.write_all(&response_bytes).await.unwrap();
            });
            let (send, sent) = capture();
            let provider = TunnelProvider::new(
                send,
                Arc::new(move |_, _| {
                    Ok(Some(ResolvedModel {
                        base_url: format!("http://{addr}/v1"),
                        api_key: String::new(),
                        model: "raw".into(),
                        reasoning_effort: None,
                        voice: None,
                        lang_voices: HashMap::new(),
                    }))
                }),
            );
            let mut msg = request(
                "r",
                0,
                true,
                if method == "GET" {
                    String::new()
                } else {
                    chunks(&serde_json::to_vec(&request_body).unwrap()).remove(0)
                },
                Some(path),
            );
            if let ProtocolMessage::OaiRequest { method: m, .. } = &mut msg {
                *m = Some(method.into());
            }
            provider.handle_message("consumer", msg).await;
            server.await.unwrap();
            let sent = sent.lock().unwrap();
            let ProtocolMessage::OaiResponse { data, status, .. } = &sent[0].1 else {
                panic!("response");
            };
            assert_eq!(*status, Some(200));
            assert_eq!(
                serde_json::from_slice::<Value>(
                    &base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .unwrap()
                )
                .unwrap(),
                response_body
            );
        }
    }
}
