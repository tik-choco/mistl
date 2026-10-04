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
use crate::config::ResolvedModel;

pub(super) const CHUNK_SIZE: usize = 12 * 1024;
pub(super) const MAX_BASE64_CHARS: usize = 24 * 1024 * 1024;
const MAX_BYTES: usize = MAX_BASE64_CHARS / 4 * 3;
const MAX_BUFFERS: usize = 32;
const MAX_PER_PEER: usize = 4;
const MAX_TOTAL_CHARS: usize = 48 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(120);
const MAX_SEQ: u64 = (MAX_BASE64_CHARS / CHUNK_SIZE) as u64;

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

pub(super) struct TunnelProvider {
    send: SendFn,
    resolve: Resolver,
    pending: Mutex<HashMap<(String, String), (Instant, Assembly)>>,
    jobs: JobLimiter,
}

impl TunnelProvider {
    pub fn new(send: SendFn, resolve: Resolver) -> Arc<Self> {
        Arc::new(Self {
            send,
            resolve,
            pending: Mutex::new(HashMap::new()),
            jobs: JobLimiter::new(8),
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

    pub async fn handle_message(&self, from: &str, msg: ProtocolMessage) {
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
            return;
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
            return;
        }
        let ready = {
            let mut pending = self.pending.lock().expect("oai pending lock");
            let now = Instant::now();
            pending.retain(|_, (at, _)| now.duration_since(*at) < TIMEOUT);
            let key = (from.to_string(), id.clone());
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
                return;
            }
            let (at, assembly) = pending
                .entry(key.clone())
                .or_insert_with(|| (now, Assembly::default()));
            *at = now;
            // Later chunks never override seq 0 metadata, including duplicates.
            if seq == 0 && !assembly.parts.contains_key(&0) {
                assembly.path = path.unwrap_or_default();
                assembly.method = method.unwrap_or_else(|| "POST".into());
                assembly.content_type = content_type.unwrap_or_else(|| "application/json".into());
            }
            match assembly.push(seq, last, data) {
                Ok(false) => return,
                Ok(true) => pending.remove(&key).expect("complete assembly").1,
                Err(err) => {
                    pending.remove(&key);
                    let code = err.to_string();
                    self.error(from, id, "Invalid or oversized OAI request.", Some(&code));
                    return;
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
            return;
        };
        match tokio::time::timeout(TIMEOUT, self.dispatch(ready)).await {
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
    }

    async fn dispatch(&self, assembly: Assembly) -> Result<TunnelResponse> {
        let path = assembly.path.clone();
        let method = assembly.method.clone();
        if !matches!(
            path.as_str(),
            "/chat/completions" | "/models" | "/embeddings"
        ) {
            bail!("unsupported_path");
        }
        if (path == "/models" && method != "GET") || (path != "/models" && method != "POST") {
            bail!("request_rejected");
        }
        let content_type = assembly.content_type.clone();
        let bytes = assembly.decode()?;
        let mut body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            if !content_type.to_ascii_lowercase().contains("json") {
                bail!("request_rejected");
            }
            serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("request_rejected"))?
        };
        let target = (self.resolve)(&path, &body)?.context("unsupported_path")?;
        if method == "POST" {
            let object = body.as_object_mut().context("request_rejected")?;
            object.remove("temperature");
            object.insert("model".into(), Value::String(target.model));
            if path == "/chat/completions" {
                object.insert("stream".into(), Value::Bool(false));
            }
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(TIMEOUT)
            .build()?;
        let url = format!("{}{path}", target.base_url.trim_end_matches('/'));
        let mut request = client.request(reqwest::Method::from_bytes(method.as_bytes())?, url);
        if !target.api_key.trim().is_empty() {
            request = request.bearer_auth(target.api_key);
        }
        if method != "GET" {
            request = request.json(&body);
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
        if body.len() > MAX_BYTES {
            bail!("OAI request too large");
        }
        let id = protocol::random_id();
        let mut rx = {
            let mut pending = self.pending.lock().expect("oai consumer lock");
            if pending.len() >= 8 {
                bail!("Too many pending OAI requests");
            }
            let (tx, rx) = mpsc::channel(64);
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
                    method: (seq == 0)
                        .then(|| if path == "/models" { "GET" } else { "POST" }.into()),
                    content_type: (seq == 0).then(|| "application/json".into()),
                },
            );
            tokio::task::yield_now().await;
        }
        tokio::time::timeout(timeout.min(TIMEOUT), async {
            let mut assembly = Assembly::default();
            let mut status = 200;
            let mut content_type = "application/json".to_string();
            loop {
                match rx.recv().await.context("OAI response closed")? {
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

    #[tokio::test]
    async fn models_get_and_embeddings_post_are_forwarded_to_the_selected_upstream() {
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
