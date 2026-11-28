//! Raw chat, Completions and Responses API replies used by both HTTP and room transports.
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use serde_json::Value;
use std::time::Duration;

use super::oai_tunnel::{MAX_BYTES, RESPONSE_IDLE_TIMEOUT, TunnelResponse};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Endpoint {
    #[default]
    Chat,
    Responses,
    Completions,
}

tokio::task_local! {
    /// Select the endpoint for the existing API proxy callback. This scope is
    /// local to one request; provider wire requests use their explicit path.
    static API_ENDPOINT: Endpoint;
}

impl Endpoint {
    pub(super) fn current() -> Self {
        API_ENDPOINT
            .try_with(|endpoint| *endpoint)
            .unwrap_or(Self::Chat)
    }

    pub(super) fn from_path(path: &str) -> Option<Self> {
        match path {
            "/chat/completions" => Some(Self::Chat),
            "/responses" => Some(Self::Responses),
            "/completions" => Some(Self::Completions),
            _ => None,
        }
    }

    pub(super) fn path(self) -> &'static str {
        match self {
            Self::Chat => "/chat/completions",
            Self::Responses => "/responses",
            Self::Completions => "/completions",
        }
    }

    pub(super) fn service(self) -> &'static str {
        match self {
            Self::Chat => super::protocol::SERVICE_OAI_CHAT,
            Self::Responses => super::protocol::SERVICE_OAI_RESPONSES,
            Self::Completions => super::protocol::SERVICE_OAI_COMPLETIONS,
        }
    }

    pub(super) async fn scope<T>(self, future: impl Future<Output = T>) -> T {
        API_ENDPOINT.scope(self, future).await
    }
}

pub(super) struct Reply {
    pub status: u16,
    pub content_type: String,
    pub body: BoxStream<'static, Result<Bytes>>,
}

impl Reply {
    pub fn is_sse(&self) -> bool {
        self.content_type
            .split(';')
            .next()
            .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
    }

    pub async fn buffered(mut self) -> Result<TunnelResponse> {
        let mut body = Vec::new();
        while let Some(chunk) = self.body.next().await {
            let chunk = chunk?;
            if body.len().saturating_add(chunk.len()) > MAX_BYTES {
                bail!("request_too_large");
            }
            body.extend_from_slice(&chunk);
        }
        Ok(TunnelResponse {
            status: self.status,
            content_type: self.content_type,
            body,
        })
    }
}

/// Only routing fields are rewritten. Even an explicit null effort belongs
/// to the client; an absent field may inherit a configured default.
pub(super) fn payload(
    raw: &[u8],
    body: &Value,
    model: &str,
    effort: Option<String>,
) -> Result<Vec<u8>> {
    payload_for(Endpoint::current(), raw, body, model, effort)
}

pub(super) fn payload_for(
    endpoint: Endpoint,
    raw: &[u8],
    body: &Value,
    model: &str,
    effort: Option<String>,
) -> Result<Vec<u8>> {
    use serde::de::{Deserializer, MapAccess, Visitor};
    use serde_json::value::RawValue;

    // Borrow the exact model value spans from the original JSON instead of
    // serializing the object again. Rewrite every duplicate model member
    // consistently so routing and upstream cannot disagree on the target.
    struct Members<'a> {
        models: Vec<&'a RawValue>,
        any: bool,
    }
    impl<'de> serde::Deserialize<'de> for Members<'de> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Members<'de>;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<Members<'de>, A::Error> {
                    let mut members = Members {
                        models: Vec::new(),
                        any: false,
                    };
                    while let Some((key, value)) = map.next_entry::<String, &'de RawValue>()? {
                        members.any = true;
                        if key == "model" {
                            members.models.push(value);
                        }
                    }
                    Ok(members)
                }
            }
            d.deserialize_map(V)
        }
    }
    let members: Members<'_> = serde_json::from_slice(raw).context("request_rejected")?;
    let encoded_model = serde_json::to_string(model)?;
    let effort =
        effort.filter(|_| endpoint == Endpoint::Chat && body.get("reasoning_effort").is_none());
    let mut out = Vec::with_capacity(raw.len());
    let mut cursor = 0;
    for value in &members.models {
        let text = value.get();
        if serde_json::from_str::<String>(text).ok().as_deref() == Some(model) {
            continue;
        }
        let start = text.as_ptr() as usize - raw.as_ptr() as usize;
        out.extend_from_slice(&raw[cursor..start]);
        out.extend_from_slice(encoded_model.as_bytes());
        cursor = start + text.len();
    }
    let closing = raw
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .context("request_rejected")?;
    out.extend_from_slice(&raw[cursor..closing]);
    if members.models.is_empty() {
        if members.any {
            out.push(b',');
        }
        out.extend_from_slice(b"\"model\":");
        out.extend_from_slice(encoded_model.as_bytes());
    }
    if let Some(effort) = effort {
        out.extend_from_slice(b",\"reasoning_effort\":");
        out.extend_from_slice(serde_json::to_string(&effort)?.as_bytes());
    }
    out.extend_from_slice(&raw[closing..]);
    Ok(out)
}

pub(super) async fn upstream(
    base_url: &str,
    api_key: &str,
    body: Vec<u8>,
    timeout: Duration,
) -> Result<Reply> {
    upstream_for(Endpoint::current(), base_url, api_key, body, timeout).await
}

pub(super) async fn upstream_for(
    endpoint: Endpoint,
    base_url: &str,
    api_key: &str,
    body: Vec<u8>,
    timeout: Duration,
) -> Result<Reply> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()?;
    let mut request = client
        .post(format!(
            "{}{}",
            base_url.trim_end_matches('/'),
            endpoint.path()
        ))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
    if !api_key.trim().is_empty() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await?;
    if response
        .content_length()
        .is_some_and(|n| n > MAX_BYTES as u64)
    {
        bail!("request_too_large");
    }
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let body =
        futures_util::stream::try_unfold((response, 0usize), |(mut response, total)| async move {
            let Some(chunk) = tokio::time::timeout(RESPONSE_IDLE_TIMEOUT, response.chunk())
                .await
                .context("OAI upstream response stalled")??
            else {
                return Ok(None);
            };
            let total = total
                .checked_add(chunk.len())
                .context("request_too_large")?;
            if total > MAX_BYTES {
                bail!("request_too_large");
            }
            Ok(Some((chunk, (response, total))))
        })
        .boxed();
    Ok(Reply {
        status,
        content_type,
        body,
    })
}

/// The authorization ledger's unit is output characters, not tokens. Keep
/// that accounting intact while observing the upstream's actual token usage
/// separately. No response data is changed or retained for telemetry.
#[derive(Default)]
pub(super) struct Usage {
    pub chars: u64,
    endpoint: Endpoint,
    tokens: Option<(Option<u64>, Option<u64>, Option<u64>)>,
    line: Vec<u8>,
    skip_line: bool,
}

impl Usage {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            ..Self::default()
        }
    }

    pub fn json(&mut self, bytes: &[u8]) {
        let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
            return;
        };
        if self.endpoint == Endpoint::Responses {
            self.responses_json(&value);
            return;
        }
        if let Some(usage) = value.get("usage").filter(|u| u.is_object()) {
            self.tokens = Some((
                usage["prompt_tokens"].as_u64(),
                usage["completion_tokens"].as_u64(),
                usage["total_tokens"].as_u64(),
            ));
        }
        if let Some(choices) = value["choices"].as_array() {
            for choice in choices {
                if self.endpoint == Endpoint::Completions {
                    if let Some(text) = choice["text"].as_str() {
                        self.chars = self.chars.saturating_add(text.chars().count() as u64);
                    }
                    continue;
                }
                let message = choice.get("delta").or_else(|| choice.get("message"));
                if let Some(text) = message.and_then(|m| m["content"].as_str()) {
                    self.chars = self.chars.saturating_add(text.chars().count() as u64);
                }
            }
        }
    }

    fn responses_json(&mut self, value: &Value) {
        // Responses SSE completion events nest usage under `response`.
        // Do not count the completed output again after counting deltas.
        let usage = value
            .get("usage")
            .or_else(|| value["response"].get("usage"));
        if let Some(usage) = usage.filter(|u| u.is_object()) {
            self.tokens = Some((
                usage["input_tokens"].as_u64(),
                usage["output_tokens"].as_u64(),
                usage["total_tokens"].as_u64(),
            ));
        }
        if value["type"] == "response.output_text.delta"
            && let Some(text) = value["delta"].as_str()
        {
            self.chars = self.chars.saturating_add(text.chars().count() as u64);
        }
        if let Some(output) = value["output"].as_array() {
            for item in output {
                if let Some(content) = item["content"].as_array() {
                    for part in content {
                        if let Some(text) = part["text"].as_str() {
                            self.chars = self.chars.saturating_add(text.chars().count() as u64);
                        }
                    }
                }
            }
        }
    }

    pub fn sse(&mut self, bytes: &[u8]) {
        // Retain at most one data line; the response itself is byte-capped.
        // A large but valid event must still contribute to API usage.
        for part in bytes.split_inclusive(|b| *b == b'\n') {
            let end = part.ends_with(b"\n");
            if !self.skip_line && self.line.len().saturating_add(part.len()) <= MAX_BYTES {
                self.line.extend_from_slice(part);
            } else {
                self.skip_line = true;
            }
            if end {
                let mut line = std::mem::take(&mut self.line);
                if !self.skip_line
                    && let Some(data) = line.strip_prefix(b"data:")
                {
                    self.json(data);
                }
                line.clear();
                self.line = line;
                self.skip_line = false;
            }
        }
    }

    pub fn record_tokens(&self) {
        if let Some((input, output, total)) = self.tokens {
            match self.endpoint {
                Endpoint::Chat | Endpoint::Completions => {
                    tracing::debug!(prompt_tokens = ?input, completion_tokens = ?output, total_tokens = ?total, "ai: upstream chat token usage")
                }
                Endpoint::Responses => {
                    tracing::debug!(input_tokens = ?input, output_tokens = ?output, total_tokens = ?total, "ai: upstream Responses API token usage")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn absent_effort_inherits_but_explicit_null_and_unknown_fields_survive() {
        for (raw, expected) in [
            (
                br#"{"model":"alias","vendor":{"z":1,"a":2}}"#.as_slice(),
                br#"{"model":"chat-model","vendor":{"z":1,"a":2},"reasoning_effort":"high"}"#
                    .as_slice(),
            ),
            (
                br#"{"model":"alias","reasoning_effort":null,"vendor":{"z":1,"a":2}}"#.as_slice(),
                br#"{"model":"chat-model","reasoning_effort":null,"vendor":{"z":1,"a":2}}"#
                    .as_slice(),
            ),
        ] {
            let body: Value = serde_json::from_slice(raw).unwrap();
            assert_eq!(
                payload(raw, &body, "chat-model", Some("high".into())).unwrap(),
                expected
            );
        }
        let raw = br#" { "vendor" : { "z":1, "a":2 }, "model" : "alias", "model":"old", "temperature": 0.40 } "#;
        let body: Value = serde_json::from_slice(raw).unwrap();
        assert_eq!(payload(raw, &body, "chat-model", None).unwrap(),
            br#" { "vendor" : { "z":1, "a":2 }, "model" : "chat-model", "model":"chat-model", "temperature": 0.40 } "#);
    }

    #[test]
    fn meter_reads_usage_across_chunks_without_mutating_bytes() {
        let mut meter = Usage::default();
        meter.sse(b"data: {\"choices\":[{\"delta\":{\"content\":\"he");
        meter.sse(b"llo\"}},{\"delta\":{\"content\":\"world\"}}]}\r\n\r\n");
        meter.sse(b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\ndata: [DONE]\n\n");
        assert_eq!(meter.chars, 10);
        assert_eq!(meter.tokens, Some((Some(2), Some(3), Some(5))));
    }

    #[test]
    fn completions_meter_counts_choice_text() {
        let mut meter = Usage::new(Endpoint::Completions);
        meter.json(br#"{"choices":[{"text":"one"},{"text":"two"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}"#);
        assert_eq!(meter.chars, 6);
        assert_eq!(meter.tokens, Some((Some(2), Some(3), Some(5))));
        meter.sse(b"data: {\"choices\":[{\"text\":\"three\"}]}\n\ndata: [DONE]\n\n");
        assert_eq!(meter.chars, 11);
    }

    #[test]
    fn responses_meter_counts_text_and_token_usage_without_double_counting_completed_output() {
        let response = br#"{"output":[{"content":[{"type":"output_text","text":"one"}]},{"content":[{"type":"output_text","text":"two"}]}],"usage":{"input_tokens":7,"output_tokens":4,"total_tokens":11}}"#;
        let mut meter = Usage::new(Endpoint::Responses);
        meter.json(response);
        assert_eq!(meter.chars, 6);
        assert_eq!(meter.tokens, Some((Some(7), Some(4), Some(11))));

        let mut meter = Usage::new(Endpoint::Responses);
        meter.sse(b"event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"on");
        meter.sse(
            b"e\"}\r\n\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"two\"}\n\n",
        );
        let event = [
            b"data: {\"type\":\"response.completed\",\"response\":".as_slice(),
            response,
            b"}\n\n",
        ]
        .concat();
        meter.sse(&event);
        assert_eq!(meter.chars, 6);
        assert_eq!(meter.tokens, Some((Some(7), Some(4), Some(11))));
    }

    #[tokio::test]
    async fn unknown_length_upstream_stream_is_capped() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            socket.read(&mut request).await.unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await.unwrap();
            let chunk = vec![b'x'; 64 * 1024];
            for _ in 0..=MAX_BYTES / chunk.len() {
                if socket.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });
        let mut reply = upstream(
            &format!("http://{addr}/v1"),
            "",
            b"{}".to_vec(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let mut len = 0;
        loop {
            match reply.body.next().await {
                Some(Ok(chunk)) => len += chunk.len(),
                Some(Err(err)) => {
                    assert_eq!(err.to_string(), "request_too_large");
                    assert!(len <= MAX_BYTES);
                    break;
                }
                None => panic!("oversized upstream stream was accepted"),
            }
        }
        drop(reply);
        server.await.unwrap();
    }
}
