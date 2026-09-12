//! Upstream OpenAI-compatible client, ported from mistai's `openai.ts`.
//!
//! Used when this node acts as a provider: p2p `llm_request`s are forwarded
//! to `POST {base_url}/chat/completions` and the streamed deltas are
//! relayed back over the network.
//!
//! ## Contract (mirrors openai.ts)
//!
//! - `base_url` has any trailing `/` stripped before appending paths.
//! - Headers: `Content-Type: application/json`,
//!   `Authorization: Bearer {api_key}` (header always sent, even when the
//!   key is empty, matching openai.ts).
//! - Request body: `{"model", "messages", "stream": true}` plus
//!   `"temperature"` only when configured. `model` = the per-request model
//!   if `Some`, else `config.model`; if neither is set, fail with a clear
//!   "no model configured" error before sending.
//! - Non-2xx -> error including status and the first 500 chars of the body.
//! - If the response `content-type` contains `text/event-stream`, parse
//!   SSE: buffer text across chunks, split on `\n`, keep the trailing
//!   partial line buffered; for each complete line: trim, require `data:`
//!   prefix (else skip), strip prefix + trim; `[DONE]` -> skip; else parse
//!   JSON (malformed -> skip silently) and extract
//!   `choices[0].delta.content`; when it is a non-empty string, append it
//!   to the accumulated result and send it into `delta_tx` (if provided).
//!   Return the accumulated full string at stream end.
//! - Otherwise (non-SSE): parse the whole body as JSON, extract
//!   `choices[0].message.content` (must be a string, else error), send it
//!   once into `delta_tx`, and return it.
//! - Use `reqwest` with `Response::chunk()` for streaming (no extra deps).

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;
use tracing::debug;

use super::protocol::ChatMessage;

/// Connection settings for the upstream endpoint.
#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    /// E.g. "http://127.0.0.1:11434/v1" or "https://api.openai.com/v1".
    pub base_url: String,
    /// Bearer token; may be empty for local upstreams.
    pub api_key: String,
    /// Model used when a request doesn't specify one.
    pub model: Option<String>,
    /// Sampling temperature; omitted from the request when `None`.
    pub temperature: Option<f64>,
    /// Reasoning effort hint ("none"|"minimal"|"low"|"medium"|"high", a free
    /// string -- not validated here); omitted from the request when
    /// `None`. Mirrors the web apps' @tik-choco/mistai client behavior.
    pub reasoning_effort: Option<String>,
}

/// Builds a fresh client for one call. `no_proxy()` matters here: upstreams
/// are typically local (Ollama/llama.cpp on 127.0.0.1) and this machine may
/// have proxy env vars set that would otherwise break loopback requests.
fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .context("ai: building upstream HTTP client")
}

fn strip_trailing_slash(base_url: &str) -> &str {
    base_url.trim_end_matches('/')
}

/// First 500 *characters* (not bytes) of `body`, mirroring `.slice(0, 500)`
/// in the TypeScript original.
fn truncate_500(body: &str) -> String {
    body.chars().take(500).collect()
}

/// Stream one chat completion from the upstream. Each delta is sent into
/// `delta_tx` (when provided) as it arrives; the full accumulated content
/// is returned at the end.
pub async fn stream_chat_completion(
    config: &UpstreamConfig,
    messages: &[ChatMessage],
    model: Option<&str>,
    delta_tx: Option<UnboundedSender<String>>,
) -> Result<String> {
    let model = model
        .map(str::to_string)
        .or_else(|| config.model.clone())
        .ok_or_else(|| {
            anyhow!("ai: no model configured for upstream request (set a default model or pass one per-request)")
        })?;

    let url = format!(
        "{}/chat/completions",
        strip_trailing_slash(&config.base_url)
    );

    let mut body = json!({
        "model": model,
        "messages": messages,
        "stream": true,
    });
    if let Some(temperature) = config.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(reasoning_effort) = &config.reasoning_effort {
        body["reasoning_effort"] = json!(reasoning_effort);
    }

    let client = build_client()?;
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", config.api_key))
        .json(&body)
        .send()
        .await
        .with_context(|| format!("LLM API request failed: POST {url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body_text = response.text().await.unwrap_or_default();
        bail!(
            "LLM API returned an error ({status}): {}",
            truncate_500(&body_text)
        );
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if content_type.contains("text/event-stream") {
        return stream_sse(response, delta_tx).await;
    }

    let text = response
        .text()
        .await
        .context("ai: reading LLM API response body")?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|_| anyhow!("ai: LLM API returned a response with an unexpected format"))?;
    let content = value
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ai: LLM API returned a response with an unexpected format"))?
        .to_string();

    if let Some(tx) = &delta_tx {
        let _ = tx.send(content.clone());
    }

    Ok(content)
}

/// Consume an SSE (`text/event-stream`) response body, forwarding content
/// deltas into `delta_tx` and returning the accumulated full content.
async fn stream_sse(
    mut response: reqwest::Response,
    delta_tx: Option<UnboundedSender<String>>,
) -> Result<String> {
    let mut buffer = String::new();
    let mut full = String::new();

    while let Some(chunk) = response
        .chunk()
        .await
        .context("ai: reading LLM API stream")?
    {
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        let mut lines: Vec<String> = buffer.split('\n').map(str::to_string).collect();
        // The last element is either an empty string (buffer ended in a
        // full line) or a partial line to keep buffered for the next chunk.
        let remainder = lines.pop().unwrap_or_default();

        for line in lines {
            let trimmed = line.trim();
            let Some(data) = trimmed.strip_prefix("data:") else {
                continue; // No `data:` prefix -- skip (e.g. SSE comments).
            };
            let data = data.trim();
            if data == "[DONE]" {
                continue;
            }

            let Ok(parsed) = serde_json::from_str::<Value>(data) else {
                continue; // Malformed SSE line -- skip silently.
            };
            let delta = parsed
                .get("choices")
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("delta"))
                .and_then(|d| d.get("content"))
                .and_then(Value::as_str);
            if let Some(delta) = delta {
                if !delta.is_empty() {
                    full.push_str(delta);
                    if let Some(tx) = &delta_tx {
                        let _ = tx.send(delta.to_string());
                    }
                }
            }
        }

        buffer = remainder;
    }

    Ok(full)
}

/// `GET {base_url}/models` -> the model id list (OpenAI shape:
/// `{"data": [{"id": "..."}]}`). Non-string/empty ids are filtered out;
/// an empty final list is an error ("upstream returned no models").
pub async fn fetch_models(config: &UpstreamConfig) -> Result<Vec<String>> {
    let url = format!("{}/models", strip_trailing_slash(&config.base_url));

    let client = build_client()?;
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", config.api_key))
        .send()
        .await
        .with_context(|| format!("Failed to fetch the model list: GET {url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body_text = response.text().await.unwrap_or_default();
        bail!(
            "Model list request returned an error ({status}): {}",
            truncate_500(&body_text)
        );
    }

    let text = response
        .text()
        .await
        .context("ai: reading model list response body")?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|_| anyhow!("ai: model list response is not valid JSON"))?;

    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("ai: model list response had an unexpected format"))?;

    let ids: Vec<String> = data
        .iter()
        .filter_map(|entry| entry.get("id").and_then(Value::as_str))
        .filter(|id| !id.is_empty())
        .map(String::from)
        .collect();

    if ids.is_empty() {
        bail!("upstream returned no models");
    }

    Ok(ids)
}

/// Parses a voices response body, tolerating the same shapes as mistai's
/// `parseVoicesBody`: the body itself is the list, or it's wrapped as
/// `{"voices": [...]}` / `{"data": [...]}`; each entry is either a bare
/// string or an object with an `id`/`name`/`voice` string field (checked in
/// that order). Pure (no I/O) so shape variations can be unit-tested
/// directly. Empty/non-string ids are filtered out.
fn parse_voices_body(body: &Value) -> Vec<String> {
    let empty: Vec<Value> = Vec::new();
    let raw_list: &[Value] = body
        .as_array()
        .or_else(|| body.get("voices").and_then(Value::as_array))
        .or_else(|| body.get("data").and_then(Value::as_array))
        .unwrap_or(&empty);

    raw_list
        .iter()
        .filter_map(|entry| {
            if let Some(s) = entry.as_str() {
                return (!s.is_empty()).then(|| s.to_string());
            }
            ["id", "name", "voice"]
                .into_iter()
                .find_map(|key| entry.get(key).and_then(Value::as_str))
                .filter(|s| !s.is_empty())
                .map(String::from)
        })
        .collect()
}

/// Tries one candidate voices endpoint; returns `None` (never an error) so
/// [`fetch_voices`] can fall through to the next candidate on *any* failure
/// (network error, non-2xx, unparseable JSON, or a parseable-but-empty
/// catalog) -- mirrors mistai's `tryVoicesEndpoint`.
async fn try_voices_endpoint(url: &str, api_key: &str) -> Option<Vec<String>> {
    let client = build_client().ok()?;
    let mut request = client.get(url);
    if !api_key.is_empty() {
        request = request.header("Authorization", format!("Bearer {api_key}"));
    }
    let response = request.send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let text = response.text().await.ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    let voices = parse_voices_body(&value);
    (!voices.is_empty()).then_some(voices)
}

/// `GET {base_url}/audio/voices`, falling back to `GET {base_url}/voices` if
/// that doesn't return a usable (non-empty, parseable) list. Unlike
/// [`fetch_models`], this never errors: OpenAI's own API has no
/// voices-listing endpoint at all, so "can't determine the list" is a common,
/// expected outcome -- callers advertise nothing (or fall back to a preset's
/// single configured voice) rather than treat this as a hard failure.
/// `Authorization` is sent only when `api_key` is non-empty (mirrors
/// mistai's `fetchVoices`, unlike this module's other requests which always
/// send the header). Ported from `mistai/src/openai.ts`'s `fetchVoices`.
pub async fn fetch_voices(base_url: &str, api_key: &str) -> Vec<String> {
    let base = strip_trailing_slash(base_url);
    for path in ["/audio/voices", "/voices"] {
        let url = format!("{base}{path}");
        if let Some(voices) = try_voices_endpoint(&url, api_key).await {
            return voices;
        }
    }
    debug!(
        %base,
        "ai: no TTS voice catalog found upstream (both /audio/voices and /voices failed \
         or returned nothing usable) -- advertising none from this fetch"
    );
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::task::JoinHandle;

    /// Reads request headers, then exactly `Content-Length` body bytes (or
    /// nothing extra if absent/zero), returning the raw bytes received.
    async fn read_request(socket: &mut TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            let n = socket.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);

            if let Some(header_end) = find_subslice(&buf, b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buf[..header_end]);
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
                let body_len = buf.len() - (header_end + 4);
                if body_len >= content_length {
                    break;
                }
            }
        }
        buf
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    /// Splits a captured raw HTTP request into (header block, body text).
    fn split_request(raw: &[u8]) -> (String, String) {
        let idx = find_subslice(raw, b"\r\n\r\n").expect("request had no header terminator");
        let headers = String::from_utf8_lossy(&raw[..idx]).to_string();
        let body = String::from_utf8_lossy(&raw[idx + 4..]).to_string();
        (headers, body)
    }

    /// Spins up a one-shot mock HTTP server on 127.0.0.1: accepts a single
    /// connection, captures the raw request bytes, writes back `writes` (as
    /// separate TCP writes, `delay` apart -- used to force cross-chunk SSE
    /// delivery), then closes the connection. Returns the base URL and a
    /// handle that resolves to the captured request bytes.
    async fn mock_server(writes: Vec<Vec<u8>>, delay: Duration) -> (String, JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            for w in writes {
                socket.write_all(&w).await.unwrap();
                socket.flush().await.unwrap();
                if delay > Duration::ZERO {
                    tokio::time::sleep(delay).await;
                }
            }
            let _ = socket.shutdown().await;
            request
        });
        (format!("http://{addr}"), handle)
    }

    /// One real HTTP chunked-transfer response, `parts` each written as a
    /// separate chunk frame (and thus, with a delay, a separate TCP write).
    fn chunked_sse_response(parts: &[&str]) -> Vec<Vec<u8>> {
        let mut writes = vec![
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec(),
        ];
        for part in parts {
            writes.push(format!("{:x}\r\n{part}\r\n", part.len()).into_bytes());
        }
        writes.push(b"0\r\n\r\n".to_vec());
        writes
    }

    fn json_response(status: u16, reason: &str, body: &str) -> Vec<Vec<u8>> {
        let resp = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        vec![resp.into_bytes()]
    }

    fn text_response(status: u16, reason: &str, body: &str) -> Vec<Vec<u8>> {
        let resp = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        vec![resp.into_bytes()]
    }

    /// Like [`mock_server`] but accepts one connection per entry in
    /// `responses`, in order, closing each before accepting the next --
    /// needed to test [`fetch_voices`]'s two-endpoint fallback, which makes
    /// up to two sequential requests (potentially to the same host:port).
    /// Returns the raw request bytes captured for each accepted connection,
    /// in accept order.
    async fn mock_sequential_server(
        responses: Vec<Vec<Vec<u8>>>,
    ) -> (String, JoinHandle<Vec<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut captured = Vec::new();
            for writes in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                for w in writes {
                    socket.write_all(&w).await.unwrap();
                    socket.flush().await.unwrap();
                }
                let _ = socket.shutdown().await;
                captured.push(request);
            }
            captured
        });
        (format!("http://{addr}"), handle)
    }

    fn request_line(raw: &[u8]) -> String {
        split_request(raw)
            .0
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    fn cfg(base_url: String, model: Option<&str>, temperature: Option<f64>) -> UpstreamConfig {
        UpstreamConfig {
            base_url,
            api_key: "test-key-123".to_string(),
            model: model.map(String::from),
            temperature,
            reasoning_effort: None,
        }
    }

    fn one_message() -> Vec<ChatMessage> {
        vec![ChatMessage {
            role: "user".to_string(),
            content: "hi".to_string(),
        }]
    }

    #[tokio::test]
    async fn sse_deltas_split_across_tcp_writes_mid_line() {
        // "Hello" is split into "Hel" + "lo" across two chunk frames/writes,
        // exercising the cross-chunk line buffer. Also includes a second
        // full delta, an SSE comment (no `data:` prefix), a malformed JSON
        // data line, and a trailing [DONE].
        let seg_a = r#"data: {"choices":[{"delta":{"content":"Hel"#;
        let seg_b = "lo\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n: keep-alive\n\ndata: not-json\n\ndata: [DONE]\n\n";

        let (base_url, server) = mock_server(
            chunked_sse_response(&[seg_a, seg_b]),
            Duration::from_millis(15),
        )
        .await;

        let (tx, mut rx) = unbounded_channel();
        let config = cfg(base_url, Some("gpt-test"), None);
        let result = stream_chat_completion(&config, &one_message(), None, Some(tx))
            .await
            .expect("stream should succeed");

        assert_eq!(result, "Hello world");

        let mut deltas = Vec::new();
        while let Ok(d) = rx.try_recv() {
            deltas.push(d);
        }
        assert_eq!(deltas, vec!["Hello".to_string(), " world".to_string()]);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn sse_keepalive_and_malformed_lines_ignored() {
        let body = "data: not-json-at-all\n\n: this is a keepalive comment\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n";
        let (base_url, server) = mock_server(chunked_sse_response(&[body]), Duration::ZERO).await;

        let config = cfg(base_url, Some("gpt-test"), None);
        let result = stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect("stream should succeed");

        assert_eq!(result, "ok");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn non_sse_json_fallback_is_parsed_once() {
        let body = r#"{"choices":[{"message":{"content":"hi there"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let (tx, mut rx) = unbounded_channel();
        let config = cfg(base_url, Some("gpt-test"), None);
        let result = stream_chat_completion(&config, &one_message(), None, Some(tx))
            .await
            .expect("fallback parse should succeed");

        assert_eq!(result, "hi there");
        assert_eq!(rx.try_recv().unwrap(), "hi there");
        assert!(rx.try_recv().is_err(), "delta should be sent exactly once");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn non_2xx_error_includes_status_and_truncated_body() {
        let long_body: String = "x".repeat(600);
        let (base_url, server) = mock_server(
            text_response(500, "Internal Server Error", &long_body),
            Duration::ZERO,
        )
        .await;

        let config = cfg(base_url, Some("gpt-test"), None);
        let err = stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect_err("non-2xx should error");

        let msg = err.to_string();
        assert!(msg.contains("500"), "error should mention status: {msg}");
        let x_count = msg.chars().filter(|c| *c == 'x').count();
        assert_eq!(
            x_count, 500,
            "body snippet should be truncated to 500 chars"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn missing_model_errors_before_any_request_is_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let notify2 = notify.clone();
        let server = tokio::spawn(async move {
            let _ = listener.accept().await;
            notify2.notify_one();
        });

        let config = cfg(format!("http://{addr}"), None, None);
        let err = stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect_err("missing model should error");
        assert!(
            err.to_string().to_lowercase().contains("model"),
            "error should mention the missing model: {err}"
        );

        let was_contacted = tokio::time::timeout(Duration::from_millis(150), notify.notified())
            .await
            .is_ok();
        assert!(
            !was_contacted,
            "no request should be sent when no model is configured"
        );

        server.abort();
    }

    #[tokio::test]
    async fn request_body_and_headers_include_every_configured_option() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let mut config = cfg(base_url, Some("config-model"), Some(0.75));
        config.reasoning_effort = Some("high".to_string());
        stream_chat_completion(&config, &one_message(), Some("override-model"), None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (headers, req_body) = split_request(&raw);
        let value: Value = serde_json::from_str(&req_body).expect("request body should be JSON");
        assert_eq!(
            value["model"],
            json!("override-model"),
            "per-request model should override the config model"
        );
        assert_eq!(value["temperature"], json!(0.75));
        assert_eq!(value["reasoning_effort"], json!("high"));
        assert_eq!(value["stream"], json!(true));
        assert!(
            headers
                .to_lowercase()
                .contains("authorization: bearer test-key-123"),
            "expected Authorization header, got: {headers}"
        );
    }

    #[tokio::test]
    async fn request_body_omits_unset_options_and_still_sends_authorization() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let mut config = cfg(base_url, Some("config-model"), None);
        config.api_key = String::new();
        stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (headers, req_body) = split_request(&raw);
        let value: Value = serde_json::from_str(&req_body).expect("request body should be JSON");
        assert_eq!(value["model"], json!("config-model"));
        assert!(
            value.get("temperature").is_none(),
            "temperature should be omitted: {value}"
        );
        assert!(
            value.get("reasoning_effort").is_none(),
            "reasoning_effort should be omitted: {value}"
        );
        assert!(
            headers.to_lowercase().contains("authorization: bearer"),
            "Authorization header should be sent even with an empty key: {headers}"
        );
    }

    #[tokio::test]
    async fn fetch_models_happy_path_filters_invalid_ids() {
        let body =
            r#"{"data":[{"id":"gpt-4"},{"id":""},{"id":123},{"notid":"x"},{"id":"gpt-3.5"}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, None, None);
        let models = fetch_models(&config)
            .await
            .expect("fetch_models should succeed");
        assert_eq!(models, vec!["gpt-4".to_string(), "gpt-3.5".to_string()]);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_models_empty_list_is_an_error() {
        let body = r#"{"data":[{"id":""},{"id":123}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, None, None);
        let err = fetch_models(&config)
            .await
            .expect_err("empty model list should error");
        assert!(err.to_string().contains("upstream returned no models"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_models_non_2xx_is_an_error() {
        let (base_url, server) = mock_server(
            text_response(503, "Service Unavailable", "down"),
            Duration::ZERO,
        )
        .await;

        let config = cfg(base_url, None, None);
        let err = fetch_models(&config)
            .await
            .expect_err("non-2xx should error");
        assert!(err.to_string().contains("503"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn base_url_trailing_slash_is_stripped() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(format!("{base_url}///"), Some("gpt-test"), None);
        stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (headers, _body) = split_request(&raw);
        let request_line = headers.lines().next().unwrap_or_default();
        assert!(
            request_line.contains("/chat/completions") && !request_line.contains("//chat"),
            "unexpected request line: {request_line}"
        );
    }

    // -- parse_voices_body: pure shape-variation tests -----------------

    #[test]
    fn parse_voices_body_accepts_bare_array_of_strings() {
        let body: Value = serde_json::from_str(r#"["alloy","echo"]"#).unwrap();
        assert_eq!(
            parse_voices_body(&body),
            vec!["alloy".to_string(), "echo".to_string()]
        );
    }

    #[test]
    fn parse_voices_body_accepts_voices_wrapper() {
        let body: Value = serde_json::from_str(r#"{"voices":["alloy","echo"]}"#).unwrap();
        assert_eq!(
            parse_voices_body(&body),
            vec!["alloy".to_string(), "echo".to_string()]
        );
    }

    #[test]
    fn parse_voices_body_accepts_data_wrapper() {
        let body: Value = serde_json::from_str(r#"{"data":["alloy","echo"]}"#).unwrap();
        assert_eq!(
            parse_voices_body(&body),
            vec!["alloy".to_string(), "echo".to_string()]
        );
    }

    #[test]
    fn parse_voices_body_prefers_id_then_name_then_voice_on_object_entries() {
        let body: Value = serde_json::from_str(
            r#"[{"id":"v-id"},{"name":"v-name"},{"voice":"v-voice"},{"id":"i","name":"n"}]"#,
        )
        .unwrap();
        assert_eq!(
            parse_voices_body(&body),
            vec![
                "v-id".to_string(),
                "v-name".to_string(),
                "v-voice".to_string(),
                "i".to_string()
            ]
        );
    }

    #[test]
    fn parse_voices_body_filters_empty_and_unrecognized_entries() {
        let body: Value =
            serde_json::from_str(r#"["alloy","",{"notavoice":1},42,null,{"id":""}]"#).unwrap();
        assert_eq!(parse_voices_body(&body), vec!["alloy".to_string()]);
    }

    #[test]
    fn parse_voices_body_returns_empty_for_unrecognized_shapes() {
        assert_eq!(
            parse_voices_body(&json!({"unexpected": "shape"})),
            Vec::<String>::new()
        );
        assert_eq!(parse_voices_body(&json!(null)), Vec::<String>::new());
        assert_eq!(
            parse_voices_body(&json!("not-a-list")),
            Vec::<String>::new()
        );
    }

    // -- fetch_voices: endpoint selection / fallback / never-errors ----

    fn voices_ok_body(voices: &str) -> Vec<Vec<u8>> {
        json_response(200, "OK", &format!(r#"{{"voices":{voices}}}"#))
    }

    #[tokio::test]
    async fn fetch_voices_uses_audio_voices_when_it_succeeds() {
        let (base_url, server) =
            mock_server(voices_ok_body(r#"["alloy","echo"]"#), Duration::ZERO).await;

        let voices = fetch_voices(&base_url, "key").await;
        assert_eq!(voices, vec!["alloy".to_string(), "echo".to_string()]);

        let raw = server.await.unwrap();
        assert!(
            request_line(&raw).contains("/audio/voices"),
            "expected the /audio/voices endpoint to be tried first: {}",
            request_line(&raw)
        );
    }

    #[tokio::test]
    async fn fetch_voices_falls_back_to_plain_voices_when_audio_voices_fails() {
        let responses = vec![
            text_response(404, "Not Found", "no such route"),
            voices_ok_body(r#"["nova"]"#),
        ];
        let (base_url, server) = mock_sequential_server(responses).await;

        let voices = fetch_voices(&base_url, "key").await;
        assert_eq!(voices, vec!["nova".to_string()]);

        let raw = server.await.unwrap();
        assert_eq!(
            raw.len(),
            2,
            "both candidate endpoints should have been tried"
        );
        assert!(request_line(&raw[0]).contains("/audio/voices"));
        assert!(
            request_line(&raw[1]).contains("/voices")
                && !request_line(&raw[1]).contains("/audio/voices"),
            "unexpected second request line: {}",
            request_line(&raw[1])
        );
    }

    #[tokio::test]
    async fn fetch_voices_falls_back_when_audio_voices_parses_but_is_empty() {
        // A 200 response with a parseable-but-empty catalog must still fall
        // through to the next candidate, not be treated as "found".
        let responses = vec![voices_ok_body("[]"), voices_ok_body(r#"["nova"]"#)];
        let (base_url, server) = mock_sequential_server(responses).await;

        let voices = fetch_voices(&base_url, "key").await;
        assert_eq!(voices, vec!["nova".to_string()]);

        let raw = server.await.unwrap();
        assert_eq!(raw.len(), 2);
    }

    #[tokio::test]
    async fn fetch_voices_returns_empty_without_erroring_when_both_endpoints_fail() {
        let responses = vec![
            text_response(404, "Not Found", "nope"),
            text_response(500, "Internal Server Error", "nope"),
        ];
        let (base_url, server) = mock_sequential_server(responses).await;

        let voices = fetch_voices(&base_url, "key").await;
        assert_eq!(voices, Vec::<String>::new());

        let raw = server.await.unwrap();
        assert_eq!(
            raw.len(),
            2,
            "both candidate endpoints should have been tried"
        );
    }

    #[tokio::test]
    async fn fetch_voices_returns_empty_for_unreachable_host_without_erroring() {
        // Nothing listening on this port: connection should fail fast and
        // fetch_voices must still resolve to `[]` rather than propagating
        // an error (its whole contract is "never throws").
        let voices = fetch_voices("http://127.0.0.1:1", "key").await;
        assert_eq!(voices, Vec::<String>::new());
    }

    #[tokio::test]
    async fn fetch_voices_sends_authorization_only_when_api_key_is_non_empty() {
        let (base_url, server) = mock_server(voices_ok_body(r#"["alloy"]"#), Duration::ZERO).await;
        fetch_voices(&base_url, "sk-test-voices").await;
        let raw = server.await.unwrap();
        let (headers, _body) = split_request(&raw);
        assert!(
            headers
                .to_lowercase()
                .contains("authorization: bearer sk-test-voices"),
            "expected Authorization header, got: {headers}"
        );

        let (base_url, server) = mock_server(voices_ok_body(r#"["alloy"]"#), Duration::ZERO).await;
        fetch_voices(&base_url, "").await;
        let raw = server.await.unwrap();
        let (headers, _body) = split_request(&raw);
        assert!(
            !headers.to_lowercase().contains("authorization"),
            "no Authorization header should be sent with an empty api_key: {headers}"
        );
    }

    #[tokio::test]
    async fn fetch_voices_strips_trailing_slash_from_base_url() {
        let (base_url, server) = mock_server(voices_ok_body(r#"["alloy"]"#), Duration::ZERO).await;
        fetch_voices(&format!("{base_url}///"), "key").await;
        let raw = server.await.unwrap();
        assert!(
            request_line(&raw).contains("/audio/voices") && !request_line(&raw).contains("///"),
            "unexpected request line: {}",
            request_line(&raw)
        );
    }
}
