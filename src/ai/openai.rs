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

    let url = format!("{}/chat/completions", strip_trailing_slash(&config.base_url));

    let mut body = json!({
        "model": model,
        "messages": messages,
        "stream": true,
    });
    if let Some(temperature) = config.temperature {
        body["temperature"] = json!(temperature);
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

    let text = response.text().await.context("ai: reading LLM API response body")?;
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

    fn cfg(base_url: String, model: Option<&str>, temperature: Option<f64>) -> UpstreamConfig {
        UpstreamConfig {
            base_url,
            api_key: "test-key-123".to_string(),
            model: model.map(String::from),
            temperature,
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

        let (base_url, server) =
            mock_server(chunked_sse_response(&[seg_a, seg_b]), Duration::from_millis(15)).await;

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
        let (base_url, server) =
            mock_server(chunked_sse_response(&[body]), Duration::ZERO).await;

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
        let (base_url, server) =
            mock_server(text_response(500, "Internal Server Error", &long_body), Duration::ZERO)
                .await;

        let config = cfg(base_url, Some("gpt-test"), None);
        let err = stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect_err("non-2xx should error");

        let msg = err.to_string();
        assert!(msg.contains("500"), "error should mention status: {msg}");
        let x_count = msg.chars().filter(|c| *c == 'x').count();
        assert_eq!(x_count, 500, "body snippet should be truncated to 500 chars");

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
        assert!(!was_contacted, "no request should be sent when no model is configured");

        server.abort();
    }

    #[tokio::test]
    async fn temperature_included_when_configured() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, Some("gpt-test"), Some(0.75));
        stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (_headers, req_body) = split_request(&raw);
        let value: Value = serde_json::from_str(&req_body).expect("request body should be JSON");
        assert_eq!(value["temperature"], json!(0.75));
        assert_eq!(value["model"], json!("gpt-test"));
        assert_eq!(value["stream"], json!(true));
    }

    #[tokio::test]
    async fn temperature_omitted_when_not_configured() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, Some("gpt-test"), None);
        stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (_headers, req_body) = split_request(&raw);
        let value: Value = serde_json::from_str(&req_body).expect("request body should be JSON");
        assert!(
            value.get("temperature").is_none(),
            "temperature should be omitted: {value}"
        );
    }

    #[tokio::test]
    async fn per_request_model_overrides_config_model() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, Some("config-model"), None);
        stream_chat_completion(&config, &one_message(), Some("override-model"), None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (_headers, req_body) = split_request(&raw);
        let value: Value = serde_json::from_str(&req_body).unwrap();
        assert_eq!(value["model"], json!("override-model"));
    }

    #[tokio::test]
    async fn authorization_header_is_always_sent() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, Some("gpt-test"), None);
        stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (headers, _body) = split_request(&raw);
        assert!(
            headers
                .to_lowercase()
                .contains("authorization: bearer test-key-123"),
            "expected Authorization header, got: {headers}"
        );
    }

    #[tokio::test]
    async fn authorization_header_sent_even_with_empty_key() {
        let body = r#"{"choices":[{"message":{"content":"ok"}}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let mut config = cfg(base_url, Some("gpt-test"), None);
        config.api_key = String::new();
        stream_chat_completion(&config, &one_message(), None, None)
            .await
            .expect("request should succeed");

        let raw = server.await.unwrap();
        let (headers, _body) = split_request(&raw);
        assert!(
            headers.to_lowercase().contains("authorization: bearer"),
            "Authorization header should be sent even with an empty key: {headers}"
        );
    }

    #[tokio::test]
    async fn fetch_models_happy_path_filters_invalid_ids() {
        let body = r#"{"data":[{"id":"gpt-4"},{"id":""},{"id":123},{"notid":"x"},{"id":"gpt-3.5"}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, None, None);
        let models = fetch_models(&config).await.expect("fetch_models should succeed");
        assert_eq!(models, vec!["gpt-4".to_string(), "gpt-3.5".to_string()]);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_models_empty_list_is_an_error() {
        let body = r#"{"data":[{"id":""},{"id":123}]}"#;
        let (base_url, server) = mock_server(json_response(200, "OK", body), Duration::ZERO).await;

        let config = cfg(base_url, None, None);
        let err = fetch_models(&config).await.expect_err("empty model list should error");
        assert!(err.to_string().contains("upstream returned no models"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_models_non_2xx_is_an_error() {
        let (base_url, server) =
            mock_server(text_response(503, "Service Unavailable", "down"), Duration::ZERO).await;

        let config = cfg(base_url, None, None);
        let err = fetch_models(&config).await.expect_err("non-2xx should error");
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
}
