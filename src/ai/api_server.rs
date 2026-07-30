//! Local OpenAI-compatible HTTP API server ("use the network as if it were
//! an API server"): apps point their OpenAI base URL at this listener and
//! requests are answered by the p2p network (or the local provider).
//!
//! Hand-rolled minimal HTTP/1.1 over tokio TCP -- same spirit as the
//! project's RTSP and IPC servers; no hyper/axum. Loopback-oriented, no
//! auth (any `Authorization` header is accepted and ignored).
//!
//! ## Endpoints
//!
//! - `GET /v1/models` -> `{"object":"list","data":[{"id":"<m>","object":"model","owned_by":"mistl"}]}`
//!   from the injected models fn.
//! - `POST /v1/chat/completions` -> body `{model?, messages, stream?}`.
//!   `messages` entries must have string `role`/`content` (400 otherwise).
//!   Calls the injected [`super::LlmCallFn`].
//!   - `stream: false`/absent: respond `200` JSON (OpenAI chat.completion
//!     shape): `{"id","object":"chat.completion","created",<unix secs>,
//!     "model","choices":[{"index":0,"message":{"role":"assistant",
//!     "content":<full>},"finish_reason":"stop"}]}`.
//!   - `stream: true`: respond `200` with `Content-Type: text/event-stream`
//!     and `Transfer-Encoding: chunked`; emit one
//!     `data: {"id","object":"chat.completion.chunk","created","model",
//!     "choices":[{"index":0,"delta":{"content":<delta>},
//!     "finish_reason":null}]}\n\n` per delta (drain the delta channel
//!     while the call future runs), then a final chunk with empty delta
//!     and `"finish_reason":"stop"`, then `data: [DONE]\n\n`, then the
//!     terminating zero-length HTTP chunk.
//! - Upstream/backend errors: before any bytes were streamed -> `502` with
//!   `{"error":{"message":...}}` (or `400` for malformed requests); once
//!   streaming has begun, log and terminate the stream.
//! - Anything else -> `404`.
//!
//! ## HTTP parsing (keep it minimal but correct)
//!
//! Read the request line + headers (case-insensitive names) up to a sane
//! cap, then exactly `Content-Length` body bytes (411 if missing on POST;
//! 413 over 10 MiB). `Connection: close` semantics -- serve one request
//! per connection, then close. Tolerate both `\r\n` and `\n`.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use super::protocol::ChatMessage;
use super::{LlmCallFn, ModelsFn};

/// Header section size cap (request-line + headers), matches doc.
const MAX_HEADER_BYTES: usize = 16 * 1024;
/// Body size cap.
const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// A running API server; dropping/stopping aborts the accept loop and all
/// connection tasks.
pub struct ApiServer {
    addr: SocketAddr,
    accept_handle: JoinHandle<()>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl ApiServer {
    /// Bind `listen` (e.g. "127.0.0.1:6478") and start serving; resolves
    /// once the socket is listening.
    pub async fn start(listen: &str, call: LlmCallFn, models: ModelsFn) -> Result<Arc<ApiServer>> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding api server to {listen}"))?;
        let addr = listener
            .local_addr()
            .context("reading bound api server address")?;

        let conns: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let conns_for_loop = conns.clone();

        let accept_handle = tokio::spawn(async move {
            loop {
                let (socket, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(error) => {
                        warn!(%error, "api_server accept failed");
                        continue;
                    }
                };
                let call = call.clone();
                let models = models.clone();
                let handle = tokio::spawn(async move {
                    if let Err(error) = handle_connection(socket, call, models).await {
                        warn!(%error, "api_server connection ended with error");
                    }
                });

                let mut guard = conns_for_loop.lock().expect("conns mutex poisoned");
                guard.retain(|h| !h.is_finished());
                guard.push(handle);
            }
        });

        Ok(Arc::new(ApiServer {
            addr,
            accept_handle,
            conns,
        }))
    }

    /// The actually-bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop accepting and tear down existing connections.
    pub fn stop(&self) {
        self.accept_handle.abort();
        let guard = self.conns.lock().expect("conns mutex poisoned");
        for handle in guard.iter() {
            handle.abort();
        }
    }
}

/// Parsed request line + headers (header names lower-cased for
/// case-insensitive lookup).
struct RequestHead {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

impl RequestHead {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Find the end of the header section (index just past the blank-line
/// terminator), tolerating both `\r\n\r\n` and bare `\n\n`.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
        return Some(pos + 4);
    }
    if let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
        return Some(pos + 2);
    }
    None
}

/// Parse the request line + header lines out of the (already delimited)
/// header section bytes.
fn parse_head(bytes: &[u8]) -> Option<RequestHead> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.split('\n');

    let request_line = lines.next()?.trim_end_matches('\r');
    let mut parts = request_line.split(' ').filter(|s| !s.is_empty());
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    // HTTP version is ignored; a request-line without it is still accepted.

    let mut headers = Vec::new();
    for line in lines {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if let Some(idx) = line.find(':') {
            let key = line[..idx].trim().to_ascii_lowercase();
            let value = line[idx + 1..].trim().to_string();
            headers.push((key, value));
        }
    }

    Some(RequestHead {
        method,
        path,
        headers,
    })
}

/// Read bytes from `stream` until the header terminator is found (capped at
/// [`MAX_HEADER_BYTES`]). Returns the parsed head plus any body bytes that
/// were already read past the terminator. `Ok(None)` means the peer closed
/// the connection before sending anything (not an error).
async fn read_request_head(stream: &mut TcpStream) -> io::Result<Option<(RequestHead, Vec<u8>)>> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(pos) = find_header_end(&buf) {
            let head = parse_head(&buf[..pos]).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "malformed request head")
            })?;
            let leftover = buf[pos..].to_vec();
            return Ok(Some((head, leftover)));
        }
        if buf.len() >= MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header section too large",
            ));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Read the remaining body bytes (on top of any `leftover` already read
/// during header parsing) until exactly `content_length` bytes are
/// available.
async fn read_body(
    stream: &mut TcpStream,
    mut leftover: Vec<u8>,
    content_length: usize,
) -> io::Result<Vec<u8>> {
    if leftover.len() >= content_length {
        leftover.truncate(content_length);
        return Ok(leftover);
    }
    let mut chunk = [0u8; 8192];
    while leftover.len() < content_length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before body complete",
            ));
        }
        let take = n.min(content_length - leftover.len());
        leftover.extend_from_slice(&chunk[..take]);
    }
    Ok(leftover)
}

async fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &Value,
) -> io::Result<()> {
    let body_bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body_bytes.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body_bytes).await?;
    stream.flush().await
}

async fn write_error(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    message: &str,
) -> io::Result<()> {
    write_json_response(
        stream,
        status,
        reason,
        &json!({"error": {"message": message}}),
    )
    .await
}

/// Write one HTTP chunked-transfer frame (`payload` may be empty, which
/// produces the terminating `0\r\n\r\n` frame) and flush.
async fn write_http_chunk(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    let header = format!("{:x}\r\n", payload.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(payload).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Random-ish id, independent of `protocol::random_id` (kept self-contained
/// here so this module never calls into the sibling `protocol` stubs).
fn gen_id(prefix: &str) -> String {
    let mut bytes = [0u8; 12];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("{prefix}-{hex}")
}

fn models_response(models: &ModelsFn) -> Value {
    let list = models();
    let data: Vec<Value> = list
        .into_iter()
        .map(|id| json!({"id": id, "object": "model", "owned_by": "mistl"}))
        .collect();
    json!({"object": "list", "data": data})
}

/// Request body shape for `POST /v1/chat/completions`. Reuses
/// [`ChatMessage`]'s derive so malformed `role`/`content` fields (wrong
/// type or missing) simply fail to deserialize -> 400.
#[derive(serde::Deserialize)]
struct ChatRequestBody {
    #[serde(default)]
    model: Option<String>,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    stream: bool,
}

async fn handle_connection(mut stream: TcpStream, call: LlmCallFn, models: ModelsFn) -> Result<()> {
    let (head, leftover) = match read_request_head(&mut stream).await {
        Ok(Some(pair)) => pair,
        Ok(None) => return Ok(()),
        Err(_) => {
            let _ = write_error(&mut stream, 400, "Bad Request", "malformed request").await;
            return Ok(());
        }
    };

    let is_post = head.method.eq_ignore_ascii_case("POST");
    let is_get = head.method.eq_ignore_ascii_case("GET");
    let path = head.path.split('?').next().unwrap_or("").to_string();

    let body: Vec<u8> = if is_post {
        let content_length: usize = match head.header("content-length") {
            Some(v) => match v.trim().parse() {
                Ok(n) => n,
                Err(_) => {
                    let _ = write_error(&mut stream, 400, "Bad Request", "invalid Content-Length")
                        .await;
                    return Ok(());
                }
            },
            None => {
                let _ = write_error(
                    &mut stream,
                    411,
                    "Length Required",
                    "Content-Length required",
                )
                .await;
                return Ok(());
            }
        };
        if content_length > MAX_BODY_BYTES {
            let _ = write_error(
                &mut stream,
                413,
                "Payload Too Large",
                "request body too large",
            )
            .await;
            return Ok(());
        }
        match read_body(&mut stream, leftover, content_length).await {
            Ok(b) => b,
            Err(_) => {
                let _ =
                    write_error(&mut stream, 400, "Bad Request", "incomplete request body").await;
                return Ok(());
            }
        }
    } else {
        Vec::new()
    };

    if is_get && path == "/v1/models" {
        let body = models_response(&models);
        let _ = write_json_response(&mut stream, 200, "OK", &body).await;
    } else if is_post && path == "/v1/chat/completions" {
        handle_chat_completions(&mut stream, &body, &call).await?;
    } else {
        let _ = write_error(&mut stream, 404, "Not Found", "not found").await;
    }

    Ok(())
}

async fn handle_chat_completions(
    stream: &mut TcpStream,
    body: &[u8],
    call: &LlmCallFn,
) -> Result<()> {
    let req: ChatRequestBody = match serde_json::from_slice(body) {
        Ok(req) => req,
        Err(error) => {
            let _ = write_error(
                stream,
                400,
                "Bad Request",
                &format!("invalid request body: {error}"),
            )
            .await;
            return Ok(());
        }
    };

    let resp_model = req.model.clone().unwrap_or_else(|| "mistl".to_string());
    let id = gen_id("chatcmpl");
    let created = unix_now();

    if !req.stream {
        let fut = (call)(req.messages, req.model, None);
        return match fut.await {
            Ok(content) => {
                let resp = json!({
                    "id": id,
                    "object": "chat.completion",
                    "created": created,
                    "model": resp_model,
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop",
                    }],
                });
                write_json_response(stream, 200, "OK", &resp).await?;
                Ok(())
            }
            Err(error) => {
                let _ = write_error(stream, 502, "Bad Gateway", &format!("{error:#}")).await;
                Ok(())
            }
        };
    }

    // Streaming path: run the call future concurrently with draining its
    // delta channel, writing one SSE event per delta as an HTTP chunk.
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let mut call_fut = Box::pin((call)(req.messages, req.model, Some(tx)));

    let header = "HTTP/1.1 200 OK\r\n\
                  Content-Type: text/event-stream\r\n\
                  Transfer-Encoding: chunked\r\n\
                  Cache-Control: no-cache\r\n\
                  Connection: close\r\n\r\n";
    stream.write_all(header.as_bytes()).await?;
    stream.flush().await?;

    let mut done: Option<Result<String>> = None;
    let mut rx_closed = false;

    loop {
        if done.is_some() && rx_closed {
            break;
        }
        tokio::select! {
            maybe_delta = rx.recv(), if !rx_closed => {
                match maybe_delta {
                    Some(delta) => {
                        let chunk = json!({
                            "id": id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": resp_model,
                            "choices": [{
                                "index": 0,
                                "delta": {"content": delta},
                                "finish_reason": null,
                            }],
                        });
                        let event = format!("data: {chunk}\n\n");
                        write_http_chunk(stream, event.as_bytes()).await?;
                    }
                    None => rx_closed = true,
                }
            }
            result = &mut call_fut, if done.is_none() => {
                done = Some(result);
            }
        }
    }

    match done.expect("loop only exits once the call future has resolved") {
        Ok(_content) => {
            let final_chunk = json!({
                "id": id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": resp_model,
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop",
                }],
            });
            let event = format!("data: {final_chunk}\n\n");
            write_http_chunk(stream, event.as_bytes()).await?;
            write_http_chunk(stream, b"data: [DONE]\n\n").await?;
            write_http_chunk(stream, b"").await?;
        }
        Err(error) => {
            warn!(%error, "api_server: backend error mid-stream, closing connection");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;

    fn fake_call_ok() -> LlmCallFn {
        Arc::new(|_messages, _model, delta_tx| {
            Box::pin(async move {
                if let Some(tx) = delta_tx {
                    let _ = tx.send("Hel".to_string());
                    let _ = tx.send("lo".to_string());
                }
                Ok("Hello".to_string())
            })
        })
    }

    fn fake_call_err() -> LlmCallFn {
        Arc::new(|_messages, _model, _delta_tx| {
            Box::pin(async move { Err(anyhow::anyhow!("upstream exploded")) })
        })
    }

    fn fake_models() -> ModelsFn {
        Arc::new(|| vec!["m1".to_string(), "m2".to_string()])
    }

    async fn start_test_server(call: LlmCallFn, models: ModelsFn) -> Arc<ApiServer> {
        ApiServer::start("127.0.0.1:0", call, models)
            .await
            .expect("server starts")
    }

    /// Send `request` on a fresh connection to `addr` and read the full
    /// response until the peer closes the socket.
    async fn send_request(addr: SocketAddr, request: &str) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        response
    }

    fn split_response(raw: &[u8]) -> (String, &[u8]) {
        let pos = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("response should have a header/body separator");
        let head = std::str::from_utf8(&raw[..pos])
            .expect("head is utf8")
            .to_string();
        (head, &raw[pos + 4..])
    }

    fn status_code(head: &str) -> u16 {
        head.lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap()
    }

    /// Undo HTTP chunked transfer-encoding framing, returning the
    /// concatenated payload bytes.
    fn de_chunk(mut body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let line_end = body
                .windows(2)
                .position(|w| w == b"\r\n")
                .expect("chunk size line");
            let size_str = std::str::from_utf8(&body[..line_end]).unwrap().trim();
            let size = usize::from_str_radix(size_str, 16).expect("hex chunk size");
            body = &body[line_end + 2..];
            if size == 0 {
                break;
            }
            out.extend_from_slice(&body[..size]);
            body = &body[size..];
            assert_eq!(&body[..2], b"\r\n", "chunk must end with CRLF");
            body = &body[2..];
        }
        out
    }

    #[tokio::test]
    async fn get_models_returns_expected_shape() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let raw = send_request(server.addr(), "GET /v1/models HTTP/1.1\r\nHost: x\r\n\r\n").await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        let json: Value = serde_json::from_slice(body).expect("valid json");
        assert_eq!(json["object"], "list");
        let ids: Vec<&str> = json["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["m1", "m2"]);
        assert_eq!(json["data"][0]["object"], "model");
        assert_eq!(json["data"][0]["owned_by"], "mistl");
    }

    #[tokio::test]
    async fn post_chat_completions_non_stream_happy_path() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload =
            json!({"messages": [{"role": "user", "content": "hi"}], "stream": false}).to_string();
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        let raw = send_request(server.addr(), &request).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        let json: Value = serde_json::from_slice(body).expect("valid json");
        assert_eq!(json["object"], "chat.completion");
        assert_eq!(json["model"], "mistl");
        assert_eq!(json["choices"][0]["message"]["content"], "Hello");
        assert_eq!(json["choices"][0]["message"]["role"], "assistant");
        assert_eq!(json["choices"][0]["finish_reason"], "stop");
    }

    #[tokio::test]
    async fn post_chat_completions_stream_emits_sse_events() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = json!({"model": "gpt-x", "messages": [{"role": "user", "content": "hi"}], "stream": true}).to_string();
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        let raw = send_request(server.addr(), &request).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        assert!(
            head.to_ascii_lowercase()
                .contains("transfer-encoding: chunked")
        );
        assert!(head.to_ascii_lowercase().contains("text/event-stream"));

        let payload_bytes = de_chunk(body);
        let text = String::from_utf8(payload_bytes).expect("sse payload is utf8");
        let events: Vec<&str> = text.split("\n\n").filter(|s| !s.is_empty()).collect();

        let deltas: Vec<String> = events
            .iter()
            .filter_map(|e| e.strip_prefix("data: "))
            .filter_map(|d| if d == "[DONE]" { None } else { Some(d) })
            .filter_map(|d| serde_json::from_str::<Value>(d).ok())
            .filter_map(|v| {
                v["choices"][0]["delta"]["content"]
                    .as_str()
                    .map(|s| s.to_string())
            })
            .collect();
        assert_eq!(deltas, vec!["Hel".to_string(), "lo".to_string()]);

        // A stop chunk (empty delta, finish_reason "stop") is present.
        let has_stop_chunk = events.iter().any(|e| {
            e.strip_prefix("data: ")
                .and_then(|d| serde_json::from_str::<Value>(d).ok())
                .map(|v| v["choices"][0]["finish_reason"] == "stop")
                .unwrap_or(false)
        });
        assert!(
            has_stop_chunk,
            "expected a finish_reason=stop chunk, got: {events:?}"
        );

        assert!(
            events.iter().any(|e| *e == "data: [DONE]"),
            "expected [DONE] event, got: {events:?}"
        );
    }

    #[tokio::test]
    async fn post_malformed_json_is_400() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = "not json";
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        let raw = send_request(server.addr(), &request).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 400);
        let json: Value = serde_json::from_slice(body).expect("valid json");
        assert!(json["error"]["message"].is_string());
    }

    #[tokio::test]
    async fn post_bad_messages_shape_is_400() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = json!({"messages": [{"role": "user", "content": 42}]}).to_string();
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        let raw = send_request(server.addr(), &request).await;
        let (head, _body) = split_response(&raw);
        assert_eq!(status_code(&head), 400);
    }

    #[tokio::test]
    async fn unknown_path_is_404() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let raw = send_request(server.addr(), "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
        let (head, _body) = split_response(&raw);
        assert_eq!(status_code(&head), 404);
    }

    #[tokio::test]
    async fn post_backend_error_non_stream_is_502() {
        let server = start_test_server(fake_call_err(), fake_models()).await;
        let payload = json!({"messages": [{"role": "user", "content": "hi"}]}).to_string();
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        let raw = send_request(server.addr(), &request).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 502);
        let json: Value = serde_json::from_slice(body).expect("valid json");
        assert!(json["error"]["message"].is_string());
    }

    #[tokio::test]
    async fn post_without_content_length_is_411() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let raw = send_request(
            server.addr(),
            "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\n\r\n",
        )
        .await;
        let (head, _body) = split_response(&raw);
        assert_eq!(status_code(&head), 411);
    }

    #[tokio::test]
    async fn stop_closes_listener_so_new_connections_fail() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let addr = server.addr();

        // Sanity: works before stop.
        let raw = send_request(addr, "GET /v1/models HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status_code(&split_response(&raw).0), 200);

        server.stop();

        // Give the abort a moment to actually drop the listener.
        for _ in 0..50 {
            if TcpStream::connect(addr).await.is_err() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("expected connections to start failing after stop()");
    }
}
