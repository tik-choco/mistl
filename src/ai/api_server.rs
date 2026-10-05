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
//! - `POST /v1/chat/completions` -> body `{model?, messages, stream?,
//!   tools?, tool_choice?}`. `messages` entries must have a string `role`
//!   and string `content` (`null` is accepted only for an assistant turn
//!   with `tool_calls`; `tool` turns need `tool_call_id`) -- 400 otherwise.
//!   Calls the injected [`super::LlmCallFn`]. Tool calls come back as
//!   `message.tool_calls` / `finish_reason: "tool_calls"` (streaming: one
//!   `delta.tool_calls` chunk after the last text delta). A tool request
//!   the network provider cannot serve -> `400` code `tools_unsupported`.
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
//! - `POST /v1/audio/speech` -> body `{input, model?, voice?,
//!   response_format?, speed?, lang?}`. Responds `200` with the **raw
//!   audio** and the format's MIME type, not JSON -- the upstream returns
//!   bytes and so does this. `model`/`voice` omitted or empty means "use the
//!   TTS configuration's", resolved through the same chain the wire path uses (see
//!   `super::resolve_tts_voice`), so both doors mean the same thing by the
//!   voices they advertise. `lang` is a **mistl extension** over OpenAI's
//!   schema: the BCP-47 hint `tts_request.lang` carries, which an OpenAI
//!   client simply never sends.
//! - `POST /v1/audio/transcriptions` -> `multipart/form-data` with a `file`
//!   part and an optional `model` field; responds `{"text": ...}`.
//!   Multipart rather than a raw body because the point of this server is
//!   that an unmodified OpenAI client can be pointed at it, and every such
//!   client sends this endpoint that way.
//! - Unresolvable explicit models -> `404` with OpenAI `model_not_found`.
//! - Upstream/backend errors: before any deltas were streamed -> `502` with
//!   `{"error":{"message":...}}` carrying a generic message (the detail is
//!   logged only, as it can embed upstream URLs/tokens) (or `400` for malformed requests); once
//!   streaming has begun, emit an SSE `{"error":...}` event and terminate
//!   the HTTP chunked body cleanly (without a success `[DONE]` event).
//! - Anything else -> `404`.
//!
//! Audio endpoints resolve ai.tts/ai.stt directly. HTTP references call their
//! endpoint; Room references use the room's advertised voice service.
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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use super::openai::{ChatOutput, ToolOptions};
use super::protocol::ChatMessage;
use super::{LlmCallFn, ModelNotFound, ModelsFn, SttFn, ToolsUnsupported, TtsFn, stt, tts};

pub(super) type OaiFn = Arc<
    dyn Fn(
            Value,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<super::oai_tunnel::TunnelResponse>> + Send>,
        > + Send
        + Sync,
>;

pub(super) struct RoomRoutes {
    pub call: LlmCallFn,
    pub models: ModelsFn,
    pub tts: TtsFn,
    pub stt: SttFn,
    pub oai: OaiFn,
}

pub(super) type RoomsFn = Arc<
    dyn Fn(
            String,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Option<RoomRoutes>>> + Send>,
        > + Send
        + Sync,
>;

/// Header section size cap (request-line + headers), matches doc.
const MAX_HEADER_BYTES: usize = 16 * 1024;
/// Body size cap for `/v1/audio/transcriptions` (multipart audio upload).
const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
/// Body size cap for the JSON endpoints (chat / speech).
const MAX_JSON_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Deadline for receiving the complete request head (slowloris guard).
const HEAD_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Max idle gap between body reads.
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Concurrent connections served; extra ones are dropped at accept.
const MAX_CONNECTIONS: usize = 32;
const SSE_RESPONSE_HEADER: &str = "HTTP/1.1 200 OK\r\n\
                                   Content-Type: text/event-stream\r\n\
                                   Transfer-Encoding: chunked\r\n\
                                   Cache-Control: no-cache\r\n\
                                   Connection: close\r\n\r\n";

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
    #[cfg(test)]
    pub async fn start(
        listen: &str,
        call: LlmCallFn,
        models: ModelsFn,
        tts_call: TtsFn,
        stt_call: SttFn,
    ) -> Result<Arc<ApiServer>> {
        Self::start_with_rooms(listen, call, models, tts_call, stt_call, None).await
    }

    pub async fn start_with_rooms(
        listen: &str,
        call: LlmCallFn,
        models: ModelsFn,
        tts_call: TtsFn,
        stt_call: SttFn,
        rooms: Option<RoomsFn>,
    ) -> Result<Arc<ApiServer>> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding api server to {listen}"))?;
        let addr = listener
            .local_addr()
            .context("reading bound api server address")?;

        let conns: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let conns_for_loop = conns.clone();

        let conn_limit = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
        let accept_handle = tokio::spawn(async move {
            loop {
                let (socket, _peer) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(error) => {
                        warn!(%error, "api_server accept failed");
                        continue;
                    }
                };
                let Ok(permit) = conn_limit.clone().try_acquire_owned() else {
                    // Over the connection cap: drop the socket.
                    continue;
                };
                let call = call.clone();
                let models = models.clone();
                let tts_call = tts_call.clone();
                let stt_call = stt_call.clone();
                let rooms = rooms.clone();
                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) =
                        handle_connection(socket, call, models, tts_call, stt_call, rooms).await
                    {
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
        let n = tokio::time::timeout(BODY_IDLE_TIMEOUT, stream.read(&mut chunk))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "body read timed out"))??;
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

/// Whether `content_type` (a raw header value) is `application/json`,
/// ignoring case and parameters such as `; charset=utf-8`.
fn content_type_is_json(content_type: Option<&str>) -> bool {
    content_type
        .and_then(|v| v.split(';').next())
        .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
}

/// Same rule as the dashboard's `host_is_allowed` (DNS-rebinding guard):
/// loopback names with an optional port, or the literal IP the connection
/// was actually accepted on.
fn host_is_allowed(host: &str, local_addr: &SocketAddr) -> bool {
    let host = host.trim();
    if host.is_empty() {
        return false;
    }
    let valid_port = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    let matches_local = |name: &str, port: Option<&str>| {
        let Ok(ip) = name.parse::<std::net::IpAddr>() else {
            return false;
        };
        if ip != local_addr.ip() {
            return false;
        }
        match port {
            Some(port) => port.parse::<u16>() == Ok(local_addr.port()),
            None => true,
        }
    };
    if let Some(rest) = host.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return false;
        };
        let addr = &rest[..end];
        let after = &rest[end + 1..];
        if !(after.is_empty() || (after.starts_with(':') && valid_port(&after[1..]))) {
            return false;
        }
        return addr == "::1" || matches_local(addr, None);
    }
    let (name, port) = match host.rsplit_once(':') {
        Some((name, port)) if valid_port(port) => (name, Some(port)),
        _ => (host, None),
    };
    if name.eq_ignore_ascii_case("127.0.0.1") || name.eq_ignore_ascii_case("localhost") {
        return true;
    }
    matches_local(name, port)
}

/// Whether an `Origin` header value names this server on loopback
/// (`http://127.0.0.1:<port>`, `http://localhost:<port>`, `http://[::1]:<port>`).
/// Browsers attach `Origin` to cross-site POSTs; native OpenAI clients send
/// none, so a present-but-foreign origin is always refused.
fn origin_is_allowed(origin: &str, local_addr: &SocketAddr) -> bool {
    let Some(authority) = origin.trim().strip_prefix("http://") else {
        return false;
    };
    let (name, port) = if let Some(rest) = authority.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return false;
        };
        let after = &rest[end + 1..];
        (
            format!("[{}]", &rest[..end]),
            after.strip_prefix(':').map(str::to_string),
        )
    } else {
        match authority.rsplit_once(':') {
            Some((n, p)) => (n.to_string(), Some(p.to_string())),
            None => (authority.to_string(), None),
        }
    };
    let name_ok = name.eq_ignore_ascii_case("127.0.0.1")
        || name.eq_ignore_ascii_case("localhost")
        || name == "[::1]";
    let port_ok = match port {
        Some(p) => p.parse::<u16>() == Ok(local_addr.port()),
        None => local_addr.port() == 80,
    };
    name_ok && port_ok
}

/// Request-level guards run before any routing: request-smuggling shapes,
/// DNS rebinding (`Host`), and cross-site browser requests (`Origin`).
/// `Err` carries `(status, reason, message)`.
fn check_request_guards(
    head: &RequestHead,
    local_addr: &SocketAddr,
) -> Result<(), (u16, &'static str, &'static str)> {
    let count = |name: &str| {
        head.headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .count()
    };
    if count("host") > 1 || count("content-length") > 1 || count("transfer-encoding") > 0 {
        return Err((400, "Bad Request", "unsupported request framing"));
    }
    if !host_is_allowed(head.header("host").unwrap_or(""), local_addr) {
        return Err((403, "Forbidden", "host not allowed"));
    }
    if let Some(origin) = head.header("origin") {
        if !origin_is_allowed(origin, local_addr) {
            return Err((403, "Forbidden", "origin not allowed"));
        }
    }
    Ok(())
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

/// What local clients see for any upstream/backend failure. The real error
/// (which can embed the upstream URL, a token in a query string, or response
/// bodies) goes to the log only.
const BACKEND_ERROR_MESSAGE: &str = "upstream backend error (see the mistl daemon log)";

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

/// One request message as an OpenAI client sends it. `content` may be
/// `null` (assistant turns that only call tools); other malformed fields
/// (wrong type) fail to deserialize -> 400.
#[derive(serde::Deserialize)]
struct ApiMessage {
    role: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Value>,
    #[serde(default)]
    tool_call_id: Option<String>,
}

/// Request body shape for `POST /v1/chat/completions`.
#[derive(serde::Deserialize)]
struct ChatRequestBody {
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default)]
    model: Option<String>,
    messages: Vec<ApiMessage>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    tools: Option<Value>,
    #[serde(default)]
    tool_choice: Option<Value>,
}

/// A validated chat request, ready for the injected call fn.
#[derive(Debug)]
struct ParsedChat {
    model: Option<String>,
    messages: Vec<ChatMessage>,
    tools: ToolOptions,
    stream: bool,
}

/// Parses and validates a `/v1/chat/completions` body. `Err` is the 400
/// message. Bounded: the caller already capped the body size, and the tool
/// fields are checked against the same limits the network enforces.
fn parse_chat_request(body: &[u8]) -> std::result::Result<ParsedChat, String> {
    use super::protocol::{MAX_TOOL_CALLS, MAX_TOOLS, MAX_TOOLS_BYTES};

    let req: ChatRequestBody =
        serde_json::from_slice(body).map_err(|e| format!("invalid request body: {e}"))?;

    let mut messages = Vec::with_capacity(req.messages.len());
    for (i, m) in req.messages.into_iter().enumerate() {
        let tool_calls = match m.tool_calls {
            None => None,
            Some(Value::Array(a)) if a.is_empty() => None,
            Some(Value::Array(a)) if a.len() > MAX_TOOL_CALLS => {
                return Err(format!(
                    "messages[{i}].tool_calls has more than {MAX_TOOL_CALLS} entries"
                ));
            }
            Some(v @ Value::Array(_)) => Some(v),
            Some(_) => return Err(format!("messages[{i}].tool_calls must be an array")),
        };
        let content = match m.content {
            Some(c) => c,
            // `content: null` is only meaningful for an assistant turn that
            // calls tools; it travels as "" (the wire content is a string).
            None if m.role == "assistant" && tool_calls.is_some() => String::new(),
            None => return Err(format!("messages[{i}].content is required")),
        };
        if m.role == "tool" && m.tool_call_id.is_none() {
            return Err(format!(
                "messages[{i}].tool_call_id is required for tool messages"
            ));
        }
        messages.push(ChatMessage {
            role: m.role,
            content,
            tool_calls,
            tool_call_id: m.tool_call_id,
        });
    }

    // Clients commonly send `tools: []` / `tools: null`; both mean "no
    // tools". `tool_choice` without tools is meaningless (upstreams reject
    // it), so it is dropped too.
    let tools = match req.tools {
        None => None,
        Some(Value::Array(a)) if a.is_empty() => None,
        Some(Value::Array(a)) => {
            if a.len() > MAX_TOOLS {
                return Err(format!("`tools` has more than {MAX_TOOLS} entries"));
            }
            let v = Value::Array(a);
            if v.to_string().len() > MAX_TOOLS_BYTES {
                return Err("`tools` is too large".to_string());
            }
            Some(v)
        }
        Some(_) => return Err("`tools` must be an array".to_string()),
    };
    let tool_choice = match req.tool_choice {
        None => None,
        Some(v @ (Value::String(_) | Value::Object(_))) => tools.is_some().then_some(v),
        Some(_) => return Err("`tool_choice` must be a string or an object".to_string()),
    };

    Ok(ParsedChat {
        model: req.model,
        messages,
        tools: ToolOptions {
            tools,
            tool_choice,
            reasoning_effort: req.reasoning_effort,
        },
        stream: req.stream,
    })
}

/// Whether `err` is the "remote provider lacks tools" refusal.
fn is_tools_unsupported(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|e| e.downcast_ref::<ToolsUnsupported>().is_some())
}

/// The 400 body for a tool request the backend cannot serve.
fn tools_unsupported_body() -> Value {
    json!({"error": {
        "message": "the AI provider on the network does not support tools",
        "type": "invalid_request_error",
        "code": "tools_unsupported",
    }})
}

/// `choices[0].message` for a finished answer: `content` is `null` when
/// empty alongside tool calls; `tool_calls` only when present.
fn completion_message(output: &ChatOutput) -> Value {
    let mut message = json!({"role": "assistant"});
    if output.tool_calls.is_some() && output.content.is_empty() {
        message["content"] = Value::Null;
    } else {
        message["content"] = json!(output.content);
    }
    if let Some(calls) = &output.tool_calls {
        message["tool_calls"] = calls.clone();
    }
    message
}

fn finish_reason(output: &ChatOutput) -> &'static str {
    if output.tool_calls.is_some() {
        "tool_calls"
    } else {
        "stop"
    }
}

/// The single streaming chunk that carries all tool calls (each with its
/// `index`), emitted after the last text delta.
fn tool_calls_stream_delta(calls: &Value) -> Value {
    let indexed: Vec<Value> = calls
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let mut c = c.clone();
            c["index"] = json!(i);
            c
        })
        .collect();
    json!({"tool_calls": indexed})
}

/// Writes the error for a failed backend call before any response bytes
/// were committed: 404 `model_not_found`, 400 `tools_unsupported`, else 502.
async fn write_backend_failure(stream: &mut TcpStream, error: &anyhow::Error) {
    if let Some(error) = error.downcast_ref::<ModelNotFound>() {
        let body = json!({"error": {
            "message": error.to_string(),
            "type": "invalid_request_error",
            "param": "model",
            "code": "model_not_found",
        }});
        let _ = write_json_response(stream, 404, "Not Found", &body).await;
    } else if is_tools_unsupported(error) {
        let _ = write_json_response(stream, 400, "Bad Request", &tools_unsupported_body()).await;
    } else {
        warn!("api_server: backend error: {error:#}");
        let _ = write_error(stream, 502, "Bad Gateway", BACKEND_ERROR_MESSAGE).await;
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    call: LlmCallFn,
    models: ModelsFn,
    tts_call: TtsFn,
    stt_call: SttFn,
    rooms: Option<RoomsFn>,
) -> Result<()> {
    let local_addr = stream.local_addr().context("reading local address")?;
    // Same rule as the dashboard: while external connections are OFF, only
    // loopback peers are served (matters when `ai.api_listen` is not
    // loopback).
    if crate::daemon::external_connections_blocked()
        && !stream
            .peer_addr()
            .context("reading peer address")?
            .ip()
            .to_canonical()
            .is_loopback()
    {
        let _ = write_error(
            &mut stream,
            403,
            "Forbidden",
            "external connections are OFF",
        )
        .await;
        return Ok(());
    }
    let (head, leftover) =
        match tokio::time::timeout(HEAD_READ_TIMEOUT, read_request_head(&mut stream)).await {
            Ok(Ok(Some(pair))) => pair,
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(_)) => {
                let _ = write_error(&mut stream, 400, "Bad Request", "malformed request").await;
                return Ok(());
            }
            Err(_) => {
                let _ = write_error(
                    &mut stream,
                    408,
                    "Request Timeout",
                    "request head timed out",
                )
                .await;
                return Ok(());
            }
        };

    if let Err((status, reason, message)) = check_request_guards(&head, &local_addr) {
        let _ = write_error(&mut stream, status, reason, message).await;
        return Ok(());
    }

    let is_post = head.method.eq_ignore_ascii_case("POST");
    let is_get = head.method.eq_ignore_ascii_case("GET");
    let mut path = head.path.split('?').next().unwrap_or("").to_string();
    let (call, models, tts_call, stt_call, oai) = if path.starts_with("/v1/rooms/") {
        let Some((room, endpoint)) = room_route(&path) else {
            write_error(&mut stream, 404, "Not Found", "invalid room route").await?;
            return Ok(());
        };
        let Some(rooms) = rooms else {
            write_error(
                &mut stream,
                404,
                "Not Found",
                "room is not an enabled Room provider in ai.providers",
            )
            .await?;
            return Ok(());
        };
        let routes = match rooms(room).await {
            Ok(Some(routes)) => routes,
            Ok(None) => {
                write_error(
                    &mut stream,
                    404,
                    "Not Found",
                    "room is not an enabled Room provider in ai.providers",
                )
                .await?;
                return Ok(());
            }
            Err(err) => {
                write_backend_failure(&mut stream, &err).await;
                return Ok(());
            }
        };
        path = format!("/v1/{endpoint}");
        (
            routes.call,
            routes.models,
            routes.tts,
            routes.stt,
            Some(routes.oai),
        )
    } else {
        (call, models, tts_call, stt_call, None)
    };

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
        let is_transcription = path == "/v1/audio/transcriptions";
        let body_cap = if is_transcription {
            MAX_BODY_BYTES
        } else {
            MAX_JSON_BODY_BYTES
        };
        if content_length > body_cap {
            let _ = write_error(
                &mut stream,
                413,
                "Payload Too Large",
                "request body too large",
            )
            .await;
            return Ok(());
        }
        if !is_transcription && !content_type_is_json(head.header("content-type")) {
            let _ = write_error(
                &mut stream,
                415,
                "Unsupported Media Type",
                "Content-Type must be application/json",
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
        handle_chat_completions_with_oai(&mut stream, &body, &call, oai.as_ref()).await?;
    } else if is_post && path == "/v1/audio/speech" {
        handle_audio_speech(&mut stream, &body, &tts_call).await?;
    } else if is_post && path == "/v1/audio/transcriptions" {
        let content_type = head.header("content-type").unwrap_or_default().to_string();
        handle_audio_transcriptions(&mut stream, &content_type, &body, &stt_call).await?;
    } else {
        let _ = write_error(&mut stream, 404, "Not Found", "not found").await;
    }

    Ok(())
}

/// Split before percent decoding, so encoded slashes belong to the room id.
fn room_route(path: &str) -> Option<(String, String)> {
    let rest = path.strip_prefix("/v1/rooms/")?;
    let (encoded, endpoint) = rest.split_once('/')?;
    if !matches!(
        endpoint,
        "models" | "chat/completions" | "audio/speech" | "audio/transcriptions"
    ) {
        return None;
    }
    let mut decoded = Vec::new();
    let mut bytes = encoded.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hi = (bytes.next()? as char).to_digit(16)?;
            let lo = (bytes.next()? as char).to_digit(16)?;
            decoded.push((hi * 16 + lo) as u8);
        } else {
            decoded.push(b);
        }
    }
    let room = String::from_utf8(decoded).ok()?;
    if room.is_empty() {
        return None;
    }
    Some((room, endpoint.into()))
}

/// `POST /v1/audio/speech`: OpenAI's shape -- `{model, input, voice,
/// response_format?, speed?}` in, raw audio out (not JSON; the upstream
/// returns bytes and so do we, with the format's own MIME type).
async fn handle_audio_speech(stream: &mut TcpStream, body: &[u8], call: &TtsFn) -> Result<()> {
    let req: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(err) => {
            let _ = write_error(
                stream,
                400,
                "Bad Request",
                &format!("invalid request body: {err}"),
            )
            .await;
            return Ok(());
        }
    };

    let input = req.get("input").and_then(Value::as_str).unwrap_or_default();
    if input.trim().is_empty() {
        let _ = write_error(stream, 400, "Bad Request", "`input` is required").await;
        return Ok(());
    }

    let params = tts::TtsParams {
        // Empty means "whatever ai.tts says" -- resolved in
        // `AiService::synthesize`, not here, so the wire path and this one
        // fill in defaults the same way.
        model: req
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        voice: req
            .get("voice")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        input: input.to_string(),
        format: req
            .get("response_format")
            .and_then(Value::as_str)
            .filter(|v| super::protocol::valid_tts_format(v))
            .map(str::to_string),
        speed: req
            .get("speed")
            .and_then(Value::as_f64)
            .filter(|v| crate::config::valid_tts_speed(*v)),
    };

    // `lang` is a mistl extension, not part of OpenAI's schema: it is the
    // same BCP-47 hint `tts_request.lang` carries over the wire, and an
    // OpenAI client that never sends it simply gets the configuration's own voice.
    let lang = req.get("lang").and_then(Value::as_str).map(str::to_string);

    match (call)(params, lang).await {
        Ok(audio) => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                audio.mime,
                audio.bytes.len()
            );
            stream.write_all(head.as_bytes()).await?;
            stream.write_all(&audio.bytes).await?;
        }
        Err(err) => {
            write_backend_failure(stream, &err).await;
        }
    }
    Ok(())
}

/// `POST /v1/audio/transcriptions`: OpenAI's shape -- `multipart/form-data`
/// with a `file` part and a `model` field, transcript back as
/// `{"text": "..."}`.
///
/// Multipart rather than a raw body because the whole point of this server
/// is that an unmodified OpenAI client can be pointed at it, and every such
/// client sends this endpoint as multipart.
async fn handle_audio_transcriptions(
    stream: &mut TcpStream,
    content_type: &str,
    body: &[u8],
    call: &SttFn,
) -> Result<()> {
    let Some(boundary) = multipart_boundary(content_type) else {
        let _ = write_error(
            stream,
            400,
            "Bad Request",
            "expected multipart/form-data with a boundary",
        )
        .await;
        return Ok(());
    };

    let parts = parse_multipart(body, &boundary);
    let Some(file) = parts.iter().find(|p| p.name == "file") else {
        let _ = write_error(stream, 400, "Bad Request", "missing `file` part").await;
        return Ok(());
    };
    if file.bytes.is_empty() {
        let _ = write_error(stream, 400, "Bad Request", "`file` part is empty").await;
        return Ok(());
    }

    let model = parts
        .iter()
        .find(|p| p.name == "model")
        .map(|p| String::from_utf8_lossy(&p.bytes).trim().to_string())
        .unwrap_or_default();

    let params = stt::SttParams {
        model,
        audio: file.bytes.clone(),
        mime: if file.content_type.is_empty() {
            "application/octet-stream".to_string()
        } else {
            file.content_type.clone()
        },
        file_name: file.file_name.clone(),
    };

    match (call)(params).await {
        Ok(text) => {
            let _ = write_json_response(stream, 200, "OK", &json!({ "text": text })).await;
        }
        Err(err) => {
            write_backend_failure(stream, &err).await;
        }
    }
    Ok(())
}

/// One `multipart/form-data` part, reduced to what this endpoint needs.
#[derive(Debug, Clone)]
struct MultipartPart {
    name: String,
    file_name: Option<String>,
    content_type: String,
    bytes: Vec<u8>,
}

/// Pull `boundary=...` out of a `Content-Type` header value, unquoting it.
fn multipart_boundary(content_type: &str) -> Option<String> {
    if !content_type
        .to_ascii_lowercase()
        .contains("multipart/form-data")
    {
        return None;
    }
    for param in content_type.split(';').skip(1) {
        // `continue`, not `?`: a parameter list may hold flags with no `=`
        // at all, and bailing out on the first of those would abandon the
        // search before ever reaching the one being looked for.
        let Some((key, value)) = param.split_once('=') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case("boundary") {
            let value = value.trim().trim_matches('"');
            if value.is_empty() {
                return None;
            }
            return Some(value.to_string());
        }
    }
    None
}

/// Minimal `multipart/form-data` reader: split on the boundary, then split
/// each part into headers and bytes at the blank line.
///
/// Deliberately not a general implementation -- no nested multipart, no
/// transfer encodings, no continuation lines. This endpoint receives one
/// audio file and one or two short text fields from an OpenAI client, and a
/// parser that only handles that is far easier to be sure of than one that
/// pretends to handle RFC 2046 in full. A part it can't make sense of is
/// skipped rather than failing the request; the caller then reports the
/// missing `file` part, which is the actionable message either way.
fn parse_multipart(body: &[u8], boundary: &str) -> Vec<MultipartPart> {
    let delimiter = format!("--{boundary}").into_bytes();
    let mut out = Vec::new();

    for segment in split_on(body, &delimiter).into_iter().skip(1) {
        // A segment starts with CRLF (or "--" on the closing delimiter) and
        // ends with the CRLF preceding the next delimiter.
        let segment = segment
            .strip_prefix(b"--".as_slice())
            .map_or(segment, |_| &[]);
        let segment = segment.strip_prefix(b"\r\n".as_slice()).unwrap_or(segment);
        let segment = segment.strip_suffix(b"\r\n".as_slice()).unwrap_or(segment);
        if segment.is_empty() {
            continue;
        }

        let Some(split) = find(segment, b"\r\n\r\n") else {
            continue;
        };
        let (head, rest) = segment.split_at(split);
        let bytes = rest[4..].to_vec();

        let mut name = String::new();
        let mut file_name = None;
        let mut content_type = String::new();
        for line in String::from_utf8_lossy(head).lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            if key.trim().eq_ignore_ascii_case("content-disposition") {
                name = quoted_param(value, "name").unwrap_or_default();
                file_name = quoted_param(value, "filename");
            } else if key.trim().eq_ignore_ascii_case("content-type") {
                content_type = value.trim().to_string();
            }
        }
        if name.is_empty() {
            continue;
        }
        out.push(MultipartPart {
            name,
            file_name,
            content_type,
            bytes,
        });
    }
    out
}

/// `name="value"` out of a header parameter list.
fn quoted_param(value: &str, key: &str) -> Option<String> {
    for param in value.split(';') {
        // Same reason as `multipart_boundary`: `Content-Disposition` always
        // leads with a bare `form-data`, so `?` here would give up before
        // looking at a single named parameter.
        let Some((k, v)) = param.split_once('=') else {
            continue;
        };
        if k.trim().eq_ignore_ascii_case(key) {
            return Some(v.trim().trim_matches('"').to_string());
        }
    }
    None
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn split_on<'a>(haystack: &'a [u8], needle: &[u8]) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut rest = haystack;
    while let Some(at) = find(rest, needle) {
        out.push(&rest[..at]);
        rest = &rest[at + needle.len()..];
    }
    out.push(rest);
    out
}

async fn handle_chat_completions_with_oai(
    stream: &mut TcpStream,
    body: &[u8],
    call: &LlmCallFn,
    oai: Option<&OaiFn>,
) -> Result<()> {
    // Validate the ordinary fields using the text parser; only image content
    // parts take the non-streaming tunnel, and SSE is adapted locally below.
    let vision = match vision_request(body) {
        Ok(value) => value,
        Err(message) => {
            write_error(stream, 400, "Bad Request", &message).await?;
            return Ok(());
        }
    };
    let mut vision_call: Option<LlmCallFn> = None;
    let normalized;
    let body = if let Some((mut raw, text_body)) = vision {
        let req = match parse_chat_request(&text_body) {
            Ok(req) => req,
            Err(message) => {
                write_error(stream, 400, "Bad Request", &message).await?;
                return Ok(());
            }
        };
        let Some(oai) = oai else {
            write_error(
                stream,
                400,
                "Bad Request",
                "image content requires a room-scoped route and an oai provider",
            )
            .await?;
            return Ok(());
        };
        raw["stream"] = json!(false);
        raw.as_object_mut()
            .expect("validated chat body")
            .remove("temperature");
        let response = match oai(raw).await {
            Ok(response) => response,
            Err(err) => {
                write_backend_failure(stream, &err).await;
                return Ok(());
            }
        };
        if !req.stream || !(200..300).contains(&response.status) {
            let content_type = if response.content_type.contains(['\r', '\n']) {
                "application/json"
            } else {
                &response.content_type
            };
            let reason = if (200..300).contains(&response.status) {
                "OK"
            } else {
                "Upstream Error"
            };
            let head = format!(
                "HTTP/1.1 {} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.status,
                response.body.len()
            );
            stream.write_all(head.as_bytes()).await?;
            stream.write_all(&response.body).await?;
            return Ok(());
        }
        let value: Value = match serde_json::from_slice(&response.body) {
            Ok(value) => value,
            Err(_) => {
                write_error(stream, 502, "Bad Gateway", BACKEND_ERROR_MESSAGE).await?;
                return Ok(());
            }
        };
        let Some(message) = value.pointer("/choices/0/message") else {
            write_error(stream, 502, "Bad Gateway", BACKEND_ERROR_MESSAGE).await?;
            return Ok(());
        };
        let output = ChatOutput {
            content: message
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .into(),
            tool_calls: message.get("tool_calls").filter(|v| v.is_array()).cloned(),
        };
        vision_call = Some(Arc::new(move |_, _, _, tx| {
            let output = output.clone();
            Box::pin(async move {
                if let Some(tx) = tx {
                    let _ = tx.send(output.content.clone());
                }
                Ok(output)
            })
        }));
        normalized = text_body;
        normalized.as_slice()
    } else {
        body
    };
    let call = vision_call.as_ref().unwrap_or(call);
    let req = match parse_chat_request(body) {
        Ok(req) => req,
        Err(message) => {
            let _ = write_error(stream, 400, "Bad Request", &message).await;
            return Ok(());
        }
    };

    let resp_model = req.model.clone().unwrap_or_else(|| "mistl".to_string());
    let id = gen_id("chatcmpl");
    let created = unix_now();

    if !req.stream {
        let fut = (call)(req.messages, req.tools, req.model, None);
        return match fut.await {
            Ok(output) => {
                let resp = json!({
                    "id": id,
                    "object": "chat.completion",
                    "created": created,
                    "model": resp_model,
                    "choices": [{
                        "index": 0,
                        "message": completion_message(&output),
                        "finish_reason": finish_reason(&output),
                    }],
                });
                write_json_response(stream, 200, "OK", &resp).await?;
                Ok(())
            }
            Err(error) => {
                write_backend_failure(stream, &error).await;
                Ok(())
            }
        };
    }

    // Streaming path: run the call future concurrently with draining its
    // delta channel, writing one SSE event per delta as an HTTP chunk.
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let mut call_fut = Box::pin((call)(req.messages, req.tools, req.model, Some(tx)));

    let mut done: Option<Result<ChatOutput>> = None;
    let mut rx_closed = false;
    // Do not commit a 200 response until the first delta is ready. If the
    // backend fails before then we can still return a useful HTTP 502 rather
    // than disguising the real error as an incomplete chunked response.
    let mut stream_started = false;

    loop {
        if done.is_some() && rx_closed {
            break;
        }
        tokio::select! {
            // A completed call may have enqueued its final delta in the same
            // poll. Prefer draining ready deltas before observing the result.
            biased;
            maybe_delta = rx.recv(), if !rx_closed => {
                match maybe_delta {
                    Some(delta) => {
                        if !stream_started {
                            stream.write_all(SSE_RESPONSE_HEADER.as_bytes()).await?;
                            stream.flush().await?;
                            stream_started = true;
                        }
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
        Ok(output) => {
            // A successful backend is allowed to produce no content. It is
            // still a valid empty SSE completion, so commit the response now.
            if !stream_started {
                stream.write_all(SSE_RESPONSE_HEADER.as_bytes()).await?;
                stream.flush().await?;
            }
            // Tool calls arrive complete (not streamed upstream-to-us), so
            // they go out as one chunk after the last text delta.
            if let Some(calls) = &output.tool_calls {
                let chunk = json!({
                    "id": id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": resp_model,
                    "choices": [{
                        "index": 0,
                        "delta": tool_calls_stream_delta(calls),
                        "finish_reason": null,
                    }],
                });
                let event = format!("data: {chunk}\n\n");
                write_http_chunk(stream, event.as_bytes()).await?;
            }
            let final_chunk = json!({
                "id": id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": resp_model,
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": finish_reason(&output),
                }],
            });
            let event = format!("data: {final_chunk}\n\n");
            write_http_chunk(stream, event.as_bytes()).await?;
            write_http_chunk(stream, b"data: [DONE]\n\n").await?;
            write_http_chunk(stream, b"").await?;
        }
        Err(error) => {
            if !stream_started {
                // No response bytes have been committed, so answer with a
                // normal OpenAI-shaped HTTP failure (generic text; see
                // BACKEND_ERROR_MESSAGE), 404 `model_not_found`, or
                // 400 `tools_unsupported`.
                write_backend_failure(stream, &error).await;
            } else {
                // HTTP status is already committed. Surface a generic error in
                // band and always finish the chunked body; abruptly closing it
                // makes clients report only "incomplete chunked read".
                warn!("api_server: backend error mid-stream: {error:#}");
                let event = format!(
                    "data: {}\n\n",
                    json!({
                        "error": {
                            "message": BACKEND_ERROR_MESSAGE,
                            "type": "backend_error",
                        }
                    })
                );
                write_http_chunk(stream, event.as_bytes()).await?;
                write_http_chunk(stream, b"").await?;
            }
        }
    }

    Ok(())
}

fn vision_request(body: &[u8]) -> std::result::Result<Option<(Value, Vec<u8>)>, String> {
    let raw: Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid request body: {e}"))?;
    let has_image = raw
        .get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages.iter().any(|m| {
                m.get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| {
                        parts
                            .iter()
                            .any(|p| p.get("type").and_then(Value::as_str) == Some("image_url"))
                    })
            })
        });
    if !has_image {
        return Ok(None);
    }
    let mut text = raw.clone();
    for message in text["messages"]
        .as_array_mut()
        .ok_or("messages must be an array")?
    {
        if let Some(parts) = message.get("content").and_then(Value::as_array) {
            let mut content = String::new();
            for part in parts {
                match part.get("type").and_then(Value::as_str) {
                    Some("text") => content.push_str(
                        part.get("text")
                            .and_then(Value::as_str)
                            .ok_or("text part requires text")?,
                    ),
                    Some("image_url")
                        if part
                            .pointer("/image_url/url")
                            .and_then(Value::as_str)
                            .is_some_and(|s| !s.is_empty()) => {}
                    _ => return Err("invalid image content part".into()),
                }
            }
            message["content"] = Value::String(content);
        }
    }
    Ok(Some((
        raw,
        serde_json::to_vec(&text).map_err(|e| e.to_string())?,
    )))
}

#[cfg(test)]
mod tests {
    async fn room_test_server() -> (Arc<ApiServer>, Arc<Mutex<Vec<Value>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let default_seen = seen.clone();
        let call: LlmCallFn = Arc::new(move |_, options, _, _| {
            default_seen
                .lock()
                .unwrap()
                .push(json!({"route":"default","effort":options.reasoning_effort}));
            Box::pin(async { Ok("default".to_string().into()) })
        });
        let records = seen.clone();
        let rooms: RoomsFn = Arc::new(move |room| {
            let records = records.clone();
            Box::pin(async move {
                if room != "team/one" {
                    return Ok(None);
                }
                let chat_records = records.clone();
                let call: LlmCallFn = Arc::new(move |_, options, model, tx| {
                    chat_records.lock().unwrap().push(
                        json!({"route":"room","effort":options.reasoning_effort,"model":model}),
                    );
                    Box::pin(async move {
                        if let Some(tx) = tx {
                            let _ = tx.send("room".into());
                        }
                        Ok("room".to_string().into())
                    })
                });
                let oai: OaiFn = Arc::new(move |body| {
                    records
                        .lock()
                        .unwrap()
                        .push(json!({"route":"oai","body":body}));
                    Box::pin(async {
                        Ok(super::super::oai_tunnel::TunnelResponse {
                            status: 200,
                            content_type: "application/json".into(),
                            body: serde_json::to_vec(
                                &json!({"choices":[{"message":{"content":"image answer"}}]}),
                            )
                            .unwrap(),
                        })
                    })
                });
                let tts: TtsFn = Arc::new(|_, _| {
                    Box::pin(async {
                        Ok(tts::TtsAudio {
                            bytes: b"room audio".to_vec(),
                            mime: "audio/mpeg".into(),
                        })
                    })
                });
                let stt: SttFn = Arc::new(|_| Box::pin(async { Ok("room transcript".into()) }));
                Ok(Some(RoomRoutes {
                    call,
                    models: Arc::new(|| vec!["room-raw".into()]),
                    tts,
                    stt,
                    oai,
                }))
            })
        });
        let server = ApiServer::start_with_rooms(
            "127.0.0.1:0",
            call,
            fake_models(),
            fake_tts(),
            fake_stt(),
            Some(rooms),
        )
        .await
        .unwrap();
        (server, seen)
    }

    async fn post_json(server: &ApiServer, path: &str, value: Value) -> Vec<u8> {
        let body = value.to_string();
        send_request(server.addr(), &format!("POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len())).await
    }

    #[test]
    fn room_route_decodes_only_the_room_segment() {
        assert_eq!(
            room_route("/v1/rooms/team%2Fone/chat/completions"),
            Some(("team/one".into(), "chat/completions".into()))
        );
        assert_eq!(
            room_route("/v1/rooms/%E6%97%A5%E6%9C%AC/models").unwrap().0,
            "\u{65e5}\u{672c}"
        );
        assert_eq!(room_route("/v1/rooms/a+b/models").unwrap().0, "a+b");
        for path in [
            "/v1/rooms/%FF/models",
            "/v1/rooms/%2/models",
            "/v1/rooms//models",
            "/v1/rooms/x/embeddings",
        ] {
            assert!(room_route(path).is_none(), "{path}");
        }
    }

    #[tokio::test]
    async fn default_and_room_chat_pass_effort_with_both_response_modes() {
        let (server, seen) = room_test_server().await;
        for path in [
            "/v1/chat/completions",
            "/v1/rooms/team%2Fone/chat/completions",
        ] {
            for stream in [false, true] {
                let raw = post_json(&server, path, json!({"model":"raw","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"none","stream":stream})).await;
                let (head, body) = split_response(&raw);
                assert_eq!(status_code(&head), 200);
                if stream {
                    assert!(
                        String::from_utf8(de_chunk(body))
                            .unwrap()
                            .contains("[DONE]")
                    );
                }
                assert_eq!(seen.lock().unwrap().last().unwrap()["effort"], "none");
                assert_eq!(
                    seen.lock().unwrap().last().unwrap()["route"],
                    if path.contains("rooms") {
                        "room"
                    } else {
                        "default"
                    }
                );
            }
        }
        server.stop();
    }

    #[tokio::test]
    async fn room_models_voice_routes_and_missing_rooms_are_isolated() {
        let (server, _) = room_test_server().await;
        let raw = send_request(
            server.addr(),
            "GET /v1/rooms/team%2Fone/models HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        assert_eq!(
            serde_json::from_slice::<Value>(body).unwrap()["data"][0]["id"],
            "room-raw"
        );
        let raw = post_json(
            &server,
            "/v1/rooms/team%2Fone/audio/speech",
            json!({"input":"hi"}),
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        assert_eq!(body, b"room audio");
        let multipart = "--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"clip.wav\"\r\nContent-Type: audio/wav\r\n\r\nRIFF\r\n--b--\r\n";
        let raw = send_request(server.addr(), &format!("POST /v1/rooms/team%2Fone/audio/transcriptions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: multipart/form-data; boundary=b\r\nContent-Length: {}\r\n\r\n{multipart}", multipart.len())).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        assert_eq!(
            serde_json::from_slice::<Value>(body).unwrap()["text"],
            "room transcript"
        );
        let raw = send_request(
            server.addr(),
            "GET /v1/rooms/missing/models HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 404);
        assert!(String::from_utf8_lossy(body).contains("enabled Room provider"));
        server.stop();
    }

    #[tokio::test]
    async fn room_image_requests_use_oai_and_adapt_buffered_responses_to_sse() {
        let (server, seen) = room_test_server().await;
        for stream in [false, true] {
            let raw = post_json(&server, "/v1/rooms/team%2Fone/chat/completions", json!({"model":"raw","reasoning_effort":"high","temperature":0.4,"stream":stream,
                "messages":[{"role":"user","content":[{"type":"text","text":"describe"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]})).await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            let payload = if stream {
                de_chunk(body)
            } else {
                body.to_vec()
            };
            assert!(String::from_utf8_lossy(&payload).contains("image answer"));
            let records = seen.lock().unwrap();
            let sent = records.last().unwrap();
            assert_eq!(sent["route"], "oai");
            assert_eq!(sent["body"]["reasoning_effort"], "high");
            assert_eq!(sent["body"]["stream"], false);
            assert!(sent["body"].get("temperature").is_none());
        }
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "image requests never use the plain chat callback"
        );
        server.stop();
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;

    fn fake_call_ok() -> LlmCallFn {
        Arc::new(|_messages, _tools, _model, delta_tx| {
            Box::pin(async move {
                if let Some(tx) = delta_tx {
                    let _ = tx.send("Hel".to_string());
                    let _ = tx.send("lo".to_string());
                }
                Ok("Hello".to_string().into())
            })
        })
    }

    fn fake_call_err() -> LlmCallFn {
        Arc::new(|_messages, _tools, _model, _delta_tx| {
            Box::pin(async move { Err(anyhow::anyhow!("upstream exploded")) })
        })
    }

    fn fake_call_midstream_err() -> LlmCallFn {
        Arc::new(|_messages, _tools, _model, delta_tx| {
            Box::pin(async move {
                if let Some(tx) = delta_tx {
                    let _ = tx.send("partial".to_string());
                }
                // Make the first delta observable before the failure so the
                // server has genuinely committed the 200/SSE response.
                tokio::task::yield_now().await;
                Err(anyhow::anyhow!("upstream failed after a delta"))
            })
        })
    }

    fn fake_models() -> ModelsFn {
        Arc::new(|| vec!["m1".to_string(), "m2".to_string()])
    }

    fn fake_tts() -> TtsFn {
        Arc::new(|params: tts::TtsParams, _lang: Option<String>| {
            Box::pin(async move {
                Ok(tts::TtsAudio {
                    // Echo the resolved voice back as the "audio", so a test
                    // can assert what actually reached the synthesizer.
                    bytes: format!("audio:{}:{}", params.voice, params.input).into_bytes(),
                    mime: "audio/mpeg".to_string(),
                })
            })
        })
    }

    fn fake_stt() -> SttFn {
        Arc::new(|params: stt::SttParams| {
            Box::pin(async move {
                Ok(format!(
                    "{}|{}|{}",
                    params.model,
                    params.file_name.unwrap_or_default(),
                    String::from_utf8_lossy(&params.audio)
                ))
            })
        })
    }

    async fn start_test_server(call: LlmCallFn, models: ModelsFn) -> Arc<ApiServer> {
        ApiServer::start("127.0.0.1:0", call, models, fake_tts(), fake_stt())
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

    #[test]
    fn multipart_boundary_is_read_quoted_or_bare() {
        assert_eq!(
            multipart_boundary("multipart/form-data; boundary=abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            multipart_boundary("multipart/form-data; charset=utf-8; boundary=\"a b\""),
            Some("a b".to_string())
        );
        // Case-insensitive, as headers are.
        assert_eq!(
            multipart_boundary("Multipart/Form-Data; BOUNDARY=xyz"),
            Some("xyz".to_string())
        );
        // Not multipart, or multipart with nothing to split on.
        assert!(multipart_boundary("application/json").is_none());
        assert!(multipart_boundary("multipart/form-data").is_none());
        assert!(multipart_boundary("multipart/form-data; boundary=").is_none());
    }

    /// The exact shape an OpenAI client sends this endpoint: one file part
    /// with a filename and content type, plus a plain text field.
    #[test]
    fn parse_multipart_reads_a_file_part_and_a_text_field() {
        let body = concat!(
            "--B\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"clip.wav\"\r\n",
            "Content-Type: audio/wav\r\n",
            "\r\n",
            "RIFFDATA\r\n",
            "--B\r\n",
            "Content-Disposition: form-data; name=\"model\"\r\n",
            "\r\n",
            "whisper-1\r\n",
            "--B--\r\n",
        )
        .as_bytes();

        let parts = parse_multipart(body, "B");
        assert_eq!(parts.len(), 2);

        let file = parts.iter().find(|p| p.name == "file").expect("file part");
        assert_eq!(file.file_name.as_deref(), Some("clip.wav"));
        assert_eq!(file.content_type, "audio/wav");
        // Byte-exact: an off-by-one in the delimiter handling would corrupt
        // every clip while still "parsing" successfully.
        assert_eq!(file.bytes, b"RIFFDATA");

        let model = parts
            .iter()
            .find(|p| p.name == "model")
            .expect("model part");
        assert_eq!(model.bytes, b"whisper-1");
    }

    /// Binary audio must survive verbatim — including bytes that look like
    /// CRLF or like the delimiter's own leading dashes.
    #[test]
    fn parse_multipart_preserves_binary_payloads() {
        let mut body = Vec::new();
        body.extend_from_slice(
            b"--B\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.bin\"\r\n\r\n",
        );
        let payload: Vec<u8> = vec![0x00, 0x0d, 0x0a, 0x2d, 0x2d, 0xff, 0x00, 0x1a];
        body.extend_from_slice(&payload);
        body.extend_from_slice(b"\r\n--B--\r\n");

        let parts = parse_multipart(&body, "B");
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].bytes, payload);
    }

    #[tokio::test]
    async fn audio_speech_returns_the_synthesized_bytes() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = json!({"model": "tts-1", "voice": "alloy", "input": "hi"}).to_string();
        let raw = send_request(
            server.addr(),
            &format!(
                "POST /v1/audio/speech HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
                payload.len()
            ),
        )
        .await;

        let (head, body) = split_response(&raw);
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        // Audio, not JSON — the upstream returns bytes and so does this.
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: audio/mpeg"),
            "{head}"
        );
        assert_eq!(body, b"audio:alloy:hi");
    }

    #[tokio::test]
    async fn audio_speech_rejects_a_request_with_no_input() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = json!({"model": "tts-1", "input": "   "}).to_string();
        let raw = send_request(
            server.addr(),
            &format!(
                "POST /v1/audio/speech HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
                payload.len()
            ),
        )
        .await;
        assert!(
            String::from_utf8_lossy(&raw).starts_with("HTTP/1.1 400 Bad Request"),
            "{}",
            String::from_utf8_lossy(&raw)
        );
    }

    #[tokio::test]
    async fn audio_transcriptions_reads_a_multipart_upload() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let body = concat!(
            "--B\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"clip.wav\"\r\n",
            "Content-Type: audio/wav\r\n",
            "\r\n",
            "RIFF\r\n",
            "--B\r\n",
            "Content-Disposition: form-data; name=\"model\"\r\n",
            "\r\n",
            "whisper-1\r\n",
            "--B--\r\n",
        );
        let raw = send_request(
            server.addr(),
            &format!(
                "POST /v1/audio/transcriptions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: multipart/form-data; boundary=B\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;

        let (head, payload) = split_response(&raw);
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        let json: Value = serde_json::from_slice(payload).expect("json body");
        // fake_stt echoes model|filename|bytes, so this asserts all three
        // survived the parse in one go.
        assert_eq!(json["text"], "whisper-1|clip.wav|RIFF");
    }

    #[tokio::test]
    async fn audio_transcriptions_requires_multipart() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let raw = send_request(
            server.addr(),
            "POST /v1/audio/transcriptions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
        )
        .await;
        assert!(
            String::from_utf8_lossy(&raw).starts_with("HTTP/1.1 400 Bad Request"),
            "{}",
            String::from_utf8_lossy(&raw)
        );
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
                assert_eq!(body, b"\r\n", "zero chunk must terminate with CRLF");
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
        let raw = send_request(
            server.addr(),
            "GET /v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .await;
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
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
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
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
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
    async fn remote_model_not_shared_is_not_reclassified_as_model_not_found() {
        let call: LlmCallFn = Arc::new(|_, _, _, _| {
            Box::pin(async {
                Err(anyhow::anyhow!(
                    "The requested model is not shared by this provider. (code: model_not_shared)"
                ))
            })
        });
        let server = start_test_server(call, fake_models()).await;
        let raw = post_json(
            &server,
            "/v1/chat/completions",
            json!({
                "model":"private", "messages":[{"role":"user","content":"hi"}]
            }),
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 502);
        let body: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["error"]["message"], BACKEND_ERROR_MESSAGE);
        assert!(body["error"]["code"].is_null());
        server.stop();
    }

    #[tokio::test]
    async fn post_chat_completions_stream_error_before_first_delta_is_502() {
        let server = start_test_server(fake_call_err(), fake_models()).await;
        let payload = json!({
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        })
        .to_string();
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );

        let raw = send_request(server.addr(), &request).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 502, "{head}");
        assert!(
            !head
                .to_ascii_lowercase()
                .contains("transfer-encoding: chunked"),
            "an immediate failure must not commit an SSE response: {head}"
        );
        let value: Value = serde_json::from_slice(body).expect("valid JSON error body");
        assert_eq!(value["error"]["message"], BACKEND_ERROR_MESSAGE);
        assert!(!String::from_utf8_lossy(body).contains("exploded"));
    }

    #[tokio::test]
    async fn post_chat_completions_midstream_error_emits_sse_error_and_terminates_chunking() {
        let server = start_test_server(fake_call_midstream_err(), fake_models()).await;
        let payload = json!({
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        })
        .to_string();
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );

        let raw = send_request(server.addr(), &request).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200, "{head}");
        assert!(
            head.to_ascii_lowercase()
                .contains("transfer-encoding: chunked"),
            "{head}"
        );

        // de_chunk also asserts that the terminating zero chunk is present;
        // the old behavior failed here with an incomplete chunk-size line.
        let text = String::from_utf8(de_chunk(body)).expect("SSE payload is UTF-8");
        let events: Vec<Value> = text
            .split("\n\n")
            .filter_map(|event| event.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str(data).ok())
            .collect();
        assert!(
            events
                .iter()
                .any(|event| { event["choices"][0]["delta"]["content"] == "partial" }),
            "expected the delta sent before failure: {text}"
        );
        assert!(
            events
                .iter()
                .any(|event| { event["error"]["message"] == BACKEND_ERROR_MESSAGE }),
            "expected a surfaced backend error: {text}"
        );
        assert!(
            !text.contains("data: [DONE]"),
            "a failed stream must not claim successful completion: {text}"
        );
    }

    #[tokio::test]
    async fn post_malformed_json_is_400() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = "not json";
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
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
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
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
        let raw = send_request(
            server.addr(),
            "GET /nope HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        )
        .await;
        let (head, _body) = split_response(&raw);
        assert_eq!(status_code(&head), 404);
    }

    #[tokio::test]
    async fn post_backend_error_non_stream_is_502() {
        let server = start_test_server(fake_call_err(), fake_models()).await;
        let payload = json!({"messages": [{"role": "user", "content": "hi"}]}).to_string();
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        let raw = send_request(server.addr(), &request).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 502);
        let json: Value = serde_json::from_slice(body).expect("valid json");
        assert_eq!(json["error"]["message"], BACKEND_ERROR_MESSAGE);
        assert!(!String::from_utf8_lossy(body).contains("exploded"));
    }

    #[tokio::test]
    async fn post_without_content_length_is_411() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let raw = send_request(
            server.addr(),
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
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
        let raw = send_request(addr, "GET /v1/models HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").await;
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

    fn local(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn host_check_accepts_loopback_and_local_ip_only() {
        let l = SocketAddr::from(([192, 168, 1, 5], 8080));
        assert!(host_is_allowed("127.0.0.1:8080", &l));
        assert!(host_is_allowed("localhost", &l));
        assert!(host_is_allowed("[::1]:8080", &l));
        assert!(host_is_allowed("192.168.1.5:8080", &l));
        assert!(!host_is_allowed("192.168.1.5:9", &l));
        assert!(!host_is_allowed("evil.example.com", &l));
        assert!(!host_is_allowed("", &l));
    }

    #[test]
    fn origin_check_requires_own_loopback_origin() {
        let l = local(8080);
        assert!(origin_is_allowed("http://127.0.0.1:8080", &l));
        assert!(origin_is_allowed("http://localhost:8080", &l));
        assert!(!origin_is_allowed("http://localhost:9999", &l));
        assert!(!origin_is_allowed("https://evil.example.com", &l));
        assert!(!origin_is_allowed("http://evil.example.com:8080", &l));
        assert!(!origin_is_allowed("null", &l));
    }

    #[test]
    fn json_content_type_accepts_params_and_rejects_others() {
        assert!(content_type_is_json(Some("application/json")));
        assert!(content_type_is_json(Some(
            "Application/JSON; charset=utf-8"
        )));
        assert!(!content_type_is_json(Some("text/plain")));
        assert!(!content_type_is_json(Some(
            "application/x-www-form-urlencoded"
        )));
        assert!(!content_type_is_json(None));
    }

    #[tokio::test]
    async fn cross_site_and_malformed_requests_are_refused() {
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let addr = server.addr();
        let body = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";
        let mk = |extra: &str, host: &str, ct: &str| {
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {host}\r\n{ct}{extra}Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let json = "Content-Type: application/json\r\n";
        // Foreign Origin.
        let raw = send_request(
            addr,
            &mk("Origin: http://evil.example.com\r\n", "127.0.0.1", json),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 403);
        // Rebinding Host.
        let raw = send_request(addr, &mk("", "evil.example.com", json)).await;
        assert_eq!(status_code(&split_response(&raw).0), 403);
        // Non-JSON content type (form-encoded CSRF).
        let raw = send_request(
            addr,
            &mk(
                "",
                "127.0.0.1",
                "Content-Type: application/x-www-form-urlencoded\r\n",
            ),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 415);
        // Duplicate Content-Length and Transfer-Encoding.
        let raw = send_request(addr, &mk("Content-Length: 1\r\n", "127.0.0.1", json)).await;
        assert_eq!(status_code(&split_response(&raw).0), 400);
        let raw = send_request(
            addr,
            &mk("Transfer-Encoding: chunked\r\n", "127.0.0.1", json),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 400);
        // Same-origin browser request is fine.
        let origin = format!("Origin: http://127.0.0.1:{}\r\n", addr.port());
        let raw = send_request(addr, &mk(&origin, "127.0.0.1", json)).await;
        assert_eq!(status_code(&split_response(&raw).0), 200);
    }

    // ---- tool calling -------------------------------------------------

    fn sample_calls() -> Value {
        json!([
            {"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Tokyo\"}"}},
            {"id":"call_2","type":"function","function":{"name":"get_time","arguments":"{}"}},
        ])
    }

    /// Asserts the call fn saw tools, and returns tool calls.
    fn fake_call_tools() -> LlmCallFn {
        Arc::new(|messages, tools, _model, delta_tx| {
            Box::pin(async move {
                assert!(tools.tools.is_some());
                assert_eq!(messages.last().unwrap().role, "tool");
                if let Some(tx) = delta_tx {
                    let _ = tx.send("Checking. ".to_string());
                }
                Ok(ChatOutput {
                    content: "Checking. ".to_string(),
                    tool_calls: Some(sample_calls()),
                })
            })
        })
    }

    fn tool_request_body(stream: bool) -> String {
        json!({
            "messages": [
                {"role": "user", "content": "weather?"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id":"c0","type":"function","function":{"name":"f","arguments":"{}"}}]},
                {"role": "tool", "tool_call_id": "c0", "content": "sunny"},
            ],
            "tools": [{"type":"function","function":{"name":"get_weather"}}],
            "tool_choice": "auto",
            "stream": stream,
        })
        .to_string()
    }

    fn post_chat(payload: &str) -> String {
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        )
    }

    #[test]
    fn parse_chat_request_accepts_tool_fields() {
        let parsed = parse_chat_request(tool_request_body(false).as_bytes()).unwrap();
        assert_eq!(parsed.messages.len(), 3);
        // `content: null` on a tool-calling assistant turn becomes "".
        assert_eq!(parsed.messages[1].content, "");
        assert!(parsed.messages[1].tool_calls.is_some());
        assert_eq!(parsed.messages[2].tool_call_id.as_deref(), Some("c0"));
        assert!(parsed.tools.tools.is_some());
        assert_eq!(parsed.tools.tool_choice, Some(json!("auto")));
        assert!(!parsed.stream);
    }

    #[test]
    fn parse_chat_request_plain_request_has_no_tool_fields() {
        let p = parse_chat_request(
            br#"{"messages":[{"role":"user","content":"hi"}],"tools":null,"tool_choice":null}"#,
        )
        .unwrap();
        assert!(p.tools.is_empty());
        assert!(!p.tools.request_uses_tools(&p.messages));
        // Empty tools array means no tools; a dangling tool_choice is dropped.
        let p = parse_chat_request(
            br#"{"messages":[{"role":"user","content":"hi"}],"tools":[],"tool_choice":"auto"}"#,
        )
        .unwrap();
        assert!(p.tools.is_empty());
    }

    #[test]
    fn parse_chat_request_rejects_bad_tool_input() {
        let msg = r#""messages":[{"role":"user","content":"hi"}]"#;
        for extra in [
            r#","tools":{}"#,
            r#","tools":"x""#,
            r#","tools":[{}],"tool_choice":5"#,
        ] {
            assert!(
                parse_chat_request(format!("{{{msg}{extra}}}").as_bytes()).is_err(),
                "{extra}"
            );
        }
        // content null only for assistant with tool_calls.
        assert!(parse_chat_request(br#"{"messages":[{"role":"user","content":null}]}"#).is_err());
        assert!(
            parse_chat_request(br#"{"messages":[{"role":"assistant","content":null}]}"#).is_err()
        );
        // tool message needs its id.
        assert!(parse_chat_request(br#"{"messages":[{"role":"tool","content":"x"}]}"#).is_err());
        // tool_calls must be an array.
        assert!(
            parse_chat_request(
                br#"{"messages":[{"role":"assistant","content":"","tool_calls":{}}]}"#
            )
            .is_err()
        );
        // Limits.
        let many: Vec<Value> = (0..=super::super::protocol::MAX_TOOLS)
            .map(|_| json!({"type":"function"}))
            .collect();
        let body = json!({"messages":[{"role":"user","content":"x"}],"tools":many}).to_string();
        assert!(parse_chat_request(body.as_bytes()).is_err());
        let fat = json!({"messages":[{"role":"user","content":"x"}],
            "tools":[{"d":"x".repeat(super::super::protocol::MAX_TOOLS_BYTES)}]})
        .to_string();
        assert!(parse_chat_request(fat.as_bytes()).is_err());
        let calls: Vec<Value> = (0..=super::super::protocol::MAX_TOOL_CALLS)
            .map(|_| json!({"id":"c"}))
            .collect();
        let body =
            json!({"messages":[{"role":"assistant","content":"","tool_calls":calls}]}).to_string();
        assert!(parse_chat_request(body.as_bytes()).is_err());
    }

    #[test]
    fn completion_message_shapes() {
        let plain = ChatOutput::from("hi".to_string());
        assert_eq!(
            completion_message(&plain),
            json!({"role":"assistant","content":"hi"})
        );
        assert_eq!(finish_reason(&plain), "stop");
        let only_calls = ChatOutput {
            content: String::new(),
            tool_calls: Some(sample_calls()),
        };
        let m = completion_message(&only_calls);
        assert_eq!(m["content"], Value::Null);
        assert_eq!(m["tool_calls"], sample_calls());
        assert_eq!(finish_reason(&only_calls), "tool_calls");
        let mixed = ChatOutput {
            content: "text".into(),
            tool_calls: Some(sample_calls()),
        };
        assert_eq!(completion_message(&mixed)["content"], "text");
    }

    #[tokio::test]
    async fn non_stream_tool_calls_response() {
        let server = start_test_server(fake_call_tools(), fake_models()).await;
        let raw = send_request(server.addr(), &post_chat(&tool_request_body(false))).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200, "{head}");
        let v: Value = serde_json::from_slice(body).unwrap();
        let choice = &v["choices"][0];
        assert_eq!(choice["finish_reason"], "tool_calls");
        assert_eq!(choice["message"]["role"], "assistant");
        assert_eq!(choice["message"]["content"], "Checking. ");
        assert_eq!(choice["message"]["tool_calls"], sample_calls());
    }

    #[tokio::test]
    async fn stream_tool_calls_come_after_text_in_one_chunk() {
        let server = start_test_server(fake_call_tools(), fake_models()).await;
        let raw = send_request(server.addr(), &post_chat(&tool_request_body(true))).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200, "{head}");
        let text = String::from_utf8(de_chunk(body)).unwrap();
        let events: Vec<&str> = text
            .split("\n\n")
            .filter(|s| !s.is_empty())
            .filter_map(|e| e.strip_prefix("data: "))
            .collect();
        assert_eq!(*events.last().unwrap(), "[DONE]");
        let chunks: Vec<Value> = events[..events.len() - 1]
            .iter()
            .map(|e| serde_json::from_str(e).unwrap())
            .collect();
        assert_eq!(chunks.len(), 3, "{text}");
        assert_eq!(chunks[0]["choices"][0]["delta"]["content"], "Checking. ");
        let tc = &chunks[1]["choices"][0];
        assert_eq!(tc["finish_reason"], Value::Null);
        let deltas = tc["delta"]["tool_calls"].as_array().unwrap();
        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0]["index"], 0);
        assert_eq!(deltas[1]["index"], 1);
        assert_eq!(deltas[0]["id"], "call_1");
        assert_eq!(deltas[0]["type"], "function");
        assert_eq!(deltas[0]["function"]["name"], "get_weather");
        assert_eq!(deltas[0]["function"]["arguments"], "{\"city\":\"Tokyo\"}");
        let last = &chunks[2]["choices"][0];
        assert_eq!(last["delta"], json!({}));
        assert_eq!(last["finish_reason"], "tool_calls");
    }

    fn fake_call_tools_unsupported() -> LlmCallFn {
        Arc::new(|_m, _t, _model, _tx| {
            Box::pin(async { Err(anyhow::Error::new(ToolsUnsupported)) })
        })
    }

    #[tokio::test]
    async fn tools_unsupported_is_a_400_before_streaming() {
        let server = start_test_server(fake_call_tools_unsupported(), fake_models()).await;
        for stream in [false, true] {
            let raw = send_request(server.addr(), &post_chat(&tool_request_body(stream))).await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 400, "{head}");
            assert!(
                !head
                    .to_ascii_lowercase()
                    .contains("transfer-encoding: chunked"),
                "{head}"
            );
            let v: Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                v,
                json!({"error":{
                    "message":"the AI provider on the network does not support tools",
                    "type":"invalid_request_error",
                    "code":"tools_unsupported"}})
            );
        }
    }

    #[tokio::test]
    async fn other_backend_errors_stay_generic_502() {
        let server = start_test_server(fake_call_err(), fake_models()).await;
        let raw = send_request(server.addr(), &post_chat(&tool_request_body(false))).await;
        let (head, _) = split_response(&raw);
        assert_eq!(status_code(&head), 502, "{head}");
    }
}
