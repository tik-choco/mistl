//! Local OpenAI-compatible HTTP API server ("use the network as if it were
//! an API server"): apps point their OpenAI base URL at this listener and
//! requests are answered by the p2p network (or the local provider).
//!
//! Hand-rolled minimal HTTP/1.1 over tokio TCP -- same spirit as the
//! project's RTSP and IPC servers; no hyper/axum. Token auth is optional
//! by default: `mst_` bearer tokens enforce per-token models and usage limits;
//! `authz.api.require_token` requires a live token for every API request.
//! Invalid tokens, disallowed models, and exhausted limits return 401, 403,
//! and 429 respectively. CORS preflights do not require a token.
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
//! - `POST /v1/systemone` / `POST /v1/rooms/<room>/systemone` -> a System
//!   One decision request (`{model?, state, questions, images?,
//!   keep_alive?}`, as Ollama's `/v1/systemone`), relayed unchanged to the
//!   upstream: unscoped, the provider `model` resolves to (an HTTP
//!   provider's `<base_url>/systemone`, or a Room provider's tunnel);
//!   room-scoped, over that room's oai tunnel. The upstream status and JSON
//!   body come back as-is.
//! - 3D generation jobs (`/v1/3d/generations`), voice catalogs
//!   (`/v1/audio/voices`, `/v1/voices`), and model details (`/v1/models/<id>`)
//!   are also available under `/v1/rooms/<room>`. Job content is downloaded
//!   in bounded windows; synchronous `b64_json` results are capped at 64 MiB.
//! - `POST /v1/images/edits` (also room-scoped) relays JSON or multipart
//!   image edits, returning the upstream JSON or image bytes unchanged.
//! - Game actions and motion endpoints also relay through both scopes.
//!   `/motion/clips` takes its routing model from the query string and
//!   `/motion/skeletons/<model>` from the path segment.
//! - Unscoped lifecycle routes `/api/ps`, `/health`, and the
//!   root `/voices` alias use the injected local relay.
//! - Unresolvable explicit models -> `404` with OpenAI `model_not_found`.
//! - Upstream/backend errors: before any deltas were streamed -> `502` with
//!   `{"error":{"message":...}}` carrying a generic message (the detail is
//!   logged only, as it can embed upstream URLs/tokens) (or `400` for malformed requests); once
//!   streaming has begun, emit an SSE `{"error":...}` event and terminate
//!   the HTTP chunked body cleanly (without a success `[DONE]` event).
//! - Anything else -> `404`.
//!
//! ## Browser callers (CORS)
//!
//! A request carrying a foreign `Origin` is refused with `403` unless that
//! origin is listed in `ai.api_allowed_origins` (read per request, compared
//! after [`crate::config::normalize_origin`]). For a listed origin, `OPTIONS`
//! preflights get `204` with the CORS grant (plus
//! `Access-Control-Allow-Private-Network` when asked, for https pages
//! calling loopback), and every response carries `Access-Control-Allow-Origin`
//! echoing it. No wildcard: any page in the user's browser could otherwise
//! spend their providers.
//!
//! Audio endpoints resolve ai.tts/ai.stt directly. HTTP references call their
//! endpoint; Room references use the room's advertised voice service.
//!
//! ## HTTP parsing (keep it minimal but correct)
//!
//! Read the request line + headers (case-insensitive names) up to a sane
//! cap, then exactly `Content-Length` body bytes (411 if missing on POST;
//! route-specific caps for JSON, audio, 3D and image uploads). `Connection: close`
//! semantics -- serve one request per connection, then close. Tolerate both
//! `\r\n` and `\n`.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use base64::Engine;
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

/// Decision relay. Takes the client's raw JSON body so nested key order
/// (meaningful for `choice` criteria) reaches the upstream unchanged.
pub(super) type DecisionFn = Arc<
    dyn Fn(
            Vec<u8>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<super::oai_tunnel::TunnelResponse>> + Send>,
        > + Send
        + Sync,
>;

/// One request for the OpenAI and tc-ai-api passthrough routes.
/// `path` is relative to the `/v1` base
/// (`/3d/generations/<id>/content?range=0-8388607`, `/audio/voices`) except
/// the root lifecycle paths (`/api/ps`, `/health`), which are
/// passed as-is. `body` is JSON when present.
pub(super) struct RelayRequest {
    pub method: String,
    pub path: String,
    pub body: Option<Vec<u8>>,
}

/// Relays a [`RelayRequest`]; the reply is the upstream status, content type
/// and body, buffered (content downloads are fetched in range windows).
pub(super) type RelayFn = Arc<
    dyn Fn(
            RelayRequest,
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
    /// `POST /systemone` over the room's oai tunnel.
    pub decision: DecisionFn,
    /// Image edits, 3D generation jobs and the voice catalog over the room.
    pub relay: RelayFn,
}

/// Extra browser origins allowed to call the API (`ai.api_allowed_origins`).
pub(super) type OriginsFn = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

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
const MAX_GEN3D_MULTIPART_BYTES: usize = 20 * 1024 * 1024 + 128 * 1024;
const MAX_GEN3D_JSON_BYTES: usize = 20 * 1024 * 1024 / 3 * 4 + 128 * 1024;
const MAX_IMAGE_EDIT_MULTIPART_BYTES: usize = 32 * 1024 * 1024 + 128 * 1024;
const MAX_IMAGE_EDIT_JSON_BYTES: usize = 32 * 1024 * 1024 / 3 * 4 + 128 * 1024;
const MAX_GEN3D_B64_BYTES: u64 = 64 * 1024 * 1024;
/// Deadline for receiving the complete request head (slowloris guard).
const HEAD_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Max idle gap between body reads.
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Concurrent connections served; extra ones are dropped at accept.
const MAX_CONNECTIONS: usize = 32;
/// Preflight cache lifetime granted to allowed origins.
const CORS_MAX_AGE_SECS: u32 = 600;
/// Cap on an echoed `Access-Control-Request-Headers` list.
const MAX_CORS_REQUEST_HEADERS_BYTES: usize = 1024;

tokio::task_local! {
    /// CORS header lines for the request being served: empty unless its
    /// `Origin` is in `ai.api_allowed_origins`. Every response head appends
    /// [`cors_headers`], so no write site can forget the grant.
    static CORS_HEADERS: String;
}

fn cors_headers() -> String {
    CORS_HEADERS.try_with(String::clone).unwrap_or_default()
}

fn sse_response_header() -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nCache-Control: no-cache\r\n{}Connection: close\r\n\r\n",
        cors_headers()
    )
}

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
        Self::start_with_rooms(
            listen, call, models, tts_call, stt_call, None, None, None, None,
        )
        .await
    }

    /// `decision` serves the unscoped `/v1/systemone` (`None`: 404);
    /// `relay` the unscoped tc-ai-api passthrough routes (`None`: 404).
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_rooms(
        listen: &str,
        call: LlmCallFn,
        models: ModelsFn,
        tts_call: TtsFn,
        stt_call: SttFn,
        decision: Option<DecisionFn>,
        relay: Option<RelayFn>,
        rooms: Option<RoomsFn>,
        origins: Option<OriginsFn>,
    ) -> Result<Arc<ApiServer>> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding api server to {listen}"))?;
        let addr = listener
            .local_addr()
            .context("reading bound api server address")?;
        if !addr.ip().to_canonical().is_loopback() && !crate::authz::api_token_required() {
            warn!(
                "local API is reachable from the network without token auth; set authz api require_token"
            );
        }

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
                let decision = decision.clone();
                let relay = relay.clone();
                let rooms = rooms.clone();
                let origins = origins.clone();
                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    let local = LocalRoutes {
                        call,
                        models,
                        tts_call,
                        stt_call,
                        decision,
                        relay,
                    };
                    if let Err(error) = handle_connection(socket, local, rooms, origins).await {
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

/// The request's `Origin` when it is one of `allowed` (CORS grant), compared
/// in normalized form; `None` for anything else.
fn cors_origin<'a>(head: &'a RequestHead, allowed: &[String]) -> Option<&'a str> {
    let origin = head.header("origin")?.trim();
    let normalized = crate::config::normalize_origin(origin)?;
    allowed
        .iter()
        .any(|a| crate::config::normalize_origin(a).as_deref() == Some(normalized.as_str()))
        .then_some(origin)
}

/// Request-level guards run before any routing: request-smuggling shapes,
/// DNS rebinding (`Host`), and cross-site browser requests (`Origin`, unless
/// listed in `allowed_origins`). `Ok` carries the origin to grant CORS to.
/// `Err` carries `(status, reason, message)`.
fn check_request_guards<'a>(
    head: &'a RequestHead,
    local_addr: &SocketAddr,
    allowed_origins: &[String],
) -> Result<Option<&'a str>, (u16, &'static str, &'static str)> {
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
        if origin_is_allowed(origin, local_addr) {
            return Ok(None);
        }
        return match cors_origin(head, allowed_origins) {
            Some(origin) => Ok(Some(origin)),
            None => Err((403, "Forbidden", "origin not allowed")),
        };
    }
    Ok(None)
}

/// The `Access-Control-Allow-Headers` value for a preflight: the requested
/// list echoed when it is a plain header-name list (the OpenAI JS SDK sends
/// several `x-stainless-*` headers besides `authorization`), else the basics.
fn cors_allow_headers(head: &RequestHead) -> &str {
    match head.header("access-control-request-headers") {
        Some(v)
            if !v.trim().is_empty()
                && v.len() <= MAX_CORS_REQUEST_HEADERS_BYTES
                && v.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_, ".contains(&b)) =>
        {
            v.trim()
        }
        _ => "authorization, content-type",
    }
}

/// `204` answer to a CORS preflight from an allowed origin.
async fn write_preflight(stream: &mut TcpStream, head: &RequestHead) -> io::Result<()> {
    // Chromium's Private Network Access asks before an https page may reach
    // loopback; the grant is only sent when asked.
    let private_network = head
        .header("access-control-request-private-network")
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
    let response = format!(
        "HTTP/1.1 204 No Content\r\n{}Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: {}\r\nAccess-Control-Max-Age: {CORS_MAX_AGE_SECS}\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
        cors_headers(),
        cors_allow_headers(head),
        if private_network {
            "Access-Control-Allow-Private-Network: true\r\n"
        } else {
            ""
        },
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

async fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &Value,
) -> io::Result<()> {
    write_json_response_with_headers(stream, status, reason, body, "").await
}

async fn write_json_response_with_headers(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &Value,
    headers: &str,
) -> io::Result<()> {
    let body_bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}{headers}Connection: close\r\n\r\n",
        body_bytes.len(),
        cors_headers()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body_bytes).await?;
    stream.flush().await
}

/// What local clients see for any upstream/backend failure. The real error
/// (which can embed the upstream URL, a token in a query string, or response
/// bodies) goes to the log only, except actionable TTS client/503 errors.
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

async fn write_denied(stream: &mut TcpStream, denied: crate::authz::Denied) -> io::Result<()> {
    let status = denied.http_status();
    let (reason, headers) = match status {
        401 => ("Unauthorized", "WWW-Authenticate: Bearer\r\n"),
        429 => ("Too Many Requests", ""),
        _ => ("Forbidden", ""),
    };
    write_json_response_with_headers(
        stream,
        status,
        reason,
        &json!({"error": {
            "message": denied.message,
            "type": "invalid_request_error",
            "code": denied.code,
        }}),
        headers,
    )
    .await
}

/// Charge each backend attempt once, including partial output on stream or
/// socket failure. Parsing and authorization failures never start an attempt.
struct ApiUsage {
    authorized: crate::authz::Authorized,
    started: bool,
    output_chars: u64,
}

impl ApiUsage {
    fn new(authorized: crate::authz::Authorized) -> Self {
        Self {
            authorized,
            started: false,
            output_chars: 0,
        }
    }
}

impl Drop for ApiUsage {
    fn drop(&mut self) {
        if self.started {
            self.authorized.record(1, self.output_chars);
        }
    }
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

fn models_response(models: &ModelsFn, allowed: Option<&[String]>) -> Value {
    let list = models();
    let data: Vec<Value> = list
        .into_iter()
        .filter(|id| allowed.is_none_or(|allowed| allowed.contains(id)))
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
    if let Some(upstream) = error
        .downcast_ref::<super::openai::UpstreamHttpError>()
        .filter(|e| e.label == "TTS API" && ((400..500).contains(&e.status) || e.status == 503))
    {
        let body = serde_json::from_str::<Value>(&upstream.detail)
            .ok()
            .filter(|v| v.get("error").is_some())
            .unwrap_or_else(|| {
                json!({"error": {
                    "message": upstream.detail, "type": "invalid_request_error"
                }})
            });
        let _ = write_json_response(stream, upstream.status, "Upstream Error", &body).await;
    } else if let Some(error) = error.downcast_ref::<ModelNotFound>() {
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

/// Handlers for the unscoped `/v1/...` routes.
struct LocalRoutes {
    call: LlmCallFn,
    models: ModelsFn,
    tts_call: TtsFn,
    stt_call: SttFn,
    decision: Option<DecisionFn>,
    relay: Option<RelayFn>,
}

async fn handle_connection(
    mut stream: TcpStream,
    local: LocalRoutes,
    rooms: Option<RoomsFn>,
    origins: Option<OriginsFn>,
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

    let allowed_origins = origins.map(|f| f()).unwrap_or_default();
    let cors = match check_request_guards(&head, &local_addr, &allowed_origins) {
        Ok(Some(origin)) => {
            format!("Access-Control-Allow-Origin: {origin}\r\nVary: Origin\r\n")
        }
        Ok(None) => String::new(),
        Err((status, reason, message)) => {
            let _ = write_error(&mut stream, status, reason, message).await;
            return Ok(());
        }
    };
    let preflight = !cors.is_empty() && head.method.eq_ignore_ascii_case("OPTIONS");
    CORS_HEADERS
        .scope(cors, async move {
            if preflight {
                let _ = write_preflight(&mut stream, &head).await;
                return Ok(());
            }
            route_request(stream, head, leftover, local, rooms).await
        })
        .await
}

/// Routes a request that passed [`check_request_guards`].
async fn route_request(
    mut stream: TcpStream,
    head: RequestHead,
    leftover: Vec<u8>,
    local: LocalRoutes,
    rooms: Option<RoomsFn>,
) -> Result<()> {
    let is_post = head.method.eq_ignore_ascii_case("POST");
    let is_get = head.method.eq_ignore_ascii_case("GET");
    let mut path = head.path.split('?').next().unwrap_or("").to_string();
    let scoped = path.starts_with("/v1/rooms/");
    let url_base = if scoped {
        format!(
            "/v1/rooms/{}",
            path[10..].split('/').next().unwrap_or_default()
        )
    } else {
        "/v1".to_string()
    };
    let (call, models, tts_call, stt_call, oai, decision, relay) = if scoped {
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
            Some(routes.decision),
            Some(routes.relay),
        )
    } else {
        (
            local.call,
            local.models,
            local.tts_call,
            local.stt_call,
            None,
            local.decision,
            local.relay,
        )
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
        let is_gen3d = path == "/v1/3d/generations";
        let has_image_upload = matches!(path.as_str(), "/v1/images/edits" | "/v1/game/act");
        let is_multipart =
            multipart_boundary(head.header("content-type").unwrap_or_default()).is_some();
        let body_cap = if is_transcription {
            MAX_BODY_BYTES
        } else if is_gen3d && is_multipart {
            MAX_GEN3D_MULTIPART_BYTES
        } else if is_gen3d {
            MAX_GEN3D_JSON_BYTES
        } else if has_image_upload && is_multipart {
            MAX_IMAGE_EDIT_MULTIPART_BYTES
        } else if has_image_upload {
            MAX_IMAGE_EDIT_JSON_BYTES
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
        if !(is_transcription
            || (is_gen3d || has_image_upload) && is_multipart
            || content_type_is_json(head.header("content-type")))
        {
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
        let allowed = match crate::authz::api_model_filter(head.header("authorization")) {
            Ok(allowed) => allowed,
            Err(denied) => {
                write_denied(&mut stream, denied).await?;
                return Ok(());
            }
        };
        let body = models_response(&models, allowed.as_deref());
        let _ = write_json_response(&mut stream, 200, "OK", &body).await;
    } else if is_get && path.starts_with("/v1/models/") {
        if let Err(denied) = crate::authz::authorize_api(head.header("authorization"), None) {
            write_denied(&mut stream, denied).await?;
            return Ok(());
        }
        let allowed = match crate::authz::api_model_filter(head.header("authorization")) {
            Ok(allowed) => allowed,
            Err(denied) => {
                write_denied(&mut stream, denied).await?;
                return Ok(());
            }
        };
        let id = percent_decode(&path[11..]);
        let list = models_response(&models, allowed.as_deref());
        let model = list["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| id.as_deref().is_some_and(|id| m["id"].as_str() == Some(id)));
        if let Some(model) = model {
            write_json_response(&mut stream, 200, "OK", model).await?;
        } else {
            write_json_response(&mut stream, 404, "Not Found", &json!({"error": {
                "message": "Model not found", "type": "invalid_request_error", "code": "model_not_found"
            }})).await?;
        }
    } else if relay_path(&head.method, &path).is_some() {
        handle_relay(
            &mut stream,
            &head,
            &path,
            body,
            relay.as_ref(),
            scoped,
            &url_base,
        )
        .await?;
    } else if is_post && path == "/v1/chat/completions" {
        handle_chat_completions_with_oai(
            &mut stream,
            &body,
            &call,
            oai.as_ref(),
            relay.as_ref().filter(|_| !scoped),
            head.header("authorization"),
        )
        .await?;
    } else if is_post && path == "/v1/audio/speech" {
        handle_audio_speech(&mut stream, &body, &tts_call, head.header("authorization")).await?;
    } else if is_post && path == "/v1/audio/transcriptions" {
        let content_type = head.header("content-type").unwrap_or_default().to_string();
        handle_audio_transcriptions(
            &mut stream,
            &content_type,
            &body,
            &stt_call,
            head.header("authorization"),
        )
        .await?;
    } else if is_post && path == "/v1/systemone" {
        handle_decision(
            &mut stream,
            &body,
            decision.as_ref(),
            head.header("authorization"),
        )
        .await?;
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
        "models"
            | "chat/completions"
            | "audio/speech"
            | "audio/transcriptions"
            | "systemone"
            | "audio/voices"
            | "voices"
            | "3d/generations"
            | "images/edits"
            | "game/act"
            | "motion/inbetween"
            | "motion/clips"
    ) && !endpoint.starts_with("models/")
        && !endpoint.starts_with("3d/generations/")
        && !endpoint.starts_with("motion/skeletons/")
    {
        return None;
    }
    let room = percent_decode(encoded)?;
    if room.is_empty() {
        return None;
    }
    Some((room, endpoint.into()))
}

pub(super) fn percent_decode(encoded: &str) -> Option<String> {
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
    String::from_utf8(decoded).ok()
}

fn relay_path<'a>(method: &str, path: &'a str) -> Option<&'a str> {
    match (method.to_ascii_uppercase().as_str(), path) {
        ("GET", "/v1/audio/voices" | "/v1/voices" | "/voices") => Some("/audio/voices"),
        ("GET", "/api/ps" | "/health") => Some(path),
        ("GET" | "POST", "/v1/3d/generations") => Some("/3d/generations"),
        ("POST", "/v1/images/edits" | "/v1/game/act" | "/v1/motion/inbetween")
        | ("GET", "/v1/motion/clips") => Some(&path[3..]),
        ("GET", p) if p.starts_with("/v1/motion/skeletons/") => Some(&p[3..]),
        ("GET" | "DELETE", p) if p.starts_with("/v1/3d/generations/") => Some(&p[3..]),
        _ => None,
    }
}

fn valid_generation_id(id: &str) -> bool {
    id.strip_prefix("gen3d_").is_some_and(|hex| {
        hex.len() == 32
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

fn generation_content_path(url: &str) -> Option<String> {
    let (_, tail) = url.rsplit_once("/3d/generations/")?;
    let id = tail.strip_suffix("/content")?;
    valid_generation_id(id).then(|| format!("/3d/generations/{id}/content"))
}

/// Rewrite only job data, including jobs nested in the list response.
fn rewrite_job_urls(value: &mut Value, base: &str) {
    if let Some(data) = value.get_mut("data").and_then(Value::as_array_mut) {
        for item in data {
            if let Some(path) = item
                .get("url")
                .and_then(Value::as_str)
                .and_then(generation_content_path)
            {
                item["url"] = json!(format!("{base}{path}"));
            }
            rewrite_job_urls(item, base);
        }
    }
}

fn multipart_json(
    body: &[u8],
    boundary: &str,
    string_fields: &[&str],
) -> std::result::Result<Vec<u8>, &'static str> {
    let mut object = serde_json::Map::new();
    for part in parse_multipart(body, boundary) {
        if object.contains_key(&part.name) {
            return Err("duplicate multipart field");
        }
        let value = if part.name == "image" {
            let mime = if part.content_type.is_empty() {
                "image/png"
            } else {
                &part.content_type
            };
            json!(format!(
                "data:{mime};base64,{}",
                base64::engine::general_purpose::STANDARD.encode(&part.bytes)
            ))
        } else {
            if part.bytes.len() > 64 * 1024 {
                return Err("multipart field too large");
            }
            let text = std::str::from_utf8(&part.bytes).map_err(|_| "invalid multipart text")?;
            if matches!(
                part.name.as_str(),
                "keep_alive" | "model" | "output_format" | "response_format" | "user"
            ) || string_fields.contains(&part.name.as_str())
            {
                json!(text)
            } else {
                serde_json::from_str::<Value>(text)
                    .ok()
                    .filter(|v| v.is_number() || v.is_boolean())
                    .unwrap_or_else(|| json!(text))
            }
        };
        object.insert(part.name, value);
    }
    if !object.contains_key("image") {
        return Err("image is required");
    }
    Ok(serde_json::to_vec(&object).expect("JSON object"))
}

async fn relay_get(relay: &RelayFn, path: String) -> Result<super::oai_tunnel::TunnelResponse> {
    relay(RelayRequest {
        method: "GET".into(),
        path,
        body: None,
    })
    .await
}

fn content_relay_error(error: &anyhow::Error) -> anyhow::Error {
    // reqwest errors embed the upstream URL; peer error text is untrusted.
    // Log only the error kind, or the outermost message, sanitized and capped.
    if let Some(request_error) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<reqwest::Error>())
    {
        let kind = if request_error.is_timeout() {
            "timed out"
        } else if request_error.is_connect() {
            "connection failed"
        } else {
            "request failed"
        };
        return anyhow::anyhow!("content window relay failed: HTTP {kind}");
    }
    let message: String = error
        .to_string()
        .chars()
        .filter(|c| !c.is_control())
        .take(200)
        .collect();
    anyhow::anyhow!("content window relay failed: {message}")
}

async fn content_window(relay: &RelayFn, path: &str, start: u64, bytes: u64) -> Result<Vec<u8>> {
    let end = start
        .saturating_add(super::oai_tunnel::CONTENT_WINDOW)
        .min(bytes)
        - 1;
    for attempt in 1..=3 {
        let error = match relay_get(relay, format!("{path}?range={start}-{end}")).await {
            Err(error) => content_relay_error(&error),
            Ok(response) if (500..600).contains(&response.status) => {
                anyhow::anyhow!("content window returned HTTP {}", response.status)
            }
            Ok(response) if response.status != 206 && response.status != 200 => {
                anyhow::bail!("invalid content window status: HTTP {}", response.status);
            }
            Ok(response) if response.body.len() as u64 != end - start + 1 => {
                anyhow::anyhow!(
                    "invalid content window length: expected {}, received {}",
                    end - start + 1,
                    response.body.len()
                )
            }
            Ok(response) if response.status == 200 && (start != 0 || end != bytes - 1) => {
                anyhow::bail!("invalid content window status: HTTP {}", response.status);
            }
            Ok(response) => return Ok(response.body),
        };
        if attempt == 3 {
            return Err(error);
        }
        #[cfg(not(test))]
        let delay = Duration::from_secs(attempt);
        #[cfg(test)]
        let delay = Duration::from_millis(attempt);
        tokio::time::sleep(delay).await;
    }
    unreachable!("content window attempts are nonzero")
}

async fn serve_generation_content(
    stream: &mut TcpStream,
    relay: &RelayFn,
    path: &str,
) -> Result<()> {
    let job_path = path.strip_suffix("/content").expect("content route");
    let response = match relay_get(relay, job_path.to_string()).await {
        Ok(response) => response,
        Err(err) => {
            write_backend_failure(stream, &err).await;
            return Ok(());
        }
    };
    if response.status != 200 {
        return write_tunnel_response(stream, &response).await;
    }
    let job: Value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
    let bytes = job["data"][0]["bytes"].as_u64().unwrap_or(0);
    if job["status"] != "completed" || bytes == 0 {
        return match relay_get(relay, path.to_string()).await {
            Ok(response) => write_tunnel_response(stream, &response).await,
            Err(err) => {
                write_backend_failure(stream, &err).await;
                Ok(())
            }
        };
    }
    let (format, mime) = match job["data"][0]["format"].as_str() {
        Some("glb") => ("glb", "model/gltf-binary"),
        Some("obj") => ("obj", "model/obj"),
        Some("stl") => ("stl", "model/stl"),
        Some("3mf") => ("3mf", "model/3mf"),
        Some("dae") => ("dae", "model/vnd.collada+xml"),
        _ => ("ply", "application/octet-stream"),
    };
    let id = job_path.rsplit('/').next().unwrap();
    stream.write_all(format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nContent-Length: {bytes}\r\nContent-Disposition: attachment; filename=\"{id}.{format}\"\r\n{}Connection: close\r\n\r\n", cors_headers()
    ).as_bytes()).await?;
    let mut start = 0;
    while start < bytes {
        // Errors after committing the headers close the socket, never append JSON.
        let window = match content_window(relay, path, start, bytes).await {
            Ok(window) => window,
            Err(err) => {
                let safe_job_path = job_path
                    .strip_prefix("/3d/generations/")
                    .filter(|id| valid_generation_id(id))
                    .map(|_| job_path)
                    .unwrap_or("<invalid job path>");
                warn!(
                    "api_server: content download failed for {safe_job_path} at byte {start}: {err:#}"
                );
                return Ok(());
            }
        };
        stream.write_all(&window).await?;
        start += window.len() as u64;
    }
    Ok(())
}

async fn inline_generation_content(relay: &RelayFn, job: &mut Value) -> Result<()> {
    let id = job
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_generation_id(id))
        .map(str::to_string);
    let data = job
        .get_mut("data")
        .and_then(Value::as_array_mut)
        .context("missing generation data")?;
    let total = data.iter().try_fold(0u64, |total, item| {
        total.checked_add(item["bytes"].as_u64()?)
    });
    anyhow::ensure!(
        total.is_some_and(|n| n <= MAX_GEN3D_B64_BYTES),
        "generation content too large or size unknown; use response_format=url"
    );
    for item in data {
        let path = item
            .get("url")
            .and_then(Value::as_str)
            .and_then(generation_content_path)
            .or_else(|| {
                id.as_ref()
                    .map(|id| format!("/3d/generations/{id}/content"))
            })
            .context("missing generation content URL")?;
        let bytes = item["bytes"].as_u64().unwrap();
        let mut content = Vec::with_capacity(bytes as usize);
        while (content.len() as u64) < bytes {
            content.extend(content_window(relay, &path, content.len() as u64, bytes).await?);
        }
        let object = item.as_object_mut().context("invalid generation data")?;
        object.remove("url");
        object.insert(
            "b64_json".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(content)),
        );
    }
    Ok(())
}

async fn handle_relay(
    stream: &mut TcpStream,
    head: &RequestHead,
    path: &str,
    mut body: Vec<u8>,
    relay: Option<&RelayFn>,
    scoped: bool,
    url_base: &str,
) -> Result<()> {
    use super::oai_tunnel;

    let method = head.method.to_ascii_uppercase();
    let path = relay_path(&method, path).expect("relay route");
    let gen3d_post = method == "POST" && path == "/3d/generations";
    let image_edit_post = method == "POST" && path == "/images/edits";
    let game_post = method == "POST" && path == "/game/act";
    let req = if method == "POST" {
        if (gen3d_post || image_edit_post || game_post)
            && let Some(boundary) =
                multipart_boundary(head.header("content-type").unwrap_or_default())
        {
            let string_fields: &[&str] = if image_edit_post {
                &["background"]
            } else if game_post {
                &["system_prompt"]
            } else {
                &[]
            };
            body = match multipart_json(&body, &boundary, string_fields) {
                Ok(body) => body,
                Err(message) => {
                    write_error(stream, 400, "Bad Request", message).await?;
                    return Ok(());
                }
            };
        }
        match serde_json::from_slice::<Value>(&body) {
            Ok(value @ Value::Object(_)) => Some(value),
            _ => {
                write_error(
                    stream,
                    400,
                    "Bad Request",
                    "request body must be a JSON object",
                )
                .await?;
                return Ok(());
            }
        }
    } else {
        None
    };
    let path = if path == "/motion/clips"
        && let Some((_, query)) = head.path.split_once('?')
    {
        format!("{path}?{query}")
    } else {
        path.to_string()
    };
    let catalog_model =
        oai_tunnel::is_motion_catalog(&path).then(|| oai_tunnel::motion_catalog_model(&path));
    let model = match &catalog_model {
        Some(result) => result.as_ref().ok().map(String::as_str),
        None => req
            .as_ref()
            .and_then(|v| v.get("model"))
            .and_then(Value::as_str)
            .filter(|m| !m.trim().is_empty()),
    };
    let authorized = match crate::authz::authorize_api(head.header("authorization"), model) {
        Ok(authorized) => authorized,
        Err(denied) => {
            write_denied(stream, denied).await?;
            return Ok(());
        }
    };
    // Authenticate first; then require an explicit model (no built-in default).
    if let Some(Err(response)) = catalog_model {
        return write_tunnel_response(stream, &response).await;
    }
    if method == "POST" && model.is_none() {
        return write_tunnel_response(stream, &oai_tunnel::missing_model()).await;
    }
    if let Some(tail) = path.strip_prefix("/3d/generations/") {
        let id = tail.strip_suffix("/content").unwrap_or(tail);
        if !valid_generation_id(id) || (method == "DELETE" && tail.ends_with("/content")) {
            write_json_response(stream, 404, "Not Found", &json!({"error": {
                "message": "generation not found", "type": "invalid_request_error", "code": "not_found", "param": "id"
            }})).await?;
            return Ok(());
        }
    }
    let Some(relay) = relay else {
        write_error(stream, 404, "Not Found", "not found").await?;
        return Ok(());
    };
    if scoped && body.len() > super::oai_tunnel::MAX_BYTES {
        write_error(
            stream,
            413,
            "Payload Too Large",
            "request body too large for room relay",
        )
        .await?;
        return Ok(());
    }
    let b64 = gen3d_post
        && req
            .as_ref()
            .is_some_and(|v| v["response_format"] == "b64_json" && v["background"] != true);
    if b64 {
        body = super::oai_tunnel::rewrite_object(&body, &[("response_format", json!("url"))], &[])?;
    }
    let mut usage = ApiUsage::new(authorized);
    usage.started = method == "POST";
    if method == "GET" && path.starts_with("/3d/generations/") && path.ends_with("/content") {
        return serve_generation_content(stream, relay, &path).await;
    }
    let response = relay(RelayRequest {
        method: method.clone(),
        path: path.clone(),
        body: (!body.is_empty()).then_some(body),
    })
    .await;
    match response {
        Ok(mut response) => {
            if path.starts_with("/3d/generations") && method != "DELETE" {
                if let Ok(mut value) = serde_json::from_slice::<Value>(&response.body) {
                    if b64
                        && response.status == 200
                        && let Err(err) = inline_generation_content(relay, &mut value).await
                    {
                        warn!("api_server: inline generation content failed: {err:#}");
                        write_json_response(stream, 502, "Bad Gateway", &json!({"error": {
                                "message": "could not inline generation content; use response_format=url", "type": "server_error"
                            }})).await?;
                        return Ok(());
                    }
                    rewrite_job_urls(
                        &mut value,
                        &format!(
                            "http://{}{url_base}",
                            head.header("host").unwrap_or_default()
                        ),
                    );
                    response.body = serde_json::to_vec(&value)?;
                } else if b64 && response.status == 200 {
                    write_error(
                        stream,
                        502,
                        "Bad Gateway",
                        "invalid generation response; use response_format=url",
                    )
                    .await?;
                    return Ok(());
                }
            }
            write_tunnel_response(stream, &response).await
        }
        Err(err) => {
            write_backend_failure(stream, &err).await;
            Ok(())
        }
    }
}

/// `POST /v1/audio/speech`: OpenAI's shape -- `{model, input, voice,
/// response_format?, speed?}` in, raw audio out (not JSON; the upstream
/// returns bytes and so do we, with the format's own MIME type).
async fn handle_audio_speech(
    stream: &mut TcpStream,
    body: &[u8],
    call: &TtsFn,
    authorization: Option<&str>,
) -> Result<()> {
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
        extra: req
            .as_object()
            .unwrap()
            .iter()
            .filter(|(k, _)| {
                !matches!(
                    k.as_str(),
                    "model" | "input" | "voice" | "response_format" | "speed" | "lang"
                )
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    };

    // `lang` is a mistl extension, not part of OpenAI's schema: it is the
    // same BCP-47 hint `tts_request.lang` carries over the wire, and an
    // OpenAI client that never sends it simply gets the configuration's own voice.
    let lang = req.get("lang").and_then(Value::as_str).map(str::to_string);

    let authorized = match crate::authz::authorize_api(authorization, Some(&params.model)) {
        Ok(authorized) => authorized,
        Err(denied) => {
            write_denied(stream, denied).await?;
            return Ok(());
        }
    };
    let mut usage = ApiUsage::new(authorized);
    usage.started = true;

    match (call)(params, lang).await {
        Ok(audio) => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
                audio.mime,
                audio.bytes.len(),
                cors_headers()
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
    authorization: Option<&str>,
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

    let authorized = match crate::authz::authorize_api(authorization, Some(&params.model)) {
        Ok(authorized) => authorized,
        Err(denied) => {
            write_denied(stream, denied).await?;
            return Ok(());
        }
    };
    let mut usage = ApiUsage::new(authorized);
    usage.started = true;

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

/// Writes a buffered oai-tunnel response as-is (status, content type, body).
async fn write_tunnel_response(
    stream: &mut TcpStream,
    response: &super::oai_tunnel::TunnelResponse,
) -> Result<()> {
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
        "HTTP/1.1 {} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
        response.status,
        response.body.len(),
        cors_headers()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    Ok(())
}

/// `POST /v1/systemone` (scoped or not): checks the envelope (an object
/// with a `state` and a non-empty `questions` object) and the token, then
/// relays it. Question semantics are left to the upstream server.
async fn handle_decision(
    stream: &mut TcpStream,
    body: &[u8],
    decision: Option<&DecisionFn>,
    authorization: Option<&str>,
) -> Result<()> {
    let Some(decision) = decision else {
        write_error(
            stream,
            404,
            "Not Found",
            "decision requests are not available",
        )
        .await?;
        return Ok(());
    };
    let req: Value = match serde_json::from_slice(body) {
        Ok(value @ Value::Object(_)) => value,
        _ => {
            write_error(
                stream,
                400,
                "Bad Request",
                "request body must be a JSON object",
            )
            .await?;
            return Ok(());
        }
    };
    if req.get("state").is_none_or(Value::is_null) {
        write_error(stream, 400, "Bad Request", "'state' is required").await?;
        return Ok(());
    }
    if req
        .get("questions")
        .and_then(Value::as_object)
        .is_none_or(|q| q.is_empty())
    {
        write_error(
            stream,
            400,
            "Bad Request",
            "'questions' must be a non-empty object",
        )
        .await?;
        return Ok(());
    }
    let model = req.get("model").and_then(Value::as_str);
    let authorized = match crate::authz::authorize_api(authorization, model) {
        Ok(authorized) => authorized,
        Err(denied) => {
            write_denied(stream, denied).await?;
            return Ok(());
        }
    };
    let mut usage = ApiUsage::new(authorized);
    usage.started = true;
    match decision(body.to_vec()).await {
        Ok(response) => write_tunnel_response(stream, &response).await,
        Err(err) => {
            write_backend_failure(stream, &err).await;
            Ok(())
        }
    }
}

async fn handle_chat_completions_with_oai(
    stream: &mut TcpStream,
    body: &[u8],
    call: &LlmCallFn,
    oai: Option<&OaiFn>,
    relay: Option<&RelayFn>,
    authorization: Option<&str>,
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
    let parsed_body = vision.as_ref().map_or(body, |text| text.as_slice());
    let req = match parse_chat_request(parsed_body) {
        Ok(req) => req,
        Err(message) => {
            write_error(stream, 400, "Bad Request", &message).await?;
            return Ok(());
        }
    };
    let authorized = match crate::authz::authorize_api(authorization, req.model.as_deref()) {
        Ok(authorized) => authorized,
        Err(denied) => {
            write_denied(stream, denied).await?;
            return Ok(());
        }
    };
    let mut usage = ApiUsage::new(authorized);
    let mut vision_call: Option<LlmCallFn> = None;
    if vision.is_some() {
        if oai.is_none() && relay.is_none() {
            write_error(
                stream,
                400,
                "Bad Request",
                "image content requires an OpenAI-compatible provider",
            )
            .await?;
            return Ok(());
        }
        usage.started = true;
        let response = if let Some(oai) = oai {
            let mut raw: Value = serde_json::from_slice(body)?;
            raw["stream"] = json!(false);
            oai(raw).await
        } else {
            relay.expect("vision route")(RelayRequest {
                method: "POST".into(),
                path: "/chat/completions".into(),
                body: Some(body.to_vec()),
            })
            .await
        };
        let response = match response {
            Ok(response) => response,
            Err(err) => {
                write_backend_failure(stream, &err).await;
                return Ok(());
            }
        };
        if !req.stream || !(200..300).contains(&response.status) {
            if let Ok(value) = serde_json::from_slice::<Value>(&response.body) {
                usage.output_chars = value
                    .pointer("/choices/0/message/content")
                    .and_then(Value::as_str)
                    .map_or(0, |text| text.chars().count() as u64);
            }
            return write_tunnel_response(stream, &response).await;
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
    }
    let call = vision_call.as_ref().unwrap_or(call);

    let resp_model = req.model.clone().unwrap_or_else(|| "mistl".to_string());
    let id = gen_id("chatcmpl");
    let created = unix_now();
    usage.started = true;

    if !req.stream {
        let fut = (call)(req.messages, req.tools, req.model, None);
        return match fut.await {
            Ok(output) => {
                usage.output_chars = output.content.chars().count() as u64;
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
                        usage.output_chars = usage.output_chars.saturating_add(delta.chars().count() as u64);
                        if !stream_started {
                            stream.write_all(sse_response_header().as_bytes()).await?;
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
            usage.output_chars = usage
                .output_chars
                .max(output.content.chars().count() as u64);
            // A successful backend is allowed to produce no content. It is
            // still a valid empty SSE completion, so commit the response now.
            if !stream_started {
                stream.write_all(sse_response_header().as_bytes()).await?;
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

pub(super) fn has_image_content(body: &Value) -> bool {
    body.get("messages")
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
        })
}

fn vision_request(body: &[u8]) -> std::result::Result<Option<Vec<u8>>, String> {
    let mut text: Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid request body: {e}"))?;
    if !has_image_content(&text) {
        return Ok(None);
    }
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
    serde_json::to_vec(&text)
        .map(Some)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::oai_tunnel::TunnelResponse;

    const GENERATION_ID: &str = "gen3d_0123456789abcdef0123456789abcdef";

    fn relay_response(status: u16, body: Value) -> TunnelResponse {
        TunnelResponse {
            status,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    fn recording_relay(
        reply: impl Fn(&RelayRequest) -> TunnelResponse + Send + Sync + 'static,
    ) -> (RelayFn, Arc<Mutex<Vec<RelayRequest>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let records = seen.clone();
        let relay: RelayFn = Arc::new(move |request| {
            let response = reply(&request);
            records.lock().unwrap().push(request);
            Box::pin(async { Ok(response) })
        });
        (relay, seen)
    }

    async fn relay_test_server(relay: RelayFn) -> Arc<ApiServer> {
        let room_relay = relay.clone();
        let rooms: RoomsFn = Arc::new(move |room| {
            assert_eq!(room, "team/one");
            let relay = room_relay.clone();
            Box::pin(async move {
                Ok(Some(RoomRoutes {
                    call: fake_call_ok(),
                    models: Arc::new(|| vec!["org/model".into()]),
                    tts: fake_tts(),
                    stt: fake_stt(),
                    oai: Arc::new(|_| Box::pin(async { anyhow::bail!("unused") })),
                    decision: Arc::new(|_| Box::pin(async { anyhow::bail!("unused") })),
                    relay,
                }))
            })
        });
        ApiServer::start_with_rooms(
            "127.0.0.1:0",
            fake_call_ok(),
            Arc::new(|| vec!["m1".into(), "org/model".into()]),
            fake_tts(),
            fake_stt(),
            None,
            Some(relay),
            Some(rooms),
            None,
        )
        .await
        .unwrap()
    }

    async fn relay_request(
        server: &ApiServer,
        method: &str,
        path: &str,
        content_type: &str,
        body: Option<&str>,
    ) -> Vec<u8> {
        let headers = body
            .map(|body| {
                format!(
                    "Content-Type: {content_type}\r\nContent-Length: {}\r\n",
                    body.len()
                )
            })
            .unwrap_or_default();
        send_request(
            server.addr(),
            &format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost:6478\r\n{headers}\r\n{}",
                body.unwrap_or_default()
            ),
        )
        .await
    }

    #[tokio::test]
    async fn tcai_routes_relay_methods_and_paths() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| relay_response(202, json!({"ok":true})));
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for (method, suffix, expected, body) in [
                (
                    "GET",
                    "audio/voices".to_string(),
                    "/audio/voices".to_string(),
                    None,
                ),
                (
                    "GET",
                    "voices".to_string(),
                    "/audio/voices".to_string(),
                    None,
                ),
                (
                    "GET",
                    "3d/generations".to_string(),
                    "/3d/generations".to_string(),
                    None,
                ),
                (
                    "POST",
                    "3d/generations".to_string(),
                    "/3d/generations".to_string(),
                    Some(r#"{"model":"3d-model"}"#),
                ),
                (
                    "GET",
                    format!("3d/generations/{GENERATION_ID}"),
                    format!("/3d/generations/{GENERATION_ID}"),
                    None,
                ),
                (
                    "DELETE",
                    format!("3d/generations/{GENERATION_ID}"),
                    format!("/3d/generations/{GENERATION_ID}"),
                    None,
                ),
            ] {
                let raw = relay_request(
                    &server,
                    method,
                    &format!("{prefix}/{suffix}"),
                    "application/json",
                    body,
                )
                .await;
                assert_eq!(status_code(&split_response(&raw).0), 202);
                let records = seen.lock().unwrap();
                let sent = records.last().unwrap();
                assert_eq!(sent.method, method);
                assert_eq!(sent.path, expected);
                assert_eq!(sent.body.as_deref(), body.map(str::as_bytes));
            }
        }
        for (method, path, expected) in [
            ("GET", "/voices", "/audio/voices"),
            ("GET", "/api/ps", "/api/ps"),
            ("GET", "/health", "/health"),
        ] {
            let raw = relay_request(&server, method, path, "", None).await;
            assert_eq!(status_code(&split_response(&raw).0), 202);
            let records = seen.lock().unwrap();
            assert_eq!(records.last().unwrap().path, expected);
            assert!(records.last().unwrap().body.is_none());
        }
        // Model unloading is not exposed.
        let raw = relay_request(
            &server,
            "POST",
            "/api/unload",
            "application/json",
            Some("{}"),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 404);
        for path in ["/v1/rooms/team%2Fone/health", "/v1/rooms/team%2Fone/api/ps"] {
            let raw = relay_request(&server, "GET", path, "", None).await;
            assert_eq!(status_code(&split_response(&raw).0), 404);
        }
        server.stop();
    }

    #[tokio::test]
    async fn tcai_invalid_generation_ids_and_absent_relay_are_404() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| panic!("invalid ids must not reach relay"));
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for id in [
                "gen3d_0123456789abcdef0123456789abcdeF",
                "gen3d_0",
                "",
                "../job",
                "%67en3d_0123456789abcdef0123456789abcdef",
            ] {
                for (method, suffix) in [("GET", ""), ("DELETE", ""), ("GET", "/content")] {
                    let raw = relay_request(
                        &server,
                        method,
                        &format!("{prefix}/3d/generations/{id}{suffix}"),
                        "",
                        None,
                    )
                    .await;
                    let (head, body) = split_response(&raw);
                    assert_eq!(status_code(&head), 404);
                    let error: Value = serde_json::from_slice(body).unwrap();
                    assert_eq!(error["error"]["message"], "generation not found");
                    assert_eq!(error["error"]["code"], "not_found");
                    assert_eq!(error["error"]["param"], "id");
                }
            }
        }
        assert!(seen.lock().unwrap().is_empty());
        server.stop();
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let raw = relay_request(&server, "GET", "/v1/voices", "", None).await;
        assert_eq!(status_code(&split_response(&raw).0), 404);
        server.stop();
    }

    #[tokio::test]
    async fn game_and_motion_routes_extract_models_and_preserve_payloads() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| TunnelResponse {
            status: 202,
            content_type: "text/csv; charset=utf-8".into(),
            body: b"time,joint\r\n0,1\r\n".to_vec(),
        });
        let server = relay_test_server(relay).await;
        let game = r#"{ "model":"game-model", "image":"data:image/png;base64,AA==" }"#;
        let motion = r#"{"model":"motion-model","start":[0,1],"format":"csv"}"#;
        let clips = "/motion/clips?tag=a+b&model=motion%2Dmodel&tag=%2B";
        let skeleton = "/motion/skeletons/motion%2Dmodel";
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for (method, path, body) in [
                ("POST", "/game/act", Some(game)),
                ("POST", "/motion/inbetween", Some(motion)),
                ("GET", clips, None),
                ("GET", skeleton, None),
            ] {
                let raw = relay_request(
                    &server,
                    method,
                    &format!("{prefix}{path}"),
                    "application/json",
                    body,
                )
                .await;
                let (head, payload) = split_response(&raw);
                assert_eq!(status_code(&head), 202);
                assert!(
                    head.to_ascii_lowercase()
                        .contains("content-type: text/csv; charset=utf-8")
                );
                assert_eq!(payload, b"time,joint\r\n0,1\r\n");
                let calls = seen.lock().unwrap();
                let sent = calls.last().unwrap();
                assert_eq!(sent.method, method);
                assert_eq!(sent.path, path);
                assert_eq!(sent.body.as_deref(), body.map(str::as_bytes));
            }
            for path in ["/motion/clips", "/motion/clips?model=", "/motion/clips?x=1"] {
                let raw = relay_request(&server, "GET", &format!("{prefix}{path}"), "", None).await;
                assert_denied(&raw, 400, "missing_required_parameter");
            }
            for model in ["", "motion%2Fmodel", "motion/model", "%zz", "%2e%2e"] {
                let path = format!("{prefix}/motion/skeletons/{model}");
                let raw = relay_request(&server, "GET", &path, "", None).await;
                assert_denied(&raw, 400, "invalid_request");
            }
            for path in ["/game/act", "/motion/inbetween"] {
                let raw = relay_request(
                    &server,
                    "POST",
                    &format!("{prefix}{path}"),
                    "application/json",
                    Some("{}"),
                )
                .await;
                assert_denied(&raw, 400, "missing_required_parameter");
            }
        }
        let multipart = concat!(
            "--b\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngame-model\r\n",
            "--b\r\nContent-Disposition: form-data; name=\"image\"; filename=\"frame.png\"\r\n",
            "Content-Type: image/png\r\n\r\nabc\r\n--b--\r\n"
        );
        let raw = relay_request(
            &server,
            "POST",
            "/v1/game/act",
            "multipart/form-data; boundary=b",
            Some(multipart),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 202);
        let payload: Value = serde_json::from_slice(
            seen.lock()
                .unwrap()
                .last()
                .unwrap()
                .body
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            payload,
            json!({"model":"game-model","image":"data:image/png;base64,YWJj"})
        );
        assert_eq!(seen.lock().unwrap().len(), 9);

        let token = create_token(json!({"models":["motion-model"]}));
        for path in [clips, skeleton] {
            let raw = token_request(&server, &format!("/v1{path}"), Some(&token), None).await;
            assert_eq!(status_code(&split_response(&raw).0), 202);
        }
        for path in [
            "/motion/clips?model=other-model",
            "/motion/skeletons/other-model",
        ] {
            let raw = token_request(&server, &format!("/v1{path}"), Some(&token), None).await;
            assert_denied(&raw, 403, "model_not_allowed");
        }
        server.stop();
    }

    #[tokio::test]
    async fn image_edits_relay_json_and_upstream_status_and_body_unchanged() {
        let _authz = authz_test().await;
        for status in [200, 400, 413, 500, 502] {
            // Preserve even job-shaped data; image edits have no job URL handling.
            let reply = format!(
                "{{ \"data\": [{{\"b64_json\":\"UE5H\",\"url\":\"/3d/generations/{GENERATION_ID}/content\"}}] }}"
            );
            let bytes = reply.as_bytes().to_vec();
            let (relay, seen) = recording_relay(move |_| TunnelResponse {
                status,
                content_type: "application/json".into(),
                body: bytes.clone(),
            });
            let server = relay_test_server(relay).await;
            let request = r#"{ "model":"image-model", "image":"data:image/png;base64,UE5H", "response_format":"b64_json" }"#;
            for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
                let raw = relay_request(
                    &server,
                    "POST",
                    &format!("{prefix}/images/edits"),
                    "application/json",
                    Some(request),
                )
                .await;
                let (head, body) = split_response(&raw);
                assert_eq!(status_code(&head), status);
                assert!(head_has(&head, "Content-Type: application/json"));
                assert_eq!(body, reply.as_bytes());
                let records = seen.lock().unwrap();
                let sent = records.last().unwrap();
                assert_eq!(sent.method, "POST");
                assert_eq!(sent.path, "/images/edits");
                assert_eq!(sent.body.as_deref(), Some(request.as_bytes()));
            }
            assert_eq!(seen.lock().unwrap().len(), 2);
            server.stop();
        }
    }

    #[tokio::test]
    async fn image_edits_binary_reply_preserves_bytes_and_content_type() {
        let _authz = authz_test().await;
        let image = b"\x89PNG\r\n\x1a\n\x00\xff\x80";
        let (relay, seen) = recording_relay(move |_| TunnelResponse {
            status: 200,
            content_type: "image/png".into(),
            body: image.to_vec(),
        });
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let raw = relay_request(
                &server,
                "POST",
                &format!("{prefix}/images/edits"),
                "application/json",
                Some(r#"{"model":"image-model","image":"UE5H","response_format":"binary"}"#),
            )
            .await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            assert!(head_has(&head, "Content-Type: image/png"));
            assert_eq!(body, image);
        }
        assert_eq!(seen.lock().unwrap().len(), 2);
        server.stop();
    }

    #[tokio::test]
    async fn image_edits_multipart_preserves_field_types() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| relay_response(200, json!({})));
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for background in ["transparent", "true", "42"] {
                let body = gen3d_form(&[
                    ("model", "image-model"),
                    ("scale", "2.5"),
                    ("response_format", "binary"),
                    ("output_format", "png"),
                    ("background", background),
                    ("keep_alive", "120"),
                ]);
                let raw = relay_request(
                    &server,
                    "POST",
                    &format!("{prefix}/images/edits"),
                    "multipart/form-data; boundary=b",
                    Some(&body),
                )
                .await;
                assert_eq!(status_code(&split_response(&raw).0), 200);
                let records = seen.lock().unwrap();
                let sent = records.last().unwrap();
                assert_eq!(sent.method, "POST");
                assert_eq!(sent.path, "/images/edits");
                let value: Value = serde_json::from_slice(sent.body.as_ref().unwrap()).unwrap();
                assert_eq!(
                    value,
                    json!({
                        "image":"data:image/png;base64,UE5H", "model":"image-model",
                        "scale":2.5, "response_format":"binary", "output_format":"png",
                        "background":background, "keep_alive":"120"
                    })
                );
            }
        }
        server.stop();
    }

    #[tokio::test]
    async fn image_edits_require_a_model_after_authorization_and_meter_usage() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| relay_response(200, json!({})));
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let path = format!("{prefix}/images/edits");
            for body in [
                "{}",
                r#"{"model":""}"#,
                r#"{"model":"  "}"#,
                r#"{"model":null}"#,
            ] {
                let raw =
                    relay_request(&server, "POST", &path, "application/json", Some(body)).await;
                let (head, body) = split_response(&raw);
                assert_eq!(status_code(&head), 400);
                assert_eq!(body, super::super::oai_tunnel::missing_model().body);
            }
        }
        require_token(true);
        let raw = token_request(
            &server,
            "/v1/images/edits",
            None,
            Some(("application/json", "{}")),
        )
        .await;
        assert_denied(&raw, 401, "unauthorized");
        let token = create_token(json!({"models":["image-model"]}));
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let path = format!("{prefix}/images/edits");
            for body in ["{}", r#"{"model":"other"}"#] {
                let raw = token_request(
                    &server,
                    &path,
                    Some(&token),
                    Some(("application/json", body)),
                )
                .await;
                assert_denied(&raw, 403, "model_not_allowed");
            }
        }
        assert!(seen.lock().unwrap().is_empty());
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let raw = token_request(
                &server,
                &format!("{prefix}/images/edits"),
                Some(&token),
                Some((
                    "application/json",
                    r#"{"model":"image-model","image":"UE5H"}"#,
                )),
            )
            .await;
            assert_eq!(status_code(&split_response(&raw).0), 200);
        }
        assert_eq!(token_usage()["requests"], 2);
        server.stop();
    }

    #[tokio::test]
    async fn image_edits_body_caps_validation_and_post_only() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| relay_response(200, json!({})));
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let path = format!("{prefix}/images/edits");
            for (content_type, cap) in [
                ("application/json", MAX_IMAGE_EDIT_JSON_BYTES),
                (
                    "multipart/form-data; boundary=b",
                    MAX_IMAGE_EDIT_MULTIPART_BYTES,
                ),
            ] {
                let raw = send_request(server.addr(), &format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n", cap + 1)).await;
                assert_eq!(status_code(&split_response(&raw).0), 413);
            }
            for content_type in ["image/png", "text/plain", "multipart/form-data"] {
                let raw = relay_request(&server, "POST", &path, content_type, Some("PNG")).await;
                assert_eq!(status_code(&split_response(&raw).0), 415);
            }
            for body in ["null", "[]", "true", "\"text\"", "{"] {
                let raw =
                    relay_request(&server, "POST", &path, "application/json", Some(body)).await;
                assert_eq!(status_code(&split_response(&raw).0), 400);
            }
            let raw = relay_request(&server, "GET", &path, "", None).await;
            assert_eq!(status_code(&split_response(&raw).0), 404);
        }
        assert!(seen.lock().unwrap().is_empty());
        // The room limit applies to the converted JSON, including base64 expansion.
        let form = gen3d_form(&[("model", "image-model")]).replace(
            "\r\nPNG\r\n",
            &format!(
                "\r\n{}\r\n",
                "x".repeat(super::super::oai_tunnel::MAX_BYTES * 3 / 4)
            ),
        );
        assert!(form.len() < super::super::oai_tunnel::MAX_BYTES);
        let json =
            json!({"model":"image-model", "image":"x".repeat(MAX_GEN3D_JSON_BYTES)}).to_string();
        for (content_type, body) in [
            ("application/json", &json),
            ("multipart/form-data; boundary=b", &form),
        ] {
            let raw = relay_request(
                &server,
                "POST",
                "/v1/rooms/team%2Fone/images/edits",
                content_type,
                Some(body),
            )
            .await;
            assert_eq!(status_code(&split_response(&raw).0), 413);
            assert!(seen.lock().unwrap().is_empty());
        }
        for (content_type, body) in [
            ("application/json", &json),
            ("multipart/form-data; boundary=b", &form),
        ] {
            let raw = relay_request(
                &server,
                "POST",
                "/v1/images/edits",
                content_type,
                Some(body),
            )
            .await;
            assert_eq!(status_code(&split_response(&raw).0), 200);
        }
        assert_eq!(seen.lock().unwrap().len(), 2);
        server.stop();
    }

    fn gen3d_form(fields: &[(&str, &str)]) -> String {
        let mut body = "--b\r\nContent-Disposition: form-data; name=\"image\"; filename=\"x.png\"\r\nContent-Type: image/png\r\n\r\nPNG\r\n".to_string();
        for (name, value) in fields {
            body.push_str(&format!(
                "--b\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            ));
        }
        body.push_str("--b--\r\n");
        body
    }

    #[tokio::test]
    async fn tcai_multipart_converts_image_and_literals_and_rejects_duplicates() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| relay_response(200, json!({})));
        let server = relay_test_server(relay).await;
        let content_type = "multipart/form-data; boundary=b";
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for remove in ["auto", "true", "false"] {
                let body = gen3d_form(&[
                    ("seed", "12"),
                    ("strength", "0.5"),
                    ("background", "true"),
                    ("model", "42"),
                    ("keep_alive", "120"),
                    ("output_format", "glb"),
                    ("response_format", "url"),
                    ("user", "false"),
                    ("other", "hello"),
                    ("remove_background", remove),
                ]);
                let raw = relay_request(
                    &server,
                    "POST",
                    &format!("{prefix}/3d/generations"),
                    content_type,
                    Some(&body),
                )
                .await;
                assert_eq!(status_code(&split_response(&raw).0), 200);
                let records = seen.lock().unwrap();
                let value: Value =
                    serde_json::from_slice(records.last().unwrap().body.as_ref().unwrap()).unwrap();
                assert_eq!(value["image"], "data:image/png;base64,UE5H");
                assert_eq!(value["seed"], 12);
                assert_eq!(value["strength"], 0.5);
                assert_eq!(value["background"], true);
                assert_eq!(value["model"], "42");
                assert_eq!(value["keep_alive"], "120");
                assert_eq!(value["user"], "false");
                assert_eq!(value["other"], "hello");
                assert_eq!(
                    value["remove_background"],
                    match remove {
                        "true" => json!(true),
                        "false" => json!(false),
                        _ => json!("auto"),
                    }
                );
            }
        }
        for body in [
            gen3d_form(&[("seed", "1"), ("seed", "2")]),
            gen3d_form(&[("image", "again")]),
            gen3d_form(&[("user", &"x".repeat(64 * 1024 + 1))]),
            "--b--\r\n".into(),
        ] {
            let raw = relay_request(
                &server,
                "POST",
                "/v1/3d/generations",
                content_type,
                Some(&body),
            )
            .await;
            assert_eq!(status_code(&split_response(&raw).0), 400);
        }
        let body = gen3d_form(&[("model", "3d-model")]).replace("Content-Type: image/png\r\n", "");
        let raw = relay_request(
            &server,
            "POST",
            "/v1/3d/generations",
            content_type,
            Some(&body),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 200);
        let records = seen.lock().unwrap();
        let value: Value =
            serde_json::from_slice(records.last().unwrap().body.as_ref().unwrap()).unwrap();
        assert_eq!(value["image"], "data:image/png;base64,UE5H");
        server.stop();
    }

    #[tokio::test]
    async fn tcai_jobs_rewrite_urls_with_host_and_encoded_room() {
        let _authz = authz_test().await;
        let (relay, _) = recording_relay(|request| {
            let job = json!({"id":GENERATION_ID,"data":[{"url":format!("http://upstream:8087/v1/3d/generations/{GENERATION_ID}/content")},
                {"url":format!("/3d/generations/{GENERATION_ID}/content")} ]});
            relay_response(
                200,
                if request.path == "/3d/generations" && request.method == "GET" {
                    json!({"data":[job]})
                } else {
                    job
                },
            )
        });
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for (method, suffix, body) in [
                (
                    "POST",
                    "3d/generations".to_string(),
                    Some(r#"{"model":"3d-model"}"#),
                ),
                ("GET", format!("3d/generations/{GENERATION_ID}"), None),
                ("GET", "3d/generations".to_string(), None),
            ] {
                let raw = relay_request(
                    &server,
                    method,
                    &format!("{prefix}/{suffix}"),
                    "application/json",
                    body,
                )
                .await;
                let (head, body) = split_response(&raw);
                assert_eq!(status_code(&head), 200);
                let value: Value = serde_json::from_slice(body).unwrap();
                let data = if method == "GET" && suffix == "3d/generations" {
                    &value["data"][0]["data"]
                } else {
                    &value["data"]
                };
                for item in data.as_array().unwrap() {
                    assert_eq!(
                        item["url"],
                        format!(
                            "http://localhost:6478{prefix}/3d/generations/{GENERATION_ID}/content"
                        )
                    );
                }
            }
        }
        server.stop();
    }

    #[tokio::test]
    async fn tcai_b64_json_uses_windows_and_caps_memory() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|request| {
            if request.path.contains("/content?") {
                return TunnelResponse {
                    status: 206,
                    content_type: "model/gltf-binary".into(),
                    body: b"GLB".to_vec(),
                };
            }
            let body: Value = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
            let bytes = if body["user"] == "large" {
                MAX_GEN3D_B64_BYTES + 1
            } else {
                3
            };
            relay_response(
                200,
                json!({"id":GENERATION_ID,"status":"completed","data":[{"format":"glb","bytes":bytes,
                "url":format!("/3d/generations/{GENERATION_ID}/content")}]}),
            )
        });
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let raw = relay_request(
                &server,
                "POST",
                &format!("{prefix}/3d/generations"),
                "application/json",
                Some(r#"{"model":"3d-model","response_format":"b64_json","nested":{"z":1,"a":2}}"#),
            )
            .await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            let job: Value = serde_json::from_slice(body).unwrap();
            assert_eq!(job["data"][0]["b64_json"], "R0xC");
            assert!(job["data"][0].get("url").is_none());
            let records = seen.lock().unwrap();
            assert_eq!(
                records[records.len() - 2].body.as_deref().unwrap(),
                br#"{"model":"3d-model","response_format":"url","nested":{"z":1,"a":2}}"#
            );
            assert_eq!(
                records.last().unwrap().path,
                format!("/3d/generations/{GENERATION_ID}/content?range=0-2")
            );
        }
        let count = seen.lock().unwrap().len();
        let raw = relay_request(
            &server,
            "POST",
            "/v1/3d/generations",
            "application/json",
            Some(r#"{"model":"3d-model","response_format":"b64_json","user":"large"}"#),
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 502);
        assert!(String::from_utf8_lossy(body).contains("response_format=url"));
        assert_eq!(seen.lock().unwrap().len(), count + 1);
        let raw = relay_request(
            &server,
            "POST",
            "/v1/3d/generations",
            "application/json",
            Some(r#"{"model":"3d-model","response_format":"b64_json","background":true}"#),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 200);
        let records = seen.lock().unwrap();
        let sent: Value =
            serde_json::from_slice(records.last().unwrap().body.as_ref().unwrap()).unwrap();
        assert_eq!(sent["response_format"], "b64_json");
        server.stop();
    }

    #[tokio::test]
    async fn tcai_content_streams_two_windows_and_ignores_client_range() {
        let _authz = authz_test().await;
        let window = super::super::oai_tunnel::CONTENT_WINDOW;
        let (relay, seen) = recording_relay(move |request| {
            if request.path.contains("/content?") {
                let first = request.path.ends_with(&format!("range=0-{}", window - 1));
                return TunnelResponse {
                    status: 206,
                    content_type: "application/octet-stream".into(),
                    body: vec![if first { 1 } else { 2 }; if first { window as usize } else { 3 }],
                };
            }
            relay_response(
                200,
                json!({"status":"completed","data":[{"format":"glb","bytes":window+3}]}),
            )
        });
        let server = relay_test_server(relay).await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let raw = send_request(server.addr(), &format!("GET {prefix}/3d/generations/{GENERATION_ID}/content HTTP/1.1\r\nHost: localhost\r\nRange: bytes=1-2\r\n\r\n")).await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            assert!(head_has(&head, "Content-Type: model/gltf-binary"));
            assert!(head_has(&head, &format!("Content-Length: {}", window + 3)));
            assert!(head_has(
                &head,
                &format!("Content-Disposition: attachment; filename=\"{GENERATION_ID}.glb\"")
            ));
            assert_eq!(body.len() as u64, window + 3);
            assert!(body[..window as usize].iter().all(|b| *b == 1));
            assert_eq!(&body[window as usize..], &[2, 2, 2]);
            let records = seen.lock().unwrap();
            assert_eq!(
                records[records.len() - 3].path,
                format!("/3d/generations/{GENERATION_ID}")
            );
            assert_eq!(
                records[records.len() - 2].path,
                format!(
                    "/3d/generations/{GENERATION_ID}/content?range=0-{}",
                    window - 1
                )
            );
            assert_eq!(
                records.last().unwrap().path,
                format!(
                    "/3d/generations/{GENERATION_ID}/content?range={window}-{}",
                    window + 2
                )
            );
        }
        server.stop();
    }

    #[tokio::test]
    async fn tcai_content_retries_transient_failures_for_stream_and_inline() {
        let _authz = authz_test().await;
        for failure in ["relay", "5xx", "length"] {
            for inline in [false, true] {
                let seen = Arc::new(Mutex::new(Vec::new()));
                let records = seen.clone();
                let relay: RelayFn = Arc::new(move |request| {
                    let mut records = records.lock().unwrap();
                    let window_attempt = records
                        .iter()
                        .filter(|path: &&String| path.contains("/content?"))
                        .count();
                    let is_window = request.path.contains("/content?");
                    records.push(request.path);
                    let response = if is_window && window_attempt == 0 {
                        match failure {
                            "relay" => Err(anyhow::anyhow!("simulated transport failure")),
                            "5xx" => Ok(relay_response(502, json!({}))),
                            "length" => Ok(TunnelResponse {
                                status: 206,
                                content_type: "application/octet-stream".into(),
                                body: b"G".to_vec(),
                            }),
                            _ => unreachable!(),
                        }
                    } else if is_window {
                        Ok(TunnelResponse {
                            status: 206,
                            content_type: "application/octet-stream".into(),
                            body: b"GLB".to_vec(),
                        })
                    } else {
                        Ok(relay_response(
                            200,
                            json!({"id":GENERATION_ID,"status":"completed","data":[{"format":"glb","bytes":3}]}),
                        ))
                    };
                    Box::pin(async move { response })
                });
                let server = relay_test_server(relay).await;
                let raw = if inline {
                    relay_request(
                        &server,
                        "POST",
                        "/v1/3d/generations",
                        "application/json",
                        Some(r#"{"model":"3d-model","response_format":"b64_json"}"#),
                    )
                    .await
                } else {
                    relay_request(
                        &server,
                        "GET",
                        &format!("/v1/3d/generations/{GENERATION_ID}/content"),
                        "",
                        None,
                    )
                    .await
                };
                let (head, body) = split_response(&raw);
                assert_eq!(status_code(&head), 200, "{failure}, inline={inline}");
                if inline {
                    let job: Value = serde_json::from_slice(body).unwrap();
                    assert_eq!(job["data"][0]["b64_json"], "R0xC");
                } else {
                    assert!(head_has(&head, "Content-Length: 3"));
                    assert_eq!(body, b"GLB");
                }
                let records = seen.lock().unwrap();
                let windows: Vec<_> = records
                    .iter()
                    .filter(|path| path.contains("/content?"))
                    .collect();
                assert_eq!(windows.len(), 2);
                assert_eq!(windows[0], windows[1]);
                assert_eq!(
                    windows[0],
                    &format!("/3d/generations/{GENERATION_ID}/content?range=0-2")
                );
                server.stop();
            }
        }
    }

    #[tokio::test]
    async fn tcai_content_stops_after_three_failures_on_later_window() {
        let _authz = authz_test().await;
        let window = super::super::oai_tunnel::CONTENT_WINDOW;
        let (relay, seen) = recording_relay(move |request| {
            if request.path.ends_with(&format!("range=0-{}", window - 1)) {
                TunnelResponse {
                    status: 206,
                    content_type: "application/octet-stream".into(),
                    body: vec![7; window as usize],
                }
            } else if request.path.contains("/content?") {
                relay_response(502, json!({"error":"upstream failed"}))
            } else {
                relay_response(
                    200,
                    json!({"status":"completed","data":[{"format":"glb","bytes":window+3}]}),
                )
            }
        });
        let server = relay_test_server(relay).await;
        let raw = relay_request(
            &server,
            "GET",
            &format!("/v1/3d/generations/{GENERATION_ID}/content"),
            "",
            None,
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        assert!(head_has(&head, &format!("Content-Length: {}", window + 3)));
        assert_eq!(body.len() as u64, window);
        assert!(body.iter().all(|byte| *byte == 7));
        let records = seen.lock().unwrap();
        assert_eq!(records.len(), 5);
        for request in &records[2..] {
            assert_eq!(
                request.path,
                format!(
                    "/3d/generations/{GENERATION_ID}/content?range={window}-{}",
                    window + 2
                )
            );
        }
        server.stop();
    }

    #[test]
    fn content_relay_error_omits_untrusted_details() {
        let error =
            anyhow::anyhow!("secret-token https://example.invalid/private?token=secret-token")
                .context("OAI response stalled");
        let safe = format!("{:#}", content_relay_error(&error));
        assert!(safe.contains("OAI response stalled"));
        assert!(!safe.contains("secret-token"));
        assert!(!safe.contains("https://"));
    }

    #[tokio::test]
    async fn tcai_content_passes_through_not_ready_and_job_errors() {
        let _authz = authz_test().await;
        for job in [
            json!({"status":"running"}),
            json!({"status":"completed","data":[{"bytes":0}]}),
            json!({"status":"completed","data":[{}]}),
        ] {
            let (relay, seen) = recording_relay(move |request| {
                if request.path.ends_with("/content") {
                    relay_response(409, json!({"error":{"message":"not completed"}}))
                } else {
                    relay_response(200, job.clone())
                }
            });
            let server = relay_test_server(relay).await;
            let raw = relay_request(
                &server,
                "GET",
                &format!("/v1/3d/generations/{GENERATION_ID}/content"),
                "",
                None,
            )
            .await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 409);
            assert_eq!(
                serde_json::from_slice::<Value>(body).unwrap()["error"]["message"],
                "not completed"
            );
            assert_eq!(seen.lock().unwrap().len(), 2);
            server.stop();
        }
        let (relay, seen) =
            recording_relay(|_| relay_response(404, json!({"error":{"message":"not found"}})));
        let server = relay_test_server(relay).await;
        let raw = relay_request(
            &server,
            "GET",
            &format!("/v1/3d/generations/{GENERATION_ID}/content"),
            "",
            None,
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 404);
        assert_eq!(seen.lock().unwrap().len(), 1);
        server.stop();
    }

    #[tokio::test]
    async fn tcai_content_drops_connection_on_bad_window() {
        let _authz = authz_test().await;
        let window = super::super::oai_tunnel::CONTENT_WINDOW;
        for (status, length, attempts) in [
            (206, 2, 3),
            (200, 3, 3),
            (200, window as usize, 1),
            (500, 3, 3),
            (404, 3, 1),
            (302, 3, 1),
        ] {
            let (relay, seen) = recording_relay(move |request| {
                if request.path.contains("/content?") {
                    TunnelResponse {
                        status,
                        content_type: "model/obj".into(),
                        body: vec![0; length],
                    }
                } else {
                    relay_response(
                        200,
                        json!({"status":"completed","data":[{"bytes":window+3,"format":"obj"}]}),
                    )
                }
            });
            let server = relay_test_server(relay).await;
            let raw = relay_request(
                &server,
                "GET",
                &format!("/v1/3d/generations/{GENERATION_ID}/content"),
                "",
                None,
            )
            .await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            assert!(head_has(&head, "Content-Type: model/obj"));
            assert!(body.is_empty());
            assert_eq!(seen.lock().unwrap().len(), 1 + attempts);
            server.stop();
        }
    }

    #[tokio::test]
    async fn tcai_content_accepts_whole_file_200_and_maps_formats() {
        let _authz = authz_test().await;
        for (format, mime) in [
            ("glb", "model/gltf-binary"),
            ("obj", "model/obj"),
            ("ply", "application/octet-stream"),
            ("stl", "model/stl"),
            ("3mf", "model/3mf"),
            ("dae", "model/vnd.collada+xml"),
        ] {
            let (relay, seen) = recording_relay(move |request| {
                if request.path.contains("/content?") {
                    TunnelResponse {
                        status: 200,
                        content_type: "application/octet-stream".into(),
                        body: b"3D".to_vec(),
                    }
                } else {
                    relay_response(
                        200,
                        json!({"status":"completed","data":[{"format":format,"bytes":2}]}),
                    )
                }
            });
            let server = relay_test_server(relay).await;
            let raw = relay_request(
                &server,
                "GET",
                &format!("/v1/3d/generations/{GENERATION_ID}/content"),
                "",
                None,
            )
            .await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            assert!(head_has(&head, &format!("Content-Type: {mime}")));
            assert!(head_has(
                &head,
                &format!("Content-Disposition: attachment; filename=\"{GENERATION_ID}.{format}\"")
            ));
            assert_eq!(body, b"3D");
            assert_eq!(
                seen.lock().unwrap().last().unwrap().path,
                format!("/3d/generations/{GENERATION_ID}/content?range=0-1")
            );
            server.stop();
        }
    }

    #[tokio::test]
    async fn tcai_relay_preserves_errors_and_binary_responses() {
        let _authz = authz_test().await;
        let relay: RelayFn = Arc::new(|request| {
            Box::pin(async move {
                match request.path.as_str() {
                    "/health" => Ok(TunnelResponse {
                        status: 503,
                        content_type: "application/problem+json".into(),
                        body: br#"{"error":{"message":"unreachable"}}"#.to_vec(),
                    }),
                    "/api/ps" => anyhow::bail!("private backend detail"),
                    _ => Ok(TunnelResponse {
                        status: 200,
                        content_type: "application/octet-stream".into(),
                        body: vec![0, 255, 1],
                    }),
                }
            })
        });
        let server = relay_test_server(relay).await;
        let raw = relay_request(&server, "GET", "/health", "", None).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 503);
        assert!(head_has(&head, "Content-Type: application/problem+json"));
        assert_eq!(body, br#"{"error":{"message":"unreachable"}}"#);
        let raw = relay_request(&server, "GET", "/api/ps", "", None).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 502);
        assert_eq!(
            serde_json::from_slice::<Value>(body).unwrap()["error"]["message"],
            BACKEND_ERROR_MESSAGE
        );
        let raw = relay_request(&server, "GET", "/v1/voices", "", None).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        assert!(head_has(&head, "Content-Type: application/octet-stream"));
        assert_eq!(body, &[0, 255, 1]);
        server.stop();
    }

    #[tokio::test]
    async fn tcai_model_details_decode_ids_and_preserve_token_requirement() {
        let _authz = authz_test().await;
        let (relay, _) = recording_relay(|_| panic!("models must not relay"));
        let server = relay_test_server(relay).await;
        require_token(true);
        let token = create_token(json!({}));
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            let path = format!("{prefix}/models/org%2Fmodel");
            let raw = token_request(&server, &path, None, None).await;
            assert_denied(&raw, 401, "unauthorized");
            let raw = token_request(&server, &path, Some(&token), None).await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            assert_eq!(
                serde_json::from_slice::<Value>(body).unwrap(),
                json!({"id":"org/model","object":"model","owned_by":"mistl"})
            );
            for id in ["missing", "%FF", "%2"] {
                let raw = token_request(
                    &server,
                    &format!("{prefix}/models/{id}"),
                    Some(&token),
                    None,
                )
                .await;
                let (head, body) = split_response(&raw);
                assert_eq!(status_code(&head), 404);
                assert_eq!(
                    serde_json::from_slice::<Value>(body).unwrap()["error"]["code"],
                    "model_not_found"
                );
            }
        }
        let restricted = create_token(json!({"models":["m1"]}));
        let raw = token_request(&server, "/v1/models/org%2Fmodel", Some(&restricted), None).await;
        assert_denied(&raw, 403, "model_not_allowed");
        server.stop();
    }

    #[tokio::test]
    async fn tcai_authorization_meters_generation_and_requires_a_model() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| relay_response(200, json!({})));
        let server = relay_test_server(relay).await;
        require_token(true);
        for path in [
            "/v1/voices".to_string(),
            "/voices".into(),
            "/api/ps".into(),
            "/health".into(),
            "/v1/3d/generations".into(),
            format!("/v1/3d/generations/{GENERATION_ID}"),
            format!("/v1/3d/generations/{GENERATION_ID}/content"),
            "/v1/rooms/team%2Fone/voices".into(),
        ] {
            let raw = token_request(&server, &path, None, None).await;
            assert_denied(&raw, 401, "unauthorized");
        }
        for path in ["/v1/3d/generations", "/v1/rooms/team%2Fone/3d/generations"] {
            let raw = token_request(&server, path, None, Some(("application/json", "{}"))).await;
            assert_denied(&raw, 401, "unauthorized");
        }
        let raw = relay_request(
            &server,
            "DELETE",
            &format!("/v1/3d/generations/{GENERATION_ID}"),
            "",
            None,
        )
        .await;
        assert_denied(&raw, 401, "unauthorized");
        assert!(seen.lock().unwrap().is_empty());
        let token = create_token(json!({"models":["3d-model"]}));
        // No built-in default model: a model-restricted token cannot create
        // a job without naming an allowed model, and nothing is metered.
        for body in ["{}", r#"{"model":""}"#, r#"{"model":null}"#] {
            let raw = token_request(
                &server,
                "/v1/3d/generations",
                Some(&token),
                Some(("application/json", body)),
            )
            .await;
            assert_denied(&raw, 403, "model_not_allowed");
        }
        let raw = token_request(
            &server,
            "/v1/3d/generations",
            Some(&token),
            Some(("application/json", r#"{"model":"3d-model"}"#)),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 200);
        assert_eq!(token_usage()["requests"], 1);
        let raw = token_request(
            &server,
            "/v1/3d/generations",
            Some(&token),
            Some(("application/json", r#"{"model":"other"}"#)),
        )
        .await;
        assert_denied(&raw, 403, "model_not_allowed");
        assert_eq!(token_usage()["requests"], 1);
        server.stop();
    }

    #[tokio::test]
    async fn tcai_body_caps_and_object_validation() {
        let _authz = authz_test().await;
        let (relay, seen) = recording_relay(|_| relay_response(200, json!({})));
        let server = relay_test_server(relay).await;
        for (path, content_type, cap) in [
            (
                "/v1/3d/generations",
                "application/json",
                MAX_GEN3D_JSON_BYTES,
            ),
            (
                "/v1/3d/generations",
                "multipart/form-data; boundary=b",
                MAX_GEN3D_MULTIPART_BYTES,
            ),
            ("/v1/audio/speech", "application/json", MAX_JSON_BODY_BYTES),
        ] {
            let raw = send_request(server.addr(), &format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n", cap+1)).await;
            assert_eq!(status_code(&split_response(&raw).0), 413);
        }
        for body in ["null", "[]", "true", "\"text\""] {
            let raw = relay_request(
                &server,
                "POST",
                "/v1/3d/generations",
                "application/json",
                Some(body),
            )
            .await;
            assert_eq!(status_code(&split_response(&raw).0), 400);
        }
        assert!(seen.lock().unwrap().is_empty());
        let body =
            json!({"model":"3d-model","image":"x".repeat(super::super::oai_tunnel::MAX_BYTES)})
                .to_string();
        let raw = relay_request(
            &server,
            "POST",
            "/v1/rooms/team%2Fone/3d/generations",
            "application/json",
            Some(&body),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 413);
        assert!(seen.lock().unwrap().is_empty());
        let raw = relay_request(
            &server,
            "POST",
            "/v1/3d/generations",
            "application/json",
            Some(&body),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 200);
        server.stop();
    }

    #[tokio::test]
    async fn speech_preserves_extra_fields_and_upstream_client_errors() {
        let _authz = authz_test().await;
        let call: TtsFn = Arc::new(|params, lang| {
            Box::pin(async move {
                assert_eq!(lang.as_deref(), Some("ja"));
                assert_eq!(params.extra, json!({"keep_alive":120,"vendor_options":{"seed":42},"stream_format":"sse","future":null}).as_object().unwrap().clone());
                Ok(tts::TtsAudio {
                    bytes: b"speech".to_vec(),
                    mime: "audio/mpeg".into(),
                })
            })
        });
        let server = ApiServer::start(
            "127.0.0.1:0",
            fake_call_ok(),
            fake_models(),
            call,
            fake_stt(),
        )
        .await
        .unwrap();
        let raw = post_json(&server, "/v1/audio/speech", json!({"model":"tts-model","input":"hi","voice":"default","response_format":"mp3","speed":1,"lang":"ja",
            "keep_alive":120,"vendor_options":{"seed":42},"stream_format":"sse","future":null})).await;
        assert_eq!(split_response(&raw).1, b"speech");
        server.stop();
        for (label, status, detail, expected) in [
            (
                "TTS API",
                422,
                r#"{"error":{"message":"bad voice","code":"invalid_voice"}}"#,
                422,
            ),
            ("TTS API", 503, "queued", 503),
            ("TTS API", 400, "bad input", 400),
            ("TTS API", 500, "private detail", 502),
            ("other", 422, "private detail", 502),
        ] {
            let call: TtsFn = Arc::new(move |_, _| {
                Box::pin(async move {
                    Err(anyhow::Error::new(super::super::openai::UpstreamHttpError {
                        label,
                        status,
                        detail: detail.into(),
                    })
                    .context("wrapped"))
                })
            });
            let server = ApiServer::start(
                "127.0.0.1:0",
                fake_call_ok(),
                fake_models(),
                call,
                fake_stt(),
            )
            .await
            .unwrap();
            let raw = post_json(&server, "/v1/audio/speech", json!({"input":"hi"})).await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), expected);
            let value: Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                value["error"]["message"],
                if expected == 502 {
                    BACKEND_ERROR_MESSAGE
                } else if status == 422 {
                    "bad voice"
                } else {
                    detail
                }
            );
            if status == 422 && label == "TTS API" {
                assert_eq!(value["error"]["code"], "invalid_voice");
            }
            server.stop();
        }
    }

    struct AuthzTestGuard {
        _lock: tokio::sync::MutexGuard<'static, ()>,
    }

    impl Drop for AuthzTestGuard {
        fn drop(&mut self) {
            crate::authz::test_reset();
        }
    }

    async fn authz_test() -> AuthzTestGuard {
        let lock = crate::authz::TEST_LOCK.lock().await;
        crate::authz::test_reset();
        AuthzTestGuard { _lock: lock }
    }

    fn create_token(extra: Value) -> String {
        let mut args = json!({"name": "API test"});
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        crate::authz::test_command("authz.token.create", args).unwrap()["token"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn require_token(required: bool) {
        crate::authz::test_command("authz.api.set", json!({"require_token": required})).unwrap();
    }

    async fn token_request(
        server: &ApiServer,
        path: &str,
        token: Option<&str>,
        body: Option<(&str, &str)>,
    ) -> Vec<u8> {
        let authorization =
            token.map_or_else(String::new, |t| format!("Authorization: Bearer {t}\r\n"));
        let request = if let Some((content_type, body)) = body {
            format!(
                "POST {path} HTTP/1.1\r\nHost: localhost\r\n{authorization}Content-Type: {content_type}\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
        } else {
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n{authorization}\r\n")
        };
        send_request(server.addr(), &request).await
    }

    fn assert_denied(raw: &[u8], status: u16, code: &str) {
        let (head, body) = split_response(raw);
        assert_eq!(status_code(&head), status, "{head}");
        let value: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(value["error"]["type"], "invalid_request_error");
        assert_eq!(value["error"]["code"], code);
        assert!(
            value["error"]["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty())
        );
        if status == 401 {
            assert!(head_has(&head, "WWW-Authenticate: Bearer"));
        }
    }

    fn token_usage() -> Value {
        let usage = crate::authz::test_command("authz.usage", json!({"service": "ai"})).unwrap();
        let rows = usage["ai"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        rows[0].clone()
    }

    const CHAT_BODY: &str = r#"{"model":"m1","messages":[{"role":"user","content":"hi"}]}"#;
    const DECISION_BODY: &str =
        r#"{"model":"m1","state":"s","questions":{"q":{"type":"noul","instructions":"?"}}}"#;
    const AUDIO_BODY: &str = "--b\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nm1\r\n--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.wav\"\r\nContent-Type: audio/wav\r\n\r\naudio\r\n--b--\r\n";

    #[tokio::test]
    async fn api_tokens_authenticate_default_and_room_endpoints() {
        let _authz = authz_test().await;
        let token = create_token(json!({}));
        let unknown_token = format!("mst_{}", "x".repeat(43));
        require_token(true);
        let (server, seen) = room_test_server().await;
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for (endpoint, body) in [
                ("models", None),
                ("chat/completions", Some(("application/json", CHAT_BODY))),
                (
                    "audio/speech",
                    Some(("application/json", r#"{"model":"m1","input":"hi"}"#)),
                ),
                (
                    "audio/transcriptions",
                    Some(("multipart/form-data; boundary=b", AUDIO_BODY)),
                ),
                ("systemone", Some(("application/json", DECISION_BODY))),
            ] {
                let path = format!("{prefix}/{endpoint}");
                for credential in [None, Some(unknown_token.as_str())] {
                    let raw = token_request(&server, &path, credential, body).await;
                    assert_denied(&raw, 401, "unauthorized");
                }
                let raw = token_request(&server, &path, Some(&token), body).await;
                let expected = if path == "/v1/systemone" { 404 } else { 200 };
                assert_eq!(status_code(&split_response(&raw).0), expected, "{path}");
            }
        }
        assert_eq!(seen.lock().unwrap().len(), 4);
        assert_eq!(token_usage()["requests"], 8);
    }

    #[tokio::test]
    async fn api_tokens_filter_models_and_refuse_disallowed_inference() {
        let _authz = authz_test().await;
        let token = create_token(json!({"models": ["m1"]}));
        require_token(true);
        let (server, seen) = room_test_server().await;
        for (path, expected) in [
            (
                "/v1/models",
                json!([{"id":"m1","object":"model","owned_by":"mistl"}]),
            ),
            ("/v1/rooms/team%2Fone/models", json!([])),
        ] {
            let raw = token_request(&server, path, Some(&token), None).await;
            let (head, body) = split_response(&raw);
            assert_eq!(status_code(&head), 200);
            assert_eq!(
                serde_json::from_slice::<Value>(body).unwrap()["data"],
                expected
            );
        }
        for prefix in ["/v1", "/v1/rooms/team%2Fone"] {
            for body in [
                json!({"model":"b", "messages":[{"role":"user","content":"hi"}]}),
                json!({"messages":[{"role":"user","content":"hi"}]}),
                json!({"model":"b", "messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]}),
            ] {
                let body = body.to_string();
                let raw = token_request(
                    &server,
                    &format!("{prefix}/chat/completions"),
                    Some(&token),
                    Some(("application/json", &body)),
                )
                .await;
                assert_denied(&raw, 403, "model_not_allowed");
            }
            let raw = token_request(
                &server,
                &format!("{prefix}/audio/speech"),
                Some(&token),
                Some(("application/json", r#"{"model":"b","input":"hi"}"#)),
            )
            .await;
            assert_denied(&raw, 403, "model_not_allowed");
            let body = AUDIO_BODY.replace("\r\nm1\r\n", "\r\nb\r\n");
            let raw = token_request(
                &server,
                &format!("{prefix}/audio/transcriptions"),
                Some(&token),
                Some(("multipart/form-data; boundary=b", &body)),
            )
            .await;
            assert_denied(&raw, 403, "model_not_allowed");
        }
        assert!(seen.lock().unwrap().is_empty());
        let info = crate::authz::test_command("authz.token.list", json!({})).unwrap();
        crate::authz::test_command(
            "authz.token.update",
            json!({"id":info[0]["id"], "models":["room-raw"]}),
        )
        .unwrap();
        let raw = token_request(&server, "/v1/rooms/team%2Fone/models", Some(&token), None).await;
        let value: Value = serde_json::from_slice(split_response(&raw).1).unwrap();
        assert_eq!(value["data"][0]["id"], "room-raw");
    }

    #[tokio::test]
    async fn api_tokens_meter_each_response_and_enforce_request_limits() {
        let _authz = authz_test().await;
        let stream_body =
            json!({"model":"m1","messages":[{"role":"user","content":"hi"}],"stream":true})
                .to_string();
        let image_body = |stream| {
            json!({"model":"m1", "stream":stream,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]}).to_string()
        };
        let image = image_body(false);
        let image_stream = image_body(true);
        let (room_server, _) = room_test_server().await;
        let default_server = start_test_server(fake_call_ok(), fake_models()).await;
        for (server, path, content_type, body, chars) in [
            (
                &default_server,
                "/v1/chat/completions",
                "application/json",
                CHAT_BODY,
                5,
            ),
            (
                &default_server,
                "/v1/chat/completions",
                "application/json",
                stream_body.as_str(),
                5,
            ),
            (
                &default_server,
                "/v1/audio/speech",
                "application/json",
                r#"{"input":"hi"}"#,
                0,
            ),
            (
                &default_server,
                "/v1/audio/transcriptions",
                "multipart/form-data; boundary=b",
                AUDIO_BODY,
                0,
            ),
            (
                &room_server,
                "/v1/rooms/team%2Fone/chat/completions",
                "application/json",
                image.as_str(),
                12,
            ),
            (
                &room_server,
                "/v1/rooms/team%2Fone/chat/completions",
                "application/json",
                image_stream.as_str(),
                12,
            ),
        ] {
            crate::authz::test_reset();
            let token = create_token(json!({"limits":{"max_requests":1}}));
            require_token(true);
            let raw = token_request(server, path, Some(&token), Some((content_type, body))).await;
            assert_eq!(status_code(&split_response(&raw).0), 200, "{path}");
            let usage = token_usage();
            assert_eq!(usage["requests"], 1);
            assert_eq!(usage["output_chars"], chars, "{path}");
            let raw = token_request(server, path, Some(&token), Some((content_type, body))).await;
            assert_denied(&raw, 429, "usage_limit_exceeded");
        }
    }

    #[tokio::test]
    async fn api_tokens_meter_partial_stream_output_and_unicode_characters() {
        let _authz = authz_test().await;
        let body = json!({"messages":[{"role":"user","content":"hi"}],"stream":true}).to_string();
        let unicode_call: LlmCallFn = Arc::new(|_, _, _, tx| {
            Box::pin(async move {
                let text = "\u{65e5}\u{672c}\u{1f30d}";
                if let Some(tx) = tx {
                    let _ = tx.send(text.into());
                }
                Ok(ChatOutput::from(text.to_string()))
            })
        });
        for (call, chars) in [(fake_call_midstream_err(), 7), (unicode_call, 3)] {
            crate::authz::test_reset();
            let token = create_token(json!({"limits":{"max_output_chars":chars}}));
            let server = start_test_server(call, fake_models()).await;
            let raw = token_request(
                &server,
                "/v1/chat/completions",
                Some(&token),
                Some(("application/json", &body)),
            )
            .await;
            assert_eq!(status_code(&split_response(&raw).0), 200);
            let usage = token_usage();
            assert_eq!(usage["requests"], 1);
            assert_eq!(usage["output_chars"], chars);
            let raw = token_request(
                &server,
                "/v1/chat/completions",
                Some(&token),
                Some(("application/json", &body)),
            )
            .await;
            assert_denied(&raw, 429, "usage_limit_exceeded");
        }
    }

    #[tokio::test]
    async fn api_tokens_keep_optional_legacy_auth_and_preflight_cors() {
        let _authz = authz_test().await;
        let server = start_cors_server(&["https://app.example.com"]).await;
        require_token(false);
        let raw = token_request(
            &server,
            "/v1/chat/completions",
            Some("sk-dummy"),
            Some(("application/json", CHAT_BODY)),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 200);
        let raw = token_request(&server, "/v1/models", Some("mst_invalid"), None).await;
        assert_denied(&raw, 401, "unauthorized");
        require_token(true);
        let raw = send_request(server.addr(), "OPTIONS /v1/models HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\nAccess-Control-Request-Method: GET\r\n\r\n").await;
        assert_eq!(status_code(&split_response(&raw).0), 204);
        let raw = send_request(
            server.addr(),
            "GET /v1/models HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\n\r\n",
        )
        .await;
        assert_denied(&raw, 401, "unauthorized");
        assert!(head_has(
            &split_response(&raw).0,
            "Access-Control-Allow-Origin: https://app.example.com"
        ));
    }

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
        let local_seen = seen.clone();
        let local_decision: DecisionFn = Arc::new(move |body: Vec<u8>| {
            local_seen
                .lock()
                .unwrap()
                .push(json!({"route":"local-decision","raw":String::from_utf8(body).unwrap()}));
            Box::pin(async {
                Ok(super::super::oai_tunnel::TunnelResponse {
                    status: 404,
                    content_type: "application/json".into(),
                    body: br#"{"error":"not a decision model"}"#.to_vec(),
                })
            })
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
                let decision_records = records.clone();
                let decision: DecisionFn = Arc::new(move |body: Vec<u8>| {
                    decision_records
                        .lock()
                        .unwrap()
                        .push(json!({"route":"decision","raw":String::from_utf8(body).unwrap()}));
                    Box::pin(async {
                        Ok(super::super::oai_tunnel::TunnelResponse {
                            status: 200,
                            content_type: "application/json".into(),
                            body: serde_json::to_vec(
                                &json!({"model":"nimble","answers":{"q":{"type":"noul","noul":0.75}}}),
                            )
                            .unwrap(),
                        })
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
                    decision,
                    relay: Arc::new(|_| Box::pin(async { anyhow::bail!("not used") })),
                }))
            })
        });
        let server = ApiServer::start_with_rooms(
            "127.0.0.1:0",
            call,
            fake_models(),
            fake_tts(),
            fake_stt(),
            Some(local_decision),
            None,
            Some(rooms),
            None,
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
        assert_eq!(
            room_route("/v1/rooms/team/systemone"),
            Some(("team".into(), "systemone".into()))
        );
    }

    #[tokio::test]
    async fn decision_requests_are_relayed_verbatim() {
        let _authz = authz_test().await;
        let (server, seen) = room_test_server().await;
        let ordered = r#"{"model":"nimble","state":"s","questions":{"q":{"type":"choice","instructions":"?","criteria":{"zebra":null,"apple":null}}}}"#;
        let raw = send_request(server.addr(), &format!("POST /v1/rooms/team%2Fone/systemone HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{ordered}", ordered.len())).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        let answer: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(answer["answers"]["q"]["noul"], 0.75);
        {
            let records = seen.lock().unwrap();
            let sent = records.last().unwrap();
            assert_eq!(sent["route"], "decision");
            assert_eq!(sent["raw"], ordered, "body is relayed byte-for-byte");
        }
        for bad in [
            json!([1]),
            json!({"questions":{"q":{"type":"noul","instructions":"?"}}}),
            json!({"state":"x","questions":{}}),
        ] {
            let raw = post_json(&server, "/v1/rooms/team%2Fone/systemone", bad.clone()).await;
            assert_eq!(status_code(&split_response(&raw).0), 400, "{bad}");
        }
        // Unscoped requests take the local resolver; the upstream status and
        // body come back unchanged.
        let raw = send_request(server.addr(), &format!("POST /v1/systemone HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{ordered}", ordered.len())).await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 404);
        assert_eq!(body, br#"{"error":"not a decision model"}"#);
        let sent = seen.lock().unwrap().last().unwrap().clone();
        assert_eq!(sent["route"], "local-decision");
        assert_eq!(sent["raw"], ordered);
        let raw = post_json(
            &server,
            "/v1/systemone",
            json!({"state":"x","questions":{}}),
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 400);
        server.stop();
    }

    #[tokio::test]
    async fn default_and_room_chat_pass_effort_with_both_response_modes() {
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
            assert_eq!(sent["body"]["temperature"], 0.4);
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
            "stt-model\r\n",
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
        assert_eq!(model.bytes, b"stt-model");
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
        let _authz = authz_test().await;
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = json!({"model": "tts-model", "voice": "alloy", "input": "hi"}).to_string();
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
        let _authz = authz_test().await;
        let server = start_test_server(fake_call_ok(), fake_models()).await;
        let payload = json!({"model": "tts-model", "input": "   "}).to_string();
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
        let _authz = authz_test().await;
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
            "stt-model\r\n",
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
        assert_eq!(json["text"], "stt-model|clip.wav|RIFF");
    }

    #[tokio::test]
    async fn audio_transcriptions_requires_multipart() {
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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

    async fn start_cors_server(allowed: &[&str]) -> Arc<ApiServer> {
        let allowed: Vec<String> = allowed.iter().map(|s| s.to_string()).collect();
        ApiServer::start_with_rooms(
            "127.0.0.1:0",
            fake_call_ok(),
            fake_models(),
            fake_tts(),
            fake_stt(),
            None,
            None,
            None,
            Some(Arc::new(move || allowed.clone())),
        )
        .await
        .unwrap()
    }

    fn head_has(head: &str, line: &str) -> bool {
        head.lines().any(|l| l.eq_ignore_ascii_case(line))
    }

    #[tokio::test]
    async fn allowed_origin_gets_preflight_and_cors_headers() {
        let _authz = authz_test().await;
        let server = start_cors_server(&["HTTPS://App.Example.com/"]).await;
        let addr = server.addr();

        let raw = send_request(
            addr,
            "OPTIONS /v1/models HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\nAccess-Control-Request-Method: GET\r\nAccess-Control-Request-Headers: authorization, x-stainless-os\r\nAccess-Control-Request-Private-Network: true\r\n\r\n",
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 204, "{head}");
        assert!(body.is_empty());
        for line in [
            "Access-Control-Allow-Origin: https://app.example.com",
            "Vary: Origin",
            "Access-Control-Allow-Methods: GET, POST, DELETE, OPTIONS",
            "Access-Control-Allow-Headers: authorization, x-stainless-os",
            "Access-Control-Allow-Private-Network: true",
        ] {
            assert!(head_has(&head, line), "missing {line:?} in {head}");
        }

        // A header list that is not plain names is not echoed.
        let raw = send_request(
            addr,
            "OPTIONS /v1/models HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\nAccess-Control-Request-Headers: x-a;evil\r\n\r\n",
        )
        .await;
        let head = split_response(&raw).0;
        assert!(head_has(
            &head,
            "Access-Control-Allow-Headers: authorization, content-type"
        ));
        assert!(!head.contains("Private-Network"), "{head}");

        let raw = send_request(
            addr,
            "GET /v1/models HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\n\r\n",
        )
        .await;
        let (head, body) = split_response(&raw);
        assert_eq!(status_code(&head), 200);
        assert!(head_has(
            &head,
            "Access-Control-Allow-Origin: https://app.example.com"
        ));
        let v: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(v["data"][0]["id"], "m1");

        // Errors after the guard carry the grant too, so the page can read them.
        let raw = send_request(
            addr,
            "GET /v1/nope HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\n\r\n",
        )
        .await;
        let head = split_response(&raw).0;
        assert_eq!(status_code(&head), 404);
        assert!(head_has(
            &head,
            "Access-Control-Allow-Origin: https://app.example.com"
        ));

        let body = "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"stream\":true}";
        let raw = send_request(
            addr,
            &format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nOrigin: https://app.example.com\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        let head = split_response(&raw).0;
        assert_eq!(status_code(&head), 200);
        assert!(head_has(&head, "Content-Type: text/event-stream"));
        assert!(head_has(
            &head,
            "Access-Control-Allow-Origin: https://app.example.com"
        ));
    }

    #[tokio::test]
    async fn unlisted_origins_stay_refused_and_get_no_grant() {
        let _authz = authz_test().await;
        let server = start_cors_server(&["https://app.example.com"]).await;
        let addr = server.addr();
        for origin in [
            "https://evil.example.com",
            "http://app.example.com",
            "https://app.example.com:8443",
            "null",
        ] {
            for method in ["OPTIONS", "GET"] {
                let raw = send_request(
                    addr,
                    &format!(
                        "{method} /v1/models HTTP/1.1\r\nHost: localhost\r\nOrigin: {origin}\r\n\r\n"
                    ),
                )
                .await;
                let head = split_response(&raw).0;
                assert_eq!(status_code(&head), 403, "{method} {origin}");
                assert!(!head.contains("Access-Control"), "{head}");
            }
        }
        // Native clients (no Origin) are unaffected and get no CORS headers.
        let raw = send_request(addr, "GET /v1/models HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        let head = split_response(&raw).0;
        assert_eq!(status_code(&head), 200);
        assert!(!head.contains("Access-Control"), "{head}");
        // The Host (DNS rebinding) guard still applies to allowed origins.
        let raw = send_request(
            addr,
            "GET /v1/models HTTP/1.1\r\nHost: evil.example.com\r\nOrigin: https://app.example.com\r\n\r\n",
        )
        .await;
        assert_eq!(status_code(&split_response(&raw).0), 403);
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
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
        let _authz = authz_test().await;
        let server = start_test_server(fake_call_err(), fake_models()).await;
        let raw = send_request(server.addr(), &post_chat(&tool_request_body(false))).await;
        let (head, _) = split_response(&raw);
        assert_eq!(status_code(&head), 502, "{head}");
    }
}
