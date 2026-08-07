//! Hand-rolled HTTP/1.1 server for the dashboard (same style as
//! `crate::ai::api_server`): tokio TCP accept loop, minimal request parse,
//! a handful of routes.
//!
//! Routes:
//! - `GET /` (and `GET /index.html`) -> 200 `text/html`, [`super::INDEX_HTML`]
//! - `GET /favicon.png` -> 200 `image/png`, [`super::FAVICON_PNG`]
//! - `POST /api/call` -> body `{"cmd": "...", "args": {...}}`; runs
//!   [`crate::daemon::dispatch`] and answers 200 with
//!   `{"ok":true,"data":...}` or `{"ok":false,"error":"..."}`
//! - `POST /api/store/upload` -> body is the raw file bytes, name carried in
//!   the `x-file-name` header (percent-encoded UTF-8); streamed to a temp
//!   file, then `store.put`'d. Answers 200 `{"ok":...}` like `/api/call`.
//! - `GET /api/store/download?cid=...` -> `store.get`'s the cid to a temp
//!   file and streams it back with a `Content-Disposition: attachment`
//!   header, or 404 JSON error if the cid is unknown.
//! - `POST /api/store/sandbox-upload` -> identical protocol to
//!   `/api/store/upload`, but the file lands in the path-jailed content
//!   sandbox via `store.sandbox.import` instead of the content store via
//!   `store.put`.
//! - `GET /api/store/sandbox-download?path=...` -> `store.sandbox.export`'s
//!   the (percent-encoded, sandbox-relative) path to a temp file and streams
//!   it back the same way `/api/store/download` does, or 404 JSON error if
//!   the path is unknown/outside the sandbox.
//! - `GET /api/dev/instance` -> 200 JSON `{"instance": "<hex>"}`, a random
//!   nonce picked once per process start. In debug builds the dashboard HTML
//!   ([`index_html_body`]) carries an extra script that polls this and
//!   reloads the page when the value changes, so a tab left open across a
//!   `just watch` rebuild+restart picks up the new daemon on its own instead
//!   of needing a manual refresh. Release builds serve the plain embedded
//!   HTML (no poll), but the route itself stays cheap and harmless either
//!   way (a per-process random number, nothing sensitive).
//! - anything else -> 404 JSON error
//!
//! Security (no auth token; loopback bind):
//! - `POST /api/call`, `POST /api/store/upload`, and
//!   `POST /api/store/sandbox-upload` require the request header
//!   `x-mistl-ui: 1`. Browsers cannot attach custom headers cross-origin
//!   without a CORS preflight, and this server never sends CORS headers
//!   (OPTIONS -> 403), so web pages from other origins can't drive the
//!   daemon (CSRF protection).
//! - `GET /api/store/download` and `GET /api/store/sandbox-download`
//!   deliberately skip the `x-mistl-ui` check; see the doc comment on
//!   `handle_store_download` for why that's still safe.
//! - The `Host` header must be `127.0.0.1[:port]` or `localhost[:port]`
//!   (DNS-rebinding protection).
//!
//! ## HTTP parsing (keep it minimal but correct)
//!
//! Read the request line + headers (case-insensitive names) up to
//! [`MAX_HEADER_BYTES`], then exactly `Content-Length` body bytes (411 if
//! missing on `POST /api/call`; 413 over [`MAX_BODY_BYTES`]). One request per
//! connection, `Connection: close`. Tolerate both `\r\n` and bare `\n`.

use std::io;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

use crate::daemon::AppState;

/// Header section size cap (request-line + headers), matches doc.
const MAX_HEADER_BYTES: usize = 16 * 1024;
/// Body size cap for `POST /api/call`.
const MAX_BODY_BYTES: usize = 1024 * 1024;
/// Body size cap for `POST /api/store/upload` (streamed to disk, not memory).
const MAX_UPLOAD_BYTES: u64 = 1024 * 1024 * 1024;
/// Chunk size used when streaming an upload/download body to/from disk.
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// Running dashboard server; aborts its accept loop on [`WebServer::close`].
pub struct WebServer {
    listen: String,
    handle: tokio::task::JoinHandle<()>,
}

impl WebServer {
    /// Human-facing URL of the dashboard root.
    pub fn url(&self) -> String {
        format!("http://{}/", self.listen)
    }

    /// Stop accepting connections.
    pub async fn close(self) {
        self.handle.abort();
    }
}

/// Bind `listen` and serve the dashboard until closed.
pub async fn serve(state: Arc<AppState>, listen: &str) -> Result<WebServer> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding web dashboard to {listen}"))?;
    let listen = listen.to_string();

    // Per-process nonce for the live-reload poll (see `/api/dev/instance`
    // above): a fresh value every time the daemon (re)starts, which is what
    // an open browser tab uses to notice `just watch` restarted it.
    let instance_id = format!("{:016x}", rand::random::<u64>());

    let handle = tokio::spawn(async move {
        loop {
            let (socket, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(error) => {
                    warn!(%error, "web dashboard accept failed");
                    continue;
                }
            };
            let state = state.clone();
            let instance_id = instance_id.clone();
            tokio::spawn(async move {
                handle_connection(socket, state, instance_id).await;
            });
        }
    });

    Ok(WebServer { listen, handle })
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

/// Like [`read_body`], but streams straight to `file` in fixed-size chunks
/// instead of accumulating the whole body in memory (uploads can be up to
/// [`MAX_UPLOAD_BYTES`]).
async fn read_body_to_file(
    stream: &mut TcpStream,
    leftover: Vec<u8>,
    content_length: u64,
    file: &mut File,
) -> io::Result<()> {
    let mut written: u64 = 0;
    if !leftover.is_empty() {
        let take = (leftover.len() as u64).min(content_length) as usize;
        file.write_all(&leftover[..take]).await?;
        written += take as u64;
    }
    let mut chunk = [0u8; STREAM_CHUNK_BYTES];
    while written < content_length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before body complete",
            ));
        }
        let take = (n as u64).min(content_length - written) as usize;
        file.write_all(&chunk[..take]).await?;
        written += take as u64;
    }
    file.flush().await
}

/// The dashboard HTML to serve for `GET /`. In debug builds this is
/// [`super::INDEX_HTML`] with [`LIVE_RELOAD_SCRIPT`] spliced in before
/// `</body>`; release builds get the embedded HTML unmodified.
fn index_html_body() -> std::borrow::Cow<'static, str> {
    if cfg!(debug_assertions) {
        std::borrow::Cow::Owned(super::INDEX_HTML.replacen("</body>", LIVE_RELOAD_SCRIPT, 1))
    } else {
        std::borrow::Cow::Borrowed(super::INDEX_HTML)
    }
}

/// Debug-only live-reload poll: fetches the per-process `/api/dev/instance`
/// nonce every second and reloads the page the first time it changes,
/// which happens whenever `just watch` rebuilds and restarts the daemon. A
/// poll that fails outright (daemon down mid-rebuild) is swallowed and just
/// retried on the next tick.
const LIVE_RELOAD_SCRIPT: &str = r#"<script>
(function () {
  var lastInstance = null;
  function poll() {
    fetch("/api/dev/instance", { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (json) {
        var id = json && json.data && json.data.instance;
        if (!id) return;
        if (lastInstance === null) { lastInstance = id; return; }
        if (id !== lastInstance) { location.reload(); }
      })
      .catch(function () { /* daemon rebuilding; retry next tick */ });
  }
  setInterval(poll, 1000);
})();
</script>
</body>"#;

/// Whether `host` (the raw `Host` header value) is acceptable for a
/// connection that was actually accepted on `local_addr` (DNS-rebinding
/// guard).
///
/// The loopback names (`127.0.0.1`, `localhost`, `[::1]`) are always
/// accepted, regardless of what the dashboard is bound to -- loopback is
/// reachable no matter which interface `ui.listen` picked. On top of that,
/// a `Host` naming the literal IP `local_addr` was accepted on is also
/// accepted: this is what lets `ui.listen`/`--host` bind to `0.0.0.0` (or a
/// specific LAN IP) and be reached from another device without every
/// `/api/call` 403ing. It doesn't weaken the guard -- a hostile page can
/// only make a browser send a `Host` claiming an address the browser
/// actually opened the TCP connection to (`local_addr` is the real accepted
/// socket, not something the client can spoof), so an attacker's own origin
/// still can't rebind their way past this check.
fn host_is_allowed(host: &str, local_addr: &std::net::SocketAddr) -> bool {
    let host = host.trim();
    if host.is_empty() {
        return false;
    }
    if let Some(rest) = host.strip_prefix('[') {
        // Bracketed IPv6 literal, e.g. "[::1]" or "[::1]:8080".
        let Some(end) = rest.find(']') else {
            return false;
        };
        let addr = &rest[..end];
        let after = &rest[end + 1..];
        let port_ok = after.is_empty() || (after.starts_with(':') && is_valid_port(&after[1..]));
        if !port_ok {
            return false;
        }
        return addr == "::1" || matches_local_addr(addr, None, local_addr);
    }
    let (name, port) = match host.rsplit_once(':') {
        Some((name, port)) if is_valid_port(port) => (name, Some(port)),
        _ => (host, None),
    };
    if name.eq_ignore_ascii_case("127.0.0.1") || name.eq_ignore_ascii_case("localhost") {
        return true;
    }
    matches_local_addr(name, port, local_addr)
}

/// Whether a request must be refused by the dashboard's guards -- the CSRF
/// header (`require_ui_header`, false only for the plain-navigation download
/// endpoints that deliberately skip it) and the `Host` allowlist -- logging
/// which one fired.
///
/// The logging is the whole point of routing every guard through here: a 403
/// tells the browser only "forbidden" and left no trace at all in
/// `daemon.log`, so a dashboard button that appeared to do nothing (a page
/// opened from `file://`, a tab reaching the daemon under a hostname the
/// allowlist doesn't cover) could not be told apart from a broken handler
/// after the fact.
fn is_guard_denied(
    head: &RequestHead,
    local_addr: &std::net::SocketAddr,
    require_ui_header: bool,
) -> bool {
    let host = head.header("host").unwrap_or("");
    if require_ui_header && head.header("x-mistl-ui").is_none() {
        warn!(
            path = %head.path,
            %host,
            "refused a dashboard request with no x-mistl-ui header (CSRF guard)"
        );
        return true;
    }
    if !host_is_allowed(host, local_addr) {
        warn!(
            path = %head.path,
            %host,
            bound = %local_addr,
            "refused a dashboard request whose Host is not loopback or the bound address (DNS-rebinding guard)"
        );
        return true;
    }
    false
}

/// Whether `name` (an IP literal) and optional `port` match `local_addr` --
/// i.e. the `Host` header claims exactly the address the connection was
/// actually accepted on.
fn matches_local_addr(name: &str, port: Option<&str>, local_addr: &std::net::SocketAddr) -> bool {
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
}

fn is_valid_port(port: &str) -> bool {
    !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
}

/// Percent-decode `input` (`%XX` escapes) into a UTF-8 string.
///
/// A malformed escape (a `%` not followed by two hex digits) fails the
/// whole decode rather than passing the raw bytes through, same as an
/// invalid UTF-8 byte sequence.
fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Percent-encode `s` for use as the value in `filename*=UTF-8''<value>`
/// (RFC 5987): everything but unreserved characters is escaped.
fn percent_encode_rfc5987(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Validate a decoded upload file name: no path separators, no `..`, no
/// characters that are illegal (or awkward) in a file name on common
/// filesystems, no control characters.
fn validate_upload_filename(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("file name must not be empty");
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err("file name must not contain path separators or `..`");
    }
    const FORBIDDEN: [char; 7] = ['<', '>', ':', '"', '|', '?', '*'];
    if name
        .chars()
        .any(|c| FORBIDDEN.contains(&c) || c.is_control())
    {
        return Err("file name contains forbidden characters");
    }
    Ok(())
}

/// Extract and percent-decode the value of `key` from a raw (undecoded)
/// query string such as `cid=abc%20def&x=1`. `None` if `key` is absent or
/// its value fails to percent-decode.
fn parse_query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if k == key { percent_decode(v) } else { None }
    })
}

/// Build the status-line + header block for a response (everything up to
/// and including the blank line before the body).
fn response_head(status: u16, reason: &str, content_type: &str, content_length: usize) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
    )
}

async fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &Value,
) -> io::Result<()> {
    let body_bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let head = response_head(status, reason, "application/json", body_bytes.len());
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body_bytes).await?;
    stream.flush().await
}

async fn write_html_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &str,
) -> io::Result<()> {
    // The dashboard HTML is baked into the binary (`include_str!`), so a
    // browser heuristically caching `/` (we send no validators) keeps
    // showing a stale UI after the daemon is rebuilt. `no-cache` forces a
    // fresh copy on every load.
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

async fn write_binary_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let head = response_head(status, reason, content_type, body.len());
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// JSON error body shape used by every non-2xx response from this server.
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
        &json!({"ok": false, "error": message}),
    )
    .await
}

/// Status-line + header block for a successful `/api/store/download`
/// response: like [`response_head`] but with a `Content-Disposition`
/// attachment header carrying `filename`.
fn download_response_head(content_length: u64, filename: &str) -> String {
    let encoded = percent_encode_rfc5987(filename);
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {content_length}\r\nContent-Disposition: attachment; filename=\"download\"; filename*=UTF-8''{encoded}\r\nConnection: close\r\n\r\n"
    )
}

/// Body shape for `POST /api/call`. `cmd` is kept as a raw [`Value`] so a
/// non-string `cmd` fails our own check (400) rather than serde's (which
/// would report a less specific message); a missing `cmd` field still fails
/// to deserialize (no `Default`), which is also a 400.
#[derive(serde::Deserialize)]
struct CallRequestBody {
    cmd: Value,
    #[serde(default)]
    args: Option<Value>,
}

async fn handle_connection(mut stream: TcpStream, state: Arc<AppState>, instance_id: String) {
    if let Err(error) = handle_connection_inner(&mut stream, state, &instance_id).await {
        debug!(%error, "web dashboard connection ended with error");
    }
}

async fn handle_connection_inner(
    stream: &mut TcpStream,
    state: Arc<AppState>,
    instance_id: &str,
) -> io::Result<()> {
    let (head, leftover) = match read_request_head(stream).await {
        Ok(Some(pair)) => pair,
        Ok(None) => return Ok(()),
        Err(_) => return write_error(stream, 400, "Bad Request", "malformed request").await,
    };

    let method = head.method.to_ascii_uppercase();
    let path = head.path.split('?').next().unwrap_or("").to_string();

    match (method.as_str(), path.as_str()) {
        ("GET", "/") | ("GET", "/index.html") => {
            // Page load: a strong signal the dashboard tab is open, used by
            // `web::autoreopen` to avoid reopening a duplicate after a
            // restart (and to decide what to persist at shutdown).
            state.note_dashboard_activity();
            write_html_response(stream, 200, "OK", index_html_body().as_ref()).await
        }
        ("GET", "/favicon.png") => {
            write_binary_response(stream, 200, "OK", "image/png", super::FAVICON_PNG).await
        }
        ("GET", "/api/dev/instance") => {
            write_json_response(
                stream,
                200,
                "OK",
                &json!({"ok": true, "data": {"instance": instance_id}}),
            )
            .await
        }
        ("POST", "/api/call") => {
            // The dashboard polls this every 5s while a tab is open (status
            // refresh), so it doubles as a liveness signal -- see the `GET
            // /` comment above.
            state.note_dashboard_activity();
            handle_api_call(stream, &head, leftover, state).await
        }
        ("POST", "/api/store/upload") => handle_store_upload(stream, &head, leftover, state).await,
        ("GET", "/api/store/download") => handle_store_download(stream, &head, state).await,
        ("POST", "/api/store/sandbox-upload") => {
            handle_store_sandbox_upload(stream, &head, leftover, state).await
        }
        ("GET", "/api/store/sandbox-download") => {
            handle_store_sandbox_download(stream, &head, state).await
        }
        // Never send CORS headers/allow preflights: any OPTIONS is refused.
        ("OPTIONS", _) => write_error(stream, 403, "Forbidden", "forbidden").await,
        _ => write_error(stream, 404, "Not Found", "not found").await,
    }
}

async fn handle_api_call(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: Arc<AppState>,
) -> io::Result<()> {
    // CSRF guard (browsers can't attach a custom header cross-origin without
    // a preflight, and we never answer preflights successfully) plus the
    // DNS-rebinding Host guard -- both logged by `is_guard_denied`.
    let local_addr = stream.local_addr()?;
    if is_guard_denied(head, &local_addr, true) {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }

    let content_length: usize = match head.header("content-length") {
        Some(v) => match v.trim().parse() {
            Ok(n) => n,
            Err(_) => {
                return write_error(stream, 400, "Bad Request", "invalid Content-Length").await;
            }
        },
        None => {
            return write_error(stream, 411, "Length Required", "Content-Length required").await;
        }
    };
    if content_length > MAX_BODY_BYTES {
        return write_error(stream, 413, "Payload Too Large", "request body too large").await;
    }
    let body = match read_body(stream, leftover, content_length).await {
        Ok(b) => b,
        Err(_) => return write_error(stream, 400, "Bad Request", "incomplete request body").await,
    };

    let parsed: CallRequestBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(error) => {
            return write_error(
                stream,
                400,
                "Bad Request",
                &format!("invalid request body: {error}"),
            )
            .await;
        }
    };
    let Some(cmd) = parsed.cmd.as_str() else {
        return write_error(stream, 400, "Bad Request", "cmd must be a string").await;
    };
    let args = parsed.args.unwrap_or_else(|| json!({}));

    // The frontend always gets HTTP 200 for a well-formed call and switches
    // on the `ok` field; only malformed/forbidden requests get non-2xx.
    match crate::daemon::dispatch(cmd, args, &state).await {
        Ok(data) => {
            write_json_response(stream, 200, "OK", &json!({"ok": true, "data": data})).await
        }
        Err(error) => {
            write_json_response(
                stream,
                200,
                "OK",
                &json!({"ok": false, "error": format!("{error:#}")}),
            )
            .await
        }
    }
}

/// `POST /api/store/upload`: body is the raw file bytes, name carried in
/// the `x-file-name` header. Streams the body to a fresh temp directory
/// (never buffered fully in memory), then hands it to `store.put`.
async fn handle_store_upload(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: Arc<AppState>,
) -> io::Result<()> {
    // Same guards as /api/call: CSRF (custom header) + DNS-rebinding (Host).
    let local_addr = stream.local_addr()?;
    if is_guard_denied(head, &local_addr, true) {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }

    let Some(raw_name) = head.header("x-file-name") else {
        return write_error(stream, 400, "Bad Request", "x-file-name header required").await;
    };
    let Some(file_name) = percent_decode(raw_name) else {
        return write_error(
            stream,
            400,
            "Bad Request",
            "x-file-name is not validly percent-encoded UTF-8",
        )
        .await;
    };
    if let Err(reason) = validate_upload_filename(&file_name) {
        return write_error(stream, 400, "Bad Request", reason).await;
    }

    let content_length: u64 = match head.header("content-length") {
        Some(v) => match v.trim().parse() {
            Ok(n) => n,
            Err(_) => {
                return write_error(stream, 400, "Bad Request", "invalid Content-Length").await;
            }
        },
        None => {
            return write_error(stream, 411, "Length Required", "Content-Length required").await;
        }
    };
    if content_length > MAX_UPLOAD_BYTES {
        return write_error(stream, 413, "Payload Too Large", "upload too large").await;
    }

    let dir = std::env::temp_dir().join(format!("mistl-upload-{:016x}", rand::random::<u64>()));
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        warn!(%error, dir = %dir.display(), "creating upload temp dir failed");
        return write_error(
            stream,
            500,
            "Internal Server Error",
            "failed to create temp directory",
        )
        .await;
    }
    let file_path = dir.join(&file_name);

    let result =
        handle_store_upload_body(stream, leftover, content_length, &file_path, &state).await;

    // Best-effort cleanup: the store already copied/hashed the bytes it
    // needs, the temp copy is not useful afterward either way.
    if let Err(error) = tokio::fs::remove_dir_all(&dir).await {
        debug!(%error, dir = %dir.display(), "removing upload temp dir failed");
    }
    result
}

async fn handle_store_upload_body(
    stream: &mut TcpStream,
    leftover: Vec<u8>,
    content_length: u64,
    file_path: &Path,
    state: &Arc<AppState>,
) -> io::Result<()> {
    let mut file = match File::create(file_path).await {
        Ok(f) => f,
        Err(error) => {
            warn!(%error, path = %file_path.display(), "creating upload file failed");
            return write_error(
                stream,
                500,
                "Internal Server Error",
                "failed to create upload file",
            )
            .await;
        }
    };
    if let Err(error) = read_body_to_file(stream, leftover, content_length, &mut file).await {
        debug!(%error, "reading upload body failed");
        return write_error(stream, 400, "Bad Request", "incomplete request body").await;
    }
    drop(file);

    match crate::daemon::dispatch(
        "store.put",
        json!({"path": file_path.to_string_lossy()}),
        state,
    )
    .await
    {
        Ok(data) => {
            write_json_response(stream, 200, "OK", &json!({"ok": true, "data": data})).await
        }
        Err(error) => {
            write_json_response(
                stream,
                200,
                "OK",
                &json!({"ok": false, "error": format!("{error:#}")}),
            )
            .await
        }
    }
}

/// `GET /api/store/download?cid=<percent-encoded cid>`.
///
/// Unlike the POST routes, this does **not** require `x-mistl-ui`. Browsers
/// never attach custom headers to a plain navigation (`<a download>`,
/// `location.href = ...`), so requiring one would make the download button
/// itself unusable; the CSRF guard on POST routes exists to stop a hostile
/// page from *driving* the daemon (side effects), but a GET download has no
/// side effect beyond reading a file that already exists in the store, and
/// this server never sends `Access-Control-Allow-Origin`, so a hostile
/// cross-origin page that triggers the navigation still cannot read the
/// response bytes back into its own JavaScript. Only the Host allowlist
/// (DNS-rebinding guard) applies here.
async fn handle_store_download(
    stream: &mut TcpStream,
    head: &RequestHead,
    state: Arc<AppState>,
) -> io::Result<()> {
    let local_addr = stream.local_addr()?;
    if is_guard_denied(head, &local_addr, false) {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }

    let query = head.path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let cid = match parse_query_param(query, "cid") {
        Some(cid) if !cid.is_empty() => cid,
        _ => return write_error(stream, 400, "Bad Request", "cid query parameter required").await,
    };

    let dir = std::env::temp_dir().join(format!("mistl-download-{:016x}", rand::random::<u64>()));
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        warn!(%error, dir = %dir.display(), "creating download temp dir failed");
        return write_error(
            stream,
            500,
            "Internal Server Error",
            "failed to create temp directory",
        )
        .await;
    }
    let output_path = dir.join("payload");

    let result = handle_store_download_inner(stream, &cid, &output_path, &state).await;
    if let Err(error) = tokio::fs::remove_dir_all(&dir).await {
        debug!(%error, dir = %dir.display(), "removing download temp dir failed");
    }
    result
}

async fn handle_store_download_inner(
    stream: &mut TcpStream,
    cid: &str,
    output_path: &Path,
    state: &Arc<AppState>,
) -> io::Result<()> {
    let data = match crate::daemon::dispatch(
        "store.get",
        json!({"id": cid, "output": output_path.to_string_lossy()}),
        state,
    )
    .await
    {
        Ok(data) => data,
        Err(error) => return write_error(stream, 404, "Not Found", &format!("{error:#}")).await,
    };
    let name = data
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("download");

    let mut file = match File::open(output_path).await {
        Ok(f) => f,
        Err(error) => {
            warn!(%error, path = %output_path.display(), "opening downloaded file failed");
            return write_error(
                stream,
                500,
                "Internal Server Error",
                "failed to read downloaded file",
            )
            .await;
        }
    };
    let content_length = match file.metadata().await {
        Ok(m) => m.len(),
        Err(error) => {
            warn!(%error, path = %output_path.display(), "stat of downloaded file failed");
            return write_error(
                stream,
                500,
                "Internal Server Error",
                "failed to stat downloaded file",
            )
            .await;
        }
    };

    let head = download_response_head(content_length, name);
    stream.write_all(head.as_bytes()).await?;
    let mut chunk = [0u8; STREAM_CHUNK_BYTES];
    loop {
        let n = file.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        stream.write_all(&chunk[..n]).await?;
    }
    stream.flush().await
}

/// `POST /api/store/sandbox-upload`: identical protocol to
/// `/api/store/upload` (body is the raw file bytes, name carried in the
/// `x-file-name` header, same size cap, same streamed-to-temp-file
/// approach), except the file lands in the path-jailed content sandbox via
/// `store.sandbox.import` instead of the content store via `store.put`.
async fn handle_store_sandbox_upload(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: Arc<AppState>,
) -> io::Result<()> {
    // Same guards as /api/call: CSRF (custom header) + DNS-rebinding (Host).
    let local_addr = stream.local_addr()?;
    if is_guard_denied(head, &local_addr, true) {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }

    let Some(raw_name) = head.header("x-file-name") else {
        return write_error(stream, 400, "Bad Request", "x-file-name header required").await;
    };
    let Some(file_name) = percent_decode(raw_name) else {
        return write_error(
            stream,
            400,
            "Bad Request",
            "x-file-name is not validly percent-encoded UTF-8",
        )
        .await;
    };
    if let Err(reason) = validate_upload_filename(&file_name) {
        return write_error(stream, 400, "Bad Request", reason).await;
    }

    let content_length: u64 = match head.header("content-length") {
        Some(v) => match v.trim().parse() {
            Ok(n) => n,
            Err(_) => {
                return write_error(stream, 400, "Bad Request", "invalid Content-Length").await;
            }
        },
        None => {
            return write_error(stream, 411, "Length Required", "Content-Length required").await;
        }
    };
    if content_length > MAX_UPLOAD_BYTES {
        return write_error(stream, 413, "Payload Too Large", "upload too large").await;
    }

    let dir = std::env::temp_dir().join(format!(
        "mistl-sandbox-upload-{:016x}",
        rand::random::<u64>()
    ));
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        warn!(%error, dir = %dir.display(), "creating upload temp dir failed");
        return write_error(
            stream,
            500,
            "Internal Server Error",
            "failed to create temp directory",
        )
        .await;
    }
    // `store.sandbox.import` names the imported sandbox entry after this
    // path's basename, so the temp file's basename must be the (sanitized)
    // client-supplied name, not a random temp name.
    let file_path = dir.join(&file_name);

    let result =
        handle_store_sandbox_upload_body(stream, leftover, content_length, &file_path, &state)
            .await;

    // Best-effort cleanup: the sandbox already copied the bytes it needs,
    // the temp copy is not useful afterward either way.
    if let Err(error) = tokio::fs::remove_dir_all(&dir).await {
        debug!(%error, dir = %dir.display(), "removing upload temp dir failed");
    }
    result
}

async fn handle_store_sandbox_upload_body(
    stream: &mut TcpStream,
    leftover: Vec<u8>,
    content_length: u64,
    file_path: &Path,
    state: &Arc<AppState>,
) -> io::Result<()> {
    let mut file = match File::create(file_path).await {
        Ok(f) => f,
        Err(error) => {
            warn!(%error, path = %file_path.display(), "creating upload file failed");
            return write_error(
                stream,
                500,
                "Internal Server Error",
                "failed to create upload file",
            )
            .await;
        }
    };
    if let Err(error) = read_body_to_file(stream, leftover, content_length, &mut file).await {
        debug!(%error, "reading upload body failed");
        return write_error(stream, 400, "Bad Request", "incomplete request body").await;
    }
    drop(file);

    match crate::daemon::dispatch(
        "store.sandbox.import",
        json!({"path": file_path.to_string_lossy()}),
        state,
    )
    .await
    {
        Ok(data) => {
            write_json_response(stream, 200, "OK", &json!({"ok": true, "data": data})).await
        }
        Err(error) => {
            write_json_response(
                stream,
                200,
                "OK",
                &json!({"ok": false, "error": format!("{error:#}")}),
            )
            .await
        }
    }
}

/// `GET /api/store/sandbox-download?path=<percent-encoded sandbox-relative path>`.
///
/// Mirrors `/api/store/download`: deliberately skips the `x-mistl-ui`
/// check. The same reasoning applies — a plain navigation (`<a download>`,
/// `location.href = ...`) can't attach a custom header so requiring one
/// would make the download button itself unusable; a GET here has no side
/// effect beyond reading a file that already exists in the sandbox; and
/// this server never sends `Access-Control-Allow-Origin`, so a hostile
/// cross-origin page that triggers the navigation still cannot read the
/// response bytes back into its own JavaScript. Sandbox files are
/// equivalent in sensitivity to store contents, so the same argument holds.
/// Only the Host allowlist (DNS-rebinding guard) applies here.
async fn handle_store_sandbox_download(
    stream: &mut TcpStream,
    head: &RequestHead,
    state: Arc<AppState>,
) -> io::Result<()> {
    let local_addr = stream.local_addr()?;
    if is_guard_denied(head, &local_addr, false) {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }

    let query = head.path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let path = match parse_query_param(query, "path") {
        Some(path) if !path.is_empty() => path,
        _ => return write_error(stream, 400, "Bad Request", "path query parameter required").await,
    };

    let dir = std::env::temp_dir().join(format!(
        "mistl-sandbox-download-{:016x}",
        rand::random::<u64>()
    ));
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        warn!(%error, dir = %dir.display(), "creating download temp dir failed");
        return write_error(
            stream,
            500,
            "Internal Server Error",
            "failed to create temp directory",
        )
        .await;
    }
    let output_path = dir.join("payload");

    let result = handle_store_sandbox_download_inner(stream, &path, &output_path, &state).await;
    if let Err(error) = tokio::fs::remove_dir_all(&dir).await {
        debug!(%error, dir = %dir.display(), "removing download temp dir failed");
    }
    result
}

async fn handle_store_sandbox_download_inner(
    stream: &mut TcpStream,
    path: &str,
    output_path: &Path,
    state: &Arc<AppState>,
) -> io::Result<()> {
    let data = match crate::daemon::dispatch(
        "store.sandbox.export",
        json!({"path": path, "output": output_path.to_string_lossy()}),
        state,
    )
    .await
    {
        Ok(data) => data,
        Err(error) => return write_error(stream, 404, "Not Found", &format!("{error:#}")).await,
    };
    let name = data
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("download");

    let mut file = match File::open(output_path).await {
        Ok(f) => f,
        Err(error) => {
            warn!(%error, path = %output_path.display(), "opening downloaded file failed");
            return write_error(
                stream,
                500,
                "Internal Server Error",
                "failed to read downloaded file",
            )
            .await;
        }
    };
    let content_length = match file.metadata().await {
        Ok(m) => m.len(),
        Err(error) => {
            warn!(%error, path = %output_path.display(), "stat of downloaded file failed");
            return write_error(
                stream,
                500,
                "Internal Server Error",
                "failed to stat downloaded file",
            )
            .await;
        }
    };

    let head = download_response_head(content_length, name);
    stream.write_all(head.as_bytes()).await?;
    let mut chunk = [0u8; STREAM_CHUNK_BYTES];
    loop {
        let n = file.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        stream.write_all(&chunk[..n]).await?;
    }
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    // -- find_header_end --------------------------------------------------

    #[test]
    fn find_header_end_crlf() {
        let buf = b"GET / HTTP/1.1\r\nHost: x\r\n\r\nbody";
        let pos = find_header_end(buf).expect("terminator found");
        assert_eq!(&buf[pos..], b"body");
    }

    #[test]
    fn find_header_end_bare_lf() {
        let buf = b"GET / HTTP/1.1\nHost: x\n\nbody";
        let pos = find_header_end(buf).expect("terminator found");
        assert_eq!(&buf[pos..], b"body");
    }

    #[test]
    fn find_header_end_missing_terminator_is_none() {
        let buf = b"GET / HTTP/1.1\r\nHost: x\r\n";
        assert_eq!(find_header_end(buf), None);
    }

    // -- parse_head ---------------------------------------------------------

    #[test]
    fn parse_head_basic() {
        let head =
            parse_head(b"POST /api/call HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\n")
                .unwrap();
        assert_eq!(head.method, "POST");
        assert_eq!(head.path, "/api/call");
        assert_eq!(head.header("host"), Some("localhost"));
        assert_eq!(head.header("content-length"), Some("3"));
    }

    #[test]
    fn parse_head_header_lookup_is_case_insensitive() {
        let head = parse_head(b"POST /api/call HTTP/1.1\r\nX-Mistl-UI: 1\r\n\r\n").unwrap();
        assert_eq!(head.header("x-mistl-ui"), Some("1"));
        assert_eq!(head.header("X-MISTL-UI"), Some("1"));
    }

    #[test]
    fn parse_head_rejects_empty_request_line() {
        assert!(parse_head(b"\r\n\r\n").is_none());
    }

    // -- host_is_allowed ------------------------------------------------------

    fn addr(s: &str) -> std::net::SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn host_allows_loopback_v4_with_and_without_port() {
        let local = addr("127.0.0.1:6480");
        assert!(host_is_allowed("127.0.0.1", &local));
        assert!(host_is_allowed("127.0.0.1:8080", &local));
    }

    #[test]
    fn host_allows_localhost_case_insensitive_with_port() {
        let local = addr("127.0.0.1:6480");
        assert!(host_is_allowed("localhost", &local));
        assert!(host_is_allowed("LOCALHOST:3000", &local));
    }

    #[test]
    fn host_allows_ipv6_loopback_with_and_without_port() {
        let local = addr("127.0.0.1:6480");
        assert!(host_is_allowed("[::1]", &local));
        assert!(host_is_allowed("[::1]:8080", &local));
    }

    #[test]
    fn host_rejects_non_loopback_names() {
        let local = addr("127.0.0.1:6480");
        assert!(!host_is_allowed("evil.com", &local));
        assert!(!host_is_allowed("127.0.0.1.evil.com", &local));
        assert!(!host_is_allowed("[::2]", &local));
        assert!(!host_is_allowed("", &local));
    }

    #[test]
    fn host_allows_literal_match_of_local_addr_for_lan_binds() {
        // `ui.listen`/`--host` bound to 0.0.0.0 (or a specific LAN IP): the
        // accepted connection's local_addr is the concrete interface IP a
        // client actually reached, and a Host header naming that same IP
        // (with or without the matching port) is accepted.
        let local = addr("192.168.1.50:6480");
        assert!(host_is_allowed("192.168.1.50:6480", &local));
        assert!(host_is_allowed("192.168.1.50", &local));
    }

    #[test]
    fn host_rejects_mismatched_ip_or_port_against_local_addr() {
        let local = addr("192.168.1.50:6480");
        assert!(!host_is_allowed("10.0.0.5:6480", &local));
        assert!(!host_is_allowed("192.168.1.50:9999", &local));
    }

    // -- is_guard_denied ------------------------------------------------------

    fn head_with(headers: &[(&str, &str)]) -> RequestHead {
        RequestHead {
            method: "POST".into(),
            path: "/api/call".into(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn guard_passes_a_well_formed_dashboard_call() {
        let head = head_with(&[("x-mistl-ui", "1"), ("host", "127.0.0.1:6480")]);
        assert!(!is_guard_denied(&head, &addr("127.0.0.1:6480"), true));
    }

    #[test]
    fn guard_denies_a_call_without_the_csrf_header() {
        let head = head_with(&[("host", "127.0.0.1:6480")]);
        assert!(is_guard_denied(&head, &addr("127.0.0.1:6480"), true));
    }

    #[test]
    fn guard_denies_a_call_from_a_disallowed_host() {
        let head = head_with(&[("x-mistl-ui", "1"), ("host", "evil.com")]);
        assert!(is_guard_denied(&head, &addr("127.0.0.1:6480"), true));
    }

    #[test]
    fn guard_without_the_header_requirement_still_enforces_host() {
        // The download endpoints skip the CSRF header on purpose (a plain
        // navigation can't attach one) but must keep the Host allowlist.
        let head = head_with(&[("host", "127.0.0.1:6480")]);
        assert!(!is_guard_denied(&head, &addr("127.0.0.1:6480"), false));
        let head = head_with(&[("host", "evil.com")]);
        assert!(is_guard_denied(&head, &addr("127.0.0.1:6480"), false));
    }

    // -- response framing -----------------------------------------------------

    #[test]
    fn response_head_has_expected_framing() {
        let head = response_head(200, "OK", "application/json", 5);
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(head.contains("Content-Type: application/json\r\n"));
        assert!(head.contains("Content-Length: 5\r\n"));
        assert!(head.contains("Connection: close\r\n"));
        assert!(head.ends_with("\r\n\r\n"));
    }

    // -- socket-level parsing (no AppState needed) -----------------------------

    #[tokio::test]
    async fn read_request_head_splits_leftover_body_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request_head(&mut socket).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"POST /api/call HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello")
            .await
            .unwrap();

        let (head, leftover) = server.await.unwrap().unwrap().unwrap();
        assert_eq!(head.method, "POST");
        assert_eq!(head.path, "/api/call");
        assert_eq!(leftover, b"hello");
    }

    #[tokio::test]
    async fn read_request_head_rejects_oversized_header_section() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request_head(&mut socket).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        // No blank-line terminator anywhere, so the cap must trip.
        let junk = vec![b'a'; MAX_HEADER_BYTES + 1];
        client.write_all(&junk).await.unwrap();

        let result = server.await.unwrap();
        assert!(result.is_err());
    }

    // -- percent_decode ------------------------------------------------------

    #[test]
    fn percent_decode_roundtrip_ascii() {
        assert_eq!(
            percent_decode("hello%20world").as_deref(),
            Some("hello world")
        );
    }

    #[test]
    fn percent_decode_roundtrip_multibyte_utf8() {
        // "caf\u{e9} \u{1F600}.txt" percent-encoded (UTF-8 bytes escaped).
        let encoded = "caf%C3%A9%20%F0%9F%98%80.txt";
        assert_eq!(
            percent_decode(encoded).as_deref(),
            Some("caf\u{e9} \u{1f600}.txt")
        );
    }

    #[test]
    fn percent_decode_passes_through_unescaped_bytes() {
        assert_eq!(
            percent_decode("plain-name.txt").as_deref(),
            Some("plain-name.txt")
        );
    }

    #[test]
    fn percent_decode_rejects_truncated_escape() {
        // A trailing `%` with fewer than two hex digits after it must fail,
        // not silently pass the `%` through.
        assert_eq!(percent_decode("bad%2"), None);
        assert_eq!(percent_decode("bad%"), None);
    }

    #[test]
    fn percent_decode_rejects_non_hex_escape() {
        assert_eq!(percent_decode("bad%zz"), None);
    }

    // -- validate_upload_filename ---------------------------------------------

    #[test]
    fn validate_upload_filename_accept_reject_table() {
        let accept = [
            "report.pdf",
            "photo (1).jpg",
            "\u{e9}t\u{e9}.txt",
            "no-extension",
        ];
        for name in accept {
            assert!(
                validate_upload_filename(name).is_ok(),
                "expected {name:?} to be accepted"
            );
        }

        let reject = [
            "",
            "..",
            "../escape.txt",
            "a/b.txt",
            "a\\b.txt",
            "..\\escape.txt",
            "weird<name.txt",
            "weird>name.txt",
            "weird:name.txt",
            "weird\"name.txt",
            "weird|name.txt",
            "weird?name.txt",
            "weird*name.txt",
            "with\ncontrol.txt",
            "with\0nul.txt",
        ];
        for name in reject {
            assert!(
                validate_upload_filename(name).is_err(),
                "expected {name:?} to be rejected"
            );
        }
    }

    // -- parse_query_param -----------------------------------------------------

    #[test]
    fn parse_query_param_finds_and_decodes_value() {
        assert_eq!(
            parse_query_param("cid=abc%20def&x=1", "cid").as_deref(),
            Some("abc def")
        );
    }

    #[test]
    fn parse_query_param_missing_key_is_none() {
        assert_eq!(parse_query_param("x=1&y=2", "cid"), None);
    }

    #[test]
    fn parse_query_param_empty_query_is_none() {
        assert_eq!(parse_query_param("", "cid"), None);
    }

    #[test]
    fn parse_query_param_malformed_value_is_none() {
        assert_eq!(parse_query_param("cid=bad%zz", "cid"), None);
    }

    // -- download_response_head -------------------------------------------------

    #[test]
    fn download_response_head_has_expected_framing_and_disposition() {
        let head = download_response_head(42, "r\u{e9}sum\u{e9}.pdf");
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(head.contains("Content-Type: application/octet-stream\r\n"));
        assert!(head.contains("Content-Length: 42\r\n"));
        assert!(head.contains("filename=\"download\""));
        assert!(head.contains("filename*=UTF-8''r%C3%A9sum%C3%A9.pdf"));
        assert!(head.ends_with("\r\n\r\n"));
    }
}
