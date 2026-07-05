//! Hand-rolled HTTP/1.1 server for the dashboard (same style as
//! `crate::ai::api_server`): tokio TCP accept loop, minimal request parse,
//! two routes.
//!
//! Routes:
//! - `GET /` (and `GET /index.html`) -> 200 `text/html`, [`super::INDEX_HTML`]
//! - `POST /api/call` -> body `{"cmd": "...", "args": {...}}`; runs
//!   [`crate::daemon::dispatch`] and answers 200 with
//!   `{"ok":true,"data":...}` or `{"ok":false,"error":"..."}`
//! - anything else -> 404 JSON error
//!
//! Security (no auth token; loopback bind):
//! - `POST /api/call` requires the request header `x-mistl-ui: 1`. Browsers
//!   cannot attach custom headers cross-origin without a CORS preflight, and
//!   this server never sends CORS headers (OPTIONS -> 403), so web pages from
//!   other origins can't drive the daemon (CSRF protection).
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
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

use crate::daemon::AppState;

/// Header section size cap (request-line + headers), matches doc.
const MAX_HEADER_BYTES: usize = 16 * 1024;
/// Body size cap for `POST /api/call`.
const MAX_BODY_BYTES: usize = 1024 * 1024;

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
            tokio::spawn(async move {
                handle_connection(socket, state).await;
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

    Some(RequestHead { method, path, headers })
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
            let head = parse_head(&buf[..pos])
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed request head"))?;
            let leftover = buf[pos..].to_vec();
            return Ok(Some((head, leftover)));
        }
        if buf.len() >= MAX_HEADER_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "header section too large"));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed mid-headers"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Read the remaining body bytes (on top of any `leftover` already read
/// during header parsing) until exactly `content_length` bytes are
/// available.
async fn read_body(stream: &mut TcpStream, mut leftover: Vec<u8>, content_length: usize) -> io::Result<Vec<u8>> {
    if leftover.len() >= content_length {
        leftover.truncate(content_length);
        return Ok(leftover);
    }
    let mut chunk = [0u8; 8192];
    while leftover.len() < content_length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed before body complete"));
        }
        let take = n.min(content_length - leftover.len());
        leftover.extend_from_slice(&chunk[..take]);
    }
    Ok(leftover)
}

/// Whether `host` (the raw `Host` header value) is one of the accepted
/// loopback forms, optionally with a `:port` suffix (DNS-rebinding guard).
fn host_is_allowed(host: &str) -> bool {
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
        return addr == "::1" && port_ok;
    }
    let name = match host.rsplit_once(':') {
        Some((name, port)) if is_valid_port(port) => name,
        _ => host,
    };
    name.eq_ignore_ascii_case("127.0.0.1") || name.eq_ignore_ascii_case("localhost")
}

fn is_valid_port(port: &str) -> bool {
    !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
}

/// Build the status-line + header block for a response (everything up to
/// and including the blank line before the body).
fn response_head(status: u16, reason: &str, content_type: &str, content_length: usize) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
    )
}

async fn write_json_response(stream: &mut TcpStream, status: u16, reason: &str, body: &Value) -> io::Result<()> {
    let body_bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let head = response_head(status, reason, "application/json", body_bytes.len());
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body_bytes).await?;
    stream.flush().await
}

async fn write_html_response(stream: &mut TcpStream, status: u16, reason: &str, body: &str) -> io::Result<()> {
    let head = response_head(status, reason, "text/html; charset=utf-8", body.len());
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

/// JSON error body shape used by every non-2xx response from this server.
async fn write_error(stream: &mut TcpStream, status: u16, reason: &str, message: &str) -> io::Result<()> {
    write_json_response(stream, status, reason, &json!({"ok": false, "error": message})).await
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

async fn handle_connection(mut stream: TcpStream, state: Arc<AppState>) {
    if let Err(error) = handle_connection_inner(&mut stream, state).await {
        debug!(%error, "web dashboard connection ended with error");
    }
}

async fn handle_connection_inner(stream: &mut TcpStream, state: Arc<AppState>) -> io::Result<()> {
    let (head, leftover) = match read_request_head(stream).await {
        Ok(Some(pair)) => pair,
        Ok(None) => return Ok(()),
        Err(_) => return write_error(stream, 400, "Bad Request", "malformed request").await,
    };

    let method = head.method.to_ascii_uppercase();
    let path = head.path.split('?').next().unwrap_or("").to_string();

    match (method.as_str(), path.as_str()) {
        ("GET", "/") | ("GET", "/index.html") => write_html_response(stream, 200, "OK", super::INDEX_HTML).await,
        ("POST", "/api/call") => handle_api_call(stream, &head, leftover, state).await,
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
    // CSRF guard: browsers can't attach a custom header cross-origin without
    // a preflight, and we never answer preflights successfully.
    if head.header("x-mistl-ui").is_none() {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }
    // DNS-rebinding guard: only accept requests that addressed us by a
    // loopback name, regardless of which interface the socket is on.
    if !host_is_allowed(head.header("host").unwrap_or("")) {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }

    let content_length: usize = match head.header("content-length") {
        Some(v) => match v.trim().parse() {
            Ok(n) => n,
            Err(_) => return write_error(stream, 400, "Bad Request", "invalid Content-Length").await,
        },
        None => return write_error(stream, 411, "Length Required", "Content-Length required").await,
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
            return write_error(stream, 400, "Bad Request", &format!("invalid request body: {error}")).await;
        }
    };
    let Some(cmd) = parsed.cmd.as_str() else {
        return write_error(stream, 400, "Bad Request", "cmd must be a string").await;
    };
    let args = parsed.args.unwrap_or_else(|| json!({}));

    // The frontend always gets HTTP 200 for a well-formed call and switches
    // on the `ok` field; only malformed/forbidden requests get non-2xx.
    match crate::daemon::dispatch(cmd, args, &state).await {
        Ok(data) => write_json_response(stream, 200, "OK", &json!({"ok": true, "data": data})).await,
        Err(error) => {
            write_json_response(stream, 200, "OK", &json!({"ok": false, "error": format!("{error:#}")})).await
        }
    }
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
        let head = parse_head(b"POST /api/call HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\n").unwrap();
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

    #[test]
    fn host_allows_loopback_v4_with_and_without_port() {
        assert!(host_is_allowed("127.0.0.1"));
        assert!(host_is_allowed("127.0.0.1:8080"));
    }

    #[test]
    fn host_allows_localhost_case_insensitive_with_port() {
        assert!(host_is_allowed("localhost"));
        assert!(host_is_allowed("LOCALHOST:3000"));
    }

    #[test]
    fn host_allows_ipv6_loopback_with_and_without_port() {
        assert!(host_is_allowed("[::1]"));
        assert!(host_is_allowed("[::1]:8080"));
    }

    #[test]
    fn host_rejects_non_loopback_names() {
        assert!(!host_is_allowed("evil.com"));
        assert!(!host_is_allowed("127.0.0.1.evil.com"));
        assert!(!host_is_allowed("[::2]"));
        assert!(!host_is_allowed(""));
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
}
