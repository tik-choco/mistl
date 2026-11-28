//! The mistl web gateway: a loopback-only HTTP/1.1 server that serves
//! installed sites at `http://<siteId>.localhost:<port>/`.
//!
//! Every site gets its own origin (the siteId is the host name). The exact
//! `Host` check is the DNS-rebinding defense: only `<siteId>.localhost:<port>`
//! and `localhost:<port>` are served. The gateway never reads or sets
//! cookies and exposes nothing of the daemon (no dashboard, no API).
//!
//! Flow for a site: unknown -> resolve the record from peers; untrusted ->
//! interstitial (the user confirms with a one-time nonce POST); trusted but
//! not installed -> start the install and show progress; ready -> stream the
//! file from local blocks. GET has no side effect except starting the
//! install of an already-trusted site.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use super::host::{self, SiteView, WebHost};
use super::site::{self, FileEntry, Manifest};
use crate::daemon::AppState;
use crate::storage::{LocalFile, Store};

const MAX_CONNECTIONS: usize = 64;
const MAX_HEAD_BYTES: usize = 16 * 1024;
const HEAD_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_TIMEOUT: Duration = Duration::from_secs(10);
/// Per write; a reader that stalls longer loses the connection (and slot).
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_FORM_BYTES: usize = 4096;
/// How long a page load waits for a peer to answer a `want`.
const RESOLVE_WAIT: Duration = Duration::from_secs(8);
const NONCE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_NONCES: usize = 256;
const CHUNK_BYTES: u64 = mistlib_core::storage::CHUNK_SIZE as u64;

const PERMISSIONS_POLICY: &str = "camera=(), microphone=(), geolocation=(), usb=(), serial=(), hid=(), bluetooth=(), payment=(), display-capture=()";

/// Running gateway; aborts its accept loop on [`GatewayHandle::close`].
pub struct GatewayHandle {
    addr: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl GatewayHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://localhost:<port>/` (the index page).
    pub fn base_url(&self) -> String {
        format!("http://localhost:{}/", self.addr.port())
    }

    /// Stop accepting connections.
    pub async fn close(self) {
        self.handle.abort();
    }
}

struct Gateway {
    state: Arc<AppState>,
    host: Arc<WebHost>,
    /// The bound port; the only port accepted in `Host`.
    port: u16,
    nonces: StdMutex<NonceStore>,
}

/// Bind `[web] listen` (loopback only) and serve until closed. Binding is
/// done up front so a busy port is reported to the caller; the host core is
/// started inside the accept task so daemon startup is not delayed.
pub async fn serve(state: Arc<AppState>) -> Result<GatewayHandle> {
    let listen = state.config().web.listen;
    let addr: SocketAddr = listen
        .parse()
        .with_context(|| format!("invalid [web] listen {listen:?}"))?;
    if !addr.ip().is_loopback() {
        bail!("[web] listen must be a loopback address, got {addr}");
    }
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the mistl web gateway to {addr}"))?;
    let addr = listener.local_addr()?;
    if !addr.ip().is_loopback() {
        bail!("mistl web gateway bound to non-loopback {addr}");
    }
    let handle = tokio::spawn(async move {
        let host = match host::host(&state).await {
            Ok(host) => host,
            Err(error) => {
                warn!(error = %format!("{error:#}"), "web gateway: host failed to start; not serving");
                return;
            }
        };
        let gw = Arc::new(Gateway {
            state,
            host,
            port: addr.port(),
            nonces: StdMutex::new(NonceStore::default()),
        });
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        loop {
            let socket = match listener.accept().await {
                Ok((socket, _peer)) => socket,
                Err(error) => {
                    warn!(%error, "web gateway accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                debug!("web gateway at connection cap; dropping a connection");
                continue;
            };
            let gw = gw.clone();
            tokio::spawn(async move {
                handle_connection(&gw, socket).await;
                drop(permit);
            });
        }
    });
    Ok(GatewayHandle { addr, handle })
}

// ---- Request parsing (copied from src/web/server.rs, stricter) ----

struct Request {
    method: String,
    target: String,
    /// Names lower-cased.
    headers: Vec<(String, String)>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    fn header_count(&self, name: &str) -> usize {
        self.headers.iter().filter(|(k, _)| k == name).count()
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
        return Some(pos + 4);
    }
    buf.windows(2).position(|w| w == b"\n\n").map(|pos| pos + 2)
}

fn parse_head(bytes: &[u8]) -> Option<Request> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.split('\n');
    let mut parts = lines.next()?.trim_end_matches('\r').split(' ');
    let method = parts.next().filter(|m| !m.is_empty())?.to_string();
    let target = parts.next().filter(|t| !t.is_empty())?.to_string();
    let version = parts.next()?;
    if parts.next().is_some() || !version.starts_with("HTTP/1.") {
        return None;
    }
    let mut headers = Vec::new();
    for line in lines {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        // No obs-fold, no header lines without a colon.
        if line.starts_with([' ', '\t']) {
            return None;
        }
        let (k, v) = line.split_once(':')?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
    }
    Some(Request {
        method,
        target,
        headers,
    })
}

async fn read_head<S: AsyncRead + Unpin>(s: &mut S) -> io::Result<Option<(Request, Vec<u8>)>> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(pos) = find_header_end(&buf) {
            let req = parse_head(&buf[..pos])
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed head"))?;
            return Ok(Some((req, buf[pos..].to_vec())));
        }
        if buf.len() >= MAX_HEAD_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "head too large"));
        }
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "closed mid-head",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

async fn read_body<S: AsyncRead + Unpin>(
    s: &mut S,
    mut body: Vec<u8>,
    len: usize,
) -> io::Result<Vec<u8>> {
    let mut chunk = [0u8; 1024];
    while body.len() < len {
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "closed mid-body",
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(len);
    Ok(body)
}

// ---- Pure helpers ----

#[derive(Debug, PartialEq, Eq)]
enum Route {
    Site(String),
    Index,
    /// `127.0.0.1:<port>` -> `http://localhost:<port>/`.
    Redirect,
    Misdirected,
}

/// Exact Host routing (DNS-rebinding defense). `host` is the single `Host`
/// header value (None if missing or repeated).
fn route_host(host: Option<&str>, port: u16) -> Route {
    let Some(host) = host else {
        return Route::Misdirected;
    };
    let host = host.trim().to_ascii_lowercase();
    let Some((name, p)) = host.rsplit_once(':') else {
        return Route::Misdirected;
    };
    if p != port.to_string() {
        return Route::Misdirected;
    }
    match name {
        "localhost" => Route::Index,
        "127.0.0.1" => Route::Redirect,
        _ => match name.strip_suffix(".localhost") {
            Some(id) if site::is_site_id(id) => Route::Site(id.to_string()),
            _ => Route::Misdirected,
        },
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ByteRange {
    Full,
    /// Inclusive byte range.
    Part(u64, u64),
    Unsatisfiable,
}

fn parse_u64(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// A single `bytes=a-b` / `a-` / `-n` range. Multi-range or malformed
/// headers fall back to the full body.
fn parse_range(header: Option<&str>, size: u64) -> ByteRange {
    let Some(h) = header.map(str::trim) else {
        return ByteRange::Full;
    };
    let Some(spec) = h
        .get(..6)
        .filter(|p| p.eq_ignore_ascii_case("bytes="))
        .map(|_| &h[6..])
    else {
        return ByteRange::Full;
    };
    if spec.contains(',') {
        return ByteRange::Full;
    }
    let Some((a, b)) = spec.split_once('-') else {
        return ByteRange::Full;
    };
    let (a, b) = (a.trim(), b.trim());
    if a.is_empty() {
        let Some(n) = parse_u64(b) else {
            return ByteRange::Full;
        };
        if n == 0 || size == 0 {
            return ByteRange::Unsatisfiable;
        }
        return ByteRange::Part(size.saturating_sub(n), size - 1);
    }
    let Some(start) = parse_u64(a) else {
        return ByteRange::Full;
    };
    let end = if b.is_empty() {
        None
    } else {
        match parse_u64(b) {
            Some(e) if e >= start => Some(e),
            _ => return ByteRange::Full,
        }
    };
    if start >= size {
        return ByteRange::Unsatisfiable;
    }
    ByteRange::Part(start, end.map_or(size - 1, |e| e.min(size - 1)))
}

/// Whether `Accept-Encoding` lists `encoding` (token match, case-insensitive,
/// `q=0` excludes; `*` counts).
fn accepts_encoding(header: Option<&str>, encoding: &str) -> bool {
    let Some(h) = header else {
        return false;
    };
    h.split(',').any(|item| {
        let mut parts = item.split(';');
        let name = parts.next().unwrap_or("").trim();
        if !(name.eq_ignore_ascii_case(encoding) || name == "*") {
            return false;
        }
        !parts.any(|p| {
            p.trim()
                .strip_prefix("q=")
                .and_then(|q| q.trim().parse::<f32>().ok())
                .is_some_and(|q| q <= 0.0)
        })
    })
}

fn if_none_match(header: Option<&str>, etag: &str) -> bool {
    let Some(h) = header else {
        return false;
    };
    h.trim() == "*"
        || h.split(',').any(|t| {
            let t = t.trim();
            t.strip_prefix("W/").unwrap_or(t) == etag
        })
}

/// Allow-POST origin check: `Origin` exactly the site origin, and
/// `Sec-Fetch-Site: same-origin` when the browser sends it.
fn origin_ok(origin: Option<&str>, expected: &str, sec_fetch_site: Option<&str>) -> bool {
    origin == Some(expected) && sec_fetch_site.is_none_or(|v| v == "same-origin")
}

fn hex_val(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

/// `application/x-www-form-urlencoded` -> pairs. None on bad encoding.
fn parse_form(body: &[u8]) -> Option<Vec<(String, String)>> {
    let decode = |s: &str| -> Option<String> {
        let src = s.as_bytes();
        let mut out = Vec::with_capacity(src.len());
        let mut i = 0;
        while i < src.len() {
            match src[i] {
                b'+' => out.push(b' '),
                b'%' => {
                    let hi = hex_val(*src.get(i + 1)?)?;
                    let lo = hex_val(*src.get(i + 2)?)?;
                    out.push((hi << 4) | lo);
                    i += 2;
                }
                b => out.push(b),
            }
            i += 1;
        }
        String::from_utf8(out).ok()
    };
    let text = std::str::from_utf8(body).ok()?;
    text.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            Some((decode(k)?, decode(v)?))
        })
        .collect()
}

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

fn random_hex() -> String {
    format!("{:032x}", rand::random::<u128>())
}

fn human_size(n: u64) -> String {
    if n < 1024 {
        return format!("{n} B");
    }
    let mut v = n as f64 / 1024.0;
    for unit in ["KiB", "MiB", "GiB"] {
        if v < 1024.0 || unit == "GiB" {
            return format!("{v:.1} {unit}");
        }
        v /= 1024.0;
    }
    unreachable!()
}

fn iso_utc(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

fn short_did(did: &str) -> String {
    if did.len() <= 28 || !did.is_ascii() {
        return did.to_string();
    }
    format!("{}…{}", &did[..20], &did[did.len() - 6..])
}

/// One-time confirmation nonces for the interstitial, bound to a siteId.
#[derive(Default)]
struct NonceStore {
    items: VecDeque<(String, String, Instant)>,
}

impl NonceStore {
    fn issue(&mut self, site: &str, now: Instant) -> String {
        let nonce = random_hex();
        self.insert(nonce.clone(), site, now);
        nonce
    }

    fn insert(&mut self, nonce: String, site: &str, now: Instant) {
        self.items
            .retain(|(_, _, t)| now.saturating_duration_since(*t) < NONCE_TTL);
        while self.items.len() >= MAX_NONCES {
            self.items.pop_front();
        }
        self.items.push_back((nonce, site.to_string(), now));
    }

    /// Single use: a presented nonce is removed whether or not it matches.
    fn consume(&mut self, site: &str, nonce: &str, now: Instant) -> bool {
        let Some(i) = self.items.iter().position(|(n, _, _)| n == nonce) else {
            return false;
        };
        let (_, bound, at) = self.items.remove(i).expect("index in range");
        bound == site && now.saturating_duration_since(at) < NONCE_TTL
    }
}

// ---- Pages ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lang {
    En,
    Ja,
}

impl Lang {
    /// `ja` if it appears before `en` (or without `en`), else English.
    fn from_accept(h: Option<&str>) -> Self {
        let h = h.unwrap_or("").to_ascii_lowercase();
        match (h.find("ja"), h.find("en")) {
            (Some(j), Some(e)) if j < e => Lang::Ja,
            (Some(_), None) => Lang::Ja,
            _ => Lang::En,
        }
    }

    fn t<'a>(self, en: &'a str, ja: &'a str) -> &'a str {
        match self {
            Lang::En => en,
            Lang::Ja => ja,
        }
    }

    fn code(self) -> &'static str {
        self.t("en", "ja")
    }
}

struct Ctx {
    lang: Lang,
    head_only: bool,
}

struct Page {
    status: u16,
    title: String,
    /// Already-escaped HTML.
    body: String,
    script: Option<&'static str>,
    refresh: Option<u32>,
    extra_headers: String,
}

impl Page {
    fn new(status: u16, title: &str, body: String) -> Self {
        Self {
            status,
            title: title.to_string(),
            body,
            script: None,
            refresh: None,
            extra_headers: String::new(),
        }
    }

    fn message(status: u16, title: &str, text: &str) -> Self {
        Self::new(
            status,
            title,
            format!("<h1>{}</h1><p>{}</p>", esc(title), esc(text)),
        )
    }
}

const CSS: &str = ":root{color-scheme:light dark;--fg:#1d1d1f;--bg:#f5f5f7;--card:#fff;--mut:#6b6b76;--acc:#3b5bdb;--bd:#e2e2e8}@media(prefers-color-scheme:dark){:root{--fg:#ececf1;--bg:#16161a;--card:#202026;--mut:#9a9aa6;--acc:#8196ff;--bd:#34343d}}*{box-sizing:border-box}body{margin:0;padding:0 16px;font:15px/1.55 system-ui,-apple-system,'Segoe UI',sans-serif;background:var(--bg);color:var(--fg)}main{max-width:640px;margin:8vh auto 0;padding:28px;background:var(--card);border:1px solid var(--bd);border-radius:14px}h1{font-size:1.3em;margin:0 0 .6em}p{margin:.5em 0}.mut{color:var(--mut)}code,.mono{font-family:ui-monospace,Consolas,monospace;font-size:.9em;word-break:break-all}dl{display:grid;grid-template-columns:auto 1fr;gap:.3em 1em;margin:1em 0}dt{color:var(--mut)}dd{margin:0;word-break:break-all}a{color:var(--acc)}.btn{display:inline-block;margin-top:.6em;padding:.55em 1.2em;border-radius:8px;border:0;background:var(--acc);color:#fff;font:inherit;cursor:pointer;text-decoration:none}progress{width:100%;height:10px}ul{padding:0;margin:0;list-style:none}li{padding:.6em 0;border-top:1px solid var(--bd)}.err{color:#d33;word-break:break-word}footer{text-align:center;color:var(--mut);font-size:.8em;margin:1.5em}";

/// Polls `/.mistl/status`; reloads when the page should change.
const PROGRESS_SCRIPT: &str = "(function(){var $=function(i){return document.getElementById(i)};function fmt(n){var u=['B','KiB','MiB','GiB'],i=0;while(n>=1024&&i<3){n/=1024;i++}return (i?n.toFixed(1):n)+' '+u[i]}function tick(){fetch('/.mistl/status',{cache:'no-store'}).then(function(r){return r.json()}).then(function(j){if(j.state==='failed'){$('err').textContent=j.error||'';$('run').hidden=true;$('fail').hidden=false;return}if(j.state!=='installing'){location.reload();return}$('bar').value=j.total_bytes?Math.floor(j.done_bytes*100/j.total_bytes):0;$('st').textContent=fmt(j.done_bytes)+' / '+fmt(j.total_bytes);setTimeout(tick,1000)}).catch(function(){setTimeout(tick,2000)})}setTimeout(tick,700)})();";

fn page_csp(nonce: Option<&str>) -> String {
    let script = nonce.map_or(String::new(), |n| format!("; script-src 'nonce-{n}'"));
    format!(
        "default-src 'none'; style-src 'unsafe-inline'{script}; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
    )
}

fn render_page(lang: Lang, page: &Page, nonce: Option<&str>) -> String {
    let refresh = page.refresh.map_or(String::new(), |s| {
        format!("<meta http-equiv=\"refresh\" content=\"{s}\">")
    });
    let script = match (page.script, nonce) {
        (Some(js), Some(n)) => format!("<script nonce=\"{n}\">{js}</script>"),
        _ => String::new(),
    };
    format!(
        "<!doctype html><html lang=\"{}\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">{refresh}<title>{} · mistl</title><style>{CSS}</style></head><body><main>{}</main><footer>mistl web</footer>{script}</body></html>",
        lang.code(),
        esc(&page.title),
        page.body
    )
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        206 => "Partial Content",
        303 => "See Other",
        304 => "Not Modified",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        411 => "Length Required",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        421 => "Misdirected Request",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    }
}

async fn write_timed<S: AsyncWrite + Unpin>(s: &mut S, bytes: &[u8]) -> io::Result<()> {
    match tokio::time::timeout(WRITE_TIMEOUT, s.write_all(bytes)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out")),
    }
}

async fn send_page<S: AsyncWrite + Unpin>(s: &mut S, ctx: &Ctx, page: Page) -> io::Result<()> {
    let nonce = page.script.map(|_| random_hex());
    let html = render_page(ctx.lang, &page, nonce.as_deref());
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nContent-Security-Policy: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n{}Connection: close\r\n\r\n",
        page.status,
        reason(page.status),
        html.len(),
        page_csp(nonce.as_deref()),
        page.extra_headers,
    );
    write_timed(s, head.as_bytes()).await?;
    if !ctx.head_only {
        write_timed(s, html.as_bytes()).await?;
    }
    s.flush().await
}

async fn send_redirect<S: AsyncWrite + Unpin>(
    s: &mut S,
    status: u16,
    location: &str,
) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {}\r\nLocation: {location}\r\nContent-Length: 0\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n\r\n",
        reason(status)
    );
    write_timed(s, head.as_bytes()).await?;
    s.flush().await
}

fn method_not_allowed(ctx: &Ctx, allow: &str) -> Page {
    let mut page = Page::message(
        405,
        ctx.lang.t("Method not allowed", "このメソッドは使えません"),
        &format!("Allow: {allow}"),
    );
    page.extra_headers = format!("Allow: {allow}\r\n");
    page
}

// ---- Handlers ----

async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(gw: &Arc<Gateway>, mut s: S) {
    let (req, leftover) = match tokio::time::timeout(HEAD_TIMEOUT, read_head(&mut s)).await {
        Err(_) | Ok(Ok(None)) => return,
        Ok(Err(error)) => {
            debug!(%error, "web gateway: bad request head");
            let ctx = Ctx {
                lang: Lang::En,
                head_only: false,
            };
            let page = Page::message(400, "Bad request", "The request could not be parsed.");
            let _ = send_page(&mut s, &ctx, page).await;
            let _ = s.shutdown().await;
            return;
        }
        Ok(Ok(Some(pair))) => pair,
    };
    if let Err(error) = handle_request(gw, &mut s, &req, leftover).await {
        debug!(%error, "web gateway: connection ended with an error");
    }
    let _ = s.shutdown().await;
}

async fn handle_request<S: AsyncRead + AsyncWrite + Unpin>(
    gw: &Arc<Gateway>,
    s: &mut S,
    req: &Request,
    leftover: Vec<u8>,
) -> io::Result<()> {
    let ctx = Ctx {
        lang: Lang::from_accept(req.header("accept-language")),
        head_only: req.method == "HEAD",
    };
    let host = (req.header_count("host") == 1)
        .then(|| req.header("host"))
        .flatten();
    let route = route_host(host, gw.port);
    if route == Route::Misdirected {
        let page = Page::message(
            421,
            ctx.lang.t("Wrong address", "アドレスが違います"),
            ctx.lang.t(
                "This server only answers http://localhost and http://<site>.localhost addresses on its own port.",
                "このサーバーは自身のポートの http://localhost と http://<site>.localhost のアドレスにのみ応答します。",
            ),
        );
        return send_page(s, &ctx, page).await;
    }
    if route == Route::Redirect {
        return send_redirect(s, 308, &format!("http://localhost:{}/", gw.port)).await;
    }
    if !req.target.starts_with('/') {
        let page = Page::message(400, "Bad request", "Unsupported request target.");
        return send_page(s, &ctx, page).await;
    }
    match route {
        Route::Site(site) => handle_site(gw, s, &ctx, req, leftover, &site).await,
        _ => {
            if req.method != "GET" && req.method != "HEAD" {
                return send_page(s, &ctx, method_not_allowed(&ctx, "GET, HEAD")).await;
            }
            send_page(s, &ctx, index_page(gw, &ctx).await).await
        }
    }
}

/// Top-level navigation (or a client that does not send fetch metadata).
fn is_document(req: &Request) -> bool {
    req.header("sec-fetch-dest")
        .is_none_or(|d| d.eq_ignore_ascii_case("document"))
}

async fn handle_site<S: AsyncRead + AsyncWrite + Unpin>(
    gw: &Arc<Gateway>,
    s: &mut S,
    ctx: &Ctx,
    req: &Request,
    leftover: Vec<u8>,
    site: &str,
) -> io::Result<()> {
    let lang = ctx.lang;
    let path = req.target.split(['?', '#']).next().unwrap_or("/");
    if path == "/.mistl/allow" {
        if req.method != "POST" {
            return send_page(s, ctx, method_not_allowed(ctx, "POST")).await;
        }
        return handle_allow(gw, s, ctx, req, leftover, site).await;
    }
    if req.method != "GET" && req.method != "HEAD" {
        return send_page(s, ctx, method_not_allowed(ctx, "GET, HEAD")).await;
    }
    if path == "/.mistl/status" {
        let mut view = gw.host.view(site).await;
        if view
            .as_ref()
            .is_some_and(|v| v.trusted && v.ready.is_none() && v.progress.is_none())
        {
            gw.host.start_install(site).await;
            view = gw.host.view(site).await;
        }
        return send_status(s, ctx, &status_json(view.as_ref())).await;
    }
    let document = is_document(req);
    let host = &gw.host;

    let mut view = host.view(site).await;
    if view.is_none() && document && host.resolve(site, RESOLVE_WAIT).await.is_some() {
        view = host.view(site).await;
    }
    let Some(view) = view else {
        if !document {
            let page = Page::message(404, lang.t("Not found", "見つかりません"), site);
            return send_page(s, ctx, page).await;
        }
        return send_page(s, ctx, resolving_page(gw, lang, site)).await;
    };

    if !view.trusted {
        if !document {
            let page = Page::message(
                403,
                lang.t("Not trusted yet", "まだ信頼されていません"),
                lang.t(
                    "Open the site's page first to confirm it.",
                    "先にサイトのページを開いて確認してください。",
                ),
            );
            return send_page(s, ctx, page).await;
        }
        let nonce = gw
            .nonces
            .lock()
            .expect("nonce lock poisoned")
            .issue(site, Instant::now());
        return send_page(s, ctx, interstitial_page(gw, lang, &view, &nonce)).await;
    }

    let Some(manifest) = view.ready.clone() else {
        // Trusted: the one GET side effect the contract allows.
        host.start_install(site).await;
        if !document {
            let mut page = Page::message(
                503,
                lang.t("Installing", "インストール中"),
                lang.t(
                    "The site is still downloading.",
                    "サイトをダウンロード中です。",
                ),
            );
            page.extra_headers = "Retry-After: 5\r\n".to_string();
            return send_page(s, ctx, page).await;
        }
        return send_page(s, ctx, progress_page(gw, lang, &view)).await;
    };

    let file_path = match site::request_path_to_file(path) {
        Ok(p) => p,
        Err(error) => {
            let page = Page::message(400, lang.t("Bad path", "不正なパス"), &error.to_string());
            return send_page(s, ctx, page).await;
        }
    };
    let Some(entry) = manifest.resolve(&file_path) else {
        let page = Page::message(
            404,
            lang.t("Not found", "見つかりません"),
            &format!("/{file_path}"),
        );
        return send_page(s, ctx, page).await;
    };
    if document {
        host.mark_opened(site).await;
        host.check_update(site);
    }
    serve_file(gw, s, ctx, req, &manifest, entry).await
}

async fn handle_allow<S: AsyncRead + AsyncWrite + Unpin>(
    gw: &Arc<Gateway>,
    s: &mut S,
    ctx: &Ctx,
    req: &Request,
    leftover: Vec<u8>,
    site: &str,
) -> io::Result<()> {
    let lang = ctx.lang;
    let denied = |text: &str| {
        let mut page = Page::message(403, lang.t("Not allowed", "許可されていません"), text);
        page.body.push_str(&format!(
            "<p><a class=\"btn\" href=\"/\">{}</a></p>",
            esc(lang.t("Back to the site", "サイトに戻る"))
        ));
        page
    };
    if req.header("transfer-encoding").is_some() {
        let page = Page::message(400, "Bad request", "Chunked bodies are not supported.");
        return send_page(s, ctx, page).await;
    }
    let Some(len) = req
        .header("content-length")
        .and_then(|v| parse_u64(v.trim()))
        .filter(|_| req.header_count("content-length") == 1)
    else {
        let page = Page::message(411, "Length required", "Content-Length is required.");
        return send_page(s, ctx, page).await;
    };
    if len > MAX_FORM_BYTES as u64 {
        let page = Page::message(413, "Too large", "The form body is too large.");
        return send_page(s, ctx, page).await;
    }
    let expected = format!("http://{site}.localhost:{}", gw.port);
    if !origin_ok(
        req.header("origin"),
        &expected,
        req.header("sec-fetch-site"),
    ) {
        let text = lang.t(
            "This confirmation did not come from the site's own page.",
            "この確認はサイト自身のページから送信されていません。",
        );
        return send_page(s, ctx, denied(text)).await;
    }
    let form_type = req.header("content-type").is_some_and(|v| {
        v.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("application/x-www-form-urlencoded")
    });
    if !form_type {
        let page = Page::message(415, "Unsupported media type", "Expected a form post.");
        return send_page(s, ctx, page).await;
    }
    let body = match tokio::time::timeout(BODY_TIMEOUT, read_body(s, leftover, len as usize)).await
    {
        Ok(Ok(body)) => body,
        Ok(Err(error)) => return Err(error),
        Err(_) => return Ok(()),
    };
    let form = parse_form(&body).unwrap_or_default();
    let field = |k: &str| form.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    let nonce = field("nonce").unwrap_or("");
    let publisher = field("publisher") == Some("1");
    let valid = !nonce.is_empty()
        && gw
            .nonces
            .lock()
            .expect("nonce lock poisoned")
            .consume(site, nonce, Instant::now());
    if !valid {
        let text = lang.t(
            "This confirmation expired or was already used. Go back and confirm again.",
            "この確認は期限切れか使用済みです。戻ってもう一度確認してください。",
        );
        return send_page(s, ctx, denied(text)).await;
    }
    if let Err(error) = gw.host.trust(site, publisher).await {
        warn!(%site, error = %format!("{error:#}"), "web gateway: trusting a site failed");
        let page = Page::message(500, lang.t("Error", "エラー"), &format!("{error:#}"));
        return send_page(s, ctx, page).await;
    }
    gw.host.start_install(site).await;
    send_redirect(s, 303, "/").await
}

fn status_json(view: Option<&SiteView>) -> Value {
    let (state, done, total, error) = match view {
        None => ("unknown", 0, 0, None),
        Some(v) if !v.trusted => ("untrusted", 0, v.record.size, None),
        Some(v) if v.ready.is_some() => ("ready", v.record.size, v.record.size, None),
        Some(v) => match &v.progress {
            Some(p) => (p.state, p.done_bytes, p.total_bytes, p.error.clone()),
            None => ("installing", 0, v.record.size, None),
        },
    };
    json!({"state": state, "done_bytes": done, "total_bytes": total, "error": error})
}

async fn send_status<S: AsyncWrite + Unpin>(s: &mut S, ctx: &Ctx, body: &Value) -> io::Result<()> {
    let body = body.to_string();
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nCross-Origin-Resource-Policy: same-origin\r\nConnection: close\r\n\r\n",
        body.len()
    );
    write_timed(s, head.as_bytes()).await?;
    if !ctx.head_only {
        write_timed(s, body.as_bytes()).await?;
    }
    s.flush().await
}

fn network_hint(gw: &Gateway, lang: Lang) -> String {
    if gw.state.network.permitted() {
        return String::new();
    }
    format!(
        "<p class=\"err\">{} <code>mistl network on</code></p>",
        esc(lang.t(
            "The mistl network is off. Turn it on with",
            "mistl ネットワークがオフです。次のコマンドでオンにしてください:",
        ))
    )
}

fn resolving_page(gw: &Gateway, lang: Lang, site: &str) -> Page {
    let title = lang.t("Looking for this site…", "サイトを探しています…");
    let body = format!(
        "<h1>{}</h1><p>{}</p>{}<p class=\"mut\">{}</p><p class=\"mono mut\">{}</p>",
        esc(title),
        esc(lang.t(
            "No peer has answered yet. This page retries every 5 seconds.",
            "まだどのピアからも応答がありません。このページは5秒ごとに再試行します。",
        )),
        network_hint(gw, lang),
        esc(lang.t(
            "The site may not exist, or nobody who has it is online right now.",
            "サイトが存在しないか、サイトを持っている人が今オンラインでない可能性があります。",
        )),
        esc(site),
    );
    let mut page = Page::new(404, title, body);
    page.refresh = Some(5);
    page
}

fn interstitial_page(gw: &Gateway, lang: Lang, view: &SiteView, nonce: &str) -> Page {
    let r = &view.record;
    let title = lang.t("Open this site?", "このサイトを開きますか？");
    let row = |k: &str, v: &str, cls: &str| {
        format!("<dt>{}</dt><dd class=\"{cls}\">{}</dd>", esc(k), esc(v))
    };
    let size = format!(
        "{} ({} {})",
        human_size(r.size),
        r.files,
        lang.t("files", "ファイル")
    );
    let body = format!(
        "<h1>{}</h1><p class=\"mut\">{}</p><dl>{}{}{}{}{}</dl><form method=\"post\" action=\"/.mistl/allow\"><input type=\"hidden\" name=\"nonce\" value=\"{}\"><p><label><input type=\"checkbox\" name=\"publisher\" value=\"1\"> {}</label></p><button class=\"btn\" type=\"submit\">{}</button></form>",
        esc(title),
        esc(lang.t(
            "This site was published on the mistl network by someone else. It runs in your browser in its own origin. Open it only if you trust the publisher. Its files will be downloaded and shared with other peers.",
            "このサイトは他の人が mistl ネットワークで公開したものです。ブラウザ内で独立したオリジンとして動作します。公開者を信頼できる場合のみ開いてください。ファイルはダウンロードされ、他のピアにも共有されます。",
        )),
        row(lang.t("Name", "名前"), &r.name, ""),
        row(lang.t("Publisher", "公開者"), &r.did, "mono"),
        row(lang.t("Size", "サイズ"), &size, ""),
        row(lang.t("Published", "公開日時"), &iso_utc(r.at), ""),
        row("URL", &gw.host.site_url(&r.site), "mono"),
        esc(nonce),
        esc(lang.t("Always trust this publisher", "この公開者を常に信頼する")),
        esc(lang.t("Trust and open", "信頼して開く")),
    );
    Page::new(200, title, body)
}

fn progress_page(gw: &Gateway, lang: Lang, view: &SiteView) -> Page {
    let r = &view.record;
    let (done, total) = match &view.progress {
        Some(p) if p.state == "installing" => (p.done_bytes, p.total_bytes),
        _ => (0, r.size),
    };
    let pct = (done.saturating_mul(100)).checked_div(total).unwrap_or(0);
    let title = lang.t("Installing", "インストール中");
    let body = format!(
        "<h1>{}: {}</h1><div id=\"run\"><p class=\"mut\">{}</p><progress id=\"bar\" max=\"100\" value=\"{pct}\"></progress><p id=\"st\" class=\"mut\">{} / {}</p></div><div id=\"fail\" hidden><p>{}</p><p class=\"err\" id=\"err\"></p>{}<a class=\"btn\" href=\"\">{}</a></div><noscript><p>{}</p></noscript>",
        esc(title),
        esc(&r.name),
        esc(lang.t(
            "Downloading the site from peers. It opens automatically when done.",
            "ピアからサイトをダウンロードしています。完了すると自動で開きます。",
        )),
        human_size(done),
        human_size(total),
        esc(lang.t("The download failed:", "ダウンロードに失敗しました:")),
        network_hint(gw, lang),
        esc(lang.t("Retry", "再試行")),
        esc(lang.t(
            "Reload this page when the download finishes.",
            "ダウンロードが終わったらこのページを再読み込みしてください。",
        )),
    );
    let mut page = Page::new(200, title, body);
    page.script = Some(PROGRESS_SCRIPT);
    page
}

async fn index_page(gw: &Gateway, ctx: &Ctx) -> Page {
    let lang = ctx.lang;
    let sites: Vec<SiteView> = gw
        .host
        .list()
        .await
        .into_iter()
        .filter(|v| v.ready_cid.is_some())
        .collect();
    let mut body = String::from("<h1>mistl web</h1>");
    if sites.is_empty() {
        body.push_str(&format!(
            "<p>{}</p><p class=\"mut\">{}</p><p><code>mistl upload ./dist</code></p>",
            esc(lang.t(
                "No sites are installed yet.",
                "インストール済みのサイトはまだありません。"
            )),
            esc(lang.t(
                "To publish a folder with a static web app or game, run:",
                "静的な Web アプリやゲームのフォルダを公開するには:",
            )),
        ));
    } else {
        body.push_str("<ul>");
        for v in &sites {
            let r = &v.record;
            body.push_str(&format!(
                "<li><a href=\"{}\"><b>{}</b></a><br><span class=\"mut\"><span class=\"mono\">{}</span> · {}</span></li>",
                esc(&gw.host.site_url(&r.site)),
                esc(&r.name),
                esc(&short_did(&r.did)),
                human_size(r.size),
            ));
        }
        body.push_str("</ul>");
    }
    Page::new(200, "mistl web", body)
}

// ---- Files ----

async fn load_chunk(store: &Store, file: &LocalFile, idx: usize) -> Result<Vec<u8>> {
    let cid = file.chunks.get(idx).context("chunk index out of range")?;
    let data = store
        .get_local(cid)
        .await?
        .with_context(|| format!("chunk {cid} is not stored locally"))?;
    let expected = CHUNK_BYTES.min(file.size - idx as u64 * CHUNK_BYTES);
    if data.len() as u64 != expected {
        bail!("chunk {cid} has the wrong size");
    }
    Ok(data)
}

async fn serve_file<S: AsyncWrite + Unpin>(
    gw: &Gateway,
    s: &mut S,
    ctx: &Ctx,
    req: &Request,
    manifest: &Manifest,
    entry: &FileEntry,
) -> io::Result<()> {
    let lang = ctx.lang;
    let served = site::served_type(&entry.p, manifest.precompressed);
    if let Some(enc) = served.encoding
        && !accepts_encoding(req.header("accept-encoding"), enc)
    {
        let page = Page::message(
            406,
            lang.t("Compressed file", "圧縮ファイル"),
            &format!(
                "{} ({enc})",
                lang.t(
                    "This file is stored compressed and your browser did not accept that encoding. mistl does not decompress files.",
                    "このファイルは圧縮されたまま保存されていますが、ブラウザがその圧縮形式を受け付けませんでした。mistl は展開を行いません。",
                )
            ),
        );
        return send_page(s, ctx, page).await;
    }

    // `entry.cid` passed `sane_cid` (alphanumeric): header-safe.
    let etag = format!("\"{}\"", entry.cid);
    let mut common = format!(
        "X-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nCross-Origin-Resource-Policy: same-origin\r\nContent-Security-Policy: frame-ancestors 'self'\r\nPermissions-Policy: {PERMISSIONS_POLICY}\r\nCache-Control: no-cache\r\nETag: {etag}\r\nAccept-Ranges: bytes\r\n"
    );
    if manifest.coi {
        common.push_str(
            "Cross-Origin-Opener-Policy: same-origin\r\nCross-Origin-Embedder-Policy: require-corp\r\n",
        );
    }
    if let Some(enc) = served.encoding {
        common.push_str(&format!(
            "Content-Encoding: {enc}\r\nVary: Accept-Encoding\r\n"
        ));
    }
    if if_none_match(req.header("if-none-match"), &etag) {
        let head = format!("HTTP/1.1 304 Not Modified\r\n{common}Connection: close\r\n\r\n");
        write_timed(s, head.as_bytes()).await?;
        return s.flush().await;
    }

    let store = gw.host.store();
    let file = match store.local_file(&entry.cid).await {
        Ok(Some(f)) if f.size == entry.size => f,
        other => {
            let why = match other {
                Err(error) => format!("{error:#}"),
                Ok(None) => "not stored locally".to_string(),
                Ok(Some(_)) => "size mismatch".to_string(),
            };
            warn!(file = %entry.p, cid = %entry.cid, %why, "web gateway: installed file unusable");
            let page = Page::message(
                500,
                lang.t("File unavailable", "ファイルを読み込めません"),
                lang.t(
                    "A file of this site is missing locally. Reinstall it with `mistl web install`.",
                    "このサイトのファイルがローカルにありません。`mistl web install` で再インストールしてください。",
                ),
            );
            return send_page(s, ctx, page).await;
        }
    };
    let size = file.size;
    let range = if req.header("if-range").is_some_and(|v| v.trim() != etag) {
        ByteRange::Full
    } else {
        parse_range(req.header("range"), size)
    };
    let (status, start, end) = match range {
        ByteRange::Full => (200, 0, size),
        ByteRange::Part(a, b) => (206, a, b + 1),
        ByteRange::Unsatisfiable => {
            let head = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{size}\r\nContent-Length: 0\r\n{common}Connection: close\r\n\r\n"
            );
            write_timed(s, head.as_bytes()).await?;
            return s.flush().await;
        }
    };

    // Load the first chunk before promising a length, so a missing file
    // gets an error page instead of a truncated body.
    let first_idx = (start / CHUNK_BYTES) as usize;
    let mut first = None;
    if end > start && !ctx.head_only {
        match load_chunk(&store, &file, first_idx).await {
            Ok(data) => first = Some(data),
            Err(error) => {
                warn!(file = %entry.p, error = %format!("{error:#}"), "web gateway: reading a file failed");
                let page = Page::message(
                    500,
                    lang.t("File unavailable", "ファイルを読み込めません"),
                    lang.t(
                        "A file of this site is missing locally. Reinstall it with `mistl web install`.",
                        "このサイトのファイルがローカルにありません。`mistl web install` で再インストールしてください。",
                    ),
                );
                return send_page(s, ctx, page).await;
            }
        }
    }

    let content_range = if status == 206 {
        format!("Content-Range: bytes {start}-{}/{size}\r\n", end - 1)
    } else {
        String::new()
    };
    let head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n{content_range}{common}Connection: close\r\n\r\n",
        reason(status),
        served.content_type,
        end - start,
    );
    write_timed(s, head.as_bytes()).await?;
    if ctx.head_only || end == start {
        return s.flush().await;
    }

    let last_idx = ((end - 1) / CHUNK_BYTES) as usize;
    for idx in first_idx..=last_idx {
        let data = match first.take() {
            Some(data) => data,
            None => match load_chunk(&store, &file, idx).await {
                Ok(data) => data,
                Err(error) => {
                    // The length is already promised: abort the connection.
                    warn!(file = %entry.p, error = %format!("{error:#}"), "web gateway: file chunk missing mid-response; aborting");
                    return Err(io::Error::other("chunk missing"));
                }
            },
        };
        let base = idx as u64 * CHUNK_BYTES;
        let lo = start.saturating_sub(base) as usize;
        let hi = (end - base).min(data.len() as u64) as usize;
        write_timed(s, &data[lo..hi]).await?;
    }
    s.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    const PORT: u16 = 6490;

    fn id() -> String {
        site::derive_site_id(&crate::identity::did_for_seed(1), "g")
    }

    #[test]
    fn host_routing_table() {
        let id = id();
        let r = |h: Option<&str>| route_host(h, PORT);
        let site_host = format!("{id}.localhost:6490");
        let upper = format!("{}.LOCALHOST:6490", id.to_uppercase());
        assert_eq!(r(Some(site_host.as_str())), Route::Site(id.clone()));
        assert_eq!(r(Some(upper.as_str())), Route::Site(id.clone()));
        assert_eq!(r(Some("localhost:6490")), Route::Index);
        assert_eq!(r(Some(" Localhost:6490 ")), Route::Index);
        assert_eq!(r(Some("127.0.0.1:6490")), Route::Redirect);
        assert_eq!(r(None), Route::Misdirected);
        let owned = [
            format!("{id}.localhost"),
            format!("{id}.localhost:6491"),
            format!("x.{id}.localhost:6490"),
            format!("{id}.localhost.evil.com:6490"),
        ];
        let fixed = [
            "",
            "localhost",
            "localhost:6491",
            "localhost:06490",
            "127.0.0.1",
            "127.0.0.1:80",
            "[::1]:6490",
            "evil.com:6490",
            "evil.com",
            "x.localhost.evil.com:6490",
            "abc.localhost:6490",
            ".localhost:6490",
        ];
        for bad in owned.iter().map(String::as_str).chain(fixed) {
            assert_eq!(r(Some(bad)), Route::Misdirected, "{bad:?}");
        }
    }

    #[test]
    fn range_table() {
        use ByteRange::*;
        let r = |h: &str, size: u64| parse_range(Some(h), size);
        assert_eq!(parse_range(None, 10), Full);
        assert_eq!(r("bytes=0-4", 10), Part(0, 4));
        assert_eq!(r("BYTES=2-", 10), Part(2, 9));
        assert_eq!(r("bytes=-3", 10), Part(7, 9));
        assert_eq!(r("bytes=-30", 10), Part(0, 9));
        assert_eq!(r("bytes=5-100", 10), Part(5, 9));
        assert_eq!(r("bytes=9-9", 10), Part(9, 9));
        assert_eq!(r("bytes=10-", 10), Unsatisfiable);
        assert_eq!(r("bytes=10-20", 10), Unsatisfiable);
        assert_eq!(r("bytes=-0", 10), Unsatisfiable);
        assert_eq!(r("bytes=0-", 0), Unsatisfiable);
        assert_eq!(r("bytes=-5", 0), Unsatisfiable);
        for full in [
            "bytes=0-1,3-4",
            "bytes=5-2",
            "bytes=a-b",
            "bytes=+1-2",
            "bytes=-",
            "bytes=1",
            "items=0-1",
            "bytes 0-1",
            "bytes=99999999999999999999-",
        ] {
            assert_eq!(r(full, 10), Full, "{full}");
        }
    }

    #[test]
    fn accept_encoding_matching() {
        let a = |h: Option<&str>, e: &str| accepts_encoding(h, e);
        assert!(a(Some("gzip, deflate, br, zstd"), "br"));
        assert!(a(Some("GZIP"), "gzip"));
        assert!(a(Some("br;q=0.5"), "br"));
        assert!(a(Some("*"), "br"));
        assert!(!a(Some("br;q=0"), "br"));
        assert!(!a(Some("gzip, deflate"), "br"));
        assert!(!a(Some("brotli"), "br"));
        assert!(!a(Some(""), "gzip"));
        assert!(!a(None, "gzip"));
    }

    #[test]
    fn etag_matching() {
        assert!(if_none_match(Some("\"bafk1\""), "\"bafk1\""));
        assert!(if_none_match(Some("\"x\", W/\"bafk1\""), "\"bafk1\""));
        assert!(if_none_match(Some("*"), "\"bafk1\""));
        assert!(!if_none_match(Some("\"bafk2\""), "\"bafk1\""));
        assert!(!if_none_match(None, "\"bafk1\""));
    }

    #[test]
    fn nonce_store_rules() {
        let now = Instant::now();
        let mut st = NonceStore::default();
        let n = st.issue("site-a", now);
        assert_eq!(n.len(), 32);
        assert!(st.consume("site-a", &n, now));
        assert!(!st.consume("site-a", &n, now), "single use");

        let n = st.issue("site-a", now);
        assert!(!st.consume("site-b", &n, now), "wrong site");
        assert!(
            !st.consume("site-a", &n, now),
            "burned by the wrong-site try"
        );

        let n = st.issue("site-a", now);
        assert!(!st.consume("site-a", &n, now + NONCE_TTL + Duration::from_secs(1)));
        assert!(!st.consume("site-a", "deadbeef", now));

        let mut st = NonceStore::default();
        for i in 0..MAX_NONCES + 10 {
            st.insert(format!("n{i}"), "s", now);
        }
        assert_eq!(st.items.len(), MAX_NONCES);
        assert!(!st.consume("s", "n0", now), "oldest evicted");
        assert!(st.consume("s", &format!("n{}", MAX_NONCES + 9), now));

        // Expired entries are pruned on insert.
        let mut st = NonceStore::default();
        st.insert("old".into(), "s", now);
        st.insert("new".into(), "s", now + NONCE_TTL + Duration::from_secs(1));
        assert_eq!(st.items.len(), 1);
    }

    #[test]
    fn form_parsing() {
        let f = parse_form(b"nonce=ab12&publisher=1&x=a+b%21&empty=&flag").unwrap();
        let expected: Vec<(String, String)> = [
            ("nonce", "ab12"),
            ("publisher", "1"),
            ("x", "a b!"),
            ("empty", ""),
            ("flag", ""),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert_eq!(f, expected);
        assert!(parse_form(b"").unwrap().is_empty());
        assert!(parse_form(b"a=%zz").is_none());
        assert!(parse_form(b"a=%4").is_none());
        assert!(parse_form(b"a=%ff").is_none());
        assert!(parse_form(&[0xff]).is_none());
    }

    #[test]
    fn html_escaping() {
        assert_eq!(
            esc("<a href=\"x\">'&'</a>"),
            "&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;"
        );
        assert_eq!(esc("日本"), "日本");
    }

    #[test]
    fn origin_check() {
        let o = "http://abc.localhost:6490";
        assert!(origin_ok(Some(o), o, None));
        assert!(origin_ok(Some(o), o, Some("same-origin")));
        assert!(!origin_ok(Some(o), o, Some("same-site")));
        assert!(!origin_ok(Some(o), o, Some("cross-site")));
        assert!(!origin_ok(None, o, None));
        assert!(!origin_ok(Some("null"), o, None));
        assert!(!origin_ok(Some("http://abc.localhost:6491"), o, None));
        assert!(!origin_ok(Some("http://localhost:6490"), o, None));
        assert!(!origin_ok(Some("http://ABC.localhost:6490"), o, None));
    }

    #[test]
    fn language_choice() {
        assert_eq!(Lang::from_accept(Some("ja,en-US;q=0.9")), Lang::Ja);
        assert_eq!(Lang::from_accept(Some("ja-JP")), Lang::Ja);
        assert_eq!(Lang::from_accept(Some("en-US,ja;q=0.8")), Lang::En);
        assert_eq!(Lang::from_accept(Some("fr")), Lang::En);
        assert_eq!(Lang::from_accept(None), Lang::En);
    }

    #[test]
    fn head_parsing() {
        let req = parse_head(b"GET /a?b HTTP/1.1\r\nHost: x\r\nX-A:  1 \r\n\r\n").unwrap();
        assert_eq!((req.method.as_str(), req.target.as_str()), ("GET", "/a?b"));
        assert_eq!(req.header("host"), Some("x"));
        assert_eq!(req.header("x-a"), Some("1"));
        assert!(parse_head(b"GET /\r\n\r\n").is_none());
        assert!(parse_head(b"GET / HTTP/2\r\n\r\n").is_none());
        assert!(parse_head(b"GET / HTTP/1.1 x\r\n\r\n").is_none());
        assert!(parse_head(b"GET / HTTP/1.1\r\nbad\r\n\r\n").is_none());
        assert!(parse_head(b"GET / HTTP/1.1\r\nA: 1\r\n folded\r\n\r\n").is_none());
    }

    #[test]
    fn pages_and_formatting() {
        assert_eq!(
            page_csp(None),
            "default-src 'none'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
        );
        assert_eq!(
            page_csp(Some("n1")),
            "default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-n1'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
        );
        let mut page = Page::message(404, "<x>", "<y>");
        page.script = Some("1");
        let html = render_page(Lang::Ja, &page, Some("n1"));
        assert!(html.contains("<title>&lt;x&gt; · mistl</title>"));
        assert!(html.contains("<p>&lt;y&gt;</p>"));
        assert!(html.contains("<script nonce=\"n1\">1</script>"));
        assert!(html.contains("lang=\"ja\""));
        assert_eq!(human_size(10), "10 B");
        assert_eq!(human_size(1536), "1.5 KiB");
        assert_eq!(human_size(40_055_603), "38.2 MiB");
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso_utc(1_764_300_000_000), "2025-11-28T03:20:00Z");
    }

    #[tokio::test]
    async fn head_reader_limits() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        client
            .write_all(b"POST /.mistl/allow HTTP/1.1\r\nHost: h\r\n\r\nnonce=1")
            .await
            .unwrap();
        let (req, left) = read_head(&mut server).await.unwrap().unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(left, b"nonce=1");

        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        let big = format!("GET / HTTP/1.1\r\nX: {}\r\n", "a".repeat(MAX_HEAD_BYTES));
        client.write_all(big.as_bytes()).await.unwrap();
        assert!(read_head(&mut server).await.is_err());
    }
}
