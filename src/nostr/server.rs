//! Bounded TCP/TLS listener with a small HTTP parser and NIP-01 sessions.
//!
//! Each session owns its subscriptions and one live-event receiver. Guards
//! release connection and subscription counters on every exit path.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{Message, protocol::Role, protocol::WebSocketConfig};
use tracing::debug;

use super::event::Event;
use super::filter::Filter;
use super::{Origin, Relay, now_secs};

const MAX_HEAD: usize = 8 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(60);
const IDLE_TIMEOUT: Duration = Duration::from_secs(180);
const MAX_PER_IP: usize = 32;
const EVENT_RATE: f64 = 20.0;
const EVENT_BURST: f64 = 100.0;
const QUERY_RATE: f64 = 10.0;
const QUERY_BURST: f64 = 50.0;
const CORS: &str = "Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: *\r\nAccess-Control-Allow-Methods: GET, OPTIONS\r\n";

type IpCounts = Arc<Mutex<HashMap<IpAddr, usize>>>;

struct ConnectionGuard {
    relay: Arc<Relay>,
    ips: IpCounts,
    ip: IpAddr,
}

impl ConnectionGuard {
    fn acquire(relay: &Arc<Relay>, ips: &IpCounts, ip: IpAddr) -> Option<Self> {
        let mut counts = ips.lock().unwrap_or_else(|e| e.into_inner());
        if !ip.is_loopback() && counts.get(&ip).copied().unwrap_or(0) >= MAX_PER_IP {
            return None;
        }
        relay
            .connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < relay.cfg().max_connections).then(|| n + 1)
            })
            .ok()?;
        if !ip.is_loopback() {
            *counts.entry(ip).or_default() += 1;
        }
        Some(Self {
            relay: relay.clone(),
            ips: ips.clone(),
            ip,
        })
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.relay.connections.fetch_sub(1, Ordering::Relaxed);
        if !self.ip.is_loopback() {
            let mut counts = self.ips.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(n) = counts.get_mut(&self.ip) {
                *n -= 1;
                if *n == 0 {
                    counts.remove(&self.ip);
                }
            }
        }
    }
}

/// Runs until shutdown, closing the listener and all accepted connections.
pub async fn serve(
    relay: Arc<Relay>,
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    mut shutdown: watch::Receiver<bool>,
) {
    let ips = Arc::new(Mutex::new(HashMap::new()));
    let mut tasks = JoinSet::new();
    let mut rejected = JoinSet::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            _ = rejected.join_next(), if !rejected.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    Err(error) => {
                        debug!(%error, "nostr: accept failed");
                        tokio::select! {
                            _ = shutdown.changed() => break,
                            _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                        }
                    }
                };
                let guard = ConnectionGuard::acquire(&relay, &ips, peer.ip());
                let relay = relay.clone();
                let tls = tls.clone();
                let shutdown = shutdown.clone();
                if let Some(guard) = guard {
                    tasks.spawn(async move {
                        let _guard = guard;
                        if let Err(error) = connection(relay, stream, tls, shutdown, false).await {
                            debug!(%peer, %error, "nostr: connection ended");
                        }
                    });
                } else if rejected.len() < MAX_PER_IP {
                    // Rejection handshakes are bounded too, including slow TLS peers.
                    rejected.spawn(async move {
                        let _ = connection(relay, stream, tls, shutdown, true).await;
                    });
                }
            }
        }
    }
    drop(listener);
    while tasks.join_next().await.is_some() {}
    while rejected.join_next().await.is_some() {}
}

async fn connection(
    relay: Arc<Relay>,
    stream: TcpStream,
    tls: Option<TlsAcceptor>,
    mut shutdown: watch::Receiver<bool>,
    rejected: bool,
) -> Result<()> {
    if *shutdown.borrow() {
        return Ok(());
    }
    if let Some(tls) = tls {
        let stream = tokio::select! {
            biased;
            _ = shutdown.changed() => return Ok(()),
            result = timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)) => result??,
        };
        http_connection(relay, stream, shutdown, rejected).await
    } else {
        http_connection(relay, stream, shutdown, rejected).await
    }
}

struct Request {
    method: String,
    headers: HashMap<String, String>,
    leftover: Vec<u8>,
}

async fn read_head<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Request> {
    let mut bytes = Vec::new();
    let end = loop {
        if let Some(i) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break i + 4;
        }
        if bytes.len() >= MAX_HEAD {
            bail!("HTTP head exceeds 8 KiB");
        }
        let mut chunk = [0u8; 1024];
        let cap = chunk.len().min(MAX_HEAD - bytes.len());
        let n = stream.read(&mut chunk[..cap]).await?;
        if n == 0 {
            bail!("incomplete HTTP head");
        }
        bytes.extend_from_slice(&chunk[..n]);
    };
    let head = std::str::from_utf8(&bytes[..end])?;
    let mut lines = head.split("\r\n");
    let mut request = lines.next().unwrap_or_default().split_whitespace();
    let method = request.next().ok_or_else(|| anyhow!("missing method"))?;
    let path = request.next().ok_or_else(|| anyhow!("missing path"))?;
    let version = request
        .next()
        .ok_or_else(|| anyhow!("missing HTTP version"))?;
    if !path.starts_with('/') || version != "HTTP/1.1" || request.next().is_some() {
        bail!("invalid request line");
    }
    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid header"))?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            || value.bytes().any(|b| b < 32 && b != b'\t' || b == 127)
        {
            bail!("invalid header");
        }
        headers
            .entry(name.to_ascii_lowercase())
            .and_modify(|old| {
                old.push(',');
                old.push_str(value.trim());
            })
            .or_insert_with(|| value.trim().to_string());
    }
    Ok(Request {
        method: method.to_string(),
        headers,
        leftover: bytes[end..].to_vec(),
    })
}

async fn response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: &str,
    kind: &str,
    body: &str,
    cors: bool,
) -> Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        body.len(),
        if cors { CORS } else { "" }
    );
    timeout(SEND_TIMEOUT, async {
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        stream.shutdown().await
    })
    .await??;
    Ok(())
}

async fn http_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    relay: Arc<Relay>,
    mut stream: S,
    mut shutdown: watch::Receiver<bool>,
    rejected: bool,
) -> Result<()> {
    // Cancellation covers HTTP reads and writes; upgraded sessions send Close themselves.
    let prepared = async {
        if rejected {
            response(
                &mut stream,
                "503 Service Unavailable",
                "text/plain",
                "Too many connections.",
                false,
            )
            .await?;
            return Ok(None);
        }
        let request = match timeout(HANDSHAKE_TIMEOUT, read_head(&mut stream)).await {
            Ok(Ok(request)) => request,
            _ => {
                response(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    "Invalid HTTP request.",
                    false,
                )
                .await?;
                return Ok(None);
            }
        };
        let header = |name: &str| {
            request
                .headers
                .get(name)
                .map(String::as_str)
                .unwrap_or_default()
        };
        let upgrade = header("upgrade").eq_ignore_ascii_case("websocket");
        if request.method == "GET" && upgrade {
            let key = header("sec-websocket-key");
            if !header("connection")
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
                || header("sec-websocket-version") != "13"
                || STANDARD
                    .decode(key)
                    .map_or(true, |decoded| decoded.len() != 16)
            {
                response(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    "Invalid WebSocket upgrade.",
                    false,
                )
                .await?;
                return Ok(None);
            }
            let mut hash = Sha1::new();
            hash.update(key.as_bytes());
            hash.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
            let accept = STANDARD.encode(hash.finalize());
            let reply = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            );
            timeout(SEND_TIMEOUT, async {
                stream.write_all(reply.as_bytes()).await?;
                stream.flush().await
            })
            .await??;
            return Ok(Some(request.leftover));
        }
        match request.method.as_str() {
            "OPTIONS" => response(&mut stream, "204 No Content", "text/plain", "", true).await?,
            "GET"
                if header("accept")
                    .to_ascii_lowercase()
                    .contains("application/nostr+json") =>
            {
                response(
                    &mut stream,
                    "200 OK",
                    "application/nostr+json",
                    &relay.info_document().to_string(),
                    true,
                )
                .await?;
            }
            "GET" => {
                response(
                    &mut stream,
                    "200 OK",
                    "text/plain",
                    "Please use a Nostr client to connect.",
                    false,
                )
                .await?
            }
            _ => {
                response(
                    &mut stream,
                    "405 Method Not Allowed",
                    "text/plain",
                    "Method not allowed.",
                    false,
                )
                .await?
            }
        }
        Ok::<_, anyhow::Error>(None)
    };
    if *shutdown.borrow() {
        return Ok(());
    }
    let leftover = tokio::select! {
        biased;
        _ = shutdown.changed() => return Ok(()),
        result = prepared => result?,
    };
    if let Some(leftover) = leftover {
        let live = relay.subscribe();
        let config = WebSocketConfig::default()
            .max_message_size(Some(relay.cfg().max_event_bytes))
            .max_frame_size(Some(relay.cfg().max_event_bytes));
        let ws = WebSocketStream::from_partially_read(stream, leftover, Role::Server, Some(config))
            .await;
        websocket(relay, ws, live, shutdown).await?;
    }
    Ok(())
}

async fn close<S: AsyncRead + AsyncWrite + Unpin>(ws: &mut WebSocketStream<S>) {
    let _ = timeout(Duration::from_secs(1), ws.send(Message::Close(None))).await;
}

async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    ws: &mut WebSocketStream<S>,
    message: Message,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    if *shutdown.borrow() {
        close(ws).await;
        bail!("relay shutting down");
    }
    tokio::select! {
        biased;
        _ = shutdown.changed() => {
            close(ws).await;
            bail!("relay shutting down");
        }
        result = timeout(SEND_TIMEOUT, ws.send(message)) => { result??; Ok(()) }
    }
}

async fn send_json<S: AsyncRead + AsyncWrite + Unpin>(
    ws: &mut WebSocketStream<S>,
    value: Value,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    send(ws, Message::Text(value.to_string().into()), shutdown).await
}

struct Bucket {
    tokens: f64,
    rate: f64,
    burst: f64,
    updated: Instant,
}

impl Bucket {
    fn new(rate: f64, burst: f64) -> Self {
        Self {
            tokens: burst,
            rate,
            burst,
            updated: Instant::now(),
        }
    }
    fn take(&mut self) -> bool {
        let now = Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.updated).as_secs_f64() * self.rate)
            .min(self.burst);
        self.updated = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

struct Session {
    relay: Arc<Relay>,
    subscriptions: HashMap<String, Vec<Filter>>,
    events: Bucket,
    queries: Bucket,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.relay
            .subscriptions
            .fetch_sub(self.subscriptions.len(), Ordering::Relaxed);
    }
}

impl Session {
    async fn text<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        ws: &mut WebSocketStream<S>,
        text: &str,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let parsed = serde_json::from_str::<Value>(text);
        let Some(parts) = parsed.as_ref().ok().and_then(Value::as_array) else {
            return send_json(
                ws,
                json!(["NOTICE", "error: expected a JSON array"]),
                shutdown,
            )
            .await;
        };
        let verb = parts.first().and_then(Value::as_str).unwrap_or_default();
        let id = parts.get(1).and_then(Value::as_str).unwrap_or_default();
        match verb {
            "EVENT" => {
                let value = parts.get(1).unwrap_or(&Value::Null);
                let id = value.get("id").and_then(Value::as_str).unwrap_or_default();
                let reply = if !self.events.take() {
                    json!(["OK", id, false, "rate-limited: slow down"])
                } else if parts.len() != 2 {
                    json!(["OK", id, false, "invalid: EVENT requires one event"])
                } else {
                    match Event::parse(value) {
                        Ok(event) => {
                            let id = event.id.clone();
                            let result = self.relay.submit(event, &Origin::Client, now_secs());
                            json!(["OK", id, result.accepted, result.message])
                        }
                        Err(error) => {
                            let error = error.to_string();
                            let message = if error.starts_with("invalid:") {
                                error
                            } else {
                                format!("invalid: {error}")
                            };
                            json!(["OK", id, false, message])
                        }
                    }
                };
                send_json(ws, reply, shutdown).await?;
            }
            "REQ" | "COUNT" => {
                let error = if !self.queries.take() {
                    Some("rate-limited: slow down".to_string())
                } else if id.is_empty() || id.chars().count() > 64 {
                    Some("invalid: subscription id must contain 1..64 characters".to_string())
                } else if parts.len() < 3 || parts.len() - 2 > self.relay.cfg().max_filters {
                    Some("invalid: wrong number of filters".to_string())
                } else {
                    None
                };
                if let Some(error) = error {
                    return send_json(ws, json!(["CLOSED", id, error]), shutdown).await;
                }
                let filters: std::result::Result<Vec<_>, _> =
                    parts.iter().skip(2).map(Filter::parse).collect();
                let filters = match filters {
                    Ok(filters) => filters,
                    Err(error) => {
                        return send_json(
                            ws,
                            json!(["CLOSED", id, format!("invalid: {error}")]),
                            shutdown,
                        )
                        .await;
                    }
                };
                if verb == "COUNT" {
                    let n = self.relay.count(&filters, now_secs());
                    return send_json(ws, json!(["COUNT", id, {"count": n}]), shutdown).await;
                }
                if !self.subscriptions.contains_key(id)
                    && self.subscriptions.len() >= self.relay.cfg().max_subscriptions
                {
                    return send_json(
                        ws,
                        json!(["CLOSED", id, "error: too many subscriptions"]),
                        shutdown,
                    )
                    .await;
                }
                let stored = self
                    .relay
                    .query(&filters, self.relay.cfg().max_limit, now_secs());
                if self.subscriptions.insert(id.to_string(), filters).is_none() {
                    self.relay.subscriptions.fetch_add(1, Ordering::Relaxed);
                }
                for event in stored {
                    send_json(ws, json!(["EVENT", id, event]), shutdown).await?;
                }
                send_json(ws, json!(["EOSE", id]), shutdown).await?;
            }
            "CLOSE" if parts.len() == 2 && parts.get(1).is_some_and(Value::is_string) => {
                if self.subscriptions.remove(id).is_some() {
                    self.relay.subscriptions.fetch_sub(1, Ordering::Relaxed);
                }
            }
            "AUTH" => send_json(ws, json!(["NOTICE", "unsupported: AUTH"]), shutdown).await?,
            _ => {
                send_json(
                    ws,
                    json!(["NOTICE", "error: unknown or malformed command"]),
                    shutdown,
                )
                .await?
            }
        }
        Ok(())
    }
}

async fn websocket<S: AsyncRead + AsyncWrite + Unpin + Send>(
    relay: Arc<Relay>,
    mut ws: WebSocketStream<S>,
    mut live: broadcast::Receiver<Arc<Event>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut session = Session {
        relay,
        subscriptions: HashMap::new(),
        events: Bucket::new(EVENT_RATE, EVENT_BURST),
        queries: Bucket::new(QUERY_RATE, QUERY_BURST),
    };
    let mut last_received = Instant::now();
    let mut ping = tokio::time::interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    loop {
        if *shutdown.borrow() {
            close(&mut ws).await;
            return Ok(());
        }
        tokio::select! {
            _ = shutdown.changed() => { close(&mut ws).await; return Ok(()); }
            _ = tokio::time::sleep_until(last_received + IDLE_TIMEOUT) => {
                close(&mut ws).await; return Ok(());
            }
            _ = ping.tick() => send(&mut ws, Message::Ping(Vec::new().into()), &mut shutdown).await?,
            message = ws.next() => {
                let Some(message) = message else { return Ok(()); };
                last_received = Instant::now();
                match message? {
                    Message::Text(text) => session.text(&mut ws, text.as_str(), &mut shutdown).await?,
                    Message::Binary(_) => send_json(&mut ws, json!(["NOTICE", "error: binary messages are unsupported"]), &mut shutdown).await?,
                    Message::Ping(data) => send(&mut ws, Message::Pong(data), &mut shutdown).await?,
                    Message::Close(_) => {
                        let _ = timeout(Duration::from_secs(1), ws.flush()).await;
                        return Ok(());
                    }
                    _ => {},
                }
            }
            event = live.recv() => match event {
                Ok(event) => {
                    for (id, filters) in &session.subscriptions {
                        if filters.iter().any(|filter| filter.matches(&event)) {
                            send_json(&mut ws, json!(["EVENT", id, *event]), &mut shutdown).await?;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => debug!(skipped, "nostr: live receiver lagged"),
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NostrConfig;
    use crate::nostr::event::test_event;
    use crate::nostr::store::{Store, StoreLimits};
    use std::net::SocketAddr;
    use tokio::task::JoinHandle;

    type Client = WebSocketStream<TcpStream>;
    const UPGRADE: &str = "GET /any/path HTTP/1.1\r\nHost: localhost\r\nuPgRaDe: WebSocket\r\ncOnNeCtIoN: keep-alive, UpGrAdE\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";

    struct Server {
        relay: Arc<Relay>,
        addr: SocketAddr,
        shutdown: watch::Sender<bool>,
        task: JoinHandle<()>,
    }

    impl Server {
        async fn start(cfg: NostrConfig) -> Self {
            let relay = Arc::new(Relay::new(
                cfg,
                Store::open(None, StoreLimits { max_events: 1000 }, 0).unwrap(),
                None,
                None,
            ));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown, rx) = watch::channel(false);
            let task = tokio::spawn(serve(relay.clone(), listener, None, rx));
            Self {
                relay,
                addr,
                shutdown,
                task,
            }
        }

        async fn client(&self) -> Client {
            let mut stream = TcpStream::connect(self.addr).await.unwrap();
            stream.write_all(UPGRADE.as_bytes()).await.unwrap();
            let head = client_head(&mut stream).await;
            assert!(
                head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
                "{head}"
            );
            assert!(head.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
            assert!(!head.contains("Sec-WebSocket-Extensions"));
            WebSocketStream::from_raw_socket(stream, Role::Client, None).await
        }

        async fn http(&self, request: &str) -> String {
            let mut stream = TcpStream::connect(self.addr).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut bytes = Vec::new();
            timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            String::from_utf8(bytes).unwrap()
        }

        async fn stop(self) {
            let _ = self.shutdown.send(true);
            timeout(Duration::from_secs(3), self.task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(self.relay.connections.load(Ordering::Relaxed), 0);
            assert_eq!(self.relay.subscriptions.load(Ordering::Relaxed), 0);
            assert!(TcpStream::connect(self.addr).await.is_err());
        }
    }

    async fn client_head(stream: &mut TcpStream) -> String {
        timeout(Duration::from_secs(3), async {
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                assert!(bytes.len() < MAX_HEAD);
                bytes.push(stream.read_u8().await.unwrap());
            }
            String::from_utf8(bytes).unwrap()
        })
        .await
        .unwrap()
    }

    async fn write(client: &mut Client, value: Value) {
        client
            .send(Message::Text(value.to_string().into()))
            .await
            .unwrap();
    }

    async fn read<S: AsyncRead + AsyncWrite + Unpin>(client: &mut WebSocketStream<S>) -> Value {
        timeout(Duration::from_secs(3), async {
            loop {
                match client.next().await.unwrap().unwrap() {
                    Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
                    Message::Ping(data) => client.send(Message::Pong(data)).await.unwrap(),
                    frame => panic!("unexpected frame: {frame:?}"),
                }
            }
        })
        .await
        .unwrap()
    }

    async fn publish(client: &mut Client, event: &Event) -> Value {
        write(client, json!(["EVENT", event])).await;
        let reply = read(client).await;
        assert_eq!(reply[0], "OK");
        assert_eq!(reply[1], event.id);
        reply
    }

    #[tokio::test]
    async fn nip11_get() {
        let server = Server::start(NostrConfig::default()).await;
        let reply = server
            .http("GET / HTTP/1.1\r\nHost: localhost\r\naCcEpT: application/nostr+json\r\n\r\n")
            .await;
        let (head, body) = reply.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"));
        assert!(head.contains("Content-Type: application/nostr+json"));
        assert!(head.contains("Access-Control-Allow-Origin: *"));
        assert!(head.contains("Access-Control-Allow-Headers: *"));
        assert!(head.contains("Access-Control-Allow-Methods: GET, OPTIONS"));
        assert!(head.contains("Connection: close"));
        assert!(head.contains(&format!("Content-Length: {}", body.len())));
        assert_eq!(
            serde_json::from_str::<Value>(body).unwrap()["name"],
            "mistl relay"
        );
        server.stop().await;
    }

    #[tokio::test]
    async fn plain_get_options_and_http_errors() {
        let server = Server::start(NostrConfig::default()).await;
        let reply = server
            .http("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await;
        assert!(reply.starts_with("HTTP/1.1 200 OK"));
        assert!(reply.contains("Content-Type: text/plain"));
        assert!(reply.ends_with("Please use a Nostr client to connect."));
        let reply = server
            .http("OPTIONS / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await;
        assert!(reply.starts_with("HTTP/1.1 204 No Content"));
        assert!(reply.contains("Access-Control-Allow-Origin: *"));
        assert!(reply.contains("Content-Length: 0"));
        assert!(
            server
                .http("POST / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 405")
        );
        assert!(
            server
                .http("invalid\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 400")
        );
        assert!(
            server
                .http(&UPGRADE.replace("Version: 13", "Version: 12"))
                .await
                .starts_with("HTTP/1.1 400")
        );
        assert!(
            server
                .http(&UPGRADE.replace("dGhlIHNhbXBsZSBub25jZQ==", "invalid"))
                .await
                .starts_with("HTTP/1.1 400")
        );
        let oversized = format!("GET / HTTP/1.1\r\nX-Padding: {}", "x".repeat(MAX_HEAD));
        assert!(server.http(&oversized).await.starts_with("HTTP/1.1 400"));
        server.stop().await;
    }

    #[tokio::test]
    async fn event_accept_invalid_and_duplicate() {
        let server = Server::start(NostrConfig::default()).await;
        let mut client = server.client().await;
        let event = test_event(&[1; 32], now_secs(), 1, vec![], "hello");
        assert_eq!(
            publish(&mut client, &event).await,
            json!(["OK", event.id, true, ""])
        );
        let duplicate = publish(&mut client, &event).await;
        assert_eq!(duplicate[2], true);
        assert!(duplicate[3].as_str().unwrap().starts_with("duplicate:"));
        write(&mut client, json!(["EVENT", {"id": "bad"}])).await;
        let invalid = read(&mut client).await;
        assert_eq!(invalid[0], "OK");
        assert_eq!(invalid[1], "bad");
        assert_eq!(invalid[2], false);
        assert!(invalid[3].as_str().unwrap().starts_with("invalid:"));
        write(&mut client, json!(["EVENT", {}])).await;
        let invalid = read(&mut client).await;
        assert_eq!(invalid[1], "");
        assert_eq!(invalid[2], false);
        let unseen = test_event(&[1; 32], now_secs(), 1, vec![], "unseen signed event");
        let mut forged = serde_json::to_value(&unseen).unwrap();
        forged["content"] = json!("changed after signing");
        write(&mut client, json!(["EVENT", forged])).await;
        assert_eq!(read(&mut client).await[2], false);
        server.stop().await;
    }

    #[tokio::test]
    async fn stored_eose_live_close_and_count() {
        let server = Server::start(NostrConfig::default()).await;
        let mut publisher = server.client().await;
        let first = test_event(&[1; 32], now_secs(), 1, vec![], "stored");
        assert_eq!(publish(&mut publisher, &first).await[2], true);
        let mut subscriber = server.client().await;
        write(
            &mut subscriber,
            json!(["REQ", "feed", {"kinds": [1], "limit": 1}]),
        )
        .await;
        assert_eq!(read(&mut subscriber).await, json!(["EVENT", "feed", first]));
        assert_eq!(read(&mut subscriber).await, json!(["EOSE", "feed"]));
        assert_eq!(server.relay.subscriptions.load(Ordering::Relaxed), 1);
        let live = test_event(&[2; 32], now_secs(), 1, vec![], "live");
        assert_eq!(publish(&mut publisher, &live).await[2], true);
        assert_eq!(read(&mut subscriber).await, json!(["EVENT", "feed", live]));
        write(
            &mut subscriber,
            json!(["COUNT", "total", {"kinds": [1], "limit": 1}]),
        )
        .await;
        assert_eq!(
            read(&mut subscriber).await,
            json!(["COUNT", "total", {"count": 2}])
        );
        write(&mut subscriber, json!(["CLOSE", "feed"])).await;
        // COUNT is a barrier proving CLOSE was processed and sent no reply.
        write(&mut subscriber, json!(["COUNT", "barrier", {}])).await;
        assert_eq!(
            read(&mut subscriber).await,
            json!(["COUNT", "barrier", {"count": 2}])
        );
        assert_eq!(server.relay.subscriptions.load(Ordering::Relaxed), 0);
        let later = test_event(&[2; 32], now_secs(), 1, vec![], "after CLOSE");
        assert_eq!(publish(&mut publisher, &later).await[2], true);
        assert!(
            timeout(Duration::from_millis(100), subscriber.next())
                .await
                .is_err()
        );
        server.stop().await;
    }

    #[tokio::test]
    async fn subscription_limits_replacement_and_bad_filters() {
        let cfg = NostrConfig {
            max_subscriptions: 1,
            max_filters: 1,
            ..NostrConfig::default()
        };
        let server = Server::start(cfg).await;
        let mut client = server.client().await;
        write(&mut client, json!(["REQ", "a", {"kinds": [1]}])).await;
        assert_eq!(read(&mut client).await, json!(["EOSE", "a"]));
        write(&mut client, json!(["REQ", "a", {"kinds": [2]}])).await;
        assert_eq!(read(&mut client).await, json!(["EOSE", "a"]));
        assert_eq!(server.relay.subscriptions.load(Ordering::Relaxed), 1);
        write(&mut client, json!(["REQ", "b", {}])).await;
        assert_eq!(
            read(&mut client).await,
            json!(["CLOSED", "b", "error: too many subscriptions"])
        );
        for request in [
            json!(["REQ", "bad", {"kinds": "wrong"}]),
            json!(["COUNT", "bad", {"since": -1}]),
            json!(["REQ", "bad"]),
            json!(["REQ", "bad", {}, {}]),
            json!(["REQ", "", {}]),
            json!(["REQ", "x".repeat(65), {}]),
            json!(["REQ", 12, {}]),
        ] {
            write(&mut client, request).await;
            let reply = read(&mut client).await;
            assert_eq!(reply[0], "CLOSED");
            assert!(reply[2].as_str().unwrap().starts_with("invalid:"));
        }
        let mut publisher = server.client().await;
        let ignored = test_event(&[1; 32], now_secs(), 1, vec![], "old filter");
        publish(&mut publisher, &ignored).await;
        let matched = test_event(&[1; 32], now_secs(), 2, vec![], "new filter");
        publish(&mut publisher, &matched).await;
        assert_eq!(read(&mut client).await, json!(["EVENT", "a", matched]));
        server.stop().await;
    }

    #[tokio::test]
    async fn malformed_binary_auth_and_ping() {
        let server = Server::start(NostrConfig::default()).await;
        let mut client = server.client().await;
        for text in [
            "not json",
            "{}",
            "[]",
            "[12]",
            "[\"UNKNOWN\"]",
            "[\"CLOSE\"]",
        ] {
            client.send(Message::Text(text.into())).await.unwrap();
            assert_eq!(read(&mut client).await[0], "NOTICE");
        }
        write(&mut client, json!(["AUTH", {}])).await;
        assert_eq!(
            read(&mut client).await,
            json!(["NOTICE", "unsupported: AUTH"])
        );
        client
            .send(Message::Binary(vec![1, 2].into()))
            .await
            .unwrap();
        assert_eq!(read(&mut client).await[0], "NOTICE");
        client.send(Message::Ping(vec![4, 5].into())).await.unwrap();
        let frame = timeout(Duration::from_secs(3), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(frame, Message::Pong(vec![4, 5].into()));
        server.stop().await;
    }

    #[tokio::test]
    async fn shutdown_closes_websocket_and_pending_http() {
        let server = Server::start(NostrConfig::default()).await;
        let mut client = server.client().await;
        write(&mut client, json!(["REQ", "a", {}])).await;
        assert_eq!(read(&mut client).await, json!(["EOSE", "a"]));
        let mut pending = TcpStream::connect(server.addr).await.unwrap();
        pending.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        // Wait for the pending socket to be counted without depending on a sleep.
        timeout(Duration::from_secs(3), async {
            while server.relay.connections.load(Ordering::Relaxed) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.shutdown.send(true).unwrap();
        let frame = timeout(Duration::from_secs(3), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(frame, Message::Close(_)));
        let mut byte = [0];
        match timeout(Duration::from_secs(3), pending.read(&mut byte))
            .await
            .unwrap()
        {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            other => panic!("pending HTTP connection was not closed: {other:?}"),
        }
        server.stop().await;
    }

    #[tokio::test]
    async fn connection_limit_and_disconnect_cleanup() {
        let cfg = NostrConfig {
            max_connections: 1,
            ..NostrConfig::default()
        };
        let server = Server::start(cfg).await;
        let mut client = server.client().await;
        assert_eq!(server.relay.connections.load(Ordering::Relaxed), 1);
        let reply = server
            .http("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await;
        assert!(reply.starts_with("HTTP/1.1 503 Service Unavailable"));
        let mut excess = TcpStream::connect(server.addr).await.unwrap();
        excess.write_all(UPGRADE.as_bytes()).await.unwrap();
        assert!(client_head(&mut excess).await.starts_with("HTTP/1.1 503"));
        write(&mut client, json!(["REQ", "a", {}])).await;
        read(&mut client).await;
        client.close(None).await.unwrap();
        drop(client);
        timeout(Duration::from_secs(3), async {
            while server.relay.connections.load(Ordering::Relaxed) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(server.relay.subscriptions.load(Ordering::Relaxed), 0);
        let _reconnected = server.client().await;
        server.stop().await;
    }

    #[tokio::test]
    async fn upgrade_preserves_pipelined_frame() {
        let server = Server::start(NostrConfig::default()).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        let payload = br#"["COUNT","pipelined",{}]"#;
        let mask = [1u8, 2, 3, 4];
        let mut bytes = UPGRADE.as_bytes().to_vec();
        bytes.extend_from_slice(&[0x81, 0x80 | payload.len() as u8]);
        bytes.extend_from_slice(&mask);
        bytes.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        stream.write_all(&bytes).await.unwrap();
        assert!(client_head(&mut stream).await.starts_with("HTTP/1.1 101"));
        let mut client = WebSocketStream::from_raw_socket(stream, Role::Client, None).await;
        assert_eq!(
            read(&mut client).await,
            json!(["COUNT", "pipelined", {"count": 0}])
        );
        server.stop().await;
    }

    #[test]
    fn per_ip_limit_and_guard_cleanup() {
        let relay = Arc::new(Relay::new(
            NostrConfig {
                max_connections: 100,
                ..NostrConfig::default()
            },
            Store::open(None, StoreLimits { max_events: 1000 }, 0).unwrap(),
            None,
            None,
        ));
        let ips = Arc::new(Mutex::new(HashMap::new()));
        let remote: IpAddr = "192.0.2.1".parse().unwrap();
        let mut guards = Vec::new();
        for _ in 0..MAX_PER_IP {
            guards.push(ConnectionGuard::acquire(&relay, &ips, remote).unwrap());
        }
        assert!(ConnectionGuard::acquire(&relay, &ips, remote).is_none());
        assert!(ConnectionGuard::acquire(&relay, &ips, "192.0.2.2".parse().unwrap()).is_some());
        drop(guards.pop());
        assert!(ConnectionGuard::acquire(&relay, &ips, remote).is_some());
        drop(guards);
        assert_eq!(relay.connections.load(Ordering::Relaxed), 0);
        assert!(ips.lock().unwrap().is_empty());
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let guards: Vec<_> = (0..40)
            .map(|_| ConnectionGuard::acquire(&relay, &ips, loopback).unwrap())
            .collect();
        assert!(ips.lock().unwrap().is_empty());
        drop(guards);
        assert_eq!(relay.connections.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn token_bucket_burst_refill_and_cap() {
        let mut bucket = Bucket::new(EVENT_RATE, EVENT_BURST);
        for _ in 0..100 {
            assert!(bucket.take());
        }
        assert!(!bucket.take());
        bucket.updated = Instant::now() - Duration::from_millis(100);
        assert!(bucket.take());
        assert!(bucket.take());
        assert!(!bucket.take());
        bucket.updated = Instant::now() - Duration::from_secs(1000);
        assert!(bucket.take());
        assert!(bucket.tokens <= EVENT_BURST - 1.0);
    }

    #[tokio::test]
    async fn rate_limited_protocol_replies() {
        let server = Server::start(NostrConfig::default()).await;
        let (stream, client_stream) = tokio::io::duplex(4096);
        let mut ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
        let mut client = WebSocketStream::from_raw_socket(client_stream, Role::Client, None).await;
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let mut session = Session {
            relay: server.relay.clone(),
            subscriptions: HashMap::new(),
            events: Bucket::new(0.0, 0.0),
            queries: Bucket::new(0.0, 0.0),
        };
        session
            .text(&mut ws, r#"["EVENT",{"id":"limited"}]"#, &mut shutdown)
            .await
            .unwrap();
        assert_eq!(
            read(&mut client).await,
            json!(["OK", "limited", false, "rate-limited: slow down"])
        );
        for verb in ["REQ", "COUNT"] {
            session
                .text(
                    &mut ws,
                    &json!([verb, "limited", {}]).to_string(),
                    &mut shutdown,
                )
                .await
                .unwrap();
            assert_eq!(
                read(&mut client).await,
                json!(["CLOSED", "limited", "rate-limited: slow down"])
            );
        }
        server.stop().await;
    }

    #[tokio::test]
    async fn oversized_websocket_frame_is_disconnected() {
        let server = Server::start(NostrConfig {
            max_event_bytes: 1024,
            ..NostrConfig::default()
        })
        .await;
        let mut client = server.client().await;
        client
            .send(Message::Text("x".repeat(1025).into()))
            .await
            .unwrap();
        let message = timeout(Duration::from_secs(3), client.next())
            .await
            .unwrap();
        assert!(matches!(
            message,
            None | Some(Err(_)) | Some(Ok(Message::Close(_)))
        ));
        server.stop().await;
    }

    #[tokio::test]
    async fn live_events_ignore_filter_limits_and_match_each_subscription() {
        let server = Server::start(NostrConfig::default()).await;
        let mut subscriber = server.client().await;
        write(
            &mut subscriber,
            json!(["REQ", "a", {"kinds": [1], "limit": 0}, {"kinds": [1], "limit": 0}]),
        )
        .await;
        assert_eq!(read(&mut subscriber).await, json!(["EOSE", "a"]));
        write(
            &mut subscriber,
            json!(["REQ", "b", {"kinds": [1], "limit": 0}]),
        )
        .await;
        assert_eq!(read(&mut subscriber).await, json!(["EOSE", "b"]));
        assert_eq!(server.relay.subscriptions.load(Ordering::Relaxed), 2);
        let mut publisher = server.client().await;
        let event = test_event(&[1; 32], now_secs(), 1, vec![], "live despite limit zero");
        publish(&mut publisher, &event).await;
        let mut ids = Vec::new();
        for _ in 0..2 {
            let reply = read(&mut subscriber).await;
            assert_eq!(reply[0], "EVENT");
            assert_eq!(reply[2], serde_json::to_value(&event).unwrap());
            ids.push(reply[1].as_str().unwrap().to_string());
        }
        ids.sort();
        assert_eq!(ids, ["a", "b"]);
        assert!(
            timeout(Duration::from_millis(100), subscriber.next())
                .await
                .is_err()
        );
        server.stop().await;
    }
}
