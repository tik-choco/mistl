//! Gateway-only WebSocket connections to external (non-mistl) Nostr relays.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use mistlib_core::signaling::nostr::{
    DEFAULT_RELAY_LIST_URL, is_local_relay, normalize_relays, parse_relay_list_json,
};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, timeout};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{self, pki_types::ServerName};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, protocol::WebSocketConfig,
};
use tracing::{debug, warn};

use super::event::Event;
use super::filter::Filter;
use super::{Origin, Relay, now_secs};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(60);
const IDLE_TIMEOUT: Duration = Duration::from_secs(150);
const MAX_URLS: usize = 16;
const OUTBOUND_BUFFER: usize = 1024;
const RELAY_LIST_CAP: usize = 256 * 1024;
const RELAY_LIST_REFRESH: Duration = Duration::from_secs(3600);
const INTEREST_DEBOUNCE: Duration = Duration::from_secs(1);
type Interest = watch::Receiver<Arc<Vec<Filter>>>;
trait UpstreamIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> UpstreamIo for T {}
type Socket = WebSocketStream<Box<dyn UpstreamIo>>;

/// Validates an upstream relay URL (`ws://` or `wss://`, with a host).
pub fn parse_url(url: &str) -> Result<()> {
    if url.len() > 2048 {
        bail!("upstream relay URL exceeds 2048 bytes");
    }
    let request = url
        .into_client_request()
        .context("invalid upstream relay URL")?;
    let uri = request.uri();
    if !matches!(uri.scheme_str(), Some("ws" | "wss")) {
        bail!("upstream relay URL must start with ws:// or wss://");
    }
    let authority = uri.authority().context("upstream relay URL has no host")?;
    if authority.as_str().contains('@') || uri.host().is_none_or(str::is_empty) {
        bail!("upstream relay URL must have a host and no userinfo");
    }
    let suffix = authority
        .as_str()
        .rsplit_once(']')
        .map_or(authority.as_str(), |(_, s)| s);
    if suffix.contains(':') && uri.port_u16().is_none() {
        bail!("invalid upstream relay port");
    }
    if url.contains('#') {
        bail!("upstream relay URL must not contain a fragment");
    }
    Ok(())
}

struct RelayStatus {
    url: String,
    role: &'static str,
    state: &'static str,
    received: u64,
    published: u64,
    error: Option<String>,
}
type Status = Arc<Mutex<RelayStatus>>;
type Statuses = Arc<Mutex<Vec<Status>>>;

fn relay_status(url: String, role: &'static str) -> Status {
    Arc::new(Mutex::new(RelayStatus {
        url,
        role,
        state: "idle",
        received: 0,
        published: 0,
        error: None,
    }))
}

#[derive(Clone)]
struct Connection {
    filters: Arc<Vec<Filter>>,
    publish: bool,
    bridge: bool,
    mirror: bool,
    mirror_kinds: Arc<Vec<u32>>,
}

impl Connection {
    fn publishes(&self, kind: u32) -> bool {
        (!self.bridge && self.publish) || (self.mirror && self.mirror_kinds.contains(&kind))
    }

    fn ingests(&self, kind: u32, subscription: &str) -> bool {
        (!(self.bridge || (self.mirror && subscription == "mistl-mirror")))
            || self.mirror_kinds.contains(&kind)
    }
}

/// What a running relay connects to, from `[nostr]`.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    /// Explicit upstream relays (`upstreams`).
    pub upstreams: Vec<String>,
    /// Static filters for the explicit upstreams (`upstream_filters`).
    pub filters: Vec<Filter>,
    /// Publish fresh events to the explicit upstreams (`upstream_publish`).
    pub publish: bool,
    /// Signaling bridge relays: `None` = bridge off, `Some(empty)` =
    /// mistlib's default relay list, fetched only when there is interest.
    pub bridge: Option<Vec<String>>,
    /// Kinds the bridge mirrors (`mirror_kinds`).
    pub mirror_kinds: Vec<u32>,
}

/// The set of upstream connections of a running relay.
pub struct Upstreams {
    gateway: watch::Receiver<bool>,
    publish: bool,
    interest: Interest,
    relays: Statuses,
    shutdown: watch::Sender<bool>,
    tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl Upstreams {
    /// Starts bounded live fan-out; disconnected connections retain no events.
    pub fn start(
        relay: Arc<Relay>,
        plan: Plan,
        gateway: watch::Receiver<bool>,
        mut outbound: mpsc::Receiver<Arc<Event>>,
    ) -> Arc<Upstreams> {
        let Plan {
            upstreams: urls,
            filters,
            publish,
            bridge,
            mirror_kinds,
        } = plan;
        let (shutdown, mut stopped) = watch::channel(false);
        let (fanout, _) = broadcast::channel(OUTBOUND_BUFFER);
        let mut tasks = Vec::new();
        let relays = Arc::new(Mutex::new(Vec::new()));
        let mut seen = HashSet::new();
        let connection = Connection {
            filters: Arc::new(filters),
            publish,
            bridge: false,
            mirror: bridge.is_some(),
            mirror_kinds: Arc::new(mirror_kinds),
        };
        let mut explicit = Vec::new();
        for url in urls {
            if !seen.insert(relay_key(&url)) {
                continue;
            }
            if explicit.len() >= MAX_URLS {
                warn!(%url, "nostr: upstream URL limit reached");
                continue;
            }
            explicit.push(url.clone());
            let status = relay_status(url.clone(), "upstream");
            relays
                .lock()
                .expect("nostr upstream statuses poisoned")
                .push(status.clone());
            tasks.push(tokio::spawn(run(
                relay.clone(),
                url,
                connection.clone(),
                gateway.clone(),
                shutdown.subscribe(),
                fanout.clone(),
                status,
            )));
        }
        // Explicit URLs win duplicates. All explicit connections also carry
        // mistl-mirror when bridging is enabled, including bridge publications
        // when upstream_publish is false; their static subscription is intact.
        if let Some(urls) = bridge {
            let connection = Connection {
                filters: Arc::new(Vec::new()),
                publish: false,
                bridge: true,
                ..connection
            };
            if urls.is_empty() && explicit.len() < MAX_URLS {
                tasks.push(tokio::spawn(default_bridges(
                    relay.clone(),
                    explicit,
                    connection,
                    gateway.clone(),
                    shutdown.subscribe(),
                    fanout.clone(),
                    relays.clone(),
                )));
            } else {
                for url in bridge_relays(urls, &explicit) {
                    let status = relay_status(url.clone(), "bridge");
                    relays
                        .lock()
                        .expect("nostr upstream statuses poisoned")
                        .push(status.clone());
                    tasks.push(tokio::spawn(run(
                        relay.clone(),
                        url,
                        connection.clone(),
                        gateway.clone(),
                        shutdown.subscribe(),
                        fanout.clone(),
                        status,
                    )));
                }
            }
        }
        let forwarding_gateway = gateway.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stopped.changed() => break,
                    event = outbound.recv() => {
                        let Some(event) = event else { break };
                        if *forwarding_gateway.borrow() { let _ = fanout.send(event); }
                    }
                }
            }
        }));
        Arc::new(Self {
            gateway,
            publish,
            interest: relay.interest(),
            relays,
            shutdown,
            tasks: tokio::sync::Mutex::new(tasks),
        })
    }

    /// Closes every connection and waits at most five seconds. Idempotent.
    pub async fn stop(&self) {
        self.shutdown.send_replace(true);
        let mut tasks = self.tasks.lock().await;
        if timeout(
            Duration::from_secs(5),
            futures_util::future::join_all(tasks.iter_mut()),
        )
        .await
        .is_err()
        {
            for task in tasks.iter() {
                task.abort();
            }
        }
        tasks.clear();
        for status in self
            .relays
            .lock()
            .expect("nostr upstream statuses poisoned")
            .iter()
        {
            state(status, "idle");
        }
    }

    pub fn status(&self) -> Value {
        let gateway = *self.gateway.borrow();
        let interest = self.interest.borrow().len();
        let relays: Vec<_> = self
            .relays
            .lock()
            .expect("nostr upstream statuses poisoned")
            .iter()
            .map(|status| {
                let status = status.lock().expect("nostr upstream status poisoned");
                // Not the gateway: no connection, so no current error either.
                let (state, error) = if gateway && (status.role != "bridge" || interest > 0) {
                    (status.state, status.error.as_deref())
                } else {
                    ("idle", None)
                };
                json!({ "url": status.url, "role": status.role, "state": state, "received": status.received,
                    "published": status.published, "error": error })
            })
            .collect();
        json!({ "gateway": gateway, "publish": self.publish, "interest": interest, "relays": relays })
    }
}

fn state(status: &Status, value: &'static str) {
    status.lock().expect("nostr upstream status poisoned").state = value;
}

fn relay_key(url: &str) -> String {
    reqwest::Url::parse(url).map_or_else(|_| url.to_owned(), |url| url.to_string())
}

/// Resolve bridge URLs once for both configured and fetched lists. Explicit
/// upstreams consume the shared 16-connection budget and win duplicates.
fn bridge_relays(urls: Vec<String>, explicit: &[String]) -> Vec<String> {
    let mut seen: HashSet<_> = explicit.iter().map(|url| relay_key(url)).collect();
    normalize_relays(urls)
        .unwrap_or_default()
        .into_iter()
        .filter(|url| {
            parse_url(url).is_ok()
                && !is_local_relay(&relay_key(url))
                && seen.insert(relay_key(url))
        })
        .take(MAX_URLS.saturating_sub(explicit.len()))
        .collect()
}

async fn fetch_relay_list() -> Result<Vec<String>> {
    let client = reqwest::Client::builder()
        .timeout(CONNECT_TIMEOUT)
        .build()?;
    let mut response = client
        .get(DEFAULT_RELAY_LIST_URL)
        .send()
        .await?
        .error_for_status()?;
    if response
        .content_length()
        .is_some_and(|len| len > RELAY_LIST_CAP as u64)
    {
        bail!("bridge relay list exceeds 256 KiB");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > RELAY_LIST_CAP.saturating_sub(body.len()) {
            bail!("bridge relay list exceeds 256 KiB");
        }
        body.extend_from_slice(&chunk);
    }
    parse_relay_list_json(std::str::from_utf8(&body)?).map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn default_bridges(
    relay: Arc<Relay>,
    explicit: Vec<String>,
    connection: Connection,
    mut gateway: watch::Receiver<bool>,
    mut stopped: watch::Receiver<bool>,
    fanout: broadcast::Sender<Arc<Event>>,
    statuses: Statuses,
) {
    // A metadata row exposes lazy resolution and fetch errors. It disappears
    // on success (including an empty resolved set); stale cached connections
    // remain usable if a later refresh fails.
    let list_status = relay_status(DEFAULT_RELAY_LIST_URL.to_owned(), "bridge");
    statuses
        .lock()
        .expect("nostr upstream statuses poisoned")
        .push(list_status.clone());
    let mut interest = relay.interest();
    let mut children = JoinSet::new();
    let mut cached = None;
    let mut next_fetch = Instant::now();
    let mut backoff = 2u64;
    loop {
        if *stopped.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = stopped.changed() => break,
            _ = activity(&mut gateway, &mut interest, true, true) => {},
        }
        tokio::select! {
            biased;
            _ = stopped.changed() => break,
            _ = activity(&mut gateway, &mut interest, true, false) => continue,
            _ = tokio::time::sleep_until(next_fetch) => {},
        }
        let result = tokio::select! {
            biased;
            _ = stopped.changed() => break,
            _ = activity(&mut gateway, &mut interest, true, false) => continue,
            result = fetch_relay_list() => result,
        };
        match result {
            Ok(urls) => {
                let urls = bridge_relays(urls, &explicit);
                if cached.as_ref() != Some(&urls) {
                    children.abort_all();
                    while children.join_next().await.is_some() {}
                    let mut statuses = statuses.lock().expect("nostr upstream statuses poisoned");
                    statuses.retain(|status| {
                        status.lock().expect("nostr upstream status poisoned").role == "upstream"
                    });
                    for url in &urls {
                        let status = relay_status(url.clone(), "bridge");
                        statuses.push(status.clone());
                        children.spawn(run(
                            relay.clone(),
                            url.clone(),
                            connection.clone(),
                            gateway.clone(),
                            stopped.clone(),
                            fanout.clone(),
                            status,
                        ));
                    }
                    cached = Some(urls);
                } else {
                    statuses
                        .lock()
                        .expect("nostr upstream statuses poisoned")
                        .retain(|status| !Arc::ptr_eq(status, &list_status));
                }
                next_fetch = Instant::now() + RELAY_LIST_REFRESH;
                backoff = 2;
            }
            Err(error) => {
                {
                    let mut status = list_status.lock().expect("nostr upstream status poisoned");
                    status.state = "backoff";
                    status.error = Some(format!("{error:#}").chars().take(512).collect());
                }
                let mut statuses = statuses.lock().expect("nostr upstream statuses poisoned");
                if !statuses
                    .iter()
                    .any(|status| Arc::ptr_eq(status, &list_status))
                {
                    statuses.push(list_status.clone());
                }
                next_fetch = Instant::now() + Duration::from_secs(backoff);
                backoff = (backoff * 2).min(300);
            }
        }
    }
    // Children see the same shutdown watch and close their sockets normally.
    while children.join_next().await.is_some() {}
}

async fn changed<T>(receiver: &mut watch::Receiver<T>) {
    // Standalone relays use a constant true watch with no sender.
    if receiver.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

fn active(gateway: &watch::Receiver<bool>, interest: &Interest, bridge: bool) -> bool {
    *gateway.borrow() && (!bridge || !interest.borrow().is_empty())
}

async fn activity(
    gateway: &mut watch::Receiver<bool>,
    interest: &mut Interest,
    bridge: bool,
    desired: bool,
) {
    while active(gateway, interest, bridge) != desired {
        tokio::select! {
            _ = changed(gateway) => {},
            _ = changed(interest), if bridge => {},
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    relay: Arc<Relay>,
    url: String,
    connection: Connection,
    mut gateway: watch::Receiver<bool>,
    mut stopped: watch::Receiver<bool>,
    fanout: broadcast::Sender<Arc<Event>>,
    status: Status,
) {
    let mut last_seen = None;
    let mut backoff = 2u64;
    let mut interest = relay.interest();
    loop {
        state(&status, "idle");
        if *stopped.borrow() {
            return;
        }
        tokio::select! {
            biased;
            _ = stopped.changed() => return,
            _ = activity(&mut gateway, &mut interest, connection.bridge, true) => {},
        }
        state(&status, "connecting");
        let result = tokio::select! {
            biased;
            _ = stopped.changed() => return,
            _ = activity(&mut gateway, &mut interest, connection.bridge, false) => continue,
            result = connect(&url, relay.cfg().max_event_bytes) => result,
        };
        let result = match result {
            Ok(mut socket) => {
                state(&status, "connected");
                status.lock().expect("nostr upstream status poisoned").error = None;
                let connected_at = Instant::now();
                let result = tokio::select! {
                    biased;
                    _ = stopped.changed() => None,
                    _ = activity(&mut gateway, &mut interest, connection.bridge, false) => None,
                    result = session(&mut socket, &relay, &url, &connection,
                        &fanout, &status, &mut last_seen) => Some(result),
                };
                let _ = timeout(Duration::from_millis(200), socket.close(None)).await;
                if connected_at.elapsed() >= PING_INTERVAL {
                    backoff = 2;
                }
                match result {
                    Some(result) => result,
                    None => continue,
                }
            }
            Err(error) => Err(error),
        };
        if !active(&gateway, &interest, connection.bridge) {
            continue;
        }
        let error = result
            .err()
            .map_or_else(|| "upstream disconnected".to_owned(), |e| format!("{e:#}"));
        debug!(%url, %error, "nostr: upstream reconnecting");
        {
            let mut status = status.lock().expect("nostr upstream status poisoned");
            status.state = "backoff";
            status.error = Some(error.chars().take(512).collect());
        }
        let jitter = 0.8 + rand::random::<f64>() * 0.2;
        tokio::select! {
            biased;
            _ = stopped.changed() => return,
            _ = activity(&mut gateway, &mut interest, connection.bridge, false) => {},
            _ = tokio::time::sleep(Duration::from_secs_f64(backoff as f64 * jitter)) => {},
        }
        backoff = (backoff * 2).min(300);
    }
}

async fn connect(url: &str, max_event_bytes: usize) -> Result<Socket> {
    parse_url(url)?;
    let request = url.into_client_request()?;
    let uri = request.uri();
    let secure = uri.scheme_str() == Some("wss");
    let host = uri
        .host()
        .context("upstream has no host")?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
    let tcp = timeout(CONNECT_TIMEOUT, TcpStream::connect((host.as_str(), port)))
        .await
        .context("upstream TCP connect timed out")??;
    let config = WebSocketConfig::default()
        .max_message_size(Some(max_event_bytes.saturating_mul(2)))
        .max_frame_size(Some(max_event_bytes.saturating_mul(2)));
    timeout(CONNECT_TIMEOUT, async {
        let stream: Box<dyn UpstreamIo> = if secure {
            let connector = TlsConnector::from(tls_config()?);
            let tls = connector.connect(ServerName::try_from(host)?, tcp).await?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        let (socket, _) =
            tokio_tungstenite::client_async_with_config(request, stream, Some(config)).await?;
        Ok::<_, anyhow::Error>(socket)
    })
    .await
    .context("upstream handshake timed out")?
}

/// Client TLS config (webpki roots), built once and shared by reconnects.
fn tls_config() -> Result<Arc<rustls::ClientConfig>> {
    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(config.clone());
    }
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(CONFIG.get_or_init(|| Arc::new(config)).clone())
}

fn filter_json(filter: &Filter) -> Value {
    let mut value = json!({});
    for (key, field) in [
        ("ids", filter.ids.as_ref().map(|v| json!(v))),
        ("authors", filter.authors.as_ref().map(|v| json!(v))),
        ("kinds", filter.kinds.as_ref().map(|v| json!(v))),
        ("since", filter.since.map(|v| json!(v))),
        ("until", filter.until.map(|v| json!(v))),
        ("limit", filter.limit.map(|v| json!(v))),
    ] {
        if let Some(field) = field {
            value[key] = field;
        }
    }
    for (letter, values) in &filter.tags {
        value[format!("#{letter}")] = json!(values);
    }
    value
}

fn request(filters: &[Filter], last_seen: Option<u64>, now: u64, sync_days: u64) -> Value {
    let earliest = now.saturating_sub(sync_days.saturating_mul(86400));
    let since = last_seen.map_or(earliest, |last| last.saturating_sub(60).max(earliest));
    let mut req = vec![json!("REQ"), json!("mistl-up")];
    for filter in filters {
        let mut value = filter_json(filter);
        if filter.since.is_none() {
            value["since"] = json!(since);
        }
        req.push(value);
    }
    Value::Array(req)
}

fn mirror_request(filters: &[Filter], now: u64) -> Value {
    let mut req = vec![json!("REQ"), json!("mistl-mirror")];
    for filter in filters {
        let mut value = filter_json(filter);
        value["since"] = json!(now.saturating_sub(60));
        req.push(value);
    }
    Value::Array(req)
}

struct Bucket {
    tokens: f64,
    updated: Instant,
}
impl Bucket {
    fn new(now: Instant) -> Self {
        Self {
            tokens: 2000.0,
            updated: now,
        }
    }
    fn take(&mut self, now: Instant) -> bool {
        self.tokens =
            (self.tokens + now.duration_since(self.updated).as_secs_f64() * 200.0).min(2000.0);
        self.updated = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

async fn send(socket: &mut Socket, message: Message) -> Result<()> {
    timeout(CONNECT_TIMEOUT, socket.send(message))
        .await
        .context("upstream send timed out")??;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn session(
    socket: &mut Socket,
    relay: &Arc<Relay>,
    url: &str,
    connection: &Connection,
    fanout: &broadcast::Sender<Arc<Event>>,
    status: &Status,
    last_seen: &mut Option<u64>,
) -> Result<()> {
    let mut outbound = fanout.subscribe();
    if !connection.filters.is_empty() {
        send(
            socket,
            Message::Text(
                request(
                    &connection.filters,
                    *last_seen,
                    now_secs(),
                    relay.cfg().sync_days,
                )
                .to_string()
                .into(),
            ),
        )
        .await?;
    }
    let mut interest = relay.interest();
    let initial = interest.borrow_and_update().clone();
    let mut mirroring = connection.mirror && !initial.is_empty();
    if mirroring {
        send(
            socket,
            Message::Text(mirror_request(&initial, now_secs()).to_string().into()),
        )
        .await?;
    }
    let mut update_at = None;
    let mut bucket = Bucket::new(Instant::now());
    let mut last_frame = Instant::now();
    let mut ping = tokio::time::interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    loop {
        tokio::select! {
            _ = changed(&mut interest), if connection.mirror => {
                if interest.borrow().is_empty() {
                    update_at = None;
                    if mirroring {
                        send(socket, Message::Text(json!(["CLOSE", "mistl-mirror"]).to_string().into())).await?;
                        mirroring = false;
                    }
                } else if update_at.is_none() {
                    update_at = Some(Instant::now() + INTEREST_DEBOUNCE);
                }
            },
            _ = async {
                match update_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                update_at = None;
                let filters = interest.borrow_and_update().clone();
                if !filters.is_empty() {
                    send(socket, Message::Text(mirror_request(&filters, now_secs()).to_string().into())).await?;
                    mirroring = true;
                }
            },
            _ = tokio::time::sleep_until(last_frame + IDLE_TIMEOUT) => bail!("upstream idle timeout"),
            _ = ping.tick() => send(socket, Message::Ping(Vec::new().into())).await?,
            event = outbound.recv() => match event {
                Ok(event) => {
                    if !connection.publishes(event.kind) { continue; }
                    send(socket, Message::Text(json!(["EVENT", &*event]).to_string().into())).await?;
                    status.lock().expect("nostr upstream status poisoned").published += 1;
                }
                Err(broadcast::error::RecvError::Lagged(count)) => debug!(%url, count, "nostr: upstream publish lagged"),
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
            message = socket.next() => {
                let message = message.context("upstream closed")??;
                last_frame = Instant::now();
                let text = match message {
                    Message::Text(text) => text,
                    Message::Close(_) => return Ok(()),
                    Message::Ping(_) => {
                        timeout(CONNECT_TIMEOUT, socket.flush()).await.context("upstream pong timed out")??;
                        continue;
                    }
                    _ => continue,
                };
                let Ok(Value::Array(parts)) = serde_json::from_str::<Value>(&text) else { continue; };
                match parts.first().and_then(Value::as_str) {
                    Some("EVENT") if parts.len() == 3 && parts[1].is_string() => {
                        if !bucket.take(Instant::now()) {
                            debug!(%url, "nostr: upstream ingest rate exceeded");
                            continue;
                        }
                        let Ok(event) = Event::parse(&parts[2]) else { continue; };
                        let subscription = parts[1].as_str().unwrap_or_default();
                        if !connection.ingests(event.kind, subscription) { continue; }
                        // Live mirror traffic must not move the resume point of
                        // the static subscription forward.
                        let mirrored = subscription == "mistl-mirror";
                        // Known ids (incl. our own publications echoed back)
                        // need no verification round trip.
                        if relay.knows(&event.id) { continue; }
                        // One job per connection bounds outstanding verification work.
                        let created_at = event.created_at;
                        let relay = relay.clone();
                        let origin = Origin::Upstream { url: url.to_owned() };
                        let submitted = tokio::task::spawn_blocking(move || relay.submit(event, &origin, now_secs())).await?;
                        if submitted.accepted && !mirrored { *last_seen = Some(last_seen.unwrap_or(0).max(created_at)); }
                        if submitted.fresh { status.lock().expect("nostr upstream status poisoned").received += 1; }
                    }
                    Some("OK") if parts.get(2).and_then(Value::as_bool) == Some(false) =>
                        debug!(%url, message = ?parts.get(3), "nostr: upstream rejected publication"),
                    Some("NOTICE") => debug!(%url, message = ?parts.get(1), "nostr: upstream notice"),
                    Some("CLOSED") => warn!(%url, message = ?parts.get(2), "nostr: upstream subscription closed"),
                    _ => {},
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NostrConfig;
    use crate::nostr::event::test_event;
    use crate::nostr::store::{Store, StoreLimits};
    use tokio::net::TcpListener;

    const TEST_TIMEOUT: Duration = Duration::from_secs(3);

    fn plan(upstreams: Vec<String>, filters: Vec<Filter>, publish: bool) -> Plan {
        Plan {
            upstreams,
            filters,
            publish,
            ..Plan::default()
        }
    }

    fn relay() -> Arc<Relay> {
        Arc::new(Relay::new(
            NostrConfig::default(),
            Store::open(None, StoreLimits { max_events: 100 }, now_secs()).unwrap(),
            None,
            None,
            None,
        ))
    }

    fn bridge_connection() -> Connection {
        Connection {
            filters: Arc::new(Vec::new()),
            publish: false,
            bridge: true,
            mirror: true,
            mirror_kinds: Arc::new(vec![25049, 25050]),
        }
    }

    #[test]
    fn bridge_resolution_excludes_local_invalid_duplicates_and_caps_total() {
        let explicit = vec!["wss://shared.example/".to_owned()];
        let mut urls = vec![
            " wss://remote.example ".to_owned(),
            "wss://remote.example/".to_owned(),
            "wss://shared.example".to_owned(),
            "ws://localhost:7447".to_owned(),
            "ws://127.42.0.1:7447".to_owned(),
            "ws://[::1]:7447/".to_owned(),
            "ws://localhost?query".to_owned(),
            "ws://127.1/".to_owned(),
            "https://bad.example".to_owned(),
            "ws://user@bad.example/".to_owned(),
            "ws://bad.example:65536".to_owned(),
            "ws://bad.example/#fragment".to_owned(),
        ];
        assert_eq!(
            bridge_relays(urls.clone(), &explicit),
            ["wss://remote.example"]
        );
        urls.extend((0..30).map(|n| format!("wss://remote{n}.example/")));
        let resolved = bridge_relays(urls, &explicit);
        assert_eq!(resolved.len() + explicit.len(), MAX_URLS);
        assert_eq!(resolved[0], "wss://remote.example");
        assert!(bridge_relays(vec!["ws://localhost".to_owned()], &[]).is_empty());
        assert!(
            bridge_relays(
                vec!["wss://ok.example".to_owned()],
                &vec!["wss://a.example".to_owned(); MAX_URLS]
            )
            .is_empty()
        );
    }

    #[test]
    fn bridge_publication_and_ingest_are_kind_limited() {
        let bridge = bridge_connection();
        let explicit = Connection {
            publish: true,
            bridge: false,
            mirror: false,
            ..bridge.clone()
        };
        for kind in [1, 25049, 25050, 30000] {
            assert!(explicit.publishes(kind));
            assert!(explicit.ingests(kind, "mistl-up"));
            assert_eq!(bridge.publishes(kind), [25049, 25050].contains(&kind));
            assert_eq!(
                bridge.ingests(kind, "anything"),
                [25049, 25050].contains(&kind)
            );
        }
        let shared = Connection {
            bridge: false,
            ..bridge
        };
        assert!(!shared.publishes(1));
        assert!(shared.publishes(25049));
        assert!(shared.ingests(1, "mistl-up"));
        assert!(!shared.ingests(1, "mistl-mirror"));
    }

    #[test]
    fn mirror_subscription_uses_live_overlap() {
        let filter = Filter::parse(&json!({"kinds": [25049], "#d": ["room"], "since": 1})).unwrap();
        let req = mirror_request(&[filter], 1000);
        assert_eq!(
            req,
            json!(["REQ", "mistl-mirror", {"kinds": [25049], "#d": ["room"], "since": 940}])
        );
        assert_eq!(mirror_request(&[Filter::default()], 10)[2]["since"], 0);
    }

    #[test]
    fn url_validation() {
        for url in [
            "ws://localhost",
            "ws://127.0.0.1:7447/nostr",
            "wss://relay.example/path?q=1",
            "ws://[::1]:1234/",
            "wss://[2001:db8::1]/",
        ] {
            assert!(parse_url(url).is_ok(), "{url}");
        }
        for url in [
            "",
            "https://relay.example",
            "ftp://relay.example",
            "ws://",
            "ws:///path",
            "ws://:80/",
            "ws://user@relay.example",
            "wss://user:password@relay.example/",
            "ws://relay.example:65536",
            "ws://relay.example:abc",
            "ws://relay.example/#fragment",
            "ws://relay example/",
        ] {
            assert!(parse_url(url).is_err(), "{url}");
        }
        let boundary = format!("ws://example.com/{}", "a".repeat(2031));
        assert_eq!(boundary.len(), 2048);
        assert!(parse_url(&boundary).is_ok());
        assert!(parse_url(&(boundary + "a")).is_err());
    }

    #[test]
    fn filters_round_trip() {
        for raw in [
            json!({}),
            json!({"ids": [], "authors": ["a".repeat(64)], "kinds": [1, 65535],
            "#p": ["peer"], "#P": [], "since": 0, "until": 12345, "limit": 0}),
        ] {
            let filter = Filter::parse(&raw).unwrap();
            assert_eq!(Filter::parse(&filter_json(&filter)).unwrap(), filter);
        }
    }

    #[test]
    fn subscription_since_injection() {
        let filters = vec![
            Filter::default(),
            Filter {
                since: Some(42),
                ..Filter::default()
            },
        ];
        let now = 1_000_000;
        for (last_seen, expected) in [
            (None, now - 86400),
            (Some(now - 100), now - 160),
            (Some(1), now - 86400),
        ] {
            let req = request(&filters, last_seen, now, 1);
            assert_eq!(req[0], "REQ");
            assert_eq!(req[1], "mistl-up");
            assert_eq!(req[2]["since"], expected);
            assert_eq!(req[3]["since"], 42);
        }
        assert_eq!(request(&filters, Some(1), 10, u64::MAX)[2]["since"], 0);
        assert_eq!(request(&filters, Some(now), now, 0)[2]["since"], now);
    }

    #[test]
    fn ingest_bucket_bounds_burst_and_refills() {
        let now = Instant::now();
        let mut bucket = Bucket::new(now);
        for _ in 0..2000 {
            assert!(bucket.take(now));
        }
        assert!(!bucket.take(now));
        let later = now + Duration::from_millis(500);
        for _ in 0..100 {
            assert!(bucket.take(later));
        }
        assert!(!bucket.take(later));
        let later = later + Duration::from_secs(100);
        for _ in 0..2000 {
            assert!(bucket.take(later));
        }
        assert!(!bucket.take(later));
    }

    async fn accept(listener: &TcpListener) -> WebSocketStream<TcpStream> {
        let (tcp, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        timeout(TEST_TIMEOUT, tokio_tungstenite::accept_async(tcp))
            .await
            .unwrap()
            .unwrap()
    }

    async fn text(socket: &mut WebSocketStream<TcpStream>) -> Value {
        let message = timeout(TEST_TIMEOUT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    async fn wait_status(upstreams: &Upstreams, field: &str, value: Value) {
        timeout(TEST_TIMEOUT, async {
            while upstreams.status()["relays"][0][field] != value {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn assert_closed(socket: &mut WebSocketStream<TcpStream>) {
        // An idle transition may send CLOSE for the subscription just before
        // run() closes the entire bridge connection.
        timeout(TEST_TIMEOUT, async {
            loop {
                let message = socket.next().await.unwrap().unwrap();
                if matches!(message, Message::Close(_)) {
                    break;
                }
                assert!(matches!(message, Message::Text(_)));
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn bridge_tracks_interest_filters_traffic_and_closes_when_idle() {
        use crate::nostr::InterestSource;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let relay = relay();
        let mut live = relay.subscribe();
        let (gateway, gateway_rx) = watch::channel(true);
        let (stopped, stopped_rx) = watch::channel(false);
        let (fanout, _) = broadcast::channel(16);
        let status = relay_status(url.clone(), "bridge");
        // URL resolution rejects loopback in production, so exercise the
        // shared connection runner directly against a loopback fake relay.
        let task = tokio::spawn(run(
            relay.clone(),
            url,
            bridge_connection(),
            gateway_rx,
            stopped_rx,
            fanout.clone(),
            status.clone(),
        ));
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let first = Filter::parse(&json!({"kinds": [25049], "#d": ["room-a"]})).unwrap();
        let second = Filter::parse(&json!({"kinds": [25049], "#d": ["room-b"]})).unwrap();
        relay.set_interest(InterestSource::Conn(1), &[first], None);
        relay.set_interest(InterestSource::Conn(2), &[second], None);
        let mut socket = accept(&listener).await;
        let req = text(&mut socket).await;
        assert_eq!(req[1], "mistl-mirror");
        assert_eq!(req[2]["kinds"], json!([25049]));
        assert_eq!(req[2]["#d"], json!(["room-a", "room-b"]));
        assert!(
            req[2]["since"]
                .as_u64()
                .unwrap()
                .abs_diff(now_secs().saturating_sub(60))
                <= 1
        );

        // A burst of updates is coalesced into the current merged interest.
        for room in ["room-c", "room-d"] {
            let changed = Filter::parse(&json!({"kinds": [25049], "#d": [room]})).unwrap();
            relay.set_interest(InterestSource::Conn(2), &[changed], None);
        }
        assert!(
            timeout(Duration::from_millis(150), socket.next())
                .await
                .is_err()
        );
        let req = text(&mut socket).await;
        assert_eq!(req[1], "mistl-mirror");
        assert_eq!(req[2]["#d"], json!(["room-a", "room-d"]));
        assert!(
            timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );

        let ordinary = Arc::new(test_event(&[1; 32], now_secs(), 1, vec![], "ordinary"));
        let mirrored = Arc::new(test_event(&[1; 32], now_secs(), 25049, vec![], "mirrored"));
        fanout.send(ordinary).unwrap();
        fanout.send(mirrored.clone()).unwrap();
        assert_eq!(text(&mut socket).await, json!(["EVENT", &*mirrored]));
        assert_eq!(status.lock().unwrap().published, 1);
        let forbidden = test_event(&[2; 32], now_secs(), 1, vec![], "forbidden");
        let inbound = test_event(&[2; 32], now_secs(), 25050, vec![], "inbound");
        for event in [&forbidden, &inbound] {
            socket
                .send(Message::Text(
                    json!(["EVENT", "mistl-mirror", event]).to_string().into(),
                ))
                .await
                .unwrap();
        }
        assert_eq!(
            timeout(TEST_TIMEOUT, live.recv())
                .await
                .unwrap()
                .unwrap()
                .id,
            inbound.id
        );
        assert!(!relay.knows(&forbidden.id));
        // Own echoes and duplicates are still skipped before verification.
        socket
            .send(Message::Text(
                json!(["EVENT", "mistl-mirror", inbound]).to_string().into(),
            ))
            .await
            .unwrap();
        socket
            .send(Message::Ping(b"probe".to_vec().into()))
            .await
            .unwrap();
        assert_eq!(
            timeout(TEST_TIMEOUT, socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::Pong(b"probe".to_vec().into())
        );
        assert_eq!(status.lock().unwrap().received, 1);

        relay.set_interest(InterestSource::Conn(1), &[], None);
        relay.set_interest(InterestSource::Conn(2), &[], None);
        assert_closed(&mut socket).await;
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let filter = Filter::parse(&json!({"kinds": [25050], "#p": ["peer"]})).unwrap();
        relay.set_interest(InterestSource::Conn(1), &[filter], None);
        let mut resumed = accept(&listener).await;
        assert_eq!(text(&mut resumed).await[2]["kinds"], json!([25050]));
        gateway.send(false).unwrap();
        assert_closed(&mut resumed).await;
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        stopped.send(true).unwrap();
        timeout(TEST_TIMEOUT, task).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn explicit_connection_carries_static_and_mirror_subscriptions() {
        use crate::nostr::InterestSource;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let relay = relay();
        let filter = Filter::parse(&json!({"kinds": [25049], "#d": ["room"]})).unwrap();
        relay.set_interest(InterestSource::Conn(1), &[filter], None);
        let (tx, rx) = mpsc::channel(4);
        let upstreams = Upstreams::start(
            relay.clone(),
            Plan {
                upstreams: vec![url.clone()],
                filters: vec![Filter::parse(&json!({"kinds": [1]})).unwrap()],
                publish: false,
                bridge: Some(vec![url]),
                mirror_kinds: vec![25049, 25050],
            },
            watch::channel(true).1,
            rx,
        );
        let mut socket = accept(&listener).await;
        assert_eq!(text(&mut socket).await[1], "mistl-up");
        assert_eq!(text(&mut socket).await[1], "mistl-mirror");
        assert_eq!(upstreams.status()["interest"], 1);
        assert_eq!(upstreams.status()["relays"].as_array().unwrap().len(), 1);
        assert_eq!(upstreams.status()["relays"][0]["role"], "upstream");
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        tx.send(Arc::new(test_event(
            &[1; 32],
            now_secs(),
            1,
            vec![],
            "ordinary",
        )))
        .await
        .unwrap();
        let mirrored = Arc::new(test_event(
            &[1; 32],
            now_secs(),
            25049,
            vec![],
            "bridge publication",
        ));
        tx.send(mirrored.clone()).await.unwrap();
        assert_eq!(text(&mut socket).await, json!(["EVENT", &*mirrored]));
        relay.set_interest(InterestSource::Conn(1), &[], None);
        assert_eq!(text(&mut socket).await, json!(["CLOSE", "mistl-mirror"]));
        assert_eq!(upstreams.status()["interest"], 0);
        // Removing mirror interest keeps the explicit subscription connected.
        assert!(
            timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
        upstreams.stop().await;
        assert_closed(&mut socket).await;
    }

    #[tokio::test]
    async fn default_bridge_list_is_lazy_without_interest() {
        let (_tx, rx) = mpsc::channel(1);
        let upstreams = Upstreams::start(
            relay(),
            Plan {
                bridge: Some(Vec::new()),
                mirror_kinds: vec![25049, 25050],
                ..Plan::default()
            },
            watch::channel(true).1,
            rx,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = upstreams.status();
        assert_eq!(status["interest"], 0);
        assert_eq!(status["relays"][0]["url"], DEFAULT_RELAY_LIST_URL);
        assert_eq!(status["relays"][0]["state"], "idle");
        assert!(status["relays"][0]["error"].is_null());
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
    }

    #[tokio::test]
    async fn gateway_subscribes_ingests_publishes_and_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let relay = relay();
        let mut live = relay.subscribe();
        let (gateway, gateway_rx) = watch::channel(false);
        let (tx, rx) = mpsc::channel(16);
        let filters = vec![Filter::parse(&json!({"kinds": [1]})).unwrap()];
        let upstreams = Upstreams::start(
            relay.clone(),
            plan(vec![url.clone(), url], filters, true),
            gateway_rx,
            rx,
        );
        assert_eq!(upstreams.status()["relays"].as_array().unwrap().len(), 1);
        assert_eq!(upstreams.status()["relays"][0]["state"], "idle");
        tx.send(Arc::new(test_event(
            &[2; 32],
            now_secs(),
            1,
            vec![],
            "drop while idle",
        )))
        .await
        .unwrap();
        assert!(
            timeout(Duration::from_millis(120), listener.accept())
                .await
                .is_err()
        );
        gateway.send(true).unwrap();
        let mut socket = accept(&listener).await;
        let req = text(&mut socket).await;
        assert_eq!(req[0], "REQ");
        assert_eq!(req[1], "mistl-up");
        assert_eq!(req[2]["kinds"], json!([1]));
        assert!(req[2]["since"].as_u64().unwrap() <= now_secs());
        assert!(
            timeout(Duration::from_millis(80), socket.next())
                .await
                .is_err()
        );

        let event = test_event(&[1; 32], now_secs(), 1, vec![], "inbound");
        socket
            .send(Message::Text(
                json!(["EVENT", "mistl-up", event]).to_string().into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            timeout(TEST_TIMEOUT, live.recv())
                .await
                .unwrap()
                .unwrap()
                .id,
            event.id
        );
        assert!(relay.knows(&event.id));
        wait_status(&upstreams, "received", json!(1)).await;
        // Duplicates and invalid signatures must not raise the fresh counter.
        socket
            .send(Message::Text(
                json!(["EVENT", "mistl-up", event]).to_string().into(),
            ))
            .await
            .unwrap();
        let mut invalid = test_event(&[1; 32], now_secs(), 1, vec![], "invalid");
        invalid.sig = "0".repeat(128);
        socket
            .send(Message::Text(
                json!(["EVENT", "mistl-up", invalid]).to_string().into(),
            ))
            .await
            .unwrap();
        socket
            .send(Message::Ping(b"probe".to_vec().into()))
            .await
            .unwrap();
        let pong = timeout(TEST_TIMEOUT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(pong, Message::Pong(b"probe".to_vec().into()));
        assert!(!relay.knows(&invalid.id));
        assert_eq!(upstreams.status()["relays"][0]["received"], 1);

        let published = Arc::new(test_event(&[2; 32], now_secs(), 1, vec![], "outbound"));
        tx.send(published.clone()).await.unwrap();
        assert_eq!(text(&mut socket).await, json!(["EVENT", &*published]));
        wait_status(&upstreams, "published", json!(1)).await;
        gateway.send(false).unwrap();
        let closed = timeout(TEST_TIMEOUT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(closed, Message::Close(_)));
        assert_eq!(upstreams.status()["relays"][0]["state"], "idle");
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        // Reacquiring gateway ownership resumes with the per-URL overlap cursor.
        gateway.send(true).unwrap();
        let mut resumed = accept(&listener).await;
        assert_eq!(
            text(&mut resumed).await[2]["since"],
            event.created_at.saturating_sub(60)
        );
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
        assert!(matches!(
            timeout(TEST_TIMEOUT, resumed.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::Close(_)
        ));
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
    }

    #[tokio::test]
    async fn standalone_publish_only_fans_out_to_every_url() {
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let urls = [&first, &second]
            .map(|l| format!("ws://{}/", l.local_addr().unwrap()))
            .to_vec();
        // The standalone gateway watch deliberately has no sender.
        let gateway = watch::channel(true).1;
        let (tx, rx) = mpsc::channel(4);
        let upstreams = Upstreams::start(relay(), plan(urls, vec![], true), gateway, rx);
        let (mut a, mut b) = tokio::join!(accept(&first), accept(&second));
        assert!(timeout(Duration::from_millis(80), a.next()).await.is_err());
        assert!(timeout(Duration::from_millis(80), b.next()).await.is_err());
        let event = Arc::new(test_event(&[3; 32], now_secs(), 1, vec![], "fanout"));
        tx.send(event.clone()).await.unwrap();
        let (a, b) = tokio::join!(text(&mut a), text(&mut b));
        assert_eq!(a, json!(["EVENT", &*event]));
        assert_eq!(b, a);
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
    }

    #[tokio::test]
    async fn url_count_is_capped_and_idle_stop_is_idempotent() {
        let (_gateway, gateway_rx) = watch::channel(false);
        let (_tx, rx) = mpsc::channel(1);
        let mut urls: Vec<_> = (0..20)
            .map(|n| format!("ws://localhost:{}/", 1000 + n))
            .collect();
        urls.insert(0, urls[0].clone());
        let upstreams = Upstreams::start(relay(), plan(urls, vec![], false), gateway_rx, rx);
        let status = upstreams.status();
        assert_eq!(status["publish"], false);
        assert_eq!(status["relays"].as_array().unwrap().len(), MAX_URLS);
        assert!(
            status["relays"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["state"] == "idle")
        );
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
    }

    #[tokio::test]
    async fn losing_gateway_cancels_a_pending_handshake() {
        use tokio::io::AsyncReadExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let (gateway, gateway_rx) = watch::channel(true);
        let (_tx, rx) = mpsc::channel(1);
        let upstreams = Upstreams::start(relay(), plan(vec![url], vec![], false), gateway_rx, rx);
        let (mut tcp, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        gateway.send(false).unwrap();
        let mut bytes = Vec::new();
        timeout(TEST_TIMEOUT, tcp.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(upstreams.status()["relays"][0]["state"], "idle");
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
    }

    #[tokio::test]
    async fn disconnected_events_are_dropped_and_backoff_is_interruptible() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let (gateway, gateway_rx) = watch::channel(true);
        let (tx, rx) = mpsc::channel(4);
        let upstreams = Upstreams::start(relay(), plan(vec![url], vec![], true), gateway_rx, rx);
        // A failed HTTP upgrade produces backoff while retaining a live listener.
        let (tcp, _) = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        drop(tcp);
        wait_status(&upstreams, "state", json!("backoff")).await;
        assert!(upstreams.status()["relays"][0]["error"].is_string());
        tx.send(Arc::new(test_event(
            &[4; 32],
            now_secs(),
            1,
            vec![],
            "drop in backoff",
        )))
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        gateway.send(false).unwrap();
        wait_status(&upstreams, "state", json!("idle")).await;
        gateway.send(true).unwrap();
        let mut socket = accept(&listener).await;
        assert!(
            timeout(Duration::from_millis(80), socket.next())
                .await
                .is_err()
        );
        assert_eq!(upstreams.status()["relays"][0]["error"], Value::Null);
        timeout(TEST_TIMEOUT, upstreams.stop()).await.unwrap();
    }
}
