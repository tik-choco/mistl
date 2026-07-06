//! Mailbox engine plumbing: lazily starts the shared p2p transport
//! (`crate::net`), registers the wire-message callback on it, and
//! implements the send / deposit / forward / fetch flows described in the
//! module doc comment (`mod.rs`).
//!
//! ## Known limitation
//!
//! File transfer is metadata-only in this version: a `kind: "file"`
//! envelope's `cid` is only guaranteed to resolve in the content store of
//! whichever node called `crate::storage::Store::put` for it (the original
//! sender). Forwarding an envelope (bot -> recipient, or direct) copies the
//! envelope, not the underlying bytes, so `store.get(cid)` on the recipient
//! will only succeed if those bytes are independently available there.
//! Actual p2p block replication/transfer is future work.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::sync::{Mutex, OnceCell};
use tracing::{debug, warn};

use crate::daemon::AppState;

use super::envelope::{Envelope, EnvelopeKind, WireMessage, node_id_for};
use super::spool::{self, SpoolEntry, SpoolKind};

/// Cap on a whole outbox flush pass (individual sends are already bounded
/// inside `crate::net`) so `mailbox.*` commands degrade to "queued" instead
/// of hanging.
const NET_TIMEOUT: Duration = crate::net::NET_TIMEOUT;

/// How often the bot forward loop re-checks `get_connected_nodes()` against
/// what it's holding.
const BOT_POLL_INTERVAL: Duration = Duration::from_secs(4);

/// Lazily-initialized mailbox service state (module-internal singleton --
/// the shared p2p engine initializes once per process, and this service's
/// room is joined once per daemon run via `crate::net::ensure_started`;
/// other services may independently join their own rooms alongside it).
pub struct MailboxService {
    pub node_id: String,
    #[allow(dead_code)]
    pub did: String,
    pub room: String,
    pub serve_as_bot: bool,
    pub data_dir: PathBuf,
    /// Guards all spool file read-modify-write sequences (across outbox,
    /// inbox, and held) so concurrent IPC requests and the bot forward loop
    /// never race on the same JSON files.
    spool_lock: Mutex<()>,
    /// Handle to the daemon's own tokio runtime, captured so the (plain,
    /// non-async) mistlib raw-message callback -- invoked from mistlib's
    /// own dispatch thread, not ours -- can spawn async work back onto it.
    runtime: tokio::runtime::Handle,
}

static SERVICE: OnceCell<Arc<MailboxService>> = OnceCell::const_new();

/// Get the mailbox service, starting it (identity load, mistlib init, room
/// join, callback registration, background loops) on first call.
pub async fn ensure_started(state: &Arc<AppState>) -> Result<Arc<MailboxService>> {
    let service = SERVICE
        .get_or_try_init(|| async { init_service(state).await })
        .await?;
    Ok(service.clone())
}

async fn init_service(state: &Arc<AppState>) -> Result<Arc<MailboxService>> {
    let identity = crate::identity::current(state)
        .await
        .context("mailbox: loading identity")?;
    let node_id = identity.node_id();
    let did = identity.did().to_string();

    let mailbox_config = state.config().mailbox;
    let room = mailbox_config
        .room_id
        .clone()
        .unwrap_or_else(|| crate::net::DEFAULT_ROOM.to_string());
    let serve_as_bot = mailbox_config.serve_as_bot;

    let data_dir = crate::config::data_dir()
        .context("mailbox: resolving data dir")?
        .join("mailbox");
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("mailbox: creating {}", data_dir.display()))?;

    let transport = crate::net::ensure_started(state, room.clone())
        .await
        .context("mailbox: starting p2p transport")?;

    let service = Arc::new(MailboxService {
        node_id,
        did,
        room: transport.room.clone(),
        serve_as_bot,
        data_dir,
        spool_lock: Mutex::new(()),
        runtime: tokio::runtime::Handle::current(),
    });

    register_handler(service.clone());

    // Prime: ask any already-connected bot for mail held for us, without
    // blocking startup on it.
    {
        let service = service.clone();
        tokio::spawn(async move { broadcast_fetch_request(&service).await });
    }

    if serve_as_bot {
        let service = service.clone();
        tokio::spawn(async move { bot_forward_loop(service).await });
    }

    Ok(service)
}

fn register_handler(service: Arc<MailboxService>) {
    crate::net::register_handler(move |event_type, from_id, data| {
        if event_type == crate::net::EVENT_RAW {
            let Ok(msg) = WireMessage::from_bytes(data) else {
                return; // Not a mailbox wire message (or corrupt); ignore.
            };
            let from_id = from_id.to_string();
            let service = service.clone();
            let rt = service.runtime.clone();
            rt.spawn(async move {
                if let Err(err) = handle_wire_message(&service, &from_id, msg).await {
                    warn!(%err, from = %from_id, "mailbox: error handling wire message");
                }
            });
        } else if event_type == crate::net::EVENT_JOIN {
            // A peer just connected: eagerly retry anything we're holding
            // or waiting on rather than sitting on the next poll tick.
            let service = service.clone();
            let rt = service.runtime.clone();
            rt.spawn(async move {
                if service.serve_as_bot {
                    let _ = forward_held_to_connected(&service).await;
                }
                flush_outbox(&service).await;
                broadcast_fetch_request(&service).await;
            });
        }
    });
}

async fn handle_wire_message(
    service: &Arc<MailboxService>,
    from_node: &str,
    msg: WireMessage,
) -> Result<()> {
    match msg {
        WireMessage::Mail { envelope } => receive_mail(service, envelope).await,
        WireMessage::Deposit { envelope } => receive_deposit(service, from_node, envelope).await,
        WireMessage::DepositAck { id } => {
            debug!(id = %id, from = %from_node, "mailbox: deposit acknowledged by recipient");
            Ok(())
        }
        WireMessage::Fetch { node_id } => handle_fetch_request(service, from_node, &node_id).await,
        WireMessage::FetchResult { envelopes } => {
            for envelope in envelopes {
                receive_mail(service, envelope).await?;
            }
            Ok(())
        }
    }
}

async fn receive_mail(service: &Arc<MailboxService>, envelope: Envelope) -> Result<()> {
    match envelope.verify() {
        Ok(true) => {}
        Ok(false) => {
            warn!(id = %envelope.id, from = %envelope.from, "mailbox: dropping mail with invalid signature");
            return Ok(());
        }
        Err(err) => {
            warn!(id = %envelope.id, %err, "mailbox: could not verify mail signature, dropping");
            return Ok(());
        }
    }

    let _guard = service.spool_lock.lock().await;
    spool::append(
        &service.data_dir,
        SpoolKind::Inbox,
        SpoolEntry {
            envelope,
            received_at: now_rfc3339(),
            to_node: None,
        },
    )
}

async fn receive_deposit(
    service: &Arc<MailboxService>,
    from_node: &str,
    envelope: Envelope,
) -> Result<()> {
    if !service.serve_as_bot {
        debug!(from = from_node, "mailbox: ignoring deposit (not serving as bot)");
        return Ok(());
    }

    match envelope.verify() {
        Ok(true) => {}
        Ok(false) => {
            warn!(id = %envelope.id, from = from_node, "mailbox: rejecting deposit with invalid signature");
            return Ok(());
        }
        Err(err) => {
            warn!(id = %envelope.id, %err, from = from_node, "mailbox: could not verify deposit signature, rejecting");
            return Ok(());
        }
    }

    let to_node = node_id_for(&envelope.to);
    let id = envelope.id.clone();
    {
        let _guard = service.spool_lock.lock().await;
        spool::append(
            &service.data_dir,
            SpoolKind::Held,
            SpoolEntry {
                envelope,
                received_at: now_rfc3339(),
                to_node: Some(to_node),
            },
        )?;
    }

    // Best-effort ack; the depositor doesn't wait on it (send already
    // returned status "deposited"), so a failure here is not fatal.
    let _ = send_wire(&service.room, from_node, &WireMessage::DepositAck { id }).await;
    Ok(())
}

async fn handle_fetch_request(
    service: &Arc<MailboxService>,
    from_node: &str,
    node_id: &str,
) -> Result<()> {
    if !service.serve_as_bot {
        return Ok(());
    }

    let matching: Vec<Envelope> = {
        let _guard = service.spool_lock.lock().await;
        let held = spool::list(&service.data_dir, SpoolKind::Held)?;
        let (matching, remaining): (Vec<_>, Vec<_>) = held
            .into_iter()
            .partition(|entry| entry.to_node.as_deref() == Some(node_id));
        if !matching.is_empty() {
            spool::write_all(&service.data_dir, SpoolKind::Held, &remaining)?;
        }
        matching.into_iter().map(|entry| entry.envelope).collect()
    };

    if matching.is_empty() {
        return Ok(());
    }

    if send_wire(
        &service.room,
        from_node,
        &WireMessage::FetchResult {
            envelopes: matching.clone(),
        },
    )
    .await
    .is_err()
    {
        // Delivery failed: put the entries back so they aren't lost.
        let _guard = service.spool_lock.lock().await;
        let mut held = spool::list(&service.data_dir, SpoolKind::Held)?;
        let received_at = now_rfc3339();
        held.extend(matching.into_iter().map(|envelope| SpoolEntry {
            envelope,
            received_at: received_at.clone(),
            to_node: Some(node_id.to_string()),
        }));
        spool::write_all(&service.data_dir, SpoolKind::Held, &held)?;
    }
    Ok(())
}

/// Build, sign, and send/deposit/queue one envelope for `mailbox.send`.
pub async fn send(
    service: &Arc<MailboxService>,
    state: &Arc<AppState>,
    to: &str,
    file: Option<String>,
    message: Option<String>,
) -> Result<Value> {
    flush_outbox(service).await;

    let identity = crate::identity::current(state)
        .await
        .context("mailbox: loading identity")?;

    let mut envelope = if let Some(path) = file {
        let data =
            std::fs::read(&path).with_context(|| format!("mailbox: reading file {path}"))?;
        let size = data.len() as u64;
        let name = std::path::Path::new(&path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string());

        let store = crate::storage::store(state)
            .await
            .context("mailbox: opening content store")?;
        let cid = store
            .put(name.as_deref().unwrap_or("mailbox-attachment"), data)
            .await
            .context("mailbox: storing attachment")?;

        let mut envelope =
            Envelope::new(identity.did().to_string(), to.to_string(), EnvelopeKind::File);
        envelope.cid = Some(cid);
        envelope.name = name;
        envelope.size = Some(size);
        envelope
    } else {
        let body = message.context("mailbox.send requires `file` or `message`")?;
        let mut envelope = Envelope::new(
            identity.did().to_string(),
            to.to_string(),
            EnvelopeKind::Message,
        );
        envelope.body = Some(body);
        envelope
    };
    envelope.sign(&identity);

    let to_node = node_id_for(to);
    let connected = connected_nodes_with_timeout().await;

    let status = if connected.iter().any(|n| n == &to_node) {
        if send_wire(&service.room, &to_node, &WireMessage::Mail { envelope: envelope.clone() })
            .await
            .is_ok()
        {
            "delivered"
        } else {
            queue_outbox(service, envelope.clone()).await?;
            "queued"
        }
    } else if let Some(bot) = connected.first() {
        if send_wire(&service.room, bot, &WireMessage::Deposit { envelope: envelope.clone() })
            .await
            .is_ok()
        {
            "deposited"
        } else {
            queue_outbox(service, envelope.clone()).await?;
            "queued"
        }
    } else {
        queue_outbox(service, envelope.clone()).await?;
        "queued"
    };

    Ok(json!({ "id": envelope.id, "status": status }))
}

/// Deposits currently held here for other (offline) peers -- backs
/// `mailbox.ls`.
pub async fn list_held(service: &Arc<MailboxService>) -> Result<Vec<SpoolEntry>> {
    let _guard = service.spool_lock.lock().await;
    spool::list(&service.data_dir, SpoolKind::Held)
}

/// Drain the local inbox -- backs `mailbox.fetch`. Also opportunistically
/// retries the outbox and asks connected peers (in the background, so this
/// never waits on the network) whether they're holding anything for us.
pub async fn fetch(service: &Arc<MailboxService>) -> Result<Vec<SpoolEntry>> {
    flush_outbox(service).await;

    let background = service.clone();
    tokio::spawn(async move { broadcast_fetch_request(&background).await });

    let _guard = service.spool_lock.lock().await;
    spool::drain(&service.data_dir, SpoolKind::Inbox)
}

async fn queue_outbox(service: &MailboxService, envelope: Envelope) -> Result<()> {
    let _guard = service.spool_lock.lock().await;
    spool::append(
        &service.data_dir,
        SpoolKind::Outbox,
        SpoolEntry {
            envelope,
            received_at: now_rfc3339(),
            to_node: None,
        },
    )
}

/// Retry everything in the outbox: called opportunistically at the start of
/// `mailbox.send`/`mailbox.fetch`, and eagerly whenever a peer connects.
/// Best-effort; failures are logged, not propagated (the outbox stays
/// intact for the next retry).
async fn flush_outbox(service: &Arc<MailboxService>) {
    let _guard = service.spool_lock.lock().await;
    let entries = match spool::list(&service.data_dir, SpoolKind::Outbox) {
        Ok(entries) => entries,
        Err(err) => {
            warn!(%err, "mailbox: failed reading outbox");
            return;
        }
    };
    if entries.is_empty() {
        return;
    }

    let connected = connected_nodes_with_timeout().await;
    if connected.is_empty() {
        return;
    }

    // Bound the whole pass, not just each individual send: a large backlog
    // of unreachable recipients must not compound into a multi-minute
    // `mailbox.send`/`mailbox.fetch` call (both await this synchronously).
    // Entries not yet attempted when the deadline passes are simply left
    // queued for the next retry.
    let deadline = tokio::time::Instant::now() + NET_TIMEOUT;
    let mut still_queued = Vec::with_capacity(entries.len());
    for entry in entries {
        if tokio::time::Instant::now() >= deadline {
            still_queued.push(entry);
            continue;
        }
        let to_node = node_id_for(&entry.envelope.to);
        let sent = if connected.iter().any(|n| n == &to_node) {
            send_wire(&service.room, &to_node, &WireMessage::Mail { envelope: entry.envelope.clone() })
                .await
                .is_ok()
        } else if let Some(bot) = connected.first() {
            send_wire(&service.room, bot, &WireMessage::Deposit { envelope: entry.envelope.clone() })
                .await
                .is_ok()
        } else {
            false
        };
        if !sent {
            still_queued.push(entry);
        }
    }

    if let Err(err) = spool::write_all(&service.data_dir, SpoolKind::Outbox, &still_queued) {
        warn!(%err, "mailbox: failed writing outbox after flush");
    }
}

/// Background loop for bots: while any deposit is held, periodically check
/// whether its recipient has connected and, if so, forward it.
async fn bot_forward_loop(service: Arc<MailboxService>) {
    let mut interval = tokio::time::interval(BOT_POLL_INTERVAL);
    loop {
        interval.tick().await;
        if let Err(err) = forward_held_to_connected(&service).await {
            debug!(%err, "mailbox: bot forward pass failed");
        }
    }
}

async fn forward_held_to_connected(service: &Arc<MailboxService>) -> Result<()> {
    let held = {
        let _guard = service.spool_lock.lock().await;
        spool::list(&service.data_dir, SpoolKind::Held)?
    };
    if held.is_empty() {
        return Ok(());
    }

    let connected = connected_nodes_with_timeout().await;
    if connected.is_empty() {
        return Ok(());
    }

    let mut delivered_ids = Vec::new();
    for entry in &held {
        let Some(to_node) = entry.to_node.as_deref() else {
            continue;
        };
        if !connected.iter().any(|n| n == to_node) {
            continue;
        }
        if send_wire(&service.room, to_node, &WireMessage::Mail { envelope: entry.envelope.clone() })
            .await
            .is_ok()
        {
            delivered_ids.push(entry.envelope.id.clone());
        }
    }

    if !delivered_ids.is_empty() {
        let _guard = service.spool_lock.lock().await;
        let mut held = spool::list(&service.data_dir, SpoolKind::Held)?;
        held.retain(|entry| !delivered_ids.contains(&entry.envelope.id));
        spool::write_all(&service.data_dir, SpoolKind::Held, &held)?;
    }
    Ok(())
}

/// Ask every currently-connected peer whether they're holding mail for us.
async fn broadcast_fetch_request(service: &Arc<MailboxService>) {
    let connected = connected_nodes_with_timeout().await;
    if connected.is_empty() {
        return;
    }
    let msg = WireMessage::Fetch {
        node_id: service.node_id.clone(),
    };
    for node in connected {
        let _ = send_wire(&service.room, &node, &msg).await;
    }
}

async fn connected_nodes_with_timeout() -> Vec<String> {
    crate::net::connected_nodes().await
}

async fn send_wire(room: &str, to_node: &str, message: &WireMessage) -> Result<()> {
    crate::net::send_direct(room, to_node, message.to_bytes()?).await
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}
