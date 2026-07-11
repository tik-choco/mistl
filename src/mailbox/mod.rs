//! Mailbox / Relay: p2p store-and-forward messaging ("p2p mail server"),
//! plus (as of the `chat_relay` addition) a standing tc-chat room relay/bot.
//! User-visible as "Relay" in the CLI (`mistl relay ...`, `mailbox` kept as a
//! `clap` alias) and the web dashboard; the internal module name, config
//! section (`[mailbox]`), and `mailbox.*` IPC command namespace are kept
//! as-is for backward compatibility.
//!
//! Peers rendezvous in a mistlib room. A message addressed to an online peer
//! is delivered directly; otherwise any node running as a bot
//! (`config.mailbox.serve_as_bot`) accepts the deposit, stores the payload in
//! the content-addressed store, and forwards it when the recipient appears.
//! Envelopes are signed with the sender's DID key (`crate::identity`) and
//! payloads are stored via `crate::storage`.
//!
//! Implementation is split across:
//! - [`envelope`]: the signed envelope type and the p2p wire message schema.
//! - [`service`]: lazy mistlib engine init/room-join, the message callback,
//!   send/deposit/forward/fetch flows, and the bot forward loop.
//! - [`spool`]: the on-disk JSON outbox/inbox/held queues.
//! - [`chat_relay`]: joins configured tc-chat rooms and relays/persists their
//!   signed `tc-chat:*` wire traffic -- an entirely separate protocol and
//!   on-disk log from the p2p-mail flow above; see its module doc.
//! - [`stable_json`]: the deterministic-JSON signing canonicalization shared
//!   by [`chat_relay`] and tc-chat.
//!
//! See `service`'s module doc for the current file-transfer limitation
//! (envelope/metadata only; no p2p block replication yet).

pub mod chat_relay;
mod envelope;
mod service;
mod spool;
mod stable_json;

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::daemon::AppState;
use envelope::EnvelopeKind;

/// Handle `mailbox.*` IPC commands:
/// - `mailbox.send` `{to, file?, message?}` -> `{id, status: "delivered"|"deposited"|"queued"}`
/// - `mailbox.ls` `{}` -> `[{id, from, to, size, held_since}]` (deposits held here)
/// - `mailbox.fetch` `{}` -> `[{id, from, message?, file?}]` (my pending mail)
/// - `mailbox.chat.rooms` `{}` -> `{enabled, rooms: [{room, joined}]}` (tc-chat relay status)
/// - `mailbox.chat.log` `{room, limit?}` -> `[{id, type, fromId, fromName, timestamp, ...}]`
///   (relayed tc-chat wires for `room`, oldest first; see [`chat_relay::chat_log`])
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "mailbox.send" | "mailbox.ls" | "mailbox.fetch" => {
            let service = service::ensure_started(state)
                .await
                .context("mailbox: starting service")?;
            match cmd {
                "mailbox.send" => cmd_send(&service, state, args).await,
                "mailbox.ls" => cmd_ls(&service).await,
                "mailbox.fetch" => cmd_fetch(&service).await,
                _ => unreachable!("matched above"),
            }
        }
        "mailbox.chat.rooms" => chat_relay::rooms_status(state).await,
        "mailbox.chat.log" => cmd_chat_log(state, args).await,
        _ => bail!("unknown mailbox command: {cmd}"),
    }
}

#[derive(Deserialize)]
struct ChatLogArgs {
    room: String,
    #[serde(default)]
    limit: Option<usize>,
}

async fn cmd_chat_log(state: &Arc<AppState>, args: Value) -> Result<Value> {
    let args: ChatLogArgs =
        serde_json::from_value(args).context("mailbox.chat.log: invalid arguments")?;
    chat_relay::chat_log(state, &args.room, args.limit.unwrap_or(50)).await
}

#[derive(Deserialize)]
struct SendArgs {
    to: String,
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

async fn cmd_send(
    service: &Arc<service::MailboxService>,
    state: &Arc<AppState>,
    args: Value,
) -> Result<Value> {
    let args: SendArgs =
        serde_json::from_value(args).context("mailbox.send: invalid arguments")?;
    if args.file.is_none() && args.message.is_none() {
        bail!("mailbox.send requires `file` or `message`");
    }
    service::send(service, state, &args.to, args.file, args.message).await
}

async fn cmd_ls(service: &Arc<service::MailboxService>) -> Result<Value> {
    let held = service::list_held(service).await?;
    let items: Vec<Value> = held
        .into_iter()
        .map(|entry| {
            json!({
                "id": entry.envelope.id,
                "from": entry.envelope.from,
                "to": entry.envelope.to,
                "size": entry.envelope.approx_size(),
                "held_since": entry.received_at,
            })
        })
        .collect();
    Ok(Value::Array(items))
}

async fn cmd_fetch(service: &Arc<service::MailboxService>) -> Result<Value> {
    let entries = service::fetch(service).await?;
    let items: Vec<Value> = entries
        .into_iter()
        .map(|entry| {
            let envelope = entry.envelope;
            let mut item = json!({
                "id": envelope.id,
                "from": envelope.from,
            });
            match envelope.kind {
                EnvelopeKind::Message => {
                    item["message"] = json!(envelope.body);
                }
                EnvelopeKind::File => {
                    item["file"] = json!({
                        "cid": envelope.cid,
                        "name": envelope.name,
                        "size": envelope.size,
                    });
                }
            }
            item
        })
        .collect();
    Ok(Value::Array(items))
}
