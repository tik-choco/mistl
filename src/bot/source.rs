//! `global-articles` source: subscribes to a `tc-news`-compatible signed
//! article room and hands the bot engine freshly-seen, unprocessed
//! [`Article`]s.
//!
//! Interop contract (matches `tc-news/src/lib/newsWire.ts` +
//! `tc-news/src/lib/globalArticlesReader.ts`, read directly for this
//! implementation):
//! - Well-known room id `tc-global-articles`
//!   (`newsWire.ts::GLOBAL_ARTICLES_ROOM_ID`), joined once (persistent for
//!   the daemon lifetime, never re-joined per tick) via
//!   [`ensure_rooms_joined`]. A pipeline's `source.rooms` may name
//!   additional/different rooms (e.g. a private `tc-news` room); empty
//!   defaults to the well-known room (see `crate::config::SourceConfig`).
//! - Each article is announced by a signed `tc-news:article` wire:
//!   `{type, id, fromId, fromName, timestamp, cid, signature, fromApp?}`.
//!   Verified with `crate::wiresign::verify_wire` -- byte-identical
//!   signing/verification contract to tc-news's own `wireSign.ts`.
//! - After joining, this module broadcasts an (unsigned, untargeted)
//!   `tc-news:history-request {type, fromId, timestamp}` -- matching
//!   `HistoryRequestWire` in `newsWire.ts` exactly (no `signature` field;
//!   `isHistoryRequestPayload` in both `useNewsRoom.ts` and
//!   `globalArticlesReader.ts` only checks `fromId`/`timestamp` are the
//!   right JS types, never verifies a signature on this wire type -- and
//!   the replay itself is addressed using the *transport's* sender id, not
//!   `wire.fromId`, so any string satisfies the contract). Any peer already
//!   in the room (a tc-news tab, or another bot) replays its stored
//!   `tc-news:article` wires back to us, verbatim signatures intact.
//! - The article body itself lives off-wire, content-addressed: `wire.cid`
//!   resolves (via `crate::storage::Store::get_remote`, scoped to the
//!   room(s) this source joined -- *not* `storage.room_ids`) to a JSON
//!   `NewsArticle` (`tc-news/src/types.ts:28-44`). `candidate.authorDid`
//!   must equal the wire's `fromId`, mirroring
//!   `globalArticlesReader.ts::hydrate`'s `authorDid !== wire.fromId` check
//!   -- a body can't claim a different author than the peer who signed its
//!   announcement.
//! - This module never answers other peers' `tc-news:history-request`s --
//!   the bot is a consumer of the global-articles feed, not a relay of it
//!   (unlike `crate::mailbox::chat_relay`, which *does* relay tc-chat). It
//!   also never re-broadcasts what it receives.
//! - [`poll_candidates`] drops any wire whose `fromId` equals this bot's own
//!   DID before resolving it, so a pipeline whose sink republishes into the
//!   same room it reads (`SinkConfig::ArticlePublish`) never feeds on its
//!   own output. Filtered per-poll (not at buffer time, unlike the
//!   chat-room source below) because [`register_handler`]'s callback is
//!   sync and process-wide -- it has no cheap access to *which* pipeline's
//!   identity to compare against, whereas [`poll_candidates`] already awaits
//!   an `AppState`.
//!
//! ## `chat-room` source
//!
//! Subscribes to one tc-chat room's signed `tc-chat:post` wires and hands
//! the bot engine `kind:"text"`, `surface:"chat"` posts (i.e. plain room chat
//! messages, not board/calendar/gallery posts or media/file/project/event
//! kinds) mapped onto [`Article`]. Interop contract (matches
//! `tc-chat/src/hooks/usePostStream.ts` + `tc-chat/src/hooks/useHistorySync.ts`
//! + `tc-chat/src/lib/chatStore.ts`, read directly for this implementation):
//! - `PostWire` (`usePostStream.ts:40-54`): `{type:"tc-chat:post", surface,
//!   id, parentId, fromId, fromName, timestamp, kind, cid, mimeType?,
//!   fileName?, fileSize?, signature}`. `surface` is one of `"chat" |
//!   "board" | "calendar" | "gallery"` (`chatStore.ts:25`); only `"chat"` is
//!   a plain room message, so [`register_chat_handler`] filters both `kind`
//!   and `surface` at buffer time (unlike the language filter above, which
//!   is a per-pipeline choice, "not room chat" is structurally useless to
//!   *every* pipeline reading this source, so excluding it before buffering
//!   is correct rather than a per-poll `Candidate { article: None }`).
//! - The post body lives off-wire at `wire.cid`, a `PostBody`
//!   (`usePostStream.ts:96-107`) JSON object; only `title`/`text` matter
//!   here. `hydratePost` (`usePostStream.ts:147-193`) only fetches this for
//!   `structuredKind` kinds (text/project/event) -- we've already narrowed
//!   to `kind:"text"`, so it's always present. This mistl daemon's own
//!   `chat-post` sink (`crate::bot::sink::build_text_post_wire` +
//!   `chat_post`) is one producer of this exact body shape
//!   (`{"title": ..., "text": ...}`); a browser tc-chat tab's `createPost`
//!   (`usePostStream.ts:301-349`) is the other.
//! - After joining, this module broadcasts a `tc-chat:history-request`
//!   **not** shaped like `tc-news:history-request` -- tc-chat's is
//!   `{type:"tc-chat:history-request", id, roomId}` (`useHistorySync.ts:22-26`),
//!   no `fromId`/`timestamp`/`signature` at all, and the replayer
//!   (`useHistorySync.ts:41-56`) keys its per-requester throttle off the
//!   *transport's* sender id, matching `roomId` against the room it's
//!   listening on (`useHistorySync.ts:63`), never checking any field of the
//!   request payload for authenticity. Broadcast after
//!   [`HISTORY_REQUEST_DELAY`], which -- happily -- already matches
//!   tc-chat's own `REQUEST_DELAY_MS` (`useHistorySync.ts:28`) at 700ms.
//! - Like the global-articles source, this bot never answers other peers'
//!   `tc-chat:history-request`s (that's `crate::mailbox::chat_relay`'s job)
//!   and skips this bot's own posts (`fromId` == this pipeline's own DID) --
//!   done in [`poll_chat_candidates`], for the same "identity isn't cheaply
//!   available in the sync room-handler callback" reason as the
//!   global-articles path above -- so a pipeline can read from and write
//!   back into the same room without self-feeding.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, OnceCell};
use tracing::debug;

use crate::daemon::AppState;

use super::persist;

/// Mirrors `newsWire.ts::GLOBAL_ARTICLES_ROOM_ID`.
pub(super) const GLOBAL_ARTICLES_ROOM_ID: &str = "tc-global-articles";

const WIRE_ARTICLE: &str = "tc-news:article";
const WIRE_HISTORY_REQUEST: &str = "tc-news:history-request";

/// Most-recent verified wires buffered per room. tc-news's own replay cap
/// (`newsWire.ts::MAX_WIRE_LOG`) is 300; this is a live receive buffer, not
/// a replay log, so a little more headroom is fine.
const BUFFER_CAP_PER_ROOM: usize = 500;
/// Delay before this bot's own catch-up history request, giving the room's
/// session (and other subscribers) time to settle after join -- matches
/// `useNewsRoom.ts`/`globalArticlesReader.ts`'s own `HISTORY_REQUEST_DELAY_MS`,
/// and (reused below for the chat-room source) tc-chat's own
/// `useHistorySync.ts::REQUEST_DELAY_MS`, both 700ms.
const HISTORY_REQUEST_DELAY: Duration = Duration::from_millis(700);
/// Budget for resolving one article body's CID over p2p. Reused by the
/// chat-room source for resolving a post body's CID -- same p2p fetch, same
/// budget.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound on how many buffered-but-unprocessed wires one
/// [`poll_candidates`] call will attempt to resolve, so a large backlog
/// can't turn a single tick into a long chain of p2p fetch timeouts --
/// anything left over simply stays unprocessed (not marked handled) and is
/// retried on the pipeline's next run. Reused as-is by
/// [`poll_chat_candidates`] for the same reason.
const POLL_CANDIDATE_LIMIT: usize = 10;

/// `tc-chat:post` wire type (`usePostStream.ts:41`), for the chat-room
/// source.
const WIRE_CHAT_POST: &str = "tc-chat:post";
/// `tc-chat:history-request` wire type (`useHistorySync.ts:23`). Note this
/// is a *different* shape than [`WIRE_HISTORY_REQUEST`] above -- see the
/// module doc's chat-room source section.
const WIRE_CHAT_HISTORY_REQUEST: &str = "tc-chat:history-request";
/// The only `PostWire.surface` value that means "a plain room chat message"
/// (`chatStore.ts:25` also has `"board" | "calendar" | "gallery"`).
const CHAT_POST_SURFACE: &str = "chat";
/// Per-room receive buffer cap for the chat-room source's hub, mirroring
/// [`BUFFER_CAP_PER_ROOM`]'s reasoning and value; kept as a separate
/// constant since the two hubs' buffers are unrelated and may reasonably
/// need different caps later.
const CHAT_BUFFER_CAP_PER_ROOM: usize = 500;

/// A verified, buffered (but not yet body-fetched) article announcement.
#[derive(Debug, Clone)]
struct BufferedWire {
    id: String,
    from_id: String,
    cid: String,
    timestamp: i64,
}

/// One `sourceLinks` entry, mirroring `tc-news/src/types.ts`'s
/// `SourceLink`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SourceLink {
    pub title: String,
    pub url: String,
}

/// A fetched, filtered `NewsArticle` (`tc-news/src/types.ts:28-44`), holding
/// only the fields the bot pipeline's transforms/sinks need.
#[derive(Debug, Clone)]
pub(super) struct Article {
    pub id: String,
    pub title: String,
    pub excerpt: String,
    pub body: String,
    pub source_links: Vec<SourceLink>,
    pub author_name: String,
    pub created_at: i64,
}

/// One buffered wire, resolved (or not) to an [`Article`]. `article` is
/// `None` when the wire was fetched and understood but semantically
/// excluded (language filter, unparseable/inconsistent body) -- such a
/// candidate should still be marked processed so it is never retried, per
/// [`poll_candidates`]'s contract. A wire whose body could not be *fetched*
/// this round (p2p timeout) never becomes a `Candidate` at all.
pub(super) struct Candidate {
    pub id: String,
    pub article: Option<Article>,
}

/// The JSON shape stored at `wire.cid` (`tc-news/src/types.ts:28-44`).
/// Fields this bot pipeline doesn't use (`tags`, `shared`, `origin`,
/// `category`, `imageUrl`) are intentionally omitted -- `serde` ignores
/// unknown fields by default.
#[derive(Debug, Deserialize)]
struct NewsArticleJson {
    id: String,
    title: String,
    #[serde(default)]
    excerpt: String,
    body: String,
    #[serde(rename = "authorDid")]
    author_did: String,
    #[serde(default, rename = "authorName")]
    author_name: String,
    #[serde(rename = "createdAt")]
    created_at: i64,
    #[serde(default, rename = "sourceLinks")]
    source_links: Vec<SourceLink>,
    #[serde(default)]
    lang: Option<String>,
}

/// Process-wide receive buffer + room-join bookkeeping, shared by every
/// pipeline whose source is `global-articles` (so two pipelines watching
/// the same room only join it once and share one buffer).
struct GlobalArticlesHub {
    /// Rooms this hub has already joined (additive-only -- see the module
    /// doc's "never re-joined per tick" note; unlike `storage::Store`'s
    /// rooms, nothing here is ever left, since several pipelines may share
    /// this hub and one pipeline's config change must not kick another's
    /// room).
    joined_rooms: Mutex<HashSet<String>>,
    buffers: Mutex<HashMap<String, VecDeque<BufferedWire>>>,
}

static HUB: OnceCell<Arc<GlobalArticlesHub>> = OnceCell::const_new();

async fn ensure_hub() -> Arc<GlobalArticlesHub> {
    HUB.get_or_init(|| async {
        let hub = Arc::new(GlobalArticlesHub {
            joined_rooms: Mutex::new(HashSet::new()),
            buffers: Mutex::new(HashMap::new()),
        });
        register_handler(hub.clone());
        hub
    })
    .await
    .clone()
}

/// Registers the process-wide room handler exactly once (guarded by
/// [`HUB`]'s `OnceCell`). Verifies and buffers `tc-news:article` wires;
/// ignores everything else, including `tc-news:history-request` (this bot
/// never answers those -- see the module doc).
fn register_handler(hub: Arc<GlobalArticlesHub>) {
    let runtime = tokio::runtime::Handle::current();
    crate::net::register_room_handler(move |event_type, room_id, _from_id, data| {
        if event_type != crate::net::EVENT_RAW {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(data) else {
            return; // not JSON; not ours
        };
        if value.get("type").and_then(Value::as_str) != Some(WIRE_ARTICLE) {
            return;
        }
        let Ok(true) = crate::wiresign::verify_wire(&value) else {
            return; // unsigned, malformed, or forged -- drop silently (untrusted peer input)
        };
        let (Some(id), Some(from_id), Some(cid)) = (
            value.get("id").and_then(Value::as_str),
            value.get("fromId").and_then(Value::as_str),
            value.get("cid").and_then(Value::as_str),
        ) else {
            return;
        };
        let wire = BufferedWire {
            id: id.to_string(),
            from_id: from_id.to_string(),
            cid: cid.to_string(),
            timestamp: value.get("timestamp").and_then(Value::as_i64).unwrap_or(0),
        };
        let room = room_id.to_string();
        let hub = hub.clone();
        runtime.spawn(async move {
            let mut buffers = hub.buffers.lock().await;
            let buf = buffers.entry(room).or_default();
            if buf.iter().any(|w| w.id == wire.id) {
                return; // duplicate delivery (live + replay, or two replayers)
            }
            buf.push_back(wire);
            if buf.len() > BUFFER_CAP_PER_ROOM {
                buf.pop_front();
            }
        });
    });
}

/// `rooms`, trimmed of blanks, or the well-known [`GLOBAL_ARTICLES_ROOM_ID`]
/// when that leaves nothing -- see `crate::config::SourceConfig::GlobalArticles`'s
/// doc comment.
fn effective_rooms(rooms: &[String]) -> Vec<String> {
    let trimmed: Vec<String> = rooms
        .iter()
        .map(|room| room.trim().to_string())
        .filter(|room| !room.is_empty())
        .collect();
    if trimmed.is_empty() {
        vec![GLOBAL_ARTICLES_ROOM_ID.to_string()]
    } else {
        trimmed
    }
}

/// Joins every room in `rooms` this hub hasn't already joined (additive
/// only -- see [`GlobalArticlesHub::joined_rooms`]'s doc), then schedules
/// this bot's own catch-up history request for each newly-joined room.
async fn ensure_rooms_joined(hub: &Arc<GlobalArticlesHub>, state: &Arc<AppState>, rooms: &[String]) -> Result<()> {
    for room in rooms {
        let already_joined = hub.joined_rooms.lock().await.contains(room);
        if already_joined {
            continue;
        }
        crate::net::ensure_started(state, room.clone())
            .await
            .with_context(|| format!("bot: joining global-articles room {room:?}"))?;
        hub.joined_rooms.lock().await.insert(room.clone());

        let state = state.clone();
        let room = room.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HISTORY_REQUEST_DELAY).await;
            request_history(&state, &room).await;
        });
    }
    Ok(())
}

/// Broadcasts this bot's `tc-news:history-request` into `room`. Unsigned by
/// design (see the module doc) -- `fromId` only needs to be *a* string;
/// this uses the bot's own DID for traceability in logs, but nothing on the
/// receiving end depends on it being a DID specifically.
async fn request_history(state: &Arc<AppState>, room: &str) {
    let identity = match crate::identity::current(state).await {
        Ok(identity) => identity,
        Err(error) => {
            debug!(%error, room, "bot: could not load identity for a global-articles history request");
            return;
        }
    };
    let wire = serde_json::json!({
        "type": WIRE_HISTORY_REQUEST,
        "fromId": identity.did(),
        "timestamp": chrono::Utc::now().timestamp_millis(),
    });
    let Ok(bytes) = serde_json::to_vec(&wire) else {
        return;
    };
    if let Err(error) = crate::net::send_broadcast(room, bytes).await {
        debug!(%error, room, "bot: global-articles history request broadcast failed");
    }
}

/// Validates a fetched article body against its announcing wire and the
/// pipeline's language filter. `None` means "never retry this one" (see
/// [`Candidate`]'s doc).
fn parse_and_filter(bytes: &[u8], announced_from_id: &str, langs: &[String]) -> Option<Article> {
    let raw: NewsArticleJson = serde_json::from_slice(bytes).ok()?;
    if raw.id.is_empty() || raw.author_did.is_empty() {
        return None;
    }
    // Mirrors `globalArticlesReader.ts::hydrate`'s
    // `candidate.authorDid !== wire.fromId` discard: a body can't claim a
    // different author than whoever signed the wire announcing it.
    if raw.author_did != announced_from_id {
        return None;
    }
    if !langs.is_empty() {
        let lang = raw.lang.as_deref().unwrap_or("");
        if !langs.iter().any(|allowed| allowed == lang) {
            return None;
        }
    }
    Some(Article {
        id: raw.id,
        title: raw.title,
        excerpt: raw.excerpt,
        body: raw.body,
        source_links: raw.source_links,
        author_name: raw.author_name,
        created_at: raw.created_at,
    })
}

/// Ensures `rooms` are joined, then returns up to [`POLL_CANDIDATE_LIMIT`]
/// not-yet-processed candidates (oldest first) for `pipeline_id`, fetching
/// and filtering each one's body along the way. A wire whose body can't be
/// resolved this round is simply omitted (left for a later run); a resolved
/// body that fails validation/filtering is included with `article: None`
/// (see [`Candidate`]).
pub(super) async fn poll_candidates(
    state: &Arc<AppState>,
    data_dir: &std::path::Path,
    pipeline_id: &str,
    rooms: &[String],
    langs: &[String],
) -> Result<Vec<Candidate>> {
    let hub = ensure_hub().await;
    let rooms = effective_rooms(rooms);
    ensure_rooms_joined(&hub, state, &rooms).await?;

    // Loaded even though this source only ever *reads*, never publishes --
    // see the module doc: a pipeline may republish into the same room via a
    // sink (e.g. `SinkConfig::ArticlePublish`), and skipping our own DID
    // here is what stops that from feeding back into this same poll.
    let identity = crate::identity::current(state)
        .await
        .context("bot: loading identity for the global-articles source")?;
    let own_did = identity.did();

    let processed = persist::load_processed(data_dir, pipeline_id).await?;

    let mut unresolved: Vec<(String, BufferedWire)> = Vec::new();
    {
        let buffers = hub.buffers.lock().await;
        for room in &rooms {
            let Some(buf) = buffers.get(room) else { continue };
            for wire in buf {
                if processed.contains(&wire.id) {
                    continue;
                }
                if wire.from_id == own_did {
                    continue; // this bot's own article -- see the module doc
                }
                if unresolved.iter().any(|(_, w)| w.id == wire.id) {
                    continue; // same article seen via more than one configured room
                }
                unresolved.push((room.clone(), wire.clone()));
            }
        }
    }
    unresolved.sort_by_key(|(_, wire)| wire.timestamp);
    unresolved.truncate(POLL_CANDIDATE_LIMIT);

    let store = crate::storage::store(state).await?;
    let mut candidates = Vec::with_capacity(unresolved.len());
    for (room, wire) in unresolved {
        match store.get_remote(&wire.cid, &[room.clone()], FETCH_TIMEOUT).await {
            Ok(bytes) => {
                let article = parse_and_filter(&bytes, &wire.from_id, langs);
                candidates.push(Candidate { id: wire.id, article });
            }
            Err(error) => {
                debug!(
                    %error, article_id = %wire.id, room,
                    "bot: could not resolve a global-articles CID this run; will retry later"
                );
                // Left unresolved: not pushed to `candidates`, so the
                // caller never marks it processed and it's retried later.
            }
        }
    }
    Ok(candidates)
}

/// A verified, buffered (but not yet body-fetched) `tc-chat:post`
/// `kind:"text"`/`surface:"chat"` announcement -- the chat-room source's
/// counterpart to [`BufferedWire`]. Carries `from_name` (unlike
/// `BufferedWire`) because [`Article::author_name`] comes straight from the
/// wire for chat posts, not from the fetched body (`tc-chat`'s `PostBody`
/// has no author field of its own -- see `chatStore.ts`'s `PostNode`, which
/// gets `fromName` from the wire the same way).
#[derive(Debug, Clone)]
struct BufferedChatWire {
    id: String,
    from_id: String,
    from_name: String,
    cid: String,
    timestamp: i64,
}

/// Process-wide receive buffer + room-join bookkeeping for the `chat-room`
/// source, mirroring [`GlobalArticlesHub`] but keyed to a single room per
/// pipeline (`SourceConfig::ChatRoom` names exactly one room, unlike
/// `GlobalArticles`'s `rooms` list) and to `tc-chat:post` instead of
/// `tc-news:article`. Several pipelines watching the same chat room share
/// one join and one buffer, same as the global-articles hub.
struct ChatHub {
    joined_rooms: Mutex<HashSet<String>>,
    buffers: Mutex<HashMap<String, VecDeque<BufferedChatWire>>>,
}

static CHAT_HUB: OnceCell<Arc<ChatHub>> = OnceCell::const_new();

async fn ensure_chat_hub() -> Arc<ChatHub> {
    CHAT_HUB
        .get_or_init(|| async {
            let hub = Arc::new(ChatHub {
                joined_rooms: Mutex::new(HashSet::new()),
                buffers: Mutex::new(HashMap::new()),
            });
            register_chat_handler(hub.clone());
            hub
        })
        .await
        .clone()
}

/// Registers the process-wide chat-room handler exactly once (guarded by
/// [`CHAT_HUB`]'s `OnceCell`), mirroring [`register_handler`]. Verifies and
/// buffers `tc-chat:post` wires that are both `kind:"text"` and
/// `surface:"chat"` (see the module doc for why that filter belongs here,
/// at buffer time); ignores everything else, including
/// `tc-chat:history-request` (this bot never answers those, matching the
/// global-articles source's same policy).
fn register_chat_handler(hub: Arc<ChatHub>) {
    let runtime = tokio::runtime::Handle::current();
    crate::net::register_room_handler(move |event_type, room_id, _from_id, data| {
        if event_type != crate::net::EVENT_RAW {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(data) else {
            return; // not JSON; not ours
        };
        if value.get("type").and_then(Value::as_str) != Some(WIRE_CHAT_POST) {
            return;
        }
        let Ok(true) = crate::wiresign::verify_wire(&value) else {
            return; // unsigned, malformed, or forged -- drop silently (untrusted peer input)
        };
        if value.get("kind").and_then(Value::as_str) != Some("text") {
            return; // media/file/project/event -- useless to every pipeline (see module doc)
        }
        if value.get("surface").and_then(Value::as_str) != Some(CHAT_POST_SURFACE) {
            return; // board/calendar/gallery, not a room chat message
        }
        let (Some(id), Some(from_id), Some(cid)) = (
            value.get("id").and_then(Value::as_str),
            value.get("fromId").and_then(Value::as_str),
            value.get("cid").and_then(Value::as_str),
        ) else {
            return;
        };
        let from_name = value.get("fromName").and_then(Value::as_str).unwrap_or("").to_string();
        let wire = BufferedChatWire {
            id: id.to_string(),
            from_id: from_id.to_string(),
            from_name,
            cid: cid.to_string(),
            timestamp: value.get("timestamp").and_then(Value::as_i64).unwrap_or(0),
        };
        let room = room_id.to_string();
        let hub = hub.clone();
        runtime.spawn(async move {
            let mut buffers = hub.buffers.lock().await;
            let buf = buffers.entry(room).or_default();
            if buf.iter().any(|w| w.id == wire.id) {
                return; // duplicate delivery (live + replay, or two replayers)
            }
            buf.push_back(wire);
            if buf.len() > CHAT_BUFFER_CAP_PER_ROOM {
                buf.pop_front();
            }
        });
    });
}

/// Joins `room` (once per daemon lifetime, shared across every pipeline
/// using this hub) if not already joined, then schedules this bot's own
/// catch-up history request -- mirrors [`ensure_rooms_joined`], just for one
/// room instead of a list (`SourceConfig::ChatRoom` names exactly one).
async fn ensure_chat_room_joined(hub: &Arc<ChatHub>, state: &Arc<AppState>, room: &str) -> Result<()> {
    let already_joined = hub.joined_rooms.lock().await.contains(room);
    if already_joined {
        return Ok(());
    }
    crate::net::ensure_started(state, room.to_string())
        .await
        .with_context(|| format!("bot: joining chat-room source room {room:?}"))?;
    hub.joined_rooms.lock().await.insert(room.to_string());

    let room = room.to_string();
    tokio::spawn(async move {
        tokio::time::sleep(HISTORY_REQUEST_DELAY).await;
        request_chat_history(&room).await;
    });
    Ok(())
}

/// Random-ish request id, matching the shape (if not the exact algorithm) of
/// tc-chat's own `newId()` -- any collision-resistant string works, nothing
/// on the receiving end re-parses it (see [`build_chat_history_request_wire`]).
fn new_history_request_id() -> String {
    format!("bot-hist-{}-{:016x}", chrono::Utc::now().timestamp_millis(), rand::random::<u64>())
}

/// Builds the `tc-chat:history-request` wire -- `{type, id, roomId}`,
/// matching `HistoryRequest` (`useHistorySync.ts:22-26`) exactly. No
/// `fromId`/`timestamp`/`signature`: unlike `tc-news:history-request`, this
/// wire type carries no identity fields at all (see the module doc); the
/// replayer's per-requester throttle keys off the *transport's* sender id,
/// not anything in this payload. Pure aside from the id, so directly
/// unit-testable.
fn build_chat_history_request_wire(room: &str) -> Value {
    json!({
        "type": WIRE_CHAT_HISTORY_REQUEST,
        "id": new_history_request_id(),
        "roomId": room,
    })
}

/// Broadcasts this bot's `tc-chat:history-request` into `room`. Needs no
/// identity (unlike [`request_history`]) -- see [`build_chat_history_request_wire`].
async fn request_chat_history(room: &str) {
    let wire = build_chat_history_request_wire(room);
    let Ok(bytes) = serde_json::to_vec(&wire) else {
        return;
    };
    if let Err(error) = crate::net::send_broadcast(room, bytes).await {
        debug!(%error, room, "bot: chat-room history request broadcast failed");
    }
}

/// The JSON shape stored at a `tc-chat:post` `kind:"text"` wire's `cid`
/// (`PostBody`, `usePostStream.ts:96-107`). Fields this bot pipeline doesn't
/// use (`roles`/`tags`/`startsAt`/`endsAt`/`location`/`thumbCid`/
/// `thumbMimeType` -- all irrelevant to a plain text chat message) are
/// intentionally omitted; `serde` ignores unknown fields by default.
#[derive(Debug, Deserialize)]
struct ChatPostBody {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

/// Max characters kept for a title synthesized from a chat post's body text
/// (no `title` field set) -- "~80 chars" per the pipeline brief.
const CHAT_TITLE_FALLBACK_MAX_CHARS: usize = 80;

/// First line of `text`, trimmed and truncated to at most `max_chars`
/// *characters* (not bytes -- a byte-slice would panic or corrupt on
/// multi-byte UTF-8 text, e.g. Japanese, which this codebase handles a lot
/// of elsewhere).
fn first_line_truncated(text: &str, max_chars: usize) -> String {
    let first_line = text.lines().next().unwrap_or("").trim();
    first_line.chars().take(max_chars).collect()
}

/// Maps a resolved `tc-chat:post` body onto the pipeline's common [`Article`]
/// shape (see [`poll_chat_candidates`]'s doc for the exact field mapping).
/// `None` when the body is unparseable or has no usable text -- `usePostStream
/// .ts`'s own `createPost` refuses to publish a text post with no
/// `text`/`title` in the first place (`!input.text?.trim() &&
/// !input.title?.trim()`, `usePostStream.ts:306`), but a malformed or
/// malicious peer could still broadcast a wire whose body has neither, so
/// this is re-checked on the consuming side too. Per [`Candidate`]'s
/// contract, a `None` here should be marked processed and never retried
/// (this body will never become valid).
fn chat_body_to_article(bytes: &[u8], wire: &BufferedChatWire) -> Option<Article> {
    let body: ChatPostBody = serde_json::from_slice(bytes).ok()?;
    let text = body.text.unwrap_or_default();
    if text.trim().is_empty() {
        return None;
    }
    let title = match body.title.as_deref().map(str::trim) {
        Some(title) if !title.is_empty() => title.to_string(),
        _ => first_line_truncated(&text, CHAT_TITLE_FALLBACK_MAX_CHARS),
    };
    Some(Article {
        id: wire.id.clone(),
        title,
        excerpt: String::new(),
        body: text,
        source_links: Vec::new(),
        author_name: wire.from_name.clone(),
        created_at: wire.timestamp,
    })
}

/// `chat-room` source: joins `room`, buffers verified `tc-chat:post`
/// `kind:"text"`/`surface:"chat"` wires (see [`register_chat_handler`]), and
/// returns not-yet-processed candidates with the post body mapped onto
/// [`Article`] -- the pipeline's common item shape. Mirrors
/// [`poll_candidates`]'s contract exactly: a body that can't be *fetched*
/// this round is omitted (retried later); a resolved-but-unusable body
/// (unparseable JSON, or no usable text -- see [`chat_body_to_article`])
/// comes back as `Candidate { article: None }` (never retried). Also skips
/// this bot's own posts (`wire.from_id == this pipeline's own DID`) here,
/// for the same "identity isn't cheaply available in the sync room-handler
/// callback" reason [`poll_candidates`] does its own-DID skip at this layer
/// rather than in [`register_chat_handler`] -- see the module doc.
///
/// Field mapping onto [`Article`]:
/// - `id` = the wire's `id`
/// - `title` = the body's `title` if present and non-blank, else the first
///   line of `text`, truncated to [`CHAT_TITLE_FALLBACK_MAX_CHARS`]
/// - `excerpt` = "" (chat posts have no separate excerpt/body split)
/// - `body` = the body's `text`
/// - `source_links` = [] (chat posts never carry structured source links)
/// - `author_name` = the wire's `fromName` (defaults to "" if the wire
///   omitted it -- see [`register_chat_handler`])
/// - `created_at` = the wire's `timestamp`
pub(super) async fn poll_chat_candidates(
    state: &Arc<AppState>,
    data_dir: &std::path::Path,
    pipeline_id: &str,
    room: &str,
) -> Result<Vec<Candidate>> {
    let room = room.trim();
    if room.is_empty() {
        bail!(
            "bot.pipelines[id={pipeline_id:?}].source.room is not set; set it with \
             `mistl config set bot.pipelines <json>`"
        );
    }

    let hub = ensure_chat_hub().await;
    ensure_chat_room_joined(&hub, state, room).await?;

    let identity = crate::identity::current(state)
        .await
        .context("bot: loading identity for the chat-room source")?;
    let own_did = identity.did();

    let processed = persist::load_processed(data_dir, pipeline_id).await?;

    let mut unresolved: Vec<BufferedChatWire> = Vec::new();
    {
        let buffers = hub.buffers.lock().await;
        if let Some(buf) = buffers.get(room) {
            for wire in buf {
                if processed.contains(&wire.id) {
                    continue;
                }
                if wire.from_id == own_did {
                    continue; // this bot's own post -- see the module doc
                }
                unresolved.push(wire.clone());
            }
        }
    }
    unresolved.sort_by_key(|wire| wire.timestamp);
    unresolved.truncate(POLL_CANDIDATE_LIMIT);

    let store = crate::storage::store(state).await?;
    let mut candidates = Vec::with_capacity(unresolved.len());
    for wire in unresolved {
        match store.get_remote(&wire.cid, &[room.to_string()], FETCH_TIMEOUT).await {
            Ok(bytes) => {
                let article = chat_body_to_article(&bytes, &wire);
                candidates.push(Candidate { id: wire.id.clone(), article });
            }
            Err(error) => {
                debug!(
                    %error, post_id = %wire.id, room,
                    "bot: could not resolve a chat-room post CID this run; will retry later"
                );
                // Left unresolved: not pushed to `candidates`, so the
                // caller never marks it processed and it's retried later.
            }
        }
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_wire_bytes() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "id": "article-1",
            "title": "Title",
            "excerpt": "Excerpt",
            "body": "Body text",
            "authorDid": "did:key:zAuthor",
            "authorName": "Reporter",
            "createdAt": 1_700_000_000_000i64,
            "sourceLinks": [{ "title": "Source", "url": "https://example.com" }],
            "lang": "ja",
        }))
        .unwrap()
    }

    #[test]
    fn parse_and_filter_accepts_a_well_formed_matching_article() {
        let article = parse_and_filter(&sample_wire_bytes(), "did:key:zAuthor", &["ja".to_string()])
            .expect("well-formed article matching the language filter must parse");
        assert_eq!(article.id, "article-1");
        assert_eq!(article.title, "Title");
        assert_eq!(article.source_links.len(), 1);
        assert_eq!(article.source_links[0].url, "https://example.com");
    }

    #[test]
    fn parse_and_filter_accepts_everything_when_langs_is_empty() {
        assert!(parse_and_filter(&sample_wire_bytes(), "did:key:zAuthor", &[]).is_some());
    }

    #[test]
    fn parse_and_filter_rejects_a_language_mismatch() {
        assert!(parse_and_filter(&sample_wire_bytes(), "did:key:zAuthor", &["en".to_string()]).is_none());
    }

    #[test]
    fn parse_and_filter_rejects_an_author_did_mismatch_with_the_wire() {
        // Mirrors globalArticlesReader.ts's `authorDid !== wire.fromId` guard.
        assert!(parse_and_filter(&sample_wire_bytes(), "did:key:zSomeoneElse", &[]).is_none());
    }

    #[test]
    fn parse_and_filter_rejects_malformed_json() {
        assert!(parse_and_filter(b"not json", "did:key:zAuthor", &[]).is_none());
    }

    #[test]
    fn parse_and_filter_rejects_missing_required_fields() {
        let bytes = serde_json::to_vec(&serde_json::json!({ "id": "a", "title": "t" })).unwrap();
        assert!(parse_and_filter(&bytes, "did:key:zAuthor", &[]).is_none());
    }

    #[test]
    fn effective_rooms_defaults_to_the_well_known_room_when_empty_or_blank() {
        assert_eq!(effective_rooms(&[]), vec![GLOBAL_ARTICLES_ROOM_ID.to_string()]);
        assert_eq!(
            effective_rooms(&["  ".to_string(), "".to_string()]),
            vec![GLOBAL_ARTICLES_ROOM_ID.to_string()]
        );
    }

    #[test]
    fn effective_rooms_trims_and_keeps_explicit_rooms() {
        assert_eq!(
            effective_rooms(&[" tc-news ".to_string(), "private-room".to_string()]),
            vec!["tc-news".to_string(), "private-room".to_string()]
        );
    }

    // -- chat-room source: body -> Article mapping --

    fn sample_chat_wire(id: &str, from_name: &str, timestamp: i64) -> BufferedChatWire {
        BufferedChatWire {
            id: id.to_string(),
            from_id: "did:key:zSender".to_string(),
            from_name: from_name.to_string(),
            cid: "bafy-chat-body".to_string(),
            timestamp,
        }
    }

    #[test]
    fn chat_body_to_article_uses_the_bodys_title_when_present() {
        let bytes =
            serde_json::to_vec(&serde_json::json!({ "title": "Hello", "text": "Hello world" })).unwrap();
        let wire = sample_chat_wire("post-1", "Ada", 1_700_000_000_000);
        let article = chat_body_to_article(&bytes, &wire).expect("well-formed body must map");
        assert_eq!(article.id, "post-1");
        assert_eq!(article.title, "Hello");
        assert_eq!(article.body, "Hello world");
        assert_eq!(article.excerpt, "");
        assert!(article.source_links.is_empty());
        assert_eq!(article.author_name, "Ada");
        assert_eq!(article.created_at, 1_700_000_000_000);
    }

    #[test]
    fn chat_body_to_article_falls_back_to_the_first_line_of_text_when_no_title() {
        let bytes = serde_json::to_vec(&serde_json::json!({ "text": "First line\nSecond line" })).unwrap();
        let wire = sample_chat_wire("post-2", "Ada", 1_700_000_000_000);
        let article = chat_body_to_article(&bytes, &wire).unwrap();
        assert_eq!(article.title, "First line");
        assert_eq!(article.body, "First line\nSecond line");
    }

    #[test]
    fn chat_body_to_article_falls_back_to_first_line_when_title_is_blank() {
        let bytes = serde_json::to_vec(&serde_json::json!({ "title": "   ", "text": "Actual text" })).unwrap();
        let wire = sample_chat_wire("post-3", "Ada", 1_700_000_000_000);
        let article = chat_body_to_article(&bytes, &wire).unwrap();
        assert_eq!(article.title, "Actual text");
    }

    #[test]
    fn chat_body_to_article_truncates_a_long_fallback_title_to_80_chars() {
        let long_line = "x".repeat(200);
        let bytes = serde_json::to_vec(&serde_json::json!({ "text": long_line })).unwrap();
        let wire = sample_chat_wire("post-4", "Ada", 1_700_000_000_000);
        let article = chat_body_to_article(&bytes, &wire).unwrap();
        assert_eq!(article.title.chars().count(), CHAT_TITLE_FALLBACK_MAX_CHARS);
    }

    #[test]
    fn chat_body_to_article_truncates_a_long_fallback_title_correctly_on_multibyte_utf8() {
        // Japanese text is multi-byte in UTF-8; a byte-slice truncation would
        // panic or corrupt this. `first_line_truncated` counts characters.
        let long_line = "あ".repeat(200);
        let bytes = serde_json::to_vec(&serde_json::json!({ "text": long_line })).unwrap();
        let wire = sample_chat_wire("post-5", "Ada", 1_700_000_000_000);
        let article = chat_body_to_article(&bytes, &wire).unwrap();
        assert_eq!(article.title.chars().count(), CHAT_TITLE_FALLBACK_MAX_CHARS);
    }

    #[test]
    fn chat_body_to_article_rejects_empty_or_whitespace_only_text_with_no_title() {
        let wire = sample_chat_wire("post-6", "Ada", 1_700_000_000_000);
        let empty = serde_json::to_vec(&serde_json::json!({ "text": "" })).unwrap();
        assert!(chat_body_to_article(&empty, &wire).is_none());
        let blank = serde_json::to_vec(&serde_json::json!({ "text": "   " })).unwrap();
        assert!(chat_body_to_article(&blank, &wire).is_none());
        let missing = serde_json::to_vec(&serde_json::json!({})).unwrap();
        assert!(chat_body_to_article(&missing, &wire).is_none());
    }

    #[test]
    fn chat_body_to_article_rejects_malformed_json() {
        let wire = sample_chat_wire("post-7", "Ada", 1_700_000_000_000);
        assert!(chat_body_to_article(b"not json", &wire).is_none());
    }

    #[test]
    fn chat_body_to_article_defaults_author_name_to_empty_when_the_wire_omitted_it() {
        let bytes = serde_json::to_vec(&serde_json::json!({ "text": "hi" })).unwrap();
        let wire = sample_chat_wire("post-8", "", 1_700_000_000_000);
        let article = chat_body_to_article(&bytes, &wire).unwrap();
        assert_eq!(article.author_name, "");
    }

    #[test]
    fn first_line_truncated_trims_and_keeps_only_the_first_line() {
        assert_eq!(first_line_truncated("  spaced out  \nsecond line", 80), "spaced out");
        assert_eq!(first_line_truncated("", 80), "");
    }

    #[test]
    fn first_line_truncated_truncates_by_character_count() {
        assert_eq!(first_line_truncated("abcdef", 3), "abc");
        assert_eq!(first_line_truncated("あいうえお", 3), "あいう");
    }

    // -- chat-room source: history-request wire shape --

    #[test]
    fn chat_history_request_wire_matches_tc_chats_history_request_shape() {
        // Mirrors `HistoryRequest` (`useHistorySync.ts:22-26`): {type, id,
        // roomId} -- deliberately no fromId/timestamp/signature, unlike
        // tc-news's history-request wire.
        let wire = build_chat_history_request_wire("room-1");
        assert_eq!(wire["type"], json!(WIRE_CHAT_HISTORY_REQUEST));
        assert_eq!(wire["roomId"], json!("room-1"));
        assert!(wire.get("id").and_then(Value::as_str).is_some_and(|id| !id.is_empty()));
        assert!(wire.get("fromId").is_none());
        assert!(wire.get("timestamp").is_none());
        assert!(wire.get("signature").is_none());
    }

    #[test]
    fn new_history_request_id_is_unique_across_calls() {
        assert_ne!(new_history_request_id(), new_history_request_id());
    }
}
