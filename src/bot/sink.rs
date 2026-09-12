//! Pipeline delivery sinks: `chat-post` (a signed `tc-chat:post` pair into a
//! tc-chat room), `webhook` (a signed JSON `POST` to an arbitrary HTTP
//! endpoint), and `article-publish` (a signed `tc-news:article` announcement
//! into a global-articles-compatible room, storing a `NewsArticle`-shaped
//! body by CID -- the producing mirror of `source`'s `GlobalArticles`
//! consumption, letting a pipeline feed tc-news readers directly, e.g.
//! translate-and-republish or chat-digest -> news). [`deliver`] runs every
//! configured sink independently and never fails -- a sink error is
//! recorded per-sink (`persist::SinkOutcome::{ok,error}`) rather than
//! propagated, per the pipeline flow's "sink失敗は run 全体を FAIL にしない"
//! rule (see `bot::run_pipeline`, which only fails a run on a
//! source/transform error).

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::{Value, json};
use tracing::debug;

use crate::config::{SinkConfig, WebhookHeader};
use crate::daemon::AppState;

use super::persist::{DeliveredItem, SinkOutcome};
use super::source::Article;
use super::transform::{TransformOutcome, extension_for_mime};

/// Runs every sink in `sinks` against `article`/`outcome` and returns the
/// [`DeliveredItem`] record for `bot.items` -- always `Ok`-shaped from the
/// caller's point of view; per-sink failures live in `sinks[].ok`/`error`.
pub(super) async fn deliver(
    state: &Arc<AppState>,
    pipeline_id: &str,
    sinks: &[SinkConfig],
    article: &Article,
    outcome: &TransformOutcome,
) -> DeliveredItem {
    let mut sink_outcomes = Vec::with_capacity(sinks.len());
    for sink in sinks {
        let (kind, target, result) = match sink {
            SinkConfig::ChatPost { room } => {
                let room = room.trim();
                if room.is_empty() {
                    (
                        "chat-post",
                        String::new(),
                        Err(anyhow::anyhow!(
                            "bot.pipelines[id={pipeline_id:?}].sinks[].room is not set; \
                             set it with `mistl config set bot.pipelines <json>`"
                        )),
                    )
                } else {
                    (
                        "chat-post",
                        room.to_string(),
                        chat_post(state, room, article, outcome).await,
                    )
                }
            }
            SinkConfig::Webhook {
                url,
                include_audio,
                max_audio_bytes,
                method,
                body_template,
                sign,
                include_body,
                headers,
            } => {
                let url = url.trim();
                if url.is_empty() {
                    (
                        "webhook",
                        String::new(),
                        Err(anyhow::anyhow!(
                            "bot.pipelines[id={pipeline_id:?}].sinks[].url is not set; \
                             set it with `mistl config set bot.pipelines <json>`"
                        )),
                    )
                } else {
                    (
                        "webhook",
                        url.to_string(),
                        webhook(
                            state,
                            url,
                            *include_audio,
                            *max_audio_bytes,
                            method.as_deref(),
                            body_template.as_deref(),
                            *sign,
                            *include_body,
                            headers,
                            pipeline_id,
                            article,
                            outcome,
                        )
                        .await,
                    )
                }
            }
            SinkConfig::ArticlePublish { room } => {
                let room = room.trim();
                if room.is_empty() {
                    (
                        "article-publish",
                        String::new(),
                        Err(anyhow::anyhow!(
                            "bot.pipelines[id={pipeline_id:?}].sinks[].room is not set; \
                             set it with `mistl config set bot.pipelines <json>`"
                        )),
                    )
                } else {
                    (
                        "article-publish",
                        room.to_string(),
                        article_publish(state, room, article, outcome).await,
                    )
                }
            }
        };
        if let Err(error) = &result {
            debug!(%error, pipeline_id, item_id = %article.id, kind, target, "bot: sink delivery failed");
        }
        sink_outcomes.push(SinkOutcome {
            kind: kind.to_string(),
            target,
            ok: result.is_ok(),
            error: result.err().map(|error| error.to_string()),
        });
    }
    DeliveredItem {
        pipeline_id: pipeline_id.to_string(),
        item_id: article.id.clone(),
        title: article.title.clone(),
        delivered_at: chrono::Utc::now().to_rfc3339(),
        sinks: sink_outcomes,
    }
}

/// `text` body for the `tc-chat:post` `kind:"text"` post: the read-aloud
/// script (or the article excerpt, if no `summarize` step ran) plus the
/// source links -- matches the pipeline flow's "テキスト投稿（タイトル+要約+
/// 元記事リンク）" description. `tc-chat`'s `PostBody` type has no dedicated
/// links field, so they're appended as plain text.
fn build_chat_text(article: &Article, script: Option<&str>) -> String {
    let mut text = script
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| article.excerpt.clone());
    if !article.source_links.is_empty() {
        text.push_str("\n\n出典:\n");
        for link in &article.source_links {
            text.push_str(&format!("- {} ({})\n", link.title, link.url));
        }
    }
    text
}

/// Random-ish wire id: timestamp + random suffix, matching the shape (if
/// not the exact algorithm) of tc-chat's own `newId()` -- any
/// collision-resistant string works, `hydratePost` never re-parses it.
fn new_wire_id() -> String {
    format!(
        "bot-{}-{:016x}",
        chrono::Utc::now().timestamp_millis(),
        rand::random::<u64>()
    )
}

/// Publishes a `tc-chat:post` text post (script + source links), then --
/// when transforms produced audio -- a second `kind:"media"` post
/// referencing the stored audio CID. Both are signed
/// (`crate::wiresign::sign_wire`) and broadcast into `room`.
///
/// Field shapes mirror `tc-chat/src/hooks/usePostStream.ts`'s `PostWire`
/// (read directly for this implementation): `surface:"chat"`,
/// `parentId: null` (both posts are top-level, not threaded to each other),
/// `kind:"text"`/`"media"`, and (media only) `mimeType`/`fileName`/`fileSize`.
/// `fromApp:"mistl"` is an additional field per the bot pipeline draft's
/// principle 7 -- `wireSign` signs every field symmetrically
/// (`stableStringify`, not a fixed enum), so an extra field never desyncs
/// signing from verification, and tc-chat's own `PostWire` type is
/// `Record<string, unknown>` (open to unknown extra fields).
/// Builds the unsigned `kind:"text"` `tc-chat:post` wire -- pure (aside from
/// the caller-supplied `id`/`timestamp`), so directly unit-testable against
/// `PostWire`'s field shape without an `AppState`. [`sign_wire`] is applied
/// by the caller.
fn build_text_post_wire(
    id: &str,
    from_id: &str,
    from_name: &str,
    timestamp: i64,
    cid: &str,
) -> Value {
    json!({
        "type": "tc-chat:post",
        "surface": "chat",
        "id": id,
        "parentId": Value::Null,
        "fromId": from_id,
        "fromName": from_name,
        "timestamp": timestamp,
        "kind": "text",
        "cid": cid,
        "fromApp": "mistl",
    })
}

/// Builds the unsigned `kind:"media"` `tc-chat:post` wire. See
/// [`build_text_post_wire`].
fn build_media_post_wire(
    id: &str,
    from_id: &str,
    from_name: &str,
    timestamp: i64,
    audio: &super::transform::AudioOutcome,
    file_name: &str,
) -> Value {
    json!({
        "type": "tc-chat:post",
        "surface": "chat",
        "id": id,
        "parentId": Value::Null,
        "fromId": from_id,
        "fromName": from_name,
        "timestamp": timestamp,
        "kind": "media",
        "cid": audio.cid,
        "mimeType": audio.mime,
        "fileName": file_name,
        "fileSize": audio.size,
        "fromApp": "mistl",
    })
}

/// `override_value` when `Some` and non-blank, else `fallback` -- the
/// title/body precedence shared by [`chat_post`]'s text-post title and
/// [`build_published_article`]'s title/body (a `translate`/`summarize`
/// transform's output wins over the source article's own text whenever it
/// actually produced something).
fn preferred_text<'a>(override_value: Option<&'a str>, fallback: &'a str) -> &'a str {
    override_value
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(fallback)
}

async fn chat_post(
    state: &Arc<AppState>,
    room: &str,
    article: &Article,
    outcome: &TransformOutcome,
) -> Result<()> {
    let identity = crate::identity::current(state)
        .await
        .context("bot: loading identity")?;
    let store = crate::storage::store(state)
        .await
        .context("bot: opening content store")?;
    let from_name = state
        .config()
        .identity
        .display_name
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "mistl".to_string());

    crate::net::ensure_started(state, room.to_string())
        .await
        .with_context(|| format!("bot: joining chat-post room {room:?}"))?;

    let title = preferred_text(outcome.title.as_deref(), &article.title);
    let body =
        json!({ "title": title, "text": build_chat_text(article, outcome.script.as_deref()) });
    let body_cid = store
        .put(&format!("{}.json", article.id), serde_json::to_vec(&body)?)
        .await
        .context("bot: storing chat-post text body")?;

    let mut text_wire = build_text_post_wire(
        &new_wire_id(),
        identity.did(),
        &from_name,
        chrono::Utc::now().timestamp_millis(),
        &body_cid,
    );
    crate::wiresign::sign_wire(&mut text_wire, &identity)?;
    crate::net::send_broadcast(room, serde_json::to_vec(&text_wire)?)
        .await
        .context("bot: broadcasting chat-post text wire")?;

    if let Some(audio) = &outcome.audio {
        let file_name = format!("{}.{}", article.id, extension_for_mime(&audio.mime));
        let mut media_wire = build_media_post_wire(
            &new_wire_id(),
            identity.did(),
            &from_name,
            chrono::Utc::now().timestamp_millis(),
            audio,
            &file_name,
        );
        crate::wiresign::sign_wire(&mut media_wire, &identity)?;
        crate::net::send_broadcast(room, serde_json::to_vec(&media_wire)?)
            .await
            .context("bot: broadcasting chat-post media wire")?;
    }
    Ok(())
}

/// `POST`s a signed `tc-bot:delivery` notice to `url`. The JSON body is
/// itself a `wireSign`-compatible signed object (`fromId` + `signature`
/// fields via `crate::wiresign::sign_wire`) -- confirmed against the
/// `WIRE_WEBHOOK_DELIVERY_WIRE` interop test vector already present in
/// `src/wiresign.rs` from Wave 1, which anticipated exactly this shape.
/// `X-Mistl-Signature` carries that same embedded signature (not a second,
/// separately-computed signature over the raw body bytes) -- a receiver can
/// verify either from the header alone or by re-canonicalizing the parsed
/// body with ordinary `wireSign.ts`-compatible tooling.
/// Builds the `item` object (minus `audio.b64`, which needs a network
/// fetch -- see [`webhook`]) for a `tc-bot:delivery` webhook body. Pure, so
/// it's directly unit-testable against the `WIRE_WEBHOOK_DELIVERY_WIRE`
/// shape without an `AppState`.
///
/// `sourceLinks` is a plain array of URLs (not the richer `{title,url}`
/// objects `tc-news`'s own `NewsArticle.sourceLinks` carries, which the
/// chat-post sink's text body preserves in full via [`build_chat_text`]) --
/// matches the `WIRE_WEBHOOK_DELIVERY_WIRE` interop test vector already
/// present in `src/wiresign.rs` from Wave 1.
fn build_webhook_item(article: &Article, outcome: &TransformOutcome) -> Value {
    let source_links: Vec<&str> = article
        .source_links
        .iter()
        .map(|link| link.url.as_str())
        .collect();
    let mut item = json!({
        "articleId": article.id,
        "title": article.title,
        "excerpt": article.excerpt,
        "sourceLinks": source_links,
        "publishedAt": chrono::DateTime::from_timestamp_millis(article.created_at)
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
    });
    if let Some(audio) = &outcome.audio {
        item["audio"] = json!({ "mime": audio.mime, "size": audio.size });
    }
    item
}

/// Builds the unsigned `tc-bot:delivery` body (`{v, type, fromId, pipeline,
/// item}`) -- pure, so it's directly unit-testable. [`sign_wire`] is applied
/// by the caller ([`webhook`]), since signing needs the real `Identity`.
fn build_webhook_body(pipeline_id: &str, from_id: &str, item: Value) -> Value {
    json!({
        "v": 1,
        "type": "tc-bot:delivery",
        "fromId": from_id,
        "pipeline": pipeline_id,
        "item": item,
    })
}

/// [`build_webhook_item`] plus, when `include_body` is set, an `item.body`
/// field carrying `article.body` verbatim -- shared by [`webhook`]'s default
/// body and [`render_webhook_template`]'s `{{item_json}}` variable, so both
/// reflect `include_body` identically.
fn build_webhook_item_with_body(
    article: &Article,
    outcome: &TransformOutcome,
    include_body: bool,
) -> Value {
    let mut item = build_webhook_item(article, outcome);
    if include_body {
        item["body"] = json!(article.body);
    }
    item
}

/// Strips the surrounding quotes from `serde_json`'s string encoding of
/// `value`, giving the JSON-escaped *content* of a string (quotes, newlines,
/// backslashes, etc. all escaped) without the quote delimiters -- used by
/// [`render_webhook_template`] so a template like `{"content":"{{title}}"}`
/// stays valid JSON even when `title` itself contains a `"` or `\n`.
fn json_escape(value: &str) -> String {
    let quoted = serde_json::to_string(value).unwrap_or_default();
    // `serde_json::to_string` on a `&str` always yields a quoted JSON
    // string (at minimum `""`), so stripping the first/last byte is safe.
    quoted
        .get(1..quoted.len().saturating_sub(1))
        .unwrap_or_default()
        .to_string()
}

/// Renders a `SinkConfig::Webhook.body_template` by substituting `{{var}}`
/// tokens -- pure, so it's directly unit-testable. Every variable except
/// `{{item_json}}` is inserted via [`json_escape`] (escaped, no surrounding
/// quotes), so it's safe to drop directly inside a JSON string literal in
/// the template; `{{item_json}}` is inserted raw, since it's already a full
/// JSON value on its own. Unknown `{{...}}` tokens are left untouched
/// verbatim. `include_body` mirrors the same flag used for the default body
/// (via [`build_webhook_item_with_body`]) so `{{item_json}}` reflects it too;
/// `include_audio`/`max_audio_bytes` have no template equivalent -- template
/// mode never embeds audio (see `SinkConfig::Webhook`'s doc comment).
fn render_webhook_template(
    template: &str,
    article: &Article,
    outcome: &TransformOutcome,
    pipeline_id: &str,
    from_id: &str,
    include_body: bool,
) -> String {
    let title = preferred_text(outcome.title.as_deref(), &article.title);
    let published_at = chrono::DateTime::from_timestamp_millis(article.created_at)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default();
    let link = article
        .source_links
        .first()
        .map(|link| link.url.as_str())
        .unwrap_or("");
    let links: String = article
        .source_links
        .iter()
        .map(|link| link.url.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let item_json = serde_json::to_string(&build_webhook_item_with_body(
        article,
        outcome,
        include_body,
    ))
    .unwrap_or_default();

    let mut result = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        result.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            // No closing `}}` -- keep the rest of the template verbatim.
            result.push_str("{{");
            result.push_str(after);
            rest = "";
            break;
        };
        let name = after[..end].trim();
        let replacement = match name {
            "id" => Some(json_escape(&article.id)),
            "title" => Some(json_escape(title)),
            "excerpt" => Some(json_escape(&article.excerpt)),
            "body" => Some(json_escape(&article.body)),
            "author" => Some(json_escape(&article.author_name)),
            "lang" => Some(json_escape(outcome.lang.as_deref().unwrap_or(""))),
            "pipeline" => Some(json_escape(pipeline_id)),
            "from_id" => Some(json_escape(from_id)),
            "published_at" => Some(json_escape(&published_at)),
            "link" => Some(json_escape(link)),
            "links" => Some(json_escape(&links)),
            "item_json" => Some(item_json.clone()),
            _ => None,
        };
        match replacement {
            Some(value) => {
                result.push_str(&value);
                rest = &after[end + 2..];
            }
            None => {
                // Unknown token: leave the original `{{...}}` untouched.
                result.push_str("{{");
                result.push_str(&after[..end + 2]);
                rest = &after[end + 2..];
            }
        }
    }
    result.push_str(rest);
    result
}

/// Resolves `SinkConfig::Webhook.method` to a `reqwest::Method`:
/// case-insensitive `post`/`put`/`patch`, defaulting to (and treating any
/// unrecognized value as) `POST` -- the "not one of post/put/patch" case is
/// flagged as a config validation warning by `bot::validate_pipeline`, not
/// re-checked/logged here.
fn parse_webhook_method(method: Option<&str>) -> reqwest::Method {
    match method.map(str::trim) {
        Some(m) if m.eq_ignore_ascii_case("put") => reqwest::Method::PUT,
        Some(m) if m.eq_ignore_ascii_case("patch") => reqwest::Method::PATCH,
        _ => reqwest::Method::POST,
    }
}

#[allow(clippy::too_many_arguments)]
async fn webhook(
    state: &Arc<AppState>,
    url: &str,
    include_audio: bool,
    max_audio_bytes: Option<u64>,
    method: Option<&str>,
    body_template: Option<&str>,
    sign: bool,
    include_body: bool,
    headers: &[WebhookHeader],
    pipeline_id: &str,
    article: &Article,
    outcome: &TransformOutcome,
) -> Result<()> {
    let identity = crate::identity::current(state)
        .await
        .context("bot: loading identity")?;

    let template = body_template
        .map(str::trim)
        .filter(|template| !template.is_empty());
    let (body_bytes, signature): (Vec<u8>, Option<String>) = if let Some(template) = template {
        // Template mode: never wiresigned, and `include_audio` has no
        // effect (see `SinkConfig::Webhook`'s doc comment).
        let rendered = render_webhook_template(
            template,
            article,
            outcome,
            pipeline_id,
            identity.did(),
            include_body,
        );
        (rendered.into_bytes(), None)
    } else {
        let mut item = build_webhook_item_with_body(article, outcome, include_body);
        if let Some(audio) = &outcome.audio
            && include_audio
            && max_audio_bytes.is_none_or(|max| audio.size <= max)
        {
            let store = crate::storage::store(state)
                .await
                .context("bot: opening content store")?;
            let bytes = store
                .get(&audio.cid)
                .await
                .context("bot: resolving audio for webhook inline delivery")?;
            item["audio"]["b64"] = json!(BASE64_STANDARD.encode(bytes));
        }

        let mut body = build_webhook_body(pipeline_id, identity.did(), item);
        let signature = if sign {
            crate::wiresign::sign_wire(&mut body, &identity)?;
            Some(
                body.get("signature")
                    .and_then(Value::as_str)
                    .context("bot: sign_wire did not produce a signature")?
                    .to_string(),
            )
        } else {
            None
        };
        (serde_json::to_vec(&body)?, signature)
    };

    let http_method = parse_webhook_method(method);
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .context("bot: building webhook HTTP client")?;
    let mut request = client
        .request(http_method.clone(), url)
        .header("Content-Type", "application/json")
        .header("X-Mistl-Did", identity.did());
    if let Some(signature) = signature {
        request = request.header("X-Mistl-Signature", signature);
    }
    for header in headers {
        let name = header.name.trim();
        if name.is_empty() {
            continue;
        }
        request = request.header(name, header.value.as_str());
    }

    let response = request
        .body(body_bytes)
        .send()
        .await
        .with_context(|| format!("bot: webhook {http_method} failed: {url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let truncated: String = text.chars().take(500).collect();
        bail!("bot: webhook {http_method} to {url} returned an error ({status}): {truncated}");
    }
    Ok(())
}

/// First ~120 chars of `body`, newlines collapsed to spaces, char-boundary
/// safe (`.chars().take(..)`, never a byte slice) -- used when publishing a
/// [`build_published_article`] body, which (unlike [`Article::excerpt`],
/// carried through unchanged from the source) must be re-derived from
/// whichever body text (`outcome.script` or `article.body`) actually ends up
/// published.
const PUBLISHED_EXCERPT_CHAR_LIMIT: usize = 120;

fn build_published_excerpt(body: &str) -> String {
    let single_line: String = body
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    single_line
        .chars()
        .take(PUBLISHED_EXCERPT_CHAR_LIMIT)
        .collect()
}

/// Builds the `NewsArticle`-shaped JSON body (`tc-news/src/types.ts:28-44`)
/// for the `article-publish` sink -- pure, so it's directly unit-testable
/// against the consuming side's expectations
/// (`source.rs`'s `NewsArticleJson`/`parse_and_filter`) without an
/// `AppState`. [`store.put`] is applied by the caller ([`article_publish`]).
///
/// - `title`/`body` honor `outcome.title`/`outcome.script` when set and
///   non-blank, else fall back to `article.title`/`article.body` -- same
///   precedence as [`chat_post`]'s text-post title.
/// - `excerpt` is re-derived from whichever body text was chosen (see
///   [`build_published_excerpt`]), not copied from `article.excerpt`, since
///   a `translate`/`summarize` transform may have replaced the body text
///   the excerpt should represent.
/// - `authorDid` is this bot's DID and MUST equal the announce wire's
///   `fromId` ([`build_article_wire`]) -- mirrors
///   `globalArticlesReader.ts::hydrate`'s `authorDid !== wire.fromId`
///   discard, applied on the receiving end by `source.rs`'s
///   `parse_and_filter`.
/// - `sourceLinks` are `{title, url}` objects (`tc-news/src/types.ts`'s
///   `SourceLink`), matching `NewsArticle.sourceLinks` exactly -- unlike the
///   `webhook` sink's [`build_webhook_item`], which flattens to plain URLs.
/// - `lang` is included only when `outcome.lang` is `Some` (omitted
///   entirely, not `null`, when unset) -- `NewsArticle.lang` is `lang?:
///   string` (`types.ts:42`), and `sanitizeArticle` in `newsWire.ts` treats
///   anything but a non-empty string as "unset" (line 164), so an omitted
///   field and an absent one read identically on the consuming end.
/// - No `fromApp` field: `NewsArticle` (`types.ts:28-44`) has no such field
///   -- `fromApp` only exists on the *wire* envelope
///   (`newsWire.ts:38`/[`build_article_wire`]), not the CID-addressed body.
fn build_published_article(
    id: &str,
    article: &Article,
    outcome: &TransformOutcome,
    author_did: &str,
    author_name: &str,
    created_at: i64,
) -> Value {
    let title = preferred_text(outcome.title.as_deref(), &article.title);
    let body = preferred_text(outcome.script.as_deref(), &article.body);
    let source_links: Vec<Value> = article
        .source_links
        .iter()
        .map(|link| json!({ "title": link.title, "url": link.url }))
        .collect();
    let mut value = json!({
        "id": id,
        "title": title,
        "body": body,
        "excerpt": build_published_excerpt(body),
        "authorDid": author_did,
        "authorName": author_name,
        "createdAt": created_at,
        "sourceLinks": source_links,
    });
    if let Some(lang) = &outcome.lang {
        value["lang"] = json!(lang);
    }
    value
}

/// Builds the unsigned `tc-news:article` announce wire
/// (`tc-news/src/lib/newsWire.ts:25-39`'s `ArticleWire`). [`sign_wire`] is
/// applied by the caller ([`article_publish`]). `fromApp:"mistl"` is
/// explicitly optional on the reading side (`isArticleWire`,
/// `newsWire.ts:271`) and, per the same symmetric-signing reasoning as
/// [`build_text_post_wire`], never desyncs signing from verification.
fn build_article_wire(
    id: &str,
    from_id: &str,
    from_name: &str,
    timestamp: i64,
    cid: &str,
) -> Value {
    json!({
        "type": "tc-news:article",
        "id": id,
        "fromId": from_id,
        "fromName": from_name,
        "timestamp": timestamp,
        "cid": cid,
        "fromApp": "mistl",
    })
}

/// Publishes the item as a signed `tc-news:article` wire into a
/// global-articles-compatible `room`: stores a `NewsArticle`-shaped JSON
/// body by CID (title/body honoring `outcome.title`/`outcome.script`/
/// `outcome.lang` overrides, `authorDid` = this bot's DID so readers'
/// `authorDid !== wire.fromId` check passes), then broadcasts the announce
/// wire -- the producing mirror of `source`'s `tc-news:article` consumption.
/// Same `id` is used for both the body's `NewsArticle.id` and the wire's
/// `id` (`newsWire.ts:27`'s `id: string; // article.id`).
async fn article_publish(
    state: &Arc<AppState>,
    room: &str,
    article: &Article,
    outcome: &TransformOutcome,
) -> Result<()> {
    let identity = crate::identity::current(state)
        .await
        .context("bot: loading identity")?;
    let store = crate::storage::store(state)
        .await
        .context("bot: opening content store")?;
    let from_name = state
        .config()
        .identity
        .display_name
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "mistl".to_string());

    crate::net::ensure_started(state, room.to_string())
        .await
        .with_context(|| format!("bot: joining article-publish room {room:?}"))?;

    let id = new_wire_id();
    let created_at = chrono::Utc::now().timestamp_millis();
    let body = build_published_article(
        &id,
        article,
        outcome,
        identity.did(),
        &from_name,
        created_at,
    );
    let body_cid = store
        .put(&format!("{id}.json"), serde_json::to_vec(&body)?)
        .await
        .context("bot: storing published article body")?;

    let mut wire = build_article_wire(&id, identity.did(), &from_name, created_at, &body_cid);
    crate::wiresign::sign_wire(&mut wire, &identity)?;
    crate::net::send_broadcast(room, serde_json::to_vec(&wire)?)
        .await
        .context("bot: broadcasting article-publish wire")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_article() -> Article {
        Article {
            id: "article-1".to_string(),
            title: "見出し".to_string(),
            excerpt: "要約文".to_string(),
            body: "本文".to_string(),
            source_links: vec![super::super::source::SourceLink {
                title: "元記事".to_string(),
                url: "https://example.com/a".to_string(),
            }],
            author_name: "記者".to_string(),
            created_at: 1_700_000_000_000,
        }
    }

    #[test]
    fn build_chat_text_prefers_the_script_and_appends_source_links() {
        let text = build_chat_text(&sample_article(), Some("台本テキスト"));
        assert!(text.starts_with("台本テキスト"));
        assert!(text.contains("出典:"));
        assert!(text.contains("元記事 (https://example.com/a)"));
    }

    #[test]
    fn build_chat_text_falls_back_to_excerpt_when_no_script() {
        let text = build_chat_text(&sample_article(), None);
        assert!(text.starts_with("要約文"));
    }

    #[test]
    fn new_wire_id_has_the_expected_prefix_and_is_unique_across_calls() {
        let a = new_wire_id();
        let b = new_wire_id();
        assert_ne!(a, b);
        assert!(a.starts_with("bot-"));
    }

    // -- wire assembly + stable_stringify signature verification
    //    (brief item: "ワイヤ組み立て（PostWire/webhookボディのstable_
    //    stringify署名検証）") --

    #[test]
    fn text_post_wire_has_the_post_wire_shape_and_signs_and_verifies() {
        let identity = crate::identity::for_test();
        let mut wire = build_text_post_wire(
            "wire-1",
            identity.did(),
            "mistl",
            1_700_000_000_000,
            "bafy-body",
        );
        assert_eq!(wire["type"], json!("tc-chat:post"));
        assert_eq!(wire["surface"], json!("chat"));
        assert_eq!(
            wire["parentId"],
            Value::Null,
            "parentId must be explicit null, not omitted"
        );
        assert_eq!(wire["kind"], json!("text"));
        assert_eq!(wire["fromApp"], json!("mistl"));

        crate::wiresign::sign_wire(&mut wire, &identity).expect("a well-formed wire must sign");
        assert!(crate::wiresign::verify_wire(&wire).unwrap());

        let mut tampered = wire.clone();
        tampered["cid"] = json!("bafy-different");
        assert!(!crate::wiresign::verify_wire(&tampered).unwrap());
    }

    #[test]
    fn media_post_wire_carries_mime_filename_and_filesize_and_signs() {
        let identity = crate::identity::for_test();
        let audio = super::super::transform::AudioOutcome {
            cid: "bafy-audio".to_string(),
            mime: "audio/mpeg".to_string(),
            size: 12345,
        };
        let mut wire = build_media_post_wire(
            "wire-2",
            identity.did(),
            "mistl",
            1_700_000_000_000,
            &audio,
            "article-1.mp3",
        );
        assert_eq!(wire["kind"], json!("media"));
        assert_eq!(wire["cid"], json!("bafy-audio"));
        assert_eq!(wire["mimeType"], json!("audio/mpeg"));
        assert_eq!(wire["fileName"], json!("article-1.mp3"));
        assert_eq!(wire["fileSize"], json!(12345));

        crate::wiresign::sign_wire(&mut wire, &identity).unwrap();
        assert!(crate::wiresign::verify_wire(&wire).unwrap());
    }

    #[test]
    fn webhook_item_uses_a_plain_source_link_url_array() {
        // Matches the `WIRE_WEBHOOK_DELIVERY_WIRE` interop vector in
        // `src/wiresign.rs` (Wave 1): `sourceLinks` there is
        // `["https://example.com/src1"]`, plain strings, not `{title,url}`
        // objects.
        let item = build_webhook_item(&sample_article(), &TransformOutcome::default());
        assert_eq!(item["sourceLinks"], json!(["https://example.com/a"]));
        assert_eq!(item["articleId"], json!("article-1"));
        assert!(
            item.get("audio").is_none(),
            "no audio outcome -> no audio field"
        );
    }

    #[test]
    fn webhook_item_includes_audio_meta_without_b64_by_default() {
        let mut outcome = TransformOutcome::default();
        outcome.audio = Some(super::super::transform::AudioOutcome {
            cid: "bafy-audio".to_string(),
            mime: "audio/mpeg".to_string(),
            size: 12345,
        });
        let item = build_webhook_item(&sample_article(), &outcome);
        assert_eq!(item["audio"]["mime"], json!("audio/mpeg"));
        assert_eq!(item["audio"]["size"], json!(12345));
        assert!(
            item["audio"].get("b64").is_none(),
            "b64 is only added by the async include_audio path"
        );
    }

    #[test]
    fn webhook_body_signs_and_verifies_end_to_end() {
        let identity = crate::identity::for_test();
        let item = build_webhook_item(&sample_article(), &TransformOutcome::default());
        let mut body = build_webhook_body("news-audio", identity.did(), item);
        assert_eq!(body["v"], json!(1));
        assert_eq!(body["type"], json!("tc-bot:delivery"));
        assert_eq!(body["pipeline"], json!("news-audio"));

        crate::wiresign::sign_wire(&mut body, &identity)
            .expect("a well-formed webhook body must sign");
        assert!(crate::wiresign::verify_wire(&body).unwrap());

        let mut tampered = body.clone();
        tampered["item"]["title"] = json!("different title");
        assert!(!crate::wiresign::verify_wire(&tampered).unwrap());
    }

    // -- flexible webhook sink: include_body / template mode / method parsing --

    #[test]
    fn build_webhook_item_with_body_is_byte_identical_to_the_legacy_item_when_include_body_is_false()
     {
        // Pins the WIRE_WEBHOOK_DELIVERY_WIRE-shaped default payload: with
        // `include_body` unset (false), the item must be exactly what
        // `build_webhook_item` alone produces.
        let article = sample_article();
        let outcome = TransformOutcome::default();
        let legacy = build_webhook_item(&article, &outcome);
        let with_flag_off = build_webhook_item_with_body(&article, &outcome, false);
        assert_eq!(legacy, with_flag_off);
        assert!(with_flag_off.get("body").is_none());
    }

    #[test]
    fn build_webhook_item_with_body_adds_the_article_body_when_true() {
        let item =
            build_webhook_item_with_body(&sample_article(), &TransformOutcome::default(), true);
        assert_eq!(item["body"], json!("本文"));
        // Everything else must still be present, unchanged.
        assert_eq!(item["articleId"], json!("article-1"));
    }

    #[test]
    fn webhook_body_with_include_body_still_signs_and_verifies() {
        let identity = crate::identity::for_test();
        let item =
            build_webhook_item_with_body(&sample_article(), &TransformOutcome::default(), true);
        let mut body = build_webhook_body("news-audio", identity.did(), item);
        assert_eq!(body["item"]["body"], json!("本文"));

        crate::wiresign::sign_wire(&mut body, &identity)
            .expect("a well-formed webhook body must sign");
        assert!(crate::wiresign::verify_wire(&body).unwrap());

        let mut tampered = body.clone();
        tampered["item"]["body"] = json!("tampered body");
        assert!(!crate::wiresign::verify_wire(&tampered).unwrap());
    }

    #[test]
    fn render_webhook_template_escapes_quotes_and_newlines_in_substituted_values() {
        let mut article = sample_article();
        article.title = "見出し \"引用\" と\n改行".to_string();
        let rendered = render_webhook_template(
            r#"{"title":"{{title}}","pipeline":"{{pipeline}}"}"#,
            &article,
            &TransformOutcome::default(),
            "news-audio",
            "did:key:zBot",
            false,
        );
        // The rendered text must itself be valid JSON, and round-trip the
        // *unescaped* original value.
        let parsed: Value =
            serde_json::from_str(&rendered).expect("rendered template must be valid JSON");
        assert_eq!(parsed["title"], json!("見出し \"引用\" と\n改行"));
        assert_eq!(parsed["pipeline"], json!("news-audio"));
    }

    #[test]
    fn render_webhook_template_title_prefers_outcome_title_over_article_title() {
        let mut outcome = TransformOutcome::default();
        outcome.title = Some("翻訳タイトル".to_string());
        let rendered = render_webhook_template(
            "{{title}}",
            &sample_article(),
            &outcome,
            "news-audio",
            "did:key:zBot",
            false,
        );
        assert_eq!(rendered, "翻訳タイトル");
    }

    #[test]
    fn render_webhook_template_item_json_is_inserted_raw_and_reflects_include_body() {
        let rendered = render_webhook_template(
            r#"{"item":{{item_json}}}"#,
            &sample_article(),
            &TransformOutcome::default(),
            "news-audio",
            "did:key:zBot",
            true,
        );
        let parsed: Value =
            serde_json::from_str(&rendered).expect("rendered template must be valid JSON");
        assert_eq!(parsed["item"]["articleId"], json!("article-1"));
        assert_eq!(
            parsed["item"]["body"],
            json!("本文"),
            "item_json must reflect include_body"
        );
    }

    #[test]
    fn render_webhook_template_leaves_unknown_tokens_untouched() {
        let rendered = render_webhook_template(
            "hello {{whatever}} world",
            &sample_article(),
            &TransformOutcome::default(),
            "news-audio",
            "did:key:zBot",
            false,
        );
        assert_eq!(rendered, "hello {{whatever}} world");
    }

    #[test]
    fn render_webhook_template_link_and_links_handle_zero_and_multiple_source_links() {
        let mut no_links = sample_article();
        no_links.source_links.clear();
        let rendered = render_webhook_template(
            "link=[{{link}}] links=[{{links}}]",
            &no_links,
            &TransformOutcome::default(),
            "news-audio",
            "did:key:zBot",
            false,
        );
        assert_eq!(rendered, "link=[] links=[]");

        let mut two_links = sample_article();
        two_links
            .source_links
            .push(super::super::source::SourceLink {
                title: "別記事".to_string(),
                url: "https://example.com/b".to_string(),
            });
        let rendered = render_webhook_template(
            "link=[{{link}}] links=[{{links}}]",
            &two_links,
            &TransformOutcome::default(),
            "news-audio",
            "did:key:zBot",
            false,
        );
        assert_eq!(
            rendered,
            "link=[https://example.com/a] links=[https://example.com/a\\nhttps://example.com/b]"
        );
    }

    #[test]
    fn parse_webhook_method_accepts_known_methods_case_insensitively() {
        assert_eq!(parse_webhook_method(Some("post")), reqwest::Method::POST);
        assert_eq!(parse_webhook_method(Some("POST")), reqwest::Method::POST);
        assert_eq!(parse_webhook_method(Some("PUT")), reqwest::Method::PUT);
        assert_eq!(parse_webhook_method(Some("put")), reqwest::Method::PUT);
        assert_eq!(parse_webhook_method(Some("PATCH")), reqwest::Method::PATCH);
        assert_eq!(parse_webhook_method(Some("patch")), reqwest::Method::PATCH);
    }

    #[test]
    fn parse_webhook_method_defaults_to_post_for_unset_or_bogus_values() {
        assert_eq!(parse_webhook_method(None), reqwest::Method::POST);
        assert_eq!(parse_webhook_method(Some("DELETE")), reqwest::Method::POST);
        assert_eq!(parse_webhook_method(Some("bogus")), reqwest::Method::POST);
    }

    // -- article-publish sink: published body / announce wire --

    #[test]
    fn preferred_text_prefers_a_non_blank_override_and_falls_back_otherwise() {
        assert_eq!(preferred_text(Some("override"), "fallback"), "override");
        assert_eq!(
            preferred_text(Some("   "), "fallback"),
            "fallback",
            "blank override must fall back"
        );
        assert_eq!(preferred_text(None, "fallback"), "fallback");
    }

    #[test]
    fn build_published_excerpt_collapses_newlines_and_truncates_at_a_char_boundary() {
        let body = "見出し\n本文が続きます。".to_string() + &"あ".repeat(200);
        let excerpt = build_published_excerpt(&body);
        assert!(
            !excerpt.contains('\n'),
            "newlines must be collapsed to spaces"
        );
        assert_eq!(
            excerpt.chars().count(),
            PUBLISHED_EXCERPT_CHAR_LIMIT,
            "must truncate to the char limit, not byte-truncate a multibyte string"
        );
    }

    #[test]
    fn build_published_article_uses_the_source_article_when_no_transform_override_is_set() {
        let value = build_published_article(
            "article-pub-1",
            &sample_article(),
            &TransformOutcome::default(),
            "did:key:zBot",
            "mistl-bot",
            1_700_000_000_000,
        );
        assert_eq!(value["id"], json!("article-pub-1"));
        assert_eq!(value["title"], json!("見出し"));
        assert_eq!(value["body"], json!("本文"));
        assert_eq!(value["excerpt"], json!("本文"));
        assert_eq!(value["authorDid"], json!("did:key:zBot"));
        assert_eq!(value["authorName"], json!("mistl-bot"));
        assert_eq!(value["createdAt"], json!(1_700_000_000_000i64));
        assert_eq!(
            value["sourceLinks"],
            json!([{ "title": "元記事", "url": "https://example.com/a" }])
        );
        assert!(
            value.get("lang").is_none(),
            "lang must be omitted entirely when the transform never set one"
        );
    }

    #[test]
    fn build_published_article_prefers_transform_overrides_when_non_blank() {
        let mut outcome = TransformOutcome::default();
        outcome.title = Some("翻訳タイトル".to_string());
        outcome.script = Some("翻訳本文".to_string());
        outcome.lang = Some("en".to_string());
        let value = build_published_article(
            "article-pub-2",
            &sample_article(),
            &outcome,
            "did:key:zBot",
            "mistl-bot",
            0,
        );
        assert_eq!(value["title"], json!("翻訳タイトル"));
        assert_eq!(value["body"], json!("翻訳本文"));
        assert_eq!(value["excerpt"], json!("翻訳本文"));
        assert_eq!(value["lang"], json!("en"));
    }

    #[test]
    fn build_published_article_falls_back_when_transform_overrides_are_blank() {
        let mut outcome = TransformOutcome::default();
        outcome.title = Some("   ".to_string());
        outcome.script = Some("".to_string());
        let value = build_published_article(
            "article-pub-3",
            &sample_article(),
            &outcome,
            "did:key:zBot",
            "mistl-bot",
            0,
        );
        assert_eq!(
            value["title"],
            json!("見出し"),
            "blank title override must fall back to the article's title"
        );
        assert_eq!(
            value["body"],
            json!("本文"),
            "blank script override must fall back to the article's body"
        );
    }

    #[test]
    fn build_published_article_source_links_are_title_url_objects_not_plain_urls() {
        // Unlike `build_webhook_item`'s flattened `sourceLinks`, this must
        // match `tc-news/src/types.ts`'s `NewsArticle.sourceLinks:
        // SourceLink[]` exactly, since a real tc-news reader deserializes
        // this body directly.
        let value = build_published_article(
            "article-pub-4",
            &sample_article(),
            &TransformOutcome::default(),
            "did:key:zBot",
            "mistl-bot",
            0,
        );
        let links = value["sourceLinks"]
            .as_array()
            .expect("sourceLinks must be an array");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["title"], json!("元記事"));
        assert_eq!(links[0]["url"], json!("https://example.com/a"));
    }

    #[test]
    fn build_published_article_round_trips_through_the_consuming_sides_news_article_json_shape() {
        // Mirrors `source.rs`'s private `NewsArticleJson` field expectations
        // (can't call it directly -- it's private -- so this asserts the
        // same field names/types a real consumer would deserialize).
        let value = build_published_article(
            "article-pub-5",
            &sample_article(),
            &TransformOutcome::default(),
            "did:key:zBot",
            "mistl-bot",
            1_700_000_000_000,
        );
        assert!(value["id"].is_string());
        assert!(value["title"].is_string());
        assert!(value["excerpt"].is_string());
        assert!(value["body"].is_string());
        assert_eq!(
            value["authorDid"],
            json!("did:key:zBot"),
            "authorDid must equal the announce wire's fromId"
        );
        assert!(value["authorName"].is_string());
        assert!(value["createdAt"].is_i64());
        assert!(value["sourceLinks"].is_array());
    }

    #[test]
    fn article_wire_has_the_article_wire_shape_and_signs_and_verifies() {
        let identity = crate::identity::for_test();
        let mut wire = build_article_wire(
            "article-pub-1",
            identity.did(),
            "mistl",
            1_700_000_000_000,
            "bafy-body",
        );
        assert_eq!(wire["type"], json!("tc-news:article"));
        assert_eq!(wire["id"], json!("article-pub-1"));
        assert_eq!(wire["fromId"], json!(identity.did()));
        assert_eq!(wire["fromName"], json!("mistl"));
        assert_eq!(wire["cid"], json!("bafy-body"));
        assert_eq!(wire["fromApp"], json!("mistl"));

        crate::wiresign::sign_wire(&mut wire, &identity).expect("a well-formed wire must sign");
        assert!(crate::wiresign::verify_wire(&wire).unwrap());

        let mut tampered = wire.clone();
        tampered["cid"] = json!("bafy-different");
        assert!(!crate::wiresign::verify_wire(&tampered).unwrap());
    }
}
