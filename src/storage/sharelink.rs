//! Parsing for `tc-share` links, compatible with the web app's
//! `src/share/shareLinks.ts` (`readShareLink`) and the Go reference at
//! `tools/storage-cli/internal/protocol/share_link.go` (`ParseShareLink`).
//!
//! A share link embeds a base64url (no padding, `RawURLEncoding` in Go /
//! `toBase64Url` in the web app) JSON payload as the `tc-share` value of a
//! URL fragment query string, e.g.
//! `https://app.example/#tc-share=<base64url-json>`. [`parse_share_link`]
//! accepts that full URL, a bare fragment (`#tc-share=...`), or just the raw
//! `tc-share=...` token, decodes and validates the payload, and returns a
//! [`LinkedShare`].
//!
//! This is a small, dependency-light reimplementation of URL fragment /
//! query parsing (see [`extract_token`] and [`find_query_param`]) rather
//! than pulling in the `url` crate, since all we need is one `key=value`
//! pair out of an `application/x-www-form-urlencoded` string.

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// A sender's display profile, embedded in a share link so the recipient can
/// show "who shared this" before the room handshake completes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShareProfile {
    pub name: String,
}

/// The decrypted/decoded and validated contents of a `tc-share` link, minus
/// the symmetric key (kept separate on [`LinkedShare::key`], as it must
/// never be persisted alongside the rest of the share record).
#[derive(Debug, Clone)]
pub struct ParsedShare {
    /// `"folder-share"` or `"file-share"` (validated by [`parse_share_link`]).
    pub type_: String,
    pub room_id: String,
    pub clock: Option<i64>,
    pub cid: Option<String>,
    pub folder_id: Option<String>,
    pub folder_name: Option<String>,
    pub file_id: Option<String>,
    pub file_name: Option<String>,
    pub owner_node_id: Option<String>,
    pub access_grant_mode: Option<String>,
    pub folder_key_hash: Option<String>,
    pub sender_profile: Option<ShareProfile>,
}

/// The result of successfully parsing a `tc-share` link: the share metadata
/// plus (for file shares) the symmetric decryption key.
#[derive(Debug, Clone)]
pub struct LinkedShare {
    pub key: Option<String>,
    pub share: ParsedShare,
}

/// Wire payload embedded (as base64url JSON) in a `tc-share` link.
///
/// Field names mirror the Go `shareLinkPayload` / TS `ShareLinkPayload`
/// exactly; this type is private, [`ParsedShare`] is the public shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ShareLinkPayload {
    v: i32,
    #[serde(rename = "type")]
    type_: String,
    #[serde(rename = "roomId")]
    room_id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    clock: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    cid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    key: Option<String>,
    #[serde(rename = "folderId", skip_serializing_if = "Option::is_none", default)]
    folder_id: Option<String>,
    #[serde(
        rename = "folderName",
        skip_serializing_if = "Option::is_none",
        default
    )]
    folder_name: Option<String>,
    #[serde(rename = "fileId", skip_serializing_if = "Option::is_none", default)]
    file_id: Option<String>,
    #[serde(rename = "fileName", skip_serializing_if = "Option::is_none", default)]
    file_name: Option<String>,
    #[serde(
        rename = "ownerNodeId",
        skip_serializing_if = "Option::is_none",
        default
    )]
    owner_node_id: Option<String>,
    #[serde(
        rename = "accessGrantMode",
        skip_serializing_if = "Option::is_none",
        default
    )]
    access_grant_mode: Option<String>,
    #[serde(
        rename = "folderKeyHash",
        skip_serializing_if = "Option::is_none",
        default
    )]
    folder_key_hash: Option<String>,
    #[serde(
        rename = "senderProfile",
        skip_serializing_if = "Option::is_none",
        default
    )]
    sender_profile: Option<ShareProfile>,
}

/// True if `value` is `None` or an empty string -- the "empty" side of Go's
/// `payload.CID == ""` / `payload.Key == ""` checks (Go strings have no
/// separate "absent" state, so a missing JSON field and an explicit `""`
/// are indistinguishable there; we treat `None` and `Some("")` the same way
/// for parity).
fn is_blank(value: &Option<String>) -> bool {
    value.as_deref().is_none_or(str::is_empty)
}

/// Validates a decoded share payload, matching Go's `validateSharePayload`
/// exactly (same checks, same error conditions).
fn validate(payload: &ShareLinkPayload) -> Result<()> {
    if payload.v != 1 {
        bail!("unsupported share link version");
    }
    if payload.type_ != "folder-share" && payload.type_ != "file-share" {
        bail!("unsupported share link type");
    }
    if payload.room_id.is_empty() {
        bail!("roomId is required");
    }
    if payload.type_ == "folder-share" {
        if is_blank(&payload.owner_node_id)
            || is_blank(&payload.folder_key_hash)
            || !is_blank(&payload.cid)
            || !is_blank(&payload.key)
        {
            bail!("invalid folder share link");
        }
        return Ok(());
    }
    // file-share
    if is_blank(&payload.cid) || is_blank(&payload.key) {
        bail!("invalid file share link");
    }
    Ok(())
}

/// Extracts the token to run query parsing over: the URL fragment (text
/// after the first `#`) if one is present, otherwise the raw input
/// unchanged -- so a full URL, a bare `#tc-share=...` fragment, and a plain
/// `tc-share=...` token are all accepted the same way `url.Parse` +
/// `.Fragment` does in the Go reference.
fn extract_token(raw: &str) -> &str {
    let token = match raw.find('#') {
        Some(idx) => &raw[idx + 1..],
        None => raw,
    };
    token.strip_prefix('#').unwrap_or(token)
}

/// Percent-decodes one `application/x-www-form-urlencoded` value: `+`
/// becomes a space and `%XX` becomes the byte `0xXX`; anything else passes
/// through unchanged (including a malformed trailing `%`, which is copied
/// through literally rather than rejected).
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 3 <= bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                match (hi, lo) {
                    (Some(hi), Some(lo)) => {
                        out.push(((hi << 4) | lo) as u8);
                        i += 3;
                    }
                    _ => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Finds `key`'s value in an `&`-separated `application/x-www-form-urlencoded`
/// query string, percent-decoding it. Returns `None` if `key` is not present.
fn find_query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let k = parts.next().unwrap_or("");
        let v = parts.next().unwrap_or("");
        if k == key {
            return Some(percent_decode(v));
        }
    }
    None
}

/// Builds a `folder-share` link token (`#tc-share=<base64url JSON>`),
/// mirroring the web app's `makeShareUrl` payload byte layout. Emitted as a
/// bare fragment token rather than a full URL because a headless daemon
/// doesn't know any particular tc-storage deployment's origin -- both this
/// module's [`parse_share_link`] and the web app accept the fragment
/// appended to any app URL. Validated with the same rules as parsing, so an
/// unbuildable share fails here instead of at the recipient.
pub fn build_folder_share_link(
    room_id: &str,
    folder_id: &str,
    folder_name: &str,
    owner_node_id: &str,
    access_grant_mode: &str,
    folder_key_hash: &str,
    sender_name: &str,
) -> Result<String> {
    let payload = ShareLinkPayload {
        v: 1,
        type_: "folder-share".to_string(),
        room_id: room_id.to_string(),
        clock: None,
        cid: None,
        key: None,
        folder_id: Some(folder_id.to_string()),
        folder_name: Some(folder_name.to_string()),
        file_id: None,
        file_name: None,
        owner_node_id: Some(owner_node_id.to_string()),
        access_grant_mode: Some(access_grant_mode.to_string()),
        folder_key_hash: Some(folder_key_hash.to_string()),
        sender_profile: Some(ShareProfile {
            name: sender_name.to_string(),
        }),
    };
    validate(&payload)?;
    let json = serde_json::to_vec(&payload).context("serializing tc-share payload")?;
    Ok(format!("#tc-share={}", URL_SAFE_NO_PAD.encode(json)))
}

/// Parses and validates a `tc-share` link, in any of the forms
/// [`extract_token`] accepts.
///
/// Mirrors Go's `ParseShareLink`: extracts the `tc-share` query parameter,
/// base64url-(no-pad)-decodes it into JSON, deserializes and validates the
/// payload, and returns the resulting [`LinkedShare`].
pub fn parse_share_link(raw: &str) -> Result<LinkedShare> {
    let token = extract_token(raw);
    let encoded = find_query_param(token, "tc-share").context("tc-share payload not found")?;
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .context("decoding tc-share payload")?;
    let payload: ShareLinkPayload =
        serde_json::from_slice(&decoded).context("parsing tc-share payload")?;
    validate(&payload)?;

    Ok(LinkedShare {
        key: payload.key,
        share: ParsedShare {
            type_: payload.type_,
            room_id: payload.room_id,
            clock: payload.clock,
            cid: payload.cid,
            folder_id: payload.folder_id,
            folder_name: payload.folder_name,
            file_id: payload.file_id,
            file_name: payload.file_name,
            owner_node_id: payload.owner_node_id,
            access_grant_mode: payload.access_grant_mode,
            folder_key_hash: payload.folder_key_hash,
            sender_profile: payload.sender_profile,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_payload(payload: &ShareLinkPayload) -> String {
        let json = serde_json::to_vec(payload).expect("serialize payload");
        URL_SAFE_NO_PAD.encode(json)
    }

    fn file_share_payload() -> ShareLinkPayload {
        ShareLinkPayload {
            v: 1,
            type_: "file-share".to_string(),
            room_id: "r".to_string(),
            clock: Some(7),
            cid: Some("bafyreiabc123".to_string()),
            key: Some("secret".to_string()),
            folder_id: Some("folder-1".to_string()),
            folder_name: Some("Folder".to_string()),
            file_id: Some("file-1".to_string()),
            file_name: Some("notes.txt".to_string()),
            owner_node_id: None,
            access_grant_mode: None,
            folder_key_hash: None,
            sender_profile: Some(ShareProfile {
                name: "Alice".to_string(),
            }),
        }
    }

    fn folder_share_payload() -> ShareLinkPayload {
        ShareLinkPayload {
            v: 1,
            type_: "folder-share".to_string(),
            room_id: "r".to_string(),
            clock: None,
            cid: None,
            key: None,
            folder_id: Some("folder-1".to_string()),
            folder_name: Some("Folder".to_string()),
            file_id: None,
            file_name: None,
            owner_node_id: Some("did:key:z6Mkabc".to_string()),
            access_grant_mode: Some("owner".to_string()),
            folder_key_hash: Some("a".repeat(64)),
            sender_profile: None,
        }
    }

    #[test]
    fn parses_valid_file_share_url() {
        let encoded = encode_payload(&file_share_payload());
        let url = format!("https://app.example/#tc-share={encoded}");

        let linked = parse_share_link(&url).expect("parse");

        assert_eq!(linked.key.as_deref(), Some("secret"));
        assert_eq!(linked.share.type_, "file-share");
        assert_eq!(linked.share.room_id, "r");
        assert_eq!(linked.share.cid.as_deref(), Some("bafyreiabc123"));
    }

    #[test]
    fn parses_valid_folder_share_url() {
        let encoded = encode_payload(&folder_share_payload());
        let url = format!("https://app.example/#tc-share={encoded}");

        let linked = parse_share_link(&url).expect("parse");

        assert_eq!(linked.key, None);
        assert_eq!(linked.share.type_, "folder-share");
        assert_eq!(linked.share.room_id, "r");
        assert_eq!(
            linked.share.owner_node_id.as_deref(),
            Some("did:key:z6Mkabc")
        );
        assert_eq!(
            linked.share.folder_key_hash.as_deref(),
            Some("a".repeat(64).as_str())
        );
        assert_eq!(linked.share.cid, None);
    }

    #[test]
    fn parses_bare_token_without_url_or_hash() {
        let encoded = encode_payload(&file_share_payload());
        let token = format!("tc-share={encoded}");

        let linked = parse_share_link(&token).expect("parse");

        assert_eq!(linked.share.room_id, "r");
        assert_eq!(linked.key.as_deref(), Some("secret"));
    }

    #[test]
    fn missing_tc_share_param_is_an_error() {
        let result = parse_share_link("https://app.example/#other=1");
        assert!(result.is_err());
    }

    #[test]
    fn wrong_version_is_an_error() {
        let mut payload = file_share_payload();
        payload.v = 2;
        let encoded = encode_payload(&payload);

        let result = parse_share_link(&format!("tc-share={encoded}"));
        assert!(result.is_err());
    }

    #[test]
    fn file_share_missing_key_is_an_error() {
        let mut payload = file_share_payload();
        payload.key = None;
        let encoded = encode_payload(&payload);

        let result = parse_share_link(&format!("tc-share={encoded}"));
        assert!(result.is_err());
    }

    #[test]
    fn folder_share_with_cid_present_is_an_error() {
        let mut payload = folder_share_payload();
        payload.cid = Some("bafyreishouldnotbehere".to_string());
        let encoded = encode_payload(&payload);

        let result = parse_share_link(&format!("tc-share={encoded}"));
        assert!(result.is_err());
    }
}
