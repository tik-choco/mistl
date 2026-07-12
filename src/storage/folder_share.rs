//! Networked folder-share receive flow: request access from a folder's
//! owner, wait for the owner's manual approval (grant), decrypt the folder
//! key, fetch the folder manifest and every file it references, and write
//! the decrypted contents into the content sandbox.
//!
//! Ported from tc-storage's `storage-cli` `internal/app/folder_share.go`
//! (`FetchFolderShare` and friends), matching its wire envelope shape
//! (`internal/protocol/envelope.go`, `accessgrant.go`, `folderkey.go`) field
//! for field so mistl can interoperate with a real tc-storage web app or Go
//! CLI acting as the folder owner. This module implements the one-shot
//! *requester* flow (`store folder-get`) plus the shared wire/crypto
//! primitives; continuous requester-side auto-sync lives in the sibling
//! [`super::folder_sync`] module and the owner/responder role (serving
//! grants and announcing changes for mistl-shared folders) in
//! [`super::folder_owner`].
//!
//! ## Node-id vs DID addressing
//!
//! tc-storage nodes use their `did:key` DID as their mistlib transport node
//! id (web: `settings.nodeId`; CLI: `runtime.go`'s `nodeID =
//! config.Identity.Did`), while mistl identifies itself on the mesh by
//! `identity::Identity::node_id()` (first 16 hex chars of `sha256(did)`), a
//! pre-existing convention shared with `mailbox`/`ai`/`stream`. This
//! asymmetry does not break the grant flow: neither owner implementation
//! replies by addressing `request.from` directly. The web app sends every
//! envelope to *all* stable mist peers in the room (`p2p.ts`
//! `transmitEnvelope` -> `sendMistPayloadToPeers(roomStablePeers)`) and the
//! Go CLI's `broadcastEnvelope` likewise unicasts to every `ConnectedPeers`
//! transport id, with recipient selection done receive-side via the
//! envelope's `targetNodeId` field. mistl therefore receives the grant on
//! its 16-hex transport id and matches it by `requestId`/`from` (we
//! deliberately do not require `targetNodeId == our node id`, since owners
//! set it to our DID). Outgoing envelopes still must set `from` to our real
//! `did:key` so the owner's Ed25519 signature validation accepts them.
//! Residual caveat: delivery relies on mistl having been connected long
//! enough to enter the owner's *stable* peer set (`observeStableMistPeers`'s
//! `stableAfterMs`); the 5-second request-resend loop below rides that out.
//!
//! ## IPC/streaming caveat
//!
//! The daemon IPC protocol is a single blocking request/response (see
//! `daemon::ipc`), not a stream, and this task keeps that protocol
//! unchanged. So unlike tc-storage-cli's `folder-get`, which prints "… "
//! progress lines to stderr *as they happen* over a long-running local
//! process, mistl's `store folder-get` is one blocking IPC call that can
//! take minutes (owner approval has a 5-minute timeout, matching the Go
//! reference): progress lines are accumulated server-side and returned in
//! the final response's `progress` array, then flushed to the CLI's stderr
//! all at once when the call completes. Not truly live, but avoids changing
//! the IPC transport.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use p256::PublicKey;
use p256::ecdh::EphemeralSecret;
use p256::elliptic_curve::sec1::ToEncodedPoint;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::sync::{OnceCell, broadcast};

use super::sharelink::{self, ShareProfile};
use super::{Store, decode_data_url, sha256_hex};
use crate::daemon::AppState;

/// `tc-storage-folder-key-v1` HMAC/hash domain prefix, matching
/// `src/crypto/folderKeyProof.ts` and the Go reference's
/// `folderKeyHashPrefix`.
const FOLDER_KEY_HASH_PREFIX: &str = "tc-storage-folder-key-v1";

/// The outcome of a completed folder-share download.
#[derive(Clone)]
pub struct FolderShareResult {
    pub folder_name: String,
    /// Sandbox-relative paths written, forward-slash normalized.
    pub files: Vec<String>,
    /// Human-readable reasons files were skipped (no content cid, decode
    /// error, checksum mismatch).
    pub skipped: Vec<String>,
}

/// Wire envelope for the folder-share protocol: the access-request/grant
/// handshake, the `folder-state` pointer message, and the incremental
/// `folder-change` push, matching the web app's `ShareEnvelope`
/// (`src/p2p/p2pTypes.ts`) / Go's `internal/protocol/envelope.go`
/// field-for-field. Field names are camelCase on the wire and are part of
/// the interop contract; do not rename without updating every
/// producer/consumer. `pub(super)` so the sibling `folder_sync` (requester
/// auto-sync) and `folder_owner` (owner/responder role) modules can produce
/// and consume the same envelopes through [`envelope_bus`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ShareEnvelope {
    #[serde(rename = "type")]
    pub(super) type_: String,
    pub(super) from: String,
    #[serde(rename = "roomId")]
    pub(super) room_id: String,
    #[serde(rename = "sentAt")]
    pub(super) sent_at: String,
    pub(super) clock: i64,
    /// `folder-change` discriminator: `file-upserted` | `file-deleted` |
    /// `folder-upserted` | `folder-deleted`.
    #[serde(rename = "changeType", skip_serializing_if = "Option::is_none", default)]
    pub(super) change_type: Option<String>,
    /// Cheap "did anything change" digest of the folder tree (see
    /// `folder_sync::folder_signature`); not cryptographically meaningful.
    #[serde(
        rename = "folderSignature",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) folder_signature: Option<String>,
    #[serde(rename = "folderId", skip_serializing_if = "Option::is_none", default)]
    pub(super) folder_id: Option<String>,
    #[serde(rename = "folderName", skip_serializing_if = "Option::is_none", default)]
    pub(super) folder_name: Option<String>,
    /// Full folder record carried by `folder-change` upserts.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(super) folder: Option<super::domain::FolderRecord>,
    #[serde(rename = "fileId", skip_serializing_if = "Option::is_none", default)]
    pub(super) file_id: Option<String>,
    #[serde(rename = "fileName", skip_serializing_if = "Option::is_none", default)]
    pub(super) file_name: Option<String>,
    /// Full file record carried by `folder-change` upserts.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(super) file: Option<super::domain::FileRecord>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(super) cid: Option<String>,
    #[serde(rename = "ownerNodeId", skip_serializing_if = "Option::is_none", default)]
    pub(super) owner_node_id: Option<String>,
    #[serde(
        rename = "accessGrantMode",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) access_grant_mode: Option<String>,
    #[serde(
        rename = "folderKeyHash",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) folder_key_hash: Option<String>,
    #[serde(
        rename = "targetNodeId",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) target_node_id: Option<String>,
    #[serde(rename = "requestId", skip_serializing_if = "Option::is_none", default)]
    pub(super) request_id: Option<String>,
    #[serde(
        rename = "senderProfile",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) sender_profile: Option<ShareProfile>,
    #[serde(
        rename = "accessPublicKey",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) access_public_key: Option<String>,
    #[serde(
        rename = "accessGrantPublicKey",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) access_grant_public_key: Option<String>,
    #[serde(
        rename = "accessGrantIv",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) access_grant_iv: Option<String>,
    #[serde(
        rename = "accessGrantCipherText",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) access_grant_cipher_text: Option<String>,
    /// HMAC-SHA256 proof keyed by the folder passphrase over
    /// `tc-storage-folder-access-grant-v1\0folderId\0requestId\0targetNodeId`
    /// (see `folderKeyProof.ts`); set on `folder-access-grant`.
    #[serde(
        rename = "accessGrantProof",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(super) access_grant_proof: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(super) signature: Option<String>,
}

impl Default for ShareEnvelope {
    /// An empty envelope with every optional field unset; producers fill in
    /// `type_`/`from`/`room_id`/`sent_at` plus whatever their message type
    /// carries, then pass it through [`sign_envelope`].
    fn default() -> Self {
        Self {
            type_: String::new(),
            from: String::new(),
            room_id: String::new(),
            sent_at: String::new(),
            clock: 0,
            change_type: None,
            folder_signature: None,
            folder_id: None,
            folder_name: None,
            folder: None,
            file_id: None,
            file_name: None,
            file: None,
            cid: None,
            owner_node_id: None,
            access_grant_mode: None,
            folder_key_hash: None,
            target_node_id: None,
            request_id: None,
            sender_profile: None,
            access_public_key: None,
            access_grant_public_key: None,
            access_grant_iv: None,
            access_grant_cipher_text: None,
            access_grant_proof: None,
            signature: None,
        }
    }
}

/// The requester's ephemeral ECDH P-256 key pair for one access request
/// (mirrors Go's `protocol.AccessRequestKey`).
pub(super) struct AccessRequestKey {
    pub(super) secret: EphemeralSecret,
    /// Base64url (no padding) uncompressed SEC1 point (65 bytes, `0x04`
    /// prefix), matching Go's `ecdh.PublicKey.Bytes()`.
    pub(super) public_b64url: String,
}

pub(super) fn create_access_request_key() -> AccessRequestKey {
    let secret = EphemeralSecret::random(&mut rand::rngs::OsRng);
    let encoded = secret.public_key().to_encoded_point(false);
    AccessRequestKey {
        secret,
        public_b64url: URL_SAFE_NO_PAD.encode(encoded.as_bytes()),
    }
}

/// Derive the AES key via ECDH (raw shared-secret X coordinate, no KDF) and
/// decrypt the folder key, mirroring Go's `DecryptFolderKeyGrant`.
pub(super) fn decrypt_folder_key_grant(
    cipher_text_b64: &str,
    iv_b64: &str,
    secret: &EphemeralSecret,
    peer_public_b64url: &str,
) -> Result<String> {
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

    let peer_raw = URL_SAFE_NO_PAD
        .decode(peer_public_b64url.trim_end_matches('='))
        .context("grant public key")?;
    let peer_public = PublicKey::from_sec1_bytes(&peer_raw).context("grant public key")?;
    let shared = secret.diffie_hellman(&peer_public);
    let key_bytes = shared.raw_secret_bytes();

    let iv = BASE64_STANDARD.decode(iv_b64).context("grant iv")?;
    let cipher_text = BASE64_STANDARD.decode(cipher_text_b64).context("grant ciphertext")?;
    let cipher = Aes256Gcm::new_from_slice(key_bytes.as_slice()).context("grant key")?;
    let nonce = Nonce::from_slice(&iv);
    let plain = cipher
        .decrypt(nonce, cipher_text.as_slice())
        .map_err(|_| anyhow::anyhow!("decrypt grant"))?;

    #[derive(Deserialize)]
    struct Payload {
        key: String,
    }
    let payload: Payload = serde_json::from_slice(&plain).context("grant payload")?;
    if payload.key.is_empty() {
        bail!("access grant did not contain a folder key");
    }
    Ok(payload.key)
}

/// `FolderKeyHash = hex(sha256("tc-storage-folder-key-v1\0folderId\0passphrase"))`,
/// matching `src/crypto/folderKeyProof.ts` / Go's `FolderKeyHash`.
pub(super) fn folder_key_hash(folder_id: &str, passphrase: &str) -> String {
    let mut buf = Vec::new();
    buf.extend_from_slice(FOLDER_KEY_HASH_PREFIX.as_bytes());
    buf.push(0);
    buf.extend_from_slice(folder_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(passphrase.trim().as_bytes());
    sha256_hex(&buf)
}

pub(super) fn matches_folder_key_hash(folder_id: &str, passphrase: &str, expected_hash: &str) -> bool {
    !expected_hash.is_empty() && folder_key_hash(folder_id, passphrase) == expected_hash
}

/// True if `did` is a well-formed Ed25519 `did:key` (multibase `z` + 0xed01
/// multicodec + 32-byte public key), matching Go's `IsEd25519DidKey`. A
/// small, self-contained duplicate of `identity`'s private DID parsing (that
/// module keeps its multicodec check internal), scoped to this file only.
pub(super) fn is_ed25519_did_key(did: &str) -> bool {
    let Some(multibase) = did.strip_prefix("did:key:") else {
        return false;
    };
    let Some(encoded) = multibase.strip_prefix('z') else {
        return false;
    };
    let Ok(bytes) = bs58::decode(encoded).into_vec() else {
        return false;
    };
    bytes.len() == 34 && bytes[0] == 0xed && bytes[1] == 0x01
}

/// Abbreviate a long identifier for progress lines, matching Go's `Short`.
pub(super) fn short(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() <= 28 {
        return value.to_string();
    }
    let head: String = chars[..14].iter().collect();
    let tail: String = chars[chars.len() - 8..].iter().collect();
    format!("{head}...{tail}")
}

/// Canonical, deterministic JSON string of `envelope` with its `signature`
/// field removed: object keys sorted, numbers formatted without a
/// superfluous decimal point. Matches Go's `EnvelopeSigningPayload` +
/// `stableStringify` byte-for-byte for the field shapes used here (ASCII
/// identifiers/timestamps; arbitrary Unicode string escaping may differ from
/// Go's `strconv.Quote`, which this doesn't attempt to replicate exactly).
fn envelope_signing_payload(envelope: &ShareEnvelope) -> Result<String> {
    let mut value = serde_json::to_value(envelope).context("serializing envelope")?;
    if let serde_json::Value::Object(map) = &mut value {
        map.remove("signature");
    }
    Ok(stable_stringify(&value))
}

fn stable_stringify(value: &serde_json::Value) -> String {
    use serde_json::Value;
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => serde_json::to_string(s).unwrap_or_default(),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(stable_stringify).collect();
            format!("[{}]", parts.join(","))
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().filter(|k| !map[*k].is_null()).collect();
            keys.sort();
            let parts: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(*k).unwrap_or_default(),
                        stable_stringify(&map[*k])
                    )
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
    }
}

pub(super) fn sign_envelope(
    mut envelope: ShareEnvelope,
    identity: &crate::identity::Identity,
) -> Result<ShareEnvelope> {
    if !is_ed25519_did_key(&envelope.from) {
        bail!("envelope sender must be an Ed25519 did:key");
    }
    let payload = envelope_signing_payload(&envelope)?;
    let signature = identity.sign(payload.as_bytes());
    envelope.signature = Some(URL_SAFE_NO_PAD.encode(signature));
    Ok(envelope)
}

pub(super) fn verify_envelope(envelope: &ShareEnvelope) -> bool {
    let Some(signature) = envelope.signature.as_deref() else {
        return false;
    };
    if !is_ed25519_did_key(&envelope.from) {
        return false;
    }
    let Ok(payload) = envelope_signing_payload(envelope) else {
        return false;
    };
    let Ok(signature_bytes) = URL_SAFE_NO_PAD.decode(signature.trim_end_matches('=')) else {
        return false;
    };
    crate::identity::verify(&envelope.from, payload.as_bytes(), &signature_bytes).unwrap_or(false)
}

/// Process-wide fan-out of verified inbound [`ShareEnvelope`]s, backed by a
/// single [`crate::net::register_handler`] registration (installed lazily on
/// first use). Mirrors the Go runtime's `subscribeEnvelopes`: every raw
/// message is parsed against this module's schema and silently ignored if
/// it doesn't match (mistl's modules coexist on the wire "by shape" --
/// see `net`'s module doc), then signature-verified before being handed to
/// subscribers.
static ENVELOPE_BUS: OnceCell<broadcast::Sender<ShareEnvelope>> = OnceCell::const_new();

pub(super) async fn envelope_bus() -> &'static broadcast::Sender<ShareEnvelope> {
    ENVELOPE_BUS
        .get_or_init(|| async {
            let (tx, _rx) = broadcast::channel(64);
            let tx_for_handler = tx.clone();
            crate::net::register_handler(move |event_type, _from, data| {
                if event_type != crate::net::EVENT_RAW {
                    return;
                }
                let Ok(envelope) = serde_json::from_slice::<ShareEnvelope>(data) else {
                    return;
                };
                if !verify_envelope(&envelope) {
                    return;
                }
                let _ = tx_for_handler.send(envelope);
            });
            tx
        })
        .await
}

/// Send `bytes` to `to` in `room`, retrying on failure (in particular
/// mistlib's "Room not joined" error while the async room join is still in
/// flight -- see the project note on `joinRoom` being fire-and-forget with
/// no ready signal) until `timeout` elapses.
pub(super) async fn send_bytes_retrying(
    room: &str,
    to: &str,
    bytes: Vec<u8>,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match crate::net::send_direct(room, to, bytes.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Poll `net::connected_nodes` until `node_id` appears or `timeout` elapses.
pub(super) async fn wait_for_peer(node_id: &str, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if crate::net::connected_nodes().await.iter().any(|n| n == node_id) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("owner {} did not connect within {:?}", short(node_id), timeout);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Send the (already-signed) access request, then wait for a matching
/// `folder-access-grant` (or `folder-access-denied`), resending the request
/// every 5 seconds while waiting -- mirrors Go's `requestAndAwaitGrant`
/// exactly (same resend cadence, same 5-minute overall timeout).
#[allow(clippy::too_many_arguments)]
pub(super) async fn request_and_await_grant(
    room: &str,
    owner: &str,
    request_id: &str,
    folder_id: &str,
    folder_key_hash_expected: &str,
    request: &ShareEnvelope,
    request_key: &AccessRequestKey,
    progress: &mut (dyn FnMut(&str) + Send),
) -> Result<(String, Option<String>)> {
    let mut rx = envelope_bus().await.subscribe();
    let bytes = serde_json::to_vec(request).context("serializing access request")?;

    send_bytes_retrying(room, owner, bytes.clone(), Duration::from_secs(30)).await?;
    progress("access request sent — waiting for owner approval…");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5 * 60);
    let mut next_resend = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out waiting for owner approval");
        }
        tokio::select! {
            _ = tokio::time::sleep_until(next_resend.min(deadline)) => {
                if tokio::time::Instant::now() >= deadline {
                    bail!("timed out waiting for owner approval");
                }
                let _ = send_bytes_retrying(room, owner, bytes.clone(), Duration::from_secs(10)).await;
                next_resend = tokio::time::Instant::now() + Duration::from_secs(5);
            }
            received = rx.recv() => {
                let envelope = match received {
                    Ok(envelope) => envelope,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => bail!("envelope channel closed unexpectedly"),
                };
                if envelope.type_ == "folder-access-denied" && envelope.request_id.as_deref() == Some(request_id) {
                    bail!("owner denied the access request");
                }
                if envelope.type_ != "folder-access-grant" || envelope.request_id.as_deref() != Some(request_id) {
                    continue;
                }
                if envelope.from != owner {
                    continue;
                }
                let (Some(public_key), Some(iv), Some(cipher_text)) = (
                    envelope.access_grant_public_key.as_deref(),
                    envelope.access_grant_iv.as_deref(),
                    envelope.access_grant_cipher_text.as_deref(),
                ) else {
                    continue;
                };
                let key = decrypt_folder_key_grant(cipher_text, iv, &request_key.secret, public_key)
                    .context("decrypt grant")?;
                if !matches_folder_key_hash(folder_id, &key, folder_key_hash_expected) {
                    bail!("granted folder key failed hash verification");
                }
                progress("access granted — folder key received");
                return Ok((key, envelope.cid.clone()));
            }
        }
    }
}

/// Wait for a `folder-state` envelope naming `folder_id`, used when a grant
/// didn't carry a `cid` inline (mirrors Go's `awaitFolderStateCID`).
pub(super) async fn await_folder_state_cid(folder_id: &str, timeout: Duration) -> Result<String> {
    let mut rx = envelope_bus().await.subscribe();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("timed out waiting for folder-state");
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(envelope)) => {
                if envelope.type_ == "folder-state"
                    && envelope.folder_id.as_deref() == Some(folder_id)
                    && let Some(cid) = envelope.cid.filter(|c| !c.is_empty())
                {
                    return Ok(cid);
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(broadcast::error::RecvError::Closed)) => bail!("envelope channel closed unexpectedly"),
            Err(_) => bail!("timed out waiting for folder-state"),
        }
    }
}

/// Fetch a content-addressed blob, trying the daemon's local store first and
/// falling back to mistlib's p2p storage engine (which resolves missing
/// blocks from room peers via `NativePeerResolver`). tc-storage owners
/// publish bundles through `mist.storage_add`, so their CIDs live in the p2p
/// engine, not in mistl's local-only `Store` (whose resolver is a no-op) --
/// without this fallback every networked share fetch dead-ends with "block
/// not found" unless the blocks happen to be local already.
pub(super) async fn fetch_blob(store: &Store, cid: &str) -> Result<Vec<u8>> {
    if let Ok(data) = store.get(cid).await {
        return Ok(data);
    }
    p2p_storage_get(cid).await
}

/// `mistlib::app::storage_get` wrapped in `spawn_blocking`: the sync app
/// functions `block_on` mistlib's internal runtime and panic if called from
/// a tokio worker thread directly.
pub(super) async fn p2p_storage_get(cid: &str) -> Result<Vec<u8>> {
    let cid = cid.to_string();
    tokio::task::spawn_blocking(move || {
        mistlib::app::storage_get(&cid).map_err(|error| anyhow::anyhow!("p2p storage_get {cid}: {error}"))
    })
    .await
    .context("p2p storage_get task")?
}

/// `mistlib::app::storage_add` wrapped in `spawn_blocking` (see
/// [`p2p_storage_get`]); returns the CID under which room peers (tc-storage
/// web clients included) can resolve the blob.
pub(super) async fn p2p_storage_add(name: &str, data: Vec<u8>) -> Result<String> {
    let name = name.to_string();
    tokio::task::spawn_blocking(move || {
        mistlib::app::storage_add(&name, &data)
            .map_err(|error| anyhow::anyhow!("p2p storage_add {name}: {error}"))
    })
    .await
    .context("p2p storage_add task")?
}

pub(super) async fn fetch_folder_bundle(
    store: &Store,
    cid: &str,
    folder_key: &str,
) -> Result<super::domain::FolderBundle> {
    let raw = fetch_blob(store, cid).await?;
    let payload: super::crypto::EncryptedPayload =
        serde_json::from_slice(&raw).context("folder bundle parse")?;
    super::crypto::decrypt_json(&payload, folder_key)
}

pub(super) async fn fetch_file_content(store: &Store, cid: &str, folder_key: &str) -> Result<Vec<u8>> {
    let raw = fetch_blob(store, cid).await?;
    let payload: super::crypto::EncryptedPayload = serde_json::from_slice(&raw).context("file bundle parse")?;
    let bundle: super::domain::FileBundle = super::crypto::decrypt_json(&payload, folder_key)?;
    let data_url = bundle.file.data_url.context("file has no content")?;
    decode_data_url(&data_url)
}

/// Replace path separators and reject empty/`.`/`..` names, matching Go's
/// `sanitizeName`.
pub(super) fn sanitize_name(name: &str) -> String {
    let trimmed = name.trim().replace(['/', '\\'], "_");
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "untitled".to_string()
    } else {
        trimmed
    }
}

pub(super) fn folder_deleted(folder: &super::domain::FolderRecord) -> bool {
    folder.deleted_at.as_deref().is_some_and(|s| !s.is_empty())
}

/// Walk `folder_id`'s ancestor chain (via `folders`) to a sanitized,
/// root-to-leaf list of path components, matching Go's
/// `remoteFolderPathParts`.
pub(super) fn folder_path_parts(
    folders: &HashMap<String, super::domain::FolderRecord>,
    folder_id: &str,
) -> Vec<String> {
    let mut parts = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut current = folder_id.to_string();
    while !current.is_empty() && visited.insert(current.clone()) {
        let Some(folder) = folders.get(&current) else {
            break;
        };
        if folder_deleted(folder) {
            break;
        }
        parts.insert(0, sanitize_name(&folder.name));
        match folder.parent_id.as_deref() {
            Some(parent) if !parent.is_empty() => current = parent.to_string(),
            _ => break,
        }
    }
    parts
}

async fn download_bundle_files(
    store: &Store,
    share: &sharelink::ParsedShare,
    bundle: super::domain::FolderBundle,
    folder_key: &str,
    progress: &mut (dyn FnMut(&str) + Send),
) -> Result<FolderShareResult> {
    let folder_name = [bundle.folder.name.as_str(), share.folder_name.as_deref().unwrap_or("")]
        .into_iter()
        .find(|s| !s.trim().is_empty())
        .unwrap_or("shared-folder")
        .to_string();
    let mut result = FolderShareResult {
        folder_name: folder_name.clone(),
        files: Vec::new(),
        skipped: Vec::new(),
    };
    progress(&format!("folder {folder_name:?}: {} file(s)", bundle.files.len()));

    let mut folders: HashMap<String, super::domain::FolderRecord> = HashMap::new();
    for folder in bundle.folders.iter().flatten() {
        if !folder.id.is_empty() && !folder_deleted(folder) {
            folders.insert(folder.id.clone(), folder.clone());
        }
    }
    if folders.is_empty() && !bundle.folder.id.is_empty() {
        folders.insert(bundle.folder.id.clone(), bundle.folder.clone());
    }

    let sandbox = super::sandbox::Sandbox::new(store.data_dir().join("sandbox"))?;

    for file in &bundle.files {
        if file.deleted_at.as_deref().is_some_and(|s| !s.is_empty()) {
            continue;
        }
        let cid = file
            .last_share_cid
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| file.last_cid.clone().filter(|s| !s.is_empty()));
        let Some(cid) = cid else {
            result.skipped.push(format!("{} (no content cid)", file.name));
            continue;
        };
        let data = match fetch_file_content(store, &cid, folder_key).await {
            Ok(data) => data,
            Err(error) => {
                result.skipped.push(format!("{} ({error})", file.name));
                continue;
            }
        };

        let mut parts = folder_path_parts(&folders, &file.folder_id);
        if parts.is_empty() {
            parts.push(sanitize_name(&folder_name));
        }
        parts.push(sanitize_name(&file.name));
        let rel = parts.join("/");

        let target = sandbox.resolve(&rel)?;
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        tokio::fs::write(&target, &data)
            .await
            .with_context(|| format!("writing {}", target.display()))?;

        if !file.checksum.is_empty() && sha256_hex(&data) != file.checksum {
            result.skipped.push(format!("{} (checksum mismatch)", file.name));
            continue;
        }
        progress(&format!("saved {rel} ({} bytes)", data.len()));
        result.files.push(rel);
    }
    Ok(result)
}

/// Registry type backing [`single_flight`]: one slot per in-flight key, each
/// slot a [`OnceCell`] that concurrent joiners await together. `pub(super)`
/// so [`super::folder_sync`]'s `start_sync` can reuse the same primitive for
/// its own initial-grant handshake.
pub(super) type SingleFlightRegistry<T> = OnceCell<Mutex<HashMap<String, Arc<OnceCell<T>>>>>;

/// Generic single-flight join: concurrent calls sharing the same `key`
/// invoke `make` exactly once and all receive a clone of its outcome. The
/// slot is removed from `registry` as soon as every joined caller has picked
/// up the result, so a later call with the same key starts a fresh run
/// rather than replaying a stale cached outcome.
pub(super) async fn single_flight<T, F, Fut>(registry: &SingleFlightRegistry<T>, key: String, make: F) -> T
where
    T: Clone,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let map = registry.get_or_init(|| async { Mutex::new(HashMap::new()) }).await;
    let slot = map
        .lock()
        .unwrap()
        .entry(key.clone())
        .or_insert_with(|| Arc::new(OnceCell::new()))
        .clone();
    let value = slot.get_or_init(make).await.clone();
    map.lock().unwrap().remove(&key);
    value
}

#[derive(Clone)]
struct SharedFetchOutcome {
    result: std::result::Result<FolderShareResult, String>,
    progress: Vec<String>,
}

/// Single-flight registry for [`fetch_folder_share`], keyed by the trimmed
/// share URL. Nothing in the daemon IPC/HTTP layer cancels an in-flight
/// `store.folder-get` call when its original caller goes away (an HTTP
/// client disconnecting mid-request, a page reload, a user re-clicking
/// "Fetch" during the busy note's multi-minute wait) -- the dispatch future
/// just keeps running to completion regardless. Without this dedup, every
/// retry would send its own `folder-access-request` with a fresh
/// `requestId`, piling up next to whatever earlier attempt is still alive:
/// the owner's approval UI re-prompts once per abandoned attempt, and a
/// grant addressed at one `requestId` never satisfies a caller still
/// waiting on another. Joining the one live operation instead means every
/// caller for the same link shares the same `requestId` and the same
/// eventual grant.
static INFLIGHT_FOLDER_FETCHES: SingleFlightRegistry<SharedFetchOutcome> = OnceCell::const_new();

/// Run the full folder-share receive flow for `share_url`: parse and
/// validate the link, wait for the owner to be reachable, request access,
/// wait for the owner's grant, fetch the folder manifest, and download every
/// file into the sandbox. `progress` receives human-readable status lines as
/// the flow advances (see the module doc for why these are batched rather
/// than streamed to the CLI in real time).
///
/// Concurrent calls for the same `share_url` join a single in-flight
/// operation instead of each starting their own access request; see
/// [`INFLIGHT_FOLDER_FETCHES`].
pub async fn fetch_folder_share(
    share_url: &str,
    store: &Store,
    state: &Arc<AppState>,
    progress: &mut (dyn FnMut(&str) + Send),
) -> Result<FolderShareResult> {
    let key = share_url.trim().to_string();
    let outcome = single_flight(&INFLIGHT_FOLDER_FETCHES, key, || async {
        let mut recorded = Vec::new();
        let mut record = |line: &str| recorded.push(line.to_string());
        let result = fetch_folder_share_once(share_url, store, state, &mut record)
            .await
            .map_err(|error| format!("{error:#}"));
        SharedFetchOutcome { result, progress: recorded }
    })
    .await;

    for line in &outcome.progress {
        progress(line);
    }
    outcome.result.map_err(anyhow::Error::msg)
}

async fn fetch_folder_share_once(
    share_url: &str,
    store: &Store,
    state: &Arc<AppState>,
    progress: &mut (dyn FnMut(&str) + Send),
) -> Result<FolderShareResult> {
    let linked = sharelink::parse_share_link(share_url)?;
    let share = linked.share;
    if share.type_ != "folder-share" {
        bail!("not a folder-share link (type {:?})", share.type_);
    }
    let owner = share
        .owner_node_id
        .clone()
        .filter(|s| !s.is_empty())
        .context("share link missing ownerNodeId")?;
    if !is_ed25519_did_key(&owner) {
        bail!("share owner is not a valid did:key");
    }
    let folder_id = share
        .folder_id
        .clone()
        .filter(|s| !s.is_empty())
        .context("share link missing folderId")?;
    let folder_key_hash_expected = share
        .folder_key_hash
        .clone()
        .filter(|s| !s.is_empty())
        .context("share link missing folderKeyHash")?;

    if share.room_id.trim().is_empty() {
        bail!("share link missing roomId");
    }
    // Join the share's own room directly (additive; `net`'s room registry is
    // refcounted, so this coexists with the configured `storage.room_ids` and
    // any other active share rooms) instead of requiring `storage.room_ids`
    // to contain it -- shares from different rooms can be fetched concurrently.
    crate::net::ensure_started(state, share.room_id.clone())
        .await
        .context("storage: joining share room")?;

    let identity = crate::identity::current(state).await?;

    progress(&format!("connecting to owner {}…", short(&owner)));
    wait_for_peer(&owner, Duration::from_secs(40)).await?;

    let request_key = create_access_request_key();
    let mut suffix = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut suffix);
    let suffix_hex: String = suffix.iter().map(|b| format!("{b:02x}")).collect();
    let request_id = format!("access-{}-{suffix_hex}", chrono::Utc::now().timestamp_millis());

    let request = ShareEnvelope {
        type_: "folder-access-request".to_string(),
        from: identity.did().to_string(),
        room_id: share.room_id.clone(),
        sent_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        folder_id: Some(folder_id.clone()),
        folder_name: share.folder_name.clone(),
        access_grant_mode: Some(
            share
                .access_grant_mode
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "owner".to_string()),
        ),
        folder_key_hash: Some(folder_key_hash_expected.clone()),
        target_node_id: Some(owner.clone()),
        request_id: Some(request_id.clone()),
        sender_profile: Some(ShareProfile { name: "mistl".to_string() }),
        access_public_key: Some(request_key.public_b64url.clone()),
        ..ShareEnvelope::default()
    };
    let signed_request = sign_envelope(request, &identity)?;

    let (folder_key, grant_cid) = request_and_await_grant(
        &share.room_id,
        &owner,
        &request_id,
        &folder_id,
        &folder_key_hash_expected,
        &signed_request,
        &request_key,
        progress,
    )
    .await?;

    let cid = match grant_cid.filter(|s| !s.is_empty()) {
        Some(cid) => cid,
        None => {
            progress("waiting for folder-state…");
            await_folder_state_cid(&folder_id, Duration::from_secs(60)).await?
        }
    };

    progress(&format!("fetching folder manifest ({})…", short(&cid)));
    let bundle = fetch_folder_bundle(store, &cid, &folder_key).await?;

    let result = download_bundle_files(store, &share, bundle, &folder_key, progress).await;
    // One-shot fetch: drop our refcount on the share's room. (Early error
    // returns above leak the refcount until the next daemon restart -- a
    // joined-but-idle room is cheap, so no guard object for now. Continuous
    // sync flows in `folder_sync` keep their room for the sync's lifetime
    // instead.)
    let _ = crate::net::leave_room(&share.room_id).await;
    result
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test]
    async fn single_flight_joins_concurrent_calls_for_the_same_key() {
        let registry: SingleFlightRegistry<u32> = OnceCell::const_new();
        let runs = Arc::new(AtomicU32::new(0));

        async fn call(registry: &SingleFlightRegistry<u32>, runs: Arc<AtomicU32>) -> u32 {
            single_flight(registry, "same-key".to_string(), || async move {
                // Yield so a single-threaded test runtime gets a chance to
                // schedule the second concurrent caller, which must then
                // find this slot already initializing and join it rather
                // than invoke its own `make`.
                tokio::task::yield_now().await;
                runs.fetch_add(1, Ordering::SeqCst);
                42u32
            })
            .await
        }

        let (a, b) = tokio::join!(call(&registry, runs.clone()), call(&registry, runs.clone()));

        assert_eq!(a, 42);
        assert_eq!(b, 42);
        assert_eq!(runs.load(Ordering::SeqCst), 1, "maker must run exactly once for concurrent joiners");
    }

    #[tokio::test]
    async fn single_flight_starts_fresh_after_the_slot_completes() {
        let registry: SingleFlightRegistry<u32> = OnceCell::const_new();
        let runs = Arc::new(AtomicU32::new(0));

        let first = single_flight(&registry, "same-key".to_string(), || {
            let runs = runs.clone();
            async move { runs.fetch_add(1, Ordering::SeqCst) }
        })
        .await;
        let second = single_flight(&registry, "same-key".to_string(), || {
            let runs = runs.clone();
            async move { runs.fetch_add(1, Ordering::SeqCst) }
        })
        .await;

        assert_eq!(first, 0);
        assert_eq!(second, 1);
        assert_eq!(runs.load(Ordering::SeqCst), 2, "a call after completion must run again, not replay a cached value");
    }

    #[tokio::test]
    async fn single_flight_keeps_different_keys_independent() {
        let registry: SingleFlightRegistry<u32> = OnceCell::const_new();

        let a = single_flight(&registry, "key-a".to_string(), || async { 1u32 }).await;
        let b = single_flight(&registry, "key-b".to_string(), || async { 2u32 }).await;

        assert_eq!(a, 1);
        assert_eq!(b, 2);
    }
}
