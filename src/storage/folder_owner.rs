//! Owner/responder role for tc-storage folder shares: publish a local
//! directory as an encrypted shared folder, hand out access grants to
//! requesters, and announce state/changes so tc-storage web clients (and
//! other mistl daemons running `folder_sync`) stay in sync.
//!
//! Mirrors the web app's owner side (`appAccessActions.ts` +
//! `appFolderSyncActions.ts`); shared wire/crypto primitives live in
//! [`super::folder_share`]:
//! - `store folder-share <dir> --passphrase <p>` walks the directory, builds
//!   `FolderRecord`/`FileRecord`s, encrypts each file as a `FileBundle` and
//!   the whole tree as a `FolderBundle` (AES-256-GCM + PBKDF2 envelope),
//!   publishes them via `folder_share::p2p_storage_add` (mistlib's p2p
//!   storage engine, so room peers can resolve the CIDs), and prints a
//!   `#tc-share=` folder-share link.
//! - Grant responder: on a verified `folder-access-request` whose
//!   `folderKeyHash` matches a shared folder, reply with a
//!   `folder-access-grant` (fresh ephemeral P-256 ECDH, raw shared secret as
//!   AES-256-GCM key, `accessGrantProof` HMAC) -- auto-approved, since the
//!   hash proves the requester already knows the passphrase-derived key
//!   identity (the web app's `accessGrantMode: "shared"` semantics; mistl is
//!   a headless daemon with no approve/deny UI).
//! - Announcer: every ~60s (and immediately on `hello`), rescan the shared
//!   directory; if changed, re-publish the bundle and broadcast
//!   `folder-share`, else broadcast `folder-state` with the current CID +
//!   `folderSignature`. Per-file edits additionally broadcast incremental
//!   `folder-change` envelopes.
//! - Multi-room: each shared folder carries its own `room_id` (defaulting to
//!   the first `storage.room_ids` entry), joined for the share's lifetime.
//!
//! ## Wire encodings (interop contract, see `folderKeyProof.ts` /
//! `accessGrantCrypto.ts` in the tc-storage repo)
//!
//! - `accessGrantProof` = **hex** (lowercase, 64 chars) of
//!   `HMAC-SHA256(key = passphrase.trim(), msg =
//!   "tc-storage-folder-access-grant-v1\0folderId\0requestId\0targetNodeId")`
//!   -- *not* base64; `folderKeyProof.ts`'s `folderAccessGrantProof` returns
//!   `hex(hmacSha256(...))` and `matchesFolderAccessGrantProof` validates
//!   against `/^[a-f0-9]{64}$/`.
//! - `accessGrantPublicKey` = base64url, no padding, of our fresh ephemeral
//!   P-256 public key's uncompressed SEC1 point (`0x04 || X || Y`, 65
//!   bytes) -- matches `accessGrantCrypto.ts`'s `toBase64Url(bytesToBase64(...))`
//!   and [`super::folder_share::decrypt_folder_key_grant`]'s decoding.
//! - `accessGrantIv` / `accessGrantCipherText` = base64 *standard* (padded)
//!   -- `accessGrantCrypto.ts`'s `bytesToBase64` wraps `btoa`, and
//!   `decrypt_folder_key_grant` decodes both with `BASE64_STANDARD`.
//! - The AES-256-GCM key is the **raw** ECDH shared secret (P-256 X
//!   coordinate, no HKDF): this is what WebCrypto's
//!   `deriveKey({name:'ECDH', public}, .... {name:'AES-GCM', length:256})`
//!   actually computes for an EC algorithm (derived bits = raw shared
//!   secret, truncated/padded to the requested length), and it's exactly
//!   what [`super::folder_share::decrypt_folder_key_grant`] already assumes
//!   on the requester side -- this module's [`build_access_grant`] and that
//!   function are round-tripped against each other in
//!   `access_grant_roundtrips_with_the_requester_side_decrypt` below, which
//!   is the strongest evidence of wire compatibility available offline.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use p256::PublicKey;
use p256::ecdh::EphemeralSecret;
use p256::elliptic_curve::sec1::ToEncodedPoint;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tracing::warn;

use super::{Store, guess_mime, sha256_hex};
use crate::daemon::AppState;

/// `tc-storage-folder-access-grant-v1` HMAC domain prefix for
/// `accessGrantProof`, matching `src/crypto/folderKeyProof.ts`'s
/// `accessGrantProofPrefix`.
const ACCESS_GRANT_PROOF_PREFIX: &str = "tc-storage-folder-access-grant-v1";

/// Re-announce/rescan cadence, matching the web app's
/// `sharedFolderReannounceIntervalMs` (`appEffectUtils.ts`).
const RESCAN_INTERVAL: Duration = Duration::from_secs(60);

/// Minimum gap between two `folder-state` announcements for the same folder
/// carrying the same signature, matching `appFolderSyncActions.ts`'s
/// `folderStateAnnouncementDedupMs`.
const FOLDER_STATE_DEDUP_WINDOW: Duration = Duration::from_secs(15);

/// One persisted shared folder (the on-disk table row and the
/// `store.folder-share.ls` wire shape). The passphrase is persisted alongside
/// (the daemon must serve grants unattended across restarts; the source
/// directory is plaintext on the same disk anyway).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedFolder {
    /// Generated tc-storage folder id (stable across republish).
    pub folder_id: String,
    pub folder_name: String,
    /// Absolute local source directory.
    pub local_dir: std::path::PathBuf,
    /// The p2p room the share is announced in.
    pub room_id: String,
    pub passphrase: String,
    pub folder_key_hash: String,
    /// `owner` (default) or `shared`; carried in the share link.
    pub access_grant_mode: String,
    /// CID of the most recently published folder bundle.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_cid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_folder_signature: Option<String>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_published_at: Option<String>,
    /// Every folder in the shared tree (root plus descendants) as last
    /// published inside the `FolderBundle` -- kept here so rescans and
    /// access-grant responses (which echo `last_cid`) work without a network
    /// round trip. Deleted folders are retained as tombstones (`deletedAt`
    /// set), matching `saveEncryptedFolderToMist`'s wire shape.
    #[serde(default)]
    pub folders: Vec<super::domain::FolderRecord>,
    /// Every file in the shared tree, same tombstone convention as `folders`.
    /// Never carries an inline `dataUrl` (content lives only in the
    /// per-file `FileBundle` published under `last_cid`/`last_share_cid`).
    #[serde(default)]
    pub files: Vec<super::domain::FileRecord>,
    /// Local relative-path <-> record-id bookkeeping for the rescan loop
    /// (stable ids across republishes, mtime/size fast path so unchanged
    /// files aren't re-hashed every tick). Purely a mistl-internal
    /// implementation detail -- not part of any tc-storage wire contract --
    /// but persisted here (and thus visible on `store.folder-share.ls`)
    /// rather than kept in a separate table.
    #[serde(default)]
    path_index: PathIndex,
}

/// See [`SharedFolder::path_index`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PathIndex {
    /// Relative, forward-slash folder path (`""` = the share root) -> folder id.
    #[serde(default)]
    folder_ids: HashMap<String, String>,
    /// Relative, forward-slash file path -> file id.
    #[serde(default)]
    file_ids: HashMap<String, String>,
    /// Relative file path -> `(mtime_millis, size)` observed at the last
    /// successful scan; lets a rescan skip re-hashing a file whose mtime and
    /// size are both unchanged.
    #[serde(default)]
    file_stats: HashMap<String, (i64, u64)>,
}

/// What `store folder-share` returns: the pasteable link plus publish stats.
#[derive(Debug, Clone, Serialize)]
pub struct ShareOutcome {
    pub share_url: String,
    pub folder_id: String,
    pub room_id: String,
    pub files_published: usize,
}

/// Guards read-modify-write access to the `shared-folders.json` table.
/// Held across the whole sequence by every caller below (not just the file
/// I/O) so a concurrent `share`/`unshare`/background-rescan can't clobber
/// each other's update; the background rescan loop holds it for its entire
/// tick (including the network publish calls), which is a deliberate
/// simplicity-over-liveness tradeoff for a low-traffic, single-operator
/// daemon (a `store folder-share stop` submitted mid-tick just waits for the
/// tick to finish rather than racing it).
static TABLE_LOCK: Mutex<()> = Mutex::const_new(());

fn table_path(store: &Store) -> PathBuf {
    store.data_dir().join("shared-folders.json")
}

async fn read_table_file(store: &Store) -> Result<Vec<SharedFolder>> {
    let path = table_path(store);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("reading {}", path.display()))?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
}

async fn write_table_file(store: &Store, table: &[SharedFolder]) -> Result<()> {
    let path = table_path(store);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(table).context("serializing shared-folders table")?;
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, &bytes)
        .await
        .with_context(|| format!("writing {}", tmp.display()))?;
    tokio::fs::rename(&tmp, &path)
        .await
        .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))?;
    Ok(())
}

/// Publish `local_dir` as a shared folder in `room` (falling back to the
/// first configured `storage.room_ids` entry) and persist it so the
/// background responder/announcer keeps serving it. Errors if the dir
/// doesn't exist or is already shared.
pub async fn share_folder(
    local_dir: &str,
    passphrase: &str,
    name: Option<&str>,
    room: Option<&str>,
    store: &Store,
    state: &Arc<AppState>,
    progress: &mut (dyn FnMut(&str) + Send),
) -> Result<ShareOutcome> {
    if passphrase.trim().is_empty() {
        bail!("passphrase is required");
    }
    let root_path = Path::new(local_dir)
        .canonicalize()
        .with_context(|| format!("resolving {local_dir}"))?;
    if !root_path.is_dir() {
        bail!("{} is not a directory", root_path.display());
    }
    // A share lives in exactly one room, so with several configured rooms
    // the first one is the default; pass an explicit `room` to pick another.
    let room = room
        .map(str::to_string)
        .or_else(|| state.config().storage.room_ids.first().cloned())
        .context("folder-share requires --room or a configured storage.room_ids entry")?;

    {
        let _guard = TABLE_LOCK.lock().await;
        let table = read_table_file(store).await?;
        if table.iter().any(|entry| entry.local_dir == root_path) {
            bail!("{} is already shared", root_path.display());
        }
    }

    crate::net::ensure_started(state, room.clone())
        .await
        .context("storage: joining share room")?;
    let identity = crate::identity::current(state).await?;

    let folder_id = generate_uuid_v4();
    let folder_name = name
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            root_path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "shared-folder".to_string());

    let now = now_rfc3339();
    let mut path_index = PathIndex::default();
    path_index
        .folder_ids
        .insert(String::new(), folder_id.clone());
    let mut entry = SharedFolder {
        folder_id: folder_id.clone(),
        folder_name: folder_name.clone(),
        local_dir: root_path.clone(),
        room_id: room.clone(),
        passphrase: passphrase.to_string(),
        folder_key_hash: super::folder_share::folder_key_hash(&folder_id, passphrase),
        access_grant_mode: "shared".to_string(),
        last_cid: None,
        last_folder_signature: None,
        created_at: now.clone(),
        last_published_at: None,
        folders: vec![root_folder_record(
            folder_id.clone(),
            folder_name.clone(),
            room.clone(),
            now.clone(),
        )],
        files: Vec::new(),
        path_index,
    };

    progress(&format!("scanning {}…", root_path.display()));
    let result = rescan_local_dir(&mut entry)?;
    progress(&format!("found {} file(s)", result.upserted_file_ids.len()));

    progress("publishing…");
    republish(&identity, &mut entry, &result).await?;
    let files_published = result.upserted_file_ids.len();

    {
        let _guard = TABLE_LOCK.lock().await;
        let mut table = read_table_file(store).await?;
        if table
            .iter()
            .any(|e| e.local_dir == root_path || e.folder_id == folder_id)
        {
            bail!("{} is already shared", root_path.display());
        }
        table.push(entry.clone());
        write_table_file(store, &table).await?;
    }

    let cid = entry.last_cid.clone().unwrap_or_default();
    let signature = entry.last_folder_signature.clone().unwrap_or_default();
    if let Err(err) =
        announce_publish(&identity, &room, &folder_id, &folder_name, &cid, &signature).await
    {
        progress(&format!(
            "warning: initial announce failed ({err}); the background responder will retry"
        ));
    }

    let share_url = super::sharelink::build_folder_share_link(
        &room,
        &folder_id,
        &folder_name,
        identity.did(),
        "shared",
        &entry.folder_key_hash,
        "mistl",
    )?;

    progress(&format!(
        "shared {folder_name:?}: {files_published} file(s) published"
    ));

    Ok(ShareOutcome {
        share_url,
        folder_id,
        room_id: room,
        files_published,
    })
}

/// All persisted shared folders. The `passphrase` field is blanked (set to
/// an empty string) on every entry before returning -- callers must not
/// treat an empty `passphrase` here as "no passphrase set"; it is always
/// scrubbed for this listing so the daemon's IPC surface (and thus any local
/// client with access to it) never round-trips folder key material that
/// wasn't explicitly requested.
pub async fn list_shared(store: &Store) -> Result<Vec<SharedFolder>> {
    let _guard = TABLE_LOCK.lock().await;
    let mut table = read_table_file(store).await?;
    for entry in &mut table {
        entry.passphrase.clear();
    }
    Ok(table)
}

/// Stop sharing `folder_id`: stop announcing/serving grants and forget the
/// entry (published blobs already resolved by peers remain wherever they
/// were replicated). Returns `false` if no such share exists.
pub async fn unshare(folder_id: &str, store: &Store, _state: &Arc<AppState>) -> Result<bool> {
    let room_id = {
        let _guard = TABLE_LOCK.lock().await;
        let mut table = read_table_file(store).await?;
        let Some(pos) = table.iter().position(|e| e.folder_id == folder_id) else {
            return Ok(false);
        };
        let removed = table.remove(pos);
        write_table_file(store, &table).await?;
        removed.room_id
    };
    let _ = crate::net::leave_room(&room_id).await;
    Ok(true)
}

/// Daemon-lifetime background task: rejoin each shared folder's room,
/// subscribe to [`super::folder_share::envelope_bus`] for
/// `folder-access-request`/`hello`, and run the ~60s rescan/announce loop.
/// A no-op when nothing is shared. Call once from daemon startup.
pub fn spawn_background(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(err) = run_background(state).await {
            warn!(%err, "folder-share (owner): background task failed to start");
        }
    });
}

async fn run_background(state: Arc<AppState>) -> Result<()> {
    let store = super::store(&state).await?;
    let initial = {
        let _guard = TABLE_LOCK.lock().await;
        read_table_file(&store).await?
    };
    if initial.is_empty() {
        return Ok(());
    }
    let identity = crate::identity::current(&state).await?;
    for entry in &initial {
        if let Err(err) = crate::net::ensure_started(&state, entry.room_id.clone()).await {
            warn!(%err, folder_id = %entry.folder_id, room = %entry.room_id, "folder-share (owner): failed to join room");
        }
    }

    let mut rx = super::folder_share::envelope_bus().await.subscribe();
    let mut ticker = tokio::time::interval(RESCAN_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // In-memory only (matches the web app's `folderStateAnnouncementsRef`,
    // itself a plain ref, never persisted): a daemon restart re-announces
    // once, which is harmless.
    let mut last_announced: HashMap<String, (String, std::time::Instant)> = HashMap::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let _guard = TABLE_LOCK.lock().await;
                match read_table_file(&store).await {
                    Ok(mut entries) => {
                        for entry in entries.iter_mut() {
                            if let Err(err) = rescan_and_announce(&identity, entry, &mut last_announced).await {
                                warn!(%err, folder_id = %entry.folder_id, "folder-share (owner): rescan failed");
                            }
                        }
                        if let Err(err) = write_table_file(&store, &entries).await {
                            warn!(%err, "folder-share (owner): failed to persist shared-folder table");
                        }
                    }
                    Err(err) => warn!(%err, "folder-share (owner): failed to load shared-folder table"),
                }
            }
            received = rx.recv() => {
                match received {
                    Ok(envelope) => {
                        if let Err(err) = dispatch_envelope(&store, &identity, &mut last_announced, envelope).await {
                            warn!(%err, "folder-share (owner): error handling envelope");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    Ok(())
}

/// Handles one envelope relevant to the owner role: `folder-access-request`
/// (serve a grant) and `hello` (announce every share in that peer's room
/// immediately, matching a fresh peer's expectation of prompt state without
/// waiting out the rest of the 60s cycle). Reloads the table fresh from disk
/// for both -- see [`TABLE_LOCK`]'s doc comment on why the background loop's
/// in-memory `entries` (owned only by the tick branch) can otherwise be
/// stale relative to a `share`/`unshare` IPC call that landed since the last
/// tick.
async fn dispatch_envelope(
    store: &Store,
    identity: &crate::identity::Identity,
    last_announced: &mut HashMap<String, (String, std::time::Instant)>,
    envelope: super::folder_share::ShareEnvelope,
) -> Result<()> {
    match envelope.type_.as_str() {
        "folder-access-request" => {
            let entries = {
                let _guard = TABLE_LOCK.lock().await;
                read_table_file(store).await?
            };
            handle_access_request(identity, &entries, &envelope).await
        }
        "hello" => {
            let entries = {
                let _guard = TABLE_LOCK.lock().await;
                read_table_file(store).await?
            };
            for entry in entries.iter().filter(|e| e.room_id == envelope.room_id) {
                announce_folder_state(identity, entry, last_announced, true).await?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Mirrors `appAccessActions.ts`'s `handleFolderAccessRequest` +
/// `approveFolderAccess`: validate the request, then auto-approve (mistl's
/// shares are always `access_grant_mode: "shared"`, so no human approval
/// step exists -- the `folderKeyHash` match already proves the requester
/// derived the same key material some other way, e.g. from the share link).
async fn handle_access_request(
    identity: &crate::identity::Identity,
    entries: &[SharedFolder],
    envelope: &super::folder_share::ShareEnvelope,
) -> Result<()> {
    if let Some(target) = envelope.target_node_id.as_deref()
        && target != identity.did()
    {
        return Ok(());
    }
    if !super::folder_share::is_ed25519_did_key(&envelope.from) {
        return Ok(());
    }
    let (Some(folder_id), Some(request_id), Some(access_public_key)) = (
        envelope.folder_id.as_deref(),
        envelope.request_id.as_deref(),
        envelope.access_public_key.as_deref(),
    ) else {
        return Ok(());
    };
    let Some(entry) = entries.iter().find(|e| e.folder_id == folder_id) else {
        return Ok(());
    };
    let hash = envelope.folder_key_hash.as_deref().unwrap_or("");
    if !super::folder_share::matches_folder_key_hash(folder_id, &entry.passphrase, hash) {
        return Ok(());
    }

    let grant = build_access_grant(
        &entry.passphrase,
        folder_id,
        request_id,
        &envelope.from,
        access_public_key,
    )?;
    let response = super::folder_share::ShareEnvelope {
        type_: "folder-access-grant".to_string(),
        from: identity.did().to_string(),
        room_id: entry.room_id.clone(),
        sent_at: now_rfc3339_nanos(),
        clock: chrono::Utc::now().timestamp_millis(),
        folder_id: Some(entry.folder_id.clone()),
        folder_name: Some(entry.folder_name.clone()),
        cid: entry.last_cid.clone(),
        target_node_id: Some(envelope.from.clone()),
        request_id: Some(request_id.to_string()),
        access_grant_proof: Some(grant.proof),
        access_grant_public_key: Some(grant.public_key),
        access_grant_iv: Some(grant.iv),
        access_grant_cipher_text: Some(grant.cipher_text),
        ..super::folder_share::ShareEnvelope::default()
    };
    let signed = super::folder_share::sign_envelope(response, identity)?;
    let bytes = serde_json::to_vec(&signed).context("serializing folder-access-grant envelope")?;
    broadcast_retrying(&entry.room_id, bytes, Duration::from_secs(10)).await
}

/// The pieces of a `folder-access-grant` envelope this module computes,
/// already wire-encoded (see the module doc's "Wire encodings" section).
struct AccessGrant {
    public_key: String,
    iv: String,
    cipher_text: String,
    proof: String,
}

/// Encrypt `passphrase` for the requester identified by
/// `requester_public_key_b64url` (their ephemeral P-256 public key, base64url
/// no-pad SEC1 uncompressed point -- `ShareEnvelope.accessPublicKey`),
/// mirroring `accessGrantCrypto.ts`'s `encryptFolderKeyForRequest`, plus the
/// `folderAccessGrantProof` HMAC.
fn build_access_grant(
    passphrase: &str,
    folder_id: &str,
    request_id: &str,
    target_node_id: &str,
    requester_public_key_b64url: &str,
) -> Result<AccessGrant> {
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

    let peer_raw = URL_SAFE_NO_PAD
        .decode(requester_public_key_b64url.trim_end_matches('='))
        .context("requester access public key")?;
    let peer_public =
        PublicKey::from_sec1_bytes(&peer_raw).context("requester access public key")?;

    let secret = EphemeralSecret::random(&mut rand::rngs::OsRng);
    let our_public = secret.public_key().to_encoded_point(false);
    let shared = secret.diffie_hellman(&peer_public);
    let key_bytes = shared.raw_secret_bytes();

    let mut iv = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut iv);
    let plaintext =
        serde_json::to_vec(&serde_json::json!({ "key": passphrase })).context("grant payload")?;
    let cipher = Aes256Gcm::new_from_slice(key_bytes.as_slice()).context("grant cipher key")?;
    let nonce = Nonce::from_slice(&iv);
    let cipher_text = cipher
        .encrypt(nonce, plaintext.as_slice())
        .map_err(|_| anyhow::anyhow!("encrypting access grant"))?;

    Ok(AccessGrant {
        public_key: URL_SAFE_NO_PAD.encode(our_public.as_bytes()),
        iv: BASE64_STANDARD.encode(iv),
        cipher_text: BASE64_STANDARD.encode(cipher_text),
        proof: folder_access_grant_proof(passphrase, folder_id, request_id, target_node_id),
    })
}

/// `hex(HMAC-SHA256(key = passphrase.trim(), msg =
/// "tc-storage-folder-access-grant-v1\0folderId\0requestId\0targetNodeId"))`,
/// matching `folderKeyProof.ts`'s `folderAccessGrantProof` exactly (hex, not
/// base64 -- see the module doc).
fn folder_access_grant_proof(
    passphrase: &str,
    folder_id: &str,
    request_id: &str,
    target_node_id: &str,
) -> String {
    type HmacSha256 = Hmac<Sha256>;
    let message =
        format!("{ACCESS_GRANT_PROOF_PREFIX}\0{folder_id}\0{request_id}\0{target_node_id}");
    let mut mac = HmacSha256::new_from_slice(passphrase.trim().as_bytes())
        .expect("HMAC-SHA256 accepts any key length");
    mac.update(message.as_bytes());
    let digest = mac.finalize().into_bytes();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Broadcasts `folder-state` for `entry`, unless `force` is false and the
/// signature is unchanged from the last announcement within
/// [`FOLDER_STATE_DEDUP_WINDOW`] (mirrors `shouldSkipFolderStateAnnouncement`
/// in `appFolderSyncActions.ts`; `force` is used for the `hello` case, which
/// the web app achieves indirectly via a changed `audienceKey` -- mistl
/// doesn't track a peer/audience set, so it just always re-announces).
async fn announce_folder_state(
    identity: &crate::identity::Identity,
    entry: &SharedFolder,
    last_announced: &mut HashMap<String, (String, std::time::Instant)>,
    force: bool,
) -> Result<()> {
    let Some(cid) = entry.last_cid.clone() else {
        return Ok(());
    };
    let signature = entry.last_folder_signature.clone().unwrap_or_default();
    if !force && should_skip_folder_state(last_announced, &entry.folder_id, &signature) {
        return Ok(());
    }

    let envelope = super::folder_share::ShareEnvelope {
        type_: "folder-state".to_string(),
        from: identity.did().to_string(),
        room_id: entry.room_id.clone(),
        sent_at: now_rfc3339_nanos(),
        clock: chrono::Utc::now().timestamp_millis(),
        folder_id: Some(entry.folder_id.clone()),
        folder_name: Some(entry.folder_name.clone()),
        cid: Some(cid),
        folder_signature: Some(signature.clone()),
        ..super::folder_share::ShareEnvelope::default()
    };
    let signed = super::folder_share::sign_envelope(envelope, identity)?;
    let bytes = serde_json::to_vec(&signed).context("serializing folder-state envelope")?;
    broadcast_retrying(&entry.room_id, bytes, Duration::from_secs(10)).await?;
    last_announced.insert(
        entry.folder_id.clone(),
        (signature, std::time::Instant::now()),
    );
    Ok(())
}

fn should_skip_folder_state(
    last_announced: &HashMap<String, (String, std::time::Instant)>,
    folder_id: &str,
    signature: &str,
) -> bool {
    last_announced
        .get(folder_id)
        .is_some_and(|(sig, at)| sig == signature && at.elapsed() < FOLDER_STATE_DEDUP_WINDOW)
}

/// Broadcasts a `folder-share` envelope (new/current CID + signature),
/// mirroring `publishSharedFolder`'s final `broadcastShare` call.
async fn announce_publish(
    identity: &crate::identity::Identity,
    room: &str,
    folder_id: &str,
    folder_name: &str,
    cid: &str,
    signature: &str,
) -> Result<()> {
    let envelope = super::folder_share::ShareEnvelope {
        type_: "folder-share".to_string(),
        from: identity.did().to_string(),
        room_id: room.to_string(),
        sent_at: now_rfc3339_nanos(),
        clock: chrono::Utc::now().timestamp_millis(),
        folder_id: Some(folder_id.to_string()),
        folder_name: Some(folder_name.to_string()),
        cid: Some(cid.to_string()),
        folder_signature: Some(signature.to_string()),
        ..super::folder_share::ShareEnvelope::default()
    };
    let signed = super::folder_share::sign_envelope(envelope, identity)?;
    let bytes = serde_json::to_vec(&signed).context("serializing folder-share envelope")?;
    broadcast_retrying(room, bytes, Duration::from_secs(10)).await
}

/// Broadcasts one incremental `folder-change` envelope for `file`, mirroring
/// `announceFolderChange`'s `file-upserted`/`file-deleted` usage in
/// `appFolderSyncActions.ts` (`cid: file?.lastCid`, `file:
/// stripFileContent(file)`).
async fn announce_file_change(
    identity: &crate::identity::Identity,
    entry: &SharedFolder,
    change_type: &str,
    file: &super::domain::FileRecord,
) -> Result<()> {
    let envelope = super::folder_share::ShareEnvelope {
        type_: "folder-change".to_string(),
        from: identity.did().to_string(),
        room_id: entry.room_id.clone(),
        sent_at: now_rfc3339_nanos(),
        clock: chrono::Utc::now().timestamp_millis(),
        change_type: Some(change_type.to_string()),
        folder_id: Some(entry.folder_id.clone()),
        folder_name: Some(entry.folder_name.clone()),
        file_id: Some(file.id.clone()),
        file_name: Some(file.name.clone()),
        file: Some(strip_file_content(file)),
        cid: file.last_cid.clone(),
        ..super::folder_share::ShareEnvelope::default()
    };
    let signed = super::folder_share::sign_envelope(envelope, identity)?;
    let bytes = serde_json::to_vec(&signed).context("serializing folder-change envelope")?;
    broadcast_retrying(&entry.room_id, bytes, Duration::from_secs(10)).await
}

fn strip_file_content(file: &super::domain::FileRecord) -> super::domain::FileRecord {
    super::domain::FileRecord {
        data_url: None,
        ..file.clone()
    }
}

/// Send `bytes` to `room` as a broadcast, retrying on failure (in particular
/// mistlib's "Room not joined" error while an async room join from `hello`'s
/// or a fresh share's `ensure_started` is still in flight) until `timeout`
/// elapses. Mirrors [`super::folder_share::send_bytes_retrying`]'s pattern,
/// but for `send_broadcast` -- every `ShareEnvelope` type this module emits
/// is broadcast, not unicast (recipient selection happens receive-side via
/// `targetNodeId`, same as the web app's `broadcastShare`; see
/// `folder_share`'s module doc on node-id vs DID addressing).
async fn broadcast_retrying(room: &str, bytes: Vec<u8>, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match crate::net::send_broadcast(room, bytes.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Rescans `entry.local_dir`, republishes on change, and announces either
/// way (a full `folder-share` republish, or a deduped `folder-state`).
async fn rescan_and_announce(
    identity: &crate::identity::Identity,
    entry: &mut SharedFolder,
    last_announced: &mut HashMap<String, (String, std::time::Instant)>,
) -> Result<()> {
    let result = rescan_local_dir(entry)?;
    if !result.changed {
        announce_folder_state(identity, entry, last_announced, false).await?;
        return Ok(());
    }

    republish(identity, entry, &result).await?;

    let folder_id = entry.folder_id.clone();
    let folder_name = entry.folder_name.clone();
    let room = entry.room_id.clone();
    let cid = entry.last_cid.clone().unwrap_or_default();
    let signature = entry.last_folder_signature.clone().unwrap_or_default();
    announce_publish(identity, &room, &folder_id, &folder_name, &cid, &signature).await?;
    last_announced.insert(folder_id, (signature, std::time::Instant::now()));

    let upserted: Vec<super::domain::FileRecord> = result
        .upserted_file_ids
        .iter()
        .filter_map(|id| entry.files.iter().find(|f| &f.id == id).cloned())
        .collect();
    let deleted: Vec<super::domain::FileRecord> = result
        .deleted_file_ids
        .iter()
        .filter_map(|id| entry.files.iter().find(|f| &f.id == id).cloned())
        .collect();
    for file in &upserted {
        announce_file_change(identity, entry, "file-upserted", file).await?;
    }
    for file in &deleted {
        announce_file_change(identity, entry, "file-deleted", file).await?;
    }
    Ok(())
}

/// Encrypts and publishes (via `folder_share::p2p_storage_add`) a
/// `FileBundle` for every file in `result.upserted_file_ids`, then a fresh
/// `FolderBundle` covering the whole tree, updating `entry`'s records/CIDs
/// in place. Used both for the very first publish (`share_folder`, where
/// every file is "upserted") and for incremental republishes, so the two
/// code paths can't drift apart.
async fn republish(
    identity: &crate::identity::Identity,
    entry: &mut SharedFolder,
    result: &RescanResult,
) -> Result<()> {
    let now = now_rfc3339();
    let root_record = entry
        .folders
        .iter()
        .find(|f| f.id == entry.folder_id)
        .cloned()
        .context("shared folder is missing its own root folder record")?;

    for file_id in &result.upserted_file_ids {
        let Some(data) = result.new_file_bytes.get(file_id) else {
            continue;
        };
        let Some(record) = entry.files.iter().find(|f| &f.id == file_id).cloned() else {
            continue;
        };
        let data_url = format!(
            "data:{};base64,{}",
            record.mime_type,
            BASE64_STANDARD.encode(data)
        );
        let bundle_file = super::domain::FileRecord {
            data_url: Some(data_url),
            ..record
        };
        let file_bundle = super::domain::FileBundle {
            version: 1,
            exported_at: now.clone(),
            origin_node: identity.did().to_string(),
            folder: root_record.clone(),
            file: bundle_file,
        };
        let encrypted = super::crypto::encrypt_json(&file_bundle, &entry.passphrase)?;
        let bytes = serde_json::to_vec(&encrypted).context("serializing file bundle")?;
        let cid =
            super::folder_share::p2p_storage_add(&format!("{file_id}.tc-file.enc.json"), bytes)
                .await?;
        if let Some(record_mut) = entry.files.iter_mut().find(|f| &f.id == file_id) {
            record_mut.last_cid = Some(cid.clone());
            record_mut.last_share_cid = Some(cid);
        }
    }

    let folder_bundle = super::domain::FolderBundle {
        version: 1,
        exported_at: now.clone(),
        origin_node: identity.did().to_string(),
        folder: root_record,
        folders: Some(entry.folders.clone()),
        files: entry.files.clone(),
    };
    let encrypted = super::crypto::encrypt_json(&folder_bundle, &entry.passphrase)?;
    let bytes = serde_json::to_vec(&encrypted).context("serializing folder bundle")?;
    let cid = super::folder_share::p2p_storage_add(
        &format!("{}.tc-folder.enc.json", entry.folder_id),
        bytes,
    )
    .await?;

    entry.last_cid = Some(cid);
    entry.last_folder_signature = Some(super::folder_sync::folder_signature(
        &entry.folder_id,
        &entry.folders,
        &entry.files,
    ));
    entry.last_published_at = Some(now);
    Ok(())
}

/// The outcome of one [`rescan_local_dir`] pass.
#[derive(Default)]
struct RescanResult {
    /// True if anything (a file's content, a file/folder appearing, or a
    /// file/folder disappearing) differs from `entry`'s last scan.
    changed: bool,
    /// File ids that are new or whose content changed -- need a fresh
    /// `FileBundle` publish.
    upserted_file_ids: Vec<String>,
    /// File ids that vanished from disk (tombstoned in `entry.files`, not
    /// removed).
    deleted_file_ids: Vec<String>,
    /// Raw bytes for every id in `upserted_file_ids`, read once here so
    /// [`republish`] doesn't re-read them from disk.
    new_file_bytes: HashMap<String, Vec<u8>>,
}

/// Walks `entry.local_dir`, updates `entry.folders`/`entry.files`/
/// `entry.path_index` in place (new folders/files get fresh ids; paths that
/// still exist keep their id; paths that vanished are tombstoned via
/// `deletedAt` rather than removed, matching `saveEncryptedFolderToMist`'s
/// wire shape), and reports what changed. Pure filesystem logic, no network
/// calls -- publishing the result is [`republish`]'s job.
fn rescan_local_dir(entry: &mut SharedFolder) -> Result<RescanResult> {
    let (walked_dirs, walked_files) = walk_shared_dir(&entry.local_dir)?;
    let now = now_rfc3339();
    let mut result = RescanResult::default();

    // -- folders: new subdirectories get fresh ids; vanished ones are tombstoned.
    let mut current_folder_paths: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    current_folder_paths.insert(String::new());
    for dir in walked_dirs.iter().filter(|d| !d.rel_path.is_empty()) {
        current_folder_paths.insert(dir.rel_path.clone());
        if entry.path_index.folder_ids.contains_key(&dir.rel_path) {
            continue;
        }
        let id = generate_uuid_v4();
        let parent_id = entry
            .path_index
            .folder_ids
            .get(dir.parent_rel_path.as_deref().unwrap_or(""))
            .cloned();
        entry
            .path_index
            .folder_ids
            .insert(dir.rel_path.clone(), id.clone());
        entry.folders.push(super::domain::FolderRecord {
            id,
            name: super::folder_share::sanitize_name(&dir.name),
            parent_id,
            sort_order: None,
            color: "slate".to_string(),
            encrypted: true,
            share_enabled: false,
            shared_room_id: String::new(),
            last_cid: None,
            last_saved_at: None,
            last_shared_at: None,
            deleted_at: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            field_versions: None,
        });
        result.changed = true;
    }
    let vanished_folder_paths: Vec<String> = entry
        .path_index
        .folder_ids
        .keys()
        .filter(|p| !p.is_empty() && !current_folder_paths.contains(*p))
        .cloned()
        .collect();
    for path in &vanished_folder_paths {
        if let Some(id) = entry.path_index.folder_ids.remove(path)
            && let Some(record) = entry.folders.iter_mut().find(|f| f.id == id)
            && record.deleted_at.is_none()
        {
            record.deleted_at = Some(now.clone());
            record.updated_at = now.clone();
            result.changed = true;
        }
    }

    // -- files: fast path on (mtime, size); sha256-confirm otherwise.
    let mut current_file_paths: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for wf in &walked_files {
        current_file_paths.insert(wf.rel_path.clone());
        let folder_id = entry
            .path_index
            .folder_ids
            .get(&wf.folder_rel_path)
            .cloned()
            .unwrap_or_else(|| entry.folder_id.clone());

        if let Some((cached_mtime, cached_size)) =
            entry.path_index.file_stats.get(&wf.rel_path).copied()
            && cached_mtime == wf.mtime_millis
            && cached_size == wf.size
            && entry.path_index.file_ids.contains_key(&wf.rel_path)
        {
            continue; // unchanged: mtime+size fast path, no re-hash needed
        }

        let data = std::fs::read(&wf.abs_path)
            .with_context(|| format!("reading {}", wf.abs_path.display()))?;
        let checksum = sha256_hex(&data);
        entry
            .path_index
            .file_stats
            .insert(wf.rel_path.clone(), (wf.mtime_millis, wf.size));

        if let Some(existing_id) = entry.path_index.file_ids.get(&wf.rel_path).cloned() {
            let Some(record) = entry.files.iter_mut().find(|f| f.id == existing_id) else {
                continue;
            };
            if record.checksum == checksum && record.deleted_at.is_none() {
                continue; // content confirmed unchanged despite a differing mtime
            }
            record.checksum = checksum;
            record.size = data.len() as i64;
            record.mime_type = guess_mime(&wf.name).to_string();
            record.version += 1;
            record.updated_at = now.clone();
            record.deleted_at = None;
            record.folder_id = folder_id;
            result.new_file_bytes.insert(existing_id.clone(), data);
            result.upserted_file_ids.push(existing_id);
            result.changed = true;
        } else {
            let id = generate_uuid_v4();
            entry
                .path_index
                .file_ids
                .insert(wf.rel_path.clone(), id.clone());
            entry.files.push(super::domain::FileRecord {
                id: id.clone(),
                folder_id,
                sort_order: None,
                name: wf.name.clone(),
                mime_type: guess_mime(&wf.name).to_string(),
                size: data.len() as i64,
                data_url: None,
                checksum,
                version: 1,
                starred: false,
                last_cid: None,
                last_share_cid: None,
                deleted_at: None,
                created_at: now.clone(),
                updated_at: now.clone(),
                field_versions: None,
            });
            result.new_file_bytes.insert(id.clone(), data);
            result.upserted_file_ids.push(id);
            result.changed = true;
        }
    }
    let vanished_file_paths: Vec<String> = entry
        .path_index
        .file_ids
        .keys()
        .filter(|p| !current_file_paths.contains(*p))
        .cloned()
        .collect();
    for path in &vanished_file_paths {
        if let Some(id) = entry.path_index.file_ids.remove(path) {
            entry.path_index.file_stats.remove(path);
            if let Some(record) = entry.files.iter_mut().find(|f| f.id == id)
                && record.deleted_at.is_none()
            {
                record.deleted_at = Some(now.clone());
                record.updated_at = now.clone();
                result.deleted_file_ids.push(id);
                result.changed = true;
            }
        }
    }

    Ok(result)
}

fn root_folder_record(
    folder_id: String,
    folder_name: String,
    room: String,
    now: String,
) -> super::domain::FolderRecord {
    super::domain::FolderRecord {
        id: folder_id,
        name: folder_name,
        parent_id: None,
        sort_order: None,
        color: "slate".to_string(),
        encrypted: true,
        share_enabled: true,
        shared_room_id: room,
        last_cid: None,
        last_saved_at: None,
        last_shared_at: None,
        deleted_at: None,
        created_at: now.clone(),
        updated_at: now,
        field_versions: None,
    }
}

/// One directory found while walking a shared tree (the synthetic root has
/// `rel_path == ""`).
struct WalkedDir {
    rel_path: String,
    parent_rel_path: Option<String>,
    name: String,
}

/// One file found while walking a shared tree.
struct WalkedFile {
    rel_path: String,
    folder_rel_path: String,
    name: String,
    abs_path: PathBuf,
    size: u64,
    mtime_millis: i64,
}

/// Recursively lists every directory and file under `root`, skipping
/// symlinks entirely (both to avoid publishing content outside the share
/// root through a symlink and to sidestep cycle detection -- a sane default
/// per the module's design brief, since a real synced share has no
/// legitimate reason to contain one). Deterministic order (sorted by file
/// name at each level) so a rescan's diff is stable.
fn walk_shared_dir(root: &Path) -> Result<(Vec<WalkedDir>, Vec<WalkedFile>)> {
    let mut dirs = vec![WalkedDir {
        rel_path: String::new(),
        parent_rel_path: None,
        name: String::new(),
    }];
    let mut files = Vec::new();
    walk_recursive(root, "", &mut dirs, &mut files)?;
    Ok((dirs, files))
}

fn walk_recursive(
    current: &Path,
    current_rel: &str,
    dirs: &mut Vec<WalkedDir>,
    files: &mut Vec<WalkedFile>,
) -> Result<()> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(current)
        .with_context(|| format!("reading directory {}", current.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("reading directory {}", current.display()))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for dir_entry in entries {
        let file_type = dir_entry
            .file_type()
            .with_context(|| format!("stat {}", dir_entry.path().display()))?;
        if file_type.is_symlink() {
            continue;
        }
        let name = dir_entry.file_name().to_string_lossy().into_owned();
        let rel_path = if current_rel.is_empty() {
            name.clone()
        } else {
            format!("{current_rel}/{name}")
        };
        if file_type.is_dir() {
            dirs.push(WalkedDir {
                rel_path: rel_path.clone(),
                parent_rel_path: Some(current_rel.to_string()),
                name: name.clone(),
            });
            walk_recursive(&dir_entry.path(), &rel_path, dirs, files)?;
        } else if file_type.is_file() {
            let metadata = dir_entry
                .metadata()
                .with_context(|| format!("stat {}", dir_entry.path().display()))?;
            let mtime_millis = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            files.push(WalkedFile {
                rel_path,
                folder_rel_path: current_rel.to_string(),
                name,
                abs_path: dir_entry.path(),
                size: metadata.len(),
                mtime_millis,
            });
        }
        // else: skip other exotic entry kinds (fifos, devices, ...).
    }
    Ok(())
}

/// Generate a random UUIDv4-format string
/// (`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`, version nibble `4`, variant bits
/// `10`) for folder/file ids -- there is no `uuid` crate dependency in this
/// workspace, so this is a small self-contained generator over 16 random
/// bytes, matching what `crypto.randomUUID()` produces shape-wise (tc-storage
/// doesn't require ids to be RFC-4122-valid UUIDs specifically, just unique
/// strings, but matching the shape costs nothing and avoids surprises in any
/// tooling that assumes it).
fn generate_uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

/// `createdAt`/`updatedAt`/`exportedAt` timestamp: millisecond-precision
/// RFC3339 with a `Z` suffix, matching JS's `new Date().toISOString()`
/// (which tc-storage uses for these fields).
fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// `ShareEnvelope.sentAt` timestamp: nanosecond-precision RFC3339, matching
/// [`super::folder_share`]'s own envelope-building convention (see
/// `fetch_folder_share`'s `sent_at` construction).
fn now_rfc3339_nanos() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal self-cleaning temp directory, mirroring the one in
    /// `super::super::tests` (that one is private to `mod.rs`'s own test
    /// module, so this is a small intentional duplicate rather than a public
    /// shared helper -- the `tempfile` crate is only a transitive
    /// dependency, not one this crate can `use` directly).
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mistl-folder-owner-test-{label}-{}-{nanos}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn test_store(dir: &Path) -> Store {
        let cfg = crate::config::StorageConfig {
            blocks_dir: None,
            capacity_bytes: 10 * 1024 * 1024 * 1024,
            room_ids: Vec::new(),
            export_dir: None,
        };
        Store::open(&cfg, dir.to_path_buf())
            .await
            .expect("open store")
    }

    fn test_entry(local_dir: PathBuf, passphrase: &str) -> SharedFolder {
        let folder_id = generate_uuid_v4();
        let now = now_rfc3339();
        let mut path_index = PathIndex::default();
        path_index
            .folder_ids
            .insert(String::new(), folder_id.clone());
        SharedFolder {
            folder_id: folder_id.clone(),
            folder_name: "Test Folder".to_string(),
            local_dir,
            room_id: "room-1".to_string(),
            passphrase: passphrase.to_string(),
            folder_key_hash: super::super::folder_share::folder_key_hash(&folder_id, passphrase),
            access_grant_mode: "shared".to_string(),
            last_cid: None,
            last_folder_signature: None,
            created_at: now.clone(),
            last_published_at: None,
            folders: vec![root_folder_record(
                folder_id,
                "Test Folder".to_string(),
                "room-1".to_string(),
                now,
            )],
            files: Vec::new(),
            path_index,
        }
    }

    // -- uuid format --------------------------------------------------

    #[test]
    fn uuid_v4_has_expected_format_and_is_random() {
        let a = generate_uuid_v4();
        let b = generate_uuid_v4();
        assert_ne!(a, b);
        for id in [&a, &b] {
            let parts: Vec<&str> = id.split('-').collect();
            assert_eq!(parts.len(), 5, "{id}");
            assert_eq!(
                [
                    parts[0].len(),
                    parts[1].len(),
                    parts[2].len(),
                    parts[3].len(),
                    parts[4].len()
                ],
                [8, 4, 4, 4, 12],
                "{id}"
            );
            assert!(
                id.chars().all(|c| c.is_ascii_hexdigit() || c == '-'),
                "{id}"
            );
            assert!(parts[2].starts_with('4'), "version nibble: {id}");
            assert!(
                matches!(parts[3].chars().next(), Some('8' | '9' | 'a' | 'b')),
                "variant bits: {id}"
            );
        }
    }

    // -- mime map -------------------------------------------------------

    #[test]
    fn mime_guess_matches_known_extensions_and_falls_back() {
        assert_eq!(guess_mime("photo.png"), "image/png");
        assert_eq!(guess_mime("Notes.TXT"), "text/plain");
        assert_eq!(guess_mime("archive.zip"), "application/zip");
        assert_eq!(guess_mime("mystery.bin"), "application/octet-stream");
        assert_eq!(guess_mime("no-extension"), "application/octet-stream");
    }

    // -- record building from a temp dir --------------------------------

    #[test]
    fn rescan_builds_records_from_a_directory_tree() {
        let dir = TempDir::new("build");
        std::fs::write(dir.path().join("root.txt"), b"hello").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("nested.txt"), b"world").unwrap();

        let mut entry = test_entry(dir.path().to_path_buf(), "correct horse battery staple");
        let result = rescan_local_dir(&mut entry).unwrap();

        assert!(result.changed);
        assert_eq!(result.upserted_file_ids.len(), 2);
        assert_eq!(entry.files.len(), 2);
        assert_eq!(entry.folders.len(), 2, "root + sub");

        let sub = entry.folders.iter().find(|f| f.name == "sub").unwrap();
        assert_eq!(sub.parent_id.as_deref(), Some(entry.folder_id.as_str()));

        let root_file = entry.files.iter().find(|f| f.name == "root.txt").unwrap();
        assert_eq!(root_file.folder_id, entry.folder_id);
        assert_eq!(root_file.checksum, sha256_hex(b"hello"));
        assert_eq!(root_file.mime_type, "text/plain");

        let nested = entry.files.iter().find(|f| f.name == "nested.txt").unwrap();
        assert_eq!(nested.folder_id, sub.id);
        assert_eq!(nested.checksum, sha256_hex(b"world"));
        assert_eq!(nested.version, 1);
        assert!(nested.deleted_at.is_none());
    }

    // -- path -> id stability across rescans -----------------------------

    #[test]
    fn rescan_preserves_ids_for_unchanged_paths_and_detects_changes() {
        let dir = TempDir::new("stability");
        let file_path = dir.path().join("a.txt");
        std::fs::write(&file_path, b"v1").unwrap();
        let mut entry = test_entry(dir.path().to_path_buf(), "pw");

        let first = rescan_local_dir(&mut entry).unwrap();
        assert_eq!(first.upserted_file_ids.len(), 1);
        let id_before = entry.files[0].id.clone();
        let version_before = entry.files[0].version;

        // Unchanged rescan: same id, nothing reported as changed.
        let second = rescan_local_dir(&mut entry).unwrap();
        assert!(!second.changed);
        assert_eq!(entry.files[0].id, id_before);

        // Content change: same id, version bumped, reported as upserted.
        std::fs::write(&file_path, b"v2-longer").unwrap();
        let third = rescan_local_dir(&mut entry).unwrap();
        assert!(third.changed);
        assert_eq!(third.upserted_file_ids, vec![id_before.clone()]);
        assert_eq!(entry.files[0].id, id_before);
        assert_eq!(entry.files[0].version, version_before + 1);
        assert_eq!(entry.files[0].checksum, sha256_hex(b"v2-longer"));

        // Deletion: tombstoned (kept, `deletedAt` set), not removed.
        std::fs::remove_file(&file_path).unwrap();
        let fourth = rescan_local_dir(&mut entry).unwrap();
        assert!(fourth.changed);
        assert_eq!(fourth.deleted_file_ids, vec![id_before.clone()]);
        assert_eq!(entry.files.len(), 1, "tombstoned, not removed");
        assert!(entry.files[0].deleted_at.is_some());

        // A rescan after deletion (nothing else changed) reports no further change.
        let fifth = rescan_local_dir(&mut entry).unwrap();
        assert!(!fifth.changed);
    }

    // -- grant crypto roundtrip (wire compat proof) ----------------------

    #[test]
    fn access_grant_roundtrips_with_the_requester_side_decrypt() {
        let passphrase = "shared secret passphrase";
        let folder_id = "folder-xyz";
        let request_id = "access-123";
        let target_node_id = "did:key:zTargetRequester";

        // Requester side: the exact key-generation helper
        // `folder_share::fetch_folder_share` uses for a real request.
        let requester = super::super::folder_share::create_access_request_key();

        // Owner side: this module's grant builder.
        let grant = build_access_grant(
            passphrase,
            folder_id,
            request_id,
            target_node_id,
            &requester.public_b64url,
        )
        .unwrap();

        assert_eq!(grant.proof.len(), 64);
        assert!(grant.proof.chars().all(|c| c.is_ascii_hexdigit()));

        // Requester decrypts with the exact function `folder_share` uses on
        // a real `folder-access-grant` envelope -- this is the strongest
        // available offline proof of wire compatibility.
        let decrypted = super::super::folder_share::decrypt_folder_key_grant(
            &grant.cipher_text,
            &grant.iv,
            &requester.secret,
            &grant.public_key,
        )
        .unwrap();
        assert_eq!(decrypted, passphrase);
    }

    // -- persistence roundtrip -------------------------------------------

    #[tokio::test]
    async fn shared_folder_table_persists_across_store_instances() {
        let dir = TempDir::new("table-persist");
        let store = test_store(dir.path()).await;
        let entry = test_entry(PathBuf::from("/tmp/example"), "pw");
        let folder_id = entry.folder_id.clone();

        write_table_file(&store, std::slice::from_ref(&entry))
            .await
            .unwrap();

        let store2 = test_store(dir.path()).await;
        let loaded = read_table_file(&store2).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].folder_id, folder_id);
        assert_eq!(
            loaded[0].passphrase, "pw",
            "raw table read keeps the passphrase"
        );
    }

    #[tokio::test]
    async fn list_shared_blanks_the_passphrase() {
        let dir = TempDir::new("blank");
        let store = test_store(dir.path()).await;
        let entry = test_entry(PathBuf::from("/tmp/example"), "super-secret");
        write_table_file(&store, std::slice::from_ref(&entry))
            .await
            .unwrap();

        let listed = list_shared(&store).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert!(
            listed[0].passphrase.is_empty(),
            "passphrase must be blanked in list_shared output"
        );
    }

    // `unshare` itself isn't exercised end-to-end here: it needs a real
    // `Arc<AppState>` (for signature parity with the other `store.*`
    // handlers), and `AppState`'s fields are private to `daemon` and its
    // descendants -- `storage` is a sibling module tree, so there is no way
    // to construct one from here without editing `daemon/mod.rs` (out of
    // scope for this file). `unshare`'s only real logic -- find-by-id,
    // remove, persist -- is the same read/write path already covered by
    // `shared_folder_table_persists_across_store_instances` above; only the
    // (untestable-from-here) `state`-typed parameter and the `leave_room`
    // side effect are unverified by a unit test.

    // -- folder-state dedup window ----------------------------------------

    #[test]
    fn folder_state_dedup_skips_within_window_and_allows_after() {
        let mut last_announced: HashMap<String, (String, std::time::Instant)> = HashMap::new();
        let folder_id = "f1".to_string();
        let sig = "sig-a".to_string();

        assert!(!should_skip_folder_state(&last_announced, &folder_id, &sig));
        last_announced.insert(folder_id.clone(), (sig.clone(), std::time::Instant::now()));

        assert!(should_skip_folder_state(&last_announced, &folder_id, &sig));
        assert!(!should_skip_folder_state(
            &last_announced,
            &folder_id,
            "sig-b"
        ));

        last_announced.insert(
            folder_id.clone(),
            (
                sig.clone(),
                std::time::Instant::now() - FOLDER_STATE_DEDUP_WINDOW - Duration::from_millis(1),
            ),
        );
        assert!(!should_skip_folder_state(&last_announced, &folder_id, &sig));
    }
}
