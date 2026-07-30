//! Identity: user profile + `did:key` Ed25519 key management.
//!
//! Interop contract (matches tc-storage `src/crypto/didIdentity.ts`):
//! - DID = `did:key:z<base58btc(0xed01 multicodec prefix + raw ed25519 pubkey)>`
//! - Private key persisted as PKCS8 in `<data_dir>/identity/`
//!
//! Other modules depend on the exact signatures of [`Identity`] and
//! [`current`]; do not change them without updating callers.

pub mod crypto;
mod delegation;
mod pairing;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;

use crate::config;
use crate::daemon::AppState;

/// Multicodec prefix for an Ed25519 public key (`0xed 0x01`), per the
/// did:key spec and tc-storage's `ed25519PublicKeyMulticodec`.
const MULTICODEC_ED25519_PUB: [u8; 2] = [0xed, 0x01];

/// Lazily-loaded singleton identity for this daemon process.
static IDENTITY: OnceCell<Arc<Identity>> = OnceCell::const_new();

/// The local user's DID identity (Ed25519 keypair + profile).
pub struct Identity {
    did: String,
    signing_key: SigningKey,
    created_at: String,
}

impl Identity {
    /// The `did:key:z6Mk...` string for this identity.
    pub fn did(&self) -> &str {
        &self.did
    }

    /// Stable node id used with mistlib (derived from the DID): the first 16
    /// hex characters of `sha256(did)`. This exact derivation is a
    /// cross-module contract shared with `crate::mailbox`.
    pub fn node_id(&self) -> String {
        node_id_for_did(&self.did)
    }

    /// Sign arbitrary bytes with the Ed25519 key, returning the raw 64-byte
    /// signature.
    pub fn sign(&self, data: &[u8]) -> Vec<u8> {
        self.signing_key.sign(data).to_bytes().to_vec()
    }

    /// The Ed25519 public key backing this identity.
    #[allow(dead_code)]
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// RFC 3339 timestamp recorded when this identity was first generated.
    pub fn created_at(&self) -> &str {
        &self.created_at
    }
}

/// Derive the mistlib node id from a DID: first 16 hex chars of `sha256(did)`.
fn node_id_for_did(did: &str) -> String {
    let digest = Sha256::digest(did.as_bytes());
    digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// Verify an Ed25519 signature against a `did:key` DID.
pub fn verify(did: &str, data: &[u8], signature: &[u8]) -> Result<bool> {
    let pubkey_bytes = pubkey_from_did(did)?;
    let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|e| anyhow!("invalid ed25519 public key: {e}"))?;
    let signature =
        Signature::try_from(signature).map_err(|e| anyhow!("invalid signature: {e}"))?;
    Ok(verifying_key.verify(data, &signature).is_ok())
}

/// Extract the raw 32-byte Ed25519 public key from a `did:key:z...` string.
///
/// `pub(crate)` (rather than private) so [`delegation`] and [`pairing`] can
/// reuse it to validate a delegation's `leaf`/`root` without re-implementing
/// did:key decoding -- there must be exactly one did:key implementation in
/// this crate.
pub(crate) fn pubkey_from_did(did: &str) -> Result<[u8; 32]> {
    let multibase = did.strip_prefix("did:key:").context("not a did:key DID")?;
    let encoded = multibase
        .strip_prefix('z')
        .context("expected base58btc multibase ('z' prefix)")?;
    let bytes = bs58::decode(encoded)
        .into_vec()
        .map_err(|e| anyhow!("invalid base58btc encoding: {e}"))?;
    if bytes.len() != 34
        || bytes[0] != MULTICODEC_ED25519_PUB[0]
        || bytes[1] != MULTICODEC_ED25519_PUB[1]
    {
        bail!("DID key is not an Ed25519 public key");
    }
    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(&bytes[2..]);
    Ok(pubkey)
}

/// Build a `did:key:z...` string from a raw 32-byte Ed25519 public key.
fn did_from_pubkey(pubkey: &[u8; 32]) -> String {
    let mut bytes = Vec::with_capacity(34);
    bytes.extend_from_slice(&MULTICODEC_ED25519_PUB);
    bytes.extend_from_slice(pubkey);
    format!("did:key:z{}", bs58::encode(bytes).into_string())
}

#[derive(Debug, Serialize, Deserialize)]
struct IdentityMeta {
    did: String,
    method: String,
    key_type: String,
    public_key_multibase: String,
    created_at: String,
}

fn identity_dir() -> Result<PathBuf> {
    let dir = config::data_dir()?.join("identity");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

fn key_path() -> Result<PathBuf> {
    Ok(identity_dir()?.join("key.pkcs8.b64"))
}

fn meta_path() -> Result<PathBuf> {
    Ok(identity_dir()?.join("meta.json"))
}

fn profile_path() -> Result<PathBuf> {
    Ok(identity_dir()?.join("profile.json"))
}

/// Load the identity from disk, if one has already been generated.
fn load_identity() -> Result<Option<Identity>> {
    let key_path = key_path()?;
    let meta_path = meta_path()?;
    if !key_path.exists() || !meta_path.exists() {
        return Ok(None);
    }

    let key_b64 = std::fs::read_to_string(&key_path)
        .with_context(|| format!("reading {}", key_path.display()))?;
    let pkcs8_bytes = BASE64
        .decode(key_b64.trim())
        .map_err(|e| anyhow!("invalid PKCS8 base64 in {}: {e}", key_path.display()))?;
    let signing_key = SigningKey::from_pkcs8_der(&pkcs8_bytes)
        .map_err(|e| anyhow!("invalid PKCS8 key in {}: {e}", key_path.display()))?;

    let meta_text = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("reading {}", meta_path.display()))?;
    let meta: IdentityMeta = serde_json::from_str(&meta_text)
        .with_context(|| format!("parsing {}", meta_path.display()))?;

    let expected_did = did_from_pubkey(&signing_key.verifying_key().to_bytes());
    if meta.did != expected_did {
        bail!(
            "identity meta at {} does not match the stored key ({} != {})",
            meta_path.display(),
            meta.did,
            expected_did
        );
    }

    Ok(Some(Identity {
        did: meta.did,
        signing_key,
        created_at: meta.created_at,
    }))
}

fn save_key(signing_key: &SigningKey) -> Result<()> {
    let der = signing_key
        .to_pkcs8_der()
        .map_err(|e| anyhow!("encoding PKCS8 key: {e}"))?;
    let encoded = BASE64.encode(der.as_bytes());
    let path = key_path()?;
    std::fs::write(&path, encoded).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn save_meta(meta: &IdentityMeta) -> Result<()> {
    let path = meta_path()?;
    std::fs::write(&path, serde_json::to_string_pretty(meta)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Generate a brand-new identity, persist it, and seed the profile with the
/// configured display name (if any) on first run.
fn generate_identity(state: &AppState) -> Result<Identity> {
    let signing_key = SigningKey::generate(&mut OsRng);
    let did = did_from_pubkey(&signing_key.verifying_key().to_bytes());
    let created_at = Utc::now().to_rfc3339();

    save_key(&signing_key)?;
    save_meta(&IdentityMeta {
        did: did.clone(),
        method: "did:key".into(),
        key_type: "Ed25519".into(),
        public_key_multibase: did.strip_prefix("did:key:").unwrap_or(&did).to_string(),
        created_at: created_at.clone(),
    })?;

    if !profile_path()?.exists() {
        let mut profile = Profile::default();
        if let Some(name) = &state.config().identity.display_name {
            profile.display_name = Some(name.clone());
        }
        profile.updated_at = Some(created_at.clone());
        save_profile(&profile)?;
    }

    Ok(Identity {
        did,
        signing_key,
        created_at,
    })
}

async fn load_or_create(state: &AppState) -> Result<Arc<Identity>> {
    if let Some(identity) = load_identity()? {
        return Ok(Arc::new(identity));
    }
    Ok(Arc::new(generate_identity(state)?))
}

/// Get (lazily loading or generating on first call) the daemon's identity.
pub async fn current(state: &AppState) -> Result<Arc<Identity>> {
    let identity = IDENTITY.get_or_try_init(|| load_or_create(state)).await?;
    Ok(Arc::clone(identity))
}

/// User profile: a few well-known fields plus an open bag of extra string
/// fields (`profile.set` accepts any field name).
///
/// This is the interop document sibling apps (tc-chat, tc-storage) read: it
/// is persisted verbatim at `<data_dir>/identity/profile.json` and returned
/// (merged with `did`) by `profile.show`. See the "Profile document" section
/// in the README for the stable on-the-wire shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Profile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
    /// Root CID (in the content store, `store.put`) of the profile image.
    /// Any ecosystem peer holding the block -- or able to resolve it over the
    /// shared CID-addressed store -- can fetch the avatar bytes by this CID,
    /// which keeps the profile portable without inlining image data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_cid: Option<String>,
    /// RFC 3339 timestamp of the last `profile.set`, so a peer that holds
    /// several observed copies of a profile can pick the freshest one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, String>,
}

impl Profile {
    /// Set (or, when `value` is empty, clear) a profile field. Known fields
    /// are typed; anything else is kept as a free-form string in `extra`.
    fn set_field(&mut self, field: &str, value: String) {
        let value = if value.is_empty() { None } else { Some(value) };
        match field {
            "display_name" => self.display_name = value,
            "bio" => self.bio = value,
            "avatar_cid" => self.avatar_cid = value,
            other => match value {
                Some(v) => {
                    self.extra.insert(other.to_string(), v);
                }
                None => {
                    self.extra.remove(other);
                }
            },
        }
    }
}

fn load_profile() -> Result<Profile> {
    let path = profile_path()?;
    if !path.exists() {
        return Ok(Profile::default());
    }
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

fn save_profile(profile: &Profile) -> Result<()> {
    let path = profile_path()?;
    std::fs::write(&path, serde_json::to_string_pretty(profile)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Merge a profile into `{ ...profile, did }`.
fn profile_with_did(profile: &Profile, did: &str) -> Result<Value> {
    let mut value = serde_json::to_value(profile)?;
    if let Value::Object(map) = &mut value {
        map.insert("did".to_string(), json!(did));
    }
    Ok(value)
}

/// Handle `profile.*` and `key.*` IPC commands:
/// - `profile.show` `{}` -> profile JSON (`{did, display_name?, bio?,
///   avatar_cid?, updated_at?, ...}`) -- the interop document read by peers
/// - `profile.set` `{field, value}` -> updated profile JSON; setting
///   `avatar_cid` points the profile at an image already in the content
///   store, and an empty `value` clears the field. Every set stamps
///   `updated_at`.
/// - `key.generate` `{}` -> `{did}` (errors if one already exists)
/// - `key.list` `{}` -> `[{did, created_at}]`
/// - `key.did` `{}` -> `{did}`
/// - `key.delegate` `{leaf, ttl?}` -> `DelegationV1` JSON (root -> leaf
///   delegation, signed by this identity as root; see
///   `protocol/docs/data-contracts/docs/did-delegation.md`'s "経路B" --
///   manual transfer). `ttl` is a duration string like `"60d"` (default),
///   range 1-365 days.
/// - `key.delegations` `{}` -> `[DelegationV1 & {expired: bool}, ...]`,
///   every delegation issued by this identity so far.
/// - `key.pair.start` `{ttl?, timeout?}` -> `{code, formatted_code, room,
///   expires_at}` -- begin "経路A" pairing: joins a code-derived room and
///   waits for a browser to claim the code and receive a delegation.
///   `timeout` defaults to `"5m"`.
/// - `key.pair.status` `{}` -> `{status, code?, leaf?, app?, delegation?,
///   expires_at?}`, `status` one of `none`/`waiting`/`issued`/`expired`/
///   `cancelled`.
/// - `key.pair.cancel` `{}` -> `{cancelled: bool}`
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "profile.show" => {
            let identity = current(state).await?;
            let profile = load_profile()?;
            profile_with_did(&profile, identity.did())
        }
        "profile.set" => {
            let field = args
                .get("field")
                .and_then(Value::as_str)
                .context("missing `field`")?;
            let value = args
                .get("value")
                .and_then(Value::as_str)
                .context("missing `value`")?;

            let identity = current(state).await?;
            let mut profile = load_profile()?;
            profile.set_field(field, value.to_string());
            profile.updated_at = Some(Utc::now().to_rfc3339());
            save_profile(&profile)?;
            profile_with_did(&profile, identity.did())
        }
        "key.generate" => {
            if load_identity()?.is_some() {
                bail!("a key already exists; refusing to overwrite it (see `mistl key did`)");
            }
            let identity = current(state).await?;
            Ok(json!({ "did": identity.did() }))
        }
        "key.list" => match load_identity()? {
            Some(identity) => Ok(json!([
                { "did": identity.did(), "created_at": identity.created_at() }
            ])),
            None => Ok(json!([])),
        },
        "key.did" => {
            let identity = current(state).await?;
            Ok(json!({ "did": identity.did() }))
        }
        "key.delegate" => {
            let leaf = args
                .get("leaf")
                .and_then(Value::as_str)
                .context("missing `leaf`")?;
            let ttl_str = args.get("ttl").and_then(Value::as_str).unwrap_or("60d");
            let ttl = delegation::parse_ttl(ttl_str)?;

            let identity = current(state).await?;
            let issued = delegation::issue(&identity, leaf, ttl, Utc::now())?;
            delegation::record_delegation(issued.clone())?;
            Ok(serde_json::to_value(&issued)?)
        }
        "key.delegations" => {
            let now = Utc::now();
            let issued = delegation::load_delegations()?;
            let out: Vec<Value> = issued
                .into_iter()
                .map(|d| {
                    let expired = chrono::DateTime::parse_from_rfc3339(&d.exp)
                        .map(|exp| exp.with_timezone(&Utc) <= now)
                        .unwrap_or(true);
                    let mut value = serde_json::to_value(&d).unwrap_or(Value::Null);
                    if let Value::Object(map) = &mut value {
                        map.insert("expired".to_string(), json!(expired));
                    }
                    value
                })
                .collect();
            Ok(json!(out))
        }
        "key.pair.start" => {
            let ttl_str = args.get("ttl").and_then(Value::as_str).unwrap_or("60d");
            let ttl = delegation::parse_ttl(ttl_str)?;
            let timeout_str = args.get("timeout").and_then(Value::as_str).unwrap_or("5m");
            let timeout = delegation::parse_short_duration(timeout_str)?;

            let (code, room, expires_at) = pairing::start_pairing(state, ttl, timeout).await?;
            Ok(json!({
                "code": code,
                "formatted_code": pairing::format_code(&code),
                "room": room,
                "expires_at": expires_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            }))
        }
        "key.pair.status" => Ok(pairing::status_json()),
        "key.pair.cancel" => Ok(json!({ "cancelled": pairing::cancel().await })),
        _ => bail!("unknown identity command: {cmd}"),
    }
}

/// Test-only constructor for callers *outside* this module (e.g.
/// `crate::wiresign`'s tests) that need a real [`Identity`] to exercise
/// `sign_wire`/`verify_wire` without touching this machine's real,
/// persistent per-user identity -- [`current`]/`generate_identity` write to
/// `crate::config::data_dir()`, which would be an unacceptable side effect
/// from a test (see the recommendation left in `wiresign.rs`'s test module
/// doc, from Wave 1's file-ownership split). A fresh in-memory Ed25519
/// keypair every call; never persisted to disk.
#[cfg(test)]
pub(crate) fn for_test() -> Identity {
    let signing_key = SigningKey::generate(&mut OsRng);
    let did = did_from_pubkey(&signing_key.verifying_key().to_bytes());
    Identity {
        did,
        signing_key,
        created_at: Utc::now().to_rfc3339(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_identity() -> Identity {
        let signing_key = SigningKey::generate(&mut OsRng);
        let did = did_from_pubkey(&signing_key.verifying_key().to_bytes());
        Identity {
            did,
            signing_key,
            created_at: Utc::now().to_rfc3339(),
        }
    }

    #[test]
    fn did_key_round_trip() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        assert!(did.starts_with("did:key:z"));
        let decoded = pubkey_from_did(&did).expect("valid did:key");
        assert_eq!(decoded, pubkey);
    }

    #[test]
    fn did_key_uses_ed25519_multicodec_prefix() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);
        let multibase = did.strip_prefix("did:key:z").unwrap();
        let raw = bs58::decode(multibase).into_vec().unwrap();
        assert_eq!(raw.len(), 34);
        assert_eq!(&raw[..2], &MULTICODEC_ED25519_PUB[..]);
        assert_eq!(&raw[2..], &pubkey[..]);
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let identity = fresh_identity();
        let data = b"hello mistl";
        let signature = identity.sign(data);
        assert_eq!(signature.len(), 64);
        assert!(verify(identity.did(), data, &signature).unwrap());
    }

    #[test]
    fn verify_rejects_tampered_data_or_wrong_did() {
        let identity = fresh_identity();
        let other = fresh_identity();
        let data = b"hello mistl";
        let signature = identity.sign(data);

        assert!(!verify(identity.did(), b"goodbye mistl", &signature).unwrap());
        assert!(!verify(other.did(), data, &signature).unwrap());
    }

    #[test]
    fn verify_rejects_malformed_did() {
        let identity = fresh_identity();
        let signature = identity.sign(b"data");
        assert!(verify("did:key:znotbase58!!", b"data", &signature).is_err());
        assert!(verify("not-a-did", b"data", &signature).is_err());
    }

    #[test]
    fn node_id_is_first_16_hex_chars_of_sha256_of_did() {
        let did = "did:key:z6MkExampleDidForNodeIdTesting";
        let node_id = node_id_for_did(did);
        assert_eq!(node_id.len(), 16);

        let digest = Sha256::digest(did.as_bytes());
        let expected: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
        assert_eq!(node_id, expected);
    }

    #[test]
    fn identity_node_id_matches_free_function() {
        let identity = fresh_identity();
        assert_eq!(identity.node_id(), node_id_for_did(identity.did()));
    }

    #[test]
    fn profile_set_field_routes_known_and_extra_fields() {
        let mut profile = Profile::default();
        profile.set_field("display_name", "Ada".to_string());
        profile.set_field("bio", "Loves math".to_string());
        profile.set_field("avatar_cid", "bafyavatarcid".to_string());
        profile.set_field("custom_field", "custom value".to_string());

        assert_eq!(profile.display_name.as_deref(), Some("Ada"));
        assert_eq!(profile.bio.as_deref(), Some("Loves math"));
        assert_eq!(profile.avatar_cid.as_deref(), Some("bafyavatarcid"));
        assert_eq!(
            profile.extra.get("custom_field").map(String::as_str),
            Some("custom value")
        );

        let value = serde_json::to_value(&profile).unwrap();
        assert_eq!(value["display_name"], "Ada");
        assert_eq!(value["avatar_cid"], "bafyavatarcid");
        assert_eq!(value["custom_field"], "custom value");
    }

    #[test]
    fn profile_set_field_with_empty_value_clears_the_field() {
        let mut profile = Profile::default();
        profile.set_field("display_name", "Ada".to_string());
        profile.set_field("avatar_cid", "bafyavatarcid".to_string());
        profile.set_field("custom_field", "custom value".to_string());

        profile.set_field("display_name", String::new());
        profile.set_field("avatar_cid", String::new());
        profile.set_field("custom_field", String::new());

        assert_eq!(profile.display_name, None);
        assert_eq!(profile.avatar_cid, None);
        assert!(!profile.extra.contains_key("custom_field"));
    }
}
