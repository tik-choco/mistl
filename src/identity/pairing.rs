//! Pairing: mistl's (the delegation issuer's) side of "path A" from the DID
//! delegation spec -- a short-lived, code-derived mistlib room where a
//! browser app proves it knows a human-readable pairing code and receives a
//! signed root -> leaf delegation in return.
//!
//! Canonical spec (byte-exact, shared with mistai's TypeScript
//! implementation of the browser/requester side, `src/identity/pairing.ts`):
//! `protocol/docs/data-contracts/docs/did-delegation.md`, section
//! "経路A: ペアリング". This module only implements the issuer side; the
//! requester side lives in mistai.
//!
//! One pairing session exists at a time per daemon ([`SESSION`]): starting a
//! new one cancels/replaces whatever was running. A session is a one-shot --
//! once it successfully issues a delegation it leaves its room immediately
//! so the spent code can't be reused, and an idle session auto-expires
//! (leaving its room) once its timeout elapses.

use std::sync::{Arc, LazyLock, Mutex};

use anyhow::{Context, Result};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use rand::RngCore;
use rand::rngs::OsRng;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use tracing::warn;

use crate::daemon::AppState;

use super::delegation::{self, DelegationV1};

/// Crockford base32 minus the visually-confusable `I`, `L`, `O`, `U` --
/// the spec's pairing-code alphabet.
const CODE_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const CODE_LEN: usize = 16;

const WIRE_REQUEST: &str = "tc-did-pair:request";
const WIRE_RESPONSE: &str = "tc-did-pair:response";

/// One pairing attempt's lifecycle, as tracked by [`SESSION`].
#[derive(Debug, Clone)]
pub enum PairingStatus {
    /// Joined the room, waiting for a `tc-did-pair:request`.
    Waiting,
    /// A delegation was issued and broadcast; the room has been left.
    Issued {
        delegation: DelegationV1,
        leaf: String,
        app: String,
    },
    /// The session's timeout elapsed with no valid request.
    Expired,
    /// Superseded by a new `key.pair.start`, or explicitly cancelled.
    Cancelled,
}

/// Snapshot of the single in-flight (or just-finished) pairing session.
#[derive(Debug, Clone)]
pub struct PairingSession {
    pub code: String,
    pub room: String,
    /// Delegation TTL to use *if* a valid request arrives -- not the
    /// session's own lifetime (see `expires_at` for that).
    pub ttl: Duration,
    pub started_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub status: PairingStatus,
}

/// The process's single pairing session, if any -- see the module doc for
/// why only one is ever live at a time.
static SESSION: LazyLock<Mutex<Option<PairingSession>>> = LazyLock::new(|| Mutex::new(None));

/// Guards one-time [`register_handler`] registration. mistlib's room-handler
/// registry (`crate::net::register_room_handler`) has no unregister, so we
/// register exactly once per process and let the handler consult the live
/// [`SESSION`] on every event, rather than registering fresh per session.
static HANDLER_REGISTERED: OnceCell<()> = OnceCell::const_new();

/// Generate a fresh 16-character pairing code from 16 cryptographically
/// random bytes, one alphabet character per byte via `alphabet[byte & 0x1f]`
/// (32 divides 256 evenly, so this is uniform -- 80 bits of entropy).
fn generate_code() -> String {
    let mut bytes = [0u8; CODE_LEN];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|b| CODE_ALPHABET[(b & 0x1f) as usize] as char)
        .collect()
}

/// Format a normalized 16-character code for display: `XXXX-XXXX-XXXX-XXXX`.
/// The hyphens are display-only -- [`normalize_code`] strips them back out.
pub(super) fn format_code(code: &str) -> String {
    code.as_bytes()
        .chunks(4)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("-")
}

/// Normalize user-entered pairing code input per the spec: uppercase, map
/// `I`/`L` -> `1` and `O` -> `0`, then drop everything outside the alphabet
/// (hyphens, spaces, `U`, ...). Returns `None` unless the result is exactly
/// 16 characters.
///
/// Not yet called from mistl's own runtime: mistl is always the pairing
/// *issuer* ([`generate_code`] side), never the party typing a code in --
/// that's the browser app (mistai's TypeScript port normalizes there). Kept
/// public and tested here for spec-compatibility and for a future
/// mistl-side consumer (e.g. a `mistl key pair check <code>` diagnostic).
#[allow(dead_code)]
pub fn normalize_code(input: &str) -> Option<String> {
    let mut result = String::with_capacity(CODE_LEN);
    for ch in input.chars() {
        if !ch.is_ascii_alphanumeric() {
            continue;
        }
        let mapped = match ch.to_ascii_uppercase() {
            'I' | 'L' => '1',
            'O' => '0',
            other => other,
        };
        if CODE_ALPHABET.contains(&(mapped as u8)) {
            result.push(mapped);
        }
    }
    (result.chars().count() == CODE_LEN).then_some(result)
}

/// Derive the mistlib room id from a normalized 16-char code:
/// `"tc-did-pair-" + hexLower(sha256("tc-did-pair-v1|room|" + code))[..32]`.
pub fn room_id(code: &str) -> String {
    let digest = Sha256::digest(format!("tc-did-pair-v1|room|{code}").as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("tc-did-pair-{}", &hex[..32])
}

/// Derive the HMAC key from a normalized 16-char code:
/// `sha256("tc-did-pair-v1|mac|" + code)`, raw 32 bytes.
pub fn mac_key(code: &str) -> [u8; 32] {
    Sha256::digest(format!("tc-did-pair-v1|mac|{code}").as_bytes()).into()
}

/// `base64url_nopad(HMAC-SHA256(key, utf8(stableStringify(msg minus "mac"))))`
/// -- the spec's message authentication for both directions of the pairing
/// protocol.
fn compute_mac(msg: &Value, key: &[u8; 32]) -> String {
    let mut map = msg.as_object().cloned().unwrap_or_default();
    map.remove("mac");
    let payload = crate::wiresign::stable_stringify(&Value::Object(map));
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
    mac.update(payload.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// `key.pair.start`: begin a new pairing session, replacing (and, if it was
/// still waiting, leaving the room of) whatever session was previously
/// running. Joins the code-derived room and ensures the response handler is
/// registered (idempotent -- see [`HANDLER_REGISTERED`]).
pub(super) async fn start_pairing(
    state: &Arc<AppState>,
    ttl: Duration,
    timeout: Duration,
) -> Result<(String, String, DateTime<Utc>)> {
    leave_waiting_session_room().await;

    let code = generate_code();
    let room = room_id(&code);
    crate::net::ensure_started(state, room.clone())
        .await
        .context("pairing: joining pairing room")?;
    ensure_handler_registered(state.clone()).await;

    let started_at = Utc::now();
    let expires_at = started_at + timeout;
    {
        let mut session = SESSION.lock().expect("pairing session lock poisoned");
        *session = Some(PairingSession {
            code: code.clone(),
            room: room.clone(),
            ttl,
            started_at,
            expires_at,
            status: PairingStatus::Waiting,
        });
    }

    // Auto-expire: if nothing has claimed the code by `timeout`, stop
    // waiting and release the room so it doesn't linger forever.
    let expire_room = room.clone();
    let sleep_for = timeout
        .to_std()
        .unwrap_or(std::time::Duration::from_secs(300));
    tokio::spawn(async move {
        tokio::time::sleep(sleep_for).await;
        expire_session(&expire_room).await;
    });

    Ok((code, room, expires_at))
}

/// If the current session is still `Waiting`, leave its room. Shared by
/// [`start_pairing`] (replacing an old session) and [`cancel`].
async fn leave_waiting_session_room() {
    let room = {
        let session = SESSION.lock().expect("pairing session lock poisoned");
        session
            .as_ref()
            .filter(|s| matches!(s.status, PairingStatus::Waiting))
            .map(|s| s.room.clone())
    };
    if let Some(room) = room
        && let Err(err) = crate::net::leave_room(&room).await
    {
        warn!(%err, room, "pairing: failed to leave room while replacing/cancelling a session");
    }
}

/// Fired [`start_pairing`]'s timeout for `room`: if the session is still
/// waiting *and still pointed at this room* (a later session may already
/// have replaced it), mark it expired and leave.
async fn expire_session(room: &str) {
    let should_leave = {
        let mut session = SESSION.lock().expect("pairing session lock poisoned");
        match session.as_mut() {
            Some(s) if s.room == room && matches!(s.status, PairingStatus::Waiting) => {
                s.status = PairingStatus::Expired;
                true
            }
            _ => false,
        }
    };
    if should_leave && let Err(err) = crate::net::leave_room(room).await {
        warn!(%err, room, "pairing: failed to leave room after session timeout");
    }
}

/// `key.pair.cancel`: cancel the session if it's still waiting. Returns
/// whether there was one to cancel.
pub(super) async fn cancel() -> bool {
    let room = {
        let mut session = SESSION.lock().expect("pairing session lock poisoned");
        match session.as_mut() {
            Some(s) if matches!(s.status, PairingStatus::Waiting) => {
                s.status = PairingStatus::Cancelled;
                Some(s.room.clone())
            }
            _ => None,
        }
    };
    let Some(room) = room else {
        return false;
    };
    if let Err(err) = crate::net::leave_room(&room).await {
        warn!(%err, room, "pairing: failed to leave room on cancel");
    }
    true
}

/// `key.pair.status` response: `{status, code?, leaf?, app?, delegation?,
/// expires_at?}`.
pub(super) fn status_json() -> Value {
    let session = SESSION.lock().expect("pairing session lock poisoned");
    let Some(session) = session.as_ref() else {
        return json!({ "status": "none" });
    };
    match &session.status {
        PairingStatus::Waiting => json!({
            "status": "waiting",
            "code": session.code,
            "started_at": session.started_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            "expires_at": session.expires_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        }),
        PairingStatus::Issued {
            delegation,
            leaf,
            app,
        } => json!({
            "status": "issued",
            "leaf": leaf,
            "app": app,
            "delegation": delegation,
        }),
        PairingStatus::Expired => json!({ "status": "expired" }),
        PairingStatus::Cancelled => json!({ "status": "cancelled" }),
    }
}

/// Register the room-event handler exactly once for the process lifetime.
async fn ensure_handler_registered(state: Arc<AppState>) {
    let runtime = tokio::runtime::Handle::current();
    HANDLER_REGISTERED
        .get_or_init(|| async move {
            register_handler(state, runtime);
        })
        .await;
}

/// The one-time-registered room handler: cheap synchronous filtering here
/// (mistlib's own dispatch thread, not tokio), full request handling
/// spawned onto the captured `runtime`. Always re-checks the *current*
/// [`SESSION`] rather than closing over one session's details, since this
/// registration outlives any single pairing attempt.
fn register_handler(state: Arc<AppState>, runtime: tokio::runtime::Handle) {
    crate::net::register_room_handler(move |event_type, evt_room, _from_id, data| {
        if event_type != crate::net::EVENT_RAW {
            return;
        }
        let owns_room = {
            let session = SESSION.lock().expect("pairing session lock poisoned");
            matches!(session.as_ref(), Some(s) if s.room == evt_room && matches!(s.status, PairingStatus::Waiting))
        };
        if !owns_room {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(data) else {
            return; // not JSON; not ours
        };
        if value.get("type").and_then(Value::as_str) != Some(WIRE_REQUEST) {
            return; // some other traffic in the room; ignore
        }

        let room = evt_room.to_string();
        let state = state.clone();
        runtime.spawn(async move {
            handle_pair_request(&state, &room, value).await;
        });
    });
}

/// Handle one `tc-did-pair:request`: MAC-verify, then leaf-validate, then
/// issue and broadcast a delegation. Every failure mode (bad MAC, missing
/// fields, malformed leaf, a session that moved on while this was in
/// flight) is a silent drop -- the spec is explicit that pairing never
/// sends an error reply for an unauthenticated/invalid request.
async fn handle_pair_request(state: &Arc<AppState>, room: &str, msg: Value) {
    let (code, ttl) = {
        let session = SESSION.lock().expect("pairing session lock poisoned");
        match session.as_ref() {
            Some(s) if s.room == room && matches!(s.status, PairingStatus::Waiting) => {
                (s.code.clone(), s.ttl)
            }
            _ => return,
        }
    };

    let key = mac_key(&code);
    let Some(mac) = msg.get("mac").and_then(Value::as_str) else {
        return;
    };
    // MAC first (proves the sender knows the code) -- only then look at
    // `leaf`, so a non-holder of the code can't even probe leaf validation.
    if compute_mac(&msg, &key) != mac {
        return;
    }
    let Some(leaf) = msg.get("leaf").and_then(Value::as_str) else {
        return;
    };
    let Some(nonce) = msg.get("nonce").and_then(Value::as_str) else {
        return;
    };
    if super::pubkey_from_did(leaf).is_err() {
        return;
    }
    let app = msg
        .get("app")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();

    let identity = match super::current(state).await {
        Ok(identity) => identity,
        Err(err) => {
            warn!(%err, "pairing: failed to load the local identity while handling a pair request");
            return;
        }
    };
    let delegation = match delegation::issue(&identity, leaf, ttl, Utc::now()) {
        Ok(d) => d,
        Err(err) => {
            warn!(%err, leaf, "pairing: refused to issue a delegation for this request");
            return;
        }
    };

    let mut response = json!({
        "v": 1,
        "type": WIRE_RESPONSE,
        "nonce": nonce,
        "delegation": delegation,
    });
    response["mac"] = json!(compute_mac(&response, &key));

    let bytes = match serde_json::to_vec(&response) {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!(%err, "pairing: failed to serialize the pair response");
            return;
        }
    };
    if let Err(err) = crate::net::send_broadcast(room, bytes).await {
        warn!(%err, room, "pairing: failed to broadcast the delegation response");
        return;
    }

    if let Err(err) = delegation::record_delegation(delegation.clone()) {
        warn!(%err, "pairing: failed to persist the issued delegation");
    }

    // One-shot: the code is now spent, so leave the room and mark the
    // session issued -- even if persistence above failed, the delegation
    // was already broadcast and is the source of truth on the wire.
    if let Err(err) = crate::net::leave_room(room).await {
        warn!(%err, room, "pairing: failed to leave room after issuing a delegation");
    }
    {
        let mut session = SESSION.lock().expect("pairing session lock poisoned");
        if let Some(s) = session.as_mut()
            && s.room == room
        {
            s.status = PairingStatus::Issued {
                delegation,
                leaf: leaf.to_string(),
                app,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_code_uppercases_and_strips_hyphens() {
        assert_eq!(
            normalize_code("abcd-1234-efgh-5678"),
            Some("ABCD1234EFGH5678".to_string())
        );
        assert_eq!(
            normalize_code("ABCD-1234-EFGH-5678"),
            Some("ABCD1234EFGH5678".to_string())
        );
    }

    #[test]
    fn normalize_code_maps_i_l_to_1_and_o_to_0() {
        assert_eq!(
            normalize_code("IIIIIIIIIIIIIIII"),
            Some("1111111111111111".to_string())
        );
        assert_eq!(
            normalize_code("llllllllllllllll"),
            Some("1111111111111111".to_string())
        );
        assert_eq!(
            normalize_code("oooooooooooooooo"),
            Some("0000000000000000".to_string())
        );
    }

    #[test]
    fn normalize_code_drops_non_alphabet_characters() {
        assert_eq!(
            normalize_code("AB CD-12*34#EFGH$5678"),
            Some("ABCD1234EFGH5678".to_string())
        );
    }

    #[test]
    fn normalize_code_drops_u_as_meaningless_noise() {
        // Unlike I/L/O, 'U' has no defined remapping (deliberately excluded
        // from the alphabet), so it's just dropped like any other
        // out-of-alphabet character.
        assert_eq!(
            normalize_code("ABCD1234EFGH567U8"),
            Some("ABCD1234EFGH5678".to_string())
        );
    }

    #[test]
    fn normalize_code_rejects_wrong_length() {
        assert_eq!(normalize_code("ABCD1234"), None);
        assert_eq!(normalize_code("ABCD1234EFGH5678ABCD1234"), None);
        assert_eq!(normalize_code(""), None);
    }

    #[test]
    fn format_code_inserts_hyphens_every_four_chars() {
        assert_eq!(format_code("ABCD1234EFGH5678"), "ABCD-1234-EFGH-5678");
    }

    #[test]
    fn compute_mac_excludes_the_mac_field_itself() {
        let key = [7u8; 32];
        let without_mac = json!({ "v": 1, "type": "tc-did-pair:request", "leaf": "did:key:zabc" });
        let mut with_mac = without_mac.clone();
        with_mac["mac"] = json!("whatever-was-here-before");

        assert_eq!(
            compute_mac(&without_mac, &key),
            compute_mac(&with_mac, &key)
        );
    }

    #[test]
    fn compute_mac_changes_when_a_real_field_changes() {
        let key = [7u8; 32];
        let a = json!({ "v": 1, "leaf": "did:key:zabc" });
        let b = json!({ "v": 1, "leaf": "did:key:zdef" });
        assert_ne!(compute_mac(&a, &key), compute_mac(&b, &key));
    }

    // ---- cross-implementation vectors ----
    //
    // Shared with mistai's TypeScript pairing implementation
    // (`src/identity/pairing.ts`): both sides derive room_id/mac_key from a
    // code via the same sha256-based formulas in did-delegation.md's "導出"
    // section, so a fixed code must produce byte-identical results on both
    // sides. Computed directly from the spec's formulas (not from a
    // from-memory reimplementation):
    //   sha256("tc-did-pair-v1|room|ABCD1234EFGH5678")[..32 hex chars]
    //   sha256("tc-did-pair-v1|mac|ABCD1234EFGH5678") (raw 32 bytes, shown as hex)

    const VECTOR_CODE: &str = "ABCD1234EFGH5678";
    const VECTOR_ROOM_ID: &str = "tc-did-pair-fb3be94907ec90a4adc84818e0c9a83b";
    const VECTOR_MAC_KEY_HEX: &str =
        "daed3202538854e2ee9460abb19fc207a7ba95be8a6d23dcf402fe42617429ef";

    #[test]
    fn room_id_matches_cross_implementation_vector() {
        assert_eq!(room_id(VECTOR_CODE), VECTOR_ROOM_ID);
    }

    #[test]
    fn mac_key_matches_cross_implementation_vector() {
        let key = mac_key(VECTOR_CODE);
        let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, VECTOR_MAC_KEY_HEX);
    }
}
