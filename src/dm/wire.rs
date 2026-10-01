//! Wire format and stateless verification of direct messages.
//!
//! Two JSON objects tagged by `t` (no `type`, `v` or `kind` key, so the
//! tc-chat / mistai / tunnel / storage handlers ignore them):
//!
//! ```json
//! {"t":"mistl-dm-v1","node":"<16 hex>","fromId":"did:key:...","to":"did:key:...",
//!  "id":"<32 hex>","ts":1700000000000,"text":"...","file":{"cid","name","size","mime","key"},
//!  "signature":"..."}
//! {"t":"mistl-dm-ack-v1","node","fromId","to","id","ts","signature"}
//! ```
//!
//! Everything but `signature` is covered by the [`crate::wiresign`] signature,
//! so the recipient (`to`), the id, the timestamp and the file key cannot be
//! altered or redirected by a relay or another peer.
//!
//! Verification is split in two so the caller can interleave stateful,
//! cheaper-than-a-signature gates (dedupe, rate limit, allowlist):
//! [`precheck`] does every stateless check except the signature, and
//! [`Prechecked::verify`] checks the signature last.

use anyhow::{Result, bail};
use serde_json::{Map, Value, json};

use crate::identity::{Identity, node_id_for_did};

pub const MSG_TAG: &str = "mistl-dm-v1";
pub const ACK_TAG: &str = "mistl-dm-ack-v1";
/// Common prefix of both tags, used as the cheap byte-level prefilter.
pub const TAG_PREFIX: &str = "mistl-dm-";

/// Largest wire message we parse (and the largest we will send).
pub const MAX_WIRE_BYTES: usize = 16 * 1024;
pub const MAX_TEXT_CHARS: usize = 4000;
pub const MAX_NAME_CHARS: usize = 200;
pub const MAX_MIME_CHARS: usize = 100;
pub const MAX_CID_CHARS: usize = 128;
pub const MAX_KEY_CHARS: usize = 128;
/// Largest attachment we send or agree to download.
pub const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// Accepted distance between a message's `ts` and the local clock.
pub const MAX_SKEW_MS: u64 = 10 * 60 * 1000;

/// File attachment metadata. `key` is the passphrase of the encrypted
/// tc-storage bundle behind `cid`; it never leaves the daemon.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileMeta {
    pub cid: String,
    pub name: String,
    pub size: u64,
    pub mime: String,
    pub key: String,
}

/// A message that passed every check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DmMessage {
    pub from_did: String,
    pub id: String,
    pub ts: u64,
    pub text: Option<String>,
    pub file: Option<FileMeta>,
}

/// A delivery receipt that passed every check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DmAck {
    pub from_did: String,
    pub id: String,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    Message(DmMessage),
    Ack(DmAck),
}

/// Why an inbound message was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    /// Not a DM wire at all (someone else's message): ignored silently.
    NotOurs,
    Oversized,
    Malformed,
    /// `node` differs from the transport sender.
    NodeMismatch,
    /// `sha256(fromId)[..16 hex]` differs from the transport sender.
    DidMismatch,
    /// `to` is not our DID.
    WrongRecipient,
    /// `fromId` is our own DID (no legitimate peer can send that).
    SelfSend,
    Stale,
    BadSignature,
}

/// What kind of wire a [`Prechecked`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Message,
    Ack,
}

/// A wire that passed every stateless check except the signature.
#[derive(Debug)]
pub struct Prechecked {
    kind: Kind,
    value: Value,
    pub from_did: String,
    pub id: String,
    pub ts: u64,
    text: Option<String>,
    file: Option<FileMeta>,
}

/// A fresh random message id (128 bits, lowercase hex).
pub fn fresh_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

pub fn is_valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn has_forbidden_control(s: &str, allow_whitespace: bool) -> bool {
    s.chars()
        .any(|c| c.is_control() && !(allow_whitespace && matches!(c, '\n' | '\r' | '\t')))
}

fn is_cid_like(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_CID_CHARS && s.bytes().all(|b| b.is_ascii_alphanumeric())
}

fn is_ascii_printable(s: &str) -> bool {
    s.bytes().all(|b| (0x20..0x7f).contains(&b))
}

/// Validates message text (length and control characters).
pub fn validate_text(text: &str) -> Result<()> {
    if text.chars().count() > MAX_TEXT_CHARS {
        bail!("text is longer than {MAX_TEXT_CHARS} characters");
    }
    if has_forbidden_control(text, true) {
        bail!("text contains control characters");
    }
    Ok(())
}

/// Validates the user-visible fields shared by the sender and the receiver.
pub fn validate_content(text: Option<&str>, file: Option<&FileMeta>) -> Result<()> {
    if text.is_none() && file.is_none() {
        bail!("a message needs text or a file");
    }
    if let Some(text) = text {
        validate_text(text)?;
        if text.is_empty() && file.is_none() {
            bail!("empty message");
        }
    }
    if let Some(file) = file {
        if !is_cid_like(&file.cid) {
            bail!("invalid file cid");
        }
        let name_chars = file.name.chars().count();
        if name_chars == 0
            || name_chars > MAX_NAME_CHARS
            || has_forbidden_control(&file.name, false)
        {
            bail!("invalid file name");
        }
        if file.mime.chars().count() > MAX_MIME_CHARS || !is_ascii_printable(&file.mime) {
            bail!("invalid file mime type");
        }
        if file.size > MAX_FILE_BYTES {
            bail!("file is larger than {} MiB", MAX_FILE_BYTES / (1024 * 1024));
        }
        if file.key.is_empty() || file.key.len() > MAX_KEY_CHARS || !is_ascii_printable(&file.key) {
            bail!("invalid file key");
        }
    }
    Ok(())
}

/// Builds and signs a message. Fails if the content is invalid or the
/// serialized wire would exceed [`MAX_WIRE_BYTES`] (the receiver would drop it).
pub fn build_message(
    identity: &Identity,
    to: &str,
    id: &str,
    ts: u64,
    text: Option<&str>,
    file: Option<&FileMeta>,
) -> Result<Vec<u8>> {
    validate_content(text, file)?;
    let mut wire = json!({
        "t": MSG_TAG,
        "node": identity.node_id(),
        "fromId": identity.did(),
        "to": to,
        "id": id,
        "ts": ts,
    });
    let map = wire.as_object_mut().expect("object literal");
    if let Some(text) = text {
        map.insert("text".into(), json!(text));
    }
    if let Some(file) = file {
        map.insert(
            "file".into(),
            json!({
                "cid": file.cid, "name": file.name, "size": file.size,
                "mime": file.mime, "key": file.key,
            }),
        );
    }
    crate::wiresign::sign_wire(&mut wire, identity)?;
    let bytes = serde_json::to_vec(&wire)?;
    if bytes.len() > MAX_WIRE_BYTES {
        bail!("message is too large to send ({} bytes)", bytes.len());
    }
    Ok(bytes)
}

/// Builds and signs the delivery receipt for `id`, addressed to `to` (the
/// original sender).
pub fn build_ack(identity: &Identity, to: &str, id: &str, ts: u64) -> Vec<u8> {
    let mut wire = json!({
        "t": ACK_TAG,
        "node": identity.node_id(),
        "fromId": identity.did(),
        "to": to,
        "id": id,
        "ts": ts,
    });
    crate::wiresign::sign_wire(&mut wire, identity).expect("fromId is the signing identity's DID");
    serde_json::to_vec(&wire).expect("ack serialization cannot fail")
}

/// Cheap byte-level prefilter: size, then the tag prefix. Run this before
/// any JSON parsing.
pub fn looks_like_dm(data: &[u8]) -> bool {
    data.len() <= MAX_WIRE_BYTES
        && data
            .windows(TAG_PREFIX.len())
            .any(|w| w == TAG_PREFIX.as_bytes())
}

fn str_field<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a str, Reject> {
    obj.get(key)
        .and_then(Value::as_str)
        .ok_or(Reject::Malformed)
}

/// Every stateless check except the signature, cheapest first:
/// size, tag, `node == from`, `node_id(fromId) == from`, `to == our DID`,
/// not self-sent, `ts` skew, field shapes.
pub fn precheck(data: &[u8], from: &str, our_did: &str, now_ms: u64) -> Result<Prechecked, Reject> {
    if data.len() > MAX_WIRE_BYTES {
        return Err(Reject::Oversized);
    }
    if !looks_like_dm(data) {
        return Err(Reject::NotOurs);
    }
    let value: Value = serde_json::from_slice(data).map_err(|_| Reject::NotOurs)?;
    let obj = value.as_object().ok_or(Reject::NotOurs)?;
    let kind = match obj.get("t").and_then(Value::as_str) {
        Some(MSG_TAG) => Kind::Message,
        Some(ACK_TAG) => Kind::Ack,
        _ => return Err(Reject::NotOurs),
    };
    if str_field(obj, "node")? != from {
        return Err(Reject::NodeMismatch);
    }
    let did = str_field(obj, "fromId")?;
    if did.len() != crate::identity::ED25519_DID_KEY_LEN || node_id_for_did(did) != from {
        return Err(Reject::DidMismatch);
    }
    if str_field(obj, "to")? != our_did {
        return Err(Reject::WrongRecipient);
    }
    if did == our_did {
        return Err(Reject::SelfSend);
    }
    let ts = obj
        .get("ts")
        .and_then(Value::as_u64)
        .ok_or(Reject::Malformed)?;
    if ts.abs_diff(now_ms) > MAX_SKEW_MS {
        return Err(Reject::Stale);
    }
    let id = str_field(obj, "id")?;
    if !is_valid_id(id) {
        return Err(Reject::Malformed);
    }
    let (mut text, mut file) = (None, None);
    if kind == Kind::Message {
        if let Some(t) = obj.get("text") {
            text = Some(t.as_str().ok_or(Reject::Malformed)?.to_string());
        }
        if let Some(f) = obj.get("file") {
            let f = f.as_object().ok_or(Reject::Malformed)?;
            file = Some(FileMeta {
                cid: str_field(f, "cid")?.to_string(),
                name: str_field(f, "name")?.to_string(),
                size: f
                    .get("size")
                    .and_then(Value::as_u64)
                    .ok_or(Reject::Malformed)?,
                mime: str_field(f, "mime")?.to_string(),
                key: str_field(f, "key")?.to_string(),
            });
        }
        validate_content(text.as_deref(), file.as_ref()).map_err(|_| Reject::Malformed)?;
    }
    Ok(Prechecked {
        kind,
        from_did: did.to_string(),
        id: id.to_string(),
        ts,
        text,
        file,
        value,
    })
}

impl Prechecked {
    pub fn is_ack(&self) -> bool {
        self.kind == Kind::Ack
    }

    /// Checks the signature (the expensive step, so it runs last).
    pub fn verify(self) -> Result<Incoming, Reject> {
        if !crate::wiresign::verify_wire(&self.value).unwrap_or(false) {
            return Err(Reject::BadSignature);
        }
        Ok(match self.kind {
            Kind::Message => Incoming::Message(DmMessage {
                from_did: self.from_did,
                id: self.id,
                ts: self.ts,
                text: self.text,
                file: self.file,
            }),
            Kind::Ack => Incoming::Ack(DmAck {
                from_did: self.from_did,
                id: self.id,
                ts: self.ts,
            }),
        })
    }
}

/// Full stateless verification (precheck + signature).
#[cfg(test)]
pub fn check_incoming(
    data: &[u8],
    from: &str,
    our_did: &str,
    now_ms: u64,
) -> Result<Incoming, Reject> {
    precheck(data, from, our_did, now_ms)?.verify()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::for_test;
    use crate::net::peer_auth::now_ms;

    fn file() -> FileMeta {
        FileMeta {
            cid: "bafybeigdyrzt5example".into(),
            name: "report.pdf".into(),
            size: 1234,
            mime: "application/pdf".into(),
            key: "k3y_k3y-k3y".into(),
        }
    }

    fn parse(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).unwrap()
    }

    fn resign(mut v: Value, id: &Identity) -> Vec<u8> {
        crate::wiresign::sign_wire(&mut v, id).unwrap();
        serde_json::to_vec(&v).unwrap()
    }

    #[test]
    fn message_round_trips_with_text_and_file() {
        let (a, b) = (for_test(), for_test());
        let now = now_ms();
        let id = fresh_id();
        let bytes = build_message(&a, b.did(), &id, now, Some("hello"), Some(&file())).unwrap();
        match check_incoming(&bytes, &a.node_id(), b.did(), now).unwrap() {
            Incoming::Message(m) => {
                assert_eq!(m.from_did, a.did());
                assert_eq!(m.id, id);
                assert_eq!(m.text.as_deref(), Some("hello"));
                assert_eq!(m.file, Some(file()));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn ack_round_trips() {
        let (a, b) = (for_test(), for_test());
        let now = now_ms();
        let id = fresh_id();
        let bytes = build_ack(&b, a.did(), &id, now);
        match check_incoming(&bytes, &b.node_id(), a.did(), now).unwrap() {
            Incoming::Ack(ack) => {
                assert_eq!(ack.from_did, b.did());
                assert_eq!(ack.id, id);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn wire_has_no_other_demux_keys() {
        let (a, b) = (for_test(), for_test());
        let m = parse(&build_message(&a, b.did(), &fresh_id(), 1, Some("x"), None).unwrap());
        let k = parse(&build_ack(&a, b.did(), &fresh_id(), 1));
        for v in [&m, &k] {
            for key in ["type", "v", "kind"] {
                assert!(v.get(key).is_none(), "wire must not carry `{key}`");
            }
        }
        assert_eq!(m["t"], MSG_TAG);
        assert_eq!(k["t"], ACK_TAG);
    }

    #[test]
    fn forged_node_is_rejected() {
        let (a, b, victim) = (for_test(), for_test(), for_test());
        let now = now_ms();
        let bytes = build_message(&a, b.did(), &fresh_id(), now, Some("hi"), None).unwrap();
        // Delivered under someone else's transport id.
        assert_eq!(
            check_incoming(&bytes, &victim.node_id(), b.did(), now).unwrap_err(),
            Reject::NodeMismatch
        );
        // Claims the victim's node id in `node` and as transport sender, but
        // signs with its own DID: node_id(fromId) != from.
        let forged = resign(
            json!({"t": MSG_TAG, "node": victim.node_id(), "fromId": a.did(), "to": b.did(),
                   "id": fresh_id(), "ts": now, "text": "hi"}),
            &a,
        );
        assert_eq!(
            check_incoming(&forged, &victim.node_id(), b.did(), now).unwrap_err(),
            Reject::DidMismatch
        );
    }

    #[test]
    fn forged_did_without_the_key_fails_the_signature() {
        let (a, b, victim) = (for_test(), for_test(), for_test());
        let now = now_ms();
        // Attacker claims the victim's DID/node and signs with its own key.
        let mut wire = json!({"t": MSG_TAG, "node": victim.node_id(), "fromId": victim.did(),
            "to": b.did(), "id": fresh_id(), "ts": now, "text": "hi"});
        let payload = crate::wiresign::signing_payload(wire.as_object().unwrap());
        use base64::Engine as _;
        wire["signature"] = json!(
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(a.sign(payload.as_bytes()))
        );
        let bytes = serde_json::to_vec(&wire).unwrap();
        assert_eq!(
            check_incoming(&bytes, &victim.node_id(), b.did(), now).unwrap_err(),
            Reject::BadSignature
        );
    }

    #[test]
    fn wrong_recipient_and_self_send_are_rejected() {
        let (a, b, c) = (for_test(), for_test(), for_test());
        let now = now_ms();
        let bytes = build_message(&a, b.did(), &fresh_id(), now, Some("hi"), None).unwrap();
        // Intercepted copy delivered to a third node.
        assert_eq!(
            check_incoming(&bytes, &a.node_id(), c.did(), now).unwrap_err(),
            Reject::WrongRecipient
        );
        // Our own message reflected back at us (to == our DID is impossible
        // for a message we sent, but a self-addressed one is refused anyway).
        let own = build_message(&a, a.did(), &fresh_id(), now, Some("hi"), None).unwrap();
        assert_eq!(
            check_incoming(&own, &a.node_id(), a.did(), now).unwrap_err(),
            Reject::SelfSend
        );
    }

    #[test]
    fn retargeting_a_signed_message_breaks_the_signature() {
        let (a, b, c) = (for_test(), for_test(), for_test());
        let now = now_ms();
        let bytes = build_message(&a, b.did(), &fresh_id(), now, Some("hi"), None).unwrap();
        let mut v = parse(&bytes);
        v["to"] = json!(c.did());
        let bytes = serde_json::to_vec(&v).unwrap();
        assert_eq!(
            check_incoming(&bytes, &a.node_id(), c.did(), now).unwrap_err(),
            Reject::BadSignature
        );
    }

    #[test]
    fn tampered_text_or_file_key_breaks_the_signature() {
        let (a, b) = (for_test(), for_test());
        let now = now_ms();
        let bytes =
            build_message(&a, b.did(), &fresh_id(), now, Some("hi"), Some(&file())).unwrap();
        let mut v = parse(&bytes);
        v["text"] = json!("pay me");
        assert_eq!(
            check_incoming(&serde_json::to_vec(&v).unwrap(), &a.node_id(), b.did(), now)
                .unwrap_err(),
            Reject::BadSignature
        );
        let mut v = parse(&bytes);
        v["file"]["key"] = json!("other");
        assert_eq!(
            check_incoming(&serde_json::to_vec(&v).unwrap(), &a.node_id(), b.did(), now)
                .unwrap_err(),
            Reject::BadSignature
        );
    }

    #[test]
    fn stale_and_future_timestamps_are_rejected() {
        let (a, b) = (for_test(), for_test());
        let now = now_ms();
        for ts in [now - MAX_SKEW_MS - 1, now + MAX_SKEW_MS + 1] {
            let bytes = build_message(&a, b.did(), &fresh_id(), ts, Some("hi"), None).unwrap();
            assert_eq!(
                check_incoming(&bytes, &a.node_id(), b.did(), now).unwrap_err(),
                Reject::Stale
            );
        }
        let edge = build_message(
            &a,
            b.did(),
            &fresh_id(),
            now - MAX_SKEW_MS,
            Some("hi"),
            None,
        )
        .unwrap();
        assert!(check_incoming(&edge, &a.node_id(), b.did(), now).is_ok());
    }

    #[test]
    fn oversized_and_foreign_wires_are_ignored() {
        let (a, b) = (for_test(), for_test());
        let big = vec![b' '; MAX_WIRE_BYTES + 1];
        assert_eq!(
            precheck(&big, &a.node_id(), b.did(), 0).unwrap_err(),
            Reject::Oversized
        );
        for foreign in [
            json!({"v": 1, "type": "consumer_hello"}),
            json!({"t": "mistl-did-hello-v1", "room": "r"}),
            json!({"kind": "data"}),
        ] {
            let bytes = serde_json::to_vec(&foreign).unwrap();
            assert_eq!(
                precheck(&bytes, &a.node_id(), b.did(), now_ms()).unwrap_err(),
                Reject::NotOurs
            );
        }
        assert!(!looks_like_dm(b"\x00\x01binary block data"));
    }

    #[test]
    fn content_limits_are_enforced_on_both_ends() {
        let (a, b) = (for_test(), for_test());
        let id = fresh_id();
        let long = "a".repeat(MAX_TEXT_CHARS + 1);
        assert!(build_message(&a, b.did(), &id, 1, Some(&long), None).is_err());
        assert!(build_message(&a, b.did(), &id, 1, None, None).is_err());
        assert!(build_message(&a, b.did(), &id, 1, Some("a\u{0}b"), None).is_err());
        let mut f = file();
        f.size = MAX_FILE_BYTES + 1;
        assert!(build_message(&a, b.did(), &id, 1, None, Some(&f)).is_err());
        let mut f = file();
        f.name = "x".repeat(MAX_NAME_CHARS + 1);
        assert!(build_message(&a, b.did(), &id, 1, None, Some(&f)).is_err());
        // Four-byte characters can push a legal-length text over the wire cap.
        let emoji = "\u{1F600}".repeat(MAX_TEXT_CHARS);
        let mut f = file();
        f.name = "\u{1F600}".repeat(MAX_NAME_CHARS);
        assert!(build_message(&a, b.did(), &id, 1, Some(&emoji), Some(&f)).is_err());

        // A hostile peer's out-of-limit (but validly signed) message is dropped.
        let now = now_ms();
        let bad = resign(
            json!({"t": MSG_TAG, "node": a.node_id(), "fromId": a.did(), "to": b.did(),
                   "id": fresh_id(), "ts": now, "text": long}),
            &a,
        );
        assert_eq!(
            check_incoming(&bad, &a.node_id(), b.did(), now).unwrap_err(),
            Reject::Malformed
        );
        let bad_id = resign(
            json!({"t": MSG_TAG, "node": a.node_id(), "fromId": a.did(), "to": b.did(),
                   "id": "../../etc", "ts": now, "text": "x"}),
            &a,
        );
        assert_eq!(
            check_incoming(&bad_id, &a.node_id(), b.did(), now).unwrap_err(),
            Reject::Malformed
        );
        let bad_file = resign(
            json!({"t": MSG_TAG, "node": a.node_id(), "fromId": a.did(), "to": b.did(),
                   "id": fresh_id(), "ts": now,
                   "file": {"cid": "../x", "name": "n", "size": 1, "mime": "", "key": "k"}}),
            &a,
        );
        assert_eq!(
            check_incoming(&bad_file, &a.node_id(), b.did(), now).unwrap_err(),
            Reject::Malformed
        );
    }
}
