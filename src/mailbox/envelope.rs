//! Signed mail envelopes and the p2p wire message schema used between mistl
//! nodes in the mailbox room.
//!
//! ## Envelope canonical form
//!
//! `Envelope::canonical_bytes` serializes every field except `sig` as
//! compact JSON with alphabetically sorted object keys, which is what gets
//! signed/verified. This relies on `serde_json::Map` being backed by a
//! `BTreeMap` (its default; do **not** enable serde_json's `preserve_order`
//! feature, which would switch it to insertion-ordered `IndexMap` and break
//! signature interop).
//!
//! ## Wire schema
//!
//! [`WireMessage`] is the payload sent over mistlib's raw messaging channel
//! (`mistlib::app::send_message_direct` / the `EVENT_RAW` callback). It tags
//! itself with a `t` field: `"mail"`, `"deposit"`, `"deposit-ack"`,
//! `"fetch"`, or `"fetch-result"`.

use anyhow::{Context, Result};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::identity::Identity;

/// A signed mailbox envelope: an inline message or a file deposit addressed
/// to a peer (by DID or mistlib node id).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Random hex id, unique per envelope.
    pub id: String,
    /// Sender's `did:key:...` DID.
    pub from: String,
    /// Recipient: either a DID or an already-resolved mistlib node id.
    pub to: String,
    pub kind: EnvelopeKind,
    /// Inline UTF-8 body for `kind: "message"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Content id in `crate::storage`, for `kind: "file"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cid: Option<String>,
    /// Original file name, for `kind: "file"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// File size in bytes, for `kind: "file"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// RFC 3339 timestamp set when the envelope was created.
    pub sent_at: String,
    /// Base64 Ed25519 signature over `canonical_bytes()`. Absent until
    /// [`Envelope::sign`] is called.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sig: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    Message,
    File,
}

impl Envelope {
    /// Build a new, unsigned envelope with a fresh random id and the current
    /// timestamp. Caller fills in `body` or `cid`/`name`/`size` and then
    /// calls [`Envelope::sign`].
    pub fn new(from: String, to: String, kind: EnvelopeKind) -> Self {
        Self {
            id: random_id(),
            from,
            to,
            kind,
            body: None,
            cid: None,
            name: None,
            size: None,
            sent_at: chrono::Utc::now().to_rfc3339(),
            sig: None,
        }
    }

    /// Deterministic JSON bytes of every field except `sig`, with object
    /// keys sorted alphabetically. This is what gets signed and verified.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut value = serde_json::to_value(self).expect("Envelope always serializes");
        if let Value::Object(map) = &mut value {
            map.remove("sig");
        }
        serde_json::to_vec(&value).expect("serde_json::Value serialization cannot fail")
    }

    /// Sign this envelope with the local identity, setting `sig` (base64).
    pub fn sign(&mut self, identity: &Identity) {
        let bytes = self.canonical_bytes();
        let signature = identity.sign(&bytes);
        self.sig = Some(BASE64.encode(signature));
    }

    /// Verify `sig` against `from`'s DID key via `crate::identity::verify`.
    pub fn verify(&self) -> Result<bool> {
        let sig_b64 = self
            .sig
            .as_deref()
            .context("envelope has no signature")?;
        let signature = BASE64
            .decode(sig_b64)
            .context("envelope sig is not valid base64")?;
        let bytes = self.canonical_bytes();
        crate::identity::verify(&self.from, &bytes, &signature)
    }

    /// Best-effort size for listings: the file size if known, else the
    /// inline body's byte length.
    pub fn approx_size(&self) -> u64 {
        match self.kind {
            EnvelopeKind::File => self.size.unwrap_or(0),
            EnvelopeKind::Message => self.body.as_ref().map_or(0, |b| b.len() as u64),
        }
    }
}

/// Wire payload exchanged between mistl nodes in the mailbox room, tagged by
/// `t`. Sent as compact JSON bytes over mistlib's raw messaging channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "kebab-case")]
pub enum WireMessage {
    /// Direct delivery to the recipient: sent by the original sender when
    /// the recipient is online, or by a bot forwarding a held deposit once
    /// the recipient appears.
    Mail { envelope: Envelope },
    /// A deposit left with a bot for an offline recipient.
    Deposit { envelope: Envelope },
    /// Optional acknowledgement that a deposit was accepted and stored.
    DepositAck { id: String },
    /// Ask any connected bot whether it holds mail for `node_id`.
    Fetch { node_id: String },
    /// Reply to `Fetch` with any held envelopes addressed to the requester.
    FetchResult { envelopes: Vec<Envelope> },
}

impl WireMessage {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// Random lowercase-hex envelope id (16 bytes -> 32 hex chars).
pub fn random_id() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Node id used with mistlib: the first 16 hex chars of `sha256(did)`.
/// Cross-module contract: `crate::identity` derives its own node id the same
/// way, so this must stay in lockstep with that implementation.
pub fn node_id_from_did(did: &str) -> String {
    let digest = Sha256::digest(did.as_bytes());
    hex_encode(&digest)[..16].to_string()
}

/// True if `s` already looks like a mistlib node id (16 lowercase hex
/// chars) rather than a `did:key:...` string.
pub fn looks_like_node_id(s: &str) -> bool {
    s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Resolve a `mailbox.send` recipient (`to`) to a mistlib node id: passed
/// through as-is if it already looks like a node id, otherwise derived from
/// the DID.
pub fn node_id_for(to: &str) -> String {
    if looks_like_node_id(to) {
        to.to_string()
    } else {
        node_id_from_did(to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_envelope() -> Envelope {
        let mut envelope = Envelope::new(
            "did:key:zFromExample".into(),
            "did:key:zToExample".into(),
            EnvelopeKind::File,
        );
        envelope.body = Some("hello world".into());
        envelope.cid = Some("bafy-example-cid".into());
        envelope.name = Some("file.txt".into());
        envelope.size = Some(42);
        envelope.sig = Some("dGVzdC1zaWc=".into());
        envelope
    }

    #[test]
    fn canonical_bytes_excludes_sig_and_is_stable() {
        let envelope = sample_envelope();
        let first = envelope.canonical_bytes();
        let second = envelope.canonical_bytes();
        assert_eq!(first, second, "canonical_bytes must be deterministic");

        let text = String::from_utf8(first).unwrap();
        assert!(!text.contains("dGVzdC1zaWc="), "sig value must be excluded");
        assert!(!text.contains("\"sig\""), "sig key must be excluded");
    }

    #[test]
    fn canonical_bytes_have_alphabetically_sorted_keys() {
        let envelope = sample_envelope();
        let text = String::from_utf8(envelope.canonical_bytes()).unwrap();
        let keys = [
            "\"body\"", "\"cid\"", "\"from\"", "\"id\"", "\"kind\"", "\"name\"", "\"sent_at\"",
            "\"size\"", "\"to\"",
        ];
        let positions: Vec<usize> = keys.iter().map(|k| text.find(k).unwrap()).collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        assert_eq!(
            positions, sorted,
            "canonical JSON keys must be alphabetically sorted: {text}"
        );
    }

    #[test]
    fn envelope_json_round_trip() {
        let envelope = sample_envelope();
        let json = serde_json::to_string(&envelope).unwrap();
        let parsed: Envelope = serde_json::from_str(&json).unwrap();
        assert_eq!(envelope, parsed);
    }

    #[test]
    fn wire_message_tags_and_round_trips() {
        let envelope = sample_envelope();
        let msg = WireMessage::Mail {
            envelope: envelope.clone(),
        };
        let bytes = msg.to_bytes().unwrap();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(text.contains("\"t\":\"mail\""));

        match WireMessage::from_bytes(&bytes).unwrap() {
            WireMessage::Mail { envelope: got } => assert_eq!(got, envelope),
            other => panic!("expected Mail, got {other:?}"),
        }

        let ack = WireMessage::DepositAck { id: "abc".into() };
        let ack_text = String::from_utf8(ack.to_bytes().unwrap()).unwrap();
        assert!(ack_text.contains("\"t\":\"deposit-ack\""));
    }

    #[test]
    fn node_id_from_did_is_first_16_hex_of_sha256() {
        let did = "did:key:z6MkExampleExampleExample";
        let full_hex: String = Sha256::digest(did.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let derived = node_id_from_did(did);
        assert_eq!(derived, full_hex[..16]);
        assert_eq!(derived.len(), 16);
    }

    #[test]
    fn looks_like_node_id_detects_16_hex_chars() {
        assert!(looks_like_node_id("0123456789abcdef"));
        assert!(!looks_like_node_id("0123456789abcde")); // 15 chars
        assert!(!looks_like_node_id("0123456789abcdeg")); // non-hex char
        assert!(!looks_like_node_id("did:key:zAbcDefGh"));
    }

    #[test]
    fn node_id_for_passes_through_node_ids_and_derives_from_dids() {
        assert_eq!(node_id_for("abcdef0123456789"), "abcdef0123456789");
        assert_eq!(
            node_id_for("did:key:zSomething"),
            node_id_from_did("did:key:zSomething")
        );
    }
}
