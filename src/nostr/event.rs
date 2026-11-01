//! NIP-01 event model: strict parsing, id computation and BIP-340
//! signature verification.
//!
//! Signatures cover the 32 raw bytes of the canonical event hash. The event
//! shape is checked separately from its hash and signature so callers can
//! distinguish malformed input from an invalid signed event.

use k256::schnorr::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// A NIP-01 event, including its id and Schnorr signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// SHA-256 hash of the canonical event JSON, in lowercase hex.
    pub id: String,
    /// The author's x-only secp256k1 public key, in lowercase hex.
    pub pubkey: String,
    /// Unix timestamp in seconds.
    pub created_at: u64,
    /// NIP-01 event kind.
    pub kind: u32,
    /// Ordered event tags and their string values.
    pub tags: Vec<Vec<String>>,
    /// Event content.
    pub content: String,
    /// BIP-340 signature over the id, in lowercase hex.
    pub sig: String,
}

/// How a relay treats a kind (NIP-01 "Kinds").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KindClass {
    /// A regular event is stored by id.
    Regular,
    /// 0, 3, 10000..20000: latest per (pubkey, kind).
    Replaceable,
    /// 20000..30000: relayed, never stored.
    Ephemeral,
    /// 30000..40000: latest per (pubkey, kind, d tag).
    Addressable,
}

/// Rejection reasons for malformed or invalid events.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventError {
    /// A required field has the wrong shape.
    #[error("invalid: {0}")]
    Shape(String),
    /// The supplied id does not match the canonical event.
    #[error("invalid: event id does not match")]
    BadId,
    /// The public key or signature is invalid.
    #[error("invalid: bad signature")]
    BadSignature,
}

/// Checks the fixed-width lowercase hex used by events and filters.
pub(super) fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn string_field<'a>(value: &'a Value, name: &str) -> Result<&'a str, EventError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| EventError::Shape(format!("{name} must be a string")))
}

fn hex_field(value: &Value, name: &str, len: usize) -> Result<String, EventError> {
    let field = string_field(value, name)?;
    if !is_lower_hex(field, len) {
        return Err(EventError::Shape(format!(
            "{name} must be {len} lowercase hex characters"
        )));
    }
    Ok(field.to_owned())
}

// The fixed buffers reject wrong lengths before k256's byte parsers, which
// expect exactly 32 bytes for a key and 64 bytes for a signature.
fn verify_schnorr(pubkey_hex: &str, msg32: &[u8; 32], sig_hex: &str) -> bool {
    let mut pubkey_bytes = [0u8; 32];
    let mut sig_bytes = [0u8; 64];
    if hex::decode_to_slice(pubkey_hex, &mut pubkey_bytes).is_err()
        || hex::decode_to_slice(sig_hex, &mut sig_bytes).is_err()
    {
        return false;
    }
    let (Ok(key), Ok(sig)) = (
        VerifyingKey::from_bytes(&pubkey_bytes),
        Signature::try_from(sig_bytes.as_slice()),
    ) else {
        return false;
    };
    key.verify_raw(msg32, &sig).is_ok()
}

impl Event {
    /// Parses the required NIP-01 fields and ignores additional fields.
    pub fn parse(value: &Value) -> Result<Event, EventError> {
        if !value.is_object() {
            return Err(EventError::Shape("event must be an object".into()));
        }
        let id = hex_field(value, "id", 64)?;
        let pubkey = hex_field(value, "pubkey", 64)?;
        let sig = hex_field(value, "sig", 128)?;
        let created_at = value
            .get("created_at")
            .and_then(Value::as_u64)
            .ok_or_else(|| EventError::Shape("created_at must be a non-negative integer".into()))?;
        let kind = value
            .get("kind")
            .and_then(Value::as_u64)
            .filter(|kind| *kind <= 65_535)
            .ok_or_else(|| EventError::Shape("kind must be an integer from 0 to 65535".into()))?
            as u32;
        let tags = value
            .get("tags")
            .and_then(Value::as_array)
            .ok_or_else(|| EventError::Shape("tags must be an array of string arrays".into()))?
            .iter()
            .map(|tag| {
                tag.as_array()
                    .ok_or_else(|| EventError::Shape("tags must be arrays of strings".into()))?
                    .iter()
                    .map(|part| {
                        part.as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| EventError::Shape("tag values must be strings".into()))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let content = string_field(value, "content")?.to_owned();
        Ok(Event {
            id,
            pubkey,
            created_at,
            kind,
            tags,
            content,
            sig,
        })
    }

    /// Serializes the NIP-01 array used to compute the event id.
    pub fn canonical_json(&self) -> String {
        serde_json::to_string(&json!([
            0,
            self.pubkey,
            self.created_at,
            self.kind,
            self.tags,
            self.content
        ]))
        .expect("event fields are JSON serializable")
    }

    /// Computes the lowercase SHA-256 hash of the canonical JSON.
    pub fn compute_id(&self) -> String {
        hex::encode(Sha256::digest(self.canonical_json().as_bytes()))
    }

    /// Verifies the id and then the BIP-340 signature over its raw bytes.
    pub fn verify(&self) -> Result<(), EventError> {
        if self.compute_id() != self.id {
            return Err(EventError::BadId);
        }
        let mut id = [0u8; 32];
        if hex::decode_to_slice(&self.id, &mut id).is_err()
            || !verify_schnorr(&self.pubkey, &id, &self.sig)
        {
            return Err(EventError::BadSignature);
        }
        Ok(())
    }

    /// Returns the NIP-01 storage class of the event's kind.
    pub fn kind_class(&self) -> KindClass {
        match self.kind {
            0 | 3 | 10_000..20_000 => KindClass::Replaceable,
            20_000..30_000 => KindClass::Ephemeral,
            30_000..40_000 => KindClass::Addressable,
            _ => KindClass::Regular,
        }
    }

    /// Returns the first `d` tag's value, or the empty string.
    pub fn d_tag(&self) -> &str {
        self.tags
            .iter()
            .find(|tag| tag.first().is_some_and(|name| name == "d"))
            .and_then(|tag| tag.get(1))
            .map_or("", String::as_str)
    }

    /// Iterates over the second element of each tag with the given name.
    pub fn tag_values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.tags.iter().filter_map(move |tag| {
            if tag.first().is_some_and(|tag_name| tag_name == name) {
                tag.get(1).map(String::as_str)
            } else {
                None
            }
        })
    }

    /// Parses the first NIP-40 expiration tag's value, if present.
    pub fn expiration(&self) -> Option<u64> {
        self.tags
            .iter()
            .find(|tag| tag.first().is_some_and(|name| name == "expiration"))
            .and_then(|tag| tag.get(1))
            .and_then(|value| value.parse().ok())
    }
}

/// Signs a well-formed event with `secret` (32-byte secp256k1 key), for
/// tests across the `nostr` module.
#[cfg(test)]
pub fn test_event(
    secret: &[u8; 32],
    created_at: u64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: &str,
) -> Event {
    let key = k256::schnorr::SigningKey::from_bytes(secret).expect("valid test secret");
    let mut event = Event {
        id: String::new(),
        pubkey: hex::encode(key.verifying_key().to_bytes()),
        created_at,
        kind,
        tags,
        content: content.to_owned(),
        sig: String::new(),
    };
    event.id = event.compute_id();
    let mut id = [0u8; 32];
    hex::decode_to_slice(&event.id, &mut id).expect("computed id is 32-byte hex");
    event.sig = hex::encode(
        key.sign_raw(&id, &[0u8; 32])
            .expect("sign test event")
            .to_bytes(),
    );
    event
}

/// Hex x-only pubkey of `secret`.
#[cfg(test)]
pub fn test_pubkey(secret: &[u8; 32]) -> String {
    let key = k256::schnorr::SigningKey::from_bytes(secret).expect("valid test secret");
    hex::encode(key.verifying_key().to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_uses_nip01_escaping() {
        let event = Event {
            id: String::new(),
            pubkey: "0".repeat(64),
            created_at: 123,
            kind: 1,
            tags: vec![vec!["p".into(), "value".into()]],
            content: "line\nquote\"slash\\tab\t\u{0001}é🚀".into(),
            sig: String::new(),
        };
        assert_eq!(
            event.canonical_json(),
            concat!(
                r#"[0,"0000000000000000000000000000000000000000000000000000000000000000",123,1,"#,
                r#"[["p","value"]],"line\nquote\"slash\\tab\t\u0001é🚀"]"#
            )
        );
    }

    #[test]
    fn bip340_official_verification_vectors() {
        // Vectors 0, 1, 5 and 6 from BIP-340's test-vectors.csv.
        let vectors = [
            (
                "f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9",
                "0000000000000000000000000000000000000000000000000000000000000000",
                concat!(
                    "e907831f80848d1069a5371b402410364bdf1c5f8307b0084c55f1ce2dca8215",
                    "25f66a4a85ea8b71e482a74f382d2ce5ebeee8fdb2172f477df4900d310536c0"
                ),
                true,
            ),
            (
                "eefdea4cdb677750a420fee807eacf21eb9898ae79b9768766e4faa04a2d4a34",
                "243f6a8885a308d313198a2e03707344a4093822299f31d0082efa98ec4e6c89",
                concat!(
                    "6cff5c3ba86c69ea4b7376f31a9bcb4f74c1976089b2d9963da2e5543e177769",
                    "69e89b4c5564d00349106b8497785dd7d1d713a8ae82b32fa79d5f7fc407d39b"
                ),
                false,
            ),
            (
                "dff1d77f2a671c5f36183726db2341be58feae1da2deced843240f7b502ba659",
                "243f6a8885a308d313198a2e03707344a4093822299f31d0082efa98ec4e6c89",
                concat!(
                    "6896bd60eeae296db48a229ff71dfe071bde413e6d43f917dc8dcf8c78de3341",
                    "8906d11ac976abccb20b091292bff4ea897efcb639ea871cfa95f6de339e4b0a"
                ),
                true,
            ),
            (
                "dff1d77f2a671c5f36183726db2341be58feae1da2deced843240f7b502ba659",
                "243f6a8885a308d313198a2e03707344a4093822299f31d0082efa98ec4e6c89",
                concat!(
                    "fff97bd5755eeea420453a14355235d382f6472f8568a18b2f057a1460297556",
                    "3cc27944640ac607cd107ae10923d9ef7a73c643e166be5ebeafa34b1ac553e2"
                ),
                false,
            ),
        ];
        for (pubkey, message, signature, expected) in vectors {
            let mut msg32 = [0u8; 32];
            hex::decode_to_slice(message, &mut msg32).unwrap();
            assert_eq!(verify_schnorr(pubkey, &msg32, signature), expected);
        }
    }

    #[test]
    fn signed_event_round_trip_and_tampering() {
        let event = test_event(&[1; 32], 123, 1, vec![], "hello");
        assert_eq!(event.pubkey, test_pubkey(&[1; 32]));
        assert_eq!(event.verify(), Ok(()));
        assert_eq!(
            Event::parse(&serde_json::to_value(&event).unwrap()),
            Ok(event.clone())
        );

        let mut changed = event.clone();
        changed.content.push('!');
        assert_eq!(changed.verify(), Err(EventError::BadId));

        let mut changed = event.clone();
        changed.sig = "00".repeat(64);
        assert_eq!(changed.verify(), Err(EventError::BadSignature));

        let mut changed = event.clone();
        changed.pubkey = test_pubkey(&[2; 32]);
        changed.id = changed.compute_id();
        assert_eq!(changed.verify(), Err(EventError::BadSignature));

        for bad_sig in ["", "aa", &"z".repeat(128), &"aa".repeat(65)] {
            let mut changed = event.clone();
            changed.sig = bad_sig.into();
            assert_eq!(changed.verify(), Err(EventError::BadSignature));
        }
        let mut changed = event;
        changed.pubkey = "bad".into();
        changed.id = changed.compute_id();
        assert_eq!(changed.verify(), Err(EventError::BadSignature));
    }

    #[test]
    fn parse_rejects_bad_shapes() {
        let event = test_event(&[1; 32], 123, 1, vec![], "hello");
        let valid = serde_json::to_value(event).unwrap();
        assert!(matches!(
            Event::parse(&json!([])),
            Err(EventError::Shape(_))
        ));
        for (field, bad) in [
            ("id", json!("A".repeat(64))),
            ("id", json!("0".repeat(63))),
            ("id", json!("é".repeat(32))),
            ("pubkey", json!("A".repeat(64))),
            ("sig", json!("0".repeat(127))),
            ("sig", json!("F".repeat(128))),
            ("created_at", json!(1.0)),
            ("created_at", json!(-1)),
            ("created_at", json!("123")),
            ("kind", json!(70_000)),
            ("kind", json!(1.0)),
            ("kind", json!(-1)),
            ("tags", json!([["p", 1]])),
            ("tags", json!(["p"])),
            ("content", json!(null)),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = bad;
            assert!(
                matches!(Event::parse(&invalid), Err(EventError::Shape(_))),
                "{field}"
            );
        }
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove("content");
        assert!(matches!(Event::parse(&missing), Err(EventError::Shape(_))));
        let mut extra = valid;
        extra["extra"] = json!({"ignored": true});
        assert!(Event::parse(&extra).is_ok());
        extra["created_at"] = json!(u64::MAX);
        extra["kind"] = json!(65_535);
        let parsed = Event::parse(&extra).unwrap();
        assert_eq!(parsed.created_at, u64::MAX);
        assert_eq!(parsed.kind, 65_535);
    }

    #[test]
    fn kind_class_boundaries() {
        let mut event = test_event(&[1; 32], 123, 1, vec![], "");
        for (kind, expected) in [
            (0, KindClass::Replaceable),
            (1, KindClass::Regular),
            (3, KindClass::Replaceable),
            (4, KindClass::Regular),
            (9_999, KindClass::Regular),
            (10_000, KindClass::Replaceable),
            (19_999, KindClass::Replaceable),
            (20_000, KindClass::Ephemeral),
            (29_999, KindClass::Ephemeral),
            (30_000, KindClass::Addressable),
            (39_999, KindClass::Addressable),
            (40_000, KindClass::Regular),
            (65_535, KindClass::Regular),
        ] {
            event.kind = kind;
            assert_eq!(event.kind_class(), expected, "kind {kind}");
        }
    }

    #[test]
    fn tag_values_d_tag_and_expiration() {
        let mut event = test_event(
            &[1; 32],
            123,
            1,
            vec![
                vec![],
                vec!["d".into()],
                vec!["d".into(), "second".into()],
                vec!["p".into(), "one".into(), "ignored".into()],
                vec!["p".into()],
                vec!["p".into(), "two".into()],
                vec!["expiration".into(), "invalid".into()],
                vec!["expiration".into(), "456".into()],
            ],
            "",
        );
        assert_eq!(event.d_tag(), "");
        assert_eq!(event.tag_values("p").collect::<Vec<_>>(), ["one", "two"]);
        assert_eq!(event.expiration(), None);
        event.tags.remove(1);
        assert_eq!(event.d_tag(), "second");
        event.tags.remove(5);
        assert_eq!(event.expiration(), Some(456));
        event.tags = vec![
            vec!["expiration".into()],
            vec!["expiration".into(), "7".into()],
        ];
        assert_eq!(event.expiration(), None);
        event.tags.clear();
        assert_eq!(event.d_tag(), "");
        assert_eq!(event.expiration(), None);
    }
}
