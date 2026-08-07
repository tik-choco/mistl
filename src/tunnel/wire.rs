//! Wire types shared with the standalone `p2p` binary's tunnel protocol.
//!
//! Ported verbatim from `p2p/src/rtc/tunnel_message.rs` and
//! `p2p/src/rtc/wire_bytes.rs` (merged into this one file per this port's
//! module layout -- see `TUNNEL_INTEGRATION_CONTRACT.md`'s file ownership
//! table). Byte-for-byte wire compatible: a deployed `p2p` binary must still
//! be able to tunnel against this daemon, so `TunnelMessage`'s field
//! names/tags and the base64 byte encoding below must not change.

use base64::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// One tunnel-protocol frame, carried as the `data` bytes of a
/// [`crate::tunnel::rtc::payload::P2pPayload::Tunnel`] envelope. `msg_type`
/// distinguishes control frames (`"open"`, `"close"`, ...) from `"data"`
/// frames; `conn_id` multiplexes many logical connections over one mistlib
/// room.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunnelMessage {
    #[serde(rename = "type")]
    pub msg_type: String,

    #[serde(rename = "conn_id")]
    pub conn_id: String,

    #[serde(rename = "target", default, skip_serializing_if = "String::is_empty")]
    pub target: String,

    #[serde(
        rename = "payload",
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_base64"
    )]
    pub payload: Option<Vec<u8>>,

    /// Per-conn, monotonically increasing (starting at 1) sequence number for
    /// `data` messages, assigned by the sender. Lets the receiver detect the
    /// gaps and duplicates that mistlib's `ReorderBuffer` (drops messages
    /// delayed >1s) and the tunnel's send-retry path (may redeliver an
    /// already-delivered payload) can otherwise introduce silently. `None`
    /// for non-`data` messages and for messages from peers running an older
    /// version that predates this field -- omitted from the wire when unset
    /// so older peers (which ignore unknown fields) keep working.
    #[serde(rename = "seq", default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

/// Bytes are encoded as a `"b64:"`-prefixed base64 string. The legacy forms
/// below are only accepted on decode for cross-version compatibility with
/// older peers/messages; new output is always base64.
#[derive(Deserialize)]
#[serde(untagged)]
enum BytesRepr {
    /// A string payload: either the current `"b64:"`-prefixed base64 form,
    /// or a legacy plain-hex string (no prefix).
    Str(String),
    /// A legacy raw JSON byte array, from before string encoding was used.
    Legacy(Vec<u8>),
}

fn decode_repr<E: serde::de::Error>(repr: BytesRepr) -> Result<Vec<u8>, E> {
    match repr {
        BytesRepr::Str(encoded) => decode_string(&encoded).map_err(E::custom),
        BytesRepr::Legacy(bytes) => Ok(bytes),
    }
}

fn encode_bytes(bytes: &[u8]) -> String {
    format!("b64:{}", BASE64_STANDARD.encode(bytes))
}

fn decode_string(encoded: &str) -> Result<Vec<u8>, String> {
    if let Some(base64) = encoded.strip_prefix("b64:") {
        BASE64_STANDARD.decode(base64).map_err(|e| e.to_string())
    } else {
        hex::decode(encoded).map_err(|e| e.to_string())
    }
}

/// `#[serde(with = "...")]` helper for a required `Vec<u8>` field. The field
/// name in the serialized struct is unaffected by this module's name --
/// only the encoding of its value is controlled here. `pub` (upstream had
/// `pub(crate)`, scoped to the standalone `p2p` crate) since other tunnel
/// workers may reuse the same byte encoding for their own wire types -- see
/// `TUNNEL_INTEGRATION_CONTRACT.md`.
pub mod vec_base64 {
    use super::*;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_bytes(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = BytesRepr::deserialize(deserializer)?;
        decode_repr(repr)
    }
}

/// `#[serde(with = "...")]` helper for an optional `Vec<u8>` field.
pub mod option_base64 {
    use super::*;

    pub fn serialize<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(bytes) => serializer.serialize_some(&encode_bytes(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = Option::<BytesRepr>::deserialize(deserializer)?;
        repr.map(decode_repr).transpose()
    }
}
