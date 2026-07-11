//! Rust port of tc-chat's `src/lib/wireSign.ts` `stableStringify` (itself a
//! copy of tc-storage's `p2pEnvelope.ts` `stableStringify`): deterministic,
//! key-sorted JSON with `undefined`/absent object entries never emitted.
//! Byte-identical to `tc-chat/cli/src/stable_json.rs`, which this is copied
//! from -- see that file's module doc for why plain `str::cmp` here agrees
//! with the TS side's `String.prototype.localeCompare` for every field name
//! this codebase signs or verifies (id, fromId, fromName, timestamp, kind,
//! cid, mimeType, fileName, fileSize, type, surface, parentId, targetId,
//! emoji, op, roomId, signature, ...).
//!
//! Used by [`super::chat_relay`] to reconstruct the exact signing payload of
//! an inbound `tc-chat:*` wire so its signature can be verified against the
//! sender's `did:key` (`crate::identity::verify`).

use serde_json::Value;

pub fn stable_stringify(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let body = entries
                .iter()
                .map(|(key, val)| format!("{}:{}", serde_json::to_string(key).unwrap(), stable_stringify(val)))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Value::Array(items) => {
            let body = items.iter().map(stable_stringify).collect::<Vec<_>>().join(",");
            format!("[{body}]")
        }
        _ => serde_json::to_string(value).unwrap(),
    }
}

/// Builds the signing payload for a wire message: every field except
/// `signature`, stably stringified. Mirrors tc-chat's `signingPayload` in
/// `wireSign.ts`.
pub fn signing_payload(wire: &serde_json::Map<String, Value>) -> String {
    let mut unsigned = wire.clone();
    unsigned.remove("signature");
    stable_stringify(&Value::Object(unsigned))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_object_keys() {
        let value = json!({"b": 1, "a": 2});
        assert_eq!(stable_stringify(&value), r#"{"a":2,"b":1}"#);
    }

    #[test]
    fn preserves_array_order() {
        let value = json!({"tags": ["b", "a"]});
        assert_eq!(stable_stringify(&value), r#"{"tags":["b","a"]}"#);
    }

    #[test]
    fn excludes_signature_from_signing_payload() {
        let mut map = serde_json::Map::new();
        map.insert("fromId".to_string(), json!("did:key:z6Mk..."));
        map.insert("signature".to_string(), json!("sig"));
        assert_eq!(signing_payload(&map), r#"{"fromId":"did:key:z6Mk..."}"#);
    }

    /// Known vector shared with `tc-chat/cli/src/stable_json.rs`'s
    /// `matches_a_known_chat_wire_payload` test, confirming this port
    /// produces byte-identical output to the upstream CLI (and, by that
    /// CLI's own byte-for-byte-with-the-web-app claim, to tc-chat's browser
    /// client too).
    #[test]
    fn matches_a_known_chat_wire_payload() {
        let mut map = serde_json::Map::new();
        map.insert("type".to_string(), json!("tc-chat:message"));
        map.insert("id".to_string(), json!("abc"));
        map.insert("fromId".to_string(), json!("did:key:z6Mk"));
        map.insert("fromName".to_string(), json!("自分"));
        map.insert("timestamp".to_string(), json!(1234567890123u64));
        map.insert("kind".to_string(), json!("text"));
        map.insert("cid".to_string(), json!("bafy..."));
        assert_eq!(
            signing_payload(&map),
            r#"{"cid":"bafy...","fromId":"did:key:z6Mk","fromName":"自分","id":"abc","kind":"text","timestamp":1234567890123,"type":"tc-chat:message"}"#
        );
    }
}
