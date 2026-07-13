//! `wireSign` interop: sign and verify P2P wire messages the same way the
//! tc-* web apps do, so a bot-run mistl node can publish/consume the shared
//! wire contracts (`tc-news:article`, `tc-chat:post`, ...) as an ordinary
//! DID peer.
//!
//! Interop contract (matches tc-chat's `src/lib/wireSign.ts` +
//! `src/crypto/didIdentity.ts`; tc-news's `src/lib/wireSign.ts` is the same
//! logic):
//! - The signing payload is every field of the wire object *except*
//!   `signature`, serialized by [`stable_stringify`]: object keys sorted,
//!   `undefined`/absent fields dropped, everything else `JSON.stringify`-
//!   compatible.
//! - The signature is over that payload's UTF-8 bytes, Ed25519, encoded as
//!   unpadded base64url (`toBase64Url(bytesToBase64(signature))` on the TS
//!   side).
//! - `fromId` is the signer's `did:key:z...` (Ed25519, `0xed01` multicodec).
//!   There is no field whitelist, so adding fields to a wire never breaks
//!   old verifiers.
//!
//! did:key <-> Ed25519 public key conversion and the actual Ed25519
//! sign/verify are **not** re-implemented here: they're reused from
//! [`crate::identity`] ([`Identity::sign`], [`Identity::did`],
//! [`crate::identity::verify`]) so there is exactly one did:key
//! implementation in this crate.
//!
//! See also the legacy Rust port in `tc-chat/cli/src/wire.rs` /
//! `tc-chat/cli/src/stable_json.rs` (signing logic only -- its wire *types*
//! are stale and intentionally not mirrored here; see
//! `tc-docs/drafts/bot-pipeline-v1.md` gap #5).

use anyhow::{Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Map, Number, Value};

use crate::identity::Identity;

/// Deterministic, key-sorted JSON stringification, byte-for-byte compatible
/// with tc-chat/tc-news's `stableStringify` (`src/lib/wireSign.ts`):
///
/// - Object keys are sorted (TS sorts with `String.prototype.localeCompare`;
///   every field name used by tik-choco wire contracts is plain ASCII
///   lowercase/camelCase, for which `localeCompare` agrees with ordinal
///   byte/code-point order, so `str::cmp` here matches it exactly -- see the
///   same reasoning documented in `tc-chat/cli/src/stable_json.rs`).
/// - Arrays keep their original order.
/// - Strings use the same escaping `JSON.stringify` uses (control chars,
///   `"`/`\` escaped; non-ASCII text incl. Japanese/emoji is emitted raw,
///   *not* `\uXXXX`-escaped) -- `serde_json`'s default string serialization
///   already matches this.
/// - Numbers: integers print as plain integers. Floats are the tricky part
///   -- `serde_json::Number`'s own `Display`/`Serialize` (via the `ryu`
///   crate) always writes a decimal point for float-typed values (e.g.
///   `2.0` -> `"2.0"`), whereas JS's `JSON.stringify` never does
///   (`JSON.stringify(2.0) === "2"`, and `JSON.stringify(-0) === "0"`)
///   because every JS number is conceptually a float with no separate
///   "integer" representation. [`format_number`] special-cases this by
///   using Rust's `f64` `Display` (which, unlike `Debug`, does *not* append
///   a trailing `.0` to whole numbers) instead of `serde_json`'s formatter.
///   Verified against real `JSON.stringify` output in the interop test
///   vectors below (`wholeFloatVal: 2.0` -> `2`, `negZeroVal: -0` -> `0`).
/// - `null` round-trips as `null` (this mirrors *JS* `null`; there is no
///   Rust equivalent of JS `undefined` filtering to reproduce here --
///   `serde_json::Value` has no "undefined" variant, so callers that want a
///   field to behave like a `signWireFields` caller's `undefined` field
///   must omit the key entirely rather than set it to `Value::Null`).
pub fn stable_stringify(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => format_number(n),
        Value::String(s) => serde_json::to_string(s).expect("strings always serialize to JSON"),
        Value::Array(items) => {
            let body = items.iter().map(stable_stringify).collect::<Vec<_>>().join(",");
            format!("[{body}]")
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let body = entries
                .iter()
                .map(|(key, val)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("keys always serialize to JSON"),
                        stable_stringify(val)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
    }
}

/// Formats a `serde_json::Number` the way JS's `JSON.stringify` formats a
/// `number`: integers as plain integers, floats via the shortest
/// round-tripping decimal with no forced trailing `.0` and `-0` normalized
/// to `0`.
///
/// Known gap vs. JS: JS switches to exponential notation for `|x| >= 1e21`
/// or `0 < |x| < 1e-6`; Rust's `f64` `Display` never does. None of the
/// tik-choco wire contracts carry numbers anywhere near those magnitudes
/// (timestamps, byte sizes, counts, scores), so this is not handled.
fn format_number(n: &Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    let f = n.as_f64().unwrap_or(0.0);
    if f == 0.0 {
        // Covers both +0.0 and -0.0: JSON.stringify(-0) === "0".
        return "0".to_string();
    }
    // `{}` (Display) for f64, unlike `{:?}` (Debug), does not append a
    // trailing ".0" for whole numbers -- matches JSON.stringify.
    format!("{f}")
}

/// Every field of `wire` except `signature`, stably stringified. Mirrors
/// `signingPayload` in `wireSign.ts`.
fn signing_payload(wire: &Map<String, Value>) -> String {
    let mut unsigned = wire.clone();
    unsigned.remove("signature");
    stable_stringify(&Value::Object(unsigned))
}

/// Signs every field of `obj` except `signature` with `identity`'s Ed25519
/// key and writes the result into `obj.signature` (unpadded base64url, as
/// `signWireFields` produces). `obj.fromId` must already be set to
/// `identity.did()` -- mirrors `signWireFields`'s
/// `identity.did !== fields.fromId` check.
pub fn sign_wire(obj: &mut Value, identity: &Identity) -> Result<()> {
    let map = obj
        .as_object_mut()
        .ok_or_else(|| anyhow!("wire must be a JSON object"))?;

    let from_id = map
        .get("fromId")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("wire is missing a string `fromId` field"))?;
    if from_id != identity.did() {
        bail!("wire fromId does not match the local DID identity");
    }

    let payload = signing_payload(map);
    let signature = URL_SAFE_NO_PAD.encode(identity.sign(payload.as_bytes()));
    map.insert("signature".to_string(), Value::String(signature));
    Ok(())
}

/// Verifies `obj.signature` against every other field, keyed by
/// `obj.fromId`. Mirrors `verifyWire`: untrusted peer input, so this never
/// panics and returns `Ok(false)` (rather than an `Err`) for every "not
/// authentic" case TS's `verifyWire` returns `false` for (missing/wrong-type
/// `fromId`/`signature`, `fromId` not an Ed25519 `did:key`, signature
/// mismatch). `Err` is reserved for truly unexpected conditions.
pub fn verify_wire(obj: &Value) -> Result<bool> {
    let Some(map) = obj.as_object() else {
        return Ok(false);
    };
    let Some(from_id) = map.get("fromId").and_then(Value::as_str) else {
        return Ok(false);
    };
    let Some(signature_str) = map.get("signature").and_then(Value::as_str) else {
        return Ok(false);
    };
    let Ok(signature_bytes) = URL_SAFE_NO_PAD.decode(signature_str) else {
        return Ok(false);
    };

    let payload = signing_payload(map);
    // `crate::identity::verify` rejects malformed/non-Ed25519 `did:key`
    // strings with an `Err`, which stands in for TS's separate
    // `isEd25519DidKey` pre-check -- both paths mean "not authentic" here.
    match crate::identity::verify(from_id, payload.as_bytes(), &signature_bytes) {
        Ok(valid) => Ok(valid),
        Err(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- Interop test vectors ----
    // Generated by running tc-chat's *actual* wireSign.ts/didIdentity.ts
    // logic under Node's WebCrypto (not a from-memory reimplementation): a
    // fixed Ed25519 keypair signs each `wire` object below exactly as
    // `signWireFields`/`stableStringify` would in a browser, producing the
    // real `payload` (signingPayload() output) and `signature` (real
    // Ed25519 signature, unpadded base64url) recorded here. See the
    // bot-pipeline-v1 scratchpad scripts `gen-vectors.mjs` (runs the ported
    // TS logic) / `gen-rust-tests.mjs` (emits this block) for exactly how;
    // not committed to this repo -- regenerate with those scripts if
    // wireSign.ts's logic ever changes.

    // Kept for a future sign_wire vector test once a test-only Identity
    // constructor exists in crate::identity (see the module doc for why
    // that's out of this change's scope); unused by the current tests.
    #[allow(dead_code)]
    const FIXED_SEED_HEX: &str = "ca22031e7789688e6f8a4e1256edcf7be407877a8b0f0734e7641e2d391be5ed";
    #[allow(dead_code)]
    const FIXED_DID: &str = "did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4";

    const WIRE_SIMPLE_TEXT_WIRE: &str = r#"{"v":1,"type":"tc-chat:post","surface":"chat","id":"msg-0001","parentId":null,"fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","fromName":"テストBot","timestamp":1700000000000,"kind":"text","cid":"bafybeigdyrzt5example"}"#;
    const PAYLOAD_SIMPLE_TEXT_WIRE: &str = r#"{"cid":"bafybeigdyrzt5example","fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","fromName":"テストBot","id":"msg-0001","kind":"text","parentId":null,"surface":"chat","timestamp":1700000000000,"type":"tc-chat:post","v":1}"#;
    const SIGNATURE_SIMPLE_TEXT_WIRE: &str = r#"TvwjJVPBov7jjqHeQ2EwZi1PkAk5mtzGxeiP9kIrvFDd_uLVZ3F5GhxuFlFBOd8GWkEtdb8dB0juhkMJx_UGAw"#;

    const WIRE_MEDIA_WIRE_WITH_OPTIONALS: &str = r#"{"type":"tc-chat:post","surface":"chat","id":"msg-0002","parentId":"msg-0001","fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","fromName":"mistl-bot","timestamp":1700000123456,"kind":"media","cid":"bafybeih2audioexample","mimeType":"audio/mpeg","fileName":"朝のニュース.mp3","fileSize":234567}"#;
    const PAYLOAD_MEDIA_WIRE_WITH_OPTIONALS: &str = r#"{"cid":"bafybeih2audioexample","fileName":"朝のニュース.mp3","fileSize":234567,"fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","fromName":"mistl-bot","id":"msg-0002","kind":"media","mimeType":"audio/mpeg","parentId":"msg-0001","surface":"chat","timestamp":1700000123456,"type":"tc-chat:post"}"#;
    const SIGNATURE_MEDIA_WIRE_WITH_OPTIONALS: &str = r#"jG1CXv8b8T0Dz1Bhvp3sSUv30n0KoSMq_FOCfYojI9hC1eKNeLzG80uH2_JTZOR9nn4_fSKmBV5l2OZ3oe4oDg"#;

    const WIRE_NESTED_AND_ARRAY_FIELDS: &str = r#"{"type":"tc-news:article","fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","id":"article-42","timestamp":1700500000000,"meta":{"langs":["ja","en"],"sourceLinks":["https://example.com/a","https://example.com/b"],"nested":{"a":1,"b":{"c":2,"d":[1,2,3]}}},"tags":[],"score":0}"#;
    const PAYLOAD_NESTED_AND_ARRAY_FIELDS: &str = r#"{"fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","id":"article-42","meta":{"langs":["ja","en"],"nested":{"a":1,"b":{"c":2,"d":[1,2,3]}},"sourceLinks":["https://example.com/a","https://example.com/b"]},"score":0,"tags":[],"timestamp":1700500000000,"type":"tc-news:article"}"#;
    const SIGNATURE_NESTED_AND_ARRAY_FIELDS: &str = r#"lcj4JU5vz0Xa8-ETIOTCMg4Xu5hYOvdOmG96zZ6Eq7wi1uwGo1LEZs1COGYJe-ebN2QCK41_3bOcztWkGv2zCQ"#;

    const WIRE_NUMERIC_EDGE_CASES: &str = r#"{"type":"tc-bot:test-numbers","fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","intVal":42,"negIntVal":-17,"floatVal":3.14159,"wholeFloatVal":2,"zeroVal":0,"negZeroVal":0,"smallFloat":0.0001,"bigInt":1700000000000,"negFloat":-2.5}"#;
    const PAYLOAD_NUMERIC_EDGE_CASES: &str = r#"{"bigInt":1700000000000,"floatVal":3.14159,"fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","intVal":42,"negFloat":-2.5,"negIntVal":-17,"negZeroVal":0,"smallFloat":0.0001,"type":"tc-bot:test-numbers","wholeFloatVal":2,"zeroVal":0}"#;
    const SIGNATURE_NUMERIC_EDGE_CASES: &str = r#"dNpnqeHNHe3gBjgSYvm1NuogNOM128ROcQuEZNJwRKadXAvYML3HFgLR7G1NCVLeKI6Vjkw729Z22npgZfGPDQ"#;

    const WIRE_NULL_AND_JAPANESE_STRINGS: &str = r#"{"type":"tc-bot:test-strings","fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","nullable":null,"japanese":"日本語のテスト文字列です。「引用」と\\バックスラッシュ、改行\nタブ\t。","emoji":"絵文字🎉テスト","empty":"","withQuotes":"he said \"hello\""}"#;
    const PAYLOAD_NULL_AND_JAPANESE_STRINGS: &str = r#"{"emoji":"絵文字🎉テスト","empty":"","fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","japanese":"日本語のテスト文字列です。「引用」と\\バックスラッシュ、改行\nタブ\t。","nullable":null,"type":"tc-bot:test-strings","withQuotes":"he said \"hello\""}"#;
    const SIGNATURE_NULL_AND_JAPANESE_STRINGS: &str = r#"kI5qt0RuzIuynUsyRrl8P6qbucMKGD_6CPD6nKEHi2g5cckkeyqmPij2PDpANXUuZLIK8eRfG-4HCJ1j0VwdDg"#;

    const WIRE_WEBHOOK_DELIVERY_WIRE: &str = r#"{"v":1,"type":"tc-bot:delivery","fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","pipeline":"news-audio","item":{"articleId":"a-1","title":"記事タイトル","excerpt":"記事の抜粋テキスト","sourceLinks":["https://example.com/src1"],"publishedAt":"2026-07-12T00:00:00.000Z","audio":{"mime":"audio/mpeg","size":12345}}}"#;
    const PAYLOAD_WEBHOOK_DELIVERY_WIRE: &str = r#"{"fromId":"did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4","item":{"articleId":"a-1","audio":{"mime":"audio/mpeg","size":12345},"excerpt":"記事の抜粋テキスト","publishedAt":"2026-07-12T00:00:00.000Z","sourceLinks":["https://example.com/src1"],"title":"記事タイトル"},"pipeline":"news-audio","type":"tc-bot:delivery","v":1}"#;
    const SIGNATURE_WEBHOOK_DELIVERY_WIRE: &str = r#"b_dBkQh7e7UOJaE92s1DksvlZPD6M57Hg8UY0P60Mbw2MRC7znJhCpyTOLzWuw80lwuO_UXXx_VXY0-Yll0vBA"#;

    const ALL_CASES: &[(&str, &str, &str)] = &[
        (WIRE_SIMPLE_TEXT_WIRE, PAYLOAD_SIMPLE_TEXT_WIRE, SIGNATURE_SIMPLE_TEXT_WIRE),
        (WIRE_MEDIA_WIRE_WITH_OPTIONALS, PAYLOAD_MEDIA_WIRE_WITH_OPTIONALS, SIGNATURE_MEDIA_WIRE_WITH_OPTIONALS),
        (WIRE_NESTED_AND_ARRAY_FIELDS, PAYLOAD_NESTED_AND_ARRAY_FIELDS, SIGNATURE_NESTED_AND_ARRAY_FIELDS),
        (WIRE_NUMERIC_EDGE_CASES, PAYLOAD_NUMERIC_EDGE_CASES, SIGNATURE_NUMERIC_EDGE_CASES),
        (WIRE_NULL_AND_JAPANESE_STRINGS, PAYLOAD_NULL_AND_JAPANESE_STRINGS, SIGNATURE_NULL_AND_JAPANESE_STRINGS),
        (WIRE_WEBHOOK_DELIVERY_WIRE, PAYLOAD_WEBHOOK_DELIVERY_WIRE, SIGNATURE_WEBHOOK_DELIVERY_WIRE),
    ];

    #[test]
    fn stable_stringify_matches_ts_for_all_vectors() {
        for (wire_json, expected_payload, _signature) in ALL_CASES {
            let wire: Value = serde_json::from_str(wire_json).expect("vector wire is valid JSON");
            let map = wire.as_object().unwrap();
            assert_eq!(&signing_payload(map), expected_payload, "payload mismatch for {wire_json}");
        }
    }

    #[test]
    fn verify_wire_accepts_ts_produced_signatures_for_all_vectors() {
        // Exercises the full verify_wire pipeline (stable_stringify +
        // did:key -> Ed25519 pubkey via crate::identity + signature check)
        // against signatures a real browser-side wireSign.ts run would
        // produce -- the strongest interop guarantee available without a
        // test-only Identity constructor (see below).
        for (wire_json, _payload, signature) in ALL_CASES {
            let mut wire: Value = serde_json::from_str(wire_json).expect("vector wire is valid JSON");
            wire.as_object_mut().unwrap().insert("signature".to_string(), json!(signature));
            assert!(verify_wire(&wire).unwrap(), "verify_wire rejected a TS-signed vector: {wire_json}");
        }
    }

    #[test]
    fn verify_wire_rejects_tampered_field() {
        let (wire_json, _payload, signature) = ALL_CASES[0];
        let mut wire: Value = serde_json::from_str(wire_json).expect("vector wire is valid JSON");
        wire.as_object_mut().unwrap().insert("signature".to_string(), json!(signature));
        wire["fromName"] = json!("attacker");
        assert!(!verify_wire(&wire).unwrap());
    }

    #[test]
    fn verify_wire_rejects_missing_from_id_or_signature() {
        assert!(!verify_wire(&json!({"signature": "abc"})).unwrap());
        assert!(!verify_wire(&json!({"fromId": "did:key:z6Mk..."})).unwrap());
        assert!(!verify_wire(&json!("not-an-object")).unwrap());
    }

    #[test]
    fn verify_wire_rejects_non_ed25519_did() {
        let wire = json!({"fromId": "did:key:znotanactualkey", "signature": "abc", "dummy": 1});
        assert!(!verify_wire(&wire).unwrap());
    }

    #[test]
    fn verify_wire_rejects_malformed_signature_encoding() {
        let wire = json!({"fromId": "did:key:z6MkrNaiFHW7PvxfTPbQJKg74twJH4v7BgcPJxQxoN6cCaQ4", "signature": "not valid base64url!!"});
        assert!(!verify_wire(&wire).unwrap());
    }

    #[test]
    fn format_number_matches_js_json_stringify_semantics() {
        // JSON.stringify(2.0) === "2"; JSON.stringify(-0) === "0" -- see
        // vectors.json's `numeric_edge_cases` case (wholeFloatVal/negZeroVal)
        // for this same behavior confirmed straight from a real JS engine.
        assert_eq!(stable_stringify(&json!(2.0)), "2");
        assert_eq!(stable_stringify(&json!(-0.0)), "0");
        assert_eq!(stable_stringify(&json!(0)), "0");
        assert_eq!(stable_stringify(&json!(3.14159)), "3.14159");
        assert_eq!(stable_stringify(&json!(-17)), "-17");
        assert_eq!(stable_stringify(&json!(1700000000000i64)), "1700000000000");
    }

    #[test]
    fn stable_stringify_sorts_keys_and_keeps_array_order() {
        assert_eq!(stable_stringify(&json!({"b": 1, "a": 2})), r#"{"a":2,"b":1}"#);
        assert_eq!(stable_stringify(&json!({"tags": ["b", "a"]})), r#"{"tags":["b","a"]}"#);
    }

    #[test]
    fn signing_payload_excludes_signature_field() {
        let wire = json!({"fromId": "did:key:z6Mk...", "signature": "sig"});
        assert_eq!(signing_payload(wire.as_object().unwrap()), r#"{"fromId":"did:key:z6Mk..."}"#);
    }

    // `sign_wire`'s own logic (the `fromId` equality check, and the
    // sign-then-encode happy path) is covered directly below, now that
    // `crate::identity::for_test` (Wave 2) gives tests a real `Identity`
    // without touching this machine's persistent per-user data directory --
    // see that function's doc comment. The vector-based tests above already
    // cover byte-exact interop with TS-produced payloads/signatures; these
    // cover `sign_wire`/`verify_wire` end to end against each other.

    #[test]
    fn sign_wire_then_verify_wire_round_trips() {
        let identity = crate::identity::for_test();
        let mut wire = json!({
            "type": "tc-bot:test",
            "fromId": identity.did(),
            "id": "wire-1",
            "nested": { "a": 1, "b": [1, 2, 3] },
        });

        sign_wire(&mut wire, &identity).expect("signing a well-formed wire must succeed");
        assert!(wire.get("signature").and_then(Value::as_str).is_some());
        assert!(verify_wire(&wire).unwrap(), "a freshly-signed wire must verify");

        // Tampering with any signed field (not just `fromId`) must invalidate it.
        let mut tampered = wire.clone();
        tampered["id"] = json!("wire-2");
        assert!(!verify_wire(&tampered).unwrap());

        // A different identity's signature must not verify against this one's DID.
        let other = crate::identity::for_test();
        assert_ne!(other.did(), identity.did());
        let mut wrong_signer_wire = json!({
            "type": "tc-bot:test",
            "fromId": other.did(),
            "id": "wire-1",
        });
        sign_wire(&mut wrong_signer_wire, &other).unwrap();
        wrong_signer_wire["fromId"] = json!(identity.did());
        assert!(!verify_wire(&wrong_signer_wire).unwrap());
    }

    #[test]
    fn sign_wire_rejects_a_from_id_mismatched_with_the_signing_identity() {
        let identity = crate::identity::for_test();
        let mut wire = json!({ "type": "tc-bot:test", "fromId": "did:key:zSomeoneElse" });
        let err = sign_wire(&mut wire, &identity).expect_err("mismatched fromId must be rejected");
        assert!(err.to_string().contains("fromId"));
    }

    #[test]
    fn sign_wire_rejects_a_wire_missing_from_id() {
        let identity = crate::identity::for_test();
        let mut wire = json!({ "type": "tc-bot:test" });
        assert!(sign_wire(&mut wire, &identity).is_err());
    }
}
