//! mistai LLM-network wire protocol, version 1.
//!
//! Byte-compatible with `@tik-choco/mistai`'s `protocol.ts`: every message
//! is a plain JSON object encoded as UTF-8 (no envelope, no length prefix)
//! with `"v": 1` and a `"type"` discriminator; optional fields are omitted
//! entirely (never `null`).
//!
//! ## decode() validation (mirrors protocol.ts)
//!
//! - Not JSON / not an object / `v != 1` / unknown `type` -> `None`.
//! - Unknown extra fields are ignored, never rejected.
//! - "non-empty string" below means `String` with `len > 0`.
//! - `provider_hello`: `models`/`services` optional; present but not an
//!   array -> the field is dropped (message still valid); non-string /
//!   empty-string array elements are filtered out. `services` missing
//!   entirely means "advertises chat only" per the wire spec, but that
//!   default is a *consumer-side* interpretation -- `decode` itself
//!   preserves `None` and leaves defaulting to callers (see
//!   `ProviderHello::effective_services`).
//! - `consumer_hello`: no fields.
//! - `llm_request`: `id` non-empty; `messages` array with >= 1 element,
//!   each `{role: "system"|"user"|"assistant", content: string}`; `model`
//!   optional string. Any violation -> `None`.
//! - `llm_response_chunk`: `id` non-empty; `delta` string (empty ok);
//!   `seq` optional, but if present must be an integer >= 0 (reject
//!   negative, fractional, non-number) -> else `None`.
//! - `llm_response_done`: `id` non-empty; `content` optional string.
//! - `llm_error`: `id` non-empty; `message` string; `code` optional string
//!   (present but non-string -> field dropped, message still valid, same
//!   as `models`/`services`).
//! - `raft_message`: `payload` non-empty string (opaque; passed through).
//! - `tts_request`: `id` non-empty; `text` string; `model`/`voice`
//!   optional strings.
//! - `tts_response` / `stt_request`: `id` non-empty; `seq` required
//!   integer >= 0; `data` string; `last` bool; `mime` non-empty string;
//!   `stt_request` additionally has optional `model` / `fileName` strings.
//! - `stt_response`: `id` non-empty; `text` string.
//! - `voice_error`: `id` non-empty; `message` string; `code` optional
//!   string (same defensive rule as `llm_error.code`).
//!
//! ## encode()
//!
//! Serialize with the exact wire field names above (`fileName`, not
//! `file_name`), skipping `None` options, `"v": 1` first is not required
//! (readers do field lookup) but keep output stable. Examples of valid
//! wire bytes:
//!
//! ```text
//! {"v":1,"type":"provider_hello","models":["gpt-4o"]}
//! {"v":1,"type":"llm_request","id":"a1","messages":[{"role":"user","content":"hi"}],"model":"gpt-4o"}
//! {"v":1,"type":"llm_response_chunk","id":"a1","delta":"Hello","seq":0}
//! ```

/// One chat turn, OpenAI-style.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    /// "system" | "user" | "assistant" (enforced by [`decode`]).
    pub role: String,
    pub content: String,
}

/// A protocol v1 message. Variant/field names map 1:1 onto the wire
/// `type`/fields documented in the module header.
#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMessage {
    ProviderHello {
        models: Option<Vec<String>>,
        /// Capability advertisement (mistai v0.4.0). Known values: "chat",
        /// "tts", "stt", "embedding"; unknown strings pass through
        /// unfiltered (forward-compat). `None` means the field was absent
        /// from the wire message -- per the wire spec this must be treated
        /// as `["chat"]` by *readers*, but `decode` itself does not apply
        /// that default (see [`advertises_service`] for the defaulted
        /// view).
        services: Option<Vec<String>>,
    },
    ConsumerHello,
    LlmRequest {
        id: String,
        messages: Vec<ChatMessage>,
        model: Option<String>,
    },
    LlmResponseChunk {
        id: String,
        delta: String,
        seq: Option<u64>,
    },
    LlmResponseDone {
        id: String,
        content: Option<String>,
    },
    LlmError {
        id: String,
        message: String,
        /// Machine-readable reason (mistai v0.4.0). Known value:
        /// `"unsupported_service"`; other non-empty strings pass through
        /// unfiltered (forward-compat).
        code: Option<String>,
    },
    RaftMessage {
        payload: String,
    },
    TtsRequest {
        id: String,
        text: String,
        model: Option<String>,
        voice: Option<String>,
    },
    TtsResponse {
        id: String,
        seq: u64,
        data: String,
        last: bool,
        mime: String,
    },
    SttRequest {
        id: String,
        seq: u64,
        data: String,
        last: bool,
        mime: String,
        model: Option<String>,
        file_name: Option<String>,
    },
    SttResponse {
        id: String,
        text: String,
    },
    VoiceError {
        id: String,
        message: String,
        /// Same defensive parsing / semantics as [`ProtocolMessage::LlmError::code`].
        code: Option<String>,
    },
}

/// Known `services` value for chat capability. `services` field absence on
/// a `provider_hello` means "chat only" per the wire spec (see
/// [`advertises_service`]).
pub const SERVICE_CHAT: &str = "chat";

/// Known `code` value meaning "provider does not offer this service at
/// all" (as opposed to a per-request upstream failure, which omits `code`).
pub const CODE_UNSUPPORTED_SERVICE: &str = "unsupported_service";

/// Whether a `provider_hello.services` value (already decoded, `None` if
/// the field was absent/invalid) advertises `service`. Applies the wire
/// spec's default: a missing `services` field is treated as `["chat"]`.
pub fn advertises_service(services: &Option<Vec<String>>, service: &str) -> bool {
    match services {
        None => service == SERVICE_CHAT,
        Some(list) => list.iter().any(|s| s == service),
    }
}

/// Encode a message to wire bytes (UTF-8 JSON).
pub fn encode(msg: &ProtocolMessage) -> Vec<u8> {
    use serde_json::{json, Map, Value};

    let value: Value = match msg {
        ProtocolMessage::ProviderHello { models, services } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("provider_hello"));
            if let Some(models) = models {
                map.insert("models".into(), json!(models));
            }
            if let Some(services) = services {
                map.insert("services".into(), json!(services));
            }
            Value::Object(map)
        }
        ProtocolMessage::ConsumerHello => {
            json!({"v": 1, "type": "consumer_hello"})
        }
        ProtocolMessage::LlmRequest { id, messages, model } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("llm_request"));
            map.insert("id".into(), json!(id));
            map.insert("messages".into(), json!(messages));
            if let Some(model) = model {
                map.insert("model".into(), json!(model));
            }
            Value::Object(map)
        }
        ProtocolMessage::LlmResponseChunk { id, delta, seq } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("llm_response_chunk"));
            map.insert("id".into(), json!(id));
            map.insert("delta".into(), json!(delta));
            if let Some(seq) = seq {
                map.insert("seq".into(), json!(seq));
            }
            Value::Object(map)
        }
        ProtocolMessage::LlmResponseDone { id, content } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("llm_response_done"));
            map.insert("id".into(), json!(id));
            if let Some(content) = content {
                map.insert("content".into(), json!(content));
            }
            Value::Object(map)
        }
        ProtocolMessage::LlmError { id, message, code } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("llm_error"));
            map.insert("id".into(), json!(id));
            map.insert("message".into(), json!(message));
            if let Some(code) = code {
                map.insert("code".into(), json!(code));
            }
            Value::Object(map)
        }
        ProtocolMessage::RaftMessage { payload } => {
            json!({"v": 1, "type": "raft_message", "payload": payload})
        }
        ProtocolMessage::TtsRequest { id, text, model, voice } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("tts_request"));
            map.insert("id".into(), json!(id));
            map.insert("text".into(), json!(text));
            if let Some(model) = model {
                map.insert("model".into(), json!(model));
            }
            if let Some(voice) = voice {
                map.insert("voice".into(), json!(voice));
            }
            Value::Object(map)
        }
        ProtocolMessage::TtsResponse { id, seq, data, last, mime } => {
            json!({
                "v": 1,
                "type": "tts_response",
                "id": id,
                "seq": seq,
                "data": data,
                "last": last,
                "mime": mime,
            })
        }
        ProtocolMessage::SttRequest { id, seq, data, last, mime, model, file_name } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("stt_request"));
            map.insert("id".into(), json!(id));
            map.insert("seq".into(), json!(seq));
            map.insert("data".into(), json!(data));
            map.insert("last".into(), json!(last));
            map.insert("mime".into(), json!(mime));
            if let Some(model) = model {
                map.insert("model".into(), json!(model));
            }
            if let Some(file_name) = file_name {
                map.insert("fileName".into(), json!(file_name));
            }
            Value::Object(map)
        }
        ProtocolMessage::SttResponse { id, text } => {
            json!({"v": 1, "type": "stt_response", "id": id, "text": text})
        }
        ProtocolMessage::VoiceError { id, message, code } => {
            let mut map = Map::new();
            map.insert("v".into(), json!(1));
            map.insert("type".into(), json!("voice_error"));
            map.insert("id".into(), json!(id));
            map.insert("message".into(), json!(message));
            if let Some(code) = code {
                map.insert("code".into(), json!(code));
            }
            Value::Object(map)
        }
    };

    serde_json::to_vec(&value).expect("ProtocolMessage always serializes")
}

/// Decode wire bytes; `None` for anything that isn't a valid v1 message.
pub fn decode(bytes: &[u8]) -> Option<ProtocolMessage> {
    use serde_json::Value;

    let value: Value = serde_json::from_slice(bytes).ok()?;
    let obj = value.as_object()?;

    // Match JS's `m.v !== 1`: accept any numeric representation of 1
    // (integer or float), reject everything else (including non-numbers).
    if obj.get("v")?.as_f64()? != 1.0 {
        return None;
    }
    let ty = obj.get("type")?.as_str()?;

    /// A non-empty string field.
    fn non_empty_str(obj: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
        let s = obj.get(key)?.as_str()?;
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    }

    /// Any string field (may be empty).
    fn any_str(obj: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
        obj.get(key)?.as_str().map(|s| s.to_string())
    }

    /// Optional string field, strict: absent -> valid with `None`; present
    /// and a string -> valid with `Some`; present but not a string -> the
    /// whole message is invalid (outer `None`). Mirrors protocol.ts's
    /// `m.field !== undefined && typeof m.field !== "string" -> return null`.
    fn opt_str(obj: &serde_json::Map<String, Value>, key: &str) -> Option<Option<String>> {
        match obj.get(key) {
            None => Some(None),
            Some(v) => v.as_str().map(|s| Some(s.to_string())),
        }
    }

    /// A JSON integer >= 0. Rejects fractional/negative/non-number values.
    fn as_u64_strict(v: &Value) -> Option<u64> {
        if !v.is_i64() && !v.is_u64() {
            return None;
        }
        let n = v.as_i64()?;
        if n < 0 {
            return None;
        }
        Some(n as u64)
    }

    /// `models` and `services` share this rule: "field-only ignored if not
    /// an array"; if it is an array, non-string *and* empty-string elements
    /// are dropped element-wise, keeping the rest (per the wire spec's
    /// unified `provider_hello` filtering rule for both fields).
    fn str_array_non_empty(v: &Value) -> Option<Vec<String>> {
        match v {
            Value::Array(arr) => Some(
                arr.iter()
                    .filter_map(|item| item.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect(),
            ),
            _ => None,
        }
    }

    /// `code`-style optional field: absent or wrong type -> `None` (field
    /// dropped, message still valid) rather than rejecting the whole
    /// message. Distinct from `opt_str`, which rejects the whole message
    /// on a type mismatch.
    fn dropped_opt_str(obj: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
        obj.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    match ty {
        "provider_hello" => {
            let models = match obj.get("models") {
                None => None,
                Some(v) => str_array_non_empty(v),
            };
            let services = match obj.get("services") {
                None => None,
                Some(v) => str_array_non_empty(v),
            };
            Some(ProtocolMessage::ProviderHello { models, services })
        }
        "consumer_hello" => Some(ProtocolMessage::ConsumerHello),
        "llm_request" => {
            let id = non_empty_str(obj, "id")?;
            let raw_messages = obj.get("messages")?.as_array()?;
            if raw_messages.is_empty() {
                return None;
            }
            let mut messages = Vec::with_capacity(raw_messages.len());
            for m in raw_messages {
                let m = m.as_object()?;
                let role = m.get("role")?.as_str()?;
                if !matches!(role, "system" | "user" | "assistant") {
                    return None;
                }
                let content = m.get("content")?.as_str()?;
                messages.push(ChatMessage {
                    role: role.to_string(),
                    content: content.to_string(),
                });
            }
            let model = opt_str(obj, "model")?;
            Some(ProtocolMessage::LlmRequest { id, messages, model })
        }
        "llm_response_chunk" => {
            let id = non_empty_str(obj, "id")?;
            let delta = any_str(obj, "delta")?;
            let seq = match obj.get("seq") {
                None => None,
                Some(v) => Some(as_u64_strict(v)?),
            };
            Some(ProtocolMessage::LlmResponseChunk { id, delta, seq })
        }
        "llm_response_done" => {
            let id = non_empty_str(obj, "id")?;
            let content = opt_str(obj, "content")?;
            Some(ProtocolMessage::LlmResponseDone { id, content })
        }
        "llm_error" => {
            let id = non_empty_str(obj, "id")?;
            let message = any_str(obj, "message")?;
            let code = dropped_opt_str(obj, "code");
            Some(ProtocolMessage::LlmError { id, message, code })
        }
        "raft_message" => {
            let payload = non_empty_str(obj, "payload")?;
            Some(ProtocolMessage::RaftMessage { payload })
        }
        "tts_request" => {
            let id = non_empty_str(obj, "id")?;
            let text = any_str(obj, "text")?;
            let model = opt_str(obj, "model")?;
            let voice = opt_str(obj, "voice")?;
            Some(ProtocolMessage::TtsRequest { id, text, model, voice })
        }
        "tts_response" => {
            let id = non_empty_str(obj, "id")?;
            let seq = as_u64_strict(obj.get("seq")?)?;
            let data = any_str(obj, "data")?;
            let last = obj.get("last")?.as_bool()?;
            let mime = non_empty_str(obj, "mime")?;
            Some(ProtocolMessage::TtsResponse { id, seq, data, last, mime })
        }
        "stt_request" => {
            let id = non_empty_str(obj, "id")?;
            let seq = as_u64_strict(obj.get("seq")?)?;
            let data = any_str(obj, "data")?;
            let last = obj.get("last")?.as_bool()?;
            let mime = non_empty_str(obj, "mime")?;
            let model = opt_str(obj, "model")?;
            let file_name = opt_str(obj, "fileName")?;
            Some(ProtocolMessage::SttRequest {
                id,
                seq,
                data,
                last,
                mime,
                model,
                file_name,
            })
        }
        "stt_response" => {
            let id = non_empty_str(obj, "id")?;
            let text = any_str(obj, "text")?;
            Some(ProtocolMessage::SttResponse { id, text })
        }
        "voice_error" => {
            let id = non_empty_str(obj, "id")?;
            let message = any_str(obj, "message")?;
            let code = dropped_opt_str(obj, "code");
            Some(ProtocolMessage::VoiceError { id, message, code })
        }
        _ => None,
    }
}

/// Random request id: UUID v4 string (lowercase hex, hyphenated), matching
/// mistai's `randomId()`.
pub fn random_id() -> String {
    let mut bytes = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
    let h = |range: std::ops::Range<usize>| {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    format!("{}-{}-{}-{}-{}", h(0..4), h(4..6), h(6..8), h(8..10), h(10..16))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::collections::BTreeSet;

    fn keys(bytes: &[u8]) -> BTreeSet<String> {
        let v: Value = serde_json::from_slice(bytes).unwrap();
        v.as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
    }

    fn parsed(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).unwrap()
    }

    // ---- encode: byte-level field checks -----------------------------

    #[test]
    fn encode_provider_hello_with_models() {
        let bytes = encode(&ProtocolMessage::ProviderHello {
            models: Some(vec!["gpt-4o".to_string()]),
            services: None,
        });
        let v = parsed(&bytes);
        assert_eq!(v["v"], json!(1));
        assert_eq!(v["type"], json!("provider_hello"));
        assert_eq!(v["models"], json!(["gpt-4o"]));
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "models".into()])
        );
    }

    #[test]
    fn encode_provider_hello_without_models_omits_field() {
        let bytes = encode(&ProtocolMessage::ProviderHello { models: None, services: None });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into()])
        );
        let v = parsed(&bytes);
        assert!(v.get("models").is_none());
    }

    #[test]
    fn encode_provider_hello_with_services() {
        let bytes = encode(&ProtocolMessage::ProviderHello {
            models: None,
            services: Some(vec!["chat".to_string()]),
        });
        let v = parsed(&bytes);
        assert_eq!(v["services"], json!(["chat"]));
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "services".into()])
        );
    }

    #[test]
    fn encode_provider_hello_without_services_omits_field() {
        let bytes = encode(&ProtocolMessage::ProviderHello {
            models: Some(vec!["gpt-4o".into()]),
            services: None,
        });
        let v = parsed(&bytes);
        assert!(v.get("services").is_none());
    }

    #[test]
    fn encode_consumer_hello() {
        let bytes = encode(&ProtocolMessage::ConsumerHello);
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into()])
        );
        assert_eq!(parsed(&bytes)["type"], json!("consumer_hello"));
    }

    #[test]
    fn encode_llm_request_with_model() {
        let bytes = encode(&ProtocolMessage::LlmRequest {
            id: "a1".into(),
            messages: vec![ChatMessage {
                role: "user".into(),
                content: "hi".into(),
            }],
            model: Some("gpt-4o".into()),
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from([
                "v".into(),
                "type".into(),
                "id".into(),
                "messages".into(),
                "model".into()
            ])
        );
        let v = parsed(&bytes);
        assert_eq!(v["messages"], json!([{"role": "user", "content": "hi"}]));
    }

    #[test]
    fn encode_llm_request_without_model_omits_field() {
        let bytes = encode(&ProtocolMessage::LlmRequest {
            id: "a1".into(),
            messages: vec![ChatMessage {
                role: "system".into(),
                content: "sys".into(),
            }],
            model: None,
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "messages".into()])
        );
    }

    #[test]
    fn encode_llm_response_chunk_with_seq() {
        let bytes = encode(&ProtocolMessage::LlmResponseChunk {
            id: "a1".into(),
            delta: "Hello".into(),
            seq: Some(0),
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "delta".into(), "seq".into()])
        );
        assert_eq!(parsed(&bytes)["seq"], json!(0));
    }

    #[test]
    fn encode_llm_response_chunk_without_seq_omits_field() {
        let bytes = encode(&ProtocolMessage::LlmResponseChunk {
            id: "a1".into(),
            delta: "".into(),
            seq: None,
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "delta".into()])
        );
    }

    #[test]
    fn encode_llm_response_done() {
        let with_content = encode(&ProtocolMessage::LlmResponseDone {
            id: "a1".into(),
            content: Some("done".into()),
        });
        assert_eq!(
            keys(&with_content),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "content".into()])
        );

        let without_content = encode(&ProtocolMessage::LlmResponseDone {
            id: "a1".into(),
            content: None,
        });
        assert_eq!(
            keys(&without_content),
            BTreeSet::from(["v".into(), "type".into(), "id".into()])
        );
    }

    #[test]
    fn encode_llm_error() {
        let bytes = encode(&ProtocolMessage::LlmError {
            id: "a1".into(),
            message: "boom".into(),
            code: None,
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "message".into()])
        );
    }

    #[test]
    fn encode_llm_error_with_code() {
        let bytes = encode(&ProtocolMessage::LlmError {
            id: "a1".into(),
            message: "unsupported".into(),
            code: Some("unsupported_service".into()),
        });
        let v = parsed(&bytes);
        assert_eq!(v["code"], json!("unsupported_service"));
        assert_eq!(
            keys(&bytes),
            BTreeSet::from([
                "v".into(),
                "type".into(),
                "id".into(),
                "message".into(),
                "code".into()
            ])
        );
    }

    #[test]
    fn encode_raft_message() {
        let bytes = encode(&ProtocolMessage::RaftMessage {
            payload: "b64==".into(),
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "payload".into()])
        );
    }

    #[test]
    fn encode_tts_request_full_and_minimal() {
        let full = encode(&ProtocolMessage::TtsRequest {
            id: "a1".into(),
            text: "hi".into(),
            model: Some("m".into()),
            voice: Some("v".into()),
        });
        assert_eq!(
            keys(&full),
            BTreeSet::from([
                "v".into(),
                "type".into(),
                "id".into(),
                "text".into(),
                "model".into(),
                "voice".into()
            ])
        );

        let minimal = encode(&ProtocolMessage::TtsRequest {
            id: "a1".into(),
            text: "hi".into(),
            model: None,
            voice: None,
        });
        assert_eq!(
            keys(&minimal),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "text".into()])
        );
    }

    #[test]
    fn encode_tts_response() {
        let bytes = encode(&ProtocolMessage::TtsResponse {
            id: "a1".into(),
            seq: 3,
            data: "AAAA".into(),
            last: true,
            mime: "audio/mpeg".into(),
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from([
                "v".into(),
                "type".into(),
                "id".into(),
                "seq".into(),
                "data".into(),
                "last".into(),
                "mime".into()
            ])
        );
        let v = parsed(&bytes);
        assert_eq!(v["last"], json!(true));
    }

    #[test]
    fn encode_stt_request_full_and_minimal() {
        let full = encode(&ProtocolMessage::SttRequest {
            id: "a1".into(),
            seq: 0,
            data: "AAAA".into(),
            last: false,
            mime: "audio/wav".into(),
            model: Some("whisper".into()),
            file_name: Some("clip.wav".into()),
        });
        assert_eq!(
            keys(&full),
            BTreeSet::from([
                "v".into(),
                "type".into(),
                "id".into(),
                "seq".into(),
                "data".into(),
                "last".into(),
                "mime".into(),
                "model".into(),
                "fileName".into()
            ])
        );
        let v = parsed(&full);
        assert_eq!(v["fileName"], json!("clip.wav"));
        assert!(v.get("file_name").is_none());

        let minimal = encode(&ProtocolMessage::SttRequest {
            id: "a1".into(),
            seq: 0,
            data: "AAAA".into(),
            last: false,
            mime: "audio/wav".into(),
            model: None,
            file_name: None,
        });
        assert_eq!(
            keys(&minimal),
            BTreeSet::from([
                "v".into(),
                "type".into(),
                "id".into(),
                "seq".into(),
                "data".into(),
                "last".into(),
                "mime".into()
            ])
        );
    }

    #[test]
    fn encode_stt_response() {
        let bytes = encode(&ProtocolMessage::SttResponse {
            id: "a1".into(),
            text: "hello".into(),
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "text".into()])
        );
    }

    #[test]
    fn encode_voice_error() {
        let bytes = encode(&ProtocolMessage::VoiceError {
            id: "a1".into(),
            message: "boom".into(),
            code: None,
        });
        assert_eq!(
            keys(&bytes),
            BTreeSet::from(["v".into(), "type".into(), "id".into(), "message".into()])
        );
    }

    #[test]
    fn encode_voice_error_with_code() {
        let bytes = encode(&ProtocolMessage::VoiceError {
            id: "a1".into(),
            message: "no voice here".into(),
            code: Some("unsupported_service".into()),
        });
        let v = parsed(&bytes);
        assert_eq!(v["code"], json!("unsupported_service"));
        assert_eq!(
            keys(&bytes),
            BTreeSet::from([
                "v".into(),
                "type".into(),
                "id".into(),
                "message".into(),
                "code".into()
            ])
        );
    }

    // ---- decode: worked examples from the module doc ------------------

    #[test]
    fn decode_doc_example_provider_hello() {
        let bytes = br#"{"v":1,"type":"provider_hello","models":["gpt-4o"]}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".to_string()]),
                services: None,
            })
        );
    }

    #[test]
    fn decode_provider_hello_with_services() {
        let bytes =
            br#"{"v":1,"type":"provider_hello","models":["gpt-4o"],"services":["chat","tts"]}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".to_string()]),
                services: Some(vec!["chat".to_string(), "tts".to_string()]),
            })
        );
    }

    #[test]
    fn decode_doc_example_llm_request() {
        let bytes = br#"{"v":1,"type":"llm_request","id":"a1","messages":[{"role":"user","content":"hi"}],"model":"gpt-4o"}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::LlmRequest {
                id: "a1".into(),
                messages: vec![ChatMessage {
                    role: "user".into(),
                    content: "hi".into()
                }],
                model: Some("gpt-4o".into()),
            })
        );
    }

    #[test]
    fn decode_doc_example_llm_response_chunk() {
        let bytes = br#"{"v":1,"type":"llm_response_chunk","id":"a1","delta":"Hello","seq":0}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::LlmResponseChunk {
                id: "a1".into(),
                delta: "Hello".into(),
                seq: Some(0),
            })
        );
    }

    // ---- roundtrip: every variant --------------------------------------

    fn assert_roundtrip(msg: ProtocolMessage) {
        let bytes = encode(&msg);
        assert_eq!(decode(&bytes), Some(msg));
    }

    #[test]
    fn roundtrip_all_variants() {
        assert_roundtrip(ProtocolMessage::ProviderHello {
            models: Some(vec!["a".into(), "b".into()]),
            services: None,
        });
        assert_roundtrip(ProtocolMessage::ProviderHello { models: None, services: None });
        assert_roundtrip(ProtocolMessage::ProviderHello {
            models: None,
            services: Some(vec!["chat".into(), "tts".into()]),
        });
        assert_roundtrip(ProtocolMessage::ConsumerHello);
        assert_roundtrip(ProtocolMessage::LlmRequest {
            id: "id1".into(),
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: "sys".into(),
                },
                ChatMessage {
                    role: "assistant".into(),
                    content: "reply".into(),
                },
            ],
            model: Some("gpt-4o".into()),
        });
        assert_roundtrip(ProtocolMessage::LlmRequest {
            id: "id1".into(),
            messages: vec![ChatMessage {
                role: "user".into(),
                content: "hi".into(),
            }],
            model: None,
        });
        assert_roundtrip(ProtocolMessage::LlmResponseChunk {
            id: "id1".into(),
            delta: "chunk".into(),
            seq: Some(7),
        });
        assert_roundtrip(ProtocolMessage::LlmResponseChunk {
            id: "id1".into(),
            delta: "".into(),
            seq: None,
        });
        assert_roundtrip(ProtocolMessage::LlmResponseDone {
            id: "id1".into(),
            content: Some("full text".into()),
        });
        assert_roundtrip(ProtocolMessage::LlmResponseDone {
            id: "id1".into(),
            content: None,
        });
        assert_roundtrip(ProtocolMessage::LlmError {
            id: "id1".into(),
            message: "oops".into(),
            code: None,
        });
        assert_roundtrip(ProtocolMessage::LlmError {
            id: "id1".into(),
            message: "not supported".into(),
            code: Some("unsupported_service".into()),
        });
        assert_roundtrip(ProtocolMessage::RaftMessage {
            payload: "cGF5bG9hZA==".into(),
        });
        assert_roundtrip(ProtocolMessage::TtsRequest {
            id: "id1".into(),
            text: "speak this".into(),
            model: Some("tts-1".into()),
            voice: Some("alloy".into()),
        });
        assert_roundtrip(ProtocolMessage::TtsRequest {
            id: "id1".into(),
            text: "speak this".into(),
            model: None,
            voice: None,
        });
        assert_roundtrip(ProtocolMessage::TtsResponse {
            id: "id1".into(),
            seq: 0,
            data: "AAAA".into(),
            last: false,
            mime: "audio/mpeg".into(),
        });
        assert_roundtrip(ProtocolMessage::SttRequest {
            id: "id1".into(),
            seq: 0,
            data: "AAAA".into(),
            last: true,
            mime: "audio/wav".into(),
            model: Some("whisper-1".into()),
            file_name: Some("input.wav".into()),
        });
        assert_roundtrip(ProtocolMessage::SttRequest {
            id: "id1".into(),
            seq: 1,
            data: "BBBB".into(),
            last: false,
            mime: "audio/wav".into(),
            model: None,
            file_name: None,
        });
        assert_roundtrip(ProtocolMessage::SttResponse {
            id: "id1".into(),
            text: "transcribed".into(),
        });
        assert_roundtrip(ProtocolMessage::VoiceError {
            id: "id1".into(),
            message: "voice oops".into(),
            code: None,
        });
        assert_roundtrip(ProtocolMessage::VoiceError {
            id: "id1".into(),
            message: "not supported".into(),
            code: Some("unsupported_service".into()),
        });
    }

    // ---- decode: rejection cases ---------------------------------------

    #[test]
    fn decode_rejects_non_json() {
        assert_eq!(decode(b"not json"), None);
    }

    #[test]
    fn decode_rejects_non_object_top_level() {
        assert_eq!(decode(b"[1,2,3]"), None);
        assert_eq!(decode(b"\"just a string\""), None);
        assert_eq!(decode(b"42"), None);
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let bytes = br#"{"v":2,"type":"consumer_hello"}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_missing_version() {
        let bytes = br#"{"type":"consumer_hello"}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_unknown_type() {
        let bytes = br#"{"v":1,"type":"not_a_real_type"}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_empty_id() {
        let bytes = br#"{"v":1,"type":"llm_error","id":"","message":"x"}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_negative_seq() {
        let bytes = br#"{"v":1,"type":"llm_response_chunk","id":"a1","delta":"x","seq":-1}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_fractional_seq() {
        let bytes = br#"{"v":1,"type":"llm_response_chunk","id":"a1","delta":"x","seq":1.5}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_missing_required_seq() {
        let bytes =
            br#"{"v":1,"type":"tts_response","id":"a1","data":"AAAA","last":true,"mime":"audio/mpeg"}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_bad_role() {
        let bytes = br#"{"v":1,"type":"llm_request","id":"a1","messages":[{"role":"admin","content":"hi"}]}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_empty_messages() {
        let bytes = br#"{"v":1,"type":"llm_request","id":"a1","messages":[]}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_missing_messages() {
        let bytes = br#"{"v":1,"type":"llm_request","id":"a1"}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_rejects_wrong_type_optional_field() {
        // `model` present but not a string -> whole message invalid (unlike
        // provider_hello.models, which degrades gracefully instead).
        let bytes =
            br#"{"v":1,"type":"llm_request","id":"a1","messages":[{"role":"user","content":"hi"}],"model":42}"#;
        assert_eq!(decode(bytes), None);
    }

    #[test]
    fn decode_provider_hello_non_array_models_degrades_but_keeps_message() {
        let bytes = br#"{"v":1,"type":"provider_hello","models":"not-an-array"}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello { models: None, services: None })
        );
    }

    #[test]
    fn decode_provider_hello_filters_non_string_models() {
        let bytes = br#"{"v":1,"type":"provider_hello","models":["gpt-4o",42,null,"claude"]}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".to_string(), "claude".to_string()]),
                services: None,
            })
        );
    }

    #[test]
    fn decode_provider_hello_filters_non_string_and_empty_models() {
        // `models` follows the same element-wise filtering rule as
        // `services`: non-string *and* empty-string elements are dropped,
        // not just non-string ones.
        let bytes =
            br#"{"v":1,"type":"provider_hello","models":["gpt-4o",42,null,"","claude"]}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello {
                models: Some(vec!["gpt-4o".to_string(), "claude".to_string()]),
                services: None,
            })
        );
    }

    #[test]
    fn decode_provider_hello_non_array_services_degrades_but_keeps_message() {
        let bytes = br#"{"v":1,"type":"provider_hello","services":42}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello { models: None, services: None })
        );
    }

    #[test]
    fn decode_provider_hello_filters_non_string_and_empty_services() {
        let bytes =
            br#"{"v":1,"type":"provider_hello","services":["chat",42,null,"","tts"]}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello {
                models: None,
                services: Some(vec!["chat".to_string(), "tts".to_string()]),
            })
        );
    }

    #[test]
    fn decode_provider_hello_services_passes_through_unknown_values() {
        // Unknown service strings are forward-compat passthrough, not
        // filtered (only non-string/empty-string elements are dropped).
        let bytes = br#"{"v":1,"type":"provider_hello","services":["chat","future-service"]}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello {
                models: None,
                services: Some(vec!["chat".to_string(), "future-service".to_string()]),
            })
        );
    }

    #[test]
    fn decode_provider_hello_services_absent_is_none() {
        let bytes = br#"{"v":1,"type":"provider_hello"}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::ProviderHello { models: None, services: None })
        );
    }

    #[test]
    fn decode_llm_error_with_code() {
        let bytes = br#"{"v":1,"type":"llm_error","id":"a1","message":"nope","code":"unsupported_service"}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::LlmError {
                id: "a1".into(),
                message: "nope".into(),
                code: Some("unsupported_service".into()),
            })
        );
    }

    #[test]
    fn decode_llm_error_non_string_code_drops_field_only() {
        let bytes = br#"{"v":1,"type":"llm_error","id":"a1","message":"nope","code":42}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::LlmError {
                id: "a1".into(),
                message: "nope".into(),
                code: None,
            })
        );
    }

    #[test]
    fn decode_voice_error_with_code() {
        let bytes = br#"{"v":1,"type":"voice_error","id":"a1","message":"nope","code":"unsupported_service"}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::VoiceError {
                id: "a1".into(),
                message: "nope".into(),
                code: Some("unsupported_service".into()),
            })
        );
    }

    #[test]
    fn decode_voice_error_non_string_code_drops_field_only() {
        let bytes = br#"{"v":1,"type":"voice_error","id":"a1","message":"nope","code":42}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::VoiceError {
                id: "a1".into(),
                message: "nope".into(),
                code: None,
            })
        );
    }

    #[test]
    fn advertises_service_defaults_missing_to_chat_only() {
        assert!(advertises_service(&None, SERVICE_CHAT));
        assert!(!advertises_service(&None, "tts"));
    }

    #[test]
    fn advertises_service_checks_explicit_list() {
        let services = Some(vec!["tts".to_string(), "stt".to_string()]);
        assert!(!advertises_service(&services, SERVICE_CHAT));
        assert!(advertises_service(&services, "tts"));
        assert!(advertises_service(&services, "stt"));
        assert!(!advertises_service(&services, "embedding"));
    }

    #[test]
    fn decode_ignores_unknown_extra_fields() {
        let bytes = br#"{"v":1,"type":"consumer_hello","unexpected":"field","another":123}"#;
        assert_eq!(decode(bytes), Some(ProtocolMessage::ConsumerHello));
    }

    #[test]
    fn decode_llm_response_chunk_seq_absent_is_valid() {
        let bytes = br#"{"v":1,"type":"llm_response_chunk","id":"a1","delta":"x"}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::LlmResponseChunk {
                id: "a1".into(),
                delta: "x".into(),
                seq: None,
            })
        );
    }

    #[test]
    fn decode_llm_response_done_content_absent_is_valid() {
        let bytes = br#"{"v":1,"type":"llm_response_done","id":"a1"}"#;
        assert_eq!(
            decode(bytes),
            Some(ProtocolMessage::LlmResponseDone {
                id: "a1".into(),
                content: None,
            })
        );
    }

    // ---- random_id --------------------------------------------------

    #[test]
    fn random_id_shape() {
        let id = random_id();
        assert_eq!(id.len(), 36);
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(
            [parts[0].len(), parts[1].len(), parts[2].len(), parts[3].len(), parts[4].len()],
            [8, 4, 4, 4, 12]
        );
        assert!(id.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        // version 4 nibble: first hex digit of the third group must be '4'.
        assert_eq!(parts[2].chars().next().unwrap(), '4');
        // RFC 4122 variant: first hex digit of the fourth group in {8,9,a,b}.
        let variant_nibble = parts[3].chars().next().unwrap();
        assert!(matches!(variant_nibble, '8' | '9' | 'a' | 'b'));
    }

    #[test]
    fn random_id_is_random() {
        let a = random_id();
        let b = random_id();
        assert_ne!(a, b);
    }
}
