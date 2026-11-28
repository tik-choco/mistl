//! Direct-upstream STT client (OpenAI-compatible `POST {base_url}/audio/transcriptions`).
//!
//! The multipart counterpart to `super::tts`: used when this node acts as a
//! network provider and needs to answer an inbound `stt_request` (see
//! `crate::ai::provider`) by forwarding the reassembled audio bytes to an
//! upstream transcription endpoint. Mirrors `tts.rs`'s conventions as
//! closely as it makes sense to for a multipart request / opaque response:
//!
//! - `base_url` has any trailing `/` stripped before appending
//!   `/audio/transcriptions` (same convention as `tts.rs`'s `/audio/speech`
//!   and `openai.rs`'s `/chat/completions`).
//! - Headers: `Authorization: Bearer {api_key}` (always sent, even when the
//!   key is empty). No explicit `Content-Type` -- `reqwest::multipart::Form`
//!   sets the `multipart/form-data; boundary=...` header itself.
//! - Body: multipart form with a `model` text part and a `file` part
//!   carrying the raw audio bytes, `req.mime` as the part's content-type,
//!   and a filename (`req.file_name` if the peer sent one, else a generic
//!   `audio.<ext>` derived from the mime type), plus bounded text fields.
//! - Non-2xx -> error including status and the first 500 chars of the body
//!   (same as `tts.rs`/`openai.rs`).
//! - Successful responses are buffered without changing their bytes or MIME.
//!   Legacy peers still receive the JSON response's `text` field only.

use anyhow::{Context, Result, bail};

use super::protocol::{MAX_STT_RESPONSE_BYTES, valid_stt_content_type, valid_stt_extra};
use crate::config::AiProviderConfig;

/// One speech-to-text request: already-reassembled audio bytes (see
/// `crate::ai::provider`'s per-`id` chunk buffer for `stt_request`) plus the
/// upstream model to transcribe with.
#[derive(Debug, Clone)]
pub struct SttParams {
    /// Upstream STT model id (e.g. "stt-model").
    pub model: String,
    /// Raw audio bytes (already base64-decoded and reassembled from the
    /// wire's chunked `stt_request`).
    pub audio: Vec<u8>,
    /// MIME type of `audio`, as reported by the peer's `stt_request.mime`.
    pub mime: String,
    /// Filename hint from the peer's `stt_request.fileName`, if any.
    pub file_name: Option<String>,
    /// Ordered text fields, including repeated names such as `include[]`.
    pub extra: Vec<(String, String)>,
}

/// Upstream response, or the text-only response of a legacy network provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SttOutput {
    Text(String),
    Raw { body: Vec<u8>, content_type: String },
}

impl From<String> for SttOutput {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for SttOutput {
    fn from(text: &str) -> Self {
        Self::Text(text.to_string())
    }
}

impl SttOutput {
    pub fn into_legacy_text(self) -> Result<String> {
        match self {
            Self::Text(text) => Ok(text),
            Self::Raw { body, .. } => {
                let json: serde_json::Value = serde_json::from_slice(&body)
                    .context("ai: parsing STT API response as JSON")?;
                json.get("text")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
                    .context("ai: STT API response missing a string \"text\" field")
            }
        }
    }
}

fn strip_trailing_slash(base_url: &str) -> &str {
    base_url.trim_end_matches('/')
}

/// First 500 *characters* (not bytes) of `body`, matching the truncation
/// convention in `tts.rs`/`openai.rs`'s error messages.
fn truncate_500(body: &str) -> String {
    body.chars().take(500).collect()
}

/// Builds a fresh client for one call. `no_proxy()` matters here for the
/// same reason as `tts::build_client`/`openai::build_client`: upstreams may
/// be local and proxy env vars would otherwise break loopback requests.
fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("ai: building STT upstream HTTP client")
}

/// Maps an audio MIME type to a filename extension, for when the peer's
/// `stt_request` didn't include a `fileName`. Falls back to `"bin"` for
/// unknown mimes -- upstreams generally sniff content instead of trusting
/// the extension, so this is a best-effort label, not a strict contract.
fn mime_to_extension(mime: &str) -> &'static str {
    match mime {
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/aac" => "aac",
        "audio/flac" => "flac",
        "audio/wav" | "audio/x-wav" => "wav",
        "audio/webm" => "webm",
        "audio/mp4" | "audio/m4a" => "m4a",
        _ => "bin",
    }
}

fn validate(req: &SttParams) -> Result<()> {
    if req.model.trim().is_empty() {
        bail!("ai: stt requires a model");
    }
    if req.audio.is_empty() {
        bail!("ai: stt requires non-empty audio");
    }
    if !valid_stt_extra(&req.extra) {
        bail!("ai: invalid or oversized STT extra fields");
    }
    Ok(())
}

/// `POST {base_url}/audio/transcriptions`: transcribes `req.audio` via the
/// OpenAI-compatible STT endpoint of `provider`. See the module doc for the
/// base_url/header/error conventions this follows.
pub async fn transcribe(provider: &AiProviderConfig, req: SttParams) -> Result<SttOutput> {
    validate(&req)?;

    let file_name = req
        .file_name
        .clone()
        .unwrap_or_else(|| format!("audio.{}", mime_to_extension(&req.mime)));
    let part = reqwest::multipart::Part::bytes(req.audio)
        .file_name(file_name)
        .mime_str(&req.mime)
        .context("ai: invalid stt audio mime type")?;
    let mut form = reqwest::multipart::Form::new()
        .text("model", req.model.clone())
        .part("file", part);
    for (name, value) in req.extra {
        form = form.text(name, value);
    }

    let url = format!(
        "{}/audio/transcriptions",
        strip_trailing_slash(&provider.base_url)
    );

    let client = build_client()?;
    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", provider.api_key))
        .multipart(form)
        .send()
        .await
        .with_context(|| format!("STT API request failed: POST {url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body_text = super::tts::read_error_text(response).await;
        return Err(anyhow::Error::new(super::openai::UpstreamHttpError {
            label: "STT API",
            status: status.as_u16(),
            detail: truncate_500(&body_text),
        }));
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .filter(|v| valid_stt_content_type(v))
        .unwrap_or("application/octet-stream")
        .to_string();
    if response
        .content_length()
        .is_some_and(|len| len > MAX_STT_RESPONSE_BYTES as u64)
    {
        bail!("ai: STT API response exceeds the maximum transcription size");
    }
    let mut response = response;
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("ai: reading STT API response body")?
    {
        if body.len().saturating_add(chunk.len()) > MAX_STT_RESPONSE_BYTES {
            bail!("ai: STT API response exceeds the maximum transcription size");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(SttOutput::Raw { body, content_type })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(audio: &[u8]) -> SttParams {
        SttParams {
            model: "stt-model".to_string(),
            audio: audio.to_vec(),
            mime: "audio/wav".to_string(),
            file_name: None,
            extra: Vec::new(),
        }
    }

    #[test]
    fn mime_to_extension_falls_back_for_unknown_types() {
        assert_eq!(mime_to_extension("application/octet-stream"), "bin");
    }

    #[test]
    fn validate_rejects_blank_model() {
        let mut req = params(b"fake-audio-bytes");
        req.model = "  ".to_string();
        assert!(validate(&req).is_err());
    }

    #[test]
    fn validate_rejects_empty_audio() {
        let req = params(b"");
        assert!(validate(&req).is_err());
    }

    #[test]
    fn validate_accepts_a_well_formed_request() {
        assert!(validate(&params(b"fake-audio-bytes")).is_ok());
    }

    #[test]
    fn validate_rejects_excessive_or_reserved_extra_fields() {
        let mut req = params(b"audio");
        req.extra = vec![(
            "prompt".into(),
            "a".repeat(super::super::protocol::MAX_VOICE_EXTRA_BYTES),
        )];
        assert!(validate(&req).is_err());
        req.extra = vec![
            ("include[]".into(), "logprobs".into());
            super::super::protocol::MAX_STT_EXTRA_FIELDS + 1
        ];
        assert!(validate(&req).is_err());
        req.extra = vec![("model".into(), "other-model".into())];
        assert!(validate(&req).is_err());
        req.extra = vec![
            ("timestamp_granularities[]".into(), "word".into()),
            ("timestamp_granularities[]".into(), "segment".into()),
        ];
        assert!(validate(&req).is_ok());
    }

    #[test]
    fn legacy_text_uses_json_transcript_and_ignores_other_metadata() {
        let raw = SttOutput::Raw {
            body: br#"{"text":"hello","duration":1}"#.to_vec(),
            content_type: "application/json".into(),
        };
        assert_eq!(raw.into_legacy_text().unwrap(), "hello");
        let malformed = SttOutput::Raw {
            body: br#"{"duration":1}"#.to_vec(),
            content_type: "application/json".into(),
        };
        assert!(malformed.into_legacy_text().is_err());
    }
}
