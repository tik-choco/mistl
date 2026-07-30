//! Direct-upstream STT client (OpenAI-compatible `POST {base_url}/audio/transcriptions`).
//!
//! The multipart counterpart to `super::tts`: used when this node acts as a
//! network provider and needs to answer an inbound `stt_request` (see
//! `crate::ai::provider`) by forwarding the reassembled audio bytes to an
//! upstream transcription endpoint. Mirrors `tts.rs`'s conventions as
//! closely as it makes sense to for a multipart request / JSON response:
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
//!   `audio.<ext>` derived from the mime type).
//! - Non-2xx -> error including status and the first 500 chars of the body
//!   (same as `tts.rs`/`openai.rs`).
//! - Response is parsed as JSON `{"text": "..."}` (OpenAI's default
//!   `verbose_json`-less response shape); missing/non-string `text` ->
//!   error, same style as `openai.rs`'s `choices[0].message.content`
//!   extraction.

use anyhow::{Context, Result, bail};

use crate::config::AiProviderConfig;

/// One speech-to-text request: already-reassembled audio bytes (see
/// `crate::ai::provider`'s per-`id` chunk buffer for `stt_request`) plus the
/// upstream model to transcribe with.
#[derive(Debug, Clone)]
pub struct SttParams {
    /// Upstream STT model id (e.g. "whisper-1").
    pub model: String,
    /// Raw audio bytes (already base64-decoded and reassembled from the
    /// wire's chunked `stt_request`).
    pub audio: Vec<u8>,
    /// MIME type of `audio`, as reported by the peer's `stt_request.mime`.
    pub mime: String,
    /// Filename hint from the peer's `stt_request.fileName`, if any.
    pub file_name: Option<String>,
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
    Ok(())
}

/// `POST {base_url}/audio/transcriptions`: transcribes `req.audio` via the
/// OpenAI-compatible STT endpoint of `provider`. See the module doc for the
/// base_url/header/error conventions this follows.
pub async fn transcribe(provider: &AiProviderConfig, req: SttParams) -> Result<String> {
    validate(&req)?;

    let file_name = req
        .file_name
        .clone()
        .unwrap_or_else(|| format!("audio.{}", mime_to_extension(&req.mime)));
    let part = reqwest::multipart::Part::bytes(req.audio)
        .file_name(file_name)
        .mime_str(&req.mime)
        .context("ai: invalid stt audio mime type")?;
    let form = reqwest::multipart::Form::new()
        .text("model", req.model.clone())
        .part("file", part);

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
        let body_text = response.text().await.unwrap_or_default();
        bail!(
            "STT API returned an error ({status}): {}",
            truncate_500(&body_text)
        );
    }

    let body: serde_json::Value = response
        .json()
        .await
        .context("ai: parsing STT API response as JSON")?;
    match body.get("text").and_then(|v| v.as_str()) {
        Some(text) => Ok(text.to_string()),
        None => bail!("ai: STT API response missing a string \"text\" field"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(audio: &[u8]) -> SttParams {
        SttParams {
            model: "whisper-1".to_string(),
            audio: audio.to_vec(),
            mime: "audio/wav".to_string(),
            file_name: None,
        }
    }

    #[test]
    fn mime_to_extension_maps_known_types() {
        assert_eq!(mime_to_extension("audio/mpeg"), "mp3");
        assert_eq!(mime_to_extension("audio/wav"), "wav");
        assert_eq!(mime_to_extension("audio/webm"), "webm");
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

    #[tokio::test]
    async fn transcribe_rejects_empty_audio_before_any_request_is_sent() {
        // Mirrors tts.rs's synthesize_rejects_over_limit_input_before_any_request_is_sent:
        // bind a listener but never accept, proving validation short-circuits
        // before any network I/O.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let notify2 = notify.clone();
        let server = tokio::spawn(async move {
            let _ = listener.accept().await;
            notify2.notify_one();
        });

        let provider = AiProviderConfig {
            id: "p1".to_string(),
            label: "P1".to_string(),
            base_url: format!("http://{addr}"),
            api_key: "key".to_string(),
        };
        let err = transcribe(&provider, params(b""))
            .await
            .expect_err("empty audio should error");
        assert!(err.to_string().contains("audio"));

        let was_contacted =
            tokio::time::timeout(std::time::Duration::from_millis(150), notify.notified())
                .await
                .is_ok();
        assert!(
            !was_contacted,
            "no request should be sent for invalid input"
        );

        server.abort();
    }
}
