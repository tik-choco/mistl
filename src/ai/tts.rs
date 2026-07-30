//! Direct-upstream TTS client (OpenAI-compatible `POST {base_url}/audio/speech`).
//!
//! Used by the bot pipeline's `tts` transform (v1 = "direct upstream", see
//! `tc-docs/drafts/bot-pipeline-v1.md`) to synthesize read-aloud audio for a
//! news-audio bot without going through the p2p AI network (that's the
//! later "network TTS" path via mistllm-wire's `tts_request`, out of scope
//! here). Mirrors `super::openai`'s conventions as closely as it makes
//! sense to for a binary-response endpoint:
//!
//! - `base_url` is used exactly as configured on the resolved provider
//!   (e.g. `AiProviderConfig::base_url`, already `/v1`-suffixed the same
//!   way `openai::UpstreamConfig::base_url` is -- see that module's doc
//!   comment), with any trailing `/` stripped before appending
//!   `/audio/speech` (no extra `/v1` is inserted here, matching how
//!   `openai::stream_chat_completion` appends `/chat/completions` directly).
//! - Headers: `Content-Type: application/json`,
//!   `Authorization: Bearer {api_key}` (always sent, even when the key is
//!   empty).
//! - Non-2xx -> error including status and the first 500 chars of the body.
//! - The response body is treated as opaque audio bytes (no SSE/JSON
//!   parsing -- OpenAI's `/audio/speech` returns the raw audio, not JSON).
//!
//! Long input is *not* chunked here: OpenAI's documented `/audio/speech`
//! input limit is 4096 characters, and `synthesize` simply errors when
//! `req.input` exceeds that. Splitting long articles into multiple
//! synthesis calls (and stitching the resulting audio) is left to the
//! caller -- tracked as a Wave 2 concern for the `tts` transform, not
//! solved here.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::config::AiProviderConfig;

/// OpenAI's documented input character limit for `/v1/audio/speech`.
pub const MAX_INPUT_CHARS: usize = 4096;

/// One text-to-speech request.
#[derive(Debug, Clone)]
pub struct TtsParams {
    /// Upstream TTS model id (e.g. "tts-1", "gpt-4o-mini-tts").
    pub model: String,
    /// Voice id (upstream-specific, e.g. "alloy").
    pub voice: String,
    /// Text to synthesize. Must be non-empty and at most
    /// [`MAX_INPUT_CHARS`] characters.
    pub input: String,
    /// Output audio format ("mp3"|"opus"|"aac"|"flac"|"wav"|"pcm"); defaults
    /// to `"mp3"` when `None`. Sent upstream as `response_format`.
    pub format: Option<String>,
    /// Playback speed multiplier; omitted from the request when `None`.
    pub speed: Option<f64>,
}

/// Synthesized audio, ready for `store.put` CID'ing by the caller (see the
/// bot pipeline draft's "sink" section).
#[derive(Debug, Clone)]
pub struct TtsAudio {
    pub bytes: Vec<u8>,
    pub mime: String,
}

fn strip_trailing_slash(base_url: &str) -> &str {
    base_url.trim_end_matches('/')
}

/// First 500 *characters* (not bytes) of `body`, matching the truncation
/// convention in `openai::stream_chat_completion`'s error messages.
fn truncate_500(body: &str) -> String {
    body.chars().take(500).collect()
}

/// Builds a fresh client for one call. `no_proxy()` matters here for the
/// same reason as `openai::build_client`: upstreams may be local and proxy
/// env vars would otherwise break loopback requests.
fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .context("ai: building TTS upstream HTTP client")
}

/// Maps an OpenAI-style `response_format` value to its MIME type. Unknown
/// formats fall back to `application/octet-stream` rather than erroring --
/// upstreams may support formats beyond this list.
pub fn format_to_mime(format: &str) -> &'static str {
    match format {
        "mp3" => "audio/mpeg",
        "opus" => "audio/ogg",
        "aac" => "audio/aac",
        "flac" => "audio/flac",
        "wav" => "audio/wav",
        "pcm" => "audio/pcm",
        _ => "application/octet-stream",
    }
}

/// Validates `req` and returns the effective `response_format` (defaulted
/// to `"mp3"`). Split out from [`synthesize`] so callers -- and tests --
/// can check a request's shape without making an HTTP call.
fn validate(req: &TtsParams) -> Result<String> {
    if req.model.trim().is_empty() {
        bail!("ai: tts requires a model");
    }
    if req.voice.trim().is_empty() {
        bail!("ai: tts requires a voice");
    }
    if req.input.is_empty() {
        bail!("ai: tts input must not be empty");
    }
    let input_len = req.input.chars().count();
    if input_len > MAX_INPUT_CHARS {
        bail!(
            "ai: tts input is {input_len} characters, exceeding the upstream \
             {MAX_INPUT_CHARS}-character limit; split the input before calling \
             synthesize (chunked synthesis is not implemented in v1)"
        );
    }
    Ok(req.format.as_deref().unwrap_or("mp3").to_string())
}

/// Builds the JSON request body sent to `/audio/speech`. Pure (no I/O) so
/// it can be unit-tested directly.
fn build_body(req: &TtsParams, format: &str) -> Value {
    let mut body = json!({
        "model": req.model,
        "input": req.input,
        "voice": req.voice,
        "response_format": format,
    });
    if let Some(speed) = req.speed {
        body["speed"] = json!(speed);
    }
    body
}

/// `POST {base_url}/audio/speech`: synthesizes `req.input` into audio bytes
/// via the OpenAI-compatible TTS endpoint of `provider`. See the module doc
/// for the base_url/header/error conventions this follows.
pub async fn synthesize(provider: &AiProviderConfig, req: TtsParams) -> Result<TtsAudio> {
    let format = validate(&req)?;
    let body = build_body(&req, &format);

    let url = format!("{}/audio/speech", strip_trailing_slash(&provider.base_url));

    let client = build_client()?;
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", provider.api_key))
        .json(&body)
        .send()
        .await
        .with_context(|| format!("TTS API request failed: POST {url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body_text = response.text().await.unwrap_or_default();
        bail!(
            "TTS API returned an error ({status}): {}",
            truncate_500(&body_text)
        );
    }

    let bytes = response
        .bytes()
        .await
        .context("ai: reading TTS API response body")?
        .to_vec();
    if bytes.is_empty() {
        bail!("ai: TTS API returned an empty audio body");
    }

    Ok(TtsAudio {
        bytes,
        mime: format_to_mime(&format).to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(input: &str) -> TtsParams {
        TtsParams {
            model: "tts-1".to_string(),
            voice: "alloy".to_string(),
            input: input.to_string(),
            format: None,
            speed: None,
        }
    }

    #[test]
    fn format_to_mime_maps_known_formats() {
        assert_eq!(format_to_mime("mp3"), "audio/mpeg");
        assert_eq!(format_to_mime("opus"), "audio/ogg");
        assert_eq!(format_to_mime("aac"), "audio/aac");
        assert_eq!(format_to_mime("wav"), "audio/wav");
        assert_eq!(format_to_mime("flac"), "audio/flac");
        assert_eq!(format_to_mime("pcm"), "audio/pcm");
    }

    #[test]
    fn format_to_mime_falls_back_for_unknown_formats() {
        assert_eq!(format_to_mime("weird"), "application/octet-stream");
    }

    #[test]
    fn validate_defaults_format_to_mp3() {
        let format = validate(&params("hello")).unwrap();
        assert_eq!(format, "mp3");
    }

    #[test]
    fn validate_keeps_explicit_format() {
        let mut req = params("hello");
        req.format = Some("opus".to_string());
        let format = validate(&req).unwrap();
        assert_eq!(format, "opus");
    }

    #[test]
    fn validate_rejects_empty_input() {
        let err = validate(&params("")).expect_err("empty input should error");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn validate_rejects_input_over_the_upstream_limit() {
        let long_input = "a".repeat(MAX_INPUT_CHARS + 1);
        let err = validate(&params(&long_input)).expect_err("over-limit input should error");
        let msg = err.to_string();
        assert!(
            msg.contains("4096"),
            "error should mention the limit: {msg}"
        );
    }

    #[test]
    fn validate_accepts_input_at_exactly_the_limit() {
        let exact_input = "a".repeat(MAX_INPUT_CHARS);
        assert!(validate(&params(&exact_input)).is_ok());
    }

    #[test]
    fn validate_counts_characters_not_bytes() {
        // Multi-byte characters must be counted as one character each, not
        // by UTF-8 byte length -- otherwise valid input could be rejected
        // (or over-limit input accepted) depending on encoding.
        let input: String = "あ".repeat(MAX_INPUT_CHARS);
        assert_eq!(input.chars().count(), MAX_INPUT_CHARS);
        assert!(validate(&params(&input)).is_ok());

        let over: String = "あ".repeat(MAX_INPUT_CHARS + 1);
        assert!(validate(&params(&over)).is_err());
    }

    #[test]
    fn validate_rejects_blank_model_or_voice() {
        let mut req = params("hello");
        req.model = "  ".to_string();
        assert!(validate(&req).is_err());

        let mut req = params("hello");
        req.voice = "".to_string();
        assert!(validate(&req).is_err());
    }

    #[test]
    fn build_body_includes_required_fields_and_default_format() {
        let req = params("read this aloud");
        let format = validate(&req).unwrap();
        let body = build_body(&req, &format);
        assert_eq!(body["model"], json!("tts-1"));
        assert_eq!(body["voice"], json!("alloy"));
        assert_eq!(body["input"], json!("read this aloud"));
        assert_eq!(body["response_format"], json!("mp3"));
        assert!(
            body.get("speed").is_none(),
            "speed should be omitted when not set"
        );
    }

    #[test]
    fn build_body_includes_speed_when_configured() {
        let mut req = params("read this aloud");
        req.speed = Some(1.25);
        let format = validate(&req).unwrap();
        let body = build_body(&req, &format);
        assert_eq!(body["speed"], json!(1.25));
    }

    #[test]
    fn build_body_uses_explicit_response_format() {
        let mut req = params("read this aloud");
        req.format = Some("wav".to_string());
        let format = validate(&req).unwrap();
        let body = build_body(&req, &format);
        assert_eq!(body["response_format"], json!("wav"));
    }

    #[tokio::test]
    async fn synthesize_rejects_over_limit_input_before_any_request_is_sent() {
        // Mirrors openai.rs's `missing_model_errors_before_any_request_is_sent`:
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
        let long_input = "a".repeat(MAX_INPUT_CHARS + 1);
        let err = synthesize(&provider, params(&long_input))
            .await
            .expect_err("over-limit input should error");
        assert!(err.to_string().contains("4096"));

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
