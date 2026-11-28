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
//! Long input is not chunked here: `synthesize` accepts up to 10000
//! characters and leaves tighter backend limits to the upstream.
//! Splitting long articles into multiple
//! synthesis calls (and stitching the resulting audio) is left to the
//! caller -- tracked as a Wave 2 concern for the `tts` transform, not
//! solved here.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::config::AiProviderConfig;

/// Local input character limit; upstreams may impose a tighter limit.
pub const MAX_INPUT_CHARS: usize = 10000;

/// One text-to-speech request.
#[derive(Debug, Clone, Default)]
pub struct TtsParams {
    /// Upstream TTS model id (e.g. "tts-model", "speech-model").
    pub model: String,
    /// Voice id (upstream-specific, e.g. "alloy"); empty is omitted.
    pub voice: String,
    /// Text to synthesize. Must be non-empty and at most
    /// [`MAX_INPUT_CHARS`] characters.
    pub input: String,
    /// Output audio format ("mp3"|"opus"|"aac"|"flac"|"wav"|"pcm"); defaults
    /// to `"mp3"` when `None`. Sent upstream as `response_format`.
    pub format: Option<String>,
    /// Playback speed multiplier; omitted from the request when `None`.
    pub speed: Option<f64>,
    /// Other bounded top-level request fields passed through to an HTTP or
    /// extension-aware room provider (including object-form `voice`).
    pub extra: serde_json::Map<String, Value>,
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

/// Keep upstream error diagnostics bounded as well as successful audio bodies.
pub(super) async fn read_error_text(mut response: reqwest::Response) -> String {
    const MAX_ERROR_BYTES: usize = 8 * 1024;
    let mut bytes = Vec::new();
    while bytes.len() < MAX_ERROR_BYTES {
        let Ok(Some(chunk)) = response.chunk().await else {
            break;
        };
        let len = chunk.len().min(MAX_ERROR_BYTES - bytes.len());
        bytes.extend_from_slice(&chunk[..len]);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Upper bound on the audio body accepted from a TTS upstream.
const MAX_TTS_AUDIO_BYTES: usize = 32 * 1024 * 1024;

/// Builds a fresh client for one call. `no_proxy()` matters here for the
/// same reason as `openai::build_client`: upstreams may be local and proxy
/// env vars would otherwise break loopback requests.
fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .context("ai: building TTS upstream HTTP client")
}

/// Validates `req` and returns the effective `response_format` (defaulted
/// to `"mp3"`). Split out from [`synthesize`] so callers -- and tests --
/// can check a request's shape without making an HTTP call.
fn validate(req: &TtsParams) -> Result<String> {
    if !super::protocol::valid_tts_extra(&req.extra) {
        bail!("ai: oversized TTS extra fields");
    }
    if req.model.trim().is_empty() {
        bail!("ai: tts requires a model");
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
    if req
        .speed
        .is_some_and(|v| !crate::config::valid_tts_speed(v))
    {
        bail!("ai: tts speed must be a finite number between 0.25 and 4.0");
    }
    Ok(req
        .format
        .as_deref()
        .or_else(|| req.extra.get("response_format").and_then(Value::as_str))
        .unwrap_or("mp3")
        .to_string())
}

/// Builds the JSON request body sent to `/audio/speech`. Pure (no I/O) so
/// it can be unit-tested directly.
fn build_body(req: &TtsParams, format: &str) -> Value {
    let mut body = json!({
        "model": req.model,
        "input": req.input,
        "response_format": format,
    });
    if !req.voice.is_empty() {
        body["voice"] = json!(req.voice);
    }
    // Object-form voice is an explicit client selection, not the resolved
    // string default. Never let the extension replace the model or input.
    if let Some(voice) = req.extra.get("voice").filter(|v| v.is_object()) {
        body["voice"] = voice.clone();
    }
    for (key, value) in &req.extra {
        if !matches!(
            key.as_str(),
            "model" | "input" | "voice" | "response_format" | "speed"
        ) {
            body[key] = value.clone();
        }
    }
    if req.format.is_none()
        && let Some(value) = req.extra.get("response_format")
    {
        body["response_format"] = value.clone();
    }
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
    let mut body = build_body(&req, &format);

    let url = format!("{}/audio/speech", strip_trailing_slash(&provider.base_url));

    let client = build_client()?;
    let response = loop {
        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", provider.api_key))
            .json(&body)
            .send()
            .await
            .with_context(|| format!("TTS API request failed: POST {url}"))?;
        if response.status().is_success() {
            break response;
        }
        let status = response.status();
        let body_text = read_error_text(response).await;
        // A format hint is optional: retry once using the backend's own
        // container when it explicitly refuses this parameter. Other errors
        // keep their normal behavior, and speed is preserved on the retry.
        if matches!(status.as_u16(), 400 | 422)
            && body_text.to_ascii_lowercase().contains("response_format")
            && body
                .as_object_mut()
                .is_some_and(|fields| fields.remove("response_format").is_some())
        {
            continue;
        }
        return Err(anyhow::Error::new(super::openai::UpstreamHttpError {
            label: "TTS API",
            status: status.as_u16(),
            detail: truncate_500(&body_text),
        }));
    };

    if response
        .content_length()
        .is_some_and(|len| len > MAX_TTS_AUDIO_BYTES as u64)
    {
        bail!("ai: TTS API response exceeds the maximum audio size");
    }
    // A backend may ignore the requested container; its response type wins.
    // Without type metadata, keep the payload opaque instead of guessing.
    let mime = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or("application/octet-stream")
        .to_string();
    let mut response = response;
    let mut bytes: Vec<u8> = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("ai: reading TTS API response body")?
    {
        if bytes.len() + chunk.len() > MAX_TTS_AUDIO_BYTES {
            bail!("ai: TTS API response exceeds the maximum audio size");
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        bail!("ai: TTS API returned an empty audio body");
    }

    Ok(TtsAudio { bytes, mime })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(input: &str) -> TtsParams {
        TtsParams {
            model: "tts-model".to_string(),
            voice: "alloy".to_string(),
            input: input.to_string(),
            format: None,
            speed: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn validate_accepts_speed_bounds_and_rejects_nonfinite_or_out_of_range() {
        for speed in [0.25, 1.0, 4.0] {
            let mut req = params("hi");
            req.speed = Some(speed);
            assert!(validate(&req).is_ok());
        }
        for speed in [0.24, 4.01, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut req = params("hi");
            req.speed = Some(speed);
            assert!(validate(&req).is_err());
        }
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
            msg.contains("10000"),
            "error should mention the limit: {msg}"
        );
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
    fn validate_rejects_blank_model_and_accepts_empty_voice() {
        let mut req = params("hello");
        req.model = "  ".to_string();
        assert!(validate(&req).is_err());

        let mut req = params("hello");
        req.voice = "".to_string();
        let format = validate(&req).unwrap();
        assert!(build_body(&req, &format).get("voice").is_none());
    }

    #[test]
    fn extras_preserve_known_fields_and_pass_through_extensions() {
        let mut req = params("hello");
        req.extra = json!({"model":"other", "input":"other", "voice":"other", "speed":2,
            "response_format":"wav", "keep_alive":"10m", "vendor_options":{"seed":7}, "stream_format":"sse"})
            .as_object().unwrap().clone();
        let body = build_body(&req, &validate(&req).unwrap());
        assert_eq!(body["model"], "tts-model");
        assert_eq!(body["input"], "hello");
        assert_eq!(body["voice"], "alloy");
        assert!(body.get("speed").is_none());
        assert_eq!(body["response_format"], "wav");
        assert_eq!(body["vendor_options"]["seed"], 7);
        assert_eq!(body["keep_alive"], "10m");
        assert_eq!(body["stream_format"], "sse");
        req.voice.clear();
        assert!(
            build_body(&req, &validate(&req).unwrap())
                .get("voice")
                .is_none()
        );
        req.format = Some("opus".into());
        assert_eq!(
            build_body(&req, &validate(&req).unwrap())["response_format"],
            "opus"
        );
    }

    #[test]
    fn object_voice_overrides_resolved_default_and_extra_size_is_bounded() {
        let mut req = params("hello");
        req.extra.insert("voice".into(), json!({"id":"speaker"}));
        assert_eq!(
            build_body(&req, &validate(&req).unwrap())["voice"],
            json!({"id":"speaker"})
        );
        req.extra.insert(
            "instructions".into(),
            json!("a".repeat(super::super::protocol::MAX_VOICE_EXTRA_BYTES)),
        );
        assert!(validate(&req).is_err());
    }

    #[test]
    fn build_body_maps_fields_and_honors_speed_and_explicit_format() {
        let mut req = params("read this aloud");
        let format = validate(&req).unwrap();
        let body = build_body(&req, &format);
        assert_eq!(body["model"], json!("tts-model"));
        assert_eq!(body["voice"], json!("alloy"));
        assert_eq!(body["input"], json!("read this aloud"));
        assert_eq!(body["response_format"], json!("mp3"));
        assert!(
            body.get("speed").is_none(),
            "speed should be omitted when not set"
        );

        req.speed = Some(1.25);
        req.format = Some("wav".to_string());
        let format = validate(&req).unwrap();
        let body = build_body(&req, &format);
        assert_eq!(body["speed"], json!(1.25));
        assert_eq!(body["response_format"], json!("wav"));
    }
}
