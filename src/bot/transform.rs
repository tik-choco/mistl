//! Pipeline transform steps: `summarize` (LLM chat completion -> read-aloud
//! script), `translate` (LLM chat completion -> translated title/script), and
//! `tts` (direct-upstream speech synthesis -> a stored, CID'd audio blob).
//! Applied in the pipeline's configured order by [`run_chain`].

use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::warn;

use crate::ai::openai::{self, UpstreamConfig};
use crate::ai::protocol::ChatMessage;
use crate::ai::tts::{self, TtsParams};
use crate::config::{AiConfig, AiProviderConfig, Config, TransformConfig};
use crate::daemon::AppState;

use super::source::Article;

/// Instructs the summarizer to produce a short, plain (Markdown-free)
/// Japanese script suitable for text-to-speech -- per the Wave 2 brief's
/// "長文対策" decision (item 5): keeping the script well under
/// `crate::ai::tts::MAX_INPUT_CHARS` avoids the truncation path in
/// [`truncate_for_tts`] in the common case. Worded generically (記事やメッセージ)
/// since a source may be either an article or a chat post.
const SUMMARIZE_SYSTEM_PROMPT: &str = "あなたは記事やメッセージを音声で読み上げるための台本を作成するアシスタントです。\
与えられたタイトルと本文をもとに、聞き手にとって分かりやすい日本語の読み上げ台本を作成してください。\
台本は日本語で1500文字以内にまとめ、見出し記号や箇条書き、Markdown記法は使わず、そのまま読み上げられる自然な文章のみを出力してください。";

/// The outcome of running a pipeline's transform chain over one [`Article`]:
/// `script` is set once a `summarize` or `translate` step has run (and is
/// fed to any later `tts` step); `title`/`lang` are set once a `translate`
/// step has run (sinks fall back to the article's own title/language when
/// unset); `audio` is set once a `tts` step has run.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct TransformOutcome {
    pub script: Option<String>,
    pub title: Option<String>,
    pub lang: Option<String>,
    pub audio: Option<AudioOutcome>,
}

/// Synthesized audio, already stored (see [`synthesize_audio`]) and ready
/// for a sink to reference by CID.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct AudioOutcome {
    pub cid: String,
    pub mime: String,
    pub size: u64,
}

/// Runs `transforms` in order against `article`. A `tts` step with no
/// preceding `summarize` step falls back to `article.excerpt`/`article.title`
/// (see [`fallback_script`]) -- a pipeline consisting of `tts` alone is
/// still a valid (if unpolished) configuration. Any step failing (model reference
/// unresolved, upstream error) aborts the chain -- per the pipeline flow's
/// "source/transform の致命的エラーのみ run FAIL" rule, a transform failure
/// fails the whole run for this article's batch (see `bot::run_pipeline`).
pub(super) async fn run_chain(
    state: &Arc<AppState>,
    config: &Config,
    pipeline_id: &str,
    transforms: &[TransformConfig],
    article: &Article,
) -> Result<TransformOutcome> {
    let mut outcome = TransformOutcome::default();
    for step in transforms {
        match step {
            TransformConfig::Summarize {
                model,
                reasoning_effort,
                ..
            } => {
                outcome.script = Some(
                    summarize(
                        Some(state),
                        &config.ai,
                        model.as_ref(),
                        reasoning_effort.as_deref(),
                        article,
                    )
                    .await?,
                );
            }
            TransformConfig::Tts {
                model,
                voice,
                format,
                speed,
                ..
            } => {
                let raw_text = outcome
                    .script
                    .clone()
                    .unwrap_or_else(|| fallback_script(article));
                let text = truncate_for_tts(&raw_text, pipeline_id, &article.id);
                outcome.audio = Some(
                    synthesize_audio(
                        state,
                        &config.ai,
                        model.as_ref(),
                        voice.as_deref(),
                        format.as_deref(),
                        *speed,
                        &text,
                        &article.id,
                    )
                    .await?,
                );
            }
            TransformConfig::Translate {
                model,
                reasoning_effort,
                target_lang,
                ..
            } => {
                let title = outcome
                    .title
                    .clone()
                    .unwrap_or_else(|| article.title.clone());
                let text = outcome
                    .script
                    .clone()
                    .unwrap_or_else(|| article.body.clone());
                let (translated_title, translated_text) = translate(
                    Some(state),
                    &config.ai,
                    model.as_ref(),
                    reasoning_effort.as_deref(),
                    target_lang,
                    &title,
                    &text,
                )
                .await?;
                outcome.title = Some(translated_title);
                outcome.script = Some(translated_text);
                outcome.lang = Some(target_lang.clone());
            }
        }
    }
    Ok(outcome)
}

/// System prompt for a `translate` upstream call: instructs the model to
/// translate the input into the language named by the BCP-47-style
/// `target_lang` tag (e.g. `"ja"`, `"en"`, `"zh"`), emitting only the
/// translation itself -- no preamble, quotes, or Markdown fences -- while
/// preserving the source's meaning and tone. When `for_title` is set, an
/// extra line constrains the output to a single-line title (mirrors the
/// title/body split in [`translate`]: two separate calls rather than
/// splitting one response, so a malformed multi-line reply can't smear a
/// title into the body or vice versa).
fn translate_system_prompt(target_lang: &str, for_title: bool) -> String {
    let mut prompt = format!(
        "あなたは与えられたテキストを翻訳する翻訳アシスタントです。\
入力されたテキストを、言語タグ \"{target_lang}\" (BCP-47形式の言語タグ。例えば \"ja\" は日本語、\
\"en\" は英語、\"zh\" は中国語を表します) が指す言語に翻訳してください。\
出力は翻訳結果のみとし、前置きの説明や引用符、Markdown記法などは一切含めないでください。\
原文の意味とトーンを保ちながら、自然な訳文にしてください。"
    );
    if for_title {
        prompt.push_str(
            "これは見出し(タイトル)の翻訳です。出力は1行の短いタイトルのみにしてください。",
        );
    }
    prompt
}

/// Calls `model_ref`'s LLM to translate `input`, resolving the model reference fresh
/// on every call (mirrors [`summarize`]'s per-call resolution) so that a
/// skipped call -- see [`translate`]'s empty-title short-circuit -- never
/// touches `ai.providers` or the upstream at all. `empty_label` names the
/// missing piece in the empty-result bail (`"title"` or `"translation"`).
async fn translate_one(
    state: Option<&Arc<AppState>>,
    ai: &AiConfig,
    model_ref: Option<&crate::config::ModelRef>,
    effort: Option<&str>,
    system_prompt: String,
    input: &str,
    empty_label: &str,
) -> Result<String> {
    let resolved = crate::config::resolve_ref(ai, model_ref).with_context(|| {
        format!(
            "bot: transform \"translate\" model reference {model_ref:?} not found in ai.providers; \
             add it with `mistl config set ai.providers <json>` (and ai.providers / \
             ai.default_ref -- see `mistl config show`)"
        )
    })?;
    let upstream = UpstreamConfig {
        base_url: resolved.base_url,
        api_key: resolved.api_key,
        model: (!resolved.model.is_empty()).then_some(resolved.model),
        reasoning_effort: effort.map(String::from),
    };
    let messages = vec![
        ChatMessage {
            role: "system".to_string(),
            content: system_prompt,
            ..Default::default()
        },
        ChatMessage {
            role: "user".to_string(),
            content: input.to_string(),
            ..Default::default()
        },
    ];
    let translated = call_completion(state, ai, model_ref, effort, &upstream, messages)
        .await
        .with_context(|| {
            format!("bot: transform \"translate\" failed calling model reference {model_ref:?}")
        })?;
    let translated = translated.trim().to_string();
    if translated.is_empty() {
        anyhow::bail!(
            "bot: transform \"translate\" (model reference {model_ref:?}) returned an empty {empty_label}"
        );
    }
    Ok(translated)
}

/// Translates `title` + `text` into `target_lang` via the model reference's LLM,
/// returning `(translated_title, translated_text)`. Uses two independent
/// upstream calls -- one per field -- rather than one call with a
/// combined prompt, so neither result depends on fragile parsing of the
/// other out of a single response (e.g. "first line is the title"). When
/// `title` is blank (some sources, like chat posts, have none), the title
/// call is skipped entirely and the original (blank) title is returned
/// unchanged -- no upstream call, no model reference resolution.
async fn translate(
    state: Option<&Arc<AppState>>,
    ai: &AiConfig,
    model_ref: Option<&crate::config::ModelRef>,
    effort: Option<&str>,
    target_lang: &str,
    title: &str,
    text: &str,
) -> Result<(String, String)> {
    let translated_title = if title.trim().is_empty() {
        title.to_string()
    } else {
        translate_one(
            state,
            ai,
            model_ref,
            effort,
            translate_system_prompt(target_lang, true),
            title,
            "title",
        )
        .await?
    };
    let translated_text = translate_one(
        state,
        ai,
        model_ref,
        effort,
        translate_system_prompt(target_lang, false),
        text,
        "translation",
    )
    .await?;
    Ok((translated_title, translated_text))
}

fn build_summarize_prompt(article: &Article) -> String {
    let mut prompt = format!("タイトル: {}\n", article.title);
    if !article.author_name.trim().is_empty() {
        prompt.push_str(&format!("著者: {}\n", article.author_name));
    }
    if !article.excerpt.trim().is_empty() {
        prompt.push_str(&format!("要約: {}\n", article.excerpt));
    }
    prompt.push_str("本文:\n");
    prompt.push_str(&article.body);
    prompt
}

async fn summarize(
    state: Option<&Arc<AppState>>,
    ai: &AiConfig,
    model_ref: Option<&crate::config::ModelRef>,
    effort: Option<&str>,
    article: &Article,
) -> Result<String> {
    let resolved = crate::config::resolve_ref(ai, model_ref).with_context(|| {
        format!(
            "bot: transform \"summarize\" model reference {model_ref:?} not found in ai.providers; \
             add it with `mistl config set ai.providers <json>` (and ai.providers / \
             ai.default_ref -- see `mistl config show`)"
        )
    })?;
    let upstream = UpstreamConfig {
        base_url: resolved.base_url,
        api_key: resolved.api_key,
        model: (!resolved.model.is_empty()).then_some(resolved.model),
        reasoning_effort: effort.map(String::from),
    };
    let messages = vec![
        ChatMessage {
            role: "system".to_string(),
            content: SUMMARIZE_SYSTEM_PROMPT.to_string(),
            ..Default::default()
        },
        ChatMessage {
            role: "user".to_string(),
            content: build_summarize_prompt(article),
            ..Default::default()
        },
    ];
    let script = call_completion(state, ai, model_ref, effort, &upstream, messages)
        .await
        .with_context(|| {
            format!("bot: transform \"summarize\" failed calling model reference {model_ref:?}")
        })?;
    let script = script.trim().to_string();
    if script.is_empty() {
        anyhow::bail!(
            "bot: transform \"summarize\" (model reference {model_ref:?}) returned an empty script"
        );
    }
    Ok(script)
}

/// Used when a `tts` step has no preceding `summarize` step to feed from.
fn fallback_script(article: &Article) -> String {
    if !article.excerpt.trim().is_empty() {
        format!("{}。{}", article.title, article.excerpt)
    } else {
        article.title.clone()
    }
}

/// Truncates `text` to [`tts::MAX_INPUT_CHARS`] at a character boundary when
/// it exceeds the upstream limit, logging a warning (the Wave 2 brief's v1
/// "長文対策" fallback -- see item 5: this is a rare path once the
/// summarizer prompt's ~1500-character instruction is honored).
fn truncate_for_tts(text: &str, pipeline_id: &str, article_id: &str) -> String {
    let char_count = text.chars().count();
    if char_count <= tts::MAX_INPUT_CHARS {
        return text.to_string();
    }
    warn!(
        pipeline_id,
        article_id,
        original_chars = char_count,
        kept_chars = tts::MAX_INPUT_CHARS,
        "bot: read-aloud script exceeded the TTS upstream's character limit; truncated at a character boundary"
    );
    text.chars().take(tts::MAX_INPUT_CHARS).collect()
}

/// Maps a synthesized-audio MIME type to a file extension for the blob's
/// stored name / a chat-post sink's `fileName`, using the actual response
/// type rather than the requested speech format.
pub(super) fn extension_for_mime(mime: &str) -> &'static str {
    match mime {
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/aac" => "aac",
        "audio/flac" => "flac",
        "audio/wav" => "wav",
        "audio/pcm" => "pcm",
        _ => "bin",
    }
}

async fn call_completion(
    state: Option<&Arc<AppState>>,
    ai: &AiConfig,
    model: Option<&crate::config::ModelRef>,
    effort: Option<&str>,
    upstream: &UpstreamConfig,
    messages: Vec<ChatMessage>,
) -> Result<String> {
    if upstream.base_url.starts_with("mist-network://") {
        let state = state.context("bot: a Room model requires the daemon AI service")?;
        crate::ai::chat_model(
            state,
            model.or(ai.default_ref.as_ref()),
            effort.map(String::from),
            messages,
        )
        .await
    } else {
        openai::stream_chat_completion(upstream, &messages, None, None).await
    }
}

async fn synthesize_audio(
    state: &Arc<AppState>,
    ai: &AiConfig,
    model_ref: Option<&crate::config::ModelRef>,
    voice_override: Option<&str>,
    format: Option<&str>,
    speed: Option<f64>,
    text: &str,
    article_id: &str,
) -> Result<AudioOutcome> {
    let resolved = crate::config::resolve_ref(ai, model_ref).with_context(|| {
        format!(
            "bot: transform \"tts\" model reference {model_ref:?} not found in ai.providers; \
             add it with `mistl config set ai.providers <json>` (and ai.providers -- see \
             `mistl config show`)"
        )
    })?;
    let voice = voice_override.map(String::from).or_else(|| {
        ai.tts.as_ref().filter(|v| crate::config::effective_ref(ai, model_ref).is_some_and(|r| r.provider_id == v.provider_id)).and_then(|v| v.voice.clone())
    })
        .filter(|voice| !voice.trim().is_empty())
        .with_context(|| {
            format!(
                "bot: transform \"tts\" model reference {model_ref:?} has no voice set; set it with \
                 `mistl config set bot.pipelines <json>` (set tts.voice or ai.tts.voice)"
            )
        }).or_else(|err| if resolved.base_url.starts_with("mist-network://") { Ok(String::new()) } else { Err(err) })?;

    let provider = AiProviderConfig {
        id: String::new(),
        label: String::new(),
        base_url: resolved.base_url,
        api_key: resolved.api_key,
        ..Default::default()
    };
    let params = TtsParams {
        model: resolved.model,
        voice,
        input: text.to_string(),
        format: format.map(str::to_string),
        speed,
    };
    let audio = if provider.base_url.starts_with("mist-network://") {
        let reference = model_ref
            .or(ai.default_ref.as_ref())
            .context("ai.default_ref is not set")?;
        crate::ai::synthesize_model(state, reference, params).await
    } else {
        tts::synthesize(&provider, params).await
    }
    .with_context(|| {
        format!("bot: transform \"tts\" failed calling model reference {model_ref:?}")
    })?;

    let store = crate::storage::store(state).await?;
    let name = format!("{article_id}.{}", extension_for_mime(&audio.mime));
    let size = audio.bytes.len() as u64;
    let cid = store.put(&name, audio.bytes).await?;
    Ok(AudioOutcome {
        cid,
        mime: audio.mime,
        size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_article() -> Article {
        Article {
            id: "article-1".to_string(),
            title: "見出し".to_string(),
            excerpt: "要約文".to_string(),
            body: "本文テキスト".to_string(),
            source_links: vec![],
            author_name: "記者".to_string(),
            created_at: 1_700_000_000_000,
        }
    }

    #[test]
    fn fallback_script_prefers_title_plus_excerpt() {
        let script = fallback_script(&sample_article());
        assert_eq!(script, "見出し。要約文");
    }

    #[test]
    fn fallback_script_falls_back_to_title_alone_when_excerpt_is_blank() {
        let mut article = sample_article();
        article.excerpt = "   ".to_string();
        assert_eq!(fallback_script(&article), "見出し");
    }

    #[test]
    fn truncate_for_tts_passes_short_text_through_unchanged() {
        assert_eq!(truncate_for_tts("short", "p", "a"), "short");
    }

    #[test]
    fn truncate_for_tts_truncates_long_text_at_a_character_boundary() {
        let long_text: String = "あ".repeat(tts::MAX_INPUT_CHARS + 100);
        let truncated = truncate_for_tts(&long_text, "p", "a");
        assert_eq!(truncated.chars().count(), tts::MAX_INPUT_CHARS);
    }

    #[tokio::test]
    async fn translate_reports_a_clear_error_when_the_model_ref_is_unresolved() {
        let ai = AiConfig::default();
        let err = translate(
            None,
            &ai,
            Some(&crate::config::ModelRef {
                provider_id: "missing-provider".into(),
                model: "m".into(),
            }),
            None,
            "en",
            "見出し",
            "本文",
        )
        .await
        .expect_err("an unresolved model reference must error");
        let msg = err.to_string();
        assert!(
            msg.contains("missing-provider"),
            "error should name the model reference: {msg}"
        );
        assert!(
            msg.contains("ai.providers"),
            "error should point at the config path: {msg}"
        );
    }

    #[tokio::test]
    async fn translate_skips_the_upstream_call_for_a_blank_title_but_still_translates_the_body() {
        // A blank title takes the `title.trim().is_empty()` branch in
        // `translate` and never reaches `translate_one` -- so it can't
        // fail even though the model reference below is unresolvable. The body has
        // no such branch, so it always goes through `translate_one` and
        // errors here on model reference resolution. Were the title path
        // mistakenly *not* skipped, the result would be unchanged (`Err`,
        // same message) since both paths share the same unresolvable
        // model reference -- so this pins down "the body call still runs and fails
        // when the title is blank", with the skip itself covered by
        // reading `translate`'s `if title.trim().is_empty()` guard.
        let ai = AiConfig::default();
        let err = translate(
            None,
            &ai,
            Some(&crate::config::ModelRef {
                provider_id: "missing-provider".into(),
                model: "m".into(),
            }),
            None,
            "en",
            "   ",
            "本文",
        )
        .await
        .expect_err("the body call must still run (and fail) even when the title is blank");
        let msg = err.to_string();
        assert!(
            msg.contains("missing-provider"),
            "error should name the model reference: {msg}"
        );
    }
}
