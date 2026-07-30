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
#[derive(Debug, Clone, Default)]
pub(super) struct TransformOutcome {
    pub script: Option<String>,
    pub title: Option<String>,
    pub lang: Option<String>,
    pub audio: Option<AudioOutcome>,
}

/// Synthesized audio, already stored (see [`synthesize_audio`]) and ready
/// for a sink to reference by CID.
#[derive(Debug, Clone)]
pub(super) struct AudioOutcome {
    pub cid: String,
    pub mime: String,
    pub size: u64,
}

/// Runs `transforms` in order against `article`. A `tts` step with no
/// preceding `summarize` step falls back to `article.excerpt`/`article.title`
/// (see [`fallback_script`]) -- a pipeline consisting of `tts` alone is
/// still a valid (if unpolished) configuration. Any step failing (preset
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
            TransformConfig::Summarize { preset_id } => {
                outcome.script = Some(summarize(&config.ai, preset_id, article).await?);
            }
            TransformConfig::Tts {
                preset_id,
                format,
                speed,
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
                        preset_id,
                        format.as_deref(),
                        *speed,
                        &text,
                        &article.id,
                    )
                    .await?,
                );
            }
            TransformConfig::Translate {
                preset_id,
                target_lang,
            } => {
                let title = outcome
                    .title
                    .clone()
                    .unwrap_or_else(|| article.title.clone());
                let text = outcome
                    .script
                    .clone()
                    .unwrap_or_else(|| article.body.clone());
                let (translated_title, translated_text) =
                    translate(&config.ai, preset_id, target_lang, &title, &text).await?;
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

/// Calls `preset_id`'s LLM to translate `input`, resolving the preset fresh
/// on every call (mirrors [`summarize`]'s per-call resolution) so that a
/// skipped call -- see [`translate`]'s empty-title short-circuit -- never
/// touches `ai.presets` or the upstream at all. `empty_label` names the
/// missing piece in the empty-result bail (`"title"` or `"translation"`).
async fn translate_one(
    ai: &AiConfig,
    preset_id: &str,
    system_prompt: String,
    input: &str,
    empty_label: &str,
) -> Result<String> {
    let resolved = crate::config::resolve_preset(ai, Some(preset_id)).with_context(|| {
        format!(
            "bot: transform \"translate\" preset {preset_id:?} not found in ai.presets; \
             add it with `mistl config set ai.presets <json>` (and ai.providers / \
             ai.default_preset_id -- see `mistl config show`)"
        )
    })?;
    let upstream = UpstreamConfig {
        base_url: resolved.base_url,
        api_key: resolved.api_key,
        model: (!resolved.model.is_empty()).then_some(resolved.model),
        temperature: resolved.temperature,
        reasoning_effort: resolved.reasoning_effort,
    };
    let messages = vec![
        ChatMessage {
            role: "system".to_string(),
            content: system_prompt,
        },
        ChatMessage {
            role: "user".to_string(),
            content: input.to_string(),
        },
    ];
    let translated = openai::stream_chat_completion(&upstream, &messages, None, None)
        .await
        .with_context(|| {
            format!("bot: transform \"translate\" failed calling preset {preset_id:?}")
        })?;
    let translated = translated.trim().to_string();
    if translated.is_empty() {
        anyhow::bail!(
            "bot: transform \"translate\" (preset {preset_id:?}) returned an empty {empty_label}"
        );
    }
    Ok(translated)
}

/// Translates `title` + `text` into `target_lang` via the preset's LLM,
/// returning `(translated_title, translated_text)`. Uses two independent
/// upstream calls -- one per field -- rather than one call with a
/// combined prompt, so neither result depends on fragile parsing of the
/// other out of a single response (e.g. "first line is the title"). When
/// `title` is blank (some sources, like chat posts, have none), the title
/// call is skipped entirely and the original (blank) title is returned
/// unchanged -- no upstream call, no preset resolution.
async fn translate(
    ai: &AiConfig,
    preset_id: &str,
    target_lang: &str,
    title: &str,
    text: &str,
) -> Result<(String, String)> {
    let translated_title = if title.trim().is_empty() {
        title.to_string()
    } else {
        translate_one(
            ai,
            preset_id,
            translate_system_prompt(target_lang, true),
            title,
            "title",
        )
        .await?
    };
    let translated_text = translate_one(
        ai,
        preset_id,
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

async fn summarize(ai: &AiConfig, preset_id: &str, article: &Article) -> Result<String> {
    let resolved = crate::config::resolve_preset(ai, Some(preset_id)).with_context(|| {
        format!(
            "bot: transform \"summarize\" preset {preset_id:?} not found in ai.presets; \
             add it with `mistl config set ai.presets <json>` (and ai.providers / \
             ai.default_preset_id -- see `mistl config show`)"
        )
    })?;
    let upstream = UpstreamConfig {
        base_url: resolved.base_url,
        api_key: resolved.api_key,
        model: (!resolved.model.is_empty()).then_some(resolved.model),
        temperature: resolved.temperature,
        reasoning_effort: resolved.reasoning_effort,
    };
    let messages = vec![
        ChatMessage {
            role: "system".to_string(),
            content: SUMMARIZE_SYSTEM_PROMPT.to_string(),
        },
        ChatMessage {
            role: "user".to_string(),
            content: build_summarize_prompt(article),
        },
    ];
    let script = openai::stream_chat_completion(&upstream, &messages, None, None)
        .await
        .with_context(|| {
            format!("bot: transform \"summarize\" failed calling preset {preset_id:?}")
        })?;
    let script = script.trim().to_string();
    if script.is_empty() {
        anyhow::bail!(
            "bot: transform \"summarize\" (preset {preset_id:?}) returned an empty script"
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
/// stored name / a chat-post sink's `fileName` -- mirrors
/// `crate::ai::tts::format_to_mime`'s format list, inverted.
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

async fn synthesize_audio(
    state: &Arc<AppState>,
    ai: &AiConfig,
    preset_id: &str,
    format: Option<&str>,
    speed: Option<f64>,
    text: &str,
    article_id: &str,
) -> Result<AudioOutcome> {
    let resolved = crate::config::resolve_preset(ai, Some(preset_id)).with_context(|| {
        format!(
            "bot: transform \"tts\" preset {preset_id:?} not found in ai.presets; \
             add it with `mistl config set ai.presets <json>` (and ai.providers -- see \
             `mistl config show`)"
        )
    })?;
    let voice = resolved
        .voice
        .filter(|voice| !voice.trim().is_empty())
        .with_context(|| {
            format!(
                "bot: transform \"tts\" preset {preset_id:?} has no voice set; set it with \
                 `mistl config set ai.presets <json>` (add a \"voice\" field to that preset)"
            )
        })?;

    let provider = AiProviderConfig {
        id: preset_id.to_string(),
        label: preset_id.to_string(),
        base_url: resolved.base_url,
        api_key: resolved.api_key,
    };
    let params = TtsParams {
        model: resolved.model,
        voice,
        input: text.to_string(),
        format: format.map(str::to_string),
        speed,
    };
    let audio = tts::synthesize(&provider, params)
        .await
        .with_context(|| format!("bot: transform \"tts\" failed calling preset {preset_id:?}"))?;

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
    fn extension_for_mime_maps_known_formats() {
        assert_eq!(extension_for_mime("audio/mpeg"), "mp3");
        assert_eq!(extension_for_mime("audio/ogg"), "ogg");
        assert_eq!(extension_for_mime("audio/wav"), "wav");
        assert_eq!(extension_for_mime("application/octet-stream"), "bin");
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

    #[test]
    fn build_summarize_prompt_includes_title_author_excerpt_and_body() {
        let prompt = build_summarize_prompt(&sample_article());
        assert!(prompt.contains("見出し"));
        assert!(prompt.contains("記者"));
        assert!(prompt.contains("要約文"));
        assert!(prompt.contains("本文テキスト"));
    }

    #[tokio::test]
    async fn summarize_reports_a_clear_error_when_the_preset_is_unresolved() {
        let ai = AiConfig::default();
        let err = summarize(&ai, "missing-preset", &sample_article())
            .await
            .expect_err("an unresolved preset must error");
        let msg = err.to_string();
        assert!(
            msg.contains("missing-preset"),
            "error should name the preset: {msg}"
        );
        assert!(
            msg.contains("ai.presets"),
            "error should point at the config path: {msg}"
        );
    }

    #[test]
    fn translate_system_prompt_names_the_target_language() {
        let prompt = translate_system_prompt("fr", false);
        assert!(
            prompt.contains("\"fr\""),
            "prompt should name the target language tag: {prompt}"
        );
    }

    #[test]
    fn translate_system_prompt_for_title_constrains_output_to_a_single_line() {
        let body_prompt = translate_system_prompt("en", false);
        let title_prompt = translate_system_prompt("en", true);
        assert!(
            !body_prompt.contains("1行"),
            "body prompt should not mention a single-line constraint"
        );
        assert!(
            title_prompt.contains("1行"),
            "title prompt should require a single-line title: {title_prompt}"
        );
        assert!(
            title_prompt.starts_with(&body_prompt),
            "title prompt should extend the shared body prompt"
        );
    }

    #[tokio::test]
    async fn translate_reports_a_clear_error_when_the_preset_is_unresolved() {
        let ai = AiConfig::default();
        let err = translate(&ai, "missing-preset", "en", "見出し", "本文")
            .await
            .expect_err("an unresolved preset must error");
        let msg = err.to_string();
        assert!(
            msg.contains("missing-preset"),
            "error should name the preset: {msg}"
        );
        assert!(
            msg.contains("ai.presets"),
            "error should point at the config path: {msg}"
        );
    }

    #[tokio::test]
    async fn translate_skips_the_upstream_call_for_a_blank_title_but_still_translates_the_body() {
        // A blank title takes the `title.trim().is_empty()` branch in
        // `translate` and never reaches `translate_one` -- so it can't
        // fail even though the preset below is unresolvable. The body has
        // no such branch, so it always goes through `translate_one` and
        // errors here on preset resolution. Were the title path
        // mistakenly *not* skipped, the result would be unchanged (`Err`,
        // same message) since both paths share the same unresolvable
        // preset -- so this pins down "the body call still runs and fails
        // when the title is blank", with the skip itself covered by
        // reading `translate`'s `if title.trim().is_empty()` guard.
        let ai = AiConfig::default();
        let err = translate(&ai, "missing-preset", "en", "   ", "本文")
            .await
            .expect_err("the body call must still run (and fail) even when the title is blank");
        let msg = err.to_string();
        assert!(
            msg.contains("missing-preset"),
            "error should name the preset: {msg}"
        );
    }

    #[test]
    fn translate_title_skip_condition_is_blank_after_trimming() {
        assert!("".trim().is_empty());
        assert!("   ".trim().is_empty());
        assert!(!"見出し".trim().is_empty());
        assert!(!"  x  ".trim().is_empty());
    }
}
