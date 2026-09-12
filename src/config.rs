use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub identity: IdentityConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub stream: StreamConfig,
    #[serde(default)]
    pub chat_relay: ChatRelayConfig,
    /// Legacy `[mailbox]` section, from before the p2p store-and-forward
    /// mail feature was removed and the tc-chat relay became its own
    /// module. Deserializable (so an old config.toml keeps loading, and its
    /// relay settings survive the rename) but never re-serialized --
    /// `Config::load` folds it into `chat_relay` via `migrate_legacy` on
    /// first read, then `save()` drops the section from disk for good. Its
    /// mail-only fields (`room_id`, `serve_as_bot`) are deliberately not
    /// modelled: serde ignores them, which is exactly the intent.
    #[serde(default, skip_serializing)]
    pub mailbox: Option<LegacyMailboxConfig>,
    #[serde(default)]
    pub ai: AiConfig,
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default)]
    pub update: UpdateConfig,
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    #[serde(default)]
    pub bot: BotConfig,
    #[serde(default)]
    pub tunnel: TunnelConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UpdateConfig {
    /// Periodically check GitHub Releases for a newer version.
    pub auto_check: bool,
    /// When a newer version is found, download + verify it and swap the
    /// on-disk binary in place (takes effect on the next daemon start). This
    /// never force-restarts a running daemon -- the update is staged.
    pub auto_apply: bool,
    /// Hours between background update checks.
    pub check_interval_hours: u64,
    /// GitHub repository (owner/name) releases are published to.
    pub repo: String,
    /// Include pre-releases when checking for updates.
    pub prerelease: bool,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            auto_check: true,
            auto_apply: true,
            check_interval_hours: 6,
            repo: "tik-choco/mistl".into(),
            prerelease: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UiConfig {
    /// Serve the embedded web dashboard from the daemon.
    pub enabled: bool,
    /// Dashboard listen address. The dashboard has no auth beyond a
    /// same-origin check, so keep it on loopback.
    pub listen: String,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: "127.0.0.1:6480".into(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentityConfig {
    /// Display name shown to peers.
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Directory for content-addressed blocks. Defaults to `<data_dir>/blocks`.
    pub blocks_dir: Option<PathBuf>,
    /// Maximum store size in bytes before LRU eviction of remote blocks.
    pub capacity_bytes: u64,
    /// tc-chat rooms the store joins for peer block exchange. Independent of
    /// `[chat_relay] rooms`/`[ai] room_id`/`[stream] room` for the same reason
    /// those are independent of each other -- the p2p
    /// transport supports multiple simultaneous rooms per process, and here
    /// it's taken further: the store joins *all* listed rooms simultaneously
    /// (mistlib supports multiple rooms per process; see `net::ensure_started`).
    /// Empty by default: the store stays purely local (no network join) until
    /// at least one room is configured. Unlike the other room settings, this
    /// one can be changed at any time -- `storage::store` re-resolves the
    /// whole list on every call, joining newly-added rooms and leaving
    /// removed ones, with no daemon restart required. Accepts either a TOML
    /// array (`room_ids = ["a", "b"]`) or, for back-compat, the old single
    /// `room_id = "a"` string form (also aliased under this field name).
    #[serde(alias = "room_id", deserialize_with = "deserialize_room_ids")]
    pub room_ids: Vec<String>,
    /// Destination directory `store.sandbox.export` writes to when no
    /// explicit `output` is given -- the "extract from the sandbox" default,
    /// used by continuous folder syncs (which materialize into a managed
    /// sandbox subdirectory rather than a user-chosen one; see
    /// `folder_sync`) to get files out onto the real filesystem. Unset by
    /// default, in which case export falls back to `<data_dir>/downloads`.
    /// Read live on every `store.sandbox.export` call, like `room_ids` above,
    /// so this can be changed (or cleared, via `config.set` with a null/empty
    /// value) at any time without a daemon restart.
    pub export_dir: Option<PathBuf>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            blocks_dir: None,
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
            room_ids: Vec::new(),
            export_dir: None,
        }
    }
}

/// Accepts either a single room-id string (the pre-multi-room config shape,
/// `room_id = "my-room"`) or a sequence of strings (`room_ids = ["a", "b"]`)
/// and normalizes both to a `Vec<String>`. A blank/whitespace-only single
/// string parses as an empty list, matching the old field's `None` meaning
/// "purely local, no room joined".
fn deserialize_room_ids<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct RoomIdsVisitor;

    impl<'de> serde::de::Visitor<'de> for RoomIdsVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a room id string or an array of room id strings")
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.trim().is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![value.to_string()])
            }
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut rooms = Vec::new();
            while let Some(room) = seq.next_element::<String>()? {
                rooms.push(room);
            }
            Ok(rooms)
        }
    }

    deserializer.deserialize_any(RoomIdsVisitor)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamConfig {
    /// RTSP listen URL served to VRChat video players.
    pub rtsp_url: String,
    /// Capture frame rate.
    pub frame_rate: u32,
    /// Capture audio alongside video.
    pub audio_capture: bool,
    /// Capture backend: "native" (built-in screen capture + OpenH264, no
    /// external tools) or "ffmpeg" (spawns ffmpeg, the pre-v0.2 pipeline).
    pub capture_backend: String,
    /// Native backend only: downscale captured frames wider than this.
    pub max_width: u32,
    /// Room joined by both `stream relay` (to receive a tc-chat screen
    /// share) and `stream share` (to publish this machine's own capture,
    /// see `stream::share`'s module doc) -- unified into one setting since
    /// a relay and a share pointed at different rooms would never see each
    /// other; the common case is one room shared by every participant.
    /// Independent of `[chat_relay] rooms` and `[ai] room_id` -- the p2p
    /// transport supports multiple simultaneous rooms per process, so this
    /// can still name its own room, or reuse one of theirs.
    pub room: Option<String>,
    /// Legacy pre-unification field name for `room` (used by `stream
    /// relay`). Deserializable so old config.toml files keep loading,
    /// merged into `room` by `migrate_legacy` on first read, then dropped
    /// from disk for good by `#[serde(skip_serializing)]`.
    #[serde(skip_serializing)]
    pub relay_room: Option<String>,
    /// Legacy pre-unification field name for `room` (used by `stream
    /// share`). See `relay_room`.
    #[serde(skip_serializing)]
    pub share_room: Option<String>,
    /// Audio codec served over RTSP for relayed shares: "aac" (transcoded
    /// from Opus; what AVPro reliably plays) or "opus" (passthrough).
    pub audio_codec: String,
    /// Cascade distribution for `stream relay`: relay nodes in the room run
    /// a Raft control plane (`crate::consensus`) to elect a leader, which
    /// re-publishes the sharer's tracks into the room so followers relay
    /// from it instead of the sharer directly. When `false` -- or when the
    /// control plane fails to start -- falls back to the pre-cascade
    /// behavior of locking onto the first video track from anyone, with a
    /// WARN log.
    pub cascade: bool,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            rtsp_url: "rtsp://127.0.0.1:8554/stream".into(),
            frame_rate: 30,
            audio_capture: false,
            capture_backend: "native".into(),
            max_width: 1920,
            room: None,
            relay_room: None,
            share_room: None,
            audio_codec: "aac".into(),
            cascade: true,
        }
    }
}

/// The two `[mailbox]` fields that outlived the mailbox module: the tc-chat
/// relay's room list and master switch, now `[chat_relay] rooms`/`enabled`.
/// See `Config::mailbox`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LegacyMailboxConfig {
    pub chat_rooms: Vec<String>,
    pub chat_relay: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ChatRelayConfig {
    /// Master switch for the tc-chat relay described by `rooms`. Kept
    /// separate from `rooms` being non-empty so a configured room list can
    /// be temporarily disabled without clearing it. Requires a daemon
    /// restart to take effect.
    pub enabled: bool,
    /// tc-chat rooms to relay (join server-lessly on the user's behalf,
    /// verify + persist signed `tc-chat:*` wires, and answer other peers'
    /// `tc-chat:history-request` replays) -- see `crate::chat_relay`.
    /// Empty (the default) means the relay never joins anything, regardless
    /// of `enabled`. Joined once at daemon start; requires a daemon restart
    /// to take effect.
    pub rooms: Vec<String>,
}

/// Connection-only settings for one upstream endpoint (the part shared
/// across however many presets point at it): "where to connect". Mirrors
/// the shared LLM config contract's `LlmProviderV1` (see
/// `protocol/docs/data-contracts/docs/llm-config.md`); mistl can't share
/// the web apps' `tc-shared-llm-config-v1` localStorage key directly (no
/// browser storage in a daemon), but `[ai]` adopts the same provider/preset
/// shape so the two stay conceptually interchangeable.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AiProviderConfig {
    pub id: String,
    pub label: String,
    pub base_url: String,
    pub api_key: String,
}

/// A named model configuration referencing a provider by id: "how to call
/// it". Mirrors the shared LLM config contract's `ModelPresetV1`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AiPresetConfig {
    pub id: String,
    pub label: String,
    pub provider_id: String,
    pub model: String,
    pub temperature: Option<f64>,
    pub reasoning_effort: Option<String>,
    /// Upstream TTS voice id (e.g. "alloy"), used by the bot pipeline's `tts`
    /// transform (`crate::ai::tts::synthesize`) when a preset is resolved for
    /// speech. Meaningless for a chat-completion preset; left unset there.
    #[serde(default)]
    pub voice: Option<String>,
    /// Per-language TTS voice overrides (mistllm-wire tts-lang-hint-v1):
    /// key is a BCP-47 *primary* language subtag (e.g. `"en"`, `"ja"` --
    /// not a full tag like `"en-US"`), matched case-insensitively; value is
    /// a real upstream voice id, exactly like [`AiPresetConfig::voice`].
    /// Consulted by `crate::ai::resolve_tts_voice` when an inbound
    /// `tts_request` carries a `lang` hint and doesn't itself specify
    /// `voice` (an explicit request `voice` always wins over `lang`).
    /// Empty (the default -- absent from an old config.toml deserializes
    /// the same way) means "no per-language overrides configured"; `voice`
    /// above remains the language-agnostic fallback either way. Meaningless
    /// for a chat-completion preset, same as `voice`. `skip_serializing_if`
    /// keeps an unconfigured (empty) map from cluttering every preset's
    /// serialized `config.toml` entry with an empty `[..lang_voices]` table.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub lang_voices: HashMap<String, String>,
    /// What this preset is for: `"chat"` | `"tts"` | `"stt"`. Purely a UI
    /// categorization hint (which single checkbox a preset gets in the
    /// dashboard's AI Network "what to provide" checklist, and which config
    /// path -- `ai.advertised_models` / `ai.tts_preset_id` / `ai.stt_preset_id`
    /// -- toggling it writes to); has no effect on request routing, which
    /// is always driven by those three fields directly, not by this one.
    /// `""` (e.g. an old config.toml predating this field) is treated the
    /// same as `"chat"`, mirroring the `default_preset_id`-style "empty
    /// string = fall back to the default" convention used elsewhere in this
    /// struct's sibling configs.
    #[serde(default)]
    pub kind: String,
}

/// Normalizes a base URL for provider-equality comparisons during legacy
/// migration (trim, then strip trailing `/`s). Mirrors the web contract's
/// `normalizeBaseUrl` (`protocol/docs/data-contracts/reference/llmConfig.ts`).
fn normalize_base_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

/// Finds an unused provider id starting from `base`: `base` itself if free,
/// else `base-2`, `base-3`, ... Used by legacy migration so it never
/// clobbers an existing provider that happens to already use the id it
/// wants for the newly-migrated one.
fn unique_provider_id(providers: &[AiProviderConfig], base: &str) -> String {
    if providers.iter().all(|p| p.id != base) {
        return base.to_string();
    }
    let mut n = 2;
    loop {
        let candidate = format!("{base}-{n}");
        if providers.iter().all(|p| p.id != candidate) {
            return candidate;
        }
        n += 1;
    }
}

/// A preset resolved against its provider: everything needed to build an
/// upstream client, in one place. Returned by [`resolve_preset`].
#[derive(Debug, Clone)]
pub struct ResolvedAiPreset {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub temperature: Option<f64>,
    pub reasoning_effort: Option<String>,
    /// See [`AiPresetConfig::voice`].
    pub voice: Option<String>,
    /// See [`AiPresetConfig::lang_voices`].
    pub lang_voices: HashMap<String, String>,
}

/// Mirrors the shared LLM config contract's `resolvePreset(config,
/// presetId?)`: resolves `preset_id` if it names a known preset, else falls
/// back to `ai.default_preset_id`; a blank/unknown `preset_id` also falls
/// back. Returns `None` if no preset could be resolved this way, or if the
/// resolved preset's `provider_id` doesn't match any configured provider.
pub fn resolve_preset(ai: &AiConfig, preset_id: Option<&str>) -> Option<ResolvedAiPreset> {
    let wanted = preset_id.map(str::trim).filter(|id| !id.is_empty());
    let preset = wanted
        .and_then(|id| ai.presets.iter().find(|p| p.id == id))
        .or_else(|| ai.presets.iter().find(|p| p.id == ai.default_preset_id))?;
    let provider = ai.providers.iter().find(|p| p.id == preset.provider_id)?;
    Some(ResolvedAiPreset {
        base_url: provider.base_url.clone(),
        api_key: provider.api_key.clone(),
        model: preset.model.clone(),
        temperature: preset.temperature,
        reasoning_effort: preset.reasoning_effort.clone(),
        voice: preset.voice.clone(),
        lang_voices: preset.lang_voices.clone(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiConfig {
    /// Room id for the AI network. Falls back to the shared default
    /// rendezvous room (`net::DEFAULT_ROOM`) when unset; the p2p transport
    /// supports multiple simultaneous rooms per process, so set this
    /// explicitly to join an existing mistai (tc-mistllm etc.) room.
    pub room_id: Option<String>,
    /// Legacy pre-provider/preset upstream URL. Deserializable (so old
    /// config.toml files keep loading) but never re-serialized --
    /// `Config::load` merges it into `providers`/`presets` via
    /// `migrate_legacy` on first read, then `save()` drops it from disk
    /// for good.
    #[serde(skip_serializing)]
    pub upstream_url: Option<String>,
    /// Legacy pre-provider/preset API key. See `upstream_url`.
    #[serde(skip_serializing)]
    pub upstream_api_key: Option<String>,
    /// Legacy pre-provider/preset default model. See `upstream_url`.
    #[serde(skip_serializing)]
    pub default_model: Option<String>,
    /// Legacy pre-provider/preset temperature. See `upstream_url`.
    #[serde(skip_serializing)]
    pub temperature: Option<f64>,
    /// Which `presets` entries (by **preset id**, not raw upstream model
    /// id) are advertised in `provider_hello.models` when this node
    /// provides -- mistllm-wire's "advertised name = preset label"
    /// contract: each selected preset's advertised name is its
    /// `label.trim()` when non-empty, else its `model`
    /// (`crate::ai::build_provider`'s `resolve_advertised_models`), and an
    /// inbound `llm_request.model` naming one of those advertised names is
    /// routed to *that preset's own* upstream (base_url/api_key/model),
    /// not forwarded to the default preset's upstream verbatim (see
    /// `crate::ai::provider::Provider::resolve_llm_call`).
    ///
    /// Empty means the legacy pass-through mode: `provider_hello` omits
    /// `models` entirely (no upstream `GET /models` fallback -- removed),
    /// and any inbound `model` is forwarded to the default preset's
    /// upstream as-is, unchecked.
    ///
    /// Old config.toml files (or direct `mistl config set
    /// ai.advertised_models` calls) may still contain raw model ids from
    /// before this field meant "preset id"; `Config::load` normalizes them
    /// via `migrate_advertised_models` on first read (id already valid ->
    /// unchanged; matches some preset's `model` -> rewritten to that
    /// preset's id; matches neither -> dropped).
    pub advertised_models: Vec<String>,
    /// Listen address of the local OpenAI-compatible API server (`ai serve`).
    pub api_listen: String,
    /// Inactivity timeout for a p2p LLM request (resets on every streamed chunk).
    pub request_timeout_secs: u64,
    /// Which `presets` entry `ai provide`/`ai serve` resolve to by default
    /// (see `resolve_preset`). "" = unset. Mirrors the shared LLM config
    /// contract's `defaultPresetId`.
    pub default_preset_id: String,
    /// Which `presets` entry answers inbound `tts_request`s when providing
    /// to the network (see `resolve_preset`). "" = TTS not offered --
    /// `provider_hello.services` omits `"tts"` and voice requests get an
    /// immediate `voice_error` (see `crate::ai::provider`). Unlike
    /// `default_preset_id`, an empty value here is never defaulted away by
    /// `resolve_preset` -- callers must check for "" themselves before
    /// resolving, since falling back to the chat default preset would
    /// silently opt a node into serving TTS it never configured.
    pub tts_preset_id: String,
    /// Same as `tts_preset_id`, for inbound `stt_request`s.
    pub stt_preset_id: String,
    /// Named upstream connections ("where to connect"). See
    /// [`AiProviderConfig`].
    pub providers: Vec<AiProviderConfig>,
    /// Named model configurations, each referencing a `providers` entry by
    /// id ("how to call it"). See [`AiPresetConfig`].
    pub presets: Vec<AiPresetConfig>,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            room_id: None,
            upstream_url: None,
            upstream_api_key: None,
            default_model: None,
            advertised_models: Vec::new(),
            temperature: None,
            api_listen: "127.0.0.1:6478".into(), // 6478 = "MIST" on a phone keypad
            request_timeout_secs: 120,
            default_preset_id: String::new(),
            tts_preset_id: String::new(),
            stt_preset_id: String::new(),
            providers: Vec::new(),
            presets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerConfig {
    /// Master switch for the background tick loop that fires due jobs.
    /// Manual `sched.*` commands (including "run now") work regardless --
    /// this only gates the automatic scheduling.
    pub enabled: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Bot pipeline engine: source -> transform(s) -> sink(s) automation runs
/// (see `crate::bot` and `tc-docs/drafts/bot-pipeline-v1.md`). Mirrors
/// `[scheduler]`'s shape -- a master switch plus a list of user-defined
/// entries -- but the entries themselves (`pipelines`) are read live on
/// every tick (see `applies_when`, and `crate::bot::spawn_background`'s doc
/// comment) rather than only at daemon start.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    /// Master switch for the background tick loop that fires due pipelines.
    /// Manual `bot.run` still works regardless -- only the automatic
    /// scheduling is gated. Mirrors `SchedulerConfig::enabled` exactly,
    /// including needing a daemon restart to take effect (see
    /// `crate::bot::spawn_background`).
    pub enabled: bool,
    pub pipelines: Vec<PipelineConfig>,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            pipelines: Vec::new(),
        }
    }
}

/// One bot pipeline definition. Field order matters for the `toml` crate's
/// serializer: scalars (`id`/`enabled`/`schedule`) must precede table-typed
/// fields (`source`, then the array-of-tables `transforms`/`sinks`) or
/// `toml::to_string_pretty` errors ("values must be emitted before
/// tables").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Schedule expression, same grammar as `[[sched]]` jobs -- parsed by
    /// `crate::scheduler::schedule::parse`.
    pub schedule: String,
    pub source: SourceConfig,
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
}

fn default_true() -> bool {
    true
}

/// Where a pipeline reads new content from. An internally-tagged enum
/// (`kind` field) -- verified to round-trip through the `toml` crate the
/// same as a hand-written flat struct would (see `config::tests::
/// bot_pipeline_config_round_trips_through_toml`), so this keeps the config
/// schema self-documenting without a `kind: String` + a pile of
/// all-optional fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SourceConfig {
    /// Subscribes to a `tc-news`-compatible signed article room (see
    /// `crate::bot::source`). `rooms` defaults to the well-known
    /// `tc-global-articles` room when empty.
    GlobalArticles {
        #[serde(default)]
        rooms: Vec<String>,
        /// `NewsArticle.lang` allowlist; empty means every language.
        #[serde(default)]
        langs: Vec<String>,
    },
    /// Subscribes to a tc-chat room's signed `tc-chat:post` text posts (see
    /// `crate::bot::source`). Posts published by this bot's own DID are
    /// skipped, so a pipeline can read from and write to the same room
    /// without feeding on itself.
    ChatRoom {
        #[serde(default)]
        room: String,
    },
}

/// One step of a pipeline's transform chain, applied in list order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TransformConfig {
    /// LLM summarization into a read-aloud script (`crate::ai::openai`,
    /// resolved via `[[ai.presets]]`). Left blank (rather than `Option`, to
    /// match `AiProviderConfig`/`AiPresetConfig`'s own style) is a runtime
    /// validation warning, not a parse error -- see `crate::bot`'s
    /// `validate_pipeline`.
    Summarize {
        #[serde(default)]
        preset_id: String,
    },
    /// Direct-upstream TTS (`crate::ai::tts::synthesize`). `preset_id`
    /// resolves the model + `AiPresetConfig::voice`; `format`/`speed`
    /// override `TtsParams` per-pipeline.
    Tts {
        #[serde(default)]
        preset_id: String,
        #[serde(default)]
        format: Option<String>,
        #[serde(default)]
        speed: Option<f64>,
    },
    /// LLM translation of the item's title + body into `target_lang`
    /// (`crate::ai::openai`, resolved via `[[ai.presets]]` like
    /// `Summarize`). `target_lang` is a BCP-47-ish language tag (`"ja"`,
    /// `"en"`, ...); empty is a runtime validation warning.
    Translate {
        #[serde(default)]
        preset_id: String,
        #[serde(default)]
        target_lang: String,
    },
}

/// One delivery target for a pipeline's output, run independently of the
/// others -- a sink failure never blocks its siblings (see `crate::bot`'s
/// module doc for the fatal-vs-non-fatal error split).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SinkConfig {
    /// Publishes a `tc-chat:post` (text, then media) into a tc-chat room.
    ChatPost {
        #[serde(default)]
        room: String,
    },
    /// `POST`s a signed JSON delivery notice to an arbitrary HTTP endpoint.
    ///
    /// By default (`body_template` unset/blank) the body is the legacy
    /// `tc-bot:delivery` envelope, signed via `crate::wiresign::sign_wire`
    /// (unless `sign` is `false`) -- see `WIRE_WEBHOOK_DELIVERY_WIRE` in
    /// `src/wiresign.rs`, which pins that default payload's exact bytes when
    /// every field below is left at its default. When `body_template` is set
    /// it is rendered (`{{var}}` substitution -- see
    /// `bot::sink::render_webhook_template`) and sent raw instead; templated
    /// bodies are never wiresigned, and `include_audio`/`max_audio_bytes`
    /// (which only inline base64 audio into the *default* body's
    /// `item.audio.b64`) are ignored in that mode.
    Webhook {
        #[serde(default)]
        url: String,
        #[serde(default)]
        include_audio: bool,
        #[serde(default)]
        max_audio_bytes: Option<u64>,
        /// HTTP method: `"POST"` (default)|`"PUT"`|`"PATCH"`, case-insensitive.
        /// Anything else is treated as `POST` with a validation warning.
        #[serde(default)]
        method: Option<String>,
        /// Raw body template (`{{var}}` substitution); `None`/blank means
        /// "use the legacy default signed JSON body" -- see the variant doc.
        #[serde(default)]
        body_template: Option<String>,
        /// Whether the default body is wiresigned (`X-Mistl-Signature`).
        /// Only meaningful when `body_template` is unset -- templated bodies
        /// are never signed regardless of this flag.
        #[serde(default = "default_true")]
        sign: bool,
        /// When `true`, the default body's `item` gains a `"body"` field
        /// (`article.body`). Ignored in template mode (use `{{body}}`
        /// there instead).
        #[serde(default)]
        include_body: bool,
        /// Custom headers, applied after the default headers so a custom
        /// `Content-Type` overrides `application/json`. Must be last: the
        /// `toml` serializer requires scalar fields before array-of-tables
        /// fields within a variant.
        #[serde(default)]
        headers: Vec<WebhookHeader>,
    },
    /// Publishes the item as a signed `tc-news:article` wire (body stored by
    /// CID) into a global-articles-compatible room -- the producing mirror
    /// of `SourceConfig::GlobalArticles`, letting a pipeline feed tc-news
    /// readers (e.g. translate-and-republish, chat digest -> news).
    ArticlePublish {
        #[serde(default)]
        room: String,
    },
}

/// A single custom HTTP header for `SinkConfig::Webhook`. Applied after the
/// sink's default headers, so a custom `Content-Type` (or any other) wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookHeader {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub value: String,
}

/// P2P WebRTC tunnel: TCP/UDP port forwarding and stdio bridging over a
/// mistlib room, ported from the standalone `p2p` tool (see `crate::tunnel`).
/// Mirrors `[scheduler]`/`[bot]`'s "master switch plus the rest is read live
/// on demand" shape: `enabled` only gates whether the daemon auto-joins at
/// startup (`crate::tunnel::spawn_background`) -- `mistl tunnel start` (and
/// every other `tunnel.*` command) works regardless, exactly like manual
/// `sched.*`/`bot.run` calls work regardless of their own `enabled` flags.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TunnelConfig {
    /// Master switch: when `true`, `crate::tunnel::spawn_background` joins
    /// `room_id` (generating and persisting one first if blank) at daemon
    /// startup and restores any forwards saved under
    /// `<data_dir>/tunnel/forward-store.json`. `false` (the default) means
    /// nothing auto-joins; `mistl tunnel start`/the dashboard's tunnel panel
    /// still start the session manually regardless of this flag.
    pub enabled: bool,
    /// mistlib room id the tunnel session joins. Blank (the default) means
    /// "generate one on first start (`mistl tunnel start`/`tunnel room`) and
    /// persist it here" -- see `crate::tunnel`'s `tunnel.room.set` handler.
    pub room_id: String,
    /// Auto-approve every inbound connection authorization request (the
    /// per-connection "a peer wants to reach target X through this node"
    /// prompt raised by an active `serve` forward -- see `crate::tunnel::auth`).
    /// `false` (the default) means an unknown peer is denied unless
    /// `allow_peers` names it or a prior trust decision already allows it.
    pub auto_accept: bool,
    /// Peer node ids auto-approved for connection authorization regardless
    /// of `auto_accept` -- mirrors the upstream `p2p serve --allow-peer` flag.
    pub allow_peers: Vec<String>,
    /// Whether this node accepts an incoming stdio-bridge session (remote
    /// command execution over the tunnel) at all. Defaults to **false**:
    /// unlike TCP/UDP forwarding (which only ever reaches addresses this
    /// node's own forward config explicitly names), a stdio session runs an
    /// arbitrary local command (`stdio_command`) on a remote peer's behalf,
    /// so it stays opt-in even when `auto_accept`/`allow_peers` would
    /// otherwise let that peer's connection through.
    pub stdio_enabled: bool,
    /// Command (argv; first element is the executable) run for an accepted
    /// stdio session. Empty by default; meaningless while `stdio_enabled` is
    /// `false`.
    pub stdio_command: Vec<String>,
}

impl Default for TunnelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            room_id: String::new(),
            auto_accept: false,
            allow_peers: Vec::new(),
            stdio_enabled: false,
            stdio_command: Vec::new(),
        }
    }
}

fn project_dirs() -> Result<directories::ProjectDirs> {
    directories::ProjectDirs::from("com", "tik-choco", "mistl")
        .context("could not determine home directory")
}

/// Per-user data directory (daemon state, keys, blocks, relay wirelogs).
pub fn data_dir() -> Result<PathBuf> {
    let dirs = project_dirs()?;
    let dir = dirs.data_dir().to_path_buf();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Per-user config directory.
pub fn config_dir() -> Result<PathBuf> {
    let dirs = project_dirs()?;
    let dir = dirs.config_dir().to_path_buf();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn config_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("config.toml"))
}

/// `set_by_path("ai.providers", ...)` support: `config.show` masks every
/// non-empty provider `api_key` to `"***"` before handing the config to a
/// client, so a client that reads-then-writes the whole `ai.providers`
/// array back unmodified would otherwise clobber real keys with the
/// literal string `"***"`. Substitutes each masked entry's `api_key` with
/// the current config's real value for the provider sharing that `id`.
/// Errors if a masked entry's `id` doesn't match any currently configured
/// provider -- there would be nothing to substitute from.
fn substitute_masked_provider_keys(
    config: &Config,
    mut value: serde_json::Value,
) -> Result<serde_json::Value> {
    let Some(entries) = value.as_array_mut() else {
        return Ok(value);
    };
    for entry in entries.iter_mut() {
        let masked = entry.get("api_key").and_then(serde_json::Value::as_str) == Some("***");
        if !masked {
            continue;
        }
        let id = entry
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let real = config
            .ai
            .providers
            .iter()
            .find(|p| p.id == id)
            .with_context(|| {
                format!("ai.providers: masked api_key for unknown provider id {id:?}")
            })?;
        entry["api_key"] = serde_json::Value::String(real.api_key.clone());
    }
    Ok(value)
}

/// Set one config field addressed as `section.field` (e.g.
/// "ai.default_preset_id"), returning the updated config. Values round-trip
/// through serde so types are validated against the real Config shape;
/// `null` clears optional fields.
pub fn set_by_path(config: &Config, path: &str, value: serde_json::Value) -> Result<Config> {
    // `ai.providers` gets special handling for masked api_keys before the
    // generic "***" rejection below -- see `substitute_masked_provider_keys`.
    let value = if path == "ai.providers" {
        substitute_masked_provider_keys(config, value)?
    } else {
        value
    };
    if value == serde_json::Value::String("***".into()) {
        anyhow::bail!("refusing to store the masked placeholder \"***\" (re-enter the real value)");
    }
    // Legacy alias: `storage.room_id` used to be the field name (a single
    // `Option<String>`). Rewrite both the path and the value shape so
    // `mistl config set storage.room_id foo` keeps working against the new
    // `storage.room_ids: Vec<String>` field -- a non-empty string becomes a
    // one-element array, null/empty becomes an empty array.
    let (path, value) = if path == "storage.room_id" {
        let rooms = match value.as_str() {
            Some(s) if !s.trim().is_empty() => serde_json::json!([s]),
            _ => serde_json::json!([]),
        };
        ("storage.room_ids", rooms)
    } else if path == "stream.relay_room" || path == "stream.share_room" {
        // Legacy alias: `stream.relay_room`/`stream.share_room` used to be
        // two separate fields, unified into `stream.room` since a relay and
        // a share pointed at different rooms would never see each other.
        // Same value shape (`Option<String>`), so just rewrite the path.
        ("stream.room", value)
    } else {
        (path, value)
    };
    let (section, field) = path
        .split_once('.')
        .with_context(|| format!("invalid config path {path:?}; expected \"section.field\""))?;
    if field.is_empty() || field.contains('.') {
        anyhow::bail!("invalid config path {path:?}; expected \"section.field\"");
    }

    let mut tree = serde_json::to_value(config).context("serializing config")?;
    let section_value = tree
        .get_mut(section)
        .with_context(|| format!("unknown config section {section:?}"))?;
    let slot = section_value
        .get_mut(field)
        .with_context(|| format!("unknown config field {section}.{field}"))?;
    *slot = value;

    serde_json::from_value(tree).with_context(|| format!("invalid value for {section}.{field}"))
}

/// When a change to `path` actually takes effect, shown to users by the CLI
/// and dashboard. Services read config as they start, so most fields apply
/// on the next `*.start`; the p2p room is joined once per daemon lifetime
/// and the dashboard listener binds at daemon startup.
pub fn applies_when(path: &str) -> &'static str {
    match path {
        "ui.enabled" | "ui.listen" => "daemon restart",
        // The background updater re-reads config each tick, but its cadence
        // and enabled state are simplest to reason about across a restart.
        "update.auto_check" | "update.check_interval_hours" => "daemon restart",
        // chat_relay/ai/stream join their rooms once at service start and hold
        // it for the process lifetime; before any p2p service ran it applies
        // on next start, but "daemon restart" is the safe universal answer.
        // `storage.room_ids` (and its legacy alias `storage.room_id`) is the
        // one exception -- `storage::store` re-resolves the whole list on
        // every call and hops rooms live, so it falls through to "next
        // service start" below (true immediately, since the next `store.*`
        // command *is* its next "start").
        "chat_relay.rooms" | "chat_relay.enabled" | "ai.room_id" | "stream.room"
        | "stream.relay_room" | "stream.share_room" => "daemon restart",
        // The background tick loop is only started once at daemon startup
        // (see `scheduler::spawn_background`); toggling it live would need
        // a way to stop an already-running loop, which isn't implemented.
        "scheduler.enabled" => "daemon restart",
        // Same reasoning as `scheduler.enabled` -- `crate::bot`'s tick loop
        // is only started once at daemon startup. `bot.pipelines` is
        // deliberately NOT listed here: the tick loop re-reads it on every
        // tick (see `crate::bot::spawn_background`), so pipeline
        // add/edit/remove takes effect on the very next tick, not a restart.
        "bot.enabled" => "daemon restart",
        // Same reasoning again -- `crate::tunnel::spawn_background` only
        // auto-joins once at daemon startup. `tunnel.room_id`/`auto_accept`/
        // `allow_peers`/`stdio_enabled`/`stdio_command` deliberately fall
        // through to the "next service start" default below: they're read
        // fresh by `tunnel.start`/each new inbound authorization, so a
        // `mistl tunnel start` (or the next connection attempt) already *is*
        // that "next start", no daemon restart required.
        "tunnel.enabled" => "daemon restart",
        // These feed the running AI provider's upstream/model resolution.
        // `daemon::dispatch`'s `config.set` handler reloads it live right
        // after a save (see `ai::reload_provider_if_running`) when one is
        // already running, so -- unlike the "next service start" paths
        // below, which need an explicit stop/start of *something* -- there
        // is nothing left for the user to do at all.
        "ai.providers"
        | "ai.presets"
        | "ai.default_preset_id"
        | "ai.tts_preset_id"
        | "ai.stt_preset_id"
        | "ai.advertised_models" => "applied immediately",
        _ => "next service start",
    }
}

impl Config {
    /// Load config from disk, writing defaults on first run. Runs the
    /// legacy `[ai]` migration (see `migrate_legacy`) and persists it when
    /// it changed anything, so callers always see the current shape.
    pub fn load() -> Result<Self> {
        let path = config_path()?;
        if !path.exists() {
            let config = Self::default();
            config.save()?;
            return Ok(config);
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut config: Self =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        if config.migrate_legacy() {
            config.save()?;
        }
        Ok(config)
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        std::fs::write(&path, toml::to_string_pretty(self)?)
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }

    /// Runs every legacy field migration (`[ai]`'s and `[stream]`'s, below)
    /// and reports whether either changed anything requiring a save.
    pub fn migrate_legacy(&mut self) -> bool {
        let ai_changed = self.migrate_legacy_ai();
        // Runs after `migrate_legacy_ai` so a config that predates both the
        // provider/preset split *and* the advertised-name contract (raw
        // `upstream_url`/`default_model` fields only) gets its freshly
        // synthesized "default" preset in place before this tries to match
        // `advertised_models` entries against `presets`.
        let advertised_changed = self.migrate_advertised_models();
        let stream_changed = self.migrate_legacy_stream_room();
        let mailbox_changed = self.migrate_legacy_mailbox();
        ai_changed || advertised_changed || stream_changed || mailbox_changed
    }

    /// Folds a legacy `[mailbox]` section's `chat_rooms`/`chat_relay` into
    /// `[chat_relay] rooms`/`enabled`, so a daemon that was relaying tc-chat
    /// rooms before the rename keeps relaying them after it. Only applies to
    /// an untouched `[chat_relay]` section: once the new section says
    /// anything at all, it wins outright rather than being merged field by
    /// field. Returns `true` whenever a legacy section was present, even if
    /// nothing moved -- saving is what drops it from disk for good, the same
    /// contract as `migrate_legacy_ai`/`migrate_legacy_stream_room`.
    fn migrate_legacy_mailbox(&mut self) -> bool {
        let Some(legacy) = self.mailbox.take() else {
            return false;
        };
        if self.chat_relay.rooms.is_empty() && !self.chat_relay.enabled {
            self.chat_relay.rooms = legacy.chat_rooms;
            self.chat_relay.enabled = legacy.chat_relay;
        }
        true
    }

    /// Merges legacy `[ai]` fields (`upstream_url`/`upstream_api_key`/
    /// `default_model`/`temperature`) into the `providers`/`presets` shape,
    /// mirroring the shared LLM config contract's migration rule
    /// (merge-never-delete, idempotent -- see llm-config.md's
    /// "マイグレーション規則"). Never overwrites an existing provider,
    /// preset, or a non-empty `default_preset_id`. A no-op on a config that
    /// never had `upstream_url` set, including one already migrated and
    /// saved -- `save()` drops the legacy fields from disk for good via
    /// `#[serde(skip_serializing)]`, so a subsequent `load()` sees no
    /// legacy `upstream_url` to migrate.
    ///
    /// Returns whether the caller should persist: `true` whenever a
    /// non-blank legacy `upstream_url` was present, even if the
    /// provider/preset it maps to already existed -- persisting is still
    /// needed to drop the now-redundant legacy fields from disk.
    fn migrate_legacy_ai(&mut self) -> bool {
        let Some(upstream_url) = self
            .ai
            .upstream_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return false;
        };

        let normalized = normalize_base_url(upstream_url);
        let legacy_key = self.ai.upstream_api_key.clone().unwrap_or_default();
        let provider_id = match self
            .ai
            .providers
            .iter()
            .find(|p| normalize_base_url(&p.base_url) == normalized && p.api_key == legacy_key)
        {
            Some(existing) => existing.id.clone(),
            None => {
                let id = unique_provider_id(&self.ai.providers, "default");
                self.ai.providers.push(AiProviderConfig {
                    id: id.clone(),
                    label: "Default".to_string(),
                    base_url: upstream_url.to_string(),
                    api_key: legacy_key,
                });
                id
            }
        };

        if !self.ai.presets.iter().any(|p| p.id == "default") {
            self.ai.presets.push(AiPresetConfig {
                id: "default".to_string(),
                label: "Default".to_string(),
                provider_id,
                model: self.ai.default_model.clone().unwrap_or_default(),
                temperature: self.ai.temperature,
                reasoning_effort: None,
                voice: None,
                lang_voices: HashMap::new(),
                kind: "chat".to_string(),
            });
        }

        if self.ai.default_preset_id.trim().is_empty() {
            self.ai.default_preset_id = "default".to_string();
        }

        true
    }

    /// Normalizes `ai.advertised_models` from a flat list of raw upstream
    /// model ids (its pre-`advertised-name-contract` shape) into a list of
    /// **preset ids**, per `AiConfig::advertised_models`'s current doc
    /// comment. Runs on every `Config::load`, not just once on an old
    /// config -- unlike `migrate_legacy_ai`/`migrate_legacy_stream_room`
    /// this isn't a one-shot "drop the legacy field for good" migration
    /// (there is no separate legacy field here to retire; `advertised_models`
    /// keeps its name and TOML key), it just keeps re-normalizing the same
    /// field, which is why it must stay idempotent.
    ///
    /// For each entry, checked against the *current* `presets` (including
    /// any preset `migrate_legacy_ai` just synthesized):
    /// - Already names a known preset id -> left as-is.
    /// - Doesn't name a preset id, but matches some preset's `model` ->
    ///   rewritten to that preset's id (first configured match wins when
    ///   more than one preset shares the same `model`), with a `warn!`.
    /// - Matches neither -> dropped, with a `warn!`.
    ///
    /// Returns whether anything changed (and thus whether the caller should
    /// persist). Idempotent: a config whose `advertised_models` already only
    /// contains valid preset ids returns `false`.
    fn migrate_advertised_models(&mut self) -> bool {
        if self.ai.advertised_models.is_empty() {
            return false;
        }
        // Cloned up front so the lookups below don't borrow `self.ai`
        // immutably while `self.ai.advertised_models` is rebuilt.
        let presets = self.ai.presets.clone();
        let mut changed = false;
        let mut next = Vec::with_capacity(self.ai.advertised_models.len());
        for entry in &self.ai.advertised_models {
            if presets.iter().any(|p| &p.id == entry) {
                next.push(entry.clone());
            } else if let Some(preset) = presets.iter().find(|p| &p.model == entry) {
                warn!(
                    old = %entry,
                    new = %preset.id,
                    "ai: advertised_models entry names a raw upstream model id, not a preset id; \
                     rewriting to the matching preset's id"
                );
                next.push(preset.id.clone());
                changed = true;
            } else {
                warn!(
                    entry = %entry,
                    "ai: advertised_models entry doesn't match any configured preset id or model; dropping it"
                );
                changed = true;
            }
        }
        if changed {
            self.ai.advertised_models = next;
        }
        changed
    }

    /// Merges legacy `[stream]` fields `relay_room`/`share_room` into the
    /// unified `room` field: never overwrites an existing `room`, and
    /// prefers `relay_room` over `share_room` when both are set (an
    /// arbitrary but stable tie-break for the rare config that had them
    /// pointed at different rooms). See `migrate_legacy_ai`'s doc for why
    /// this returns `true` (and thus triggers a re-save) whenever either
    /// legacy field was non-blank, even if `room` was already set: saving
    /// is what drops the now-redundant legacy fields from disk for good.
    fn migrate_legacy_stream_room(&mut self) -> bool {
        let relay_room = self
            .stream
            .relay_room
            .take()
            .filter(|s| !s.trim().is_empty());
        let share_room = self
            .stream
            .share_room
            .take()
            .filter(|s| !s.trim().is_empty());
        let Some(legacy_room) = relay_room.or(share_room) else {
            return false;
        };

        if self.stream.room.is_none() {
            self.stream.room = Some(legacy_room);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_by_path_legacy_stream_relay_room_and_share_room_alias_to_room() {
        let config = Config::default();
        let updated = set_by_path(&config, "stream.relay_room", json!("my-room")).unwrap();
        assert_eq!(updated.stream.room, Some("my-room".to_string()));

        let updated = set_by_path(&config, "stream.share_room", json!("my-room")).unwrap();
        assert_eq!(updated.stream.room, Some("my-room".to_string()));
    }

    #[test]
    fn set_by_path_rejects_wrong_type_and_unknown_fields() {
        let config = Config::default();
        assert!(set_by_path(&config, "stream.frame_rate", json!("fast")).is_err());
        assert!(set_by_path(&config, "stream.nope", json!(1)).is_err());
        assert!(set_by_path(&config, "nope.field", json!(1)).is_err());
        assert!(set_by_path(&config, "noseparator", json!(1)).is_err());
        assert!(set_by_path(&config, "ai.upstream_api_key", json!("***")).is_err());
    }

    /// The real risk the six deleted per-field `set_by_path` tests only
    /// half-covered: a struct field rename silently breaking a path string
    /// the dashboard or CLI still sends, since `set_by_path` is generic
    /// serde-reflection over `Config` with no compile-time link to these
    /// path strings. One entry per path actually referenced from
    /// `src/web/assets/index.html`'s `SETTINGS_SCHEMA`/AI "what to
    /// provide"/bot-pipeline editor, `src/cli.rs`, and the `applies_when`
    /// match arms below -- at least one path per `Config` section (except
    /// the legacy, never-written `[mailbox]` section).
    #[test]
    fn set_by_path_accepts_every_path_the_cli_and_dashboard_send() {
        let config = Config::default();
        let cases: &[(&str, serde_json::Value)] = &[
            ("identity.display_name", json!("Someone")),
            ("storage.capacity_bytes", json!(1_073_741_824u64)),
            ("storage.room_ids", json!(["room-a"])),
            ("storage.blocks_dir", json!("C:\\blocks")),
            ("storage.export_dir", json!("C:\\exports")),
            ("stream.rtsp_url", json!("rtsp://127.0.0.1:8554/stream")),
            ("stream.frame_rate", json!(30)),
            ("stream.capture_backend", json!("ffmpeg")),
            ("stream.max_width", json!(1920)),
            ("stream.room", json!("room-a")),
            ("stream.audio_codec", json!("opus")),
            ("stream.audio_capture", json!(true)),
            ("stream.cascade", json!(true)),
            ("chat_relay.rooms", json!(["room-a"])),
            ("chat_relay.enabled", json!(true)),
            ("ai.room_id", json!("room-a")),
            ("ai.request_timeout_secs", json!(30)),
            ("ai.api_listen", json!("127.0.0.1:6480")),
            ("ai.default_preset_id", json!("default")),
            ("ai.tts_preset_id", json!("tts-default")),
            ("ai.stt_preset_id", json!("stt-default")),
            ("ai.advertised_models", json!(["a", "b"])),
            ("ai.providers", json!([])),
            ("ai.presets", json!([])),
            ("scheduler.enabled", json!(true)),
            ("bot.enabled", json!(true)),
            ("bot.pipelines", json!([])),
            ("update.auto_check", json!(true)),
            ("update.auto_apply", json!(true)),
            ("update.check_interval_hours", json!(12)),
            ("update.prerelease", json!(true)),
            ("update.repo", json!("tik-choco/mistl")),
            ("ui.listen", json!("127.0.0.1:6480")),
            ("ui.enabled", json!(true)),
            ("tunnel.enabled", json!(true)),
            ("tunnel.room_id", json!("abc12345")),
            ("tunnel.auto_accept", json!(true)),
            ("tunnel.allow_peers", json!(["peer-a"])),
            ("tunnel.stdio_enabled", json!(true)),
            ("tunnel.stdio_command", json!(["bash", "-lc"])),
        ];
        for (path, value) in cases {
            let result = set_by_path(&config, path, value.clone());
            assert!(result.is_ok(), "path {path:?} failed: {:?}", result.err());
        }
    }

    #[test]
    fn migrate_legacy_creates_provider_preset_and_default_from_legacy_fields() {
        let mut config = Config::default();
        config.ai.upstream_url = Some("http://127.0.0.1:11434/v1/".to_string());
        config.ai.upstream_api_key = Some("sk-legacy".to_string());
        config.ai.default_model = Some("llama3".to_string());
        config.ai.temperature = Some(0.5);

        assert!(config.migrate_legacy());

        assert_eq!(config.ai.providers.len(), 1);
        let provider = &config.ai.providers[0];
        assert_eq!(provider.id, "default");
        assert_eq!(provider.label, "Default");
        assert_eq!(provider.base_url, "http://127.0.0.1:11434/v1/");
        assert_eq!(provider.api_key, "sk-legacy");

        assert_eq!(config.ai.presets.len(), 1);
        let preset = &config.ai.presets[0];
        assert_eq!(preset.id, "default");
        assert_eq!(preset.provider_id, "default");
        assert_eq!(preset.model, "llama3");
        assert_eq!(preset.temperature, Some(0.5));
        assert_eq!(preset.reasoning_effort, None);

        assert_eq!(config.ai.default_preset_id, "default");
    }

    #[test]
    fn migrate_legacy_is_idempotent() {
        let mut config = Config::default();
        config.ai.upstream_url = Some("http://127.0.0.1:11434/v1".to_string());
        config.ai.default_model = Some("llama3".to_string());

        assert!(config.migrate_legacy());
        assert!(
            config.migrate_legacy(),
            "legacy fields are still present in memory, so persisting again is still requested"
        );

        assert_eq!(
            config.ai.providers.len(),
            1,
            "provider must not be duplicated"
        );
        assert_eq!(config.ai.presets.len(), 1, "preset must not be duplicated");
        assert_eq!(config.ai.default_preset_id, "default");
    }

    #[test]
    fn migrate_legacy_never_overwrites_an_existing_preset_or_default_preset_id() {
        let mut config = Config::default();
        config.ai.upstream_url = Some("http://127.0.0.1:11434/v1".to_string());
        config.ai.default_model = Some("llama3".to_string());
        config.ai.presets.push(AiPresetConfig {
            id: "default".to_string(),
            label: "Custom".to_string(),
            provider_id: "elsewhere".to_string(),
            model: "custom-model".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        config.ai.default_preset_id = "other".to_string();

        assert!(config.migrate_legacy());

        // The pre-existing "default" preset is untouched...
        assert_eq!(config.ai.presets.len(), 1);
        assert_eq!(config.ai.presets[0].model, "custom-model");
        assert_eq!(config.ai.presets[0].provider_id, "elsewhere");
        // ...and the already-set default_preset_id is left alone.
        assert_eq!(config.ai.default_preset_id, "other");
        // A provider is still merged in -- merge-never-delete applies per
        // entity, independently of the preset already existing.
        assert_eq!(config.ai.providers.len(), 1);
    }

    #[test]
    fn migrate_legacy_is_a_noop_on_a_pristine_config() {
        let mut config = Config::default();
        assert!(!config.migrate_legacy());
        assert!(config.ai.providers.is_empty());
        assert!(config.ai.presets.is_empty());
        assert_eq!(config.ai.default_preset_id, "");
        assert_eq!(config.stream.room, None);
    }

    #[test]
    fn migrate_advertised_models_leaves_valid_preset_ids_untouched() {
        let mut config = Config::default();
        config.ai.presets.push(AiPresetConfig {
            id: "chat-a".to_string(),
            label: "Chat A".to_string(),
            provider_id: "p1".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        config.ai.advertised_models = vec!["chat-a".to_string()];

        assert!(
            !config.migrate_legacy(),
            "already-valid preset ids need no rewrite"
        );
        assert_eq!(config.ai.advertised_models, vec!["chat-a".to_string()]);
    }

    #[test]
    fn migrate_advertised_models_rewrites_a_raw_model_id_to_its_preset_id() {
        let mut config = Config::default();
        config.ai.presets.push(AiPresetConfig {
            id: "chat-a".to_string(),
            label: "Chat A".to_string(),
            provider_id: "p1".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        config.ai.advertised_models = vec!["gpt-4o".to_string()];

        assert!(config.migrate_legacy());
        assert_eq!(config.ai.advertised_models, vec!["chat-a".to_string()]);
        // Idempotent: the second run sees only the already-rewritten id.
        assert!(!config.migrate_legacy());
        assert_eq!(config.ai.advertised_models, vec!["chat-a".to_string()]);
    }

    #[test]
    fn migrate_advertised_models_prefers_the_first_configured_preset_when_several_share_a_model() {
        let mut config = Config::default();
        config.ai.presets.push(AiPresetConfig {
            id: "first".to_string(),
            label: "First".to_string(),
            provider_id: "p1".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        config.ai.presets.push(AiPresetConfig {
            id: "second".to_string(),
            label: "Second".to_string(),
            provider_id: "p2".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        config.ai.advertised_models = vec!["gpt-4o".to_string()];

        assert!(config.migrate_legacy());
        assert_eq!(config.ai.advertised_models, vec!["first".to_string()]);
    }

    #[test]
    fn migrate_advertised_models_drops_an_entry_matching_no_preset() {
        let mut config = Config::default();
        config.ai.presets.push(AiPresetConfig {
            id: "chat-a".to_string(),
            label: "Chat A".to_string(),
            provider_id: "p1".to_string(),
            model: "gpt-4o".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "chat".to_string(),
        });
        config.ai.advertised_models = vec!["chat-a".to_string(), "ghost-model".to_string()];

        assert!(config.migrate_legacy());
        assert_eq!(config.ai.advertised_models, vec!["chat-a".to_string()]);
    }

    #[test]
    fn migrate_advertised_models_is_a_noop_when_empty() {
        let mut config = Config::default();
        assert!(!config.migrate_legacy());
        assert!(config.ai.advertised_models.is_empty());
    }

    #[test]
    fn migrate_legacy_ai_synthesized_default_preset_satisfies_a_matching_advertised_entry() {
        // A config predating both the provider/preset split *and* the
        // advertised-name contract: legacy upstream fields plus a raw model
        // id in advertised_models. migrate_legacy_ai must run first so the
        // "default" preset it synthesizes is available for this rewrite in
        // the very same migrate_legacy() call.
        let mut config = Config::default();
        config.ai.upstream_url = Some("http://127.0.0.1:11434/v1".to_string());
        config.ai.default_model = Some("llama3".to_string());
        config.ai.advertised_models = vec!["llama3".to_string()];

        assert!(config.migrate_legacy());
        assert_eq!(config.ai.advertised_models, vec!["default".to_string()]);
    }

    #[test]
    fn migrate_legacy_merges_stream_relay_room_into_room() {
        let mut config = Config::default();
        config.stream.relay_room = Some("my-room".into());
        assert!(config.migrate_legacy());
        assert_eq!(config.stream.room, Some("my-room".to_string()));
        // Legacy field is cleared once merged.
        assert_eq!(config.stream.relay_room, None);
        // Idempotent: re-running finds nothing left to migrate.
        assert!(!config.migrate_legacy());
    }

    #[test]
    fn migrate_legacy_merges_stream_share_room_into_room_when_relay_room_unset() {
        let mut config = Config::default();
        config.stream.share_room = Some("my-room".into());
        assert!(config.migrate_legacy());
        assert_eq!(config.stream.room, Some("my-room".to_string()));
    }

    #[test]
    fn migrate_legacy_stream_room_prefers_relay_room_over_share_room() {
        let mut config = Config::default();
        config.stream.relay_room = Some("relay-room".into());
        config.stream.share_room = Some("share-room".into());
        assert!(config.migrate_legacy());
        assert_eq!(config.stream.room, Some("relay-room".to_string()));
    }

    #[test]
    fn migrate_legacy_never_overwrites_an_existing_stream_room() {
        let mut config = Config::default();
        config.stream.room = Some("kept".into());
        config.stream.relay_room = Some("legacy".into());
        // Still reports a change so the legacy field gets dropped from disk.
        assert!(config.migrate_legacy());
        assert_eq!(config.stream.room, Some("kept".to_string()));
    }

    #[test]
    fn set_by_path_round_trips_ai_providers_presets_and_default_preset_id() {
        let config = Config::default();
        let updated = set_by_path(
            &config,
            "ai.providers",
            json!([{ "id": "p1", "label": "P1", "base_url": "http://x/v1", "api_key": "k" }]),
        )
        .unwrap();
        assert_eq!(updated.ai.providers.len(), 1);
        assert_eq!(updated.ai.providers[0].id, "p1");

        let updated = set_by_path(
            &updated,
            "ai.presets",
            json!([{ "id": "pr1", "label": "Pr1", "provider_id": "p1", "model": "m1" }]),
        )
        .unwrap();
        assert_eq!(updated.ai.presets.len(), 1);
        assert_eq!(updated.ai.presets[0].provider_id, "p1");

        let updated = set_by_path(&updated, "ai.default_preset_id", json!("pr1")).unwrap();
        assert_eq!(updated.ai.default_preset_id, "pr1");
    }

    #[test]
    fn set_by_path_ai_providers_substitutes_masked_api_key_from_current_config() {
        let mut config = Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "p1".to_string(),
            label: "P1".to_string(),
            base_url: "http://x/v1".to_string(),
            api_key: "real-secret".to_string(),
        });

        let updated = set_by_path(
            &config,
            "ai.providers",
            json!([{ "id": "p1", "label": "P1 renamed", "base_url": "http://x/v1", "api_key": "***" }]),
        )
        .unwrap();

        assert_eq!(updated.ai.providers[0].api_key, "real-secret");
        assert_eq!(updated.ai.providers[0].label, "P1 renamed");
    }

    #[test]
    fn set_by_path_ai_providers_masked_api_key_for_unknown_id_errors() {
        let config = Config::default();
        let err = set_by_path(
            &config,
            "ai.providers",
            json!([{ "id": "ghost", "label": "Ghost", "base_url": "http://x/v1", "api_key": "***" }]),
        )
        .expect_err("masked api_key for an unknown provider id should error");
        assert!(err.to_string().contains("ghost"));
    }

    #[test]
    fn migrate_legacy_mailbox_moves_relay_settings_into_chat_relay() {
        let mut config: Config = toml::from_str(
            "[mailbox]\nroom_id = \"old-room\"\nserve_as_bot = true\nchat_rooms = [\"room-a\"]\nchat_relay = true\n",
        )
        .unwrap();
        assert!(config.migrate_legacy());
        assert_eq!(config.chat_relay.rooms, vec!["room-a"]);
        assert!(config.chat_relay.enabled);
        // Taken, so `save()` writes no `[mailbox]` section at all.
        assert!(config.mailbox.is_none());
        assert!(!toml::to_string_pretty(&config).unwrap().contains("mailbox"));
    }

    #[test]
    fn migrate_legacy_mailbox_never_overrides_a_configured_chat_relay() {
        let mut config: Config = toml::from_str(
            "[mailbox]\nchat_rooms = [\"old-room\"]\nchat_relay = true\n\n[chat_relay]\nrooms = [\"new-room\"]\nenabled = false\n",
        )
        .unwrap();
        assert!(config.migrate_legacy());
        assert_eq!(config.chat_relay.rooms, vec!["new-room"]);
        assert!(!config.chat_relay.enabled);
    }

    #[test]
    fn scheduler_defaults_enabled_and_set_by_path_toggles_it() {
        let config = Config::default();
        assert!(config.scheduler.enabled);
        let updated = set_by_path(&config, "scheduler.enabled", json!(false)).unwrap();
        assert!(!updated.scheduler.enabled);
        // Not covered by `applies_when_answers_the_documented_table` (that
        // table only covers the room-setting/ai-provider paths), so kept
        // here rather than dropped as a duplicate.
        assert_eq!(applies_when("scheduler.enabled"), "daemon restart");
    }

    /// The `applies_when` table has one genuinely non-obvious answer worth
    /// pinning: `storage.room_ids` (and its legacy alias `storage.room_id`)
    /// answers "next service start" while every other room setting answers
    /// "daemon restart", plus the `ai.*` provider-family paths that answer
    /// "applied immediately". Everything else is a straightforward string
    /// match not worth enumerating test-by-test.
    #[test]
    fn applies_when_answers_the_documented_table() {
        // Unlike chat_relay/ai/stream room settings, storage's rooms are
        // re-resolved live on every `store.*` call (see `storage::store`),
        // so changing them should never tell the user to restart the daemon.
        // Both the current field name and its legacy alias answer the same.
        let next_service_start = ["storage.room_ids", "storage.room_id"];
        // Room settings that join once at service start and hold for the
        // process lifetime -- daemon restart is the safe universal answer.
        let daemon_restart = [
            "chat_relay.rooms",
            "chat_relay.enabled",
            "ai.room_id",
            "stream.room",
            "stream.relay_room",
            "stream.share_room",
        ];
        // These feed reload_provider_if_running (ai::mod), unlike
        // ai.room_id above which still needs a restart (joins its room once).
        let applied_immediately = [
            "ai.providers",
            "ai.presets",
            "ai.default_preset_id",
            "ai.tts_preset_id",
            "ai.stt_preset_id",
            "ai.advertised_models",
        ];
        for path in next_service_start {
            assert_eq!(applies_when(path), "next service start", "path: {path}");
        }
        for path in daemon_restart {
            assert_eq!(applies_when(path), "daemon restart", "path: {path}");
        }
        for path in applied_immediately {
            assert_eq!(applies_when(path), "applied immediately", "path: {path}");
        }
    }

    #[test]
    fn storage_room_ids_parses_legacy_single_string_toml() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            room_id = "my-room"
            "#,
        )
        .unwrap();
        assert_eq!(config.storage.room_ids, vec!["my-room".to_string()]);
    }

    #[test]
    fn storage_room_ids_parses_legacy_blank_string_toml_as_empty() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            room_id = "   "
            "#,
        )
        .unwrap();
        assert!(config.storage.room_ids.is_empty());
    }

    #[test]
    fn storage_room_ids_parses_array_toml() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            room_ids = ["a", "b"]
            "#,
        )
        .unwrap();
        assert_eq!(
            config.storage.room_ids,
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn set_by_path_legacy_storage_room_id_string_becomes_one_element_array() {
        let config = Config::default();
        let updated = set_by_path(&config, "storage.room_id", json!("my-room")).unwrap();
        assert_eq!(updated.storage.room_ids, vec!["my-room".to_string()]);
    }

    #[test]
    fn set_by_path_legacy_storage_room_id_null_clears_the_list() {
        let mut config = Config::default();
        config.storage.room_ids = vec!["existing".to_string()];
        let updated = set_by_path(&config, "storage.room_id", serde_json::Value::Null).unwrap();
        assert!(updated.storage.room_ids.is_empty());
    }

    #[test]
    fn set_by_path_legacy_storage_room_id_empty_string_clears_the_list() {
        let mut config = Config::default();
        config.storage.room_ids = vec!["existing".to_string()];
        let updated = set_by_path(&config, "storage.room_id", json!("")).unwrap();
        assert!(updated.storage.room_ids.is_empty());
    }

    #[test]
    fn set_by_path_updates_storage_room_ids_directly() {
        let config = Config::default();
        let updated =
            set_by_path(&config, "storage.room_ids", json!(["room-a", "room-b"])).unwrap();
        assert_eq!(
            updated.storage.room_ids,
            vec!["room-a".to_string(), "room-b".to_string()]
        );
    }

    // -- `[bot]` schema: internally-tagged `kind` enums round-trip through TOML --

    fn sample_pipeline() -> PipelineConfig {
        PipelineConfig {
            id: "news-audio".to_string(),
            enabled: true,
            schedule: "@every 30m".to_string(),
            source: SourceConfig::GlobalArticles {
                rooms: vec!["tc-global-articles".to_string()],
                langs: vec!["ja".to_string()],
            },
            transforms: vec![
                TransformConfig::Summarize {
                    preset_id: "worker".to_string(),
                },
                TransformConfig::Tts {
                    preset_id: "tts-default".to_string(),
                    format: Some("mp3".to_string()),
                    speed: None,
                },
            ],
            sinks: vec![
                SinkConfig::ChatPost {
                    room: "chat-room".to_string(),
                },
                SinkConfig::Webhook {
                    url: "https://example.com/hook".to_string(),
                    include_audio: false,
                    max_audio_bytes: Some(5_242_880),
                    method: None,
                    body_template: None,
                    sign: true,
                    include_body: false,
                    headers: Vec::new(),
                },
            ],
        }
    }

    /// A second pipeline exercising the v2 kinds (`chat-room` source,
    /// `translate` transform, `article-publish` sink) alongside the v1
    /// kinds covered by `sample_pipeline` above -- folded into the same
    /// round-trip test rather than kept as a separate one, since both
    /// pipelines just live in the same `bot.pipelines` array.
    fn sample_pipeline_v2() -> PipelineConfig {
        PipelineConfig {
            id: "chat-digest".to_string(),
            enabled: true,
            schedule: "@every 1h".to_string(),
            source: SourceConfig::ChatRoom {
                room: "team-room".to_string(),
            },
            transforms: vec![TransformConfig::Translate {
                preset_id: "worker".to_string(),
                target_lang: "en".to_string(),
            }],
            sinks: vec![SinkConfig::ArticlePublish {
                room: "tc-global-articles".to_string(),
            }],
        }
    }

    /// Confirms the brief's decision point: does `#[serde(tag = "kind",
    /// rename_all = "kebab-case")]` (an internally-tagged enum) round-trip
    /// through the `toml` crate's serializer/deserializer for
    /// `bot.pipelines`' `source`/`transforms`/`sinks`? If this ever starts
    /// failing (e.g. a `toml` upgrade regresses internally-tagged enum
    /// support), the fallback is a flat struct (`kind: String` + `Option`
    /// fields) with the same TOML surface -- see the module doc note next to
    /// `SourceConfig`/`TransformConfig`/`SinkConfig`.
    #[test]
    fn bot_pipeline_config_round_trips_through_toml() {
        let mut config = Config::default();
        config.bot.pipelines.push(sample_pipeline());
        config.bot.pipelines.push(sample_pipeline_v2());

        let text = toml::to_string_pretty(&config).expect("bot config must serialize to TOML");
        assert!(text.contains(r#"kind = "global-articles""#), "got:\n{text}");
        assert!(text.contains(r#"kind = "summarize""#), "got:\n{text}");
        assert!(text.contains(r#"kind = "tts""#), "got:\n{text}");
        assert!(text.contains(r#"kind = "chat-post""#), "got:\n{text}");
        assert!(text.contains(r#"kind = "webhook""#), "got:\n{text}");
        assert!(text.contains(r#"kind = "chat-room""#), "got:\n{text}");
        assert!(text.contains(r#"kind = "translate""#), "got:\n{text}");
        assert!(text.contains(r#"kind = "article-publish""#), "got:\n{text}");

        let reloaded: Config = toml::from_str(&text).expect("bot config must parse back from TOML");
        assert_eq!(reloaded.bot.pipelines.len(), 2);
        let pipeline = &reloaded.bot.pipelines[0];
        assert_eq!(pipeline.id, "news-audio");
        assert_eq!(pipeline.schedule, "@every 30m");
        match &pipeline.source {
            SourceConfig::GlobalArticles { rooms, langs } => {
                assert_eq!(rooms, &vec!["tc-global-articles".to_string()]);
                assert_eq!(langs, &vec!["ja".to_string()]);
            }
            other => panic!("expected GlobalArticles, got {other:?}"),
        }
        assert_eq!(pipeline.transforms.len(), 2);
        match &pipeline.transforms[0] {
            TransformConfig::Summarize { preset_id } => assert_eq!(preset_id, "worker"),
            other => panic!("expected Summarize, got {other:?}"),
        }
        match &pipeline.transforms[1] {
            TransformConfig::Tts {
                preset_id,
                format,
                speed,
            } => {
                assert_eq!(preset_id, "tts-default");
                assert_eq!(format.as_deref(), Some("mp3"));
                assert_eq!(*speed, None);
            }
            other => panic!("expected Tts, got {other:?}"),
        }
        assert_eq!(pipeline.sinks.len(), 2);
        match &pipeline.sinks[0] {
            SinkConfig::ChatPost { room } => assert_eq!(room, "chat-room"),
            other => panic!("expected ChatPost, got {other:?}"),
        }
        match &pipeline.sinks[1] {
            SinkConfig::Webhook {
                url,
                include_audio,
                max_audio_bytes,
                sign,
                headers,
                ..
            } => {
                assert_eq!(url, "https://example.com/hook");
                assert!(!include_audio);
                assert_eq!(*max_audio_bytes, Some(5_242_880));
                assert!(*sign, "sign must default to true");
                assert!(headers.is_empty());
            }
            other => panic!("expected Webhook, got {other:?}"),
        }

        let pipeline_v2 = &reloaded.bot.pipelines[1];
        assert_eq!(pipeline_v2.id, "chat-digest");
        match &pipeline_v2.source {
            SourceConfig::ChatRoom { room } => assert_eq!(room, "team-room"),
            other => panic!("expected ChatRoom, got {other:?}"),
        }
        match &pipeline_v2.transforms[0] {
            TransformConfig::Translate {
                preset_id,
                target_lang,
            } => {
                assert_eq!(preset_id, "worker");
                assert_eq!(target_lang, "en");
            }
            other => panic!("expected Translate, got {other:?}"),
        }
        match &pipeline_v2.sinks[0] {
            SinkConfig::ArticlePublish { room } => assert_eq!(room, "tc-global-articles"),
            other => panic!("expected ArticlePublish, got {other:?}"),
        }
    }

    /// Confirms the flexible-webhook-sink fields (`method`, `body_template`,
    /// `sign`, `include_body`, `headers`) round-trip through `toml`, and in
    /// particular that `headers` (a `Vec<WebhookHeader>`, i.e. an
    /// array-of-tables) being the *last* field in the variant is required --
    /// the `toml` serializer errors if a table/array-of-tables field
    /// precedes a scalar field within the same struct/variant.
    #[test]
    fn bot_pipeline_webhook_extended_fields_round_trip_through_toml() {
        let mut config = Config::default();
        config.bot.pipelines.push(PipelineConfig {
            id: "webhook-extended".to_string(),
            enabled: true,
            schedule: "@every 1h".to_string(),
            source: SourceConfig::ChatRoom {
                room: "team-room".to_string(),
            },
            transforms: vec![],
            sinks: vec![SinkConfig::Webhook {
                url: "https://example.com/hook".to_string(),
                include_audio: true,
                max_audio_bytes: None,
                method: Some("PUT".to_string()),
                body_template: Some(r#"{"title":"{{title}}"}"#.to_string()),
                sign: false,
                include_body: true,
                headers: vec![
                    WebhookHeader {
                        name: "X-Api-Key".to_string(),
                        value: "secret".to_string(),
                    },
                    WebhookHeader {
                        name: "Content-Type".to_string(),
                        value: "application/custom".to_string(),
                    },
                ],
            }],
        });

        let text = toml::to_string_pretty(&config)
            .expect("extended webhook config must serialize to TOML");
        let reloaded: Config =
            toml::from_str(&text).expect("extended webhook config must parse back from TOML");
        match &reloaded.bot.pipelines[0].sinks[0] {
            SinkConfig::Webhook {
                url,
                method,
                body_template,
                sign,
                include_body,
                headers,
                ..
            } => {
                assert_eq!(url, "https://example.com/hook");
                assert_eq!(method.as_deref(), Some("PUT"));
                assert_eq!(body_template.as_deref(), Some(r#"{"title":"{{title}}"}"#));
                assert!(!sign);
                assert!(include_body);
                assert_eq!(headers.len(), 2);
                assert_eq!(headers[0].name, "X-Api-Key");
                assert_eq!(headers[0].value, "secret");
                assert_eq!(headers[1].name, "Content-Type");
                assert_eq!(headers[1].value, "application/custom");
            }
            other => panic!("expected Webhook, got {other:?}"),
        }
    }

    /// A `SinkConfig::Webhook` with none of the new fields set must still
    /// parse (backward compatibility for existing configs) and default
    /// `sign` to `true` and everything else to empty/`None`/`false`.
    #[test]
    fn bot_pipeline_webhook_legacy_toml_still_parses_with_new_field_defaults() {
        let text = r#"
            [[bot.pipelines]]
            id = "legacy-webhook"
            enabled = true
            schedule = "@every 1h"

            [bot.pipelines.source]
            kind = "chat-room"
            room = "team-room"

            [[bot.pipelines.sinks]]
            kind = "webhook"
            url = "https://example.com/hook"
            include_audio = false
        "#;
        let config: Config = toml::from_str(text).expect("legacy webhook config must still parse");
        match &config.bot.pipelines[0].sinks[0] {
            SinkConfig::Webhook {
                method,
                body_template,
                sign,
                include_body,
                headers,
                ..
            } => {
                assert_eq!(*method, None);
                assert_eq!(*body_template, None);
                assert!(*sign, "sign must default to true for legacy configs");
                assert!(!include_body);
                assert!(headers.is_empty());
            }
            other => panic!("expected Webhook, got {other:?}"),
        }
    }

    #[test]
    fn bot_pipeline_config_matches_the_brief_toml_shape_verbatim() {
        // The brief's `config.toml [bot]` schema, parsed exactly as written
        // (minus the placeholder `<chat room id>` and the commented-out
        // optional fields) -- confirms the schema is usable hand-written,
        // not just round-tripped from a Rust value.
        let text = r#"
            [bot]
            enabled = true

            [[bot.pipelines]]
            id = "news-audio"
            enabled = true
            schedule = "@every 30m"

            [bot.pipelines.source]
            kind = "global-articles"
            rooms = ["tc-global-articles"]
            langs = ["ja"]

            [[bot.pipelines.transforms]]
            kind = "summarize"
            preset_id = "worker"

            [[bot.pipelines.transforms]]
            kind = "tts"
            preset_id = "tts-default"
            format = "mp3"

            [[bot.pipelines.sinks]]
            kind = "chat-post"
            room = "chat-room"

            [[bot.pipelines.sinks]]
            kind = "webhook"
            url = "https://example.com/hook"
            include_audio = false
        "#;
        let config: Config = toml::from_str(text).expect("brief's [bot] schema must parse");
        assert!(config.bot.enabled);
        assert_eq!(config.bot.pipelines.len(), 1);
        assert_eq!(config.bot.pipelines[0].id, "news-audio");
    }

    #[test]
    fn set_by_path_updates_bot_enabled_and_applies_when_requires_a_restart() {
        let config = Config::default();
        let updated = set_by_path(&config, "bot.enabled", json!(false)).unwrap();
        assert!(!updated.bot.enabled);
        assert_eq!(applies_when("bot.enabled"), "daemon restart");
        assert_eq!(applies_when("bot.pipelines"), "next service start");
    }

    #[test]
    fn ai_preset_config_lang_voices_round_trips_through_toml() {
        let mut config = Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "openai".to_string(),
            label: "OpenAI".to_string(),
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "sk-test".to_string(),
        });
        let mut lang_voices = HashMap::new();
        lang_voices.insert("en".to_string(), "af_heart".to_string());
        lang_voices.insert("ja".to_string(), "jf_alpha".to_string());
        config.ai.presets.push(AiPresetConfig {
            id: "tts-default".to_string(),
            label: "TTS".to_string(),
            provider_id: "openai".to_string(),
            model: "tts-1".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: Some("alloy".to_string()),
            lang_voices,
            kind: "tts".to_string(),
        });

        let text = toml::to_string_pretty(&config).unwrap();
        let reloaded: Config = toml::from_str(&text).unwrap();
        assert_eq!(
            reloaded.ai.presets[0]
                .lang_voices
                .get("en")
                .map(String::as_str),
            Some("af_heart")
        );
        assert_eq!(
            reloaded.ai.presets[0]
                .lang_voices
                .get("ja")
                .map(String::as_str),
            Some("jf_alpha")
        );

        let resolved = resolve_preset(&reloaded.ai, Some("tts-default")).unwrap();
        assert_eq!(
            resolved.lang_voices.get("en").map(String::as_str),
            Some("af_heart")
        );
        // `voice` remains the language-agnostic fallback, independent of
        // `lang_voices`.
        assert_eq!(resolved.voice.as_deref(), Some("alloy"));
    }

    #[test]
    fn ai_preset_config_lang_voices_absent_defaults_to_empty() {
        // A hand-written config.toml predating this field (no `lang_voices`
        // key at all in the preset table) must still deserialize:
        // `#[serde(default)]` gives an empty map, not a parse error.
        let text = r#"
            [[ai.presets]]
            id = "tts-default"
            label = "TTS"
            provider_id = "openai"
            model = "tts-1"
            voice = "alloy"
        "#;
        let config: Config = toml::from_str(text).expect("preset without lang_voices must parse");
        assert!(config.ai.presets[0].lang_voices.is_empty());
    }

    #[test]
    fn ai_tts_stt_preset_id_round_trips_and_resolves() {
        let mut config = Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "openai".to_string(),
            label: "OpenAI".to_string(),
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "sk-test".to_string(),
        });
        config.ai.presets.push(AiPresetConfig {
            id: "tts-default".to_string(),
            label: "TTS".to_string(),
            provider_id: "openai".to_string(),
            model: "tts-1".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: Some("alloy".to_string()),
            lang_voices: HashMap::new(),
            kind: "tts".to_string(),
        });
        config.ai.presets.push(AiPresetConfig {
            id: "stt-default".to_string(),
            label: "STT".to_string(),
            provider_id: "openai".to_string(),
            model: "whisper-1".to_string(),
            temperature: None,
            reasoning_effort: None,
            voice: None,
            lang_voices: HashMap::new(),
            kind: "stt".to_string(),
        });
        config.ai.tts_preset_id = "tts-default".to_string();
        config.ai.stt_preset_id = "stt-default".to_string();

        let text = toml::to_string_pretty(&config).unwrap();
        let reloaded: Config = toml::from_str(&text).unwrap();
        assert_eq!(reloaded.ai.tts_preset_id, "tts-default");
        assert_eq!(reloaded.ai.stt_preset_id, "stt-default");

        let tts = resolve_preset(&reloaded.ai, Some(&reloaded.ai.tts_preset_id)).unwrap();
        assert_eq!(tts.model, "tts-1");
        assert_eq!(tts.voice.as_deref(), Some("alloy"));
        let stt = resolve_preset(&reloaded.ai, Some(&reloaded.ai.stt_preset_id)).unwrap();
        assert_eq!(stt.model, "whisper-1");

        // An unset id must not silently fall back to default_preset_id at
        // the config layer -- resolve_preset() itself *would* fall back
        // (see its doc comment), so callers (ai::mod's provide_start) are
        // responsible for checking for "" before ever calling it. This
        // test just documents that default_preset_id and tts/stt_preset_id
        // are independent fields.
        assert_eq!(Config::default().ai.tts_preset_id, "");
        assert_eq!(Config::default().ai.stt_preset_id, "");
    }

    #[test]
    fn tunnel_config_defaults_are_disabled_and_empty() {
        let config = Config::default();
        assert!(!config.tunnel.enabled);
        assert_eq!(config.tunnel.room_id, "");
        assert!(!config.tunnel.auto_accept);
        assert!(config.tunnel.allow_peers.is_empty());
        // Remote command execution must stay opt-in even after `enabled` and
        // `auto_accept` are both turned on for TCP/UDP forwarding.
        assert!(!config.tunnel.stdio_enabled);
        assert!(config.tunnel.stdio_command.is_empty());
    }
}
