use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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
    #[serde(default)]
    pub network: NetworkConfig,
}

/// Peer-authentication policy shared by every room protocol (see
/// `crate::net::peer_auth`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct NetworkConfig {
    /// did:key identities allowed to be "members" for the modules that
    /// consult `peer_auth::admitted` (e.g. consensus). Empty (the default)
    /// means open: any DID-verified peer is admitted. Applied live by
    /// `config.set` and at daemon startup.
    pub membership_allowlist: Vec<String>,
}

impl NetworkConfig {
    /// Validates the allowlist entries (Ed25519 did:key: 56 chars, `z6Mk`
    /// multibase prefix).
    pub fn validate(&self) -> Result<()> {
        for did in &self.membership_allowlist {
            if did.len() != 56 || !did.starts_with("did:key:z6Mk") {
                anyhow::bail!(
                    "network.membership_allowlist: {did:?} is not a valid Ed25519 did:key (expected 56 chars starting with \"did:key:z6Mk\")"
                );
            }
        }
        Ok(())
    }

    /// Pushes the allowlist into the peer-auth layer: empty -> open.
    pub fn apply(&self) {
        crate::net::peer_auth::set_membership_allowlist(if self.membership_allowlist.is_empty() {
            None
        } else {
            Some(self.membership_allowlist.clone())
        });
    }
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
    /// Escape hatch restoring the legacy behaviour of answering every valid
    /// folder-access request (anyone holding a share link) with the folder
    /// passphrase, without owner approval. Off by default: requests from
    /// requesters not on the folder's allowlist wait for approval
    /// (`store.folder-access.approve`). Read live.
    pub folder_auto_grant: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            blocks_dir: None,
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
            room_ids: Vec::new(),
            export_dir: None,
            folder_auto_grant: false,
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

/// HTTP endpoint or Room provider, including discovery cache and per-room sharing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiProviderConfig {
    pub id: String,
    pub label: String,
    pub base_url: String,
    pub api_key: String,
    pub enabled: bool,
    pub models: Vec<String>,
    pub models_fetched_at: Option<String>,
    pub provide: bool,
    pub shared: Vec<ModelRef>,
}

impl Default for AiProviderConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            label: String::new(),
            base_url: String::new(),
            api_key: String::new(),
            enabled: true,
            models: Vec::new(),
            models_fetched_at: None,
            provide: false,
            shared: Vec::new(),
        }
    }
}

impl AiProviderConfig {
    pub fn room(&self) -> Option<&str> {
        self.base_url
            .strip_prefix("mist-network://")
            .filter(|r| !r.trim().is_empty())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRef {
    pub provider_id: String,
    pub model: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct VoiceConfig {
    pub provider_id: String,
    pub model: String,
    pub voice: Option<String>,
    pub lang_voices: HashMap<String, String>,
    pub speed: Option<f64>,
}

impl VoiceConfig {
    pub fn model_ref(&self) -> ModelRef {
        ModelRef {
            provider_id: self.provider_id.clone(),
            model: self.model.clone(),
        }
    }
}

/// Deserialize-only legacy preset, retained solely for Config::load migration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AiPresetConfig {
    pub id: String,
    pub label: String,
    pub provider_id: String,
    pub model: String,
    pub temperature: Option<f64>,
    pub reasoning_effort: Option<String>,
    pub voice: Option<String>,
    pub lang_voices: HashMap<String, String>,
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

/// Connection and model resolved without modifying the stored reference.
#[derive(Debug, Clone)]
pub struct ResolvedModel {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub voice: Option<String>,
    pub lang_voices: HashMap<String, String>,
}

/// Exact resolution for sharing: never borrow another provider or model.
pub fn resolve_ref_exact(ai: &AiConfig, reference: &ModelRef) -> Option<ResolvedModel> {
    let provider = ai
        .providers
        .iter()
        .find(|p| p.id == reference.provider_id && p.enabled)?;
    if reference.model.trim().is_empty() {
        return None;
    }
    Some(ResolvedModel {
        base_url: provider.base_url.clone(),
        api_key: provider.api_key.clone(),
        model: reference.model.clone(),
        reasoning_effort: None,
        voice: None,
        lang_voices: HashMap::new(),
    })
}

/// Tasks may fall back to the configured default when their provider is disabled.
/// Missing references/providers are errors; no first-provider/model guessing.
pub fn effective_ref<'a>(
    ai: &'a AiConfig,
    reference: Option<&'a ModelRef>,
) -> Option<&'a ModelRef> {
    let reference = reference.or(ai.default_ref.as_ref())?;
    let provider = ai
        .providers
        .iter()
        .find(|p| p.id == reference.provider_id)?;
    if !provider.enabled {
        let default = ai.default_ref.as_ref()?;
        resolve_ref_exact(ai, default)?;
        return Some(default);
    }
    Some(reference)
}

pub fn resolve_ref(ai: &AiConfig, reference: Option<&ModelRef>) -> Option<ResolvedModel> {
    let reference = effective_ref(ai, reference)?;
    resolve_ref_exact(ai, reference)
}

pub fn resolve_voice(ai: &AiConfig, voice: Option<&VoiceConfig>) -> Option<ResolvedModel> {
    let voice = voice?;
    let mut resolved = resolve_ref(ai, Some(&voice.model_ref()))?;
    resolved.voice = voice.voice.clone();
    resolved.lang_voices = voice.lang_voices.clone();
    Some(resolved)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiConfig {
    pub default_ref: Option<ModelRef>,
    pub default_reasoning_effort: Option<String>,
    pub tts: Option<VoiceConfig>,
    pub stt: Option<ModelRef>,
    pub providers: Vec<AiProviderConfig>,
    pub api_listen: String,
    pub request_timeout_secs: u64,
    pub trusted_providers: Vec<String>,
    // Deserialize-only fields, consumed by Config::load's migration.
    #[serde(skip_serializing)]
    pub room_id: Option<String>,
    #[serde(skip_serializing)]
    pub upstream_url: Option<String>,
    #[serde(skip_serializing)]
    pub upstream_api_key: Option<String>,
    #[serde(skip_serializing)]
    pub default_model: Option<String>,
    #[serde(skip_serializing)]
    pub temperature: Option<f64>,
    #[serde(skip_serializing)]
    pub advertised_models: Vec<String>,
    #[serde(skip_serializing)]
    pub default_preset_id: String,
    #[serde(skip_serializing)]
    pub tts_preset_id: String,
    #[serde(skip_serializing)]
    pub stt_preset_id: String,
    #[serde(skip_serializing)]
    pub presets: Vec<AiPresetConfig>,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            default_ref: None,
            default_reasoning_effort: None,
            tts: None,
            stt: None,
            providers: Vec::new(),
            api_listen: "127.0.0.1:6478".into(),
            request_timeout_secs: 120,
            trusted_providers: Vec::new(),
            room_id: None,
            upstream_url: None,
            upstream_api_key: None,
            default_model: None,
            temperature: None,
            advertised_models: Vec::new(),
            default_preset_id: String::new(),
            tts_preset_id: String::new(),
            stt_preset_id: String::new(),
            presets: Vec::new(),
        }
    }
}

pub fn valid_reasoning_effort(value: &str) -> bool {
    matches!(
        value,
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
    )
}

pub fn valid_tts_speed(value: f64) -> bool {
    value.is_finite() && (0.25..=4.0).contains(&value)
}

impl AiConfig {
    fn validate(&self) -> Result<()> {
        if self
            .default_reasoning_effort
            .as_deref()
            .is_some_and(|v| !valid_reasoning_effort(v))
        {
            anyhow::bail!(
                "ai.default_reasoning_effort must be none, minimal, low, medium, high, xhigh or max (null clears it)"
            );
        }
        if self
            .tts
            .as_ref()
            .and_then(|v| v.speed)
            .is_some_and(|v| !valid_tts_speed(v))
        {
            anyhow::bail!(
                "ai.tts.speed must be a finite number between 0.25 and 4.0 (null clears it)"
            );
        }
        Ok(())
    }

    fn discard_invalid_options(&mut self) {
        if self
            .default_reasoning_effort
            .as_deref()
            .is_some_and(|v| !valid_reasoning_effort(v))
        {
            tracing::warn!("ai: ignoring invalid ai.default_reasoning_effort");
            self.default_reasoning_effort = None;
        }
        if let Some(tts) = &mut self.tts
            && tts.speed.is_some_and(|v| !valid_tts_speed(v))
        {
            tracing::warn!("ai: ignoring invalid ai.tts.speed");
            tts.speed = None;
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
        /// Author allowlist: signer DIDs (`did:key:...`) and/or node ids
        /// (16 hex chars = first 8 bytes of `sha256(did)`). Matched only
        /// against the wire's *signature-verified* `fromId`. Empty means
        /// every author is accepted (the historical behaviour).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        trusted_authors: Vec<String>,
    },
    /// Subscribes to a tc-chat room's signed `tc-chat:post` text posts (see
    /// `crate::bot::source`). Posts published by this bot's own DID are
    /// skipped, so a pipeline can read from and write to the same room
    /// without feeding on itself.
    ChatRoom {
        #[serde(default)]
        room: String,
        /// Same semantics as `GlobalArticles::trusted_authors`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        trusted_authors: Vec<String>,
    },
}

/// One step of a pipeline's transform chain, applied in list order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TransformConfig {
    Summarize {
        #[serde(default)]
        model: Option<ModelRef>,
        #[serde(default)]
        reasoning_effort: Option<String>,
        #[serde(default, skip_serializing)]
        preset_id: String,
    },
    Tts {
        #[serde(default)]
        model: Option<ModelRef>,
        #[serde(default)]
        voice: Option<String>,
        #[serde(default, skip_serializing)]
        preset_id: String,
        #[serde(default)]
        format: Option<String>,
        #[serde(default)]
        speed: Option<f64>,
    },
    Translate {
        #[serde(default)]
        model: Option<ModelRef>,
        #[serde(default)]
        reasoning_effort: Option<String>,
        #[serde(default, skip_serializing)]
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
    /// so it stays opt-in. Even when enabled, stdio ignores `auto_accept`:
    /// only an explicit trust decision or `allow_peers` admits the peer.
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

/// Per-user data directory (daemon state, keys, blocks, relay wirelogs).
pub fn data_dir() -> Result<PathBuf> {
    crate::runtime::dirs(false)
}

/// Per-user config directory.
pub fn config_dir() -> Result<PathBuf> {
    crate::runtime::dirs(true)
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
        // Only restore while the destination is unchanged: a masked key
        // paired with a new base_url would ship the real key to that host.
        let submitted_url = entry
            .get("base_url")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&real.base_url);
        if url_origin(submitted_url) != url_origin(&real.base_url) {
            anyhow::bail!(
                "ai.providers: provider {id:?} base_url changed to a different host; re-enter its api_key instead of reusing the masked \"***\""
            );
        }
        entry["api_key"] = serde_json::Value::String(real.api_key.clone());
    }
    Ok(value)
}

const MASK: &str = "***";

/// `scheme://host:port` of a URL, lowercased, with userinfo/path/query
/// dropped. A masked secret may only be restored when this is unchanged --
/// otherwise a client (or an attacker driving `config.set`) could re-point a
/// destination and have the daemon attach the stored credential to it.
fn url_origin(url: &str) -> String {
    let url = url.trim();
    let (scheme, rest) = url.split_once("://").unwrap_or(("", url));
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    format!(
        "{}://{}",
        scheme.to_ascii_lowercase(),
        host.to_ascii_lowercase()
    )
}

/// Masks a webhook URL for `config.show`: userinfo becomes `***@` and a
/// query string becomes `?***`. A URL with neither is returned unchanged
/// (nothing secret to hide), so the mask is deterministic and
/// [`substitute_masked_webhook_secrets`] can recognise a round-tripped one.
pub fn mask_webhook_url(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest),
        None => (String::new(), url),
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let (userinfo, host) = match authority.rsplit_once('@') {
        Some((_, host)) => (true, host),
        None => (false, authority),
    };
    let (path, query) = match tail.find(['?', '#']) {
        Some(i) => (&tail[..i], true),
        None => (tail, false),
    };
    if !userinfo && !query {
        return url.to_string();
    }
    format!(
        "{scheme}{}{host}{path}{}",
        if userinfo { "***@" } else { "" },
        if query { "?***" } else { "" }
    )
}

/// `config.show` support: masks secrets inside `bot.pipelines[].sinks[]`
/// webhook sinks -- every non-empty header value becomes `"***"` and the
/// URL's userinfo/query is hidden (see [`mask_webhook_url`]). Empty header
/// values stay as-is so a client can tell "set" from "not set".
pub fn mask_bot_webhook_secrets(value: &mut serde_json::Value) {
    let Some(pipelines) = value["bot"]["pipelines"].as_array_mut() else {
        return;
    };
    for pipeline in pipelines {
        let Some(sinks) = pipeline["sinks"].as_array_mut() else {
            continue;
        };
        for sink in sinks {
            if sink["kind"] != "webhook" {
                continue;
            }
            if let Some(url) = sink["url"].as_str() {
                sink["url"] = serde_json::Value::String(mask_webhook_url(url));
            }
            if let Some(headers) = sink["headers"].as_array_mut() {
                for header in headers {
                    if matches!(header["value"].as_str(), Some(v) if !v.is_empty()) {
                        header["value"] = serde_json::Value::String(MASK.into());
                    }
                }
            }
        }
    }
}

/// `set_by_path("bot.pipelines", ...)` support, mirroring
/// [`substitute_masked_provider_keys`]: a webhook sink whose `url` equals the
/// masked form of the current sink's URL (same pipeline id, same sink
/// index) gets the real URL back, and a header value of `"***"` gets the
/// real value of the same-named header of that sink. A mask with nothing to
/// substitute from is an error -- the placeholder is never stored.
fn substitute_masked_webhook_secrets(
    config: &Config,
    mut value: serde_json::Value,
) -> Result<serde_json::Value> {
    let Some(pipelines) = value.as_array_mut() else {
        return Ok(value);
    };
    for pipeline in pipelines.iter_mut() {
        let pid = pipeline
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let current = config.bot.pipelines.iter().find(|p| p.id == pid);
        let Some(sinks) = pipeline.get_mut("sinks").and_then(|s| s.as_array_mut()) else {
            continue;
        };
        for (index, sink) in sinks.iter_mut().enumerate() {
            if sink.get("kind").and_then(serde_json::Value::as_str) != Some("webhook") {
                continue;
            }
            let current_sink = current.and_then(|p| match p.sinks.get(index) {
                Some(SinkConfig::Webhook { url, headers, .. }) => Some((url, headers)),
                _ => None,
            });
            if let Some(submitted) = sink.get("url").and_then(serde_json::Value::as_str) {
                match current_sink {
                    Some((real, _)) if submitted == mask_webhook_url(real) => {
                        sink["url"] = serde_json::Value::String(real.clone());
                    }
                    _ if submitted.contains("?***") || submitted.contains("***@") => {
                        anyhow::bail!(
                            "bot.pipelines: masked webhook url for pipeline {pid:?} sink {index} has nothing to restore it from (re-enter the real url)"
                        );
                    }
                    _ => {}
                }
            }
            // The (possibly just-restored) submitted URL must still point at
            // the same origin as the stored one before any header secret is
            // restored, or a masked `Authorization` would follow a changed
            // URL to a new host.
            let same_origin = match (
                sink.get("url").and_then(serde_json::Value::as_str),
                current_sink,
            ) {
                (Some(submitted), Some((real, _))) => url_origin(submitted) == url_origin(real),
                (None, Some(_)) => true,
                _ => false,
            };
            let Some(headers) = sink.get_mut("headers").and_then(|h| h.as_array_mut()) else {
                continue;
            };
            for header in headers.iter_mut() {
                if header.get("value").and_then(serde_json::Value::as_str) != Some(MASK) {
                    continue;
                }
                if !same_origin {
                    anyhow::bail!(
                        "bot.pipelines: webhook url for pipeline {pid:?} sink {index} changed to a different host; re-enter its masked header values instead of reusing \"***\""
                    );
                }
                let name = header
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let real = current_sink
                    .and_then(|(_, hs)| hs.iter().find(|h| h.name == name))
                    .with_context(|| {
                        format!(
                            "bot.pipelines: masked header {name:?} for pipeline {pid:?} sink {index} has nothing to restore it from"
                        )
                    })?;
                header["value"] = serde_json::Value::String(real.value.clone());
            }
        }
    }
    Ok(value)
}

/// Set one config field addressed as `section.field` (e.g.
/// "ai.default_ref"), returning the updated config. Values round-trip
/// through serde so types are validated against the real Config shape;
/// `null` clears optional fields.
pub fn set_by_path(config: &Config, path: &str, value: serde_json::Value) -> Result<Config> {
    // Only this nested setting is writable; whole-section writes remain forbidden.
    if path == "ai.tts.speed" {
        if !value.is_null() && !value.as_f64().is_some_and(valid_tts_speed) {
            anyhow::bail!(
                "ai.tts.speed must be a finite number between 0.25 and 4.0 (null clears it)"
            );
        }
        let mut updated = config.clone();
        let voice = updated
            .ai
            .tts
            .as_mut()
            .context("configure ai.tts before setting ai.tts.speed")?;
        voice.speed = value.as_f64();
        updated.ai.validate()?;
        updated.network.validate()?;
        return Ok(updated);
    }
    // `ai.providers` gets special handling for masked api_keys before the
    // generic "***" rejection below -- see `substitute_masked_provider_keys`.
    let value = if path == "ai.providers" {
        substitute_masked_provider_keys(config, value)?
    } else if path == "bot.pipelines" {
        substitute_masked_webhook_secrets(config, value)?
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

    let updated: Config = serde_json::from_value(tree)
        .with_context(|| format!("invalid value for {section}.{field}"))?;
    updated.network.validate()?;
    updated.ai.validate()?;
    Ok(updated)
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
        "network.membership_allowlist" | "ai.trusted_providers" => "applied immediately",
        "ai.providers"
        | "ai.default_ref"
        | "ai.default_reasoning_effort"
        | "ai.tts"
        | "ai.tts.speed"
        | "ai.stt" => "applied immediately",
        _ => "next service start",
    }
}

impl Config {
    /// Validates the `[network]` section (see [`NetworkConfig::validate`]).
    pub fn validate_network(&self) -> Result<()> {
        self.network.validate()
    }

    /// Load config from disk, writing defaults on first run. Runs the
    /// legacy `[ai]` migration (see `migrate_legacy`) and persists it when
    /// it changed anything, so callers always see the current shape.
    pub fn load() -> Result<Self> {
        let path = config_path()?;
        if !path.exists() {
            let mut config = Self::default();
            config.ui.listen = format!("127.0.0.1:{}", crate::runtime::initial_ui_port()?);
            if !crate::runtime::legacy_default() {
                let api = std::net::TcpListener::bind("127.0.0.1:0")?;
                let rtsp = std::net::TcpListener::bind("127.0.0.1:0")?;
                config.ai.api_listen = api.local_addr()?.to_string();
                config.stream.rtsp_url = format!("rtsp://{}/stream", rtsp.local_addr()?);
                let room = format!("mistl-dev-{}", crate::runtime::context().instance);
                config.ai.providers.push(AiProviderConfig {
                    id: "room".into(),
                    label: room.clone(),
                    base_url: format!("mist-network://{room}"),
                    ..Default::default()
                });
                config.update.auto_check = false;
                config.update.auto_apply = false;
            }
            config.save()?;
            return Ok(config);
        }
        crate::statefile::restrict_existing(&path);
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let (config, migrated) =
            Self::parse_loaded(&text).with_context(|| format!("parsing {}", path.display()))?;
        if migrated {
            config.save()?;
        }
        Ok(config)
    }

    fn parse_loaded(text: &str) -> Result<(Self, bool)> {
        let mut tree: toml::Value = toml::from_str(text)?;
        if let Some(ai) = tree.get_mut("ai").and_then(toml::Value::as_table_mut) {
            // Wrong scalar types must not prevent the rest of the config loading.
            // Value validation also runs after migration so inherited options
            // receive the same checks as the current configuration.
            if ai
                .get("default_reasoning_effort")
                .is_some_and(|v| v.as_str().is_none())
            {
                tracing::warn!("ai: ignoring invalid ai.default_reasoning_effort");
                ai.remove("default_reasoning_effort");
            }
            if let Some(tts) = ai.get_mut("tts").and_then(toml::Value::as_table_mut)
                && tts
                    .get("speed")
                    .is_some_and(|v| v.as_float().is_none() && v.as_integer().is_none())
            {
                tracing::warn!("ai: ignoring invalid ai.tts.speed");
                tts.remove("speed");
            }
        }
        let mut config: Self = tree.try_into()?;
        config.ai.discard_invalid_options();
        let migrated = config.migrate_legacy();
        config.ai.discard_invalid_options();
        Ok((config, migrated))
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        // config.toml holds API keys and webhook credentials: owner-only and
        // atomically replaced.
        crate::statefile::write_private(&path, toml::to_string_pretty(self)?.as_bytes())
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
        let advertised_changed = self.migrate_model_refs();
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
                    ..Default::default()
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

    /// One-time preset and room migration. New fields always win.
    fn migrate_model_refs(&mut self) -> bool {
        let ai = &mut self.ai;
        let mut changed = ai.room_id.is_some()
            || !ai.presets.is_empty()
            || !ai.default_preset_id.is_empty()
            || !ai.tts_preset_id.is_empty()
            || !ai.stt_preset_id.is_empty()
            || !ai.advertised_models.is_empty()
            || ai.upstream_url.is_some()
            || ai.upstream_api_key.is_some()
            || ai.default_model.is_some()
            || ai.temperature.is_some();
        let model_ref = |p: &AiPresetConfig| ModelRef {
            provider_id: p.provider_id.clone(),
            model: p.model.clone(),
        };
        if ai.default_reasoning_effort.is_none() {
            ai.default_reasoning_effort = ai
                .presets
                .iter()
                .find(|p| p.id == ai.default_preset_id)
                .and_then(|p| p.reasoning_effort.clone());
        }
        if ai.default_ref.is_none() {
            ai.default_ref = ai
                .presets
                .iter()
                .find(|p| p.id == ai.default_preset_id)
                .map(model_ref);
        }
        if ai.tts.is_none() {
            ai.tts = ai
                .presets
                .iter()
                .find(|p| p.id == ai.tts_preset_id)
                .map(|p| VoiceConfig {
                    provider_id: p.provider_id.clone(),
                    model: p.model.clone(),
                    voice: p.voice.clone(),
                    lang_voices: p.lang_voices.clone(),
                    speed: None,
                });
        }
        if ai.stt.is_none() {
            ai.stt = ai
                .presets
                .iter()
                .find(|p| p.id == ai.stt_preset_id)
                .map(model_ref);
        }
        for pipeline in &mut self.bot.pipelines {
            for transform in &mut pipeline.transforms {
                let (model, preset_id, effort, voice) = match transform {
                    TransformConfig::Summarize {
                        model,
                        preset_id,
                        reasoning_effort,
                    }
                    | TransformConfig::Translate {
                        model,
                        preset_id,
                        reasoning_effort,
                        ..
                    } => (model, preset_id, Some(reasoning_effort), None),
                    TransformConfig::Tts {
                        model,
                        preset_id,
                        voice,
                        ..
                    } => (model, preset_id, None, Some(voice)),
                };
                if !preset_id.is_empty() {
                    changed = true;
                    if let Some(preset) = ai.presets.iter().find(|p| &p.id == preset_id) {
                        if model.is_none() {
                            *model = Some(model_ref(preset));
                        }
                        if let Some(effort) = effort {
                            if effort.is_none() {
                                *effort = preset.reasoning_effort.clone();
                            }
                        }
                        if let Some(voice) = voice {
                            if voice.is_none() {
                                *voice = preset.voice.clone();
                            }
                        }
                    } else {
                        // Preserve a visibly unusable assignment instead of adopting the default.
                        if model.is_none() {
                            *model = Some(ModelRef {
                                provider_id: String::new(),
                                model: preset_id.clone(),
                            });
                        }
                    }
                    preset_id.clear();
                }
            }
        }
        let legacy_room = ai.room_id.take().filter(|r| !r.trim().is_empty());
        let advertised = std::mem::take(&mut ai.advertised_models);
        if legacy_room.is_some() || !advertised.is_empty() {
            let room = legacy_room.unwrap_or_else(|| crate::net::DEFAULT_ROOM.to_string());
            let url = format!("mist-network://{room}");
            let index = if let Some(index) = ai.providers.iter().position(|p| p.base_url == url) {
                index
            } else {
                let id = unique_provider_id(&ai.providers, "room");
                ai.providers.push(AiProviderConfig {
                    id,
                    label: room,
                    base_url: url,
                    ..Default::default()
                });
                ai.providers.len() - 1
            };
            let shared: Vec<_> = advertised
                .iter()
                .filter_map(|id| {
                    let preset = ai
                        .presets
                        .iter()
                        .find(|p| &p.id == id)
                        .or_else(|| ai.presets.iter().find(|p| &p.model == id))?;
                    let provider = ai.providers.iter().find(|p| p.id == preset.provider_id)?;
                    if provider.room().is_some() {
                        return None;
                    }
                    Some(model_ref(preset))
                })
                .collect();
            if ai.providers[index].shared.is_empty() {
                ai.providers[index].shared = shared;
            }
            if !advertised.is_empty() {
                ai.providers[index].provide = true;
            }
        }
        ai.presets.clear();
        ai.default_preset_id.clear();
        ai.tts_preset_id.clear();
        ai.stt_preset_id.clear();
        ai.upstream_url = None;
        ai.upstream_api_key = None;
        ai.default_model = None;
        ai.temperature = None;
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
    fn ai_effort_and_speed_config_roundtrip_and_validation() {
        let mut config = Config::default();
        config = set_by_path(&config, "ai.tts", serde_json::json!({
            "provider_id":"http", "model":"speech", "voice":"speaker", "lang_voices":{"ja":"ja-speaker"}
        })).unwrap();
        for effort in ["none", "minimal", "low", "medium", "high", "xhigh", "max"] {
            config = set_by_path(
                &config,
                "ai.default_reasoning_effort",
                serde_json::json!(effort),
            )
            .unwrap();
            for speed in [0.25, 1.25, 4.0] {
                config = set_by_path(&config, "ai.tts.speed", serde_json::json!(speed)).unwrap();
                let loaded: Config = toml::from_str(&toml::to_string(&config).unwrap()).unwrap();
                assert_eq!(loaded.ai.default_reasoning_effort.as_deref(), Some(effort));
                assert_eq!(loaded.ai.tts.as_ref().unwrap().speed, Some(speed));
                let json = serde_json::to_value(loaded).unwrap();
                assert_eq!(json["ai"]["default_reasoning_effort"], effort);
                assert_eq!(json["ai"]["tts"]["speed"], speed);
                assert_eq!(json["ai"]["tts"]["voice"], "speaker");
                assert_eq!(json["ai"]["tts"]["lang_voices"]["ja"], "ja-speaker");
            }
        }
        for effort in [
            serde_json::json!("invalid"),
            serde_json::json!("HIGH"),
            serde_json::json!(1),
        ] {
            assert!(set_by_path(&config, "ai.default_reasoning_effort", effort).is_err());
        }
        for speed in [
            serde_json::json!(0.24),
            serde_json::json!(4.01),
            serde_json::json!("1.0"),
        ] {
            assert!(set_by_path(&config, "ai.tts.speed", speed.clone()).is_err());
            assert!(
                set_by_path(
                    &config,
                    "ai.tts",
                    serde_json::json!({"provider_id":"http","model":"speech","speed":speed})
                )
                .is_err()
            );
        }
        config = set_by_path(
            &config,
            "ai.default_reasoning_effort",
            serde_json::Value::Null,
        )
        .unwrap();
        config = set_by_path(&config, "ai.tts.speed", serde_json::Value::Null).unwrap();
        assert!(config.ai.default_reasoning_effort.is_none());
        assert!(config.ai.tts.unwrap().speed.is_none());
        let ai: AiConfig = toml::from_str("").unwrap();
        assert!(ai.default_reasoning_effort.is_none());
        assert!(
            toml::from_str::<VoiceConfig>("provider_id = 'http'\nmodel = 'speech'")
                .unwrap()
                .speed
                .is_none()
        );
        assert!(set_by_path(&Config::default(), "ai.tts.speed", serde_json::json!(1)).is_err());
    }

    #[test]
    fn default_preset_effort_migrates_once_and_new_value_wins() {
        for (new, expected) in [(None, "high"), (Some("none"), "none")] {
            let mut config: Config = toml::from_str(
                r#"
                [ai]
                default_preset_id = "chat"
                [[ai.presets]]
                id = "other"
                reasoning_effort = "low"
                [[ai.presets]]
                id = "chat"
                provider_id = "http"
                model = "raw"
                reasoning_effort = "high"
            "#,
            )
            .unwrap();
            config.ai.default_reasoning_effort = new.map(String::from);
            assert!(config.migrate_legacy());
            assert_eq!(
                config.ai.default_reasoning_effort.as_deref(),
                Some(expected)
            );
            assert!(!config.migrate_legacy());
            let loaded: Config = toml::from_str(&toml::to_string(&config).unwrap()).unwrap();
            assert_eq!(
                loaded.ai.default_reasoning_effort.as_deref(),
                Some(expected)
            );
        }
    }

    #[test]
    fn invalid_loaded_ai_options_are_ignored_after_migration() {
        let mut config: Config = toml::from_str(
            r#"
            [ai]
            default_preset_id = "old"
            tts = {provider_id="http",model="speech",speed=inf}
            [[ai.presets]]
            id = "old"
            reasoning_effort = "future-effort"
        "#,
        )
        .unwrap();
        config.migrate_legacy();
        config.ai.discard_invalid_options();
        assert!(config.ai.default_reasoning_effort.is_none());
        assert!(config.ai.tts.unwrap().speed.is_none());
        let mut ai: AiConfig = toml::from_str("default_reasoning_effort = 'bad'").unwrap();
        ai.discard_invalid_options();
        assert!(ai.default_reasoning_effort.is_none());
        let (config, migrated) = Config::parse_loaded(
            r#"
            [ai]
            default_reasoning_effort = 42
            tts = {provider_id="http",model="speech",speed="fast"}
        "#,
        )
        .unwrap();
        assert!(!migrated);
        assert!(config.ai.default_reasoning_effort.is_none());
        assert!(config.ai.tts.unwrap().speed.is_none());
        let (config, migrated) = Config::parse_loaded(
            r#"
            [ai]
            default_reasoning_effort = "invalid"
            default_preset_id = "old"
            [[ai.presets]]
            id = "old"
            reasoning_effort = "low"
        "#,
        )
        .unwrap();
        assert!(migrated);
        assert_eq!(config.ai.default_reasoning_effort.as_deref(), Some("low"));
    }

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
            ("ai.request_timeout_secs", json!(30)),
            ("ai.api_listen", json!("127.0.0.1:6480")),
            (
                "ai.default_ref",
                json!({ "provider_id": "p", "model": "m" }),
            ),
            ("ai.tts", json!({ "provider_id": "p", "model": "speech" })),
            ("ai.stt", json!(null)),
            ("ai.providers", json!([])),
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

    fn legacy_ai_config() -> Config {
        toml::from_str(r#"
            [ai]
            room_id = "legacy-room"
            default_preset_id = "chat"
            tts_preset_id = "speech"
            stt_preset_id = "listen"
            advertised_models = ["chat", "raw", "network", "missing"]
            [[ai.providers]]
            id = "http"
            base_url = "http://local/v1"
            api_key = "secret"
            [[ai.providers]]
            id = "network"
            base_url = "mist-network://other"
            [[ai.presets]]
            id = "chat"
            label = "Friendly label"
            provider_id = "http"
            model = "raw"
            reasoning_effort = "high"
            [[ai.presets]]
            id = "speech"
            provider_id = "http"
            model = "speech-raw"
            voice = "speaker"
            lang_voices = { en = "english" }
            [[ai.presets]]
            id = "listen"
            provider_id = "http"
            model = "listen-raw"
            [[ai.presets]]
            id = "network"
            provider_id = "network"
            model = "remote"
            [[bot.pipelines]]
            id = "task"
            schedule = "@every 1h"
            source = { kind = "chat-room", room = "chat" }
            transforms = [ { kind = "summarize", preset_id = "chat" },
                { kind = "translate", preset_id = "chat", target_lang = "en", reasoning_effort = "low" },
                { kind = "tts", preset_id = "speech" } ]
        "#).unwrap()
    }

    #[test]
    fn legacy_presets_become_refs_and_room_shared_raw_ids() {
        let mut config = legacy_ai_config();
        assert!(config.migrate_legacy());
        assert_eq!(
            config.ai.default_ref,
            Some(ModelRef {
                provider_id: "http".into(),
                model: "raw".into()
            })
        );
        assert_eq!(
            config.ai.tts.as_ref().unwrap().voice.as_deref(),
            Some("speaker")
        );
        assert_eq!(config.ai.tts.as_ref().unwrap().lang_voices["en"], "english");
        assert_eq!(config.ai.stt.as_ref().unwrap().model, "listen-raw");
        let room = config
            .ai
            .providers
            .iter()
            .find(|p| p.room() == Some("legacy-room"))
            .unwrap();
        assert!(room.provide);
        assert_eq!(room.shared.len(), 2);
        assert_eq!(room.shared[0].model, "raw");
        assert!(room.shared.iter().all(|r| r.provider_id == "http"));
        assert!(config.ai.presets.is_empty());
        assert!(!config.migrate_legacy());
        let text = toml::to_string_pretty(&config).unwrap();
        let ai = serde_json::to_value(&config.ai).unwrap();
        for field in ["presets", "advertised_models", "room_id", "temperature"] {
            assert!(ai.get(field).is_none(), "{field}: {ai}");
        }
        assert!(!text.contains("preset_id"));
        let mut reloaded: Config = toml::from_str(&text).unwrap();
        assert!(!reloaded.migrate_legacy());
    }

    #[test]
    fn task_migration_keeps_effort_and_voice() {
        let mut config = legacy_ai_config();
        config.migrate_legacy();
        match &config.bot.pipelines[0].transforms[0] {
            TransformConfig::Summarize {
                model,
                reasoning_effort,
                ..
            } => {
                assert_eq!(model.as_ref().unwrap().model, "raw");
                assert_eq!(reasoning_effort.as_deref(), Some("high"));
            }
            _ => panic!("summarize"),
        }
        match &config.bot.pipelines[0].transforms[1] {
            TransformConfig::Translate {
                reasoning_effort, ..
            } => assert_eq!(reasoning_effort.as_deref(), Some("low")),
            _ => panic!("translate"),
        }
        match &config.bot.pipelines[0].transforms[2] {
            TransformConfig::Tts { voice, .. } => assert_eq!(voice.as_deref(), Some("speaker")),
            _ => panic!("tts"),
        }
    }

    #[test]
    fn migration_keeps_new_assignments_and_existing_room_provider() {
        let mut config = legacy_ai_config();
        config.ai.default_ref = Some(ModelRef {
            provider_id: "network".into(),
            model: "kept".into(),
        });
        config.ai.providers.push(AiProviderConfig {
            id: "existing-room".into(),
            label: "Kept".into(),
            base_url: "mist-network://legacy-room".into(),
            ..Default::default()
        });
        config.migrate_legacy();
        assert_eq!(config.ai.default_ref.as_ref().unwrap().model, "kept");
        assert_eq!(
            config
                .ai
                .providers
                .iter()
                .filter(|p| p.room() == Some("legacy-room"))
                .count(),
            1
        );
        assert_eq!(config.ai.providers.last().unwrap().label, "Kept");
    }

    #[test]
    fn oldest_upstream_migration_uses_default_room_and_is_idempotent() {
        let mut config: Config = toml::from_str(
            r#"[ai]
            upstream_url = "http://local/v1"
            upstream_api_key = "key"
            default_model = "raw"
            temperature = 0.5
            advertised_models = ["raw"]"#,
        )
        .unwrap();
        assert!(config.migrate_legacy());
        assert!(!config.migrate_legacy());
        assert_eq!(config.ai.default_ref.as_ref().unwrap().model, "raw");
        let room = config
            .ai
            .providers
            .iter()
            .find(|p| p.room() == Some(crate::net::DEFAULT_ROOM))
            .unwrap();
        assert!(room.provide);
        assert_eq!(room.shared[0].model, "raw");
    }

    #[test]
    fn disabled_ref_uses_default_without_rewriting_config() {
        let mut config = legacy_ai_config();
        config.migrate_legacy();
        config.ai.providers.push(AiProviderConfig {
            id: "disabled".into(),
            enabled: false,
            ..Default::default()
        });
        let reference = ModelRef {
            provider_id: "disabled".into(),
            model: "kept".into(),
        };
        assert_eq!(
            resolve_ref(&config.ai, Some(&reference)).unwrap().model,
            "raw"
        );
        assert_eq!(reference.model, "kept");
        assert!(resolve_ref_exact(&config.ai, &reference).is_none());
        config.ai.providers[0].enabled = false;
        assert!(resolve_ref(&config.ai, Some(&reference)).is_none());
        assert_eq!(config.ai.default_ref.as_ref().unwrap().provider_id, "http");
    }

    #[test]
    fn missing_ref_does_not_adopt_default_or_first_provider() {
        let mut config = legacy_ai_config();
        config.migrate_legacy();
        let reference = ModelRef {
            provider_id: "missing".into(),
            model: "kept".into(),
        };
        assert!(resolve_ref(&config.ai, Some(&reference)).is_none());
    }

    #[test]
    fn legacy_ai_fields_are_deserialize_only_and_not_settable() {
        let config = Config::default();
        for field in [
            "room_id",
            "presets",
            "default_preset_id",
            "tts_preset_id",
            "stt_preset_id",
            "advertised_models",
        ] {
            assert!(set_by_path(&config, &format!("ai.{field}"), json!(null)).is_err());
            assert!(
                serde_json::to_value(&config.ai)
                    .unwrap()
                    .get(field)
                    .is_none()
            );
        }
    }

    #[test]
    fn provider_new_fields_and_optional_voice_round_trip() {
        let config = Config::default();
        let config = set_by_path(&config, "ai.providers", json!([{ "id": "room", "base_url": "mist-network://room",
            "enabled": false, "models": ["raw"], "models_fetched_at": "2026-10-01T00:00:00Z", "provide": true,
            "shared": [{ "provider_id": "http", "model": "raw" }] }])).unwrap();
        let config = set_by_path(
            &config,
            "ai.default_ref",
            json!({ "provider_id": "room", "model": "raw" }),
        )
        .unwrap();
        let config = set_by_path(&config, "ai.tts", json!({ "provider_id": "http", "model": "speech", "voice": "speaker", "lang_voices": { "en": "english" } })).unwrap();
        let reloaded: Config = toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();
        assert!(!reloaded.ai.providers[0].enabled);
        assert!(reloaded.ai.providers[0].provide);
        assert_eq!(reloaded.ai.providers[0].shared[0].model, "raw");
        assert_eq!(
            reloaded.ai.tts.as_ref().unwrap().lang_voices["en"],
            "english"
        );
        assert!(
            set_by_path(&reloaded, "ai.tts", json!(null))
                .unwrap()
                .ai
                .tts
                .is_none()
        );
        assert!(AiProviderConfig::default().enabled);
    }

    #[test]
    fn ai_json_contract_contains_only_current_fields() {
        let mut config = Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "http".into(),
            base_url: "http://local/v1".into(),
            ..Default::default()
        });
        assert_eq!(
            serde_json::to_value(&config.ai).unwrap(),
            json!({
                "default_ref": null, "default_reasoning_effort": null, "tts": null, "stt": null,
                "providers": [{ "id": "http", "label": "", "base_url": "http://local/v1", "api_key": "",
                    "enabled": true, "models": [], "models_fetched_at": null, "provide": false, "shared": [] }],
                "api_listen": "127.0.0.1:6478", "request_timeout_secs": 120, "trusted_providers": []
            })
        );
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
    fn set_by_path_ai_providers_substitutes_masked_api_key_from_current_config() {
        let mut config = Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "p1".to_string(),
            label: "P1".to_string(),
            base_url: "http://x/v1".to_string(),
            api_key: "real-secret".to_string(),
            ..Default::default()
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
        let applied_immediately = ["ai.providers", "ai.default_ref", "ai.tts", "ai.stt"];
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
                trusted_authors: vec![],
                rooms: vec!["tc-global-articles".to_string()],
                langs: vec!["ja".to_string()],
            },
            transforms: vec![
                TransformConfig::Summarize {
                    preset_id: String::new(),
                    model: Some(crate::config::ModelRef {
                        provider_id: "openai".into(),
                        model: "worker".into(),
                    }),
                    reasoning_effort: None,
                },
                TransformConfig::Tts {
                    preset_id: String::new(),
                    model: Some(crate::config::ModelRef {
                        provider_id: "openai".into(),
                        model: "tts-default".into(),
                    }),
                    voice: None,
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
                trusted_authors: vec!["did:key:zAlice".to_string(), "0123456789abcdef".to_string()],
            },
            transforms: vec![TransformConfig::Translate {
                preset_id: String::new(),
                model: Some(crate::config::ModelRef {
                    provider_id: "openai".into(),
                    model: "worker".into(),
                }),
                reasoning_effort: None,
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
    fn source_trusted_authors_defaults_empty_and_round_trips_through_json() {
        // Legacy config without the field parses with an empty allowlist and
        // serializes back without it (backward compatible).
        let legacy: SourceConfig =
            serde_json::from_value(serde_json::json!({"kind": "chat-room", "room": "r"})).unwrap();
        let value = serde_json::to_value(&legacy).unwrap();
        assert!(value.get("trusted_authors").is_none());

        let set: SourceConfig = serde_json::from_value(serde_json::json!({
            "kind": "global-articles", "trusted_authors": ["did:key:zA"]
        }))
        .unwrap();
        let value = serde_json::to_value(&set).unwrap();
        assert_eq!(value["trusted_authors"], serde_json::json!(["did:key:zA"]));
    }

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
            SourceConfig::GlobalArticles {
                rooms,
                langs,
                trusted_authors,
            } => {
                assert!(trusted_authors.is_empty());
                assert_eq!(rooms, &vec!["tc-global-articles".to_string()]);
                assert_eq!(langs, &vec!["ja".to_string()]);
            }
            other => panic!("expected GlobalArticles, got {other:?}"),
        }
        assert_eq!(pipeline.transforms.len(), 2);
        match &pipeline.transforms[0] {
            TransformConfig::Summarize { model, .. } => {
                assert_eq!(model.as_ref().unwrap().model, "worker")
            }
            other => panic!("expected Summarize, got {other:?}"),
        }
        match &pipeline.transforms[1] {
            TransformConfig::Tts {
                model,
                format,
                speed,
                ..
            } => {
                assert_eq!(model.as_ref().unwrap().model, "tts-default");
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
            SourceConfig::ChatRoom {
                room,
                trusted_authors,
            } => {
                assert_eq!(room, "team-room");
                assert_eq!(trusted_authors, &["did:key:zAlice", "0123456789abcdef"]);
            }
            other => panic!("expected ChatRoom, got {other:?}"),
        }
        match &pipeline_v2.transforms[0] {
            TransformConfig::Translate {
                model, target_lang, ..
            } => {
                assert_eq!(model.as_ref().unwrap().model, "worker");
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
                trusted_authors: vec![],
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

    fn webhook_pipeline(url: &str, headers: &[(&str, &str)]) -> PipelineConfig {
        let mut pipeline = sample_pipeline_v2();
        pipeline.id = "hooked".into();
        pipeline.sinks = vec![
            SinkConfig::ChatPost { room: "r".into() },
            SinkConfig::Webhook {
                url: url.into(),
                include_audio: false,
                max_audio_bytes: None,
                method: None,
                body_template: None,
                sign: true,
                include_body: false,
                headers: headers
                    .iter()
                    .map(|(n, v)| WebhookHeader {
                        name: (*n).into(),
                        value: (*v).into(),
                    })
                    .collect(),
            },
        ];
        pipeline
    }

    #[test]
    fn mask_webhook_url_hides_userinfo_and_query_only() {
        assert_eq!(
            mask_webhook_url("https://h.example/p"),
            "https://h.example/p"
        );
        assert_eq!(
            mask_webhook_url("https://h.example/p?token=abc"),
            "https://h.example/p?***"
        );
        assert_eq!(
            mask_webhook_url("https://user:pw@h.example:8443/p?x=1#f"),
            "https://***@h.example:8443/p?***"
        );
        assert_eq!(
            mask_webhook_url("https://u@h.example"),
            "https://***@h.example"
        );
    }

    #[test]
    fn config_show_masking_hides_webhook_headers_and_url_secrets() {
        let mut config = Config::default();
        config.bot.pipelines.push(webhook_pipeline(
            "https://h.example/p?key=SECRET",
            &[("Authorization", "Bearer SECRET"), ("X-Empty", "")],
        ));
        let mut value = serde_json::to_value(&config).unwrap();
        mask_bot_webhook_secrets(&mut value);
        let text = value.to_string();
        assert!(!text.contains("SECRET"), "{text}");
        let sink = &value["bot"]["pipelines"][0]["sinks"][1];
        assert_eq!(sink["url"], "https://h.example/p?***");
        assert_eq!(sink["headers"][0]["value"], "***");
        assert_eq!(sink["headers"][1]["value"], "");
    }

    #[test]
    fn set_by_path_bot_pipelines_restores_masked_webhook_secrets() {
        let mut config = Config::default();
        config.bot.pipelines.push(webhook_pipeline(
            "https://h.example/p?key=SECRET",
            &[("Authorization", "Bearer SECRET")],
        ));
        let mut shown = serde_json::to_value(&config).unwrap();
        mask_bot_webhook_secrets(&mut shown);
        let updated =
            set_by_path(&config, "bot.pipelines", shown["bot"]["pipelines"].clone()).unwrap();
        match &updated.bot.pipelines[0].sinks[1] {
            SinkConfig::Webhook { url, headers, .. } => {
                assert_eq!(url, "https://h.example/p?key=SECRET");
                assert_eq!(headers[0].value, "Bearer SECRET");
            }
            other => panic!("expected webhook, got {other:?}"),
        }
    }

    #[test]
    fn set_by_path_bot_pipelines_accepts_new_secrets_and_rejects_orphan_masks() {
        let mut config = Config::default();
        config
            .bot
            .pipelines
            .push(webhook_pipeline("https://h.example/p", &[]));
        // A freshly typed secret is stored as-is.
        let fresh = serde_json::to_value(vec![webhook_pipeline(
            "https://h.example/p?k=NEW",
            &[("X-Api-Key", "NEW")],
        )])
        .unwrap();
        let updated = set_by_path(&config, "bot.pipelines", fresh).unwrap();
        match &updated.bot.pipelines[0].sinks[1] {
            SinkConfig::Webhook { url, headers, .. } => {
                assert_eq!(url, "https://h.example/p?k=NEW");
                assert_eq!(headers[0].value, "NEW");
            }
            other => panic!("expected webhook, got {other:?}"),
        }
        // A masked header with no counterpart in the current config errors.
        let orphan = serde_json::to_value(vec![webhook_pipeline(
            "https://h.example/p",
            &[("Authorization", "***")],
        )])
        .unwrap();
        assert!(set_by_path(&config, "bot.pipelines", orphan).is_err());
        // So does a masked url that no longer matches.
        let orphan_url =
            serde_json::to_value(vec![webhook_pipeline("https://h.example/p?***", &[])]).unwrap();
        assert!(set_by_path(&config, "bot.pipelines", orphan_url).is_err());
    }

    #[test]
    fn masked_provider_key_is_not_restored_when_base_url_host_changes() {
        let mut config = Config::default();
        config.ai.providers.push(AiProviderConfig {
            id: "p1".into(),
            label: "P1".into(),
            base_url: "https://api.example.com/v1".into(),
            api_key: "real-secret".into(),
            ..Default::default()
        });
        let evil = json!([{ "id": "p1", "label": "P1", "base_url": "https://evil.example/v1", "api_key": "***" }]);
        let err = set_by_path(&config, "ai.providers", evil).unwrap_err();
        assert!(err.to_string().contains("re-enter"), "{err}");
        // Scheme or port change counts too.
        for url in [
            "http://api.example.com/v1",
            "https://api.example.com:8443/v1",
        ] {
            let v = json!([{ "id": "p1", "label": "P1", "base_url": url, "api_key": "***" }]);
            assert!(set_by_path(&config, "ai.providers", v).is_err(), "{url}");
        }
        // Path change on the same origin is fine; a re-entered key is fine.
        let ok = json!([{ "id": "p1", "label": "P1", "base_url": "https://API.example.com/v2", "api_key": "***" }]);
        assert_eq!(
            set_by_path(&config, "ai.providers", ok)
                .unwrap()
                .ai
                .providers[0]
                .api_key,
            "real-secret"
        );
        let fresh = json!([{ "id": "p1", "label": "P1", "base_url": "https://new.example/v1", "api_key": "new-key" }]);
        assert_eq!(
            set_by_path(&config, "ai.providers", fresh)
                .unwrap()
                .ai
                .providers[0]
                .api_key,
            "new-key"
        );
    }

    #[test]
    fn masked_webhook_header_is_not_restored_when_url_host_changes() {
        let mut config = Config::default();
        config.bot.pipelines.push(webhook_pipeline(
            "https://h.example/p",
            &[("Authorization", "Bearer SECRET")],
        ));
        let moved = serde_json::to_value(vec![webhook_pipeline(
            "https://evil.example/p",
            &[("Authorization", "***")],
        )])
        .unwrap();
        let err = set_by_path(&config, "bot.pipelines", moved).unwrap_err();
        assert!(err.to_string().contains("re-enter"), "{err}");
        // Same host, different path keeps restoring.
        let same = serde_json::to_value(vec![webhook_pipeline(
            "https://h.example/other",
            &[("Authorization", "***")],
        )])
        .unwrap();
        let updated = set_by_path(&config, "bot.pipelines", same).unwrap();
        match &updated.bot.pipelines[0].sinks[1] {
            SinkConfig::Webhook { headers, .. } => assert_eq!(headers[0].value, "Bearer SECRET"),
            other => panic!("expected webhook, got {other:?}"),
        }
    }

    #[test]
    fn legacy_upstream_fields_cannot_be_set_via_config_set() {
        // `ai.upstream_url`/`upstream_api_key` are skip_serializing legacy
        // fields, so `set_by_path` cannot address them: the destination can
        // never be changed independently of the stored key.
        let config = Config::default();
        assert!(set_by_path(&config, "ai.upstream_url", json!("https://evil.example")).is_err());
        assert!(set_by_path(&config, "ai.upstream_api_key", json!("k")).is_err());
    }

    fn did_ok() -> String {
        crate::identity::did_for_seed(1)
    }

    #[test]
    fn membership_allowlist_is_validated() {
        let config = Config::default();
        let updated =
            set_by_path(&config, "network.membership_allowlist", json!([did_ok()])).unwrap();
        assert_eq!(updated.network.membership_allowlist, vec![did_ok()]);
        for bad in ["did:key:zAlice", "did:web:example.com", &did_ok()[..55]] {
            assert!(
                set_by_path(&config, "network.membership_allowlist", json!([bad])).is_err(),
                "{bad}"
            );
        }
        assert!(Config::default().network.membership_allowlist.is_empty());
    }

    #[test]
    fn network_apply_maps_empty_to_open_and_list_to_some() {
        let mut net = NetworkConfig::default();
        net.apply();
        assert_eq!(crate::net::peer_auth::membership_allowlist(), None);
        net.membership_allowlist = vec![did_ok()];
        net.apply();
        assert_eq!(
            crate::net::peer_auth::membership_allowlist(),
            Some(vec![did_ok()])
        );
        NetworkConfig::default().apply();
    }

    #[test]
    fn trusted_providers_round_trips_and_defaults_empty() {
        let config = Config::default();
        assert!(config.ai.trusted_providers.is_empty());
        let updated = set_by_path(&config, "ai.trusted_providers", json!([did_ok()])).unwrap();
        assert_eq!(updated.ai.trusted_providers, vec![did_ok()]);
        assert_eq!(applies_when("ai.trusted_providers"), "applied immediately");
    }
}
