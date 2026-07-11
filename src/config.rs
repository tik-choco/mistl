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
    pub mailbox: MailboxConfig,
    #[serde(default)]
    pub ai: AiConfig,
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default)]
    pub update: UpdateConfig,
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
    /// tc-chat room the store joins for peer block exchange. Independent of
    /// `[mailbox] room_id`/`[ai] room_id`/`[stream] relay_room`/`share_room`
    /// for the same reason those are independent of each other -- the p2p
    /// transport supports multiple simultaneous rooms per process. Unset by
    /// default: the store stays purely local (no network join) until a room
    /// is configured. Unlike the other room settings, this one can be
    /// changed (or cleared) at any time -- `storage::store` re-resolves it
    /// on every call, hopping to the new room or leaving it entirely, with
    /// no daemon restart required.
    pub room_id: Option<String>,
    /// Destination directory `store.sandbox.export` writes to when no
    /// explicit `output` is given -- the "extract from the sandbox" default,
    /// used by continuous folder syncs (which materialize into a managed
    /// sandbox subdirectory rather than a user-chosen one; see
    /// `folder_sync`) to get files out onto the real filesystem. Unset by
    /// default, in which case export falls back to `<data_dir>/downloads`.
    /// Read live on every `store.sandbox.export` call, like `room_id` above,
    /// so this can be changed (or cleared, via `config.set` with a null/empty
    /// value) at any time without a daemon restart.
    pub export_dir: Option<PathBuf>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            blocks_dir: None,
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
            room_id: None,
            export_dir: None,
        }
    }
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
    /// Room joined by `stream relay` to receive a tc-chat screen share.
    /// Independent of `[mailbox] room_id` and `[ai] room_id` -- the p2p
    /// transport supports multiple simultaneous rooms per process, so this
    /// can name its own room, or reuse one of theirs.
    pub relay_room: Option<String>,
    /// Room joined by `stream share` to publish this machine's own screen
    /// capture into (see `stream::share`'s module doc). Independent of
    /// `relay_room`/`[mailbox] room_id`/`[ai] room_id` for the same reason --
    /// this can name its own room or reuse one of theirs.
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
            relay_room: None,
            share_room: None,
            audio_codec: "aac".into(),
            cascade: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MailboxConfig {
    /// Room id used for mailbox rendezvous with peers.
    pub room_id: Option<String>,
    /// Act as a mailbox bot: hold deposits addressed to offline peers.
    pub serve_as_bot: bool,
    /// tc-chat rooms to relay (join server-lessly on the user's behalf,
    /// verify + persist signed `tc-chat:*` wires, and answer other peers'
    /// `tc-chat:history-request` replays) -- see `crate::mailbox::chat_relay`.
    /// Empty (the default) means the relay never joins anything, regardless
    /// of `chat_relay`. Joined once at daemon start, like `room_id` above;
    /// requires a daemon restart to take effect.
    pub chat_rooms: Vec<String>,
    /// Master switch for the tc-chat relay described by `chat_rooms`. Kept
    /// separate from `chat_rooms` being non-empty so a configured room list
    /// can be temporarily disabled without clearing it. Requires a daemon
    /// restart to take effect.
    pub chat_relay: bool,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            room_id: None,
            serve_as_bot: true,
            chat_rooms: Vec::new(),
            chat_relay: false,
        }
    }
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
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiConfig {
    /// Room id for the AI network. Defaults to the mailbox room for
    /// backward-compat convenience when unset, but the p2p transport now
    /// supports multiple simultaneous rooms per process, so this no longer
    /// needs to match `[mailbox] room_id` -- set it explicitly to join an
    /// existing mistai (tc-mistllm etc.) room distinct from mailbox's.
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
    /// Models advertised in provider_hello. Empty = fetch from the resolved
    /// preset's provider `GET /models` at provide start.
    pub advertised_models: Vec<String>,
    /// Listen address of the local OpenAI-compatible API server (`ai serve`).
    pub api_listen: String,
    /// Inactivity timeout for a p2p LLM request (resets on every streamed chunk).
    pub request_timeout_secs: u64,
    /// Which `presets` entry `ai provide`/`ai serve` resolve to by default
    /// (see `resolve_preset`). "" = unset. Mirrors the shared LLM config
    /// contract's `defaultPresetId`.
    pub default_preset_id: String,
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
            providers: Vec::new(),
            presets: Vec::new(),
        }
    }
}

fn project_dirs() -> Result<directories::ProjectDirs> {
    directories::ProjectDirs::from("com", "tik-choco", "mistl")
        .context("could not determine home directory")
}

/// Per-user data directory (daemon state, keys, blocks, mailbox spool).
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
        // mailbox/ai/stream join their room once at service start and hold
        // it for the process lifetime; before any p2p service ran it applies
        // on next start, but "daemon restart" is the safe universal answer.
        // `storage.room_id` is the one exception -- `storage::store`
        // re-resolves it on every call and hops rooms live, so it falls
        // through to "next service start" below (true immediately, since
        // the next `store.*` command *is* its next "start").
        "mailbox.room_id"
        | "mailbox.chat_rooms"
        | "mailbox.chat_relay"
        | "ai.room_id"
        | "stream.relay_room"
        | "stream.share_room" => "daemon restart",
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
    pub fn migrate_legacy(&mut self) -> bool {
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
            });
        }

        if self.ai.default_preset_id.trim().is_empty() {
            self.ai.default_preset_id = "default".to_string();
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_by_path_updates_a_string_option() {
        let config = Config::default();
        let updated = set_by_path(&config, "ai.default_preset_id", json!("default")).unwrap();
        assert_eq!(updated.ai.default_preset_id, "default");
        // Untouched fields survive.
        assert_eq!(updated.stream.frame_rate, config.stream.frame_rate);
    }

    #[test]
    fn set_by_path_clears_option_with_null() {
        let mut config = Config::default();
        config.stream.relay_room = Some("room".into());
        let updated = set_by_path(&config, "stream.relay_room", serde_json::Value::Null).unwrap();
        assert_eq!(updated.stream.relay_room, None);
    }

    #[test]
    fn set_by_path_updates_and_clears_storage_export_dir() {
        let config = Config::default();
        assert_eq!(config.storage.export_dir, None, "unset by default");

        let updated = set_by_path(&config, "storage.export_dir", json!("C:\\exports")).unwrap();
        assert_eq!(updated.storage.export_dir, Some(PathBuf::from("C:\\exports")));

        let cleared = set_by_path(&updated, "storage.export_dir", serde_json::Value::Null).unwrap();
        assert_eq!(cleared.storage.export_dir, None);
    }

    #[test]
    fn set_by_path_updates_numbers_bools_and_arrays() {
        let config = Config::default();
        let updated = set_by_path(&config, "stream.frame_rate", json!(60)).unwrap();
        assert_eq!(updated.stream.frame_rate, 60);
        let updated = set_by_path(&config, "mailbox.serve_as_bot", json!(false)).unwrap();
        assert!(!updated.mailbox.serve_as_bot);
        let updated = set_by_path(&config, "ai.advertised_models", json!(["a", "b"])).unwrap();
        assert_eq!(updated.ai.advertised_models, vec!["a", "b"]);
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

        assert_eq!(config.ai.providers.len(), 1, "provider must not be duplicated");
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
    fn mailbox_chat_relay_defaults_off() {
        let config = Config::default();
        assert!(config.mailbox.chat_rooms.is_empty());
        assert!(!config.mailbox.chat_relay);
    }

    #[test]
    fn set_by_path_updates_mailbox_chat_relay_fields() {
        let config = Config::default();
        let updated =
            set_by_path(&config, "mailbox.chat_rooms", json!(["room-a", "room-b"])).unwrap();
        assert_eq!(updated.mailbox.chat_rooms, vec!["room-a", "room-b"]);
        let updated = set_by_path(&updated, "mailbox.chat_relay", json!(true)).unwrap();
        assert!(updated.mailbox.chat_relay);
    }

    #[test]
    fn applies_when_mailbox_chat_relay_settings_require_a_restart() {
        assert_eq!(applies_when("mailbox.chat_rooms"), "daemon restart");
        assert_eq!(applies_when("mailbox.chat_relay"), "daemon restart");
    }

    #[test]
    fn applies_when_storage_room_id_does_not_require_a_restart() {
        // Unlike mailbox/ai/stream room settings, storage's room is
        // re-resolved live on every `store.*` call (see `storage::store`),
        // so changing it should never tell the user to restart the daemon.
        assert_eq!(applies_when("storage.room_id"), "next service start");
        assert_eq!(applies_when("mailbox.room_id"), "daemon restart");
    }
}
