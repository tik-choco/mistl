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
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            blocks_dir: None,
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
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
    /// Audio codec served over RTSP for relayed shares: "aac" (transcoded
    /// from Opus; what AVPro reliably plays) or "opus" (passthrough).
    pub audio_codec: String,
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
            audio_codec: "aac".into(),
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
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            room_id: None,
            serve_as_bot: true,
        }
    }
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
    /// Upstream OpenAI-compatible endpoint base URL used when providing,
    /// e.g. "http://127.0.0.1:11434/v1" (Ollama) or "https://api.openai.com/v1".
    pub upstream_url: Option<String>,
    /// API key sent to the upstream endpoint (Bearer).
    pub upstream_api_key: Option<String>,
    /// Model requested when a client doesn't specify one.
    pub default_model: Option<String>,
    /// Models advertised in provider_hello. Empty = fetch from upstream
    /// `GET /models` at provide start.
    pub advertised_models: Vec<String>,
    /// Sampling temperature forwarded to the upstream (unset = upstream default).
    pub temperature: Option<f64>,
    /// Listen address of the local OpenAI-compatible API server (`ai serve`).
    pub api_listen: String,
    /// Inactivity timeout for a p2p LLM request (resets on every streamed chunk).
    pub request_timeout_secs: u64,
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

/// Set one config field addressed as `section.field` (e.g.
/// "ai.upstream_url"), returning the updated config. Values round-trip
/// through serde so types are validated against the real Config shape;
/// `null` clears optional fields.
pub fn set_by_path(config: &Config, path: &str, value: serde_json::Value) -> Result<Config> {
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
        "mailbox.room_id" | "ai.room_id" | "stream.relay_room" => {
            // The room only pins once the p2p engine has joined; before any
            // p2p service ran it applies on next start. "daemon restart" is
            // the safe universal answer.
            "daemon restart"
        }
        _ => "next service start",
    }
}

impl Config {
    /// Load config from disk, writing defaults on first run.
    pub fn load() -> Result<Self> {
        let path = config_path()?;
        if !path.exists() {
            let config = Self::default();
            config.save()?;
            return Ok(config);
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        std::fs::write(&path, toml::to_string_pretty(self)?)
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_by_path_updates_a_string_option() {
        let config = Config::default();
        let updated = set_by_path(&config, "ai.upstream_url", json!("http://x/v1")).unwrap();
        assert_eq!(updated.ai.upstream_url.as_deref(), Some("http://x/v1"));
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
}
