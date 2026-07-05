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
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            rtsp_url: "rtsp://127.0.0.1:8554/stream".into(),
            frame_rate: 30,
            audio_capture: false,
            capture_backend: "native".into(),
            max_width: 1920,
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
    /// Room id for the AI network. mistlib supports one room per process,
    /// so this defaults to the mailbox room; set both to the same value to
    /// join an existing mistai (tc-mistllm etc.) room.
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
