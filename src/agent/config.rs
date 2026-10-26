//! Agent session options. There is no agent config file: the LLM connection
//! (providers, default model, default reasoning effort) is mistl's own
//! `ai.*` configuration, reached through the local OpenAI-compatible API, and
//! everything here comes from `mistl agent` flags.

use crate::agent::types::ToolMode;

#[derive(Debug, Clone)]
pub struct Config {
    /// Raw model id sent to mistl's API; empty lets mistl pick `ai.default_ref`.
    pub model: String,
    pub tool_mode: ToolMode,
    /// OpenAI `reasoning_effort`; `None` omits the field so mistl applies
    /// `ai.default_reasoning_effort`.
    pub reasoning_effort: Option<String>,
    /// Per-invocation timeout for mistl commands.
    pub mistl_timeout_secs: u64,
    /// `just` executable (name on PATH or a full path) for justfile recipes.
    pub just_bin: String,
    /// Timeout for foreground recipes and shell commands run by the agent
    /// (user `!` commands have none; Esc cancels them).
    pub command_timeout_secs: u64,
    /// Max model round-trips per user message.
    pub max_steps: usize,
    /// Run state-changing tools without asking.
    pub auto_approve: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            model: String::new(),
            // Native tools, with session fallback to prompt mode.
            tool_mode: ToolMode::Native,
            reasoning_effort: None,
            mistl_timeout_secs: 60,
            just_bin: "just".into(),
            command_timeout_secs: 600,
            max_steps: 12,
            auto_approve: false,
        }
    }
}

/// Lowercased effort, `None` for empty or `default`.
pub fn normalize_effort(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty() && s != "default")
}
