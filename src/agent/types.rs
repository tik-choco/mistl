//! Shared types: the fixed contract between the LLM client, the mistl tool,
//! the agent loop, and the TUI.

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

/// One OpenAI chat message. Serialized as-is into `/v1/chat/completions`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Assistant turns that requested tools (native tool mode only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    /// Set on `Role::Tool` messages (native tool mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl Message {
    pub fn system(text: impl Into<String>) -> Self {
        Self::plain(Role::System, text)
    }
    pub fn user(text: impl Into<String>) -> Self {
        Self::plain(Role::User, text)
    }
    pub fn assistant(text: impl Into<String>) -> Self {
        Self::plain(Role::Assistant, text)
    }
    pub fn tool_result(call_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: Some(text.into()),
            tool_calls: Vec::new(),
            tool_call_id: Some(call_id.into()),
        }
    }
    fn plain(role: Role, text: impl Into<String>) -> Self {
        Self {
            role,
            content: Some(text.into()),
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }
}

/// OpenAI wire shape: `{"id","type":"function","function":{"name","arguments"}}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type", default = "function_type")]
    pub kind: String,
    pub function: FunctionCall,
}

fn function_type() -> String {
    "function".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    /// JSON-encoded arguments object, exactly as the model produced it.
    pub arguments: String,
}

/// OpenAI wire shape: `{"type":"function","function":{"name","description","parameters"}}`.
#[derive(Debug, Clone, Serialize)]
pub struct ToolSpec {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub function: FunctionSpec,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionSpec {
    pub name: &'static str,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// How tool calls travel between mistan and the model.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ToolMode {
    /// OpenAI `tools` / `tool_calls` (OpenAI, Ollama, LM Studio, vLLM, ...).
    #[default]
    Native,
    /// Tools described in the system prompt; the model answers with fenced
    /// `mistl` blocks. Works with chat providers that do not support tools.
    Prompt,
}

/// Result of one streamed completion.
#[derive(Debug, Clone, Default)]
pub struct AssistantTurn {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
}

/// How risky a mistl invocation is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Safety {
    /// Read-only query; may run without asking.
    ReadOnly,
    /// Changes state (config, daemon, files, network); needs user approval.
    Mutating,
    /// Never run from the agent (interactive, blocks forever, or unparsable).
    Blocked(String),
}

/// Outcome of running one tool.
#[derive(Debug, Clone)]
pub struct ToolOutput {
    /// Text handed back to the model (already truncated / ANSI-stripped).
    pub text: String,
    pub ok: bool,
}

/// Agent -> UI.
#[derive(Debug)]
pub enum AgentEvent {
    /// Streaming assistant text delta (prompt-mode tool blocks included as
    /// they arrive; the UI shows it raw).
    TextDelta(String),
    /// The assistant finished one completion (before any tool runs).
    AssistantDone,
    /// A tool is about to run (after approval, if any).
    ToolStart {
        id: String,
        title: String,
    },
    /// Live output of a running tool (stdout and stderr interleaved, ANSI
    /// stripped, arbitrary chunk boundaries). Display only: the final text
    /// handed to the model arrives in `ToolEnd`.
    ToolOutput {
        id: String,
        chunk: String,
    },
    /// A tool finished.
    ToolEnd {
        id: String,
        ok: bool,
        output: String,
    },
    /// Approval needed. The UI must answer exactly once via `reply`.
    ApprovalRequest {
        title: String,
        reason: String,
        reply: oneshot::Sender<Approval>,
    },
    /// Non-fatal notice (e.g. step limit reached).
    Info(String),
    /// The effective tool mode changed for this session.
    ToolModeChanged(ToolMode),
    /// Result of a model-list request (`error` set on failure).
    Models {
        models: Vec<String>,
        error: Option<String>,
    },
    Error(String),
    /// The whole user turn is over (success, error, or cancel).
    TurnDone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Approval {
    Yes,
    No,
    /// Approve this and every later mutating call in this session.
    Always,
}

/// UI -> agent.
#[derive(Debug)]
pub enum UserCommand {
    Send(String),
    /// Forget the conversation (keeps the system prompt).
    Clear,
    SetModel(String),
    /// Session-only reasoning effort (`None` = do not send the field).
    SetEffort(Option<String>),
    /// List the model ids of mistl's local API (`GET /v1/models`).
    ListModels,
    Mistl(MistlOp),
    /// `!<command>` typed by the user: run it in the workspace shell without
    /// approval, stream it as a tool entry (ToolStart/ToolOutput/ToolEnd, then
    /// TurnDone), and show the command and its output to the model with the
    /// next `Send`.
    Shell(String),
}

/// What the UI knows about the startup directory (header, `/just`, `!`
/// completions). Built by `workspace::Workspace::summary`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WorkspaceSummary {
    /// The startup directory (display form).
    pub root: String,
    /// The justfile in use (display form), when one was found.
    pub justfile: Option<String>,
    /// Public recipes in justfile order (modules flattened as `mod::recipe`).
    pub recipes: Vec<RecipeInfo>,
    /// Why recipes could not be listed (e.g. `just` missing); recipes is empty then.
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecipeInfo {
    /// Name as passed to `just`, e.g. `build` or `docs::serve`.
    pub name: String,
    /// Parameter signature in justfile syntax, e.g. `filter="" *args`.
    pub params: String,
    /// Doc comment, if any.
    pub doc: Option<String>,
}

/// mistl management actions outside the model's tool loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MistlOp {
    /// Report the mistl instance and whether its daemon is running.
    Info,
    /// Start the daemon if it is not running.
    Start,
}
