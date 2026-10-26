//! Agent loop: model/tool round-trips and direct workspace shell commands.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::agent::config::{self, Config};
use crate::agent::llm::{LlmClient, is_tools_unsupported};
use crate::agent::mistl_tool;
use crate::agent::prompt;
use crate::agent::tools::{self, Toolbox};
use crate::agent::types::{
    AgentEvent, Approval, Message, MistlOp, Role, Safety, ToolCall, ToolMode, ToolOutput,
    UserCommand,
};
use crate::agent::workspace::Workspace;

pub struct AgentHandle {
    pub commands: mpsc::UnboundedSender<UserCommand>,
    pub events: mpsc::UnboundedReceiver<AgentEvent>,
    cancel: Arc<Mutex<CancellationToken>>,
}

impl AgentHandle {
    /// Abort the in-flight user turn (model stream and running tool).
    pub fn cancel_turn(&self) {
        self.cancel.lock().unwrap().cancel();
    }
}

struct Agent {
    cfg: Config,
    llm: LlmClient,
    toolbox: Toolbox,
    workspace_prompt: String,
    pending_shell: PendingShell,
    shell_sequence: u64,
    messages: Vec<Message>,
    mode: ToolMode,
    catalog: String,
    auto_approve: bool,
    endpoint_ready: bool,
    tx: mpsc::UnboundedSender<AgentEvent>,
    model_catalog: Option<CachedModels>,
}

struct CachedModels {
    fetched: Instant,
    models: Vec<String>,
}

#[derive(Default)]
struct PendingShell(Vec<String>);

impl PendingShell {
    fn push(&mut self, command: &str, output: &ToolOutput) {
        self.0.push(tools::redact_output(&format!(
            "[The user ran a shell command in the workspace]\n$ {command}\n{}",
            output.text
        )));
    }

    fn prepend(&mut self, text: String) -> String {
        if self.0.is_empty() {
            return text;
        }
        let mut notes = std::mem::take(&mut self.0).join("\n\n");
        notes.push_str("\n\n");
        notes.push_str(&text);
        notes
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

/// Rewrite native tool history without retaining any tool-only wire fields.
fn prompt_history(messages: &[Message]) -> Vec<Message> {
    let mut history = Vec::new();
    let mut titles = HashMap::new();
    let mut results = Vec::new();
    for message in messages {
        if message.role == Role::Tool {
            let title = message
                .tool_call_id
                .as_ref()
                .and_then(|id| titles.get(id))
                .cloned()
                .unwrap_or_else(|| "tool (unknown call)".into());
            let text = message.content.clone().unwrap_or_default();
            // Native history stores only text. Commands carry an exit code;
            // file and process tools report failures with an error prefix.
            let first = text.lines().next().unwrap_or_default();
            let ok = if first.starts_with("exit code:") {
                first == "exit code: 0" && !text.contains("[note]")
            } else {
                !first.starts_with("error:")
                    && !first.starts_with("refused:")
                    && first != "cancelled"
                    && first != "the user declined to run this command"
            };
            results.push((title, ToolOutput { text, ok }));
            continue;
        }
        if !results.is_empty() {
            history.push(Message::user(prompt::format_prompt_results(&results)));
            results.clear();
        }
        let mut content = message.content.clone().unwrap_or_default();
        if message.role == Role::Assistant {
            for call in &message.tool_calls {
                let (title, fence, command) =
                    match tools::parse_call(&call.function.name, &call.function.arguments) {
                        Ok(inv) => {
                            let title = tools::title(&inv);
                            let (fence, command) = tools::prompt_render(&inv);
                            (title, fence, command)
                        }
                        Err(e) => (
                            call.function.name.clone(),
                            match call.function.name.as_str() {
                                mistl_tool::TOOL_MISTL => "mistl",
                                mistl_tool::TOOL_HELP => "mistl-help",
                                _ => "tool",
                            },
                            format!(
                                "# invalid tool call: {e}\n# {} {}",
                                call.function.name, call.function.arguments
                            ),
                        ),
                    };
                titles.insert(call.id.clone(), title);
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str(&format!("```{fence}\n{command}\n```"));
            }
        }
        history.push(Message {
            role: message.role,
            content: Some(content),
            tool_calls: Vec::new(),
            tool_call_id: None,
        });
    }
    if !results.is_empty() {
        history.push(Message::user(prompt::format_prompt_results(&results)));
    }
    history
}

/// Spawn the agent task on the current tokio runtime.
pub fn spawn(cfg: Config, workspace: Workspace) -> Result<AgentHandle> {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<UserCommand>();
    let (ev_tx, ev_rx) = mpsc::unbounded_channel::<AgentEvent>();
    let cancel = Arc::new(Mutex::new(CancellationToken::new()));

    let mut agent = Agent {
        llm: LlmClient::new(&cfg),
        workspace_prompt: workspace.prompt_section(),
        toolbox: Toolbox::new(&cfg, workspace),
        pending_shell: PendingShell::default(),
        shell_sequence: 1,
        messages: Vec::new(),
        mode: cfg.tool_mode,
        catalog: String::new(),
        auto_approve: cfg.auto_approve,
        endpoint_ready: false,
        tx: ev_tx,
        model_catalog: None,
        cfg,
    };
    let cancel_slot = cancel.clone();
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            // Every command gets a fresh token so Esc can interrupt slow ones.
            let token = CancellationToken::new();
            *cancel_slot.lock().unwrap() = token.clone();
            match cmd {
                UserCommand::Send(text) => {
                    agent.turn(text, &token).await;
                    let _ = agent.tx.send(AgentEvent::TurnDone);
                }
                UserCommand::Clear => agent.clear(),
                UserCommand::SetModel(m) => {
                    agent.cfg.model = m.clone();
                    agent.llm.set_model(m.clone());
                    agent.send(AgentEvent::Info(format!("model: {m}")));
                }
                UserCommand::SetEffort(e) => {
                    let e = config::normalize_effort(e);
                    agent.cfg.reasoning_effort = e.clone();
                    agent.llm.set_reasoning_effort(e.clone());
                    agent.send(AgentEvent::Info(format!(
                        "reasoning effort: {}",
                        e.as_deref().unwrap_or("default")
                    )));
                }
                UserCommand::ListModels => agent.list_models(&token).await,
                UserCommand::Mistl(op) => {
                    agent.mistl_op(op, &token).await;
                    agent.send(AgentEvent::TurnDone);
                }
                UserCommand::Shell(command) => {
                    agent.shell(command, &token).await;
                    agent.send(AgentEvent::TurnDone);
                }
            }
        }
        agent.toolbox.processes.stop_all();
    });

    Ok(AgentHandle {
        commands: cmd_tx,
        events: ev_rx,
        cancel,
    })
}

impl Agent {
    fn send(&self, ev: AgentEvent) {
        let _ = self.tx.send(ev);
    }

    fn report(&self, e: anyhow::Error) {
        if e.to_string() == "cancelled" {
            self.send(AgentEvent::Info("cancelled".into()));
        } else {
            self.send(AgentEvent::Error(format!("{e:#}")));
        }
    }

    fn system_prompt(&self) -> String {
        prompt::system_prompt(self.mode, &self.catalog, &self.workspace_prompt)
    }

    fn clear(&mut self) {
        self.messages.truncate(1);
        self.pending_shell.clear();
    }

    async fn shell(&mut self, command: String, token: &CancellationToken) {
        if command.trim().is_empty() {
            self.send(AgentEvent::Info("enter a command after !".into()));
            return;
        }
        // A fresh id keeps multiple direct commands distinct in the UI.
        let id = format!("shell-{}", self.shell_sequence);
        self.shell_sequence += 1;
        self.send(AgentEvent::ToolStart {
            id: id.clone(),
            title: tools::redact_output(&format!("! {command}")),
        });
        let tx = self.tx.clone();
        let live_id = id.clone();
        let mut on_output = move |chunk: &str| {
            let _ = tx.send(AgentEvent::ToolOutput {
                id: live_id.clone(),
                chunk: chunk.into(),
            });
        };
        let output = self
            .toolbox
            .run_user_shell(&command, token, &mut on_output)
            .await;
        self.pending_shell.push(&command, &output);
        self.send(AgentEvent::ToolEnd {
            id,
            ok: output.ok,
            output: output.text,
        });
    }

    /// Discover the local API through the same IPC boundary as the CLI.
    async fn ensure_endpoint(&mut self, token: &CancellationToken) -> Result<()> {
        if self.endpoint_ready {
            return Ok(());
        }
        if self.toolbox.mistl.ensure_daemon(token).await? {
            self.send(AgentEvent::Info("started the mistl daemon".into()));
        }
        let listen = self.toolbox.mistl.serve_start(token).await?;
        let url = format!("http://{listen}/v1");
        self.send(AgentEvent::Info(format!("AI network: via mistl ({url})")));
        self.llm.set_base_url(url);
        self.endpoint_ready = true;
        Ok(())
    }

    async fn list_models(&mut self, token: &CancellationToken) {
        let result = async {
            if let Some(cached) = &self.model_catalog
                && cached.fetched.elapsed() < Duration::from_secs(10)
            {
                return Ok(cached.models.clone());
            }
            self.ensure_endpoint(token).await?;
            let models = self.llm.list_models(token).await?;
            self.model_catalog = Some(CachedModels {
                fetched: Instant::now(),
                models: models.clone(),
            });
            Ok::<_, anyhow::Error>(models)
        }
        .await;
        match result {
            Ok(models) => self.send(AgentEvent::Models {
                models,
                error: None,
            }),
            Err(e) => self.send(AgentEvent::Models {
                models: Vec::new(),
                error: Some(format!("{e:#}")),
            }),
        }
    }

    async fn mistl_op(&mut self, op: MistlOp, token: &CancellationToken) {
        match op {
            MistlOp::Info => {
                // Status includes a dashboard session URL, so do not render it.
                match self.toolbox.mistl.daemon_status(token).await {
                    Ok(_) => self.send(AgentEvent::Info(format!(
                        "mistl instance {}: daemon running",
                        crate::runtime::context().instance,
                    ))),
                    Err(e) if e.to_string().contains("daemon is not running") => {
                        self.send(AgentEvent::Info(format!(
                            "mistl instance {}: daemon stopped; use /mistl start",
                            crate::runtime::context().instance,
                        )));
                    }
                    Err(e) => self.report(e),
                }
            }
            MistlOp::Start => match self.toolbox.mistl.ensure_daemon(token).await {
                Ok(started) => {
                    if started {
                        self.endpoint_ready = false;
                        self.model_catalog = None;
                    }
                    self.send(AgentEvent::Info(if started {
                        "started the mistl daemon".into()
                    } else {
                        "the mistl daemon is already running".into()
                    }));
                }
                Err(e) => self.report(e),
            },
        }
    }

    fn fallback_to_prompt(&mut self, err: &anyhow::Error) -> bool {
        if self.mode != ToolMode::Native || !is_tools_unsupported(err) {
            return false;
        }
        self.mode = ToolMode::Prompt;
        self.messages = prompt_history(&self.messages);
        self.messages[0] = Message::system(self.system_prompt());
        self.send(AgentEvent::Info(
            "the AI provider does not support tools; switched to prompt mode".into(),
        ));
        self.send(AgentEvent::ToolModeChanged(self.mode));
        true
    }

    async fn turn(&mut self, text: String, token: &CancellationToken) {
        if let Err(e) = self.ensure_endpoint(token).await {
            self.report(e);
            return;
        }
        if self.messages.is_empty() {
            self.catalog = tokio::select! {
                _ = token.cancelled() => {
                    self.send(AgentEvent::Info("cancelled".into()));
                    return;
                }
                catalog = self.toolbox.mistl.catalog() => catalog,
            };
            let sys = self.system_prompt();
            self.messages.push(Message::system(sys));
        }
        let text = self.pending_shell.prepend(text);
        self.messages.push(Message::user(text));

        for _ in 0..self.cfg.max_steps {
            let turn = loop {
                let specs = if self.mode == ToolMode::Native {
                    tools::specs(&self.toolbox.workspace)
                } else {
                    Vec::new()
                };
                let tx = self.tx.clone();
                let res = self
                    .llm
                    .complete(
                        &self.messages,
                        &specs,
                        &mut |d| {
                            let _ = tx.send(AgentEvent::TextDelta(d.to_string()));
                        },
                        token,
                    )
                    .await;
                match res {
                    Ok(t) => break t,
                    Err(e) if !token.is_cancelled() && self.fallback_to_prompt(&e) => continue,
                    Err(e) => {
                        if token.is_cancelled() || e.to_string() == "cancelled" {
                            self.send(AgentEvent::Info("cancelled".into()));
                        } else {
                            self.send(AgentEvent::Error(format!("{e:#}")));
                        }
                        return;
                    }
                };
            };
            self.send(AgentEvent::AssistantDone);

            let native = self.mode == ToolMode::Native;
            let calls: Vec<ToolCall> = if native {
                self.messages.push(Message {
                    role: Role::Assistant,
                    content: (!turn.content.is_empty()).then(|| turn.content.clone()),
                    tool_calls: turn.tool_calls.clone(),
                    tool_call_id: None,
                });
                turn.tool_calls
            } else {
                self.messages.push(Message::assistant(turn.content.clone()));
                prompt::parse_prompt_calls(&turn.content)
            };
            if calls.is_empty() {
                return;
            }

            // (call id, title, output); stops early if the turn is cancelled.
            let mut results: Vec<(String, String, ToolOutput)> = Vec::new();
            for call in &calls {
                let (title, out) = self.run_call(call, token).await;
                results.push((call.id.clone(), title, out));
                if token.is_cancelled() {
                    break;
                }
            }

            if native {
                // Every tool_call id needs a response, even if cancelled.
                for call in &calls {
                    let text = results
                        .iter()
                        .find(|(id, _, _)| *id == call.id)
                        .map(|(_, _, o)| o.text.clone())
                        .unwrap_or_else(|| "cancelled".into());
                    self.messages
                        .push(Message::tool_result(call.id.clone(), text));
                }
            } else {
                let pairs: Vec<(String, ToolOutput)> = results
                    .iter()
                    .map(|(_, t, o)| (t.clone(), o.clone()))
                    .collect();
                self.messages
                    .push(Message::user(prompt::format_prompt_results(&pairs)));
            }

            if token.is_cancelled() {
                self.send(AgentEvent::Info("cancelled".into()));
                return;
            }
        }
        self.send(AgentEvent::Info(format!(
            "step limit reached ({} model calls); send another message to continue",
            self.cfg.max_steps
        )));
    }

    async fn run_call(
        &mut self,
        call: &ToolCall,
        token: &CancellationToken,
    ) -> (String, ToolOutput) {
        let fail = |text: String| ToolOutput {
            text: tools::redact_output(&text),
            ok: false,
        };
        let inv = match tools::parse_call(&call.function.name, &call.function.arguments) {
            Ok(i) => i,
            Err(e) => {
                return (call.function.name.clone(), fail(format!("error: {e:#}")));
            }
        };
        let title = tools::title(&inv);
        match tools::classify(&inv, &self.toolbox.workspace) {
            Safety::ReadOnly => {}
            Safety::Blocked(r) => return (title, fail(format!("refused: {r}"))),
            Safety::Mutating => {
                if !self.auto_approve {
                    let (reply, rx) = oneshot::channel();
                    self.send(AgentEvent::ApprovalRequest {
                        title: title.clone(),
                        reason: tools::approval_reason(&inv).into(),
                        reply,
                    });
                    let answer = tokio::select! {
                        _ = token.cancelled() => Approval::No,
                        a = rx => a.unwrap_or(Approval::No),
                    };
                    match answer {
                        Approval::Yes => {}
                        Approval::Always => self.auto_approve = true,
                        Approval::No => {
                            let text = if token.is_cancelled() {
                                "cancelled"
                            } else {
                                "the user declined to run this command"
                            };
                            return (title, fail(text.into()));
                        }
                    }
                }
            }
        }
        self.send(AgentEvent::ToolStart {
            id: call.id.clone(),
            title: title.clone(),
        });
        let tx = self.tx.clone();
        let id = call.id.clone();
        let mut on_output = move |chunk: &str| {
            let _ = tx.send(AgentEvent::ToolOutput {
                id: id.clone(),
                chunk: chunk.into(),
            });
        };
        let out = self.toolbox.run(&inv, token, &mut on_output).await;
        self.send(AgentEvent::ToolEnd {
            id: call.id.clone(),
            ok: out.ok,
            output: out.text.clone(),
        });
        (title, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::types::FunctionCall;

    fn workspace() -> Workspace {
        Workspace {
            root: ".".into(),
            justfile: None,
            just_bin: "just".into(),
        }
    }

    fn call(id: &str, name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.into(),
            kind: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: arguments.into(),
            },
        }
    }

    #[test]
    fn converts_native_history_to_prompt() {
        let calls = vec![
            call(
                "run",
                "mistl",
                r#"{"args":["mistl","ai","chat","hello \"world\"","C:\\tmp\\folder name"]}"#,
            ),
            call(
                "help",
                "mistl_help",
                r#"{"command":"mistl store folder-share --help"}"#,
            ),
            call("root", "mistl_help", "{}"),
        ];
        let input = vec![
            Message::system("system"),
            Message::user("question"),
            Message {
                role: Role::Assistant,
                content: Some("Checking.".into()),
                tool_calls: calls.clone(),
                tool_call_id: None,
            },
            Message::tool_result("run", "exit code: 0\n--- stdout ---\nhello"),
            Message::tool_result("help", "error: refused"),
            Message::tool_result("root", "cancelled"),
            Message::assistant("answer"),
            Message::user("next"),
        ];
        let converted = prompt_history(&input);
        assert_eq!(converted.len(), 6);
        assert_eq!(&converted[..2], &input[..2]);
        let content = converted[2].content.as_deref().unwrap();
        assert!(content.starts_with("Checking.\n\n```mistl\n"));
        assert!(content.contains("```mistl-help\nstore folder-share\n```"));
        assert!(content.ends_with("```mistl-help\n\n```"));
        // The parser ignores empty help blocks; the serialized history still keeps them.
        let reconstructed = prompt::parse_prompt_calls(content);
        for (original, rebuilt) in calls.iter().zip(&reconstructed) {
            assert_eq!(
                mistl_tool::parse_call(&original.function.name, &original.function.arguments)
                    .unwrap(),
                mistl_tool::parse_call(&rebuilt.function.name, &rebuilt.function.arguments)
                    .unwrap(),
            );
        }
        assert_eq!(reconstructed.len(), 2);
        let pairs = vec![
            (
                mistl_tool::title(
                    &mistl_tool::parse_call("mistl", &calls[0].function.arguments).unwrap(),
                ),
                ToolOutput {
                    text: input[3].content.clone().unwrap(),
                    ok: true,
                },
            ),
            (
                "mistl store folder-share --help".into(),
                ToolOutput {
                    text: "error: refused".into(),
                    ok: false,
                },
            ),
            (
                "mistl --help".into(),
                ToolOutput {
                    text: "cancelled".into(),
                    ok: false,
                },
            ),
        ];
        assert_eq!(
            converted[3],
            Message::user(prompt::format_prompt_results(&pairs))
        );
        assert_eq!(&converted[4..], &input[6..]);
        for message in &converted {
            assert_ne!(message.role, Role::Tool);
            assert!(message.content.is_some());
            assert!(message.tool_calls.is_empty());
            assert!(message.tool_call_id.is_none());
        }
        assert_eq!(prompt_history(&converted), converted);
        assert_eq!(input[2].tool_calls, calls);
        let wire = serde_json::to_value(&converted).unwrap();
        for message in wire.as_array().unwrap() {
            assert!(message["content"].is_string());
            assert!(message.get("tool_calls").is_none());
            assert!(message.get("tool_call_id").is_none());
        }
    }

    #[test]
    fn conversion_handles_missing_content_and_invalid_calls() {
        let input = vec![
            Message {
                role: Role::Assistant,
                content: None,
                tool_calls: vec![call("bad", "mistl", "{bad")],
                tool_call_id: Some("stray".into()),
            },
            Message::tool_result("bad", "error: invalid arguments"),
            Message {
                role: Role::User,
                content: None,
                tool_calls: Vec::new(),
                tool_call_id: None,
            },
            Message {
                role: Role::Tool,
                content: None,
                tool_calls: Vec::new(),
                tool_call_id: None,
            },
        ];
        let converted = prompt_history(&input);
        assert!(
            converted[0]
                .content
                .as_ref()
                .unwrap()
                .contains("```mistl\n# invalid tool call:")
        );
        assert!(
            converted[1]
                .content
                .as_ref()
                .unwrap()
                .contains("$ mistl\nerror: invalid arguments")
        );
        assert_eq!(converted[2], Message::user(""));
        assert!(
            converted[3]
                .content
                .as_ref()
                .unwrap()
                .contains("$ tool (unknown call)")
        );
        assert!(prompt_history(&[]).is_empty());
        for message in converted {
            assert_ne!(message.role, Role::Tool);
            assert!(message.content.is_some());
            assert!(message.tool_calls.is_empty());
            assert!(message.tool_call_id.is_none());
        }
    }

    #[test]
    fn fallback_changes_mode_once_and_rebuilds_system_prompt() {
        let cfg = Config::default();
        let (tx, mut events) = mpsc::unbounded_channel();
        let mut agent = Agent {
            llm: LlmClient::new(&cfg),
            workspace_prompt: "WORKSPACE".into(),
            toolbox: Toolbox::new(&cfg, workspace()),
            pending_shell: PendingShell::default(),
            shell_sequence: 1,
            model_catalog: None,
            mode: cfg.tool_mode,
            catalog: "CATALOG".into(),
            messages: vec![Message::system("native"), Message::user("question")],
            auto_approve: false,
            endpoint_ready: true,
            tx,
            cfg,
        };
        assert!(!agent.fallback_to_prompt(&anyhow::anyhow!("tools_unsupported")));
        let err = anyhow::Error::new(crate::ai::openai::UpstreamHttpError {
            status: 400,
            label: "test",
            detail: r#"{"error":{"code":"tools_unsupported"}}"#.into(),
        });
        assert!(agent.fallback_to_prompt(&err));
        assert_eq!(agent.mode, ToolMode::Prompt);
        assert_eq!(
            agent.messages[0],
            Message::system(prompt::system_prompt(
                ToolMode::Prompt,
                "CATALOG",
                "WORKSPACE"
            ))
        );
        assert_eq!(agent.messages[1], Message::user("question"));
        assert!(matches!(events.try_recv().unwrap(), AgentEvent::Info(s)
            if s == "the AI provider does not support tools; switched to prompt mode"));
        assert!(matches!(
            events.try_recv().unwrap(),
            AgentEvent::ToolModeChanged(ToolMode::Prompt)
        ));
        agent.messages.truncate(1);
        assert!(!agent.fallback_to_prompt(&err));
        assert_eq!(agent.mode, ToolMode::Prompt);
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn converts_workspace_history_and_keeps_options() {
        let calls = vec![
            call("just", "just", r#"{"recipe":"build","args":["--release"]}"#),
            call("bg", "just", r#"{"recipe":"serve","background":true}"#),
            call("ls", "list_dir", r#"{"path":"target"}"#),
            call(
                "find",
                "find_files",
                r#"{"glob":"**/*.exe","include_ignored":true}"#,
            ),
            call(
                "grep",
                "grep",
                r#"{"pattern":"foo","case_insensitive":true}"#,
            ),
            call(
                "read",
                "read_file",
                r#"{"path":"src/main.rs","offset":20,"limit":10}"#,
            ),
            call("sh", "shell", r#"{"command":"echo hi","background":true}"#),
            call("ps", "process_list", "{}"),
            call("out", "process_output", r#"{"id":2,"tail_bytes":100}"#),
            call("stop", "process_stop", r#"{"id":2}"#),
        ];
        let mut input = vec![Message {
            role: Role::Assistant,
            content: None,
            tool_calls: calls.clone(),
            tool_call_id: None,
        }];
        input.push(Message::tool_result("just", "exit code: 0"));
        input.push(Message::tool_result("bg", "started process 1: just serve"));
        input.push(Message::tool_result("ls", "error: directory missing"));
        let converted = prompt_history(&input);
        let rebuilt = prompt::parse_prompt_calls(converted[0].content.as_deref().unwrap());
        assert_eq!(rebuilt.len(), calls.len());
        for (original, rebuilt) in calls.iter().zip(rebuilt) {
            assert_eq!(
                tools::parse_call(&original.function.name, &original.function.arguments).unwrap(),
                tools::parse_call(&rebuilt.function.name, &rebuilt.function.arguments).unwrap()
            );
        }
        let results = converted[1].content.as_deref().unwrap();
        assert!(results.starts_with("[tool results]\n$ just build --release"));
        assert!(results.contains("$ just serve &\nstarted process 1: just serve\nstatus: ok"));
        assert!(results.contains("$ list_dir target\nerror: directory missing\nstatus: failed"));
        assert_eq!(prompt_history(&converted), converted);
    }

    #[test]
    fn pending_shell_context_is_ordered_and_consumed_once() {
        let mut pending = PendingShell::default();
        assert_eq!(pending.prepend("question".into()), "question");
        pending.push(
            "echo one",
            &ToolOutput {
                ok: true,
                text: "exit code: 0\n--- stdout ---\none".into(),
            },
        );
        pending.push(
            "bad-command",
            &ToolOutput {
                ok: false,
                text: "exit code: 1".into(),
            },
        );
        assert_eq!(
            pending.prepend("explain".into()),
            "[The user ran a shell command in the workspace]\n$ echo one\nexit code: 0\n--- stdout ---\none\n\n[The user ran a shell command in the workspace]\n$ bad-command\nexit code: 1\n\nexplain"
        );
        assert_eq!(pending.prepend("next".into()), "next");
        pending.push(
            "echo hi",
            &ToolOutput {
                ok: true,
                text: "hi".into(),
            },
        );
        pending.clear();
        assert_eq!(pending.prepend("clean".into()), "clean");
    }

    #[tokio::test]
    async fn user_shell_redacts_live_final_and_pending_model_context() {
        let cfg = Config::default();
        let (tx, mut events) = mpsc::unbounded_channel();
        let mut agent = Agent {
            llm: LlmClient::new(&cfg),
            toolbox: Toolbox::new(&cfg, workspace()),
            workspace_prompt: String::new(),
            pending_shell: PendingShell::default(),
            shell_sequence: 1,
            model_catalog: None,
            messages: Vec::new(),
            mode: cfg.tool_mode,
            catalog: String::new(),
            auto_approve: false,
            endpoint_ready: true,
            tx,
            cfg,
        };
        #[cfg(windows)]
        let command = "[Console]::Out.Write('dashboard: http://localhost/?tok'); Start-Sleep -Milliseconds 100; [Console]::Out.WriteLine('en=secret')";
        #[cfg(not(windows))]
        let command = "printf 'dashboard: http://localhost/?tok'; sleep 0.1; printf 'en=secret\\n'";
        agent.shell(command.into(), &CancellationToken::new()).await;
        let mut live = String::new();
        let mut final_text = None;
        while let Ok(event) = events.try_recv() {
            match event {
                AgentEvent::ToolOutput { chunk, .. } => live.push_str(&chunk),
                AgentEvent::ToolEnd { output, ok, .. } => {
                    assert!(ok);
                    final_text = Some(output);
                }
                _ => {}
            }
        }
        let context = agent.pending_shell.prepend("explain".into());
        for text in [&live, &final_text.unwrap(), &context] {
            assert!(!text.contains("token=secret"), "{text}");
            assert!(text.contains("?token=<redacted>"), "{text}");
        }
        let call = call("bad", "just", r#"{"recipe":"missing?token=secret"}"#);
        let (_, result) = agent.run_call(&call, &CancellationToken::new()).await;
        assert!(!result.text.contains("token=secret"));
        assert!(result.text.contains("?token=<redacted>"));
    }

    #[test]
    fn clear_drops_pending_shell_context() {
        let cfg = Config::default();
        let (tx, _events) = mpsc::unbounded_channel();
        let mut agent = Agent {
            llm: LlmClient::new(&cfg),
            toolbox: Toolbox::new(&cfg, workspace()),
            workspace_prompt: "WORKSPACE".into(),
            pending_shell: PendingShell::default(),
            shell_sequence: 1,
            model_catalog: None,
            messages: vec![Message::system("system"), Message::user("old")],
            mode: cfg.tool_mode,
            catalog: String::new(),
            auto_approve: false,
            endpoint_ready: true,
            tx,
            cfg,
        };
        let output = ToolOutput {
            ok: true,
            text: "hi".into(),
        };
        agent.pending_shell.push("echo hi", &output);
        agent.clear();
        assert_eq!(agent.messages, vec![Message::system("system")]);
        assert_eq!(agent.pending_shell.prepend("clean".into()), "clean");
        assert!(agent.system_prompt().contains("WORKSPACE"));
    }

    #[tokio::test]
    async fn workspace_commands_request_their_specific_approval() {
        let cfg = Config::default();
        let (tx, mut events) = mpsc::unbounded_channel();
        let mut ws = workspace();
        ws.justfile = Some(crate::agent::workspace::Justfile {
            path: "justfile".into(),
            dir: ".".into(),
            default_recipe: None,
            source: String::new(),
            error: None,
            recipes: vec![crate::agent::workspace::Recipe {
                name: "build".into(),
                doc: None,
                params: vec![],
                private: false,
            }],
        });
        let mut agent = Agent {
            llm: LlmClient::new(&cfg),
            toolbox: Toolbox::new(&cfg, ws),
            workspace_prompt: "WORKSPACE".into(),
            pending_shell: PendingShell::default(),
            shell_sequence: 1,
            model_catalog: None,
            messages: Vec::new(),
            mode: cfg.tool_mode,
            catalog: String::new(),
            auto_approve: false,
            endpoint_ready: true,
            tx,
            cfg,
        };
        let token = CancellationToken::new();
        for (name, args, expected_title, expected_reason) in [
            (
                "just",
                r#"{"recipe":"build"}"#,
                "just build",
                "runs a justfile recipe",
            ),
            (
                "shell",
                r#"{"command":"echo hi"}"#,
                "$ echo hi",
                "runs a shell command",
            ),
            (
                "process_stop",
                r#"{"id":2}"#,
                "process_stop 2",
                "stops a background process",
            ),
        ] {
            let call = call("test", name, args);
            let run = agent.run_call(&call, &token);
            let refuse = async {
                match events.recv().await.unwrap() {
                    AgentEvent::ApprovalRequest {
                        title,
                        reason,
                        reply,
                    } => {
                        assert_eq!(title, expected_title);
                        assert_eq!(reason, expected_reason);
                        reply.send(Approval::No).unwrap();
                    }
                    event => panic!("unexpected event: {event:?}"),
                }
            };
            let ((title, output), ()) = tokio::join!(run, refuse);
            assert_eq!(title, expected_title);
            assert!(!output.ok && output.text.contains("declined"));
            assert!(events.try_recv().is_err());
        }
        // Both failures return before dispatch, without touching the stubs.
        for (name, args, error) in [
            (
                "just",
                r#"{"recipe":"missing"}"#,
                "available recipes: build",
            ),
            ("shell", "{bad", "invalid tool arguments JSON"),
        ] {
            let (_, output) = agent.run_call(&call("bad", name, args), &token).await;
            assert!(!output.ok && output.text.contains(error));
            assert!(events.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn cancelled_startup_leaves_history_empty_for_retry() {
        let cfg = Config::default();
        let (tx, mut events) = mpsc::unbounded_channel();
        let mut agent = Agent {
            llm: LlmClient::new(&cfg),
            workspace_prompt: "WORKSPACE".into(),
            toolbox: Toolbox::new(&cfg, workspace()),
            pending_shell: PendingShell::default(),
            shell_sequence: 1,
            model_catalog: None,
            messages: Vec::new(),
            mode: cfg.tool_mode,
            catalog: String::new(),
            auto_approve: false,
            endpoint_ready: false,
            tx,
            cfg,
        };
        for _ in 0..2 {
            let token = CancellationToken::new();
            token.cancel();
            agent.turn("question".into(), &token).await;
            assert!(agent.messages.is_empty());
            assert!(!agent.endpoint_ready);
            assert!(
                matches!(events.try_recv().unwrap(), AgentEvent::Info(reason)
                if reason == "cancelled")
            );
            assert!(events.try_recv().is_err());
        }
    }
}
