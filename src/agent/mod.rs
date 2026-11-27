//! Natural-language terminal agent for the workspace and mistl daemon.

mod config;
mod llm;
mod mistl_tool;
mod process;
mod prompt;
mod safety;
mod session;
mod tools;
mod tui;
mod types;
mod vm;
mod workspace;

use std::io::Write;

use anyhow::Result;

use crate::agent::types::{AgentEvent, Approval, ApprovalMode, ToolMode, UserCommand};

#[derive(clap::Args)]
pub struct AgentArgs {
    /// Model id
    #[arg(long, short)]
    model: Option<String>,
    /// Tool calling style: native (OpenAI tools) or prompt (fenced blocks)
    #[arg(long, value_parser = parse_tool_mode)]
    tool_mode: Option<ToolMode>,
    /// Reasoning effort sent as `reasoning_effort` (low, medium, high, ...)
    #[arg(long)]
    reasoning_effort: Option<String>,
    /// Run state-changing tools without asking
    #[arg(long, short = 'y')]
    yes: bool,
    /// Approval mode: ask, auto (default), or yolo
    #[arg(long, value_parser = parse_approval_mode)]
    approval: Option<ApprovalMode>,
    /// Model for the auto-mode safety check (default: the agent's model)
    #[arg(long)]
    safety_model: Option<String>,
    /// Safety-check API: auto (probe the decision API, else chat), chat, or decision
    #[arg(long, value_parser = parse_safety_api)]
    safety_api: Option<safety::SafetyApi>,
    /// Non-interactive: answer one prompt on stdout and exit (no TUI).
    /// State-changing commands need auto approval or --yes.
    #[arg(long, short = 'p')]
    prompt: Option<String>,
    /// Run workspace tools in the VM with this server id
    #[arg(long)]
    vm: Option<String>,
    /// Executor node ID (optional when this node knows the VM's HA group)
    #[arg(long, requires = "vm")]
    node: Option<String>,
    /// Print model ids from mistl's local API and exit
    #[arg(long)]
    list_models: bool,
}

fn parse_tool_mode(s: &str) -> Result<ToolMode, String> {
    match s {
        "native" => Ok(ToolMode::Native),
        "prompt" => Ok(ToolMode::Prompt),
        _ => Err("expected `native` or `prompt`".into()),
    }
}

fn parse_approval_mode(s: &str) -> Result<ApprovalMode, String> {
    ApprovalMode::parse(s).ok_or_else(|| "expected `ask`, `auto`, or `yolo`".into())
}

fn parse_safety_api(s: &str) -> Result<safety::SafetyApi, String> {
    safety::SafetyApi::parse(s).ok_or_else(|| "expected `auto`, `chat`, or `decision`".into())
}

fn approval_mode(yes: bool, explicit: Option<ApprovalMode>) -> Result<ApprovalMode> {
    if yes && explicit.is_some_and(|m| m != ApprovalMode::Yolo) {
        anyhow::bail!("--yes conflicts with --approval ask or auto");
    }
    Ok(if yes {
        ApprovalMode::Yolo
    } else {
        explicit.unwrap_or_default()
    })
}

pub fn run(args: AgentArgs) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_async(args))
}

async fn run_async(cli: AgentArgs) -> Result<()> {
    let defaults = config::Config::default();
    let cfg = config::Config {
        model: cli.model.unwrap_or_default(),
        tool_mode: cli.tool_mode.unwrap_or(defaults.tool_mode),
        reasoning_effort: config::normalize_effort(cli.reasoning_effort),
        approval: approval_mode(cli.yes, cli.approval)?,
        safety_model: cli.safety_model,
        safety_api: cli.safety_api.unwrap_or_default(),
        ..defaults
    };

    let vm = match cli.vm {
        Some(id) => Some(vm::Vm::connect(id, cli.node).await?),
        None => None,
    };
    let ws = match &vm {
        Some(vm) => workspace::Workspace::for_vm(vm.clone()),
        None => workspace::Workspace::detect(&std::env::current_dir()?, &cfg.just_bin),
    };
    let result = match (cli.list_models, cli.prompt) {
        (true, _) => list_models(cfg, ws).await,
        (false, Some(prompt)) => headless(cfg, ws, prompt).await,
        (false, None) => {
            async {
                let mut info = tui::UiInfo::from_config(&cfg);
                info.workspace = ws.summary();
                let handle = session::spawn(cfg, ws)?;
                tui::run(info, handle).await
            }
            .await
        }
    };
    if let Some(vm) = vm {
        vm.disconnect().await;
    }
    result
}

async fn list_models(cfg: config::Config, ws: workspace::Workspace) -> Result<()> {
    let mut handle = session::spawn(cfg, ws)?;
    handle.commands.send(UserCommand::ListModels)?;
    while let Some(ev) = handle.events.recv().await {
        match ev {
            AgentEvent::Models { models, error } => {
                if let Some(e) = error {
                    anyhow::bail!("{e}");
                }
                for m in models {
                    println!("{m}");
                }
                return Ok(());
            }
            AgentEvent::Info(m) => eprintln!("[info] {m}"),
            _ => {}
        }
    }
    anyhow::bail!("agent stopped before answering")
}

async fn headless(cfg: config::Config, ws: workspace::Workspace, prompt: String) -> Result<()> {
    let mut handle = session::spawn(cfg, ws)?;
    handle.commands.send(UserCommand::Send(prompt))?;
    let mut out = std::io::stdout();
    let mut failed = false;
    while let Some(ev) = handle.events.recv().await {
        match ev {
            AgentEvent::TextDelta(t) => {
                print!("{t}");
                out.flush()?;
            }
            AgentEvent::AssistantDone => println!(),
            AgentEvent::ToolStart { title, .. } => eprintln!("\n[run] {title}"),
            AgentEvent::ToolOutput { chunk, .. } => {
                let mut err = std::io::stderr().lock();
                err.write_all(chunk.as_bytes())?;
                err.flush()?;
            }
            AgentEvent::ToolEnd { ok, output, .. } => {
                eprintln!(
                    "[{}] {}",
                    if ok { "ok" } else { "failed" },
                    first_line(&output)
                );
            }
            AgentEvent::ApprovalRequest {
                title,
                reason,
                reply,
            } => {
                // Auto mode only asks for what it will not allow by itself.
                let hint = if reason.starts_with("auto mode:") {
                    "run it yourself or re-run with --yes"
                } else {
                    "re-run with --approval auto or --yes"
                };
                eprintln!("[refused] {title} — {reason} (needs approval; {hint})");
                let _ = reply.send(Approval::No);
            }
            AgentEvent::Info(m) => eprintln!("[info] {m}"),
            AgentEvent::ToolModeChanged(mode) => eprintln!(
                "[info] tool mode: {}",
                match mode {
                    ToolMode::Native => "native",
                    ToolMode::Prompt => "prompt",
                }
            ),
            AgentEvent::Error(m) => {
                eprintln!("[error] {m}");
                failed = true;
            }
            AgentEvent::Models { .. } => {}
            AgentEvent::AutoReview {
                title,
                allowed,
                reason,
            } => {
                eprintln!(
                    "[auto] {}: {title} — {reason}",
                    if allowed { "allowed" } else { "denied" }
                );
            }
            AgentEvent::ApprovalModeChanged(mode) => {
                eprintln!("[info] approval: {}", mode.as_str())
            }
            AgentEvent::TurnDone => break,
        }
    }
    if failed {
        anyhow::bail!("agent turn failed");
    }
    Ok(())
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        agent: AgentArgs,
    }

    #[test]
    fn vm_flags_work_with_a_headless_prompt() {
        let args = Cli::try_parse_from([
            "agent", "--vm", "guest", "--node", "executor", "-p", "hello",
        ])
        .unwrap()
        .agent;
        assert_eq!(args.vm.as_deref(), Some("guest"));
        assert_eq!(args.node.as_deref(), Some("executor"));
        assert_eq!(args.prompt.as_deref(), Some("hello"));
        assert!(Cli::try_parse_from(["agent", "--node", "executor"]).is_err());
    }

    #[test]
    fn approval_flags_validate_alias_and_model() {
        assert_eq!(approval_mode(false, None).unwrap(), ApprovalMode::Auto);
        for mode in [ApprovalMode::Ask, ApprovalMode::Auto, ApprovalMode::Yolo] {
            let args = Cli::try_parse_from([
                "agent",
                "--approval",
                mode.as_str(),
                "--safety-model",
                "safety-model",
            ])
            .unwrap()
            .agent;
            assert_eq!(approval_mode(args.yes, args.approval).unwrap(), mode);
            assert_eq!(args.safety_model.as_deref(), Some("safety-model"));
        }
        assert!(Cli::try_parse_from(["agent", "--approval", "invalid"]).is_err());
        for alias in ["-y", "--yes"] {
            let args = Cli::try_parse_from(["agent", alias]).unwrap().agent;
            assert_eq!(
                approval_mode(args.yes, args.approval).unwrap(),
                ApprovalMode::Yolo
            );
            assert!(approval_mode(true, Some(ApprovalMode::Ask)).is_err());
            assert!(approval_mode(true, Some(ApprovalMode::Auto)).is_err());
            let args = Cli::try_parse_from(["agent", alias, "--approval", "yolo"])
                .unwrap()
                .agent;
            assert_eq!(
                approval_mode(args.yes, args.approval).unwrap(),
                ApprovalMode::Yolo
            );
        }
    }
}
