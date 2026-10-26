//! Natural-language terminal agent for the workspace and mistl daemon.

mod config;
mod llm;
mod mistl_tool;
mod process;
mod prompt;
mod session;
mod tools;
mod tui;
mod types;
mod workspace;

use std::io::Write;

use anyhow::Result;

use crate::agent::types::{AgentEvent, Approval, ToolMode, UserCommand};

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
    /// Non-interactive: answer one prompt on stdout and exit (no TUI).
    /// State-changing commands are refused unless --yes is given.
    #[arg(long, short = 'p')]
    prompt: Option<String>,
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
        auto_approve: cli.yes,
        ..defaults
    };

    let ws = workspace::Workspace::detect(&std::env::current_dir()?, &cfg.just_bin);
    if cli.list_models {
        return list_models(cfg, ws).await;
    }
    match cli.prompt {
        Some(prompt) => headless(cfg, ws, prompt).await,
        None => {
            let mut info = tui::UiInfo::from_config(&cfg);
            info.workspace = ws.summary();
            let handle = session::spawn(cfg, ws)?;
            tui::run(info, handle).await
        }
    }
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
            AgentEvent::ApprovalRequest { title, reply, .. } => {
                // auto_approve never reaches here; refuse without a terminal.
                eprintln!("[refused] {title} (needs approval; re-run with --yes)");
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
