//! Terminal UI.

mod app;
mod clipboard;
mod input;
mod text;
mod ui;

use std::io::{Stdout, Write, stdout};
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture, Event,
    EventStream,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::io::AsyncWriteExt;

use crate::agent::config::Config;
use crate::agent::session::AgentHandle;

/// Static facts shown in the header / status bar.
#[derive(Debug, Clone)]
pub struct UiInfo {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub tool_mode: crate::agent::types::ToolMode,
    pub approval: crate::agent::types::ApprovalMode,
    pub instance: String,
    /// The startup directory and its justfile (set by the agent entry point after detection).
    pub workspace: crate::agent::types::WorkspaceSummary,
}

impl UiInfo {
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            reasoning_effort: cfg.reasoning_effort.clone(),
            model: cfg.model.clone(),
            instance: crate::runtime::context().instance.clone(),
            tool_mode: cfg.tool_mode,
            approval: cfg.approval,
            workspace: Default::default(),
        }
    }
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(
        stdout(),
        DisableMouseCapture,
        DisableBracketedPaste,
        LeaveAlternateScreen
    );
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    if let Err(e) = execute!(
        stdout(),
        EnterAlternateScreen,
        EnableBracketedPaste,
        EnableMouseCapture
    ) {
        restore_terminal();
        return Err(e.into());
    }
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        prev(info);
    }));
    match Terminal::new(CrosstermBackend::new(stdout())) {
        Ok(terminal) => Ok(terminal),
        Err(e) => {
            restore_terminal();
            Err(e.into())
        }
    }
}

pub async fn run(info: UiInfo, handle: AgentHandle) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let result = event_loop(&mut terminal, info, handle).await;
    restore_terminal();
    let _ = terminal.show_cursor();
    result
}

/// Upper bound on queued agent events applied between two redraws.
const MAX_EVENTS_PER_FRAME: usize = 4096;

enum CopyResult {
    Clipboard {
        count: usize,
        result: Result<clipboard::Method>,
        osc: Option<String>,
    },
    Export(Result<std::path::PathBuf>),
}

async fn export_transcript(path: &Path, text: &str) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    file.write_all(text.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    info: UiInfo,
    mut handle: AgentHandle,
) -> Result<()> {
    let mut app = app::App::new(&info);
    let mut events = EventStream::new();
    // Only one clipboard/export task at a time, bounding queued transcript copies.
    let mut copying = false;
    let (copy_tx, mut copy_rx) = tokio::sync::mpsc::channel(1);
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    while !app.quit {
        for eff in app.take_effects() {
            match eff {
                app::Effect::Command(c) => {
                    let _ = handle.commands.send(c);
                }
                app::Effect::CancelTurn => handle.cancel_turn(),
                app::Effect::Copy(text) if !copying => {
                    copying = true;
                    let tx = copy_tx.clone();
                    tokio::spawn(async move {
                        let count = text.chars().count();
                        let result = clipboard::copy(&text).await;
                        let osc = clipboard::osc52(&text);
                        let _ = tx.send(CopyResult::Clipboard { count, result, osc }).await;
                    });
                }
                app::Effect::Export { path, text } if !copying => {
                    copying = true;
                    let tx = copy_tx.clone();
                    tokio::spawn(async move {
                        let result = export_transcript(&path, &text).await.map(|()| path);
                        let _ = tx.send(CopyResult::Export(result)).await;
                    });
                }
                app::Effect::Copy(_) | app::Effect::Export { .. } => {
                    app.set_hint("Copy/export already in progress.")
                }
                app::Effect::Osc52(sequence) => {
                    let mut output = stdout().lock();
                    if let Err(e) = output
                        .write_all(sequence.as_bytes())
                        .and_then(|()| output.flush())
                    {
                        app.set_hint(format!("terminal clipboard error: {e}"));
                    }
                }
                app::Effect::MouseCapture(enabled) => {
                    if enabled {
                        execute!(stdout(), EnableMouseCapture)?;
                    } else {
                        execute!(stdout(), DisableMouseCapture)?;
                    }
                }
            }
        }
        terminal.draw(|f| ui::draw(f, &mut app))?;
        tokio::select! {
            Some(result) = copy_rx.recv() => {
                copying = false;
                match result {
                    CopyResult::Clipboard { count, result, osc } => app.copy_completed(count, result, osc),
                    CopyResult::Export(Ok(path)) => app.set_hint(format!("Exported {}", path.display())),
                    CopyResult::Export(Err(e)) => app.set_hint(format!("export error: {e}")),
                }
            }
            ev = events.next() => match ev {
                Some(Ok(Event::Key(k))) => app.on_key(k),
                Some(Ok(Event::Paste(s))) => app.on_paste(&s),
                Some(Ok(Event::Mouse(m))) => app.on_mouse(m),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => break,
            },
            ev = handle.events.recv(), if !app.events_closed => match ev {
                Some(ev) => {
                    app.on_agent_event(ev);
                    // Apply everything already queued (e.g. a burst of live
                    // command output) before the next redraw, so drawing
                    // never falls behind a fast producer.
                    for _ in 0..MAX_EVENTS_PER_FRAME {
                        match handle.events.try_recv() {
                            Ok(ev) => app.on_agent_event(ev),
                            Err(_) => break,
                        }
                    }
                }
                None => app.on_events_closed(),
            },
            _ = tick.tick() => app.on_tick(),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn export_creates_utf8_file_and_refuses_overwrite() {
        let dir = std::env::temp_dir().join(format!(
            "mistl-tui-export-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let mut info = UiInfo::from_config(&Config::default());
        info.workspace.root = dir.to_string_lossy().into_owned();
        let mut app = app::App::new(&info);
        app.on_paste("/export");
        app.on_key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        ));
        let effects = app.take_effects();
        let [app::Effect::Export { path, text }] = effects.as_slice() else {
            panic!("expected export")
        };
        assert_eq!(path.parent().unwrap(), dir);
        let name = path.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("mistl-agent-"));
        assert!(
            chrono::NaiveDateTime::parse_from_str(
                name.strip_prefix("mistl-agent-")
                    .unwrap()
                    .strip_suffix(".md")
                    .unwrap(),
                "%Y%m%d-%H%M%S"
            )
            .is_ok()
        );
        assert_eq!(*text, app.transcript_text());
        export_transcript(&path, "> request\n\n日本語 🦀")
            .await
            .unwrap();
        assert!(export_transcript(&path, "replacement").await.is_err());
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "> request\n\n日本語 🦀"
        );
        assert!(
            export_transcript(&dir.join("missing/file.md"), "text")
                .await
                .is_err()
        );
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn header_uses_session_options_and_runtime_instance() {
        let cfg = Config {
            model: "test-model".into(),
            reasoning_effort: Some("high".into()),
            ..Config::default()
        };
        let info = UiInfo::from_config(&cfg);
        assert_eq!(info.model, cfg.model);
        assert_eq!(info.reasoning_effort, cfg.reasoning_effort);
        assert_eq!(info.tool_mode, cfg.tool_mode);
        assert_eq!(info.approval, cfg.approval);
        assert_eq!(info.instance, crate::runtime::context().instance);
        assert!(UiInfo::from_config(&Config::default()).model.is_empty());
    }
}
