//! Terminal UI.

mod app;
mod input;
mod text;
mod ui;

use std::io::{Stdout, stdout};
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste, Event, EventStream};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::agent::config::Config;
use crate::agent::session::AgentHandle;

/// Static facts shown in the header / status bar.
#[derive(Debug, Clone)]
pub struct UiInfo {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub tool_mode: crate::agent::types::ToolMode,
    pub auto_approve: bool,
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
            auto_approve: cfg.auto_approve,
            workspace: Default::default(),
        }
    }
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(stdout(), DisableBracketedPaste, LeaveAlternateScreen);
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    if let Err(e) = execute!(stdout(), EnterAlternateScreen, EnableBracketedPaste) {
        restore_terminal();
        return Err(e.into());
    }
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        prev(info);
    }));
    Ok(Terminal::new(CrosstermBackend::new(stdout()))?)
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

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    info: UiInfo,
    mut handle: AgentHandle,
) -> Result<()> {
    let mut app = app::App::new(&info);
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    while !app.quit {
        for eff in app.take_effects() {
            match eff {
                app::Effect::Command(c) => {
                    let _ = handle.commands.send(c);
                }
                app::Effect::CancelTurn => handle.cancel_turn(),
            }
        }
        terminal.draw(|f| ui::draw(f, &mut app))?;
        tokio::select! {
            ev = events.next() => match ev {
                Some(Ok(Event::Key(k))) => app.on_key(k),
                Some(Ok(Event::Paste(s))) => app.on_paste(&s),
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
        assert_eq!(info.auto_approve, cfg.auto_approve);
        assert_eq!(info.instance, crate::runtime::context().instance);
        assert!(UiInfo::from_config(&Config::default()).model.is_empty());
    }
}
