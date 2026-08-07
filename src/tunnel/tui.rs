//! Terminal UI for `mistl tunnel tui`.
//!
//! Upstream `p2p`'s TUI (`p2p/src/tui.rs`) held a live in-process
//! `SessionContext` and every redraw/action ran against shared, `tokio`
//! `Mutex`-guarded state in the same process. In mistl the tunnel session
//! lives in the daemon (see the integration contract), so this TUI is
//! re-seated as a thin daemon IPC client: once per tick it polls
//! `tunnel.status` via `crate::daemon::ipc::client_request`, and every key
//! action is one more blocking IPC round-trip to a `tunnel.*` command
//! instead of a direct method call on session state.
//!
//! `client_request` is documented as blocking and meant for a
//! caller with no tokio runtime (see its doc comment in
//! `src/daemon/ipc.rs`), and a terminal UI's event loop wants a plain
//! blocking loop anyway, so -- unlike upstream, which was `async` throughout
//! -- there is deliberately no tokio runtime or `async fn` anywhere in this
//! module or its submodules (`tui/app.rs`, `tui/ui.rs`, `tui/format.rs`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyEventKind};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;

mod app;
mod format;
mod ui;

use app::App;

/// Status refresh cadence. Upstream refreshed its in-process state every
/// 120ms, which was cheap (just reading local `Mutex`-guarded state). Here
/// each tick is a real IPC round-trip to the daemon over a loopback TCP
/// socket, so this is a little more relaxed; still fast enough that new
/// peers/forwards/chat messages show up well within a third of a second.
const TICK: Duration = Duration::from_millis(250);
const EVENT_POLL: Duration = Duration::from_millis(100);

/// Runs the tunnel TUI until the user quits (`q` or Ctrl+C). Entry point
/// called by the `mistl tunnel tui` CLI command (see `src/cli.rs`).
///
/// Sets up the terminal (raw mode + alternate screen) and restores it on
/// every exit path: normal return, an `Err` bubbling out of the render/event
/// loop, and a panic inside it. A custom panic hook is installed for the
/// duration of this call so a bug in the loop can never leave the caller's
/// terminal stuck in raw/alternate-screen mode. The hook is intentionally
/// not chained to whatever hook was previously installed and isn't restored
/// afterwards: `mistl tunnel tui` is a leaf CLI subcommand whose process
/// exits right after this function returns, so there's nothing meaningful
/// left to hand back to.
pub fn run() -> Result<()> {
    std::panic::set_hook(Box::new(|panic_info| {
        let _ = disable_raw_mode();
        let mut stdout = std::io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
        eprintln!("{panic_info}");
    }));

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal);

    // Best-effort teardown: ignore errors here so a failure restoring the
    // terminal can't shadow (or replace) the loop's own `result`, which is
    // what the caller actually needs to see.
    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = terminal.show_cursor();

    result
}

fn run_loop<B: Backend>(terminal: &mut Terminal<B>) -> Result<()> {
    // Same single reader-thread + mpsc-channel pattern as upstream: crossterm
    // event polling blocks, so it runs on its own thread and feeds the main
    // loop non-blockingly via `try_recv`.
    let running = Arc::new(AtomicBool::new(true));
    let (tx, rx) = mpsc::channel::<Event>();
    let reader_flag = running.clone();
    std::thread::spawn(move || {
        while reader_flag.load(Ordering::Relaxed) {
            if event::poll(EVENT_POLL).unwrap_or(false) {
                match event::read() {
                    Ok(ev) => {
                        if tx.send(ev).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });

    let mut app = App::new();
    app.refresh();

    loop {
        terminal.draw(|f| ui::draw(f, &app))?;

        while let Ok(ev) = rx.try_recv() {
            if let Event::Key(key) = ev
                && key.kind == KeyEventKind::Press
            {
                app.handle_key(key);
            }
        }

        if app.should_quit {
            break;
        }

        std::thread::sleep(TICK);
        app.refresh();
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}
