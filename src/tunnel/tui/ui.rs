//! Rendering for the tunnel TUI. Layout, panes, colors and key bindings are
//! kept as close to `p2p/src/tui/ui.rs` as the new data source allows -- the
//! value of this file is the exact look-and-feel upstream already tuned.
//! What changed: the data comes from `App`'s JSON-derived `Snapshot`
//! (`super::app`) instead of live session state, there's a dedicated
//! "daemon not connected" screen, and three small additions the integration
//! contract calls for that upstream's TUI didn't have: a self-id/room/status
//! header field, a notice line, and the chat/room popups.

use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table};

use super::app::{App, Focus, PendingRow, Popup};
use super::format::{
    endpoint, format_chat, format_event, format_notice, format_pending_outgoing, human_bytes,
    is_error_notice, proto_name, role_label, short_id, state_name, trust_name,
};

pub(super) fn draw(f: &mut Frame, app: &App) {
    if let Some(err) = &app.daemon_error {
        draw_daemon_error(f, err);
        return;
    }

    let chunks = Layout::vertical([
        Constraint::Length(1), // header
        Constraint::Min(6),    // forwards / pending
        Constraint::Length(8), // audit log
        Constraint::Length(1), // notice
        Constraint::Length(1), // footer
    ])
    .split(f.area());

    draw_header(f, chunks[0], app);

    let body = Layout::horizontal([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(chunks[1]);
    draw_forwards(f, body[0], app);
    draw_pending(f, body[1], app);

    draw_events(f, chunks[2], app);
    draw_notice(f, chunks[3], app);
    draw_footer(f, chunks[4], app);

    match &app.popup {
        Popup::Add(form) => draw_add_popup(f, form, app.message.as_deref()),
        Popup::Trust(sel) => draw_trust_popup(f, app, *sel),
        Popup::Chat(input) => draw_chat_popup(f, app, input),
        Popup::Room(input) => draw_room_popup(f, input),
        Popup::None => {}
    }
}

/// Full-screen replacement for the normal panes when the last
/// `tunnel.status` call failed. Per the integration contract, a daemon
/// that simply isn't running must render a clear, actionable message
/// instead of the TUI crashing or exiting.
fn draw_daemon_error(f: &mut Frame, err: &str) {
    let chunks = Layout::vertical([Constraint::Min(3), Constraint::Length(1)]).split(f.area());

    let text = if err.contains("daemon is not running") {
        "daemon is not running -- start it with `mistl daemon start`".to_string()
    } else {
        format!("tunnel.status failed: {}", err)
    };
    let p = Paragraph::new(text)
        .alignment(Alignment::Center)
        .style(Style::default().add_modifier(Modifier::BOLD));
    f.render_widget(p, chunks[0]);

    let footer = Paragraph::new(" [q]uit ").style(Style::default().add_modifier(Modifier::DIM));
    f.render_widget(footer, chunks[1]);
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let snap = &app.snapshot;
    let peers = if snap.peers.is_empty() {
        "peers: 0 (waiting…)".to_string()
    } else {
        format!(
            "peers: {} [{}]",
            snap.peers.len(),
            snap.peers
                .iter()
                .map(|p| short_id(p))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let serve = snap
        .forwards
        .iter()
        .filter(|s| s.direction == "serve")
        .count();
    let conn = snap.forwards.len() - serve;
    let status = if snap.running {
        "running"
    } else if snap.enabled {
        "enabled/stopped"
    } else {
        "disabled"
    };
    let text = format!(
        " tunnel [{}] — self: {}   room: {}   {}   serve: {} / conn: {}   pending: {} ",
        status,
        short_id(&snap.self_id),
        snap.room,
        peers,
        serve,
        conn,
        app.pending_rows().len()
    );
    let p = Paragraph::new(text).style(Style::default().add_modifier(Modifier::BOLD));
    f.render_widget(p, area);
}

fn draw_forwards(f: &mut Frame, area: Rect, app: &App) {
    let header = Row::new(vec![
        "ROLE", "KEY", "PROTO", "ENDPOINT", "STATE", "CONNS", "IN", "OUT",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let mut rows: Vec<Row> = Vec::new();
    for (i, s) in app.snapshot.forwards.iter().enumerate() {
        let selected = app.focus == Focus::Forwards && i == app.forwards_sel;
        let style = if selected {
            Style::default().add_modifier(Modifier::REVERSED)
        } else {
            Style::default()
        };
        rows.push(
            Row::new(vec![
                Cell::from(role_label(&s.direction)),
                Cell::from(s.target.clone()),
                Cell::from(proto_name(&s.proto)),
                Cell::from(endpoint(s)),
                Cell::from(state_name(&s.state).to_string()),
                Cell::from(s.active_conns.to_string()),
                Cell::from(human_bytes(s.bytes_rx)),
                Cell::from(human_bytes(s.bytes_tx)),
            ])
            .style(style),
        );

        // Per-peer breakdown for the selected, expanded forward. `peers` is
        // not part of the frozen `tunnel.status` shape (see
        // `super::app::ForwardRow`), so this degrades to "(no peers)" on any
        // daemon build that doesn't emit it -- same as an empty list would.
        if selected && app.expanded {
            if s.peers.is_empty() {
                rows.push(
                    Row::new(vec![Cell::from(""), Cell::from("  └ (no peers)")])
                        .style(Style::default().add_modifier(Modifier::DIM)),
                );
            }
            for peer in &s.peers {
                rows.push(
                    Row::new(vec![
                        Cell::from(""),
                        Cell::from(format!("  └ {}", short_id(&peer.peer_id))),
                        Cell::from(""),
                        Cell::from(""),
                        Cell::from(""),
                        Cell::from(peer.active_conns.to_string()),
                        Cell::from(human_bytes(peer.bytes_rx)),
                        Cell::from(human_bytes(peer.bytes_tx)),
                    ])
                    .style(Style::default().add_modifier(Modifier::DIM)),
                );
            }
        }
    }

    let widths = [
        Constraint::Length(6),
        Constraint::Length(12),
        Constraint::Length(5),
        Constraint::Min(14),
        Constraint::Length(9),
        Constraint::Length(5),
        Constraint::Length(8),
        Constraint::Length(8),
    ];

    let border_style = focus_border(app.focus == Focus::Forwards);
    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(" Forwards "),
    );
    f.render_widget(table, area);
}

fn draw_pending(f: &mut Frame, area: Rect, app: &App) {
    let rows = app.pending_rows();
    let items: Vec<ListItem> = if rows.is_empty() {
        vec![ListItem::new("(none)")]
    } else {
        rows.iter()
            .enumerate()
            .map(|(i, row)| {
                let selected = app.focus == Focus::Pending && i == app.pending_sel;
                let text = match row {
                    PendingRow::Forward(r) => format!(
                        "[FWD] {} wants {} {} (you become the server)",
                        short_id(&r.peer_id),
                        r.proto.to_uppercase(),
                        r.remote_addr
                    ),
                    PendingRow::Conn(p) => format!(
                        "[CONN] {} → {} ({})",
                        short_id(&p.peer_id),
                        p.target,
                        p.remote_addr
                    ),
                    PendingRow::Outgoing(o) => format_pending_outgoing(o),
                };
                let style = if selected {
                    Style::default().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                };
                ListItem::new(text).style(style)
            })
            .collect()
    };

    let border_style = focus_border(app.focus == Focus::Pending);
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(" Pending (y/Y allow, n/N deny) "),
    );
    f.render_widget(list, area);
}

fn draw_events(f: &mut Frame, area: Rect, app: &App) {
    let max = area.height.saturating_sub(2) as usize;
    let items: Vec<ListItem> = app
        .snapshot
        .events
        .iter()
        .rev()
        .take(max)
        .map(|e| ListItem::new(format_event(e)))
        .collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(" Audit log "));
    f.render_widget(list, area);
}

/// Shows the single most recent session notice (`SessionNotice`), if any.
/// New relative to upstream, which had no such feed in the ratatui TUI.
fn draw_notice(f: &mut Frame, area: Rect, app: &App) {
    let Some(notice) = app.snapshot.notices.last() else {
        return;
    };
    let style = if is_error_notice(notice) {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::Yellow)
    };
    let p = Paragraph::new(format!(" {} ", format_notice(notice))).style(style);
    f.render_widget(p, area);
}

fn draw_footer(f: &mut Frame, area: Rect, app: &App) {
    let hint = if let Some(msg) = &app.message {
        format!(" {} ", msg)
    } else {
        " [a]dd forward  [d]elete  [Enter]expand  [Tab]focus  [t]rust  [y/n]approve  \
          [c]hat  [r]oom  [s]tart/stop  [q]uit — dashboard: `mistl ui` "
            .to_string()
    };
    let p = Paragraph::new(hint).style(Style::default().add_modifier(Modifier::DIM));
    f.render_widget(p, area);
}

fn draw_add_popup(f: &mut Frame, form: &super::app::AddForm, message: Option<&str>) {
    let area = centered_rect(70, 45, f.area());
    f.render_widget(Clear, area);
    f.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(" Add forward "),
        area,
    );
    let inner = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Min(1),
    ])
    .margin(1)
    .split(area);

    let proto_label = if form.proto_tcp {
        "( TCP )  UDP "
    } else {
        " TCP  ( UDP )"
    };
    f.render_widget(
        field_line(
            "Proto",
            proto_label,
            form.field == super::app::AddField::Proto,
        ),
        inner[0],
    );
    f.render_widget(
        field_line(
            "Local ",
            &form.local,
            form.field == super::app::AddField::Local,
        ),
        inner[1],
    );
    f.render_widget(
        field_line(
            "Remote",
            &form.remote,
            form.field == super::app::AddField::Remote,
        ),
        inner[2],
    );

    let help = Paragraph::new(
        "Local = your own listen ip:port   Remote = peer's ip:port\n\
         Tab/↑↓=field  ←→/Space=proto  Enter=send request  Esc=cancel",
    )
    .style(Style::default().add_modifier(Modifier::DIM));
    f.render_widget(help, inner[3]);

    if let Some(msg) = message {
        let err = Paragraph::new(msg).style(Style::default().add_modifier(Modifier::BOLD));
        f.render_widget(err, inner[4]);
    }
}

fn field_line<'a>(label: &'a str, value: &'a str, focused: bool) -> Paragraph<'a> {
    let marker = if focused { "▶ " } else { "  " };
    let cursor = if focused { "_" } else { "" };
    let style = if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    Paragraph::new(format!("{}{}: {}{}", marker, label, value, cursor)).style(style)
}

fn draw_trust_popup(f: &mut Frame, app: &App, sel: usize) {
    let area = centered_rect(70, 60, f.area());
    f.render_widget(Clear, area);
    let items: Vec<ListItem> = if app.snapshot.trust.is_empty() {
        vec![ListItem::new("(empty)")]
    } else {
        app.snapshot
            .trust
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let text = format!(
                    "{} {} → {}",
                    trust_name(&e.decision),
                    short_id(&e.peer_id),
                    e.target
                );
                let style = if i == sel {
                    Style::default().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                };
                ListItem::new(text).style(style)
            })
            .collect()
    };
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Trust store ([x]remove  [Esc]close) "),
    );
    f.render_widget(list, area);
}

/// New relative to upstream: a scrollback + input-line popup for
/// `tunnel.chat.send`. Mirrors the styling of the other popups (one bordered
/// `Block`, unbordered content laid out inside via `Layout::margin`) rather
/// than upstream's separate raw-terminal `p2p chat` mode
/// (`p2p/src/app/chat.rs`), since this is now just another `App` popup.
fn draw_chat_popup(f: &mut Frame, app: &App, input: &str) {
    let area = centered_rect(70, 60, f.area());
    f.render_widget(Clear, area);
    f.render_widget(Block::default().borders(Borders::ALL).title(" Chat "), area);
    let inner = Layout::vertical([Constraint::Min(3), Constraint::Length(1)])
        .margin(1)
        .split(area);

    let max = inner[0].height as usize;
    let items: Vec<ListItem> = app
        .snapshot
        .chat
        .iter()
        .rev()
        .take(max)
        .map(|m| ListItem::new(format_chat(m)))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let list = List::new(items);
    f.render_widget(list, inner[0]);

    let input_p = Paragraph::new(format!("> {}_", input));
    f.render_widget(input_p, inner[1]);
}

/// New relative to upstream: a single-field popup for `tunnel.room.set`.
fn draw_room_popup(f: &mut Frame, input: &str) {
    let area = centered_rect(60, 25, f.area());
    f.render_widget(Clear, area);
    f.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(" Change room "),
        area,
    );
    let inner = Layout::vertical([Constraint::Length(2), Constraint::Min(1)])
        .margin(1)
        .split(area);
    f.render_widget(field_line("Room", input, true), inner[0]);
    let help = Paragraph::new("Enter=submit (empty = daemon generates one)  Esc=cancel")
        .style(Style::default().add_modifier(Modifier::DIM));
    f.render_widget(help, inner[1]);
}

fn focus_border(focused: bool) -> Style {
    if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default()
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(vertical[1])[1]
}
