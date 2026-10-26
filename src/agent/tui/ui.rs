//! Rendering.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use super::app::{App, Entry, Picker, PickerKind, ToolStatus};
use super::text::{str_width, truncate_text, wrap_text};
use crate::agent::types::ToolMode;

const MAX_INPUT_ROWS: usize = 6;
const COLLAPSED_LINES: usize = 6;
const LIVE_LINES: usize = 8;
const MAX_SUGGESTIONS: usize = 8;

fn dim() -> Style {
    Style::default().fg(Color::DarkGray)
}

/// Push wrapped `text` with a first-line prefix and an indent for the rest.
fn push_wrapped(
    out: &mut Vec<Line<'static>>,
    text: &str,
    width: usize,
    first: Span<'static>,
    indent: usize,
    style: Style,
) {
    let pw = str_width(&first.content).max(indent);
    let inner = width.saturating_sub(pw).max(1);
    let mut first = Some(first);
    for l in wrap_text(text, inner) {
        let head = match first.take() {
            Some(f) => f,
            None => Span::raw(" ".repeat(pw)),
        };
        out.push(Line::from(vec![head, Span::styled(l, style)]));
    }
}

/// Split `**bold**` segments into spans. Unbalanced markers stay literal.
fn bold_spans(line: &str, base: Style) -> Vec<Span<'static>> {
    let parts: Vec<&str> = line.split("**").collect();
    if parts.len() < 3 || parts.len().is_multiple_of(2) {
        return vec![Span::styled(line.to_string(), base)];
    }
    parts
        .iter()
        .enumerate()
        .filter(|(_, p)| !p.is_empty())
        .map(|(i, p)| {
            let st = if i % 2 == 1 {
                base.add_modifier(Modifier::BOLD)
            } else {
                base
            };
            Span::styled(p.to_string(), st)
        })
        .collect()
}

fn assistant_lines(text: &str, width: usize, out: &mut Vec<Line<'static>>) {
    let code_style = Style::default().fg(Color::Yellow);
    let mut in_code = false;
    for raw in text.split('\n') {
        let is_fence = raw.trim_start().starts_with("```");
        if is_fence {
            in_code = !in_code;
            for l in wrap_text(raw, width.saturating_sub(2).max(1)) {
                out.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(l, code_style.add_modifier(Modifier::DIM)),
                ]));
            }
        } else if in_code {
            for l in wrap_text(raw, width.saturating_sub(2).max(1)) {
                out.push(Line::from(vec![
                    Span::styled("▎ ", code_style),
                    Span::styled(l, code_style),
                ]));
            }
        } else {
            for l in wrap_text(raw, width.max(1)) {
                out.push(Line::from(bold_spans(&l, Style::default())));
            }
        }
    }
}

pub fn build_transcript(app: &App, width: usize) -> Vec<Line<'static>> {
    let mut out: Vec<Line<'static>> = Vec::new();
    for (i, e) in app.entries.iter().enumerate() {
        if i > 0 {
            out.push(Line::raw(""));
        }
        match e {
            Entry::User(t) => push_wrapped(
                &mut out,
                t,
                width,
                Span::styled(
                    "> ",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                2,
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Entry::Assistant { text, .. } => assistant_lines(text, width, &mut out),
            Entry::Info(t) => {
                for l in wrap_text(t, width.max(1)) {
                    out.push(Line::styled(l, dim()));
                }
            }
            Entry::Error(t) => push_wrapped(
                &mut out,
                t,
                width,
                Span::styled("✗ ", Style::default().fg(Color::Red)),
                2,
                Style::default().fg(Color::Red),
            ),
            Entry::Tool {
                title,
                status,
                live_output,
                ..
            } => {
                let (mark, color) = match status {
                    ToolStatus::Running => (app.spinner().to_string(), Color::Yellow),
                    ToolStatus::Done { ok: true, .. } => ("✓".to_string(), Color::Green),
                    ToolStatus::Done { ok: false, .. } => ("✗".to_string(), Color::Red),
                };
                let head = if matches!(status, ToolStatus::Running) {
                    format!("▶ {mark} ")
                } else {
                    format!("{mark} ")
                };
                push_wrapped(
                    &mut out,
                    title,
                    width,
                    Span::styled(head, Style::default().fg(color)),
                    2,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                );
                if matches!(status, ToolStatus::Running) && !live_output.is_empty() {
                    let all: Vec<&str> = live_output.lines().collect();
                    let start = if app.expand_tools {
                        0
                    } else {
                        all.len().saturating_sub(LIVE_LINES)
                    };
                    for line in &all[start..] {
                        for wrapped in wrap_text(line, width.saturating_sub(2).max(1)) {
                            out.push(Line::from(vec![
                                Span::raw("  "),
                                Span::styled(wrapped, dim()),
                            ]));
                        }
                    }
                }
                if let ToolStatus::Done { output, .. } = status {
                    let trimmed = output.trim_end();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let all: Vec<&str> = trimmed.lines().collect();
                    let shown = if app.expand_tools {
                        all.len()
                    } else {
                        all.len().min(COLLAPSED_LINES)
                    };
                    for l in &all[..shown] {
                        for w in wrap_text(l, width.saturating_sub(2).max(1)) {
                            out.push(Line::from(vec![Span::raw("  "), Span::styled(w, dim())]));
                        }
                    }
                    if shown < all.len() {
                        out.push(Line::styled(
                            format!("  (+{} lines, press Ctrl+O to expand)", all.len() - shown),
                            dim().add_modifier(Modifier::ITALIC),
                        ));
                    }
                }
            }
        }
    }
    out
}

pub fn draw(f: &mut Frame, app: &mut App) {
    let area = f.area();
    let width = area.width as usize;
    let (rows, cursor) = app.input.layout(width.saturating_sub(4).max(2));
    let shown_rows = rows.len().clamp(1, MAX_INPUT_ROWS);
    let input_h = shown_rows as u16 + 2;

    let [header, body, input_area, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(input_h.min(area.height.saturating_sub(3).max(3))),
        Constraint::Length(1),
    ])
    .areas(area);

    draw_header(f, app, header);
    draw_transcript(f, app, body);
    draw_input(f, app, input_area, &rows, cursor);
    draw_footer(f, app, footer);
    if !app.modal_open() {
        draw_suggestions(f, app, body);
    }
    if app.current_approval().is_some() {
        draw_modal(f, app, area);
    } else if let Some(picker) = &app.picker {
        draw_picker(f, picker, area);
    }
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let model = if app.model.is_empty() {
        "mistl default"
    } else {
        &app.model
    };
    let mode = match app.tool_mode {
        ToolMode::Native => "native",
        ToolMode::Prompt => "prompt",
    };
    let mut spans = vec![Span::styled(
        " mistl agent ",
        Style::default().fg(Color::Black).bg(Color::Cyan),
    )];
    let workspace = app
        .workspace
        .root
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default();
    if !workspace.is_empty() {
        // Leave room for the recipe status even with a long directory name.
        let name_width = (area.width as usize).saturating_sub(28).max(4);
        spans.push(Span::raw(format!(
            " {} ",
            truncate_text(workspace, name_width)
        )));
    }
    if app.workspace.justfile.is_some() {
        let status = if app.workspace.error.is_some() {
            "just: error ".into()
        } else {
            format!("just: {} recipes ", app.workspace.recipes.len())
        };
        spans.push(Span::styled(status, dim()));
    }
    spans.extend([
        Span::raw(format!(" {model} ")),
        Span::styled(format!("instance:{} ", app.instance), dim()),
        Span::styled(format!("tools:{mode} "), dim()),
    ]);
    let effort = app.reasoning_effort.as_deref().unwrap_or("default");
    spans.push(Span::styled(format!("effort:{effort} "), dim()));
    if app.auto_approve {
        spans.push(Span::styled(
            " auto-approve ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ));
    }
    let mut remaining = area.width as usize;
    let mut fitted = Vec::new();
    for span in spans {
        if remaining == 0 {
            break;
        }
        let text = truncate_text(&span.content, remaining);
        remaining = remaining.saturating_sub(str_width(&text));
        fitted.push(Span::styled(text, span.style));
    }
    f.render_widget(Paragraph::new(Line::from(fitted)), area);
}

fn draw_transcript(f: &mut Frame, app: &mut App, area: Rect) {
    let lines = build_transcript(app, area.width as usize);
    let h = area.height as usize;
    let max_top = lines.len().saturating_sub(h);
    app.last_total = lines.len();
    app.last_height = h;
    let top = match app.scroll_top {
        None => max_top,
        Some(t) => t.min(max_top),
    };
    let visible: Vec<Line> = lines.into_iter().skip(top).take(h).collect();
    f.render_widget(Paragraph::new(visible), area);
    if top < max_top {
        let msg = format!(" ↓ {} more lines (End) ", max_top - top);
        let w = str_width(&msg) as u16;
        if area.width > w && area.height > 0 {
            let r = Rect::new(area.x + area.width - w, area.y + area.height - 1, w, 1);
            f.render_widget(
                Paragraph::new(Span::styled(
                    msg,
                    Style::default().fg(Color::Black).bg(Color::DarkGray),
                )),
                r,
            );
        }
    }
}

fn draw_input(f: &mut Frame, app: &App, area: Rect, rows: &[String], cursor: (usize, usize)) {
    let shell_mode = app.input.text().starts_with('!');
    let input_color = if shell_mode {
        Color::Yellow
    } else {
        Color::Cyan
    };
    let border = if shell_mode {
        Style::default().fg(input_color)
    } else if app.busy {
        dim()
    } else {
        Style::default().fg(Color::Cyan)
    };
    let mut block = Block::default().borders(Borders::ALL).border_style(border);
    if shell_mode {
        block = block.title(Span::styled(" ! shell ", border));
    }
    let inner = block.inner(area);
    f.render_widget(block, area);
    let h = inner.height as usize;
    let off = (cursor.0 + 1).saturating_sub(h);
    let mut lines: Vec<Line> = Vec::new();
    if app.input.is_empty() {
        lines.push(Line::styled("Type a message…", dim()));
    } else {
        for r in rows.iter().skip(off).take(h) {
            lines.push(Line::raw(r.clone()));
        }
    }
    // 2-column gutter for the prompt marker
    let gutter = Rect::new(inner.x, inner.y, 2.min(inner.width), inner.height);
    let text_area = Rect::new(
        inner.x + gutter.width,
        inner.y,
        inner.width.saturating_sub(gutter.width),
        inner.height,
    );
    f.render_widget(
        Paragraph::new(Line::styled(
            if shell_mode { "! " } else { "> " },
            Style::default()
                .fg(input_color)
                .add_modifier(Modifier::BOLD),
        )),
        gutter,
    );
    f.render_widget(Paragraph::new(lines), text_area);
    if !app.modal_open() {
        let cy = text_area.y + cursor.0.saturating_sub(off) as u16;
        let cx = text_area.x + cursor.1 as u16;
        if cx < text_area.x + text_area.width.max(1) && cy < text_area.y + text_area.height.max(1) {
            f.set_cursor_position((cx, cy));
        }
    }
}

/// Command completion popup, anchored to the bottom of `area` (just
/// above the input box).
fn draw_suggestions(f: &mut Frame, app: &App, area: Rect) {
    let list = app.suggestions();
    if list.is_empty() || area.height < 3 || area.width < 10 {
        return;
    }
    let rows = list
        .len()
        .min(MAX_SUGGESTIONS)
        .min(area.height as usize - 2);
    let sel = app.suggest_sel.min(list.len() - 1);
    let top = (sel + 1).saturating_sub(rows);
    let cmd_w = list
        .iter()
        .map(|s| str_width(&s.text))
        .max()
        .unwrap_or(0)
        .min(area.width.saturating_sub(6) as usize);
    let desc_w = (area.width as usize).saturating_sub(cmd_w + 6);
    let mut lines: Vec<Line> = Vec::new();
    for (i, s) in list.iter().enumerate().skip(top).take(rows) {
        let command = truncate_text(&s.text, cmd_w);
        let pad = " ".repeat(cmd_w - str_width(&command) + 2);
        let (cmd_style, desc_style) = if i == sel {
            let st = Style::default().fg(Color::Black).bg(Color::Cyan);
            (st.add_modifier(Modifier::BOLD), st)
        } else {
            (Style::default().fg(Color::Cyan), dim())
        };
        lines.push(Line::from(vec![
            Span::styled(format!(" {command}{pad}"), cmd_style),
            Span::styled(format!("{} ", truncate_text(&s.desc, desc_w)), desc_style),
        ]));
    }
    let content_w = lines.iter().map(Line::width).max().unwrap_or(0) as u16;
    let w = (content_w + 2).min(area.width);
    let h = rows as u16 + 2;
    let rect = Rect::new(area.x, area.y + area.height - h, w, h);
    let mut block = Block::default().borders(Borders::ALL).border_style(dim());
    if list.len() > rows {
        block = block.title_bottom(format!(" {}/{} ", sel + 1, list.len()));
    }
    f.render_widget(Clear, rect);
    f.render_widget(Paragraph::new(lines).block(block), rect);
}

fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let line = if app.events_closed {
        Line::styled(
            " agent stopped - press any key to quit",
            Style::default().fg(Color::Red),
        )
    } else if app.current_approval().is_some() {
        Line::styled(
            " y yes  n no  a always (this session)",
            Style::default().fg(Color::Yellow),
        )
    } else if app.fetching {
        Line::from(vec![
            Span::styled(
                format!(" {} ", app.spinner()),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw("fetching models…"),
            Span::styled("   Esc cancel", dim()),
        ])
    } else if let Some((h, _)) = &app.hint {
        Line::styled(format!(" {h}"), Style::default().fg(Color::Yellow))
    } else if !app.busy && !app.suggestions().is_empty() {
        Line::styled(" ↑↓ select  Tab complete  Enter run  Esc hide", dim())
    } else if app.busy {
        Line::from(vec![
            Span::styled(
                format!(" {} ", app.spinner()),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw(app.busy_label()),
            Span::styled("   Esc cancel  PgUp/PgDn scroll  Ctrl+O expand", dim()),
        ])
    } else {
        Line::styled(
            " Enter send  Alt+Enter newline  PgUp/PgDn scroll  Ctrl+O expand  /help  Ctrl+C quit",
            dim(),
        )
    };
    f.render_widget(Paragraph::new(line), area);
}

fn draw_modal(f: &mut Frame, app: &App, area: Rect) {
    let Some(p) = app.current_approval() else {
        return;
    };
    let w = area.width.saturating_sub(4).clamp(20, 80).min(area.width);
    let inner_w = w.saturating_sub(4) as usize;
    let mut lines: Vec<Line> = Vec::new();
    for l in wrap_text(&p.title, inner_w) {
        lines.push(Line::styled(
            l,
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(Color::Yellow),
        ));
    }
    if !p.reason.trim().is_empty() {
        lines.push(Line::raw(""));
        for l in wrap_text(&p.reason, inner_w) {
            lines.push(Line::styled(l, dim()));
        }
    }
    lines.push(Line::raw(""));
    lines.push(Line::from(vec![
        Span::styled("[y]", Style::default().fg(Color::Green)),
        Span::raw(" yes  "),
        Span::styled("[n]", Style::default().fg(Color::Red)),
        Span::raw(" no  "),
        Span::styled("[a]", Style::default().fg(Color::Yellow)),
        Span::raw(" always (this session)"),
    ]));
    let queued = app.approvals.len().saturating_sub(1);
    let mut block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .title(" Approval needed ");
    if queued > 0 {
        block = block.title_bottom(format!(" +{queued} more waiting "));
    }
    let h = (lines.len() as u16 + 2).min(area.height);
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(w)) / 2,
        area.y + (area.height.saturating_sub(h)) / 2,
        w,
        h,
    );
    f.render_widget(Clear, rect);
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false })
            .alignment(Alignment::Left),
        rect,
    );
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let w = width.min(area.width);
    let h = height.min(area.height);
    Rect::new(
        area.x + (area.width - w) / 2,
        area.y + (area.height - h) / 2,
        w,
        h,
    )
}

fn draw_picker(f: &mut Frame, picker: &Picker, area: Rect) {
    let rect = centered(area, area.width.saturating_sub(4).clamp(20, 80), 18);
    let title = match picker.kind {
        PickerKind::Model => " Models: mistl ",
        PickerKind::Effort => " Reasoning effort ",
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(title);
    let inner = block.inner(rect);
    f.render_widget(Clear, rect);
    f.render_widget(block, rect);
    let [filter, list, count, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(inner);
    f.render_widget(Paragraph::new(format!("Filter: {}", picker.filter)), filter);
    let items = picker.filtered();
    let height = list.height as usize;
    let start = picker
        .selected
        .saturating_sub(height / 2)
        .min(items.len().saturating_sub(height));
    let lines: Vec<Line> = if items.is_empty() {
        vec![Line::styled("No matches", dim())]
    } else {
        items
            .iter()
            .enumerate()
            .skip(start)
            .take(height)
            .map(|(i, item)| {
                if i == picker.selected {
                    Line::styled(
                        format!("> {item}"),
                        Style::default().fg(Color::Black).bg(Color::Cyan),
                    )
                } else {
                    Line::raw(format!("  {item}"))
                }
            })
            .collect()
    };
    f.render_widget(Paragraph::new(lines), list);
    f.render_widget(
        Paragraph::new(Line::styled(
            format!(
                "{}/{} matches  ({}/{})",
                items.len(),
                picker.items.len(),
                if items.is_empty() {
                    0
                } else {
                    picker.selected + 1
                },
                items.len()
            ),
            dim(),
        )),
        count,
    );
    f.render_widget(
        Paragraph::new(Line::styled(
            "Type filter  ↑↓/PgUp/PgDn move  Enter choose  Esc cancel",
            dim(),
        )),
        footer,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::config::Config;
    use crate::agent::tui::UiInfo;
    use crate::agent::types::AgentEvent;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use tokio::sync::oneshot;

    fn command(app: &mut App, text: &str) {
        app.on_paste(text);
        app.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        app.take_effects();
    }

    fn render(app: &mut App, width: u16, height: u16) -> (String, bool) {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        let frame = terminal.draw(|f| draw(f, app)).unwrap();
        let text = frame
            .buffer
            .content
            .iter()
            .map(|c| c.symbol())
            .collect::<String>();
        let input_cursor =
            terminal.get_cursor_position().unwrap() != ratatui::layout::Position::ORIGIN;
        (text, input_cursor)
    }

    #[test]
    fn renders_command_suggestions() {
        let mut app = App::new(&UiInfo::from_config(&Config::default()));
        app.on_paste("/");
        let (text, cursor) = render(&mut app, 100, 20);
        assert!(text.contains("/help"));
        assert!(text.contains("show keys and commands"));
        assert!(
            text.contains("1/17"),
            "scroll position when the list overflows"
        );
        assert!(text.contains("Tab complete"));
        assert!(cursor);
        app.on_paste("mo");
        let (text, _) = render(&mut app, 100, 20);
        assert!(text.contains("/model"));
        assert!(!text.contains("show keys and commands"));
        // Tiny terminals skip the popup instead of panicking.
        render(&mut app, 12, 6);
    }

    #[test]
    fn renders_live_output_tail_expansion_and_final_replacement() {
        let mut app = App::new(&UiInfo::from_config(&Config::default()));
        app.entries.clear();
        app.on_agent_event(AgentEvent::ToolStart {
            id: "shell".into(),
            title: "! command".into(),
        });
        app.on_agent_event(AgentEvent::ToolOutput {
            id: "shell".into(),
            chunk: (0..12).map(|i| format!("output-{i:02}\n")).collect(),
        });
        let transcript = |app: &App| {
            build_transcript(app, 80)
                .iter()
                .map(Line::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        };
        let text = transcript(&app);
        assert!(text.contains("! command"));
        assert!(!text.contains("output-03"));
        assert!(text.contains("output-04"));
        assert!(text.contains("output-11"));
        assert_eq!(text.lines().count(), 9);
        app.expand_tools = true;
        let text = transcript(&app);
        assert!(text.contains("output-00"));
        assert_eq!(text.lines().count(), 13);
        app.on_agent_event(AgentEvent::ToolEnd {
            id: "shell".into(),
            ok: true,
            output: "final output".into(),
        });
        let text = transcript(&app);
        assert!(text.contains("final output"));
        assert!(!text.contains("output-"));
    }

    #[test]
    fn shell_input_uses_distinct_border_and_title() {
        let mut app = App::new(&UiInfo::from_config(&Config::default()));
        app.input.set("!echo hello");
        app.busy = true;
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let frame = terminal.draw(|f| draw(f, &mut app)).unwrap();
        assert!(
            frame
                .buffer
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>()
                .contains(" ! shell ")
        );
        assert!(
            frame
                .buffer
                .content
                .iter()
                .any(|c| c.symbol() == "!" && c.fg == Color::Yellow)
        );
        app.input.set("message");
        let (text, _) = render(&mut app, 80, 12);
        assert!(!text.contains(" ! shell "));
    }

    #[test]
    fn workspace_header_shows_name_and_recipe_status_with_truncation() {
        let mut info = UiInfo::from_config(&Config::default());
        info.workspace.root = "parent/project".into();
        info.workspace.justfile = Some("parent/justfile".into());
        info.workspace.recipes = vec![crate::agent::types::RecipeInfo::default(); 3];
        let mut app = App::new(&info);
        let (text, _) = render(&mut app, 100, 12);
        assert!(text.contains("project just: 3 recipes"));
        app.workspace.error = Some("could not load recipes".into());
        let (text, _) = render(&mut app, 100, 12);
        assert!(text.contains("project just: error"));
        app.workspace.justfile = None;
        let (text, _) = render(&mut app, 100, 12);
        assert!(!text.contains("just:"));
        app.workspace.root = format!("parent/{}", "long-name".repeat(20));
        app.workspace.justfile = Some("parent/justfile".into());
        let (text, _) = render(&mut app, 40, 12);
        assert!(text.contains('…'));
        assert!(text.contains("just: error"));
        for width in [1, 8, 20] {
            render(&mut app, width, 6);
        }
    }

    #[test]
    fn renders_header_and_fetching_then_scrolled_picker_without_cursor() {
        let cfg = Config {
            reasoning_effort: Some("high".into()),
            ..Config::default()
        };
        let mut info = UiInfo::from_config(&cfg);
        info.instance = "test-instance".into();
        let mut app = App::new(&info);
        let (text, cursor) = render(&mut app, 120, 30);
        assert!(text.contains("mistl default"));
        assert!(text.contains("tools:native"));
        assert!(text.contains("effort:high"));
        assert!(text.contains("instance:test-instance"));
        assert!(cursor);
        command(&mut app, "/model");
        let (text, _) = render(&mut app, 120, 30);
        assert!(text.contains("fetching models…"));
        app.on_agent_event(AgentEvent::Models {
            models: (0..100).map(|i| format!("model-{i:03}")).collect(),
            error: None,
        });
        app.picker.as_mut().unwrap().selected = 70;
        let (text, cursor) = render(&mut app, 120, 30);
        assert!(text.contains("Models: mistl"));
        assert!(text.contains("> model-070"));
        assert!(text.contains("100/100"));
        assert!(!text.contains("model-000"));
        assert!(!cursor);
    }

    #[test]
    fn approval_takes_priority_over_picker_without_cursor() {
        let mut app = App::new(&UiInfo::from_config(&Config::default()));
        command(&mut app, "/effort");
        let (reply, _receive) = oneshot::channel();
        app.on_agent_event(AgentEvent::ApprovalRequest {
            title: "Run command?".into(),
            reason: "".into(),
            reply,
        });
        let (text, cursor) = render(&mut app, 120, 30);
        assert!(text.contains("Approval needed"));
        assert!(!text.contains("Reasoning effort"));
        assert!(!cursor);
    }

    #[test]
    fn picker_renders_in_small_terminal() {
        let mut app = App::new(&UiInfo::from_config(&Config::default()));
        command(&mut app, "/effort");
        for (width, height) in [(1, 1), (8, 3), (20, 5)] {
            render(&mut app, width, height);
        }
    }
}
