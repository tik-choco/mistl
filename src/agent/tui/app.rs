//! Pure UI state and event handling (no terminal access).

use std::collections::VecDeque;
use std::ops::Range;
use std::path::PathBuf;

use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Span;
use tokio::sync::oneshot;

use super::UiInfo;
use super::clipboard::Method;
use super::input::InputBuffer;
use super::text::{str_width, truncate_text};
use crate::agent::config::normalize_effort;
use crate::agent::types::{
    AgentEvent, Approval, ApprovalMode, MistlOp, ToolMode, UserCommand, WorkspaceSummary,
};

pub const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
/// Ticks (~100ms each) a footer hint stays visible.
const HINT_TICKS: u64 = 30;
const MAX_LIVE_OUTPUT: usize = 64 * 1024;

pub const EFFORTS: [&str; 7] = [
    "default", "none", "minimal", "low", "medium", "high", "xhigh",
];

/// Slash commands offered as completions, in display order. Bare commands
/// come before their argument forms so an exact match is selected first.
const COMMANDS: [(&str, &str); 21] = [
    ("/help", "show keys and commands"),
    ("/clear", "clear the conversation"),
    ("/copy", "copy the last assistant reply"),
    ("/copy all", "copy the whole transcript"),
    ("/copy tool", "copy the last full tool output"),
    ("/export", "save transcript to a new file"),
    ("/just", "list workspace recipes"),
    ("/model", "choose a model"),
    ("/models", "choose a model"),
    ("/effort", "choose reasoning effort"),
    ("/approval", "show approval mode"),
    ("/approval ask", "ask before changes"),
    ("/approval auto", "review changes automatically"),
    ("/approval yolo", "run changes without asking"),
    ("/mouse", "toggle mouse capture (F2)"),
    ("/mouse on", "capture wheel events"),
    ("/mouse off", "allow terminal text selection"),
    ("/mistl", "show mistl instance info"),
    ("/mistl start", "start mistl"),
    ("/quit", "quit"),
    ("/exit", "quit"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    pub text: String,
    pub desc: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerKind {
    Model,
    Effort,
}

pub struct Picker {
    pub kind: PickerKind,
    pub items: Vec<String>,
    pub filter: String,
    /// Index in the filtered list.
    pub selected: usize,
}

impl Picker {
    fn new(kind: PickerKind, items: Vec<String>, current: &str) -> Self {
        let selected = items.iter().position(|s| s == current).unwrap_or(0);
        Self {
            kind,
            items,
            filter: String::new(),
            selected,
        }
    }

    pub fn filtered(&self) -> Vec<&str> {
        let filter = self.filter.to_lowercase();
        self.items
            .iter()
            .filter(|s| s.to_lowercase().contains(&filter))
            .map(String::as_str)
            .collect()
    }

    fn choice(&self) -> Option<String> {
        self.filtered()
            .get(self.selected)
            .map(|s| (*s).to_string())
            .or_else(|| {
                (self.kind == PickerKind::Effort && !self.filter.trim().is_empty())
                    .then(|| self.filter.trim().to_string())
            })
    }

    fn edit(&mut self, text: &str) {
        self.filter.push_str(text);
        self.selected = 0;
    }

    fn on_key(&mut self, key: KeyEvent) {
        let max = self.filtered().len().saturating_sub(1);
        match key.code {
            KeyCode::Up => self.selected = self.selected.saturating_sub(1),
            KeyCode::Down => self.selected = (self.selected + 1).min(max),
            KeyCode::PageUp => self.selected = self.selected.saturating_sub(10),
            KeyCode::PageDown => self.selected = (self.selected + 10).min(max),
            KeyCode::Backspace => {
                self.filter.pop();
                self.selected = 0;
            }
            KeyCode::Char(c)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.edit(&c.to_string())
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ToolStatus {
    Running,
    Done { ok: bool, output: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    User(String),
    Assistant {
        text: String,
        done: bool,
    },
    Tool {
        id: String,
        title: String,
        status: ToolStatus,
        live_output: String,
    },
    Info(String),
    AutoReview {
        title: String,
        allowed: bool,
        reason: String,
    },
    Error(String),
}

pub struct PendingApproval {
    pub title: String,
    pub reason: String,
    reply: Option<oneshot::Sender<Approval>>,
}

/// Side effects the event loop must perform after handling an input.
#[derive(Debug)]
pub enum Effect {
    Command(UserCommand),
    CancelTurn,
    MouseCapture(bool),
    Copy(String),
    Osc52(String),
    Export { path: PathBuf, text: String },
}

/// Absolute wrapped row and display-cell column, independent of scrolling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TextPoint {
    pub row: usize,
    pub col: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Selection {
    pub anchor: TextPoint,
    pub head: TextPoint,
}

pub struct App {
    pub workspace: WorkspaceSummary,
    pub reasoning_effort: Option<String>,
    pub models: Vec<String>,
    pub fetching: bool,
    pub picker: Option<Picker>,
    pending_picker: bool,
    pub instance: String,
    /// Info operations also emit TurnDone, but never own the busy turn.
    pending_mistl_info: usize,
    pub model: String,
    pub tool_mode: ToolMode,
    pub approval: ApprovalMode,
    pub mouse_capture: bool,
    pub entries: Vec<Entry>,
    pub input: InputBuffer,
    pub busy: bool,
    pub expand_tools: bool,
    /// `None` = follow the bottom; `Some(top_line)` = pinned view.
    pub scroll_top: Option<usize>,
    pub last_total: usize,
    pub last_height: usize,
    pub transcript_rect: Rect,
    /// Only the wrapped lines already built by the renderer, replaced each frame.
    pub last_lines: Vec<String>,
    pub selection: Option<Selection>,
    drag_anchor: Option<TextPoint>,
    pub approvals: VecDeque<PendingApproval>,
    pub tick: u64,
    pub quit: bool,
    pub events_closed: bool,
    pub hint: Option<(String, u64)>,
    history: Vec<String>,
    hist_idx: Option<usize>,
    draft: String,
    /// Selected row of the command completion popup.
    pub suggest_sel: usize,
    /// Popup dismissed with Esc; cleared by the next edit.
    suggest_hidden: bool,
    effects: Vec<Effect>,
}

pub const HELP: &str = "\
Keys:
  Enter            send message
  Alt+Enter/Ctrl+J newline
  Esc              cancel the running turn
  Wheel/PgUp/PgDn  scroll (Shift+wheel scrolls by page)
  Ctrl+Up/Down     scroll by line (Shift+Up/Down too)
  Ctrl+Home/End    top / follow bottom (Home/End too on empty input)
  Shift+Tab        cycle approval: ask / auto / yolo
  F2               toggle mouse capture for text selection
  Left drag        select transcript text and copy on release
  Shift+drag       terminal selection where supported (e.g. Windows Terminal)
  Ctrl+O           expand/collapse tool output
  Up/Down          input history (single-line input)
  Ctrl+U           clear input
  Ctrl+C           cancel turn / quit
  Ctrl+D           quit (empty input)
  /                command completions: Up/Down select, Tab complete,
                   Enter run (or complete a partial command), Esc hide
Commands:
  /help            this help
  /clear           clear conversation
  /copy [all|tool] last assistant reply / whole transcript / full last tool output
  /export [path]  save transcript; default mistl-agent-YYYYMMDD-HHMMSS.md in workspace
  /just            list workspace recipes and the justfile path
  /model [id]      choose or switch model (/models also opens the picker)
  /effort [level]  choose or set reasoning effort (default omits the field)
  /approval [mode] show or set ask / auto / yolo
  /mouse [on|off]  toggle capture; off permits terminal text selection (F2 too)
  /mistl [start]   show instance info or start mistl
  /quit, /exit     quit
  !<command>       run a shell command in the workspace; its output is shared with the model on your next message
Modal keys:
  Type/Up/Down/PgUp/PgDn  filter and navigate a picker; Enter chooses
  Esc             close a picker
Copy uses one newline per selected display row (including soft wraps).
Export refuses to overwrite. Clipboard uses the OS helper and, up to 100000
UTF-8 bytes, OSC 52; terminal copy requires terminal support/permission.";

impl App {
    pub fn new(info: &UiInfo) -> Self {
        let mut app = Self {
            workspace: info.workspace.clone(),
            reasoning_effort: info.reasoning_effort.clone(),
            models: Vec::new(),
            fetching: false,
            picker: None,
            pending_picker: false,
            instance: info.instance.clone(),
            pending_mistl_info: 0,
            model: info.model.clone(),
            tool_mode: info.tool_mode,
            approval: info.approval,
            mouse_capture: true,
            entries: Vec::new(),
            input: InputBuffer::default(),
            busy: false,
            expand_tools: false,
            scroll_top: None,
            last_total: 0,
            last_height: 0,
            transcript_rect: Rect::default(),
            last_lines: Vec::new(),
            selection: None,
            drag_anchor: None,
            approvals: VecDeque::new(),
            tick: 0,
            quit: false,
            events_closed: false,
            hint: None,
            history: Vec::new(),
            hist_idx: None,
            draft: String::new(),
            suggest_sel: 0,
            suggest_hidden: false,
            effects: Vec::new(),
        };
        app.entries.push(Entry::Info(
            "Welcome to mistl agent. Type a request such as \"AI network status?\". Type /help for keys and commands.".into(),
        ));
        app
    }

    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    pub fn spinner(&self) -> char {
        SPINNER[(self.tick as usize) % SPINNER.len()]
    }

    pub fn on_tick(&mut self) {
        self.tick = self.tick.wrapping_add(1);
        if let Some((_, t)) = &self.hint
            && self.tick.wrapping_sub(*t) > HINT_TICKS
        {
            self.hint = None;
        }
    }

    pub fn set_hint(&mut self, s: impl Into<String>) {
        self.hint = Some((s.into(), self.tick));
    }

    pub fn copy_completed(
        &mut self,
        count: usize,
        result: anyhow::Result<Method>,
        osc: Option<String>,
    ) {
        match result {
            Ok(method) => {
                if let Some(sequence) = osc {
                    self.effects.push(Effect::Osc52(sequence));
                }
                self.set_hint(match method {
                    Method::Native => format!("copied {count} chars"),
                    Method::Terminal => format!("copied via terminal ({count} chars)"),
                });
            }
            Err(e) => self.set_hint(format!("clipboard error: {e}")),
        }
    }

    pub fn clear_selection(&mut self) {
        self.selection = None;
        self.drag_anchor = None;
    }

    /// Preserve anchors across append-only streaming; invalidate on reflow/rebuild.
    pub fn remember_transcript(&mut self, rect: Rect, lines: Vec<String>) {
        let rebuilt = self.last_lines.len() > lines.len()
            || self
                .last_lines
                .iter()
                .zip(&lines)
                .enumerate()
                .any(|(i, (old, new))| {
                    if old == new || spinner_only_changed(old, new) {
                        false
                    } else {
                        i + 1 != self.last_lines.len() || !new.starts_with(old)
                    }
                });
        if rect.width != self.transcript_rect.width || rebuilt {
            self.clear_selection();
        }
        self.transcript_rect = rect;
        self.last_lines = lines;
    }

    /// Clamped display-cell range, including wide characters touched at either end.
    pub fn selection_range(&self, row: usize) -> Option<Range<usize>> {
        let selection = self.selection?;
        let (start, end) = if selection.anchor <= selection.head {
            (selection.anchor, selection.head)
        } else {
            (selection.head, selection.anchor)
        };
        let line = self.last_lines.get(row)?;
        if row < start.row || row > end.row {
            return None;
        }
        let width = str_width(line);
        let from = if row == start.row {
            cell_start(line, start.col)
        } else {
            0
        };
        let to = if row == end.row {
            cell_end(line, end.col)
        } else {
            width
        };
        Some(from.min(width)..to.min(width))
    }

    pub fn selected_text(&self) -> String {
        let Some(selection) = self.selection else {
            return String::new();
        };
        let first = selection.anchor.row.min(selection.head.row);
        let last = selection
            .anchor
            .row
            .max(selection.head.row)
            .min(self.last_lines.len().saturating_sub(1));
        (first..=last)
            .filter_map(|row| {
                let range = self.selection_range(row)?;
                let mut col = 0;
                let mut text = String::new();
                let span = Span::raw(&self.last_lines[row]);
                for grapheme in span.styled_graphemes(Style::default()) {
                    let width = str_width(grapheme.symbol);
                    if (width > 0 && col < range.end && col + width > range.start)
                        || (width == 0 && col > range.start && col <= range.end)
                    {
                        text.push_str(grapheme.symbol);
                    }
                    col += width;
                }
                Some(plain_text(&text).trim_end().to_string())
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn mouse_point(&self, mouse: MouseEvent) -> Option<TextPoint> {
        let rect = self.transcript_rect;
        if rect.height == 0 || rect.width == 0 || self.last_lines.is_empty() {
            return None;
        }
        let top = self
            .scroll_top
            .unwrap_or(self.last_total.saturating_sub(self.last_height));
        let y = mouse.row.clamp(rect.y, rect.bottom().saturating_sub(1));
        let row = (top + usize::from(y - rect.y)).min(self.last_lines.len() - 1);
        let col = usize::from(mouse.column.saturating_sub(rect.x)).min(rect.width as usize);
        Some(TextPoint {
            row,
            col: col.min(str_width(&self.last_lines[row])),
        })
    }

    /// Full entry content, independent of viewport width or tool preview limits.
    pub fn transcript_text(&self) -> String {
        self.entries
            .iter()
            .map(|entry| {
                plain_text(&match entry {
                    Entry::User(text) => format!("> {text}"),
                    Entry::Assistant { text, .. } | Entry::Info(text) => text.clone(),
                    Entry::Error(text) => format!("✗ {text}"),
                    Entry::AutoReview {
                        title,
                        allowed,
                        reason,
                    } => format!(
                        "{}: {reason} · {title}",
                        if *allowed {
                            "✓ auto"
                        } else {
                            "✗ auto denied"
                        }
                    ),
                    Entry::Tool {
                        title,
                        status,
                        live_output,
                        ..
                    } => {
                        let (mark, output) = match status {
                            ToolStatus::Running => (format!("▶ {}", self.spinner()), live_output),
                            ToolStatus::Done { ok, output } => {
                                ((if *ok { "✓" } else { "✗" }).into(), output)
                            }
                        };
                        if output.is_empty() {
                            format!("{mark} {title}")
                        } else {
                            format!("{mark} {title}\n{output}")
                        }
                    }
                })
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    fn copy_command(&mut self, arg: &str) {
        let text = match arg {
            "" => self.entries.iter().rev().find_map(|entry| match entry {
                Entry::Assistant { text, .. } => Some(plain_text(text)),
                _ => None,
            }),
            "all" => Some(self.transcript_text()),
            "tool" => self.entries.iter().rev().find_map(|entry| match entry {
                Entry::Tool {
                    status,
                    live_output,
                    ..
                } => Some(plain_text(match status {
                    ToolStatus::Running => live_output,
                    ToolStatus::Done { output, .. } => output,
                })),
                _ => None,
            }),
            _ => {
                self.set_hint("Usage: /copy [all|tool]");
                return;
            }
        };
        match text {
            Some(text) if !text.is_empty() => self.effects.push(Effect::Copy(text)),
            _ => self.set_hint("Nothing to copy."),
        }
    }

    fn export_path(&self, arg: &str) -> PathBuf {
        let path = if arg.is_empty() {
            PathBuf::from(format!(
                "mistl-agent-{}.md",
                chrono::Local::now().format("%Y%m%d-%H%M%S")
            ))
        } else {
            PathBuf::from(arg)
        };
        if path.is_absolute() {
            path
        } else {
            PathBuf::from(&self.workspace.root).join(path)
        }
    }

    /// Label for the footer while busy.
    pub fn busy_label(&self) -> String {
        let running = self.entries.iter().rev().find_map(|e| match e {
            Entry::Tool {
                title,
                status: ToolStatus::Running,
                ..
            } => Some(title.clone()),
            _ => None,
        });
        match running {
            Some(t) => format!("running {t}"),
            None => "thinking…".into(),
        }
    }

    // ---------------------------------------------------------------- agent

    pub fn on_agent_event(&mut self, ev: AgentEvent) {
        match ev {
            AgentEvent::TextDelta(t) => match self.entries.last_mut() {
                Some(Entry::Assistant { text, done: false }) => text.push_str(&t),
                _ => self.entries.push(Entry::Assistant {
                    text: t,
                    done: false,
                }),
            },
            AgentEvent::AssistantDone => self.close_assistant(),
            AgentEvent::ToolStart { id, title } => {
                self.close_assistant();
                self.entries.push(Entry::Tool {
                    id,
                    title,
                    status: ToolStatus::Running,
                    live_output: String::new(),
                });
            }
            AgentEvent::ToolEnd { id, ok, output } => {
                let found = self.entries.iter_mut().rev().find_map(|e| match e {
                    Entry::Tool {
                        id: i,
                        status,
                        live_output,
                        ..
                    } if *i == id => {
                        live_output.clear();
                        Some(status)
                    }
                    _ => None,
                });
                match found {
                    Some(status) => *status = ToolStatus::Done { ok, output },
                    None => self.entries.push(Entry::Tool {
                        title: id.clone(),
                        id,
                        status: ToolStatus::Done { ok, output },
                        live_output: String::new(),
                    }),
                }
            }
            AgentEvent::ApprovalRequest {
                title,
                reason,
                reply,
            } => {
                self.approvals.push_back(PendingApproval {
                    title,
                    reason,
                    reply: Some(reply),
                });
            }
            AgentEvent::ToolOutput { id, chunk } => {
                if let Some(output) = self.entries.iter_mut().rev().find_map(|e| match e {
                    Entry::Tool {
                        id: i,
                        status: ToolStatus::Running,
                        live_output,
                        ..
                    } if *i == id => Some(live_output),
                    _ => None,
                }) {
                    output.push_str(&chunk);
                    if output.len() > MAX_LIVE_OUTPUT {
                        let mut start = output.len() - MAX_LIVE_OUTPUT;
                        while !output.is_char_boundary(start) {
                            start += 1;
                        }
                        output.drain(..start);
                    }
                }
            }
            AgentEvent::Info(m) => self.entries.push(Entry::Info(m)),
            AgentEvent::ToolModeChanged(mode) => {
                self.tool_mode = mode;
            }
            AgentEvent::AutoReview {
                title,
                allowed,
                reason,
            } => {
                self.close_assistant();
                self.entries.push(Entry::AutoReview {
                    title,
                    allowed,
                    reason,
                });
            }
            AgentEvent::ApprovalModeChanged(mode) => self.approval = mode,
            AgentEvent::Models { models, error } => {
                self.fetching = false;
                let requested = std::mem::take(&mut self.pending_picker);
                if let Some(error) = error {
                    self.entries
                        .push(Entry::Error(format!("could not list models: {error}")));
                } else {
                    self.models = models;
                    if requested {
                        if let Some(picker) = &mut self.picker {
                            picker.items = self.models.clone();
                            picker.selected = picker
                                .selected
                                .min(picker.filtered().len().saturating_sub(1));
                        } else {
                            self.picker = Some(Picker::new(
                                PickerKind::Model,
                                self.models.clone(),
                                &self.model,
                            ));
                        }
                    }
                }
            }
            AgentEvent::Error(m) => self.entries.push(Entry::Error(m)),
            AgentEvent::TurnDone => {
                if self.pending_mistl_info > 0 {
                    self.pending_mistl_info -= 1;
                } else {
                    self.close_assistant();
                    self.busy = false;
                }
            }
        }
    }

    fn close_assistant(&mut self) {
        if let Some(Entry::Assistant { done, .. }) = self.entries.last_mut() {
            *done = true;
        }
    }

    pub fn on_events_closed(&mut self) {
        self.events_closed = true;
        self.busy = false;
        self.fetching = false;
        self.entries.push(Entry::Error(
            "Agent stopped unexpectedly. Press any key to quit.".into(),
        ));
    }

    // ------------------------------------------------------------ approvals

    pub fn current_approval(&self) -> Option<&PendingApproval> {
        self.approvals.front()
    }

    /// Answer the front approval request exactly once.
    pub fn answer_approval(&mut self, a: Approval) {
        let Some(mut p) = self.approvals.pop_front() else {
            return;
        };
        if let Some(tx) = p.reply.take() {
            let _ = tx.send(a);
        }
        if a == Approval::Always {
            self.approval = ApprovalMode::Yolo;
        }
    }

    // ---------------------------------------------------------------- input

    pub fn on_paste(&mut self, s: &str) {
        if !self.approvals.is_empty() || self.events_closed {
            return;
        }
        let text: String = s.chars().filter(|c| !c.is_control()).collect();
        if let Some(picker) = &mut self.picker {
            picker.edit(&text);
        } else {
            self.input.insert_str(s);
            self.suggest_sel = 0;
            self.suggest_hidden = false;
        }
    }

    pub fn scroll_up(&mut self, n: usize) {
        let max = self.last_total.saturating_sub(self.last_height);
        let cur = self.scroll_top.unwrap_or(max).min(max);
        self.scroll_top = Some(cur.saturating_sub(n));
    }

    pub fn scroll_down(&mut self, n: usize) {
        let max = self.last_total.saturating_sub(self.last_height);
        if let Some(t) = self.scroll_top {
            let nt = t.saturating_add(n);
            self.scroll_top = if nt >= max { None } else { Some(nt) };
        }
    }

    pub fn on_mouse(&mut self, mouse: MouseEvent) {
        if !self.mouse_capture || self.events_closed || !self.approvals.is_empty() {
            return;
        }
        if matches!(
            mouse.kind,
            MouseEventKind::Down(MouseButton::Left)
                | MouseEventKind::Drag(MouseButton::Left)
                | MouseEventKind::Up(MouseButton::Left)
        ) {
            if self.modal_open() || !self.suggestions().is_empty() {
                return;
            }
            match mouse.kind {
                MouseEventKind::Down(_)
                    if self
                        .transcript_rect
                        .contains((mouse.column, mouse.row).into()) =>
                {
                    self.clear_selection();
                    self.drag_anchor = self.mouse_point(mouse);
                }
                MouseEventKind::Drag(_) if self.drag_anchor.is_some() => {
                    // Pin the viewport while selecting, even as new output arrives.
                    self.scroll_top = Some(
                        self.scroll_top
                            .unwrap_or(self.last_total.saturating_sub(self.last_height)),
                    );
                    if mouse.row < self.transcript_rect.y {
                        self.scroll_up(1);
                    } else if mouse.row >= self.transcript_rect.bottom() {
                        let max = self.last_total.saturating_sub(self.last_height);
                        self.scroll_top =
                            Some(self.scroll_top.unwrap_or(max).saturating_add(1).min(max));
                    }
                    if let (Some(anchor), Some(head)) = (self.drag_anchor, self.mouse_point(mouse))
                    {
                        if anchor != head {
                            self.selection = Some(Selection { anchor, head });
                        }
                    }
                }
                MouseEventKind::Up(_) => {
                    if self.drag_anchor.take().is_some() && self.selection.is_some() {
                        let text = self.selected_text();
                        if !text.is_empty() {
                            self.effects.push(Effect::Copy(text));
                        }
                    }
                }
                _ => {}
            }
            return;
        }
        let up = match mouse.kind {
            MouseEventKind::ScrollUp => true,
            MouseEventKind::ScrollDown => false,
            _ => return,
        };
        let page = mouse.modifiers.contains(KeyModifiers::SHIFT);
        if let Some(picker) = &mut self.picker {
            let n = if page { 10 } else { 1 };
            picker.selected = if up {
                picker.selected.saturating_sub(n)
            } else {
                picker
                    .selected
                    .saturating_add(n)
                    .min(picker.filtered().len().saturating_sub(1))
            };
        } else {
            let suggestions = self.suggestions();
            if !suggestions.is_empty() {
                let n = if page { 8 } else { 1 };
                let sel = self.suggest_sel.min(suggestions.len() - 1);
                self.suggest_sel = if up {
                    sel.saturating_sub(n)
                } else {
                    sel.saturating_add(n).min(suggestions.len() - 1)
                };
            } else {
                let n = if page {
                    self.last_height.saturating_sub(1).max(1)
                } else {
                    3
                };
                if up {
                    self.scroll_up(n);
                } else {
                    self.scroll_down(n);
                }
            }
        }
    }

    fn set_mouse_capture(&mut self, enabled: bool) {
        self.clear_selection();
        self.mouse_capture = enabled;
        self.effects.push(Effect::MouseCapture(enabled));
        self.set_hint(if enabled {
            "Mouse capture on: drag to copy; wheel scrolls; /copy for full text."
        } else {
            "Mouse capture off: select/copy text; F2 or /mouse on to scroll with the wheel."
        });
    }

    fn set_approval_mode(&mut self, mode: ApprovalMode) {
        self.approval = mode;
        self.effects
            .push(Effect::Command(UserCommand::SetApprovalMode(mode)));
    }

    pub fn on_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }
        if self.events_closed {
            self.quit = true;
            return;
        }
        if key.code == KeyCode::F(2) {
            self.set_mouse_capture(!self.mouse_capture);
            return;
        }
        if !self.approvals.is_empty() {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    self.answer_approval(Approval::Yes)
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.answer_approval(Approval::No)
                }
                KeyCode::Char('a') | KeyCode::Char('A') => self.answer_approval(Approval::Always),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.answer_approval(Approval::No)
                }
                _ => {}
            }
            return;
        }

        if self.picker.is_some() {
            self.on_picker_key(key);
            return;
        }
        if key.code == KeyCode::BackTab {
            self.set_approval_mode(self.approval.next());
            return;
        }

        let before = self.input.text().to_string();
        if !self.on_suggest_key(key) {
            self.on_input_key(key);
        }
        if self.input.text() != before {
            self.suggest_sel = 0;
            self.suggest_hidden = false;
        }
    }

    fn on_input_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let alt = key.modifiers.contains(KeyModifiers::ALT);
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        let page = self.last_height.saturating_sub(1).max(1);

        match key.code {
            KeyCode::Char('c') if ctrl => {
                if self.busy || self.fetching {
                    self.effects.push(Effect::CancelTurn);
                } else {
                    self.quit = true;
                }
            }
            KeyCode::Char('d') if ctrl => {
                if self.input.is_empty() {
                    self.quit = true;
                }
            }
            KeyCode::Char('o') if ctrl => {
                self.clear_selection();
                self.expand_tools = !self.expand_tools;
            }
            KeyCode::Char('u') if ctrl => self.input.clear(),
            KeyCode::Char('j') if ctrl => self.input.insert_char('\n'),
            KeyCode::Enter if alt || shift || ctrl => self.input.insert_char('\n'),
            KeyCode::Enter => self.submit(),
            KeyCode::Esc => {
                if self.busy || self.fetching {
                    self.pending_picker = false;
                    self.effects.push(Effect::CancelTurn);
                }
            }
            KeyCode::PageUp => self.scroll_up(page),
            KeyCode::PageDown => self.scroll_down(page),
            KeyCode::Up if ctrl || shift => self.scroll_up(1),
            KeyCode::Down if ctrl || shift => self.scroll_down(1),
            KeyCode::Up => {
                if self.input.is_multiline() {
                    self.input.up();
                } else {
                    self.history_prev();
                }
            }
            KeyCode::Down => {
                if self.input.is_multiline() {
                    self.input.down();
                } else {
                    self.history_next();
                }
            }
            KeyCode::Left => self.input.left(),
            KeyCode::Right => self.input.right(),
            KeyCode::Home if ctrl || self.input.is_empty() => self.scroll_top = Some(0),
            KeyCode::Home => self.input.home(),
            KeyCode::End => {
                if ctrl || self.input.is_empty() {
                    self.scroll_top = None;
                } else {
                    self.input.end();
                }
            }
            KeyCode::Backspace => self.input.backspace(),
            KeyCode::Delete => self.input.delete(),
            KeyCode::Tab => self.input.insert_str("  "),
            KeyCode::Char(c) if !ctrl && !alt => self.input.insert_char(c),
            _ => {}
        }
    }

    pub fn modal_open(&self) -> bool {
        self.current_approval().is_some() || self.picker.is_some()
    }

    /// Slash and shell recipe completions; empty when the popup
    /// should not be shown.
    pub fn suggestions(&self) -> Vec<Suggestion> {
        let text = self.input.text();
        if self.suggest_hidden
            || self.hist_idx.is_some()
            || self.input.is_multiline()
            || !(text.starts_with('/') || text.starts_with('!'))
        {
            return Vec::new();
        }
        let mut all: Vec<Suggestion> = Vec::new();
        if text.starts_with('!') {
            all.extend(self.workspace.recipes.iter().map(|recipe| {
                let mut desc = recipe.params.clone();
                if let Some(doc) = &recipe.doc {
                    if !desc.is_empty() && !doc.is_empty() {
                        desc.push_str("  # ");
                    }
                    desc.push_str(doc);
                }
                Suggestion {
                    text: format!("!just {}", recipe.name),
                    desc: truncate_text(&desc.split_whitespace().collect::<Vec<_>>().join(" "), 80),
                }
            }));
        } else {
            for (cmd, desc) in COMMANDS {
                all.push(Suggestion {
                    text: cmd.into(),
                    desc: desc.into(),
                });
                match cmd {
                    "/models" => all.extend(self.models.iter().map(|m| Suggestion {
                        text: format!("/model {m}"),
                        desc: "switch to this model".into(),
                    })),
                    "/effort" => all.extend(EFFORTS.iter().map(|e| Suggestion {
                        text: format!("/effort {e}"),
                        desc: "set reasoning effort".into(),
                    })),
                    _ => {}
                }
            }
        }
        let typed = text.to_lowercase();
        let found: Vec<Suggestion> = all
            .into_iter()
            .filter(|s| s.text.to_lowercase().starts_with(&typed))
            .collect();
        // A lone exact match has nothing left to suggest.
        if found.len() == 1 && found[0].text == text && !self.recipe_has_params(text) {
            return Vec::new();
        }
        found
    }

    fn recipe_has_params(&self, command: &str) -> bool {
        command.strip_prefix("!just ").is_some_and(|name| {
            self.workspace
                .recipes
                .iter()
                .any(|recipe| recipe.name == name && !recipe.params.trim().is_empty())
        })
    }

    /// Handle a key aimed at the completion popup. Returns false when the key
    /// should fall through to the normal input handling.
    fn on_suggest_key(&mut self, key: KeyEvent) -> bool {
        if !key.modifiers.is_empty() && key.code != KeyCode::BackTab {
            return false;
        }
        let list = self.suggestions();
        if list.is_empty() {
            return false;
        }
        let n = list.len();
        let sel = self.suggest_sel.min(n - 1);
        match key.code {
            KeyCode::Up | KeyCode::BackTab => self.suggest_sel = (sel + n - 1) % n,
            KeyCode::Down => self.suggest_sel = (sel + 1) % n,
            KeyCode::Tab => {
                let chosen = &list[sel].text;
                // Completing an already complete command steps into its
                // arguments, e.g. "/effort" -> "/effort ".
                let next = if chosen == self.input.text() || self.recipe_has_params(chosen) {
                    format!("{chosen} ")
                } else {
                    chosen.clone()
                };
                self.input.set(&next);
                self.suggest_sel = 0;
            }
            KeyCode::Enter => {
                if list[sel].text.starts_with('!') {
                    let chosen = &list[sel].text;
                    if self.recipe_has_params(chosen) {
                        self.input.set(&format!("{chosen} "));
                    } else {
                        self.input.set(chosen);
                        self.submit();
                    }
                    self.suggest_sel = 0;
                    return true;
                }
                if list[sel].text == self.input.text() {
                    return false;
                }
                let chosen = list[sel].text.clone();
                self.input.set(&chosen);
                self.suggest_sel = 0;
                // A command that takes no further input runs right away;
                // otherwise leave it in the editor so arguments can be added.
                if !list
                    .iter()
                    .any(|s| s.text.starts_with(&format!("{chosen} ")))
                {
                    self.submit();
                }
            }
            KeyCode::Esc if !(self.busy || self.fetching) => self.suggest_hidden = true,
            _ => return false,
        }
        true
    }

    fn request_models(&mut self) {
        if self.fetching {
            self.set_hint(
                "Already fetching models. Wait for it to finish, or press Esc to cancel.",
            );
            return;
        }
        self.fetching = true;
        self.pending_picker = true;
        if !self.models.is_empty() {
            self.picker = Some(Picker::new(
                PickerKind::Model,
                self.models.clone(),
                &self.model,
            ));
        }
        self.effects.push(Effect::Command(UserCommand::ListModels));
    }

    fn set_model(&mut self, model: String) {
        self.model = model.clone();
        self.entries
            .push(Entry::Info(format!("Model set to {model}.")));
        self.effects
            .push(Effect::Command(UserCommand::SetModel(model)));
    }

    fn set_effort(&mut self, effort: &str) {
        let effort = normalize_effort(Some(effort.into()));
        self.reasoning_effort = effort.clone();
        self.effects
            .push(Effect::Command(UserCommand::SetEffort(effort)));
    }

    fn on_picker_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Esc {
            self.picker = None;
            if self.pending_picker {
                self.pending_picker = false;
                self.effects.push(Effect::CancelTurn);
            }
        } else if key.code == KeyCode::Enter {
            if self.busy {
                self.set_hint("Cannot change model or effort while a turn is running.");
                return;
            }
            let choice = self.picker.as_ref().and_then(Picker::choice);
            if let Some(choice) = choice {
                let picker = self.picker.take().unwrap();
                if self.pending_picker {
                    self.pending_picker = false;
                    self.effects.push(Effect::CancelTurn);
                }
                match picker.kind {
                    PickerKind::Model => self.set_model(choice),
                    PickerKind::Effort => self.set_effort(&choice),
                }
            }
        } else if let Some(picker) = &mut self.picker {
            picker.on_key(key);
        }
    }

    fn history_prev(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let idx = match self.hist_idx {
            None => {
                self.draft = self.input.text().to_string();
                self.history.len() - 1
            }
            Some(i) => i.saturating_sub(1),
        };
        self.hist_idx = Some(idx);
        let s = self.history[idx].clone();
        self.input.set(&s);
    }

    fn history_next(&mut self) {
        let Some(i) = self.hist_idx else {
            return;
        };
        if i + 1 >= self.history.len() {
            self.hist_idx = None;
            let d = std::mem::take(&mut self.draft);
            self.input.set(&d);
        } else {
            self.hist_idx = Some(i + 1);
            let s = self.history[i + 1].clone();
            self.input.set(&s);
        }
    }

    fn submit(&mut self) {
        let typed = self.input.text().to_string();
        let shell_mode = self.input.text().starts_with('!');
        let text = self.input.text().trim().to_string();
        if text.is_empty() {
            return;
        }
        if let Some(cmd) = text.strip_prefix('/') {
            self.input.clear();
            self.remember(&text);
            self.slash(cmd.trim());
            return;
        }
        if self.busy || self.fetching {
            self.set_hint("A turn is running. Wait for it to finish, or press Esc to cancel.");
            return;
        }
        if shell_mode {
            let command = text[1..].trim();
            if command.is_empty() {
                self.set_hint("Usage: !<command>");
                return;
            }
            let command = command.to_string();
            self.input.clear();
            self.remember(&text);
            self.busy = true;
            self.scroll_top = None;
            self.effects
                .push(Effect::Command(UserCommand::Shell(command)));
            return;
        }
        self.input.clear();
        self.remember(&text);
        self.entries.push(Entry::User(text.clone()));
        self.busy = true;
        self.scroll_top = None;
        self.effects.push(Effect::Command(UserCommand::Send(typed)));
    }

    fn remember(&mut self, text: &str) {
        if self.history.last().map(|s| s.as_str()) != Some(text) {
            self.history.push(text.to_string());
        }
        self.hist_idx = None;
        self.draft.clear();
    }

    fn slash(&mut self, cmd: &str) {
        let (name, arg) = match cmd.split_once(char::is_whitespace) {
            Some((n, a)) => (n, a.trim()),
            None => (cmd, ""),
        };
        if self.busy && matches!(name, "model" | "models" | "effort" | "mistl") {
            self.set_hint(
                "Cannot change model or effort, or manage mistl while a turn is running (Esc to cancel).",
            );
            return;
        }
        match name {
            "copy" => self.copy_command(arg),
            "export" => self.effects.push(Effect::Export {
                path: self.export_path(arg),
                text: self.transcript_text(),
            }),
            "approval" => {
                if arg.is_empty() {
                    self.entries.push(Entry::Info(format!(
                        "Approval: {}. Choices: /approval ask | auto | yolo (Shift+Tab cycles).",
                        self.approval.as_str()
                    )));
                } else if let Some(mode) = ApprovalMode::parse(arg) {
                    self.set_approval_mode(mode);
                } else {
                    self.set_hint("Usage: /approval ask|auto|yolo");
                }
            }
            "mouse" => match arg {
                "" => self.set_mouse_capture(!self.mouse_capture),
                "on" => self.set_mouse_capture(true),
                "off" => self.set_mouse_capture(false),
                _ => self.set_hint("Usage: /mouse [on|off]"),
            },
            "help" | "?" => self.entries.push(Entry::Info(HELP.into())),
            "just" => {
                let mut text = match &self.workspace.justfile {
                    Some(path) => {
                        let mut text = format!("Justfile: {path}");
                        for recipe in &self.workspace.recipes {
                            text.push('\n');
                            text.push_str(&recipe.name);
                            if !recipe.params.is_empty() {
                                text.push(' ');
                                text.push_str(&recipe.params);
                            }
                            if let Some(doc) = &recipe.doc
                                && !doc.is_empty()
                            {
                                text.push_str("  # ");
                                text.push_str(doc);
                            }
                        }
                        text
                    }
                    None => format!("No justfile found in {}.", self.workspace.root),
                };
                if let Some(error) = &self.workspace.error {
                    text.push_str(&format!("\nError: {error}"));
                }
                self.entries.push(Entry::Info(text));
                self.scroll_top = None;
            }
            "clear" => {
                if self.busy {
                    self.set_hint("Cannot /clear while a turn is running (Esc to cancel).");
                    return;
                }
                self.entries.clear();
                self.clear_selection();
                self.last_lines.clear();
                self.scroll_top = None;
                self.effects.push(Effect::Command(UserCommand::Clear));
            }
            "model" if !arg.is_empty() => self.set_model(arg.into()),
            "model" | "models" => self.request_models(),
            "effort" if !arg.is_empty() => self.set_effort(arg),
            "effort" => {
                self.pending_picker = false;
                self.picker = Some(Picker::new(
                    PickerKind::Effort,
                    EFFORTS.iter().map(|s| (*s).into()).collect(),
                    self.reasoning_effort.as_deref().unwrap_or("default"),
                ))
            }
            "mistl" => {
                let op = match arg {
                    "" => MistlOp::Info,
                    "start" => MistlOp::Start,
                    _ => {
                        self.set_hint("Usage: /mistl [start]");
                        return;
                    }
                };
                self.busy = op != MistlOp::Info;
                if op == MistlOp::Info {
                    self.pending_mistl_info += 1;
                }
                self.effects.push(Effect::Command(UserCommand::Mistl(op)));
            }
            "quit" | "exit" => self.quit = true,
            _ => self
                .entries
                .push(Entry::Info(format!("Unknown command: /{name}. Try /help."))),
        }
    }
}

fn spinner_only_changed(old: &str, new: &str) -> bool {
    fn tail(s: &str) -> Option<&str> {
        let s = s.strip_prefix("▶ ")?;
        let ch = s.chars().next()?;
        SPINNER.contains(&ch).then(|| &s[ch.len_utf8()..])
    }
    matches!((tail(old), tail(new)), (Some(a), Some(b)) if a == b)
}

fn cell_start(line: &str, target: usize) -> usize {
    let mut col = 0;
    let span = Span::raw(line);
    for grapheme in span.styled_graphemes(Style::default()) {
        let width = str_width(grapheme.symbol);
        if col + width > target {
            return col;
        }
        col += width;
    }
    col
}

fn cell_end(line: &str, target: usize) -> usize {
    let mut col = 0;
    let span = Span::raw(line);
    for grapheme in span.styled_graphemes(Style::default()) {
        col += str_width(grapheme.symbol);
        if col > target {
            return col;
        }
    }
    col
}

/// Remove terminal control sequences from copied/exported plain text.
pub fn plain_text(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\x1b' => match chars.next() {
                Some('[') => {
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']' | 'P' | 'X' | '^' | '_') => {
                    while let Some(c) = chars.next() {
                        if c == '\x07' {
                            break;
                        }
                        if c == '\x1b' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                Some(c) if (' '..='/').contains(&c) => {
                    for c in chars.by_ref() {
                        if ('0'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                _ => {}
            },
            '\n' | '\t' => out.push(ch),
            c if !c.is_control() => out.push(c),
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyEventState;

    fn info() -> UiInfo {
        UiInfo::from_config(&crate::agent::config::Config::default())
    }

    fn key(code: KeyCode, mods: KeyModifiers, kind: KeyEventKind) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: mods,
            kind,
            state: KeyEventState::NONE,
        }
    }
    fn press(app: &mut App, code: KeyCode) {
        app.on_key(key(code, KeyModifiers::NONE, KeyEventKind::Press));
    }

    fn command(app: &mut App, text: &str) {
        app.on_paste(text);
        press(app, KeyCode::Enter);
    }

    #[test]
    fn new_slash_commands_and_busy_guard() {
        let mut app = App::new(&info());
        command(&mut app, "/mistl start");
        assert!(app.busy);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Mistl(MistlOp::Start))]
        ));
        for cmd in [
            "/mistl",
            "/mistl start",
            "/model",
            "/model id",
            "/models",
            "/effort",
            "/effort high",
        ] {
            command(&mut app, cmd);
            assert!(app.take_effects().is_empty(), "{cmd}");
            assert!(!app.modal_open());
            assert!(!app.fetching);
        }
        app.on_agent_event(AgentEvent::TurnDone);
        command(&mut app, "/mistl start");
        assert!(app.busy);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Mistl(MistlOp::Start))]
        ));
        app.on_agent_event(AgentEvent::TurnDone);
        command(&mut app, "/mistl");
        assert!(!app.busy);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Mistl(MistlOp::Info))]
        ));
        command(&mut app, "/effort high");
        assert_eq!(app.reasoning_effort.as_deref(), Some("high"));
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Command(UserCommand::SetEffort(Some(e)))] if e == "high")
        );
        command(&mut app, "/effort default");
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::SetEffort(None))]
        ));
        command(&mut app, "/model");
        assert!(app.fetching);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::ListModels)]
        ));
        press(&mut app, KeyCode::Esc);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::CancelTurn]
        ));
        app.on_agent_event(AgentEvent::Models {
            models: vec![],
            error: Some("cancelled".into()),
        });
        assert!(!app.fetching);
        assert!(app.picker.is_none());
        command(&mut app, "/models");
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::ListModels)]
        ));
    }

    #[test]
    fn models_open_requested_picker_without_manual_choice() {
        let mut app = App::new(&info());
        command(&mut app, "/model");
        app.take_effects();
        app.on_agent_event(AgentEvent::Models {
            models: vec!["Alpha".into(), "beta".into(), "ALPINE".into()],
            error: None,
        });
        assert!(!app.fetching);
        assert_eq!(app.models.len(), 3);
        app.on_paste("aLp");
        assert_eq!(app.picker.as_ref().unwrap().filtered(), ["Alpha", "ALPINE"]);
        press(&mut app, KeyCode::Down);
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.model, "ALPINE");
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Command(UserCommand::SetModel(m))] if m == "ALPINE")
        );
        command(&mut app, "/model");
        app.take_effects();
        app.on_agent_event(AgentEvent::Models {
            models: app.models.clone(),
            error: None,
        });
        app.on_paste("custom-model");
        assert!(app.picker.as_ref().unwrap().filtered().is_empty());
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.model, "ALPINE");
        assert!(app.picker.is_some());
        assert!(app.input.is_empty());
    }

    #[test]
    fn picker_navigation_effort_and_cancel_swallow_keys() {
        let mut picker = Picker::new(
            PickerKind::Model,
            (0..25).map(|i| format!("m{i}")).collect(),
            "m12",
        );
        picker.on_key(key(
            KeyCode::PageDown,
            KeyModifiers::NONE,
            KeyEventKind::Press,
        ));
        assert_eq!(picker.selected, 22);
        picker.on_key(key(
            KeyCode::PageDown,
            KeyModifiers::NONE,
            KeyEventKind::Press,
        ));
        assert_eq!(picker.selected, 24);
        picker.on_key(key(
            KeyCode::PageUp,
            KeyModifiers::NONE,
            KeyEventKind::Press,
        ));
        assert_eq!(picker.selected, 14);
        let mut app = App::new(&info());
        command(&mut app, "/effort");
        app.on_paste("HIGH");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.reasoning_effort.as_deref(), Some("high"));
        app.take_effects();
        command(&mut app, "/effort");
        app.on_paste("default");
        press(&mut app, KeyCode::Enter);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::SetEffort(None))]
        ));
        command(&mut app, "/effort");
        app.on_key(key(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
            KeyEventKind::Press,
        ));
        assert!(!app.quit);
        press(&mut app, KeyCode::Esc);
        assert!(app.take_effects().is_empty());
        assert!(!app.modal_open());
    }

    #[test]
    fn mistl_info_completion_does_not_release_a_later_turn() {
        let mut app = App::new(&info());
        command(&mut app, "/mistl");
        command(&mut app, "/mistl");
        command(&mut app, "question");
        assert!(app.busy);
        app.on_agent_event(AgentEvent::TurnDone);
        assert!(app.busy);
        app.on_agent_event(AgentEvent::TurnDone);
        assert!(app.busy);
        app.on_agent_event(AgentEvent::TurnDone);
        assert!(!app.busy);
    }

    #[test]
    fn model_revalidation_keeps_filter_and_does_not_reopen_after_cancel() {
        let mut app = App::new(&info());
        app.models = vec!["Alpha".into(), "beta".into()];
        command(&mut app, "/model");
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::ListModels)]
        ));
        app.on_paste("alp");
        app.on_agent_event(AgentEvent::Models {
            models: vec!["Alpha".into(), "ALPINE".into()],
            error: None,
        });
        assert_eq!(app.picker.as_ref().unwrap().filter, "alp");
        assert_eq!(app.picker.as_ref().unwrap().filtered(), ["Alpha", "ALPINE"]);
        press(&mut app, KeyCode::Esc);
        command(&mut app, "/model");
        app.take_effects();
        press(&mut app, KeyCode::Esc);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::CancelTurn]
        ));
        app.on_agent_event(AgentEvent::Models {
            models: vec!["late".into()],
            error: None,
        });
        assert!(!app.fetching);
        assert!(app.picker.is_none());
        assert_eq!(app.models, ["late"]);
    }

    #[test]
    fn runtime_tool_mode_updates_header_state() {
        let mut app = App::new(&info());
        app.on_agent_event(AgentEvent::ToolModeChanged(ToolMode::Prompt));
        assert_eq!(app.tool_mode, ToolMode::Prompt);
    }

    #[test]
    fn streaming_deltas_accumulate() {
        let mut a = App::new(&info());
        a.on_agent_event(AgentEvent::TextDelta("こん".into()));
        a.on_agent_event(AgentEvent::TextDelta("にちは".into()));
        a.on_agent_event(AgentEvent::AssistantDone);
        a.on_agent_event(AgentEvent::TextDelta("next".into()));
        let asst: Vec<_> = a
            .entries
            .iter()
            .filter(|e| matches!(e, Entry::Assistant { .. }))
            .collect();
        assert_eq!(asst.len(), 2);
        assert_eq!(
            asst[0],
            &Entry::Assistant {
                text: "こんにちは".into(),
                done: true
            }
        );
        assert_eq!(
            asst[1],
            &Entry::Assistant {
                text: "next".into(),
                done: false
            }
        );
    }

    #[test]
    fn tool_start_end_pair_by_id() {
        let mut a = App::new(&info());
        a.on_agent_event(AgentEvent::ToolStart {
            id: "1".into(),
            title: "mistl a".into(),
        });
        a.on_agent_event(AgentEvent::ToolStart {
            id: "2".into(),
            title: "mistl b".into(),
        });
        a.on_agent_event(AgentEvent::ToolEnd {
            id: "1".into(),
            ok: true,
            output: "x".into(),
        });
        let statuses: Vec<_> = a
            .entries
            .iter()
            .filter_map(|e| match e {
                Entry::Tool { id, status, .. } => Some((id.clone(), status.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            statuses[0],
            (
                "1".into(),
                ToolStatus::Done {
                    ok: true,
                    output: "x".into()
                }
            )
        );
        assert_eq!(statuses[1], ("2".into(), ToolStatus::Running));
        assert_eq!(a.busy_label(), "running mistl b");
    }

    #[test]
    fn approval_answers_exactly_once_and_queues() {
        let mut a = App::new(&info());
        let (t1, mut r1) = oneshot::channel();
        let (t2, mut r2) = oneshot::channel();
        a.on_agent_event(AgentEvent::ApprovalRequest {
            title: "one".into(),
            reason: "".into(),
            reply: t1,
        });
        a.on_agent_event(AgentEvent::ApprovalRequest {
            title: "two".into(),
            reason: "".into(),
            reply: t2,
        });
        // keys other than the modal ones are swallowed
        press(&mut a, KeyCode::Char('x'));
        assert!(a.input.is_empty());
        press(&mut a, KeyCode::Char('y'));
        assert_eq!(r1.try_recv().unwrap(), Approval::Yes);
        assert!(r2.try_recv().is_err());
        assert_eq!(a.current_approval().unwrap().title, "two");
        assert_eq!(a.approval, ApprovalMode::Ask);
        press(&mut a, KeyCode::Char('a'));
        assert_eq!(r2.try_recv().unwrap(), Approval::Always);
        assert_eq!(a.approval, ApprovalMode::Yolo);
        // nothing pending: extra keys do not panic or re-answer
        press(&mut a, KeyCode::Char('y'));
        assert_eq!(a.input.text(), "y");
    }

    #[test]
    fn send_busy_and_release_ignored() {
        let mut a = App::new(&info());
        a.on_key(key(
            KeyCode::Char('h'),
            KeyModifiers::NONE,
            KeyEventKind::Release,
        ));
        assert!(a.input.is_empty());
        a.on_paste("日本語\nline2");
        press(&mut a, KeyCode::Enter);
        assert!(a.busy);
        assert!(matches!(
            a.take_effects()[0],
            Effect::Command(UserCommand::Send(_))
        ));
        a.on_paste("again");
        press(&mut a, KeyCode::Enter);
        assert!(a.take_effects().is_empty());
        assert!(a.hint.is_some());
        a.on_agent_event(AgentEvent::TurnDone);
        assert!(!a.busy);
    }

    #[test]
    fn slash_commands() {
        let mut a = App::new(&info());
        a.on_paste("/model foo");
        press(&mut a, KeyCode::Enter);
        assert_eq!(a.model, "foo");
        assert!(matches!(
            a.take_effects()[0],
            Effect::Command(UserCommand::SetModel(_))
        ));
        a.on_paste("/clear");
        press(&mut a, KeyCode::Enter);
        assert!(a.entries.is_empty());
        a.on_paste("/nope");
        press(&mut a, KeyCode::Enter);
        assert!(matches!(a.entries[0], Entry::Info(_)));
        a.on_paste("/quit");
        press(&mut a, KeyCode::Enter);
        assert!(a.quit);
    }

    fn texts(a: &App) -> Vec<String> {
        a.suggestions().into_iter().map(|s| s.text).collect()
    }

    fn workspace_info() -> UiInfo {
        let mut info = info();
        info.workspace = WorkspaceSummary {
            root: "project".into(),
            justfile: Some("project/justfile".into()),
            recipes: vec![
                crate::agent::types::RecipeInfo {
                    name: "build".into(),
                    params: String::new(),
                    doc: Some("Build the application".into()),
                },
                crate::agent::types::RecipeInfo {
                    name: "test".into(),
                    params: "filter=\"\" *args".into(),
                    doc: Some("Run tests".into()),
                },
            ],
            error: None,
        };
        info
    }

    #[test]
    fn shell_submit_records_history_without_user_entry() {
        let mut app = App::new(&workspace_info());
        let entries = app.entries.clone();
        app.scroll_top = Some(0);
        command(&mut app, "!  echo hello  ");
        assert!(matches!(app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Shell(cmd))] if cmd == "echo hello"));
        assert!(app.busy);
        assert!(app.input.is_empty());
        assert_eq!(app.scroll_top, None);
        assert_eq!(app.entries, entries);
        press(&mut app, KeyCode::Up);
        assert_eq!(app.input.text(), "!  echo hello");
        app.on_agent_event(AgentEvent::ToolStart {
            id: "shell".into(),
            title: "! echo hello".into(),
        });
        assert_eq!(app.busy_label(), "running ! echo hello");
        app.on_agent_event(AgentEvent::TurnDone);
        assert!(!app.busy);
    }

    #[test]
    fn shell_empty_and_busy_or_fetching_keep_input_and_show_hint() {
        let mut app = App::new(&info());
        for input in ["!", "!   "] {
            app.input.set(input);
            press(&mut app, KeyCode::Enter);
            assert_eq!(app.hint.as_ref().unwrap().0, "Usage: !<command>");
            assert_eq!(app.input.text(), input);
            assert!(!app.busy);
            assert!(app.take_effects().is_empty());
            assert!(app.history.is_empty());
        }
        for (busy, fetching) in [(true, false), (false, true)] {
            app.busy = busy;
            app.fetching = fetching;
            for input in ["!echo hello", "!"] {
                app.input.set(input);
                press(&mut app, KeyCode::Enter);
                assert!(
                    app.hint
                        .as_ref()
                        .unwrap()
                        .0
                        .starts_with("A turn is running.")
                );
                assert_eq!(app.input.text(), input);
                assert!(app.take_effects().is_empty());
                assert!(app.history.is_empty());
            }
        }
        app.fetching = false;
        app.input.set(" !echo hello");
        press(&mut app, KeyCode::Enter);
        assert!(matches!(app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Send(text))] if text == " !echo hello"));
    }

    #[test]
    fn shell_recipe_suggestions_filter_complete_and_run() {
        let mut app = App::new(&workspace_info());
        app.input.set("!");
        assert_eq!(texts(&app), ["!just build", "!just test"]);
        assert_eq!(app.suggestions()[1].desc, "filter=\"\" *args # Run tests");
        app.input.set("!JUST B");
        assert_eq!(texts(&app), ["!just build"]);
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.input.text(), "!just build");
        assert!(app.take_effects().is_empty());
        press(&mut app, KeyCode::Enter);
        assert!(matches!(app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Shell(cmd))] if cmd == "just build"));
        app.on_agent_event(AgentEvent::TurnDone);
        app.input.set("!just b");
        press(&mut app, KeyCode::Enter);
        assert!(matches!(app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Shell(cmd))] if cmd == "just build"));
        app.on_agent_event(AgentEvent::TurnDone);
        for input in ["!just t", "!just test"] {
            app.input.set(input);
            press(&mut app, KeyCode::Enter);
            assert_eq!(app.input.text(), "!just test ");
            assert!(!app.busy);
            assert!(app.take_effects().is_empty());
        }
        app.input.set("!just t");
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.input.text(), "!just test ");
        app.on_paste("pattern");
        press(&mut app, KeyCode::Enter);
        assert!(matches!(app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Shell(cmd))] if cmd == "just test pattern"));
    }

    #[test]
    fn shell_recipe_descriptions_are_bounded_and_popup_can_hide() {
        let mut info = workspace_info();
        info.workspace.recipes[0].doc = Some("long description ".repeat(50));
        let mut app = App::new(&info);
        app.input.set("!");
        assert!(super::super::text::str_width(&app.suggestions()[0].desc) <= 80);
        assert!(app.suggestions()[0].desc.ends_with('…'));
        press(&mut app, KeyCode::Esc);
        assert!(app.suggestions().is_empty());
        press(&mut app, KeyCode::Char('j'));
        assert!(!app.suggestions().is_empty());
        app.input.set("!echo");
        assert!(app.suggestions().is_empty());
    }

    #[test]
    fn live_tool_output_appends_by_id_caps_utf8_and_ignores_finished_or_unknown() {
        let mut app = App::new(&info());
        for id in ["one", "two"] {
            app.on_agent_event(AgentEvent::ToolStart {
                id: id.into(),
                title: id.into(),
            });
        }
        for chunk in ["hello ", "world\n"] {
            app.on_agent_event(AgentEvent::ToolOutput {
                id: "one".into(),
                chunk: chunk.into(),
            });
        }
        assert!(matches!(&app.entries[app.entries.len() - 2],
            Entry::Tool { live_output, .. } if live_output == "hello world\n"));
        assert!(matches!(app.entries.last().unwrap(),
            Entry::Tool { live_output, .. } if live_output.is_empty()));
        let before = app.entries.clone();
        app.on_agent_event(AgentEvent::ToolOutput {
            id: "unknown".into(),
            chunk: "ignored".into(),
        });
        assert_eq!(app.entries, before);
        let chunk = format!("{}END", "界".repeat(MAX_LIVE_OUTPUT));
        app.on_agent_event(AgentEvent::ToolOutput {
            id: "one".into(),
            chunk: chunk.clone(),
        });
        let Entry::Tool { live_output, .. } = &app.entries[app.entries.len() - 2] else {
            panic!()
        };
        assert!(live_output.len() <= MAX_LIVE_OUTPUT);
        assert!(live_output.len() >= MAX_LIVE_OUTPUT - 3);
        assert!(chunk.ends_with(live_output.as_str()));
        app.on_agent_event(AgentEvent::ToolEnd {
            id: "one".into(),
            ok: true,
            output: "final".into(),
        });
        let before = app.entries.clone();
        app.on_agent_event(AgentEvent::ToolOutput {
            id: "one".into(),
            chunk: "late".into(),
        });
        assert_eq!(app.entries, before);
        assert!(matches!(&app.entries[app.entries.len() - 2],
            Entry::Tool { status: ToolStatus::Done { output, .. }, live_output, .. }
                if output == "final" && live_output.is_empty()));
    }

    #[test]
    fn just_info_lists_recipes_and_errors_even_while_busy() {
        let mut app = App::new(&workspace_info());
        app.busy = true;
        command(&mut app, "/just");
        assert_eq!(app.entries.last(), Some(&Entry::Info(
            "Justfile: project/justfile\nbuild  # Build the application\ntest filter=\"\" *args  # Run tests".into())));
        assert!(app.busy);
        assert!(app.take_effects().is_empty());
        app.workspace.error = Some("just unavailable".into());
        command(&mut app, "/just");
        assert!(
            matches!(app.entries.last(), Some(Entry::Info(text)) if text.ends_with("Error: just unavailable"))
        );
        app.workspace.justfile = None;
        app.workspace.recipes.clear();
        command(&mut app, "/just");
        assert_eq!(
            app.entries.last(),
            Some(&Entry::Info(
                "No justfile found in project.\nError: just unavailable".into()
            ))
        );
        app.workspace.error = None;
        command(&mut app, "/just");
        assert_eq!(
            app.entries.last(),
            Some(&Entry::Info("No justfile found in project.".into()))
        );
    }

    #[test]
    fn suggestions_filter_and_hide() {
        let mut a = App::new(&info());
        assert!(a.suggestions().is_empty());
        press(&mut a, KeyCode::Char('/'));
        assert_eq!(texts(&a).len(), COMMANDS.len() + EFFORTS.len());
        a.on_paste("MI");
        assert_eq!(texts(&a), ["/mistl", "/mistl start"]);
        a.input.set("/clear");
        assert!(a.suggestions().is_empty(), "lone exact match");
        a.input.set("hello /");
        assert!(a.suggestions().is_empty());
        a.models = vec!["Alpha".into(), "beta".into()];
        a.input.set("/model a");
        assert_eq!(texts(&a), ["/model Alpha"]);
        a.input.set("/ef");
        press(&mut a, KeyCode::Esc);
        assert!(a.suggestions().is_empty());
        press(&mut a, KeyCode::Char('f'));
        assert!(!a.suggestions().is_empty(), "editing shows it again");
    }

    #[test]
    fn suggestions_select_complete_and_run() {
        let mut a = App::new(&info());
        a.on_paste("/mi");
        press(&mut a, KeyCode::Down);
        assert_eq!(a.suggest_sel, 1);
        press(&mut a, KeyCode::Tab);
        assert_eq!(a.input.text(), "/mistl start");
        assert!(a.take_effects().is_empty());
        // Enter completes a partial command and runs it when it takes no args.
        a.input.set("/q");
        press(&mut a, KeyCode::Enter);
        assert!(a.quit);

        let mut a = App::new(&info());
        a.on_paste("/ef");
        // "/effort" takes arguments: Enter completes but does not run.
        press(&mut a, KeyCode::Enter);
        assert_eq!(a.input.text(), "/effort");
        assert!(a.picker.is_none());
        // Tab on the complete command steps into its arguments.
        press(&mut a, KeyCode::Tab);
        assert_eq!(a.input.text(), "/effort ");
        press(&mut a, KeyCode::Up);
        press(&mut a, KeyCode::Enter);
        assert!(
            matches!(a.take_effects().as_slice(), [Effect::Command(UserCommand::SetEffort(Some(e)))] if e == "xhigh")
        );
        assert!(a.input.is_empty());

        // Exact "/effort" with more candidates still runs on Enter.
        a.on_paste("/effort");
        press(&mut a, KeyCode::Enter);
        assert!(a.picker.is_some());
    }

    #[test]
    fn suggestions_leave_history_and_scroll_alone() {
        let mut a = App::new(&info());
        a.on_paste("/help");
        press(&mut a, KeyCode::Enter);
        press(&mut a, KeyCode::Up);
        assert_eq!(a.input.text(), "/help");
        assert!(a.suggestions().is_empty(), "browsing history");
        press(&mut a, KeyCode::Down);
        a.on_paste("/");
        a.on_key(key(KeyCode::Up, KeyModifiers::SHIFT, KeyEventKind::Press));
        assert_eq!(a.suggest_sel, 0);
    }

    fn wheel(app: &mut App, kind: MouseEventKind, modifiers: KeyModifiers) {
        app.on_mouse(MouseEvent {
            kind,
            column: 1,
            row: 2,
            modifiers,
        });
    }

    #[test]
    fn approval_cycles_and_slash_updates_mode_without_changing_input() {
        let mut app = App::new(&info());
        app.input.set("draft");
        for mode in [ApprovalMode::Auto, ApprovalMode::Yolo, ApprovalMode::Ask] {
            app.on_key(key(
                KeyCode::BackTab,
                KeyModifiers::SHIFT,
                KeyEventKind::Press,
            ));
            assert_eq!(app.approval, mode);
            assert_eq!(app.input.text(), "draft");
            assert!(matches!(app.take_effects().as_slice(),
                [Effect::Command(UserCommand::SetApprovalMode(m))] if *m == mode));
        }
        app.input.clear();
        command(&mut app, "/approval auto");
        assert_eq!(app.approval, ApprovalMode::Auto);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::Command(UserCommand::SetApprovalMode(
                ApprovalMode::Auto
            ))]
        ));
        app.on_agent_event(AgentEvent::ApprovalModeChanged(ApprovalMode::Ask));
        command(&mut app, "/approval");
        assert!(
            matches!(app.entries.last(), Some(Entry::Info(s)) if s.contains("Approval: ask") && s.contains("auto | yolo"))
        );
        command(&mut app, "/effort");
        press(&mut app, KeyCode::BackTab);
        assert_eq!(app.approval, ApprovalMode::Ask);
        app.take_effects();
        let (reply, _rx) = oneshot::channel();
        app.on_agent_event(AgentEvent::ApprovalRequest {
            title: "action".into(),
            reason: "review".into(),
            reply,
        });
        press(&mut app, KeyCode::BackTab);
        assert!(app.take_effects().is_empty());
    }

    #[test]
    fn wheel_pins_pages_and_resumes_follow() {
        let mut app = App::new(&info());
        app.last_total = 100;
        app.last_height = 10;
        wheel(&mut app, MouseEventKind::ScrollUp, KeyModifiers::NONE);
        assert_eq!(app.scroll_top, Some(87));
        wheel(&mut app, MouseEventKind::ScrollUp, KeyModifiers::SHIFT);
        assert_eq!(app.scroll_top, Some(78));
        wheel(&mut app, MouseEventKind::ScrollDown, KeyModifiers::SHIFT);
        assert_eq!(app.scroll_top, Some(87));
        wheel(&mut app, MouseEventKind::ScrollDown, KeyModifiers::NONE);
        assert_eq!(app.scroll_top, None);
        app.input.set("draft");
        app.on_key(key(
            KeyCode::Home,
            KeyModifiers::CONTROL,
            KeyEventKind::Press,
        ));
        assert_eq!(app.scroll_top, Some(0));
        app.on_key(key(
            KeyCode::End,
            KeyModifiers::CONTROL,
            KeyEventKind::Press,
        ));
        assert_eq!(app.scroll_top, None);
        assert_eq!(app.input.text(), "draft");
        app.input.clear();
        press(&mut app, KeyCode::Home);
        assert_eq!(app.scroll_top, Some(0));
        press(&mut app, KeyCode::End);
        assert_eq!(app.scroll_top, None);
    }

    #[test]
    fn wheel_navigates_picker_and_suggestions_and_ignores_approval() {
        let mut app = App::new(&info());
        command(&mut app, "/effort");
        wheel(&mut app, MouseEventKind::ScrollDown, KeyModifiers::NONE);
        assert_eq!(app.picker.as_ref().unwrap().selected, 1);
        wheel(&mut app, MouseEventKind::ScrollUp, KeyModifiers::NONE);
        assert_eq!(app.picker.as_ref().unwrap().selected, 0);
        assert_eq!(app.scroll_top, None);
        press(&mut app, KeyCode::Esc);
        app.input.set("/");
        wheel(&mut app, MouseEventKind::ScrollDown, KeyModifiers::NONE);
        assert_eq!(app.suggest_sel, 1);
        assert_eq!(app.scroll_top, None);
        let (reply, _rx) = oneshot::channel();
        app.on_agent_event(AgentEvent::ApprovalRequest {
            title: "action".into(),
            reason: "review".into(),
            reply,
        });
        wheel(&mut app, MouseEventKind::ScrollDown, KeyModifiers::NONE);
        assert_eq!(app.suggest_sel, 1);
    }

    #[test]
    fn mouse_toggle_emits_capture_effect_and_disables_wheel() {
        let mut app = App::new(&info());
        command(&mut app, "/mouse");
        assert!(!app.mouse_capture);
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::MouseCapture(false)]
        ));
        assert!(app.hint.as_ref().unwrap().0.contains("select/copy"));
        app.last_total = 100;
        app.last_height = 10;
        wheel(&mut app, MouseEventKind::ScrollUp, KeyModifiers::NONE);
        assert_eq!(app.scroll_top, None);
        press(&mut app, KeyCode::F(2));
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::MouseCapture(true)]
        ));
        command(&mut app, "/mouse off");
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::MouseCapture(false)]
        ));
        command(&mut app, "/mouse on");
        assert!(matches!(
            app.take_effects().as_slice(),
            [Effect::MouseCapture(true)]
        ));
    }

    #[test]
    fn send_keeps_user_text_as_typed() {
        let mut app = App::new(&info());
        command(&mut app, "  request\nwith spacing  ");
        assert!(matches!(app.take_effects().as_slice(),
            [Effect::Command(UserCommand::Send(s))] if s == "  request\nwith spacing  "));
    }

    fn mouse(app: &mut App, kind: MouseEventKind, column: u16, row: u16) {
        app.on_mouse(MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        });
    }

    fn selection_app() -> App {
        let mut app = App::new(&info());
        let lines = [
            "zero",
            "one",
            "日a\u{301}本  ",
            "wrapped",
            "tail",
            "five",
            "six",
            "seven",
        ];
        app.last_total = lines.len();
        app.last_height = 3;
        app.remember_transcript(
            Rect::new(2, 2, 12, 3),
            lines.into_iter().map(str::to_string).collect(),
        );
        app.scroll_top = Some(2);
        app
    }

    #[test]
    fn drag_copies_absolute_rows_in_scrolled_view_and_click_clears() {
        let mut app = selection_app();
        mouse(&mut app, MouseEventKind::Down(MouseButton::Left), 3, 2);
        mouse(&mut app, MouseEventKind::Drag(MouseButton::Left), 4, 3);
        assert_eq!(app.selected_text(), "日a\u{301}本\nwra");
        assert_eq!(app.selection_range(2), Some(0..7));
        assert_eq!(app.selection_range(3), Some(0..3));
        mouse(&mut app, MouseEventKind::Up(MouseButton::Left), 4, 3);
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Copy(text)] if text == "日a\u{301}本\nwra")
        );
        app.scroll_down(1);
        assert_eq!(app.selected_text(), "日a\u{301}本\nwra");
        mouse(&mut app, MouseEventKind::Down(MouseButton::Left), 4, 3);
        mouse(&mut app, MouseEventKind::Up(MouseButton::Left), 4, 3);
        assert!(app.selection.is_none());
        assert!(app.take_effects().is_empty());
    }

    #[test]
    fn selection_clamps_ranges_and_keeps_combining_characters() {
        let mut app = selection_app();
        let a = TextPoint { row: 2, col: 2 };
        let b = TextPoint { row: 2, col: 3 };
        app.selection = Some(Selection { anchor: b, head: a });
        assert_eq!(app.selection_range(2), Some(2..5));
        assert_eq!(app.selected_text(), "a\u{301}本");
        app.selection = Some(Selection {
            anchor: TextPoint { row: 3, col: 200 },
            head: TextPoint { row: 2, col: 1 },
        });
        assert_eq!(app.selection_range(2), Some(0..7));
        assert_eq!(app.selection_range(3), Some(0..7));
        assert_eq!(app.selection_range(1), None);
        assert_eq!(app.selection_range(100), None);
        assert_eq!(app.selected_text(), "日a\u{301}本\nwrapped");
        app.last_lines[2] = "👩‍💻日本".into();
        app.selection = Some(Selection {
            anchor: TextPoint { row: 2, col: 1 },
            head: TextPoint { row: 2, col: 2 },
        });
        assert_eq!(app.selection_range(2), Some(0..4));
        assert_eq!(app.selected_text(), "👩‍💻日");
    }

    #[test]
    fn drag_past_edges_scrolls_one_line_and_ignores_input_and_modals() {
        let mut app = selection_app();
        mouse(&mut app, MouseEventKind::Down(MouseButton::Left), 2, 6);
        mouse(&mut app, MouseEventKind::Drag(MouseButton::Left), 3, 3);
        assert!(app.selection.is_none());
        mouse(&mut app, MouseEventKind::Down(MouseButton::Left), 2, 3);
        mouse(&mut app, MouseEventKind::Drag(MouseButton::Left), 3, 1);
        assert_eq!(app.scroll_top, Some(1));
        assert_eq!(app.selection.unwrap().head.row, 1);
        mouse(&mut app, MouseEventKind::Drag(MouseButton::Left), 3, 8);
        assert_eq!(app.scroll_top, Some(2));
        assert_eq!(app.selection.unwrap().head.row, 4);
        mouse(&mut app, MouseEventKind::Up(MouseButton::Left), 3, 8);
        assert!(matches!(app.take_effects().as_slice(), [Effect::Copy(_)]));
        app.clear_selection();
        app.slash("effort");
        mouse(&mut app, MouseEventKind::Down(MouseButton::Left), 2, 2);
        mouse(&mut app, MouseEventKind::Drag(MouseButton::Left), 3, 3);
        assert!(app.selection.is_none());
        app.picker = None;
        let (reply, _receive) = oneshot::channel();
        app.on_agent_event(AgentEvent::ApprovalRequest {
            title: "approve".into(),
            reason: String::new(),
            reply,
        });
        mouse(&mut app, MouseEventKind::Down(MouseButton::Left), 2, 2);
        mouse(&mut app, MouseEventKind::Drag(MouseButton::Left), 3, 3);
        assert!(app.selection.is_none());
    }

    #[test]
    fn selection_survives_streaming_and_scroll_but_clears_on_rebuild_and_width() {
        let mut app = selection_app();
        let selection = Selection {
            anchor: TextPoint { row: 2, col: 0 },
            head: TextPoint { row: 3, col: 1 },
        };
        app.selection = Some(selection);
        let mut lines = app.last_lines.clone();
        lines.last_mut().unwrap().push_str(" streaming");
        lines.push("more".into());
        app.remember_transcript(app.transcript_rect, lines);
        assert_eq!(app.selection, Some(selection));
        let mut lines = app.last_lines.clone();
        lines[2] = "rebuilt".into();
        app.remember_transcript(app.transcript_rect, lines);
        assert!(app.selection.is_none());
        app.selection = Some(selection);
        let mut rect = app.transcript_rect;
        rect.width += 1;
        app.remember_transcript(rect, app.last_lines.clone());
        assert!(app.selection.is_none());
        app.selection = Some(selection);
        app.on_key(key(
            KeyCode::Char('o'),
            KeyModifiers::CONTROL,
            KeyEventKind::Press,
        ));
        assert!(app.selection.is_none());
        app.selection = Some(selection);
        command(&mut app, "/clear");
        assert!(app.selection.is_none());
        assert!(app.last_lines.is_empty());
    }

    #[test]
    fn copy_variants_use_full_latest_entries_and_export_uses_same_transcript() {
        let mut app = App::new(&info());
        let output: String = (0..20).map(|n| format!("output-{n}\n")).collect();
        app.entries = vec![
            Entry::User("request\ncontinued".into()),
            Entry::Assistant {
                text: "old reply".into(),
                done: true,
            },
            Entry::Tool {
                id: "tool".into(),
                title: "$ command".into(),
                status: ToolStatus::Done {
                    ok: true,
                    output: output.clone(),
                },
                live_output: String::new(),
            },
            Entry::Assistant {
                text: "full **reply**\n日本語\x1b[31m red\x1b[0m".into(),
                done: true,
            },
            Entry::Info("info".into()),
            Entry::AutoReview {
                title: "$ command".into(),
                allowed: false,
                reason: "reason".into(),
            },
            Entry::Error("error".into()),
        ];
        command(&mut app, "/copy");
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Copy(text)] if text == "full **reply**\n日本語 red")
        );
        command(&mut app, "/copy tool");
        assert!(matches!(app.take_effects().as_slice(), [Effect::Copy(text)] if text == &output));
        let transcript = app.transcript_text();
        assert!(
            transcript.starts_with("> request\ncontinued\n\nold reply\n\n✓ $ command\noutput-0")
        );
        assert!(transcript.ends_with("info\n\n✗ auto denied: reason · $ command\n\n✗ error"));
        assert!(!transcript.contains('\x1b'));
        command(&mut app, "/copy all");
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Copy(text)] if text == &transcript)
        );
        app.workspace.root = "workspace".into();
        command(&mut app, "/export folder/my log.md");
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Export { path, text }] if path == &PathBuf::from("workspace").join("folder/my log.md") && text == &transcript)
        );
        let absolute = std::env::temp_dir().join("absolute.md");
        assert_eq!(app.export_path(absolute.to_str().unwrap()), absolute);
        app.on_agent_event(AgentEvent::ToolStart {
            id: "live".into(),
            title: "$ live".into(),
        });
        app.on_agent_event(AgentEvent::ToolOutput {
            id: "live".into(),
            chunk: "latest live output".into(),
        });
        command(&mut app, "/copy tool");
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Copy(text)] if text == "latest live output")
        );
        app.entries.clear();
        for command in ["/copy", "/copy tool", "/copy all", "/copy unknown"] {
            app.slash(command.strip_prefix('/').unwrap());
            assert!(app.take_effects().is_empty());
            assert!(app.hint.is_some());
        }
    }

    #[test]
    fn clipboard_feedback_schedules_terminal_effect_and_counts_characters() {
        let mut app = App::new(&info());
        app.copy_completed(3, Ok(Method::Terminal), Some("frame".into()));
        assert!(
            matches!(app.take_effects().as_slice(), [Effect::Osc52(frame)] if frame == "frame")
        );
        assert_eq!(
            app.hint.as_ref().unwrap().0,
            "copied via terminal (3 chars)"
        );
        app.copy_completed(5, Ok(Method::Native), None);
        assert_eq!(app.hint.as_ref().unwrap().0, "copied 5 chars");
        app.copy_completed(5, Err(anyhow::anyhow!("failed")), None);
        assert!(app.hint.as_ref().unwrap().0.contains("clipboard error"));
    }

    #[test]
    fn copies_strip_terminal_sequences_and_controls() {
        assert_eq!(
            plain_text("\x1b[31m日本語\x1b[0m\n\x1b]0;title\x07ok\x1b]52;c;secret\x1b\\!\r\0"),
            "日本語\nok!"
        );
        assert_eq!(plain_text("\x1b(Btext\x1b[unterminated"), "textnterminated");
    }
}
