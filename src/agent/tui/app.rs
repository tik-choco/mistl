//! Pure UI state and event handling (no terminal access).

use std::collections::VecDeque;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use tokio::sync::oneshot;

use super::UiInfo;
use super::input::InputBuffer;
use super::text::truncate_text;
use crate::agent::config::normalize_effort;
use crate::agent::types::{AgentEvent, Approval, MistlOp, ToolMode, UserCommand, WorkspaceSummary};

pub const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
/// Ticks (~100ms each) a footer hint stays visible.
const HINT_TICKS: u64 = 30;
const MAX_LIVE_OUTPUT: usize = 64 * 1024;

pub const EFFORTS: [&str; 7] = [
    "default", "none", "minimal", "low", "medium", "high", "xhigh",
];

/// Slash commands offered as completions, in display order. Bare commands
/// come before their argument forms so an exact match is selected first.
const COMMANDS: [(&str, &str); 10] = [
    ("/help", "show keys and commands"),
    ("/clear", "clear the conversation"),
    ("/just", "list workspace recipes"),
    ("/model", "choose a model"),
    ("/models", "choose a model"),
    ("/effort", "choose reasoning effort"),
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
    pub auto_approve: bool,
    pub entries: Vec<Entry>,
    pub input: InputBuffer,
    pub busy: bool,
    pub expand_tools: bool,
    /// `None` = follow the bottom; `Some(top_line)` = pinned view.
    pub scroll_top: Option<usize>,
    pub last_total: usize,
    pub last_height: usize,
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
  PgUp/PgDn        scroll by page
  Ctrl+Up/Down     scroll by line (Shift+Up/Down too)
  End              jump to bottom (when input is empty)
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
  /just            list workspace recipes and the justfile path
  /model [id]      choose or switch model (/models also opens the picker)
  /effort [level]  choose or set reasoning effort (default omits the field)
  /mistl [start]   show instance info or start mistl
  /quit, /exit     quit
  !<command>       run a shell command in the workspace; its output is shared with the model on your next message
Modal keys:
  Type/Up/Down/PgUp/PgDn  filter and navigate a picker; Enter chooses
  Esc             close a picker";

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
            auto_approve: info.auto_approve,
            entries: Vec::new(),
            input: InputBuffer::default(),
            busy: false,
            expand_tools: false,
            scroll_top: None,
            last_total: 0,
            last_height: 0,
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

    fn set_hint(&mut self, s: impl Into<String>) {
        self.hint = Some((s.into(), self.tick));
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
            self.auto_approve = true;
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
            let nt = t + n;
            self.scroll_top = if nt >= max { None } else { Some(nt) };
        }
    }

    pub fn on_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }
        if self.events_closed {
            self.quit = true;
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
            KeyCode::Char('o') if ctrl => self.expand_tools = !self.expand_tools,
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
            KeyCode::Home => self.input.home(),
            KeyCode::End => {
                if self.input.is_empty() {
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
        self.effects.push(Effect::Command(UserCommand::Send(text)));
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
        assert!(!a.auto_approve);
        press(&mut a, KeyCode::Char('a'));
        assert_eq!(r2.try_recv().unwrap(), Approval::Always);
        assert!(a.auto_approve);
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
            [Effect::Command(UserCommand::Send(text))] if text == "!echo hello"));
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
}
