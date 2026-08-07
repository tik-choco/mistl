//! Application state for the tunnel TUI.
//!
//! Upstream (`p2p/src/tui/app.rs`) held a live in-process `TuiContext`
//! (= `SessionContext`) and every action was an `async` method call directly
//! on shared session state guarded by `tokio::Mutex`es. Here the session
//! lives in the daemon, so `App` instead holds a plain-old-data mirror of
//! the daemon's `tunnel.status` JSON (the `Snapshot`/`*Row` types below,
//! kept intentionally dumb) and every action is a **blocking**
//! `crate::daemon::ipc::client_request` call -- see that function's own doc
//! comment, which already assumes a synchronous, no-tokio caller. Because of
//! that, this whole module is synchronous: no `async fn`, no `.await`.
//!
//! Every model type below derives `Default` and is deserialized with a
//! container-level `#[serde(default)]`, so a `tunnel.status` response
//! missing a field (older/newer daemon build, partial daemon-side rollout)
//! degrades to an empty/zero value instead of a hard deserialize error. If
//! the whole payload doesn't even parse as an object, `refresh()` falls back
//! to `Snapshot::default()` rather than panicking or crashing the UI.

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use serde::Deserialize;

// ---------------------------------------------------------------------
// Wire-shape mirrors of `tunnel.status` (see TUNNEL_INTEGRATION_CONTRACT.md
// and `crate::tunnel::session::Snapshot::to_json`, the single source of
// truth for this JSON). Kept as plain structs with public-to-the-tui-module
// fields; no behavior lives on them.
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct ForwardPeerRow {
    pub(super) peer_id: String,
    pub(super) active_conns: u64,
    // The daemon counts bytes from its own perspective (`bytes_in` =
    // received, `bytes_out` = sent); the UI labels the same numbers RX/TX.
    // Renamed rather than relabelled so `ui.rs` keeps upstream's wording.
    #[serde(rename = "bytes_out")]
    pub(super) bytes_tx: u64,
    #[serde(rename = "bytes_in")]
    pub(super) bytes_rx: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct ForwardRow {
    pub(super) direction: String,
    pub(super) proto: String,
    pub(super) addr: String,
    pub(super) listen_port: i32,
    pub(super) target: String,
    pub(super) state: String,
    pub(super) active_conns: u64,
    /// See [`ForwardPeerRow`] for why these two are renamed.
    #[serde(rename = "bytes_out")]
    pub(super) bytes_tx: u64,
    #[serde(rename = "bytes_in")]
    pub(super) bytes_rx: u64,
    /// Per-peer breakdown. Not part of the frozen `tunnel.status` shape as
    /// documented in the integration contract; kept here (defaulting to
    /// empty) so upstream's "expand row" UX degrades to "(no peers)"
    /// gracefully instead of being deleted outright if a future
    /// `Snapshot::to_json` grows this field.
    pub(super) peers: Vec<ForwardPeerRow>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct PendingAuthRow {
    /// The daemon's own queue index, sent back verbatim as `tunnel.auth.*`'s
    /// numeric `id` -- a `String` here would fail to deserialize outright.
    pub(super) id: u64,
    pub(super) peer_id: String,
    pub(super) proto: String,
    /// `forward_key` / `target_addr` on the wire: the forward being used and
    /// the address it would reach. Kept under upstream's UI names so
    /// `ui.rs`'s rendering is untouched.
    #[serde(rename = "forward_key")]
    pub(super) target: String,
    #[serde(rename = "target_addr")]
    pub(super) remote_addr: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct PendingForwardRow {
    pub(super) req_id: String,
    pub(super) peer_id: String,
    pub(super) proto: String,
    pub(super) remote_addr: String,
    pub(super) target: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
/// A forward request this node sent that no peer has answered yet.
/// `ForwardNegotiator::list_outgoing` tracks neither a request id nor a sent
/// timestamp, so -- unlike the contract's illustrative sketch -- there is no
/// `req_id`/`sent_ms` to render; the local listen port and remote address
/// identify the pending request instead.
pub(super) struct PendingOutgoingRow {
    pub(super) peer_id: String,
    pub(super) proto: String,
    pub(super) listen_port: i32,
    pub(super) local_addr: String,
    pub(super) remote_addr: String,
    pub(super) target: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct TrustRow {
    /// Opaque id echoed straight back to `tunnel.trust.revoke`. The trust
    /// store records no creation timestamp, so there is no `created_ms`.
    pub(super) key: String,
    pub(super) peer_id: String,
    #[serde(rename = "forward_key")]
    pub(super) target: String,
    pub(super) decision: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct EventRow {
    pub(super) timestamp_ms: u64,
    pub(super) peer_id: String,
    #[serde(rename = "forward_key")]
    pub(super) target: String,
    pub(super) decision: String,
    pub(super) source: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct NoticeRow {
    pub(super) timestamp_ms: u64,
    pub(super) kind: String,
    pub(super) text: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[allow(dead_code)]
pub(super) struct ChatRow {
    pub(super) timestamp_ms: u64,
    pub(super) peer_id: String,
    pub(super) mine: bool,
    pub(super) text: String,
}

/// Mirrors the full `tunnel.status` response: `Snapshot::to_json()`'s fields
/// plus the `enabled`/`running` flags `tunnel.status` adds on top.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(super) struct Snapshot {
    pub(super) self_id: String,
    pub(super) room: String,
    pub(super) peers: Vec<String>,
    pub(super) forwards: Vec<ForwardRow>,
    pub(super) pending_auth: Vec<PendingAuthRow>,
    pub(super) pending_forwards: Vec<PendingForwardRow>,
    pub(super) pending_outgoing: Vec<PendingOutgoingRow>,
    pub(super) trust: Vec<TrustRow>,
    pub(super) events: Vec<EventRow>,
    pub(super) notices: Vec<NoticeRow>,
    pub(super) chat: Vec<ChatRow>,
    pub(super) enabled: bool,
    pub(super) running: bool,
}

// ---------------------------------------------------------------------
// UI state (focus, popups, forms) -- structurally close to upstream.
// ---------------------------------------------------------------------

#[derive(PartialEq, Eq, Clone, Copy)]
pub(super) enum Focus {
    Forwards,
    Pending,
}

/// Fields of the add-forward form. As in upstream, direction is implicit
/// (this form always requests a `connect` forward: the local node listens
/// locally and reaches a target on a peer). There is deliberately no
/// peer-picker popup anymore (upstream's `Popup::SelectPeer`): the daemon's
/// forward controller now resolves which peer to ask via
/// `RTCManagerHandle::select_server_peer_for`, so `tunnel.forward.add`
/// doesn't take a peer id at all -- see the integration contract.
#[derive(PartialEq, Eq, Clone, Copy)]
pub(super) enum AddField {
    Proto,
    Local,
    Remote,
}

#[derive(Clone)]
pub(super) struct AddForm {
    pub(super) proto_tcp: bool,
    pub(super) local: String,
    pub(super) remote: String,
    pub(super) field: AddField,
}

impl Default for AddForm {
    fn default() -> Self {
        Self {
            proto_tcp: true,
            local: String::new(),
            remote: String::new(),
            field: AddField::Proto,
        }
    }
}

/// A row in the unified pending pane. `Outgoing` is new relative to
/// upstream: it surfaces `pending_outgoing` (forward requests this node
/// sent that are still awaiting the peer's answer) for visibility. It isn't
/// actionable -- there is no IPC command to cancel/resolve it locally, the
/// contract only lets us see it.
pub(super) enum PendingRow {
    Conn(PendingAuthRow),
    Forward(PendingForwardRow),
    Outgoing(PendingOutgoingRow),
}

pub(super) enum Popup {
    None,
    Add(AddForm),
    Trust(usize),
    /// Chat input buffer. New relative to upstream (which had no chat pane
    /// in the ratatui TUI at all -- chat was a separate `p2p chat` terminal
    /// mode, see `p2p/src/app/chat.rs`). The integration contract calls for
    /// "send a chat message" as one of the TUI's key actions, so it's
    /// folded in here as an overlay instead of disturbing the main layout.
    Chat(String),
    /// Room-id input buffer, prefilled with the current room. New relative
    /// to upstream for the same reason as `Chat`: "change room" is one of
    /// the contract's required key actions.
    Room(String),
}

pub(super) struct App {
    pub(super) focus: Focus,
    pub(super) popup: Popup,
    pub(super) snapshot: Snapshot,
    /// Set when the last `tunnel.status` call failed (daemon not running,
    /// IPC error, ...). While set, `ui::draw` shows a dedicated "not
    /// connected" screen instead of the normal panes, per the integration
    /// contract's requirement to never crash on a daemon that isn't up.
    pub(super) daemon_error: Option<String>,
    pub(super) forwards_sel: usize,
    pub(super) pending_sel: usize,
    pub(super) expanded: bool,
    pub(super) message: Option<String>,
    pub(super) should_quit: bool,
}

impl App {
    pub(super) fn new() -> Self {
        Self {
            focus: Focus::Forwards,
            popup: Popup::None,
            snapshot: Snapshot::default(),
            daemon_error: None,
            forwards_sel: 0,
            pending_sel: 0,
            expanded: false,
            message: None,
            should_quit: false,
        }
    }

    pub(super) fn pending_rows(&self) -> Vec<PendingRow> {
        let mut rows: Vec<PendingRow> = self
            .snapshot
            .pending_forwards
            .iter()
            .cloned()
            .map(PendingRow::Forward)
            .collect();
        rows.extend(
            self.snapshot
                .pending_auth
                .iter()
                .cloned()
                .map(PendingRow::Conn),
        );
        rows.extend(
            self.snapshot
                .pending_outgoing
                .iter()
                .cloned()
                .map(PendingRow::Outgoing),
        );
        rows
    }

    /// Polls `tunnel.status` and replaces the local snapshot. Called once
    /// before the first draw and then once per tick (see `super::tui`).
    /// Never panics: a transport-level error (daemon not running, bad
    /// response, ...) is captured in `daemon_error` instead of propagated.
    pub(super) fn refresh(&mut self) {
        match crate::daemon::ipc::client_request("tunnel.status", serde_json::json!({})) {
            Ok(value) => {
                self.daemon_error = None;
                self.snapshot = serde_json::from_value(value).unwrap_or_default();
                let forwards_len = self.snapshot.forwards.len();
                if self.forwards_sel >= forwards_len {
                    self.forwards_sel = forwards_len.saturating_sub(1);
                }
                let pending_len = self.pending_rows().len();
                if self.pending_sel >= pending_len {
                    self.pending_sel = pending_len.saturating_sub(1);
                }
            }
            Err(e) => {
                self.daemon_error = Some(e.to_string());
            }
        }
    }

    pub(super) fn handle_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        self.message = None;
        match std::mem::replace(&mut self.popup, Popup::None) {
            Popup::Add(form) => self.handle_add_key(key, form),
            Popup::Trust(sel) => self.handle_trust_key(key, sel),
            Popup::Chat(input) => self.handle_chat_key(key, input),
            Popup::Room(input) => self.handle_room_key(key, input),
            Popup::None => self.handle_main_key(key),
        }
    }

    fn handle_main_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Forwards => Focus::Pending,
                    Focus::Pending => Focus::Forwards,
                };
            }
            KeyCode::Char('a') => self.popup = Popup::Add(AddForm::default()),
            KeyCode::Char('t') => self.popup = Popup::Trust(0),
            KeyCode::Char('c') => self.popup = Popup::Chat(String::new()),
            KeyCode::Char('r') => self.popup = Popup::Room(self.snapshot.room.clone()),
            KeyCode::Char('s') => self.toggle_running(),
            KeyCode::Enter | KeyCode::Char(' ') if self.focus == Focus::Forwards => {
                self.expanded = !self.expanded;
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_sel(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_sel(1),
            KeyCode::Char('d') if self.focus == Focus::Forwards => self.remove_selected(),
            KeyCode::Char('y') if self.focus == Focus::Pending => self.resolve_pending(true, false),
            KeyCode::Char('Y') if self.focus == Focus::Pending => self.resolve_pending(true, true),
            KeyCode::Char('n') if self.focus == Focus::Pending => {
                self.resolve_pending(false, false)
            }
            KeyCode::Char('N') if self.focus == Focus::Pending => self.resolve_pending(false, true),
            // Upstream's 'w' started an in-process Web UI sharing this
            // session's context (`p2p/src/tui/app.rs::open_web_ui`). mistl's
            // dashboard is always up on the daemon regardless of whether the
            // TUI is running, so there's nothing to "start" here -- point
            // the user at the always-available `mistl ui` instead (see the
            // footer help text in `ui.rs`).
            _ => {}
        }
    }

    /// Starts or stops the tunnel session on the daemon (`tunnel.start` /
    /// `tunnel.stop`). Not present in upstream, which had no such lifecycle
    /// -- starting the `p2p` process itself *was* starting the session.
    /// Here the daemon may be running independently of whether the TUI is
    /// attached, so the TUI needs a way to flip it on/off.
    fn toggle_running(&mut self) {
        let (cmd, verb) = if self.snapshot.running {
            ("tunnel.stop", "stopped")
        } else {
            ("tunnel.start", "started")
        };
        let res = crate::daemon::ipc::client_request(cmd, serde_json::json!({}));
        self.message = Some(match res {
            Ok(_) => format!("tunnel {}", verb),
            Err(e) => e.to_string(),
        });
        self.refresh();
    }

    fn move_sel(&mut self, delta: i32) {
        let pending_len = self.pending_rows().len();
        let (sel, len) = match self.focus {
            Focus::Forwards => (&mut self.forwards_sel, self.snapshot.forwards.len()),
            Focus::Pending => (&mut self.pending_sel, pending_len),
        };
        if len == 0 {
            return;
        }
        let next = (*sel as i32 + delta).rem_euclid(len as i32);
        *sel = next as usize;
    }

    /// Removes the selected forward via `tunnel.forward.remove`. The row's
    /// `target` field doubles as its key (matches upstream, where
    /// `ForwardStatus::key` was always set to `spec.target` at creation
    /// time -- see `p2p/src/controller.rs::add_forward`).
    fn remove_selected(&mut self) {
        let Some(row) = self.snapshot.forwards.get(self.forwards_sel) else {
            return;
        };
        let target = row.target.clone();
        let res = crate::daemon::ipc::client_request(
            "tunnel.forward.remove",
            serde_json::json!({ "target": target }),
        );
        self.message = Some(match res {
            Ok(_) => format!("removed {}", target),
            Err(e) => e.to_string(),
        });
        self.refresh();
    }

    /// Resolves the selected pending row. `allow` picks approve/accept vs.
    /// deny/reject; `remember` is only meaningful for `PendingRow::Conn`
    /// (mapped to `tunnel.auth.{approve,deny}`'s `remember` flag, upstream's
    /// `AuthDecision::{Allow,Deny}Always`); `tunnel.forward.{accept,reject}`
    /// has no such flag, so `Y`/`N` behave the same as `y`/`n` on a
    /// `PendingRow::Forward`.
    fn resolve_pending(&mut self, allow: bool, remember: bool) {
        let rows = self.pending_rows();
        let Some(row) = rows.get(self.pending_sel) else {
            return;
        };
        match row {
            PendingRow::Conn(item) => {
                let id = item.id;
                let cmd = if allow {
                    "tunnel.auth.approve"
                } else {
                    "tunnel.auth.deny"
                };
                let res = crate::daemon::ipc::client_request(
                    cmd,
                    serde_json::json!({ "id": id, "remember": remember }),
                );
                self.message = Some(match res {
                    Ok(_) => format!("{} #{}", if allow { "approved" } else { "denied" }, id),
                    Err(e) => e.to_string(),
                });
            }
            PendingRow::Forward(item) => {
                let req_id = item.req_id.clone();
                let cmd = if allow {
                    "tunnel.forward.accept"
                } else {
                    "tunnel.forward.reject"
                };
                let res = crate::daemon::ipc::client_request(
                    cmd,
                    serde_json::json!({ "req_id": req_id }),
                );
                self.message = Some(match res {
                    Ok(_) => format!(
                        "{} forward #{}",
                        if allow { "accepted" } else { "rejected" },
                        req_id
                    ),
                    Err(e) => e.to_string(),
                });
            }
            PendingRow::Outgoing(_) => {
                self.message =
                    Some("outgoing request: nothing to resolve here, waiting on the peer".into());
            }
        }
        self.refresh();
    }

    fn handle_add_key(&mut self, key: KeyEvent, mut form: AddForm) {
        match key.code {
            KeyCode::Esc => return,
            KeyCode::Enter => {
                self.submit_add(form);
                return;
            }
            KeyCode::Tab | KeyCode::Down => form.field = next_field(form.field),
            KeyCode::Up => form.field = prev_field(form.field),
            KeyCode::Left | KeyCode::Right if form.field == AddField::Proto => {
                form.proto_tcp = !form.proto_tcp;
            }
            KeyCode::Char(' ') if form.field == AddField::Proto => {
                form.proto_tcp = !form.proto_tcp;
            }
            KeyCode::Backspace => match form.field {
                AddField::Local => {
                    form.local.pop();
                }
                AddField::Remote => {
                    form.remote.pop();
                }
                AddField::Proto => {}
            },
            KeyCode::Char(c) => match form.field {
                AddField::Local => form.local.push(c),
                AddField::Remote => form.remote.push(c),
                AddField::Proto => {}
            },
            _ => {}
        }
        self.popup = Popup::Add(form);
    }

    fn submit_add(&mut self, form: AddForm) {
        let local = form.local.trim();
        let remote = form.remote.trim();
        if local.is_empty() || remote.is_empty() {
            self.message = Some("enter local and remote ip:port".into());
            self.popup = Popup::Add(form);
            return;
        }
        let Some(listen_port) = parse_port(local) else {
            self.message = Some("local must be ip:port or a bare port number".into());
            self.popup = Popup::Add(form);
            return;
        };
        if parse_port(remote).is_none() {
            self.message = Some("remote must be ip:port or a bare port number".into());
            self.popup = Popup::Add(form);
            return;
        }
        if self.snapshot.peers.is_empty() {
            self.message = Some("no connected peers".into());
            self.popup = Popup::Add(form);
            return;
        }

        let proto = if form.proto_tcp { "tcp" } else { "udp" };
        // Target shape mirrors upstream's TUI (`p2p/src/tui/app.rs::submit_add`):
        // "<proto>:<remote>", carrying the *full* remote address rather than
        // just its port (unlike the CLI's `parse_connect_forward`, which
        // keeps only the port). The daemon's `tunnel.forward.add` handler
        // and the dashboard's add-forward form (`src/web/assets/index.html`)
        // must agree on this shape with whatever the daemon-side controller
        // expects; flagged for the orchestrator to double check across W3
        // (daemon-side forward add) / W5 (IPC handler) / W6 (dashboard).
        let target = format!("{}:{}", proto, remote);
        let args = serde_json::json!({
            "direction": "connect",
            "proto": proto,
            "addr": "",
            "listen_port": listen_port,
            "target": target,
        });
        let res = crate::daemon::ipc::client_request("tunnel.forward.add", args);
        self.message = Some(match res {
            Ok(_) => format!("forward request sent: {}", target),
            Err(e) => e.to_string(),
        });
        self.refresh();
    }

    fn handle_trust_key(&mut self, key: KeyEvent, mut sel: usize) {
        let len = self.snapshot.trust.len();
        match key.code {
            KeyCode::Esc | KeyCode::Char('t') | KeyCode::Char('q') => return,
            KeyCode::Up | KeyCode::Char('k') if len > 0 => {
                sel = (sel as i32 - 1).rem_euclid(len as i32) as usize;
            }
            KeyCode::Down | KeyCode::Char('j') if len > 0 => {
                sel = (sel + 1) % len;
            }
            KeyCode::Char('x') | KeyCode::Delete if len > 0 => {
                if let Some(entry) = self.snapshot.trust.get(sel) {
                    let key_str = entry.key.clone();
                    let res = crate::daemon::ipc::client_request(
                        "tunnel.trust.revoke",
                        serde_json::json!({ "key": key_str }),
                    );
                    self.message = Some(match res {
                        Ok(_) => "trust entry removed".into(),
                        Err(e) => e.to_string(),
                    });
                    self.refresh();
                }
                let len_after = self.snapshot.trust.len();
                if sel >= len_after {
                    sel = len_after.saturating_sub(1);
                }
            }
            _ => {}
        }
        self.popup = Popup::Trust(sel);
    }

    /// Handles keys while the chat popup (`c`) is open. Unlike `Add`/`Room`,
    /// `Enter` does not close the popup -- it sends the message and clears
    /// the input line so the user can keep chatting, matching how a normal
    /// chat client behaves. Only `Esc` closes it.
    fn handle_chat_key(&mut self, key: KeyEvent, mut input: String) {
        match key.code {
            KeyCode::Esc => return,
            KeyCode::Enter => {
                let text = input.trim().to_string();
                if !text.is_empty() {
                    let res = crate::daemon::ipc::client_request(
                        "tunnel.chat.send",
                        serde_json::json!({ "text": text }),
                    );
                    if let Err(e) = res {
                        self.message = Some(e.to_string());
                    }
                    self.refresh();
                }
                input.clear();
            }
            KeyCode::Backspace => {
                input.pop();
            }
            KeyCode::Char(c) => input.push(c),
            _ => {}
        }
        self.popup = Popup::Chat(input);
    }

    /// Handles keys while the room-change popup (`r`) is open. `Enter`
    /// submits `tunnel.room.set` (an empty room lets the daemon generate
    /// one, per the contract) and closes the popup either way.
    fn handle_room_key(&mut self, key: KeyEvent, mut input: String) {
        match key.code {
            KeyCode::Esc => return,
            KeyCode::Enter => {
                let room = input.trim().to_string();
                let res = crate::daemon::ipc::client_request(
                    "tunnel.room.set",
                    serde_json::json!({ "room": room }),
                );
                self.message = Some(match res {
                    Ok(value) => {
                        let actual = value
                            .get("room")
                            .and_then(|v| v.as_str())
                            .unwrap_or(&room)
                            .to_string();
                        format!("room set to {}", actual)
                    }
                    Err(e) => e.to_string(),
                });
                self.refresh();
                return;
            }
            KeyCode::Backspace => {
                input.pop();
            }
            KeyCode::Char(c) => input.push(c),
            _ => {}
        }
        self.popup = Popup::Room(input);
    }
}

fn next_field(f: AddField) -> AddField {
    match f {
        AddField::Proto => AddField::Local,
        AddField::Local => AddField::Remote,
        AddField::Remote => AddField::Proto,
    }
}

fn prev_field(f: AddField) -> AddField {
    match f {
        AddField::Proto => AddField::Remote,
        AddField::Local => AddField::Proto,
        AddField::Remote => AddField::Local,
    }
}

/// Parses the port out of an address that may be a bare port number
/// (`"9000"`) or an `ip:port` pair (`"127.0.0.1:9000"`). Self-contained
/// (pure string parsing, no session state) rather than delegating to
/// another worker's helper (upstream delegated to
/// `crate::app::session::parse_addr_port`, which isn't part of the frozen
/// cross-worker API surface here).
fn parse_port(addr: &str) -> Option<i32> {
    if let Ok(p) = addr.parse::<i32>() {
        return Some(p);
    }
    addr.rsplit(':').next()?.parse::<i32>().ok()
}
