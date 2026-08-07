//! Chat-message formatting helpers shared by every front end that renders
//! `crate::tunnel::session::ChatMessage` (the dashboard's chat panel, W6;
//! `mistl tunnel tui`, W7). Ported from the standalone `p2p` crate's
//! `src/app/chat.rs`, which was a full interactive terminal chat mode
//! (`p2p chat <room>`): raw-mode stdin, a redrawing single-line input
//! prompt, Ctrl+T (toggle chat-log visibility) / `/id` (toggle peer-id
//! display) commands, all built directly on `crossterm`'s event loop.
//!
//! None of that terminal machinery survives the port. mistl's daemon owns
//! the tunnel's lifecycle now (see the integration contract), so there is no
//! foreground terminal to run a raw-mode input loop against, and chat
//! sending/receiving already lives in
//! `crate::tunnel::session::SessionContext::send_chat` plus the
//! `on_chat_message` hook `SessionContext::build` wires up -- both front
//! ends read the result via `Snapshot::chat`/`Snapshot::to_json`'s `"chat"`
//! array and drive `tunnel.chat.send` for sending, with no per-frontend chat
//! loop needed. What's left and worth keeping is the *formatting* logic
//! upstream's terminal loop had inline: a peer id short enough to fit a chat
//! line, and the "[shortid] text" vs. plain "text" toggle upstream called
//! `/id`. Kept here as small, independently testable functions so the
//! dashboard and TUI don't each reinvent (and potentially diverge on) the
//! same truncation/formatting rule.

use crate::tunnel::session::ChatMessage;

/// Truncates a peer id to the same 8-character prefix upstream's terminal
/// chat mode showed next to each line
/// (`&peer_id[..peer_id.len().min(8)]` in `p2p/src/app/chat.rs`). mistl's
/// peer ids are 16 hex characters (see `crate::net::Transport::node_id`),
/// so in practice this always returns the first half.
// Not yet called outside this module's own tests: no front end has wired up
// the `/id` toggle described above, but the helper is kept (with `pub`) so
// the dashboard/TUI can reuse it as soon as one does, per the module doc.
#[allow(dead_code)]
pub fn short_peer_id(peer_id: &str) -> &str {
    &peer_id[..peer_id.len().min(8)]
}

/// Formats a single chat line for display, mirroring upstream's
/// `chat_loop`'s per-message formatting: `"[shortid] text"` when `show_ids`
/// is set, otherwise the bare message text. Own messages (`msg.mine`) get no
/// special treatment here -- upstream distinguished sent-vs-received with a
/// `"> "` prompt-echo prefix rather than by branching this formatter, and a
/// front end that wants to visually distinguish them (as the dashboard's
/// chat panel does with CSS, aligning `mine` bubbles to one side) should
/// branch on `msg.mine` itself rather than have this function bake in one
/// particular visual convention.
// Same situation as `short_peer_id` above: no current caller outside tests.
#[allow(dead_code)]
pub fn format_chat_line(msg: &ChatMessage, show_ids: bool) -> String {
    if show_ids {
        format!("[{}] {}", short_peer_id(&msg.peer_id), msg.text)
    } else {
        msg.text.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(peer_id: &str, mine: bool, text: &str) -> ChatMessage {
        ChatMessage {
            timestamp_ms: 0,
            peer_id: peer_id.to_string(),
            mine,
            text: text.to_string(),
        }
    }

    #[test]
    fn short_peer_id_truncates_to_eight_chars() {
        assert_eq!(short_peer_id("0123456789abcdef"), "01234567");
        assert_eq!(short_peer_id("abc"), "abc");
        assert_eq!(short_peer_id(""), "");
    }

    #[test]
    fn format_chat_line_prefixes_short_id_when_shown() {
        let m = msg("0123456789abcdef", false, "hello");
        assert_eq!(format_chat_line(&m, true), "[01234567] hello");
        assert_eq!(format_chat_line(&m, false), "hello");
    }

    #[test]
    fn format_chat_line_ignores_mine_flag() {
        // See the doc comment: `mine` is deliberately not consulted here.
        let sent = msg("peer-1", true, "hi");
        let received = msg("peer-1", false, "hi");
        assert_eq!(
            format_chat_line(&sent, true),
            format_chat_line(&received, true)
        );
    }
}
