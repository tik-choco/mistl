//! Pure formatting helpers for the tunnel TUI.
//!
//! Ported near-verbatim from `p2p/src/tui/format.rs`. The only structural
//! change is the input types: upstream's functions took the daemon's
//! in-process enums (`Direction`, `Proto`, `ForwardState`, `TrustDecision`,
//! ...) directly off `SessionContext`. This TUI is now a daemon IPC client
//! (see `super::tui`'s module doc), so it only ever sees whatever shape
//! crossed the wire as JSON in a `tunnel.status` response -- these functions
//! take the plain-string/row types deserialized in `super::app` instead, and
//! degrade gracefully (never panic) on a value they don't recognize.

use super::app::{ChatRow, EventRow, NoticeRow, PendingOutgoingRow};

/// `"serve"` / `"connect"` -> the same two arrows upstream used. Any other
/// string (a daemon running a shape this build doesn't know about) is
/// rendered as-is rather than panicking.
pub(super) fn role_label(direction: &str) -> String {
    match direction {
        "serve" => "SERVE→".to_string(),
        "connect" => "←CONN".to_string(),
        other => other.to_string(),
    }
}

pub(super) fn proto_name(proto: &str) -> String {
    proto.to_uppercase()
}

pub(super) fn state_name(state: &str) -> &str {
    if state.is_empty() { "unknown" } else { state }
}

/// Splits a node-scoped target into its base target and the pinned node id:
/// `"tcp:127.0.0.1:22@node-a"` -> `("tcp:127.0.0.1:22", Some("node-a"))`.
///
/// This is a deliberate, small duplication of
/// `p2p/src/forward_args.rs::split_node_scope` (pure string logic, no
/// state). `crate::tunnel::forward_args` is owned by another worker porting
/// this crate in parallel and is not part of the frozen cross-worker API
/// surface in the integration contract, so this file avoids depending on its
/// exact visibility/signature and instead carries its own copy of the (tiny,
/// stable) splitting rule.
fn split_node_scope(target: &str) -> (&str, Option<&str>) {
    match target.rsplit_once('@') {
        Some((base, scope)) if !scope.is_empty() && !base.is_empty() => (base, Some(scope)),
        _ => (target, None),
    }
}

/// Renders a forward row's endpoint column. Mirrors upstream's
/// `endpoint(spec: &ForwardSpec)` exactly, reading the JSON-sourced
/// `super::app::ForwardRow` instead of the daemon's internal `ForwardSpec`.
pub(super) fn endpoint(row: &super::app::ForwardRow) -> String {
    match row.direction.as_str() {
        "serve" => format!("serving {}", row.addr),
        _ => {
            let (base, scope) = split_node_scope(&row.target);
            match scope {
                Some(node) => format!(":{} → peer {} @ {}", row.listen_port, base, node),
                None => format!(":{} → peer {}", row.listen_port, base),
            }
        }
    }
}

pub(super) fn trust_name(decision: &str) -> &str {
    match decision {
        "allow" => "allow",
        "deny" => "deny ",
        other => other,
    }
}

pub(super) fn format_event(e: &EventRow) -> String {
    format!(
        "{} {:<6} {:<6} {} → {}",
        format_ts_ms(e.timestamp_ms),
        e.decision,
        e.source,
        short_id(&e.peer_id),
        e.target,
    )
}

pub(super) fn format_notice(n: &NoticeRow) -> String {
    format!("[{}] {}", format_ts_ms(n.timestamp_ms), n.text)
}

/// Whether a notice should be rendered in the "error" style. Matches
/// case-insensitively since the exact casing of `NoticeKind`'s JSON
/// representation isn't pinned by the integration contract.
pub(super) fn is_error_notice(n: &NoticeRow) -> bool {
    n.kind.eq_ignore_ascii_case("error")
}

pub(super) fn format_chat(m: &ChatRow) -> String {
    if m.mine {
        format!("> {}", m.text)
    } else {
        format!("[{}] {}", short_id(&m.peer_id), m.text)
    }
}

pub(super) fn format_pending_outgoing(o: &PendingOutgoingRow) -> String {
    format!(
        "[OUT] waiting on {} re: {} {} → {}",
        short_id(&o.peer_id),
        o.proto.to_uppercase(),
        if o.local_addr.is_empty() {
            o.listen_port.to_string()
        } else {
            o.local_addr.clone()
        },
        if o.remote_addr.is_empty() {
            o.target.clone()
        } else {
            o.remote_addr.clone()
        }
    )
}

pub(super) fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

pub(super) fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "K", "M", "G"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}", n)
    } else {
        format!("{:.1}{}", value, UNITS[unit])
    }
}

/// Formats a `*_ms` timestamp as a local wall-clock `HH:MM:SS`. Upstream's
/// audit log instead showed a monotonically increasing in-process sequence
/// number (`AuthEvent::sequence`), which the wire-level `tunnel.status`
/// shape (per-event `timestamp_ms`, no sequence) doesn't carry, so this
/// swaps in a compact clock time instead.
fn format_ts_ms(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| dt.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "--:--:--".to_string())
}
