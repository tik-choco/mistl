//! Renders the `events` command's output (the auth audit log). Ported
//! unchanged (besides the import path) from `p2p/src/control_shell/events.rs`.

use anyhow::{Result, anyhow};

use crate::tunnel::auth::{AuthAuditLog, AuthDecision, AuthEvent, AuthEventSource};

pub(super) async fn format(audit_log: Option<&AuthAuditLog>) -> Result<String> {
    let Some(log) = audit_log else {
        return Err(anyhow!("events are unavailable in this context"));
    };
    Ok(format_auth_events(log.list().await))
}

fn format_auth_events(events: Vec<AuthEvent>) -> String {
    if events.is_empty() {
        return "no auth events\n".to_string();
    }

    let mut out = String::from("seq decision source peer target proto addr\n");
    for event in events {
        out.push_str(&format!(
            "{} {} {} {} {} {} {}\n",
            event.sequence,
            auth_decision_name(event.decision),
            event_source_name(event.source),
            event.peer_id,
            event.forward_key,
            event.proto,
            event.target_addr
        ));
    }
    out
}

fn auth_decision_name(decision: AuthDecision) -> &'static str {
    match decision {
        AuthDecision::Allow | AuthDecision::AllowAlways => "allow",
        AuthDecision::Deny | AuthDecision::DenyAlways => "deny",
    }
}

fn event_source_name(source: AuthEventSource) -> &'static str {
    match source {
        AuthEventSource::Policy => "policy",
        AuthEventSource::TrustStore => "trust",
        AuthEventSource::Pending => "pending",
    }
}
