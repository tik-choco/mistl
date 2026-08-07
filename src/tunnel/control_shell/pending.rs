//! Parses/renders the `pending`/`approve`/`deny` commands (the pending
//! connection-authorization queue). Ported unchanged (besides the import
//! path) from `p2p/src/control_shell/pending.rs`.

use anyhow::{Result, anyhow};

use crate::tunnel::auth::{AuthDecision, PendingAuthorizations};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolveCommand {
    pub(crate) id: u64,
    pub(crate) decision: AuthDecision,
}

pub(crate) fn parse_resolve(parts: &[&str], allow: bool) -> Result<ResolveCommand> {
    if !(parts.len() == 2 || parts.len() == 3) {
        return Err(anyhow!(
            "usage: {} <pending-id> [always]",
            if allow { "approve" } else { "deny" }
        ));
    }
    let id = parts[1].parse::<u64>()?;
    let remember = match parts.get(2).copied() {
        Some("always") => true,
        Some(_) => return Err(anyhow!("third argument must be `always`")),
        None => false,
    };
    let decision = match (allow, remember) {
        (true, true) => AuthDecision::AllowAlways,
        (true, false) => AuthDecision::Allow,
        (false, true) => AuthDecision::DenyAlways,
        (false, false) => AuthDecision::Deny,
    };
    Ok(ResolveCommand { id, decision })
}

pub(crate) async fn format(pending: Option<&PendingAuthorizations>) -> Result<String> {
    let pending = pending.ok_or_else(|| anyhow!("pending auth commands are unavailable"))?;
    let items = pending.list().await;
    if items.is_empty() {
        return Ok("no pending auth requests\n".to_string());
    }

    let mut out = String::from("id peer target proto addr\n");
    for item in items {
        out.push_str(&format!(
            "{} {} {} {} {}\n",
            item.id,
            item.request.peer_id,
            item.request.forward_key,
            item.request.proto,
            item.request.target_addr
        ));
    }
    Ok(out)
}

pub(crate) async fn resolve(
    pending: Option<&PendingAuthorizations>,
    command: ResolveCommand,
) -> Result<String> {
    let pending = pending.ok_or_else(|| anyhow!("pending auth commands are unavailable"))?;
    if pending.resolve(command.id, command.decision).await {
        Ok(format!(
            "{} pending {}\n",
            decision_name(command.decision),
            command.id
        ))
    } else {
        Err(anyhow!("pending auth request {} not found", command.id))
    }
}

fn decision_name(decision: AuthDecision) -> &'static str {
    match decision {
        AuthDecision::Allow | AuthDecision::AllowAlways => "approved",
        AuthDecision::Deny | AuthDecision::DenyAlways => "denied",
    }
}
