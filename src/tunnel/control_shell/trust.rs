//! Parses/renders the `trust` command family (`list`/`allow`/`deny`/
//! `remove`). Ported unchanged (besides the import path) from
//! `p2p/src/control_shell/trust.rs`.

use anyhow::{Result, anyhow};

use crate::tunnel::auth::{TrustDecision, TrustEntry, TrustKey, TrustStore};

pub(super) enum Command {
    List,
    Remember {
        peer_id: String,
        forward_key: String,
        decision: TrustDecision,
    },
    Remove {
        peer_id: String,
        forward_key: String,
    },
}

pub(super) fn parse(parts: &[&str]) -> Result<Command> {
    match parts {
        [_, "list"] => Ok(Command::List),
        [_, "allow", peer_id, forward_key] => Ok(Command::Remember {
            peer_id: (*peer_id).to_string(),
            forward_key: (*forward_key).to_string(),
            decision: TrustDecision::Allow,
        }),
        [_, "deny", peer_id, forward_key] => Ok(Command::Remember {
            peer_id: (*peer_id).to_string(),
            forward_key: (*forward_key).to_string(),
            decision: TrustDecision::Deny,
        }),
        [_, "remove" | "rm", peer_id, forward_key] => Ok(Command::Remove {
            peer_id: (*peer_id).to_string(),
            forward_key: (*forward_key).to_string(),
        }),
        _ => Err(anyhow!(
            "usage: trust list | trust allow <peer-id> <target> | trust deny <peer-id> <target> | trust remove <peer-id> <target>"
        )),
    }
}

pub(super) async fn execute(trust_store: Option<&TrustStore>, command: Command) -> Result<String> {
    let Some(store) = trust_store else {
        return Err(anyhow!("trust commands are unavailable in this context"));
    };

    match command {
        Command::List => Ok(format_entries(store.list().await)),
        Command::Remember {
            peer_id,
            forward_key,
            decision,
        } => {
            let key = TrustKey {
                peer_id,
                forward_key,
            };
            store.remember(key.clone(), decision).await?;
            Ok(format!(
                "trusted {} {} {}\n",
                key.peer_id,
                key.forward_key,
                decision_name(decision)
            ))
        }
        Command::Remove {
            peer_id,
            forward_key,
        } => {
            let key = TrustKey {
                peer_id,
                forward_key,
            };
            if store.remove(&key).await? {
                Ok(format!(
                    "removed trust {} {}\n",
                    key.peer_id, key.forward_key
                ))
            } else {
                Ok(format!(
                    "trust not found {} {}\n",
                    key.peer_id, key.forward_key
                ))
            }
        }
    }
}

fn format_entries(entries: Vec<TrustEntry>) -> String {
    if entries.is_empty() {
        return "no trust entries\n".to_string();
    }

    let mut out = String::from("peer target decision\n");
    for entry in entries {
        out.push_str(&format!(
            "{} {} {}\n",
            entry.key.peer_id,
            entry.key.forward_key,
            decision_name(entry.decision)
        ));
    }
    out
}

fn decision_name(decision: TrustDecision) -> &'static str {
    match decision {
        TrustDecision::Allow => "allow",
        TrustDecision::Deny => "deny",
    }
}
