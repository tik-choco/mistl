//! Parses a single text-shell line into a [`ShellCommand`]. Ported unchanged
//! (besides the import path) from `p2p/src/control_shell/commands.rs`.

use anyhow::{Result, anyhow};

use crate::tunnel::controller::{Direction, ForwardSpec, Proto};
use crate::tunnel::forward_args::{forward_key, parse_connect_forward, parse_forward};

use super::{pending, trust};

pub(super) enum ShellCommand {
    Add(ForwardSpec),
    Remove(String),
    List,
    Events,
    Pending,
    ResolvePending(pending::ResolveCommand),
    Trust(trust::Command),
    Help,
    Quit,
}

pub(super) fn parse_command(line: &str) -> Result<Option<ShellCommand>> {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let Some(command) = parts.first().copied() else {
        return Ok(None);
    };

    match command {
        "add" if parts.len() == 3 => parse_add(parts[1], parts[2]).map(Some),
        "remove" | "rm" if parts.len() == 2 => Ok(Some(ShellCommand::Remove(parts[1].into()))),
        "list" | "ls" if parts.len() == 1 => Ok(Some(ShellCommand::List)),
        "events" | "ev" if parts.len() == 1 => Ok(Some(ShellCommand::Events)),
        "pending" | "p" if parts.len() == 1 => Ok(Some(ShellCommand::Pending)),
        "approve" | "allow" | "a" => pending::parse_resolve(&parts, true)
            .map(ShellCommand::ResolvePending)
            .map(Some),
        "deny" | "d" => pending::parse_resolve(&parts, false)
            .map(ShellCommand::ResolvePending)
            .map(Some),
        "trust" | "t" => trust::parse(&parts).map(ShellCommand::Trust).map(Some),
        "help" | "h" if parts.len() == 1 => Ok(Some(ShellCommand::Help)),
        "quit" | "q" | "exit" if parts.len() == 1 => Ok(Some(ShellCommand::Quit)),
        _ => Err(anyhow!("unknown command; type `help` for usage")),
    }
}

fn parse_add(direction: &str, forward: &str) -> Result<ShellCommand> {
    match direction {
        "serve" => {
            let (proto, addr, _) = parse_forward(forward);
            Ok(ShellCommand::Add(ForwardSpec {
                direction: Direction::Serve,
                proto: Proto::from_name(proto)?,
                addr: addr.to_string(),
                listen_port: -1,
                target: forward_key(proto, addr),
            }))
        }
        "connect" => {
            let (proto, listen_port, target) = parse_connect_forward(forward);
            Ok(ShellCommand::Add(ForwardSpec {
                direction: Direction::Connect,
                proto: Proto::from_name(proto)?,
                addr: String::new(),
                listen_port,
                target,
            }))
        }
        _ => Err(anyhow!("direction must be `serve` or `connect`")),
    }
}
