//! A line-oriented text command shell over the forward controller and
//! auth/trust bookkeeping (`add`, `remove`, `list`, `events`, `pending`,
//! `approve`/`deny`, `trust ...`, `help`, `quit`). Ported from the
//! standalone `p2p` crate's `src/control_shell.rs` (plus `commands.rs`,
//! `events.rs`, `pending.rs`, `trust.rs`), which upstream's
//! `app::shell::run_control_shell` (a CLI entry point, not ported -- see
//! below) drove over process stdin/stdout as an alternative to the TUI.
//!
//! Only the import paths changed (`crate::auth`/`crate::controller` ->
//! `crate::tunnel::auth`/`crate::tunnel::controller`); [`run`] itself is
//! still generic over any `AsyncBufRead`/`AsyncWrite` pair, so it isn't
//! actually a "CLI entry point" the way `app::shell::run_control_shell`
//! was -- that function's *only* job was wiring `run` up to
//! `tokio::io::stdin()`/`stdout()` plus a `Ctrl-C`/manager-lifecycle
//! wrapper, which doesn't apply once the daemon (not a per-invocation
//! process) owns the tunnel's lifecycle (see the integration contract).
//! `run` is kept as-is (marked `#[allow(dead_code)]` where currently
//! unwired) in case a future debug surface wants a text-command console
//! over some other transport (e.g. a raw TCP/websocket debug port); nothing
//! in this port currently drives it.

use anyhow::Result;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::tunnel::auth::{AuthAuditLog, PendingAuthorizations, TrustStore};
use crate::tunnel::controller::{Direction, ForwardController, ForwardSpec, ForwardState};

mod commands;
mod events;
mod pending;
mod trust;

use commands::{ShellCommand, parse_command};

#[derive(Debug)]
pub struct ShellOutcome {
    pub output: String,
    pub should_quit: bool,
}

#[allow(dead_code)]
pub async fn run<R, W>(
    controller: ForwardController,
    trust_store: TrustStore,
    audit_log: AuthAuditLog,
    pending_auth: PendingAuthorizations,
    mut reader: R,
    mut writer: W,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    writer.write_all(help_text().as_bytes()).await?;
    writer.write_all(b"> ").await?;
    writer.flush().await?;

    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }

        let outcome = execute_line_with_context(
            &controller,
            Some(&trust_store),
            Some(&audit_log),
            Some(&pending_auth),
            &line,
        )
        .await?;
        if !outcome.output.is_empty() {
            writer.write_all(outcome.output.as_bytes()).await?;
        }
        if outcome.should_quit {
            break;
        }
        writer.write_all(b"> ").await?;
        writer.flush().await?;
    }

    Ok(())
}

#[cfg(test)]
pub(crate) async fn execute_line(
    controller: &ForwardController,
    line: &str,
) -> Result<ShellOutcome> {
    execute_line_with_trust(controller, None, line).await
}

#[cfg(test)]
pub(crate) async fn execute_line_with_trust(
    controller: &ForwardController,
    trust_store: Option<&TrustStore>,
    line: &str,
) -> Result<ShellOutcome> {
    execute_line_with_context(controller, trust_store, None, None, line).await
}

pub async fn execute_line_with_context(
    controller: &ForwardController,
    trust_store: Option<&TrustStore>,
    audit_log: Option<&AuthAuditLog>,
    pending_auth: Option<&PendingAuthorizations>,
    line: &str,
) -> Result<ShellOutcome> {
    let Some(command) = parse_command(line)? else {
        return Ok(ShellOutcome {
            output: String::new(),
            should_quit: false,
        });
    };

    match command {
        ShellCommand::Add(spec) => {
            let key = controller.add_forward(spec).await?;
            Ok(ShellOutcome {
                output: format!("added {}\n", key),
                should_quit: false,
            })
        }
        ShellCommand::Remove(key) => {
            controller.remove_forward(&key).await?;
            Ok(ShellOutcome {
                output: format!("removed {}\n", key),
                should_quit: false,
            })
        }
        ShellCommand::List => Ok(ShellOutcome {
            output: format_statuses(controller.list_forwards().await),
            should_quit: false,
        }),
        ShellCommand::Events => Ok(ShellOutcome {
            output: events::format(audit_log).await?,
            should_quit: false,
        }),
        ShellCommand::Pending => Ok(ShellOutcome {
            output: pending::format(pending_auth).await?,
            should_quit: false,
        }),
        ShellCommand::ResolvePending(command) => Ok(ShellOutcome {
            output: pending::resolve(pending_auth, command).await?,
            should_quit: false,
        }),
        ShellCommand::Trust(command) => Ok(ShellOutcome {
            output: trust::execute(trust_store, command).await?,
            should_quit: false,
        }),
        ShellCommand::Help => Ok(ShellOutcome {
            output: help_text(),
            should_quit: false,
        }),
        ShellCommand::Quit => Ok(ShellOutcome {
            output: "bye\n".to_string(),
            should_quit: true,
        }),
    }
}

fn format_statuses(statuses: Vec<crate::tunnel::controller::ForwardStatus>) -> String {
    if statuses.is_empty() {
        return "no forwards\n".to_string();
    }

    let mut out = String::from("key direction proto endpoint state conns in out\n");
    for status in statuses {
        out.push_str(&format!(
            "{} {} {} {} {} {} {} {}\n",
            status.key,
            direction_name(status.spec.direction),
            status.spec.proto.as_str(),
            endpoint(&status.spec),
            state_name(&status.state),
            status.active_conns,
            status.bytes_in,
            status.bytes_out
        ));
    }
    out
}

fn endpoint(spec: &ForwardSpec) -> String {
    match spec.direction {
        Direction::Serve => spec.addr.clone(),
        Direction::Connect => format!(":{}", spec.listen_port),
    }
}

fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Serve => "serve",
        Direction::Connect => "connect",
    }
}

fn state_name(state: &ForwardState) -> &str {
    match state {
        ForwardState::Listening => "listening",
        ForwardState::Error(_) => "error",
        ForwardState::Stopped => "stopped",
    }
}

fn help_text() -> String {
    [
        "commands:",
        "  add serve <[proto://]<addr>>",
        "  add connect <[proto://]<listen-port>[:remote-port]>",
        "  remove <target>",
        "  list",
        "  events",
        "  pending",
        "  approve <pending-id> [always]",
        "  deny <pending-id> [always]",
        "  trust list",
        "  trust allow <peer-id> <target>",
        "  trust deny <peer-id> <target>",
        "  trust remove <peer-id> <target>",
        "  quit",
        "",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests;
