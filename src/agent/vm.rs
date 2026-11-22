//! SSH-backed commands for the agent's guest workspace.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::agent::process::{self, CommandSpec};
use crate::agent::tools::Invocation;
use crate::agent::workspace;
use crate::daemon::ipc::client_request;

pub(super) const ROOT: &str = "/home/mistl/work";
pub(super) const CONTEXT: &str = "Commands execute inside a persistent, disposable Ubuntu VM as user `mistl` with passwordless sudo. Files under /home/mistl/work persist across sessions. Host files are unavailable in the guest and commands run on the guest OS. The guest's access to host network services depends on the VM's network configuration.";
const FILE_TOOLS: &str = include_str!("vm_files.py");

#[derive(Debug, Clone)]
pub(super) struct Vm {
    ssh_args: Vec<String>,
    host_cwd: PathBuf,
    forward: Option<String>,
}

impl Vm {
    pub(super) async fn connect(id: String, node: Option<String>) -> Result<Self> {
        let unreachable = || format!("cannot reach VM {id:?}");
        let host_cwd = std::env::current_dir()?;
        let args = json!({"id": id, "node": node});
        let connection = tokio::task::spawn_blocking(move || client_request("vm.connect", args))
            .await
            .with_context(unreachable)?
            .with_context(unreachable)?;
        let forward = connection["target"].as_str().map(str::to_owned);
        let initialized: Result<Self> = async {
            let vm = Self {
                ssh_args: ssh_args(&connection)?,
                host_cwd,
                forward: forward.clone(),
            };
            let command = vm.remote_command(
                "-c",
                &format!("mkdir -p -- '{ROOT}' && [ \"$(realpath -e -- '{ROOT}')\" = '{ROOT}' ]"),
            );
            process::run_stdout(&command, Duration::from_secs(20), &CancellationToken::new())
                .await
                .with_context(|| format!("SSH could not initialize the workspace at {ROOT}"))?;
            Ok(vm)
        }
        .await;
        match initialized {
            Ok(vm) => Ok(vm),
            Err(error) => {
                disconnect(forward).await;
                Err(error).with_context(unreachable)
            }
        }
    }

    pub(super) async fn disconnect(self) {
        disconnect(self.forward).await;
    }

    pub(super) fn shell_command(&self, command: &str) -> CommandSpec {
        // Exit before running any line of the command if the workspace is gone.
        self.remote_command("-lc", &format!("cd -- '{ROOT}' || exit\n{command}"))
    }

    pub(super) async fn run_file_tool(
        &self,
        inv: &Invocation,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<String> {
        let args = match inv {
            Invocation::ListDir { path } => {
                vec![
                    "list_dir".into(),
                    workspace_path(path.as_deref().unwrap_or("."))?,
                ]
            }
            Invocation::FindFiles {
                glob,
                include_ignored,
            } => {
                vec![
                    "find_files".into(),
                    glob.clone(),
                    include_ignored.to_string(),
                ]
            }
            Invocation::Grep {
                pattern,
                path,
                glob,
                case_insensitive,
            } => vec![
                "grep".into(),
                workspace_path(path.as_deref().unwrap_or("."))?,
                pattern.clone(),
                glob.clone().unwrap_or_default(),
                case_insensitive.to_string(),
            ],
            Invocation::ReadFile {
                path,
                offset,
                limit,
            } => vec![
                "read_file".into(),
                workspace_path(path)?,
                offset.unwrap_or(1).max(1).to_string(),
                limit.unwrap_or(400).min(2000).to_string(),
            ],
            _ => unreachable!("only file tools dispatched here"),
        };
        ensure!(
            args.iter().all(|arg| !arg.contains('\0')),
            "file tool argument contains a NUL byte"
        );
        let arguments = args
            .iter()
            .map(|arg| shell_quote(arg))
            .collect::<Vec<_>>()
            .join(" ");
        let spec = self.remote_command(
            "-c",
            &format!(
                "cd -P -- '{ROOT}' && exec /usr/bin/timeout {} /usr/bin/python3 -I -c {} {arguments}",
                timeout.as_secs().max(1), shell_quote(FILE_TOOLS),
            ),
        );
        let text = process::run_stdout(&spec, timeout, cancel).await?;
        Ok(workspace::capped(&text, workspace::MAX_OUTPUT_BYTES))
    }

    fn remote_command(&self, bash_flag: &str, command: &str) -> CommandSpec {
        // OpenSSH joins these arguments into a command line for the guest shell.
        let mut args = self.ssh_args.clone();
        args.extend([
            "--".into(),
            "bash".into(),
            bash_flag.into(),
            shell_quote(command),
        ]);
        CommandSpec {
            program: "ssh".into(),
            args,
            cwd: self.host_cwd.clone(),
        }
    }
}

async fn disconnect(forward: Option<String>) {
    if let Some(target) = forward {
        match tokio::task::spawn_blocking(move || {
            client_request("vm.disconnect", json!({"target": target}))
        })
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "could not remove VM SSH forward"),
            Err(error) => tracing::warn!(%error, "VM SSH disconnect task failed"),
        }
    }
}

fn ssh_args(connection: &Value) -> Result<Vec<String>> {
    let field = |name: &str| {
        connection[name]
            .as_str()
            .filter(|value| !value.is_empty())
            .with_context(|| format!("missing VM SSH {name}"))
    };
    let port = connection["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .filter(|&port| port != 0)
        .context("missing or invalid VM SSH port")?;
    let user = field("user")?;
    ensure!(user == "mistl", "unexpected VM SSH user");
    // Ignore host SSH configuration, which can enable proxies and forwarding.
    Ok(vec![
        "-F".into(),
        "none".into(),
        "-i".into(),
        field("key")?.into(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
        "-o".into(),
        format!("HostKeyAlias={}", field("host_key_alias")?),
        "-o".into(),
        format!(
            "UserKnownHostsFile={}",
            ssh_path_token(field("known_hosts")?)
        ),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "LogLevel=ERROR".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-p".into(),
        port.to_string(),
        format!("{user}@127.0.0.1"),
    ])
}

fn ssh_path_token(path: &str) -> String {
    // Match the CLI's OpenSSH config token handling, including MSYS ssh.
    let path = path.replace('%', "%%");
    if !path
        .chars()
        .any(|c| c.is_whitespace() || c == '"' || c == '\'')
    {
        return path;
    }
    format!("\"{}\"", path.replace('\\', "\\\\").replace('"', "\\\""))
}

fn shell_quote(word: &str) -> String {
    format!("'{}'", word.replace('\'', "'\\''"))
}

fn workspace_path(path: &str) -> Result<String> {
    let relative = if path.starts_with('/') {
        if path == ROOT {
            "."
        } else {
            path.strip_prefix(ROOT)
                .and_then(|rest| rest.strip_prefix('/'))
                .context("path is outside the workspace root")?
        }
    } else {
        path
    };
    let relative = relative.trim_start_matches('/');
    let mut depth = 0;
    for part in relative.split('/') {
        match part {
            "" | "." => {}
            ".." if depth == 0 => bail!("path is outside the workspace root"),
            ".." => depth -= 1,
            _ => depth += 1,
        }
    }
    // Keep interior `..` components so the guest resolves them after symlinks.
    Ok(if relative.is_empty() {
        ".".into()
    } else {
        relative.into()
    })
}

#[cfg(test)]
pub(super) fn test_vm() -> Vm {
    let connection = json!({
        "port": 2222,
        "user": "mistl",
        "key": r"C:\Users\A B\key",
        "known_hosts": r"C:\Users\A B\known_hosts",
        "host_key_alias": "mistl-vm-test",
    });
    Vm {
        ssh_args: ssh_args(&connection).unwrap(),
        host_cwd: ".".into(),
        forward: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_argv_keeps_windows_paths_and_posix_command_quoting_separate() {
        let vm = test_vm();
        let spec = vm.shell_command("printf '%s\\n' \"$HOME's files\"");
        assert_eq!(spec.program, "ssh");
        assert_eq!(spec.cwd, PathBuf::from("."));
        assert_eq!(
            spec.args,
            [
                "-F",
                "none",
                "-i",
                r"C:\Users\A B\key",
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "HostKeyAlias=mistl-vm-test",
                "-o",
                r#"UserKnownHostsFile="C:\\Users\\A B\\known_hosts""#,
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "BatchMode=yes",
                "-o",
                "LogLevel=ERROR",
                "-o",
                "ConnectTimeout=10",
                "-p",
                "2222",
                "mistl@127.0.0.1",
                "--",
                "bash",
                "-lc",
                "'cd -- '\\''/home/mistl/work'\\'' || exit\nprintf '\\''%s\\n'\\'' \"$HOME'\\''s files\"'",
            ]
        );
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("a b'$(pwd)"), "'a b'\\''$(pwd)'");
        assert_eq!(
            ssh_path_token(r"C:\Users\A\known_hosts"),
            r"C:\Users\A\known_hosts"
        );
        assert_eq!(
            ssh_path_token("/home/100%/known_hosts"),
            "/home/100%%/known_hosts"
        );
        assert_eq!(
            ssh_path_token("/home/a'b/known_hosts"),
            r#""/home/a'b/known_hosts""#
        );
    }

    #[test]
    fn guest_file_paths_use_posix_components_and_stay_within_root() {
        for (path, expected) in [
            (ROOT, "."),
            ("/home/mistl/work/", "."),
            ("/home/mistl/work//a", "a"),
            ("/home/mistl/work/a/../b", "a/../b"),
            ("one's file", "one's file"),
            ("nested/a..b", "nested/a..b"),
        ] {
            assert_eq!(workspace_path(path).unwrap(), expected);
        }
        for path in [
            "/etc/passwd",
            "/home/mistl/work-other/a",
            "../a",
            "a/../../b",
            "/home/mistl/work/../work",
        ] {
            assert!(workspace_path(path).is_err(), "allowed {path:?}");
        }
    }

    #[tokio::test]
    async fn path_confinement_applies_before_ssh_for_each_file_tool_with_a_path() {
        let vm = test_vm();
        let token = CancellationToken::new();
        for (name, arguments) in [
            ("list_dir", r#"{"path":"/etc"}"#),
            ("read_file", r#"{"path":"../file"}"#),
            (
                "grep",
                r#"{"pattern":"a","path":"/home/mistl/work/../../etc"}"#,
            ),
        ] {
            let inv = crate::agent::tools::parse_call(name, arguments).unwrap();
            let error = vm
                .run_file_tool(&inv, Duration::from_secs(1), &token)
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), "path is outside the workspace root");
        }
    }
}
