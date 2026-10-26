//! The mistl tool: schema, safety classification, and subprocess runner.

use std::ffi::OsString;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::agent::config::Config;
use crate::agent::process;
use crate::agent::types::{FunctionSpec, Safety, ToolOutput, ToolSpec};

pub const TOOL_MISTL: &str = "mistl";
pub const TOOL_HELP: &str = "mistl_help";

/// One parsed tool request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    /// `mistl <args...>`
    Run(Vec<String>),
    /// `mistl <path...> --help` (empty path = top level)
    Help(Vec<String>),
}

/// Native-mode tool definitions.
pub fn tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            kind: "function",
            function: FunctionSpec {
                name: TOOL_MISTL,
                description: "Run the mistl CLI with these arguments (do not include the leading \
                    \"mistl\"), e.g. [\"store\",\"ls\"], [\"ai\",\"status\"], \
                    [\"config\",\"show\"]. Many commands print \
                    JSON. Read-only queries run immediately; state-changing commands require \
                    user approval and may be declined."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "args": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Command-line arguments, one array element per argument"
                        }
                    },
                    "required": ["args"]
                }),
            },
        },
        ToolSpec {
            kind: "function",
            function: FunctionSpec {
                name: TOOL_HELP,
                description: "Show `mistl <command> --help`. Use it to learn the exact syntax of \
                    a subcommand before running it. Omit `command` for the top-level help."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "Subcommand path, e.g. \"store folder-share\""
                        }
                    }
                }),
            },
        },
    ]
}

/// Parse a tool call (name + JSON arguments) into an invocation.
pub fn parse_call(name: &str, arguments: &str) -> Result<Invocation> {
    let trimmed = arguments.trim();
    let value: Value = if trimmed.is_empty() {
        json!({})
    } else {
        serde_json::from_str(trimmed).map_err(|e| anyhow!("invalid tool arguments JSON: {e}"))?
    };
    match name {
        TOOL_MISTL => {
            let mut args = words_from(value.get("args"))?;
            drop_leading_mistl(&mut args);
            Ok(Invocation::Run(args))
        }
        TOOL_HELP => {
            let mut path = words_from(value.get("command").or_else(|| value.get("args")))?;
            drop_leading_mistl(&mut path);
            if matches!(path.last().map(String::as_str), Some("--help" | "-h")) {
                path.pop();
            }
            if !is_help_path(&path) {
                bail!(
                    "mistl_help accepts only a subcommand path, without flags or positional values"
                );
            }
            Ok(Invocation::Help(path))
        }
        other => bail!("unknown tool: {other}"),
    }
}

/// Accepts an array of strings (numbers/bools stringified), a single string
/// (shell-split), or nothing.
fn words_from(v: Option<&Value>) -> Result<Vec<String>> {
    match v {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(split_args(s)),
        Some(Value::Array(items)) => items
            .iter()
            .map(|i| match i {
                Value::String(s) => Ok(s.clone()),
                Value::Number(n) => Ok(n.to_string()),
                Value::Bool(b) => Ok(b.to_string()),
                _ => Err(anyhow!("args must be an array of strings")),
            })
            .collect(),
        Some(_) => bail!("args must be an array of strings"),
    }
}

fn drop_leading_mistl(args: &mut Vec<String>) {
    if matches!(
        args.first().map(String::as_str),
        Some("mistl" | "mistl.exe")
    ) {
        args.remove(0);
    }
}

/// Small shell-style word splitter: whitespace separates words; single quotes
/// are literal; double quotes group; a backslash escapes a quote character
/// (otherwise it is kept literally so Windows paths survive).
pub(crate) fn split_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_word = false;
    let mut quote: Option<char> = None;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some('\'') => {
                if c == '\'' {
                    quote = None;
                } else {
                    cur.push(c);
                }
            }
            Some(q) => {
                if c == q {
                    quote = None;
                } else if c == '\\' && matches!(chars.peek(), Some('"') | Some('\\')) {
                    // Inside double quotes `\\` and `\"` are escapes.
                    cur.push(chars.next().unwrap());
                } else {
                    cur.push(c);
                }
            }
            None => {
                if c.is_whitespace() {
                    if in_word {
                        out.push(std::mem::take(&mut cur));
                        in_word = false;
                    }
                } else if c == '"' || c == '\'' {
                    quote = Some(c);
                    in_word = true;
                } else if c == '\\' && matches!(chars.peek(), Some('"') | Some('\'')) {
                    cur.push(chars.next().unwrap());
                    in_word = true;
                } else {
                    cur.push(c);
                    in_word = true;
                }
            }
        }
    }
    if in_word {
        out.push(cur);
    }
    out
}

/// Subcommand paths (leading non-flag words) that are read-only. Matching is
/// by prefix, so trailing positionals (`ai chat "hi"`, `chat log room`) are fine.
const READ_ONLY: &[&[&str]] = &[
    &["status"],
    &["build-info"],
    &["daemon", "status"],
    &["network", "status"],
    &["profile", "show"],
    &["key", "list"],
    &["key", "did"],
    &["key", "delegations"],
    &["store", "ls"],
    &["store", "parse-link"],
    &["store", "sandbox", "ls"],
    &["store", "sandbox-list"],
    &["store", "folder-sync", "ls"],
    &["store", "folder-share", "ls"],
    &["store", "folder-access", "ls"],
    &["stream", "status"],
    &["chat", "rooms"],
    &["chat", "log"],
    &["ai", "status"],
    &["ai", "models"],
    &["ai", "chat"],
    &["sched", "ls"],
    &["sched", "logs"],
    &["sched", "next"],
    &["bot", "list"],
    &["bot", "logs"],
    &["bot", "items"],
    &["bot", "status"],
    &["tunnel", "status"],
    &["tunnel", "ls"],
    &["config", "show"],
    &["update", "check"],
    &["update", "status"],
    &["autostart", "status"],
];

/// Fail-closed help paths: only an exact subcommand path of mistl's own CLI
/// (read from its clap definition, so it never drifts) counts as a help
/// request; positionals and option values must never turn a state-changing
/// invocation into an approval-free one.
fn is_help_path(path: &[String]) -> bool {
    use clap::CommandFactory;
    let mut command = crate::cli::Cli::command();
    for word in path {
        match command.find_subcommand(word) {
            Some(sub) if sub.get_name() == word => command = sub.clone(),
            _ => return false,
        }
    }
    true
}

/// Global flags the agent controls itself.
const FORBIDDEN_FLAGS: &[&str] = &["--instance", "--state-dir", "--no-tray"];

pub fn classify(inv: &Invocation) -> Safety {
    let args = match inv {
        Invocation::Help(args) | Invocation::Run(args) => args,
    };
    for a in args {
        let flag = a.split('=').next().unwrap_or(a);
        if FORBIDDEN_FLAGS.contains(&flag) {
            return Safety::Blocked(format!(
                "{flag} is controlled by the agent and cannot be set"
            ));
        }
    }
    if matches!(inv, Invocation::Help(_)) {
        return if is_help_path(args) {
            Safety::ReadOnly
        } else {
            Safety::Blocked("mistl_help accepts only a subcommand path".into())
        };
    }
    if args.is_empty() {
        return Safety::Blocked(
            "no arguments: bare `mistl` opens the dashboard (use `ui`, or ask for help)".into(),
        );
    }
    // Only a path plus help/version flags is help-only. In particular flags
    // after `--` are CLI positional values, and values consumed by options
    // must not masquerade as help. Do not guess how unknown flags consume args.
    let help_only = !args.iter().any(|a| a == "--")
        && args
            .iter()
            .any(|a| matches!(a.as_str(), "--help" | "-h" | "--version" | "-V"))
        && is_help_path(
            &args
                .iter()
                .filter(|a| !matches!(a.as_str(), "--help" | "-h" | "--version" | "-V"))
                .cloned()
                .collect::<Vec<_>>(),
        );
    if help_only {
        return Safety::ReadOnly;
    }

    // Leading non-flag words; the store-level `--json` flag may sit anywhere.
    let mut words: Vec<&str> = Vec::new();
    for a in args {
        if a == "--json" {
            continue;
        }
        if a.starts_with('-') {
            break;
        }
        words.push(a.as_str());
    }
    let w = |i: usize| words.get(i).copied();

    if w(0) == Some("daemon") && w(1) == Some("run") {
        return Safety::Blocked(
            "`daemon run` runs in the foreground forever; use `daemon start`".into(),
        );
    }
    if w(0) == Some("tunnel") && w(1) == Some("tui") {
        return Safety::Blocked("`tunnel tui` is an interactive terminal UI".into());
    }
    if w(0) == Some("help") {
        return Safety::ReadOnly;
    }
    // `update` alone defaults to `update check`.
    if words == ["update"] {
        return Safety::ReadOnly;
    }
    if w(0) == Some("tunnel") && w(1) == Some("trust") {
        let revoking = args
            .iter()
            .any(|a| a == "--revoke" || a.starts_with("--revoke="));
        return if revoking {
            Safety::Mutating
        } else {
            Safety::ReadOnly
        };
    }
    if w(0) == Some("tunnel") && w(1) == Some("room") {
        let list = args.iter().any(|a| a == "--list");
        let new = args.iter().any(|a| a == "--new");
        return if list && !new && w(2).is_none() {
            Safety::ReadOnly
        } else {
            Safety::Mutating
        };
    }
    if READ_ONLY
        .iter()
        .any(|p| words.len() >= p.len() && p.iter().zip(&words).all(|(a, b)| a == b))
    {
        return Safety::ReadOnly;
    }
    Safety::Mutating
}

/// Human-readable one-liner, e.g. `mistl store ls`.
pub fn title(inv: &Invocation) -> String {
    let (words, help) = match inv {
        Invocation::Run(a) => (a.as_slice(), false),
        Invocation::Help(p) => (p.as_slice(), true),
    };
    let mut s = String::from("mistl");
    for w in words {
        s.push(' ');
        s.push_str(&quote_word(w));
    }
    if help {
        s.push_str(" --help");
    }
    s
}

fn quote_word(w: &str) -> String {
    if !w.is_empty()
        && !w
            .chars()
            .any(|c| c.is_whitespace() || c == '"' || c == '\'')
    {
        return w.to_string();
    }
    format!("\"{}\"", w.replace('\\', "\\\\").replace('"', "\\\""))
}

pub struct MistlRunner {
    prefix: Vec<OsString>,
    timeout: Duration,
}

impl MistlRunner {
    pub fn new(cfg: &Config) -> Self {
        let context = crate::runtime::context();
        let mut prefix = vec![
            OsString::from("--no-tray"),
            OsString::from("--instance"),
            OsString::from(&context.instance),
        ];
        if let Some(root) = &context.root {
            prefix.push("--state-dir".into());
            prefix.push(root.as_os_str().to_owned());
        }
        Self {
            prefix,
            timeout: Duration::from_secs(cfg.mistl_timeout_secs.max(1)),
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        cancel: &CancellationToken,
        work: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        if cancel.is_cancelled() {
            bail!("cancelled");
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => bail!("cancelled"),
            result = tokio::time::timeout(self.timeout, tokio::task::spawn_blocking(work)) => {
                result.map_err(|_| anyhow!("daemon request timed out"))?
                    .map_err(|e| anyhow!("daemon task failed: {e}"))?
            }
        }
    }

    pub async fn daemon_status(&self, cancel: &CancellationToken) -> Result<Value> {
        self.blocking(cancel, || {
            crate::daemon::ipc::client_request("daemon.status", json!({}))
        })
        .await
    }

    /// Returns true when the daemon needed to be started. Quiet startup is
    /// idempotent if another CLI starts the same instance in the meantime.
    pub async fn ensure_daemon(&self, cancel: &CancellationToken) -> Result<bool> {
        match self.daemon_status(cancel).await {
            Ok(_) => return Ok(false),
            Err(e) if e.to_string().contains("daemon is not running") => {}
            Err(e) => return Err(e),
        }
        self.blocking(cancel, crate::daemon::start_background_quiet)
            .await?;
        Ok(true)
    }

    pub async fn run(
        &self,
        inv: &Invocation,
        cancel: &CancellationToken,
        on_output: &mut (dyn FnMut(&str) + Send),
    ) -> ToolOutput {
        let args: Vec<String> = match inv {
            Invocation::Run(a) => a.clone(),
            Invocation::Help(p) => p.iter().cloned().chain(["--help".to_string()]).collect(),
        };
        self.exec(&args, self.timeout, cancel, on_output).await
    }

    pub async fn serve_start(&self, cancel: &CancellationToken) -> Result<String> {
        let value = self
            .blocking(cancel, || {
                crate::daemon::ipc::client_request("ai.serve.start", json!({}))
            })
            .await?;
        extract_listen(&value)
    }

    /// Top-level `mistl --help` text for the system prompt (cached by the
    /// caller); a short error note if mistl cannot be started.
    pub async fn catalog(&self) -> String {
        let out = self
            .exec(
                &["--help".to_string()],
                Duration::from_secs(10),
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await;
        match extract_stdout(&out.text) {
            Some(s) if out.ok && !s.trim().is_empty() => s,
            _ => format!(
                "(mistl --help unavailable: {})",
                out.text
                    .lines()
                    .find(|l| !l.trim().is_empty())
                    .unwrap_or("unknown error")
            ),
        }
    }

    async fn exec(
        &self,
        args: &[String],
        timeout: Duration,
        cancel: &CancellationToken,
        on_output: &mut (dyn FnMut(&str) + Send),
    ) -> ToolOutput {
        let bin = match std::env::current_exe() {
            Ok(bin) => bin,
            Err(e) => {
                return ToolOutput {
                    text: format!("cannot locate the running mistl executable: {e}"),
                    ok: false,
                };
            }
        };
        let mut cmd = Command::new(&bin);
        cmd.args(&self.prefix)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "warn")
            .kill_on_drop(true);
        #[cfg(windows)]
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW

        process::run_mistl(cmd, timeout, cancel, on_output).await
    }
}

fn extract_listen(value: &Value) -> Result<String> {
    value
        .get("listen")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(String::from)
        .ok_or_else(|| {
            anyhow!("mistl ai serve start response is missing a nonempty string `listen`")
        })
}

/// Pull the stdout section out of the formatted output text.
fn extract_stdout(text: &str) -> Option<String> {
    let start = text.find("--- stdout ---\n")? + "--- stdout ---\n".len();
    let rest = &text[start..];
    let end = rest.find("\n--- stderr ---").unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Remove ANSI escape sequences (CSI, OSC, and two-byte escapes).
pub(crate) fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('[') => {
                for n in it.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&n) {
                        break;
                    }
                }
            }
            Some(']') => {
                while let Some(n) = it.next() {
                    if n == '\x07' {
                        break;
                    }
                    if n == '\x1b' {
                        it.next(); // ST: ESC \
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Keep the head and tail of `s` within `max` bytes, marking the cut.
pub(crate) fn truncate_middle(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let half = max / 2;
    let mut head_end = half;
    while !s.is_char_boundary(head_end) {
        head_end -= 1;
    }
    let mut tail_start = s.len() - half;
    while !s.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    format!(
        "{}\n... [{} bytes truncated] ...\n{}",
        &s[..head_end],
        tail_start - head_end,
        &s[tail_start..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_listen_json() {
        for listen in ["127.0.0.1:8123", "[::1]:8123"] {
            assert_eq!(extract_listen(&json!({"listen": listen})).unwrap(), listen);
        }
        for invalid in [
            json!({}),
            json!({"listen": null}),
            json!({"listen": 42}),
            json!({"listen": " "}),
        ] {
            assert!(extract_listen(&invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn subprocess_uses_the_current_instance_and_state_root() {
        let runner = MistlRunner::new(&Config::default());
        let context = crate::runtime::context();
        assert_eq!(
            &runner.prefix[..3],
            &[
                OsString::from("--no-tray"),
                OsString::from("--instance"),
                OsString::from(&context.instance),
            ]
        );
        if let Some(root) = &context.root {
            assert_eq!(runner.prefix[3], "--state-dir");
            assert_eq!(runner.prefix[4], root.as_os_str());
        } else {
            assert_eq!(runner.prefix.len(), 3);
        }
    }

    fn run(a: &[&str]) -> Invocation {
        Invocation::Run(a.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn parse_array_and_string_args() {
        assert_eq!(
            parse_call("mistl", r#"{"args":["store","ls"]}"#).unwrap(),
            run(&["store", "ls"])
        );
        assert_eq!(
            parse_call("mistl", r#"{"args":"mistl ai chat 'hello world'"}"#).unwrap(),
            run(&["ai", "chat", "hello world"])
        );
        assert_eq!(
            parse_call("mistl", r#"{"args":["mistl","status"]}"#).unwrap(),
            run(&["status"])
        );
        assert_eq!(
            parse_call("mistl", r#"{"args":["tunnel","approve",3]}"#).unwrap(),
            run(&["tunnel", "approve", "3"])
        );
    }

    #[test]
    fn parse_help_and_errors() {
        assert_eq!(
            parse_call("mistl_help", "").unwrap(),
            Invocation::Help(vec![])
        );
        assert_eq!(
            parse_call("mistl_help", r#"{"command":"store folder-share --help"}"#).unwrap(),
            Invocation::Help(vec!["store".into(), "folder-share".into()])
        );
        assert!(parse_call("nope", "{}").is_err());
        assert!(parse_call("mistl", "{bad").is_err());
        assert!(parse_call("mistl", r#"{"args":[{"a":1}]}"#).is_err());
    }

    #[test]
    fn classify_samples() {
        assert!(matches!(classify(&run(&[])), Safety::Blocked(_)));
        assert!(matches!(
            classify(&run(&["--instance", "x", "status"])),
            Safety::Blocked(_)
        ));
        assert!(matches!(
            classify(&run(&["--state-dir=x", "status"])),
            Safety::Blocked(_)
        ));
        assert!(matches!(
            classify(&run(&["status", "--no-tray"])),
            Safety::Blocked(_)
        ));
        for flag in ["--instance=other", "--state-dir=elsewhere", "--no-tray"] {
            assert!(matches!(
                classify(&Invocation::Help(vec![flag.into()])),
                Safety::Blocked(_)
            ));
        }
        assert!(matches!(
            classify(&run(&["daemon", "run"])),
            Safety::Blocked(_)
        ));
        assert!(matches!(
            classify(&run(&["tunnel", "tui"])),
            Safety::Blocked(_)
        ));
        assert_eq!(classify(&run(&["store", "ls"])), Safety::ReadOnly);
        assert_eq!(classify(&run(&["store", "--json", "ls"])), Safety::ReadOnly);
        assert_eq!(classify(&run(&["store", "ls", "--json"])), Safety::ReadOnly);
        assert_eq!(
            classify(&run(&["store", "sandbox", "ls"])),
            Safety::ReadOnly
        );
        assert_eq!(
            classify(&run(&["store", "folder-share", "ls"])),
            Safety::ReadOnly
        );
        assert_eq!(classify(&run(&["ai", "chat", "hi"])), Safety::ReadOnly);
        assert_eq!(
            classify(&run(&["chat", "log", "room1", "-l", "5"])),
            Safety::ReadOnly
        );
        assert_eq!(classify(&run(&["update"])), Safety::ReadOnly);
        assert_eq!(classify(&run(&["tunnel", "trust"])), Safety::ReadOnly);
        assert_eq!(
            classify(&run(&["tunnel", "room", "--list"])),
            Safety::ReadOnly
        );
        assert_eq!(
            classify(&run(&["store", "put", "--help"])),
            Safety::ReadOnly
        );
        assert_eq!(classify(&Invocation::Help(vec![])), Safety::ReadOnly);
        assert_eq!(classify(&run(&["store", "put", "ls"])), Safety::Mutating);
        assert_eq!(
            classify(&run(&["config", "set", "a.b", "1"])),
            Safety::Mutating
        );
        assert_eq!(classify(&run(&["daemon", "start"])), Safety::Mutating);
        assert_eq!(classify(&run(&["ui"])), Safety::Mutating);
        assert_eq!(
            classify(&run(&["tunnel", "trust", "--revoke", "a@b"])),
            Safety::Mutating
        );
        assert_eq!(classify(&run(&["tunnel", "room"])), Safety::Mutating);
        assert_eq!(classify(&run(&["update", "apply"])), Safety::Mutating);
        assert_eq!(classify(&run(&["sched", "rm", "job-1"])), Safety::Mutating);
    }

    #[test]
    fn help_never_bypasses_positional_or_option_value_approval() {
        for args in [
            vec!["profile", "set", "display_name", "--", "--help"],
            vec!["profile", "set", "display_name", "--help"],
            vec!["profile", "set", "display_name", "--", "-h"],
            vec!["profile", "set", "display_name", "--", "--version"],
            vec!["profile", "set", "display_name", "--", "-V"],
            vec!["store", "put", "file.txt", "--help"],
            vec!["daemon", "start", "--host", "--help"],
            vec!["store", "put", "--", "--help"],
            vec!["config", "set", "profile.name", "value", "-h"],
        ] {
            assert_eq!(classify(&run(&args)), Safety::Mutating, "{args:?}");
            let request = json!({"args": args}).to_string();
            assert!(parse_call(TOOL_HELP, &request).is_err(), "{request}");
            assert!(matches!(
                classify(&Invocation::Help(split_args(&args.join(" ")))),
                Safety::Blocked(_)
            ));
        }
        for path in [
            "",
            "profile set",
            "store folder-share start",
            "daemon restart",
        ] {
            for flag in ["--help", "-h"] {
                let args = split_args(&format!("{path} {flag}"));
                assert_eq!(classify(&Invocation::Run(args)), Safety::ReadOnly);
            }
        }
        for path in [
            "profile set display_name",
            "store put filename",
            "--",
            "--help status",
        ] {
            assert!(parse_call(TOOL_HELP, &json!({"command": path}).to_string()).is_err());
        }
    }

    #[test]
    fn title_quoting() {
        assert_eq!(title(&run(&["store", "ls"])), "mistl store ls");
        assert_eq!(
            title(&run(&["ai", "chat", "a b"])),
            r#"mistl ai chat "a b""#
        );
        assert_eq!(
            title(&Invocation::Help(vec!["store".into()])),
            "mistl store --help"
        );
    }

    #[test]
    fn split_shell_words() {
        assert_eq!(
            split_args(r#"a "b c" 'd e' f\"g"#),
            vec!["a", "b c", "d e", "f\"g"]
        );
        assert_eq!(split_args("  x   y "), vec!["x", "y"]);
        assert_eq!(split_args(r#"a "" b"#), vec!["a", "", "b"]);
        assert_eq!(split_args(r"C:\tmp\x"), vec![r"C:\tmp\x"]);
        assert!(split_args("   ").is_empty());
    }

    #[test]
    fn ansi_and_truncation() {
        assert_eq!(
            strip_ansi("\x1b[31mred\x1b[0m ok \x1b]0;t\x07!"),
            "red ok !"
        );
        let s = "a".repeat(100);
        assert_eq!(truncate_middle(&s, 200), s);
        let t = truncate_middle(&"é".repeat(100), 50);
        assert!(t.contains("bytes truncated"));
        assert!(t.len() < 120);
    }
}
