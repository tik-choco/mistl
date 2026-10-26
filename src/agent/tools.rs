//! Tool registry, safety policy, and workspace command dispatch.

use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::agent::config::Config;
use crate::agent::mistl_tool::{self, MistlRunner, split_args};
use crate::agent::process::{self, CommandSpec, ProcessManager};
use crate::agent::types::{FunctionSpec, Safety, ToolOutput, ToolSpec};
use crate::agent::workspace::{self, Workspace};

/// One token-query sanitizer for tool results. Keep at most a partial query
/// key across chunks; suppress values as they arrive, even without newlines.
#[derive(Default)]
pub(super) struct OutputRedactor {
    prefix: String,
    value: bool,
    masked: bool,
}

impl OutputRedactor {
    pub(super) fn push(&mut self, text: &str) -> String {
        let mut out = String::new();
        for c in text.chars() {
            if self.value {
                if !c.is_whitespace() && !matches!(c, '&' | '\"' | '\'' | '<' | '>') {
                    if !self.masked {
                        out.push_str("<redacted>");
                        self.masked = true;
                    }
                    continue;
                }
                self.value = false;
            }
            self.prefix.push(c);
            while !self.prefix.is_empty()
                && !["?token=", "&token="]
                    .iter()
                    .any(|key| key.starts_with(&self.prefix))
            {
                out.push(self.prefix.remove(0));
            }
            if matches!(self.prefix.as_str(), "?token=" | "&token=") {
                out.push_str(&self.prefix);
                self.prefix.clear();
                self.value = true;
                self.masked = false;
            }
        }
        out
    }

    pub(super) fn finish(&mut self) -> String {
        self.value = false;
        std::mem::take(&mut self.prefix)
    }
}

pub(super) fn redact_output(text: &str) -> String {
    let mut redactor = OutputRedactor::default();
    let mut text = redactor.push(text);
    text.push_str(&redactor.finish());
    text
}

fn sanitize_output(mut output: ToolOutput) -> ToolOutput {
    output.text = redact_output(&output.text);
    output
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    Mistl(mistl_tool::Invocation),
    Just {
        recipe: String,
        args: Vec<String>,
        background: bool,
    },
    ListDir {
        path: Option<String>,
    },
    FindFiles {
        glob: String,
        include_ignored: bool,
    },
    Grep {
        pattern: String,
        path: Option<String>,
        glob: Option<String>,
        case_insensitive: bool,
    },
    ReadFile {
        path: String,
        offset: Option<usize>,
        limit: Option<usize>,
    },
    Shell {
        command: String,
        background: bool,
    },
    ProcessList,
    ProcessOutput {
        id: u32,
        tail_bytes: Option<usize>,
    },
    ProcessStop {
        id: u32,
    },
}

pub fn specs(workspace: &Workspace) -> Vec<ToolSpec> {
    let mut tools = mistl_tool::tool_specs();
    let string = json!({"type": "string"});
    let boolean = json!({"type": "boolean", "default": false});
    let integer = json!({"type": "integer", "minimum": 1});
    let mut add = |name, description: &str, properties, required| {
        tools.push(ToolSpec {
            kind: "function",
            function: FunctionSpec {
                name,
                description: description.into(),
                parameters: json!({"type": "object", "properties": properties, "required": required}),
            },
        });
    };
    if public_recipes(workspace).next().is_some() {
        add(
            "just",
            "Run a loaded justfile recipe. Requires approval. Use background for long-lived apps.",
            json!({"recipe": string, "args": {"type": "array", "items": string}, "background": boolean}),
            json!(["recipe"]),
        );
    }
    add(
        "list_dir",
        "List a workspace directory, including ignored entries.",
        json!({"path": string}),
        json!([]),
    );
    add(
        "find_files",
        "Find workspace files by glob; enable include_ignored for build outputs such as target/.",
        json!({"glob": string, "include_ignored": boolean}),
        json!(["glob"]),
    );
    add(
        "grep",
        "Search workspace text files with a regular expression.",
        json!({"pattern": string, "path": string, "glob": string, "case_insensitive": boolean}),
        json!(["pattern"]),
    );
    add(
        "read_file",
        "Read numbered lines of a workspace text file (offset defaults to 1, limit to 400).",
        json!({"path": string, "offset": integer, "limit": integer}),
        json!(["path"]),
    );
    add(
        "shell",
        "Run a shell command in the workspace. Requires approval. Use background for long-lived apps.",
        json!({"command": string, "background": boolean}),
        json!(["command"]),
    );
    add(
        "process_list",
        "List background processes started in this session.",
        json!({}),
        json!([]),
    );
    add(
        "process_output",
        "Inspect a background process status and recent output (default tail: 4096 bytes).",
        json!({"id": integer, "tail_bytes": {"type": "integer", "minimum": 0}}),
        json!(["id"]),
    );
    add(
        "process_stop",
        "Stop a background process tree. Requires approval.",
        json!({"id": integer}),
        json!(["id"]),
    );
    tools
}

fn public_recipes(workspace: &Workspace) -> impl Iterator<Item = &str> {
    workspace
        .justfile
        .iter()
        .filter(|j| j.error.is_none())
        .flat_map(|j| &j.recipes)
        .filter(|r| !r.private)
        .map(|r| r.name.as_str())
}

pub fn parse_call(name: &str, arguments: &str) -> Result<Invocation> {
    if matches!(name, mistl_tool::TOOL_MISTL | mistl_tool::TOOL_HELP) {
        return mistl_tool::parse_call(name, arguments).map(Invocation::Mistl);
    }
    let value: Value = if arguments.trim().is_empty() {
        json!({})
    } else {
        serde_json::from_str(arguments).map_err(|e| anyhow!("invalid tool arguments JSON: {e}"))?
    };
    if !value.is_object() {
        bail!("tool arguments must be a JSON object");
    }
    let required = |key| -> Result<String> {
        optional_string(&value, key)?
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("{key} is required and must not be empty"))
    };
    let id = || -> Result<u32> {
        let n = optional_int(&value, "id")?
            .filter(|n| *n > 0)
            .ok_or_else(|| anyhow!("id must be a positive integer"))?;
        u32::try_from(n).map_err(|_| anyhow!("id is too large"))
    };
    Ok(match name {
        "just" => Invocation::Just {
            recipe: required("recipe")?,
            args: words(value.get("args"))?,
            background: flag(&value, "background")?,
        },
        "list_dir" => Invocation::ListDir {
            path: optional_string(&value, "path")?,
        },
        "find_files" => Invocation::FindFiles {
            glob: required("glob")?,
            include_ignored: flag(&value, "include_ignored")?,
        },
        "grep" => Invocation::Grep {
            pattern: required("pattern")?,
            path: optional_string(&value, "path")?,
            glob: optional_string(&value, "glob")?,
            case_insensitive: flag(&value, "case_insensitive")?,
        },
        "read_file" => {
            let offset = optional_int(&value, "offset")?;
            if offset == Some(0) {
                bail!("offset must be at least 1");
            }
            Invocation::ReadFile {
                path: required("path")?,
                offset,
                limit: optional_int(&value, "limit")?,
            }
        }
        "shell" => Invocation::Shell {
            command: required("command")?,
            background: flag(&value, "background")?,
        },
        "process_list" => Invocation::ProcessList,
        "process_output" => Invocation::ProcessOutput {
            id: id()?,
            tail_bytes: optional_int(&value, "tail_bytes")?,
        },
        "process_stop" => Invocation::ProcessStop { id: id()? },
        "invalid_tool" => bail!("{}", required("error")?),
        _ => bail!("unknown tool: {name}"),
    })
}

fn optional_string(value: &Value, key: &str) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(Value::Number(n)) => Ok(Some(n.to_string())),
        _ => bail!("{key} must be a string"),
    }
}

fn optional_int(value: &Value, key: &str) -> Result<Option<usize>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| anyhow!("{key} must be a nonnegative integer")),
        Some(Value::String(s)) => s
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| anyhow!("{key} must be a nonnegative integer")),
        _ => bail!("{key} must be a nonnegative integer"),
    }
}

fn flag(value: &Value, key: &str) -> Result<bool> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        _ => bail!("{key} must be a boolean"),
    }
}

fn words(value: Option<&Value>) -> Result<Vec<String>> {
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(split_args(s)),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| match v {
                Value::String(s) => Ok(s.clone()),
                Value::Number(n) => Ok(n.to_string()),
                Value::Bool(b) => Ok(b.to_string()),
                _ => bail!("args must contain strings, numbers, or booleans"),
            })
            .collect(),
        _ => bail!("args must be an array or a shell-split string"),
    }
}

pub fn classify(inv: &Invocation, workspace: &Workspace) -> Safety {
    match inv {
        Invocation::Mistl(inv) => mistl_tool::classify(inv),
        Invocation::Just { recipe, .. }
            if !public_recipes(workspace).any(|name| name == recipe) =>
        {
            Safety::Blocked(unknown_recipe(workspace, recipe))
        }
        Invocation::Just { .. } | Invocation::Shell { .. } | Invocation::ProcessStop { .. } => {
            Safety::Mutating
        }
        _ => Safety::ReadOnly,
    }
}

fn unknown_recipe(workspace: &Workspace, recipe: &str) -> String {
    let recipes = public_recipes(workspace).collect::<Vec<_>>().join(", ");
    format!(
        "unknown just recipe {recipe:?}; available recipes: {}",
        if recipes.is_empty() {
            "(none)"
        } else {
            &recipes
        }
    )
}

pub fn approval_reason(inv: &Invocation) -> &'static str {
    match inv {
        Invocation::Just { .. } => "runs a justfile recipe",
        Invocation::Shell { .. } => "runs a shell command",
        Invocation::ProcessStop { .. } => "stops a background process",
        _ => "changes state",
    }
}

fn quote(word: &str) -> String {
    // Reuse mistl's quoting so it round-trips through the shared splitter.
    mistl_tool::title(&mistl_tool::Invocation::Run(vec![word.into()]))[6..].to_string()
}

pub fn title(inv: &Invocation) -> String {
    redact_output(&match inv {
        Invocation::Mistl(inv) => mistl_tool::title(inv),
        Invocation::Just {
            recipe,
            args,
            background,
        } => {
            let mut title = format!("just {}", quote(recipe));
            for arg in args {
                title.push(' ');
                title.push_str(&quote(arg));
            }
            if *background {
                title.push_str(" &");
            }
            title
        }
        Invocation::ListDir { path } => format!("list_dir {}", path.as_deref().unwrap_or(".")),
        Invocation::FindFiles { glob, .. } => format!("find_files {glob}"),
        Invocation::Grep { pattern, path, .. } => {
            format!("grep {} {}", json!(pattern), path.as_deref().unwrap_or("."))
        }
        Invocation::ReadFile {
            path,
            offset,
            limit,
        } => {
            let start = offset.unwrap_or(1);
            format!(
                "read_file {path}:{start}-{}",
                start.saturating_add(limit.unwrap_or(400)).saturating_sub(1)
            )
        }
        Invocation::Shell {
            command,
            background,
        } => format!("$ {command}{}", if *background { " &" } else { "" }),
        Invocation::ProcessList => "process_list".into(),
        Invocation::ProcessOutput { id, .. } => format!("process_output {id}"),
        Invocation::ProcessStop { id } => format!("process_stop {id}"),
    })
}

/// A prompt-mode fence and body preserving all invocation options.
pub fn prompt_render(inv: &Invocation) -> (&'static str, String) {
    // Line-based fences hold one call per line, so words containing a line
    // break must travel in the JSON `tool` fence instead.
    let multiline = |words: &[String]| words.iter().any(|w| w.contains(['\n', '\r']));
    let (name, arguments) = match inv {
        Invocation::Mistl(mistl_tool::Invocation::Run(args)) if multiline(args) => {
            (mistl_tool::TOOL_MISTL, json!({"args": args}))
        }
        Invocation::Just {
            recipe,
            args,
            background,
        } if recipe.contains(['\n', '\r']) || multiline(args) => (
            "just",
            json!({"recipe": recipe, "args": args, "background": background}),
        ),
        Invocation::Mistl(mistl_tool::Invocation::Run(args)) => {
            return (
                "mistl",
                args.iter().map(|a| quote(a)).collect::<Vec<_>>().join(" "),
            );
        }
        Invocation::Mistl(mistl_tool::Invocation::Help(path)) => {
            return (
                "mistl-help",
                path.iter().map(|a| quote(a)).collect::<Vec<_>>().join(" "),
            );
        }
        Invocation::Just {
            recipe,
            args,
            background: false,
        } => {
            return (
                "just",
                std::iter::once(recipe)
                    .chain(args)
                    .map(|a| quote(a))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }
        Invocation::Just {
            recipe,
            args,
            background,
        } => (
            "just",
            json!({"recipe": recipe, "args": args, "background": background}),
        ),
        Invocation::ListDir { path } => ("list_dir", json!({"path": path})),
        Invocation::FindFiles {
            glob,
            include_ignored,
        } => (
            "find_files",
            json!({"glob": glob, "include_ignored": include_ignored}),
        ),
        Invocation::Grep {
            pattern,
            path,
            glob,
            case_insensitive,
        } => (
            "grep",
            json!({"pattern": pattern, "path": path, "glob": glob, "case_insensitive": case_insensitive}),
        ),
        Invocation::ReadFile {
            path,
            offset,
            limit,
        } => (
            "read_file",
            json!({"path": path, "offset": offset, "limit": limit}),
        ),
        Invocation::Shell {
            command,
            background,
        } => (
            "shell",
            json!({"command": command, "background": background}),
        ),
        Invocation::ProcessList => ("process_list", json!({})),
        Invocation::ProcessOutput { id, tail_bytes } => (
            "process_output",
            json!({"id": id, "tail_bytes": tail_bytes}),
        ),
        Invocation::ProcessStop { id } => ("process_stop", json!({"id": id})),
    };
    (
        "tool",
        json!({"name": name, "arguments": arguments}).to_string(),
    )
}

pub struct Toolbox {
    pub mistl: MistlRunner,
    pub workspace: Workspace,
    pub processes: ProcessManager,
    command_timeout: Duration,
}

impl Toolbox {
    pub fn new(cfg: &Config, workspace: Workspace) -> Self {
        Self {
            mistl: MistlRunner::new(cfg),
            workspace,
            processes: ProcessManager::new(),
            command_timeout: Duration::from_secs(cfg.command_timeout_secs.max(1)),
        }
    }

    pub async fn run(
        &self,
        inv: &Invocation,
        token: &CancellationToken,
        on_output: &mut (dyn FnMut(&str) + Send),
    ) -> ToolOutput {
        sanitize_output(self.run_inner(inv, token, on_output).await)
    }

    /// User-entered commands bypass approvals and timeouts, but share the
    /// same output sanitizer as model tools (including live process output).
    pub async fn run_user_shell(
        &self,
        command: &str,
        token: &CancellationToken,
        on_output: &mut (dyn FnMut(&str) + Send),
    ) -> ToolOutput {
        let spec = process::shell_command(command, &self.workspace.root);
        sanitize_output(process::run(&spec, None, token, on_output).await)
    }

    async fn run_inner(
        &self,
        inv: &Invocation,
        token: &CancellationToken,
        on_output: &mut (dyn FnMut(&str) + Send),
    ) -> ToolOutput {
        if token.is_cancelled() {
            return failure("cancelled");
        }
        match inv {
            Invocation::Mistl(inv) => self.mistl.run(inv, token, on_output).await,
            Invocation::Just {
                recipe,
                args,
                background,
            } => {
                if !self.workspace.has_recipe(recipe) {
                    return failure(unknown_recipe(&self.workspace, recipe));
                }
                if let Safety::Blocked(reason) = classify(inv, &self.workspace) {
                    return failure(reason);
                }
                let justfile = self
                    .workspace
                    .justfile
                    .as_ref()
                    .expect("validated recipe has a justfile");
                let spec = CommandSpec {
                    program: self.workspace.just_bin.clone(),
                    args: std::iter::once(recipe.clone())
                        .chain(args.clone())
                        .collect(),
                    cwd: justfile.dir.clone(),
                };
                self.run_command(spec, title(inv), *background, token, on_output)
                    .await
            }
            Invocation::Shell {
                command,
                background,
            } => {
                self.run_command(
                    process::shell_command(command, &self.workspace.root),
                    title(inv),
                    *background,
                    token,
                    on_output,
                )
                .await
            }
            Invocation::ProcessList => ToolOutput {
                ok: true,
                text: self.processes.list(),
            },
            Invocation::ProcessOutput { id, tail_bytes } => {
                output(self.processes.output(*id, *tail_bytes))
            }
            Invocation::ProcessStop { id } => output(self.processes.stop(*id).await),
            _ => {
                let root = self.workspace.root.clone();
                let inv = inv.clone();
                let cancel = token.clone();
                let task = tokio::task::spawn_blocking(move || match inv {
                    Invocation::ListDir { path } => {
                        workspace::list_dir(&root, path.as_deref(), &cancel)
                    }
                    Invocation::FindFiles {
                        glob,
                        include_ignored,
                    } => workspace::find_files(&root, &glob, include_ignored, &cancel),
                    Invocation::Grep {
                        pattern,
                        path,
                        glob,
                        case_insensitive,
                    } => workspace::grep(
                        &root,
                        &pattern,
                        path.as_deref(),
                        glob.as_deref(),
                        case_insensitive,
                        &cancel,
                    ),
                    Invocation::ReadFile {
                        path,
                        offset,
                        limit,
                    } => workspace::read_file(&root, &path, offset, limit, &cancel),
                    _ => unreachable!("only file tools dispatched here"),
                });
                tokio::select! {
                    _ = token.cancelled() => failure("cancelled"),
                    result = task => output(result.map_err(anyhow::Error::from).and_then(|r| r)),
                }
            }
        }
    }

    async fn run_command(
        &self,
        spec: CommandSpec,
        title: String,
        background: bool,
        token: &CancellationToken,
        on_output: &mut (dyn FnMut(&str) + Send),
    ) -> ToolOutput {
        if background {
            output(self.processes.start(spec, title.clone()).map(|id| format!("started process {id}: {title}; use process_output {id} to inspect it and process_stop {id} to stop it")))
        } else {
            process::run(&spec, Some(self.command_timeout), token, on_output).await
        }
    }
}

fn failure(error: impl std::fmt::Display) -> ToolOutput {
    ToolOutput {
        ok: false,
        text: format!("error: {error}"),
    }
}

fn output(result: Result<String>) -> ToolOutput {
    match result {
        Ok(text) => ToolOutput { ok: true, text },
        Err(e) => failure(format!("{e:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::workspace::{Justfile, Recipe};

    #[test]
    fn token_redaction_is_bounded_and_handles_every_chunk_boundary() {
        let input = "http://localhost/?token=secret&x=1 '&token=other' \"?token=quoted\" <?token=html> ?token=日本語\n?token=last";
        let expected = "http://localhost/?token=<redacted>&x=1 '&token=<redacted>' \"?token=<redacted>\" <?token=<redacted>> ?token=<redacted>\n?token=<redacted>";
        assert_eq!(redact_output(input), expected);
        assert_eq!(redact_output(expected), expected);
        for split in (0..=input.len()).filter(|i| input.is_char_boundary(*i)) {
            let mut stream = OutputRedactor::default();
            let mut output = stream.push(&input[..split]);
            output.push_str(&stream.push(&input[split..]));
            output.push_str(&stream.finish());
            assert_eq!(output, expected, "split {split}");
        }
        let mut stream = OutputRedactor::default();
        let mut output = String::new();
        for c in input.chars() {
            output.push_str(&stream.push(&c.to_string()));
        }
        output.push_str(&stream.finish());
        assert_eq!(output, expected);
        assert_eq!(
            redact_output("?token= &token=\" ?tok &token"),
            "?token= &token=\" ?tok &token"
        );
        let mut stream = OutputRedactor::default();
        assert_eq!(stream.push("?token="), "?token=");
        for _ in 0..1000 {
            let _ = stream.push(&"s".repeat(4096));
            assert!(stream.prefix.len() <= 6);
        }
        assert!(stream.finish().is_empty());
    }

    #[tokio::test]
    async fn all_tool_results_and_user_shell_output_are_redacted() {
        let toolbox = Toolbox::new(&Config::default(), workspace());
        let token = CancellationToken::new();
        let inv = Invocation::Just {
            recipe: "missing?token=secret".into(),
            args: vec![],
            background: false,
        };
        let result = toolbox.run(&inv, &token, &mut |_| {}).await;
        assert!(!result.text.contains("secret"));
        assert!(result.text.contains("?token=<redacted>"));
        #[cfg(windows)]
        let command = "[Console]::Out.Write('http://localhost/?tok'); Start-Sleep -Milliseconds 100; [Console]::Out.WriteLine('en=secret&ok=1'); [Console]::Error.WriteLine('?token=stderr-secret')";
        #[cfg(not(windows))]
        let command = "printf 'http://localhost/?tok'; sleep 0.1; printf 'en=secret&ok=1\\n'; printf '?token=stderr-secret\\n' >&2";
        let mut live = String::new();
        let output = toolbox
            .run_user_shell(command, &token, &mut |chunk| live.push_str(chunk))
            .await;
        assert!(output.ok, "{}", output.text);
        for text in [&output.text, &live] {
            assert!(!text.contains("secret"), "{text}");
            assert!(text.contains("?token=<redacted>&ok=1"));
        }
        live.clear();
        let output = toolbox
            .run(
                &Invocation::Shell {
                    command: command.into(),
                    background: false,
                },
                &token,
                &mut |chunk| live.push_str(chunk),
            )
            .await;
        assert!(output.ok);
        for text in [&output.text, &live] {
            assert!(!text.contains("secret"));
            assert!(text.contains("?token=<redacted>&ok=1"));
        }
    }

    fn workspace() -> Workspace {
        Workspace {
            root: ".".into(),
            just_bin: "just".into(),
            justfile: Some(Justfile {
                path: "justfile".into(),
                dir: ".".into(),
                default_recipe: Some("build".into()),
                source: "build:\n    cargo build\n".into(),
                error: None,
                recipes: vec![
                    Recipe {
                        name: "build".into(),
                        doc: None,
                        params: vec![],
                        private: false,
                    },
                    Recipe {
                        name: "docs::serve".into(),
                        doc: None,
                        params: vec![],
                        private: false,
                    },
                    Recipe {
                        name: "_hidden".into(),
                        doc: None,
                        params: vec![],
                        private: true,
                    },
                ],
            }),
        }
    }

    #[test]
    fn registry_depends_on_available_tools() {
        let mut ws = workspace();
        let names = |ws: &Workspace| {
            specs(ws)
                .into_iter()
                .map(|s| s.function.name)
                .collect::<Vec<_>>()
        };
        let available = names(&ws);
        assert_eq!(
            available,
            [
                "mistl",
                "mistl_help",
                "just",
                "list_dir",
                "find_files",
                "grep",
                "read_file",
                "shell",
                "process_list",
                "process_output",
                "process_stop"
            ]
        );
        let with_mistl = names(&ws);
        assert!(with_mistl.contains(&"mistl") && with_mistl.contains(&"mistl_help"));
        ws.justfile.as_mut().unwrap().error = Some("recipe load failed".into());
        assert!(!names(&ws).contains(&"just"));
        ws.justfile.as_mut().unwrap().error = None;
        ws.justfile.as_mut().unwrap().recipes.retain(|r| r.private);
        assert!(!names(&ws).contains(&"just"));
        ws.justfile = None;
        assert_eq!(names(&ws).len(), 10);
    }

    #[test]
    fn lenient_arguments_and_defaults() {
        assert_eq!(
            parse_call("just", r#"{"recipe":"build","args":["--jobs",2,true]}"#).unwrap(),
            Invocation::Just {
                recipe: "build".into(),
                args: vec!["--jobs".into(), "2".into(), "true".into()],
                background: false
            }
        );
        assert_eq!(
            parse_call(
                "just",
                r#"{"recipe":"docs::serve","args":"\"two words\" '' C:\\tmp"}"#
            )
            .unwrap(),
            Invocation::Just {
                recipe: "docs::serve".into(),
                args: vec!["two words".into(), "".into(), "C:\\tmp".into()],
                background: false
            }
        );
        assert_eq!(
            parse_call("read_file", r#"{"path":123,"offset":"2","limit":30}"#).unwrap(),
            Invocation::ReadFile {
                path: "123".into(),
                offset: Some(2),
                limit: Some(30)
            }
        );
        assert_eq!(
            parse_call("process_output", r#"{"id":"3","tail_bytes":"0"}"#).unwrap(),
            Invocation::ProcessOutput {
                id: 3,
                tail_bytes: Some(0)
            }
        );
        assert_eq!(
            parse_call("list_dir", "").unwrap(),
            Invocation::ListDir { path: None }
        );
        assert_eq!(
            parse_call("process_list", "{}").unwrap(),
            Invocation::ProcessList
        );
        assert_eq!(
            parse_call("mistl", r#"{"args":"mistl status"}"#).unwrap(),
            Invocation::Mistl(mistl_tool::Invocation::Run(vec!["status".into()]))
        );
    }

    #[test]
    fn rejects_invalid_arguments() {
        for (name, args) in [
            ("shell", "{}"),
            ("shell", r#"{"command":" "}"#),
            ("shell", r#"{"command":"echo hi","background":"false"}"#),
            ("find_files", "{bad"),
            ("grep", "[]"),
            ("just", r#"{"recipe":"build","args":[{}]}"#),
            ("read_file", r#"{"path":"x","offset":0}"#),
            ("read_file", r#"{"path":"x","limit":-1}"#),
            ("read_file", r#"{"path":"x","offset":1.5}"#),
            ("process_stop", r#"{"id":0}"#),
            ("process_stop", r#"{"id":4294967296}"#),
            ("process_output", r#"{"id":"NaN"}"#),
            ("unknown", "{}"),
        ] {
            assert!(parse_call(name, args).is_err(), "{name}: {args}");
        }
    }

    #[test]
    fn safety_and_approval_reasons() {
        let ws = workspace();
        for (name, args, safety) in [
            ("mistl", r#"{"args":["status"]}"#, Safety::ReadOnly),
            ("mistl", r#"{"args":["daemon","start"]}"#, Safety::Mutating),
            ("just", r#"{"recipe":"build"}"#, Safety::Mutating),
            ("just", r#"{"recipe":"docs::serve"}"#, Safety::Mutating),
            ("list_dir", "{}", Safety::ReadOnly),
            (
                "find_files",
                r#"{"glob":"**/*.exe","include_ignored":true}"#,
                Safety::ReadOnly,
            ),
            ("grep", r#"{"pattern":"foo"}"#, Safety::ReadOnly),
            ("read_file", r#"{"path":"src/main.rs"}"#, Safety::ReadOnly),
            ("shell", r#"{"command":"echo hello"}"#, Safety::Mutating),
            ("process_list", "{}", Safety::ReadOnly),
            ("process_output", r#"{"id":1}"#, Safety::ReadOnly),
            ("process_stop", r#"{"id":1}"#, Safety::Mutating),
        ] {
            assert_eq!(
                classify(&parse_call(name, args).unwrap(), &ws),
                safety,
                "{name}"
            );
        }
        for recipe in ["missing", "_hidden"] {
            let inv = parse_call("just", &json!({"recipe": recipe}).to_string()).unwrap();
            assert!(matches!(classify(&inv, &ws), Safety::Blocked(message)
                if message.contains("available recipes: build, docs::serve") && !message.contains("recipes: _hidden")));
        }
        for (name, args, reason) in [
            ("just", r#"{"recipe":"build"}"#, "runs a justfile recipe"),
            ("shell", r#"{"command":"echo hi"}"#, "runs a shell command"),
            ("process_stop", r#"{"id":1}"#, "stops a background process"),
        ] {
            assert_eq!(approval_reason(&parse_call(name, args).unwrap()), reason);
        }
    }

    #[test]
    fn titles_and_prompt_round_trips() {
        for (name, args, expected) in [
            ("mistl", r#"{"args":["status"]}"#, "mistl status"),
            ("mistl_help", r#"{"command":"store"}"#, "mistl store --help"),
            (
                "just",
                r#"{"recipe":"build","args":["--release"]}"#,
                "just build --release",
            ),
            (
                "just",
                r#"{"recipe":"docs::serve","args":["a b","","x\"y","C:\\tmp\\a b"],"background":true}"#,
                "just docs::serve \"a b\" \"\" \"x\\\"y\" \"C:\\\\tmp\\\\a b\" &",
            ),
            ("list_dir", r#"{"path":"target"}"#, "list_dir target"),
            (
                "find_files",
                r#"{"glob":"**/*.exe","include_ignored":true}"#,
                "find_files **/*.exe",
            ),
            (
                "grep",
                r#"{"pattern":"foo","path":"src","glob":"**/*.rs","case_insensitive":true}"#,
                "grep \"foo\" src",
            ),
            (
                "read_file",
                r#"{"path":"src/main.rs"}"#,
                "read_file src/main.rs:1-400",
            ),
            (
                "read_file",
                r#"{"path":"src/main.rs","offset":4,"limit":2}"#,
                "read_file src/main.rs:4-5",
            ),
            (
                "shell",
                r#"{"command":"echo hi","background":true}"#,
                "$ echo hi &",
            ),
            ("process_list", "{}", "process_list"),
            (
                "process_output",
                r#"{"id":2,"tail_bytes":8192}"#,
                "process_output 2",
            ),
            ("process_stop", r#"{"id":2}"#, "process_stop 2"),
            (
                "just",
                r#"{"recipe":"ask","args":["first\nsecond"]}"#,
                "just ask \"first\nsecond\"",
            ),
            (
                "mistl",
                r#"{"args":["ai","chat","a\r\nb"]}"#,
                "mistl ai chat \"a\r\nb\"",
            ),
        ] {
            let inv = parse_call(name, args).unwrap();
            assert_eq!(title(&inv), expected);
            let (fence, body) = prompt_render(&inv);
            let rebuilt =
                crate::agent::prompt::parse_prompt_calls(&format!("```{fence}\n{body}\n```"));
            assert_eq!(rebuilt.len(), 1, "{name}");
            assert_eq!(
                parse_call(&rebuilt[0].function.name, &rebuilt[0].function.arguments).unwrap(),
                inv
            );
        }
    }
}
