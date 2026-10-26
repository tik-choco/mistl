//! System prompt and prompt-mode tool protocol.

use serde_json::json;

use crate::agent::mistl_tool::{TOOL_HELP, TOOL_MISTL, split_args};
use crate::agent::types::{FunctionCall, ToolCall, ToolMode, ToolOutput};

pub fn system_prompt(mode: ToolMode, catalog: &str, workspace: &str) -> String {
    let mut s = String::from(
        "You are mistl agent, a terminal assistant for the user's workspace and the tik-choco ecosystem.\n\
\n\
Rules:\n\
- Answer in the user's language.\n\
- Prefer read-only queries to inspect state before changing anything.\n\
- State-changing commands need the user's approval and may be declined; respect a refusal and do not retry the same command.\n\
- Never invent command output; base answers on actual results.\n\
- Keep answers short. Many commands print JSON; summarize it for humans instead of pasting it.\n\
- Never print or request secrets. Use a passphrase only if the user provides it.\n",
    );
    s.push_str("\nMistl: operate the local P2P node with `mistl` (args array, no leading mistl) and `mistl_help` (command path). Use help before guessing unfamiliar syntax.\nCommand overview (`mistl --help`):\n");
    s.push_str(catalog.trim());
    s.push_str("\nAI settings live in mistl: inspect `mistl config show` and subcommand help first. Mistl uses ai.providers (HTTP or mist-network://<room>, enabled/models/provide/shared), ai.default_ref = {provider_id, model}, ai.tts and ai.stt. ai.status reports rooms[]. Model ids on the network are raw ids. Do not create presets, set default_preset_id/advertised_models, or send temperature.\n");
    s.push('\n');
    s.push_str("\nWorkspace:\n");
    s.push_str(workspace.trim());
    s.push_str("\nUse the justfile to learn how the project builds and runs; prefer its recipes over raw commands.\n\
Use list_dir, find_files, and read_file to locate build outputs; find_files needs include_ignored: true for ignored directories such as target/.\n\
Tools: just(recipe, args, background), list_dir(path?), find_files(glob, include_ignored), grep(pattern, path?, glob?, case_insensitive), read_file(path, offset?, limit?), shell(command, background), process_list(), process_output(id, tail_bytes?), process_stop(id). File paths stay within the workspace.\n\
Run long-lived programs (servers, GUI/TUI apps) with background: true; inspect with process_output and stop with process_stop. stdin is null, so interactive programs may exit or misbehave.\n");
    match mode {
        ToolMode::Native => {
            s.push_str("\nUse the provided native tools when a tool is needed.\n");
        }
        ToolMode::Prompt => {
            s.push_str(
                "\nFor mistl, output a fenced block (shell-style quoting, no leading mistl):\n\
```mistl\n\
store ls\n\
```\n\
To read help for a subcommand:\n\
```mistl-help\n\
store folder-share\n\
```\n\
",
            );
            s.push_str("\nTool protocol: use a ```just block with one recipe per line, shell-style quoting, e.g.:\n\
```just\n\
build --release\n\
```\n\
For other tools (or just with background: true), use a ```tool block containing exactly one JSON object; it may span lines:\n\
```tool\n\
{\"name\":\"read_file\",\"arguments\":{\"path\":\"src/main.rs\"}}\n\
```\n\
After emitting blocks, STOP and wait for the next user message starting with [tool results]. When no tool is needed, answer normally.\n");
        }
    }
    s
}

/// Extract prompt-mode tool calls from a finished assistant message.
pub fn parse_prompt_calls(text: &str) -> Vec<ToolCall> {
    #[derive(PartialEq)]
    enum Kind {
        Mistl,
        Help,
        Just,
        Tool,
        Shell,
        Other,
    }
    let mut calls: Vec<ToolCall> = Vec::new();
    let mut kind: Option<Kind> = None;
    let mut body = String::new();

    let mut push = |name: &str, arguments: String| {
        calls.push(ToolCall {
            id: format!("p{}", calls.len() + 1),
            kind: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments,
            },
        });
    };

    for raw in text.lines() {
        let line = raw.trim();
        match &kind {
            None => {
                if let Some(info) = line.strip_prefix("```") {
                    let lang = info.trim().to_ascii_lowercase();
                    kind = Some(match lang.as_str() {
                        "mistl" => Kind::Mistl,
                        "mistl-help" => Kind::Help,
                        "just" => Kind::Just,
                        "tool" => Kind::Tool,
                        "sh" | "bash" | "shell" | "console" => Kind::Shell,
                        _ => Kind::Other,
                    });
                }
            }
            Some(k) => {
                if line.starts_with("```") {
                    if *k == Kind::Tool {
                        let (name, arguments) = json_call(&body);
                        push(&name, arguments);
                        body.clear();
                    }
                    kind = None;
                    continue;
                }
                if *k == Kind::Tool {
                    body.push_str(raw);
                    body.push('\n');
                    continue;
                }
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                match k {
                    Kind::Mistl => {
                        let l = line.strip_prefix("$ ").unwrap_or(line);
                        push(TOOL_MISTL, json!({ "args": split_args(l) }).to_string());
                    }
                    Kind::Help => {
                        push(TOOL_HELP, json!({ "command": line }).to_string());
                    }
                    Kind::Just => {
                        let mut words = split_args(line.strip_prefix("$ ").unwrap_or(line));
                        if !words.is_empty() {
                            let recipe = words.remove(0);
                            push("just", json!({"recipe": recipe, "args": words}).to_string());
                        }
                    }
                    Kind::Tool => unreachable!(),
                    Kind::Shell => {
                        let l = line.strip_prefix("$ ").unwrap_or(line);
                        if l.starts_with("mistl ") {
                            push(TOOL_MISTL, json!({ "args": split_args(l) }).to_string());
                        }
                    }
                    Kind::Other => {}
                }
            }
        }
    }
    if kind == Some(Kind::Tool) {
        // An unterminated JSON tool block is an error, never silently discarded.
        push(
            "invalid_tool",
            json!({"error": "unterminated tool block", "body": body}).to_string(),
        );
    }
    calls
}

fn json_call(body: &str) -> (String, String) {
    let parsed = serde_json::from_str::<serde_json::Value>(body);
    if let Ok(value) = &parsed
        && let Some(name) = value.get("name").and_then(|v| v.as_str())
        && let Some(arguments) = value.get("arguments").filter(|v| v.is_object())
    {
        return (name.into(), arguments.to_string());
    }
    let error = match parsed {
        Err(e) => format!("invalid tool block JSON: {e}"),
        Ok(_) => "tool block requires a name and an arguments object".into(),
    };
    (
        "invalid_tool".into(),
        json!({"error": error, "body": body}).to_string(),
    )
}

/// Build the user-role message that feeds prompt-mode results back. Each entry
/// is `(title, output)`.
pub fn format_prompt_results(results: &[(String, ToolOutput)]) -> String {
    let mut s = String::from("[tool results]\n");
    for (title, out) in results {
        s.push_str("$ ");
        s.push_str(title);
        s.push('\n');
        s.push_str(out.text.trim_end());
        s.push('\n');
        s.push_str(if out.ok {
            "status: ok\n"
        } else {
            "status: failed\n"
        });
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::mistl_tool::{Invocation, parse_call};

    fn inv(c: &ToolCall) -> Invocation {
        parse_call(&c.function.name, &c.function.arguments).unwrap()
    }

    #[test]
    fn parses_mistl_blocks() {
        let text = "Let me look.\n```mistl\nstore ls\n# comment\n\nai chat \"hi there\"\n```\nand\n```mistl-help\nstore folder-share\n```";
        let calls = parse_prompt_calls(text);
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].id, "p1");
        assert_eq!(calls[0].kind, "function");
        assert_eq!(
            inv(&calls[0]),
            Invocation::Run(vec!["store".into(), "ls".into()])
        );
        assert_eq!(
            inv(&calls[1]),
            Invocation::Run(vec!["ai".into(), "chat".into(), "hi there".into()])
        );
        assert_eq!(
            inv(&calls[2]),
            Invocation::Help(vec!["store".into(), "folder-share".into()])
        );
    }

    #[test]
    fn shell_blocks_only_when_mistl() {
        let text = "```bash\nls -la\n$ mistl status\nmistl ai models\n```\n```python\nmistl x\n```";
        let calls = parse_prompt_calls(text);
        assert_eq!(calls.len(), 2);
        assert_eq!(inv(&calls[0]), Invocation::Run(vec!["status".into()]));
        assert_eq!(
            inv(&calls[1]),
            Invocation::Run(vec!["ai".into(), "models".into()])
        );
    }

    #[test]
    fn no_blocks() {
        assert!(parse_prompt_calls("plain answer").is_empty());
    }

    #[test]
    fn results_format() {
        let r = vec![
            (
                "mistl store ls".to_string(),
                ToolOutput {
                    text: "exit code: 0\n".into(),
                    ok: true,
                },
            ),
            (
                "mistl x".to_string(),
                ToolOutput {
                    text: "boom".into(),
                    ok: false,
                },
            ),
        ];
        let s = format_prompt_results(&r);
        assert!(s.starts_with("[tool results]\n$ mistl store ls\nexit code: 0\nstatus: ok\n"));
        assert!(s.ends_with("$ mistl x\nboom\nstatus: failed\n"));
    }

    #[test]
    fn prompt_modes() {
        let p = system_prompt(ToolMode::Prompt, "CATALOG", "WORKSPACE");
        assert!(
            p.contains("```mistl-help") && p.contains("CATALOG") && p.contains("[tool results]")
        );
        let n = system_prompt(ToolMode::Native, "CATALOG", "WORKSPACE");
        assert!(!n.contains("```mistl") && n.contains("CATALOG"));
        assert!(p.contains("WORKSPACE") && p.contains("```tool"));
        assert!(p.contains("include_ignored: true") && p.contains("background: true"));
    }

    #[test]
    fn parses_just_and_multiline_json_in_order() {
        let text = "```just\nbuild --release\ndocs::serve \"two words\"\n```\n```tool\n{\n  \"name\": \"shell\",\n  \"arguments\": {\"command\": \"echo hi\", \"background\": true}\n}\n```\n```tool\n{\"name\":\"process_list\",\"arguments\":{}}\n```";
        let calls = parse_prompt_calls(text);
        assert_eq!(calls.len(), 4);
        let invs = calls
            .iter()
            .map(|call| {
                crate::agent::tools::parse_call(&call.function.name, &call.function.arguments)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(
            matches!(&invs[0], crate::agent::tools::Invocation::Just { recipe, args, background: false } if recipe == "build" && args == &["--release"])
        );
        assert!(
            matches!(&invs[1], crate::agent::tools::Invocation::Just { recipe, args, .. } if recipe == "docs::serve" && args == &["two words"])
        );
        assert!(
            matches!(&invs[2], crate::agent::tools::Invocation::Shell { command, background: true } if command == "echo hi")
        );
        assert_eq!(invs[3], crate::agent::tools::Invocation::ProcessList);
        assert_eq!(calls[3].id, "p4");
    }

    #[test]
    fn malformed_json_blocks_produce_errors() {
        for body in [
            "{bad",
            "",
            "[]",
            "{}",
            r#"{"name":"shell","arguments":"echo hi"}"#,
            r#"{"name":"process_list","arguments":{}} {"name":"process_list","arguments":{}}"#,
        ] {
            let calls = parse_prompt_calls(&format!("```tool\n{body}\n```"));
            assert_eq!(calls.len(), 1, "{body}");
            let err = crate::agent::tools::parse_call(
                &calls[0].function.name,
                &calls[0].function.arguments,
            )
            .unwrap_err();
            assert!(err.to_string().contains("tool block"));
        }
        let calls = parse_prompt_calls("```tool\n{\"name\":\"process_list\",\"arguments\":{}}");
        assert_eq!(calls.len(), 1);
        assert!(
            crate::agent::tools::parse_call(&calls[0].function.name, &calls[0].function.arguments)
                .unwrap_err()
                .to_string()
                .contains("unterminated")
        );
    }
}
