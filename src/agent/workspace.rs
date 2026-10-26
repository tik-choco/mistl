//! The startup directory as a project: justfile discovery and recipes, plus
//! read-only file tools confined to the workspace root.
//!
//! Everything here is synchronous; async callers use `spawn_blocking`.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use globset::{GlobBuilder, GlobMatcher};
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::agent::mistl_tool::truncate_middle;
use crate::agent::types::{RecipeInfo, WorkspaceSummary};

const MAX_OUTPUT_BYTES: usize = 12 * 1024;
const MAX_DUMP_BYTES: usize = 8 * 1024 * 1024;
const DUMP_TIMEOUT: Duration = Duration::from_secs(5);

/// Cap on the justfile text embedded in the system prompt.
pub const MAX_JUSTFILE_PROMPT_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    /// `name` or `name="default"`
    Singular,
    /// `+name` (one or more)
    Plus,
    /// `*name` (zero or more)
    Star,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Param {
    pub name: String,
    pub kind: ParamKind,
    /// Default value in justfile syntax, e.g. `""` or `"debug"`.
    pub default: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipe {
    /// Name as passed to `just` (`build`, or `docs::serve` for module recipes).
    pub name: String,
    pub doc: Option<String>,
    pub params: Vec<Param>,
    /// `[private]` or `_`-prefixed; hidden from the model and the UI.
    pub private: bool,
}

#[derive(Debug, Clone)]
pub struct Justfile {
    pub path: PathBuf,
    /// Directory `just` runs recipes in (the justfile's directory).
    pub dir: PathBuf,
    pub recipes: Vec<Recipe>,
    /// The recipe `just` runs with no arguments.
    pub default_recipe: Option<String>,
    /// Justfile text (capped at `MAX_JUSTFILE_PROMPT_BYTES`, cut marked).
    pub source: String,
    /// Why recipes could not be loaded (`just` missing, parse error); the
    /// justfile was still found.
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Workspace {
    /// Canonical startup directory; file tools are confined to it.
    pub root: PathBuf,
    pub justfile: Option<Justfile>,
    /// `just` executable used for recipes.
    pub just_bin: String,
}

impl Workspace {
    /// Inspect `cwd`: find a justfile (in `cwd` or its ancestors, like `just`
    /// does) and load its recipes via `just --dump --dump-format json`.
    /// Never fails: problems end up in `Justfile::error`.
    pub fn detect(cwd: &Path, just_bin: &str) -> Workspace {
        let root = cwd.canonicalize().unwrap_or_else(|_| cwd.to_path_buf());
        let justfile = find_justfile(&root).map(|path| {
            let dir = path.parent().unwrap_or(&root).to_path_buf();
            let mut justfile = Justfile {
                path,
                dir,
                recipes: Vec::new(),
                default_recipe: None,
                source: String::new(),
                error: None,
            };
            let loaded = (|| -> Result<()> {
                let mut bytes = Vec::new();
                File::open(&justfile.path)
                    .context("cannot read justfile")?
                    .take((MAX_JUSTFILE_PROMPT_BYTES + 1) as u64)
                    .read_to_end(&mut bytes)
                    .context("cannot read justfile")?;
                if bytes.len() > MAX_JUSTFILE_PROMPT_BYTES {
                    // Lossy decoding also handles a multibyte character cut at the cap.
                    let text = String::from_utf8_lossy(&bytes[..MAX_JUSTFILE_PROMPT_BYTES]);
                    justfile.source = capped(&text, MAX_JUSTFILE_PROMPT_BYTES - 40);
                    justfile.source.push_str("\n… [justfile text truncated]\n");
                } else {
                    justfile.source = String::from_utf8(bytes).context("justfile is not UTF-8")?;
                }
                let dump = dump_justfile(just_bin, &justfile.path, &justfile.dir)?;
                (justfile.recipes, justfile.default_recipe) = parse_dump(&dump)?;
                Ok(())
            })();
            if let Err(error) = loaded {
                justfile.error = Some(short_line(&format!("{error:#}")));
            }
            justfile
        });
        Workspace {
            root,
            justfile,
            just_bin: just_bin.to_string(),
        }
    }

    /// For the UI.
    pub fn summary(&self) -> WorkspaceSummary {
        WorkspaceSummary {
            root: display_path(&self.root),
            justfile: self
                .justfile
                .as_ref()
                .map(|j| relative_path(&self.root, &j.path)),
            recipes: self.justfile.as_ref().map_or_else(Vec::new, |j| {
                j.recipes
                    .iter()
                    .filter(|r| !r.private)
                    .map(|r| RecipeInfo {
                        name: r.name.clone(),
                        params: parameter_signature(&r.params),
                        doc: r.doc.clone(),
                    })
                    .collect()
            }),
            error: self.justfile.as_ref().and_then(|j| j.error.clone()),
        }
    }

    /// System-prompt section describing the workspace: root, justfile path,
    /// recipe list with params/docs, and the (capped) justfile text.
    pub fn prompt_section(&self) -> String {
        let mut text = format!("## Workspace\nRoot: {}\n", display_path(&self.root));
        let Some(justfile) = &self.justfile else {
            text.push_str("No justfile found in the workspace or its ancestors.\n");
            return text;
        };
        text.push_str(&format!(
            "Justfile: {}\n",
            relative_path(&self.root, &justfile.path)
        ));
        if let Some(error) = &justfile.error {
            text.push_str(&format!("Recipes could not be loaded: {error}\n"));
        }
        for recipe in self.summary().recipes {
            text.push_str(&format!("- {}", recipe.name));
            if !recipe.params.is_empty() {
                text.push_str(&format!(" {}", recipe.params));
            }
            if let Some(doc) = recipe.doc {
                text.push_str(&format!(" — {}", short_line(&doc)));
            }
            text.push('\n');
        }
        if let Some(default) = &justfile.default_recipe {
            text.push_str(&format!("Default recipe: {default}\n"));
        }
        // Choose a fence that cannot be closed by the justfile's contents.
        let fence = "`".repeat(
            justfile
                .source
                .split(|c| c != '`')
                .map(str::len)
                .max()
                .unwrap_or(0)
                .max(2)
                + 1,
        );
        text.push_str(&format!("\n{fence}just\n{}\n{fence}\n", justfile.source));
        text
    }

    /// Whether `name` is a public recipe of the loaded justfile.
    pub fn has_recipe(&self, name: &str) -> bool {
        self.justfile
            .as_ref()
            .is_some_and(|j| j.recipes.iter().any(|r| !r.private && r.name == name))
    }
}

/// `justfile` / `Justfile` / `.justfile` (any case) in `start` or an ancestor.
pub fn find_justfile(start: &Path) -> Option<PathBuf> {
    for directory in start.ancestors() {
        let Ok(entries) = fs::read_dir(directory) else {
            continue;
        };
        let mut candidates: Vec<_> = entries
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                (name.eq_ignore_ascii_case("justfile") || name.eq_ignore_ascii_case(".justfile"))
                    && entry.path().is_file()
            })
            .map(|entry| entry.path())
            .collect();
        // Prefer the unprefixed spelling if both accepted names are present.
        candidates.sort_by_key(|p| {
            (
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with('.')),
                p.clone(),
            )
        });
        if let Some(path) = candidates.into_iter().next() {
            return Some(path);
        }
    }
    None
}

/// Parse `just --dump --dump-format json`: (recipes in source order,
/// modules flattened as `mod::recipe`; default recipe).
pub fn parse_dump(json: &str) -> Result<(Vec<Recipe>, Option<String>)> {
    let dump: Value = serde_json::from_str(json).context("invalid just dump JSON")?;
    let mut recipes = Vec::new();
    parse_module(&dump, "", &mut recipes)?;
    let default = dump.get("first").and_then(Value::as_str).map(str::to_owned);
    Ok((recipes, default))
}

// just 1.x dumps have no source positions ("priors" counts dependencies, not
// lines). Use each module's first recipe, then name order; modules follow by name.
fn parse_module(dump: &Value, prefix: &str, output: &mut Vec<Recipe>) -> Result<()> {
    let recipes = dump
        .get("recipes")
        .and_then(Value::as_object)
        .context("just dump is missing a recipes map")?;
    let first = dump.get("first").and_then(Value::as_str);
    let mut names: Vec<_> = recipes.keys().collect();
    names.sort_by_key(|name| (Some(name.as_str()) != first, *name));
    for name in names {
        let value = &recipes[name];
        if !value.is_object() {
            bail!("invalid recipe {prefix}{name}")
        }
        let mut params = Vec::new();
        if let Some(parameters) = value.get("parameters") {
            for parameter in parameters
                .as_array()
                .context("recipe parameters must be an array")?
            {
                let name = parameter
                    .get("name")
                    .and_then(Value::as_str)
                    .context("recipe parameter is missing a name")?
                    .to_string();
                let kind = match parameter.get("kind").and_then(Value::as_str) {
                    Some("singular") => ParamKind::Singular,
                    Some("plus") => ParamKind::Plus,
                    Some("star") => ParamKind::Star,
                    _ => bail!("unknown parameter kind for {name}"),
                };
                let default = parameter
                    .get("default")
                    .filter(|v| !v.is_null())
                    .map(render_expression);
                params.push(Param {
                    name,
                    kind,
                    default,
                });
            }
        }
        let private = value
            .get("private")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || name.starts_with('_')
            || value
                .get("attributes")
                .and_then(Value::as_array)
                .is_some_and(|attrs| attrs.iter().any(|a| a.as_str() == Some("private")));
        output.push(Recipe {
            name: format!("{prefix}{name}"),
            doc: value.get("doc").and_then(Value::as_str).map(str::to_owned),
            params,
            private,
        });
    }
    if let Some(modules) = dump.get("modules") {
        let modules = modules
            .as_object()
            .context("just dump modules must be a map")?;
        let mut names: Vec<_> = modules.keys().collect();
        names.sort();
        for name in names {
            parse_module(&modules[name], &format!("{prefix}{name}::"), output)?;
        }
    }
    Ok(())
}

fn render_expression(value: &Value) -> String {
    if let Some(literal) = value.as_str() {
        // JSON's quoting is also valid for ordinary just double-quoted strings.
        return serde_json::to_string(literal).unwrap_or_else(|_| "…".into());
    }
    if let Some(tree) = value.as_array() {
        match tree.first().and_then(Value::as_str) {
            Some("variable") => {
                return tree
                    .get(1)
                    .and_then(Value::as_str)
                    .unwrap_or("…")
                    .to_string();
            }
            Some("concatenate") if tree.len() == 3 => {
                return format!(
                    "({} + {})",
                    render_expression(&tree[1]),
                    render_expression(&tree[2])
                );
            }
            _ => {}
        }
    }
    "…".into()
}

fn parameter_signature(params: &[Param]) -> String {
    params
        .iter()
        .map(|param| {
            let prefix = match param.kind {
                ParamKind::Singular => "",
                ParamKind::Plus => "+",
                ParamKind::Star => "*",
            };
            let default = param
                .default
                .as_ref()
                .map_or_else(String::new, |d| format!("={d}"));
            format!("{prefix}{}{default}", param.name)
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn dump_justfile(just_bin: &str, path: &Path, dir: &Path) -> Result<String> {
    let mut command = Command::new(just_bin);
    command
        .arg("--justfile")
        .arg(path)
        .arg("--working-directory")
        .arg(dir)
        .args(["--dump", "--dump-format", "json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let deadline = Instant::now() + DUMP_TIMEOUT;
    let mut child = command
        .spawn()
        .context("cannot start just (is it installed?)")?;
    let (sender, receiver) = mpsc::channel();
    fn drain(
        reader: impl Read + Send + 'static,
        is_stdout: bool,
        sender: mpsc::Sender<(bool, std::io::Result<Vec<u8>>)>,
    ) {
        thread::spawn(move || {
            let mut bytes = Vec::new();
            let result = reader
                .take((MAX_DUMP_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
                .map(|_| bytes);
            let _ = sender.send((is_stdout, result));
        });
    }
    if let Some(stdout) = child.stdout.take() {
        drain(stdout, true, sender.clone());
    }
    if let Some(stderr) = child.stderr.take() {
        drain(stderr, false, sender);
    }
    let result = (|| -> Result<String> {
        let status = loop {
            if let Some(status) = child.try_wait().context("cannot wait for just")? {
                break status;
            }
            if Instant::now() >= deadline {
                bail!("just dump timed out after 5 seconds")
            }
            thread::sleep(Duration::from_millis(20));
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        for _ in 0..2 {
            let (is_stdout, bytes) = receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .context("just dump output timed out")?;
            let bytes = bytes.context("cannot read just dump output")?;
            if bytes.len() > MAX_DUMP_BYTES {
                bail!("just dump output exceeds 8 MiB")
            }
            if is_stdout {
                stdout = bytes;
            } else {
                stderr = bytes;
            }
        }
        if !status.success() {
            bail!(
                "just dump failed ({status}): {}",
                short_line(&String::from_utf8_lossy(&stderr))
            )
        }
        String::from_utf8(stdout).context("just dump output is not UTF-8")
    })();
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    result
}

// ------------------------------------------------------------ file tools
//
// Paths may be relative to `root` or absolute; anything resolving outside
// `root` (after canonicalization, so `..` and symlinks cannot escape) is an
// error. Results are plain text for the model, capped to ~12 KB.

/// List a directory (default: root), including
/// gitignored ones such as build output, marking those `(ignored)`;
/// directories end with `/`, files show their size.
/// Skips `.git` and escaping symlinks; displays at most 500 entries.
/// Returns `cancelled` if cancellation is requested during the walk.
pub fn list_dir(root: &Path, path: Option<&str>, cancel: &CancellationToken) -> Result<String> {
    check_cancel(cancel)?;
    let (root, directory) = confined(root, path.unwrap_or("."))?;
    if !directory.is_dir() {
        bail!("path is not a directory")
    }
    let mut visible = HashSet::new();
    for entry in walker(&directory, false).max_depth(Some(1)).build() {
        check_cancel(cancel)?;
        if let Ok(entry) = entry {
            visible.insert(entry.path().to_path_buf());
        }
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(&directory).context("cannot list directory")? {
        check_cancel(cancel)?;
        let entry = entry.context("cannot read directory entry")?;
        if entry
            .file_name()
            .to_string_lossy()
            .eq_ignore_ascii_case(".git")
        {
            continue;
        }
        let path = entry.path();
        // A listing must not inspect the target of an escaping symlink.
        let Ok(target) = resolve(&root, &path) else {
            continue;
        };
        let metadata = fs::metadata(target).context("cannot inspect directory entry")?;
        let is_dir = metadata.is_dir();
        let ignored = if visible.contains(&path) {
            ""
        } else {
            " (ignored)"
        };
        let name = relative_path(&root, &path);
        let line = if is_dir {
            format!("{name}/{ignored}")
        } else {
            format!("{name}  {}{ignored}", human_size(metadata.len()))
        };
        entries.push((!is_dir, name, line));
    }
    entries.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let total = entries.len();
    let mut lines: Vec<_> = entries
        .into_iter()
        .take(500)
        .map(|(_, _, line)| line)
        .collect();
    if total > 500 {
        lines.push(format!("… {} more entries", total - 500));
    }
    if lines.is_empty() {
        lines.push("(empty directory)".into());
    }
    check_cancel(cancel)?;
    Ok(capped(&lines.join("\n"), MAX_OUTPUT_BYTES))
}

/// Files whose root-relative path matches `glob` (e.g. `**/*.rs`,
/// `target/**/*.exe`). Honors .gitignore unless `include_ignored`.
/// Counts all matches, displays at most 300, and always skips `.git`.
/// Returns `cancelled` if cancellation is requested during the walk.
pub fn find_files(
    root: &Path,
    glob: &str,
    include_ignored: bool,
    cancel: &CancellationToken,
) -> Result<String> {
    check_cancel(cancel)?;
    let (root, _) = confined(root, ".")?;
    let matcher = FileGlob::new(glob)?;
    let mut paths = Vec::new();
    let mut total = 0;
    for entry in walker(&root, include_ignored).build() {
        check_cancel(cancel)?;
        let entry = entry.context("cannot walk workspace")?;
        if !is_confined_file(&root, entry.path()) {
            continue;
        }
        let path = relative_path(&root, entry.path());
        if matcher.matches(&path) {
            total += 1;
            if paths.len() < 300 {
                paths.push(path);
            }
        }
    }
    paths.sort();
    if total > paths.len() {
        paths.push(format!("… {} more files", total - paths.len()));
    }
    let mut text = format!("{total} files matched\n");
    text.push_str(&paths.join("\n"));
    check_cancel(cancel)?;
    Ok(capped(&text, MAX_OUTPUT_BYTES))
}

/// Regex search over text files under `path` (default: root), optionally
/// filtered by `glob`; `path:line: text` lines. Honors .gitignore; skips
/// binary and files over 1 MiB. Reads are bounded even if a file grows.
/// Counts all matching lines and displays at most 200, cut to 300 characters.
/// Returns `cancelled` if cancellation is requested during walks or reads.
pub fn grep(
    root: &Path,
    pattern: &str,
    path: Option<&str>,
    glob: Option<&str>,
    case_insensitive: bool,
    cancel: &CancellationToken,
) -> Result<String> {
    check_cancel(cancel)?;
    let (root, target) = confined(root, path.unwrap_or("."))?;
    let regex = RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .context("invalid regex")?;
    let glob = glob.map(FileGlob::new).transpose()?;
    let mut lines = Vec::new();
    let mut total = 0;
    // Walk from root so an explicitly requested ignored file/directory is not
    // exempted as a walk root. Prune unrelated branches before descending.
    let scope = target.clone();
    let mut search = walker(&root, false);
    search.filter_entry(move |entry| {
        !entry
            .file_name()
            .to_string_lossy()
            .eq_ignore_ascii_case(".git")
            && (entry.path().starts_with(&scope) || scope.starts_with(entry.path()))
    });
    for entry in search.build() {
        check_cancel(cancel)?;
        let entry = entry.context("cannot walk search path")?;
        if !is_confined_file(&root, entry.path()) {
            continue;
        }
        let name = relative_path(&root, entry.path());
        if glob.as_ref().is_some_and(|g| !g.matches(&name)) {
            continue;
        }
        if fs::metadata(entry.path())
            .context("cannot inspect search file")?
            .len()
            > 1024 * 1024
        {
            continue;
        }
        // Bound the read as well: a file can grow after the metadata check.
        let mut reader = File::open(entry.path())
            .context("cannot open search file")?
            .take(1024 * 1024 + 1);
        let mut bytes = Vec::with_capacity(1024 * 1024 + 1);
        let mut chunk = [0; 8192];
        loop {
            check_cancel(cancel)?;
            let count = reader.read(&mut chunk).context("cannot read search file")?;
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..count]);
        }
        if bytes.len() > 1024 * 1024 || bytes[..bytes.len().min(8192)].contains(&0) {
            continue;
        }
        let Ok(text) = std::str::from_utf8(&bytes) else {
            continue;
        };
        for (index, line) in text.lines().enumerate() {
            check_cancel(cancel)?;
            if regex.is_match(line) {
                total += 1;
                if lines.len() < 200 {
                    lines.push(format!("{name}:{}: {}", index + 1, short_line(line)));
                }
            }
        }
    }
    if total > lines.len() {
        lines.push(format!("… {} more matching lines", total - lines.len()));
    }
    let mut text = format!("{total} matching lines\n");
    text.push_str(&lines.join("\n"));
    check_cancel(cancel)?;
    Ok(capped(&text, MAX_OUTPUT_BYTES))
}

/// Numbered lines `offset..offset+limit` (1-based offset; defaults 1 and
/// 400, limit capped at 2000) of a UTF-8 text file, with the total line count.
/// Keeps at most 301 characters per line and displays 300 plus a cut marker;
/// the rest is validated and counted using bounded chunks.
/// Returns `cancelled` if cancellation is requested during reads.
pub fn read_file(
    root: &Path,
    path: &str,
    offset: Option<usize>,
    limit: Option<usize>,
    cancel: &CancellationToken,
) -> Result<String> {
    check_cancel(cancel)?;
    let (root, target) = confined(root, path)?;
    if !target.is_file() {
        bail!("path is not a file")
    }
    let offset = offset.unwrap_or(1).max(1);
    let limit = limit.unwrap_or(400).min(2000);
    let mut reader = BufReader::new(File::open(&target).context("cannot open file")?);
    if reader.fill_buf().context("cannot read file")?.contains(&0) {
        bail!("cannot read binary file")
    }
    let mut lines = Vec::new();
    let mut total = 0;
    while let Some(line) = read_display_line(&mut reader, cancel)? {
        total += 1;
        if total >= offset && total - offset < limit {
            lines.push(format!("{total}: {line}"));
        }
    }
    let end = offset
        .saturating_add(lines.len())
        .saturating_sub(1)
        .min(total);
    let mut text = format!("{}: {total} total lines\n", relative_path(&root, &target));
    text.push_str(&lines.join("\n"));
    if end < total {
        text.push_str(&format!(
            "\nRead more with read_file offset={} limit={limit}.",
            end + 1
        ));
    } else {
        text.push_str("\n(end of file)");
    }
    if text.len() > MAX_OUTPUT_BYTES {
        text.push_str(
            "\nOutput truncated; use read_file with a smaller limit to read all selected lines.",
        );
    }
    check_cancel(cancel)?;
    Ok(capped(&text, MAX_OUTPUT_BYTES))
}

fn check_cancel(cancel: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() {
        bail!("cancelled")
    }
    Ok(())
}

// Validate every byte, including the discarded part of a long line. Pending
// UTF-8 bytes span buffer boundaries; only the display prefix is retained.
fn read_display_line(
    reader: &mut BufReader<File>,
    cancel: &CancellationToken,
) -> Result<Option<String>> {
    let mut line = String::new();
    let mut pending = Vec::with_capacity(reader.capacity() + 3);
    let mut captured = 0;
    let mut overflow = false;
    let mut any_bytes = false;
    loop {
        check_cancel(cancel)?;
        let chunk = reader.fill_buf().context("cannot read text file")?;
        if chunk.is_empty() {
            if !pending.is_empty() {
                bail!("cannot read text file (binary or invalid UTF-8)")
            }
            return Ok(any_bytes.then(|| short_line(&line)));
        }
        let newline = chunk.iter().position(|&byte| byte == b'\n');
        let content_len = newline.unwrap_or(chunk.len());
        let consumed = content_len + usize::from(newline.is_some());
        if chunk[..content_len].contains(&0) {
            bail!("cannot read binary file")
        }
        any_bytes = true;
        pending.extend_from_slice(&chunk[..content_len]);
        let valid_len = match std::str::from_utf8(&pending) {
            Ok(_) => pending.len(),
            Err(error) if error.error_len().is_none() && newline.is_none() => error.valid_up_to(),
            Err(_) => bail!("cannot read text file (binary or invalid UTF-8)"),
        };
        let text = std::str::from_utf8(&pending[..valid_len])?;
        let mut chars = text.chars();
        for character in chars.by_ref().take(301 - captured) {
            line.push(character);
            captured += 1;
        }
        overflow |= chars.next().is_some();
        pending.drain(..valid_len);
        reader.consume(consumed);
        if newline.is_some() {
            // Match BufRead::lines(): remove CR only when followed by LF.
            if !overflow && line.ends_with('\r') {
                line.pop();
            }
            return Ok(Some(short_line(&line)));
        }
    }
}

fn confined(root: &Path, path: &str) -> Result<(PathBuf, PathBuf)> {
    let root = root
        .canonicalize()
        .context("workspace root does not exist or cannot be accessed")?;
    let target = resolve(&root, &root.join(path))?;
    Ok((root, target))
}

fn resolve(root: &Path, target: &Path) -> Result<PathBuf> {
    let target = target
        .canonicalize()
        .context("path does not exist or cannot be accessed")?;
    if !target.starts_with(root) {
        bail!("path is outside the workspace root")
    }
    Ok(target)
}

fn is_confined_file(root: &Path, path: &Path) -> bool {
    resolve(root, path).is_ok_and(|p| p.is_file())
}

fn walker(path: &Path, include_ignored: bool) -> WalkBuilder {
    let mut builder = WalkBuilder::new(path);
    builder
        .hidden(false)
        .require_git(false)
        .follow_links(false)
        .ignore(!include_ignored)
        .git_ignore(!include_ignored)
        .git_exclude(!include_ignored)
        .git_global(false)
        .sort_by_file_name(|a, b| a.cmp(b))
        .filter_entry(|entry| {
            !entry
                .file_name()
                .to_string_lossy()
                .eq_ignore_ascii_case(".git")
        });
    builder
}

struct FileGlob {
    matcher: GlobMatcher,
    filename_only: bool,
}

impl FileGlob {
    fn new(pattern: &str) -> Result<Self> {
        Ok(Self {
            matcher: GlobBuilder::new(pattern)
                .literal_separator(true)
                .build()
                .context("invalid glob")?
                .compile_matcher(),
            filename_only: !pattern.contains('/'),
        })
    }

    fn matches(&self, path: &str) -> bool {
        self.matcher.is_match(path)
            || (self.filename_only
                && self
                    .matcher
                    .is_match(path.rsplit('/').next().unwrap_or(path)))
    }
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn relative_path(root: &Path, path: &Path) -> String {
    display_path(path.strip_prefix(root).unwrap_or(path))
}

fn human_size(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    if bytes < 1024 * 1024 {
        return format!("{:.1} KiB", bytes as f64 / 1024.0);
    }
    if bytes < 1024 * 1024 * 1024 {
        return format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0));
    }
    format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

fn short_line(text: &str) -> String {
    let mut chars = text
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c });
    let mut line: String = chars.by_ref().take(300).collect();
    if chars.next().is_some() {
        line.push('…');
    }
    line
}

fn capped(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    // Reserve enough space for truncate_middle's byte-count marker.
    truncate_middle(text, max.saturating_sub(100))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "mistl-agent-workspace-{}-{stamp}-{}",
                std::process::id(),
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }

        fn write(&self, name: &str, text: impl AsRef<[u8]>) {
            let path = self.0.join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, text).unwrap();
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn discovery_checks_ancestors_case_and_nearest_file() {
        let temp = TempDir::new();
        temp.write("JuStFiLe", "default:\n");
        fs::create_dir_all(temp.0.join("child/deep")).unwrap();
        assert_eq!(
            find_justfile(&temp.0.join("child/deep")),
            Some(temp.0.join("JuStFiLe"))
        );
        temp.write("child/.JUSTFILE", "near:\n");
        assert_eq!(
            find_justfile(&temp.0.join("child/deep")),
            Some(temp.0.join("child/.JUSTFILE"))
        );
        // A directory named justfile is not a candidate.
        fs::create_dir(temp.0.join("child/deep/justfile")).unwrap();
        assert_eq!(
            find_justfile(&temp.0.join("child/deep")),
            Some(temp.0.join("child/.JUSTFILE"))
        );
    }

    fn fixture() -> String {
        serde_json::json!({
            "first": "zdefault",
            "recipes": {
                "alpha": {"doc": "Build things", "parameters": [
                    {"name": "filter", "kind": "singular", "default": "a\"b\\c"},
                    {"name": "files", "kind": "plus", "default": null},
                    {"name": "args", "kind": "star", "default": null}
                ]},
                "zdefault": {"parameters": []},
                "_hidden": {"parameters": []},
                "secret": {"private": true, "parameters": []},
                "attribute": {"attributes": ["private"], "parameters": []}
            },
            "modules": {"docs": {
                "first": "serve", "recipes": {
                    "build": {"parameters": [{"name": "mode", "kind": "singular", "default": ["variable", "profile"]}]},
                    "serve": {"doc": "Serve docs", "parameters": []}
                }, "modules": {"nested": {"first": "x", "recipes": {"x": {"parameters": []}}, "modules": {}}}
            }}
        }).to_string()
    }

    #[test]
    fn dump_parses_modules_privacy_defaults_and_signatures() {
        let (recipes, default) = parse_dump(&fixture()).unwrap();
        assert_eq!(default.as_deref(), Some("zdefault"));
        assert_eq!(
            recipes.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            [
                "zdefault",
                "_hidden",
                "alpha",
                "attribute",
                "secret",
                "docs::serve",
                "docs::build",
                "docs::nested::x"
            ]
        );
        let alpha = recipes.iter().find(|r| r.name == "alpha").unwrap();
        assert_eq!(
            parameter_signature(&alpha.params),
            "filter=\"a\\\"b\\\\c\" +files *args"
        );
        assert_eq!(alpha.doc.as_deref(), Some("Build things"));
        assert_eq!(recipes.iter().filter(|r| r.private).count(), 3);
        assert_eq!(
            recipes
                .iter()
                .find(|r| r.name == "docs::build")
                .unwrap()
                .params[0]
                .default
                .as_deref(),
            Some("profile")
        );
        assert_eq!(
            render_expression(&serde_json::json!(["unknown", "tree"])),
            "…"
        );
        assert_eq!(
            render_expression(&serde_json::json!(["concatenate", "a", "b"])),
            "(\"a\" + \"b\")"
        );
    }

    #[test]
    fn dump_rejects_malformed_data() {
        for dump in [
            "not json",
            "{}",
            r#"{"recipes":[],"modules":{}}"#,
            r#"{"recipes":{"a":{"parameters":[{"name":"x","kind":"bad"}]}}}"#,
            r#"{"recipes":{"a":null}}"#,
        ] {
            assert!(parse_dump(dump).is_err(), "accepted {dump}");
        }
    }

    #[test]
    fn summary_and_prompt_expose_public_recipes_and_source() {
        let temp = TempDir::new();
        let (recipes, default_recipe) = parse_dump(&fixture()).unwrap();
        let workspace = Workspace {
            root: temp.0.clone(),
            just_bin: "just".into(),
            justfile: Some(Justfile {
                path: temp.0.join("justfile"),
                dir: temp.0.clone(),
                recipes,
                default_recipe,
                source: "# ```\nzdefault:\n".into(),
                error: None,
            }),
        };
        let summary = workspace.summary();
        assert_eq!(summary.justfile.as_deref(), Some("justfile"));
        assert_eq!(summary.recipes.len(), 5);
        assert_eq!(
            summary.recipes[1].params,
            "filter=\"a\\\"b\\\\c\" +files *args"
        );
        assert!(workspace.has_recipe("docs::serve"));
        assert!(!workspace.has_recipe("_hidden"));
        assert!(!workspace.has_recipe("secret"));
        assert!(!workspace.has_recipe("missing"));
        let prompt = workspace.prompt_section();
        assert!(prompt.contains("Default recipe: zdefault"));
        assert!(prompt.contains("- docs::serve — Serve docs"));
        assert!(prompt.contains("````just"));
        assert!(!prompt.contains("- secret"));
        let workspace = Workspace {
            justfile: None,
            ..workspace
        };
        assert!(workspace.prompt_section().contains("No justfile"));
        assert!(workspace.summary().recipes.is_empty());
    }

    #[test]
    fn missing_just_keeps_source_and_reports_error_with_cap() {
        let temp = TempDir::new();
        temp.write("justfile", "# あ\n".repeat(MAX_JUSTFILE_PROMPT_BYTES));
        let workspace = Workspace::detect(&temp.0, "mistl-agent-no-such-just-executable");
        let justfile = workspace.justfile.as_ref().unwrap();
        assert!(justfile.source.len() <= MAX_JUSTFILE_PROMPT_BYTES);
        assert!(justfile.source.contains("truncated"));
        assert!(
            justfile
                .error
                .as_ref()
                .unwrap()
                .contains("cannot start just")
        );
        assert!(
            workspace
                .prompt_section()
                .contains("Recipes could not be loaded")
        );
        assert!(workspace.summary().recipes.is_empty());
    }

    #[test]
    fn confinement_rejects_parent_absolute_outside_and_missing_paths() {
        let temp = TempDir::new();
        temp.write("root/inside.txt", "safe");
        temp.write("outside.txt", "outside");
        let root = temp.0.join("root");
        assert!(
            read_file(
                &root,
                "../outside.txt",
                None,
                None,
                &CancellationToken::new()
            )
            .unwrap_err()
            .to_string()
            .contains("outside")
        );
        assert!(
            read_file(
                &root,
                &temp.0.join("outside.txt").to_string_lossy(),
                None,
                None,
                &CancellationToken::new()
            )
            .is_err()
        );
        assert!(list_dir(&root, Some(".."), &CancellationToken::new()).is_err());
        assert!(
            grep(
                &root,
                "outside",
                Some(".."),
                None,
                false,
                &CancellationToken::new()
            )
            .is_err()
        );
        assert!(
            read_file(&root, "missing", None, None, &CancellationToken::new())
                .unwrap_err()
                .to_string()
                .contains("does not exist")
        );
        assert!(
            read_file(
                &root,
                "../root/inside.txt",
                None,
                None,
                &CancellationToken::new()
            )
            .is_ok()
        );
        assert!(
            read_file(
                &root,
                &root.join("inside.txt").to_string_lossy(),
                None,
                None,
                &CancellationToken::new()
            )
            .is_ok()
        );
    }

    #[test]
    fn confinement_skips_escaping_symlinks() {
        let temp = TempDir::new();
        temp.write("root/inside.txt", "inside");
        temp.write("outside.txt", "outside");
        let link = temp.0.join("root/link.txt");
        #[cfg(unix)]
        let result = std::os::unix::fs::symlink(temp.0.join("outside.txt"), &link);
        #[cfg(windows)]
        let result = std::os::windows::fs::symlink_file(temp.0.join("outside.txt"), &link);
        // Windows may require Developer Mode or elevated symlink privileges.
        if result.is_err() {
            return;
        }
        let root = temp.0.join("root");
        assert!(read_file(&root, "link.txt", None, None, &CancellationToken::new()).is_err());
        assert!(
            !find_files(&root, "*.txt", true, &CancellationToken::new())
                .unwrap()
                .contains("link.txt")
        );
        assert!(
            !grep(
                &root,
                "outside",
                None,
                None,
                false,
                &CancellationToken::new()
            )
            .unwrap()
            .contains("outside.txt")
        );
    }

    fn file_fixture() -> TempDir {
        let temp = TempDir::new();
        temp.write(".gitignore", "target/\n*.log\n");
        temp.write("src/main.rs", "first\nHello World\nhello again\nlast\n");
        temp.write("target/app.exe", "build output");
        temp.write("app.exe", "top-level executable");
        temp.write("error.log", "hello ignored");
        temp.write(".hidden", "hello hidden");
        temp.write(".git/config", "hello git metadata");
        temp.write("binary.dat", b"hello\0binary");
        temp
    }

    #[test]
    fn list_marks_ignored_entries_without_git_and_sorts_dirs_first() {
        let temp = file_fixture();
        let listing = list_dir(&temp.0, None, &CancellationToken::new()).unwrap();
        assert!(
            listing.starts_with("src/\ntarget/ (ignored)\n"),
            "{listing}"
        );
        assert!(listing.contains("error.log  13 B (ignored)"));
        assert!(listing.contains(".hidden"));
        assert!(!listing.contains(".git/"));
        assert!(!listing.contains(".git ("));
        assert!(
            list_dir(&temp.0, Some("target"), &CancellationToken::new())
                .unwrap()
                .contains("target/app.exe")
        );
        assert!(list_dir(&temp.0, Some("src/main.rs"), &CancellationToken::new()).is_err());
    }

    #[test]
    fn find_respects_ignore_and_matches_basenames_anywhere() {
        let temp = file_fixture();
        let normal = find_files(&temp.0, "*.exe", false, &CancellationToken::new()).unwrap();
        assert!(normal.contains("1 files matched\napp.exe"));
        assert!(!normal.contains("target/app.exe"));
        let all = find_files(&temp.0, "*.exe", true, &CancellationToken::new()).unwrap();
        assert!(all.contains("2 files matched\napp.exe\ntarget/app.exe"));
        assert!(
            find_files(&temp.0, "**/*.rs", false, &CancellationToken::new())
                .unwrap()
                .contains("src/main.rs")
        );
        assert!(
            !find_files(&temp.0, "*", true, &CancellationToken::new())
                .unwrap()
                .contains(".git/config")
        );
        assert!(find_files(&temp.0, "[", false, &CancellationToken::new()).is_err());
        assert!(
            find_files(&temp.0, "nothing", false, &CancellationToken::new())
                .unwrap()
                .starts_with("0 files matched")
        );
    }

    #[test]
    fn grep_filters_case_binary_large_and_ignored_files() {
        let temp = file_fixture();
        temp.write("large.txt", "hello".repeat(220_000));
        let output = grep(
            &temp.0,
            "hello",
            None,
            Some("*.rs"),
            true,
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(output.contains("2 matching lines"));
        assert!(output.contains("src/main.rs:2: Hello World"));
        assert!(output.contains("src/main.rs:3: hello again"));
        let sensitive = grep(
            &temp.0,
            "hello",
            Some("src/main.rs"),
            None,
            false,
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(sensitive.starts_with("1 matching lines"));
        let all = grep(
            &temp.0,
            "hello",
            None,
            None,
            true,
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(all.contains(".hidden:1:"));
        for skipped in ["error.log:", "binary.dat:", "large.txt:", ".git/config:"] {
            assert!(!all.contains(skipped));
        }
        assert!(
            grep(
                &temp.0,
                "hello",
                Some("error.log"),
                None,
                false,
                &CancellationToken::new()
            )
            .unwrap()
            .starts_with("0 matching lines")
        );
        assert!(
            grep(
                &temp.0,
                "build",
                Some("target"),
                None,
                false,
                &CancellationToken::new()
            )
            .unwrap()
            .starts_with("0 matching lines")
        );
        assert!(grep(&temp.0, "[", None, None, false, &CancellationToken::new()).is_err());
    }

    #[test]
    fn read_numbers_offset_limit_total_and_refuses_binary() {
        let temp = file_fixture();
        let output = read_file(
            &temp.0,
            "src/main.rs",
            Some(2),
            Some(2),
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(output.starts_with("src/main.rs: 4 total lines\n2: Hello World\n3: hello again"));
        assert!(output.contains("offset=4 limit=2"));
        assert!(!output.contains("1: first"));
        assert!(
            read_file(&temp.0, "binary.dat", None, None, &CancellationToken::new())
                .unwrap_err()
                .to_string()
                .contains("binary")
        );
        assert!(read_file(&temp.0, "src", None, None, &CancellationToken::new()).is_err());
        assert!(
            read_file(
                &temp.0,
                "src/main.rs",
                None,
                None,
                &CancellationToken::new()
            )
            .unwrap()
            .contains("4: last\n(end of file)")
        );
        assert!(
            read_file(
                &temp.0,
                "src/main.rs",
                Some(100),
                Some(10),
                &CancellationToken::new()
            )
            .unwrap()
            .contains("(end of file)")
        );
        temp.write("empty", "");
        assert!(
            read_file(&temp.0, "empty", None, None, &CancellationToken::new())
                .unwrap()
                .contains("0 total lines")
        );
    }

    #[test]
    fn file_results_are_bounded_and_unicode_lines_are_cut_safely() {
        let temp = TempDir::new();
        temp.write("text", format!("{}\n", "界".repeat(500)).repeat(2100));
        let output =
            read_file(&temp.0, "text", None, Some(9999), &CancellationToken::new()).unwrap();
        assert!(output.len() <= MAX_OUTPUT_BYTES);
        assert!(output.contains("truncated"));
        assert!(output.contains("smaller limit"));
        assert!(output.contains("2100 total lines"));
        assert_eq!(short_line(&"界".repeat(500)).chars().count(), 301);
        let output = grep(&temp.0, "界", None, None, false, &CancellationToken::new()).unwrap();
        // This file is over 1 MiB and must be skipped.
        assert!(output.starts_with("0 matching lines"));
        temp.write("many", "hello\n".repeat(250));
        let output = grep(
            &temp.0,
            "hello",
            None,
            None,
            false,
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(output.contains("250 matching lines"));
        assert!(output.contains("50 more matching lines"));
    }

    #[test]
    fn huge_single_line_is_cut_with_bounded_reads() {
        use std::io::Write;

        let temp = TempDir::new();
        let mut file = File::create(temp.0.join("huge.txt")).unwrap();
        let chunk = [b'x'; 8192];
        for _ in 0..8192 {
            file.write_all(&chunk).unwrap();
        }
        drop(file);
        let start = Instant::now();
        let output = read_file(
            &temp.0,
            "huge.txt",
            None,
            Some(1),
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(start.elapsed() < Duration::from_secs(10));
        assert!(output.contains("1 total lines"));
        assert!(output.contains(&format!("1: {}…", "x".repeat(300))));
        assert!(output.ends_with("(end of file)"));
        assert!(output.len() < 400);
    }

    #[test]
    fn bounded_lines_validate_discarded_bytes_and_buffer_boundaries() {
        let temp = TempDir::new();
        let cancel = CancellationToken::new();
        // Split a UTF-8 character across the 8192-byte reader boundary, then
        // count following lines even though only the first is displayed.
        temp.write("split.txt", format!("{}界\r\nsecond\n", "x".repeat(8191)));
        let output = read_file(&temp.0, "split.txt", None, Some(1), &cancel).unwrap();
        assert!(output.contains("2 total lines"));
        assert!(output.contains(&format!("1: {}…", "x".repeat(300))));
        assert!(output.contains("offset=2 limit=1"));
        temp.write(
            "crlf.txt",
            format!("{}\r\n{}\r", "界".repeat(300), "x".repeat(300)),
        );
        let output = read_file(&temp.0, "crlf.txt", None, None, &cancel).unwrap();
        assert!(output.contains(&format!("1: {}\n2: {}…", "界".repeat(300), "x".repeat(300))));

        for tail in [b"\0".as_slice(), b"\xff", b"\xe7", b"\xe7\n"] {
            let mut bytes = vec![b'x'; 8191];
            bytes.extend_from_slice(tail);
            temp.write("invalid.txt", bytes);
            let error = read_file(&temp.0, "invalid.txt", None, Some(1), &cancel).unwrap_err();
            assert!(error.to_string().contains("binary"));
        }
    }

    #[test]
    fn pre_cancelled_file_tools_return_cancelled() {
        let temp = file_fixture();
        let cancel = CancellationToken::new();
        cancel.cancel();
        for result in [
            list_dir(&temp.0, None, &cancel),
            find_files(&temp.0, "*.rs", false, &cancel),
            grep(&temp.0, "hello", None, None, false, &cancel),
            read_file(&temp.0, "src/main.rs", None, None, &cancel),
        ] {
            assert_eq!(result.unwrap_err().to_string(), "cancelled");
        }
    }

    #[test]
    fn detects_recipes_with_real_just_when_available() {
        let mut command = Command::new("just");
        command
            .arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x0800_0000);
        }
        if !command.status().is_ok_and(|s| s.success()) {
            return;
        }
        let temp = TempDir::new();
        temp.write("justfile", "# List recipes\nzdefault:\n    @echo default\n\n# Run checks\ncheck filter=\"\" *args:\n    @echo check\n\n_private:\n    @echo private\n");
        let workspace = Workspace::detect(&temp.0, "just");
        let justfile = workspace.justfile.as_ref().unwrap();
        assert!(justfile.error.is_none(), "{:?}", justfile.error);
        assert_eq!(justfile.default_recipe.as_deref(), Some("zdefault"));
        assert_eq!(
            workspace
                .summary()
                .recipes
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>(),
            ["zdefault", "check"]
        );
        assert_eq!(workspace.summary().recipes[1].params, "filter=\"\" *args");
        assert!(!workspace.has_recipe("_private"));
        temp.write("justfile", "invalid recipe syntax\n");
        let failed = Workspace::detect(&temp.0, "just");
        let error = failed.justfile.unwrap().error.unwrap();
        assert!(error.contains("just dump failed"), "{error}");
    }
}
