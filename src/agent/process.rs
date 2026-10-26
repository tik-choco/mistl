//! Running project commands: foreground runs with live output, and
//! background processes (dev servers, the app itself) the agent can inspect
//! and stop.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::agent::mistl_tool::{strip_ansi, truncate_middle};
use crate::agent::tools::OutputRedactor;
use crate::agent::types::ToolOutput;
use crate::child_process::{self, ProcessTree};

/// Output cap handed to the model (head + tail kept), same as mistl's.
pub const MAX_OUTPUT_BYTES: usize = 12 * 1024;

/// A program invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
}

/// A shell command line run by the platform shell in `cwd`:
/// Windows `powershell.exe -NoLogo -NoProfile -NonInteractive -Command <line>`,
/// elsewhere `sh -c <line>`.
pub fn shell_command(line: &str, cwd: &Path) -> CommandSpec {
    #[cfg(windows)]
    let (program, args) = (
        "powershell.exe",
        vec!["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", line],
    );
    #[cfg(not(windows))]
    let (program, args) = ("sh", vec!["-c", line]);
    CommandSpec {
        program: program.into(),
        args: args.into_iter().map(String::from).collect(),
        cwd: cwd.into(),
    }
}

/// Run to completion. stdin is null; `NO_COLOR=1`; no console window on
/// Windows. Every stdout/stderr chunk is passed (ANSI-stripped) to
/// `on_output` as it arrives. On timeout or cancel the whole process tree is
/// killed. Like `MistlRunner::exec`, waits for process exit and then at most
/// 300 ms for the pipes (a spawned background child may keep them open).
///
/// The text has the same shape as mistl results: `exit code: N`, then
/// `--- stdout ---` / `--- stderr ---` sections, then a `[note]` line for
/// timeout/cancel, capped with `truncate_middle(MAX_OUTPUT_BYTES)`.
/// `timeout: None` waits until exit or cancel.
pub async fn run(
    spec: &CommandSpec,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
    on_output: &mut (dyn FnMut(&str) + Send),
) -> ToolOutput {
    let (child, tree) = match spawn(spec) {
        Ok(spawned) => spawned,
        Err(error) => {
            return ToolOutput {
                text: format!("error: {error:#}"),
                ok: false,
            };
        }
    };
    run_child(child, Some(tree), timeout, cancel, on_output).await
}

/// A mistl client can start a persistent daemon. Do not attach a tree guard:
/// spawn_daemon uses CREATE_NEW_PROCESS_GROUP, but does not break away from a
/// Windows job. Closing a KILL_ON_JOB_CLOSE job would kill that daemon too.
/// Keep kill_on_drop for the direct client and the same bounded pipe drain.
pub(super) async fn run_mistl(
    mut command: Command,
    timeout: Duration,
    cancel: &CancellationToken,
    on_output: &mut (dyn FnMut(&str) + Send),
) -> ToolOutput {
    match command.spawn() {
        Ok(child) => run_child(child, None, Some(timeout), cancel, on_output).await,
        Err(error) => ToolOutput {
            text: format!("failed to start mistl: {error}"),
            ok: false,
        },
    }
}

async fn run_child(
    child: Child,
    tree: Option<Arc<Mutex<ProcessTree>>>,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
    on_output: &mut (dyn FnMut(&str) + Send),
) -> ToolOutput {
    let mut stdout = Capture::default();
    let mut stderr = Capture::default();
    let end = drive(
        child,
        tree.as_deref(),
        timeout,
        cancel,
        &mut |is_stderr, text| {
            if is_stderr {
                stderr.append(text);
            } else {
                stdout.append(text);
            }
            on_output(text);
        },
        &mut |_| {},
    )
    .await;
    let exited = matches!(&end, End::Exit(_));
    let (code, mut note, mut ok) = match end {
        End::Exit(Ok(status)) => (exit_code(status), None, status.success()),
        End::Exit(Err(error)) => (
            "unknown".into(),
            Some(format!("wait failed: {error}")),
            false,
        ),
        End::Timeout(result) => (
            "none".into(),
            Some(format!(
                "timed out after {}s; {}",
                timeout.unwrap_or_default().as_secs(),
                kill_note(&result)
            )),
            false,
        ),
        End::Cancelled(result) => (
            "none".into(),
            Some(format!("cancelled by user; {}", kill_note(&result))),
            false,
        ),
    };
    // This guard also kills the tree if this future is dropped while awaiting drive.
    if exited
        && let Some(tree) = &tree
        && let Err(error) = lock(tree).kill()
    {
        ok = false;
        let failure = format!("process tree could not be killed: {error:#}");
        note = Some(note.map_or_else(|| failure.clone(), |note| format!("{note}; {failure}")));
    }
    let mut text = format!("exit code: {code}");
    for (name, capture) in [("stdout", stdout), ("stderr", stderr)] {
        let output = capture.text();
        if !output.trim().is_empty() {
            text.push_str(&format!("\n--- {name} ---\n{}", output.trim_end()));
        }
    }
    if let Some(note) = note {
        text.push_str(&format!("\n[{note}]"));
    }
    ToolOutput {
        text: truncate_middle(&text, MAX_OUTPUT_BYTES),
        ok,
    }
}

/// Background processes started by the agent in this session. Cheap to
/// clone (shared state). Dropping the last clone kills every process tree
/// still running.
#[derive(Clone, Default)]
pub struct ProcessManager {
    inner: Arc<Mutex<Inner>>,
}

impl ProcessManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn in the background; output goes to an in-memory log (last
    /// 256 KiB, combined stdout/stderr, ANSI-stripped). Returns the id
    /// (1, 2, ...).
    pub fn start(&self, spec: CommandSpec, title: String) -> Result<u32> {
        let runtime = tokio::runtime::Handle::try_current()
            .context("starting a process requires a Tokio runtime")?;
        let mut inner = lock(&self.inner);
        let id = inner
            .next_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("process ids exhausted"))?;
        let (child, tree) = spawn(&spec)?;
        let cancel = CancellationToken::new();
        inner.next_id = id;
        inner.processes.insert(
            id,
            Process {
                title,
                spec,
                tree: tree.clone(),
                started: Instant::now(),
                status: Status::Running,
                stop_error: None,
                descendants_killed: false,
                log: String::new(),
                cancel: cancel.clone(),
                task: None,
            },
        );
        let weak = Arc::downgrade(&self.inner);
        let exit_weak = weak.clone();
        let task = runtime.spawn(async move {
            drive(
                child,
                Some(&tree),
                None,
                &cancel,
                &mut |_, text| {
                    if let Some(inner) = weak.upgrade()
                        && let Some(process) = lock(&inner).processes.get_mut(&id)
                    {
                        append_tail(&mut process.log, text, LOG_BYTES);
                    }
                },
                &mut |end| {
                    if let Some(inner) = exit_weak.upgrade()
                        && let Some(process) = lock(&inner).processes.get_mut(&id)
                        && matches!(process.status, Status::Running)
                    {
                        process.status = match end {
                            End::Exit(Ok(status)) => Status::Exited(status.code()),
                            End::Exit(Err(error)) => {
                                append_tail(
                                    &mut process.log,
                                    &format!("\n[wait failed: {error}]\n"),
                                    LOG_BYTES,
                                );
                                Status::Exited(None)
                            }
                            End::Timeout(result) | End::Cancelled(result) => {
                                if let Err(error) = result {
                                    process.stop_error = Some(error.clone());
                                    Status::Running
                                } else {
                                    Status::Killed
                                }
                            }
                        };
                    }
                },
            )
            .await;
        });
        inner
            .processes
            .get_mut(&id)
            .expect("process just inserted")
            .task = Some(task);
        Ok(id)
    }

    /// One line per process: id, running / exited (code), uptime, title.
    pub fn list(&self) -> String {
        let inner = lock(&self.inner);
        if inner.processes.is_empty() {
            return "no background processes".into();
        }
        inner
            .processes
            .iter()
            .map(|(&id, process)| process.line(id))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Status line plus the last `tail_bytes` of the log (default 4 KiB).
    pub fn output(&self, id: u32, tail_bytes: Option<usize>) -> Result<String> {
        let inner = lock(&self.inner);
        let process = inner.get(id)?;
        Ok(format!(
            "{}\n{}",
            process.line(id),
            tail(&process.log, tail_bytes.unwrap_or(4096))
        ))
    }

    /// Kill the process tree; returns a status line with the last log lines.
    pub async fn stop(&self, id: u32) -> Result<String> {
        {
            let mut inner = lock(&self.inner);
            inner.get(id)?;
            inner
                .processes
                .get_mut(&id)
                .expect("process checked")
                .kill();
        }
        // Include the task's final pipe drain, without waiting on inherited pipes forever.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let finished = lock(&self.inner)
                .get(id)?
                .task
                .as_ref()
                .is_none_or(JoinHandle::is_finished);
            if finished || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let inner = lock(&self.inner);
        let process = inner.get(id)?;
        let lines = process.log.lines().rev().take(20).collect::<Vec<_>>();
        let output = format!(
            "{}\n{}",
            process.line(id),
            lines.into_iter().rev().collect::<Vec<_>>().join("\n")
        );
        if process.stop_error.is_some() {
            Err(anyhow!(output))
        } else {
            Ok(output)
        }
    }

    /// Kill every process tree, including descendants of exited parents (called on exit).
    pub fn stop_all(&self) {
        lock(&self.inner).kill_all();
    }
}

const READER_GRACE: Duration = Duration::from_millis(300);
const STREAM_BYTES: usize = 1024 * 1024;
const LOG_BYTES: usize = 256 * 1024;

fn spawn(spec: &CommandSpec) -> Result<(Child, Arc<Mutex<ProcessTree>>)> {
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("NO_COLOR", "1");
    child_process::prepare(&mut command);
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to start `{}`", spec.program))?;
    let tree = Arc::new(Mutex::new(ProcessTree::attach(&mut child)?));
    Ok((child, tree))
}

enum End {
    Exit(std::io::Result<ExitStatus>),
    Timeout(std::result::Result<bool, String>),
    Cancelled(std::result::Result<bool, String>),
}

fn kill_note(result: &std::result::Result<bool, String>) -> String {
    match result {
        Ok(_) => "process killed".into(),
        Err(error) => format!("process tree could not be killed: {error}"),
    }
}

struct Readers(Vec<JoinHandle<()>>);
impl Drop for Readers {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

async fn pump<R: AsyncRead + Unpin>(
    mut reader: R,
    stderr: bool,
    tx: mpsc::Sender<(bool, Vec<u8>)>,
) {
    let mut bytes = [0; 4096];
    loop {
        match reader.read(&mut bytes).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if tx.send((stderr, bytes[..n].to_vec())).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn drive(
    mut child: Child,
    tree: Option<&Mutex<ProcessTree>>,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
    on_output: &mut (dyn FnMut(bool, &str) + Send),
    on_exit: &mut (dyn FnMut(&End) + Send),
) -> End {
    let (tx, mut rx) = mpsc::channel(32);
    let mut readers = Readers(Vec::new());
    if let Some(stdout) = child.stdout.take() {
        readers
            .0
            .push(tokio::spawn(pump(stdout, false, tx.clone())));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.0.push(tokio::spawn(pump(stderr, true, tx.clone())));
    }
    drop(tx);
    let timer = async {
        match timeout {
            Some(duration) => tokio::time::sleep(duration).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(timer);
    let mut streams = [CleanStream::default(), CleanStream::default()];
    let mut open = true;
    let end = loop {
        tokio::select! {
            status = child.wait() => {
                break End::Exit(status);
            },
            _ = &mut timer => break End::Timeout(kill(&mut child, tree)),
            _ = cancel.cancelled() => break End::Cancelled(kill(&mut child, tree)),
            chunk = rx.recv(), if open => match chunk {
                Some((stderr, bytes)) => deliver(&mut streams, stderr, &bytes, on_output),
                None => open = false,
            },
        }
    };
    if !matches!(end, End::Exit(_)) {
        // Reap after a successful tree kill; never wait indefinitely on a failed kill.
        let _ = tokio::time::timeout(READER_GRACE, child.wait()).await;
    }
    on_exit(&end);
    let _ = tokio::time::timeout(READER_GRACE, async {
        while let Some((stderr, bytes)) = rx.recv().await {
            deliver(&mut streams, stderr, &bytes, on_output);
        }
    })
    .await;
    for (index, stream) in streams.iter_mut().enumerate() {
        let text = stream.finish();
        if !text.is_empty() {
            on_output(index == 1, &text);
        }
    }
    end
}

fn kill(child: &mut Child, tree: Option<&Mutex<ProcessTree>>) -> std::result::Result<bool, String> {
    match tree {
        Some(tree) => lock(tree).kill().map_err(|error| format!("{error:#}")),
        None => child
            .start_kill()
            .map(|()| true)
            .map_err(|error| error.to_string()),
    }
}

fn deliver(
    streams: &mut [CleanStream; 2],
    stderr: bool,
    bytes: &[u8],
    output: &mut (dyn FnMut(bool, &str) + Send),
) {
    let text = streams[usize::from(stderr)].push(bytes);
    if !text.is_empty() {
        output(stderr, &text);
    }
}

#[derive(Default)]
enum Escape {
    #[default]
    Text,
    Start,
    Csi,
    Osc,
    OscEnd,
}

#[derive(Default)]
struct CleanStream {
    utf8: Vec<u8>,
    escape: Escape,
    redactor: OutputRedactor,
}

impl CleanStream {
    fn push(&mut self, bytes: &[u8]) -> String {
        self.utf8.extend_from_slice(bytes);
        let mut decoded = String::new();
        let mut consumed = 0;
        while consumed < self.utf8.len() {
            match std::str::from_utf8(&self.utf8[consumed..]) {
                Ok(text) => {
                    decoded.push_str(text);
                    consumed = self.utf8.len();
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    decoded.push_str(
                        std::str::from_utf8(&self.utf8[consumed..consumed + valid])
                            .expect("valid prefix"),
                    );
                    consumed += valid;
                    if let Some(length) = error.error_len() {
                        decoded.push('\u{fffd}');
                        consumed += length;
                    } else {
                        break;
                    }
                }
            }
        }
        self.utf8.drain(..consumed);
        self.clean(&decoded)
    }

    fn finish(&mut self) -> String {
        let decoded = String::from_utf8_lossy(&self.utf8).into_owned();
        self.utf8.clear();
        let mut text = self.clean(&decoded);
        text.push_str(&self.redactor.finish());
        text
    }

    fn clean(&mut self, text: &str) -> String {
        let mut complete = String::new();
        for c in text.chars() {
            self.escape = match self.escape {
                Escape::Text => {
                    if c == '\x1b' {
                        Escape::Start
                    } else {
                        complete.push(c);
                        Escape::Text
                    }
                }
                Escape::Start => match c {
                    '[' => Escape::Csi,
                    ']' => Escape::Osc,
                    _ => Escape::Text,
                },
                Escape::Csi => {
                    if ('\u{40}'..='\u{7e}').contains(&c) {
                        Escape::Text
                    } else {
                        Escape::Csi
                    }
                }
                Escape::Osc => match c {
                    '\x07' => Escape::Text,
                    '\x1b' => Escape::OscEnd,
                    _ => Escape::Osc,
                },
                Escape::OscEnd => {
                    if c == '\\' || c == '\x07' {
                        Escape::Text
                    } else if c == '\x1b' {
                        Escape::OscEnd
                    } else {
                        Escape::Osc
                    }
                }
            };
        }
        // The state carries incomplete escapes without retaining unbounded OSC payloads.
        // Redact before capture/truncation and before live delivery. Otherwise
        // a retained tail could start halfway through a secret query value.
        self.redactor.push(&strip_ansi(&complete))
    }
}

fn tail(text: &str, bytes: usize) -> &str {
    let mut start = text.len().saturating_sub(bytes);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

fn append_tail(log: &mut String, text: &str, max: usize) -> usize {
    log.push_str(text);
    let retained = tail(log, max).len();
    let removed = log.len() - retained;
    log.drain(..removed);
    removed
}

#[derive(Default)]
struct Capture {
    head: String,
    tail: String,
    omitted: usize,
}
impl Capture {
    fn append(&mut self, text: &str) {
        // Once text has spilled into the tail, preserve its position even
        // if a multibyte character left a little unused room in the head.
        let mut end = if self.tail.is_empty() && self.omitted == 0 {
            (STREAM_BYTES / 2 - self.head.len()).min(text.len())
        } else {
            0
        };
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        self.head.push_str(&text[..end]);
        self.omitted += append_tail(&mut self.tail, &text[end..], STREAM_BYTES / 2);
    }
    fn text(self) -> String {
        if self.omitted == 0 {
            format!("{}{}", self.head, self.tail)
        } else {
            format!(
                "{}\n... [{} bytes truncated] ...\n{}",
                self.head, self.omitted, self.tail
            )
        }
    }
}

fn exit_code(status: ExitStatus) -> String {
    status
        .code()
        .map_or_else(|| "none".into(), |code| code.to_string())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
struct Inner {
    next_id: u32,
    processes: BTreeMap<u32, Process>,
}
impl Inner {
    fn get(&self, id: u32) -> Result<&Process> {
        self.processes.get(&id).ok_or_else(|| {
            anyhow!(
                "unknown process {id}; known ids: {}",
                if self.processes.is_empty() {
                    "none".into()
                } else {
                    self.processes
                        .keys()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            )
        })
    }
    fn kill_all(&mut self) {
        for process in self.processes.values_mut() {
            process.kill();
        }
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.kill_all();
        // Release the task's tree guard too, even if termination failed and
        // the direct child would otherwise keep the task alive indefinitely.
        for process in self.processes.values() {
            if let Some(task) = &process.task {
                task.abort();
            }
        }
    }
}

enum Status {
    Running,
    Exited(Option<i32>),
    Killed,
}
struct Process {
    title: String,
    spec: CommandSpec,
    tree: Arc<Mutex<ProcessTree>>,
    started: Instant,
    status: Status,
    stop_error: Option<String>,
    descendants_killed: bool,
    log: String,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}
impl Process {
    fn kill(&mut self) {
        match lock(&self.tree).kill() {
            Ok(killed) => {
                self.stop_error = None;
                if matches!(self.status, Status::Running) {
                    self.cancel.cancel();
                    self.status = Status::Killed;
                } else if killed && matches!(self.status, Status::Exited(_)) {
                    self.descendants_killed = true;
                }
            }
            Err(error) => self.stop_error = Some(format!("{error:#}")),
        }
    }
    fn line(&self, id: u32) -> String {
        let mut status = match self.status {
            Status::Running => "running".into(),
            Status::Exited(code) => format!(
                "exited ({})",
                code.map_or_else(|| "none".into(), |code| code.to_string())
            ),
            Status::Killed => "killed".into(),
        };
        if self.descendants_killed {
            status.push_str("; remaining descendants killed");
        }
        if let Some(error) = &self.stop_error {
            status.push_str(&format!("; process tree could not be killed: {error}"));
        }
        let seconds = self.started.elapsed().as_secs();
        let uptime = if seconds < 60 {
            format!("{seconds}s")
        } else {
            format!("{}m{:02}s", seconds / 60, seconds % 60)
        };
        let title = if self.title.is_empty() {
            &self.spec.program
        } else {
            &self.title
        };
        format!("{id}  {status}  {uptime}  {title}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(windows: &str, unix: &str) -> CommandSpec {
        shell_command(
            if cfg!(windows) { windows } else { unix },
            &std::env::current_dir().unwrap(),
        )
    }

    fn sleeper() -> CommandSpec {
        spec("Start-Sleep -Seconds 30", "sleep 30")
    }

    async fn wait_for_output(manager: &ProcessManager, id: u32, expected: &str) {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if manager.output(id, None).unwrap().contains(expected) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("process did not emit expected output");
    }

    async fn assert_process_dead(pid: u32) {
        let command = spec(
            &format!(
                "if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 1 }} else {{ exit 0 }}"
            ),
            &format!(
                "state=$(ps -o stat= -p {pid}); case \"$state\" in ''|Z*) exit 0;; *) exit 1;; esac"
            ),
        );
        let result = run(
            &command,
            Some(Duration::from_secs(4)),
            &CancellationToken::new(),
            &mut |_| {},
        )
        .await;
        assert!(result.ok, "process {pid} survived: {}", result.text);
    }

    #[test]
    fn shell_spec() {
        let command = shell_command("echo hello", Path::new("."));
        assert_eq!(command.cwd, Path::new("."));
        #[cfg(windows)]
        {
            assert_eq!(command.program, "powershell.exe");
            assert_eq!(
                command.args,
                [
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "echo hello"
                ]
            );
        }
        #[cfg(not(windows))]
        {
            assert_eq!(command.program, "sh");
            assert_eq!(command.args, ["-c", "echo hello"]);
        }
    }

    #[test]
    fn utf8_and_ansi_boundaries() {
        let input = "before \x1b[31m日本語\x1b[0m \x1b]0;title\x1b\\after \x1b]x\x07!\x1bM";
        for split in 0..=input.len() {
            let mut stream = CleanStream::default();
            let mut text = stream.push(&input.as_bytes()[..split]);
            text.push_str(&stream.push(&input.as_bytes()[split..]));
            text.push_str(&stream.finish());
            assert_eq!(text, "before 日本語 after !", "split {split}");
        }
        let mut stream = CleanStream::default();
        let text = input
            .as_bytes()
            .iter()
            .map(|byte| stream.push(&[*byte]))
            .collect::<String>();
        assert_eq!(text, "before 日本語 after !");
        assert!(stream.finish().is_empty());
    }

    #[test]
    fn malformed_utf8_and_unfinished_escapes() {
        let mut stream = CleanStream::default();
        assert_eq!(stream.push(b"ok\xff\xe6"), "ok\u{fffd}");
        assert_eq!(stream.finish(), "\u{fffd}");
        assert_eq!(stream.push(b"a\x1b[31"), "a");
        assert!(stream.finish().is_empty());
        assert_eq!(stream.push(b"mb"), "b");
        assert!(stream.push(b"\x1b]").is_empty());
        assert!(stream.push(&vec![b'x'; STREAM_BYTES]).is_empty());
        assert!(stream.utf8.is_empty());
        assert_eq!(stream.push(b"\x1b\\done"), "done");
    }

    #[test]
    fn bounded_capture_and_utf8_safe_tail() {
        let mut capture = Capture::default();
        capture.append("head\n");
        for _ in 0..400 {
            capture.append(&"界".repeat(1024));
        }
        capture.append("\ntail");
        assert!(capture.head.len() + capture.tail.len() <= STREAM_BYTES);
        let text = capture.text();
        assert!(text.starts_with("head\n"));
        assert!(text.contains("bytes truncated"));
        assert!(text.ends_with("\ntail"));
        let mut log = "prefix".to_string();
        append_tail(&mut log, &"界".repeat(LOG_BYTES), LOG_BYTES);
        assert!(log.len() <= LOG_BYTES);
        assert_eq!(tail("a日本", 5), "本");
        assert_eq!(tail("a日本", 0), "");

        let prefix = "a".repeat(STREAM_BYTES / 2 - 1);
        let mut capture = Capture::default();
        capture.append(&prefix);
        capture.append("界");
        capture.append("z");
        assert_eq!(capture.text(), format!("{prefix}界z"));
    }

    #[test]
    fn redaction_precedes_capture_and_keeps_streams_separate() {
        let mut streams = [CleanStream::default(), CleanStream::default()];
        let mut captures = [Capture::default(), Capture::default()];
        let mut live = [String::new(), String::new()];
        let mut output = |stderr: bool, text: &str| {
            captures[usize::from(stderr)].append(text);
            live[usize::from(stderr)].push_str(text);
        };
        deliver(&mut streams, false, b"head\n?tok", &mut output);
        deliver(&mut streams, true, b"?token=stderr-secret\n", &mut output);
        deliver(&mut streams, false, b"en=\x1b[31msecret", &mut output);
        for _ in 0..100 {
            deliver(&mut streams, false, &[b's'; 4096], &mut output);
        }
        deliver(&mut streams, false, b"\x1b[0m&ok=1\ntail", &mut output);
        for (index, stream) in streams.iter_mut().enumerate() {
            output(index == 1, &stream.finish());
        }
        for (index, capture) in captures.into_iter().enumerate() {
            let text = capture.text();
            assert!(!text.contains("secret"));
            assert!(text.contains("?token=<redacted>"));
            assert_eq!(text, live[index]);
        }
        assert!(live[0].ends_with("&ok=1\ntail"));
    }

    fn mistl_test_command(spec: &CommandSpec) -> Command {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .current_dir(&spec.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);
        command
    }

    #[tokio::test]
    async fn mistl_capture_is_bounded() {
        let command = spec(
            "[Console]::Out.WriteLine('head'); [Console]::Out.Write('x' * 2000000); [Console]::Out.WriteLine('tail'); [Console]::Error.WriteLine('errhead'); [Console]::Error.Write('y' * 2000000); [Console]::Error.WriteLine('errtail')",
            "printf 'head\\n'; head -c 2000000 /dev/zero | tr '\\000' x; printf 'tail\\n'; printf 'errhead\\n' >&2; head -c 2000000 /dev/zero | tr '\\000' y >&2; printf 'errtail\\n' >&2",
        );
        let result = run_mistl(
            mistl_test_command(&command),
            Duration::from_secs(10),
            &CancellationToken::new(),
            &mut |_| {},
        )
        .await;
        assert!(result.ok, "{}", result.text);
        assert!(result.text.contains("head"));
        assert!(result.text.contains("errtail"));
        assert!(result.text.contains("bytes truncated"));
        assert!(result.text.len() < MAX_OUTPUT_BYTES + 100);
    }

    #[tokio::test]
    async fn mistl_keeps_daemon_descendants_on_exit_timeout_and_cancel() {
        for mode in ["exit", "timeout", "cancel"] {
            let command = spec(
                &format!(
                    "$p = Start-Process -FilePath powershell.exe -WindowStyle Hidden -ArgumentList '-NoLogo','-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 10' -PassThru; [Console]::Out.WriteLine($p.Id); {}",
                    if mode == "exit" {
                        "exit 0"
                    } else {
                        "Start-Sleep -Seconds 30"
                    }
                ),
                &format!(
                    "sleep 10 & echo $!; {}",
                    if mode == "exit" { "exit 0" } else { "sleep 30" }
                ),
            );
            let cancel = CancellationToken::new();
            let started = Instant::now();
            let mut live = String::new();
            let result = run_mistl(
                mistl_test_command(&command),
                Duration::from_secs(2),
                &cancel,
                &mut |chunk| {
                    live.push_str(chunk);
                    if mode == "cancel" && live.contains('\n') {
                        cancel.cancel();
                    }
                },
            )
            .await;
            let pid: u32 = live.trim().parse().expect("descendant pid");
            // Probe and clean up our simulated daemon before asserting results.
            let probe = spec(
                &format!(
                    "if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ Stop-Process -Id {pid} -Force; exit 0 }} else {{ exit 1 }}"
                ),
                &format!("kill -0 {pid} && kill -9 {pid}"),
            );
            let alive = run(
                &probe,
                Some(Duration::from_secs(4)),
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await;
            assert!(
                alive.ok,
                "daemon descendant died during {mode}: {}",
                alive.text
            );
            assert_eq!(result.ok, mode == "exit", "{}", result.text);
            assert!(started.elapsed() < Duration::from_secs(7));
            if mode != "exit" {
                assert!(result.text.contains("process killed"));
            }
        }
    }

    #[tokio::test]
    async fn dropping_mistl_run_kills_client_but_keeps_daemon() {
        let command = spec(
            "$p = Start-Process -FilePath powershell.exe -WindowStyle Hidden -ArgumentList '-NoLogo','-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 10' -PassThru; [Console]::Out.WriteLine($PID); [Console]::Out.WriteLine($p.Id); Start-Sleep -Seconds 30",
            "echo $$; sleep 10 & echo $!; wait",
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            run_mistl(
                mistl_test_command(&command),
                Duration::from_secs(30),
                &CancellationToken::new(),
                &mut |text| {
                    let _ = tx.send(text.to_string());
                },
            )
            .await
        });
        let pids = tokio::time::timeout(Duration::from_secs(4), async {
            let mut text = String::new();
            while text.lines().count() < 2 || !text.ends_with('\n') {
                text.push_str(&rx.recv().await.expect("client exited before printing pids"));
            }
            text.lines()
                .map(|line| line.trim().parse::<u32>().unwrap())
                .collect::<Vec<_>>()
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_process_dead(pids[0]).await;
        let pid = pids[1];
        let probe = spec(
            &format!(
                "if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ Stop-Process -Id {pid} -Force; exit 0 }} else {{ exit 1 }}"
            ),
            &format!("kill -0 {pid} && kill -9 {pid}"),
        );
        let alive = run(
            &probe,
            Some(Duration::from_secs(4)),
            &CancellationToken::new(),
            &mut |_| {},
        )
        .await;
        assert!(
            alive.ok,
            "daemon died when client future was dropped: {}",
            alive.text
        );
    }

    #[tokio::test]
    async fn exit_and_stream_sections() {
        let command = spec(
            "[Console]::Out.WriteLine('hello'); [Console]::Error.WriteLine('problem'); exit 7",
            "printf 'hello\\n'; printf 'problem\\n' >&2; exit 7",
        );
        let mut live = String::new();
        let result = run(
            &command,
            Some(Duration::from_secs(4)),
            &CancellationToken::new(),
            &mut |text| live.push_str(text),
        )
        .await;
        assert!(!result.ok);
        assert_eq!(
            result.text,
            "exit code: 7\n--- stdout ---\nhello\n--- stderr ---\nproblem"
        );
        assert!(live.contains("hello"));
        assert!(live.contains("problem"));
    }

    #[tokio::test]
    async fn chunks_arrive_before_exit() {
        let command = spec(
            "[Console]::Out.WriteLine('ready'); Start-Sleep -Seconds 30",
            "printf 'ready\\n'; sleep 30",
        );
        let cancel = CancellationToken::new();
        let started = Instant::now();
        let mut live = String::new();
        let result = run(&command, None, &cancel, &mut |text| {
            live.push_str(text);
            if live.contains("ready") {
                cancel.cancel();
            }
        })
        .await;
        assert!(live.contains("ready"));
        assert!(result.text.contains("cancelled by user; process killed"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn timeout_kills_quickly() {
        let started = Instant::now();
        let result = run(
            &sleeper(),
            Some(Duration::from_secs(1)),
            &CancellationToken::new(),
            &mut |_| {},
        )
        .await;
        assert!(!result.ok);
        assert!(result.text.contains("[timed out after 1s; process killed]"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn cancellation_kills_quickly() {
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let started = Instant::now();
        let result = run(&sleeper(), None, &cancel, &mut |_| {}).await;
        assert!(!result.ok);
        assert!(result.text.contains("[cancelled by user; process killed]"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn cancellation_kills_descendants() {
        let command = spec(
            "$p = Start-Process -FilePath powershell.exe -WindowStyle Hidden -ArgumentList '-NoLogo','-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 30' -PassThru; [Console]::Out.WriteLine($p.Id); Start-Sleep -Seconds 30",
            "sleep 30 & echo $!; wait",
        );
        let cancel = CancellationToken::new();
        let mut output = String::new();
        let result = run(
            &command,
            Some(Duration::from_secs(4)),
            &cancel,
            &mut |chunk| {
                output.push_str(chunk);
                if output.contains('\n') {
                    cancel.cancel();
                }
            },
        )
        .await;
        assert!(result.text.contains("cancelled by user; process killed"));
        let pid = output.trim().parse().expect("descendant pid");
        assert_process_dead(pid).await;
    }

    #[tokio::test]
    async fn success_strips_ansi_and_bounds_result() {
        let command = spec(
            "[Console]::Out.WriteLine($env:NO_COLOR); [Console]::Out.Write(([char]27).ToString() + '[31m' + ('x' * 20000) + ([char]27).ToString() + '[0m'); exit 0",
            "printf '%s\\n' \"$NO_COLOR\"; printf '\\033[31m'; head -c 20000 /dev/zero | tr '\\000' x; printf '\\033[0m'",
        );
        let mut live = String::new();
        let result = run(
            &command,
            Some(Duration::from_secs(4)),
            &CancellationToken::new(),
            &mut |chunk| live.push_str(chunk),
        )
        .await;
        assert!(result.ok);
        assert!(
            result
                .text
                .replace("\r\n", "\n")
                .starts_with("exit code: 0\n--- stdout ---\n1\n")
        );
        assert!(result.text.contains("bytes truncated"));
        assert!(result.text.len() < MAX_OUTPUT_BYTES + 100);
        assert!(!result.text.contains('\x1b'));
        assert!(!live.contains('\x1b'));
        assert_eq!(live.replace("\r\n", "\n").trim().len(), 20002);
    }

    #[tokio::test]
    async fn background_output_stop_and_ids() {
        let manager = ProcessManager::new();
        assert_eq!(manager.list(), "no background processes");
        let command = spec(
            "[Console]::Out.WriteLine('first'); [Console]::Error.WriteLine('second'); Start-Sleep -Seconds 30",
            "printf 'first\\n'; printf 'second\\n' >&2; sleep 30",
        );
        let id = manager.start(command, "just run".into()).unwrap();
        assert_eq!(id, 1);
        wait_for_output(&manager, id, "first").await;
        wait_for_output(&manager, id, "second").await;
        assert!(manager.list().contains("1  running"));
        assert!(manager.list().ends_with("just run"));
        assert!(manager.output(id, Some(0)).unwrap().ends_with('\n'));
        let stopped = manager.clone().stop(id).await.unwrap();
        assert!(stopped.contains("killed"));
        assert!(stopped.contains("first"));
        assert!(stopped.contains("second"));
        assert!(manager.list().contains("killed"));
        let id2 = manager.start(sleeper(), "sleep".into()).unwrap();
        assert_eq!(id2, 2);
        let error = manager.output(99, None).unwrap_err().to_string();
        assert!(error.contains("unknown process 99"));
        assert!(error.contains("known ids: 1, 2"));
        assert!(manager.stop(99).await.is_err());
        manager.stop_all();
        assert!(manager.output(id2, None).unwrap().contains("killed"));
    }

    #[tokio::test]
    async fn background_records_natural_exit() {
        let manager = ProcessManager::new();
        let id = manager
            .start(
                spec(
                    "[Console]::Out.WriteLine('done'); exit 0",
                    "printf 'done\\n'; exit 0",
                ),
                "finish".into(),
            )
            .unwrap();
        wait_for_output(&manager, id, "exited (0)").await;
        wait_for_output(&manager, id, "done").await;
        assert!(manager.stop(id).await.unwrap().contains("exited (0)"));
    }

    #[tokio::test]
    async fn background_stop_kills_descendants_after_parent_exit() {
        let manager = ProcessManager::new();
        let id = manager
            .start(
                spec(
                    "$p = Start-Process -FilePath powershell.exe -WindowStyle Hidden -ArgumentList '-NoLogo','-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 30' -PassThru; [Console]::Out.WriteLine($p.Id); exit 0",
                    "sleep 30 & echo $!; exit 0",
                ),
                "spawn child and exit".into(),
            )
            .unwrap();
        wait_for_output(&manager, id, "exited (0)").await;
        // The pid is flushed before the status callback, but the pipe reader
        // can still be draining the final output when that callback runs.
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if !lock(&manager.inner).get(id).unwrap().log.trim().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let pid = lock(&manager.inner)
            .get(id)
            .unwrap()
            .log
            .trim()
            .parse()
            .expect("descendant pid");
        let stopped = manager.stop(id).await.unwrap();
        assert!(stopped.contains("exited (0); remaining descendants killed"));
        assert_process_dead(pid).await;
    }

    #[tokio::test]
    async fn dropping_run_future_kills_tree() {
        let command = spec(
            "$p = Start-Process -FilePath powershell.exe -WindowStyle Hidden -ArgumentList '-NoLogo','-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 30' -PassThru; [Console]::Out.WriteLine($PID); [Console]::Out.WriteLine($p.Id); Start-Sleep -Seconds 30",
            "echo $$; sleep 30 & echo $!; wait",
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            run(&command, None, &CancellationToken::new(), &mut |chunk| {
                tx.send(chunk.to_string()).unwrap();
            })
            .await
        });
        let pids = tokio::time::timeout(Duration::from_secs(4), async {
            let mut output = String::new();
            while output.lines().count() < 2 || !output.ends_with('\n') {
                output.push_str(&rx.recv().await.expect("run ended before printing pids"));
            }
            output
                .lines()
                .map(|line| line.trim().parse::<u32>().expect("process pid"))
                .collect::<Vec<_>>()
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        for pid in pids {
            assert_process_dead(pid).await;
        }
    }

    #[tokio::test]
    async fn cleanup_kills_descendants_after_parent_exit() {
        for drop_manager in [false, true] {
            let manager = ProcessManager::new();
            let id = manager
                .start(
                    spec(
                        "$p = Start-Process -FilePath powershell.exe -WindowStyle Hidden -ArgumentList '-NoLogo','-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 30' -PassThru; [Console]::Out.WriteLine($p.Id); exit 0",
                        "sleep 30 & echo $!; exit 0",
                    ),
                    "spawn child and exit".into(),
                )
                .unwrap();
            wait_for_output(&manager, id, "exited (0)").await;
            // Wait for the complete background task, including its pipe drain.
            tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    if lock(&manager.inner)
                        .get(id)
                        .unwrap()
                        .task
                        .as_ref()
                        .unwrap()
                        .is_finished()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let pid = lock(&manager.inner)
                .get(id)
                .unwrap()
                .log
                .trim()
                .parse()
                .unwrap();
            if drop_manager {
                drop(manager);
            } else {
                manager.stop_all();
                assert!(manager.list().contains("remaining descendants killed"));
            }
            assert_process_dead(pid).await;
        }
    }

    #[tokio::test]
    async fn spawn_failure_and_unknown_id() {
        let manager = ProcessManager::new();
        assert!(
            manager
                .output(1, None)
                .unwrap_err()
                .to_string()
                .contains("known ids: none")
        );
        let command = CommandSpec {
            program: "mistl-agent-missing-test-command".into(),
            args: vec![],
            cwd: std::env::current_dir().unwrap(),
        };
        assert!(manager.start(command.clone(), "missing".into()).is_err());
        assert_eq!(manager.list(), "no background processes");
        let result = run(&command, None, &CancellationToken::new(), &mut |_| {}).await;
        assert!(!result.ok);
        assert!(result.text.contains("failed to start"));
        assert!(result.text.starts_with("error: "));
    }

    #[test]
    fn drop_last_clone_outside_runtime() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let manager = ProcessManager::new();
        let weak = Arc::downgrade(&manager.inner);
        let clone = manager.clone();
        runtime.block_on(async {
            manager
                .start(
                    spec(
                        "[Console]::Out.WriteLine($PID); [Console]::Out.WriteLine('ready'); Start-Sleep -Seconds 30",
                        "echo $$; echo ready; sleep 30",
                    ),
                    "sleep".into(),
                )
                .unwrap();
            wait_for_output(&manager, 1, "ready").await;
        });
        let pid = lock(&manager.inner)
            .get(1)
            .unwrap()
            .log
            .lines()
            .next()
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        drop(manager);
        assert!(weak.upgrade().is_some());
        drop(clone);
        assert!(weak.upgrade().is_none());
        runtime.block_on(assert_process_dead(pid));
        drop(runtime);
        assert!(
            ProcessManager::new()
                .start(sleeper(), "sleep".into())
                .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inherited_pipes_do_not_delay_exit() {
        let started = Instant::now();
        // The grandchild exits soon on its own, but outlives the pipe grace period.
        let result = run(
            &spec("", "sleep 2 & printf 'parent done\\n'"),
            None,
            &CancellationToken::new(),
            &mut |_| {},
        )
        .await;
        assert!(result.ok);
        assert!(result.text.contains("parent done"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
