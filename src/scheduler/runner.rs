//! Job execution: spawns the shell command, captures combined stdout+stderr,
//! enforces a hard timeout, and appends a [`super::RunRecord`] to the run
//! log. Shared by the background tick loop and the manual "run now" path
//! (`sched.run`) -- both just call [`execute_and_record`].

use std::collections::VecDeque;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{info, warn};

use super::{Job, RunRecord};

/// Overlapping runs of the same job are allowed (matches tc-sched), so this
/// takes no lock on the job -- it's a pure "run this command once" helper.
const RUN_TIMEOUT: Duration = Duration::from_secs(3600);
const OUTPUT_CAP: usize = 64 * 1024;

/// Runs `job.command` to completion (or until [`RUN_TIMEOUT`]), then appends
/// the outcome to the run log. Errors persisting the record are logged, not
/// propagated -- there's no caller left to hand them to once a background
/// tick has fired.
pub(super) async fn execute_and_record(data_dir: &Path, job: &Job) {
    let started_at = Utc::now();
    info!(job_id = %job.id, job_name = %job.name, "scheduler: job starting");
    let (exit_code, output) = run_command(&job.command).await;
    let ended_at = Utc::now();
    let ok = exit_code == 0;
    info!(job_id = %job.id, job_name = %job.name, exit_code, ok, "scheduler: job finished");

    let record = RunRecord {
        job_id: job.id.clone(),
        job_name: job.name.clone(),
        started_at: started_at.to_rfc3339(),
        ended_at: ended_at.to_rfc3339(),
        exit_code,
        ok,
        output,
    };
    if let Err(error) = super::append_run_record(data_dir, &record).await {
        warn!(%error, job_id = %job.id, "scheduler: failed to append run record");
    }
}

/// `cmd /C <command>` on Windows, `sh -c <command>` elsewhere (matches how
/// tc-sched shells out). Returns `(exit_code, combined_output)`; a spawn
/// failure or timeout is reported as exit code -1 with the reason noted in
/// the output text (there's no process to pull a real exit code from).
async fn run_command(command: &str) -> (i64, String) {
    run_command_with_timeout(command, RUN_TIMEOUT).await
}

async fn run_command_with_timeout(command: &str, timeout: Duration) -> (i64, String) {
    let mut cmd = if cfg!(windows) {
        let mut c = tokio::process::Command::new("cmd");
        c.arg("/C").arg(command);
        c
    } else {
        let mut c = tokio::process::Command::new("sh");
        c.arg("-c").arg(command);
        c
    };
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    crate::child_process::prepare(&mut cmd);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => return (-1, format!("failed to spawn command: {error}")),
    };
    let _process_tree = match crate::child_process::ProcessTree::attach(&mut child) {
        Ok(tree) => tree,
        Err(error) => return (-1, format!("could not own command process tree: {error:#}")),
    };

    // One shared ring bounds memory even while the child is still running.
    // Include pipe EOF in the deadline: a descendant may inherit a pipe
    // after the shell exits. Borrowed read futures are cancelled on timeout,
    // rather than leaving detached reader tasks waiting forever.
    let stdout = child.stdout.take().expect("stdout piped above");
    let stderr = child.stderr.take().expect("stderr piped above");
    let output = Arc::new(Mutex::new(OutputTail::default()));
    let result = tokio::time::timeout(timeout, async {
        tokio::try_join!(
            child.wait(),
            drain_output(stdout, output.clone()),
            drain_output(stderr, output.clone()),
        )
    })
    .await;
    let (exit_code, note) = match result {
        Ok(Ok((status, (), ()))) => (status.code().unwrap_or(-1) as i64, None),
        Ok(Err(error)) => {
            let _ = child.kill().await;
            (
                -1,
                Some(format!("[scheduler: command I/O failed: {error}]")),
            )
        }
        Err(_) => {
            let _ = child.kill().await;
            (
                -1,
                Some(format!(
                    "[scheduler: command or output timed out after {} seconds]",
                    timeout.as_secs_f64()
                )),
            )
        }
    };
    let mut text = output.lock().expect("output lock").text();
    if let Some(note) = note {
        text.push('\n');
        text.push_str(&note);
    }
    (exit_code, text)
}

#[derive(Default)]
struct OutputTail {
    bytes: VecDeque<u8>,
    truncated: bool,
}

impl OutputTail {
    fn push(&mut self, chunk: &[u8]) {
        let excess = (self.bytes.len() + chunk.len()).saturating_sub(OUTPUT_CAP);
        if excess > 0 {
            self.truncated = true;
            self.bytes.drain(..excess.min(self.bytes.len()));
        }
        self.bytes
            .extend(&chunk[chunk.len().saturating_sub(OUTPUT_CAP)..]);
    }

    fn text(&self) -> String {
        let mut bytes: Vec<u8> = self.bytes.iter().copied().collect();
        // A ring eviction may start halfway through a multibyte character.
        if self.truncated {
            let start = bytes
                .iter()
                .position(|b| b & 0xc0 != 0x80)
                .unwrap_or(bytes.len());
            bytes.drain(..start);
        }
        let text = truncate_output(&bytes);
        if self.truncated && !text.starts_with("[scheduler: output truncated") {
            format!("[scheduler: output truncated to the last {OUTPUT_CAP} bytes]\n{text}")
        } else {
            text
        }
    }
}

async fn drain_output(
    mut reader: impl AsyncRead + Unpin,
    output: Arc<Mutex<OutputTail>>,
) -> std::io::Result<()> {
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            return Ok(());
        }
        output.lock().expect("output lock").push(&chunk[..count]);
    }
}

/// Keeps only the last [`OUTPUT_CAP`] bytes (never splitting a UTF-8
/// character), prefixed with a marker line noting the truncation.
fn truncate_output(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes).into_owned();
    if text.len() <= OUTPUT_CAP {
        return text;
    }
    let mut start = text.len() - OUTPUT_CAP;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    format!(
        "[scheduler: output truncated to the last {OUTPUT_CAP} bytes]\n{}",
        &text[start..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drains_large_output_with_bounded_memory_and_preserves_tail() {
        use tokio::io::AsyncWriteExt;
        let (mut writer, reader) = tokio::io::duplex(4096);
        let output = Arc::new(Mutex::new(OutputTail::default()));
        let data = vec![b'x'; OUTPUT_CAP * 20];
        let send = async {
            writer.write_all(&data).await.unwrap();
            writer.write_all(b"FINAL-OUTPUT").await.unwrap();
            writer.shutdown().await.unwrap();
        };
        let (_, result) = tokio::join!(send, drain_output(reader, output.clone()));
        result.unwrap();
        let tail = output.lock().unwrap();
        assert_eq!(tail.bytes.len(), OUTPUT_CAP);
        assert!(tail.text().starts_with("[scheduler: output truncated"));
        assert!(tail.text().ends_with("FINAL-OUTPUT"));
    }

    #[test]
    fn ring_does_not_corrupt_a_split_utf8_character() {
        let mut tail = OutputTail::default();
        tail.push("あ".as_bytes());
        tail.push(&vec![b'x'; OUTPUT_CAP - 1]);
        let text = tail.text();
        assert!(!text.contains('\u{fffd}'));
        assert!(text.ends_with(&"x".repeat(OUTPUT_CAP - 1)));
    }

    #[tokio::test]
    async fn long_running_shell_times_out_and_keeps_partial_output() {
        let command = if cfg!(windows) {
            "for /L %i in (1,1,2147483647) do @echo tick"
        } else {
            "while :; do echo tick; done"
        };
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            run_command_with_timeout(command, Duration::from_millis(500)),
        )
        .await
        .unwrap();
        assert_eq!(result.0, -1);
        assert!(result.1.contains("timed out"));
        assert!(result.1.contains("tick"));
        assert!(result.1.len() < OUTPUT_CAP + 512);
    }

    #[tokio::test]
    async fn pipe_without_eof_can_be_cancelled_without_detached_readers() {
        use tokio::io::AsyncWriteExt;
        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(b"partial").await.unwrap();
        let output = Arc::new(Mutex::new(OutputTail::default()));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                drain_output(reader, output.clone())
            )
            .await
            .is_err()
        );
        assert_eq!(output.lock().unwrap().text(), "partial");
        assert!(writer.write_all(b"reader has been dropped").await.is_err());
    }

    #[tokio::test]
    async fn run_command_captures_output_and_exit_code() {
        // "echo hi" is a valid command line for both `cmd /C` and `sh -c`.
        let (exit_code, output) = run_command("echo hi").await;
        assert_eq!(exit_code, 0);
        assert!(output.contains("hi"), "output was {output:?}");
    }

    #[tokio::test]
    async fn run_command_reports_nonzero_exit_code() {
        // "exit 3" also behaves the same under both shells.
        let (exit_code, _output) = run_command("exit 3").await;
        assert_eq!(exit_code, 3);
    }

    #[test]
    fn truncate_output_keeps_only_the_tail_with_a_marker() {
        let bytes = vec![b'a'; OUTPUT_CAP + 100];
        let text = truncate_output(&bytes);
        assert!(text.starts_with("[scheduler: output truncated"));
        assert_eq!(text.len() - text.find('\n').unwrap() - 1, OUTPUT_CAP);
    }

    #[test]
    fn truncate_output_passes_short_output_through_unchanged() {
        assert_eq!(truncate_output(b"hello"), "hello");
    }
}
