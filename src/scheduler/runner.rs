//! Job execution: spawns the shell command, captures combined stdout+stderr,
//! enforces a hard timeout, and appends a [`super::RunRecord`] to the run
//! log. Shared by the background tick loop and the manual "run now" path
//! (`sched.run`) -- both just call [`execute_and_record`].

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use tokio::io::AsyncReadExt;
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
        .stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => return (-1, format!("failed to spawn command: {error}")),
    };

    // Drain both pipes concurrently so a chatty child can't deadlock on a
    // full stdout/stderr buffer while we're only waiting on `child.wait()`.
    let mut stdout = child.stdout.take().expect("stdout piped above");
    let mut stderr = child.stderr.take().expect("stderr piped above");
    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf).await;
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf).await;
        buf
    });

    let (exit_code, timed_out) = match tokio::time::timeout(RUN_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) => (status.code().unwrap_or(-1) as i64, false),
        Ok(Err(error)) => return (-1, format!("waiting for command failed: {error}")),
        Err(_elapsed) => {
            // Killing closes the pipes, so the drain tasks below still
            // terminate and hand back whatever partial output was captured.
            let _ = child.kill().await;
            (-1, true)
        }
    };

    let mut combined = stdout_task.await.unwrap_or_default();
    combined.extend_from_slice(&stderr_task.await.unwrap_or_default());
    let mut text = truncate_output(&combined);
    if timed_out {
        text.push_str("\n[scheduler: command timed out after 1 hour and was killed]");
    }
    (exit_code, text)
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
