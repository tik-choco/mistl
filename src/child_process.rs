//! Owned commands must not outlive their process-tree guard.
use anyhow::{Context, Result};
use tokio::process::{Child, Command};

pub struct ProcessTree {
    finished: bool,
    #[cfg(windows)]
    job: usize,
    #[cfg(unix)]
    pgid: i32,
}

pub fn prepare(command: &mut Command) {
    command.kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    #[cfg(unix)]
    command.process_group(0);
}

impl ProcessTree {
    pub fn attach(child: &mut Child) -> Result<Self> {
        #[cfg(windows)]
        {
            use windows_sys::Win32::{Foundation::CloseHandle, System::JobObjects::*};
            // SAFETY: the child owns its process handle. Job handle is closed on
            // every failure path and transferred to this guard only on success.
            unsafe {
                let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
                if job.is_null() {
                    let error = std::io::Error::last_os_error();
                    let _ = child.start_kill();
                    return Err(error).context("creating command process tree");
                }
                let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
                limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                let configured = SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    &limits as *const _ as *const _,
                    std::mem::size_of_val(&limits) as u32,
                ) != 0;
                let assigned = configured
                    && child
                        .raw_handle()
                        .is_some_and(|handle| AssignProcessToJobObject(job, handle as _) != 0);
                if !assigned {
                    let error = std::io::Error::last_os_error();
                    CloseHandle(job);
                    let _ = child.start_kill();
                    return Err(error).context("isolating command process tree");
                }
                Ok(Self {
                    finished: false,
                    job: job as usize,
                })
            }
        }
        #[cfg(unix)]
        {
            Ok(Self {
                finished: false,
                pgid: child.id().context("child already exited")? as i32,
            })
        }
    }

    /// Kill all remaining descendants, even after the direct child exits.
    /// Returns false if the tree is empty or was already killed. Failures can
    /// be retried; a successful kill disarms repeated process-group signals.
    pub fn kill(&mut self) -> Result<bool> {
        if self.finished {
            return Ok(false);
        }
        #[cfg(windows)]
        let killed = {
            use windows_sys::Win32::System::JobObjects::*;
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            // SAFETY: this guard owns the job and accounting is a correctly
            // sized writable buffer for the requested information class.
            if unsafe {
                QueryInformationJobObject(
                    self.job as _,
                    JobObjectBasicAccountingInformation,
                    &mut accounting as *mut _ as *mut _,
                    std::mem::size_of_val(&accounting) as u32,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error())
                    .context("querying command process tree");
            }
            if accounting.ActiveProcesses == 0 {
                false
            } else {
                // SAFETY: this job contains only the owned invocation's tree.
                if unsafe { TerminateJobObject(self.job as _, 1) } == 0 {
                    return Err(std::io::Error::last_os_error())
                        .context("terminating command process tree");
                }
                true
            }
        };
        #[cfg(unix)]
        let killed = {
            // SAFETY: prepare created this group for our own child.
            if unsafe { libc::kill(-self.pgid, libc::SIGKILL) } == 0 {
                true
            } else {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error).context("terminating command process group");
                }
                false
            }
        };
        self.finished = true;
        Ok(killed)
    }
}
impl Drop for ProcessTree {
    fn drop(&mut self) {
        #[cfg(windows)]
        {
            // SAFETY: this guard uniquely owns the job handle.
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(self.job as _);
            }
        }
        #[cfg(unix)]
        {
            if let Err(error) = self.kill() {
                tracing::warn!("process tree cleanup failed: {error:#}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use std::time::Duration;

    fn sleeper() -> (Child, ProcessTree) {
        #[cfg(windows)]
        let mut command = {
            let mut command = Command::new("powershell.exe");
            command.args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ]);
            command
        };
        #[cfg(unix)]
        let mut command = {
            let mut command = Command::new("sh");
            command.args(["-c", "sleep 30"]);
            command
        };
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        prepare(&mut command);
        // Exercise the tree guard independently of Tokio's child cleanup.
        command.kill_on_drop(false);
        let mut child = command.spawn().unwrap();
        let tree = ProcessTree::attach(&mut child).unwrap();
        (child, tree)
    }

    #[tokio::test]
    async fn explicit_kill_is_idempotent() {
        let (mut child, mut tree) = sleeper();
        assert!(tree.kill().unwrap());
        assert!(!tree.kill().unwrap());
        let status = tokio::time::timeout(Duration::from_secs(4), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success());
    }

    #[tokio::test]
    async fn dropping_guard_kills_child() {
        let (mut child, tree) = sleeper();
        assert!(child.try_wait().unwrap().is_none());
        drop(tree);
        // Closing a Windows job can terminate its processes with exit code 0.
        // Prompt exit of this 30-second sleeper proves the guard killed it.
        tokio::time::timeout(Duration::from_secs(4), child.wait())
            .await
            .expect("tree guard did not terminate its child")
            .unwrap();
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn kill_failure_can_be_retried() {
        let (mut child, mut tree) = sleeper();
        let job = std::mem::replace(&mut tree.job, 0);
        let result = tree.kill();
        tree.job = job;
        assert!(result.is_err());
        assert!(!tree.finished);
        assert!(child.try_wait().unwrap().is_none());
        assert!(tree.kill().unwrap());
        tokio::time::timeout(Duration::from_secs(4), child.wait())
            .await
            .unwrap()
            .unwrap();
    }
}
