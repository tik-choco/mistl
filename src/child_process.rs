//! Daemon-owned commands must not outlive a disconnect/restart.
use anyhow::{Context, Result};
use tokio::process::{Child, Command};

pub struct ProcessTree {
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
                    child.start_kill()?;
                    return Err(std::io::Error::last_os_error().into());
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
                Ok(Self { job: job as usize })
            }
        }
        #[cfg(unix)]
        {
            Ok(Self {
                pgid: child.id().context("child already exited")? as i32,
            })
        }
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
            // SAFETY: pgid is the process group created for our own child.
            unsafe {
                libc::kill(-self.pgid, libc::SIGKILL);
            }
        }
    }
}
