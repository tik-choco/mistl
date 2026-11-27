//! Host capability detection: QEMU binaries, accelerators, passt, ssh tools.
use std::env;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::config::VmConfig;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Resources {
    pub cpus: u32,
    pub mem_total_mb: u64,
    pub mem_free_mb: u64,
    pub disk_free_gb: u64,
}

pub fn resources(path: &Path) -> Resources {
    let (mem_total_mb, mem_free_mb) = memory_mb();
    Resources {
        cpus: std::thread::available_parallelism().map_or(1, |cpus| cpus.get() as u32),
        mem_total_mb,
        mem_free_mb,
        disk_free_gb: disk_free_gb(path),
    }
}

#[cfg(target_os = "linux")]
fn memory_mb() -> (u64, u64) {
    let Ok(info) = std::fs::read_to_string("/proc/meminfo") else {
        return (0, 0);
    };
    let read_mb = |field: &str| {
        info.lines()
            .find_map(|line| line.strip_prefix(field))
            .and_then(|value| value.split_ascii_whitespace().next()?.parse::<u64>().ok())
            .unwrap_or_default()
            / 1024
    };
    (read_mb("MemTotal:"), read_mb("MemAvailable:"))
}

#[cfg(windows)]
fn memory_mb() -> (u64, u64) {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    let mut info = MEMORYSTATUSEX {
        dwLength: std::mem::size_of::<MEMORYSTATUSEX>() as u32,
        ..Default::default()
    };
    // SAFETY: info is writable and its size is set as required by the API.
    if unsafe { GlobalMemoryStatusEx(&mut info) } == 0 {
        return (0, 0);
    }
    (info.ullTotalPhys >> 20, info.ullAvailPhys >> 20)
}

#[cfg(not(any(windows, target_os = "linux")))]
fn memory_mb() -> (u64, u64) {
    (0, 0)
}

#[cfg(unix)]
fn disk_free_gb(path: &Path) -> u64 {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return 0;
    };
    let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL-terminated and info can hold the result.
    if unsafe { libc::statvfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
        return 0;
    }
    // SAFETY: statvfs succeeded and initialized info.
    let info = unsafe { info.assume_init() };
    let bytes = u128::from(info.f_bavail) * u128::from(info.f_frsize);
    u64::try_from(bytes >> 30).unwrap_or(u64::MAX)
}

#[cfg(windows)]
fn disk_free_gb(path: &Path) -> u64 {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let path: Vec<_> = path.as_os_str().encode_wide().chain([0]).collect();
    let mut available = 0;
    // SAFETY: path is NUL-terminated and available is writable. The other
    // two outputs are optional.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            path.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if ok == 0 { 0 } else { available >> 30 }
}

#[cfg(not(any(unix, windows)))]
fn disk_free_gb(_path: &Path) -> u64 {
    0
}

pub(super) fn require_ha_support(os: &str) -> Result<()> {
    anyhow::ensure!(
        matches!(os, "windows" | "linux"),
        "VM HA is unavailable on {os}: QEMU is not tied to the daemon lifetime"
    );
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct QemuIdentity {
    pid: u32,
    started: u64,
    #[serde(default)]
    boot_id: String,
}

pub(super) fn remember_qemu(dir: &Path, pid: Option<u32>) -> Result<()> {
    if require_ha_support(env::consts::OS).is_err() {
        return Ok(());
    }
    let pid = pid.context("QEMU exited before its process could be recorded")?;
    let process =
        QemuIdentity::inspect(pid)?.context("QEMU exited before its process could be recorded")?;
    crate::statefile::write_private(
        &dir.join("qemu-process.json"),
        &serde_json::to_vec(&process)?,
    )
    .context("saving QEMU process identity")
}

pub(super) fn fence_previous_qemu(dir: &Path) -> Result<()> {
    if require_ha_support(env::consts::OS).is_err() {
        return Ok(());
    }
    let path = dir.join("qemu-process.json");
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("reading previous QEMU process identity"),
    };
    let process: QemuIdentity =
        serde_json::from_slice(&bytes).context("invalid previous QEMU process identity")?;
    anyhow::ensure!(
        process.pid > 0 && process.pid != std::process::id(),
        "invalid previous QEMU PID"
    );
    process
        .terminate()
        .context("could not confirm the previous QEMU process exited")?;
    std::fs::remove_file(path).context("removing previous QEMU process identity")
}

#[cfg(windows)]
mod qemu_identity {
    use super::*;
    use std::io::Error;
    use windows_sys::Win32::{Foundation::*, System::Threading::*};

    struct Process(HANDLE);

    impl Process {
        fn open(pid: u32, access: u32) -> Result<Option<Self>> {
            // SAFETY: the returned handle is owned by Process and closed on drop.
            let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | access, 0, pid) };
            if !handle.is_null() {
                return Ok(Some(Self(handle)));
            }
            let error = Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                return Ok(None);
            }
            Err(error).context("opening QEMU process")
        }

        fn started(&self) -> Result<u64> {
            let [mut created, mut exited, mut kernel, mut user] = [FILETIME::default(); 4];
            // SAFETY: the process handle is live and the output pointers are writable.
            if unsafe { GetProcessTimes(self.0, &mut created, &mut exited, &mut kernel, &mut user) }
                == 0
            {
                return Err(Error::last_os_error()).context("reading QEMU process creation time");
            }
            Ok(u64::from(created.dwHighDateTime) << 32 | u64::from(created.dwLowDateTime))
        }
    }

    impl Drop for Process {
        fn drop(&mut self) {
            unsafe { CloseHandle(self.0) };
        }
    }

    impl QemuIdentity {
        pub(super) fn inspect(pid: u32) -> Result<Option<Self>> {
            Process::open(pid, 0)?
                .map(|process| {
                    Ok(Self {
                        pid,
                        started: process.started()?,
                        boot_id: String::new(),
                    })
                })
                .transpose()
        }

        pub(super) fn terminate(&self) -> Result<()> {
            let Some(probe) = Process::open(self.pid, PROCESS_SYNCHRONIZE)? else {
                return Ok(());
            };
            if probe.started()? != self.started {
                return Ok(());
            }
            // SAFETY: the handle stays bound to the process whose creation time was checked.
            if unsafe { WaitForSingleObject(probe.0, 0) } == WAIT_OBJECT_0 {
                return Ok(());
            }
            let Some(process) = Process::open(self.pid, PROCESS_TERMINATE | PROCESS_SYNCHRONIZE)?
            else {
                return Ok(());
            };
            if process.started()? != self.started {
                return Ok(());
            }
            // SAFETY: terminate and wait both address this same verified process handle.
            let terminated = unsafe { TerminateProcess(process.0, 1) } != 0;
            let error = (!terminated).then(Error::last_os_error);
            match unsafe { WaitForSingleObject(process.0, if terminated { 5000 } else { 0 }) } {
                WAIT_OBJECT_0 => Ok(()),
                WAIT_TIMEOUT if error.is_none() => {
                    bail!("timed out waiting for previous QEMU to exit")
                }
                _ => Err(error.unwrap_or_else(Error::last_os_error))
                    .context("terminating previous QEMU"),
            }
        }
    }
}

#[cfg(target_os = "linux")]
mod qemu_identity {
    use super::*;
    use std::io::{Error, ErrorKind};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    fn boot_id() -> Result<String> {
        Ok(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
            .trim()
            .to_owned())
    }

    fn started(pid: u32) -> Result<Option<u64>> {
        let stat = match std::fs::read(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("reading QEMU process stat"),
        };
        // The command name in parentheses can contain spaces and closing parentheses.
        let end = stat
            .iter()
            .rposition(|&b| b == b')')
            .context("invalid QEMU process stat")?;
        let start = std::str::from_utf8(&stat[end + 1..])?
            .split_ascii_whitespace()
            .nth(19)
            .context("missing QEMU process start time")?;
        Ok(Some(start.parse()?))
    }

    impl QemuIdentity {
        pub(super) fn inspect(pid: u32) -> Result<Option<Self>> {
            started(pid)?
                .map(|started| {
                    Ok(Self {
                        pid,
                        started,
                        boot_id: boot_id()?,
                    })
                })
                .transpose()
        }

        pub(super) fn terminate(&self) -> Result<()> {
            if boot_id()? != self.boot_id || started(self.pid)? != Some(self.started) {
                return Ok(());
            }
            // Open a stable process reference and check its identity again before signaling.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, self.pid, 0u32) };
            if fd < 0 {
                let error = Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    return Ok(());
                }
                return Err(error).context("opening previous QEMU pidfd");
            }
            // SAFETY: pidfd_open returned an owned file descriptor.
            let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
            if started(self.pid)? != Some(self.started) {
                return Ok(());
            }
            let mut wait = libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            if unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    fd.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0u32,
                )
            } < 0
            {
                let error = Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error).context("terminating previous QEMU");
                }
            }
            match unsafe { libc::poll(&mut wait, 1, 5000) } {
                0 => bail!("timed out waiting for previous QEMU to exit"),
                n if n > 0 && wait.revents & libc::POLLIN != 0 => Ok(()),
                _ => Err(Error::last_os_error()).context("waiting for previous QEMU to exit"),
            }
        }
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
impl QemuIdentity {
    fn inspect(_pid: u32) -> Result<Option<Self>> {
        bail!("QEMU process tracking is unavailable")
    }
    fn terminate(&self) -> Result<()> {
        bail!("QEMU process tracking is unavailable")
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HostInfo {
    pub os: String,
    pub arch: String,
    pub qemu_system: Option<PathBuf>,
    pub qemu_img: Option<PathBuf>,
    pub qemu_version: Option<String>,
    /// Accelerators the QEMU binary reports (`-accel help`).
    pub accels: Vec<String>,
    /// Accelerator that will be used (`[vm] accel` resolved), if any.
    pub accel: Option<String>,
    /// `/dev/kvm` openable read-write (Linux only).
    pub kvm_access: Option<bool>,
    pub passt: Option<PathBuf>,
    /// Network mode that will be used: `passt`, `slirp` or `isolated`.
    pub network: String,
    pub ssh: Option<PathBuf>,
    pub ssh_keygen: Option<PathBuf>,
    /// Human-readable blockers; empty means VMs can run here.
    pub problems: Vec<String>,
    /// Non-fatal caveats (e.g. guest can reach host loopback under slirp).
    pub warnings: Vec<String>,
}

impl HostInfo {
    pub fn ready(&self) -> bool {
        self.problems.is_empty()
    }
}

pub async fn detect(cfg: &VmConfig) -> HostInfo {
    let mut host = HostInfo {
        os: env::consts::OS.into(),
        arch: env::consts::ARCH.into(),
        ..HostInfo::default()
    };
    if host.arch != "x86_64" {
        host.problems.push(format!(
            "host architecture {} is unsupported in stage 1",
            host.arch
        ));
    }
    let qemu_name = executable_name("qemu-system-x86_64");
    let configured = cfg.qemu.trim();
    if !configured.is_empty() {
        let path = Path::new(configured);
        let candidate = if path.is_dir() {
            path.join(&qemu_name)
        } else {
            path.to_owned()
        };
        host.qemu_system = executable(candidate);
        if host.qemu_system.is_none() {
            host.warnings
                .push("[vm] qemu was not executable; searching for QEMU".into());
        }
    }
    if host.qemu_system.is_none() {
        host.qemu_system =
            find_on_path(&qemu_name).or_else(|| find_in_dirs(&qemu_name, qemu_default_dirs()));
    }
    if let Some(qemu) = &host.qemu_system {
        host.qemu_img = executable(qemu.with_file_name(executable_name("qemu-img")));
        if host.qemu_img.is_none() {
            host.problems
                .push("qemu-img was not found alongside qemu-system-x86_64".into());
        }
        let (version, accelerators) = tokio::join!(
            probe(qemu, &["--version"]),
            probe(qemu, &["-accel", "help"])
        );
        match version {
            Ok(output) => {
                host.qemu_version = output
                    .lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty())
                    .map(str::to_owned);
                if host.qemu_version.is_none() {
                    host.problems.push("QEMU reported no version".into());
                }
            }
            Err(error) => host
                .problems
                .push(format!("could not query the QEMU version: {error}")),
        }
        match accelerators {
            Ok(output) => host.accels = parse_accelerators(&output),
            Err(error) => host
                .problems
                .push(format!("could not query QEMU accelerators: {error}")),
        }
    } else {
        host.problems
            .push("qemu-system-x86_64 was not found; set [vm] qemu or install QEMU".into());
    }

    if cfg!(target_os = "linux") {
        host.kvm_access = Some(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/kvm")
                .is_ok(),
        );
        host.passt = find_on_path("passt");
    }
    resolve_accel(&mut host, cfg.accel.trim());
    resolve_network(&mut host, cfg.network.trim());

    host.ssh = find_ssh_tool("ssh");
    host.ssh_keygen = find_ssh_tool("ssh-keygen");
    if host.ssh.is_none() {
        host.problems
            .push("ssh was not found; install an OpenSSH client".into());
    }
    if host.ssh_keygen.is_none() {
        host.problems
            .push("ssh-keygen was not found; install an OpenSSH client".into());
    }
    host
}

pub(super) fn find_ssh_tool(name: &str) -> Option<PathBuf> {
    let name = executable_name(name);
    find_on_path(&name).or_else(|| {
        let dirs = if cfg!(windows) {
            vec![
                PathBuf::from(env::var_os("SystemRoot").unwrap_or_else(|| r"C:\Windows".into()))
                    .join("System32")
                    .join("OpenSSH"),
            ]
        } else {
            vec![PathBuf::from("/usr/bin"), PathBuf::from("/usr/local/bin")]
        };
        find_in_dirs(&name, dirs)
    })
}

fn executable_name(name: &str) -> String {
    format!("{name}{}", env::consts::EXE_SUFFIX)
}

fn executable(path: PathBuf) -> Option<PathBuf> {
    let metadata = path.metadata().ok()?;
    if !metadata.is_file() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return None;
        }
    }
    // QEMU changes its working directory to the VM directory on launch.
    std::path::absolute(path).ok()
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    find_in_dirs(name, env::split_paths(&env::var_os("PATH")?))
}

fn find_in_dirs(name: &str, dirs: impl IntoIterator<Item = PathBuf>) -> Option<PathBuf> {
    dirs.into_iter().find_map(|dir| executable(dir.join(name)))
}

fn qemu_default_dirs() -> Vec<PathBuf> {
    match env::consts::OS {
        "windows" => {
            let mut dirs: Vec<_> = ["ProgramFiles", "ProgramW6432", "ProgramFiles(x86)"]
                .into_iter()
                .filter_map(env::var_os)
                .map(|dir| PathBuf::from(dir).join("qemu"))
                .collect();
            dirs.push(PathBuf::from(r"C:\Program Files\qemu"));
            if let Some(dir) = env::var_os("LOCALAPPDATA") {
                dirs.push(PathBuf::from(dir).join("mistl-vm-p0").join("qemu"));
            }
            dirs
        }
        "macos" => [
            "/opt/homebrew/bin",
            "/usr/local/bin",
            "/opt/local/bin",
            "/usr/bin",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect(),
        _ => ["/usr/bin", "/usr/local/bin", "/bin"]
            .into_iter()
            .map(PathBuf::from)
            .collect(),
    }
}

async fn probe(qemu: &Path, args: &[&str]) -> Result<String> {
    let mut command = Command::new(qemu);
    command.args(args).stdin(Stdio::null());
    crate::child_process::prepare(&mut command);
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .context("QEMU probe timed out")?
        .context("running QEMU")?;
    if !output.status.success() {
        bail!("QEMU exited with {}", output.status);
    }
    Ok(format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn parse_accelerators(output: &str) -> Vec<String> {
    let Some((_, names)) = output.split_once("Accelerators supported in QEMU binary:") else {
        return Vec::new();
    };
    names
        .trim_start()
        .lines()
        .map(str::trim)
        .take_while(|name| {
            name.starts_with(|c: char| c.is_ascii_lowercase())
                && name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
        .map(str::to_owned)
        .collect()
}

fn resolve_accel(host: &mut HostInfo, configured: &str) {
    let supported = |accel: &str| host.accels.iter().any(|item| item == accel);
    let accel = match configured {
        "auto" => {
            let preferred = match (host.os.as_str(), host.kvm_access) {
                ("linux", Some(true)) => "kvm",
                ("windows", _) => "whpx",
                ("macos", _) => "hvf",
                _ => "tcg",
            };
            if supported(preferred) {
                preferred
            } else {
                "tcg"
            }
        }
        "kvm" | "whpx" | "hvf" | "tcg" => configured,
        _ => {
            host.problems
                .push("[vm] accel must be auto, kvm, whpx, hvf or tcg".into());
            return;
        }
    };
    if host.qemu_system.is_none() {
        return;
    }
    if !supported(accel) {
        host.problems
            .push(format!("QEMU does not report support for {accel}"));
    } else if accel == "kvm" && host.kvm_access != Some(true) {
        host.problems
            .push("KVM requires read-write access to /dev/kvm".into());
    } else {
        host.accel = Some(accel.into());
        if accel == "tcg" {
            host.warnings
                .push("TCG uses software emulation and is slow".into());
        }
    }
}

fn resolve_network(host: &mut HostInfo, configured: &str) {
    let network = match configured {
        "auto" if host.passt.is_some() => "passt",
        "auto" => "slirp",
        "passt" | "slirp" | "isolated" => configured,
        _ => {
            host.problems
                .push("[vm] network must be auto, passt, slirp or isolated".into());
            return;
        }
    };
    host.network = network.into();
    if network == "passt" && host.passt.is_none() {
        host.problems
            .push("passt networking requires the passt executable on Linux".into());
    }
    if network == "slirp" {
        host.warnings
            .push("slirp allows the guest to reach host loopback services".into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_resources_are_plausible() {
        let info = resources(&env::temp_dir());
        assert!(info.cpus > 0);
        #[cfg(any(windows, target_os = "linux"))]
        assert!(info.mem_total_mb > 0);
        assert!(info.mem_free_mb <= info.mem_total_mb);
    }

    #[test]
    fn ha_requires_a_platform_with_daemon_owned_qemu() {
        assert!(require_ha_support("windows").is_ok());
        assert!(require_ha_support("linux").is_ok());
        let error = require_ha_support("macos").unwrap_err().to_string();
        assert!(error.contains("macos") && error.contains("daemon lifetime"));
        assert!(require_ha_support("freebsd").is_err());
    }

    #[test]
    fn parses_accel_help_and_falls_back_without_kvm_access() {
        let mut host = HostInfo {
            os: "linux".into(),
            qemu_system: Some("qemu-system-x86_64".into()),
            accels: parse_accelerators("Accelerators supported in QEMU binary:\r\nkvm\r\ntcg\r\n"),
            kvm_access: Some(false),
            ..HostInfo::default()
        };
        assert_eq!(host.accels, ["kvm", "tcg"]);
        resolve_accel(&mut host, "auto");
        assert_eq!(host.accel.as_deref(), Some("tcg"));
        assert!(host.warnings.iter().any(|warning| warning.contains("slow")));
        resolve_accel(&mut host, "kvm");
        assert!(
            host.problems
                .iter()
                .any(|problem| problem.contains("/dev/kvm"))
        );
    }

    #[test]
    fn auto_uses_platform_accelerators_and_warns_for_slirp() {
        for (os, accel) in [("linux", "kvm"), ("windows", "whpx"), ("macos", "hvf")] {
            let mut host = HostInfo {
                os: os.into(),
                qemu_system: Some("qemu-system-x86_64".into()),
                accels: vec!["tcg".into(), accel.into()],
                kvm_access: (os == "linux").then_some(true),
                ..HostInfo::default()
            };
            resolve_accel(&mut host, "auto");
            resolve_network(&mut host, "auto");
            assert_eq!(host.accel.as_deref(), Some(accel));
            assert_eq!(host.network, "slirp");
            assert!(host.ready());
            assert_eq!(host.warnings.len(), 1);
        }
    }
}
