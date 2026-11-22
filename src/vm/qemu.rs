//! QEMU process: argv construction, launch, QMP over stdio, power-down.

use std::future::pending;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::{Instant, timeout, timeout_at};
use tokio_util::sync::CancellationToken;

use crate::child_process::{self, ProcessTree};

use super::host::HostInfo;
use super::store::VmRecord;

const PASST_SOCKET: &str = "passt.sock";
const QMP_TIMEOUT: Duration = Duration::from_secs(2);
const QMP_PAUSE_TIMEOUT: Duration = Duration::from_secs(30);
const POWERDOWN_TIMEOUT: Duration = Duration::from_secs(60);

// Paths in QEMU's comma-separated options are relative to the VM directory.
// This also keeps the passt socket address short even in deep state directories.
fn argv(
    record: &VmRecord,
    accel: &str,
    network: &str,
    linux: bool,
    paused: bool,
) -> Result<Vec<String>> {
    let mut args: Vec<String> = [
        "-display",
        "none",
        "-nodefaults",
        "-monitor",
        "none",
        "-qmp",
        "stdio",
        "-chardev",
        "file,id=serial,path=serial.log,append=on",
        "-serial",
        "chardev:serial",
        "-accel",
        accel,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();

    if paused {
        args.push("-S".into());
    }
    // WHPX uses the default CPU model that boots on the stage 1 host.
    if matches!(accel, "kvm" | "hvf") {
        args.extend(["-cpu".into(), "host".into()]);
    }
    args.extend([
        "-m".into(),
        record.memory_mb.to_string(),
        "-smp".into(),
        record.cpus.to_string(),
    ]);
    args.extend(
        [
            "-drive",
            "if=none,id=root,file=disk.qcow2,format=qcow2",
            "-device",
            "virtio-blk-pci,drive=root,bootindex=0",
            "-blockdev",
            "driver=vvfat,node-name=seed,dir=seed,label=cidata,read-only=on",
            "-device",
            "virtio-blk-pci,drive=seed",
        ]
        .into_iter()
        .map(str::to_owned),
    );

    let netdev = match network {
        "passt" => format!("stream,id=n0,server=off,addr.type=unix,addr.path={PASST_SOCKET}"),
        "slirp" | "isolated" => {
            let restrict = if network == "isolated" {
                ",restrict=on"
            } else {
                ""
            };
            format!(
                "user,id=n0{restrict},hostfwd=tcp:127.0.0.1:{}-:22",
                record.ssh_port
            )
        }
        _ => bail!("invalid VM network mode"),
    };
    args.extend([
        "-netdev".into(),
        netdev,
        "-device".into(),
        "virtio-net-pci,netdev=n0".into(),
    ]);
    if linux {
        args.extend([
            "-sandbox".into(),
            "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny".into(),
        ]);
    }
    Ok(args)
}

pub(super) struct Process {
    child: Child,
    tree: ProcessTree,
    qmp: Qmp<ChildStdout, ChildStdin>,
    passt: Option<Passt>,
}

impl Process {
    pub(super) async fn spawn(
        dir: &Path,
        record: &VmRecord,
        host: &HostInfo,
        paused: bool,
    ) -> Result<Self> {
        let executable = host.qemu_system.as_deref().context("QEMU is unavailable")?;
        let accel = host
            .accel
            .as_deref()
            .context("QEMU accelerator is unavailable")?;
        let args = argv(
            record,
            accel,
            &host.network,
            cfg!(target_os = "linux"),
            paused,
        )?;
        let passt = if host.network == "passt" {
            let executable = host.passt.as_deref().context("passt is unavailable")?;
            Some(Passt::spawn(executable, dir, record.ssh_port).await?)
        } else {
            None
        };

        let mut command = Command::new(executable);
        command
            .args(args)
            .current_dir(dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        prepare_vm_process(&mut command);
        let mut child = command.spawn().context("starting QEMU")?;
        let tree = ProcessTree::attach(&mut child)?;
        let output = child.stdout.take().context("QEMU stdout is unavailable")?;
        let input = child.stdin.take().context("QEMU stdin is unavailable")?;
        Ok(Self {
            child,
            tree,
            qmp: Qmp::new(output, input),
            passt,
        })
    }

    pub(super) fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub(super) async fn handshake(&mut self) -> Result<()> {
        timeout(Duration::from_secs(8), self.qmp.handshake())
            .await
            .context("timed out waiting for QEMU's QMP handshake")?
    }

    pub(super) async fn command(&mut self, command: &str) -> Result<()> {
        // QMP stop can wait for in-flight block I/O to flush.
        timeout(QMP_PAUSE_TIMEOUT, self.qmp.execute(command))
            .await
            .with_context(|| format!("timed out waiting for QMP {command}"))?
    }

    pub(super) async fn resume(&mut self, allowed: impl FnOnce() -> bool) -> Result<bool> {
        timeout(QMP_TIMEOUT, self.qmp.execute_if("cont", allowed))
            .await
            .context("timed out waiting for QMP cont")?
    }

    pub(super) async fn wait(&mut self) -> Result<ExitStatus> {
        let passt_exit = async {
            match &mut self.passt {
                Some(passt) => passt.child.wait().await,
                None => pending().await,
            }
        };
        tokio::select! {
            biased;
            result = wait_qemu(&mut self.child, &mut self.qmp) => result,
            result = passt_exit => bail!("passt exited with {}", result.context("waiting for passt")?),
        }
    }

    pub(super) async fn stop(
        &mut self,
        force: &CancellationToken,
        kill: &CancellationToken,
    ) -> Result<()> {
        if !force.is_cancelled() && !kill.is_cancelled() {
            let deadline = Instant::now() + POWERDOWN_TIMEOUT;
            let powerdown = tokio::select! {
                biased;
                _ = kill.cancelled() => return self.kill().await,
                result = timeout(QMP_TIMEOUT, self.qmp.execute("system_powerdown")) => result,
            };
            match powerdown {
                Ok(Ok(())) => tokio::select! {
                    biased;
                    _ = kill.cancelled() => {},
                    _ = force.cancelled() => {},
                    result = timeout_at(deadline, wait_qemu(&mut self.child, &mut self.qmp)) => {
                        match result {
                            Ok(Ok(_)) => return Ok(()),
                            Ok(Err(error)) => tracing::debug!("QEMU ACPI shutdown failed: {error:#}"),
                            Err(_) => tracing::warn!("QEMU ACPI shutdown timed out"),
                        }
                    },
                },
                Ok(Err(error)) => tracing::debug!("QEMU rejected ACPI shutdown: {error:#}"),
                Err(_) => tracing::debug!("QMP system_powerdown reply timed out"),
            }
        }
        if !kill.is_cancelled() {
            let quit = async {
                // QEMU can close QMP before returning its final reply.
                let _ = self.qmp.execute("quit").await;
                wait_qemu(&mut self.child, &mut self.qmp).await
            };
            tokio::select! {
                biased;
                _ = kill.cancelled() => {},
                result = timeout(QMP_TIMEOUT, quit) => {
                    if matches!(result, Ok(Ok(_))) {
                        return Ok(());
                    }
                }
            }
        }
        self.kill().await
    }

    pub(super) async fn kill(&mut self) -> Result<()> {
        terminate(&mut self.child, &mut self.tree)
            .await
            .context("terminating QEMU")
    }

    pub(super) async fn stop_passt(&mut self) {
        if let Some(mut passt) = self.passt.take()
            && let Err(error) = terminate(&mut passt.child, &mut passt.tree).await
        {
            tracing::warn!("could not terminate passt: {error:#}");
        }
    }
}

async fn terminate(child: &mut Child, tree: &mut ProcessTree) -> Result<()> {
    if let Err(error) = tree.kill() {
        tracing::warn!("process tree termination failed: {error:#}");
        child.kill().await?;
    } else {
        child.wait().await?;
    }
    Ok(())
}

fn prepare_vm_process(command: &mut Command) {
    child_process::prepare(command);
    #[cfg(target_os = "linux")]
    {
        let parent = std::process::id() as libc::pid_t;
        // SAFETY: only async-signal-safe Linux syscalls run between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(
                    libc::PR_SET_PDEATHSIG,
                    libc::SIGKILL as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                // Detect a daemon exit before the parent-death signal was installed.
                if libc::getppid() != parent {
                    return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
                }
                Ok(())
            });
        }
    }
}

async fn wait_qemu(
    child: &mut Child,
    qmp: &mut Qmp<ChildStdout, ChildStdin>,
) -> Result<ExitStatus> {
    let status = tokio::select! {
        status = child.wait() => status,
        _ = qmp.drain() => child.wait().await,
    };
    status.context("waiting for QEMU to exit")
}

struct Passt {
    child: Child,
    tree: ProcessTree,
    #[cfg(target_os = "linux")]
    socket: std::path::PathBuf,
}

impl Passt {
    #[cfg(target_os = "linux")]
    async fn spawn(executable: &Path, dir: &Path, port: u16) -> Result<Self> {
        let socket = dir.join(PASST_SOCKET);
        if is_passt_socket(&socket)? {
            std::fs::remove_file(&socket).context("removing stale passt socket")?;
        }
        let mut command = Command::new(executable);
        command
            .current_dir(dir)
            .args([
                "-f",
                "--socket",
                PASST_SOCKET,
                "-t",
                &format!("127.0.0.1/{port}:22"),
                "--no-map-gw",
                "-q",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        prepare_vm_process(&mut command);
        let mut child = command.spawn().context("starting passt")?;
        let tree = ProcessTree::attach(&mut child)?;
        let mut passt = Self {
            child,
            tree,
            socket,
        };

        timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = passt.child.try_wait()? {
                    bail!("passt exited with {status}");
                }
                if is_passt_socket(&passt.socket)? {
                    return Ok(passt);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("timed out waiting for the passt socket")?
    }

    #[cfg(not(target_os = "linux"))]
    async fn spawn(_executable: &Path, _dir: &Path, _port: u16) -> Result<Self> {
        bail!("passt networking requires Linux")
    }
}

#[cfg(target_os = "linux")]
fn is_passt_socket(path: &Path) -> Result<bool> {
    use std::os::unix::fs::FileTypeExt;

    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => Ok(true),
        Ok(_) => bail!("the passt socket path is occupied by a non-socket file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).context("checking the passt socket"),
    }
}

#[cfg(target_os = "linux")]
impl Drop for Passt {
    fn drop(&mut self) {
        let _ = self.tree.kill();
        if matches!(is_passt_socket(&self.socket), Ok(true)) {
            let _ = std::fs::remove_file(&self.socket);
        }
    }
}

struct Qmp<R, W> {
    output: Lines<BufReader<R>>,
    input: W,
    next_id: u64,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Qmp<R, W> {
    fn new(output: R, input: W) -> Self {
        Self {
            output: BufReader::new(output).lines(),
            input,
            next_id: 1,
        }
    }

    async fn read_message(&mut self) -> Result<Value> {
        let line = self
            .output
            .next_line()
            .await?
            .context("QMP closed unexpectedly")?;
        serde_json::from_str(&line).context("invalid QMP message")
    }

    async fn handshake(&mut self) -> Result<()> {
        if self.read_message().await?.get("QMP").is_none() {
            bail!("expected the QMP greeting");
        }
        self.execute("qmp_capabilities").await
    }

    async fn execute(&mut self, command: &str) -> Result<()> {
        self.execute_if(command, || true).await.map(|_| ())
    }

    async fn execute_if(&mut self, command: &str, allowed: impl FnOnce() -> bool) -> Result<bool> {
        let id = self.next_id;
        self.next_id += 1;
        let mut request = serde_json::to_vec(&json!({"execute": command, "id": id}))?;
        request.push(b'\n');
        if !allowed() {
            return Ok(false);
        }
        self.input
            .write_all(&request)
            .await
            .context("writing QMP command")?;
        self.input.flush().await.context("flushing QMP command")?;
        loop {
            let reply = self.read_message().await?;
            if reply.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if reply.get("return").is_some() {
                return Ok(true);
            }
            let error_class = reply
                .pointer("/error/class")
                .and_then(Value::as_str)
                .unwrap_or("invalid reply");
            bail!("QMP {command} failed: {error_class}");
        }
    }

    async fn drain(&mut self) {
        while let Ok(Some(_)) = self.output.next_line().await {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> VmRecord {
        VmRecord {
            server_id: "0123456789abcdef".into(),
            name: "test".into(),
            owner_did: String::new(),
            allowed_dids: vec![],
            memory_mb: 2048,
            cpus: 2,
            disk_gb: 20,
            base_sha256: String::new(),
            base_url: String::new(),
            ssh_port: 22200,
            autostart: false,
            ssh_authorized_keys: vec![],
            created_at: String::new(),
        }
    }

    fn option<'a>(args: &'a [String], name: &str) -> &'a str {
        &args[args.iter().position(|arg| arg == name).unwrap() + 1]
    }

    #[test]
    fn whpx_keeps_default_cpu_and_pins_root_before_seed() {
        let args = argv(&record(), "whpx", "slirp", false, false).unwrap();
        assert!(
            !args
                .iter()
                .any(|arg| matches!(arg.as_str(), "-cpu" | "-sandbox" | "-nographic" | "-S"))
        );
        assert_eq!(option(&args, "-qmp"), "stdio");
        assert_eq!(option(&args, "-monitor"), "none");
        assert_eq!(option(&args, "-m"), "2048");
        assert_eq!(option(&args, "-smp"), "2");
        assert_eq!(
            option(&args, "-device"),
            "virtio-blk-pci,drive=root,bootindex=0"
        );
        assert_eq!(
            option(&args, "-blockdev"),
            "driver=vvfat,node-name=seed,dir=seed,label=cidata,read-only=on"
        );
        assert_eq!(
            option(&args, "-netdev"),
            "user,id=n0,hostfwd=tcp:127.0.0.1:22200-:22"
        );
    }

    #[test]
    fn ha_qemu_starts_paused() {
        let args = argv(&record(), "whpx", "slirp", false, true).unwrap();
        assert!(args.iter().any(|arg| arg == "-S"));
    }

    #[tokio::test]
    async fn qmp_does_not_write_cont_when_lease_check_fails() {
        let (client, server) = tokio::io::duplex(512);
        let (output, input) = tokio::io::split(client);
        let mut qmp = Qmp::new(output, input);
        assert!(!qmp.execute_if("cont", || false).await.unwrap());
        drop(qmp);
        assert!(
            BufReader::new(server)
                .lines()
                .next_line()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn kvm_uses_passt_and_linux_sandbox() {
        let args = argv(&record(), "kvm", "passt", true, false).unwrap();
        assert_eq!(option(&args, "-cpu"), "host");
        assert_eq!(
            option(&args, "-netdev"),
            "stream,id=n0,server=off,addr.type=unix,addr.path=passt.sock"
        );
        assert!(option(&args, "-sandbox").contains("spawn=deny"));
    }

    #[test]
    fn isolated_network_restricts_guest_egress_and_keeps_ssh() {
        let args = argv(&record(), "tcg", "isolated", false, false).unwrap();
        assert_eq!(
            option(&args, "-netdev"),
            "user,id=n0,restrict=on,hostfwd=tcp:127.0.0.1:22200-:22"
        );
    }

    #[tokio::test]
    async fn qmp_matches_replies_by_id_among_events() {
        let (client, server) = tokio::io::duplex(512);
        let (output, input) = tokio::io::split(client);
        let mut qmp = Qmp::new(output, input);
        let fake_qemu = async {
            let (input, mut output) = tokio::io::split(server);
            let mut input = BufReader::new(input).lines();
            output.write_all(b"{\"QMP\":{}}\r\n").await.unwrap();
            let request: Value =
                serde_json::from_str(&input.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(request, json!({"execute":"qmp_capabilities","id":1}));
            output
                .write_all(
                    b"{\"event\":\"RESET\"}\n{\"return\":{},\"id\":99}\n{\"return\":{},\"id\":1}\n",
                )
                .await
                .unwrap();
        };
        let (handshake, ()) = tokio::join!(qmp.handshake(), fake_qemu);
        handshake.unwrap();
    }
}
