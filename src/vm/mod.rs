//! Personal Linux VMs and local encrypted checkpoints.
//!
//! One QEMU guest per server id, booted from a pinned cloud image plus a
//! qcow2 overlay that holds all of the guest's own state. Guest SSH is
//! forwarded to a host loopback port and, when the tunnel is running,
//! published as the tunnel target `vm-<server_id>-ssh` so allowed peers can
//! reach it by server id. QEMU is driven over QMP on its stdio pipes (no
//! management socket the guest could reach), and guests are powered down when
//! the daemon stops.
//!
//! Layout under `<data_dir>/vm/`: `images/<sha256>.img` (base images),
//! `<server_id>/{vm.json,disk.qcow2,seed/,serial.log,vm.key,ckpt/}`, `ssh/` (this
//! instance's client key + known_hosts).

pub(crate) mod checkpoint;
mod ha;
pub mod host;
mod image;
mod net;
mod pool;
mod qemu;
mod seed;
pub(crate) mod sshkey;
mod store;
mod wire;

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, OwnedMutexGuard, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::config::VmConfig;
use crate::daemon::AppState;

use self::checkpoint::{CheckpointStats, CheckpointStore, Manifest};
use self::host::HostInfo;
use self::sshkey::ClientKey;
use self::store::VmRecord;

pub(crate) use net::authorize_vm_target;

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(Registry::default()));
static CREATE_LOCK: Mutex<()> = Mutex::const_new(());
static KEY_LOCK: Mutex<()> = Mutex::const_new(());
type VmLocks = std::sync::Mutex<HashMap<String, Weak<Mutex<()>>>>;
static VM_LOCKS: LazyLock<VmLocks> = LazyLock::new(Default::default);
// Serialize process registration with automatic protection of the same VM.
static VM_START_LOCKS: LazyLock<VmLocks> = LazyLock::new(Default::default);

/// Smallest guest a cloud image boots and updates in reliably: below 1 GiB of RAM
/// cloud-init and apt get OOM-killed, and a few GiB of free disk are needed on top
/// of the base image for package upgrades.
pub const MIN_MEMORY_MB: u32 = 1024;
pub const MIN_DISK_GB: u32 = 8;
const TUNNEL_TIMEOUT: Duration = Duration::from_secs(3);
const LOG_WINDOW: u64 = 2 * 1024 * 1024;
const SERIAL_ROTATE_BYTES: u64 = 8 * 1024 * 1024;
const SERIAL_STOP_BYTES: u64 = 64 * 1024 * 1024;
const SERIAL_CHECK_INTERVAL: Duration = Duration::from_secs(10);
static ANSI_ESCAPE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(concat!(
        r"(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~]",
        r"|(?:\x1B\]|\x9D)[^\x07\x1B\x9C\n]*(?:\x07|\x1B\\|\x9C)",
        r"|\x1B[P^_X][^\x1B\n]*\x1B\\",
        r"|\x1B[ -/]*[0-~]",
    ))
    .expect("valid ANSI escape pattern")
});

#[derive(Default)]
struct Registry {
    running: HashMap<String, Running>,
    last_errors: HashMap<String, String>,
    shutting_down: bool,
}

struct Running {
    memory_mb: u32,
    cpus: u32,
    process: ProcessStatus,
    control: Arc<Control>,
    qmp: mpsc::Sender<QmpRequest>,
}

#[derive(Clone)]
struct ProcessStatus {
    state: &'static str,
    pid: Option<u32>,
    since: Option<Instant>,
    accel: Option<String>,
    network: String,
}

impl ProcessStatus {
    fn new(state: &'static str, host: &HostInfo) -> Self {
        Self {
            state,
            pid: None,
            since: None,
            accel: host.accel.clone(),
            network: host.network.clone(),
        }
    }
}

#[derive(Default)]
struct Control {
    lease: Option<ha::RunLease>,
    stop: CancellationToken,
    force: CancellationToken,
    kill: CancellationToken,
    finished: CancellationToken,
    termination_error: OnceLock<String>,
}

impl Control {
    async fn wait_finished(&self) -> Result<()> {
        self.finished.cancelled().await;
        if let Some(error) = self.termination_error.get() {
            bail!("QEMU termination failed: {error}");
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum QmpAction {
    LeasePause,
    LeaseResume,
    CheckpointPause,
    CheckpointResume,
}

struct QmpRequest {
    action: QmpAction,
    done: oneshot::Sender<Result<()>>,
}

async fn qmp_request(qmp: &mpsc::Sender<QmpRequest>, action: QmpAction) -> Result<()> {
    let (done, result) = oneshot::channel();
    qmp.send(QmpRequest { action, done })
        .await
        .map_err(|_| anyhow::anyhow!("VM supervisor is no longer accepting QMP commands"))?;
    result
        .await
        .context("VM supervisor exited during QMP command")?
}

pub(crate) async fn lock_vm(id: &str) -> Result<OwnedMutexGuard<()>> {
    lock_vm_in(id, &VM_LOCKS).await
}

async fn lock_vm_start(id: &str) -> Result<OwnedMutexGuard<()>> {
    lock_vm_in(id, &VM_START_LOCKS).await
}

async fn lock_vm_in(id: &str, locks: &VmLocks) -> Result<OwnedMutexGuard<()>> {
    ensure!(store::valid_server_id(id), "invalid VM server id");
    let lock = {
        let mut locks = locks.lock().expect("VM operation locks");
        if let Some(lock) = locks.get(id).and_then(Weak::upgrade) {
            lock
        } else {
            locks.retain(|_, lock| lock.strong_count() != 0);
            let lock = Arc::new(Mutex::new(()));
            locks.insert(id.to_owned(), Arc::downgrade(&lock));
            lock
        }
    };
    Ok(lock.lock_owned().await)
}

impl Running {
    fn request_stop(&mut self, force: bool) -> Arc<Control> {
        self.process.state = "stopping";
        if force {
            self.control.force.cancel();
        }
        self.control.stop.cancel();
        self.control.clone()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateArgs {
    name: Option<String>,
    memory_mb: Option<u32>,
    cpus: Option<u32>,
    disk_gb: Option<u32>,
    #[serde(default)]
    ssh_keys: Vec<String>,
    #[serde(default)]
    allow: Vec<String>,
    #[serde(default)]
    autostart: bool,
    start: Option<bool>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
struct VmSize {
    memory_mb: u32,
    cpus: u32,
    disk_gb: u32,
}

impl From<&VmRecord> for VmSize {
    fn from(record: &VmRecord) -> Self {
        Self {
            memory_mb: record.memory_mb,
            cpus: record.cpus,
            disk_gb: record.disk_gb,
        }
    }
}

impl CreateArgs {
    fn size(&self, cfg: &VmConfig, host_limits: bool) -> Result<VmSize> {
        let max = |limit| if host_limits { limit } else { u32::MAX };
        Ok(VmSize {
            memory_mb: resource(
                self.memory_mb,
                cfg.default_memory_mb,
                MIN_MEMORY_MB,
                max(cfg.max_memory_mb),
                "memory_mb",
            )?,
            cpus: resource(self.cpus, cfg.default_cpus, 1, max(cfg.max_cpus), "cpus")?,
            disk_gb: resource(
                self.disk_gb,
                cfg.default_disk_gb,
                MIN_DISK_GB,
                max(cfg.max_disk_gb),
                "disk_gb",
            )?,
        })
    }
}

#[derive(Serialize)]
struct VmStatus<'a> {
    #[serde(flatten)]
    record: &'a VmRecord,
    state: &'static str,
    pid: Option<u32>,
    accel: Option<String>,
    network: String,
    published: bool,
    ssh_target: String,
    host_key_fingerprint: Option<String>,
    uptime_secs: u64,
    last_error: Option<String>,
}

/// IPC entry point for `vm.*`. Contract: see `.agents/vmhost/stage1-spec.md`.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "vm.ha.enable" | "vm.ha.join" | "vm.ha.status" | "vm.ha.disable" | "vm.move" => {
            ha::handle(cmd, args, state).await
        }
        "vm.pool.status" | "vm.pool.create" | "vm.pool.join" | "vm.pool.invite"
        | "vm.pool.leave" => pool::handle(cmd, args, state).await,
        "vm.protect" => {
            let id = server_id(&args)?.to_owned();
            let state = state.clone();
            tokio::spawn(async move { pool::protect(&id, &state).await }).await?
        }
        "vm.doctor" => {
            let host = host::detect(&state.config().vm).await;
            let mut info = serde_json::to_value(&host)?;
            info["ready"] = json!(host.ready());
            Ok(info)
        }
        "vm.key" => {
            let root = store::vm_root()?;
            let key = ensure_client_key(&root).await?;
            Ok(json!({"public_key": key.public, "private_key_path": key.private}))
        }
        "vm.create" => {
            let args = serde_json::from_value(args).context("invalid VM create arguments")?;
            // Provisioning must finish or roll back even if the caller disconnects.
            let state = state.clone();
            tokio::spawn(async move { pool::place_create(args, &state).await }).await?
        }
        "vm.disconnect" => {
            let target = args["target"]
                .as_str()
                .context("vm.disconnect requires a target")?;
            net::disconnect(state, target).await?;
            Ok(json!({"disconnected": true}))
        }
        "vm.list" => {
            let root = store::vm_root()?;
            let records = store::load_all(&root)?;
            let mut vms = statuses(&records, &root, state)
                .await
                .into_iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            ha::list(&mut vms);
            Ok(json!({"vms": vms}))
        }
        "vm.checkpoint" => {
            let id = server_id(&args)?;
            if let Some(group) = ha::group(&store::vm_root()?, id)? {
                return group.checkpoint().await;
            }
            let (.., stats) = create_checkpoint(id, 0, state).await?;
            Ok(serde_json::to_value(stats)?)
        }
        "vm.checkpoints" => list_checkpoints(server_id(&args)?).await,
        "vm.restore" => {
            let seq = args
                .get("seq")
                .filter(|value| !value.is_null())
                .map(|value| {
                    value
                        .as_u64()
                        .filter(|&seq| seq > 0)
                        .context("seq must be a positive integer")
                })
                .transpose()?;
            let id = server_id(&args)?;
            if let Some(group) = ha::group(&store::vm_root()?, id)? {
                ensure!(
                    group.writable(),
                    "VM restore requires the current holder's lease"
                );
            }
            restore(id, seq, state).await
        }
        "vm.status" | "vm.start" | "vm.stop" | "vm.rm" | "vm.logs" | "vm.connect" => {
            let id = server_id(&args)?;
            let root = store::vm_root()?;
            match cmd {
                "vm.start" => {
                    let start = lock_vm_start(id).await?;
                    if let Some(group) = ha::group(&root, id)? {
                        drop(start);
                        group.set_running(true, false).await
                    } else {
                        start_with_lock(&root, id, state, start).await
                    }
                }
                "vm.stop" => {
                    let force = args
                        .get("force")
                        .map(|value| value.as_bool().context("force must be a boolean"))
                        .transpose()?
                        .unwrap_or(false);
                    if let Some(g) = ha::group(&root, id)? {
                        return g.set_running(false, force).await;
                    }
                    let record = stop(&root, id, force).await?;
                    status(&record, &root, state).await
                }
                "vm.rm" => {
                    let _vm = lock_vm(id).await?;
                    let mut registry = REGISTRY.lock().await;
                    ensure!(
                        !registry.running.contains_key(id),
                        "VM {id} must be stopped before removal"
                    );
                    let group = ha::group(&root, id)?;
                    if let Some(g) = &group {
                        ensure!(g.removable(), "disable the VM group before removal");
                    } else {
                        store::load(&root, id)?;
                    }
                    store::remove(&root, id)?;
                    if let Some(g) = group {
                        g.forget();
                    }
                    registry.last_errors.remove(id);
                    Ok(json!({"removed": true}))
                }
                "vm.logs" => {
                    let count = args
                        .get("lines")
                        .map(|value| {
                            value
                                .as_u64()
                                .context("lines must be a non-negative integer")
                        })
                        .transpose()?
                        .unwrap_or(100)
                        .min(2000) as usize;
                    store::load(&root, id)?;
                    let lines =
                        tail_log(&store::vm_dir(&root, id).join("serial.log"), count).await?;
                    Ok(json!({"lines": lines}))
                }
                "vm.connect" => {
                    let node = args
                        .get("node")
                        .filter(|value| !value.is_null())
                        .map(|value| value.as_str().context("node must be a string"))
                        .transpose()?;
                    connect(&root, id, node, state).await
                }
                _ => {
                    let group = ha::group(&root, id)?;
                    let mut row = if group.is_some()
                        && !store::vm_dir(&root, id).join("vm.json").try_exists()?
                    {
                        json!({})
                    } else {
                        status(&store::load(&root, id)?, &root, state).await?
                    };
                    if let Some(g) = group {
                        g.decorate(&mut row);
                    }
                    Ok(row)
                }
            }
        }
        _ => bail!("unknown command: {cmd}"),
    }
}

fn server_id(args: &Value) -> Result<&str> {
    let id = args["id"]
        .as_str()
        .context("VM command requires a server id")?;
    ensure!(store::valid_server_id(id), "invalid VM server id");
    Ok(id)
}

fn usable_host(host: &HostInfo) -> Result<()> {
    ensure!(
        host.ready(),
        "VM host is not ready: {}",
        host.problems.join("; ")
    );
    Ok(())
}

async fn ensure_client_key(root: &Path) -> Result<ClientKey> {
    let keygen = host::find_ssh_tool("ssh-keygen").context("ssh-keygen was not found")?;
    let _lock = KEY_LOCK.lock().await;
    sshkey::ensure_client_key(root, &keygen).await
}

fn resource(value: Option<u32>, default: u32, min: u32, max: u32, name: &str) -> Result<u32> {
    let value = value.unwrap_or(default);
    ensure!(
        min <= max,
        "{name}: the host limit [vm] max_{name} = {max} is below the minimum {min}"
    );
    ensure!(
        (min..=max).contains(&value),
        "{name} must be between {min} and {max}"
    );
    Ok(value)
}

fn validate_ssh_key(key: &str) -> Result<()> {
    ensure!(
        !key.contains(['\r', '\n', '\0', '\u{85}', '\u{2028}', '\u{2029}']),
        "SSH public keys must be single lines"
    );
    let mut fields = key.split_whitespace();
    let kind = fields.next().context("SSH public key is empty")?;
    let blob = fields
        .next()
        .context("SSH public key is missing its encoded data")?;
    let data = base64::engine::general_purpose::STANDARD
        .decode(blob)
        .context("SSH public key data is not valid base64")?;
    ensure!(
        data.starts_with(&(kind.len() as u32).to_be_bytes())
            && data[4..].starts_with(kind.as_bytes())
            && data.len() > 4 + kind.len(),
        "SSH public key type does not match its encoded data"
    );
    Ok(())
}

fn available_ssh_port(base: u16, records: &[VmRecord]) -> Result<u16> {
    let used: HashSet<_> = records.iter().map(|record| record.ssh_port).collect();
    (base.max(1)..=u16::MAX)
        .find(|port| {
            !used.contains(port) && TcpListener::bind((Ipv4Addr::LOCALHOST, *port)).is_ok()
        })
        .context("no free loopback SSH port at or above [vm] ssh_port_base")
}

fn create_directory(root: &Path) -> Result<(String, PathBuf)> {
    let builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    let builder = {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = builder;
        builder.mode(0o700);
        builder
    };
    loop {
        let id = store::new_server_id();
        let dir = store::vm_dir(root, &id);
        match builder.create(&dir) {
            Ok(()) => return Ok((id, dir)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("creating VM directory"),
        }
    }
}

async fn create(
    args: CreateArgs,
    owner_did: Option<String>,
    expected_pool: Option<&pool::PoolState>,
    state: &Arc<AppState>,
) -> Result<Value> {
    let cfg = state.config().vm;
    let VmSize {
        memory_mb,
        cpus,
        disk_gb,
    } = args.size(&cfg, true)?;
    for did in &args.allow {
        crate::identity::pubkey_from_did(did)
            .context("allow contains an invalid Ed25519 did:key")?;
    }
    for key in &args.ssh_keys {
        validate_ssh_key(key)?;
    }
    let root = store::vm_root()?;
    let host = host::detect(&cfg).await;
    usable_host(&host)?;
    if let Some(expected) = expected_pool {
        pool::require_current(expected, state)?;
    }
    ensure!(
        !REGISTRY.lock().await.shutting_down,
        "daemon is shutting down"
    );
    check_create_slots(&root, cfg.max_vms)?;
    let record = {
        let owner_did = match owner_did {
            Some(did) => did,
            None => crate::identity::current(state).await?.did().to_owned(),
        };
        let client_key = ensure_client_key(&root).await?;
        let mut ssh_authorized_keys = args.ssh_keys;
        if !ssh_authorized_keys.contains(&client_key.public) {
            ssh_authorized_keys.push(client_key.public);
        }

        let base = image::ensure_base_image(&cfg, &root).await?;
        ensure!(
            !REGISTRY.lock().await.shutting_down,
            "daemon is shutting down"
        );
        let qemu_img = host
            .qemu_img
            .as_deref()
            .context("qemu-img is unavailable")?;
        // A virtual disk smaller than the image would cut off its root partition.
        let base_bytes = image::virtual_size(qemu_img, &base.path).await?;
        ensure!(
            u64::from(disk_gb) << 30 >= base_bytes,
            "disk_gb must be at least {} for this base image",
            base_bytes.div_ceil(1 << 30)
        );
        let (server_id, dir) = create_directory(&root)?;
        let name = args.name.unwrap_or_else(|| format!("vm-{server_id}"));
        let persist = async {
            image::create_overlay(qemu_img, &base.path, &dir.join("disk.qcow2"), disk_gb).await?;
            seed::write_seed(&dir.join("seed"), &server_id, &name, &ssh_authorized_keys)?;
            let _create = CREATE_LOCK.lock().await;
            if let Some(expected) = expected_pool {
                pool::require_current(expected, state)?;
            }
            check_create_slots(&root, state.config().vm.max_vms)?;
            // Serialize port selection and saving with starts that reassign a port.
            let registry = REGISTRY.lock().await;
            ensure!(!registry.shutting_down, "daemon is shutting down");
            let records = store::load_all_except(&root, Some(&server_id))?;
            let ssh_port = available_ssh_port(cfg.ssh_port_base, &records)?;
            let record = VmRecord {
                server_id: server_id.clone(),
                name,
                owner_did,
                allowed_dids: args.allow,
                memory_mb,
                cpus,
                disk_gb,
                base_sha256: base.sha256,
                base_url: base.url,
                ssh_port,
                autostart: args.autostart,
                ssh_authorized_keys,
                created_at: chrono::Utc::now().to_rfc3339(),
            };
            store::save(&root, &record)?;
            Ok::<_, anyhow::Error>(record)
        };
        match persist.await {
            Ok(record) => record,
            Err(error) => {
                if let Err(cleanup) = store::remove(&root, &server_id) {
                    tracing::warn!(id = %server_id, "VM creation cleanup failed: {cleanup:#}");
                }
                return Err(error).context("provisioning VM");
            }
        }
    };

    let id = &record.server_id;
    // Configure protection before the initial start so a guest never boots
    // standalone and then has to be fenced while the voters join.
    let _initial_start = if let Some(pool) = expected_pool {
        let (done, started) = oneshot::channel();
        pool::auto_protect(id, pool, Some(started), state).await;
        Some(done)
    } else {
        None
    };
    let vm = if args.start.unwrap_or(true) {
        async {
            let start = lock_vm_start(id).await?;
            if let Some(group) = ha::group(&root, id)? {
                drop(start);
                pool::wait_for_lease(&group).await?;
                group.set_running(true, false).await
            } else {
                start_with_host(&root, id, &state.config().vm, host, state, start).await
            }
        }
        .await
        .with_context(|| format!("VM {id} was created but could not be started"))?
    } else {
        serde_json::to_value(status_object(
            &root,
            &record,
            ProcessStatus::new("stopped", &host),
            false,
            None,
        ))?
    };
    Ok(json!({"vm": vm}))
}

fn check_create_slots(root: &Path, max_vms: u32) -> Result<()> {
    ensure!(
        ha::slots(root)? < max_vms as usize,
        "host VM limit ({max_vms}) reached"
    );
    Ok(())
}

fn check_running_budget(registry: &Registry, record: &VmRecord, cfg: &VmConfig) -> Result<()> {
    let memory_mb = registry
        .running
        .values()
        .map(|vm| u64::from(vm.memory_mb))
        .sum::<u64>();
    let cpus = registry
        .running
        .values()
        .map(|vm| u64::from(vm.cpus))
        .sum::<u64>();
    ensure!(
        record.memory_mb > 0
            && memory_mb + u64::from(record.memory_mb) <= u64::from(cfg.max_memory_mb),
        "starting VM would exceed the host memory limit ({} MiB), or VM memory is zero",
        cfg.max_memory_mb
    );
    ensure!(
        record.cpus > 0 && cpus + u64::from(record.cpus) <= u64::from(cfg.max_cpus),
        "starting VM would exceed the host CPU limit ({}), or VM CPUs are zero",
        cfg.max_cpus
    );
    Ok(())
}

async fn start(root: &Path, id: &str, state: &Arc<AppState>) -> Result<Value> {
    let start = lock_vm_start(id).await?;
    start_with_lock(root, id, state, start).await
}

async fn start_with_lock(
    root: &Path,
    id: &str,
    state: &Arc<AppState>,
    start: OwnedMutexGuard<()>,
) -> Result<Value> {
    let cfg = state.config().vm;
    let host = host::detect(&cfg).await;
    start_with_host(root, id, &cfg, host, state, start).await
}

async fn start_with_host(
    root: &Path,
    id: &str,
    cfg: &VmConfig,
    host: HostInfo,
    state: &Arc<AppState>,
    start: OwnedMutexGuard<()>,
) -> Result<Value> {
    let _vm = lock_vm(id).await?;
    let registry = REGISTRY.lock().await;
    let group = ha::group(root, id)?;
    ensure!(
        group.as_ref().is_none_or(|g| g.writable()),
        "VM start requires the current holder's lease"
    );
    ensure!(!registry.shutting_down, "daemon is shutting down");
    let mut record = store::load(root, id)?;
    if let Some(running) = registry.running.get(id) {
        ensure!(
            running.process.state == "running",
            "VM {id} is {}",
            running.process.state
        );
        drop(registry);
        drop(start);
        return status(&record, root, state).await;
    }
    drop(registry);
    let dir = store::vm_dir(root, id);
    tokio::task::spawn_blocking({
        let dir = dir.clone();
        move || host::fence_previous_qemu(&dir)
    })
    .await??;
    let mut registry = REGISTRY.lock().await;
    ensure!(!registry.shutting_down, "daemon is shutting down");
    let lease = group
        .map(|g| {
            g.run_lease()
                .context("VM start requires the current holder's lease")
        })
        .transpose()?;
    usable_host(&host)?;
    check_running_budget(&registry, &record, cfg)?;
    if record.ssh_port == 0 || TcpListener::bind((Ipv4Addr::LOCALHOST, record.ssh_port)).is_err() {
        record.ssh_port = available_ssh_port(cfg.ssh_port_base, &store::load_all(root)?)?;
        store::save(root, &record)?;
    }
    rotate_serial_log(&dir)?;
    registry.last_errors.remove(id);
    let control = Arc::new(Control {
        lease,
        ..Control::default()
    });
    let (qmp, requests) = mpsc::channel(4);
    registry.running.insert(
        id.to_owned(),
        Running {
            memory_mb: record.memory_mb,
            cpus: record.cpus,
            process: ProcessStatus::new("starting", &host),
            control: control.clone(),
            qmp,
        },
    );
    let (started, result) = oneshot::channel();
    lifecycle::spawn(
        record.clone(),
        dir,
        host,
        state.clone(),
        control,
        requests,
        started,
    );
    drop(registry);
    // Automatic protection can now see the starting process.
    drop(start);
    result
        .await
        .context("VM supervisor exited during startup")??;
    status(&record, root, state).await
}

fn rotate_serial_log(dir: &Path) -> Result<()> {
    let serial = dir.join("serial.log");
    match std::fs::metadata(&serial) {
        Ok(metadata) if metadata.len() > SERIAL_ROTATE_BYTES => {
            std::fs::rename(serial, dir.join("serial.log.1")).context("rotating VM serial log")
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("checking VM serial log before startup"),
    }
}

async fn wait_for_serial_log_limit(serial: &Path) {
    loop {
        tokio::time::sleep(SERIAL_CHECK_INTERVAL).await;
        if tokio::fs::metadata(serial)
            .await
            .is_ok_and(|metadata| metadata.len() > SERIAL_STOP_BYTES)
        {
            return;
        }
    }
}

mod lifecycle {
    use super::*;

    pub(super) fn spawn(
        record: VmRecord,
        dir: PathBuf,
        host: HostInfo,
        state: Arc<AppState>,
        control: Arc<Control>,
        requests: mpsc::Receiver<QmpRequest>,
        started: oneshot::Sender<Result<()>>,
    ) {
        // The tunnel calls back through the dispatcher. Check the supervisor's
        // Send bound outside the module that defines the VM handler future.
        tokio::spawn(supervise(
            record, dir, host, state, control, requests, started,
        ));
    }
}

async fn supervise(
    record: VmRecord,
    dir: PathBuf,
    host: HostInfo,
    state: Arc<AppState>,
    control: Arc<Control>,
    mut requests: mpsc::Receiver<QmpRequest>,
    started: oneshot::Sender<Result<()>>,
) {
    let id = &record.server_id;
    let mut started = Some(started);
    let mut process = None;
    let mut registered = false;
    let result = async {
        let root = dir.parent().context("missing VM data directory")?;
        let can_continue = || {
            if control.kill.is_cancelled() || control.stop.is_cancelled() {
                return false;
            }
            match ha::group(root, id) {
                Ok(Some(group)) => group.can_resume(control.lease),
                Ok(None) => control.lease.is_none(),
                Err(_) => false,
            }
        };
        let key = ensure_client_key(root).await?;
        let mut keys = record.ssh_authorized_keys.clone();
        keys.push(key.public);
        let mut seen = HashSet::new();
        keys.retain(|key| seen.insert(key.clone()));
        seed::write_seed(&dir.join("seed"), id, &record.name, &keys)?;
        process = Some(tokio::select! {
            biased;
            _ = control.kill.cancelled() => bail!("VM start was fenced"),
            _ = control.stop.cancelled() => bail!("VM start was cancelled"),
            process = qemu::Process::spawn(&dir, &record, &host, control.lease.is_some()) => process?,
        });
        let child = process.as_mut().unwrap();
        host::remember_qemu(&dir, child.pid())?;
        tokio::select! {
            biased;
            _ = control.kill.cancelled() => bail!("VM start was fenced"),
            _ = control.stop.cancelled() => bail!("VM start was cancelled"),
            result = child.handshake() => result?,
        }

        let group = ha::group(root, id)?;
        let mut lease_paused = false;
        if control.lease.is_some() {
            lease_paused = !tokio::select! {
                biased;
                _ = control.kill.cancelled() => bail!("VM start was fenced"),
                _ = control.stop.cancelled() => bail!("VM start was cancelled"),
                resumed = child.resume(can_continue) => resumed?,
            };
        }
        registered = true;
        if group.is_none() {
            net::register_record(&record);
        }
        if group.is_none() && state.network.permitted() && !control.stop.is_cancelled() {
            let publication = tokio::time::timeout(
                TUNNEL_TIMEOUT,
                net::publish_ssh(&state, id, record.ssh_port),
            );
            let published = tokio::select! {
                biased;
                _ = control.kill.cancelled() => bail!("VM start was fenced"),
                exit = child.wait() => bail!("QEMU exited during startup with {}", exit?),
                _ = control.stop.cancelled() => bail!("VM start was cancelled"),
                published = publication => published,
            };
            match published {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => tracing::warn!(%id, "VM SSH publication failed: {error:#}"),
                Err(_) => tracing::warn!(%id, "VM SSH publication timed out"),
            }
        }

        {
            let mut registry = REGISTRY.lock().await;
            ensure!(!control.kill.is_cancelled(), "VM start was fenced");
            ensure!(!control.stop.is_cancelled(), "VM start was cancelled");
            let status = &mut registry
                .running
                .get_mut(id)
                .expect("VM reservation exists")
                .process;
            status.state = if lease_paused { "paused" } else { "running" };
            status.pid = child.pid();
            status.since = Some(Instant::now());
        }
        if let Some(group) = group { group.schedule_fence(true); }
        let _ = started.take().unwrap().send(Ok(()));
        let serial = dir.join("serial.log");
        let mut checkpoint_paused = false;
        let serial_limit = wait_for_serial_log_limit(&serial);
        tokio::pin!(serial_limit);
        loop {
            tokio::select! {
                biased;
                _ = control.kill.cancelled() => break Ok(true),
                _ = control.stop.cancelled() => break Ok(true),
                result = child.wait() => {
                    let exit = result?;
                    if !exit.success() {
                        tracing::warn!(%id, "QEMU exited with {exit}");
                    }
                    break Ok(false);
                }
                _ = &mut serial_limit => {
                    let reason = "VM serial log exceeded 64 MiB";
                    {
                        let mut registry = REGISTRY.lock().await;
                        registry
                            .running
                            .get_mut(id)
                            .expect("VM reservation exists")
                            .process
                            .state = "stopping";
                        registry.last_errors.insert(id.clone(), reason.to_owned());
                    }
                    tracing::warn!(%id, "{reason}");
                    child.kill().await?;
                    break Ok(false);
                }
                Some(request) = requests.recv() => {
                    let command = match request.action {
                        QmpAction::LeasePause => {
                            lease_paused = true;
                            Some("stop")
                        }
                        QmpAction::LeaseResume => {
                            lease_paused = !can_continue();
                            (!lease_paused && !checkpoint_paused).then_some("cont")
                        }
                        QmpAction::CheckpointPause => {
                            checkpoint_paused = true;
                            Some("stop")
                        }
                        QmpAction::CheckpointResume => {
                            checkpoint_paused = false;
                            (!lease_paused).then_some("cont")
                        }
                    };
                    let result = if let Some(command) = command {
                        tokio::select! {
                            biased;
                            _ = control.kill.cancelled() => break Ok(true),
                            _ = control.stop.cancelled() => break Ok(true),
                            result = async {
                                if command == "cont" {
                                    let resumed = child.resume(can_continue).await?;
                                    lease_paused = !resumed;
                                    Ok(())
                                } else {
                                    child.command(command).await
                                }
                            } => result,
                        }
                    } else {
                        Ok(())
                    };
                    if result.is_ok() && (lease_paused || matches!(request.action, QmpAction::LeaseResume)) {
                        let mut registry = REGISTRY.lock().await;
                        if !control.stop.is_cancelled()
                            && let Some(vm) = registry.running.get_mut(id)
                        {
                            vm.process.state = if lease_paused { "paused" } else { "running" };
                        }
                    }
                    let _ = request.done.send(result);
                }
            }
        }
    }
    .await;
    drop(requests);

    REGISTRY
        .lock()
        .await
        .running
        .get_mut(id)
        .expect("VM reservation exists")
        .process
        .state = "stopping";
    if registered {
        // Close the authorization gate even if the tunnel is already gone.
        net::unregister_acl(id);
    }
    let unpublish = async {
        if registered && state.network.permitted() {
            match tokio::time::timeout(TUNNEL_TIMEOUT, net::unpublish_ssh(&state, id)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%id, "VM SSH removal failed: {error:#}"),
                Err(_) => tracing::warn!(%id, "VM SSH removal timed out"),
            }
        }
    };
    let terminate = async {
        if let Some(mut child) = process {
            let terminated = match &result {
                Ok(true) => child.stop(&control.force, &control.kill).await,
                Ok(false) => Ok(()),
                Err(_) => child.kill().await,
            };
            if let Err(error) = terminated {
                tracing::warn!(%id, "QEMU termination failed: {error:#}");
                let _ = control.termination_error.set(format!("{error:#}"));
            }
            child.stop_passt().await;
        }
    };
    tokio::join!(unpublish, terminate);
    REGISTRY.lock().await.running.remove(id);
    control.finished.cancel();
    if let Err(error) = result {
        if let Some(started) = started {
            let _ = started.send(Err(error));
        } else {
            tracing::warn!(%id, "VM process stopped: {error:#}");
        }
    }
}

async fn stop(root: &Path, id: &str, force: bool) -> Result<VmRecord> {
    let _vm = lock_vm(id).await?;
    let (record, control) = {
        let mut registry = REGISTRY.lock().await;
        let record = store::load(root, id)?;
        let control = registry
            .running
            .get_mut(id)
            .map(|vm| vm.request_stop(force));
        (record, control)
    };
    if let Some(control) = control {
        control.wait_finished().await?;
    }
    Ok(record)
}

// Fencing must be able to take effect while a checkpoint is reading the overlay.
async fn active_qmp(id: &str) -> Result<Option<(mpsc::Sender<QmpRequest>, bool)>> {
    ensure!(store::valid_server_id(id), "invalid VM server id");
    let registry = REGISTRY.lock().await;
    let Some(vm) = registry.running.get(id) else {
        return Ok(None);
    };
    ensure!(
        matches!(vm.process.state, "running" | "paused"),
        "VM {id} is {}",
        vm.process.state
    );
    Ok(Some((vm.qmp.clone(), vm.process.state == "paused")))
}

#[allow(dead_code)] // Used by lease fencing.
pub(crate) async fn kill(id: &str) -> Result<()> {
    ensure!(store::valid_server_id(id), "invalid VM server id");
    let control = REGISTRY.lock().await.running.get_mut(id).map(|vm| {
        vm.control.kill.cancel();
        vm.request_stop(true)
    });
    if let Some(control) = control {
        control.wait_finished().await?;
    }
    Ok(())
}

async fn sync_guest(root: &Path, record: &VmRecord) -> Result<()> {
    let key = root.join("ssh").join("id_ed25519");
    let connection = json!({
        "port": record.ssh_port,
        "user": "mistl",
        "key": key,
        "known_hosts": sshkey::known_hosts_path(root),
        "host_key_alias": sshkey::host_key_alias(&record.server_id),
    });
    let options = ["-n", "-T", "-o", "BatchMode=yes"];
    let command = sshkey::ssh_command(&connection, &options, &["sync".into()])?;
    let mut command = tokio::process::Command::from(command);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    crate::child_process::prepare(&mut command);
    let status = command
        .status()
        .await
        .context("running guest sync over SSH")?;
    ensure!(status.success(), "guest sync exited with {status}");
    Ok(())
}

/// Quiesce and checkpoint a VM. The task owns the resume even if its caller disconnects.
pub(crate) async fn create_checkpoint(
    id: &str,
    epoch: u64,
    state: &Arc<AppState>,
) -> Result<(Manifest, CheckpointStats)> {
    let id = id.to_owned();
    let keep = state.config().vm.checkpoint_keep;
    tokio::spawn(async move {
        let _vm = lock_vm(&id).await?;
        let root = store::vm_root()?;
        let group = ha::group(&root, &id)?;
        if let Some(g) = &group {
            ensure!(
                epoch != 0 && g.writable(),
                "VM checkpoint requires the current holder's lease"
            );
        }
        let record = store::load(&root, &id)?;
        let active = active_qmp(&id).await?;
        if matches!(active, Some((_, false))) {
            match tokio::time::timeout(Duration::from_secs(10), sync_guest(&root, &record)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::debug!(%id, "VM checkpoint guest sync failed: {error:#}")
                }
                Err(_) => tracing::debug!(%id, "VM checkpoint guest sync timed out"),
            }
        }

        let known_hosts = sshkey::export_known_hosts(&root, &id)?;
        checkpoint::load_or_create_key(&root, &id)?;
        let checkpoints = CheckpointStore::open(&root, &id)?;
        let mut manifest = checkpoints.prepare(record, epoch, known_hosts)?;
        if let Some(g) = &group {
            manifest.parent_seq = g.checkpoint_parent();
        }
        if epoch > 0 {
            // Old holders may retain unreplicated sequences. Give each term a
            // disjoint range, while retaining the actual parent in the manifest.
            ensure!(
                epoch <= u64::from(u32::MAX),
                "VM checkpoint epoch exhausted"
            );
            manifest.seq = manifest.seq.max(((epoch - 1) << 32) + 1);
            ensure!(
                manifest.seq >> 32 == epoch - 1,
                "VM checkpoint epoch range exhausted"
            );
        }
        let disk = store::vm_dir(&root, &id).join("disk.qcow2");
        let pause_at = Instant::now();
        let captured = async {
            if let Some((qmp, _)) = &active {
                qmp_request(qmp, QmpAction::CheckpointPause).await?;
            }
            tokio::task::spawn_blocking(move || {
                let stats = checkpoints.capture(&disk, &mut manifest)?;
                Ok::<_, anyhow::Error>((checkpoints, manifest, stats))
            })
            .await?
        }
        .await;

        let pause_ms = if let Some((qmp, _)) = &active {
            let resumed = qmp_request(qmp, QmpAction::CheckpointResume).await;
            if let Err(error) = resumed {
                let context = format!("releasing VM checkpoint pause failed: {error:#}");
                return match captured {
                    Ok(_) => Err(error).context("releasing VM checkpoint pause"),
                    Err(capture) => Err(capture).context(context),
                };
            }
            pause_at.elapsed().as_millis() as u64
        } else {
            0
        };
        let (checkpoints, manifest, mut stats) = captured?;
        stats.pause_ms = pause_ms;
        tokio::task::spawn_blocking(move || {
            checkpoints.commit(&manifest)?;
            prune_checkpoints(&checkpoints, &manifest.server_id, keep, group.as_deref());
            Ok((manifest, stats))
        })
        .await?
    })
    .await?
}

fn prune_checkpoints(
    checkpoints: &CheckpointStore,
    id: &str,
    keep: usize,
    group: Option<&ha::Group>,
) {
    let result = match group {
        Some(group) => group.prune_checkpoints(checkpoints, keep),
        None => checkpoints.prune(keep, |_| false),
    };
    if let Err(error) = result {
        tracing::warn!(%id, "VM checkpoint retention failed: {error:#}");
    }
}

async fn list_checkpoints(id: &str) -> Result<Value> {
    let _vm = lock_vm(id).await?;
    let root = store::vm_root()?;
    if ha::group(&root, id)?.is_none() {
        store::load(&root, id)?;
    }
    if !store::vm_dir(&root, id).join("ckpt").try_exists()? {
        return Ok(json!({"checkpoints": []}));
    }
    let id = id.to_owned();
    tokio::task::spawn_blocking(move || {
        let checkpoints = CheckpointStore::open(&root, &id)?;
        let entries = checkpoints
            .sequences()?
            .into_iter()
            .map(|seq| {
                let manifest = checkpoints.load_manifest(seq)?;
                Ok(json!({
                    "seq": seq, "epoch": manifest.epoch, "created_at": manifest.created_at,
                    "disk_bytes": manifest.disk_bytes,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(json!({"checkpoints": entries}))
    })
    .await?
}

struct RestoreFile(PathBuf);

impl Drop for RestoreFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Restore a stopped VM from an authenticated, complete local checkpoint.
pub(crate) async fn restore(id: &str, seq: Option<u64>, state: &Arc<AppState>) -> Result<Value> {
    let id = id.to_owned();
    let state = state.clone();
    tokio::spawn(async move {
        let _vm = lock_vm(&id).await?;
        let root = store::vm_root()?;
        restore_stopped(&root, &id, seq, None, &state).await
    })
    .await?
}

async fn restore_elected(group: &Arc<ha::Group>, epoch: u64, state: &Arc<AppState>) -> Result<u64> {
    let _vm = lock_vm(&group.id).await?;
    let seq = tokio::task::spawn_blocking({
        let group = group.clone();
        move || group.newest_restore(&CheckpointStore::open(&group.root, &group.id)?, epoch)
    })
    .await??;
    restore_stopped(&group.root, &group.id, Some(seq), Some(epoch), state).await?;
    Ok(seq)
}

async fn restore_stopped(
    root: &Path,
    id: &str,
    seq: Option<u64>,
    election_epoch: Option<u64>,
    state: &Arc<AppState>,
) -> Result<Value> {
    let group = ha::group(root, id)?;
    if let Some(g) = &group {
        ensure!(
            match election_epoch {
                Some(epoch) => g.may_restore(epoch, seq),
                None => g.writable(),
            },
            "VM restore requires the current holder's lease"
        );
    }
    ensure!(
        !REGISTRY.lock().await.running.contains_key(id),
        "VM {id} must be stopped before restore"
    );
    let dir = store::vm_dir(root, id);
    ensure!(dir.join("ckpt").try_exists()?, "VM {id} has no checkpoints");
    let checkpoints = CheckpointStore::open(root, id)?;
    let seq = match seq {
        Some(seq) => seq,
        None => checkpoints
            .sequences()?
            .first()
            .copied()
            .context("VM has no checkpoints")?,
    };
    let mut previous = None;
    if dir.join("vm.json").try_exists()? {
        previous = Some(store::load(root, id)?);
    }
    let cfg = state.config().vm;
    let host = host::detect(&cfg).await;
    let qemu_img = host
        .qemu_img
        .as_deref()
        .context("qemu-img is unavailable")?;
    let (checkpoints, manifest) = tokio::task::spawn_blocking(move || {
        let manifest = checkpoints.load_manifest(seq)?;
        Ok::<_, anyhow::Error>((checkpoints, manifest))
    })
    .await??;
    let base = image::ensure_base_by_hash(
        root,
        &manifest.base_sha256,
        &manifest.base_url,
        state.network.permitted(),
    )
    .await?;

    let temp = RestoreFile(dir.join("disk.qcow2.restore"));
    match std::fs::remove_file(&temp.0) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("removing stale temporary restored VM overlay"),
    }
    let (checkpoints, manifest, temp) = tokio::task::spawn_blocking(move || {
        checkpoints.reconstruct(&manifest, &temp.0)?;
        Ok::<_, anyhow::Error>((checkpoints, manifest, temp))
    })
    .await??;
    image::rebase_unsafe(qemu_img, &base.path, &temp.0).await?;
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(&temp.0)
        .await?
        .sync_all()
        .await
        .context("syncing rebased VM overlay")?;
    sshkey::merge_known_hosts(root, id, &manifest.known_hosts)?;

    let mut record = manifest.record;
    {
        let mut registry = REGISTRY.lock().await;
        ensure!(!registry.shutting_down, "daemon is shutting down");
        if let Some(epoch) = election_epoch {
            ensure!(
                group
                    .as_ref()
                    .is_some_and(|g| g.may_restore(epoch, Some(seq))),
                "VM lease changed during restore"
            );
        }
        record.ssh_port =
            available_ssh_port(cfg.ssh_port_base, &store::load_all_except(root, Some(id))?)?;
        store::save(root, &record)?;
        if let Err(error) = std::fs::rename(&temp.0, dir.join("disk.qcow2")) {
            let rollback = match previous {
                Some(previous) => store::save(root, &previous),
                None => std::fs::remove_file(dir.join("vm.json")).map_err(Into::into),
            };
            rollback.with_context(|| {
                format!("restoring old VM record after overlay replacement failed: {error}")
            })?;
            return Err(error).context("publishing restored VM overlay");
        }
        registry.last_errors.remove(id);
    }

    let id = id.to_owned();
    tokio::task::spawn_blocking(move || {
        prune_checkpoints(&checkpoints, &id, cfg.checkpoint_keep, group.as_deref())
    })
    .await?;
    status(&record, root, state).await
}

fn status_object<'a>(
    root: &Path,
    record: &'a VmRecord,
    process: ProcessStatus,
    published: bool,
    last_error: Option<String>,
) -> VmStatus<'a> {
    let id = &record.server_id;
    let serial = store::vm_dir(root, id).join("serial.log");
    VmStatus {
        record,
        state: process.state,
        pid: process.pid,
        accel: process.accel,
        network: process.network,
        published,
        ssh_target: net::ssh_target(id),
        host_key_fingerprint: sshkey::host_fingerprint_from_serial(&serial).or_else(|| {
            sshkey::host_fingerprint_from_serial(&serial.with_file_name("serial.log.1"))
        }),
        uptime_secs: process.since.map_or(0, |since| since.elapsed().as_secs()),
        last_error,
    }
}

async fn statuses<'a>(
    records: &'a [VmRecord],
    root: &Path,
    state: &Arc<AppState>,
) -> Vec<VmStatus<'a>> {
    let entries: Vec<_> = {
        let registry = REGISTRY.lock().await;
        records
            .iter()
            .map(|record| {
                let process = registry
                    .running
                    .get(&record.server_id)
                    .map(|vm| vm.process.clone());
                let last_error = registry.last_errors.get(&record.server_id).cloned();
                (process, last_error)
            })
            .collect()
    };
    let stopped = async {
        if entries.iter().any(|(process, _)| process.is_none()) {
            let host = host::detect(&state.config().vm).await;
            ProcessStatus::new("stopped", &host)
        } else {
            ProcessStatus::new("stopped", &HostInfo::default())
        }
    };
    let tunnel = async {
        if entries.iter().any(|(process, _)| process.is_some()) && state.network.permitted() {
            if let Ok(Ok(status)) = tokio::time::timeout(
                TUNNEL_TIMEOUT,
                crate::tunnel::handle("tunnel.status", json!({}), state),
            )
            .await
            {
                return status;
            }
        }
        Value::Null
    };
    let (stopped, tunnel) = tokio::join!(stopped, tunnel);
    records
        .iter()
        .zip(entries)
        .map(|(record, (process, last_error))| {
            let target = net::ssh_target(&record.server_id);
            let addr = format!("127.0.0.1:{}", record.ssh_port);
            let published = process.is_some()
                && tunnel["running"] == true
                && tunnel["forwards"].as_array().is_some_and(|forwards| {
                    forwards.iter().any(|forward| {
                        forward["target"] == target
                            && forward["addr"] == addr
                            && forward["direction"] == "serve"
                            && forward["state"] == "listening"
                    })
                });
            status_object(
                root,
                record,
                process.unwrap_or_else(|| stopped.clone()),
                published,
                last_error,
            )
        })
        .collect()
}

async fn status(record: &VmRecord, root: &Path, state: &Arc<AppState>) -> Result<Value> {
    let statuses = statuses(std::slice::from_ref(record), root, state).await;
    Ok(serde_json::to_value(&statuses[0])?)
}

async fn tail_log(path: &Path, lines: usize) -> Result<Vec<String>> {
    if lines == 0 {
        return Ok(Vec::new());
    }
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).context("opening VM serial log"),
    };
    let length = file.metadata().await?.len();
    let offset = length.saturating_sub(LOG_WINDOW);
    file.seek(std::io::SeekFrom::Start(offset.saturating_sub(1)))
        .await?;
    let mut data = Vec::new();
    file.take(LOG_WINDOW + 1).read_to_end(&mut data).await?;
    let data = if offset != 0 && !data.is_empty() {
        // Keep the suffix if a single line fills the whole window.
        let start = data[..data.len() - 1]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(1, |index| index + 1);
        &data[start..]
    } else {
        &data
    };
    Ok(log_lines(data, lines))
}

fn log_lines(data: &[u8], count: usize) -> Vec<String> {
    let text = String::from_utf8_lossy(data);
    let mut lines: Vec<_> = text
        .lines()
        .rev()
        .take(count)
        .map(|line| ANSI_ESCAPE.replace_all(line, "").into_owned())
        .collect();
    lines.reverse();
    lines
}

async fn local_ssh_port(root: &Path, id: &str) -> Result<Option<u16>> {
    let registry = REGISTRY.lock().await;
    match store::load(root, id) {
        Ok(record) => {
            ensure!(
                registry
                    .running
                    .get(id)
                    .is_some_and(|vm| vm.process.state == "running"),
                "VM is not running"
            );
            Ok(Some(record.ssh_port))
        }
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == ErrorKind::NotFound) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

async fn connect(
    root: &Path,
    id: &str,
    node: Option<&str>,
    state: &Arc<AppState>,
) -> Result<Value> {
    let group = ha::group(root, id)?;
    let holder = group.as_ref().map(|g| g.holder());
    let remote = group
        .as_ref()
        .is_some_and(|g| node.or(holder.as_deref()) != Some(g.me.as_str()));
    let node = node.or(if remote { holder.as_deref() } else { None });
    let port = if remote {
        None
    } else {
        local_ssh_port(root, id).await?
    };
    let node = node.filter(|node| !node.trim().is_empty());
    ensure!(
        port.is_some() || node.is_some(),
        "remote VM SSH requires --node <executor node id>"
    );
    let key = ensure_client_key(root).await?;
    let mut result = match (port, node) {
        (Some(port), _) => json!({"port": port, "local": true}),
        (None, Some(node)) => {
            let mut forward = net::connect(state, id, node).await?;
            forward["local"] = json!(false);
            forward
        }
        (None, None) => unreachable!("VM SSH requires a local port or remote node"),
    };
    result["user"] = json!("mistl");
    result["key"] = json!(key.private);
    result["known_hosts"] = json!(sshkey::known_hosts_path(root));
    result["host_key_alias"] = json!(sshkey::host_key_alias(id));
    Ok(result)
}

/// Starts VMs marked `autostart` when `[vm] enabled` is set.
pub fn spawn_background(state: Arc<AppState>) {
    if let Err(error) = ha::load(&state) {
        tracing::warn!(%error, "could not load VM groups");
    }
    pool::spawn_background(state.clone());
    if !state.config().vm.enabled {
        return;
    }
    tokio::spawn(async move {
        let result =
            store::vm_root().and_then(|root| store::load_all(&root).map(|records| (root, records)));
        match result {
            Ok((root, records)) => {
                for record in records.into_iter().filter(|record| record.autostart) {
                    let id = &record.server_id;
                    let Ok(start) = lock_vm_start(id).await else {
                        continue;
                    };
                    if !matches!(ha::group(&root, id), Ok(None)) {
                        continue;
                    }
                    if let Err(error) = start_with_lock(&root, id, &state, start).await {
                        tracing::warn!(%id, "VM autostart failed: {error:#}");
                    }
                }
            }
            Err(error) => tracing::warn!("could not load VMs for autostart: {error:#}"),
        }
    });
}

/// ACPI-powers-down every running guest (bounded wait), then kills leftovers.
pub async fn shutdown_all() {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let controls: Vec<_> = {
        let mut registry = REGISTRY.lock().await;
        registry.shutting_down = true;
        registry
            .running
            .values_mut()
            .map(|vm| vm.request_stop(false))
            .collect()
    };
    let wait = || {
        futures_util::future::join_all(controls.iter().map(|control| control.finished.cancelled()))
    };
    if tokio::time::timeout_at(deadline - Duration::from_secs(3), wait())
        .await
        .is_ok()
    {
        return;
    }
    for control in &controls {
        control.force.cancel();
    }
    if tokio::time::timeout_at(deadline - Duration::from_secs(1), wait())
        .await
        .is_err()
    {
        for control in &controls {
            control.kill.cancel();
        }
        let _ = tokio::time::timeout_at(deadline, wait()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_tail_keeps_order_and_strips_ansi() {
        let input = b"discard\r\n\x1b[31mred\x1b[0m\n\x1b]0;title\x07last\n";
        assert_eq!(log_lines(input, 2), ["red", "last"]);
        assert!(log_lines(input, 0).is_empty());
    }

    #[test]
    fn serial_rotation_keeps_one_log_and_only_runs_above_the_limit() {
        let dir = std::env::temp_dir().join(format!("mistl-vm-serial-{}", store::new_server_id()));
        std::fs::create_dir(&dir).unwrap();
        let serial = dir.join("serial.log");
        let previous = dir.join("serial.log.1");

        rotate_serial_log(&dir).unwrap();
        std::fs::write(&previous, b"old").unwrap();
        let file = std::fs::File::create(&serial).unwrap();
        file.set_len(SERIAL_ROTATE_BYTES).unwrap();
        drop(file);
        rotate_serial_log(&dir).unwrap();
        assert_eq!(std::fs::read(&previous).unwrap(), b"old");

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&serial)
            .unwrap();
        file.set_len(SERIAL_ROTATE_BYTES + 1).unwrap();
        drop(file);
        rotate_serial_log(&dir).unwrap();
        assert!(!serial.exists());
        assert_eq!(
            std::fs::metadata(&previous).unwrap().len(),
            SERIAL_ROTATE_BYTES + 1
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn resources_validate_defaults_and_explicit_values() {
        assert_eq!(resource(None, 4, 1, 4, "cpus").unwrap(), 4);
        assert_eq!(resource(Some(1), 4, 1, 4, "cpus").unwrap(), 1);
        assert!(resource(Some(0), 4, 1, 4, "cpus").is_err());
        assert!(resource(None, 4, 1, 3, "cpus").is_err());
        assert_eq!(resource(Some(8), 20, 8, 200, "disk_gb").unwrap(), 8);
        assert!(resource(Some(1), 20, 8, 200, "disk_gb").is_err());
        assert!(resource(Some(512), 2048, 1024, 8192, "memory_mb").is_err());
        assert!(resource(None, 4, 8, 4, "disk_gb").is_err());
    }

    #[test]
    fn create_arguments_do_not_accept_an_owner_override() {
        let args: CreateArgs = serde_json::from_value(json!({"name": "guest"})).unwrap();
        let mut cfg = VmConfig::default();
        cfg.default_memory_mb = MIN_MEMORY_MB * 2;
        cfg.max_memory_mb = MIN_MEMORY_MB;
        assert_eq!(
            args.size(&cfg, false).unwrap().memory_mb,
            cfg.default_memory_mb
        );
        assert!(args.size(&cfg, true).is_err());
        assert!(serde_json::from_value::<CreateArgs>(json!({"owner_did": "override"})).is_err());
    }

    #[test]
    fn checkpoint_retention_validates_through_config_set() {
        let config = crate::config::Config::default();
        assert_eq!(config.vm.checkpoint_keep, 3);
        for keep in [1, 16] {
            let updated =
                crate::config::set_by_path(&config, "vm.checkpoint_keep", json!(keep)).unwrap();
            assert_eq!(updated.vm.checkpoint_keep, keep);
        }
        for invalid in [json!(0), json!(17), json!(-1), json!("3")] {
            assert!(crate::config::set_by_path(&config, "vm.checkpoint_keep", invalid).is_err());
        }
        let config: VmConfig = serde_json::from_value(json!({"enabled": false})).unwrap();
        assert_eq!(config.checkpoint_keep, 3);
    }

    #[test]
    fn starting_and_stopping_vms_hold_their_resource_reservations() {
        let cfg = VmConfig {
            max_memory_mb: 4096,
            max_cpus: 2,
            ..VmConfig::default()
        };
        let host = HostInfo::default();
        let mut registry = Registry::default();
        for (id, state) in [("first", "starting"), ("second", "stopping")] {
            registry.running.insert(
                id.into(),
                Running {
                    memory_mb: 1024,
                    cpus: 1,
                    process: ProcessStatus::new(state, &host),
                    control: Arc::new(Control::default()),
                    qmp: mpsc::channel(1).0,
                },
            );
        }
        let mut record = VmRecord {
            server_id: "0123456789abcdef".into(),
            name: "test".into(),
            owner_did: String::new(),
            allowed_dids: vec![],
            memory_mb: 2048,
            cpus: 1,
            disk_gb: 20,
            base_sha256: String::new(),
            base_url: String::new(),
            ssh_port: 22200,
            autostart: false,
            ssh_authorized_keys: vec![],
            created_at: String::new(),
        };
        assert!(
            check_running_budget(&registry, &record, &cfg)
                .unwrap_err()
                .to_string()
                .contains("CPU")
        );
        registry.running.remove("second");
        assert!(check_running_budget(&registry, &record, &cfg).is_ok());
        record.memory_mb = 4096;
        assert!(
            check_running_budget(&registry, &record, &cfg)
                .unwrap_err()
                .to_string()
                .contains("memory")
        );
    }

    #[test]
    fn ssh_key_must_match_its_encoded_type() {
        use base64::engine::general_purpose::STANDARD;
        let blob =
            STANDARD.encode(b"\0\0\0\x0bssh-ed25519\0\0\0\x20abcdefghijklmnopqrstuvwxyz012345");
        assert!(validate_ssh_key(&format!("ssh-ed25519 {blob} test")).is_ok());
        assert!(validate_ssh_key(&format!("ssh-rsa {blob}")).is_err());
        assert!(validate_ssh_key(&format!("ssh-ed25519 {blob}\nextra")).is_err());
    }
}
