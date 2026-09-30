//! Resolved before config access and forwarded to every child daemon.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, Instant},
};
pub const CHANNEL: &str = env!("MISTL_CHANNEL");
pub const BUILD_ID: &str = env!("MISTL_BUILD_ID");
pub const IPC_VERSION: u32 = 1;
pub struct RuntimeContext {
    pub instance: String,
    pub root: Option<PathBuf>,
    pub no_tray: bool,
}
static CONTEXT: OnceLock<RuntimeContext> = OnceLock::new();
// Retained until process exit, including all native library threads.
static DAEMON_LOCK: OnceLock<File> = OnceLock::new();
pub fn default_instance() -> String {
    if CHANNEL == "dev" {
        format!("worktree-{}", env!("MISTL_WORKSPACE_ID"))
    } else {
        "default".into()
    }
}
pub fn context() -> &'static RuntimeContext {
    CONTEXT.get_or_init(|| RuntimeContext {
        instance: default_instance(),
        root: if cfg!(test) {
            Some(std::env::temp_dir().join(format!("mistl-unit-{}", std::process::id())))
        } else {
            None
        },
        no_tray: false,
    })
}
pub fn initialize(instance: Option<String>, root: Option<PathBuf>, no_tray: bool) -> Result<()> {
    let instance = instance.unwrap_or_else(default_instance);
    validate_instance(&instance)?;
    let root = root
        .map(|p| {
            if p.is_absolute() {
                Ok(p)
            } else {
                std::env::current_dir().map(|d| d.join(p))
            }
        })
        .transpose()?;
    CONTEXT
        .set(RuntimeContext {
            instance,
            root,
            no_tray,
        })
        .map_err(|_| anyhow::anyhow!("runtime already initialized"))
}
fn validate_instance(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        bail!("instance must contain 1-64 ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}
pub fn legacy_default() -> bool {
    CHANNEL == "stable" && context().instance == "default" && context().root.is_none()
}
pub fn dirs(config: bool) -> Result<PathBuf> {
    let base =
        directories::ProjectDirs::from("com", "tik-choco", "mistl").context("no home directory")?;
    let path = if let Some(root) = &context().root {
        root.join(CHANNEL)
            .join(&context().instance)
            .join(if config { "config" } else { "data" })
    } else if legacy_default() {
        if config {
            base.config_dir().to_owned()
        } else {
            base.data_dir().to_owned()
        }
    } else {
        let base = if config {
            base.config_dir()
        } else {
            base.data_dir()
        };
        base.join("instances")
            .join(CHANNEL)
            .join(&context().instance)
    };
    crate::statefile::create_private_dir(&path)?;
    Ok(path)
}
pub fn child_args(command: &mut std::process::Command) {
    command.args(launch_arguments());
}
pub fn build_info() -> Value {
    json!({"version": env!("CARGO_PKG_VERSION"), "channel": CHANNEL,
        "instance": context().instance, "instance_id": registration_name(), "build_id": BUILD_ID, "commit": env!("MISTL_COMMIT"),
        "dirty": env!("MISTL_DIRTY").parse::<bool>().ok(), "profile": env!("MISTL_PROFILE"),
        "target": env!("MISTL_TARGET"), "ipc_version": IPC_VERSION,
        "mistlib_version": mistlib::build_info::get_version(),
        "mistlib": serde_json::from_str::<Value>(&mistlib::build_info::get_build_info()).unwrap_or(Value::Null)})
}
pub fn acquire_daemon_lock() -> Result<()> {
    let file = File::options()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dirs(false)?.join("daemon.lock"))?;
    let start = Instant::now();
    loop {
        match file.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) if start.elapsed() < Duration::from_secs(8) => {
                std::thread::sleep(Duration::from_millis(50))
            }
            Err(err) => bail!(
                "instance {} already running or lock unavailable: {err}",
                context().instance
            ),
        }
    }
    DAEMON_LOCK
        .set(file)
        .map_err(|_| anyhow::anyhow!("daemon lock already held"))?;
    // Old releases have no lock/handshake. Refuse a live legacy discovery entry.
    if let Ok(bytes) = std::fs::read(dirs(false)?.join("daemon.json"))
        && let Ok(info) = serde_json::from_slice::<Value>(&bytes)
        && info["ipc_version"].as_u64() != Some(IPC_VERSION as u64)
        && let Some(port) = info["port"].as_u64().and_then(|p| u16::try_from(p).ok())
        && std::net::TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_secs(1),
        )
        .is_ok()
    {
        bail!("legacy daemon is still running; stop it with its original binary before migration");
    }
    let marker = dirs(false)?.join("instance.json");
    let expected = json!({"schema":1,"channel":CHANNEL,"instance":context().instance});
    if marker.exists() {
        let actual: Value = serde_json::from_slice(&std::fs::read(&marker)?)?;
        if actual != expected {
            bail!("incompatible instance owner/schema at {}", marker.display());
        }
    } else {
        crate::statefile::write_bytes(&marker, &serde_json::to_vec(&expected)?)?;
    }
    Ok(())
}
pub fn require_installable() -> Result<()> {
    if !legacy_default() {
        bail!(
            "install/uninstall require stable/default; development instances cannot overwrite the installed release"
        );
    }
    Ok(())
}
pub fn offline_listen(listen: &str) -> String {
    format!(
        "127.0.0.1:{}",
        listen.rsplit_once(':').map(|(_, p)| p).unwrap_or("6480")
    )
}
pub fn initial_ui_port() -> Result<u16> {
    if legacy_default() {
        return Ok(6480);
    }
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}
pub fn same_file_owner(path: &Path, run_id: &str) -> bool {
    std::fs::read(path)
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .is_some_and(|v| v["run_id"] == run_id)
}
pub fn registration_name() -> String {
    use sha2::{Digest, Sha256};
    if legacy_default() {
        return "mistl".into();
    }
    let suffix = context()
        .root
        .as_ref()
        .map(|p| format!("-{:x}", Sha256::digest(p.to_string_lossy().as_bytes()))[..9].to_owned())
        .unwrap_or_default();
    format!("mistl-{}-{}{suffix}", CHANNEL, context().instance)
}
pub fn launch_arguments() -> Vec<String> {
    let mut args = Vec::new();
    if !legacy_default() {
        args.extend(["--instance".into(), context().instance.clone()]);
    }
    if let Some(root) = &context().root {
        args.extend(["--state-dir".into(), root.to_string_lossy().into_owned()]);
    }
    if context().no_tray {
        args.push("--no-tray".into());
    }
    args
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_path_instances() {
        for name in ["", "../stable", "a/b", "a\\b", ".", "日本語"] {
            assert!(validate_instance(name).is_err());
        }
        assert!(validate_instance("repro-123_a").is_ok());
    }
    #[test]
    fn offline_ui_stays_local() {
        assert_eq!(offline_listen("[::]:6481"), "127.0.0.1:6481");
    }
}
