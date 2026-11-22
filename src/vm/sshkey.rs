//! This instance's SSH client key and host-key pinning helpers.
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::LazyLock;

use anyhow::{Context, Result, ensure};
use regex::Regex;
use serde_json::Value;
use tokio::{process::Command, sync::Mutex};

static CLIENT_KEY_LOCK: Mutex<()> = Mutex::const_new(());
static KNOWN_HOSTS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
const SERIAL_TAIL_BYTES: u64 = 512 * 1024;
const FINGERPRINTS_BEGIN: &str = "-----BEGIN SSH HOST KEY FINGERPRINTS-----";
const FINGERPRINTS_END: &str = "-----END SSH HOST KEY FINGERPRINTS-----";

#[derive(Debug, Clone)]
pub struct ClientKey {
    pub private: PathBuf,
    /// OpenSSH one-line public key.
    pub public: String,
}

/// Creates `<vm_root>/ssh/id_ed25519` with `ssh-keygen` on first use and
/// restricts its permissions so OpenSSH accepts it (Windows ACL included).
pub async fn ensure_client_key(vm_root: &Path, ssh_keygen: &Path) -> Result<ClientKey> {
    let _guard = CLIENT_KEY_LOCK.lock().await;
    let dir = vm_root.join("ssh");
    crate::statefile::create_private_dir(&dir).context("creating VM SSH directory")?;
    let private = dir.join("id_ed25519");
    let public_path = dir.join("id_ed25519.pub");

    if !private.try_exists()? {
        let mut command = Command::new(ssh_keygen);
        command
            .args(["-q", "-t", "ed25519", "-N", "", "-C", "mistl-vm", "-f"])
            .arg(&private)
            .stdin(Stdio::null());
        crate::child_process::prepare(&mut command);
        let result = command
            .output()
            .await
            .context("running ssh-keygen for VM client key")?;
        ensure!(
            result.status.success(),
            "ssh-keygen failed to create VM client key: {}",
            result.status
        );
    }
    restrict_private_key(&private).await?;

    let public = match fs::read_to_string(&public_path) {
        Ok(public) => public,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Recover if key generation was interrupted before writing the public key.
            let mut command = Command::new(ssh_keygen);
            command
                .args(["-y", "-P", "", "-f"])
                .arg(&private)
                .stdin(Stdio::null());
            crate::child_process::prepare(&mut command);
            let result = command
                .output()
                .await
                .context("recovering VM client public key")?;
            ensure!(
                result.status.success(),
                "ssh-keygen failed to read VM client key: {}",
                result.status
            );
            let public =
                String::from_utf8(result.stdout).context("VM client public key is not UTF-8")?;
            public_key_line(&public)?;
            crate::statefile::write_bytes(&public_path, public.as_bytes())?;
            public
        }
        Err(error) => return Err(error).context("reading VM client public key"),
    };
    Ok(ClientKey {
        private,
        public: public_key_line(&public)?.to_owned(),
    })
}

fn public_key_line(public: &str) -> Result<&str> {
    let line = public.trim_end_matches(['\r', '\n']);
    ensure!(
        !line.contains(['\r', '\n']) && line.starts_with("ssh-ed25519 "),
        "invalid VM client public key"
    );
    Ok(line)
}

#[cfg(windows)]
async fn restrict_private_key(path: &Path) -> Result<()> {
    let username =
        std::env::var("USERNAME").context("USERNAME is required to secure the VM client key")?;
    ensure!(!username.is_empty(), "USERNAME is empty");
    let mut command = Command::new("icacls");
    command
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(format!("{username}:F"))
        .stdin(Stdio::null());
    crate::child_process::prepare(&mut command);
    let result = command
        .output()
        .await
        .context("running icacls for VM client key")?;
    ensure!(
        result.status.success(),
        "icacls failed to secure VM client key: {}",
        result.status
    );
    Ok(())
}

#[cfg(unix)]
async fn restrict_private_key(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .context("setting VM client key permissions")
}

pub fn known_hosts_path(vm_root: &Path) -> PathBuf {
    vm_root.join("ssh").join("known_hosts")
}

/// `HostKeyAlias` used for every connection to a server id, so the pinned
/// host key follows the server id rather than the loopback port.
pub fn host_key_alias(server_id: &str) -> String {
    format!("mistl-vm-{server_id}")
}

fn read_known_hosts(vm_root: &Path) -> Result<String> {
    match fs::read_to_string(known_hosts_path(vm_root)) {
        Ok(text) => Ok(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error).context("reading VM known_hosts"),
    }
}

pub(crate) fn validate_known_hosts(server_id: &str, lines: &[String]) -> Result<()> {
    let alias = host_key_alias(server_id);
    for line in lines {
        ensure!(
            !line.contains(['\0', '\r', '\n', '\u{85}', '\u{2028}', '\u{2029}']),
            "VM known_hosts entry must be a single line"
        );
        let (host, key) = line
            .split_once([' ', '\t'])
            .context("invalid VM known_hosts entry")?;
        ensure!(host == alias, "VM known_hosts entry has the wrong alias");
        super::validate_ssh_key(key).context("invalid VM host public key")?;
    }
    Ok(())
}

pub(crate) fn export_known_hosts(vm_root: &Path, server_id: &str) -> Result<Vec<String>> {
    let alias = host_key_alias(server_id);
    let text = read_known_hosts(vm_root)?;
    let mut seen = HashSet::new();
    let lines: Vec<_> = text
        .lines()
        .filter(|line| line.split_ascii_whitespace().next() == Some(alias.as_str()))
        .filter(|line| seen.insert(*line))
        .map(str::to_owned)
        .collect();
    validate_known_hosts(server_id, &lines)?;
    Ok(lines)
}

pub(crate) fn merge_known_hosts(vm_root: &Path, server_id: &str, lines: &[String]) -> Result<()> {
    validate_known_hosts(server_id, lines)?;
    if lines.is_empty() {
        return Ok(());
    }
    let _lock = KNOWN_HOSTS_LOCK.lock().expect("VM known_hosts lock");
    let mut text = read_known_hosts(vm_root)?;
    let mut seen: HashSet<_> = text.lines().collect();
    let additions: Vec<_> = lines
        .iter()
        .filter(|line| seen.insert(line.as_str()))
        .collect();
    if additions.is_empty() {
        return Ok(());
    }
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    for line in additions {
        text.push_str(line);
        text.push('\n');
    }
    crate::statefile::write_private(&known_hosts_path(vm_root), text.as_bytes())
        .context("writing VM known_hosts")
}

/// Encodes a path as one OpenSSH config token; filename expansion decodes
/// doubled percent signs. Quotes are added only when the path contains
/// whitespace or quotes (OpenSSH's argv_split then decodes `\\` and `\"`).
/// Unquoted paths keep their backslashes verbatim. Avoiding quotes in the
/// common case matters: the MSYS `ssh` that Git Bash puts first on PATH
/// rejects an embedded quote in a `-o` argument ("invalid quotes").
pub(crate) fn ssh_path_token(path: &str) -> String {
    let path = path.replace('%', "%%");
    if !path
        .chars()
        .any(|c| c.is_whitespace() || c == '"' || c == '\'')
    {
        return path;
    }
    format!("\"{}\"", path.replace('\\', "\\\\").replace('"', "\\\""))
}

pub(crate) fn ssh_command(
    connection: &Value,
    options: &[&str],
    extra: &[std::ffi::OsString],
) -> Result<std::process::Command> {
    let field = |name: &str| {
        connection[name]
            .as_str()
            .with_context(|| format!("missing VM SSH {name}"))
    };
    let port = connection["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .filter(|&port| port != 0)
        .context("missing or invalid VM SSH port")?;
    let key = field("key")?;
    let alias = field("host_key_alias")?;
    let known_hosts = field("known_hosts")?;
    let user = field("user")?;
    let known_hosts_option = format!("UserKnownHostsFile={}", ssh_path_token(known_hosts));
    let mut command = std::process::Command::new("ssh");
    command
        .args(["-i", key, "-o", "IdentitiesOnly=yes"])
        .arg("-o")
        .arg(format!("HostKeyAlias={alias}"))
        .args([
            "-o",
            &known_hosts_option,
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "HashKnownHosts=no",
        ])
        .args(options)
        .arg("-p")
        .arg(port.to_string())
        .arg(format!("{user}@127.0.0.1"))
        .args(extra);
    Ok(command)
}

/// Parses the ED25519 fingerprint cloud-init prints to the serial console.
pub fn host_fingerprint_from_serial(serial_log: &Path) -> Option<String> {
    let mut file = File::open(serial_log).ok()?;
    let length = file.metadata().ok()?.len();
    let start = length.saturating_sub(SERIAL_TAIL_BYTES);
    let mut partial_line = false;
    if start != 0 {
        file.seek(SeekFrom::Start(start - 1)).ok()?;
        let mut previous = [0];
        file.read_exact(&mut previous).ok()?;
        partial_line = previous[0] != b'\n';
    }
    let mut tail = Vec::new();
    file.take(length - start).read_to_end(&mut tail).ok()?;
    let text = String::from_utf8_lossy(&tail);
    let text = if partial_line {
        text.split_once('\n')?.1
    } else {
        &text
    };
    cloud_init_fingerprint(text)
}

fn cloud_init_fingerprint(text: &str) -> Option<String> {
    static ANSI: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)|[@-Z\\-_])")
            .expect("valid ANSI escape expression")
    });
    let mut in_block = false;
    let mut fingerprint = None;
    let mut completed = None;
    for line in text.lines() {
        let line = ANSI.replace_all(line, "");
        if line.contains(FINGERPRINTS_BEGIN) {
            in_block = true;
            fingerprint = None;
        } else if in_block && line.contains(FINGERPRINTS_END) {
            completed = fingerprint.take();
            in_block = false;
        } else if in_block {
            let mut fields = line.split_ascii_whitespace();
            if fields.next_back() == Some("(ED25519)") {
                fingerprint = fields
                    .find(|field| {
                        field.strip_prefix("SHA256:").is_some_and(|value| {
                            !value.is_empty()
                                && value
                                    .bytes()
                                    .all(|b| b.is_ascii_alphanumeric() || b"+/=".contains(&b))
                        })
                    })
                    .map(str::to_owned);
            }
        }
    }
    completed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_ed25519_fingerprint_from_latest_completed_block() {
        let text = format!(
            "ci-info: 256 SHA256:outside root@host (ED25519)\r\n\
             {FINGERPRINTS_BEGIN}\n\
             ec2: 256 SHA256:old root@host (ED25519)\n\
             {FINGERPRINTS_END}\n\
             [   12.500] ci-info: \x1b[32m{FINGERPRINTS_BEGIN}\x1b[0m\r\n\
             [   12.501] ec2: 3072 SHA256:rsa root@host (RSA)\r\n\
             [   12.502] ec2: 256 SHA256:\x1b[34mnew+/key\x1b[0m root@host (ED25519)\r\n\
             [   12.503] ci-info: {FINGERPRINTS_END}\r\n\
             {FINGERPRINTS_BEGIN}\n\
             ec2: 256 SHA256:incomplete root@host (ED25519)\n"
        );
        assert_eq!(
            cloud_init_fingerprint(&text).as_deref(),
            Some("SHA256:new+/key")
        );
        assert_eq!(
            cloud_init_fingerprint(&format!(
                "{FINGERPRINTS_BEGIN}\nec2: 256 SHA256:key root@host (ED25519)\n"
            )),
            None
        );
        assert_eq!(
            cloud_init_fingerprint(&format!(
                "{FINGERPRINTS_BEGIN}\nec2: 3072 SHA256:rsa root@host (RSA)\n{FINGERPRINTS_END}"
            )),
            None
        );
    }

    #[test]
    fn serial_reader_ignores_blocks_outside_the_tail() {
        let path =
            std::env::temp_dir().join(format!("mistl-vm-serial-{:016x}", rand::random::<u64>()));
        let block = format!(
            "{FINGERPRINTS_BEGIN}\n256 SHA256:key root@host (ED25519)\n{FINGERPRINTS_END}\n"
        );
        let padding = "x".repeat(SERIAL_TAIL_BYTES as usize);
        fs::write(&path, format!("{block}{padding}")).unwrap();
        assert_eq!(host_fingerprint_from_serial(&path), None);
        fs::write(&path, format!("{padding}\n{block}")).unwrap();
        assert_eq!(
            host_fingerprint_from_serial(&path).as_deref(),
            Some("SHA256:key")
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn client_key_is_a_single_openssh_line() {
        assert_eq!(
            public_key_line("ssh-ed25519 example mistl-vm\r\n").unwrap(),
            "ssh-ed25519 example mistl-vm"
        );
        assert!(public_key_line("ssh-ed25519 example\nsecond").is_err());
        assert!(public_key_line("").is_err());
    }

    #[test]
    fn known_hosts_merge_is_alias_scoped_and_idempotent() {
        use base64::Engine as _;
        let root =
            std::env::temp_dir().join(format!("mistl-vm-hosts-{:016x}", rand::random::<u64>()));
        let id = "0123456789abcdef";
        let other = "fedcba9876543210";
        let blob = base64::engine::general_purpose::STANDARD
            .encode(b"\0\0\0\x0bssh-ed25519\0\0\0\x20abcdefghijklmnopqrstuvwxyz012345");
        let line = format!("mistl-vm-{id} ssh-ed25519 {blob}");
        let other_line = format!("mistl-vm-{other} ssh-ed25519 {blob}");
        merge_known_hosts(&root, other, std::slice::from_ref(&other_line)).unwrap();
        merge_known_hosts(&root, id, &[line.clone(), line.clone()]).unwrap();
        merge_known_hosts(&root, id, std::slice::from_ref(&line)).unwrap();
        assert!(merge_known_hosts(&root, id, std::slice::from_ref(&other_line)).is_err());
        assert!(merge_known_hosts(&root, id, &[format!("{line}\n{other_line}")]).is_err());
        assert!(
            merge_known_hosts(&root, id, &[format!("mistl-vm-{id},* ssh-ed25519 {blob}")]).is_err()
        );
        assert_eq!(export_known_hosts(&root, id).unwrap(), std::slice::from_ref(&line));
        assert_eq!(
            fs::read_to_string(known_hosts_path(&root)).unwrap(),
            format!("{other_line}\n{line}\n")
        );
        fs::remove_dir_all(root).unwrap();
    }
}
