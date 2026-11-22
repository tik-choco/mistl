//! cloud-init NoCloud seed directory (served to the guest as a vvfat drive
//! labelled `cidata`).
use std::path::Path;

use anyhow::{Result, ensure};

/// Writes the NoCloud data and the current boot-time SSH keys into `seed_dir`.
pub fn write_seed(
    seed_dir: &Path,
    server_id: &str,
    hostname: &str,
    ssh_authorized_keys: &[String],
) -> Result<()> {
    let meta_data = format!(
        "instance-id: {}\nlocal-hostname: {}\n",
        yaml_string(server_id)?,
        yaml_string(&sanitize_hostname(hostname))?,
    );
    let user_data = user_data(ssh_authorized_keys)?;
    crate::statefile::write_bytes(&seed_dir.join("meta-data"), meta_data.as_bytes())?;
    crate::statefile::write_bytes(&seed_dir.join("user-data"), user_data.as_bytes())?;
    let mut keys = ssh_authorized_keys.join("\n");
    if !keys.is_empty() {
        keys.push('\n');
    }
    crate::statefile::write_bytes(&seed_dir.join("mistl-authorized_keys"), keys.as_bytes())
}

fn sanitize_hostname(hostname: &str) -> String {
    let mut sanitized = String::new();
    for c in hostname.chars() {
        if c.is_ascii_alphanumeric() {
            sanitized.push(c.to_ascii_lowercase());
        } else if !sanitized.is_empty() && !sanitized.ends_with('-') {
            sanitized.push('-');
        }
        if sanitized.len() == 63 {
            break;
        }
    }
    let sanitized = sanitized.trim_end_matches('-');
    if sanitized.is_empty() {
        "mistl".into()
    } else {
        sanitized.into()
    }
}

fn user_data(keys: &[String]) -> Result<String> {
    let mut data = String::from(concat!(
        "#cloud-config\n",
        "ssh_pwauth: false\n",
        "disable_root: true\n",
        "bootcmd:\n",
        "  - |\n",
        "    set -e\n",
        "    id mistl >/dev/null 2>&1 || exit 0\n",
        "    mkdir -p /run/mistl-seed\n",
        "    mount -o ro LABEL=cidata /run/mistl-seed || mount -o ro LABEL=CIDATA /run/mistl-seed\n",
        "    trap 'umount /run/mistl-seed' EXIT\n",
        "    install -d -m 700 -o mistl -g mistl /home/mistl/.ssh\n",
        "    install -m 600 -o mistl -g mistl /run/mistl-seed/mistl-authorized_keys /home/mistl/.ssh/authorized_keys\n",
        "users:\n",
        "  - name: mistl\n",
        "    sudo: 'ALL=(ALL) NOPASSWD:ALL'\n",
        "    shell: /bin/bash\n",
        "    lock_passwd: true\n",
        "    ssh_authorized_keys:",
    ));
    if keys.is_empty() {
        data.push_str(" []\n");
    } else {
        data.push('\n');
        for key in keys {
            ensure!(
                !key.contains(['\0', '\r', '\n', '\u{85}', '\u{2028}', '\u{2029}']),
                "SSH authorized key must be a single line"
            );
            data.push_str("      - ");
            data.push_str(&yaml_string(key)?);
            data.push('\n');
        }
    }
    Ok(data)
}

fn yaml_string(value: &str) -> Result<String> {
    use std::fmt::Write;

    // JSON's quoted strings use YAML-compatible escapes. Escape the remaining
    // characters that YAML treats as line breaks or disallows in plain text.
    let mut quoted = String::new();
    for c in serde_json::to_string(value)?.chars() {
        match c {
            '\x7f'..='\u{9f}' | '\u{2028}' | '\u{2029}' | '\u{feff}' | '\u{fffe}' | '\u{ffff}' => {
                write!(quoted, "\\u{:04x}", u32::from(c))?;
            }
            _ => quoted.push(c),
        }
    }
    Ok(quoted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostnames_are_valid_dns_labels() {
        assert_eq!(sanitize_hostname(" --Demo__Host.日本語 "), "demo-host");
        assert_eq!(sanitize_hostname("日本語"), "mistl");
        assert_eq!(
            sanitize_hostname(&format!("{}-b", "a".repeat(62))),
            "a".repeat(62)
        );
        assert_eq!(sanitize_hostname(&"A".repeat(64)), "a".repeat(63));
    }

    #[test]
    fn writes_nocloud_files_and_installs_current_keys_each_boot() {
        let dir =
            std::env::temp_dir().join(format!("mistl-vm-seed-{:016x}", rand::random::<u64>()));
        let key = r#"ssh-ed25519 example name: "quoted" \ # comment"#.to_owned();
        write_seed(&dir, "0123456789abcdef", "A VM", std::slice::from_ref(&key)).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("meta-data")).unwrap(),
            "instance-id: \"0123456789abcdef\"\nlocal-hostname: \"a-vm\"\n"
        );
        let user = std::fs::read_to_string(dir.join("user-data")).unwrap();
        assert!(user.starts_with("#cloud-config\nssh_pwauth: false\ndisable_root: true\n"));
        assert!(user.contains(concat!(
            "\n  - name: mistl\n",
            "    sudo: 'ALL=(ALL) NOPASSWD:ALL'\n",
            "    shell: /bin/bash\n"
        )));
        assert!(user.ends_with(concat!(
            "\n    ssh_authorized_keys:\n",
            r#"      - "ssh-ed25519 example name: \"quoted\" \\ # comment""#,
            "\n"
        )));
        assert!(user.contains("\nbootcmd:\n  - |\n"));
        assert!(user.contains("mount -o ro LABEL=cidata /run/mistl-seed"));
        assert!(user.contains("trap 'umount /run/mistl-seed' EXIT"));
        assert!(user.contains("install -d -m 700 -o mistl -g mistl /home/mistl/.ssh"));
        assert!(user.contains(concat!(
            "install -m 600 -o mistl -g mistl /run/mistl-seed/mistl-authorized_keys ",
            "/home/mistl/.ssh/authorized_keys",
        )));
        assert_eq!(
            std::fs::read_to_string(dir.join("mistl-authorized_keys")).unwrap(),
            format!("{key}\n"),
        );
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 3);
        assert_eq!(yaml_string("a\u{85}b").unwrap(), r#""a\u0085b""#);
        assert!(
            user_data(&[])
                .unwrap()
                .ends_with("ssh_authorized_keys: []\n")
        );

        for invalid in ["first\nsecond", "first\rsecond", "first\u{2028}second"] {
            assert!(write_seed(&dir, "0123456789abcdef", "other", &[invalid.into()]).is_err());
        }
        assert_eq!(
            std::fs::read_to_string(dir.join("user-data")).unwrap(),
            user
        );
        write_seed(&dir, "0123456789abcdef", "A VM", &["updated key".into()]).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("mistl-authorized_keys")).unwrap(),
            "updated key\n",
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
