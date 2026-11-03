//! Put the install dir on the user's `PATH` so `mistl` works from any shell.
//!
//! - Windows: appends the install dir to the per-user `Path` value under
//!   `HKCU\Environment` (keeping its registry type, normally
//!   `REG_EXPAND_SZ`), then broadcasts `WM_SETTINGCHANGE("Environment")` so
//!   newly opened terminals see it. No admin rights. Shells that are already
//!   open keep their old environment.
//! - Unix: the install dir is `~/.local/bin`, which login shells normally
//!   already have on `PATH`; mistl never edits shell rc files. Enabling only
//!   reports whether the dir is reachable and, if not, the line to add.
//!
//! Stable/default instances only (like install itself): a dev build must not
//! shadow the installed release on `PATH`.

use std::path::Path;

#[cfg(windows)]
use anyhow::Context;
use anyhow::Result;
use tracing::info;

use super::install_dir;

/// Whether `dir` is one of the `;`/`:`-separated entries of `path_value`
/// (case-insensitive and ignoring a trailing separator on Windows).
fn contains_entry(path_value: &str, dir: &Path) -> bool {
    let wanted = normalize(&dir.display().to_string());
    split(path_value).any(|entry| normalize(entry) == wanted)
}

/// `path_value` with `dir` appended, or `None` when it is already present.
#[cfg_attr(not(windows), allow(dead_code))]
fn with_entry(path_value: &str, dir: &Path) -> Option<String> {
    if contains_entry(path_value, dir) {
        return None;
    }
    let trimmed = path_value.trim_end_matches(SEPARATOR);
    let dir = dir.display().to_string();
    Some(if trimmed.is_empty() {
        dir
    } else {
        format!("{trimmed}{SEPARATOR}{dir}")
    })
}

/// `path_value` without any entry equal to `dir`, or `None` when absent.
#[cfg_attr(not(windows), allow(dead_code))]
fn without_entry(path_value: &str, dir: &Path) -> Option<String> {
    if !contains_entry(path_value, dir) {
        return None;
    }
    let wanted = normalize(&dir.display().to_string());
    let kept: Vec<&str> = path_value
        .split(SEPARATOR)
        .filter(|entry| entry.is_empty() || normalize(entry) != wanted)
        .collect();
    Some(
        kept.join(&SEPARATOR.to_string())
            .trim_end_matches(SEPARATOR)
            .to_string(),
    )
}

const SEPARATOR: char = if cfg!(windows) { ';' } else { ':' };

fn split(path_value: &str) -> impl Iterator<Item = &str> {
    path_value.split(SEPARATOR).filter(|e| !e.trim().is_empty())
}

fn normalize(entry: &str) -> String {
    let entry = entry.trim().trim_matches('"');
    if cfg!(windows) {
        entry.trim_end_matches(['\\', '/']).to_lowercase()
    } else {
        entry.trim_end_matches('/').to_string()
    }
}

/// Whether the install dir is on the user's `PATH` (Windows: the persisted
/// per-user value; Unix: this process's `PATH`).
pub fn cli_path_enabled() -> bool {
    let Ok(dir) = install_dir() else {
        return false;
    };
    #[cfg(windows)]
    {
        read_user_path()
            .map(|(value, _)| contains_entry(&value, &dir))
            .unwrap_or(false)
    }
    #[cfg(not(windows))]
    {
        std::env::var("PATH")
            .map(|value| contains_entry(&value, &dir))
            .unwrap_or(false)
    }
}

/// Add (`true`) or remove (`false`) the install dir on the user's `PATH`.
/// Idempotent. Returns whether the dir is on `PATH` afterwards.
#[cfg(windows)]
pub fn set_cli_path(enabled: bool) -> Result<bool> {
    crate::runtime::require_installable()?;
    let dir = install_dir()?;
    let (current, kind) = read_user_path()?;
    let updated = if enabled {
        with_entry(&current, &dir)
    } else {
        without_entry(&current, &dir)
    };
    if let Some(value) = updated {
        write_user_path(&value, kind)?;
        broadcast_environment_change();
        info!(dir = %dir.display(), enabled, "install: updated the user PATH");
    }
    Ok(enabled)
}

/// Unix: never edits shell rc files; enabling fails with the line to add
/// when `~/.local/bin` is not on `PATH`, disabling is a no-op.
#[cfg(not(windows))]
pub fn set_cli_path(enabled: bool) -> Result<bool> {
    crate::runtime::require_installable()?;
    let on = cli_path_enabled();
    if enabled && !on {
        let dir = install_dir()?;
        anyhow::bail!(
            "{} is not on PATH; add `export PATH=\"{}:$PATH\"` to your shell profile",
            dir.display(),
            dir.display()
        );
    }
    info!(on, "install: PATH check");
    Ok(on)
}

#[cfg(windows)]
const ENV_KEY: &str = "Environment";

/// The raw per-user `Path` value and its registry type (`REG_EXPAND_SZ`
/// when absent, matching what Windows itself creates).
#[cfg(windows)]
fn read_user_path() -> Result<(String, winreg::enums::RegType)> {
    use winreg::RegKey;
    use winreg::enums::{HKEY_CURRENT_USER, REG_EXPAND_SZ, REG_SZ};

    let env = RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey(ENV_KEY)
        .context("install: opening HKCU\\Environment")?;
    match env.get_raw_value("Path") {
        Ok(raw) => {
            let kind = match raw.vtype {
                REG_SZ => REG_SZ,
                _ => REG_EXPAND_SZ,
            };
            let units: Vec<u16> = raw
                .bytes
                .chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                .take_while(|&unit| unit != 0)
                .collect();
            Ok((String::from_utf16_lossy(&units), kind))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok((String::new(), REG_EXPAND_SZ))
        }
        Err(error) => Err(error).context("install: reading the user PATH"),
    }
}

/// Writes `value` back with the type it was read with, so `%VAR%` entries
/// in a `REG_EXPAND_SZ` Path keep expanding.
#[cfg(windows)]
fn write_user_path(value: &str, kind: winreg::enums::RegType) -> Result<()> {
    use winreg::enums::{HKEY_CURRENT_USER, KEY_SET_VALUE};
    use winreg::{RegKey, RegValue};

    let env = RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey_with_flags(ENV_KEY, KEY_SET_VALUE)
        .context("install: opening HKCU\\Environment for writing")?;
    let bytes: Vec<u8> = value
        .encode_utf16()
        .chain(std::iter::once(0))
        .flat_map(u16::to_le_bytes)
        .collect();
    env.set_raw_value("Path", &RegValue { bytes, vtype: kind })
        .context("install: writing the user PATH")
}

/// Tells Explorer (and through it, newly started terminals) that the
/// environment changed. Best-effort, bounded by a 5 s timeout so a hung
/// top-level window cannot stall the install.
#[cfg(windows)]
fn broadcast_environment_change() {
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        HWND_BROADCAST, SMTO_ABORTIFHUNG, SendMessageTimeoutW, WM_SETTINGCHANGE,
    };

    let name: Vec<u16> = "Environment".encode_utf16().chain(Some(0)).collect();
    let mut result = 0usize;
    // SAFETY: `name` is a NUL-terminated UTF-16 string that outlives the
    // call, and `result` is a valid out pointer.
    unsafe {
        SendMessageTimeoutW(
            HWND_BROADCAST,
            WM_SETTINGCHANGE,
            0,
            name.as_ptr() as isize,
            SMTO_ABORTIFHUNG,
            5000,
            &mut result,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sep(parts: &[&str]) -> String {
        parts.join(&SEPARATOR.to_string())
    }

    #[test]
    fn appends_once_and_keeps_existing_entries() {
        let dir = PathBuf::from(if cfg!(windows) {
            r"C:\Apps\mistl"
        } else {
            "/home/jo/.local/bin"
        });
        let base = sep(&["/a", "/b"]);
        let added = with_entry(&base, &dir).unwrap();
        assert_eq!(added, sep(&["/a", "/b", &dir.display().to_string()]));
        assert!(with_entry(&added, &dir).is_none(), "must not duplicate");
        assert_eq!(with_entry("", &dir).unwrap(), dir.display().to_string());
        // A trailing separator does not produce an empty entry.
        let trailing = format!("{base}{SEPARATOR}");
        assert_eq!(with_entry(&trailing, &dir).unwrap(), added);
    }

    #[test]
    fn removes_only_the_install_dir() {
        let dir = PathBuf::from(if cfg!(windows) {
            r"C:\Apps\mistl"
        } else {
            "/home/jo/.local/bin"
        });
        let d = dir.display().to_string();
        let value = sep(&["/a", &d, "/b"]);
        assert_eq!(without_entry(&value, &dir).unwrap(), sep(&["/a", "/b"]));
        assert!(without_entry(&sep(&["/a", "/b"]), &dir).is_none());
        assert_eq!(without_entry(&d, &dir).unwrap(), "");
    }

    #[cfg(windows)]
    #[test]
    fn windows_matching_ignores_case_quotes_and_trailing_slash() {
        let dir = PathBuf::from(r"C:\Users\Jo\AppData\Local\Programs\mistl");
        assert!(contains_entry(
            r"C:\x;c:\users\jo\appdata\local\programs\MISTL\",
            &dir
        ));
        assert!(contains_entry(
            r#""C:\Users\Jo\AppData\Local\Programs\mistl""#,
            &dir
        ));
        assert!(!contains_entry(
            r"C:\Users\Jo\AppData\Local\Programs\mistl2",
            &dir
        ));
    }
}
