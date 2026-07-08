//! Per-user self-install, Start-Menu shortcut, and login autostart.
//!
//! mistl ships as one portable executable; "installing" copies the running
//! exe to a fixed per-user location so updates, shortcuts, and autostart
//! all have a stable path to point at. No admin rights are needed:
//!
//! - Windows: `%LOCALAPPDATA%\Programs\mistl\mistl.exe`, plus a Start-Menu
//!   shortcut (`mistl.lnk`, launches bare `mistl` = opens the dashboard).
//! - Linux: `~/.local/bin/mistl` (or `$XDG_BIN_HOME`); macOS falls back to
//!   `~/.local/bin` too. No shortcut on Unix.
//!
//! Autostart is opt-in, per-user, and launches the daemon headless
//! (`"<exe>" daemon run`), NOT the dashboard:
//!
//! - Windows: `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`, value
//!   name `mistl`.
//! - Linux: XDG autostart entry `~/.config/autostart/mistl.desktop`.
//! - macOS: LaunchAgent `~/Library/LaunchAgents/com.tik-choco.mistl.plist`
//!   (picked up at the next login; not `launchctl load`ed immediately).
//!
//! Uninstall removes the shortcut, the autostart entry, and the installed
//! binary -- best-effort when the daemon is running from the install dir
//! (`self_delete` handles deleting a running exe on Windows; the dir
//! itself may only clear once the process exits). On Unix the install dir
//! is the shared `~/.local/bin`, so only the binary itself is removed.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::daemon::AppState;

/// Handle `install.*` and `autostart.*` IPC commands:
/// - `install.status` `{}` -> `{installed, install_dir, exe_path,
///   running_from, autostart_enabled}`.
/// - `install.install` `{autostart?: bool}` (default true) ->
///   `{installed: true, exe_path}`.
/// - `install.uninstall` `{}` -> `{uninstalled: true}`.
/// - `autostart.enable` / `autostart.disable` / `autostart.status` `{}` ->
///   `{autostart_enabled: bool}`.
pub async fn handle(cmd: &str, args: Value, _state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "install.status" => {
            let running_from = std::env::current_exe()
                .map(|path| path.display().to_string())
                .unwrap_or_default();
            Ok(json!({
                "installed": is_installed(),
                "install_dir": install_dir()?.display().to_string(),
                "exe_path": installed_exe_path()?.display().to_string(),
                "running_from": running_from,
                "autostart_enabled": autostart_enabled(),
            }))
        }
        "install.install" => {
            let autostart = args
                .get("autostart")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let exe = install(autostart)?;
            Ok(json!({ "installed": true, "exe_path": exe.display().to_string() }))
        }
        "install.uninstall" => {
            uninstall()?;
            Ok(json!({ "uninstalled": true }))
        }
        "autostart.enable" => {
            set_autostart(true)?;
            Ok(json!({ "autostart_enabled": true }))
        }
        "autostart.disable" => {
            set_autostart(false)?;
            Ok(json!({ "autostart_enabled": false }))
        }
        "autostart.status" => Ok(json!({ "autostart_enabled": autostart_enabled() })),
        _ => bail!("unknown install command: {cmd}"),
    }
}

/// Fixed per-user install directory: `%LOCALAPPDATA%\Programs\mistl` on
/// Windows, `$XDG_BIN_HOME` or `~/.local/bin` on Unix.
pub fn install_dir() -> Result<PathBuf> {
    let base = directories::BaseDirs::new()
        .context("install: could not determine the home directory")?;
    if cfg!(windows) {
        Ok(base.data_local_dir().join("Programs").join("mistl"))
    } else {
        // `executable_dir` is Some on Linux only; macOS falls back too.
        Ok(base
            .executable_dir()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| base.home_dir().join(".local").join("bin")))
    }
}

/// `install_dir()/mistl` (`.exe` on Windows).
pub fn installed_exe_path() -> Result<PathBuf> {
    Ok(install_dir()?.join(format!("mistl{}", std::env::consts::EXE_SUFFIX)))
}

/// Whether this process is running the installed copy: `current_exe`
/// canonicalizes to `installed_exe_path` (false when the installed copy
/// doesn't exist).
pub fn is_installed() -> bool {
    let (Ok(current), Ok(installed)) = (std::env::current_exe(), installed_exe_path()) else {
        return false;
    };
    match (current.canonicalize(), installed.canonicalize()) {
        (Ok(current), Ok(installed)) => current == installed,
        _ => false,
    }
}

/// Copy the running exe into the install dir, (re)create the Start-Menu
/// shortcut, and optionally enable autostart. Returns the installed exe
/// path. Safe to re-run: an install from the installed copy itself skips
/// the self-copy (copying a file over itself would fail) and just
/// refreshes shortcut/autostart.
pub fn install(enable_autostart: bool) -> Result<PathBuf> {
    let current = std::env::current_exe().context("install: resolving current executable")?;
    let dir = install_dir()?;
    let dest = installed_exe_path()?;
    std::fs::create_dir_all(&dir).with_context(|| format!("install: creating {}", dir.display()))?;

    if paths_equal(&current, &dest) {
        info!(exe = %dest.display(), "install: already running from the install dir; skipping self-copy");
    } else {
        if dest.exists() {
            // Replacing a possibly-running previous install: unlink works on
            // Unix even while the binary runs; on Windows a running exe
            // can't be deleted but CAN be renamed out of the way.
            if std::fs::remove_file(&dest).is_err() {
                let old = dest.with_extension("old");
                let _ = std::fs::remove_file(&old);
                std::fs::rename(&dest, &old).with_context(|| {
                    format!("install: moving the existing {} out of the way", dest.display())
                })?;
                // Fails while the old exe still runs; a stale `.old` is harmless.
                let _ = std::fs::remove_file(&old);
            }
        }
        std::fs::copy(&current, &dest).with_context(|| {
            format!(
                "install: copying {} -> {}",
                current.display(),
                dest.display()
            )
        })?;
        info!(from = %current.display(), to = %dest.display(), "install: copied executable");
    }

    // The shortcut is a convenience; its failure must not fail the install.
    #[cfg(windows)]
    {
        if let Err(error) = create_start_menu_shortcut(&dest) {
            warn!(%error, "install: could not create the Start Menu shortcut");
        }
    }

    if enable_autostart {
        set_autostart(true)?;
    }

    Ok(dest)
}

/// Remove the autostart entry, the Start-Menu shortcut, and the installed
/// binary (plus, on Windows, the dedicated install dir). Best-effort when
/// running from the install dir: the exe is deleted via the self-delete
/// dance, but the dir may only clear once this process exits.
pub fn uninstall() -> Result<()> {
    // Pointers first: registry/shortcut entries reference the exe we're
    // about to delete, and their failure shouldn't strand the binary.
    if let Err(error) = set_autostart(false) {
        warn!(%error, "install: could not remove the autostart entry");
    }
    #[cfg(windows)]
    {
        if let Err(error) = remove_start_menu_shortcut() {
            warn!(%error, "install: could not remove the Start Menu shortcut");
        }
    }

    let exe = installed_exe_path()?;
    if is_installed() {
        // Deleting the running exe needs the same rename dance as replacing it.
        self_replace::self_delete().context("install: deleting the running executable")?;
    } else if exe.exists() {
        std::fs::remove_file(&exe)
            .with_context(|| format!("install: removing {}", exe.display()))?;
    }
    // Leftover from a previous in-place upgrade, if any.
    let _ = std::fs::remove_file(exe.with_extension("old"));

    // The install dir is dedicated to mistl on Windows only; on Unix it is
    // the shared ~/.local/bin, where we only own the binary itself.
    #[cfg(windows)]
    {
        let dir = install_dir()?;
        if dir.exists() {
            if let Err(error) = std::fs::remove_dir_all(&dir) {
                // Expected while running from the install dir: self_delete
                // keeps a handle open until the process exits.
                warn!(
                    %error,
                    dir = %dir.display(),
                    "install: could not remove the install dir (clears after the process exits)"
                );
            }
        }
    }

    info!("install: uninstalled");
    Ok(())
}

/// Enable or disable launching `"<exe>" daemon run` at login (Windows:
/// HKCU Run value `mistl`).
#[cfg(windows)]
pub fn set_autostart(enabled: bool) -> Result<()> {
    use winreg::RegKey;
    use winreg::enums::HKEY_CURRENT_USER;

    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let (run, _) = hkcu
        .create_subkey(RUN_KEY_PATH)
        .context("install: opening the HKCU Run key")?;
    if enabled {
        let command = autostart_command(&autostart_exe()?);
        run.set_value(RUN_VALUE_NAME, &command)
            .context("install: writing the HKCU Run value")?;
        info!(%command, "install: autostart enabled");
    } else {
        match run.delete_value(RUN_VALUE_NAME) {
            Ok(()) => info!("install: autostart disabled"),
            // Already absent: disabling is idempotent.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("install: removing the HKCU Run value"),
        }
    }
    Ok(())
}

/// Enable or disable launching `"<exe>" daemon run` at login (Linux: XDG
/// autostart desktop entry).
#[cfg(all(unix, not(target_os = "macos")))]
pub fn set_autostart(enabled: bool) -> Result<()> {
    write_or_remove_autostart_file(enabled, desktop_entry)
}

/// Enable or disable launching `"<exe>" daemon run` at login (macOS:
/// LaunchAgent plist, picked up at the next login).
#[cfg(target_os = "macos")]
pub fn set_autostart(enabled: bool) -> Result<()> {
    write_or_remove_autostart_file(enabled, launch_agent_plist)
}

/// Whether the per-user autostart entry currently exists.
#[cfg(windows)]
pub fn autostart_enabled() -> bool {
    use winreg::RegKey;
    use winreg::enums::HKEY_CURRENT_USER;

    RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey(RUN_KEY_PATH)
        .and_then(|run| run.get_value::<String, _>(RUN_VALUE_NAME))
        .is_ok()
}

/// Whether the per-user autostart entry currently exists.
#[cfg(not(windows))]
pub fn autostart_enabled() -> bool {
    autostart_file_path().map(|path| path.exists()).unwrap_or(false)
}

#[cfg(windows)]
const RUN_KEY_PATH: &str = r"Software\Microsoft\Windows\CurrentVersion\Run";
#[cfg(windows)]
const RUN_VALUE_NAME: &str = "mistl";

/// Shared Unix flow: write the autostart file rendered by `render`, or
/// remove it (idempotently) when disabling.
#[cfg(unix)]
fn write_or_remove_autostart_file(enabled: bool, render: fn(&Path) -> String) -> Result<()> {
    let path = autostart_file_path()?;
    if enabled {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("install: creating {}", parent.display()))?;
        }
        let exe = autostart_exe()?;
        std::fs::write(&path, render(&exe))
            .with_context(|| format!("install: writing {}", path.display()))?;
        info!(path = %path.display(), "install: autostart enabled");
    } else {
        match std::fs::remove_file(&path) {
            Ok(()) => info!(path = %path.display(), "install: autostart disabled"),
            // Already absent: disabling is idempotent.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("install: removing {}", path.display()));
            }
        }
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn autostart_file_path() -> Result<PathBuf> {
    let base = directories::BaseDirs::new()
        .context("install: could not determine the home directory")?;
    Ok(base.config_dir().join("autostart").join("mistl.desktop"))
}

#[cfg(target_os = "macos")]
fn autostart_file_path() -> Result<PathBuf> {
    let base = directories::BaseDirs::new()
        .context("install: could not determine the home directory")?;
    Ok(base
        .home_dir()
        .join("Library")
        .join("LaunchAgents")
        .join("com.tik-choco.mistl.plist"))
}

/// Exe the autostart entry points at: the installed copy when present (a
/// stable path across updates), else the currently running one.
fn autostart_exe() -> Result<PathBuf> {
    let installed = installed_exe_path()?;
    if installed.exists() {
        return Ok(installed);
    }
    std::env::current_exe().context("install: resolving current executable")
}

/// Login command: quoted exe + `daemon run` (headless daemon, no
/// dashboard). Used verbatim as the HKCU Run value and the XDG `Exec=`.
#[cfg_attr(target_os = "macos", allow(dead_code))]
fn autostart_command(exe: &Path) -> String {
    format!("\"{}\" daemon run", exe.display())
}

/// XDG autostart entry (Linux).
#[cfg_attr(not(all(unix, not(target_os = "macos"))), allow(dead_code))]
fn desktop_entry(exe: &Path) -> String {
    format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Name=mistl\n\
         Comment=mistl P2P daemon\n\
         Exec={}\n\
         X-GNOME-Autostart-enabled=true\n",
        autostart_command(exe)
    )
}

/// LaunchAgent plist (macOS). `ProgramArguments` avoids shell quoting.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn launch_agent_plist(exe: &Path) -> String {
    let exe = xml_escape(&exe.display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tik-choco.mistl</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>daemon</string>
        <string>run</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
"#
    )
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Path equality tolerating symlinks/8.3 short names via canonicalization,
/// falling back to a literal compare when either side can't be resolved
/// (e.g. it doesn't exist yet).
fn paths_equal(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

#[cfg(windows)]
fn start_menu_shortcut_path() -> Result<PathBuf> {
    let base = directories::BaseDirs::new()
        .context("install: could not determine the home directory")?;
    // data_dir() is %APPDATA% (Roaming) on Windows.
    Ok(base
        .data_dir()
        .join(r"Microsoft\Windows\Start Menu\Programs")
        .join("mistl.lnk"))
}

/// Create the Start-Menu shortcut pointing at the installed exe (bare
/// `mistl` opens the dashboard). Dependency-free via the WScript.Shell COM
/// object; callers treat failure as a warning, not a hard error.
#[cfg(windows)]
fn create_start_menu_shortcut(exe: &Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    let lnk = start_menu_shortcut_path()?;
    if let Some(parent) = lnk.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("install: creating {}", parent.display()))?;
    }
    let workdir = exe.parent().unwrap_or_else(|| Path::new("."));
    let script = format!(
        "$s=(New-Object -ComObject WScript.Shell).CreateShortcut('{lnk}'); \
         $s.TargetPath='{exe}'; $s.WorkingDirectory='{dir}'; \
         $s.Description='mistl P2P daemon'; $s.Save()",
        lnk = ps_single_quote(&lnk.display().to_string()),
        exe = ps_single_quote(&exe.display().to_string()),
        dir = ps_single_quote(&workdir.display().to_string()),
    );
    let output = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &script,
        ])
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .context("install: spawning powershell for the Start Menu shortcut")?;
    if !output.status.success() {
        bail!(
            "install: shortcut creation failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    info!(lnk = %lnk.display(), "install: Start Menu shortcut created");
    Ok(())
}

#[cfg(windows)]
fn remove_start_menu_shortcut() -> Result<()> {
    let lnk = start_menu_shortcut_path()?;
    match std::fs::remove_file(&lnk) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("install: removing {}", lnk.display())),
    }
}

/// Escape for a PowerShell single-quoted string (`''` is a literal `'`).
#[cfg_attr(not(windows), allow(dead_code))]
fn ps_single_quote(text: &str) -> String {
    text.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn autostart_command_quotes_the_exe_and_runs_the_daemon_headless() {
        let command = autostart_command(Path::new(r"C:\Users\Jo Do\mistl.exe"));
        assert_eq!(command, r#""C:\Users\Jo Do\mistl.exe" daemon run"#);
        assert!(command.ends_with("daemon run"), "must not launch the dashboard");
    }

    #[test]
    fn desktop_entry_is_a_valid_xdg_autostart_stanza() {
        let entry = desktop_entry(Path::new("/home/jo/.local/bin/mistl"));
        assert!(entry.starts_with("[Desktop Entry]\n"));
        assert!(entry.contains("Exec=\"/home/jo/.local/bin/mistl\" daemon run\n"));
        assert!(entry.contains("Type=Application\n"));
    }

    #[test]
    fn launch_agent_plist_escapes_xml_and_splits_args() {
        let plist = launch_agent_plist(Path::new("/Users/jo & co/mistl"));
        assert!(plist.contains("<string>/Users/jo &amp; co/mistl</string>"));
        assert!(plist.contains("<string>daemon</string>"));
        assert!(plist.contains("<string>run</string>"));
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("com.tik-choco.mistl"));
    }

    #[test]
    fn xml_escape_handles_the_reserved_characters() {
        assert_eq!(xml_escape("a & b <c> d"), "a &amp; b &lt;c&gt; d");
        assert_eq!(xml_escape("plain"), "plain");
    }

    #[test]
    fn ps_single_quote_doubles_embedded_quotes() {
        assert_eq!(ps_single_quote("O'Brien"), "O''Brien");
        assert_eq!(ps_single_quote("no quotes"), "no quotes");
    }

    #[test]
    fn installed_exe_lives_directly_in_the_install_dir() {
        let dir = install_dir().unwrap();
        let exe = installed_exe_path().unwrap();
        assert_eq!(exe.parent().unwrap(), dir);
        let name = exe.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, format!("mistl{}", std::env::consts::EXE_SUFFIX));
    }

    #[test]
    fn paths_equal_falls_back_to_literal_compare_for_missing_paths() {
        assert!(paths_equal(
            Path::new("/definitely/missing/a"),
            Path::new("/definitely/missing/a")
        ));
        assert!(!paths_equal(
            Path::new("/definitely/missing/a"),
            Path::new("/definitely/missing/b")
        ));
    }
}
