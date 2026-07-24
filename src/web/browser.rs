//! Shared "open a URL in the default browser" helper, used by both the CLI
//! (`mistl` / `mistl ui`) and the daemon's dashboard auto-reopen.

use std::process::Stdio;

/// Try to open `url` in the platform default browser. Returns whether the
/// opener process reported success; never prints to stdout/stderr (child
/// stdio is dropped).
pub fn open_in_browser(url: &str) -> bool {
    // Headless/minimal environments (containers, WSL without a registered
    // browser, servers) have no opener at all -- xdg-open in particular
    // shell-probes a chain of text browsers (www-browser, links2, elinks,
    // links, lynx, w3m) and prints a "not found" line for each before giving
    // up. That's harmless (callers already fall back to printing the URL)
    // but reads like a wall of errors, so the child's stdout/stderr are
    // dropped rather than inherited.

    #[cfg(windows)]
    let opened = std::process::Command::new("cmd")
        .args(["/c", "start", "", url])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    #[cfg(target_os = "macos")]
    let opened = std::process::Command::new("open")
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    #[cfg(all(unix, not(target_os = "macos")))]
    let opened = std::process::Command::new("xdg-open")
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    opened
}
