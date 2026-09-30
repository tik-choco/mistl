//! Dashboard authentication: one persistent random token, exchanged once for
//! an `HttpOnly` session cookie.
//!
//! The token lives in `<data_dir>/dashboard-token` (0600 on Unix), so the
//! browser cookie survives daemon restarts. Every opener of the dashboard
//! (`mistl ui`, tray, auto-reopen, `daemon status`) uses [`dashboard_url`],
//! which appends `?token=<t>`; the server answers that with a 303 to `/` plus
//! `Set-Cookie: mistl_session_<port>=<t>`, and every other route requires the cookie.

use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use rand::RngCore;

/// Token file name inside the daemon data dir.
pub const TOKEN_FILE: &str = "dashboard-token";
/// Session cookie name prefix; the dashboard port is appended.
///
/// Cookies are scoped by host, not port, so two daemons on 127.0.0.1 (e.g. a
/// stable and an isolated dev instance) would otherwise overwrite each
/// other's session.
pub const COOKIE_PREFIX: &str = "mistl_session_";
/// Token length in random bytes (hex-encoded to twice as many characters).
const TOKEN_BYTES: usize = 32;

/// Process-wide cached token (read or created on first use).
pub fn token() -> Result<&'static str> {
    static TOKEN: OnceLock<String> = OnceLock::new();
    if let Some(t) = TOKEN.get() {
        return Ok(t);
    }
    let dir = crate::config::data_dir()?;
    let t = load_or_create(&dir)?;
    Ok(TOKEN.get_or_init(|| t))
}

/// Read the token file, creating it (atomically, 0600 on Unix) when absent or
/// malformed. Safe against a CLI and a daemon creating it concurrently: the
/// loser of the race adopts the winner's file.
pub fn load_or_create(dir: &Path) -> Result<String> {
    let path = dir.join(TOKEN_FILE);
    if let Some(existing) = read_valid(&path) {
        return Ok(existing);
    }
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let mut raw = [0u8; TOKEN_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut raw);
    let fresh: String = raw.iter().map(|b| format!("{b:02x}")).collect();

    let tmp = path.with_extension(format!("{:016x}.tmp", rand::random::<u64>()));
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?;
    file.write_all(fresh.as_bytes())?;
    file.sync_all()?;
    drop(file);
    // `hard_link` fails if the target exists, which makes creation race-free;
    // a malformed leftover file is replaced by rename instead.
    let linked = std::fs::hard_link(&tmp, &path).is_ok();
    if !linked && read_valid(&path).is_none() {
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming {} into {}", tmp.display(), path.display()))?;
        return Ok(fresh);
    }
    let _ = std::fs::remove_file(&tmp);
    read_valid(&path).with_context(|| format!("reading {}", path.display()))
}

fn read_valid(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let text = text.trim();
    (text.len() >= TOKEN_BYTES * 2 && text.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| text.to_string())
}

/// The URL to open/show: the plain dashboard URL plus `?token=<t>`. Falls back
/// to the plain URL (which serves the "locked" page) if the token can't be
/// read, rather than failing the caller.
pub fn dashboard_url(listen: &str) -> String {
    let base = super::dashboard_base_url(listen);
    match token() {
        Ok(t) => format!("{base}?token={t}"),
        Err(error) => {
            tracing::warn!(%error, "web: could not load the dashboard token");
            base
        }
    }
}

/// `url` with any `token=` query value masked, for log lines.
pub fn redact(url: &str) -> String {
    match url.split_once("?token=") {
        Some((base, _)) => format!("{base}?token=<redacted>"),
        None => url.to_string(),
    }
}

/// Constant-time equality (length is not secret).
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Value of cookie `name` in a raw `Cookie` header.
pub fn cookie_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header.split(';').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k.trim() == name).then(|| v.trim())
    })
}

/// Session cookie name for the dashboard served on `port`.
pub fn cookie_name(port: u16) -> String {
    format!("{COOKIE_PREFIX}{port}")
}

/// `Set-Cookie` value that establishes the session for the dashboard on `port`.
pub fn session_cookie(token: &str, port: u16) -> String {
    format!(
        "{}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=31536000",
        cookie_name(port)
    )
}

/// Value of the `token` parameter in a raw query string (tokens are hex, so
/// no percent-decoding is needed; anything else simply won't match).
pub fn query_token(query: &str) -> Option<&str> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == "token").then_some(v)
    })
}

/// Whether the `Cookie` header value carries the valid session token for the
/// dashboard on `port`.
pub fn cookie_authorized(cookie_header: Option<&str>, token: &str, port: u16) -> bool {
    cookie_header
        .and_then(|h| cookie_value(h, &cookie_name(port)))
        .is_some_and(|v| ct_eq(v.as_bytes(), token.as_bytes()))
}

/// Bilingual page shown to a client that has no valid session.
pub const LOCKED_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>mistl - locked</title>
<style>body{font:16px/1.6 system-ui,sans-serif;max-width:36rem;margin:12vh auto;padding:0 1rem;color:#222;background:#fafafa}
@media(prefers-color-scheme:dark){body{color:#ddd;background:#181818}}code{background:#8882;padding:.1em .4em;border-radius:4px}</style>
</head><body>
<h1>mistl dashboard is locked</h1>
<p>Open the dashboard with <code>mistl</code> or <code>mistl ui</code>, from the tray icon, or use the URL shown by <code>mistl daemon status</code> (it contains the access token).</p>
<h1>mistl ダッシュボードはロックされています</h1>
<p><code>mistl</code> または <code>mistl ui</code>、トレイアイコンから開くか、<code>mistl daemon status</code> に表示される URL(アクセストークン付き)を使ってください。</p>
</body></html>
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ct_eq_compares_content_and_length() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn cookie_value_finds_among_several() {
        let h = "a=1; mistl_session_6480=tok; b=2";
        assert_eq!(cookie_value(h, &cookie_name(6480)), Some("tok"));
        assert_eq!(cookie_value(h, "b"), Some("2"));
        assert_eq!(cookie_value(h, "missing"), None);
        // A name that merely ends with ours must not match.
        assert_eq!(
            cookie_value("xmistl_session_6480=t", &cookie_name(6480)),
            None
        );
    }

    #[test]
    fn cookie_authorized_requires_exact_token() {
        assert!(cookie_authorized(
            Some("mistl_session_6480=abc"),
            "abc",
            6480
        ));
        assert!(!cookie_authorized(
            Some("mistl_session_6480=abd"),
            "abc",
            6480
        ));
        // Another instance's session (different port) does not unlock this one.
        assert!(!cookie_authorized(
            Some("mistl_session_6481=abc"),
            "abc",
            6480
        ));
        assert!(!cookie_authorized(Some("other=abc"), "abc", 6480));
        assert!(!cookie_authorized(None, "abc", 6480));
    }

    #[test]
    fn query_token_extracts_value() {
        assert_eq!(query_token("token=abc"), Some("abc"));
        assert_eq!(query_token("x=1&token=abc"), Some("abc"));
        assert_eq!(query_token("x=1"), None);
    }

    #[test]
    fn redact_masks_token() {
        assert_eq!(
            redact("http://127.0.0.1:6480/?token=secret"),
            "http://127.0.0.1:6480/?token=<redacted>"
        );
        assert_eq!(redact("http://h/"), "http://h/");
    }

    #[test]
    fn token_file_is_created_once_and_reused() {
        let dir = std::env::temp_dir().join(format!("mistl-auth-{:016x}", rand::random::<u64>()));
        let first = load_or_create(&dir).unwrap();
        assert_eq!(first.len(), TOKEN_BYTES * 2);
        assert_eq!(load_or_create(&dir).unwrap(), first);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
