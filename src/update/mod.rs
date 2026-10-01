//! Self-update from GitHub Releases: check, verify, and stage new binaries.
//!
//! Policy is "download & stage": `update.apply` (and the background task
//! when `[update] auto_apply` is on) downloads the release binary for this
//! build's target triple, verifies its SHA-256 against the release's
//! `SHA256SUMS.txt`, and swaps the on-disk executable in place via
//! `self_replace` -- which handles the Windows rename-then-write dance, so
//! even the currently running daemon can replace its own exe. The staged
//! version takes effect on the next daemon start; background auto-update
//! never force-restarts. An explicit `update.apply {"restart": true}` asks
//! the daemon to restart itself (`AppState::request_restart`) after a
//! successful swap.
//!
//! Release layout (produced by the release workflow): raw binaries named
//! `mistl-v{version}-{target-triple}` (`.exe` on Windows) plus a
//! `SHA256SUMS.txt` with `sha256sum`-style `<hex>  <filename>` lines.
//! Releases without a raw binary for [`TARGET`] (e.g. older archive-only
//! releases) are reported as `asset_available: false` and are never
//! staged; users are pointed at the release page instead. A binary whose
//! checksum can't be verified is never written over the exe.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::config::UpdateConfig;
use crate::daemon::AppState;

/// GitHub repo used when `[update] repo` is empty.
pub const REPO_DEFAULT: &str = "tik-choco/mistl";
/// Rust target triple this binary was built for (emitted by build.rs).
pub const TARGET: &str = env!("MISTL_TARGET");
/// Version baked into this binary. CI stamps it to the release tag, so it
/// equals the tag minus the leading `v`.
pub const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Minisign public key (the base64 line of `minisign.pub`, `RW...`) that
/// release checksums must be signed with. Baked in at build time from the
/// `MISTL_RELEASE_PUBKEY` env var (see docs/release-signing.md); `None` in
/// dev builds and until a release key exists, in which case the updater
/// keeps its unsigned behaviour and logs a warning.
pub const RELEASE_PUBKEY: Option<&str> = option_env!("MISTL_RELEASE_PUBKEY");

/// [`RELEASE_PUBKEY`], treating an empty value (CI exports the variable even
/// when the repo variable is unset) as "no key".
fn release_pubkey() -> Option<&'static str> {
    RELEASE_PUBKEY.filter(|key| !key.trim().is_empty())
}

/// Release asset holding the detached minisign signature of `SHA256SUMS.txt`.
const SUMS_SIG_ASSET: &str = "SHA256SUMS.txt.minisig";
/// A minisig file is ~350 bytes; anything bigger is not one.
const MAX_SIG_BYTES: u64 = 4 * 1024;

/// Delay before the background task's first check after daemon startup,
/// so updates never compete with startup work.
const FIRST_CHECK_DELAY: Duration = Duration::from_secs(30);
/// Config re-poll cadence while `[update] auto_check` is off, so enabling
/// it via `config.set` applies without a daemon restart.
const DISABLED_RECHECK: Duration = Duration::from_secs(15 * 60);

/// Handle `update.*` IPC commands:
/// - `update.status` `{}` -> version/target/config plus install state and
///   the last background/manual check result.
/// - `update.check` `{}` -> `{current, latest, update_available,
///   asset_available, notes_url}`.
/// - `update.apply` `{restart?: bool}` -> `{updated: false, current,
///   latest}` when already up to date, else download + verify + stage and
///   `{updated: true, from, to, restart: "none"|"scheduled"}`.
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    if cmd != "update.status" {
        state.network.require_online()?;
        if crate::runtime::CHANNEL != "stable" {
            bail!("self-update is disabled for development/preview builds");
        }
    }
    match cmd {
        "update.status" => cmd_status(state),
        "update.check" => cmd_check(state).await,
        "update.apply" => cmd_apply(args, state).await,
        _ => bail!("unknown update command: {cmd}"),
    }
}

fn cmd_status(state: &Arc<AppState>) -> Result<Value> {
    let update = state.config().update;
    // The exe updates replace is the one this process runs from.
    let exe_path = std::env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    let (last_check, last_result) = last_check();
    Ok(json!({
        "current": CURRENT_VERSION,
        "target": TARGET,
        "repo": effective_repo(&update.repo),
        "auto_check": update.auto_check,
        "auto_apply": update.auto_apply,
        "check_interval_hours": update.check_interval_hours,
        "installed": crate::install::is_installed(),
        "exe_path": exe_path,
        "autostart_enabled": crate::install::autostart_enabled(),
        "last_check": last_check,
        "last_result": last_result,
    }))
}

async fn cmd_check(state: &Arc<AppState>) -> Result<Value> {
    let update = state.config().update;
    let repo = effective_repo(&update.repo);
    let client = build_client()?;
    let release = fetch_latest_release_recorded(&client, &repo, update.prerelease).await?;

    let update_available = is_update_available(CURRENT_VERSION, &release.version);
    let asset_available = release
        .asset(&asset_name(&release.version, TARGET))
        .is_some();
    record_check(if update_available {
        format!("v{} available", release.version)
    } else {
        "up to date".to_string()
    });
    debug!(
        current = CURRENT_VERSION,
        latest = %release.version,
        update_available,
        asset_available,
        "update: checked"
    );

    Ok(json!({
        "current": CURRENT_VERSION,
        "latest": release.version,
        "update_available": update_available,
        "asset_available": asset_available,
        "notes_url": release.notes_url,
    }))
}

async fn cmd_apply(args: Value, state: &Arc<AppState>) -> Result<Value> {
    let restart = args
        .get("restart")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let update = state.config().update;
    let repo = effective_repo(&update.repo);
    let client = build_client()?;
    let release = fetch_latest_release_recorded(&client, &repo, update.prerelease).await?;

    if !is_update_available(CURRENT_VERSION, &release.version) {
        record_check("up to date".to_string());
        return Ok(json!({
            "updated": false,
            "current": CURRENT_VERSION,
            "latest": release.version,
        }));
    }

    let name = asset_name(&release.version, TARGET);
    let Some(asset) = release.asset(&name) else {
        record_check(format!(
            "v{} available, but no {TARGET} binary on the release",
            release.version
        ));
        bail!(
            "release v{} has no prebuilt binary for {TARGET} (expected asset {name}); \
             download it manually from {}",
            release.version,
            release.notes_url
        );
    };

    if let Err(error) = stage(&client, &release, asset).await {
        record_check(format!("apply failed: {error:#}"));
        return Err(error);
    }
    record_check(format!("updated to v{}", release.version));
    info!(
        from = CURRENT_VERSION,
        to = %release.version,
        "update: staged; takes effect on next daemon start"
    );

    // Only after a verified successful replace may the daemon restart.
    let restart_mode = if restart {
        state.request_restart();
        "scheduled"
    } else {
        "none"
    };
    Ok(json!({
        "updated": true,
        "from": CURRENT_VERSION,
        "to": release.version,
        "restart": restart_mode,
    }))
}

/// Spawn the periodic background auto-check (and, with `[update]
/// auto_apply`, auto-stage) task. Called once from the daemon after
/// startup. Errors are logged and the loop continues -- this task never
/// takes the daemon down, and it never restarts it either: staged updates
/// apply on the next daemon start.
pub fn spawn_auto_update(state: Arc<AppState>) {
    if crate::runtime::CHANNEL != "stable" || !state.network.permitted() {
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(FIRST_CHECK_DELAY).await;
        loop {
            // Re-read config each round so `config.set` hot-reloads apply.
            let update = state.config().update;
            if !update.auto_check {
                tokio::time::sleep(DISABLED_RECHECK).await;
                continue;
            }
            if let Err(error) = auto_check_once(&update).await {
                warn!(%error, "update: background check failed");
                record_check(format!("auto-check failed: {error:#}"));
            }
            let interval =
                Duration::from_secs(update.check_interval_hours.max(1).saturating_mul(3600));
            tokio::time::sleep(interval).await;
        }
    });
}

/// One background round: check, and stage when allowed. Missing assets and
/// disabled auto-apply are normal outcomes (recorded, not errors).
async fn auto_check_once(update: &UpdateConfig) -> Result<()> {
    let repo = effective_repo(&update.repo);
    let client = build_client()?;
    let release = fetch_latest_release(&client, &repo, update.prerelease).await?;

    if !is_update_available(CURRENT_VERSION, &release.version) {
        record_check("up to date".to_string());
        debug!(latest = %release.version, "update: up to date");
        return Ok(());
    }

    let name = asset_name(&release.version, TARGET);
    let Some(asset) = release.asset(&name) else {
        record_check(format!(
            "v{} available, but no {TARGET} binary on the release",
            release.version
        ));
        warn!(
            latest = %release.version,
            target = TARGET,
            notes = %release.notes_url,
            "update: new release has no prebuilt binary for this target; update manually"
        );
        return Ok(());
    };

    if update.auto_apply && !is_default_repo(&repo) {
        record_check(format!(
            "v{} available (auto_apply skipped: custom repo {repo})",
            release.version
        ));
        warn!(
            %repo,
            latest = %release.version,
            "update: refusing to auto-apply from a non-default [update] repo; run `mistl update apply` to install manually"
        );
        return Ok(());
    }

    if !update.auto_apply {
        record_check(format!("v{} available", release.version));
        info!(
            latest = %release.version,
            "update: new version available (auto_apply is off; run `mistl update apply`)"
        );
        return Ok(());
    }

    stage(&client, &release, asset).await?;
    record_check(format!("staged v{}", release.version));
    info!(
        from = CURRENT_VERSION,
        to = %release.version,
        "update: auto-staged; takes effect on next daemon start"
    );
    Ok(())
}

/// One release as returned by the GitHub API (only what we consume).
struct ReleaseInfo {
    /// Version from `tag_name`, leading `v` stripped.
    version: String,
    /// `html_url`: the human release page (notes, manual downloads).
    notes_url: String,
    assets: Vec<ReleaseAsset>,
}

struct ReleaseAsset {
    name: String,
    download_url: String,
}

impl ReleaseInfo {
    fn asset(&self, name: &str) -> Option<&ReleaseAsset> {
        self.assets.iter().find(|asset| asset.name == name)
    }
}

/// Hosts a release download (including every redirect hop) may touch.
fn is_allowed_download_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host == "github.com"
        || host == "api.github.com"
        || host == "githubusercontent.com"
        || host.ends_with(".githubusercontent.com")
}

/// Only `https://` URLs on GitHub-owned hosts are ever fetched by the
/// updater, so a tampered release API response cannot point it elsewhere.
fn is_allowed_download_url(url: &str) -> bool {
    match reqwest::Url::parse(url) {
        Ok(url) => {
            url.scheme() == "https"
                && url.username().is_empty()
                && url.password().is_none()
                && url.host_str().is_some_and(is_allowed_download_host)
        }
        Err(_) => false,
    }
}

/// Whether `repo` is the built-in default (only that repo may auto-apply).
fn is_default_repo(repo: &str) -> bool {
    repo.trim().eq_ignore_ascii_case(REPO_DEFAULT)
}

/// Upper bound on any single downloaded release asset.
const MAX_DOWNLOAD_BYTES: u64 = 256 * 1024 * 1024;

/// GitHub requires a `User-Agent' (403s without one); `.no_proxy()`
/// matches the rest of this repo (local proxy env vars must not intercept
/// daemon traffic).
fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(format!("mistl/{CURRENT_VERSION}"))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 {
                attempt.error("too many redirects")
            } else if is_allowed_download_url(attempt.url().as_str()) {
                attempt.follow()
            } else {
                attempt.error("redirect to a non-GitHub or non-https URL is not allowed")
            }
        }))
        .build()
        .context("update: building HTTP client")
}

async fn fetch_latest_release(
    client: &reqwest::Client,
    repo: &str,
    prerelease: bool,
) -> Result<ReleaseInfo> {
    // `releases/latest` never returns prereleases or drafts; opting into
    // prereleases means "newest release of any kind" via the list endpoint.
    let url = if prerelease {
        format!("https://api.github.com/repos/{repo}/releases?per_page=10")
    } else {
        format!("https://api.github.com/repos/{repo}/releases/latest")
    };
    let response = client
        .get(&url)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .with_context(|| format!("update: GET {url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body: String = response
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(300)
            .collect();
        bail!("update: GitHub API returned {status} for {url}: {body}");
    }
    let value: Value = response
        .json()
        .await
        .with_context(|| format!("update: parsing GitHub API response from {url}"))?;
    let release = if prerelease {
        value
            .as_array()
            .and_then(|releases| releases.first())
            .cloned()
            .with_context(|| format!("update: {repo} has no releases"))?
    } else {
        value
    };
    parse_release(&release)
}

fn parse_release(release: &Value) -> Result<ReleaseInfo> {
    let tag = release
        .get("tag_name")
        .and_then(Value::as_str)
        .context("update: release JSON has no tag_name")?;
    let assets = release
        .get("assets")
        .and_then(Value::as_array)
        .map(|assets| {
            assets
                .iter()
                .filter_map(|asset| {
                    Some(ReleaseAsset {
                        name: asset.get("name")?.as_str()?.to_string(),
                        download_url: asset.get("browser_download_url")?.as_str()?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(ReleaseInfo {
        version: tag.strip_prefix('v').unwrap_or(tag).to_string(),
        notes_url: release
            .get("html_url")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        assets,
    })
}

/// [`fetch_latest_release`], recording a failure into the last-check slot
/// so `update.status` can show why the last check errored.
async fn fetch_latest_release_recorded(
    client: &reqwest::Client,
    repo: &str,
    prerelease: bool,
) -> Result<ReleaseInfo> {
    match fetch_latest_release(client, repo, prerelease).await {
        Ok(release) => Ok(release),
        Err(error) => {
            record_check(format!("check failed: {error:#}"));
            Err(error)
        }
    }
}

/// Download a release asset. GitHub redirects `browser_download_url` to a
/// signed URL; reqwest follows redirects by default.
async fn download(client: &reqwest::Client, url: &str, max_bytes: u64) -> Result<Vec<u8>> {
    if !is_allowed_download_url(url) {
        bail!("update: refusing to download from {url} (only https on GitHub hosts is allowed)");
    }
    let mut response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("update: GET {url}"))?;
    if !response.status().is_success() {
        bail!("update: download returned {} for {url}", response.status());
    }
    if response.content_length().is_some_and(|len| len > max_bytes) {
        bail!("update: {url} is larger than {max_bytes} bytes; refusing");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("update: reading {url}"))?
    {
        if body.len() as u64 + chunk.len() as u64 > max_bytes {
            bail!("update: {url} exceeded {max_bytes} bytes; refusing");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Check the detached minisign signature over `SHA256SUMS.txt`.
///
/// With a pinned key a missing or invalid signature is a hard error (no
/// fallback). Without one (dev builds) only the checksums are checked, as
/// before, with a warning. Only prehashed ("ED", the minisign default)
/// signatures are accepted; the trusted comment signature is verified too.
fn verify_sums_signature(pubkey: Option<&str>, sums: &[u8], sig: Option<&[u8]>) -> Result<()> {
    let Some(pubkey) = pubkey else {
        warn!(
            "update: this build has no release public key (MISTL_RELEASE_PUBKEY); \
             SHA256SUMS.txt is NOT authenticated"
        );
        return Ok(());
    };
    // Accept either the bare base64 line or a whole `minisign.pub` file.
    let key_line = pubkey
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty() && !line.starts_with("untrusted comment:"))
        .context("update: embedded release public key is empty")?;
    let key = minisign_verify::PublicKey::from_base64(key_line).map_err(|error| {
        anyhow::anyhow!("update: embedded release public key is invalid: {error}")
    })?;
    let sig = sig.with_context(|| {
        format!("update: release has no {SUMS_SIG_ASSET}; refusing an unsigned release")
    })?;
    let sig = std::str::from_utf8(sig).context("update: signature is not UTF-8")?;
    let sig = minisign_verify::Signature::decode(sig)
        .map_err(|error| anyhow::anyhow!("update: malformed {SUMS_SIG_ASSET}: {error}"))?;
    key.verify(sums, &sig, false).map_err(|error| {
        anyhow::anyhow!("update: SHA256SUMS.txt signature check failed: {error}; refusing")
    })?;
    debug!(
        trusted_comment = sig.trusted_comment(),
        "update: checksums signature verified"
    );
    Ok(())
}

/// Download + verify + self-replace. The SHA-256 is checked against the
/// release's `SHA256SUMS.txt` BEFORE anything touches the on-disk exe; an
/// unverifiable binary is never installed.
async fn stage(
    client: &reqwest::Client,
    release: &ReleaseInfo,
    asset: &ReleaseAsset,
) -> Result<()> {
    // One replace at a time (an explicit apply racing background auto-apply).
    let _guard = apply_lock().lock().await;

    let sums_asset = release
        .assets
        .iter()
        .find(|asset| asset.name.eq_ignore_ascii_case("SHA256SUMS.txt"))
        .with_context(|| {
            format!(
                "release v{} has no SHA256SUMS.txt; refusing to install an unverifiable binary \
                 (see {})",
                release.version, release.notes_url
            )
        })?;
    let sums = download(client, &sums_asset.download_url, MAX_DOWNLOAD_BYTES).await?;
    // Authenticate SHA256SUMS.txt BEFORE trusting any checksum in it.
    let sig = match release.asset(SUMS_SIG_ASSET) {
        Some(sig_asset) => {
            match download(client, &sig_asset.download_url, MAX_SIG_BYTES).await {
                Ok(bytes) => Some(bytes),
                // A failed signature download is fatal only when a key is pinned.
                Err(error) if release_pubkey().is_some() => return Err(error),
                Err(error) => {
                    warn!("update: could not download {SUMS_SIG_ASSET}: {error:#}");
                    None
                }
            }
        }
        None => None,
    };
    verify_sums_signature(release_pubkey(), &sums, sig.as_deref())?;
    let expected = parse_sha256sums(&String::from_utf8_lossy(&sums), &asset.name)
        .with_context(|| format!("update: SHA256SUMS.txt has no entry for {}", asset.name))?;

    info!(asset = %asset.name, url = %asset.download_url, "update: downloading");
    let binary = download(client, &asset.download_url, MAX_DOWNLOAD_BYTES).await?;
    let actual = sha256_hex(&binary);
    if actual != expected {
        bail!(
            "update: sha256 mismatch for {} (expected {expected}, got {actual}); refusing to \
             install",
            asset.name
        );
    }
    debug!(sha256 = %actual, size = binary.len(), "update: verified");

    tokio::task::spawn_blocking(move || replace_current_exe(&binary))
        .await
        .context("update: replace task panicked")??;
    Ok(())
}

/// Swap the verified bytes in over the running executable: write them to a
/// sibling temp file (same volume, so the swap is a rename), mark it
/// executable on Unix, then `self_replace` -- which handles replacing a
/// running exe on Windows. The temp file is removed on both paths.
fn replace_current_exe(bytes: &[u8]) -> Result<()> {
    let exe = std::env::current_exe().context("update: resolving current executable")?;
    let dir = exe
        .parent()
        .context("update: current executable has no parent directory")?;
    // `create_new` (O_EXCL) with an unpredictable name: never follows or
    // clobbers a pre-planted file/symlink in the exe directory.
    let temp = dir.join(format!(
        ".mistl-update-{}-{:016x}.tmp",
        std::process::id(),
        rand::random::<u64>()
    ));
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .with_context(|| format!("update: creating {}", temp.display()))?;
        if let Err(error) = file.write_all(bytes) {
            drop(file);
            let _ = std::fs::remove_file(&temp);
            return Err(error).with_context(|| format!("update: writing {}", temp.display()));
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("update: marking {} executable", temp.display()))?;
    }

    let result = self_replace::self_replace(&temp)
        .with_context(|| format!("update: replacing {}", exe.display()));
    let _ = std::fs::remove_file(&temp);
    result
}

/// `[update] repo`, falling back to [`REPO_DEFAULT`] when empty.
fn effective_repo(configured: &str) -> String {
    let repo = configured.trim();
    if repo.is_empty() {
        REPO_DEFAULT.to_string()
    } else {
        repo.to_string()
    }
}

/// Release asset filename for `version` on `target`:
/// `mistl-v{version}-{target}`, plus `.exe` for Windows targets.
fn asset_name(version: &str, target: &str) -> String {
    let ext = if target.contains("windows") {
        ".exe"
    } else {
        ""
    };
    format!("mistl-v{version}-{target}{ext}")
}

/// `latest > current` by semver. Unparseable versions (odd tags, dev
/// builds) are logged and treated as "no update" rather than failing.
fn is_update_available(current: &str, latest: &str) -> bool {
    let parsed_current = match semver::Version::parse(current) {
        Ok(version) => version,
        Err(error) => {
            warn!(%error, current, "update: current version is not semver; skipping compare");
            return false;
        }
    };
    let parsed_latest = match semver::Version::parse(latest) {
        Ok(version) => version,
        Err(error) => {
            warn!(%error, latest, "update: latest release tag is not semver; skipping compare");
            return false;
        }
    };
    parsed_latest > parsed_current
}

/// Find the SHA-256 for `filename` in `sha256sum`-format text: one
/// `<hex><whitespace><filename>` per line (coreutils writes two spaces, or
/// ` *` in binary mode). Returns the lowercased hex.
fn parse_sha256sums(sums: &str, filename: &str) -> Option<String> {
    sums.lines().find_map(|line| {
        let (hash, name) = line.trim().split_once(char::is_whitespace)?;
        let name = name.trim().trim_start_matches('*');
        (name == filename && hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
            .then(|| hash.to_ascii_lowercase())
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn apply_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// `(when_rfc3339, short_result)` of the most recent check or apply
/// attempt, reported by `update.status`. In-memory only; resets on daemon
/// restart.
fn last_check_cell() -> &'static Mutex<Option<(String, String)>> {
    static CELL: OnceLock<Mutex<Option<(String, String)>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

fn record_check(result: impl Into<String>) {
    *last_check_cell()
        .lock()
        .expect("update: last-check lock poisoned") =
        Some((chrono::Utc::now().to_rfc3339(), result.into()));
}

fn last_check() -> (Option<String>, Option<String>) {
    match last_check_cell()
        .lock()
        .expect("update: last-check lock poisoned")
        .clone()
    {
        Some((at, result)) => (Some(at), Some(result)),
        None => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Vector from minisign-verify's own tests: prehashed ("ED") signature over
    // the bytes `test`.
    const TEST_KEY: &str = "RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3";
    const TEST_SIG: &str = "untrusted comment: signature from minisign secret key
RUQf6LRCGA9i559r3g7V1qNyJDApGip8MfqcadIgT9CuhV3EMhHoN1mGTkUidF/z7SrlQgXdy8ofjb7bNJJylDOocrCo8KLzZwo=
trusted comment: timestamp:1556193335\tfile:test
y/rUw2y8/hOUYjZU71eHp/Wo1KZ40fGy2VJEDl34XMJM+TX48Ss/17u3IvIfbVR1FkZZSNCisQbuQY+bHwhEBg==";

    #[test]
    fn valid_signature_is_accepted() {
        verify_sums_signature(Some(TEST_KEY), b"test", Some(TEST_SIG.as_bytes())).unwrap();
        // A whole minisign.pub file is accepted as the pinned key too.
        let pub_file =
            format!("untrusted comment: minisign public key E7620F1842B4E81F\n{TEST_KEY}\n");
        verify_sums_signature(Some(&pub_file), b"test", Some(TEST_SIG.as_bytes())).unwrap();
    }

    #[test]
    fn tampered_sums_are_rejected() {
        let err =
            verify_sums_signature(Some(TEST_KEY), b"tesT", Some(TEST_SIG.as_bytes())).unwrap_err();
        assert!(
            format!("{err:#}").contains("signature check failed"),
            "{err:#}"
        );
    }

    #[test]
    fn tampered_trusted_comment_is_rejected() {
        let sig = TEST_SIG.replace("file:test", "file:evil");
        assert!(verify_sums_signature(Some(TEST_KEY), b"test", Some(sig.as_bytes())).is_err());
    }

    #[test]
    fn wrong_key_is_rejected() {
        // Same layout as a minisign key ("Ed" + key id + pubkey) but a
        // different key, so the key id no longer matches.
        use base64::Engine;
        use ed25519_dalek::SigningKey;
        let other = SigningKey::from_bytes(&[7u8; 32]).verifying_key();
        let mut raw = b"Ed".to_vec();
        raw.extend_from_slice(&[1u8; 8]);
        raw.extend_from_slice(other.as_bytes());
        let other_key = base64::engine::general_purpose::STANDARD.encode(raw);
        assert!(
            verify_sums_signature(Some(&other_key), b"test", Some(TEST_SIG.as_bytes())).is_err()
        );
    }

    #[test]
    fn missing_signature_with_pinned_key_is_an_error() {
        let err = verify_sums_signature(Some(TEST_KEY), b"test", None).unwrap_err();
        assert!(format!("{err:#}").contains("unsigned"), "{err:#}");
    }

    #[test]
    fn malformed_signature_or_key_is_an_error() {
        assert!(verify_sums_signature(Some(TEST_KEY), b"test", Some(b"garbage")).is_err());
        assert!(
            verify_sums_signature(Some("not-a-key"), b"test", Some(TEST_SIG.as_bytes())).is_err()
        );
        assert!(verify_sums_signature(Some("  "), b"test", Some(TEST_SIG.as_bytes())).is_err());
    }

    #[test]
    fn no_pinned_key_keeps_unsigned_behaviour() {
        verify_sums_signature(None, b"anything", None).unwrap();
        verify_sums_signature(None, b"anything", Some(b"junk")).unwrap();
    }

    #[test]
    fn asset_names_add_exe_only_for_windows_targets() {
        assert_eq!(
            asset_name("0.2.0", "x86_64-pc-windows-msvc"),
            "mistl-v0.2.0-x86_64-pc-windows-msvc.exe"
        );
        assert_eq!(
            asset_name("0.2.0", "x86_64-unknown-linux-gnu"),
            "mistl-v0.2.0-x86_64-unknown-linux-gnu"
        );
        assert_eq!(
            asset_name("1.0.0-rc.1", "aarch64-apple-darwin"),
            "mistl-v1.0.0-rc.1-aarch64-apple-darwin"
        );
    }

    #[test]
    fn sha256sums_parsing_handles_coreutils_variants() {
        let sums = format!(
            "{}  mistl-v0.2.0-x86_64-pc-windows-msvc.exe\n{}  other-file\n",
            "a".repeat(64),
            "b".repeat(64)
        );
        assert_eq!(
            parse_sha256sums(&sums, "mistl-v0.2.0-x86_64-pc-windows-msvc.exe"),
            Some("a".repeat(64))
        );
        assert_eq!(parse_sha256sums(&sums, "other-file"), Some("b".repeat(64)));
        assert_eq!(parse_sha256sums(&sums, "missing"), None);

        // Binary-mode `*` marker, CRLF line endings, uppercase hex.
        let sums = format!("{} *mistl.exe\r\n", "A".repeat(64));
        assert_eq!(parse_sha256sums(&sums, "mistl.exe"), Some("a".repeat(64)));

        // Tab separator.
        let sums = format!("{}\tmistl\n", "c".repeat(64));
        assert_eq!(parse_sha256sums(&sums, "mistl"), Some("c".repeat(64)));
    }

    #[test]
    fn sha256sums_rejects_malformed_lines() {
        assert_eq!(parse_sha256sums("not-a-sums-file", "x"), None);
        assert_eq!(parse_sha256sums("", "x"), None);
        // Short hash.
        assert_eq!(parse_sha256sums("deadbeef  x", "x"), None);
        // Non-hex hash of the right length.
        let sums = format!("{}  x", "z".repeat(64));
        assert_eq!(parse_sha256sums(&sums, "x"), None);
    }

    #[test]
    fn semver_compare_drives_update_available() {
        assert!(is_update_available("0.1.0", "0.2.0"));
        assert!(is_update_available("0.1.0", "0.1.1"));
        assert!(!is_update_available("0.2.0", "0.2.0"));
        assert!(!is_update_available("0.3.0", "0.2.9"));
        // Prereleases order below their release per semver.
        assert!(is_update_available("0.1.0", "0.2.0-rc.1"));
        assert!(!is_update_available("0.2.0", "0.2.0-rc.1"));
        // Unparseable on either side = no update, not an error.
        assert!(!is_update_available("not-a-version", "0.2.0"));
        assert!(!is_update_available("0.1.0", "nightly"));
    }

    #[test]
    fn release_json_parses_tag_notes_and_assets() {
        let value = serde_json::json!({
            "tag_name": "v0.2.0",
            "html_url": "https://github.com/tik-choco/mistl/releases/tag/v0.2.0",
            "assets": [
                { "name": "mistl-v0.2.0-x86_64-pc-windows-msvc.exe",
                  "browser_download_url": "https://example.com/mistl.exe" },
                { "name": "SHA256SUMS.txt",
                  "browser_download_url": "https://example.com/sums" },
                { "name": 42 }
            ]
        });
        let release = parse_release(&value).unwrap();
        assert_eq!(release.version, "0.2.0");
        assert_eq!(
            release.notes_url,
            "https://github.com/tik-choco/mistl/releases/tag/v0.2.0"
        );
        // The malformed third asset is skipped, not fatal.
        assert_eq!(release.assets.len(), 2);
        assert!(release.asset("SHA256SUMS.txt").is_some());
        assert!(release.asset("missing").is_none());

        // No tag_name is an error; a tag without the leading `v` is fine.
        assert!(parse_release(&serde_json::json!({ "html_url": "x" })).is_err());
        let release = parse_release(&serde_json::json!({ "tag_name": "0.3.0" })).unwrap();
        assert_eq!(release.version, "0.3.0");
        assert!(release.assets.is_empty());
        assert_eq!(release.notes_url, "");
    }

    #[test]
    fn download_urls_are_limited_to_https_github_hosts() {
        for ok in [
            "https://github.com/tik-choco/mistl/releases/download/v1/x.exe",
            "https://objects.githubusercontent.com/a/b?sig=1",
            "https://release-assets.githubusercontent.com/x",
            "https://api.github.com/repos/a/b",
        ] {
            assert!(is_allowed_download_url(ok), "{ok}");
        }
        for bad in [
            "http://github.com/x",
            "https://evil.example/x",
            "https://github.com.evil.example/x",
            "https://evilgithubusercontent.com/x",
            "https://user@github.com/x",
            "file:///etc/passwd",
            "not a url",
        ] {
            assert!(!is_allowed_download_url(bad), "{bad}");
        }
    }

    #[test]
    fn only_the_default_repo_counts_as_default() {
        assert!(is_default_repo(REPO_DEFAULT));
        assert!(!is_default_repo("me/fork"));
    }

    #[test]
    fn empty_repo_falls_back_to_default() {
        assert_eq!(effective_repo(""), REPO_DEFAULT);
        assert_eq!(effective_repo("   "), REPO_DEFAULT);
        assert_eq!(effective_repo("me/fork"), "me/fork");
        assert_eq!(effective_repo(" me/fork "), "me/fork");
    }

    #[test]
    fn sha256_hex_matches_known_vector() {
        // sha256("") -- the canonical empty-input vector.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
