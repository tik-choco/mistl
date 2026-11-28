//! Dashboard uploads for the "Web" page: the browser streams a built site
//! folder file by file into a staging directory, then `finish` publishes that
//! directory with the regular `web.publish` command.
//!
//! - `POST /api/web/upload/begin` `{name, entry?, spa?, coi?}` -> `{upload_id}`
//! - `PUT /api/web/upload/<id>/file?path=<percent-encoded path>`, raw body
//! - `POST /api/web/upload/<id>/finish` `{dry_run?}` -> the `web.publish` result
//! - `DELETE /api/web/upload/<id>` -> abort
//!
//! Staging lives in `<data_dir>/web/staging/<id>/`. A file is accepted only if
//! `web.publish` would publish it (the shared path rule, no dotfiles except
//! `.well-known`), so the staged tree is exactly the published one. Like
//! `web.publish`, this is for local dashboard sessions only.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use super::server::{
    RequestHead, is_guard_denied, percent_decode, read_body, read_body_to_file, write_error,
    write_json_response,
};
use crate::daemon::{AppState, Caller};
use crate::web_host::site;

pub(super) const ROUTE_PREFIX: &str = "/api/web/upload/";
/// Upload sessions open at once.
const MAX_SESSIONS: usize = 2;
/// A session with no request for this long is dropped with its files.
const IDLE_EXPIRY: Duration = Duration::from_secs(30 * 60);
/// Body cap for the `begin` / `finish` JSON.
const MAX_JSON_BYTES: usize = 4 * 1024;
/// File bodies streamed at once, over all sessions (the page sends ≤ 3).
const MAX_PARALLEL_PUTS: usize = 6;

static PUT_SLOTS: Semaphore = Semaphore::const_new(MAX_PARALLEL_PUTS);
static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    sessions: Vec::new(),
});
/// Serializes staging-root sweeps with session directory creation.
static SWEEP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
/// Whether this process has swept leftovers of earlier runs.
static SWEPT: AtomicBool = AtomicBool::new(false);

/// Publish options fixed at `begin`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Options {
    name: String,
    entry: Option<String>,
    spa: bool,
    coi: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Open,
    /// `web.publish` is reading the staging directory.
    Finishing,
    /// Aborted while files were still being written; the last one removes it.
    Aborted,
}

struct Session {
    id: String,
    dir: PathBuf,
    opts: Options,
    last: Instant,
    /// Manifest paths reserved or written.
    files: HashSet<String>,
    bytes: u64,
    /// File bodies being written.
    active: usize,
    phase: Phase,
}

#[derive(Debug, PartialEq, Eq)]
enum Reject {
    Unknown,
    TooManySessions,
    Closed,
    Busy,
    Duplicate(String),
    TooManyFiles,
    FileTooLarge,
    SiteTooLarge(u64),
}

impl Reject {
    fn status(&self) -> (u16, &'static str) {
        match self {
            Reject::Unknown => (404, "Not Found"),
            Reject::TooManySessions => (503, "Service Unavailable"),
            Reject::Closed | Reject::Busy | Reject::Duplicate(_) => (409, "Conflict"),
            Reject::TooManyFiles | Reject::FileTooLarge | Reject::SiteTooLarge(_) => {
                (413, "Payload Too Large")
            }
        }
    }

    fn message(&self) -> String {
        match self {
            Reject::Unknown => "unknown or expired upload; start again".to_string(),
            Reject::TooManySessions => {
                format!("{MAX_SESSIONS} site uploads are already in progress; try again later")
            }
            Reject::Closed => "this upload was finished or cancelled".to_string(),
            Reject::Busy => "files are still being uploaded".to_string(),
            Reject::Duplicate(rel) => format!("{rel:?} was already uploaded"),
            Reject::TooManyFiles => format!("the site has more than {} files", site::MAX_FILES),
            Reject::FileTooLarge => format!(
                "the file is over the per-file limit of {} bytes",
                site::MAX_FILE_BYTES
            ),
            Reject::SiteTooLarge(cap) => format!("the site is over the size limit of {cap} bytes"),
        }
    }
}

struct Registry {
    sessions: Vec<Session>,
}

impl Registry {
    fn position(&self, id: &str) -> Option<usize> {
        self.sessions.iter().position(|s| s.id == id)
    }

    /// Drops idle sessions (never one with a write in flight or a publish
    /// running) and returns their directories for deletion.
    fn expire(&mut self, now: Instant) -> Vec<PathBuf> {
        let mut gone = Vec::new();
        self.sessions.retain(|s| {
            let idle = s.active == 0
                && s.phase != Phase::Finishing
                && now.saturating_duration_since(s.last) >= IDLE_EXPIRY;
            if idle {
                gone.push(s.dir.clone());
            }
            !idle
        });
        gone
    }

    fn begin(
        &mut self,
        id: String,
        dir: PathBuf,
        opts: Options,
        now: Instant,
    ) -> Result<(), Reject> {
        if self.sessions.len() >= MAX_SESSIONS {
            return Err(Reject::TooManySessions);
        }
        self.sessions.push(Session {
            id,
            dir,
            opts,
            last: now,
            files: HashSet::new(),
            bytes: 0,
            active: 0,
            phase: Phase::Open,
        });
        Ok(())
    }

    fn live_ids(&self) -> HashSet<String> {
        self.sessions.iter().map(|s| s.id.clone()).collect()
    }

    fn is_open(&self, id: &str) -> bool {
        self.position(id)
            .is_some_and(|i| self.sessions[i].phase == Phase::Open)
    }

    /// Claims `rel` and `size` bytes of the budget for a write; returns the
    /// session directory. `max_total` is `[web] max_site_bytes`.
    fn reserve(
        &mut self,
        id: &str,
        rel: &str,
        size: u64,
        max_total: u64,
        now: Instant,
    ) -> Result<PathBuf, Reject> {
        let i = self.position(id).ok_or(Reject::Unknown)?;
        let s = &mut self.sessions[i];
        if s.phase != Phase::Open {
            return Err(Reject::Closed);
        }
        if size > site::MAX_FILE_BYTES {
            return Err(Reject::FileTooLarge);
        }
        if s.files.contains(rel) {
            return Err(Reject::Duplicate(rel.to_string()));
        }
        if s.files.len() >= site::MAX_FILES {
            return Err(Reject::TooManyFiles);
        }
        let cap = max_total.min(site::MAX_SITE_BYTES);
        let total = s.bytes.saturating_add(size);
        if total > cap {
            return Err(Reject::SiteTooLarge(cap));
        }
        s.files.insert(rel.to_string());
        s.bytes = total;
        s.active += 1;
        s.last = now;
        Ok(s.dir.clone())
    }

    /// Ends a write started by [`Registry::reserve`]; a failed one gives its
    /// path and bytes back. Returns the directory to delete when the session
    /// was aborted meanwhile and this was its last write.
    fn end_write(
        &mut self,
        id: &str,
        rel: &str,
        size: u64,
        ok: bool,
        now: Instant,
    ) -> Option<PathBuf> {
        let i = self.position(id)?;
        let s = &mut self.sessions[i];
        s.active = s.active.saturating_sub(1);
        s.last = now;
        if !ok && s.files.remove(rel) {
            s.bytes = s.bytes.saturating_sub(size);
        }
        if s.phase == Phase::Aborted && s.active == 0 {
            return Some(self.sessions.remove(i).dir);
        }
        None
    }

    /// Locks the session for publishing once no write is in flight.
    fn start_finish(&mut self, id: &str) -> Result<(PathBuf, Options), Reject> {
        let i = self.position(id).ok_or(Reject::Unknown)?;
        let s = &mut self.sessions[i];
        if s.phase != Phase::Open {
            return Err(Reject::Closed);
        }
        if s.active > 0 {
            return Err(Reject::Busy);
        }
        s.phase = Phase::Finishing;
        Ok((s.dir.clone(), s.opts.clone()))
    }

    fn remove(&mut self, id: &str) -> Option<PathBuf> {
        let i = self.position(id)?;
        Some(self.sessions.remove(i).dir)
    }

    /// Removes the session now (returning its directory) when no write is in
    /// flight; otherwise the last write removes it.
    fn abort(&mut self, id: &str) -> Result<Option<PathBuf>, Reject> {
        let i = self.position(id).ok_or(Reject::Unknown)?;
        if self.sessions[i].phase == Phase::Finishing {
            return Err(Reject::Closed);
        }
        if self.sessions[i].active == 0 {
            return Ok(Some(self.sessions.remove(i).dir));
        }
        self.sessions[i].phase = Phase::Aborted;
        Ok(None)
    }
}

fn registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner())
}

/// `[0-9a-f]{32}`, as made by [`new_upload_id`].
fn is_upload_id(s: &str) -> bool {
    s.len() == 32
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn new_upload_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// Checks a client path against what `web.publish` publishes: the shared
/// manifest path rule, and no dotfile / dot-folder segment except
/// `.well-known` (`publish::scan` skips those). On Windows, names the
/// filesystem would alter or refuse are rejected too.
fn staged_path(raw: &str) -> Result<String, String> {
    site::validate_path(raw).map_err(|e| format!("cannot upload {raw:?}: {e}"))?;
    for seg in raw.split('/') {
        if seg.starts_with('.') && seg != ".well-known" {
            return Err(format!(
                "cannot upload {raw:?}: dotfiles are not published (except .well-known)"
            ));
        }
        if cfg!(windows) && !portable_on_windows(seg) {
            return Err(format!(
                "cannot upload {raw:?}: {seg:?} is not a valid file name on Windows"
            ));
        }
    }
    Ok(raw.to_string())
}

/// Whether Windows stores `seg` under exactly that name: no reserved
/// characters, no trailing dot/space, not a device name.
fn portable_on_windows(seg: &str) -> bool {
    if seg.ends_with('.')
        || seg.ends_with(' ')
        || seg
            .chars()
            .any(|c| matches!(c, '<' | '>' | ':' | '"' | '|' | '*'))
    {
        return false;
    }
    let stem = seg
        .split('.')
        .next()
        .unwrap_or("")
        .trim_end()
        .to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ["COM", "LPT"].iter().any(|p| {
            stem.strip_prefix(p)
                .is_some_and(|n| matches!(n.as_bytes(), [b'1'..=b'9']))
        });
    !reserved
}

/// Optional string field: absent, null or blank -> `None`.
fn opt_str<'a>(v: &'a Value, key: &str) -> Result<Option<&'a str>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.trim()).filter(|s| !s.is_empty())),
        Some(_) => Err(format!("`{key}` must be a string")),
    }
}

fn opt_bool(v: &Value, key: &str) -> Result<Option<bool>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(format!("`{key}` must be true, false or null")),
    }
}

/// The `begin` body. `name` is required: the staging directory has no
/// meaningful name to derive one from.
fn parse_options(v: &Value) -> Result<Options, String> {
    let name = opt_str(v, "name")?.ok_or("missing `name`")?;
    if !site::valid_name(name) {
        return Err(format!(
            "invalid site name {name:?}: use 1-48 lowercase letters, digits and `-` (not at the ends)"
        ));
    }
    let entry = opt_str(v, "entry")?;
    if let Some(entry) = entry {
        site::validate_path(entry).map_err(|e| format!("invalid entry {entry:?}: {e}"))?;
    }
    Ok(Options {
        name: name.to_string(),
        entry: entry.map(str::to_string),
        spa: opt_bool(v, "spa")?.unwrap_or(false),
        coi: opt_bool(v, "coi")?,
    })
}

fn staging_root() -> anyhow::Result<PathBuf> {
    let dir = crate::config::data_dir()?.join("web").join("staging");
    Ok(std::path::absolute(dir)?)
}

/// Deletes everything under `root` that is not a live session directory.
async fn sweep(root: &Path, live: &HashSet<String>) {
    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.file_name().to_str().is_some_and(|n| live.contains(n)) {
            continue;
        }
        let path = entry.path();
        let removed = match entry.file_type().await {
            Ok(kind) if kind.is_dir() => tokio::fs::remove_dir_all(&path).await,
            _ => tokio::fs::remove_file(&path).await,
        };
        if let Err(error) = removed {
            debug!(%error, path = %path.display(), "web upload: removing stale staging entry failed");
        }
    }
}

async fn remove_dirs(dirs: Vec<PathBuf>) {
    for dir in dirs {
        if let Err(error) = tokio::fs::remove_dir_all(&dir).await
            && error.kind() != io::ErrorKind::NotFound
        {
            warn!(%error, dir = %dir.display(), "web upload: removing staging directory failed");
        }
    }
}

/// Expires idle sessions; on the first request of this process also clears
/// staging left by an earlier run.
async fn housekeeping(root: &Path) {
    let expired = registry().expire(Instant::now());
    remove_dirs(expired).await;
    if !SWEPT.swap(true, Ordering::SeqCst) {
        let _sweep = SWEEP_LOCK.lock().await;
        let live = registry().live_ids();
        sweep(root, &live).await;
    }
}

async fn write_reject(stream: &mut TcpStream, reject: Reject) -> io::Result<()> {
    let (status, reason) = reject.status();
    write_error(stream, status, reason, &reject.message()).await
}

/// `/api/web/upload/...`: same guards as the other dashboard uploads, plus
/// local sessions only (`web.publish` is not available remotely).
pub(super) async fn handle(
    stream: &mut TcpStream,
    method: &str,
    path: &str,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: Arc<AppState>,
    caller: Caller,
) -> io::Result<()> {
    if is_guard_denied(head, &stream.local_addr()?, true) {
        return write_error(stream, 403, "Forbidden", "forbidden").await;
    }
    if caller != (Caller::Http { remote: false }) {
        return write_error(
            stream,
            403,
            "Forbidden",
            "publishing a site is not available from a remote dashboard session; \
             use the mistl CLI or open the dashboard on this machine",
        )
        .await;
    }
    let root = match staging_root() {
        Ok(root) => root,
        Err(error) => {
            warn!(%error, "web upload: staging directory unavailable");
            return write_error(
                stream,
                500,
                "Internal Server Error",
                "staging directory unavailable",
            )
            .await;
        }
    };
    housekeeping(&root).await;

    let rest = path.strip_prefix(ROUTE_PREFIX).unwrap_or("");
    if rest == "begin" {
        if method != "POST" {
            return write_error(stream, 404, "Not Found", "not found").await;
        }
        return begin(stream, head, leftover, &state, &root).await;
    }
    let (id, tail) = match rest.split_once('/') {
        Some((id, tail)) => (id, Some(tail)),
        None => (rest, None),
    };
    if !is_upload_id(id) {
        return write_reject(stream, Reject::Unknown).await;
    }
    match (method, tail) {
        ("PUT", Some("file")) => put_file(stream, head, leftover, &state, id).await,
        ("POST", Some("finish")) => finish(stream, head, leftover, &state, caller, id).await,
        ("DELETE", None) => abort(stream, id).await,
        _ => write_error(stream, 404, "Not Found", "not found").await,
    }
}

/// Reads a small JSON object body; a missing or empty body is `{}`.
async fn read_json(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
) -> Result<Value, (u16, &'static str, &'static str)> {
    let len: usize = match head.header("content-length") {
        None => 0,
        Some(v) => v
            .trim()
            .parse()
            .map_err(|_| (400, "Bad Request", "invalid Content-Length"))?,
    };
    if len > MAX_JSON_BYTES {
        return Err((413, "Payload Too Large", "request body too large"));
    }
    if len == 0 {
        return Ok(json!({}));
    }
    let body = read_body(stream, leftover, len)
        .await
        .map_err(|_| (400, "Bad Request", "incomplete request body"))?;
    match serde_json::from_slice::<Value>(&body) {
        Ok(value) if value.is_object() => Ok(value),
        _ => Err((400, "Bad Request", "body must be a JSON object")),
    }
}

async fn begin(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: &Arc<AppState>,
    root: &Path,
) -> io::Result<()> {
    let body = match read_json(stream, head, leftover).await {
        Ok(body) => body,
        Err((status, reason, message)) => {
            return write_error(stream, status, reason, message).await;
        }
    };
    let opts = match parse_options(&body) {
        Ok(opts) => opts,
        Err(message) => return write_error(stream, 400, "Bad Request", &message).await,
    };
    if !state.config().web.enabled {
        return write_error(
            stream,
            409,
            "Conflict",
            "mistl web is disabled; enable it with `mistl config set web.enabled true`",
        )
        .await;
    }
    let _sweep = SWEEP_LOCK.lock().await;
    let id = new_upload_id();
    let dir = root.join(&id);
    let begun = {
        let mut reg = registry();
        reg.begin(id.clone(), dir.clone(), opts, Instant::now())
            .map(|()| reg.live_ids())
    };
    let live = match begun {
        Ok(live) => live,
        Err(reject) => return write_reject(stream, reject).await,
    };
    sweep(root, &live).await;
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        warn!(%error, dir = %dir.display(), "web upload: creating staging directory failed");
        registry().remove(&id);
        return write_error(
            stream,
            500,
            "Internal Server Error",
            "failed to create the staging directory",
        )
        .await;
    }
    write_json_response(
        stream,
        200,
        "OK",
        &json!({"ok": true, "data": {"upload_id": id}}),
    )
    .await
}

struct Failure {
    status: u16,
    reason: &'static str,
    message: String,
}

impl From<Reject> for Failure {
    fn from(reject: Reject) -> Self {
        let (status, reason) = reject.status();
        Failure {
            status,
            reason,
            message: reject.message(),
        }
    }
}

async fn put_file(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: &Arc<AppState>,
    id: &str,
) -> io::Result<()> {
    let query = head.path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let raw = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("path="))
        .and_then(percent_decode);
    let Some(raw) = raw else {
        return write_error(
            stream,
            400,
            "Bad Request",
            "missing or malformed `path` query parameter",
        )
        .await;
    };
    let rel = match staged_path(&raw) {
        Ok(rel) => rel,
        Err(message) => return write_error(stream, 400, "Bad Request", &message).await,
    };
    let size: u64 = match head.header("content-length") {
        Some(v) => match v.trim().parse() {
            Ok(n) => n,
            Err(_) => {
                return write_error(stream, 400, "Bad Request", "invalid Content-Length").await;
            }
        },
        None => {
            return write_error(stream, 411, "Length Required", "Content-Length required").await;
        }
    };
    let Ok(_slot) = PUT_SLOTS.try_acquire() else {
        return write_error(
            stream,
            503,
            "Service Unavailable",
            "too many uploads in progress",
        )
        .await;
    };
    let max_total = state.config().web.max_site_bytes;
    let reserved = registry().reserve(id, &rel, size, max_total, Instant::now());
    let dir = match reserved {
        Ok(dir) => dir,
        Err(reject) => return write_reject(stream, reject).await,
    };
    // Dot-named, so `web.publish` would skip a leftover temp file anyway.
    let tmp = dir.join(format!(".part-{:016x}", rand::random::<u64>()));
    let dest = rel.split('/').fold(dir, |mut path, seg| {
        path.push(seg);
        path
    });
    let outcome = write_file(stream, leftover, size, &tmp, &dest, id, &rel).await;
    if outcome.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    let gone = registry().end_write(id, &rel, size, outcome.is_ok(), Instant::now());
    if let Some(dir) = gone {
        remove_dirs(vec![dir]).await;
    }
    match outcome {
        Ok(()) => {
            write_json_response(
                stream,
                200,
                "OK",
                &json!({"ok": true, "data": {"path": rel, "size": size}}),
            )
            .await
        }
        Err(failure) => write_error(stream, failure.status, failure.reason, &failure.message).await,
    }
}

/// Streams the body to `tmp` (bounded buffer), then renames it to `dest`.
async fn write_file(
    stream: &mut TcpStream,
    leftover: Vec<u8>,
    size: u64,
    tmp: &Path,
    dest: &Path,
    id: &str,
    rel: &str,
) -> Result<(), Failure> {
    // `dest` is the session directory joined with validated segments, so
    // every created folder is inside it.
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| Failure {
                status: 409,
                reason: "Conflict",
                message: format!("cannot create the folder for {rel:?}: {error}"),
            })?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp)
        .await
        .map_err(|error| {
            warn!(%error, path = %tmp.display(), "web upload: creating temp file failed");
            Failure {
                status: 500,
                reason: "Internal Server Error",
                message: "failed to create the upload file".to_string(),
            }
        })?;
    if let Err(error) = read_body_to_file(stream, leftover, size, &mut file).await {
        debug!(%error, "web upload: reading file body failed");
        return Err(Failure {
            status: 400,
            reason: "Bad Request",
            message: "incomplete request body".to_string(),
        });
    }
    drop(file);
    commit(id, tmp, dest, rel)
}

/// Moves a complete file into place unless the session closed meanwhile or
/// the name is taken (case-insensitive filesystems fold `A.js` / `a.js`).
fn commit(id: &str, tmp: &Path, dest: &Path, rel: &str) -> Result<(), Failure> {
    let reg = registry();
    if !reg.is_open(id) {
        return Err(Reject::Closed.into());
    }
    if std::fs::symlink_metadata(dest).is_ok() {
        return Err(Reject::Duplicate(rel.to_string()).into());
    }
    std::fs::rename(tmp, dest).map_err(|error| Failure {
        status: 500,
        reason: "Internal Server Error",
        message: format!("saving {rel:?} failed: {error}"),
    })
}

async fn finish(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: &Arc<AppState>,
    caller: Caller,
    id: &str,
) -> io::Result<()> {
    let body = match read_json(stream, head, leftover).await {
        Ok(body) => body,
        Err((status, reason, message)) => {
            return write_error(stream, status, reason, message).await;
        }
    };
    let dry_run = match opt_bool(&body, "dry_run") {
        Ok(dry_run) => dry_run.unwrap_or(false),
        Err(message) => return write_error(stream, 400, "Bad Request", &message).await,
    };
    let started = registry().start_finish(id);
    let (dir, opts) = match started {
        Ok(started) => started,
        Err(reject) => return write_reject(stream, reject).await,
    };
    let args = json!({
        "path": dir.to_string_lossy(),
        "name": opts.name,
        "entry": opts.entry,
        "spa": opts.spa,
        "coi": opts.coi,
        "dry_run": dry_run,
    });
    let result = crate::daemon::dispatch_as(caller, "web.publish", args, state).await;
    // Published files are in the content store now; staging is never reused.
    registry().remove(id);
    remove_dirs(vec![dir]).await;
    let response = match result {
        Ok(data) => json!({"ok": true, "data": data}),
        Err(error) => json!({"ok": false, "error": format!("{error:#}")}),
    };
    write_json_response(stream, 200, "OK", &response).await
}

async fn abort(stream: &mut TcpStream, id: &str) -> io::Result<()> {
    let aborted = registry().abort(id);
    match aborted {
        Ok(dir) => {
            if let Some(dir) = dir {
                remove_dirs(vec![dir]).await;
            }
            write_json_response(
                stream,
                200,
                "OK",
                &json!({"ok": true, "data": {"aborted": true}}),
            )
            .await
        }
        Err(reject) => write_reject(stream, reject).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> Options {
        Options {
            name: "game".into(),
            entry: None,
            spa: false,
            coi: None,
        }
    }

    fn registry_with(ids: &[&str], now: Instant) -> Registry {
        let mut reg = Registry {
            sessions: Vec::new(),
        };
        for id in ids {
            reg.begin(id.to_string(), PathBuf::from(*id), opts(), now)
                .unwrap();
        }
        reg
    }

    #[test]
    fn staged_paths_match_what_publish_keeps() {
        for ok in [
            "index.html",
            "Build/game.wasm.br",
            ".well-known/x.json",
            "a/.well-known/x",
            "日本/ファイル.png",
        ] {
            assert_eq!(staged_path(ok).as_deref(), Ok(ok), "{ok}");
        }
        for bad in [
            "",
            "/abs",
            "a//b",
            "../x",
            "a/./b",
            "a\\b",
            "a%20b",
            "a?b",
            ".env",
            ".git/config",
            "a/.hidden",
            ".well-known/.x",
            ".mistl/allow",
            ".part-0000000000000000",
        ] {
            assert!(staged_path(bad).is_err(), "{bad:?}");
        }
        assert_eq!(staged_path("CON.txt").is_err(), cfg!(windows));
        assert_eq!(staged_path("a:b").is_err(), cfg!(windows));
    }

    #[test]
    fn windows_portability() {
        for ok in ["index.html", "COM0", "COMPUTER.txt", "con-game", "a b"] {
            assert!(portable_on_windows(ok), "{ok}");
        }
        for bad in [
            "CON", "con.txt", "Nul", "COM1", "lpt9.log", "AUX .x", "a.", "a ", "a:b", "a*b", "a|b",
            "a\"b", "a<b",
        ] {
            assert!(!portable_on_windows(bad), "{bad:?}");
        }
    }

    #[test]
    fn upload_ids() {
        let id = new_upload_id();
        assert!(is_upload_id(&id), "{id}");
        assert_ne!(id, new_upload_id());
        assert!(!is_upload_id(&id[1..]));
        assert!(!is_upload_id(
            &id.to_uppercase().replace(char::is_numeric, "A")
        ));
        assert!(!is_upload_id("../../../../../../../../../../etc"));
    }

    #[test]
    fn begin_options() {
        assert_eq!(
            parse_options(&json!({"name": " game ", "entry": "", "coi": null})).unwrap(),
            opts()
        );
        assert_eq!(
            parse_options(&json!({"name": "g", "entry": "play.html", "spa": true, "coi": false}))
                .unwrap(),
            Options {
                name: "g".into(),
                entry: Some("play.html".into()),
                spa: true,
                coi: Some(false),
            }
        );
        for bad in [
            json!({}),
            json!({"name": "Bad Name"}),
            json!({"name": 1}),
            json!({"name": "g", "entry": "../x"}),
            json!({"name": "g", "spa": "yes"}),
            json!({"name": "g", "coi": "auto"}),
        ] {
            assert!(parse_options(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn session_cap_and_expiry() {
        let t0 = Instant::now();
        let mut reg = registry_with(&["a", "b"], t0);
        assert_eq!(
            reg.begin("c".into(), "c".into(), opts(), t0),
            Err(Reject::TooManySessions)
        );
        // A write in flight or a running publish keeps a session alive.
        reg.reserve("a", "index.html", 1, u64::MAX, t0).unwrap();
        reg.start_finish("b").unwrap();
        let later = t0 + IDLE_EXPIRY;
        assert!(reg.expire(later).is_empty());
        assert_eq!(reg.end_write("a", "index.html", 1, true, t0), None);
        assert!(reg.expire(later - Duration::from_secs(1)).is_empty());
        assert_eq!(reg.expire(later), vec![PathBuf::from("a")]);
        assert_eq!(reg.live_ids(), HashSet::from(["b".to_string()]));
        assert_eq!(reg.remove("b"), Some(PathBuf::from("b")));
        reg.begin("c".into(), "c".into(), opts(), t0).unwrap();
    }

    #[test]
    fn reserve_enforces_limits_and_duplicates() {
        let t0 = Instant::now();
        let mut reg = registry_with(&["a"], t0);
        assert_eq!(reg.reserve("x", "f", 1, 100, t0), Err(Reject::Unknown));
        assert_eq!(
            reg.reserve("a", "big", site::MAX_FILE_BYTES + 1, u64::MAX, t0),
            Err(Reject::FileTooLarge)
        );
        assert_eq!(reg.reserve("a", "f1", 60, 100, t0), Ok(PathBuf::from("a")));
        assert_eq!(
            reg.reserve("a", "f1", 1, 100, t0),
            Err(Reject::Duplicate("f1".into()))
        );
        assert_eq!(
            reg.reserve("a", "f2", 41, 100, t0),
            Err(Reject::SiteTooLarge(100))
        );
        // A failed write gives back its name and bytes.
        reg.end_write("a", "f1", 60, false, t0);
        reg.reserve("a", "f2", 100, 100, t0).unwrap();
        reg.end_write("a", "f2", 100, true, t0);
        reg.reserve("a", "f1", 0, 100, t0).unwrap();
        reg.end_write("a", "f1", 0, true, t0);
        assert_eq!(
            reg.reserve("a", "f3", 1, 100, t0),
            Err(Reject::SiteTooLarge(100))
        );
        // A lowered `[web] max_site_bytes` applies to the next file.
        assert_eq!(
            reg.reserve("a", "f3", 1, 0, t0),
            Err(Reject::SiteTooLarge(0))
        );

        let s = &mut reg.sessions[0];
        s.files = (0..site::MAX_FILES).map(|i| i.to_string()).collect();
        s.bytes = 0;
        assert_eq!(
            reg.reserve("a", "one-more", 0, 100, t0),
            Err(Reject::TooManyFiles)
        );
    }

    #[test]
    fn finish_and_abort_lifecycle() {
        let t0 = Instant::now();
        let mut reg = registry_with(&["a", "b"], t0);
        reg.reserve("a", "index.html", 1, u64::MAX, t0).unwrap();
        assert_eq!(reg.start_finish("a").map(|_| ()), Err(Reject::Busy));
        reg.end_write("a", "index.html", 1, true, t0);
        let (dir, o) = reg.start_finish("a").unwrap();
        assert_eq!((dir, o), (PathBuf::from("a"), opts()));
        assert!(!reg.is_open("a"));
        assert_eq!(
            reg.reserve("a", "late.js", 1, u64::MAX, t0),
            Err(Reject::Closed)
        );
        assert_eq!(reg.start_finish("a").map(|_| ()), Err(Reject::Closed));
        assert_eq!(reg.abort("a"), Err(Reject::Closed));

        // Abort with a write in flight: the last write removes the session.
        reg.reserve("b", "index.html", 1, u64::MAX, t0).unwrap();
        assert_eq!(reg.abort("b"), Ok(None));
        assert!(!reg.is_open("b"));
        assert_eq!(
            reg.end_write("b", "index.html", 1, false, t0),
            Some(PathBuf::from("b"))
        );
        assert_eq!(reg.abort("b"), Err(Reject::Unknown));

        let mut reg = registry_with(&["c"], t0);
        assert_eq!(reg.abort("c"), Ok(Some(PathBuf::from("c"))));
        assert!(reg.sessions.is_empty());
    }

    #[test]
    fn rejects_map_to_http_statuses() {
        assert_eq!(Reject::Unknown.status().0, 404);
        assert_eq!(Reject::TooManySessions.status().0, 503);
        assert_eq!(Reject::Duplicate("a".into()).status().0, 409);
        assert_eq!(Reject::SiteTooLarge(1).status().0, 413);
        assert!(
            Reject::Duplicate("a/b.js".into())
                .message()
                .contains("a/b.js")
        );
    }

    #[tokio::test]
    async fn sweep_keeps_only_live_sessions() {
        let root =
            std::env::temp_dir().join(format!("mistl-web-staging-{:016x}", rand::random::<u64>()));
        std::fs::create_dir_all(root.join("live/sub")).unwrap();
        std::fs::create_dir_all(root.join("stale/sub")).unwrap();
        std::fs::write(root.join("stale/sub/f"), b"x").unwrap();
        std::fs::write(root.join("stray"), b"x").unwrap();
        sweep(&root, &HashSet::from(["live".to_string()])).await;
        let mut left: Vec<String> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(left, ["live"]);
        sweep(&root.join("missing"), &HashSet::new()).await;
        std::fs::remove_dir_all(&root).unwrap();
    }
}
