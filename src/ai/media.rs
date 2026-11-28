//! Daemon-side media generation jobs (`ai.media.*`): image, video and music
//! requests relayed to providers, with results stored as local files.
//! Contract: `.agents/mediagen/design.md` section "Daemon media jobs".

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::{sync::OnceCell, task::JoinHandle};
use tracing::warn;

use super::api_server::{JobContent, RelayFn, RelayRequest};
use super::oai_tunnel::TunnelResponse;
use crate::daemon::AppState;

const MAX_RUNNING: usize = 4;
const MAX_KEPT: usize = 100;
const MAX_PARAMS_BYTES: usize = 32 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const JOB_TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60);
const INDEX: &str = "index.json";
static STORE: OnceCell<Arc<Store>> = OnceCell::const_new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Image,
    Video,
    Music,
}

impl Kind {
    fn path(self) -> &'static str {
        match self {
            Self::Image => "/images/generations",
            Self::Video => "/videos",
            Self::Music => "/music",
        }
    }
}

#[derive(Debug, Deserialize)]
struct Generate {
    kind: Kind,
    model: String,
    room: Option<String>,
    #[serde(default)]
    params: Map<String, Value>,
}

impl Generate {
    fn body(&mut self) -> Result<Vec<u8>> {
        ensure!(!self.model.trim().is_empty(), "model is required");
        ensure!(self.model.len() <= 512, "model is too long");
        ensure!(
            self.room.as_ref().is_none_or(|room| room.len() <= 512),
            "room is too long"
        );
        let mut body = serde_json::to_vec(&self.params)?;
        ensure!(body.len() <= MAX_PARAMS_BYTES, "params exceed 32 MiB");
        let mut params = std::mem::take(&mut self.params);
        params.insert("model".into(), json!(self.model));
        if self.kind == Kind::Image {
            params.insert("response_format".into(), json!("b64_json"));
        }
        body.clear();
        serde_json::to_writer(&mut body, &params)?;
        Ok(body)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MediaFile {
    index: usize,
    name: String,
    bytes: u64,
    mime: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    id: String,
    kind: Kind,
    model: String,
    room: Option<String>,
    status: Status,
    progress: Option<f64>,
    error: Option<String>,
    created_at: i64,
    finished_at: Option<i64>,
    metered: bool,
    files: Vec<MediaFile>,
}

impl Job {
    fn new(request: Generate, metered: bool) -> Self {
        Self {
            id: format!("media_{}", uuid::Uuid::new_v4().simple()),
            kind: request.kind,
            model: request.model,
            room: request.room,
            status: Status::Running,
            progress: None,
            error: None,
            created_at: now(),
            finished_at: None,
            metered,
            files: Vec::new(),
        }
    }

    fn fail(&mut self, message: &str) {
        self.status = Status::Failed;
        self.error = Some(sanitize(message));
        self.finished_at = Some(now());
        self.files.clear();
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn sanitize(message: &str) -> String {
    message
        .chars()
        .filter(|c| !c.is_control())
        .take(300)
        .collect()
}

fn valid_local_id(id: &str) -> bool {
    id.strip_prefix("media_").is_some_and(|s| {
        s.len() == 32
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

fn valid_upstream_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[derive(Default, Serialize, Deserialize)]
struct Index {
    // Newest first, including jobs created in the same second.
    jobs: Vec<Job>,
}

struct Active {
    task: JoinHandle<()>,
    relay: RelayFn,
    upstream_path: Arc<Mutex<Option<String>>>,
}

#[derive(Default)]
struct Inner {
    index: Index,
    active: HashMap<String, Active>,
}

#[derive(Clone, Copy)]
struct Limits {
    poll: Duration,
    timeout: Duration,
    file_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            poll: POLL_INTERVAL,
            timeout: JOB_TIMEOUT,
            file_bytes: MAX_FILE_BYTES,
        }
    }
}

struct Store {
    dir: PathBuf,
    inner: Mutex<Inner>,
    limits: Limits,
}

impl Store {
    fn load(dir: PathBuf, limits: Limits) -> Result<Arc<Self>> {
        crate::statefile::create_private_dir(&dir)?;
        let mut index: Index = crate::statefile::read(&dir, INDEX)?;
        let mut ids = HashSet::new();
        ensure!(
            index
                .jobs
                .iter()
                .all(|j| valid_local_id(&j.id) && ids.insert(j.id.clone())),
            "invalid media index"
        );
        index.jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        index.jobs.truncate(MAX_KEPT);
        for job in &mut index.jobs {
            if job.status == Status::Running {
                job.fail("interrupted by restart");
            }
        }
        Self::save(&dir, &index)?;
        let retained: HashSet<_> = index
            .jobs
            .iter()
            .filter(|j| j.status == Status::Completed)
            .map(|j| j.id.as_str())
            .collect();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && !retained.contains(entry.file_name().to_string_lossy().as_ref())
            {
                remove_dir(&entry.path())?;
            }
        }
        Ok(Arc::new(Self {
            dir,
            inner: Mutex::new(Inner {
                index,
                ..Default::default()
            }),
            limits,
        }))
    }

    fn save(dir: &Path, index: &Index) -> Result<()> {
        crate::statefile::write_private(&dir.join(INDEX), &serde_json::to_vec(index)?)
    }

    fn list(&self) -> Value {
        json!(self.inner.lock().expect("media lock").index)
    }

    fn start(self: &Arc<Self>, job: Job, body: Vec<u8>, relay: RelayFn) -> Result<String> {
        let mut inner = self.inner.lock().expect("media lock");
        ensure!(
            inner
                .index
                .jobs
                .iter()
                .filter(|j| j.status == Status::Running)
                .count()
                < MAX_RUNNING,
            "too many running generations"
        );
        let evicted = if inner.index.jobs.len() >= MAX_KEPT {
            let pos = inner
                .index
                .jobs
                .iter()
                .rposition(|j| j.status != Status::Running)
                .context("too many running generations")?;
            Some((pos, inner.index.jobs.remove(pos)))
        } else {
            None
        };
        let id = job.id.clone();
        let kind = job.kind;
        inner.index.jobs.insert(0, job);
        if let Err(error) = Self::save(&self.dir, &inner.index) {
            inner.index.jobs.remove(0);
            if let Some((pos, old)) = evicted {
                inner.index.jobs.insert(pos, old);
            }
            return Err(error);
        }
        if let Some((_, old)) = evicted {
            self.cleanup(&old.id);
        }
        // Install the active entry before the new task can publish its result.
        let store = self.clone();
        let task_id = id.clone();
        let task_relay = relay.clone();
        let upstream_path = Arc::new(Mutex::new(None));
        let task_path = upstream_path.clone();
        let task = tokio::spawn(async move {
            let result = store.run(&task_id, kind, body, task_relay, task_path).await;
            store.finish(&task_id, result);
        });
        inner.active.insert(
            id.clone(),
            Active {
                task,
                relay,
                upstream_path,
            },
        );
        Ok(id)
    }

    fn progress(&self, id: &str, progress: Option<f64>) {
        let Some(progress) = progress else { return };
        let mut inner = self.inner.lock().expect("media lock");
        if let Some(job) = inner.index.jobs.iter_mut().find(|j| j.id == id)
            && job.status == Status::Running
            && job.progress != Some(progress)
        {
            job.progress = Some(progress);
            self.save_or_warn(&inner.index);
        }
    }

    fn finish(&self, id: &str, result: Result<Vec<MediaFile>>) {
        let mut inner = self.inner.lock().expect("media lock");
        if let Some(job) = inner.index.jobs.iter_mut().find(|j| j.id == id) {
            match result {
                Ok(files) => {
                    job.status = Status::Completed;
                    job.finished_at = Some(now());
                    job.progress = Some(100.0);
                    job.files = files;
                }
                Err(error) => {
                    self.cleanup(id);
                    job.fail(&error.to_string());
                }
            }
            self.save_or_warn(&inner.index);
        }
        inner.active.remove(id);
    }

    fn save_or_warn(&self, index: &Index) {
        if let Err(error) = Self::save(&self.dir, index) {
            warn!(%error, "saving media jobs failed");
        }
    }

    fn cleanup(&self, id: &str) {
        if let Err(error) = remove_dir(&self.dir.join(id)) {
            warn!(%error, "removing media files failed");
        }
    }

    async fn remove(&self, id: &str) -> Result<bool> {
        let active = {
            let mut inner = self.inner.lock().expect("media lock");
            let Some(pos) = inner.index.jobs.iter().position(|j| j.id == id) else {
                return Ok(false);
            };
            let job = inner.index.jobs.remove(pos);
            if let Err(error) = Self::save(&self.dir, &inner.index) {
                inner.index.jobs.insert(pos, job);
                return Err(error);
            }
            inner.active.remove(id)
        };
        if let Some(active) = active {
            // Wait for the task to drop its file handle before unlinking.
            active.task.abort();
            let _ = active.task.await;
            let path = active
                .upstream_path
                .lock()
                .expect("media upstream lock")
                .clone();
            let removed = remove_dir(&self.dir.join(id));
            if let Some(path) = path {
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    (active.relay)(RelayRequest {
                        method: "DELETE".into(),
                        path,
                        body: None,
                    }),
                )
                .await;
            }
            removed?;
        } else {
            remove_dir(&self.dir.join(id))?;
        }
        Ok(true)
    }

    async fn run(
        &self,
        id: &str,
        kind: Kind,
        body: Vec<u8>,
        relay: RelayFn,
        upstream_path: Arc<Mutex<Option<String>>>,
    ) -> Result<Vec<MediaFile>> {
        let dir = self.dir.join(id);
        if kind == Kind::Image {
            let response = request_json(&relay, "POST", kind.path(), Some(body)).await?;
            return save_images(&dir, response, self.limits.file_bytes);
        }
        tokio::time::timeout(self.limits.timeout, async {
            let mut job = request_json(&relay, "POST", kind.path(), Some(body)).await?;
            let upstream_id = job["id"]
                .as_str()
                .filter(|id| valid_upstream_id(id))
                .context("invalid upstream job id")?;
            let path = format!("{}/{upstream_id}", kind.path());
            *upstream_path.lock().expect("media upstream lock") = Some(path.clone());
            loop {
                self.progress(id, job["progress"].as_f64());
                match job["status"].as_str() {
                    Some("completed") => break,
                    Some("failed") => bail!("{}", upstream_error(&job, "generation failed")),
                    Some("queued" | "in_progress") => {}
                    _ => bail!("invalid upstream job status"),
                }
                tokio::time::sleep(self.limits.poll).await;
                job = request_json(&relay, "GET", &path, None).await?;
            }
            let file = save_content(
                &dir,
                relay,
                format!("{path}/content"),
                self.limits.file_bytes,
            )
            .await?;
            Ok(vec![file])
        })
        .await
        .context("timed out")?
    }
}

fn remove_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("removing media directory"),
    }
}

fn upstream_error(job: &Value, fallback: &str) -> String {
    job["error"]["message"]
        .as_str()
        .map(sanitize)
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn response_error(response: &TunnelResponse) -> anyhow::Error {
    let fallback = format!("provider returned HTTP {}", response.status);
    let message = serde_json::from_slice::<Value>(&response.body)
        .ok()
        .map(|body| upstream_error(&body, &fallback))
        .unwrap_or(fallback);
    anyhow::anyhow!("{message}")
}

async fn request_json(
    relay: &RelayFn,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<Value> {
    let response = relay(RelayRequest {
        method: method.into(),
        path: path.into(),
        body,
    })
    .await
    .context("provider request failed")?;
    if !(200..300).contains(&response.status) {
        return Err(response_error(&response));
    }
    serde_json::from_slice(&response.body).context("invalid provider response")
}

fn sniff_image(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else {
        "application/octet-stream"
    }
}

// Only passive media types are stored for inline dashboard playback. An
// arbitrary upstream Content-Type (e.g. HTML) must not become dashboard code.
fn file_type(mime: &str) -> (&'static str, &'static str) {
    match mime
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "image/png" => ("image/png", "png"),
        "image/jpeg" => ("image/jpeg", "jpg"),
        "image/webp" => ("image/webp", "webp"),
        "image/gif" => ("image/gif", "gif"),
        "video/mp4" => ("video/mp4", "mp4"),
        "video/webm" => ("video/webm", "webm"),
        "video/quicktime" => ("video/quicktime", "mov"),
        "audio/mpeg" | "audio/mp3" => ("audio/mpeg", "mp3"),
        "audio/wav" | "audio/x-wav" | "audio/wave" => ("audio/wav", "wav"),
        "audio/ogg" | "application/ogg" => ("audio/ogg", "ogg"),
        "video/ogg" => ("video/ogg", "ogg"),
        "audio/flac" | "audio/x-flac" => ("audio/flac", "flac"),
        "audio/mp4" | "audio/m4a" | "audio/x-m4a" => ("audio/mp4", "m4a"),
        "audio/aac" | "audio/aacp" => ("audio/aac", "aac"),
        "audio/webm" => ("audio/webm", "webm"),
        _ => ("application/octet-stream", "bin"),
    }
}

fn check_size(total: u64, next: usize, cap: u64) -> Result<u64> {
    total
        .checked_add(next as u64)
        .filter(|n| *n <= cap)
        .context("media result exceeds the per-job file size limit")
}

fn create_file(dir: &Path, index: usize, mime: &str) -> Result<(std::fs::File, MediaFile)> {
    let (mime, ext) = file_type(mime);
    let name = format!("{index}.{ext}");
    crate::statefile::create_private_dir(dir).context("creating media directory")?;
    let file = std::fs::File::create(dir.join(&name)).context("creating media file")?;
    Ok((
        file,
        MediaFile {
            index,
            name,
            bytes: 0,
            mime: mime.into(),
        },
    ))
}

fn save_images(dir: &Path, response: Value, cap: u64) -> Result<Vec<MediaFile>> {
    let data = response["data"].as_array().context("no image data")?;
    let mut files = Vec::new();
    let mut total = 0;
    for item in data {
        let Some(encoded) = item["b64_json"].as_str().filter(|s| !s.is_empty()) else {
            continue;
        };
        let bytes = STANDARD.decode(encoded).context("invalid base64 image")?;
        total = check_size(total, bytes.len(), cap)?;
        let (mut file, mut info) = create_file(dir, files.len(), sniff_image(&bytes))?;
        file.write_all(&bytes).context("writing media file")?;
        info.bytes = bytes.len() as u64;
        files.push(info);
    }
    ensure!(!files.is_empty(), "no image data");
    Ok(files)
}

async fn save_content(dir: &Path, relay: RelayFn, path: String, cap: u64) -> Result<MediaFile> {
    let mut content = JobContent::open(relay, path)
        .await?
        .map_err(|response| response_error(&response))?;
    let (mut file, mut info) = create_file(dir, 0, content.content_type())?;
    // Synchronous writes of individual bounded windows: task cancellation
    // drops the file, never leaving a blocking file operation racing removal.
    while let Some(bytes) = content.next().await? {
        info.bytes = check_size(info.bytes, bytes.len(), cap)?;
        file.write_all(&bytes).context("writing media file")?;
    }
    Ok(info)
}

fn metered(service: &super::AiService, room: Option<&str>, model: &str) -> bool {
    let config = service.state.effective_config();
    let room_metered = |room: &str| {
        service
            .rooms
            .read()
            .expect("ai rooms lock")
            .get(room)
            .and_then(|session| session.consumer.model_info().remove(model))
            .is_some_and(|info| info.metered)
    };
    if let Some(room) = room {
        return room_metered(room);
    }
    // Mirror unscoped model routing so an unrelated provider advertising a
    // model with the same name does not change this job's metering flag.
    let Ok(reference) = super::resolve_api_ref(&config.ai, Some(model), &service.live_models())
    else {
        return false;
    };
    config
        .ai
        .providers
        .iter()
        .find(|p| p.id == reference.provider_id)
        .is_some_and(|p| {
            if let Some(room) = p.room() {
                room_metered(room)
            } else {
                p.model_info
                    .get(&reference.model)
                    .is_some_and(|info| info.metered)
            }
        })
}

pub(super) async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    let store = STORE
        .get_or_try_init(|| async {
            Store::load(
                crate::config::data_dir()?.join("ai-media"),
                Limits::default(),
            )
        })
        .await?;
    match cmd {
        "ai.media.generate" => {
            let mut request: Generate = serde_json::from_value(args)?;
            let body = request.body()?;
            let service = super::ensure_started(state).await?;
            let relay: RelayFn = if let Some(room) = request.room.as_ref() {
                service
                    .clone()
                    .api_room(room.clone())
                    .await?
                    .context("ai: room is not enabled")?
                    .relay
            } else {
                let service = service.clone();
                Arc::new(move |request| Box::pin(service.clone().relay_request(request)))
            };
            let metered = metered(&service, request.room.as_deref(), &request.model);
            let id = store.start(Job::new(request, metered), body, relay)?;
            Ok(json!({"id": id}))
        }
        "ai.media.list" => Ok(store.list()),
        "ai.media.remove" => {
            let id = args["id"].as_str().context("ai.media.remove requires id")?;
            Ok(json!({"removed": store.remove(id).await?}))
        }
        _ => bail!("unknown command: {cmd}"),
    }
}

#[cfg(test)]
mod tests {
    use super::super::oai_tunnel::CONTENT_WINDOW;
    use super::*;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("mistl-media-test-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn store(&self, cap: u64) -> Arc<Store> {
            Store::load(
                self.0.clone(),
                Limits {
                    poll: Duration::from_millis(5),
                    timeout: Duration::from_secs(30),
                    file_bytes: cap,
                },
            )
            .unwrap()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn job(kind: Kind) -> Job {
        Job::new(
            Generate {
                kind,
                model: match kind {
                    Kind::Image => "image-model",
                    Kind::Video => "video-model",
                    Kind::Music => "music-model",
                }
                .into(),
                room: Some("test-room".into()),
                params: Map::new(),
            },
            true,
        )
    }

    fn json_response(status: u16, body: Value) -> TunnelResponse {
        TunnelResponse {
            status,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    fn fake(f: impl Fn(RelayRequest) -> TunnelResponse + Send + Sync + 'static) -> RelayFn {
        Arc::new(move |request| {
            let response = f(request);
            Box::pin(async move { Ok(response) })
        })
    }

    fn pending() -> RelayFn {
        Arc::new(|_| Box::pin(std::future::pending()))
    }

    fn snapshot(store: &Store, id: &str) -> Job {
        store
            .inner
            .lock()
            .unwrap()
            .index
            .jobs
            .iter()
            .find(|j| j.id == id)
            .unwrap()
            .clone()
    }

    async fn finish(store: &Store, id: &str) -> Job {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let job = snapshot(store, id);
                if job.status != Status::Running {
                    return job;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("job did not finish")
    }

    #[test]
    fn generate_body_forces_model_and_image_format() {
        let mut request: Generate = serde_json::from_value(json!({
            "kind": "image", "model": "image-model",
            "params": {"model":"wrong-model","response_format":"url","prompt":"test","n":2}
        }))
        .unwrap();
        let body: Value = serde_json::from_slice(&request.body().unwrap()).unwrap();
        assert_eq!(
            body,
            json!({"model":"image-model","response_format":"b64_json","prompt":"test","n":2})
        );
        let mut request: Generate =
            serde_json::from_value(json!({"kind":"music","model":"music-model"})).unwrap();
        request
            .params
            .insert("prompt".into(), Value::String("a".repeat(MAX_PARAMS_BYTES)));
        assert!(request.body().unwrap_err().to_string().contains("32 MiB"));
        assert!(
            serde_json::from_value::<Generate>(json!({"kind":"speech","model":"speech-model"}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<Generate>(
                json!({"kind":"image","model":"image-model","params":[]})
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn image_lifecycle_saves_every_image_and_sniffs_mime() {
        let scratch = Scratch::new();
        let store = scratch.store(1024);
        let images = vec![
            (b"\x89PNG\r\n\x1a\nabc".to_vec(), "image/png", "png"),
            (b"\xff\xd8\xffabc".to_vec(), "image/jpeg", "jpg"),
            (b"RIFF0000WEBPabc".to_vec(), "image/webp", "webp"),
            (b"GIF89aabc".to_vec(), "image/gif", "gif"),
            (b"other".to_vec(), "application/octet-stream", "bin"),
        ];
        let data: Vec<_> = images
            .iter()
            .map(|(bytes, _, _)| json!({"b64_json": STANDARD.encode(bytes)}))
            .collect();
        let relay = fake(move |r| {
            assert_eq!(r.method, "POST");
            assert_eq!(r.path, "/images/generations");
            json_response(200, json!({"data":data}))
        });
        let id = store
            .start(job(Kind::Image), b"{}".to_vec(), relay)
            .unwrap();
        assert!(valid_local_id(&id));
        let result = finish(&store, &id).await;
        assert_eq!(result.status, Status::Completed);
        assert_eq!(result.progress, Some(100.0));
        assert_eq!(result.files.len(), images.len());
        assert!(result.metered && result.finished_at.is_some());
        assert_eq!(result.room.as_deref(), Some("test-room"));
        for (i, (bytes, mime, ext)) in images.iter().enumerate() {
            let file = &result.files[i];
            assert_eq!(file.index, i);
            assert_eq!(file.name, format!("{i}.{ext}"));
            assert_eq!(file.mime, *mime);
            assert_eq!(file.bytes, bytes.len() as u64);
            assert_eq!(
                std::fs::read(scratch.0.join(&id).join(&file.name)).unwrap(),
                *bytes
            );
        }
        let saved: Index = crate::statefile::read(&scratch.0, INDEX).unwrap();
        assert_eq!(saved.jobs[0].files, result.files);
        assert_eq!(store.list()["jobs"][0]["status"], "completed");
        assert!(store.remove(&id).await.unwrap());
        assert!(!scratch.0.join(&id).exists());
        assert!(!store.remove(&id).await.unwrap());
    }

    #[tokio::test]
    async fn video_lifecycle_polls_then_downloads_multiple_windows() {
        let scratch = Scratch::new();
        let store = scratch.store(CONTENT_WINDOW + 10);
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = requests.clone();
        let relay = fake(move |r| {
            captured
                .lock()
                .unwrap()
                .push(format!("{} {}", r.method, r.path));
            match (r.method.as_str(), r.path.as_str()) {
                ("POST", "/videos") => {
                    json_response(202, json!({"id":"video_1","status":"queued","progress":0}))
                }
                ("GET", "/videos/video_1") => {
                    let count = captured
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|r| *r == "GET /videos/video_1")
                        .count();
                    if count == 1 {
                        json_response(
                            200,
                            json!({"id":"video_1","status":"in_progress","progress":50}),
                        )
                    } else {
                        json_response(
                            200,
                            json!({"id":"video_1","status":"completed","progress":100}),
                        )
                    }
                }
                ("GET", path)
                    if path
                        == format!("/videos/video_1/content?range=0-{}", CONTENT_WINDOW - 1) =>
                {
                    TunnelResponse {
                        status: 206,
                        content_type: "video/mp4".into(),
                        body: vec![1; CONTENT_WINDOW as usize],
                    }
                }
                ("GET", path)
                    if path
                        == format!(
                            "/videos/video_1/content?range={}-{}",
                            CONTENT_WINDOW,
                            2 * CONTENT_WINDOW - 1
                        ) =>
                {
                    TunnelResponse {
                        status: 206,
                        content_type: "video/mp4".into(),
                        body: vec![2; 3],
                    }
                }
                _ => panic!("unexpected relay: {} {}", r.method, r.path),
            }
        });
        let id = store.start(job(Kind::Video), vec![], relay).unwrap();
        let result = finish(&store, &id).await;
        assert_eq!(result.status, Status::Completed, "{:?}", result.error);
        let file = &result.files[0];
        assert_eq!(file.name, "0.mp4");
        assert_eq!(file.mime, "video/mp4");
        assert_eq!(file.bytes, CONTENT_WINDOW + 3);
        let bytes = std::fs::read(scratch.0.join(&id).join(&file.name)).unwrap();
        assert_eq!(bytes.len() as u64, CONTENT_WINDOW + 3);
        assert_eq!(&bytes[..3], &[1; 3]);
        assert_eq!(&bytes[CONTENT_WINDOW as usize..], &[2; 3]);
        assert_eq!(requests.lock().unwrap().len(), 5);
    }

    #[tokio::test]
    async fn failed_job_sanitizes_upstream_message_and_no_image_data_fails() {
        let scratch = Scratch::new();
        let store = scratch.store(1024);
        let message = format!("bad\r\n\x1b{}", "é".repeat(400));
        let relay = fake(move |r| {
            assert_eq!(r.path, "/music");
            json_response(
                201,
                json!({"id":"music_1","status":"failed","progress":17,"error":{"message":message}}),
            )
        });
        let id = store.start(job(Kind::Music), vec![], relay).unwrap();
        let result = finish(&store, &id).await;
        assert_eq!(result.status, Status::Failed);
        assert_eq!(result.progress, Some(17.0));
        let error = result.error.unwrap();
        assert!(error.starts_with("badé"));
        assert_eq!(error.chars().count(), 300);
        assert!(!error.chars().any(char::is_control));
        let id = store
            .start(
                job(Kind::Image),
                vec![],
                fake(|_| json_response(200, json!({"data":[{"url":"ignored"}]}))),
            )
            .unwrap();
        assert_eq!(
            finish(&store, &id).await.error.as_deref(),
            Some("no image data")
        );
    }

    #[tokio::test]
    async fn timeout_does_not_wait_three_hours() {
        let scratch = Scratch::new();
        let store = Store::load(
            scratch.0.clone(),
            Limits {
                timeout: Duration::from_millis(30),
                ..Limits::default()
            },
        )
        .unwrap();
        let relay = fake(|_| json_response(200, json!({"id":"video_1","status":"queued"})));
        let id = store.start(job(Kind::Video), vec![], relay).unwrap();
        let result = finish(&store, &id).await;
        assert_eq!(result.status, Status::Failed);
        assert_eq!(result.error.as_deref(), Some("timed out"));
    }

    #[tokio::test]
    async fn file_total_cap_removes_partial_image_results() {
        let scratch = Scratch::new();
        let store = scratch.store(10);
        let relay = fake(|_| {
            json_response(
                200,
                json!({"data":[
                    {"b64_json":STANDARD.encode(b"123456")},
                    {"b64_json":STANDARD.encode(b"123456")}
                ]}),
            )
        });
        let id = store.start(job(Kind::Image), vec![], relay).unwrap();
        let result = finish(&store, &id).await;
        assert_eq!(result.status, Status::Failed);
        assert!(result.error.unwrap().contains("size limit"));
        assert!(result.files.is_empty());
        assert!(!scratch.0.join(&id).exists());
        assert!(check_size(MAX_FILE_BYTES, 1, MAX_FILE_BYTES).is_err());
        assert_eq!(
            check_size(MAX_FILE_BYTES - 1, 1, MAX_FILE_BYTES).unwrap(),
            MAX_FILE_BYTES
        );
    }

    #[tokio::test]
    async fn content_cap_removes_partial_download() {
        let scratch = Scratch::new();
        let store = scratch.store(5);
        let relay = fake(|r| match r.method.as_str() {
            "POST" => json_response(202, json!({"id":"music_1","status":"completed"})),
            "GET" => TunnelResponse {
                status: 200,
                content_type: "audio/mpeg".into(),
                body: b"123456".to_vec(),
            },
            _ => panic!("unexpected relay"),
        });
        let id = store.start(job(Kind::Music), vec![], relay).unwrap();
        let result = finish(&store, &id).await;
        assert_eq!(result.status, Status::Failed);
        assert!(result.error.unwrap().contains("size limit"));
        assert!(result.files.is_empty());
        assert!(!scratch.0.join(&id).exists());
    }

    #[tokio::test]
    async fn four_running_jobs_limit_and_oldest_finished_eviction() {
        let scratch = Scratch::new();
        let mut jobs = Vec::new();
        for i in 0..MAX_KEPT {
            let mut j = job(Kind::Image);
            j.status = Status::Completed;
            j.created_at = i as i64;
            jobs.push(j);
        }
        let oldest = jobs[0].id.clone();
        std::fs::create_dir(scratch.0.join(&oldest)).unwrap();
        std::fs::write(scratch.0.join(&oldest).join("0.png"), b"old").unwrap();
        Store::save(&scratch.0, &Index { jobs }).unwrap();
        let store = scratch.store(1024);
        let mut active = Vec::new();
        for _ in 0..MAX_RUNNING {
            active.push(store.start(job(Kind::Image), vec![], pending()).unwrap());
        }
        let error = store
            .start(job(Kind::Image), vec![], pending())
            .unwrap_err();
        assert_eq!(error.to_string(), "too many running generations");
        let index = store.list();
        let jobs = index["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), MAX_KEPT);
        assert_eq!(jobs[0]["id"], active[MAX_RUNNING - 1]);
        assert!(!jobs.iter().any(|j| j["id"] == oldest));
        assert!(!scratch.0.join(&oldest).exists());
        for id in &active {
            assert!(store.remove(id).await.unwrap());
        }
    }

    #[tokio::test]
    async fn remove_cancels_download_and_deletes_upstream_job() {
        let scratch = Scratch::new();
        let store = scratch.store(MAX_FILE_BYTES);
        let downloading = Arc::new(tokio::sync::Notify::new());
        let deleted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let notified = downloading.clone();
        let marked = deleted.clone();
        let relay: RelayFn = Arc::new(move |r| {
            let notified = notified.clone();
            let marked = marked.clone();
            Box::pin(async move {
                match (r.method.as_str(), r.path.as_str()) {
                    ("POST", "/music") => Ok(json_response(
                        202,
                        json!({"id":"music_1","status":"completed"}),
                    )),
                    ("GET", path)
                        if path
                            == format!("/music/music_1/content?range=0-{}", CONTENT_WINDOW - 1) =>
                    {
                        Ok(TunnelResponse {
                            status: 206,
                            content_type: "audio/mpeg".into(),
                            body: vec![0; CONTENT_WINDOW as usize],
                        })
                    }
                    ("GET", path)
                        if path
                            == format!(
                                "/music/music_1/content?range={}-{}",
                                CONTENT_WINDOW,
                                2 * CONTENT_WINDOW - 1
                            ) =>
                    {
                        notified.notify_one();
                        std::future::pending().await
                    }
                    ("DELETE", "/music/music_1") => {
                        marked.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok(json_response(200, json!({})))
                    }
                    _ => panic!("unexpected relay: {} {}", r.method, r.path),
                }
            })
        });
        let id = store.start(job(Kind::Music), vec![], relay).unwrap();
        tokio::time::timeout(Duration::from_secs(5), downloading.notified())
            .await
            .unwrap();
        assert_eq!(
            std::fs::metadata(scratch.0.join(&id).join("0.mp3"))
                .unwrap()
                .len(),
            CONTENT_WINDOW
        );
        assert!(store.remove(&id).await.unwrap());
        assert!(deleted.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!scratch.0.join(&id).exists());
        assert_eq!(store.list(), json!({"jobs":[]}));
    }

    #[test]
    fn reload_marks_running_failed_and_cleans_orphan_directories() {
        let scratch = Scratch::new();
        let running = job(Kind::Video);
        let running_id = running.id.clone();
        let mut completed = job(Kind::Image);
        completed.status = Status::Completed;
        let completed_id = completed.id.clone();
        for id in [running_id.as_str(), completed_id.as_str(), "orphan"] {
            std::fs::create_dir(scratch.0.join(id)).unwrap();
            std::fs::write(scratch.0.join(id).join("0.bin"), b"test").unwrap();
        }
        Store::save(
            &scratch.0,
            &Index {
                jobs: vec![running, completed],
            },
        )
        .unwrap();
        let store = scratch.store(1024);
        let failed = snapshot(&store, &running_id);
        assert_eq!(failed.status, Status::Failed);
        assert_eq!(failed.error.as_deref(), Some("interrupted by restart"));
        assert!(failed.finished_at.is_some());
        assert!(!scratch.0.join(&running_id).exists());
        assert!(!scratch.0.join("orphan").exists());
        assert!(scratch.0.join(&completed_id).join("0.bin").exists());
        let saved: Index = crate::statefile::read(&scratch.0, INDEX).unwrap();
        assert_eq!(saved.jobs[0].status, Status::Failed);
    }
}
