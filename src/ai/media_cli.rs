//! CLI client for the local AI API's media routes.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::{Args, Subcommand};
use reqwest::multipart::{Form, Part};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_UPLOAD: u64 = 20 * 1024 * 1024;
const MAX_IMAGE_UPLOAD: u64 = 32 * 1024 * 1024;
const MAX_TRANSCRIPTION_BODY: u64 = 10 * 1024 * 1024;
const MAX_ERROR_BODY: usize = 64 * 1024;

#[derive(Args)]
pub struct SpeechArgs {
    /// Text to synthesize
    text: String,
    /// Output audio file
    #[arg(short, long)]
    output: PathBuf,
    /// TTS model (default: configured ai.tts model)
    #[arg(short, long)]
    model: Option<String>,
    /// Voice to request
    #[arg(long)]
    voice: Option<String>,
    /// Audio response format
    #[arg(long)]
    format: Option<String>,
    /// Playback speed
    #[arg(long)]
    speed: Option<f64>,
    /// Route through this room
    #[arg(long)]
    room: Option<String>,
    /// Replace an existing output file
    #[arg(long)]
    force: bool,
}

#[derive(Args)]
pub struct VoicesArgs {
    /// Route through this room
    #[arg(long)]
    room: Option<String>,
}

#[derive(Args)]
pub struct TranscribeArgs {
    /// Audio file to transcribe
    file: PathBuf,
    /// Transcription model (default: configured ai.stt model)
    #[arg(short, long)]
    model: Option<String>,
    /// Route through this room
    #[arg(long)]
    room: Option<String>,
}

#[derive(Subcommand)]
pub enum ImageAction {
    /// Generate images from a text prompt
    Generate(MediaGenerateArgs),
    /// Edit an image using the selected model
    Edit {
        /// Source image
        #[arg(long)]
        image: PathBuf,
        /// Image model to request
        #[arg(short, long)]
        model: String,
        /// Output image file
        #[arg(short, long)]
        output: PathBuf,
        /// Additional upstream parameter (key=value; may be repeated)
        #[arg(long)]
        param: Vec<String>,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
        /// Replace an existing output file
        #[arg(long)]
        force: bool,
    },
}

#[derive(Args)]
pub struct MediaGenerateArgs {
    /// Text prompt for the generation
    prompt: String,
    /// Model to request
    #[arg(short, long)]
    model: String,
    /// Output file
    #[arg(short, long)]
    output: PathBuf,
    /// Additional upstream parameter (key=value; may be repeated)
    #[arg(long)]
    param: Vec<String>,
    /// Route through this room
    #[arg(long)]
    room: Option<String>,
    /// Replace existing output files
    #[arg(long)]
    force: bool,
}

#[derive(Args)]
pub struct JobGenerateArgs {
    #[command(flatten)]
    media: MediaGenerateArgs,
    /// Print the submitted job without waiting or writing the output
    #[arg(long)]
    no_wait: bool,
}

#[derive(Subcommand)]
pub enum VideoAction {
    /// Generate a video from a text prompt and optional reference image
    Generate {
        #[command(flatten)]
        args: JobGenerateArgs,
        /// Reference image to upload
        #[arg(long)]
        image: Option<PathBuf>,
    },
    #[command(flatten)]
    Job(MediaJobAction),
}

#[derive(Subcommand)]
pub enum MusicAction {
    /// Generate music from a text prompt
    Generate(JobGenerateArgs),
    #[command(flatten)]
    Job(MediaJobAction),
}

#[derive(Subcommand)]
pub enum MediaJobAction {
    /// List generation jobs
    #[command(visible_alias = "ls")]
    List {
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
    },
    /// Show a generation job
    Status {
        /// Job id
        id: String,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
    },
    /// Download a completed generation
    #[command(visible_alias = "get")]
    Download {
        /// Job id
        id: String,
        /// Output file
        #[arg(short, long)]
        output: PathBuf,
        /// Content variant to download
        #[arg(long)]
        variant: Option<String>,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
        /// Replace an existing output file
        #[arg(long)]
        force: bool,
    },
    /// Delete a generation job
    #[command(visible_alias = "rm")]
    Delete {
        /// Job id
        id: String,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JobFamily {
    ThreeD,
    Video,
    Music,
}

impl JobFamily {
    fn path(self) -> &'static str {
        match self {
            Self::ThreeD => "/3d/generations",
            Self::Video => "/videos",
            Self::Music => "/music",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::ThreeD => "3D",
            Self::Video => "video",
            Self::Music => "music",
        }
    }

    fn job_path(self, id: &str) -> Result<String> {
        if self == Self::ThreeD {
            return generation_path(id);
        }
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            bail!("invalid {} job id: {id}", self.name());
        }
        Ok(format!("{}/{id}", self.path()))
    }
}

#[derive(Subcommand)]
pub enum ThreeDAction {
    /// Generate a 3D asset from an image
    Generate {
        /// Source image
        #[arg(long)]
        image: PathBuf,
        /// 3D model to request
        #[arg(short, long)]
        model: String,
        /// Save the completed asset to this file (ignored with --no-wait)
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Additional upstream parameter (key=value; may be repeated; background defaults to true)
        #[arg(long)]
        param: Vec<String>,
        /// Return the submitted job without waiting for completion
        #[arg(long)]
        no_wait: bool,
        /// Maximum generation and polling time in seconds
        #[arg(long, default_value_t = 2400)]
        timeout: u64,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
        /// Replace an existing output file
        #[arg(long)]
        force: bool,
    },
    /// List 3D generation jobs
    Ls {
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
    },
    /// Show one 3D generation job
    Status {
        /// Generation id
        id: String,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
    },
    /// Wait for a 3D generation job to finish (and optionally save it)
    Wait {
        /// Generation id
        id: String,
        /// Save the completed asset to this file
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Maximum waiting time in seconds
        #[arg(long, default_value_t = 300)]
        timeout: u64,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
        /// Replace an existing output file
        #[arg(long)]
        force: bool,
    },
    /// Download a completed 3D asset
    Get {
        /// Generation id
        id: String,
        /// Output asset file
        #[arg(short, long)]
        output: PathBuf,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
        /// Replace an existing output file
        #[arg(long)]
        force: bool,
    },
    /// Delete a 3D generation job
    Rm {
        /// Generation id
        id: String,
        /// Route through this room
        #[arg(long)]
        room: Option<String>,
    },
}

pub enum Action {
    Speech(SpeechArgs),
    Voices(VoicesArgs),
    Transcribe(TranscribeArgs),
    Image(ImageAction),
    Video(VideoAction),
    Music(MusicAction),
    ThreeD(ThreeDAction),
}

/// Start the API through the CLI's daemon helper, then run its HTTP client.
pub fn run(action: Action, start_api: impl FnOnce() -> Result<Value>) -> Result<()> {
    validate_action(&action)?;
    let reply = start_api()?;
    let listen = reply
        .get("listen")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow!("ai.serve.start response is missing `listen`"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("creating media client runtime")?;
    runtime.block_on(run_async(action, listen))
}

fn validate_action(action: &Action) -> Result<()> {
    match action {
        Action::Speech(args) => {
            check_output(&args.output, args.force)?;
            if args.speed.is_some_and(|s| !s.is_finite()) {
                bail!("speed must be a finite number");
            }
        }
        Action::Image(ImageAction::Edit {
            model,
            output,
            param,
            force,
            ..
        }) => {
            if model.trim().is_empty() {
                bail!("image model must not be empty");
            }
            image_params(param)?;
            check_output(output, *force)?;
        }
        Action::Image(ImageAction::Generate(args)) => {
            media_body(args, true)?;
            check_output(&args.output, args.force)?;
        }
        Action::Video(VideoAction::Generate { args, .. })
        | Action::Music(MusicAction::Generate(args)) => {
            media_body(&args.media, false)?;
            if !args.no_wait {
                check_output(&args.media.output, args.media.force)?;
            }
        }
        Action::Video(VideoAction::Job(action)) => validate_media_job(JobFamily::Video, action)?,
        Action::Music(MusicAction::Job(action)) => validate_media_job(JobFamily::Music, action)?,
        Action::ThreeD(ThreeDAction::Generate {
            model,
            output,
            param,
            no_wait,
            timeout,
            force,
            ..
        }) => {
            if model.trim().is_empty() {
                bail!("3D model must not be empty");
            }
            if *timeout == 0 {
                bail!("timeout must be greater than zero");
            }
            generation_params(param, *no_wait)?;
            if !*no_wait && let Some(path) = output {
                check_output(path, *force)?;
            }
        }
        Action::ThreeD(ThreeDAction::Get { output, force, .. }) => {
            check_output(output, *force)?;
        }
        Action::ThreeD(ThreeDAction::Wait {
            output,
            timeout,
            force,
            ..
        }) => {
            if *timeout == 0 {
                bail!("timeout must be greater than zero");
            }
            if let Some(path) = output {
                check_output(path, *force)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_media_job(family: JobFamily, action: &MediaJobAction) -> Result<()> {
    match action {
        MediaJobAction::List { .. } => {}
        MediaJobAction::Status { id, .. } | MediaJobAction::Delete { id, .. } => {
            family.job_path(id)?;
        }
        MediaJobAction::Download {
            id,
            variant,
            output,
            force,
            ..
        } => {
            media_content_path(family, id, variant.as_deref())?;
            check_output(output, *force)?;
        }
    }
    Ok(())
}

fn check_output(path: &Path, force: bool) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) if !force => bail!(
            "output file already exists: {} (use --force to replace it)",
            path.display()
        ),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("checking output {}", path.display())),
    }
}

fn room_prefix(listen: &str, room: Option<&str>) -> Result<String> {
    let base = format!("http://{listen}/v1");
    match room {
        None => Ok(base),
        Some("") => bail!("room must not be empty"),
        // URL parsers normalize dot segments even when percent-encoded.
        Some("." | "..") => bail!("room cannot be a dot path segment"),
        Some(room) => Ok(format!("{base}/rooms/{}", encode_segment(room))),
    }
}

fn encode_segment(value: &str) -> String {
    let mut encoded = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            encoded.push(b as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{b:02X}").expect("writing to String");
        }
    }
    encoded
}

fn speech_body(args: &SpeechArgs) -> Value {
    let mut body = json!({"input": args.text});
    let object = body.as_object_mut().expect("speech object");
    if let Some(value) = &args.model {
        object.insert("model".into(), json!(value));
    }
    if let Some(value) = &args.voice {
        object.insert("voice".into(), json!(value));
    }
    if let Some(value) = &args.format {
        object.insert("response_format".into(), json!(value));
    }
    if let Some(value) = args.speed {
        object.insert("speed".into(), json!(value));
    }
    body
}

fn parse_params(params: &[String]) -> Result<Vec<(String, Value)>> {
    let mut seen = HashSet::new();
    params
        .iter()
        .map(|item| {
            let (key, text) = item
                .split_once('=')
                .ok_or_else(|| anyhow!("invalid --param {item:?}: expected key=value"))?;
            if key.is_empty() || key.trim() != key || key.chars().any(char::is_control) {
                bail!("invalid --param key {key:?}");
            }
            if matches!(key, "image" | "model") || !seen.insert(key.to_string()) {
                bail!("duplicate or reserved --param key {key:?}");
            }
            let value = serde_json::from_str::<Value>(text)
                .ok()
                .filter(|v| v.is_number() || v.is_boolean())
                .unwrap_or_else(|| json!(text));
            Ok((key.to_string(), value))
        })
        .collect()
}

fn generation_params(params: &[String], no_wait: bool) -> Result<Vec<(String, Value)>> {
    let mut params = parse_params(params)?;
    match params.iter().find(|(key, _)| key == "background") {
        None => params.push(("background".into(), json!(true))),
        Some((_, Value::Bool(true))) => {}
        Some(_) if no_wait => bail!("--no-wait requires background=true"),
        Some(_) => {}
    }
    Ok(params)
}

fn image_params(params: &[String]) -> Result<Vec<(String, Value)>> {
    let params = parse_params(params)?;
    if params.iter().any(|(key, _)| key == "response_format") {
        bail!("reserved --param key \"response_format\": image edits require binary output");
    }
    Ok(params)
}

fn media_body(args: &MediaGenerateArgs, image: bool) -> Result<Value> {
    if args.model.trim().is_empty() {
        bail!("model must not be empty");
    }
    if args.prompt.trim().is_empty() {
        bail!("prompt must not be empty");
    }
    let mut body = json!({"model": args.model, "prompt": args.prompt});
    let fields = body.as_object_mut().expect("media object");
    for (key, value) in parse_params(&args.param)? {
        if key == "prompt" || key == "input_reference" || (image && key == "response_format") {
            bail!("reserved --param key {key:?}");
        }
        fields.insert(key, value);
    }
    if image {
        fields.insert("response_format".into(), json!("b64_json"));
    }
    Ok(body)
}

fn media_form(image: Part, body: &Value) -> Form {
    let mut form = Form::new().part("input_reference", image);
    for (key, value) in body.as_object().expect("media object") {
        let text = value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        form = form.text(key.clone(), text);
    }
    form
}

fn media_content_path(family: JobFamily, id: &str, variant: Option<&str>) -> Result<String> {
    let mut path = format!("{}/content", family.job_path(id)?);
    if let Some(variant) = variant {
        if variant.is_empty()
            || variant.len() > 32
            || !variant
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            bail!("invalid content variant: {variant}");
        }
        path.push_str("?variant=");
        path.push_str(variant);
    }
    Ok(path)
}

fn guessed_mime(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "wav" => "audio/wav",
        "mp3" => "audio/mpeg",
        "m4a" | "mp4" => "audio/mp4",
        "ogg" | "oga" => "audio/ogg",
        "opus" => "audio/opus",
        "flac" => "audio/flac",
        "webm" => "audio/webm",
        "aac" => "audio/aac",
        _ => "application/octet-stream",
    }
}

async fn file_part(path: &Path, max_upload: u64) -> Result<Part> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening upload {}", path.display()))?;
    if file.metadata().await?.len() > max_upload {
        bail!(
            "upload {} exceeds the {} MiB limit",
            path.display(),
            max_upload / (1024 * 1024)
        );
    }
    let mut bytes = Vec::new();
    file.take(max_upload + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > max_upload {
        bail!(
            "upload {} exceeds the {} MiB limit",
            path.display(),
            max_upload / (1024 * 1024)
        );
    }
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("upload");
    Ok(Part::bytes(bytes)
        .file_name(filename.to_string())
        .mime_str(guessed_mime(path))?)
}

fn transcription_form(file: Part, args: &TranscribeArgs) -> Form {
    let form = Form::new().part("file", file);
    match &args.model {
        Some(model) => form.text("model", model.clone()),
        None => form,
    }
}

fn generation_form(image: Part, model: &str, params: &[(String, Value)]) -> Form {
    let mut form = Form::new()
        .part("image", image)
        .text("model", model.to_string());
    for (key, value) in params {
        let text = value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        form = form.text(key.clone(), text);
    }
    form
}

async fn checked(response: reqwest::Response) -> Result<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let mut response = response;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let available = MAX_ERROR_BODY.saturating_sub(body.len());
        body.extend_from_slice(&chunk[..chunk.len().min(available)]);
        if body.len() == MAX_ERROR_BODY {
            break;
        }
    }
    let parsed = serde_json::from_slice::<Value>(&body).ok();
    let message = parsed
        .as_ref()
        .and_then(|v| v.pointer("/error/message"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| String::from_utf8_lossy(&body).trim().to_string());
    if message.is_empty() {
        bail!("HTTP {status}");
    }
    bail!("HTTP {status}: {message}")
}

async fn response_json(response: reqwest::Response) -> Result<Value> {
    let response = checked(response).await?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }
    response.json().await.context("reading API JSON response")
}

fn generation_path(id: &str) -> Result<String> {
    let valid = id.strip_prefix("gen3d_").is_some_and(|hex| {
        hex.len() == 32
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    });
    if !valid {
        bail!("invalid 3D generation id: {id}");
    }
    Ok(format!("/3d/generations/{id}"))
}

#[derive(Debug, PartialEq, Eq)]
enum JobStatus {
    Pending,
    Completed,
    Failed(String),
}

fn job_status(family: JobFamily, job: &Value) -> Result<JobStatus> {
    match job.get("status").and_then(Value::as_str) {
        Some("queued" | "pending" | "running" | "processing" | "in_progress") => {
            Ok(JobStatus::Pending)
        }
        Some("completed") => Ok(JobStatus::Completed),
        Some("failed" | "cancelled" | "canceled") => {
            let reason = job
                .pointer("/error/message")
                .and_then(Value::as_str)
                .or_else(|| job.get("error").and_then(Value::as_str))
                .or_else(|| job.get("message").and_then(Value::as_str))
                .unwrap_or("generation failed or was cancelled");
            Ok(JobStatus::Failed(reason.to_string()))
        }
        None if family == JobFamily::ThreeD
            && job.pointer("/data/0/url").and_then(Value::as_str).is_some() =>
        {
            Ok(JobStatus::Completed)
        }
        Some(status) => bail!("unknown {} job status: {status}", family.name()),
        None => bail!("{} response is missing a job status", family.name()),
    }
}

fn job_id(family: JobFamily, job: &Value) -> Result<&str> {
    let id = job
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{} response is missing a generation id", family.name()))?;
    family.job_path(id)?;
    Ok(id)
}

async fn publish_output(temp: &Path, path: &Path, force: bool) -> Result<()> {
    if force {
        tokio::fs::rename(temp, path)
            .await
            .with_context(|| format!("replacing output {}", path.display()))?;
    } else {
        tokio::fs::hard_link(temp, path).await.with_context(|| {
            format!(
                "creating output {} (use --force to replace an existing file)",
                path.display()
            )
        })?;
    }
    Ok(())
}

fn temporary_output(path: &Path) -> PathBuf {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .join(format!(".mistl-media-{}.tmp", uuid::Uuid::new_v4()))
}

async fn write_response(
    mut response: reqwest::Response,
    path: &Path,
    force: bool,
) -> Result<Value> {
    response = checked(response).await?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let content_length = response.content_length();
    // Stage alongside the destination so failed downloads never truncate it.
    let temp = temporary_output(path);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .await
        .with_context(|| format!("creating temporary output for {}", path.display()))?;
    let result: Result<u64> = async {
        let mut bytes = 0u64;
        let interrupted = |bytes: u64| match content_length {
            Some(total) => format!(
                "download interrupted after {bytes} of {total} bytes; the daemon log has the cause; retry the download"
            ),
            None => format!(
                "download interrupted after {bytes} bytes; the daemon log has the cause; retry the download"
            ),
        };
        while let Some(chunk) = response
            .chunk()
            .await
            .with_context(|| interrupted(bytes))?
        {
            file.write_all(&chunk).await.context("writing download")?;
            bytes += chunk.len() as u64;
        }
        if content_length.is_some_and(|total| bytes < total) {
            bail!("{}", interrupted(bytes));
        }
        file.flush().await?;
        drop(file);
        publish_output(&temp, path, force).await?;
        Ok(bytes)
    }
    .await;
    let _ = tokio::fs::remove_file(&temp).await;
    let bytes = result?;
    Ok(json!({"path": path.to_string_lossy(), "bytes": bytes, "content_type": content_type}))
}

fn image_extension(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some("jpg")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        Some("webp")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("gif")
    } else {
        None
    }
}

fn image_output_path(output: &Path, index: usize, bytes: &[u8]) -> Result<PathBuf> {
    if index == 0 {
        return Ok(output.to_owned());
    }
    let mut name = output
        .file_stem()
        .ok_or_else(|| anyhow!("invalid image output path"))?
        .to_os_string();
    name.push(format!("-{index}"));
    if let Some(ext) = image_extension(bytes) {
        name.push(".");
        name.push(ext);
    } else if let Some(ext) = output.extension() {
        name.push(".");
        name.push(ext);
    }
    Ok(output.with_file_name(name))
}

async fn save_images(response: Value, output: &Path, force: bool) -> Result<Value> {
    let data = response
        .get("data")
        .and_then(Value::as_array)
        .filter(|data| !data.is_empty())
        .ok_or_else(|| anyhow!("image response is missing data"))?;
    let mut images = Vec::new();
    for (index, image) in data.iter().enumerate() {
        let encoded = image
            .get("b64_json")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("image {index} is missing b64_json"))?;
        let bytes = STANDARD
            .decode(encoded)
            .with_context(|| format!("decoding image {index}"))?;
        let path = image_output_path(output, index, &bytes)?;
        check_output(&path, force)?;
        images.push((path, bytes));
    }
    drop(response);
    let mut saved = Vec::new();
    for (path, bytes) in images {
        let temp = temporary_output(&path);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .await
            .with_context(|| format!("creating temporary output for {}", path.display()))?;
        let result: Result<()> = async {
            file.write_all(&bytes).await?;
            file.flush().await?;
            drop(file);
            publish_output(&temp, &path, force).await
        }
        .await;
        let _ = tokio::fs::remove_file(&temp).await;
        result?;
        saved.push(json!({"path": path.to_string_lossy(), "bytes": bytes.len()}));
    }
    Ok(json!({"files": saved}))
}

async fn run_async(action: Action, listen: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    match action {
        Action::Speech(args) => {
            let base = room_prefix(listen, args.room.as_deref())?;
            let response = client
                .post(format!("{base}/audio/speech"))
                .json(&speech_body(&args))
                .send()
                .await?;
            println!(
                "{}",
                write_response(response, &args.output, args.force).await?
            );
        }
        Action::Voices(args) => {
            let base = room_prefix(listen, args.room.as_deref())?;
            println!(
                "{}",
                response_json(client.get(format!("{base}/audio/voices")).send().await?).await?
            );
        }
        Action::Transcribe(args) => {
            let base = room_prefix(listen, args.room.as_deref())?;
            let file = file_part(&args.file, MAX_UPLOAD).await?;
            let request = client
                .post(format!("{base}/audio/transcriptions"))
                .multipart(transcription_form(file, &args))
                .build()?;
            let length = request
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            if length.is_some_and(|n| n > MAX_TRANSCRIPTION_BODY) {
                bail!("transcription multipart body exceeds the local API's 10 MiB limit");
            }
            println!("{}", response_json(client.execute(request).await?).await?);
        }
        Action::Image(ImageAction::Edit {
            image,
            model,
            output,
            param,
            room,
            force,
        }) => {
            let base = room_prefix(listen, room.as_deref())?;
            let params = image_params(&param)?;
            let image = file_part(&image, MAX_IMAGE_UPLOAD).await?;
            let form = generation_form(image, &model, &params).text("response_format", "binary");
            let response = client
                .post(format!("{base}/images/edits"))
                .timeout(Duration::from_secs(11 * 60))
                .multipart(form)
                .send()
                .await?;
            println!("{}", write_response(response, &output, force).await?);
        }
        Action::Image(ImageAction::Generate(args)) => {
            let base = room_prefix(listen, args.room.as_deref())?;
            let body = media_body(&args, true)?;
            let response = response_json(
                client
                    .post(format!("{base}/images/generations"))
                    .timeout(Duration::from_secs(11 * 60))
                    .json(&body)
                    .send()
                    .await?,
            )
            .await?;
            println!("{}", save_images(response, &args.output, args.force).await?);
        }
        Action::Video(VideoAction::Generate { args, image }) => {
            generate_media(&client, listen, JobFamily::Video, args, image).await?;
        }
        Action::Music(MusicAction::Generate(args)) => {
            generate_media(&client, listen, JobFamily::Music, args, None).await?;
        }
        Action::Video(VideoAction::Job(action)) => {
            run_media_job(&client, listen, JobFamily::Video, action).await?
        }
        Action::Music(MusicAction::Job(action)) => {
            run_media_job(&client, listen, JobFamily::Music, action).await?
        }
        Action::ThreeD(action) => run_3d(&client, listen, action).await?,
    }
    Ok(())
}

async fn generate_media(
    client: &reqwest::Client,
    listen: &str,
    family: JobFamily,
    args: JobGenerateArgs,
    image: Option<PathBuf>,
) -> Result<()> {
    let base = room_prefix(listen, args.media.room.as_deref())?;
    let body = media_body(&args.media, false)?;
    let request = client
        .post(format!("{base}{}", family.path()))
        .timeout(Duration::from_secs(6 * 60));
    let request = match image {
        Some(image) => request.multipart(media_form(
            file_part(&image, MAX_IMAGE_UPLOAD).await?,
            &body,
        )),
        None => request.json(&body),
    };
    let mut job = response_json(request.send().await?).await?;
    if let JobStatus::Failed(reason) = job_status(family, &job)? {
        bail!("{} generation failed: {reason}", family.name());
    }
    let path = family.job_path(job_id(family, &job)?)?;
    if args.no_wait {
        println!("{job}");
        return Ok(());
    }
    tokio::time::timeout(
        Duration::from_secs(3 * 60 * 60),
        poll_job(client, &base, &path, &mut job, family),
    )
    .await
    .map_err(|_| anyhow!("{} generation timed out after 3 hours", family.name()))??;
    let response = client.get(format!("{base}{path}/content")).send().await?;
    println!(
        "{}",
        write_response(response, &args.media.output, args.media.force).await?
    );
    Ok(())
}

async fn run_media_job(
    client: &reqwest::Client,
    listen: &str,
    family: JobFamily,
    action: MediaJobAction,
) -> Result<()> {
    let result = match action {
        MediaJobAction::List { room } => {
            let base = room_prefix(listen, room.as_deref())?;
            response_json(
                client
                    .get(format!("{base}{}", family.path()))
                    .send()
                    .await?,
            )
            .await?
        }
        MediaJobAction::Status { id, room } => {
            let base = room_prefix(listen, room.as_deref())?;
            let path = family.job_path(&id)?;
            response_json(client.get(format!("{base}{path}")).send().await?).await?
        }
        MediaJobAction::Download {
            id,
            output,
            variant,
            room,
            force,
        } => {
            let base = room_prefix(listen, room.as_deref())?;
            let path = media_content_path(family, &id, variant.as_deref())?;
            write_response(
                client.get(format!("{base}{path}")).send().await?,
                &output,
                force,
            )
            .await?
        }
        MediaJobAction::Delete { id, room } => {
            let base = room_prefix(listen, room.as_deref())?;
            let path = family.job_path(&id)?;
            response_json(client.delete(format!("{base}{path}")).send().await?).await?
        }
    };
    println!("{result}");
    Ok(())
}

async fn run_3d(client: &reqwest::Client, listen: &str, action: ThreeDAction) -> Result<()> {
    run_3d_with_output(
        client,
        listen,
        action,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    )
    .await
}

async fn poll_job(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    job: &mut Value,
    family: JobFamily,
) -> Result<()> {
    let mut delay = Duration::from_secs(if family == JobFamily::ThreeD { 2 } else { 5 });
    loop {
        match job_status(family, job)? {
            JobStatus::Completed => return Ok(()),
            JobStatus::Failed(reason) => bail!("{} generation failed: {reason}", family.name()),
            JobStatus::Pending => {
                if family != JobFamily::ThreeD {
                    let status = job["status"].as_str().unwrap_or("pending");
                    if let Some(progress) = job["progress"].as_f64() {
                        eprintln!("{} job {path}: {status} ({progress}%)", family.name());
                    } else {
                        eprintln!("{} job {path}: {status}", family.name());
                    }
                }
                tokio::time::sleep(delay).await;
                *job = response_json(client.get(format!("{base}{path}")).send().await?).await?;
                if family == JobFamily::ThreeD {
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
            }
        }
    }
}

async fn run_3d_with_output(
    client: &reqwest::Client,
    listen: &str,
    action: ThreeDAction,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> Result<()> {
    match action {
        ThreeDAction::Generate {
            image,
            model,
            output,
            param,
            no_wait,
            timeout,
            room,
            force,
        } => {
            if no_wait && output.is_some() {
                writeln!(
                    stderr,
                    "note: --output is ignored with --no-wait; use `mistl ai 3d get <id> -o <file>` when the job completes"
                )?;
            }
            let base = room_prefix(listen, room.as_deref())?;
            let image = file_part(&image, MAX_UPLOAD).await?;
            let params = generation_params(&param, no_wait)?;
            let form = generation_form(image, &model, &params);
            let deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_secs(timeout))
                .ok_or_else(|| anyhow!("3D generation timeout is too large"))?;
            let result = tokio::time::timeout_at(deadline, async {
                let mut job = response_json(
                    client
                        .post(format!("{base}/3d/generations"))
                        .multipart(form)
                        .send()
                        .await?,
                )
                .await?;
                match job_status(JobFamily::ThreeD, &job)? {
                    JobStatus::Failed(reason) => bail!("3D generation failed: {reason}"),
                    JobStatus::Pending if !no_wait => {
                        let path = generation_path(job_id(JobFamily::ThreeD, &job)?)?;
                        poll_job(client, &base, &path, &mut job, JobFamily::ThreeD).await?;
                    }
                    _ => {}
                }
                Ok::<_, anyhow::Error>(job)
            })
            .await
            .map_err(|_| anyhow!("3D generation timed out after {timeout} seconds"))??;
            if !no_wait
                && job_status(JobFamily::ThreeD, &result)? == JobStatus::Completed
                && let Some(path) = output
            {
                let route = generation_path(job_id(JobFamily::ThreeD, &result)?)?;
                let response = client.get(format!("{base}{route}/content")).send().await?;
                write_response(response, &path, force).await?;
            }
            writeln!(stdout, "{result}")?;
        }
        ThreeDAction::Ls { room } => {
            let base = room_prefix(listen, room.as_deref())?;
            writeln!(
                stdout,
                "{}",
                response_json(client.get(format!("{base}/3d/generations")).send().await?).await?
            )?;
        }
        ThreeDAction::Status { id, room } => {
            let base = room_prefix(listen, room.as_deref())?;
            let path = generation_path(&id)?;
            writeln!(
                stdout,
                "{}",
                response_json(client.get(format!("{base}{path}")).send().await?).await?
            )?;
        }
        ThreeDAction::Wait {
            id,
            output,
            timeout,
            room,
            force,
        } => {
            let base = room_prefix(listen, room.as_deref())?;
            let path = generation_path(&id)?;
            let deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_secs(timeout))
                .ok_or_else(|| anyhow!("3D wait timeout is too large"))?;
            let mut latest = None;
            let completed = match tokio::time::timeout_at(deadline, async {
                latest =
                    Some(response_json(client.get(format!("{base}{path}")).send().await?).await?);
                poll_job(
                    client,
                    &base,
                    &path,
                    latest.as_mut().unwrap(),
                    JobFamily::ThreeD,
                )
                .await
            })
            .await
            {
                Ok(result) => {
                    result?;
                    true
                }
                Err(_) if latest.is_some() => false,
                Err(_) => bail!("timed out after {timeout}s before receiving 3D job status"),
            };
            let job = latest.expect("a completed or pending wait has a job");
            if completed && let Some(output) = output {
                let response = client.get(format!("{base}{path}/content")).send().await?;
                write_response(response, &output, force).await?;
            }
            writeln!(stdout, "{job}")?;
            if !completed {
                let status = job["status"].as_str().unwrap_or("pending");
                writeln!(
                    stderr,
                    "note: job {id} is still {status} after {timeout}s; run `mistl ai 3d wait {id}` again"
                )?;
            }
        }
        ThreeDAction::Get {
            id,
            output,
            room,
            force,
        } => {
            let base = room_prefix(listen, room.as_deref())?;
            let path = generation_path(&id)?;
            let response = client.get(format!("{base}{path}/content")).send().await?;
            writeln!(
                stdout,
                "{}",
                write_response(response, &output, force).await?
            )?;
        }
        ThreeDAction::Rm { id, room } => {
            let base = room_prefix(listen, room.as_deref())?;
            let path = generation_path(&id)?;
            writeln!(
                stdout,
                "{}",
                response_json(client.delete(format!("{base}{path}")).send().await?).await?
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use tokio::net::TcpListener;

    fn temp_path(ext: &str) -> PathBuf {
        std::env::temp_dir().join(format!("mistl-media-test-{}.{}", uuid::Uuid::new_v4(), ext))
    }

    fn speech() -> SpeechArgs {
        SpeechArgs {
            text: "hello".into(),
            output: PathBuf::from("speech.wav"),
            model: None,
            voice: None,
            format: None,
            speed: None,
            room: None,
            force: false,
        }
    }

    fn image_edit(image: PathBuf, output: PathBuf) -> ImageAction {
        ImageAction::Edit {
            image,
            model: "image-model".into(),
            output,
            param: vec![],
            room: None,
            force: false,
        }
    }

    fn media_args(model: &str, output: PathBuf) -> MediaGenerateArgs {
        MediaGenerateArgs {
            prompt: "a landscape".into(),
            model: model.into(),
            output,
            param: vec!["seed=42".into()],
            room: Some("team/one".into()),
            force: false,
        }
    }

    fn generation_action(
        family: JobFamily,
        args: JobGenerateArgs,
        image: Option<PathBuf>,
    ) -> Action {
        if family == JobFamily::Video {
            Action::Video(VideoAction::Generate { args, image })
        } else {
            Action::Music(MusicAction::Generate(args))
        }
    }

    fn job_action(family: JobFamily, action: MediaJobAction) -> Action {
        if family == JobFamily::Video {
            Action::Video(VideoAction::Job(action))
        } else {
            Action::Music(MusicAction::Job(action))
        }
    }

    #[test]
    fn clap_parses_media_commands() {
        for kind in ["image", "video", "music"] {
            let args = [
                "mistl",
                "ai",
                kind,
                "generate",
                "a landscape",
                "-m",
                "media-model",
                "-o",
                "out.bin",
                "--param",
                "seed=42",
                "--room",
                "team",
                "--force",
            ];
            assert!(crate::cli::Cli::try_parse_from(args).is_ok());
            assert!(crate::cli::Cli::try_parse_from(&args[..7]).is_err());
            let mut args = args.to_vec();
            args.extend(["--image", "in.png"]);
            assert_eq!(
                crate::cli::Cli::try_parse_from(args).is_ok(),
                kind == "video"
            );
        }
        for kind in ["video", "music"] {
            for suffix in [
                vec!["list"],
                vec!["status", "job_1"],
                vec![
                    "download",
                    "job_1",
                    "-o",
                    "out.bin",
                    "--variant",
                    "preview",
                    "--force",
                ],
                vec!["delete", "job_1"],
                vec![
                    "generate",
                    "prompt",
                    "-m",
                    "media-model",
                    "-o",
                    "out.bin",
                    "--no-wait",
                ],
            ] {
                let mut args = vec!["mistl", "ai", kind];
                args.extend(suffix);
                args.extend(["--room", "team"]);
                assert!(
                    crate::cli::Cli::try_parse_from(args.clone()).is_ok(),
                    "{args:?}"
                );
            }
        }
    }

    #[test]
    fn validates_media_before_starting_api() {
        let path = temp_path("bin");
        std::fs::write(&path, b"original").unwrap();
        for family in [JobFamily::Video, JobFamily::Music] {
            let args = JobGenerateArgs {
                media: media_args("media-model", path.clone()),
                no_wait: false,
            };
            assert!(
                run(generation_action(family, args, None), || panic!(
                    "must not start API"
                ))
                .is_err()
            );
            assert!(
                validate_action(&generation_action(
                    family,
                    JobGenerateArgs {
                        media: media_args("media-model", path.clone()),
                        no_wait: true
                    },
                    None
                ))
                .is_ok()
            );
            for id in ["", "../other", "a?b", "a/b"] {
                assert!(family.job_path(id).is_err());
            }
            assert!(family.job_path(&"x".repeat(129)).is_err());
            assert!(media_content_path(family, "job-1", Some("preview&range=1-2")).is_err());
        }
        assert!(
            run(
                Action::Image(ImageAction::Generate(media_args(
                    "image-model",
                    path.clone()
                ))),
                || panic!("must not start API")
            )
            .is_err()
        );
        for param in ["prompt=other", "model=other", "response_format=url", "seed"] {
            let mut args = media_args("image-model", path.clone());
            args.force = true;
            args.param = vec![param.into()];
            assert!(media_body(&args, true).is_err());
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn image_generation_posts_json_and_saves_multiple_images() {
        let output = temp_path("png");
        let images: [&[u8]; 5] = [
            b"\x89PNG\r\n\x1a\n",
            b"\xff\xd8\xff",
            b"RIFF\0\0\0\0WEBP",
            b"GIF89a",
            b"unknown",
        ];
        let response = json!({"data": images.iter().map(|bytes| json!({"b64_json": STANDARD.encode(bytes)})).collect::<Vec<_>>()});
        let mut args = media_args("image-model", output.clone());
        args.param.push("n=5".into());
        let (listen, server) = mock(vec![(
            200,
            "application/json",
            response.to_string().into_bytes(),
        )])
        .await;
        run_async(Action::Image(ImageAction::Generate(args)), &listen)
            .await
            .unwrap();
        let requests = server.await.unwrap();
        let request = String::from_utf8_lossy(&requests[0]);
        let (headers, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with("POST /v1/rooms/team%2Fone/images/generations "));
        assert_eq!(
            serde_json::from_str::<Value>(body).unwrap(),
            json!({"prompt":"a landscape","model":"image-model","response_format":"b64_json","n":5,"seed":42})
        );
        for (index, (bytes, ext)) in images
            .iter()
            .zip(["png", "jpg", "webp", "gif", "png"])
            .enumerate()
        {
            let path = if index == 0 {
                output.clone()
            } else {
                output.with_file_name(format!(
                    "{}-{index}.{ext}",
                    output.file_stem().unwrap().to_str().unwrap()
                ))
            };
            assert_eq!(tokio::fs::read(&path).await.unwrap(), *bytes);
            tokio::fs::remove_file(path).await.unwrap();
        }
    }

    #[tokio::test]
    async fn image_generation_preflights_additional_outputs() {
        let output = temp_path("png");
        let second = image_output_path(&output, 1, b"GIF89a").unwrap();
        tokio::fs::write(&second, b"original").await.unwrap();
        let response = json!({"data":[{"b64_json": STANDARD.encode(b"first")},{"b64_json": STANDARD.encode(b"GIF89a")}]});
        assert!(save_images(response.clone(), &output, false).await.is_err());
        assert!(!output.exists());
        assert_eq!(tokio::fs::read(&second).await.unwrap(), b"original");
        save_images(response, &output, true).await.unwrap();
        assert_eq!(tokio::fs::read(&second).await.unwrap(), b"GIF89a");
        tokio::fs::remove_file(second).await.unwrap();
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn media_jobs_submit_poll_and_download_for_both_families() {
        for family in [JobFamily::Video, JobFamily::Music] {
            let output = temp_path("bin");
            let model = format!("{}-model", family.name());
            let (listen, server) = mock(vec![
                (
                    202,
                    "application/json",
                    br#"{"id":"job-1","status":"queued"}"#.to_vec(),
                ),
                (
                    200,
                    "application/json",
                    br#"{"id":"job-1","status":"in_progress","progress":50}"#.to_vec(),
                ),
                (
                    200,
                    "application/json",
                    br#"{"id":"job-1","status":"completed"}"#.to_vec(),
                ),
                (200, "application/octet-stream", b"media bytes".to_vec()),
            ])
            .await;
            let action = generation_action(
                family,
                JobGenerateArgs {
                    media: media_args(&model, output.clone()),
                    no_wait: false,
                },
                None,
            );
            run_async(action, &listen).await.unwrap();
            let requests = server.await.unwrap();
            let base = format!("/v1/rooms/team%2Fone{}", family.path());
            let post = String::from_utf8_lossy(&requests[0]);
            assert!(post.starts_with(&format!("POST {base} ")));
            assert_eq!(
                serde_json::from_str::<Value>(post.split_once("\r\n\r\n").unwrap().1).unwrap(),
                json!({"prompt":"a landscape","model":model,"seed":42})
            );
            for request in &requests[1..3] {
                assert!(
                    String::from_utf8_lossy(request).starts_with(&format!("GET {base}/job-1 "))
                );
            }
            assert!(
                String::from_utf8_lossy(&requests[3])
                    .starts_with(&format!("GET {base}/job-1/content "))
            );
            assert_eq!(tokio::fs::read(&output).await.unwrap(), b"media bytes");
            tokio::fs::remove_file(output).await.unwrap();
        }
    }

    #[tokio::test]
    async fn video_reference_image_uses_multipart_and_no_wait() {
        let image = temp_path("png");
        let output = temp_path("mp4");
        tokio::fs::write(&image, b"reference bytes").await.unwrap();
        tokio::fs::write(&output, b"original").await.unwrap();
        let (listen, server) = mock(vec![(
            201,
            "application/json",
            br#"{"id":"job-1","status":"queued"}"#.to_vec(),
        )])
        .await;
        let mut media = media_args("video-model", output.clone());
        media.room = None;
        let action = generation_action(
            JobFamily::Video,
            JobGenerateArgs {
                media,
                no_wait: true,
            },
            Some(image.clone()),
        );
        validate_action(&action).unwrap();
        run_async(action, &listen).await.unwrap();
        let requests = server.await.unwrap();
        let request = String::from_utf8_lossy(&requests[0]);
        assert!(request.starts_with("POST /v1/videos "));
        for text in [
            "name=\"input_reference\"; filename=\"",
            "image/png",
            "reference bytes",
            "name=\"prompt\"\r\n\r\na landscape",
            "name=\"model\"\r\n\r\nvideo-model",
            "name=\"seed\"\r\n\r\n42",
        ] {
            assert!(request.contains(text), "missing {text:?}");
        }
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"original");
        tokio::fs::remove_file(image).await.unwrap();
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn media_job_management_paths_and_failed_status() {
        for family in [JobFamily::Video, JobFamily::Music] {
            let output = temp_path("bin");
            let room = Some("team/one".into());
            let id = "job-1".to_string();
            let (listen, server) = mock(vec![
                (200, "application/json", br#"{"data":[]}"#.to_vec()),
                (
                    200,
                    "application/json",
                    br#"{"id":"job-1","status":"completed"}"#.to_vec(),
                ),
                (200, "application/octet-stream", b"preview".to_vec()),
                (204, "application/json", vec![]),
                (
                    200,
                    "application/json",
                    br#"{"id":"job-2","status":"failed","error":{"message":"upstream failed"}}"#
                        .to_vec(),
                ),
            ])
            .await;
            for action in [
                MediaJobAction::List { room: room.clone() },
                MediaJobAction::Status {
                    id: id.clone(),
                    room: room.clone(),
                },
                MediaJobAction::Download {
                    id: id.clone(),
                    output: output.clone(),
                    variant: Some("preview_1".into()),
                    room: room.clone(),
                    force: false,
                },
                MediaJobAction::Delete { id, room },
            ] {
                run_async(job_action(family, action), &listen)
                    .await
                    .unwrap();
            }
            let args = JobGenerateArgs {
                media: media_args("media-model", output.clone()),
                no_wait: false,
            };
            assert!(
                run_async(generation_action(family, args, None), &listen)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("upstream failed")
            );
            let requests = server.await.unwrap();
            let base = format!("/v1/rooms/team%2Fone{}", family.path());
            for (request, expected) in requests.iter().zip([
                format!("GET {base} "),
                format!("GET {base}/job-1 "),
                format!("GET {base}/job-1/content?variant=preview_1 "),
                format!("DELETE {base}/job-1 "),
            ]) {
                assert!(String::from_utf8_lossy(request).starts_with(&expected));
            }
            assert_eq!(tokio::fs::read(&output).await.unwrap(), b"preview");
            tokio::fs::remove_file(output).await.unwrap();
        }
    }

    // Capture complete requests, including multipart bodies, without an upstream.
    async fn mock(
        responses: Vec<(u16, &'static str, Vec<u8>)>,
    ) -> (String, tokio::task::JoinHandle<Vec<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, content_type, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 4096];
                let header_end = loop {
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    request.extend_from_slice(&buffer[..n]);
                    if let Some(pos) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                while request.len() < header_end + length {
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert_ne!(n, 0);
                    request.extend_from_slice(&buffer[..n]);
                }
                requests.push(request);
                let head = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(head.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
            requests
        });
        (addr, task)
    }

    #[test]
    fn speech_json_omits_unset_fields() {
        let mut args = speech();
        assert_eq!(speech_body(&args), json!({"input":"hello"}));
        args.model = Some("tts-model".into());
        args.voice = Some("speaker".into());
        args.format = Some("wav".into());
        args.speed = Some(1.25);
        assert_eq!(
            speech_body(&args),
            json!({"input":"hello","model":"tts-model","voice":"speaker","response_format":"wav","speed":1.25})
        );
    }

    #[test]
    fn encodes_the_whole_room_as_one_path_segment() {
        let prefix = room_prefix("127.0.0.1:6478", Some("team/one + \u{65e5}\u{672c}")).unwrap();
        assert_eq!(
            prefix,
            "http://127.0.0.1:6478/v1/rooms/team%2Fone%20%2B%20%E6%97%A5%E6%9C%AC"
        );
        let request = reqwest::Client::new()
            .get(format!("{prefix}/audio/voices"))
            .build()
            .unwrap();
        assert_eq!(
            request.url().path(),
            "/v1/rooms/team%2Fone%20%2B%20%E6%97%A5%E6%9C%AC/audio/voices"
        );
        assert!(room_prefix("localhost:6478", Some("..")).is_err());
    }

    #[test]
    fn params_preserve_numbers_booleans_and_strings() {
        let input = [
            "seed=42",
            "strength=0.5",
            "background=true",
            "note=a=b",
            "style=glb",
        ]
        .map(str::to_string);
        assert_eq!(
            parse_params(&input).unwrap(),
            vec![
                ("seed".into(), json!(42)),
                ("strength".into(), json!(0.5)),
                ("background".into(), json!(true)),
                ("note".into(), json!("a=b")),
                ("style".into(), json!("glb")),
            ]
        );
        for bad in [
            vec!["no-equals"],
            vec!["=1"],
            vec!["image=bad"],
            vec!["model=bad"],
            vec!["seed=1", "seed=2"],
        ] {
            assert!(
                parse_params(&bad.into_iter().map(str::to_string).collect::<Vec<_>>()).is_err()
            );
        }
        for no_wait in [false, true] {
            assert_eq!(
                generation_params(&[], no_wait).unwrap(),
                vec![("background".into(), json!(true))]
            );
            assert_eq!(
                generation_params(&["background=true".into()], no_wait).unwrap(),
                vec![("background".into(), json!(true))]
            );
        }
        assert_eq!(
            generation_params(&["background=false".into()], false).unwrap(),
            vec![("background".into(), json!(false))]
        );
        assert_eq!(
            generation_params(&["background=custom".into()], false).unwrap(),
            vec![("background".into(), json!("custom"))]
        );
        for value in ["false", "0", "custom"] {
            assert!(generation_params(&[format!("background={value}")], true).is_err());
        }
    }

    #[test]
    fn parses_job_status_and_ids() {
        assert_eq!(
            job_status(JobFamily::ThreeD, &json!({"status":"queued"})).unwrap(),
            JobStatus::Pending
        );
        assert_eq!(
            job_status(JobFamily::ThreeD, &json!({"status":"running"})).unwrap(),
            JobStatus::Pending
        );
        assert_eq!(
            job_status(JobFamily::ThreeD, &json!({"status":"completed"})).unwrap(),
            JobStatus::Completed
        );
        assert_eq!(
            job_status(JobFamily::ThreeD, &json!({"data":[{"url":"/content"}]})).unwrap(),
            JobStatus::Completed
        );
        assert_eq!(
            job_status(
                JobFamily::ThreeD,
                &json!({"status":"failed","error":{"message":"out of memory"}})
            )
            .unwrap(),
            JobStatus::Failed("out of memory".into())
        );
        assert!(job_status(JobFamily::ThreeD, &json!({"status":"mystery"})).is_err());
        assert!(generation_path("../../other").is_err());
        assert!(generation_path("gen3d_0123456789abcdef0123456789abcdef").is_ok());
    }

    #[test]
    fn refuses_existing_output_before_starting_the_api() {
        let path = temp_path("wav");
        std::fs::write(&path, b"original").unwrap();
        let mut args = speech();
        args.output = path.clone();
        assert!(run(Action::Speech(args), || panic!("must not start API")).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
        assert!(check_output(&path, true).is_ok());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn image_edit_validates_before_starting_the_api() {
        let image = temp_path("png");
        let output = temp_path("png");
        std::fs::write(&output, b"original").unwrap();
        let action = Action::Image(image_edit(image.clone(), output.clone()));
        let error = run(action, || panic!("must not start API")).unwrap_err();
        assert!(error.to_string().contains("output file already exists"));
        assert_eq!(std::fs::read(&output).unwrap(), b"original");

        for param in ["response_format=binary", "response_format=b64_json"] {
            let action = Action::Image(ImageAction::Edit {
                image: image.clone(),
                model: "image-model".into(),
                output: output.clone(),
                param: vec![param.into()],
                room: None,
                force: true,
            });
            let error = run(action, || panic!("must not start API")).unwrap_err();
            assert!(error.to_string().contains("response_format"));
        }
        let action = Action::Image(ImageAction::Edit {
            image,
            model: " ".into(),
            output: output.clone(),
            param: vec![],
            room: None,
            force: true,
        });
        let error = run(action, || panic!("must not start API")).unwrap_err();
        assert!(error.to_string().contains("image model must not be empty"));
        std::fs::remove_file(output).unwrap();
    }

    #[test]
    fn clap_parses_image_edit_and_requires_image_model_and_output() {
        for args in [
            vec![
                "mistl",
                "ai",
                "image",
                "edit",
                "-m",
                "image-model",
                "-o",
                "out.png",
            ],
            vec![
                "mistl", "ai", "image", "edit", "--image", "in.png", "-o", "out.png",
            ],
            vec![
                "mistl",
                "ai",
                "image",
                "edit",
                "--image",
                "in.png",
                "-m",
                "image-model",
            ],
        ] {
            assert!(crate::cli::Cli::try_parse_from(args).is_err());
        }
        let parsed = crate::cli::Cli::try_parse_from([
            "mistl",
            "ai",
            "image",
            "edit",
            "--image",
            "in.png",
            "-m",
            "image-model",
            "-o",
            "out.webp",
            "--param",
            "scale=2",
            "--param",
            "output_format=webp",
            "--room",
            "team/one",
            "--force",
        ])
        .unwrap();
        let Some(crate::cli::Command::Ai {
            action:
                crate::cli::AiAction::Image {
                    action:
                        ImageAction::Edit {
                            image,
                            model,
                            output,
                            param,
                            room,
                            force,
                        },
                },
        }) = parsed.command
        else {
            panic!("expected image edit command");
        };
        assert_eq!(image, PathBuf::from("in.png"));
        assert_eq!(model, "image-model");
        assert_eq!(output, PathBuf::from("out.webp"));
        assert_eq!(param, ["scale=2", "output_format=webp"]);
        assert_eq!(room.as_deref(), Some("team/one"));
        assert!(force);
    }

    #[test]
    fn clap_requires_3d_model_and_accepts_room_and_force() {
        assert!(
            crate::cli::Cli::try_parse_from(["mistl", "ai", "3d", "generate", "--image", "x.png"])
                .is_err()
        );
        for args in [
            vec![
                "mistl", "ai", "speech", "hi", "-o", "x.wav", "--room", "r", "--force",
            ],
            vec!["mistl", "ai", "voices", "--room", "r"],
            vec!["mistl", "ai", "transcribe", "x.wav", "--room", "r"],
            vec![
                "mistl", "ai", "3d", "generate", "--image", "x.png", "-m", "3d-model", "--param",
                "seed=1", "--room", "r",
            ],
            vec!["mistl", "ai", "3d", "ls", "--room", "r"],
            vec!["mistl", "ai", "3d", "status", "id", "--room", "r"],
            vec![
                "mistl",
                "ai",
                "3d",
                "wait",
                "id",
                "-o",
                "x.glb",
                "--timeout",
                "30",
                "--room",
                "r",
                "--force",
            ],
            vec![
                "mistl", "ai", "3d", "get", "id", "-o", "x.glb", "--room", "r", "--force",
            ],
            vec!["mistl", "ai", "3d", "rm", "id", "--room", "r"],
        ] {
            assert!(crate::cli::Cli::try_parse_from(&args).is_ok(), "{args:?}");
        }
    }

    #[test]
    fn wait_validates_timeout_and_output_before_starting_the_api() {
        let id = "gen3d_0123456789abcdef0123456789abcdef";
        let action = Action::ThreeD(ThreeDAction::Wait {
            id: id.into(),
            output: None,
            timeout: 0,
            room: None,
            force: false,
        });
        let error = run(action, || panic!("must not start API")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("timeout must be greater than zero")
        );

        let path = temp_path("glb");
        std::fs::write(&path, b"original").unwrap();
        let action = Action::ThreeD(ThreeDAction::Wait {
            id: id.into(),
            output: Some(path.clone()),
            timeout: 1,
            room: None,
            force: false,
        });
        assert!(run(action, || panic!("must not start API")).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
        let action = Action::ThreeD(ThreeDAction::Wait {
            id: id.into(),
            output: Some(path.clone()),
            timeout: 1,
            room: None,
            force: true,
        });
        validate_action(&action).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn wait_completes_and_downloads_to_the_requested_room() {
        const ID: &str = "gen3d_0123456789abcdef0123456789abcdef";
        let output = temp_path("glb");
        let (listen, server) = mock(vec![
            (
                200,
                "application/json",
                format!(r#"{{"id":"{ID}","status":"in_progress"}}"#).into_bytes(),
            ),
            (
                200,
                "application/json",
                format!(r#"{{"id":"{ID}","status":"completed"}}"#).into_bytes(),
            ),
            (200, "model/gltf-binary", b"GLB bytes".to_vec()),
        ])
        .await;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        run_3d_with_output(
            &reqwest::Client::builder().no_proxy().build().unwrap(),
            &listen,
            ThreeDAction::Wait {
                id: ID.into(),
                output: Some(output.clone()),
                timeout: 8,
                room: Some("team/one".into()),
                force: false,
            },
            &mut stdout,
            &mut stderr,
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&stdout).unwrap()["status"],
            "completed"
        );
        assert!(stderr.is_empty());
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"GLB bytes");
        let requests = server.await.unwrap();
        let job_route = format!("GET /v1/rooms/team%2Fone/3d/generations/{ID}");
        for request in &requests[..2] {
            assert!(String::from_utf8_lossy(request).starts_with(&format!("{job_route} ")));
        }
        assert!(
            String::from_utf8_lossy(&requests[2]).starts_with(&format!("{job_route}/content "))
        );
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn wait_timeout_prints_the_latest_pending_job_and_succeeds() {
        const ID: &str = "gen3d_0123456789abcdef0123456789abcdef";
        let output = temp_path("glb");
        let (listen, server) = mock(vec![
            (
                200,
                "application/json",
                format!(r#"{{"id":"{ID}","status":"queued"}}"#).into_bytes(),
            ),
            (
                200,
                "application/json",
                format!(r#"{{"id":"{ID}","status":"in_progress"}}"#).into_bytes(),
            ),
        ])
        .await;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(8),
            run_3d_with_output(
                &reqwest::Client::builder().no_proxy().build().unwrap(),
                &listen,
                ThreeDAction::Wait {
                    id: ID.into(),
                    output: Some(output.clone()),
                    timeout: 3,
                    room: None,
                    force: false,
                },
                &mut stdout,
                &mut stderr,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&stdout).unwrap()["status"],
            "in_progress"
        );
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            format!(
                "note: job {ID} is still in_progress after 3s; run `mistl ai 3d wait {ID}` again\n"
            )
        );
        assert!(!output.exists());
        assert_eq!(server.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn wait_returns_the_failure_reason() {
        const ID: &str = "gen3d_0123456789abcdef0123456789abcdef";
        let (listen, server) = mock(vec![(
            200,
            "application/json",
            br#"{"status":"failed","error":{"message":"out of memory"}}"#.to_vec(),
        )])
        .await;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = run_3d_with_output(
            &reqwest::Client::builder().no_proxy().build().unwrap(),
            &listen,
            ThreeDAction::Wait {
                id: ID.into(),
                output: None,
                timeout: 3,
                room: None,
                force: false,
            },
            &mut stdout,
            &mut stderr,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "3D generation failed: out of memory");
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn wait_timeout_covers_a_stalled_status_request() {
        const ID: &str = "gen3d_0123456789abcdef0123456789abcdef";
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            first.read(&mut buffer).await.unwrap();
            let body = format!(r#"{{"id":"{ID}","status":"running"}}"#);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            first.write_all(response.as_bytes()).await.unwrap();
            let (mut second, _) = listener.accept().await.unwrap();
            second.read(&mut buffer).await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(8),
            run_3d_with_output(
                &reqwest::Client::builder().no_proxy().build().unwrap(),
                &listen,
                ThreeDAction::Wait {
                    id: ID.into(),
                    output: None,
                    timeout: 3,
                    room: None,
                    force: false,
                },
                &mut stdout,
                &mut stderr,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&stdout).unwrap()["status"],
            "running"
        );
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .contains("still running after 3s")
        );
        server.abort();
    }

    #[tokio::test]
    async fn wait_fails_if_no_status_arrives_before_the_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            run_3d_with_output(
                &reqwest::Client::builder().no_proxy().build().unwrap(),
                &listen,
                ThreeDAction::Wait {
                    id: "gen3d_0123456789abcdef0123456789abcdef".into(),
                    output: None,
                    timeout: 1,
                    room: None,
                    force: false,
                },
                &mut stdout,
                &mut stderr,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "timed out after 1s before receiving 3D job status"
        );
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn uploads_transcription_with_named_multipart_fields() {
        let path = temp_path("wav");
        tokio::fs::write(&path, b"WAVE").await.unwrap();
        let (listen, server) = mock(vec![(
            200,
            "application/json",
            br#"{"text":"hello"}"#.to_vec(),
        )])
        .await;
        run_async(
            Action::Transcribe(TranscribeArgs {
                file: path.clone(),
                model: Some("stt-model".into()),
                room: Some("team/one".into()),
            }),
            &listen,
        )
        .await
        .unwrap();
        let requests = server.await.unwrap();
        let request = String::from_utf8_lossy(&requests[0]);
        assert!(request.starts_with("POST /v1/rooms/team%2Fone/audio/transcriptions "));
        for text in [
            "name=\"file\"",
            "filename=\"",
            "audio/wav",
            "WAVE",
            "name=\"model\"",
            "stt-model",
        ] {
            assert!(request.contains(text), "missing {text:?}: {request}");
        }
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn image_edit_posts_multipart_and_writes_binary_for_local_and_room_routes() {
        let image = temp_path("png");
        tokio::fs::write(&image, b"source image").await.unwrap();
        for (room, path, force) in [
            (None, "/v1/images/edits", false),
            (Some("team/one"), "/v1/rooms/team%2Fone/images/edits", true),
        ] {
            let output = temp_path("webp");
            if force {
                tokio::fs::write(&output, b"original").await.unwrap();
            }
            let action = Action::Image(ImageAction::Edit {
                image: image.clone(),
                model: "image-model".into(),
                output: output.clone(),
                param: vec![
                    "scale=2".into(),
                    "background=transparent".into(),
                    "output_format=webp".into(),
                ],
                room: room.map(str::to_owned),
                force,
            });
            validate_action(&action).unwrap();
            let result = b"\x89WEBP\0\xff";
            let (listen, server) = mock(vec![(200, "image/webp", result.to_vec())]).await;
            run_async(action, &listen).await.unwrap();
            let requests = server.await.unwrap();
            let request = String::from_utf8_lossy(&requests[0]);
            assert!(request.starts_with(&format!("POST {path} ")));
            for text in [
                "name=\"image\"; filename=\"",
                "image/png",
                "source image",
                "name=\"model\"\r\n\r\nimage-model\r\n",
                "name=\"scale\"\r\n\r\n2\r\n",
                "name=\"background\"\r\n\r\ntransparent\r\n",
                "name=\"output_format\"\r\n\r\nwebp\r\n",
                "name=\"response_format\"\r\n\r\nbinary\r\n",
            ] {
                assert!(request.contains(text), "missing {text:?}: {request}");
            }
            assert_eq!(request.matches("name=\"response_format\"").count(), 1);
            assert_eq!(tokio::fs::read(&output).await.unwrap(), result);
            tokio::fs::remove_file(output).await.unwrap();
        }
        tokio::fs::remove_file(image).await.unwrap();
    }

    #[tokio::test]
    async fn image_edit_displays_api_error_without_replacing_output() {
        let image = temp_path("png");
        let output = temp_path("png");
        tokio::fs::write(&image, b"source image").await.unwrap();
        tokio::fs::write(&output, b"original").await.unwrap();
        let action = Action::Image(ImageAction::Edit {
            image: image.clone(),
            model: "image-model".into(),
            output: output.clone(),
            param: vec![],
            room: None,
            force: true,
        });
        let (listen, server) = mock(vec![(
            502,
            "application/json",
            br#"{"error":{"message":"image result exceeds the room relay limit","code":"response_too_large"}}"#.to_vec(),
        )])
        .await;
        let error = run_async(action, &listen).await.unwrap_err().to_string();
        assert!(error.contains("502"));
        assert!(error.contains("image result exceeds the room relay limit"));
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"original");
        server.await.unwrap();
        tokio::fs::remove_file(image).await.unwrap();
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn image_edit_accepts_32_mib_and_refuses_larger_upload_before_request() {
        let image = temp_path("png");
        let output = temp_path("png");
        let file = tokio::fs::File::create(&image).await.unwrap();
        file.set_len(MAX_IMAGE_UPLOAD).await.unwrap();
        drop(file);
        drop(file_part(&image, MAX_IMAGE_UPLOAD).await.unwrap());

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&image)
            .await
            .unwrap();
        file.set_len(MAX_IMAGE_UPLOAD + 1).await.unwrap();
        drop(file);
        let error = run_async(
            Action::Image(image_edit(image.clone(), output.clone())),
            "127.0.0.1:1",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("32 MiB"));
        assert!(!output.exists());
        tokio::fs::remove_file(image).await.unwrap();
    }

    #[tokio::test]
    async fn speech_posts_json_and_writes_audio() {
        let output = temp_path("wav");
        let mut args = speech();
        args.output = output.clone();
        let (listen, server) = mock(vec![(200, "audio/wav", b"WAVE".to_vec())]).await;
        run_async(Action::Speech(args), &listen).await.unwrap();
        let requests = server.await.unwrap();
        let request = String::from_utf8_lossy(&requests[0]);
        let (headers, body) = request.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with("POST /v1/audio/speech "));
        assert_eq!(
            serde_json::from_str::<Value>(body).unwrap(),
            json!({"input":"hello"})
        );
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"WAVE");
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn generates_polls_and_streams_content() {
        const ID: &str = "gen3d_0123456789abcdef0123456789abcdef";
        let image = temp_path("png");
        let output = temp_path("glb");
        tokio::fs::write(&image, b"PNG").await.unwrap();
        let (listen, server) = mock(vec![
            (
                200,
                "application/json",
                format!(r#"{{"id":"{ID}","status":"queued"}}"#).into_bytes(),
            ),
            (
                200,
                "application/json",
                format!(r#"{{"id":"{ID}","status":"running"}}"#).into_bytes(),
            ),
            (
                200,
                "application/json",
                format!(r#"{{"id":"{ID}","status":"completed","data":[{{"url":"/content"}}]}}"#)
                    .into_bytes(),
            ),
            (200, "model/gltf-binary", b"GLB bytes".to_vec()),
        ])
        .await;
        run_async(
            Action::ThreeD(ThreeDAction::Generate {
                image: image.clone(),
                model: "3d-model".into(),
                output: Some(output.clone()),
                param: vec!["seed=42".into()],
                no_wait: false,
                timeout: 15,
                room: Some("team/one".into()),
                force: false,
            }),
            &listen,
        )
        .await
        .unwrap();
        let requests = server.await.unwrap();
        let post = String::from_utf8_lossy(&requests[0]);
        assert!(post.starts_with("POST /v1/rooms/team%2Fone/3d/generations "));
        for text in [
            "name=\"image\"",
            "image/png",
            "PNG",
            "name=\"model\"",
            "3d-model",
            "name=\"seed\"",
            "42",
            "name=\"background\"\r\n\r\ntrue\r\n",
        ] {
            assert!(post.contains(text), "missing {text:?}: {post}");
        }
        assert_eq!(post.matches("name=\"background\"").count(), 1);
        for request in &requests[1..=2] {
            assert!(
                String::from_utf8_lossy(request)
                    .starts_with(&format!("GET /v1/rooms/team%2Fone/3d/generations/{ID} "))
            );
        }
        assert!(String::from_utf8_lossy(&requests[3]).starts_with(&format!(
            "GET /v1/rooms/team%2Fone/3d/generations/{ID}/content "
        )));
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"GLB bytes");
        tokio::fs::remove_file(image).await.unwrap();
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn explicit_background_false_is_passed_through() {
        let image = temp_path("png");
        tokio::fs::write(&image, b"PNG").await.unwrap();
        let (listen, server) = mock(vec![(
            200,
            "application/json",
            br#"{"id":"gen3d_0123456789abcdef0123456789abcdef","status":"completed"}"#.to_vec(),
        )])
        .await;
        run_async(
            Action::ThreeD(ThreeDAction::Generate {
                image: image.clone(),
                model: "3d-model".into(),
                output: None,
                param: vec!["background=false".into()],
                no_wait: false,
                timeout: 10,
                room: None,
                force: false,
            }),
            &listen,
        )
        .await
        .unwrap();
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        let post = String::from_utf8_lossy(&requests[0]);
        assert!(post.starts_with("POST /v1/3d/generations "));
        assert!(post.contains("name=\"background\"\r\n\r\nfalse\r\n"));
        assert_eq!(post.matches("name=\"background\"").count(), 1);
        tokio::fs::remove_file(image).await.unwrap();
    }

    #[tokio::test]
    async fn displays_http_status_and_api_error() {
        let (listen, server) = mock(vec![(
            404,
            "application/json",
            br#"{"error":{"message":"unknown model"}}"#.to_vec(),
        )])
        .await;
        let error = run_async(Action::Voices(VoicesArgs { room: None }), &listen)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("404"));
        assert!(error.contains("unknown model"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn no_wait_returns_a_pending_job_without_downloading() {
        let image = temp_path("png");
        let output = temp_path("glb");
        tokio::fs::write(&image, b"PNG").await.unwrap();
        let (listen, server) = mock(vec![(
            200,
            "application/json",
            br#"{"id":"gen3d_0123456789abcdef0123456789abcdef","status":"running"}"#.to_vec(),
        )])
        .await;
        run_async(
            Action::ThreeD(ThreeDAction::Generate {
                image: image.clone(),
                model: "3d-model".into(),
                output: Some(output.clone()),
                param: vec![],
                no_wait: true,
                timeout: 10,
                room: None,
                force: false,
            }),
            &listen,
        )
        .await
        .unwrap();
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        let request = String::from_utf8_lossy(&requests[0]);
        assert!(request.contains("name=\"background\"\r\n\r\ntrue\r\n"));
        assert!(!output.exists());
        tokio::fs::remove_file(image).await.unwrap();
    }

    #[tokio::test]
    async fn no_wait_ignores_output_even_when_the_job_is_already_completed() {
        let image = temp_path("png");
        let output = temp_path("glb");
        tokio::fs::write(&image, b"PNG").await.unwrap();
        tokio::fs::write(&output, b"original").await.unwrap();
        let (listen, server) = mock(vec![(
            200,
            "application/json",
            br#"{"id":"gen3d_0123456789abcdef0123456789abcdef","status":"completed"}"#.to_vec(),
        )])
        .await;
        let action = Action::ThreeD(ThreeDAction::Generate {
            image: image.clone(),
            model: "3d-model".into(),
            output: Some(output.clone()),
            param: vec![],
            no_wait: true,
            timeout: 10,
            room: None,
            force: false,
        });
        validate_action(&action).unwrap();
        run_async(action, &listen).await.unwrap();
        assert_eq!(server.await.unwrap().len(), 1);
        assert_eq!(tokio::fs::read(&output).await.unwrap(), b"original");
        tokio::fs::remove_file(image).await.unwrap();
        tokio::fs::remove_file(output).await.unwrap();
    }

    #[tokio::test]
    async fn generation_timeout_covers_polling() {
        let image = temp_path("png");
        tokio::fs::write(&image, b"PNG").await.unwrap();
        let (listen, server) = mock(vec![(
            200,
            "application/json",
            br#"{"id":"gen3d_0123456789abcdef0123456789abcdef","status":"running"}"#.to_vec(),
        )])
        .await;
        let error = run_async(
            Action::ThreeD(ThreeDAction::Generate {
                image: image.clone(),
                model: "3d-model".into(),
                output: None,
                param: vec![],
                no_wait: false,
                timeout: 1,
                room: None,
                force: false,
            }),
            &listen,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("timed out after 1 seconds"));
        assert_eq!(server.await.unwrap().len(), 1);
        tokio::fs::remove_file(image).await.unwrap();
    }

    #[tokio::test]
    async fn output_race_and_failed_download_preserve_existing_file() {
        let path = temp_path("wav");
        tokio::fs::write(&path, b"original").await.unwrap();
        let (listen, server) = mock(vec![(200, "audio/wav", b"new audio".to_vec())]).await;
        let response = reqwest::get(format!("http://{listen}/")).await.unwrap();
        assert!(write_response(response, &path, false).await.is_err());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"original");
        server.await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            stream.read(&mut buffer).await.unwrap();
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort").await.unwrap();
        });
        let response = reqwest::get(format!("http://{listen}/")).await.unwrap();
        assert!(write_response(response, &path, true).await.is_err());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"original");
        server.await.unwrap();

        let (listen, server) = mock(vec![(200, "audio/wav", b"replacement".to_vec())]).await;
        let response = reqwest::get(format!("http://{listen}/")).await.unwrap();
        let summary = write_response(response, &path, true).await.unwrap();
        assert_eq!(summary["bytes"], 11);
        assert_eq!(summary["content_type"], "audio/wav");
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"replacement");
        server.await.unwrap();
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn truncated_download_reports_progress_and_removes_temporary_file() {
        let directory = temp_path("dir");
        tokio::fs::create_dir(&directory).await.unwrap();
        let path = directory.join("download.bin");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            stream.read(&mut buffer).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort",
                )
                .await
                .unwrap();
        });
        let response = reqwest::get(format!("http://{listen}/")).await.unwrap();
        let error = write_response(response, &path, false).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("download interrupted after 5 of 100 bytes"));
        assert!(message.contains("daemon log"));
        assert!(message.contains("retry"));
        assert!(!path.exists());
        assert!(
            tokio::fs::read_dir(&directory)
                .await
                .unwrap()
                .next_entry()
                .await
                .unwrap()
                .is_none()
        );
        server.await.unwrap();
        tokio::fs::remove_dir(directory).await.unwrap();
    }

    #[tokio::test]
    async fn transcription_checks_the_full_multipart_limit() {
        let path = temp_path("wav");
        let file = tokio::fs::File::create(&path).await.unwrap();
        file.set_len(MAX_TRANSCRIPTION_BODY).await.unwrap();
        drop(file);
        let error = run_async(
            Action::Transcribe(TranscribeArgs {
                file: path.clone(),
                model: None,
                room: None,
            }),
            "127.0.0.1:1",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("10 MiB"));
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn empty_delete_response_is_accepted() {
        let (listen, server) = mock(vec![(204, "application/json", vec![])]).await;
        run_async(
            Action::ThreeD(ThreeDAction::Rm {
                id: "gen3d_0123456789abcdef0123456789abcdef".into(),
                room: None,
            }),
            &listen,
        )
        .await
        .unwrap();
        let requests = server.await.unwrap();
        assert!(String::from_utf8_lossy(&requests[0]).starts_with("DELETE /v1/3d/generations/"));
    }

    #[tokio::test]
    async fn refuses_oversized_upload_before_request() {
        let path = temp_path("wav");
        let file = tokio::fs::File::create(&path).await.unwrap();
        file.set_len(MAX_UPLOAD + 1).await.unwrap();
        drop(file);
        let error = file_part(&path, MAX_UPLOAD)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("20 MiB"));
        tokio::fs::remove_file(path).await.unwrap();
    }
}
