//! CLI client for the local AI API's audio and 3D routes.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Subcommand};
use reqwest::multipart::{Form, Part};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_UPLOAD: u64 = 20 * 1024 * 1024;
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

async fn file_part(path: &Path) -> Result<Part> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening upload {}", path.display()))?;
    if file.metadata().await?.len() > MAX_UPLOAD {
        bail!("upload {} exceeds the 20 MiB limit", path.display());
    }
    let mut bytes = Vec::new();
    file.take(MAX_UPLOAD + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > MAX_UPLOAD {
        bail!("upload {} exceeds the 20 MiB limit", path.display());
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

fn job_status(job: &Value) -> Result<JobStatus> {
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
        None if job.pointer("/data/0/url").and_then(Value::as_str).is_some() => {
            Ok(JobStatus::Completed)
        }
        Some(status) => bail!("unknown 3D job status: {status}"),
        None => bail!("3D response is missing a job status"),
    }
}

fn job_id(job: &Value) -> Result<&str> {
    let id = job
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("3D response is missing a generation id"))?;
    generation_path(id)?;
    Ok(id)
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
    // Stage alongside the destination so failed downloads never truncate it.
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temp = parent.join(format!(".mistl-media-{}.tmp", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .await
        .with_context(|| format!("creating temporary output for {}", path.display()))?;
    let result: Result<u64> = async {
        let mut bytes = 0u64;
        while let Some(chunk) = response.chunk().await.context("reading download")? {
            file.write_all(&chunk).await.context("writing download")?;
            bytes += chunk.len() as u64;
        }
        file.flush().await?;
        drop(file);
        if force {
            tokio::fs::rename(&temp, path)
                .await
                .with_context(|| format!("replacing output {}", path.display()))?;
        } else {
            tokio::fs::hard_link(&temp, path).await.with_context(|| {
                format!(
                    "creating output {} (use --force to replace an existing file)",
                    path.display()
                )
            })?;
        }
        Ok(bytes)
    }
    .await;
    let _ = tokio::fs::remove_file(&temp).await;
    let bytes = result?;
    Ok(json!({"path": path.to_string_lossy(), "bytes": bytes, "content_type": content_type}))
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
            let file = file_part(&args.file).await?;
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
        Action::ThreeD(action) => run_3d(&client, listen, action).await?,
    }
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

async fn poll_3d_job(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    job: &mut Value,
) -> Result<()> {
    let mut delay = Duration::from_secs(2);
    loop {
        match job_status(job)? {
            JobStatus::Completed => return Ok(()),
            JobStatus::Failed(reason) => bail!("3D generation failed: {reason}"),
            JobStatus::Pending => {
                tokio::time::sleep(delay).await;
                *job = response_json(client.get(format!("{base}{path}")).send().await?).await?;
                delay = (delay * 2).min(Duration::from_secs(10));
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
            let image = file_part(&image).await?;
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
                match job_status(&job)? {
                    JobStatus::Failed(reason) => bail!("3D generation failed: {reason}"),
                    JobStatus::Pending if !no_wait => {
                        let path = generation_path(job_id(&job)?)?;
                        poll_3d_job(client, &base, &path, &mut job).await?;
                    }
                    _ => {}
                }
                Ok::<_, anyhow::Error>(job)
            })
            .await
            .map_err(|_| anyhow!("3D generation timed out after {timeout} seconds"))??;
            if !no_wait
                && job_status(&result)? == JobStatus::Completed
                && let Some(path) = output
            {
                let route = generation_path(job_id(&result)?)?;
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
                poll_3d_job(client, &base, &path, latest.as_mut().unwrap()).await
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
            job_status(&json!({"status":"queued"})).unwrap(),
            JobStatus::Pending
        );
        assert_eq!(
            job_status(&json!({"status":"running"})).unwrap(),
            JobStatus::Pending
        );
        assert_eq!(
            job_status(&json!({"status":"completed"})).unwrap(),
            JobStatus::Completed
        );
        assert_eq!(
            job_status(&json!({"data":[{"url":"/content"}]})).unwrap(),
            JobStatus::Completed
        );
        assert_eq!(
            job_status(&json!({"status":"failed","error":{"message":"out of memory"}})).unwrap(),
            JobStatus::Failed("out of memory".into())
        );
        assert!(job_status(&json!({"status":"mystery"})).is_err());
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
        let error = file_part(&path).await.err().unwrap().to_string();
        assert!(error.contains("20 MiB"));
        tokio::fs::remove_file(path).await.unwrap();
    }
}
