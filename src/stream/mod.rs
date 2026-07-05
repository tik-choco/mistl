//! Stream: screen capture -> H264 -> built-in RTSP server, playable by
//! VRChat video players (AVPro). Port of mistlink's Go pipeline:
//! ffmpeg (gdigrab) -> MPEG-TS over local UDP -> H264 NAL extraction ->
//! RTP packetization -> RTSP server with dummy SPS/PPS keepalive.

mod capture;
mod ingest;
mod rtp_out;
mod rtsp;

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::warn;

use crate::daemon::AppState;

/// The running pipeline: ffmpeg capture child, the ingest task feeding it
/// into the RTSP server, and the RTSP server itself.
struct Pipeline {
    capture: capture::Capture,
    ingest_task: tokio::task::JoinHandle<()>,
    rtsp: Arc<rtsp::RtspServer>,
    rtsp_url: String,
    started_at: Instant,
}

/// Module-internal state: at most one pipeline runs at a time.
static PIPELINE: OnceLock<Mutex<Option<Pipeline>>> = OnceLock::new();

fn pipeline() -> &'static Mutex<Option<Pipeline>> {
    PIPELINE.get_or_init(|| Mutex::new(None))
}

/// Handle `stream.*` IPC commands:
/// - `stream.start` `{}` -> `{rtsp_url}` (idempotent: returns existing URL)
/// - `stream.stop` `{}` -> `{stopped: bool}`
/// - `stream.status` `{}` -> `{running, rtsp_url?, clients?}`
pub async fn handle(cmd: &str, _args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "stream.start" => start(state).await,
        "stream.stop" => stop().await,
        "stream.status" => status().await,
        _ => bail!("`{cmd}` is not implemented yet"),
    }
}

async fn start(state: &Arc<AppState>) -> Result<Value> {
    let mut guard = pipeline().lock().await;
    if let Some(existing) = guard.as_ref() {
        return Ok(json!({ "rtsp_url": existing.rtsp_url }));
    }

    let cfg = &state.config.stream;

    let parsed = rtsp_types::Url::parse(&cfg.rtsp_url).context("invalid stream.rtsp_url")?;
    let host = parsed.host_str().unwrap_or("127.0.0.1").to_string();
    let port = parsed.port().unwrap_or(8554);
    let path = if parsed.path().is_empty() {
        "/stream".to_string()
    } else {
        parsed.path().to_string()
    };

    let bind_ip: IpAddr = host.parse().unwrap_or_else(|_| "127.0.0.1".parse().expect("valid IP"));
    let bind_addr = SocketAddr::new(bind_ip, port);

    // Bind the ingest UDP socket *before* spawning ffmpeg, so we own the
    // port ffmpeg is told to send to (no "guess a free port and hope"
    // race).
    let ingest_socket = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .context("binding stream ingest UDP socket")?;
    let ingest_port = ingest_socket.local_addr()?.port();

    let capture = capture::Capture::spawn(cfg.frame_rate, ingest_port, cfg.audio_capture).await?;

    let rtsp = match rtsp::RtspServer::start(bind_addr, cfg.frame_rate).await {
        Ok(rtsp) => rtsp,
        Err(error) => {
            capture.stop().await;
            return Err(error).context("starting RTSP server");
        }
    };

    let rtsp_for_ingest = rtsp.clone();
    let ingest_task = tokio::spawn(async move {
        if let Err(error) = ingest::run(ingest_socket, rtsp_for_ingest).await {
            warn!(%error, "stream ingest task ended");
        }
    });

    let advertise_host = if bind_ip.is_unspecified() {
        lan_ip().map(|ip| ip.to_string()).unwrap_or_else(|| "127.0.0.1".to_string())
    } else {
        host
    };
    let rtsp_url = format!("rtsp://{advertise_host}:{port}{path}");

    *guard = Some(Pipeline {
        capture,
        ingest_task,
        rtsp,
        rtsp_url: rtsp_url.clone(),
        started_at: Instant::now(),
    });

    Ok(json!({ "rtsp_url": rtsp_url }))
}

async fn stop() -> Result<Value> {
    let mut guard = pipeline().lock().await;
    match guard.take() {
        Some(pipeline) => {
            pipeline.ingest_task.abort();
            pipeline.rtsp.stop().await;
            pipeline.capture.stop().await;
            Ok(json!({ "stopped": true }))
        }
        None => Ok(json!({ "stopped": false })),
    }
}

async fn status() -> Result<Value> {
    let guard = pipeline().lock().await;
    match guard.as_ref() {
        Some(pipeline) => Ok(json!({
            "running": true,
            "rtsp_url": pipeline.rtsp_url,
            "clients": pipeline.rtsp.client_count().await,
            "uptime_secs": pipeline.started_at.elapsed().as_secs(),
        })),
        None => Ok(json!({ "running": false })),
    }
}

/// Best-effort LAN IP discovery (used when `stream.rtsp_url` is configured
/// with a `0.0.0.0` host, so the URL handed back to the caller is one a
/// VRChat client on the LAN can actually reach). Doesn't require any
/// network access: connecting a UDP socket only consults the routing table.
fn lan_ip() -> Option<IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    socket.local_addr().ok().map(|addr| addr.ip())
}
