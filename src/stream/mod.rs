//! Stream: screen capture -> H264 -> built-in RTSP server, playable by
//! VRChat video players (AVPro). Two capture backends are selectable via
//! `stream.capture_backend`:
//!
//! - `"native"` (default): in-process capture and encode, no external
//!   tools. `native.rs` grabs BGRA frames off the primary monitor with
//!   Windows Graphics Capture (the `windows-capture` crate), converts them
//!   to I420, encodes with OpenH264, and hands the resulting Annex-B access
//!   units directly to the RTSP server below -- mirroring mistlink's Go
//!   pipeline (GDI capture + OpenH264 via `pion/mediadevices`), but without
//!   MPEG-TS or a UDP hop in between. Windows-only; `stream.start` returns a
//!   clear error on other platforms.
//! - `"ffmpeg"`: the pre-v0.2 pipeline -- ffmpeg (gdigrab) -> MPEG-TS over
//!   local UDP -> H264 NAL extraction -> RTP packetization -> RTSP server.
//!   Requires `ffmpeg` on `PATH`.
//!
//! Both backends feed the same [`rtsp::RtspServer`], which packetizes into
//! RTP and serves it with a dummy SPS/PPS/NALU keepalive while no real video
//! is flowing yet.

mod capture;
mod ingest;
#[cfg(windows)]
mod native;
mod relay;
mod rtp_out;
mod rtsp;
mod selftest;

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::warn;

use crate::config::StreamConfig;
use crate::daemon::AppState;

/// Which capture backend is driving a running [`Pipeline`], selected via
/// `stream.capture_backend`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaptureBackend {
    Native,
    Ffmpeg,
    /// p2p relay of a tc-chat screen share (selected by `stream.relay.start`,
    /// not by `stream.capture_backend`).
    Relay,
    /// Synthetic local test feed (selected by `stream.selftest.start`): a
    /// moving H264 pattern plus an optional tone, for verifying the
    /// RTSP/AVPro serving path (multi-viewer fan-out, the audio track) end to
    /// end without the p2p leg the real relay needs.
    SelfTest,
}

impl CaptureBackend {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "native" => Ok(Self::Native),
            "ffmpeg" => Ok(Self::Ffmpeg),
            other => {
                bail!("invalid stream.capture_backend {other:?}; valid values: \"native\", \"ffmpeg\"")
            }
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Ffmpeg => "ffmpeg",
            Self::Relay => "relay",
            Self::SelfTest => "selftest",
        }
    }
}

/// The running capture backend: either the ffmpeg child process plus its
/// MPEG-TS ingest task, or the in-process native capture/encode pipeline.
enum Backend {
    Ffmpeg {
        capture: capture::Capture,
        ingest_task: tokio::task::JoinHandle<()>,
    },
    #[cfg(windows)]
    Native(native::NativeCapture),
    Relay(relay::RelayCapture),
    /// The synthetic self-test feed task (see [`selftest::run`]); stopping it
    /// is just aborting that task, which drops its own generator tasks.
    SelfTest(tokio::task::JoinHandle<()>),
}

impl Backend {
    /// Stops whichever backend is running. For ffmpeg this aborts the
    /// ingest task and kills the child process; for native this aborts the
    /// forward task and stops the Windows Graphics Capture thread.
    async fn stop(self) {
        match self {
            Backend::Ffmpeg { capture, ingest_task } => {
                ingest_task.abort();
                capture.stop().await;
            }
            #[cfg(windows)]
            Backend::Native(native) => native.stop().await,
            Backend::Relay(relay) => relay.stop().await,
            Backend::SelfTest(task) => task.abort(),
        }
    }
}

/// The running pipeline: the capture backend and the RTSP server it feeds.
struct Pipeline {
    backend: Backend,
    backend_kind: CaptureBackend,
    rtsp: Arc<rtsp::RtspServer>,
    rtsp_url: String,
    started_at: Instant,
    relay_room: Option<String>,
}

/// Module-internal state: at most one pipeline runs at a time.
static PIPELINE: OnceLock<Mutex<Option<Pipeline>>> = OnceLock::new();

fn pipeline() -> &'static Mutex<Option<Pipeline>> {
    PIPELINE.get_or_init(|| Mutex::new(None))
}

/// Handle `stream.*` IPC commands:
/// - `stream.start` `{}` -> `{rtsp_url}` (idempotent: returns existing URL)
/// - `stream.relay.start` `{room?}` -> `{rtsp_url, room}` (tc-chat share relay)
/// - `stream.stop` `{}` -> `{stopped: bool}`
/// - `stream.status` `{}` -> `{running, rtsp_url?, clients?, backend?}`
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "stream.start" => {
            let backend = CaptureBackend::parse(&state.config().stream.capture_backend)?;
            start(state, backend, None, None).await
        }
        "stream.relay.start" => {
            let room = args
                .get("room")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| state.config().stream.relay_room.clone());
            let Some(room) = room else {
                bail!(
                    "no relay room: pass --room or set stream.relay_room to the \
                     tc-chat room id of the screen share"
                );
            };
            start(state, CaptureBackend::Relay, Some(room), None).await
        }
        "stream.selftest.start" => {
            let opts = parse_selftest_opts(&args)?;
            start(state, CaptureBackend::SelfTest, None, Some(opts)).await
        }
        "stream.stop" => stop().await,
        "stream.status" => status().await,
        _ => bail!("`{cmd}` is not implemented yet"),
    }
}

/// Parse the `stream.selftest.start` arguments into [`selftest::SelfTestOpts`].
/// Audio defaults to AAC (the VRChat codec) so the two-track path is exercised
/// by default; `audio: "none"` makes it video-only. Width/height/fps/seconds
/// fall back to the [`selftest::SelfTestOpts::default`] values when absent.
fn parse_selftest_opts(args: &Value) -> Result<selftest::SelfTestOpts> {
    let defaults = selftest::SelfTestOpts::default();
    let audio = match args.get("audio").and_then(Value::as_str) {
        None | Some("aac") => Some(rtp_out::AudioCodec::Aac),
        Some("opus") => Some(rtp_out::AudioCodec::Opus),
        Some("none") => None,
        Some(other) => bail!("invalid selftest audio {other:?}; valid values: \"aac\", \"opus\", \"none\""),
    };
    let as_u32 = |key: &str, fallback: u32| args.get(key).and_then(Value::as_u64).map_or(fallback, |v| v as u32);
    Ok(selftest::SelfTestOpts {
        width: as_u32("width", defaults.width),
        height: as_u32("height", defaults.height),
        frame_rate: as_u32("fps", defaults.frame_rate),
        audio,
        duration: args.get("seconds").and_then(Value::as_u64).map(Duration::from_secs),
    })
}

async fn start(
    state: &Arc<AppState>,
    backend_kind: CaptureBackend,
    relay_room: Option<String>,
    selftest_opts: Option<selftest::SelfTestOpts>,
) -> Result<Value> {
    let mut guard = pipeline().lock().await;
    if let Some(existing) = guard.as_ref() {
        if existing.backend_kind != backend_kind {
            bail!(
                "stream already running with backend {:?}; run `stream stop` first",
                existing.backend_kind.as_str()
            );
        }
        return Ok(json!({ "rtsp_url": existing.rtsp_url }));
    }

    let cfg = &state.config().stream;
    // Relayed shares carry audio; the self-test feed carries whatever audio
    // codec its options asked for (so the two-track path is exercised); local
    // capture stays video-only for now.
    let audio = match backend_kind {
        CaptureBackend::Relay => Some(rtp_out::AudioCodec::parse(&cfg.audio_codec)?),
        CaptureBackend::SelfTest => selftest_opts.as_ref().and_then(|o| o.audio),
        _ => None,
    };

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

    let rtsp = rtsp::RtspServer::start(bind_addr, cfg.frame_rate, audio)
        .await
        .context("starting RTSP server")?;

    let backend = match backend_kind {
        CaptureBackend::Ffmpeg => start_ffmpeg_backend(cfg, &rtsp).await,
        CaptureBackend::Native => start_native_backend(cfg, &rtsp).await,
        CaptureBackend::Relay => {
            let room = relay_room.clone().expect("relay backend requires a room");
            let codec = audio.expect("relay backend always has an audio codec");
            relay::RelayCapture::spawn(state, room, codec, rtsp.clone())
                .await
                .map(Backend::Relay)
        }
        CaptureBackend::SelfTest => {
            let opts = selftest_opts.expect("selftest backend requires opts");
            let rtsp_for_selftest = rtsp.clone();
            let handle = tokio::spawn(async move {
                if let Err(error) = selftest::run(rtsp_for_selftest, opts).await {
                    warn!(%error, "stream selftest feed ended with error");
                }
            });
            Ok(Backend::SelfTest(handle))
        }
    };

    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => {
            rtsp.stop().await;
            return Err(error);
        }
    };

    let advertise_host = if bind_ip.is_unspecified() {
        lan_ip().map(|ip| ip.to_string()).unwrap_or_else(|| "127.0.0.1".to_string())
    } else {
        host
    };
    let rtsp_url = format!("rtsp://{advertise_host}:{port}{path}");

    let response = match &relay_room {
        Some(room) => json!({ "rtsp_url": rtsp_url, "room": room }),
        None => json!({ "rtsp_url": rtsp_url }),
    };

    *guard = Some(Pipeline {
        backend,
        backend_kind,
        rtsp,
        rtsp_url: rtsp_url.clone(),
        started_at: Instant::now(),
        relay_room,
    });

    Ok(response)
}

/// Binds the ffmpeg ingest UDP socket *before* spawning ffmpeg, so we own
/// the port ffmpeg is told to send to (no "guess a free port and hope"
/// race), spawns ffmpeg, and starts the MPEG-TS ingest task feeding its
/// output into `rtsp`.
async fn start_ffmpeg_backend(cfg: &StreamConfig, rtsp: &Arc<rtsp::RtspServer>) -> Result<Backend> {
    let ingest_socket = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .context("binding stream ingest UDP socket")?;
    let ingest_port = ingest_socket.local_addr()?.port();

    let capture = capture::Capture::spawn(cfg.frame_rate, ingest_port, cfg.audio_capture).await?;

    let rtsp_for_ingest = rtsp.clone();
    let ingest_task = tokio::spawn(async move {
        if let Err(error) = ingest::run(ingest_socket, rtsp_for_ingest).await {
            warn!(%error, "stream ingest task ended");
        }
    });

    Ok(Backend::Ffmpeg { capture, ingest_task })
}

#[cfg(windows)]
async fn start_native_backend(cfg: &StreamConfig, rtsp: &Arc<rtsp::RtspServer>) -> Result<Backend> {
    native::NativeCapture::spawn(cfg.frame_rate, cfg.max_width, rtsp.clone())
        .await
        .map(Backend::Native)
}

#[cfg(not(windows))]
async fn start_native_backend(_cfg: &StreamConfig, _rtsp: &Arc<rtsp::RtspServer>) -> Result<Backend> {
    bail!(
        "stream.capture_backend = \"native\" is only supported on Windows; set \
         stream.capture_backend = \"ffmpeg\" on this platform"
    )
}

async fn stop() -> Result<Value> {
    let mut guard = pipeline().lock().await;
    match guard.take() {
        Some(pipeline) => {
            pipeline.backend.stop().await;
            pipeline.rtsp.stop().await;
            Ok(json!({ "stopped": true }))
        }
        None => Ok(json!({ "stopped": false })),
    }
}

async fn status() -> Result<Value> {
    let guard = pipeline().lock().await;
    match guard.as_ref() {
        Some(pipeline) => {
            let mut value = json!({
                "running": true,
                "rtsp_url": pipeline.rtsp_url,
                "clients": pipeline.rtsp.client_count().await,
                "uptime_secs": pipeline.started_at.elapsed().as_secs(),
                "backend": pipeline.backend_kind.as_str(),
            });
            if let Some(room) = &pipeline.relay_room {
                value["room"] = json!(room);
                if let Backend::Relay(relay) = &pipeline.backend {
                    value["publisher"] = json!(relay.publisher());
                }
            }
            Ok(value)
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_backend_names() {
        assert_eq!(CaptureBackend::parse("native").unwrap(), CaptureBackend::Native);
        assert_eq!(CaptureBackend::parse("ffmpeg").unwrap(), CaptureBackend::Ffmpeg);
    }

    #[test]
    fn rejects_unknown_backend_name() {
        let message = CaptureBackend::parse("obs").unwrap_err().to_string();
        assert!(message.contains("obs"), "{message}");
        assert!(message.contains("native"), "{message}");
        assert!(message.contains("ffmpeg"), "{message}");
    }

    #[test]
    fn as_str_roundtrips_through_parse() {
        for backend in [CaptureBackend::Native, CaptureBackend::Ffmpeg] {
            assert_eq!(CaptureBackend::parse(backend.as_str()).unwrap(), backend);
        }
    }
}
