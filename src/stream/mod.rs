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
//!
//! Giving `stream.start` an explicit room layers a third thing on top of
//! the same local RTSP serving: it also publishes the capture into that
//! mistlib room, reusing the native backend's pipeline regardless of
//! `stream.capture_backend` (Windows only) -- see `share.rs`'s module doc.
//! Unlike `relay`/`share`, `start` never falls back to the `stream.room`
//! config default for this -- it's the generic entry point, so staying
//! local has to be the outcome whenever nothing was asked for explicitly.

mod capture;
mod ingest;
#[cfg(windows)]
mod native;
mod relay;
mod rtp_out;
mod rtsp;
mod selftest;
#[cfg(windows)]
mod share;

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
    /// Publish this machine's own screen capture into a mistlib room
    /// (selected by `stream.share.start`), so any consensus-elected relay in
    /// that room picks it up like a tc-chat share -- see `share.rs`'s module
    /// doc.
    Share,
}

impl CaptureBackend {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "native" => Ok(Self::Native),
            "ffmpeg" => Ok(Self::Ffmpeg),
            other => {
                bail!(
                    "invalid stream.capture_backend {other:?}; valid values: \"native\", \"ffmpeg\""
                )
            }
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Ffmpeg => "ffmpeg",
            Self::Relay => "relay",
            Self::SelfTest => "selftest",
            Self::Share => "share",
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
    #[cfg(windows)]
    Share(share::ShareCapture),
}

impl Backend {
    /// Stops whichever backend is running. For ffmpeg this aborts the
    /// ingest task and kills the child process; for native this aborts the
    /// forward task and stops the Windows Graphics Capture thread.
    async fn stop(self) {
        match self {
            Backend::Ffmpeg {
                capture,
                ingest_task,
            } => {
                ingest_task.abort();
                capture.stop().await;
            }
            #[cfg(windows)]
            Backend::Native(native) => native.stop().await,
            Backend::Relay(relay) => relay.stop().await,
            Backend::SelfTest(task) => task.abort(),
            #[cfg(windows)]
            Backend::Share(share) => share.stop().await,
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
    /// The room this pipeline's backend joined, if it joined one at all
    /// (`Relay` and `Share`; `Native`/`Ffmpeg`/`SelfTest` have none).
    room: Option<String>,
}

/// Module-internal state: at most one pipeline runs at a time.
static PIPELINE: OnceLock<Mutex<Option<Pipeline>>> = OnceLock::new();

fn pipeline() -> &'static Mutex<Option<Pipeline>> {
    PIPELINE.get_or_init(|| Mutex::new(None))
}

/// Handle `stream.*` IPC commands:
/// - `stream.start` `{room?}` -> `{rtsp_url, room?}` (local screen capture,
///   idempotent: returns the existing URL if already running. Deliberately
///   does *not* fall back to `stream.room` like `relay`/`share` below do --
///   this is the generic entry point, so an explicit `room` is required to
///   opt into publishing; omit it (there is no config-driven way to) and it
///   always stays purely local, using whichever backend
///   `stream.capture_backend` names. Given a room, it also publishes the
///   capture into that mistlib room via the same native-capture pipeline
///   `stream.share.start` below uses, so any consensus-elected relay in
///   that room -- or a direct viewer -- can pick it up. See `share.rs`'s
///   module doc, especially its "Loopback" section)
/// - `stream.relay.start` `{room?}` -> `{rtsp_url, room}` (tc-chat share relay)
/// - `stream.share.start` `{room?}` -> `{rtsp_url, room}` (back-compat alias
///   for `stream.start` that, unlike it, falls back to `stream.room` and
///   errors instead of starting local-only when neither is given)
/// - `stream.share.stop` `{}` -> `{stopped: bool}` (identical to `stream.stop`;
///   named separately so a share-specific caller doesn't need to know it
///   shares the single-pipeline slot with every other backend)
/// - `stream.stop` `{}` -> `{stopped: bool}`
/// - `stream.status` `{}` -> `{running, rtsp_url?, clients?, backend?}` (the
///   `relay` backend additionally reports `publisher` and `cascade` -- see
///   `relay`'s module doc's "Cascade distribution" section for the latter's
///   shape; a `share` field -- `{active, room?, track_id?, ...}` -- is always
///   present so a caller can check it regardless of what else is running)
pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    match cmd {
        "stream.start" => {
            let room = args.get("room").and_then(Value::as_str).map(str::to_string);
            match room {
                Some(room) => start(state, CaptureBackend::Share, Some(room), None).await,
                None => {
                    let backend = CaptureBackend::parse(&state.config().stream.capture_backend)?;
                    start(state, backend, None, None).await
                }
            }
        }
        "stream.relay.start" => {
            let room = args
                .get("room")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| state.config().stream.room.clone());
            let Some(room) = room else {
                bail!(
                    "no relay room: pass --room or set stream.room to the \
                     tc-chat room id of the screen share"
                );
            };
            start(state, CaptureBackend::Relay, Some(room), None).await
        }
        "stream.selftest.start" => {
            let opts = parse_selftest_opts(&args)?;
            start(state, CaptureBackend::SelfTest, None, Some(opts)).await
        }
        "stream.share.start" => {
            let room = args
                .get("room")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| state.config().stream.room.clone());
            let Some(room) = room else {
                bail!("share requires a room: pass --room or set stream.room");
            };
            start(state, CaptureBackend::Share, Some(room), None).await
        }
        "stream.share.stop" => stop().await,
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
        Some(other) => {
            bail!("invalid selftest audio {other:?}; valid values: \"aac\", \"opus\", \"none\"")
        }
    };
    let as_u32 = |key: &str, fallback: u32| {
        args.get(key)
            .and_then(Value::as_u64)
            .map_or(fallback, |v| v as u32)
    };
    Ok(selftest::SelfTestOpts {
        width: as_u32("width", defaults.width),
        height: as_u32("height", defaults.height),
        frame_rate: as_u32("fps", defaults.frame_rate),
        audio,
        duration: args
            .get("seconds")
            .and_then(Value::as_u64)
            .map(Duration::from_secs),
    })
}

async fn start(
    state: &Arc<AppState>,
    backend_kind: CaptureBackend,
    room: Option<String>,
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
        if backend_kind == CaptureBackend::Share {
            bail!("already sharing; run `stream stop` (or `stream.share.stop`) first");
        }
        return Ok(json!({ "rtsp_url": existing.rtsp_url }));
    }

    let cfg = &state.config().stream;
    // Relayed shares carry audio; the self-test feed carries whatever audio
    // codec its options asked for (so the two-track path is exercised); local
    // capture (including a share, for now -- see share.rs's "Audio" section)
    // stays video-only.
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

    let bind_ip: IpAddr = host
        .parse()
        .unwrap_or_else(|_| "127.0.0.1".parse().expect("valid IP"));
    let bind_addr = SocketAddr::new(bind_ip, port);

    let rtsp = rtsp::RtspServer::start(bind_addr, cfg.frame_rate, audio)
        .await
        .context("starting RTSP server")?;

    let backend = match backend_kind {
        CaptureBackend::Ffmpeg => start_ffmpeg_backend(cfg, &rtsp).await,
        CaptureBackend::Native => start_native_backend(cfg, &rtsp).await,
        CaptureBackend::Relay => {
            let room = room.clone().expect("relay backend requires a room");
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
        CaptureBackend::Share => {
            let room = room.clone().expect("share backend requires a room");
            start_share_backend(state, cfg, room, &rtsp).await
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
        lan_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "127.0.0.1".to_string())
    } else {
        host
    };
    let rtsp_url = format!("rtsp://{advertise_host}:{port}{path}");

    let response = match &room {
        Some(room) => json!({ "rtsp_url": rtsp_url, "room": room }),
        None => json!({ "rtsp_url": rtsp_url }),
    };

    *guard = Some(Pipeline {
        backend,
        backend_kind,
        rtsp,
        rtsp_url: rtsp_url.clone(),
        started_at: Instant::now(),
        room,
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

    Ok(Backend::Ffmpeg {
        capture,
        ingest_task,
    })
}

#[cfg(windows)]
async fn start_native_backend(cfg: &StreamConfig, rtsp: &Arc<rtsp::RtspServer>) -> Result<Backend> {
    native::NativeCapture::spawn(cfg.frame_rate, cfg.max_width, rtsp.clone(), None)
        .await
        .map(Backend::Native)
}

#[cfg(not(windows))]
async fn start_native_backend(
    _cfg: &StreamConfig,
    _rtsp: &Arc<rtsp::RtspServer>,
) -> Result<Backend> {
    bail!(
        "stream.capture_backend = \"native\" is only supported on Windows; set \
         stream.capture_backend = \"ffmpeg\" on this platform"
    )
}

#[cfg(windows)]
async fn start_share_backend(
    state: &Arc<AppState>,
    cfg: &StreamConfig,
    room: String,
    rtsp: &Arc<rtsp::RtspServer>,
) -> Result<Backend> {
    share::ShareCapture::spawn(state, room, cfg.frame_rate, cfg.max_width, rtsp.clone())
        .await
        .map(Backend::Share)
}

#[cfg(not(windows))]
async fn start_share_backend(
    _state: &Arc<AppState>,
    _cfg: &StreamConfig,
    _room: String,
    _rtsp: &Arc<rtsp::RtspServer>,
) -> Result<Backend> {
    bail!("stream share (screen capture) is only supported on Windows")
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
            // How long ago real media last moved (`null` = never): the web
            // dashboard compares these against a staleness threshold to
            // animate its topology edges only while data actually flows
            // (dummy RTSP keepalives don't refresh these -- see
            // `RtspServer::flow_ages_ms`).
            let (video_age_ms, audio_age_ms) = pipeline.rtsp.flow_ages_ms().await;
            // Delivery-aware siblings of the above: age since media was last
            // actually dispatched to a *playing* RTSP session, not just
            // received from the browser -- see `RtspServer::delivered_flow_ages_ms`.
            let (video_delivered_age_ms, audio_delivered_age_ms) =
                pipeline.rtsp.delivered_flow_ages_ms().await;
            let mut value = json!({
                "running": true,
                "rtsp_url": pipeline.rtsp_url,
                "clients": pipeline.rtsp.client_count().await,
                "uptime_secs": pipeline.started_at.elapsed().as_secs(),
                "backend": pipeline.backend_kind.as_str(),
                "flow": {
                    "video_age_ms": video_age_ms,
                    "audio_age_ms": audio_age_ms,
                    "video_delivered_age_ms": video_delivered_age_ms,
                    "audio_delivered_age_ms": audio_delivered_age_ms,
                },
            });
            if let Some(room) = &pipeline.room {
                value["room"] = json!(room);
                if let Backend::Relay(relay) = &pipeline.backend {
                    value["publisher"] = json!(relay.publisher());
                    value["cascade"] = relay.cascade_status();
                    // Audio-pipeline observability (attached/rtp_packets/
                    // rtp_bytes/frames_sent/transcode_errors) -- see
                    // `RelayCapture::audio_status`'s doc for how to read
                    // these when diagnosing "no sound in VRChat".
                    value["audio"] = relay.audio_status();
                }
            }
            // `share` is always present (active: false when this pipeline
            // isn't a share) so a caller can check it without first checking
            // `backend`.
            #[cfg(windows)]
            {
                value["share"] = match &pipeline.backend {
                    Backend::Share(share) => share.status(),
                    _ => json!({ "active": false }),
                };
            }
            #[cfg(not(windows))]
            {
                value["share"] = json!({ "active": false });
            }
            Ok(value)
        }
        None => Ok(json!({ "running": false, "share": { "active": false } })),
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
        assert_eq!(
            CaptureBackend::parse("native").unwrap(),
            CaptureBackend::Native
        );
        assert_eq!(
            CaptureBackend::parse("ffmpeg").unwrap(),
            CaptureBackend::Ffmpeg
        );
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
