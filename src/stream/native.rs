//! Native (ffmpeg-free) capture backend: Windows Graphics Capture (via the
//! `windows-capture` crate) grabs BGRA frames off the primary monitor,
//! converts them to I420, encodes them with OpenH264, and hands the
//! resulting Annex-B access units directly to the RTSP server -- the same
//! hand-off [`super::ingest`] uses for the `ffmpeg` backend, just without a
//! MPEG-TS/UDP hop in between. Mirrors mistlink's Go pipeline (GDI capture +
//! OpenH264 via `pion/mediadevices`), adapted to in-process capture here.
//!
//! Windows-only: [`NativeCapture::spawn`] is the only entry point other
//! modules call, and `stream::mod` only compiles this module `#[cfg(windows)]`.

#![cfg(windows)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use anyhow::Result;
use openh264::OpenH264API;
use openh264::encoder::{
    BitRate, EncodedBitStream, Encoder, EncoderConfig, FrameRate, IntraFramePeriod,
    RateControlMode, UsageType,
};
use openh264::formats::YUVBuffer;
use tokio::sync::mpsc;
use windows_capture::capture::{CaptureControl, Context, GraphicsCaptureApiHandler};
use windows_capture::frame::Frame;
use windows_capture::graphics_capture_api::InternalCaptureControl;
use windows_capture::monitor::Monitor;
use windows_capture::settings::{
    ColorFormat, CursorCaptureSettings, DirtyRegionSettings, DrawBorderSettings,
    MinimumUpdateIntervalSettings, SecondaryWindowSettings, Settings,
};

use super::rtsp::RtspServer;

const NAL_TYPE_SPS: u8 = 7;
const NAL_TYPE_PPS: u8 = 8;

/// Target encoder bitrate: ~4 Mbps, matching a reasonable 1080p30 real-time
/// screen-share budget.
const TARGET_BITRATE_BPS: u32 = 4_000_000;

/// Error type for the capture handler. `windows-capture` recommends
/// `Box<dyn Error + Send + Sync>` for this exact use case (see its own
/// example in `lib.rs`), which lets every underlying error (`openh264`,
/// `windows-capture`, `windows`) convert in via `?`.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A running native capture pipeline: the Windows Graphics Capture session
/// (on its own OS thread, owned by `windows-capture`) and the tokio task that
/// forwards encoded access units into the RTSP server.
pub struct NativeCapture {
    control: Option<CaptureControl<Capturer, BoxError>>,
    forward_task: tokio::task::JoinHandle<()>,
}

impl NativeCapture {
    /// Starts capturing the primary monitor at `frame_rate` fps (extra
    /// frames beyond that are dropped), downscaling to `max_width` when
    /// wider, encoding with OpenH264, and forwarding access units to `rtsp`.
    ///
    /// `share` is an optional second consumer of every encoded access unit
    /// (Annex-B bytes, same shape `rtsp` gets): `stream::share::ShareCapture`
    /// passes one so it can RTP-packetize and publish the exact same capture
    /// into a mistlib room alongside the local RTSP feed, without a second
    /// capture/encode pipeline.
    pub async fn spawn(
        frame_rate: u32,
        max_width: u32,
        rtsp: Arc<RtspServer>,
        share: Option<mpsc::UnboundedSender<Vec<u8>>>,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<EncodedAu>();
        let forward_task = tokio::spawn(forward_loop(rx, rtsp, share));

        let flags = NativeFlags {
            frame_rate: frame_rate.max(1),
            max_width,
            tx,
        };

        // Setting up WinRT/D3D11/the capture session involves blocking COM
        // and device-creation calls, and `start_free_threaded` itself blocks
        // briefly waiting for the new thread to report back -- none of that
        // belongs on a tokio worker thread.
        let control = tokio::task::spawn_blocking(move || start_capture(flags))
            .await
            .context("native capture setup task panicked")??;

        Ok(Self {
            control: Some(control),
            forward_task,
        })
    }

    /// Stops the capture thread and the forward task, waiting for the
    /// capture thread to exit. `CaptureControl::stop` posts a `WM_QUIT` to
    /// the capture thread's message loop and joins it, so this normally
    /// returns in well under a second.
    pub async fn stop(mut self) {
        self.forward_task.abort();
        if let Some(control) = self.control.take() {
            let _ = tokio::task::spawn_blocking(move || control.stop()).await;
        }
    }
}

impl Drop for NativeCapture {
    fn drop(&mut self) {
        self.forward_task.abort();
        if let Some(control) = self.control.take() {
            // Best-effort: `CaptureControl::stop` blocks joining the capture
            // thread, which Drop can't `.await`. Explicit `stop()` (used by
            // `stream.stop`) already does this properly; this is only a
            // backstop for a `NativeCapture` dropped without it.
            std::thread::spawn(move || {
                let _ = control.stop();
            });
        }
    }
}

fn start_capture(flags: NativeFlags) -> Result<CaptureControl<Capturer, BoxError>> {
    let monitor = Monitor::primary().context("finding the primary monitor to capture")?;

    let settings = Settings::new(
        monitor,
        CursorCaptureSettings::Default,
        DrawBorderSettings::WithoutBorder,
        SecondaryWindowSettings::Default,
        MinimumUpdateIntervalSettings::Default,
        DirtyRegionSettings::Default,
        ColorFormat::Bgra8,
        flags,
    );

    Capturer::start_free_threaded(settings).context("starting Windows Graphics Capture")
}

/// Flags passed through [`Settings`] into [`Capturer::new`].
struct NativeFlags {
    frame_rate: u32,
    max_width: u32,
    tx: mpsc::UnboundedSender<EncodedAu>,
}

/// One encoded access unit (optional SPS/PPS NALs + exactly one slice NAL,
/// each still start-code prefixed), plus the SPS/PPS caches to hand to the
/// RTSP server -- mirroring [`super::ingest::process_nal`]'s behavior for
/// the ffmpeg backend.
struct EncodedAu {
    au: Vec<u8>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}

/// Receives encoded access units from the capture/encode thread and forwards
/// them to the RTSP server (and, if `share` is set, to the share publisher
/// too -- one clone of the Annex-B bytes per extra consumer, cheap next to
/// the encode work already done), doing the `.await`-requiring work the
/// capture callback (a plain synchronous fn on a non-tokio thread) can't do
/// itself.
async fn forward_loop(
    mut rx: mpsc::UnboundedReceiver<EncodedAu>,
    rtsp: Arc<RtspServer>,
    share: Option<mpsc::UnboundedSender<Vec<u8>>>,
) {
    while let Some(unit) = rx.recv().await {
        if let Some(share_tx) = &share {
            // Best-effort: a closed receiver (share stopped, capture still
            // draining its last few frames) just means nobody's listening.
            let _ = share_tx.send(unit.au.clone());
        }
        if let Some(sps) = unit.sps {
            rtsp.update_sps(sps).await;
        }
        if let Some(pps) = unit.pps {
            rtsp.update_pps(pps).await;
        }
        rtsp.send_video_access_unit(&unit.au).await;
    }
}

/// The `windows-capture` event handler: owns the OpenH264 encoder and
/// converts/encodes/forwards each captured frame.
struct Capturer {
    encoder: Encoder,
    tx: mpsc::UnboundedSender<EncodedAu>,
    max_width: u32,
    frame_interval: Duration,
    last_encoded_at: Option<Instant>,
}

impl GraphicsCaptureApiHandler for Capturer {
    type Flags = NativeFlags;
    type Error = BoxError;

    fn new(ctx: Context<Self::Flags>) -> Result<Self, Self::Error> {
        let frame_rate = ctx.flags.frame_rate;
        // Closed GOP twice the frame rate, matching the ffmpeg backend's
        // keyframe cadence (see capture.rs) so a freshly-connected client
        // isn't stuck waiting long for an IDR frame.
        let gop = frame_rate.saturating_mul(2).max(1);

        let config = EncoderConfig::new()
            .bitrate(BitRate::from_bps(TARGET_BITRATE_BPS))
            .max_frame_rate(FrameRate::from_hz(frame_rate as f32))
            .rate_control_mode(RateControlMode::Bitrate)
            .usage_type(UsageType::ScreenContentRealTime)
            .intra_frame_period(IntraFramePeriod::from_num_frames(gop));
        // No B-frame setting exists: OpenH264 only ever emits I/P slices
        // (no B slice support in the SVC encoder), so the "zero B-frames"
        // requirement holds with no extra configuration.

        let encoder = Encoder::with_api_config(OpenH264API::from_source(), config)?;

        Ok(Self {
            encoder,
            tx: ctx.flags.tx,
            max_width: ctx.flags.max_width,
            frame_interval: Duration::from_secs_f64(1.0 / f64::from(frame_rate)),
            last_encoded_at: None,
        })
    }

    fn on_frame_arrived(
        &mut self,
        frame: &mut Frame,
        _capture_control: InternalCaptureControl,
    ) -> Result<(), Self::Error> {
        // Frame pacing: encode at most `frame_rate` frames/sec, dropping the
        // rest. When the screen is static and no frames arrive at all, we
        // simply aren't called -- rtsp.rs's dummy SPS/PPS keepalive covers
        // that idle case.
        let now = Instant::now();
        if let Some(last) = self.last_encoded_at {
            if now.duration_since(last) < self.frame_interval {
                return Ok(());
            }
        }

        let width = frame.width();
        let height = frame.height();
        if width < 2 || height < 2 {
            return Ok(());
        }

        let mut buffer = frame.buffer()?;
        let row_pitch = buffer.row_pitch();
        let raw: &[u8] = &*buffer.as_raw_buffer();

        let (out_w, out_h) = scaled_dimensions(width, height, self.max_width);
        let yuv_bytes = bgra_to_i420(raw, width, height, row_pitch, out_w, out_h);
        let yuv = YUVBuffer::from_vec(yuv_bytes, out_w as usize, out_h as usize);

        let stream = self.encoder.encode(&yuv)?;
        let unit = access_unit_from_stream(&stream);
        drop(stream);

        self.last_encoded_at = Some(now);
        // If the forward task has ended (e.g. the pipeline is shutting
        // down), there's nobody left to send to; just drop the frame.
        let _ = self.tx.send(unit);

        Ok(())
    }
}

/// Extracts an [`EncodedAu`] from one `encode()` call's output: every NAL
/// across every layer, concatenated in order (already Annex-B start-code
/// prefixed by OpenH264), plus the SPS/PPS bytes if this access unit
/// contained them (only true on IDR frames).
fn access_unit_from_stream(stream: &EncodedBitStream<'_>) -> EncodedAu {
    let mut au = Vec::new();
    let mut sps = None;
    let mut pps = None;

    for l in 0..stream.num_layers() {
        let Some(layer) = stream.layer(l) else {
            continue;
        };
        for n in 0..layer.nal_count() {
            let Some(nal) = layer.nal_unit(n) else {
                continue;
            };
            au.extend_from_slice(nal);

            match nal_type(nal) {
                Some(NAL_TYPE_SPS) => sps = Some(nal[4..].to_vec()),
                Some(NAL_TYPE_PPS) => pps = Some(nal[4..].to_vec()),
                _ => {}
            }
        }
    }

    EncodedAu { au, sps, pps }
}

/// OpenH264 prefixes every NAL it emits with a 4-byte Annex-B start code
/// (`00 00 00 01`); the NAL header byte immediately follows it (confirmed by
/// the `openh264` crate's own `tests/encode.rs`, e.g.
/// `nal_unit(0)[..5] == [0, 0, 0, 1, 0x67]` for an SPS).
fn nal_type(nal: &[u8]) -> Option<u8> {
    nal.get(4).map(|header| header & 0x1F)
}

/// Computes output dimensions after downscaling `width`x`height` so the
/// width doesn't exceed `max_width` (`0` meaning "no limit"), preserving
/// aspect ratio and rounding both dimensions down to the nearest even number
/// (required by the encoder's 4:2:0 macroblocks).
fn scaled_dimensions(width: u32, height: u32, max_width: u32) -> (u32, u32) {
    if max_width == 0 || width <= max_width {
        return (even_floor(width), even_floor(height));
    }

    let scale = f64::from(max_width) / f64::from(width);
    let out_h = ((f64::from(height) * scale).round() as u32).max(2);
    (even_floor(max_width), even_floor(out_h))
}

fn even_floor(n: u32) -> u32 {
    if n < 2 { 2 } else { n & !1 }
}

/// Converts a BGRA8 frame buffer (row stride `stride` bytes, which may
/// exceed `width * 4` because of D3D texture row padding -- see
/// [`windows_capture::frame::FrameBuffer::has_padding`]) into a planar I420
/// (YUV 4:2:0) buffer sized `(out_w, out_h)`, resampling with
/// nearest-neighbor when downscaling and averaging the 2x2 luma block under
/// each chroma sample. A fast, simple filter is enough for v0.2 (the encoder
/// already discards most fine detail at real-time screen-share bitrates).
///
/// Luma/chroma use the BT.601 studio-range integer coefficients, the same
/// ones `openh264`'s own `formats::rgb2yuv::write_yuv_scalar` uses, so this
/// hand-rolled conversion stays numerically consistent with the crate's
/// built-in (but RGB8-only, not BGRA-capable at full speed) helpers.
fn bgra_to_i420(
    bgra: &[u8],
    width: u32,
    height: u32,
    stride: u32,
    out_w: u32,
    out_h: u32,
) -> Vec<u8> {
    let (width, height, stride) = (width as usize, height as usize, stride as usize);
    let (out_w, out_h) = (out_w as usize, out_h as usize);

    let sample = |x: usize, y: usize| -> (i32, i32, i32) {
        let sx = (x * width / out_w).min(width.saturating_sub(1));
        let sy = (y * height / out_h).min(height.saturating_sub(1));
        let base = sy * stride + sx * 4;
        // BGRA byte order: B, G, R, A.
        (
            i32::from(bgra[base + 2]),
            i32::from(bgra[base + 1]),
            i32::from(bgra[base]),
        )
    };

    let mut yuv = vec![0u8; out_w * out_h * 3 / 2];
    let (y_plane, uv_plane) = yuv.split_at_mut(out_w * out_h);
    let (u_plane, v_plane) = uv_plane.split_at_mut(out_w * out_h / 4);

    for y in 0..out_h {
        for x in 0..out_w {
            let (r, g, b) = sample(x, y);
            y_plane[y * out_w + x] = (((66 * r + 129 * g + 25 * b) >> 8) + 16).clamp(0, 255) as u8;
        }
    }

    let half_w = out_w / 2;
    for cy in 0..out_h / 2 {
        for cx in 0..half_w {
            let (r0, g0, b0) = sample(cx * 2, cy * 2);
            let (r1, g1, b1) = sample(cx * 2 + 1, cy * 2);
            let (r2, g2, b2) = sample(cx * 2, cy * 2 + 1);
            let (r3, g3, b3) = sample(cx * 2 + 1, cy * 2 + 1);
            let r = (r0 + r1 + r2 + r3) / 4;
            let g = (g0 + g1 + g2 + g3) / 4;
            let b = (b0 + b1 + b2 + b3) / 4;

            let idx = cy * half_w + cx;
            u_plane[idx] = (((-38 * r + 112 * b - 74 * g) >> 8) + 128).clamp(0, 255) as u8;
            v_plane[idx] = (((112 * r - 18 * b - 94 * g) >> 8) + 128).clamp(0, 255) as u8;
        }
    }

    yuv
}

#[cfg(test)]
mod tests {
    use super::*;

    fn solid_bgra(b: u8, g: u8, r: u8, width: u32, height: u32) -> Vec<u8> {
        let mut buf = vec![0u8; (width * height * 4) as usize];
        for px in buf.chunks_exact_mut(4) {
            px[0] = b;
            px[1] = g;
            px[2] = r;
            px[3] = 255;
        }
        buf
    }

    #[test]
    fn bgra_to_i420_matches_bt601_reference_values() {
        // Red matches openh264's own formats::yuv test for pure red
        // (255,0,0): y=81, u=90, v=239 -- confirms our hand-rolled
        // coefficients agree with the crate's built-in RGB8->YUV conversion.
        // White and black are the degenerate full-luma-range cases.
        let cases: [(&str, u8, u8, u8, u8, u8, u8); 3] = [
            ("white", 255, 255, 255, 235, 128, 128),
            ("black", 0, 0, 0, 16, 128, 128),
            ("red", 0, 0, 255, 81, 90, 239),
        ];
        for (name, b, g, r, expected_y, expected_u, expected_v) in cases {
            let bgra = solid_bgra(b, g, r, 2, 2);
            let yuv = bgra_to_i420(&bgra, 2, 2, 2 * 4, 2, 2);
            assert_eq!(
                &yuv[0..4],
                &[expected_y, expected_y, expected_y, expected_y],
                "{name}: unexpected luma"
            );
            assert_eq!(yuv[4], expected_u, "{name}: unexpected U chroma");
            assert_eq!(yuv[5], expected_v, "{name}: unexpected V chroma");
        }
    }

    #[test]
    fn bgra_to_i420_handles_row_padding() {
        // 2x2 image padded to a 16-byte stride (D3D staging textures are
        // frequently padded past `width * 4`).
        let (width, height, stride) = (2u32, 2u32, 16u32);
        let mut bgra = vec![0u8; (stride * height) as usize];
        for y in 0..height as usize {
            for x in 0..width as usize {
                let base = y * stride as usize + x * 4;
                bgra[base] = 0; // B
                bgra[base + 1] = 0; // G
                bgra[base + 2] = 255; // R
                bgra[base + 3] = 255; // A
            }
        }
        let yuv = bgra_to_i420(&bgra, width, height, stride, width, height);
        assert_eq!(&yuv[0..4], &[81, 81, 81, 81]);
    }

    #[test]
    fn scaled_dimensions_preserves_aspect_and_forces_even() {
        assert_eq!(scaled_dimensions(3840, 2160, 1920), (1920, 1080));
        assert_eq!(scaled_dimensions(1920, 1080, 1920), (1920, 1080));
        assert_eq!(scaled_dimensions(1921, 1081, 1921), (1920, 1080));
        assert_eq!(scaled_dimensions(2561, 1441, 1920), (1920, 1080));
    }

    #[test]
    fn scaled_dimensions_passthrough_when_under_max_width() {
        assert_eq!(scaled_dimensions(1280, 720, 1920), (1280, 720));
    }

    #[test]
    fn nal_type_reads_header_after_start_code() {
        assert_eq!(nal_type(&[0, 0, 0, 1, 0x67]), Some(NAL_TYPE_SPS));
        assert_eq!(nal_type(&[0, 0, 0, 1, 0x68]), Some(NAL_TYPE_PPS));
        assert_eq!(nal_type(&[0, 0, 0, 1, 0x65]), Some(5)); // IDR slice, not SPS/PPS
        assert_eq!(nal_type(&[0, 0, 0, 1]), None); // no header byte present
    }
}
