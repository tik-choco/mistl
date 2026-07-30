//! Synthetic RTSP self-test feed: generates decoder-valid H264 (a visibly
//! moving test pattern) and, optionally, a synthetic sine-tone audio track,
//! and pumps them into an already-started [`RtspServer`] the exact same way
//! a real capture backend (`native.rs`) or the p2p relay (`relay.rs`) would.
//!
//! This exists because this machine can't reach the p2p network the real
//! relay (`relay.rs`) depends on, so the tc-chat -> VRChat path can't be
//! exercised live here. What *can* be tested locally is the RTSP-serving
//! half of the pipeline: point a standard RTSP client (ffprobe/ffplay) at
//! the server and confirm it decodes a moving picture (and, if enabled,
//! audio) end-to-end, with no external tools or network access required to
//! produce the feed.
//!
//! Structure mirrors `native.rs`'s split: the OpenH264 encoder is
//! synchronous, so it's driven from a dedicated OS thread and forwards
//! encoded access units to an async task over an `mpsc` channel; that task
//! owns all the `.await`-requiring calls into `rtsp`. Audio encoding
//! (fdk-aac / opus) is cheap enough to run directly in its own async task,
//! matching how `relay.rs`'s audio tasks already do it.
//!
//! Driven by `stream.selftest.start` (CLI: `mistl stream selftest`), which
//! starts the RTSP server with the requested audio codec and spawns
//! [`run`] to feed it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use openh264::OpenH264API;
use openh264::encoder::{
    BitRate, EncodedBitStream, Encoder, EncoderConfig, FrameRate, IntraFramePeriod,
    RateControlMode, UsageType,
};
use openh264::formats::YUVBuffer;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::rtp_out::{self, AudioCodec};
use super::rtsp::RtspServer;

/// H264 NAL unit type constants (RFC 6184 section 5.4), matching `native.rs`.
const NAL_TYPE_SPS: u8 = 7;
const NAL_TYPE_PPS: u8 = 8;

/// Target encoder bitrate for the synthetic feed -- generous for a small
/// test pattern, nowhere near real screen-share bitrates.
const TARGET_BITRATE_BPS: u32 = 3_000_000;

/// Samples per channel in one AAC-LC frame (fixed by the codec), matching
/// `relay.rs`'s `OpusToAac`.
const AAC_FRAME_SAMPLES: usize = 1024;

/// One Opus frame at 20ms / 48 kHz, a common real-time frame size.
const OPUS_FRAME_SAMPLES: usize = 960;

/// Default frame size and rate for [`SelfTestOpts::default`] -- small enough
/// to encode cheaply, large enough to be worth looking at in ffplay.
pub const DEFAULT_WIDTH: u32 = 640;
pub const DEFAULT_HEIGHT: u32 = 360;
pub const DEFAULT_FRAME_RATE: u32 = 30;

/// Tone frequency for the synthetic audio track.
const TONE_HZ: f32 = 440.0;

/// Options controlling the synthetic self-test feed.
pub struct SelfTestOpts {
    pub width: u32,
    pub height: u32,
    pub frame_rate: u32,
    pub audio: Option<AudioCodec>,
    pub duration: Option<Duration>,
}

impl Default for SelfTestOpts {
    fn default() -> Self {
        Self {
            width: DEFAULT_WIDTH,
            height: DEFAULT_HEIGHT,
            frame_rate: DEFAULT_FRAME_RATE,
            audio: None,
            duration: None,
        }
    }
}

/// Feed synthetic decoder-valid H264 (a visibly moving test pattern so a
/// human can confirm live video in ffplay) and, if `opts.audio` is set,
/// synthetic audio (a sine tone) into an already-started `rtsp` server.
/// Returns when `opts.duration` elapses; if `duration` is `None` it runs
/// until the spawned tasks are aborted (including indirectly, by the caller
/// aborting the task this future itself runs in -- see [`AbortOnDrop`]).
/// Video and audio each run on their own task, paced to real time
/// (`opts.frame_rate` for video, 48kHz/1024 samples for AAC or 48kHz/960
/// samples for Opus).
pub async fn run(rtsp: Arc<RtspServer>, opts: SelfTestOpts) -> Result<()> {
    let video_forward = spawn_video(rtsp.clone(), &opts)?;
    let mut tasks = vec![video_forward];
    if let Some(codec) = opts.audio {
        tasks.push(spawn_audio(rtsp.clone(), codec));
    }
    // Guarantees the generator tasks stop whether `run` returns normally
    // (duration elapsed) or its future is dropped from cancellation (the
    // caller aborted the task hosting this `run` call) -- a bare
    // `JoinHandle` left on the stack would just be detached, not aborted, on
    // the latter path.
    let _guard = AbortOnDrop(tasks);

    match opts.duration {
        Some(duration) => tokio::time::sleep(duration).await,
        None => std::future::pending::<()>().await,
    }

    Ok(())
}

/// Aborts every wrapped task handle when dropped, so cancelling `run`'s
/// future (rather than letting it return normally) still stops the
/// generator tasks instead of leaking them running detached in the
/// background.
struct AbortOnDrop(Vec<JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

// --- video -------------------------------------------------------------

/// One encoded access unit (optional SPS/PPS NALs + one or more slice NALs,
/// each still start-code prefixed), mirroring `native.rs`'s `EncodedAu`.
struct EncodedAu {
    au: Vec<u8>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
}

/// Wraps an OpenH264 encoder configured the same way as the native capture
/// backend (`native.rs`): screen-content-real-time usage, bitrate-controlled,
/// with a GOP length of `frame_rate * 2` so keyframes recur regularly (the
/// very first encoded frame is always an IDR carrying SPS+PPS). Generating
/// the test pattern and encoding it are kept behind this one entry point
/// (`encode_frame`) so both can be unit-tested without a running
/// `RtspServer` or any async runtime.
struct VideoEncoder {
    encoder: Encoder,
    width: u32,
    height: u32,
}

impl VideoEncoder {
    fn new(width: u32, height: u32, frame_rate: u32) -> Result<Self> {
        // Even dimensions are required by the encoder's 4:2:0 macroblocks;
        // floored once here so the stored size always matches what
        // `generate_test_pattern_i420` and `YUVBuffer` agree on.
        let width = even_floor(width.max(2));
        let height = even_floor(height.max(2));
        let frame_rate = frame_rate.max(1);
        let gop = frame_rate.saturating_mul(2).max(1);

        let config = EncoderConfig::new()
            .bitrate(BitRate::from_bps(TARGET_BITRATE_BPS))
            .max_frame_rate(FrameRate::from_hz(frame_rate as f32))
            .rate_control_mode(RateControlMode::Bitrate)
            .usage_type(UsageType::ScreenContentRealTime)
            .intra_frame_period(IntraFramePeriod::from_num_frames(gop));

        let encoder = Encoder::with_api_config(OpenH264API::from_source(), config)
            .context("creating OpenH264 encoder for selftest video")?;

        Ok(Self {
            encoder,
            width,
            height,
        })
    }

    /// Generates the synthetic test-pattern frame for `frame_index`, encodes
    /// it, and returns the resulting access unit (SPS/PPS present only on
    /// IDR frames, same as `native.rs`).
    fn encode_frame(&mut self, frame_index: u64) -> Result<EncodedAu> {
        let yuv_bytes = generate_test_pattern_i420(self.width, self.height, frame_index);
        let yuv = YUVBuffer::from_vec(yuv_bytes, self.width as usize, self.height as usize);
        let stream = self
            .encoder
            .encode(&yuv)
            .context("encoding selftest video frame")?;
        Ok(access_unit_from_stream(&stream))
    }
}

/// Extracts an [`EncodedAu`] from one `encode()` call's output: every NAL
/// across every layer, concatenated in order (already Annex-B start-code
/// prefixed by OpenH264), plus the SPS/PPS bytes if this access unit
/// contained them. Identical logic to `native.rs::access_unit_from_stream`,
/// duplicated here rather than shared because `native.rs` is
/// `#[cfg(windows)]`-gated as a whole file and this module must compile on
/// every platform.
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
/// (`00 00 00 01`); the NAL header byte immediately follows it (see
/// `native.rs::nal_type`).
fn nal_type(nal: &[u8]) -> Option<u8> {
    nal.get(4).map(|header| header & 0x1F)
}

fn even_floor(n: u32) -> u32 {
    if n < 2 { 2 } else { n & !1 }
}

/// Generates one I420 (planar YUV 4:2:0) frame of a synthetic test pattern
/// that visibly moves from one `frame_index` to the next: a horizontally
/// scrolling luma gradient plus a bright square sweeping across the frame,
/// so a human watching in ffplay can confirm the video is live rather than
/// a frozen frame. Chroma is flat mid-gray (128) -- the moving pattern only
/// needs to prove liveness, not color. `width`/`height` are assumed already
/// even (see [`even_floor`]).
fn generate_test_pattern_i420(width: u32, height: u32, frame_index: u64) -> Vec<u8> {
    let w = width.max(2) as usize;
    let h = height.max(2) as usize;

    let mut yuv = vec![0u8; w * h * 3 / 2];
    let (y_plane, uv_plane) = yuv.split_at_mut(w * h);
    let (u_plane, v_plane) = uv_plane.split_at_mut(w * h / 4);

    let scroll = (frame_index as usize * 4) % w;
    for y in 0..h {
        for x in 0..w {
            let shifted = (x + scroll) % w;
            y_plane[y * w + x] = (16 + (shifted * 219 / w)) as u8;
        }
    }

    // A bright square sweeping left-to-right (wrapping around), so motion is
    // unambiguous even at a glance.
    let box_size = (w.min(h) / 6).max(2);
    let travel = w.saturating_sub(box_size).max(1);
    let box_x = (frame_index as usize * 6) % travel;
    let box_y = h / 2 - (box_size / 2).min(h / 2);
    for by in 0..box_size {
        for bx in 0..box_size {
            let (px, py) = (box_x + bx, box_y + by);
            if px < w && py < h {
                y_plane[py * w + px] = 235;
            }
        }
    }

    u_plane.fill(128);
    v_plane.fill(128);

    yuv
}

/// Starts the video generator: an OpenH264 encoder built up front (so a bad
/// config fails `run` immediately instead of silently doing nothing on a
/// background thread) then driven from a dedicated OS thread -- the encoder
/// is synchronous and, per `native.rs`, shouldn't run on a tokio worker --
/// paced to `opts.frame_rate`. Returns the async forwarder task's handle;
/// the generator thread notices the forwarder going away (channel send
/// starts failing) and exits within one frame interval.
fn spawn_video(rtsp: Arc<RtspServer>, opts: &SelfTestOpts) -> Result<JoinHandle<()>> {
    let (tx, rx) = mpsc::unbounded_channel::<EncodedAu>();
    let frame_rate = opts.frame_rate.max(1);
    let mut encoder = VideoEncoder::new(opts.width, opts.height, frame_rate)?;

    info!(
        width = encoder.width,
        height = encoder.height,
        frame_rate,
        "selftest: video generator started"
    );

    std::thread::spawn(move || {
        let frame_interval = Duration::from_secs_f64(1.0 / f64::from(frame_rate));
        let mut frame_index: u64 = 0;
        loop {
            let start = Instant::now();
            let au = match encoder.encode_frame(frame_index) {
                Ok(au) => au,
                Err(error) => {
                    warn!(%error, "selftest: video encode failed, stopping generator");
                    break;
                }
            };
            if tx.send(au).is_err() {
                break; // forwarder gone (aborted / rtsp shutting down)
            }
            frame_index = frame_index.wrapping_add(1);

            let elapsed = start.elapsed();
            if elapsed < frame_interval {
                std::thread::sleep(frame_interval - elapsed);
            }
        }
    });

    Ok(tokio::spawn(forward_video_loop(rx, rtsp)))
}

/// Receives encoded access units from the generator thread and forwards
/// them into `rtsp`, mirroring `native.rs::forward_loop`.
async fn forward_video_loop(mut rx: mpsc::UnboundedReceiver<EncodedAu>, rtsp: Arc<RtspServer>) {
    while let Some(unit) = rx.recv().await {
        if let Some(sps) = unit.sps {
            rtsp.update_sps(sps).await;
        }
        if let Some(pps) = unit.pps {
            rtsp.update_pps(pps).await;
        }
        rtsp.send_video_access_unit(&unit.au).await;
    }
    debug!("selftest: video forwarder ended");
}

// --- audio ---------------------------------------------------------------

/// Generates a continuous sine tone as interleaved stereo i16 PCM, keeping
/// phase continuous across calls to `next_chunk` so consecutive frames don't
/// click at the chunk boundary.
struct SineWaveGenerator {
    sample_rate: u32,
    freq_hz: f32,
    sample_index: u64,
}

impl SineWaveGenerator {
    fn new(sample_rate: u32, freq_hz: f32) -> Self {
        Self {
            sample_rate,
            freq_hz,
            sample_index: 0,
        }
    }

    /// Produces `samples_per_channel` interleaved stereo i16 samples
    /// (`2 * samples_per_channel` values total), continuing the phase from
    /// the previous call.
    fn next_chunk(&mut self, samples_per_channel: usize) -> Vec<i16> {
        let mut pcm = Vec::with_capacity(samples_per_channel * 2);
        for i in 0..samples_per_channel {
            let t = (self.sample_index + i as u64) as f32 / self.sample_rate as f32;
            let sample =
                (2.0 * std::f32::consts::PI * self.freq_hz * t).sin() * i16::MAX as f32 * 0.25;
            let sample = sample as i16;
            pcm.push(sample); // left
            pcm.push(sample); // right
        }
        self.sample_index += samples_per_channel as u64;
        pcm
    }
}

/// Wraps an `fdk-aac` encoder configured the same way as `relay.rs`'s
/// `OpusToAac` (Raw transport, 48kHz stereo, AAC-LC, 128kbps CBR), encoding
/// one already-1024-samples/channel PCM chunk per call so it's directly
/// unit-testable without the Opus decode step `OpusToAac` also does.
struct AacEncoder {
    encoder: fdk_aac::enc::Encoder,
    output_buf: [u8; 4096],
}

impl AacEncoder {
    fn new() -> Result<Self> {
        let encoder = fdk_aac::enc::Encoder::new(fdk_aac::enc::EncoderParams {
            bit_rate: fdk_aac::enc::BitRate::Cbr(128_000),
            sample_rate: rtp_out::AUDIO_CLOCK_RATE,
            transport: fdk_aac::enc::Transport::Raw,
            channels: fdk_aac::enc::ChannelMode::Stereo,
            audio_object_type: fdk_aac::enc::AudioObjectType::Mpeg4LowComplexity,
        })
        .map_err(|error| anyhow!("creating AAC encoder: {error}"))?;

        Ok(Self {
            encoder,
            output_buf: [0u8; 4096],
        })
    }

    /// Encodes one `AAC_FRAME_SAMPLES`-samples/channel stereo PCM chunk
    /// (`AAC_FRAME_SAMPLES * AAC_CHANNELS` interleaved i16 samples) into an
    /// AAC-LC raw frame. Returns `None` while the encoder is still priming
    /// (normal for its first frame or two of output, per `fdk-aac`).
    fn encode_chunk(&mut self, pcm: &[i16]) -> Result<Option<Vec<u8>>> {
        match self.encoder.encode(pcm, &mut self.output_buf) {
            Ok(info) if info.output_size > 0 => {
                Ok(Some(self.output_buf[..info.output_size].to_vec()))
            }
            Ok(_) => Ok(None),
            Err(error) => Err(anyhow!("AAC encode failed: {error}")),
        }
    }
}

fn spawn_audio(rtsp: Arc<RtspServer>, codec: AudioCodec) -> JoinHandle<()> {
    match codec {
        AudioCodec::Aac => tokio::spawn(audio_loop_aac(rtsp)),
        AudioCodec::Opus => tokio::spawn(audio_loop_opus(rtsp)),
    }
}

/// Generates a sine tone, encodes it to AAC-LC 1024-sample frames, and sends
/// them to `rtsp`, paced to one frame (~21.33ms) per iteration.
async fn audio_loop_aac(rtsp: Arc<RtspServer>) {
    let mut encoder = match AacEncoder::new() {
        Ok(encoder) => encoder,
        Err(error) => {
            warn!(%error, "selftest: failed to set up AAC encoder; audio disabled");
            return;
        }
    };
    let mut tone = SineWaveGenerator::new(rtp_out::AUDIO_CLOCK_RATE, TONE_HZ);
    let frame_duration =
        Duration::from_secs_f64(AAC_FRAME_SAMPLES as f64 / f64::from(rtp_out::AUDIO_CLOCK_RATE));
    let mut ts: u32 = 0;

    info!("selftest: audio generator started (codec=aac)");

    loop {
        let start = Instant::now();
        let pcm = tone.next_chunk(AAC_FRAME_SAMPLES);
        match encoder.encode_chunk(&pcm) {
            Ok(Some(frame)) => rtsp.send_audio_frame(&frame, ts).await,
            Ok(None) => {}
            Err(error) => warn!(%error, "selftest: AAC encode failed, dropping frame"),
        }
        ts = ts.wrapping_add(AAC_FRAME_SAMPLES as u32);

        let elapsed = start.elapsed();
        if elapsed < frame_duration {
            tokio::time::sleep(frame_duration - elapsed).await;
        }
    }
}

/// Generates a sine tone and encodes it to Opus 960-sample (20ms) frames,
/// sending them to `rtsp` paced to real time.
async fn audio_loop_opus(rtsp: Arc<RtspServer>) {
    let mut encoder = match opus::Encoder::new(
        rtp_out::AUDIO_CLOCK_RATE,
        opus::Channels::Stereo,
        opus::Application::Audio,
    ) {
        Ok(encoder) => encoder,
        Err(error) => {
            warn!(%error, "selftest: failed to set up Opus encoder; audio disabled");
            return;
        }
    };
    let mut tone = SineWaveGenerator::new(rtp_out::AUDIO_CLOCK_RATE, TONE_HZ);
    let frame_duration = Duration::from_millis(20);
    let mut ts: u32 = 0;
    let mut buf = vec![0u8; 4000];

    info!("selftest: audio generator started (codec=opus)");

    loop {
        let start = Instant::now();
        let pcm = tone.next_chunk(OPUS_FRAME_SAMPLES);
        match encoder.encode(&pcm, &mut buf) {
            Ok(len) => rtsp.send_audio_frame(&buf[..len], ts).await,
            Err(error) => warn!(%error, "selftest: Opus encode failed, dropping frame"),
        }
        ts = ts.wrapping_add(OPUS_FRAME_SAMPLES as u32);

        let elapsed = start.elapsed();
        if elapsed < frame_duration {
            tokio::time::sleep(frame_duration - elapsed).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scans an Annex-B buffer of concatenated, 4-byte-start-code-prefixed
    /// NALs (the only form OpenH264 emits -- see `nal_type`) for every NAL
    /// type present, in order.
    fn scan_nal_types(au: &[u8]) -> Vec<u8> {
        let mut types = Vec::new();
        let mut i = 0;
        while i + 4 <= au.len() {
            if au[i..i + 4] == [0, 0, 0, 1] {
                if let Some(&header) = au.get(i + 4) {
                    types.push(header & 0x1F);
                }
                i += 4;
            } else {
                i += 1;
            }
        }
        types
    }

    #[test]
    fn first_encoded_video_frame_is_idr_with_sps_and_pps() {
        let mut encoder = VideoEncoder::new(64, 64, 30).expect("creating video encoder");
        let au = encoder.encode_frame(0).expect("encoding first frame");

        assert!(!au.au.is_empty(), "first access unit should be non-empty");
        assert!(au.sps.is_some(), "first frame should carry SPS");
        assert!(au.pps.is_some(), "first frame should carry PPS");

        let nal_types = scan_nal_types(&au.au);
        assert!(
            nal_types.contains(&NAL_TYPE_SPS),
            "expected an SPS NAL, got {nal_types:?}"
        );
        assert!(
            nal_types.contains(&NAL_TYPE_PPS),
            "expected a PPS NAL, got {nal_types:?}"
        );
        assert!(
            nal_types.contains(&5),
            "expected an IDR slice NAL (type 5), got {nal_types:?}"
        );
    }

    #[test]
    fn subsequent_video_frames_encode_without_error() {
        let mut encoder = VideoEncoder::new(64, 64, 30).expect("creating video encoder");
        for frame_index in 0..5u64 {
            let au = encoder.encode_frame(frame_index).expect("encoding frame");
            assert!(
                !au.au.is_empty(),
                "frame {frame_index} should produce a non-empty access unit"
            );
        }
    }

    #[test]
    fn generate_test_pattern_moves_between_frames() {
        let frame0 = generate_test_pattern_i420(64, 64, 0);
        let frame1 = generate_test_pattern_i420(64, 64, 5);
        assert_eq!(frame0.len(), frame1.len());
        assert_ne!(
            frame0, frame1,
            "the test pattern should visibly differ between frame indices"
        );
    }

    #[test]
    fn aac_encoder_emits_at_least_one_frame_from_200ms_of_pcm() {
        let mut encoder = AacEncoder::new().expect("creating AAC encoder");
        let mut tone = SineWaveGenerator::new(rtp_out::AUDIO_CLOCK_RATE, TONE_HZ);

        // ~250ms of audio in 1024-sample/48kHz chunks (~11 chunks) -- enough
        // to clear the encoder's priming delay.
        let mut emitted = 0usize;
        for _ in 0..12 {
            let pcm = tone.next_chunk(AAC_FRAME_SAMPLES);
            if let Some(frame) = encoder.encode_chunk(&pcm).expect("AAC encode") {
                assert!(!frame.is_empty(), "AAC frame should be non-empty");
                emitted += 1;
            }
        }

        assert!(
            emitted >= 1,
            "expected at least one AAC frame from ~250ms of PCM"
        );
    }

    #[test]
    fn opus_encoder_emits_a_frame_for_one_20ms_chunk() {
        let mut encoder = opus::Encoder::new(
            rtp_out::AUDIO_CLOCK_RATE,
            opus::Channels::Stereo,
            opus::Application::Audio,
        )
        .expect("creating Opus encoder");
        let mut tone = SineWaveGenerator::new(rtp_out::AUDIO_CLOCK_RATE, TONE_HZ);

        let pcm = tone.next_chunk(OPUS_FRAME_SAMPLES);
        let mut buf = vec![0u8; 4000];
        let len = encoder.encode(&pcm, &mut buf).expect("Opus encode");
        assert!(len > 0, "Opus frame should be non-empty");
    }

    #[test]
    fn sine_wave_generator_keeps_phase_continuous_across_chunks() {
        let mut a = SineWaveGenerator::new(48_000, 440.0);
        let mut b = SineWaveGenerator::new(48_000, 440.0);

        let a_combined = {
            let mut first = a.next_chunk(100);
            first.extend(a.next_chunk(100));
            first
        };
        let b_combined = b.next_chunk(200);

        assert_eq!(
            a_combined, b_combined,
            "two chunks back-to-back should equal one chunk of the combined length"
        );
    }
}
