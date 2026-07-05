//! Minimal RTSP 1.0 server (OPTIONS/DESCRIBE/SETUP/PLAY/TEARDOWN) for an
//! H264 video stream with an optional AAC or Opus audio track, playable by
//! VRChat video players (AVPro). Session handling, the dummy SPS/PPS/NALU
//! keepalive loop, the AAC AU-header framing, and periodic RTCP Sender
//! Reports mirror mistlink's `internal/rtsp/server.go` and
//! `aac_processor.go` (built on `gortsplib`, which supplies SRs and session
//! bookkeeping for free there); here the RTSP message framing comes from the
//! `rtsp-types` crate and everything else (session table, transport
//! negotiation, RTP/RTCP fan-out, timestamp rebasing, keepalive timer) is
//! hand-rolled.
//!
//! A session covers one RTSP client and can hold up to two media
//! transports (video and/or audio), set up via two separate `SETUP`
//! requests that share one `Session` id -- the second `SETUP` attaches to
//! the session created by the first rather than allocating a new one. Which
//! media a `SETUP` targets is read from a trailing `trackID=`/`streamid=`
//! in the request URI (as emitted in this server's own SDP `a=control`
//! lines); an absent trackID means video, for compatibility with clients
//! that don't send per-track SETUPs on a video-only stream.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rtsp_types::headers::{self, RtpLowerTransport, RtpProfile, RtpTransport, RtpTransportParameters, Transport, Transports};
use rtsp_types::{Method, Request, Response, StatusCode, Version};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::rtp_out::{self, DUMMY_NALU, RtpHeaderFields};

/// How long after the last real access unit the dummy keepalive resumes.
const REAL_DATA_IDLE: Duration = Duration::from_millis(1000);

/// How often RTCP Sender Reports go out per playing session/media, needed
/// for A/V lipsync (mistlink got these for free from gortsplib).
const SR_INTERVAL: Duration = Duration::from_secs(3);

/// Nominal audio timestamp step substituted when a source RTP timestamp
/// can't be trusted (see [`rebase_timestamp`]): a 20ms frame at the fixed
/// 48 kHz audio clock rate.
const AUDIO_NOMINAL_STEP: u32 = 960;

/// Which media track a session transport or dispatch targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Media {
    Video,
    Audio,
}

/// Per-client RTSP session: up to one transport per media, and whether
/// `PLAY` has been issued.
struct Session {
    video: Option<SessionTransport>,
    audio: Option<SessionTransport>,
    playing: bool,
}

/// How to reach one client for one media's RTP/RTCP traffic.
enum SessionTransport {
    Udp {
        rtp_addr: SocketAddr,
        rtcp_addr: SocketAddr,
    },
    Tcp {
        rtp_channel: u8,
        rtcp_channel: u8,
        tx: mpsc::UnboundedSender<Vec<u8>>,
    },
}

/// Running counters needed to fill in an RTCP Sender Report for one media,
/// plus enough to extrapolate the RTP timestamp at send time.
#[derive(Default)]
struct MediaStats {
    packet_count: u32,
    octet_count: u32,
    last_ts: u32,
    last_ts_at: Option<Instant>,
    /// Whether anything has been sent yet -- SRs are skipped for media that
    /// hasn't produced a single packet.
    active: bool,
}

/// Shared mutable state, behind one lock; traffic here is at most
/// video-frame-rate (plus a much slower audio-frame and SR cadence), so a
/// single `tokio::sync::Mutex` is plenty.
struct Inner {
    sessions: HashMap<String, Session>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    /// When the last real access unit was fanned out. The dummy keepalive
    /// stands down only while real data flowed recently: damage-driven
    /// capture backends (Windows.Graphics.Capture) emit nothing at all on a
    /// static screen, and AVPro drops the stream if RTP goes silent. Driven
    /// by video activity only -- mistlink has no audio keepalive.
    last_real_au: Option<Instant>,
    real_seq: u16,
    real_ts: u32,
    dummy_seq: u16,
    dummy_ts: u32,
    payloader: rtp_out::H264Rtp,
    /// Rebase state for [`RtspServer::send_video_access_unit_at`]: the last
    /// source (publisher) RTP timestamp seen, used to compute deltas.
    last_source_video_ts: Option<u32>,
    audio_seq: u16,
    audio_ts: u32,
    /// Rebase state for [`RtspServer::send_audio_frame`], mirroring
    /// `last_source_video_ts`.
    last_source_audio_ts: Option<u32>,
    video_stats: MediaStats,
    audio_stats: MediaStats,
}

/// The RTSP server: one TCP listener plus one shared UDP socket used to
/// send RTP/RTCP to all UDP-transport clients.
pub struct RtspServer {
    rtp_socket: UdpSocket,
    frame_rate: u32,
    /// The audio codec this server was configured with, if any. `None`
    /// means video-only: no audio SDP section, `send_audio_frame` is a
    /// no-op.
    audio: Option<rtp_out::AudioCodec>,
    inner: Mutex<Inner>,
    background_tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl RtspServer {
    fn new(rtp_socket: UdpSocket, frame_rate: u32, audio: Option<rtp_out::AudioCodec>) -> Arc<Self> {
        Arc::new(Self {
            rtp_socket,
            frame_rate,
            audio,
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
                sps: None,
                pps: None,
                last_real_au: None,
                real_seq: rand::random(),
                real_ts: rand::random(),
                dummy_seq: 0,
                dummy_ts: 0,
                payloader: rtp_out::H264Rtp::new(),
                last_source_video_ts: None,
                audio_seq: rand::random(),
                audio_ts: rand::random(),
                last_source_audio_ts: None,
                video_stats: MediaStats::default(),
                audio_stats: MediaStats::default(),
            }),
            background_tasks: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Bind the RTSP TCP listener and a shared RTP UDP socket on
    /// `bind_addr`'s IP, then spawn the accept loop, the dummy keepalive
    /// loop, and the RTCP Sender Report loop as background tasks. `audio`
    /// adds a second (audio) track to the SDP and enables
    /// [`Self::send_audio_frame`]; `None` serves the historical video-only
    /// stream, byte-identical to before this track was added.
    pub async fn start(bind_addr: SocketAddr, frame_rate: u32, audio: Option<rtp_out::AudioCodec>) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("binding RTSP listener on {bind_addr}"))?;
        let rtp_socket = UdpSocket::bind((bind_addr.ip(), 0))
            .await
            .context("binding RTP UDP socket")?;

        let server = Self::new(rtp_socket, frame_rate, audio);

        let accept_server = server.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, peer_addr)) => {
                        let server = accept_server.clone();
                        tokio::spawn(async move {
                            handle_connection(server, socket, peer_addr).await;
                        });
                    }
                    Err(error) => warn!(%error, "rtsp accept failed"),
                }
            }
        });

        let dummy_server = server.clone();
        let dummy_task = tokio::spawn(async move {
            dummy_keepalive_loop(dummy_server).await;
        });

        let sr_server = server.clone();
        let sr_task = tokio::spawn(async move {
            sender_report_loop(sr_server).await;
        });

        server
            .background_tasks
            .lock()
            .expect("background_tasks mutex poisoned")
            .extend([accept_task, dummy_task, sr_task]);

        Ok(server)
    }

    /// Stop accepting connections and stop the keepalive/SR loops. Existing
    /// client connections are dropped when their tasks next try to use
    /// state that's gone (handled by simply aborting all our own tasks).
    pub async fn stop(&self) {
        let tasks = std::mem::take(
            &mut *self
                .background_tasks
                .lock()
                .expect("background_tasks mutex poisoned"),
        );
        for task in tasks {
            task.abort();
        }
    }

    pub async fn client_count(&self) -> usize {
        self.inner
            .lock()
            .await
            .sessions
            .values()
            .filter(|session| session.playing)
            .count()
    }

    /// Cache the latest SPS NAL (used for SDP `sprop-parameter-sets`).
    pub async fn update_sps(&self, sps: Vec<u8>) {
        self.inner.lock().await.sps = Some(sps);
    }

    /// Cache the latest PPS NAL (used for SDP `sprop-parameter-sets`).
    pub async fn update_pps(&self, pps: Vec<u8>) {
        self.inner.lock().await.pps = Some(pps);
    }

    /// Packetize one Annex-B access unit and fan it out as RTP to every
    /// playing session's video transport; also marks real data as flowing
    /// so the dummy keepalive loop stands down while frames keep arriving.
    pub async fn send_video_access_unit(&self, annex_b: &[u8]) {
        let mut inner = self.inner.lock().await;
        inner.last_real_au = Some(Instant::now());

        let payloads = inner.payloader.payload(annex_b);
        if payloads.is_empty() {
            return;
        }

        let ts = inner.real_ts;
        let step = rtp_out::CLOCK_RATE / self.frame_rate.max(1);
        inner.real_ts = inner.real_ts.wrapping_add(step);

        let last = payloads.len() - 1;
        for (i, payload) in payloads.iter().enumerate() {
            let seq = inner.real_seq;
            inner.real_seq = inner.real_seq.wrapping_add(1);
            let bytes = rtp_out::serialize_rtp_packet(
                rtp_out::PAYLOAD_TYPE_H264,
                rtp_out::VIDEO_SSRC,
                RtpHeaderFields {
                    sequence_number: seq,
                    timestamp: ts,
                    marker: i == last,
                },
                payload,
            );
            record_stats(&mut inner.video_stats, ts, payload.len());
            Self::dispatch(&inner.sessions, &self.rtp_socket, Media::Video, &bytes).await;
        }
    }

    /// Like [`Self::send_video_access_unit`], but with an explicit source
    /// RTP timestamp (90 kHz) instead of frame-rate stepping -- used by the
    /// relay backend, whose frames carry the publisher's own pacing. The
    /// timestamp is rebased into this server's timestamp space so it keeps
    /// advancing sanely even across publisher restarts or clock jumps (see
    /// [`rebase_timestamp`]).
    pub async fn send_video_access_unit_at(&self, annex_b: &[u8], source_rtp_ts: u32) {
        let mut inner = self.inner.lock().await;
        inner.last_real_au = Some(Instant::now());

        let payloads = inner.payloader.payload(annex_b);
        if payloads.is_empty() {
            return;
        }

        let step = rtp_out::CLOCK_RATE / self.frame_rate.max(1);
        let mut last_source = inner.last_source_video_ts;
        let mut output_ts = inner.real_ts;
        let ts = rebase_timestamp(&mut last_source, &mut output_ts, source_rtp_ts, rtp_out::CLOCK_RATE, step);
        inner.last_source_video_ts = last_source;
        inner.real_ts = output_ts;

        let last = payloads.len() - 1;
        for (i, payload) in payloads.iter().enumerate() {
            let seq = inner.real_seq;
            inner.real_seq = inner.real_seq.wrapping_add(1);
            let bytes = rtp_out::serialize_rtp_packet(
                rtp_out::PAYLOAD_TYPE_H264,
                rtp_out::VIDEO_SSRC,
                RtpHeaderFields {
                    sequence_number: seq,
                    timestamp: ts,
                    marker: i == last,
                },
                payload,
            );
            record_stats(&mut inner.video_stats, ts, payload.len());
            Self::dispatch(&inner.sessions, &self.rtp_socket, Media::Video, &bytes).await;
        }
    }

    /// Send one encoded audio frame (AAC raw frame or Opus packet, per the
    /// configured audio codec) with its source RTP timestamp (48 kHz
    /// clock), rebased the same way as video. No-op if this server wasn't
    /// configured with an audio codec.
    pub async fn send_audio_frame(&self, frame: &[u8], source_rtp_ts: u32) {
        let Some(codec) = self.audio else {
            return;
        };

        let mut inner = self.inner.lock().await;

        let mut last_source = inner.last_source_audio_ts;
        let mut output_ts = inner.audio_ts;
        let ts = rebase_timestamp(
            &mut last_source,
            &mut output_ts,
            source_rtp_ts,
            rtp_out::AUDIO_CLOCK_RATE,
            AUDIO_NOMINAL_STEP,
        );
        inner.last_source_audio_ts = last_source;
        inner.audio_ts = output_ts;

        let seq = inner.audio_seq;
        inner.audio_seq = inner.audio_seq.wrapping_add(1);

        let (payload_type, payload): (u8, Vec<u8>) = match codec {
            rtp_out::AudioCodec::Aac => {
                let header = rtp_out::aac_au_header(frame.len());
                let mut buf = Vec::with_capacity(header.len() + frame.len());
                buf.extend_from_slice(&header);
                buf.extend_from_slice(frame);
                (rtp_out::PAYLOAD_TYPE_AAC, buf)
            }
            rtp_out::AudioCodec::Opus => (rtp_out::PAYLOAD_TYPE_OPUS, frame.to_vec()),
        };

        let bytes = rtp_out::serialize_rtp_packet(
            payload_type,
            rtp_out::AUDIO_SSRC,
            RtpHeaderFields {
                sequence_number: seq,
                timestamp: ts,
                marker: false,
            },
            &payload,
        );
        record_stats(&mut inner.audio_stats, ts, payload.len());
        Self::dispatch(&inner.sessions, &self.rtp_socket, Media::Audio, &bytes).await;
    }

    async fn build_sdp(&self) -> String {
        let (sps, pps) = {
            let inner = self.inner.lock().await;
            match (&inner.sps, &inner.pps) {
                (Some(sps), Some(pps)) => (sps.clone(), pps.clone()),
                _ => (rtp_out::DUMMY_SPS.to_vec(), rtp_out::DUMMY_PPS.to_vec()),
            }
        };

        use base64::Engine as _;
        let sps_b64 = base64::engine::general_purpose::STANDARD.encode(&sps);
        let pps_b64 = base64::engine::general_purpose::STANDARD.encode(&pps);

        let mut sdp = format!(
            "v=0\r\n\
             o=- 0 0 IN IP4 127.0.0.1\r\n\
             s=mistl\r\n\
             c=IN IP4 0.0.0.0\r\n\
             t=0 0\r\n\
             a=tool:mistl\r\n\
             a=control:*\r\n\
             a=recvonly\r\n\
             m=video 0 RTP/AVP {pt}\r\n\
             a=rtpmap:{pt} H264/{clock_rate}\r\n\
             a=fmtp:{pt} packetization-mode=1;sprop-parameter-sets={sps_b64},{pps_b64}\r\n",
            pt = rtp_out::PAYLOAD_TYPE_H264,
            clock_rate = rtp_out::CLOCK_RATE,
        );

        // Video-only mode must serve byte-identical SDP to the historical
        // single-track format -- no trailing trackID line, nothing else
        // appended below.
        if let Some(codec) = self.audio {
            sdp.push_str("a=control:trackID=0\r\n");
            match codec {
                rtp_out::AudioCodec::Aac => {
                    sdp.push_str(&format!(
                        "m=audio 0 RTP/AVP {pt}\r\n\
                         a=rtpmap:{pt} mpeg4-generic/48000/2\r\n\
                         a=fmtp:{pt} profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3;config=1190\r\n\
                         a=control:trackID=1\r\n",
                        pt = rtp_out::PAYLOAD_TYPE_AAC,
                    ));
                }
                rtp_out::AudioCodec::Opus => {
                    sdp.push_str(&format!(
                        "m=audio 0 RTP/AVP {pt}\r\n\
                         a=rtpmap:{pt} opus/48000/2\r\n\
                         a=control:trackID=1\r\n",
                        pt = rtp_out::PAYLOAD_TYPE_OPUS,
                    ));
                }
            }
        }

        sdp
    }

    /// Send `bytes` (a full RTP packet) to every playing session's
    /// transport for `media`, over UDP or wrapped as an RTSP interleaved
    /// `Data` frame over TCP.
    async fn dispatch(sessions: &HashMap<String, Session>, rtp_socket: &UdpSocket, media: Media, bytes: &[u8]) {
        for session in sessions.values() {
            if !session.playing {
                continue;
            }
            let transport = match media {
                Media::Video => session.video.as_ref(),
                Media::Audio => session.audio.as_ref(),
            };
            let Some(transport) = transport else {
                continue;
            };
            match transport {
                SessionTransport::Udp { rtp_addr, .. } => {
                    let _ = rtp_socket.send_to(bytes, *rtp_addr).await;
                }
                SessionTransport::Tcp { rtp_channel, tx, .. } => {
                    let mut framed = Vec::with_capacity(4 + bytes.len());
                    framed.push(b'$');
                    framed.push(*rtp_channel);
                    framed.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                    framed.extend_from_slice(bytes);
                    let _ = tx.send(framed);
                }
            }
        }
    }

    /// Send `bytes` (a full RTCP packet) to one session's transport for one
    /// media, over UDP (to the stored client RTCP address) or interleaved
    /// over TCP (on that media's RTCP channel).
    async fn dispatch_rtcp(transport: &SessionTransport, rtp_socket: &UdpSocket, bytes: &[u8]) {
        match transport {
            SessionTransport::Udp { rtcp_addr, .. } => {
                let _ = rtp_socket.send_to(bytes, *rtcp_addr).await;
            }
            SessionTransport::Tcp { rtcp_channel, tx, .. } => {
                let mut framed = Vec::with_capacity(4 + bytes.len());
                framed.push(b'$');
                framed.push(*rtcp_channel);
                framed.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                framed.extend_from_slice(bytes);
                let _ = tx.send(framed);
            }
        }
    }
}

/// Bump `stats` after emitting one RTP packet for a media: packet/octet
/// counts and the wall-clock anchor used to extrapolate the SR's RTP
/// timestamp between real sends.
fn record_stats(stats: &mut MediaStats, ts: u32, payload_len: usize) {
    stats.packet_count = stats.packet_count.wrapping_add(1);
    stats.octet_count = stats.octet_count.wrapping_add(payload_len as u32);
    stats.last_ts = ts;
    stats.last_ts_at = Some(Instant::now());
    stats.active = true;
}

/// Rebase a publisher's own RTP timestamp into this server's output
/// timestamp space. `last_source_ts` is the previous call's `source_ts`
/// (`None` on the first call); `output_ts` is this media's running output
/// timestamp counter.
///
/// `delta = source_ts.wrapping_sub(last_source_ts)`, reinterpreted as a
/// signed 32-bit delta: if it's negative (time went backwards -- clock
/// jitter or reordering) or larger than `clamp` (a discontinuity, e.g. the
/// publisher restarted its RTP timestamp base), `nominal_step` is
/// substituted instead so playback keeps advancing sanely. The delta is
/// then added to `output_ts`.
///
/// On the first call (`last_source_ts` is `None`) no delta is applied --
/// `output_ts` is returned unchanged so video continues seamlessly from
/// whatever timestamp space the keepalive/frame-stepped path was already
/// in, and `last_source_ts` is just seeded for the next call.
fn rebase_timestamp(
    last_source_ts: &mut Option<u32>,
    output_ts: &mut u32,
    source_ts: u32,
    clamp: u32,
    nominal_step: u32,
) -> u32 {
    if let Some(last) = *last_source_ts {
        let delta_raw = source_ts.wrapping_sub(last) as i32;
        let delta = if delta_raw < 0 || (delta_raw as u32) > clamp {
            nominal_step
        } else {
            delta_raw as u32
        };
        *output_ts = output_ts.wrapping_add(delta);
    }
    *last_source_ts = Some(source_ts);
    *output_ts
}

/// Mirrors mistlink's `dummyPacketLoop`: while no real video is flowing,
/// periodically emit a filler NALU RTP packet so AVPro's session doesn't
/// time out. Cadence backs off once a client is actually connected (500ms)
/// versus idle (100ms), matching the Go reference exactly. Video-only --
/// mistlink has no audio keepalive, so audio transports never see this
/// traffic.
async fn dummy_keepalive_loop(server: Arc<RtspServer>) {
    let mut current_interval = Duration::from_millis(100);
    let mut ticker = tokio::time::interval(current_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let mut inner = server.inner.lock().await;

        let client_count = inner.sessions.values().filter(|s| s.playing).count();
        let target_interval = if client_count > 0 {
            Duration::from_millis(500)
        } else {
            Duration::from_millis(100)
        };
        if target_interval != current_interval {
            current_interval = target_interval;
            ticker = tokio::time::interval(current_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        }

        if inner
            .last_real_au
            .is_some_and(|at| at.elapsed() < REAL_DATA_IDLE)
        {
            continue;
        }

        let seq = inner.dummy_seq;
        inner.dummy_seq = inner.dummy_seq.wrapping_add(1);
        let ts = inner.dummy_ts;
        inner.dummy_ts = inner.dummy_ts.wrapping_add(rtp_out::DUMMY_TIMESTAMP_INCREMENT);

        let bytes = rtp_out::serialize_rtp_packet(
            rtp_out::PAYLOAD_TYPE_H264,
            rtp_out::VIDEO_SSRC,
            RtpHeaderFields {
                sequence_number: seq,
                timestamp: ts,
                marker: true,
            },
            DUMMY_NALU,
        );
        record_stats(&mut inner.video_stats, ts, DUMMY_NALU.len());
        RtspServer::dispatch(&inner.sessions, &server.rtp_socket, Media::Video, &bytes).await;
    }
}

/// Every [`SR_INTERVAL`], send an RTCP Sender Report to each playing
/// session's active media transports. Skips a media entirely once for the
/// whole tick if it hasn't sent a single packet yet (a fresh audio track
/// before the first encoded frame, for instance).
async fn sender_report_loop(server: Arc<RtspServer>) {
    let mut ticker = tokio::time::interval(SR_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let inner = server.inner.lock().await;

        let video_sr = inner
            .video_stats
            .active
            .then(|| build_sender_report(&inner.video_stats, rtp_out::VIDEO_SSRC, rtp_out::CLOCK_RATE));
        let audio_sr = inner
            .audio_stats
            .active
            .then(|| build_sender_report(&inner.audio_stats, rtp_out::AUDIO_SSRC, rtp_out::AUDIO_CLOCK_RATE));

        for session in inner.sessions.values() {
            if !session.playing {
                continue;
            }
            if let (Some(bytes), Some(transport)) = (&video_sr, &session.video) {
                RtspServer::dispatch_rtcp(transport, &server.rtp_socket, bytes).await;
            }
            if let (Some(bytes), Some(transport)) = (&audio_sr, &session.audio) {
                RtspServer::dispatch_rtcp(transport, &server.rtp_socket, bytes).await;
            }
        }
    }
}

/// Build one media's Sender Report: the RTP timestamp is extrapolated from
/// the last packet actually sent, by elapsed wall-clock time at that
/// media's clock rate.
fn build_sender_report(stats: &MediaStats, ssrc: u32, clock_rate: u32) -> Vec<u8> {
    let elapsed = stats.last_ts_at.map(|at| at.elapsed()).unwrap_or_default();
    let extrapolated = (elapsed.as_secs_f64() * clock_rate as f64) as u32;
    let rtp_timestamp = stats.last_ts.wrapping_add(extrapolated);
    let (ntp_seconds, ntp_fraction) = rtp_out::ntp_now();
    rtp_out::serialize_sender_report(
        ssrc,
        ntp_seconds,
        ntp_fraction,
        rtp_timestamp,
        stats.packet_count,
        stats.octet_count,
    )
    .to_vec()
}

/// Reads RTSP requests off one TCP connection, dispatches them, and writes
/// back responses (and, once a TCP-interleaved SETUP has happened, RTP/RTCP
/// `Data` frames pushed from elsewhere via `tcp_tx`).
async fn handle_connection(server: Arc<RtspServer>, stream: TcpStream, peer_addr: SocketAddr) {
    let (mut read_half, write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let writer_task = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(bytes) = rx.recv().await {
            if write_half.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let mut buf: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 8192];
    let mut session_ids: Vec<String> = Vec::new();

    'conn: loop {
        loop {
            match rtsp_types::Message::<Vec<u8>>::parse(&buf) {
                Ok((message, consumed)) => {
                    buf.drain(0..consumed);
                    if let rtsp_types::Message::Request(request) = message {
                        let (response, new_session) =
                            handle_request(&server, &request, peer_addr, tx.clone()).await;
                        if let Some(id) = new_session {
                            session_ids.push(id);
                        }
                        let mut out = Vec::new();
                        if response.write(&mut out).is_err() || tx.send(out).is_err() {
                            break 'conn;
                        }
                    }
                    // Data/Response messages from the client (e.g. RTCP
                    // sent over an interleaved channel) are ignored.
                }
                Err(rtsp_types::ParseError::Incomplete(_)) => break,
                Err(rtsp_types::ParseError::Error) => {
                    debug!(%peer_addr, "bad RTSP message, closing connection");
                    break 'conn;
                }
            }
        }

        match read_half.read(&mut read_buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&read_buf[..n]),
        }
    }

    {
        let mut inner = server.inner.lock().await;
        for id in &session_ids {
            inner.sessions.remove(id);
        }
    }
    writer_task.abort();
}

fn base_response(version: Version, status: StatusCode, cseq: Option<headers::CSeq>) -> rtsp_types::ResponseBuilder {
    let mut builder = Response::builder(version, status);
    if let Some(cseq) = cseq {
        builder = builder.typed_header(&cseq);
    }
    builder
}

/// Handles one parsed RTSP request, returning the response to send and,
/// for a `SETUP` that allocated a brand new session (i.e. not the second
/// `SETUP` attaching to an existing one), the newly-allocated session id
/// (so the connection task can clean it up on disconnect).
async fn handle_request(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    peer_addr: SocketAddr,
    tcp_tx: mpsc::UnboundedSender<Vec<u8>>,
) -> (Response<Vec<u8>>, Option<String>) {
    let version = request.version();
    let cseq = request.typed_header::<headers::CSeq>().ok().flatten();

    match request.method() {
        Method::Options => {
            let response = base_response(version, StatusCode::Ok, cseq)
                .typed_header(
                    &headers::Public::builder()
                        .method(Method::Options)
                        .method(Method::Describe)
                        .method(Method::Setup)
                        .method(Method::Play)
                        .method(Method::Teardown)
                        .build(),
                )
                .build(Vec::new());
            (response, None)
        }
        Method::Describe => {
            let sdp = server.build_sdp().await;
            let mut builder =
                base_response(version, StatusCode::Ok, cseq).header(headers::CONTENT_TYPE, "application/sdp");
            if let Some(uri) = request.request_uri() {
                builder = builder.header(headers::CONTENT_BASE, uri.as_str().to_string());
            }
            (builder.build(sdp.into_bytes()), None)
        }
        Method::Setup => handle_setup(server, request, peer_addr, tcp_tx, version, cseq).await,
        Method::Play => handle_play(server, request, version, cseq).await,
        Method::Teardown => handle_teardown(server, request, version, cseq).await,
        Method::GetParameter => (base_response(version, StatusCode::Ok, cseq).build(Vec::new()), None),
        _ => (
            base_response(version, StatusCode::MethodNotAllowed, cseq).build(Vec::new()),
            None,
        ),
    }
}

/// Reads a trailing `trackID=`/`streamid=` off the request URI (as emitted
/// in this server's own SDP `a=control` lines) to tell which media a
/// `SETUP` targets. Absent, unparsable, or anything other than `1` means
/// video -- this keeps video-only clients (which never see a `trackID` in
/// the SDP at all) working unchanged.
fn parse_track_kind(request: &Request<Vec<u8>>) -> Media {
    let Some(uri) = request.request_uri() else {
        return Media::Video;
    };
    let lower = uri.as_str().to_ascii_lowercase();
    for key in ["trackid=", "streamid="] {
        if let Some(idx) = lower.rfind(key) {
            let rest = &lower[idx + key.len()..];
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse::<u32>() {
                return if n == 1 { Media::Audio } else { Media::Video };
            }
        }
    }
    Media::Video
}

async fn handle_setup(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    peer_addr: SocketAddr,
    tcp_tx: mpsc::UnboundedSender<Vec<u8>>,
    version: Version,
    cseq: Option<headers::CSeq>,
) -> (Response<Vec<u8>>, Option<String>) {
    let Ok(Some(transports)) = request.typed_header::<Transports>() else {
        return (
            base_response(version, StatusCode::UnsupportedTransport, cseq).build(Vec::new()),
            None,
        );
    };

    let rtp_transport = transports.iter().find_map(|t| match t {
        Transport::Rtp(rtp) => Some(rtp.clone()),
        Transport::Other(_) => None,
    });

    let Some(rtp_transport) = rtp_transport else {
        return (
            base_response(version, StatusCode::UnsupportedTransport, cseq).build(Vec::new()),
            None,
        );
    };

    let (session_transport, response_transport) = if let Some(interleaved) = rtp_transport.params.interleaved {
        let rtp_channel = interleaved.0;
        let rtcp_channel = interleaved.1.unwrap_or(rtp_channel.wrapping_add(1));
        let session_transport = SessionTransport::Tcp {
            rtp_channel,
            rtcp_channel,
            tx: tcp_tx,
        };
        let response_transport = Transports::from(vec![Transport::Rtp(RtpTransport {
            profile: RtpProfile::Avp,
            lower_transport: Some(RtpLowerTransport::Tcp),
            params: RtpTransportParameters {
                unicast: true,
                interleaved: Some(interleaved),
                ..Default::default()
            },
        })]);
        (session_transport, response_transport)
    } else if let Some(client_port) = rtp_transport.params.client_port {
        let rtp_addr = SocketAddr::new(peer_addr.ip(), client_port.0);
        let rtcp_addr = SocketAddr::new(peer_addr.ip(), client_port.1.unwrap_or(client_port.0.wrapping_add(1)));
        let server_rtp_port = server.rtp_socket.local_addr().map(|a| a.port()).unwrap_or(0);
        let session_transport = SessionTransport::Udp { rtp_addr, rtcp_addr };
        let response_transport = Transports::from(vec![Transport::Rtp(RtpTransport {
            profile: RtpProfile::Avp,
            lower_transport: Some(RtpLowerTransport::Udp),
            params: RtpTransportParameters {
                unicast: true,
                client_port: Some(client_port),
                server_port: Some((server_rtp_port, Some(server_rtp_port + 1))),
                ..Default::default()
            },
        })]);
        (session_transport, response_transport)
    } else {
        return (
            base_response(version, StatusCode::UnsupportedTransport, cseq).build(Vec::new()),
            None,
        );
    };

    let track_kind = parse_track_kind(request);
    let requested_session = request.typed_header::<headers::Session>().ok().flatten();

    let mut inner = server.inner.lock().await;

    let session_id = if let Some(existing) = &requested_session {
        if !inner.sessions.contains_key(existing.0.as_str()) {
            return (
                base_response(version, StatusCode::SessionNotFound, cseq).build(Vec::new()),
                None,
            );
        }
        existing.0.clone()
    } else {
        let id = new_session_id();
        inner.sessions.insert(
            id.clone(),
            Session {
                video: None,
                audio: None,
                playing: false,
            },
        );
        id
    };
    let is_new_session = requested_session.is_none();

    {
        let session = inner
            .sessions
            .get_mut(&session_id)
            .expect("session just inserted or looked up above");
        match track_kind {
            Media::Video => session.video = Some(session_transport),
            Media::Audio => session.audio = Some(session_transport),
        }
    }
    drop(inner);

    let response = base_response(version, StatusCode::Ok, cseq)
        .typed_header(&response_transport)
        .typed_header(&headers::Session::with_timeout(session_id.clone(), 60))
        .build(Vec::new());
    (response, if is_new_session { Some(session_id) } else { None })
}

async fn handle_play(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    version: Version,
    cseq: Option<headers::CSeq>,
) -> (Response<Vec<u8>>, Option<String>) {
    let Ok(Some(session)) = request.typed_header::<headers::Session>() else {
        return (
            base_response(version, StatusCode::SessionNotFound, cseq).build(Vec::new()),
            None,
        );
    };

    let mut inner = server.inner.lock().await;
    let Some(entry) = inner.sessions.get_mut(session.0.as_str()) else {
        return (
            base_response(version, StatusCode::SessionNotFound, cseq).build(Vec::new()),
            None,
        );
    };
    entry.playing = true;
    drop(inner);

    let response = base_response(version, StatusCode::Ok, cseq)
        .typed_header(&headers::Session::from(session.0))
        .build(Vec::new());
    (response, None)
}

async fn handle_teardown(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    version: Version,
    cseq: Option<headers::CSeq>,
) -> (Response<Vec<u8>>, Option<String>) {
    if let Ok(Some(session)) = request.typed_header::<headers::Session>() {
        let mut inner = server.inner.lock().await;
        inner.sessions.remove(session.0.as_str());
    }
    (base_response(version, StatusCode::Ok, cseq).build(Vec::new()), None)
}

fn new_session_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtsp_types::Url;

    async fn test_server() -> Arc<RtspServer> {
        let rtp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        RtspServer::new(rtp_socket, 30, None)
    }

    async fn test_server_with_audio(codec: rtp_out::AudioCodec) -> Arc<RtspServer> {
        let rtp_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        RtspServer::new(rtp_socket, 30, Some(codec))
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:5555".parse().unwrap()
    }

    #[tokio::test]
    async fn options_lists_supported_methods() {
        let server = test_server().await;
        let request = Request::builder(Method::Options, Version::V1_0)
            .header(headers::CSEQ, "1")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let (response, session) = handle_request(&server, &request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::Ok);
        assert!(session.is_none());
        let public = response.typed_header::<headers::Public>().unwrap().unwrap();
        assert!(public.contains(&Method::Describe));
        assert!(public.contains(&Method::Setup));
        assert!(public.contains(&Method::Play));
        assert!(public.contains(&Method::Teardown));
    }

    #[tokio::test]
    async fn describe_returns_sdp_with_h264_media_and_dummy_params() {
        let server = test_server().await;
        let request = Request::builder(Method::Describe, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream").unwrap())
            .header(headers::CSEQ, "2")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let (response, session) = handle_request(&server, &request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::Ok);
        assert!(session.is_none());
        assert_eq!(
            response.header(&headers::CONTENT_TYPE).map(|v| v.as_str()),
            Some("application/sdp")
        );
        let body = String::from_utf8(response.body().clone()).unwrap();
        assert!(body.contains("m=video 0 RTP/AVP 96"));
        assert!(body.contains(&format!("rtpmap:{} H264/90000", rtp_out::PAYLOAD_TYPE_H264)));
        assert!(body.contains("sprop-parameter-sets="));
    }

    #[tokio::test]
    async fn video_only_sdp_matches_historical_format_exactly() {
        use base64::Engine as _;

        let server = test_server().await;
        server.update_sps(vec![0x67, 0x42, 0x00, 0x0a]).await;
        server.update_pps(vec![0x68, 0xce, 0x3c, 0x80]).await;

        let sdp = server.build_sdp().await;

        let sps_b64 = base64::engine::general_purpose::STANDARD.encode([0x67, 0x42, 0x00, 0x0a]);
        let pps_b64 = base64::engine::general_purpose::STANDARD.encode([0x68, 0xce, 0x3c, 0x80]);
        let expected = format!(
            "v=0\r\n\
             o=- 0 0 IN IP4 127.0.0.1\r\n\
             s=mistl\r\n\
             c=IN IP4 0.0.0.0\r\n\
             t=0 0\r\n\
             a=tool:mistl\r\n\
             a=control:*\r\n\
             a=recvonly\r\n\
             m=video 0 RTP/AVP 96\r\n\
             a=rtpmap:96 H264/90000\r\n\
             a=fmtp:96 packetization-mode=1;sprop-parameter-sets={sps_b64},{pps_b64}\r\n",
        );

        assert_eq!(sdp, expected);
        assert!(!sdp.contains("trackID"));
        assert!(!sdp.contains("m=audio"));
    }

    #[tokio::test]
    async fn two_track_sdp_includes_aac_audio_section() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Aac).await;
        let sdp = server.build_sdp().await;

        assert!(sdp.contains("m=video 0 RTP/AVP 96\r\n"));
        assert!(sdp.contains("a=control:trackID=0\r\n"));
        assert!(sdp.contains("m=audio 0 RTP/AVP 112\r\n"));
        assert!(sdp.contains("a=rtpmap:112 mpeg4-generic/48000/2\r\n"));
        assert!(sdp.contains(
            "a=fmtp:112 profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3;config=1190\r\n"
        ));
        assert!(sdp.contains("a=control:trackID=1\r\n"));
    }

    #[tokio::test]
    async fn two_track_sdp_includes_opus_audio_section() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Opus).await;
        let sdp = server.build_sdp().await;

        assert!(sdp.contains("a=control:trackID=0\r\n"));
        assert!(sdp.contains("m=audio 0 RTP/AVP 111\r\n"));
        assert!(sdp.contains("a=rtpmap:111 opus/48000/2\r\n"));
        assert!(sdp.contains("a=control:trackID=1\r\n"));
        assert!(!sdp.contains("mpeg4-generic"));
    }

    #[test]
    fn parse_track_kind_variants() {
        let video_uris = [
            "rtsp://host/stream",
            "rtsp://host/stream/trackID=0",
            "rtsp://host/stream/streamid=0",
            "rtsp://host/stream/TrackID=0",
        ];
        for uri in video_uris {
            let request = Request::builder(Method::Setup, Version::V1_0)
                .request_uri(Url::parse(uri).unwrap())
                .build(Vec::new());
            assert_eq!(parse_track_kind(&request), Media::Video, "uri={uri}");
        }

        let audio_uris = [
            "rtsp://host/stream/trackID=1",
            "rtsp://host/stream/streamid=1",
            "rtsp://host/stream/TrackID=1",
        ];
        for uri in audio_uris {
            let request = Request::builder(Method::Setup, Version::V1_0)
                .request_uri(Url::parse(uri).unwrap())
                .build(Vec::new());
            assert_eq!(parse_track_kind(&request), Media::Audio, "uri={uri}");
        }
    }

    #[test]
    fn rebase_timestamp_normal_delta() {
        let mut last = Some(1000u32);
        let mut output = 50_000u32;
        let ts = rebase_timestamp(&mut last, &mut output, 1000 + 3000, 90_000, 3000);
        assert_eq!(ts, 53_000);
        assert_eq!(output, 53_000);
        assert_eq!(last, Some(4000));
    }

    #[test]
    fn rebase_timestamp_wraparound_is_a_small_positive_delta() {
        let mut last = Some(u32::MAX - 5);
        let mut output = 1000u32;
        let source = 10u32; // wrapped past u32::MAX
        let ts = rebase_timestamp(&mut last, &mut output, source, 90_000, 3000);
        assert_eq!(ts, 1016); // delta = 16 (5 back to MAX, then +11 to reach 10)
        assert_eq!(last, Some(10));
    }

    #[test]
    fn rebase_timestamp_clamps_large_forward_jump() {
        let mut last = Some(1000u32);
        let mut output = 50_000u32;
        let source = 1000 + 200_000; // exceeds the 90_000 clamp
        let ts = rebase_timestamp(&mut last, &mut output, source, 90_000, 3000);
        assert_eq!(ts, 53_000); // nominal step substituted instead of the raw jump
    }

    #[test]
    fn rebase_timestamp_negative_delta_substitutes_nominal_step() {
        let mut last = Some(5000u32);
        let mut output = 50_000u32;
        let source = 4000u32; // went backwards
        let ts = rebase_timestamp(&mut last, &mut output, source, 90_000, 3000);
        assert_eq!(ts, 53_000);
    }

    #[test]
    fn rebase_timestamp_first_call_seeds_without_advancing() {
        let mut last: Option<u32> = None;
        let mut output = 12_345u32;
        let ts = rebase_timestamp(&mut last, &mut output, 999_999, 90_000, 3000);
        assert_eq!(ts, 12_345); // continues seamlessly from the existing counter
        assert_eq!(output, 12_345);
        assert_eq!(last, Some(999_999));
    }

    #[tokio::test]
    async fn setup_udp_then_play_marks_session_playing() {
        let server = test_server().await;

        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5000-5001")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (response, session_id) = handle_request(&server, &setup_request, peer(), tx.clone()).await;
        assert_eq!(response.status(), StatusCode::Ok);
        let session_id = session_id.expect("SETUP should allocate a session id");

        {
            let inner = server.inner.lock().await;
            assert!(!inner.sessions.get(&session_id).unwrap().playing);
        }

        let play_request = Request::builder(Method::Play, Version::V1_0)
            .header(headers::CSEQ, "2")
            .header(headers::SESSION, session_id.clone())
            .build(Vec::new());
        let (response, _) = handle_request(&server, &play_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);

        let inner = server.inner.lock().await;
        assert!(inner.sessions.get(&session_id).unwrap().playing);
    }

    #[tokio::test]
    async fn setup_tcp_interleaved_is_accepted() {
        let server = test_server().await;
        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP/TCP;unicast;interleaved=0-1")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (response, session_id) = handle_request(&server, &setup_request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::Ok);
        assert!(session_id.is_some());
        let transport_header = response.header(&headers::TRANSPORT).unwrap().as_str();
        assert!(transport_header.contains("interleaved=0-1"));
    }

    #[tokio::test]
    async fn setup_video_then_audio_attaches_to_same_session() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Opus).await;

        let video_setup = Request::builder(Method::Setup, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/trackID=0").unwrap())
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5000-5001")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (response, session_id) = handle_request(&server, &video_setup, peer(), tx.clone()).await;
        assert_eq!(response.status(), StatusCode::Ok);
        let session_id = session_id.expect("first SETUP allocates a session");

        let audio_setup = Request::builder(Method::Setup, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/trackID=1").unwrap())
            .header(headers::CSEQ, "2")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5002-5003")
            .header(headers::SESSION, session_id.clone())
            .build(Vec::new());
        let (response2, session_id2) = handle_request(&server, &audio_setup, peer(), tx).await;
        assert_eq!(response2.status(), StatusCode::Ok);
        // The second SETUP attaches to the existing session rather than
        // allocating a new one.
        assert!(session_id2.is_none());

        let inner = server.inner.lock().await;
        assert_eq!(inner.sessions.len(), 1);
        let session = inner.sessions.get(&session_id).unwrap();
        assert!(session.video.is_some());
        assert!(session.audio.is_some());
    }

    #[tokio::test]
    async fn setup_with_unknown_session_id_is_rejected() {
        let server = test_server().await;
        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=6000-6001")
            .header(headers::SESSION, "does-not-exist".to_string())
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (response, session_id) = handle_request(&server, &setup_request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::SessionNotFound);
        assert!(session_id.is_none());
    }

    #[tokio::test]
    async fn teardown_removes_session() {
        let server = test_server().await;
        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=6000-6001")
            .build(Vec::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let (_, session_id) = handle_request(&server, &setup_request, peer(), tx.clone()).await;
        let session_id = session_id.unwrap();

        let teardown_request = Request::builder(Method::Teardown, Version::V1_0)
            .header(headers::CSEQ, "2")
            .header(headers::SESSION, session_id.clone())
            .build(Vec::new());
        let (response, _) = handle_request(&server, &teardown_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);

        let inner = server.inner.lock().await;
        assert!(!inner.sessions.contains_key(&session_id));
    }

    #[test]
    fn dummy_nal_constants_match_mistlink_reference() {
        assert_eq!(rtp_out::DUMMY_SPS, &[0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2]);
        assert_eq!(rtp_out::DUMMY_PPS, &[0x68, 0xce, 0x3c, 0x80]);
        assert_eq!(rtp_out::DUMMY_NALU, &[0x0c, 0xff, 0xff, 0xff]);
    }
}
