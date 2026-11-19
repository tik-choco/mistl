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
use rtsp_types::headers::{
    self, RtpLowerTransport, RtpProfile, RtpTransport, RtpTransportParameters, Transport,
    Transports,
};
use rtsp_types::{Method, Request, Response, StatusCode, Version};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::rtp_out::{self, DUMMY_NALU, RtpHeaderFields};

/// Maximum buffered size of an incomplete RTSP request; a client exceeding
/// it is disconnected instead of growing the buffer without bound.
const MAX_REQUEST_BUF: usize = 64 * 1024;

/// Maximum simultaneous RTSP client connections.
const MAX_CONNECTIONS: usize = 16;

/// How long a connection may sit on a partial request, or (before it has
/// created any session) send nothing at all, before it is dropped. Playing
/// connections that are between requests are deliberately not timed out.
const REQUEST_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-connection queue, large enough for several hundred-packet IDRs.
/// At the video MTU, 2,048 interleaved packets occupy about 2.75 MiB.
const WRITER_QUEUE_FRAMES: usize = 2048;

const UDP_VIDEO_BURST_PACKETS: usize = 16;
const UDP_VIDEO_BURST_GAP: Duration = Duration::from_micros(250);
const UDP_VIDEO_PACING_BUDGET: Duration = Duration::from_millis(4);

/// How long after the last real access unit the dummy keepalive resumes.
const REAL_DATA_IDLE: Duration = Duration::from_millis(1000);

/// How often RTCP Sender Reports go out per playing session/media, needed
/// for A/V lipsync (mistlink got these for free from gortsplib).
const SR_INTERVAL: Duration = Duration::from_secs(3);

/// Minimum spacing between periodic "RTP outflow summary" diagnostic logs
/// for a single session, so a fast-playing session doesn't spam the log at
/// frame rate. Diagnostic only -- see [`record_rtp_outflow`].
const RTP_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// Nominal audio timestamp step substituted when a source RTP timestamp
/// can't be trusted (see [`rebase_timestamp`]): a 20ms frame at the fixed
/// 48 kHz audio clock rate.
const AUDIO_NOMINAL_STEP: u32 = 960;

/// Fixed RTP timestamp step per emitted AAC-LC frame: AAC-LC is always 1024
/// samples per frame at the fixed 48 kHz clock this track uses (see
/// [`RtspServer::send_audio_frame`] for why this is stamped instead of the
/// source-derived rebase used for Opus).
const AAC_TIMESTAMP_STEP: u32 = 1024;

/// Which media track a session transport targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Media {
    Video,
    Audio,
}

/// Per-client RTSP session: up to one transport per media, and whether
/// `PLAY` has been issued.
#[derive(Default)]
struct Session {
    video: Option<SessionTransport>,
    audio: Option<SessionTransport>,
    playing: bool,
    /// A TCP access unit could not fit in the writer queue. Resume only once
    /// an entire IDR access unit fits.
    video_resyncing: bool,
    last_video_resync_log: Option<Instant>,
    /// RTP outflow diagnostics only (see [`record_rtp_outflow`]): running
    /// packet/byte counters and the last sequence number sent to this
    /// session, plus whether the first-packet-since-PLAY line has already
    /// been logged and when the last periodic summary line was logged.
    rtp_sent_packets: u64,
    rtp_sent_bytes: u64,
    rtp_last_seq: u16,
    rtp_first_logged: bool,
    rtp_last_summary_at: Option<Instant>,
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
        tx: mpsc::Sender<Vec<u8>>,
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
    /// The most recent access unit that contained an IDR (keyframe) NAL,
    /// cached so a client that `PLAY`s during a static period (no new
    /// frames from the damage-driven Windows.Graphics.Capture backend)
    /// still gets a keyframe immediately instead of black video until the
    /// next real IDR. Sending this on `PLAY` adds no latency to the live
    /// path -- it's an extra unicast send to the joining session only.
    last_idr_au: Option<Vec<u8>>,
    /// The single video sequence-number/timestamp space for the video SSRC.
    /// *Everything* sent on [`rtp_out::VIDEO_SSRC`] -- real access units,
    /// the join keyframe, and the dummy keepalive (see
    /// [`Inner::next_keepalive_packet`]) -- allocates from these two
    /// counters. They used to be split into separate `real_*`/`dummy_*`
    /// pairs, which meant every hand-off between the keepalive and the real
    /// stream (a >1s stall from a static screen, a gap-drop waiting for an
    /// IDR, a screen switch's PLI round trip) produced two abrupt, unrelated
    /// jumps in the same SSRC's sequence/timestamp space -- a classic way to
    /// wedge a player's RTP jitter buffer until a manual Resync. One shared
    /// space keeps seq monotonic (+1 per packet) and ts monotonic
    /// (nominal-stepped through idle periods) across those seams instead.
    real_seq: u16,
    real_ts: u32,
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
    /// When a real video access unit / audio frame was last actually
    /// dispatched to at least one playing session's transport (as opposed
    /// to merely having arrived from the browser) -- see
    /// [`RtspServer::delivered_flow_ages_ms`]. `None` if that media has
    /// never reached an actual RTSP client.
    last_delivered_video: Option<Instant>,
    last_delivered_audio: Option<Instant>,
}

impl Inner {
    fn serialize_video(&mut self, payloads: &[bytes::Bytes], ts: u32) -> Vec<Vec<u8>> {
        payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                let seq = self.real_seq;
                self.real_seq = self.real_seq.wrapping_add(1);
                record_stats(&mut self.video_stats, ts, payload.len());
                rtp_out::serialize_rtp_packet(
                    rtp_out::PAYLOAD_TYPE_H264,
                    rtp_out::VIDEO_SSRC,
                    RtpHeaderFields {
                        sequence_number: seq,
                        timestamp: ts,
                        marker: i + 1 == payloads.len(),
                    },
                    payload,
                )
            })
            .collect()
    }

    /// Builds the next dummy-keepalive RTP packet *in the same
    /// sequence/timestamp space as the real video stream*: it consumes the
    /// next `real_seq` and advances `real_ts` by the nominal
    /// [`rtp_out::DUMMY_TIMESTAMP_INCREMENT`] step (100ms at the 90 kHz
    /// clock -- the keepalive's own idle cadence), exactly the way
    /// [`rebase_timestamp`] substitutes a nominal step for the real path
    /// when a source timestamp can't be trusted. The keepalive/real-data
    /// hand-off is therefore seamless in both directions: the keepalive
    /// picks up one seq past the last real packet, and when real data
    /// resumes it picks up one seq past the last keepalive packet with a
    /// timestamp that kept advancing through the idle period, so a player's
    /// jitter buffer never sees the sequence/timestamp cliff that the old
    /// separate `dummy_seq`/`dummy_ts` counters produced at every seam.
    fn next_keepalive_packet(&mut self) -> Vec<u8> {
        let seq = self.real_seq;
        self.real_seq = self.real_seq.wrapping_add(1);
        let ts = self.real_ts;
        self.real_ts = self
            .real_ts
            .wrapping_add(rtp_out::DUMMY_TIMESTAMP_INCREMENT);

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
        record_stats(&mut self.video_stats, ts, DUMMY_NALU.len());
        bytes
    }
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
    /// Keep live video, join keyframes and filler in RTP order while UDP
    /// pacing releases `inner` for audio and RTSP requests.
    video_output: Mutex<()>,
    keyframe_requested: Notify,
    background_tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl RtspServer {
    fn new(
        rtp_socket: UdpSocket,
        frame_rate: u32,
        audio: Option<rtp_out::AudioCodec>,
    ) -> Arc<Self> {
        Arc::new(Self {
            rtp_socket,
            frame_rate,
            audio,
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
                sps: None,
                pps: None,
                last_real_au: None,
                last_idr_au: None,
                real_seq: rand::random(),
                real_ts: rand::random(),
                payloader: rtp_out::H264Rtp::new(),
                last_source_video_ts: None,
                audio_seq: rand::random(),
                audio_ts: rand::random(),
                last_source_audio_ts: None,
                video_stats: MediaStats::default(),
                audio_stats: MediaStats::default(),
                last_delivered_video: None,
                last_delivered_audio: None,
            }),
            video_output: Mutex::new(()),
            keyframe_requested: Notify::new(),
            background_tasks: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Bind the RTSP TCP listener and a shared RTP UDP socket on
    /// `bind_addr`'s IP, then spawn the accept loop, the dummy keepalive
    /// loop, and the RTCP Sender Report loop as background tasks. `audio`
    /// adds a second (audio) track to the SDP and enables
    /// [`Self::send_audio_frame`]; `None` serves the historical video-only
    /// stream, byte-identical to before this track was added.
    pub async fn start(
        bind_addr: SocketAddr,
        frame_rate: u32,
        audio: Option<rtp_out::AudioCodec>,
    ) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("binding RTSP listener on {bind_addr}"))?;
        let rtp_socket = UdpSocket::bind((bind_addr.ip(), 0))
            .await
            .context("binding RTP UDP socket")?;

        let server = Self::new(rtp_socket, frame_rate, audio);

        let accept_server = server.clone();
        let conn_slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, peer_addr)) => {
                        let Ok(permit) = conn_slots.clone().try_acquire_owned() else {
                            warn!(%peer_addr, "RTSP connection limit reached, rejecting");
                            continue;
                        };
                        let server = accept_server.clone();
                        tokio::spawn(async move {
                            handle_connection(server, socket, peer_addr).await;
                            drop(permit);
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

    /// Milliseconds since *real* media last flowed through this server:
    /// `(video_ms, audio_ms)`, each `None` if that media has never carried a
    /// frame. Video is driven by `last_real_au` (stamped only by the real
    /// access-unit paths -- the dummy keepalive deliberately does not count),
    /// audio by `audio_stats.last_ts_at` (audio has no keepalive path at
    /// all). Backs `stream.status`'s `flow` field, which the web dashboard
    /// uses to animate topology edges only while data is actually moving.
    pub async fn flow_ages_ms(&self) -> (Option<u64>, Option<u64>) {
        let inner = self.inner.lock().await;
        let video = inner.last_real_au.map(|at| at.elapsed().as_millis() as u64);
        let audio = inner
            .audio_stats
            .last_ts_at
            .map(|at| at.elapsed().as_millis() as u64);
        (video, audio)
    }

    /// Sibling of [`Self::flow_ages_ms`], but delivery-aware: milliseconds
    /// since a real video access unit / audio frame was last actually
    /// dispatched to at least one *playing* session's transport for that
    /// media, i.e. since an RTSP client (not just this server) plausibly
    /// received it -- `(video_ms, audio_ms)`, each `None` if that media has
    /// never been delivered to any client. `flow_ages_ms` only proves media
    /// arrived at this server *from* the browser; a stream with zero RTSP
    /// clients attached (or one that only ever SETUP'd the other track)
    /// reads as "flowing" there even though nothing has ever gone out.
    /// Backs `stream.status`'s `flow.video_delivered_age_ms` /
    /// `audio_delivered_age_ms`, which the web dashboard uses to tell
    /// "receiving, no client attached" apart from genuinely flowing to a
    /// client.
    pub async fn delivered_flow_ages_ms(&self) -> (Option<u64>, Option<u64>) {
        let inner = self.inner.lock().await;
        let video = inner
            .last_delivered_video
            .map(|at| at.elapsed().as_millis() as u64);
        let audio = inner
            .last_delivered_audio
            .map(|at| at.elapsed().as_millis() as u64);
        (video, audio)
    }

    /// Cache the latest SPS NAL (used for SDP `sprop-parameter-sets`).
    pub async fn update_sps(&self, sps: Vec<u8>) {
        self.inner.lock().await.sps = Some(sps);
    }

    /// Cache the latest PPS NAL (used for SDP `sprop-parameter-sets`).
    pub async fn update_pps(&self, pps: Vec<u8>) {
        self.inner.lock().await.pps = Some(pps);
    }

    pub(super) fn request_keyframe(&self) {
        self.keyframe_requested.notify_one();
    }

    pub(super) async fn wait_for_keyframe_request(&self) {
        self.keyframe_requested.notified().await;
    }

    /// Packetize one Annex-B access unit and fan it out as RTP to every
    /// playing session's video transport; also marks real data as flowing
    /// so the dummy keepalive loop stands down while frames keep arriving.
    pub async fn send_video_access_unit(&self, annex_b: &[u8]) {
        self.send_video(annex_b, None).await;
    }

    /// Like [`Self::send_video_access_unit`], but with an explicit source
    /// RTP timestamp (90 kHz) instead of frame-rate stepping -- used by the
    /// relay backend, whose frames carry the publisher's own pacing. The
    /// timestamp is rebased into this server's timestamp space so it keeps
    /// advancing sanely even across publisher restarts or clock jumps (see
    /// [`rebase_timestamp`]).
    pub async fn send_video_access_unit_at(&self, annex_b: &[u8], source_rtp_ts: u32) {
        self.send_video(annex_b, Some(source_rtp_ts)).await;
    }

    async fn send_video(&self, annex_b: &[u8], source_rtp_ts: Option<u32>) {
        let _output = self.video_output.lock().await;
        let mut inner = self.inner.lock().await;
        inner.last_real_au = Some(Instant::now());
        let is_idr = au_contains_idr(annex_b);
        if is_idr {
            inner.last_idr_au = Some(annex_b.to_vec());
        }

        let payloads = inner.payloader.payload(annex_b);
        if payloads.is_empty() {
            return;
        }

        let step = rtp_out::CLOCK_RATE / self.frame_rate.max(1);
        let ts = if let Some(source_rtp_ts) = source_rtp_ts {
            let Inner {
                last_source_video_ts,
                real_ts,
                ..
            } = &mut *inner;
            rebase_timestamp(
                last_source_video_ts,
                real_ts,
                source_rtp_ts,
                rtp_out::CLOCK_RATE,
                step,
            )
        } else {
            let ts = inner.real_ts;
            inner.real_ts = ts.wrapping_add(step);
            ts
        };
        let packets = inner.serialize_video(&payloads, ts);
        let (tcp_delivered, udp_targets) = self.stage_video(&mut inner, &packets, is_idr, None);
        if tcp_delivered {
            inner.last_delivered_video = Some(Instant::now());
        }
        drop(inner);
        if self.send_udp_video(&packets, &udp_targets).await {
            self.inner.lock().await.last_delivered_video = Some(Instant::now());
        }
    }

    /// Send one encoded audio frame (AAC raw frame or Opus packet, per the
    /// configured audio codec) with its source RTP timestamp (48 kHz
    /// clock). No-op if this server wasn't configured with an audio codec.
    ///
    /// Opus keeps the historical rebase path (`source_rtp_ts` carried over
    /// into this server's output timestamp space, nominal-stepped through
    /// discontinuities -- see [`rebase_timestamp`]). AAC-LC frames are
    /// always exactly 1024 samples at 48 kHz, so instead the output
    /// timestamp advances by a fixed `+1024` per frame regardless of the
    /// source timestamp's own cadence/jitter: stamping the Opus-derived
    /// rebase (960/frame, with occasional larger jumps) onto AAC access
    /// units would give the decoder a timeline that disagrees with the
    /// payload duration, which is exactly the kind of mismatch that leaves
    /// legacy parsers (Windows Media Foundation's RTSP stack, in
    /// particular) unable to play the audio at all. Mirrors mistlink's
    /// `aac_processor.go`. `last_source_audio_ts` doubles as the "have we
    /// seeded yet" flag for the AAC path, exactly as it's used for Opus's
    /// rebase.
    pub async fn send_audio_frame(&self, frame: &[u8], source_rtp_ts: u32) {
        let Some(codec) = self.audio else {
            return;
        };

        let mut inner = self.inner.lock().await;

        let ts = match codec {
            rtp_out::AudioCodec::Aac => {
                let ts = if inner.last_source_audio_ts.is_none() {
                    inner.audio_ts
                } else {
                    inner.audio_ts.wrapping_add(AAC_TIMESTAMP_STEP)
                };
                inner.last_source_audio_ts = Some(source_rtp_ts);
                inner.audio_ts = ts;
                ts
            }
            rtp_out::AudioCodec::Opus => {
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
                ts
            }
        };

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

        // RFC 3640 §3.1: the marker bit MUST be set on the packet containing
        // the end of an Access Unit. Every AAC packet mistl sends is one
        // complete AU, so it's always set; RFC 7587 doesn't use the marker
        // for Opus, so it stays clear there.
        let marker = matches!(codec, rtp_out::AudioCodec::Aac);

        let bytes = rtp_out::serialize_rtp_packet(
            payload_type,
            rtp_out::AUDIO_SSRC,
            RtpHeaderFields {
                sequence_number: seq,
                timestamp: ts,
                marker,
            },
            &payload,
        );
        record_stats(&mut inner.audio_stats, ts, payload.len());
        let delivered = self.dispatch_audio(&mut inner.sessions, &bytes).await;
        if delivered {
            inner.last_delivered_audio = Some(Instant::now());
        }
    }

    async fn build_sdp(&self, control_base: &str) -> String {
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

        // WMPlayer/AVPro builds its SETUP URL from this per-media control
        // line rather than the DESCRIBE URI, so it must always be present
        // (in both video-only and two-track mode) and, when we know the
        // request URI, an absolute URL -- see media_control.
        let video_control = media_control(control_base, 0);
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
             a=fmtp:{pt} packetization-mode=1;sprop-parameter-sets={sps_b64},{pps_b64}\r\n\
             a=control:{video_control}\r\n",
            pt = rtp_out::PAYLOAD_TYPE_H264,
            clock_rate = rtp_out::CLOCK_RATE,
        );

        if let Some(codec) = self.audio {
            let audio_control = media_control(control_base, 1);
            match codec {
                rtp_out::AudioCodec::Aac => {
                    sdp.push_str(&format!(
                        "m=audio 0 RTP/AVP {pt}\r\n\
                         a=rtpmap:{pt} mpeg4-generic/48000/2\r\n\
                         a=fmtp:{pt} config=1190; indexdeltalength=3; indexlength=3; mode=AAC-hbr; profile-level-id=1; sizelength=13; streamtype=5\r\n\
                         a=control:{audio_control}\r\n",
                        pt = rtp_out::PAYLOAD_TYPE_AAC,
                    ));
                }
                rtp_out::AudioCodec::Opus => {
                    sdp.push_str(&format!(
                        "m=audio 0 RTP/AVP {pt}\r\n\
                         a=rtpmap:{pt} opus/48000/2\r\n\
                         a=control:{audio_control}\r\n",
                        pt = rtp_out::PAYLOAD_TYPE_OPUS,
                    ));
                }
            }
        }

        sdp
    }

    /// Queue complete TCP video access units and collect UDP destinations
    /// for sending after the shared state lock is released.
    fn stage_video(
        &self,
        inner: &mut Inner,
        packets: &[Vec<u8>],
        is_idr: bool,
        only_session: Option<&str>,
    ) -> (bool, Vec<SocketAddr>) {
        let mut tcp_delivered = false;
        let mut udp_targets = Vec::new();
        for (session_id, session) in &mut inner.sessions {
            if !session.playing || only_session.is_some_and(|id| id != session_id.as_str()) {
                continue;
            }
            match session.video.as_ref() {
                Some(SessionTransport::Udp { rtp_addr, .. }) => udp_targets.push(*rtp_addr),
                Some(SessionTransport::Tcp {
                    rtp_channel, tx, ..
                }) => {
                    if session.video_resyncing && !is_idr {
                        continue;
                    }
                    // Reserve every slot first. If the queue would run out
                    // partway through this AU, no fragment reaches the writer.
                    match tx.try_reserve_many(packets.len()) {
                        Ok(permits) => {
                            for (permit, bytes) in permits.zip(packets) {
                                permit.send(interleaved_frame(*rtp_channel, bytes));
                            }
                        }
                        Err(mpsc::error::TrySendError::Full(())) => {
                            if !session.video_resyncing {
                                session.video_resyncing = true;
                                self.request_keyframe();
                                let now = Instant::now();
                                if session
                                    .last_video_resync_log
                                    .is_none_or(|at| now.duration_since(at) >= RTP_LOG_INTERVAL)
                                {
                                    debug!(%session_id, packets = packets.len(), "RTP TCP-interleaved queue full; waiting for keyframe");
                                    session.last_video_resync_log = Some(now);
                                }
                            }
                            continue;
                        }
                        Err(mpsc::error::TrySendError::Closed(())) => continue,
                    }
                    session.video_resyncing = false;
                    tcp_delivered = true;
                }
                None => continue,
            }
            for bytes in packets {
                record_rtp_outflow(session_id, session, Media::Video, bytes);
            }
        }
        (tcp_delivered, udp_targets)
    }

    async fn send_udp_video(&self, packets: &[Vec<u8>], targets: &[SocketAddr]) -> bool {
        if targets.is_empty() {
            return false;
        }

        let pacing_deadline = Instant::now() + UDP_VIDEO_PACING_BUDGET;
        let mut bursts = udp_video_bursts(packets).peekable();
        let mut delivered = false;
        while let Some(burst) = bursts.next() {
            for target in targets {
                for bytes in burst {
                    match self.rtp_socket.send_to(bytes, target).await {
                        Ok(_) => delivered = true,
                        Err(error) => warn!(peer = %target, %error, "RTP UDP send failed"),
                    }
                }
            }
            if bursts.peek().is_some() {
                // Yield to receivers between bursts without relying on the
                // Windows timer granularity for sub-millisecond sleeps.
                let resume_at = (Instant::now() + UDP_VIDEO_BURST_GAP).min(pacing_deadline);
                loop {
                    tokio::task::yield_now().await;
                    if Instant::now() >= resume_at {
                        break;
                    }
                }
            }
        }
        delivered
    }

    async fn dispatch_audio(&self, sessions: &mut HashMap<String, Session>, bytes: &[u8]) -> bool {
        let mut delivered = false;
        for (session_id, session) in sessions.iter_mut() {
            if !session.playing {
                continue;
            }
            let Some(transport) = session.audio.as_ref() else {
                continue;
            };
            match transport {
                SessionTransport::Udp { rtp_addr, .. } => {
                    if let Err(error) = self.rtp_socket.send_to(bytes, *rtp_addr).await {
                        warn!(%session_id, peer = %rtp_addr, %error, "RTP UDP send failed");
                        continue;
                    }
                }
                SessionTransport::Tcp {
                    rtp_channel, tx, ..
                } => {
                    if tx.try_send(interleaved_frame(*rtp_channel, bytes)).is_err() {
                        continue;
                    }
                }
            }
            delivered = true;
            record_rtp_outflow(session_id, session, Media::Audio, bytes);
        }
        delivered
    }

    /// Send `bytes` (a full RTCP packet) to one session's transport for one
    /// media, over UDP (to the stored client RTCP address) or interleaved
    /// over TCP (on that media's RTCP channel).
    async fn dispatch_rtcp(
        session_id: &str,
        transport: &SessionTransport,
        rtp_socket: &UdpSocket,
        bytes: &[u8],
    ) {
        match transport {
            SessionTransport::Udp { rtcp_addr, .. } => {
                if let Err(error) = rtp_socket.send_to(bytes, *rtcp_addr).await {
                    warn!(%session_id, peer = %rtcp_addr, %error, "RTCP UDP send failed");
                }
            }
            SessionTransport::Tcp {
                rtcp_channel, tx, ..
            } => {
                if let Err(error) = tx.try_send(interleaved_frame(*rtcp_channel, bytes)) {
                    warn!(%session_id, %error, "RTCP TCP-interleaved send failed");
                }
            }
        }
    }

    /// Called with `video_output` held after the PLAY response is queued.
    /// The cached IDR lets clients join a static screen immediately.
    async fn start_playback(&self, session_id: &str) {
        let mut inner = self.inner.lock().await;

        let Some(session) = inner.sessions.get_mut(session_id) else {
            return;
        };
        session.playing = true;
        info!(%session_id, "PLAY: session marked playing");
        if session.video.is_none() || session.video_resyncing {
            return;
        }
        // The cached IDR gives the viewer a picture at once, but the live
        // P-frames after it reference frames this viewer never got, so ask for
        // a fresh keyframe either way (stream-lab: startup waited for the
        // periodic PLI otherwise).
        self.request_keyframe();
        let Some(idr_au) = &inner.last_idr_au else {
            debug!(%session_id, "keyframe-on-join: no cached IDR yet");
            return;
        };

        // Prepend cached SPS/PPS so the decoder can init even if the
        // cached IDR access unit didn't inline its own parameter sets;
        // duplication here is harmless.
        let mut bundle = Vec::with_capacity(idr_au.len() + 16);
        if let Some(sps) = &inner.sps {
            bundle.extend_from_slice(&[0, 0, 0, 1]);
            bundle.extend_from_slice(sps);
        }
        if let Some(pps) = &inner.pps {
            bundle.extend_from_slice(&[0, 0, 0, 1]);
            bundle.extend_from_slice(pps);
        }
        bundle.extend_from_slice(idr_au);

        let payloads = inner.payloader.payload(&bundle);
        if payloads.is_empty() {
            info!(%session_id, "keyframe-on-join skipped: payloader produced no packets");
            return;
        }

        let ts = inner.real_ts;
        let first_seq = inner.real_seq;
        let packets = inner.serialize_video(&payloads, ts);
        let (tcp_delivered, udp_targets) =
            self.stage_video(&mut inner, &packets, true, Some(session_id));
        drop(inner);

        if tcp_delivered || self.send_udp_video(&packets, &udp_targets).await {
            info!(%session_id, packets = packets.len(), first_seq, ts, "keyframe-on-join: sent cached IDR");
        }
    }
}

fn interleaved_frame(channel: u8, bytes: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(4 + bytes.len());
    framed.extend_from_slice(&[b'$', channel]);
    framed.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    framed.extend_from_slice(bytes);
    framed
}

fn udp_video_bursts(packets: &[Vec<u8>]) -> std::slice::Chunks<'_, Vec<u8>> {
    packets.chunks(UDP_VIDEO_BURST_PACKETS)
}

/// Scan an Annex-B buffer (one or more NAL units, each prefixed by a
/// `00 00 01` or `00 00 00 01` start code) for an IDR slice NAL (type 5,
/// the low 5 bits of the NAL header byte).
fn au_contains_idr(annex_b: &[u8]) -> bool {
    let mut i = 0;
    while i + 2 < annex_b.len() {
        let is_start_code_3 = annex_b[i] == 0 && annex_b[i + 1] == 0 && annex_b[i + 2] == 1;
        let is_start_code_4 = i + 3 < annex_b.len()
            && annex_b[i] == 0
            && annex_b[i + 1] == 0
            && annex_b[i + 2] == 0
            && annex_b[i + 3] == 1;
        if is_start_code_4 {
            let header_idx = i + 4;
            if header_idx < annex_b.len() && (annex_b[header_idx] & 0x1F) == 5 {
                return true;
            }
            i = header_idx;
        } else if is_start_code_3 {
            let header_idx = i + 3;
            if header_idx < annex_b.len() && (annex_b[header_idx] & 0x1F) == 5 {
                return true;
            }
            i = header_idx;
        } else {
            i += 1;
        }
    }
    false
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

/// Diagnostic-only RTP outflow visibility for one session: logs the first
/// packet sent to it since PLAY (seq/ts/payload size), then at most one
/// periodic summary line per [`RTP_LOG_INTERVAL`] (packets/bytes/last seq)
/// so a session that's actually receiving RTP doesn't spam the log while
/// one that never gets anything stays silent -- both are useful signals when
/// tracking down where an AVPro client stalls. `bytes` is the full
/// serialized RTP packet (12-byte header + payload); seq/ts are read back
/// out of the header rather than threaded through as extra arguments.
fn record_rtp_outflow(session_id: &str, session: &mut Session, media: Media, bytes: &[u8]) {
    if bytes.len() < 12 {
        return;
    }
    let seq = u16::from_be_bytes([bytes[2], bytes[3]]);
    let ts = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let payload_len = bytes.len() - 12;

    session.rtp_sent_packets += 1;
    session.rtp_sent_bytes += bytes.len() as u64;
    session.rtp_last_seq = seq;

    if !session.rtp_first_logged {
        session.rtp_first_logged = true;
        session.rtp_last_summary_at = Some(Instant::now());
        info!(%session_id, ?media, seq, ts, payload_len, "first RTP packet sent to session");
        return;
    }

    let now = Instant::now();
    let due = match session.rtp_last_summary_at {
        Some(at) => now.duration_since(at) >= RTP_LOG_INTERVAL,
        None => true,
    };
    if due {
        session.rtp_last_summary_at = Some(now);
        info!(
            %session_id,
            packets = session.rtp_sent_packets,
            bytes = session.rtp_sent_bytes,
            last_seq = session.rtp_last_seq,
            "RTP outflow summary"
        );
    }
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
/// traffic. The packet itself comes from [`Inner::next_keepalive_packet`],
/// which shares the real stream's seq/ts counters -- see its doc for why.
async fn dummy_keepalive_loop(server: Arc<RtspServer>) {
    let mut current_interval = Duration::from_millis(100);
    let mut ticker = tokio::time::interval(current_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let _output = server.video_output.lock().await;
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

        let packets = [inner.next_keepalive_packet()];
        // Keepalives are diagnostic filler, not real media -- deliberately
        // not counted toward `last_delivered_video`, mirroring `last_real_au`.
        let (_, udp_targets) = server.stage_video(&mut inner, &packets, false, None);
        drop(inner);
        server.send_udp_video(&packets, &udp_targets).await;
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

        let video_sr = inner.video_stats.active.then(|| {
            build_sender_report(&inner.video_stats, rtp_out::VIDEO_SSRC, rtp_out::CLOCK_RATE)
        });
        let audio_sr = inner.audio_stats.active.then(|| {
            build_sender_report(
                &inner.audio_stats,
                rtp_out::AUDIO_SSRC,
                rtp_out::AUDIO_CLOCK_RATE,
            )
        });

        for (session_id, session) in inner.sessions.iter() {
            if !session.playing {
                continue;
            }
            if let (Some(bytes), Some(transport)) = (&video_sr, &session.video) {
                RtspServer::dispatch_rtcp(session_id, transport, &server.rtp_socket, bytes).await;
            }
            if let (Some(bytes), Some(transport)) = (&audio_sr, &session.audio) {
                RtspServer::dispatch_rtcp(session_id, transport, &server.rtp_socket, bytes).await;
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
    info!(%peer_addr, "RTSP connection accepted");

    let (mut read_half, write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(WRITER_QUEUE_FRAMES);

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
    let mut request_count: u32 = 0;

    'conn: loop {
        loop {
            match rtsp_types::Message::<Vec<u8>>::parse(&buf) {
                Ok((message, consumed)) => {
                    buf.drain(0..consumed);
                    if let rtsp_types::Message::Request(request) = message {
                        let method: &str = request.method().into();
                        let cseq = request
                            .typed_header::<headers::CSeq>()
                            .ok()
                            .flatten()
                            .map(u32::from);
                        let uri = request.request_uri().map(|u| u.as_str());
                        let user_agent = request.header(&headers::USER_AGENT).map(|v| v.as_str());
                        if request.method() == &Method::Setup {
                            let transport = request.header(&headers::TRANSPORT).map(|v| v.as_str());
                            info!(%peer_addr, method, ?cseq, ?uri, ?user_agent, ?transport, "RTSP request");
                        } else {
                            info!(%peer_addr, method, ?cseq, ?uri, ?user_agent, "RTSP request");
                        }
                        request_count += 1;

                        // Keep the video counters stable from building PLAY's
                        // RTP-Info until the cached IDR has been dispatched.
                        let _video_output = if request.method() == Method::Play {
                            Some(server.video_output.lock().await)
                        } else {
                            None
                        };
                        let (response, new_session, play_session) =
                            handle_request(&server, &request, peer_addr, tx.clone()).await;
                        if let Some(id) = new_session {
                            session_ids.push(id);
                        }

                        let status = u16::from(response.status());
                        match request.method() {
                            Method::Setup => {
                                let transport =
                                    response.header(&headers::TRANSPORT).map(|v| v.as_str());
                                info!(%peer_addr, method, status, ?transport, "RTSP response");
                            }
                            Method::Play => {
                                let rtp_info =
                                    response.header(&headers::RTP_INFO).map(|v| v.as_str());
                                info!(%peer_addr, method, status, ?rtp_info, "RTSP response");
                            }
                            _ => {
                                info!(%peer_addr, method, status, "RTSP response");
                            }
                        }

                        let mut out = Vec::new();
                        if response.write(&mut out).is_err() || tx.try_send(out).is_err() {
                            break 'conn;
                        }
                        // Queue PLAY 200 before enabling RTP for this session.
                        if let Some(id) = play_session {
                            server.start_playback(&id).await;
                        }
                    }
                    // Data/Response messages from the client (e.g. RTCP
                    // sent over an interleaved channel) are ignored.
                }
                Err(rtsp_types::ParseError::Incomplete(_)) => break,
                Err(error @ rtsp_types::ParseError::Error) => {
                    let preview_len = buf.len().min(64);
                    let hex: String = buf[..preview_len]
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    let ascii: String = buf[..preview_len]
                        .iter()
                        .map(|&b| {
                            if b.is_ascii_graphic() || b == b' ' {
                                b as char
                            } else {
                                '.'
                            }
                        })
                        .collect();
                    info!(
                        %peer_addr, %error, request_count, hex = %hex, ascii = %ascii,
                        "bad RTSP message, closing connection"
                    );
                    break 'conn;
                }
            }
        }

        // Bound how long we wait on a half-sent request or a connection that
        // never established a session; established sessions may idle freely.
        let read_result = if !buf.is_empty() || session_ids.is_empty() {
            match tokio::time::timeout(REQUEST_IDLE_TIMEOUT, read_half.read(&mut read_buf)).await {
                Ok(result) => result,
                Err(_) => {
                    info!(%peer_addr, request_count, "RTSP connection closed: idle timeout");
                    break;
                }
            }
        } else {
            read_half.read(&mut read_buf).await
        };
        match read_result {
            Ok(0) => {
                info!(%peer_addr, request_count, "RTSP connection closed: EOF");
                break;
            }
            Err(error) => {
                info!(%peer_addr, request_count, %error, "RTSP connection closed: read error");
                break;
            }
            Ok(n) => {
                if buf.len() + n > MAX_REQUEST_BUF {
                    info!(%peer_addr, request_count, "RTSP connection closed: request buffer overflow");
                    break;
                }
                buf.extend_from_slice(&read_buf[..n]);
            }
        }
    }

    {
        let mut inner = server.inner.lock().await;
        for id in &session_ids {
            if inner.sessions.remove(id).is_some() {
                info!(%peer_addr, session_id = %id, "session removed: connection dropped without TEARDOWN");
            }
        }
    }
    writer_task.abort();
}

fn base_response(
    version: Version,
    status: StatusCode,
    cseq: Option<headers::CSeq>,
) -> rtsp_types::ResponseBuilder {
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
    tcp_tx: mpsc::Sender<Vec<u8>>,
) -> (Response<Vec<u8>>, Option<String>, Option<String>) {
    // The third element tells the connection loop which session to activate
    // after queueing the PLAY response.
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
            (response, None, None)
        }
        Method::Describe => {
            // The control base is this request's URI with any trailing '/'
            // trimmed, so build_sdp can append "/trackID=N" to it. Absent a
            // request URI at all, fall back to relative controls.
            let control_base = request
                .request_uri()
                .map(|uri| uri.as_str().trim_end_matches('/').to_string())
                .unwrap_or_default();
            let sdp = server.build_sdp(&control_base).await;
            let mut builder = base_response(version, StatusCode::Ok, cseq)
                .header(headers::CONTENT_TYPE, "application/sdp");
            if let Some(uri) = request.request_uri() {
                // gortsplib parity: Content-Base always carries a trailing
                // slash, which relative-URL resolution treats as "this is a
                // directory" (RFC 3986 5.3) -- without it some clients would
                // resolve a relative control against the parent path.
                let mut content_base = uri.as_str().to_string();
                if !content_base.ends_with('/') {
                    content_base.push('/');
                }
                builder = builder.header(headers::CONTENT_BASE, content_base);
            }
            (builder.build(sdp.into_bytes()), None, None)
        }
        Method::Setup => {
            let (response, new_session) =
                handle_setup(server, request, peer_addr, tcp_tx, version, cseq).await;
            (response, new_session, None)
        }
        Method::Play => {
            let (response, play_session) = handle_play(server, request, version, cseq).await;
            (response, None, play_session)
        }
        Method::Teardown => {
            let (response, new_session) = handle_teardown(server, request, version, cseq).await;
            (response, new_session, None)
        }
        Method::GetParameter => (
            base_response(version, StatusCode::Ok, cseq).build(Vec::new()),
            None,
            None,
        ),
        _ => (
            base_response(version, StatusCode::MethodNotAllowed, cseq).build(Vec::new()),
            None,
            None,
        ),
    }
}

/// Builds the `a=control` value for one media section: an absolute URL
/// rooted at `control_base` (the DESCRIBE request URI, trailing '/'
/// trimmed) when known, so WMPlayer/AVPro-style clients that build their
/// SETUP URL straight from this line get something they can use as-is.
/// Falls back to a bare relative track id when the DESCRIBE had no request
/// URI to build on.
fn media_control(control_base: &str, track_id: u32) -> String {
    if control_base.is_empty() {
        format!("trackID={track_id}")
    } else {
        format!("{control_base}/trackID={track_id}")
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
    tcp_tx: mpsc::Sender<Vec<u8>>,
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

    let (session_transport, response_transport) =
        if let Some(interleaved) = rtp_transport.params.interleaved {
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
            let rtcp_addr = SocketAddr::new(
                peer_addr.ip(),
                client_port.1.unwrap_or(client_port.0.wrapping_add(1)),
            );
            let server_rtp_port = server
                .rtp_socket
                .local_addr()
                .map(|a| a.port())
                .unwrap_or(0);
            let session_transport = SessionTransport::Udp {
                rtp_addr,
                rtcp_addr,
            };
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
        inner.sessions.insert(id.clone(), Session::default());
        id
    };
    let is_new_session = requested_session.is_none();

    match &session_transport {
        SessionTransport::Udp {
            rtp_addr,
            rtcp_addr,
        } => {
            info!(
                %peer_addr, session_id = %session_id, ?track_kind, new_session = is_new_session,
                %rtp_addr, %rtcp_addr, "session transport set up (UDP)"
            );
        }
        SessionTransport::Tcp {
            rtp_channel,
            rtcp_channel,
            ..
        } => {
            info!(
                %peer_addr, session_id = %session_id, ?track_kind, new_session = is_new_session,
                rtp_channel, rtcp_channel, "session transport set up (TCP interleaved)"
            );
        }
    }

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
    (
        response,
        if is_new_session {
            Some(session_id)
        } else {
            None
        },
    )
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

    let inner = server.inner.lock().await;
    let Some(entry) = inner.sessions.get(session.0.as_str()) else {
        return (
            base_response(version, StatusCode::SessionNotFound, cseq).build(Vec::new()),
            None,
        );
    };
    let has_video = entry.video.is_some();
    let has_audio = entry.audio.is_some();
    // Grab the next seq/ts of each media to advertise in RTP-Info.
    let video_seq = inner.real_seq;
    let video_ts = inner.real_ts;
    let audio_seq = inner.audio_seq;
    let audio_ts = inner.audio_ts;
    drop(inner);

    // RTP-Info advertises the sequence number and RTP timestamp of the first
    // packet the client will see, so it can initialize playback timing.
    // gortsplib (mistlink's RTSP stack, the known-good VRChat reference)
    // always sends this on PLAY and AVPro relies on it to start rendering --
    // its absence was a key reason VRChat showed nothing.
    //
    // One entry PER set-up track (RFC 2326 §12.33), addressed by the same
    // `<base>/trackID=N` control URLs the SDP hands out. Windows' MF/AVPro
    // RTSP client anchors each stream's timeline on its own rtptime; the old
    // single aggregate entry carried only the *video* timeline, so the audio
    // track -- whose timestamps sit on a completely unrelated base -- was
    // treated as far-future data and never rendered (video fine, audio
    // silent, while ffmpeg-based clients that ignore RTP-Info played both).
    let base = request
        .request_uri()
        .map(|uri| uri.to_string())
        .unwrap_or_default();
    let base = base.trim_end_matches('/');
    let track_entry = |track_id: u8, seq: u16, ts: u32| {
        if base.is_empty() {
            format!("seq={seq};rtptime={ts}")
        } else {
            format!("url={base}/trackID={track_id};seq={seq};rtptime={ts}")
        }
    };
    let mut entries = Vec::new();
    if has_video {
        entries.push(track_entry(0, video_seq, video_ts));
    }
    if has_audio {
        entries.push(track_entry(1, audio_seq, audio_ts));
    }
    if entries.is_empty() {
        // No transport set up at all (degenerate but legal): keep the old
        // aggregate-URL shape rather than sending an empty header.
        entries.push(track_entry(0, video_seq, video_ts));
    }
    let rtp_info = entries.join(",");

    let session_id = session.0.clone();
    let response = base_response(version, StatusCode::Ok, cseq)
        .typed_header(&headers::Session::from(session.0))
        .header(headers::RTP_INFO, rtp_info)
        .build(Vec::new());
    (response, Some(session_id))
}

async fn handle_teardown(
    server: &Arc<RtspServer>,
    request: &Request<Vec<u8>>,
    version: Version,
    cseq: Option<headers::CSeq>,
) -> (Response<Vec<u8>>, Option<String>) {
    if let Ok(Some(session)) = request.typed_header::<headers::Session>() {
        let mut inner = server.inner.lock().await;
        if inner.sessions.remove(session.0.as_str()).is_some() {
            info!(session_id = %session.0, "session removed: TEARDOWN");
        } else {
            info!(session_id = %session.0, "TEARDOWN for unknown session id");
        }
    }
    (
        base_response(version, StatusCode::Ok, cseq).build(Vec::new()),
        None,
    )
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
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);

        let (response, session, _) = handle_request(&server, &request, peer(), tx).await;

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
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);

        let (response, session, _) = handle_request(&server, &request, peer(), tx).await;

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
        assert!(body.contains("a=control:rtsp://127.0.0.1:8554/stream/trackID=0\r\n"));
        assert_eq!(
            response.header(&headers::CONTENT_BASE).map(|v| v.as_str()),
            Some("rtsp://127.0.0.1:8554/stream/")
        );
    }

    #[tokio::test]
    async fn video_only_sdp_includes_absolute_video_control() {
        use base64::Engine as _;

        // What this test uniquely covers beyond `describe_returns_sdp_with_h264_media_and_dummy_params`:
        // the base64 sprop-parameter-sets built from the specific SPS/PPS set
        // here, the absolute `a=control:` line, and that no audio section is
        // emitted for a video-only server.
        let server = test_server().await;
        server.update_sps(vec![0x67, 0x42, 0x00, 0x0a]).await;
        server.update_pps(vec![0x68, 0xce, 0x3c, 0x80]).await;

        let sdp = server.build_sdp("rtsp://127.0.0.1:8554/stream").await;

        let sps_b64 = base64::engine::general_purpose::STANDARD.encode([0x67, 0x42, 0x00, 0x0a]);
        let pps_b64 = base64::engine::general_purpose::STANDARD.encode([0x68, 0xce, 0x3c, 0x80]);

        assert!(sdp.contains(&format!(
            "a=fmtp:96 packetization-mode=1;sprop-parameter-sets={sps_b64},{pps_b64}\r\n"
        )));
        assert!(sdp.contains("a=control:rtsp://127.0.0.1:8554/stream/trackID=0\r\n"));
        assert!(!sdp.contains("m=audio"));
    }

    #[tokio::test]
    async fn two_track_sdp_includes_aac_audio_section() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Aac).await;
        let sdp = server.build_sdp("rtsp://127.0.0.1:8554/stream").await;

        assert!(sdp.contains("m=video 0 RTP/AVP 96\r\n"));
        assert!(sdp.contains("a=control:rtsp://127.0.0.1:8554/stream/trackID=0\r\n"));
        assert!(sdp.contains("m=audio 0 RTP/AVP 112\r\n"));
        assert!(sdp.contains("a=rtpmap:112 mpeg4-generic/48000/2\r\n"));
        assert!(sdp.contains(
            "a=fmtp:112 config=1190; indexdeltalength=3; indexlength=3; mode=AAC-hbr; profile-level-id=1; sizelength=13; streamtype=5\r\n"
        ));
        assert!(sdp.contains("streamtype=5"));
        assert!(sdp.contains("a=control:rtsp://127.0.0.1:8554/stream/trackID=1\r\n"));
    }

    #[tokio::test]
    async fn two_track_sdp_includes_opus_audio_section() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Opus).await;
        let sdp = server.build_sdp("rtsp://127.0.0.1:8554/stream").await;

        assert!(sdp.contains("a=control:rtsp://127.0.0.1:8554/stream/trackID=0\r\n"));
        assert!(sdp.contains("m=audio 0 RTP/AVP 111\r\n"));
        assert!(sdp.contains("a=rtpmap:111 opus/48000/2\r\n"));
        assert!(sdp.contains("a=control:rtsp://127.0.0.1:8554/stream/trackID=1\r\n"));
        assert!(!sdp.contains("mpeg4-generic"));
    }

    #[tokio::test]
    async fn build_sdp_falls_back_to_relative_control_without_base() {
        let server = test_server().await;
        let sdp = server.build_sdp("").await;

        assert!(sdp.contains("a=control:trackID=0\r\n"));
        assert!(!sdp.contains("rtsp://"));
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
    async fn setup_udp_then_play_without_cached_idr_requests_keyframe() {
        let server = test_server().await;

        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5000-5001")
            .build(Vec::new());
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        let (response, session_id, _) =
            handle_request(&server, &setup_request, peer(), tx.clone()).await;
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
        let (response, _, play_session) = handle_request(&server, &play_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);
        assert_eq!(play_session.as_deref(), Some(session_id.as_str()));

        assert_eq!(server.client_count().await, 0);
        let _output = server.video_output.lock().await;
        server.start_playback(&session_id).await;
        assert_eq!(server.client_count().await, 1);
        tokio::time::timeout(Duration::from_secs(1), server.wait_for_keyframe_request())
            .await
            .expect("PLAY should request a keyframe without a cached IDR");
    }

    #[tokio::test]
    async fn play_advertises_per_track_rtp_info_with_each_medias_own_timeline() {
        // Windows' MF/AVPro RTSP client anchors each stream's playback
        // timeline on its own RTP-Info rtptime. A single aggregate entry
        // carrying the video timeline made the audio track (independent
        // timestamp base) look like far-future data: video played, audio
        // stayed silent. PLAY must emit one entry per set-up track.
        let server = test_server_with_audio(rtp_out::AudioCodec::Aac).await;
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);

        let setup_video = Request::builder(Method::Setup, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/trackID=0").unwrap())
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5000-5001")
            .build(Vec::new());
        let (response, session_id, _) =
            handle_request(&server, &setup_video, peer(), tx.clone()).await;
        assert_eq!(response.status(), StatusCode::Ok);
        let session_id = session_id.expect("SETUP should allocate a session id");

        let setup_audio = Request::builder(Method::Setup, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/trackID=1").unwrap())
            .header(headers::CSEQ, "2")
            .header(headers::SESSION, session_id.clone())
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5002-5003")
            .build(Vec::new());
        let (response, _, _) = handle_request(&server, &setup_audio, peer(), tx.clone()).await;
        assert_eq!(response.status(), StatusCode::Ok);

        // Give the two media distinct, recognizable timelines.
        {
            let mut inner = server.inner.lock().await;
            inner.real_seq = 100;
            inner.real_ts = 90_000;
            inner.audio_seq = 200;
            inner.audio_ts = 48_000;
        }

        let play_request = Request::builder(Method::Play, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/").unwrap())
            .header(headers::CSEQ, "3")
            .header(headers::SESSION, session_id)
            .build(Vec::new());
        let (response, _, _) = handle_request(&server, &play_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);

        let rtp_info = response
            .header(&headers::RTP_INFO)
            .expect("PLAY must carry RTP-Info")
            .as_str();
        assert_eq!(
            rtp_info,
            "url=rtsp://127.0.0.1:8554/stream/trackID=0;seq=100;rtptime=90000,\
             url=rtsp://127.0.0.1:8554/stream/trackID=1;seq=200;rtptime=48000"
        );
    }

    #[tokio::test]
    async fn setup_video_only_with_absolute_trackid_selects_video() {
        // The SDP now always emits an absolute per-media control URL (e.g.
        // "rtsp://host/stream/trackID=0") even in video-only mode, so a
        // SETUP built from that line -- not just the bare "/stream" URI --
        // must keep selecting the video track.
        let server = test_server().await;
        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/trackID=0").unwrap())
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5000-5001")
            .build(Vec::new());
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        let (response, session_id, _) = handle_request(&server, &setup_request, peer(), tx).await;

        assert_eq!(response.status(), StatusCode::Ok);
        let session_id = session_id.expect("SETUP should allocate a session id");
        let inner = server.inner.lock().await;
        assert!(inner.sessions.get(&session_id).unwrap().video.is_some());
    }

    #[tokio::test]
    async fn rtspt_uri_is_accepted_for_tcp_interleaved_setup() {
        let server = test_server().await;
        let (message, _) = rtsp_types::Message::<Vec<u8>>::parse(
            b"SETUP rtspt://127.0.0.1:8554/stream/trackID=0 RTSP/1.0\r\n\
              CSeq: 1\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n",
        )
        .unwrap();
        let rtsp_types::Message::Request(setup_request) = message else {
            panic!("expected a SETUP request");
        };
        assert_eq!(setup_request.request_uri().unwrap().scheme(), "rtspt");
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        let (response, session_id, _) = handle_request(&server, &setup_request, peer(), tx).await;

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
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        let (response, session_id, _) =
            handle_request(&server, &video_setup, peer(), tx.clone()).await;
        assert_eq!(response.status(), StatusCode::Ok);
        let session_id = session_id.expect("first SETUP allocates a session");

        let audio_setup = Request::builder(Method::Setup, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/trackID=1").unwrap())
            .header(headers::CSEQ, "2")
            .header(headers::TRANSPORT, "RTP/AVP;unicast;client_port=5002-5003")
            .header(headers::SESSION, session_id.clone())
            .build(Vec::new());
        let (response2, session_id2, _) = handle_request(&server, &audio_setup, peer(), tx).await;
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
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        let (response, session_id, _) = handle_request(&server, &setup_request, peer(), tx).await;

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
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        let (_, session_id, _) = handle_request(&server, &setup_request, peer(), tx.clone()).await;
        let session_id = session_id.unwrap();

        let teardown_request = Request::builder(Method::Teardown, Version::V1_0)
            .header(headers::CSEQ, "2")
            .header(headers::SESSION, session_id.clone())
            .build(Vec::new());
        let (response, _, _) = handle_request(&server, &teardown_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);

        let inner = server.inner.lock().await;
        assert!(!inner.sessions.contains_key(&session_id));
    }

    // --- keepalive/real-data shared seq/ts space -----------------------------

    /// Parse `(seq, ts)` back out of a serialized RTP packet's header.
    fn rtp_seq_ts(bytes: &[u8]) -> (u16, u32) {
        (
            u16::from_be_bytes([bytes[2], bytes[3]]),
            u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        )
    }

    /// A minimal one-NAL Annex-B access unit the payloader turns into
    /// exactly one RTP packet.
    fn tiny_au() -> Vec<u8> {
        vec![0, 0, 0, 1, 0x65, 0x01, 0x02, 0x03]
    }

    #[tokio::test]
    async fn keepalive_packet_continues_the_real_video_seq_ts_space() {
        let server = test_server().await;

        // A real access unit consumes one seq and steps the shared ts.
        server.send_video_access_unit(&tiny_au()).await;

        let mut inner = server.inner.lock().await;
        let (seq_after_real, ts_after_real) = (inner.real_seq, inner.real_ts);

        // The keepalive packet must pick up exactly where the real stream
        // left off -- same counters, no separate dummy seq/ts space.
        let packet = inner.next_keepalive_packet();
        let (seq, ts) = rtp_seq_ts(&packet);
        assert_eq!(
            seq, seq_after_real,
            "keepalive must consume the next real seq"
        );
        assert_eq!(
            ts, ts_after_real,
            "keepalive must send at the current real ts"
        );
        assert_eq!(inner.real_seq, seq_after_real.wrapping_add(1));
        assert_eq!(
            inner.real_ts,
            ts_after_real.wrapping_add(rtp_out::DUMMY_TIMESTAMP_INCREMENT),
            "keepalive advances the shared ts by its nominal step"
        );
    }

    #[tokio::test]
    async fn consecutive_keepalive_packets_step_seq_by_one_and_ts_by_nominal() {
        let server = test_server().await;
        let mut inner = server.inner.lock().await;

        let (seq_a, ts_a) = rtp_seq_ts(&inner.next_keepalive_packet());
        let (seq_b, ts_b) = rtp_seq_ts(&inner.next_keepalive_packet());
        let (seq_c, ts_c) = rtp_seq_ts(&inner.next_keepalive_packet());

        assert_eq!(seq_b, seq_a.wrapping_add(1));
        assert_eq!(seq_c, seq_b.wrapping_add(1));
        assert_eq!(ts_b.wrapping_sub(ts_a), rtp_out::DUMMY_TIMESTAMP_INCREMENT);
        assert_eq!(ts_c.wrapping_sub(ts_b), rtp_out::DUMMY_TIMESTAMP_INCREMENT);
    }

    #[tokio::test]
    async fn real_data_resuming_after_keepalive_continues_the_shared_space() {
        let server = test_server().await;

        // Real stream runs (relay path, source-timestamped)...
        server.send_video_access_unit_at(&tiny_au(), 10_000).await;

        // ...stalls, so the keepalive takes over for a few packets...
        let (seq_after_keepalive, ts_after_keepalive) = {
            let mut inner = server.inner.lock().await;
            inner.next_keepalive_packet();
            inner.next_keepalive_packet();
            (inner.real_seq, inner.real_ts)
        };

        // ...and real data resumes with a source-ts jump far beyond the
        // rebase clamp (a >1s stall). rebase_timestamp substitutes the
        // nominal step, and the packet must continue from the counters the
        // keepalive advanced -- one continuous seq/ts space, not a cliff
        // back to a parallel "real" space.
        server
            .send_video_access_unit_at(&tiny_au(), 10_000 + 900_000)
            .await;

        let inner = server.inner.lock().await;
        assert_eq!(
            inner.real_seq,
            seq_after_keepalive.wrapping_add(1),
            "the resumed real packet consumes the seq right after the keepalive's"
        );
        let nominal_step = rtp_out::CLOCK_RATE / 30; // test server frame rate
        assert_eq!(
            inner.real_ts,
            ts_after_keepalive.wrapping_add(nominal_step),
            "the resumed real packet's ts continues from the keepalive-advanced counter"
        );
        assert_eq!(
            inner.video_stats.last_ts, inner.real_ts,
            "the packet itself went out at the keepalive-advanced (nominal-stepped) ts"
        );
    }

    // --- AAC output cadence/marker (RFC 3640) --------------------------------

    #[tokio::test]
    async fn aac_frames_step_ts_by_fixed_1024_regardless_of_source_jitter() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Aac).await;

        server.send_audio_frame(&[0u8; 4], 1_000).await;
        let ts_a = server.inner.lock().await.audio_ts;

        // A big forward jump and then a backwards jump in the *source*
        // timestamp -- neither should perturb the AAC output cadence, unlike
        // the Opus rebase path this deliberately bypasses.
        server.send_audio_frame(&[0u8; 4], 50_000).await;
        let ts_b = server.inner.lock().await.audio_ts;
        server.send_audio_frame(&[0u8; 4], 1_200).await;
        let ts_c = server.inner.lock().await.audio_ts;

        assert_eq!(ts_b.wrapping_sub(ts_a), AAC_TIMESTAMP_STEP);
        assert_eq!(ts_c.wrapping_sub(ts_b), AAC_TIMESTAMP_STEP);
    }

    #[tokio::test]
    async fn opus_frames_still_use_the_rebase_path() {
        // Guards against the AAC fixed-step change leaking into Opus:
        // consecutive normal-cadence source timestamps should rebase 1:1 as
        // before, not step by the AAC constant.
        let server = test_server_with_audio(rtp_out::AudioCodec::Opus).await;

        server.send_audio_frame(&[0u8; 2], 1_000).await;
        let ts_a = server.inner.lock().await.audio_ts;
        server.send_audio_frame(&[0u8; 2], 1_960).await;
        let ts_b = server.inner.lock().await.audio_ts;

        assert_eq!(ts_b.wrapping_sub(ts_a), 960);
    }

    /// Sets up a TCP-interleaved session for `codec`'s audio track, PLAYs
    /// it, sends one audio frame, and returns the raw RTP packet bytes
    /// (interleave framing stripped) that went out on the channel.
    async fn first_audio_packet(codec: rtp_out::AudioCodec) -> Vec<u8> {
        let server = test_server_with_audio(codec).await;
        let setup_request = Request::builder(Method::Setup, Version::V1_0)
            .request_uri(Url::parse("rtsp://127.0.0.1:8554/stream/trackID=1").unwrap())
            .header(headers::CSEQ, "1")
            .header(headers::TRANSPORT, "RTP/AVP/TCP;unicast;interleaved=2-3")
            .build(Vec::new());
        let (tx, mut rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        let (response, session_id, _) =
            handle_request(&server, &setup_request, peer(), tx.clone()).await;
        assert_eq!(response.status(), StatusCode::Ok);
        let session_id = session_id.expect("SETUP should allocate a session id");

        let play_request = Request::builder(Method::Play, Version::V1_0)
            .header(headers::CSEQ, "2")
            .header(headers::SESSION, session_id)
            .build(Vec::new());
        let (response, _, play_session) = handle_request(&server, &play_request, peer(), tx).await;
        assert_eq!(response.status(), StatusCode::Ok);
        {
            let _output = server.video_output.lock().await;
            server.start_playback(&play_session.unwrap()).await;
        }

        server.send_audio_frame(&[0xAA, 0xBB], 1_000).await;

        let framed = rx.recv().await.expect("expected an interleaved RTP frame");
        // Strip the 4-byte "$" + channel + 2-byte length interleave header.
        framed[4..].to_vec()
    }

    #[tokio::test]
    async fn aac_packet_has_marker_bit_set() {
        let bytes = first_audio_packet(rtp_out::AudioCodec::Aac).await;
        assert_eq!(
            bytes[1] & 0x80,
            0x80,
            "RFC 3640 3.1: the marker bit must be set on the packet containing the end of an AU"
        );
    }

    #[tokio::test]
    async fn opus_packet_does_not_have_marker_bit_set() {
        let bytes = first_audio_packet(rtp_out::AudioCodec::Opus).await;
        assert_eq!(
            bytes[1] & 0x80,
            0,
            "RFC 7587 doesn't use the marker bit for Opus"
        );
    }

    // --- delivery-aware flow indicator ---------------------------------------

    fn tcp_transport(rtp_channel: u8, tx: &mpsc::Sender<Vec<u8>>) -> SessionTransport {
        SessionTransport::Tcp {
            rtp_channel,
            rtcp_channel: rtp_channel + 1,
            tx: tx.clone(),
        }
    }

    #[tokio::test]
    async fn tcp_queue_full_mid_access_unit_drops_entire_au_and_resumes_at_next_idr() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Aac).await;
        let (tx, mut rx) = mpsc::channel(3);
        let (other_tx, other_rx) = mpsc::channel(16);
        {
            let mut inner = server.inner.lock().await;
            inner.sessions.insert(
                "slow".into(),
                Session {
                    video: Some(tcp_transport(0, &tx)),
                    audio: Some(tcp_transport(2, &tx)),
                    playing: true,
                    ..Session::default()
                },
            );
            inner.sessions.insert(
                "other".into(),
                Session {
                    video: Some(tcp_transport(0, &other_tx)),
                    playing: true,
                    ..Session::default()
                },
            );
        }

        let fragmented_au = |nal_header| {
            let mut au = vec![0, 0, 0, 1, nal_header];
            au.resize(rtp_out::RTP_MTU + 4, 0x2A);
            au
        };
        let delta = fragmented_au(0x41);
        let idr = fragmented_au(0x65);

        // The two-packet delta would fill the queue partway through.
        tx.try_send(vec![1]).unwrap();
        tx.try_send(vec![2]).unwrap();
        server.send_video_access_unit(&delta).await;
        assert_eq!(rx.try_recv().unwrap(), [1]);
        assert_eq!(rx.try_recv().unwrap(), [2]);
        assert!(rx.is_empty());
        assert_eq!(other_rx.len(), 2);
        tokio::time::timeout(Duration::from_secs(1), server.wait_for_keyframe_request())
            .await
            .expect("queue overflow should request a keyframe");

        server.send_video_access_unit(&delta).await;
        assert!(rx.is_empty());
        server.send_audio_frame(&[0xAA, 0xBB], 1_000).await;
        assert_eq!(rx.try_recv().unwrap()[1], 2);

        tx.try_send(vec![3]).unwrap();
        tx.try_send(vec![4]).unwrap();
        server.send_video_access_unit(&idr).await;
        assert_eq!(rx.try_recv().unwrap(), [3]);
        assert_eq!(rx.try_recv().unwrap(), [4]);
        assert!(rx.is_empty());
        server.send_video_access_unit(&delta).await;
        assert!(rx.is_empty());

        server.send_video_access_unit(&idr).await;
        let first = rx.try_recv().unwrap();
        let last = rx.try_recv().unwrap();
        assert!(rx.is_empty());
        assert_eq!(first[17], 0x80 | 5); // FU-A start of the IDR NAL
        assert_eq!(last[17], 0x40 | 5); // FU-A end of the IDR NAL
        assert_eq!(first[5] & 0x80, 0);
        assert_eq!(last[5] & 0x80, 0x80);
        assert_eq!(
            rtp_seq_ts(&last[4..]).0,
            rtp_seq_ts(&first[4..]).0.wrapping_add(1)
        );
        server.send_video_access_unit(&delta).await;
        assert_eq!(rx.len(), 2);
        assert_eq!(other_rx.len(), 12);
    }

    #[test]
    fn udp_video_bursts_preserve_packet_order_and_split_at_the_limit() {
        let packets: Vec<_> = (0..35).map(|id| vec![id]).collect();
        let bursts: Vec<_> = udp_video_bursts(&packets).collect();
        assert_eq!(bursts, [&packets[..16], &packets[16..32], &packets[32..]]);
        assert_eq!(udp_video_bursts(&packets[..16]).len(), 1);
        assert_eq!(udp_video_bursts(&[]).len(), 0);
    }

    #[tokio::test]
    async fn paced_udp_idr_reaches_loopback_in_order() {
        let server = test_server().await;
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let rtp_addr = receiver.local_addr().unwrap();
        server.inner.lock().await.sessions.insert(
            "udp".into(),
            Session {
                video: Some(SessionTransport::Udp {
                    rtp_addr,
                    rtcp_addr: rtp_addr,
                }),
                playing: true,
                ..Session::default()
            },
        );
        let mut au = tiny_au();
        au.resize(rtp_out::RTP_MTU * 120, 0x2A);
        let expected = rtp_out::H264Rtp::new().payload(&au).len();
        let send = async {
            let start = Instant::now();
            server.send_video_access_unit_at(&au, 1_000).await;
            eprintln!("paced UDP IDR: {expected} packets in {:?}", start.elapsed());
        };
        let receive = async {
            let mut packet = [0; rtp_out::RTP_MTU];
            let mut last_seq: Option<u16> = None;
            for i in 0..expected {
                let len = receiver.recv(&mut packet).await.unwrap();
                let (seq, _) = rtp_seq_ts(&packet[..len]);
                if let Some(previous) = last_seq {
                    assert_eq!(seq, previous.wrapping_add(1));
                }
                assert_eq!(packet[1] & 0x80 != 0, i + 1 == expected);
                last_seq = Some(seq);
            }
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(send, receive);
        })
        .await
        .expect("the entire paced IDR should reach the UDP receiver");
    }

    #[tokio::test]
    async fn audio_frame_stamps_last_delivered_audio_only_when_a_session_receives_it() {
        let server = test_server_with_audio(rtp_out::AudioCodec::Aac).await;

        // No sessions attached: the frame is produced but never delivered.
        server.send_audio_frame(&[0u8; 4], 1_000).await;
        assert!(server.inner.lock().await.last_delivered_audio.is_none());

        // Attach a playing session with an audio transport, then send again.
        let (tx, _rx) = mpsc::channel(WRITER_QUEUE_FRAMES);
        {
            let mut inner = server.inner.lock().await;
            inner.sessions.insert(
                "s1".to_string(),
                Session {
                    audio: Some(tcp_transport(2, &tx)),
                    playing: true,
                    ..Session::default()
                },
            );
        }
        server.send_audio_frame(&[0u8; 4], 2_000).await;
        assert!(server.inner.lock().await.last_delivered_audio.is_some());
        // Video was never dispatched to any session.
        assert!(server.inner.lock().await.last_delivered_video.is_none());
    }
}
