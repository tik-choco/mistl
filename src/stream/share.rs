//! Share: publish this machine's own screen capture into a mistlib room, so
//! any consensus-elected relay in that room (or a direct tc-chat-style
//! viewer) picks it up exactly like a browser sharer would -- no separate
//! WHIP/HTTP ingest path, no second signaling stack. Everything goes through
//! mistlib's own (Nostr) signaling, the same as every other module in this
//! crate.
//!
//! Reuses [`super::native`]'s Windows Graphics Capture + OpenH264 pipeline
//! (`native::NativeCapture`) rather than running a second capture/encode
//! loop: `NativeCapture::spawn`'s optional `share` channel hands this module
//! a clone of every encoded Annex-B access unit alongside the copy that
//! still goes straight to the local RTSP server. [`publish_loop`]
//! RTP-packetizes that copy with [`rtp_out::H264Rtp`] (the same STAP-A/FU-A
//! payloader `rtsp.rs` uses for its own RTP output) and writes each packet to
//! a `TrackLocalStaticRTP` published into the room via
//! `mistlib::publish_local_track` -- the identical native-side publish
//! surface `relay.rs`'s cascade leader uses to re-broadcast a share it
//! received from someone else.
//!
//! # Loopback
//!
//! `mistlib::publish_local_track` (`transports/webrtc/publish.rs` in
//! mistlib-native) attaches the published track to every already-connected
//! *remote* peer's `RTCPeerConnection` and to every peer that connects
//! afterward; it has no notion of a connection back to this same process.
//! Concretely: `WebRtcTransport::publish_local_track` iterates
//! `self.peers` (remote sessions only) and calls
//! `peer.add_local_track`/renegotiates -- there is no local loop. mistlib's
//! `MediaTrackEvent` delivery (`net::mod.rs`'s media consumer, which
//! `stream::relay` subscribes to) is fed exclusively by each
//! `RTCPeerConnection`'s `on_track` callback, which only ever fires for
//! tracks *received* from a peer -- never for ones this process published
//! itself (`mistlib-native/tests/loopback_media.rs`'s
//! `publish_local_track_delivers_media_to_an_already_connected_peer` proves
//! the receiving side is a genuinely separate transport/peer, not the
//! publisher itself). So if this node is alone in the room, or is itself
//! running `stream relay` for the same room, its own published share never
//! arrives back as a `MediaTrackEvent` here, and `stream::relay` would never
//! feed it to this node's own RTSP output.
//!
//! That is exactly why [`ShareCapture::spawn`] leaves `NativeCapture`'s
//! existing direct-to-RTSP feed running unconditionally (see `rtsp` below):
//! it is the only path that makes the sharer's own VRChat output work when
//! nobody else -- or no elected cascade leader -- is relaying this share
//! from the room side. Remote peers (including other mistl relay nodes not
//! also running consensus as *this* room's relay) still receive the track
//! normally over the published route.
//!
//! One related caveat worth flagging even though it's out of scope to fix
//! here (it lives in `relay.rs`'s cascade policy, which this task doesn't
//! own): a leader's `accepts_remote` only accepts a video track from a
//! remote peer that is *not* itself a known consensus relay peer -- a
//! heuristic meant to keep a follower's cascade re-publish from being
//! mistaken for a fresh original share. If this sharer node *also* runs
//! `stream relay`/consensus for the same room, the leader would see this
//! node's node id in `relay_peers` and ignore this share's track. This does
//! not affect the common case (a node that only shares, not also relays),
//! but is worth documenting for anyone running both roles on one node.
//!
//! # Audio
//!
//! Deferred. WASAPI loopback capture needs either a new dependency
//! (`cpal`, or direct `windows` crate `Win32_Media_Audio` bindings -- this
//! crate depends on `windows` only transitively via `windows-capture`,
//! without that feature enabled) or hand-rolled `IAudioClient` COM bindings;
//! both are more than a "reasonably small path" addition and everything must
//! stay buildable `--offline` against the already-vendored lockfile. Opus
//! *encoding* itself needs no new dependency -- `opus = "0.3"` is already a
//! direct dependency and already used for encoding elsewhere
//! (`selftest.rs`'s `audio_loop_opus`) -- so wiring a capture source into it
//! is the natural follow-up once one exists.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use mistlib::webrtc::api::media_engine::MIME_TYPE_H264;
use mistlib::webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use mistlib::webrtc::track::track_local::TrackLocalWriter;
use mistlib::webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use rand::RngCore;
use rtp::header::Header;
use rtp::packet::Packet;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::daemon::AppState;

use super::native::NativeCapture;
use super::rtp_out::{self, H264Rtp};
use super::rtsp::RtspServer;

/// Track id prefix, matching the shape tc-chat uses for its own screen-share
/// tracks (`tc-chat-screen:<uuid>`) so nothing downstream needs a mistl-
/// specific special case for the id format itself.
pub const TRACK_ID_PREFIX: &str = "mistl-screen";

/// `stream_id` mistlib groups this track's `TrackLocalStaticRTP` under.
const STREAM_ID: &str = "mistl-share";

/// A running local screen share: the reused native capture pipeline plus the
/// task that packetizes its second output copy into RTP and republishes it
/// into the configured room.
pub struct ShareCapture {
    native: NativeCapture,
    publish_task: JoinHandle<()>,
    room: String,
    track_id: String,
    video_track: Arc<TrackLocalStaticRTP>,
    started_at: Instant,
    packets_sent: Arc<AtomicU64>,
}

impl ShareCapture {
    /// Joins `room` (via the shared net transport), publishes a fresh
    /// `mistl-screen:<uuid>` video track into it, and starts the native
    /// capture/encode pipeline feeding both `rtsp` (directly, so this node's
    /// own VRChat output works regardless of relay election -- see the
    /// module doc's "Loopback" section) and the freshly published track.
    pub async fn spawn(
        state: &Arc<AppState>,
        room: String,
        frame_rate: u32,
        max_width: u32,
        rtsp: Arc<RtspServer>,
    ) -> Result<Self> {
        crate::net::ensure_started(state, room.clone())
            .await
            .context("share: joining room")?;

        let track_id = format!("{TRACK_ID_PREFIX}:{}", random_uuid_like());
        let video_track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: rtp_out::CLOCK_RATE,
                ..Default::default()
            },
            track_id.clone(),
            STREAM_ID.to_string(),
        ));

        mistlib::publish_local_track(&room, video_track.clone())
            .await
            .context("share: publishing video track into room")?;

        let (au_tx, au_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let packets_sent = Arc::new(AtomicU64::new(0));
        let publish_task = tokio::spawn(publish_loop(
            au_rx,
            video_track.clone(),
            frame_rate,
            packets_sent.clone(),
        ));

        let native = match NativeCapture::spawn(frame_rate, max_width, rtsp, Some(au_tx)).await {
            Ok(native) => native,
            Err(error) => {
                publish_task.abort();
                if let Err(unpublish_error) =
                    mistlib::unpublish_local_track(&room, video_track).await
                {
                    warn!(%unpublish_error, "share: unpublishing video track after failed capture start also failed");
                }
                return Err(error);
            }
        };

        Ok(Self {
            native,
            publish_task,
            room,
            track_id,
            video_track,
            started_at: Instant::now(),
            packets_sent,
        })
    }

    /// Stops the native capture pipeline, the RTP-publish task, and
    /// unpublishes the video track from the room.
    pub async fn stop(self) {
        self.native.stop().await;
        self.publish_task.abort();
        if let Err(error) = mistlib::unpublish_local_track(&self.room, self.video_track).await {
            warn!(%error, "share: unpublishing video track failed");
        }
    }

    /// `stream.status`'s `share` field.
    pub fn status(&self) -> Value {
        json!({
            "active": true,
            "room": self.room,
            "track_id": self.track_id,
            "uptime_secs": self.started_at.elapsed().as_secs(),
            "video_packets_sent": self.packets_sent.load(Ordering::Relaxed),
            // Audio is not shared yet -- see the module doc's "Audio" section.
            "audio": false,
        })
    }
}

/// Receives Annex-B access units from the reused native capture pipeline,
/// RTP-packetizes each with [`H264Rtp`] (STAP-A/single-NALU/FU-A, matching
/// `rtsp.rs`'s own RTP output), and writes every resulting packet straight
/// to `track` via `TrackLocalWriter::write_rtp`. Sequence numbers and the
/// 90kHz timestamp are owned entirely by this loop (unlike `relay.rs`'s
/// cascade republish, which forwards already-timestamped RTP it read off a
/// remote track, this loop originates fresh RTP from raw NALs) --
/// `TrackLocalStaticRTP::write_rtp` overwrites only `ssrc`/`payload_type` per
/// peer binding to match what was actually negotiated with each viewer
/// (`webrtc::track::track_local::track_local_static_rtp`), so the values put
/// here are placeholders that never reach the wire unchanged.
async fn publish_loop(
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
    track: Arc<TrackLocalStaticRTP>,
    frame_rate: u32,
    packets_sent: Arc<AtomicU64>,
) {
    let mut payloader = H264Rtp::new();
    let mut seq: u16 = 0;
    let mut ts: u32 = 0;
    let step = rtp_out::CLOCK_RATE / frame_rate.max(1);

    while let Some(au) = rx.recv().await {
        let payloads = payloader.payload(&au);
        if !payloads.is_empty() {
            let last = payloads.len() - 1;
            for (i, payload) in payloads.iter().enumerate() {
                let packet = Packet {
                    header: Header {
                        version: 2,
                        marker: i == last,
                        payload_type: rtp_out::PAYLOAD_TYPE_H264,
                        sequence_number: seq,
                        timestamp: ts,
                        ssrc: rtp_out::VIDEO_SSRC,
                        ..Default::default()
                    },
                    payload: payload.clone(),
                };
                seq = seq.wrapping_add(1);
                match track.write_rtp(&packet).await {
                    Ok(_) => {
                        packets_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        debug!(%error, "share: writing RTP packet to published track failed");
                    }
                }
            }
        }
        ts = ts.wrapping_add(step);
    }
}

/// A random 32-hex-character id, formatted like a UUID's hex digits (no
/// dashes -- nothing downstream parses this as an actual `uuid::Uuid`, it
/// only needs to be unique per share session). Hand-rolled rather than
/// pulling in the `uuid` crate (present in `Cargo.lock` only transitively,
/// not a direct dependency here, and offline builds can't add one): `rand`
/// is already a direct dependency, so 16 random bytes hex-encoded gives the
/// same practical uniqueness a v4 UUID would.
fn random_uuid_like() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_uuid_like_is_32_lowercase_hex_chars() {
        let id = random_uuid_like();
        assert_eq!(id.len(), 32);
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn random_uuid_like_is_not_constant() {
        // Not a proof of randomness, just a guard against an accidental
        // all-zero buffer or similar degenerate implementation.
        let a = random_uuid_like();
        let b = random_uuid_like();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn publish_loop_ends_when_the_sender_is_dropped() {
        // No real TrackLocalStaticRTP writer is exercised here (that needs a
        // bound peer connection, covered by mistlib-native's own
        // `loopback_media` tests) -- this just proves the loop is a normal
        // "drain until closed" consumer, matching `native.rs::forward_loop`'s
        // shape, so `ShareCapture::stop`'s `publish_task.abort()` isn't the
        // only way this task ever ends.
        let track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                clock_rate: rtp_out::CLOCK_RATE,
                ..Default::default()
            },
            "test-track".to_string(),
            STREAM_ID.to_string(),
        ));
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let packets_sent = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(publish_loop(rx, track, 30, packets_sent));
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("publish_loop should end shortly after its sender is dropped")
            .expect("publish_loop task should not panic");
    }
}
