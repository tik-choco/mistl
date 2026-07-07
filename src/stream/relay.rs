//! Relay backend: receive a tc-chat screen share (real WebRTC media tracks
//! over the mist p2p network) and re-serve it through the local RTSP server
//! for VRChat's AVPro player -- the pipeline mistlink pioneered in Go.
//!
//! Flow:
//! 1. `net::ensure_started` joins the share's room; `net::set_media_consumer`
//!    subscribes this module to remote track arrivals (mistlib delivers
//!    [`mistlib::MediaTrackEvent`]s: `remote_id`, `track`, `receiver`, `pc`).
//! 2. Video track (H264, pinned by mistlib's media engine): `read_rtp` loop
//!    -> depacketize (single NAL / STAP-A / FU-A) -> group into Annex-B
//!    access units -> extract SPS/PPS -> `rtsp.update_sps/update_pps` +
//!    `rtsp.send_video_access_unit_at(au, rtp_ts)`. Send PLI on start and
//!    periodically until the first IDR arrives (browsers only emit keyframes
//!    on request).
//! 3. Audio track (Opus): `read_rtp` loop -> per `AudioCodec` either decode
//!    Opus + encode AAC-LC (48 kHz stereo, 1024-sample frames, mistlink's
//!    scheme) or pass Opus packets through ->
//!    `rtsp.send_audio_frame(frame, rtp_ts)`.
//!
//! # Publisher lock-in
//!
//! A control task consumes every [`mistlib::MediaTrackEvent`] mistlib
//! delivers (from every peer in the room, not just one publisher). It locks
//! onto the first peer whose *video* track arrives, spawns the video/PLI
//! tasks for it, and attaches that peer's audio track whenever it shows up
//! (before or after the video track -- tc-chat's negotiation order isn't
//! guaranteed, so an audio track arriving before its sibling video track is
//! buffered in `pending_audio` until the lock happens). Tracks from any other
//! peer are ignored while locked. [`decide_video`]/[`decide_audio`] are pure
//! functions capturing that policy so it's testable without real network
//! types.
//!
//! # Cascade distribution (consensus-driven)
//!
//! When `[stream] cascade = true` (the default) and a consensus control
//! plane ([`crate::consensus::RelayConsensus`]) starts successfully for this
//! room, the lock-in policy above is extended via [`CascadePolicy`]: the
//! elected LEADER accepts video from any peer that isn't itself a relay node
//! (i.e. the browser sharer) -- same effective behavior as the no-cascade
//! case -- while FOLLOWERS accept only from the current leader's
//! *re-published* tracks (see below), ignoring the sharer entirely. When
//! cascade is disabled by config, or `RelayConsensus::start` fails, the relay
//! falls back to the original "lock onto the first video from anyone"
//! behavior (logged at WARN).
//!
//! The leader additionally re-publishes the sharer's H264/Opus RTP straight
//! through -- raw passthrough, no transcoding -- as its own local tracks via
//! `mistlib::publish_local_track`. The tracks are created lazily the moment
//! the leader locks onto the sharer, and torn down (`unpublish_local_track`
//! + dropped) on unlock, role loss, or `stop()`. This is the cascade/SFU
//! building block: the sharer uplinks once (to the leader), and every relay
//! in the room -- including ones with no direct connection to the sharer,
//! since mistlib's DNVE3 overlay is a selective mesh rather than a full one
//! -- can reach the share via the leader's re-publish instead of needing a
//! direct link to the sharer itself.
//!
//! `control_loop` reacts to [`crate::consensus::RelayConsensus::subscribe`]
//! (`tokio::select!` alongside the media-event channel): a role or leader
//! change unlocks -- tearing down the forwarding tasks and any active
//! re-publish -- so the next matching track re-establishes the lock under
//! the new policy.
//!
//! ## v1 limitation: a follower's PLI is a no-op
//!
//! Followers' periodic PLI (see [`PLI_INTERVAL`]) targets the leader's
//! re-published track, and mistlib's cascade plumbing does not forward that
//! request back to the original sharer -- only the leader's own PLI (sent
//! directly to the sharer) can actually trigger a keyframe. This bounds a
//! late-joining follower's keyframe latency to the leader's existing ~5s PLI
//! cadence rather than the follower's own PLI having any effect; acceptable
//! for v1 since it's the same order of magnitude as the pre-cascade
//! single-relay keyframe wait.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use mistlib::MediaTrackEvent;
use mistlib::webrtc::api::media_engine::{MIME_TYPE_H264, MIME_TYPE_OPUS};
use mistlib::webrtc::peer_connection::RTCPeerConnection;
use mistlib::webrtc::rtcp::packet::Packet as RtcpPacket;
use mistlib::webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use mistlib::webrtc::rtp::codecs::h264::H264Packet;
use mistlib::webrtc::rtp::packetizer::Depacketizer;
use mistlib::webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTPCodecType};
use mistlib::webrtc::track::track_local::TrackLocalWriter;
use mistlib::webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use mistlib::webrtc::track::track_remote::TrackRemote;
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::consensus::{ConsensusRole, ConsensusView, RelayConsensus};
use crate::daemon::AppState;
use crate::stream::rtp_out::{self, AudioCodec};
use crate::stream::rtsp::RtspServer;

/// A leader's re-published video + audio tracks, kept alive for exactly as
/// long as it's locked onto the sharer (see [`create_and_publish_republish_tracks`]).
type RepublishTracks = (Arc<TrackLocalStaticRTP>, Arc<TrackLocalStaticRTP>);

/// H264 NAL unit type constants (RFC 6184 section 5.4), matching `native.rs`.
const NAL_TYPE_SPS: u8 = 7;
const NAL_TYPE_PPS: u8 = 8;
const NAL_TYPE_IDR: u8 = 5;

/// Interval between PictureLossIndication requests to the publisher, sent
/// immediately on lock and repeated at this cadence thereafter -- browsers
/// only emit a keyframe on request, and this also lets a late-joining VRChat
/// viewer get an IDR within one interval (mistlink does the same).
const PLI_INTERVAL: Duration = Duration::from_secs(5);

/// Cadence of the periodic INFO throughput summary (`summary_task`): frequent
/// enough that a viewer watching the log can tell within a few seconds that
/// the pipeline is alive, without approaching debug-level chatter.
const SUMMARY_INTERVAL: Duration = Duration::from_secs(5);

/// Samples per channel in one AAC-LC frame (fixed by the codec), and the
/// interleaved-sample chunk size (stereo) that implies.
const AAC_FRAME_SAMPLES: usize = 1024;
const AAC_CHANNELS: usize = 2;

/// Largest Opus frame `opus::Decoder::decode` can produce: 120 ms at 48 kHz,
/// stereo.
const MAX_OPUS_DECODE_SAMPLES: usize = 48_000 / 1000 * 120 * AAC_CHANNELS;

/// Throughput counters shared between the media-forwarding tasks and the
/// periodic `summary_task`: how many video access units / audio frames were
/// actually forwarded to `rtsp` since the counters were last reset. Cheap
/// `Relaxed` atomics -- exact ordering doesn't matter, only the totals over
/// each summary interval.
#[derive(Default)]
struct RelayCounters {
    video_au: AtomicU64,
    audio_frames: AtomicU64,
}

/// This relay's cascade wiring: either genuinely disabled (`[stream]
/// cascade = false`, or a consensus start failure) or a live control-plane
/// handle. Cheap to clone (an `Arc` clone at most), so both [`RelayCapture`]
/// (status/stop) and `control_loop` (policy/role-change reactions) can each
/// hold their own copy.
#[derive(Clone)]
enum Cascade {
    Disabled,
    Active(Arc<RelayConsensus>),
}

/// The running relay: the control task consuming media events, plus shared
/// state other code (`stream.status`, `stop`) needs to reach into.
pub struct RelayCapture {
    publisher: Arc<StdMutex<Option<String>>>,
    active_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
    control_task: JoinHandle<()>,
    cascade: Cascade,
    /// This node's mistlib node id -- needed for the `cascade.self` status
    /// field independent of whether consensus is active.
    node_id: String,
    /// The room id actually joined on the wire (tc-chat's derived channel
    /// id, not the friendly room name) -- what `mistlib::publish_local_track`
    /// / `unpublish_local_track` expect.
    channel_room: String,
    /// The leader's currently-published re-broadcast tracks, if any are
    /// live right now. Shared with `control_loop` so `stop()` can unpublish
    /// them even though `control_task` itself is aborted rather than
    /// awaited to completion.
    republish: Arc<StdMutex<Option<RepublishTracks>>>,
}

impl RelayCapture {
    /// Join `room` (via the shared net transport) and start relaying the
    /// first screen share that appears there into `rtsp`.
    pub async fn spawn(
        state: &Arc<AppState>,
        room: String,
        audio_codec: AudioCodec,
        rtsp: Arc<RtspServer>,
    ) -> Result<Self> {
        // tc-chat joins its rooms under a derived channel id (not the raw room
        // name — see net::channel_id_for / tc-chat's channelIdFor), so the raw
        // name is never the on-wire topic. Derive the same channel here so the
        // relay lands in the same swarm and can see the shared screen. Callers
        // pass the friendly room id (e.g. `--room global`); the derivation is
        // owned here so users never handle the opaque `tcch-…` form. The same
        // channel id is also what the consensus control plane and
        // `mistlib::publish_local_track`/`unpublish_local_track` need as
        // their `room` -- it's the actual on-wire session id.
        let channel_room = crate::net::channel_id_for(&room);
        crate::net::ensure_started(state, channel_room.clone()).await?;

        let node_id = crate::identity::current(state)
            .await
            .context("relay: loading identity for cascade")?
            .node_id();

        let cascade = if state.config().stream.cascade {
            match RelayConsensus::start(state, channel_room.clone()).await {
                Ok(consensus) => Cascade::Active(consensus),
                Err(error) => {
                    warn!(
                        %error,
                        "cascade: consensus failed to start; falling back to direct-lock relay (no leader election)"
                    );
                    Cascade::Disabled
                }
            }
        } else {
            warn!(
                "cascade: disabled via [stream] cascade = false; using direct-lock relay (no leader election)"
            );
            Cascade::Disabled
        };

        let (tx, rx) = mpsc::unbounded_channel();
        crate::net::set_media_consumer(Some(tx));

        let publisher = Arc::new(StdMutex::new(None));
        let active_tasks = Arc::new(StdMutex::new(Vec::new()));
        let counters = Arc::new(RelayCounters::default());
        let republish: Arc<StdMutex<Option<RepublishTracks>>> = Arc::new(StdMutex::new(None));

        let control_task = tokio::spawn(control_loop(
            rx,
            rtsp.clone(),
            audio_codec,
            publisher.clone(),
            active_tasks.clone(),
            counters.clone(),
            cascade.clone(),
            channel_room.clone(),
            republish.clone(),
        ));

        let summary_handle = tokio::spawn(summary_task(rtsp, publisher.clone(), counters));
        active_tasks
            .lock()
            .expect("relay active tasks lock poisoned")
            .push(summary_handle);

        Ok(Self {
            publisher,
            active_tasks,
            control_task,
            cascade,
            node_id,
            channel_room,
            republish,
        })
    }

    /// Node id of the peer currently being relayed, if a share is live. In
    /// cascade mode this is whoever we're locked onto directly -- the sharer
    /// if we're the leader, the leader if we're a follower.
    pub fn publisher(&self) -> Option<String> {
        self.publisher
            .lock()
            .expect("relay publisher lock poisoned")
            .clone()
    }

    /// `stream.status`'s `cascade` field -- see the module doc's "Cascade
    /// distribution" section for the exact contract (the dashboard is built
    /// against this shape).
    pub fn cascade_status(&self) -> Value {
        match &self.cascade {
            Cascade::Disabled => json!({ "enabled": false }),
            Cascade::Active(consensus) => {
                let locked = self
                    .publisher
                    .lock()
                    .expect("relay publisher lock poisoned")
                    .is_some();
                cascade_status_json(
                    consensus.role(),
                    consensus.leader(),
                    &self.node_id,
                    consensus.relay_peers(),
                    locked,
                )
            }
        }
    }

    /// Stop the relay tasks, unpublish any live cascade re-broadcast, shut
    /// down the consensus control plane, and unsubscribe from media events.
    pub async fn stop(self) {
        self.control_task.abort();
        for task in self
            .active_tasks
            .lock()
            .expect("relay active tasks lock poisoned")
            .drain(..)
        {
            task.abort();
        }

        let tracks = self
            .republish
            .lock()
            .expect("relay republish lock poisoned")
            .take();
        if let Some((video, audio)) = tracks {
            if let Err(error) = mistlib::unpublish_local_track(&self.channel_room, video).await {
                debug!(%error, "cascade: unpublishing video re-broadcast track on stop failed");
            }
            if let Err(error) = mistlib::unpublish_local_track(&self.channel_room, audio).await {
                debug!(%error, "cascade: unpublishing audio re-broadcast track on stop failed");
            }
        }

        if let Cascade::Active(consensus) = &self.cascade {
            consensus.shutdown().await;
        }

        crate::net::set_media_consumer(None);
    }
}

/// Pure JSON builder for the `cascade` field of `stream.status`, decoupled
/// from a live [`RelayConsensus`] so the exact shape is unit-testable
/// without standing up a real consensus session (which needs a joined room
/// and mistlib's engine running). `locked` is whether we currently have a
/// publisher locked in (`RelayCapture::publisher().is_some()`); `source` is
/// derived from it plus `role` (leader locked = "sharer", follower locked =
/// "leader", not locked = `null`).
fn cascade_status_json(
    role: ConsensusRole,
    leader: Option<String>,
    self_id: &str,
    relay_peers: Vec<String>,
    locked: bool,
) -> Value {
    let source = if locked {
        match role {
            ConsensusRole::Leader => Some("sharer"),
            ConsensusRole::Follower => Some("leader"),
            ConsensusRole::Candidate | ConsensusRole::Unknown => None,
        }
    } else {
        None
    };
    json!({
        "enabled": true,
        "role": role,
        "leader": leader,
        "self": self_id,
        "relay_peers": relay_peers,
        "source": source,
    })
}

/// What to do with an incoming *audio* track, given the current lock state.
/// Kept (with [`decide_video`]) as pure functions -- no network/task types --
/// so the lock-in policy is unit-testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AudioDecision {
    /// Peer is already locked and this is its (not-yet-attached) audio track.
    Attach,
    /// No publisher locked yet; hold this audio track in case its sibling
    /// video track locks the same peer shortly.
    Buffer,
    /// Not relevant right now.
    Ignore,
}

/// The cascade context [`decide_video`]/[`decide_audio`] evaluate against --
/// a snapshot of [`ConsensusView`] (or the absence of one), kept as a plain
/// data type so the policy matrix is unit-testable without a real
/// [`RelayConsensus`].
#[derive(Debug, Clone, PartialEq)]
enum CascadePolicy {
    /// `[stream] cascade = false`, or consensus failed to start: today's
    /// behavior -- lock onto the first video track from anyone.
    Disabled,
    /// Consensus is running for this room; `relay_peers` includes self.
    Active {
        role: ConsensusRole,
        leader: Option<String>,
        relay_peers: Vec<String>,
    },
}

/// Whether `policy` would accept a video/audio track from `remote_id` as the
/// publisher, independent of whether we're already locked onto someone else.
/// Leader accepts anyone who isn't a known relay peer (i.e. the sharer);
/// follower accepts only the current leader; a role that hasn't resolved
/// past candidate/unknown, or a follower with no leader hint yet, accepts
/// no one (safer to wait than to guess).
fn accepts_remote(remote_id: &str, policy: &CascadePolicy) -> bool {
    match policy {
        CascadePolicy::Disabled => true,
        CascadePolicy::Active { role: ConsensusRole::Leader, relay_peers, .. } => {
            !relay_peers.iter().any(|peer| peer == remote_id)
        }
        CascadePolicy::Active { role: ConsensusRole::Follower, leader: Some(leader), .. } => {
            remote_id == leader
        }
        CascadePolicy::Active { .. } => false,
    }
}

/// Whether an incoming *video* track locks its peer as the publisher.
fn decide_video(locked: Option<&str>, remote_id: &str, policy: &CascadePolicy) -> bool {
    locked.is_none() && accepts_remote(remote_id, policy)
}

/// Decision for an incoming *audio* track.
fn decide_audio(
    locked: Option<&str>,
    remote_id: &str,
    audio_attached: bool,
    policy: &CascadePolicy,
) -> AudioDecision {
    match locked {
        Some(id) if id == remote_id && !audio_attached => AudioDecision::Attach,
        Some(_) => AudioDecision::Ignore,
        None if accepts_remote(remote_id, policy) => AudioDecision::Buffer,
        None => AudioDecision::Ignore,
    }
}

/// `role` rendered for the `cascade: role=... leader=... peers=...` log line
/// (see the module doc's "Cascade distribution" section).
fn role_str(role: ConsensusRole) -> &'static str {
    match role {
        ConsensusRole::Leader => "leader",
        ConsensusRole::Follower => "follower",
        ConsensusRole::Candidate => "candidate",
        ConsensusRole::Unknown => "unknown",
    }
}

fn leader_display(leader: &Option<String>) -> &str {
    leader.as_deref().unwrap_or("none")
}

/// Builds the [`CascadePolicy`] `decide_video`/`decide_audio` should use
/// right now, from the cascade wiring and the most recently observed
/// [`ConsensusView`] (`None` until the control plane has published its
/// first view).
fn build_policy(cascade: &Cascade, view: Option<&ConsensusView>) -> CascadePolicy {
    match cascade {
        Cascade::Disabled => CascadePolicy::Disabled,
        Cascade::Active(_) => match view {
            Some(view) => CascadePolicy::Active {
                role: view.role,
                leader: view.leader.clone(),
                relay_peers: view.peers.clone(),
            },
            None => CascadePolicy::Active {
                role: ConsensusRole::Unknown,
                leader: None,
                relay_peers: Vec::new(),
            },
        },
    }
}

/// Awaits the next consensus view change, or never resolves when cascade
/// isn't active -- lets `control_loop`'s `tokio::select!` carry an optional
/// branch without special-casing the loop body per iteration.
async fn watch_changed(
    view_rx: &mut Option<watch::Receiver<ConsensusView>>,
) -> std::result::Result<(), watch::error::RecvError> {
    match view_rx {
        Some(rx) => rx.changed().await,
        None => std::future::pending().await,
    }
}

/// Creates this leader's video (H264) + audio (Opus) re-broadcast tracks and
/// publishes both into `channel_room` via `mistlib::publish_local_track`.
/// Codec parameters match the sharer's own tracks (H264 @ 90kHz, Opus @
/// 48kHz stereo) since this is a raw RTP passthrough, not a transcode -- see
/// `mistlib-native/tests/loopback_media.rs` for the same shape used against
/// a live connection. If publishing the audio track fails after the video
/// track already succeeded, the video track is unpublished again so a
/// partially-published pair is never left live.
async fn create_and_publish_republish_tracks(channel_room: &str) -> Result<RepublishTracks> {
    let video = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_H264.to_owned(),
            clock_rate: 90_000,
            ..Default::default()
        },
        "video".to_string(),
        "mistl-cascade".to_string(),
    ));
    let audio = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: rtp_out::AUDIO_CLOCK_RATE,
            channels: 2,
            ..Default::default()
        },
        "audio".to_string(),
        "mistl-cascade".to_string(),
    ));

    mistlib::publish_local_track(channel_room, video.clone())
        .await
        .context("cascade: publishing re-broadcast video track")?;
    if let Err(error) = mistlib::publish_local_track(channel_room, audio.clone()).await {
        let _ = mistlib::unpublish_local_track(channel_room, video).await;
        return Err(error).context("cascade: publishing re-broadcast audio track");
    }

    Ok((video, audio))
}

/// Unlocks the current publisher: clears the shared lock state, aborts the
/// per-publisher forwarding tasks (video/PLI/audio), and -- if the leader
/// was re-publishing -- unpublishes and drops the re-broadcast tracks.
/// Shared by both unlock paths in `control_loop` (the publisher's track
/// ending, and a cascade role/leader change invalidating the current lock).
async fn unlock(
    channel_room: &str,
    publisher: &Arc<StdMutex<Option<String>>>,
    active_tasks: &Arc<StdMutex<Vec<JoinHandle<()>>>>,
    republish: &Arc<StdMutex<Option<RepublishTracks>>>,
) {
    *publisher.lock().expect("relay publisher lock poisoned") = None;
    for task in active_tasks
        .lock()
        .expect("relay active tasks lock poisoned")
        .drain(..)
    {
        task.abort();
    }

    let tracks = republish
        .lock()
        .expect("relay republish lock poisoned")
        .take();
    if let Some((video, audio)) = tracks {
        if let Err(error) = mistlib::unpublish_local_track(channel_room, video).await {
            debug!(%error, "cascade: unpublishing video re-broadcast track failed");
        }
        if let Err(error) = mistlib::unpublish_local_track(channel_room, audio).await {
            debug!(%error, "cascade: unpublishing audio re-broadcast track failed");
        }
    }
}

/// Consumes every [`MediaTrackEvent`] mistlib delivers, applying the
/// lock-in policy above, until the channel closes (i.e. `stop()` dropped the
/// sender via `set_media_consumer(None)` -- actually the channel itself is
/// owned here; `stop()` instead aborts this task directly, so in practice
/// this only returns if mistlib itself drops its sender). Also reacts to
/// cascade role/leader changes via `cascade`'s consensus handle, when active.
async fn control_loop(
    mut media_rx: mpsc::UnboundedReceiver<MediaTrackEvent>,
    rtsp: Arc<RtspServer>,
    audio_codec: AudioCodec,
    publisher: Arc<StdMutex<Option<String>>>,
    active_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
    counters: Arc<RelayCounters>,
    cascade: Cascade,
    channel_room: String,
    republish: Arc<StdMutex<Option<RepublishTracks>>>,
) {
    let (ended_tx, mut ended_rx) = mpsc::unbounded_channel::<String>();
    let mut locked: Option<String> = None;
    let mut audio_attached = false;
    let mut pending_audio: HashMap<String, MediaTrackEvent> = HashMap::new();
    // The leader's own copy of the audio re-broadcast track, kept alongside
    // `locked` so a sibling audio track arriving after the video lock (the
    // common case; see `pending_audio`) still gets republished.
    let mut audio_republish: Option<Arc<TrackLocalStaticRTP>> = None;

    let mut view_rx = match &cascade {
        Cascade::Active(consensus) => Some(consensus.subscribe()),
        Cascade::Disabled => None,
    };
    let mut current_view: Option<ConsensusView> = view_rx.as_ref().map(|rx| rx.borrow().clone());
    if let Some(view) = &current_view {
        info!(
            "cascade: role={} leader={} peers={}",
            role_str(view.role),
            leader_display(&view.leader),
            view.peers.len()
        );
    }

    loop {
        let policy = build_policy(&cascade, current_view.as_ref());

        tokio::select! {
            event = media_rx.recv() => {
                let Some(event) = event else { break };
                let remote_id = event.remote_id.0.clone();
                let kind = event.track.kind();

                match kind {
                    RTPCodecType::Video => {
                        if !decide_video(locked.as_deref(), &remote_id, &policy) {
                            debug!(%remote_id, "relay: ignoring video track (publisher already locked, or not accepted by cascade policy)");
                            continue;
                        }

                        info!(%remote_id, "relay: locking onto publisher");
                        *publisher.lock().expect("relay publisher lock poisoned") = Some(remote_id.clone());
                        locked = Some(remote_id.clone());
                        audio_attached = false;

                        let is_leader = matches!(&policy, CascadePolicy::Active { role: ConsensusRole::Leader, .. });
                        let is_follower = matches!(&policy, CascadePolicy::Active { role: ConsensusRole::Follower, .. });
                        let (video_republish, new_audio_republish) = if is_leader {
                            match create_and_publish_republish_tracks(&channel_room).await {
                                Ok((video, audio)) => {
                                    *republish.lock().expect("relay republish lock poisoned") =
                                        Some((video.clone(), audio.clone()));
                                    info!("cascade: re-publishing share into room");
                                    (Some(video), Some(audio))
                                }
                                Err(error) => {
                                    warn!(%error, "cascade: failed to publish re-broadcast tracks; continuing as direct relay only");
                                    (None, None)
                                }
                            }
                        } else {
                            if is_follower {
                                info!("cascade: following leader {remote_id}");
                            }
                            (None, None)
                        };
                        audio_republish = new_audio_republish;

                        let video_handle = tokio::spawn(video_task(
                            event.track.clone(),
                            rtsp.clone(),
                            remote_id.clone(),
                            ended_tx.clone(),
                            counters.clone(),
                            video_republish,
                        ));
                        let pli_handle = tokio::spawn(pli_task(event.pc.clone(), event.track.ssrc()));
                        active_tasks
                            .lock()
                            .expect("relay active tasks lock poisoned")
                            .extend([video_handle, pli_handle]);

                        if let Some(audio_event) = pending_audio.remove(&remote_id) {
                            info!(%remote_id, codec = audio_codec.as_str(), "relay: audio track attached");
                            let audio_handle = tokio::spawn(audio_task(
                                audio_event.track,
                                rtsp.clone(),
                                audio_codec,
                                remote_id.clone(),
                                counters.clone(),
                                audio_republish.clone(),
                            ));
                            active_tasks
                                .lock()
                                .expect("relay active tasks lock poisoned")
                                .push(audio_handle);
                            audio_attached = true;
                        }
                    }
                    RTPCodecType::Audio => match decide_audio(locked.as_deref(), &remote_id, audio_attached, &policy) {
                        AudioDecision::Attach => {
                            info!(%remote_id, codec = audio_codec.as_str(), "relay: audio track attached");
                            let audio_handle = tokio::spawn(audio_task(
                                event.track,
                                rtsp.clone(),
                                audio_codec,
                                remote_id.clone(),
                                counters.clone(),
                                audio_republish.clone(),
                            ));
                            active_tasks
                                .lock()
                                .expect("relay active tasks lock poisoned")
                                .push(audio_handle);
                            audio_attached = true;
                        }
                        AudioDecision::Buffer => {
                            pending_audio.insert(remote_id, event);
                        }
                        AudioDecision::Ignore => {}
                    },
                    RTPCodecType::Unspecified => {
                        debug!(%remote_id, "relay: ignoring track of unspecified kind");
                    }
                }
            }
            Some(ended_id) = ended_rx.recv() => {
                if locked.as_deref() == Some(ended_id.as_str()) {
                    info!(remote_id = %ended_id, "relay: publisher's video track ended; unlocking");
                    unlock(&channel_room, &publisher, &active_tasks, &republish).await;
                    locked = None;
                    audio_attached = false;
                    audio_republish = None;
                    pending_audio.remove(&ended_id);
                }
            }
            changed = watch_changed(&mut view_rx) => {
                match changed {
                    Ok(()) => {
                        let new_view = view_rx
                            .as_ref()
                            .expect("view_rx is Some after an Ok(()) change notification")
                            .borrow()
                            .clone();
                        let role_changed = current_view.as_ref().map(|v| v.role) != Some(new_view.role);
                        let leader_changed =
                            current_view.as_ref().map(|v| v.leader.clone()) != Some(new_view.leader.clone());

                        if role_changed || leader_changed {
                            info!(
                                "cascade: role={} leader={} peers={}",
                                role_str(new_view.role),
                                leader_display(&new_view.leader),
                                new_view.peers.len()
                            );
                            if locked.is_some() {
                                info!("cascade: leader changed -> re-locking");
                                unlock(&channel_room, &publisher, &active_tasks, &republish).await;
                                locked = None;
                                audio_attached = false;
                                audio_republish = None;
                                pending_audio.clear();
                            }
                        }
                        current_view = Some(new_view);
                    }
                    Err(_) => {
                        // The consensus control plane stopped (shutdown, or
                        // the watch sender was dropped) -- stop polling it.
                        view_rx = None;
                    }
                }
            }
        }
    }
}

/// Reads RTP off the locked video track, depacketizes H264 into Annex-B
/// access units, extracts SPS/PPS, and forwards each complete AU to `rtsp`.
/// When `republish` is `Some` (leader, cascade active), the raw RTP packet
/// is also written straight through to it -- before depacketization, so
/// followers receive the exact same H264 bitstream the sharer sent, no
/// transcode. Notifies `ended_tx` with `remote_id` when the track ends (read
/// error/EOF) so the control task can clear the lock.
async fn video_task(
    track: Arc<TrackRemote>,
    rtsp: Arc<RtspServer>,
    remote_id: String,
    ended_tx: mpsc::UnboundedSender<String>,
    counters: Arc<RelayCounters>,
    republish: Option<Arc<TrackLocalStaticRTP>>,
) {
    let mut depacketizer = H264Packet::default();
    let mut assembler = AuAssembler::new();
    let mut seen_first_keyframe = false;

    loop {
        let (packet, _attrs) = match track.read_rtp().await {
            Ok(v) => v,
            Err(error) => {
                debug!(%remote_id, %error, "relay: video track ended");
                break;
            }
        };

        if let Some(republish) = &republish {
            if let Err(error) = republish.write_rtp(&packet).await {
                debug!(%remote_id, %error, "cascade: republishing video RTP packet failed");
            }
        }

        let chunk = match depacketizer.depacketize(&packet.payload) {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(%remote_id, %error, "relay: failed to depacketize H264 RTP packet");
                continue;
            }
        };

        for (au_ts, au) in assembler.push(packet.header.timestamp, packet.header.marker, &chunk) {
            if au.is_empty() {
                continue;
            }

            let nals = scan_au_nals(&au);
            if let Some(sps) = nals.sps {
                rtsp.update_sps(sps).await;
            }
            if let Some(pps) = nals.pps {
                rtsp.update_pps(pps).await;
            }
            if nals.has_idr {
                if !seen_first_keyframe {
                    seen_first_keyframe = true;
                    info!(%remote_id, "relay: first keyframe (IDR) received from publisher");
                } else {
                    debug!(%remote_id, "relay: forwarded IDR access unit");
                }
            }
            rtsp.send_video_access_unit_at(&au, au_ts).await;
            counters.video_au.fetch_add(1, Ordering::Relaxed);
        }
    }

    let _ = ended_tx.send(remote_id);
}

/// Sends a PictureLossIndication immediately and every [`PLI_INTERVAL`]
/// thereafter; `tokio::time::interval`'s first tick fires immediately, so
/// this single loop covers both the "bootstrap decode" and "keep late
/// viewers fed" requirements without a separate initial send.
async fn pli_task(pc: Arc<RTCPeerConnection>, media_ssrc: u32) {
    let mut interval = tokio::time::interval(PLI_INTERVAL);
    loop {
        interval.tick().await;
        send_pli(&pc, media_ssrc).await;
    }
}

async fn send_pli(pc: &Arc<RTCPeerConnection>, media_ssrc: u32) {
    let pli: Box<dyn RtcpPacket + Send + Sync> = Box::new(PictureLossIndication {
        sender_ssrc: 0,
        media_ssrc,
    });
    if let Err(error) = pc.write_rtcp(&[pli]).await {
        warn!(%error, "relay: sending PLI failed");
    }
}

/// Reads RTP off the locked audio (Opus) track and forwards it per
/// `codec`: passthrough for [`AudioCodec::Opus`], transcode to AAC-LC for
/// [`AudioCodec::Aac`]. When `republish` is `Some` (leader, cascade active),
/// the raw Opus RTP packet is also written straight through to it, ahead of
/// whichever local codec path `codec` takes -- followers always receive raw
/// Opus from the leader regardless of what this leader serves over its own
/// RTSP.
async fn audio_task(
    track: Arc<TrackRemote>,
    rtsp: Arc<RtspServer>,
    codec: AudioCodec,
    remote_id: String,
    counters: Arc<RelayCounters>,
    republish: Option<Arc<TrackLocalStaticRTP>>,
) {
    match codec {
        AudioCodec::Opus => audio_task_opus(track, rtsp, remote_id, counters, republish).await,
        AudioCodec::Aac => audio_task_aac(track, rtsp, remote_id, counters, republish).await,
    }
}

async fn audio_task_opus(
    track: Arc<TrackRemote>,
    rtsp: Arc<RtspServer>,
    remote_id: String,
    counters: Arc<RelayCounters>,
    republish: Option<Arc<TrackLocalStaticRTP>>,
) {
    loop {
        match track.read_rtp().await {
            Ok((packet, _attrs)) => {
                if let Some(republish) = &republish {
                    if let Err(error) = republish.write_rtp(&packet).await {
                        debug!(%remote_id, %error, "cascade: republishing audio RTP packet failed");
                    }
                }
                rtsp.send_audio_frame(&packet.payload, packet.header.timestamp).await;
                counters.audio_frames.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                debug!(%remote_id, %error, "relay: audio track ended");
                break;
            }
        }
    }
}

async fn audio_task_aac(
    track: Arc<TrackRemote>,
    rtsp: Arc<RtspServer>,
    remote_id: String,
    counters: Arc<RelayCounters>,
    republish: Option<Arc<TrackLocalStaticRTP>>,
) {
    let mut transcoder = match OpusToAac::new() {
        Ok(t) => t,
        Err(error) => {
            warn!(%remote_id, %error, "relay: failed to set up Opus->AAC transcoder; audio disabled for this share");
            return;
        }
    };

    loop {
        match track.read_rtp().await {
            Ok((packet, _attrs)) => {
                if let Some(republish) = &republish {
                    if let Err(error) = republish.write_rtp(&packet).await {
                        debug!(%remote_id, %error, "cascade: republishing audio RTP packet failed");
                    }
                }
                for (frame, ts) in transcoder.push(&packet.payload, packet.header.timestamp) {
                    rtsp.send_audio_frame(&frame, ts).await;
                    counters.audio_frames.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(error) => {
                debug!(%remote_id, %error, "relay: audio track ended");
                break;
            }
        }
    }
}

/// Logs one INFO-level throughput summary every [`SUMMARY_INTERVAL`] -- the
/// daemon's default `mistl=info` filter otherwise shows nothing between the
/// lock/attach milestones, so this is what lets someone watching the log
/// confirm at a glance that video and audio are still flowing and how many
/// local VRChat/RTSP viewers are attached. Reads and resets `counters` each
/// tick to derive a per-second rate; when no publisher is locked it logs a
/// single quieter line instead of a zeroed-out throughput line.
async fn summary_task(rtsp: Arc<RtspServer>, publisher: Arc<StdMutex<Option<String>>>, counters: Arc<RelayCounters>) {
    let mut interval = tokio::time::interval(SUMMARY_INTERVAL);
    interval.tick().await; // first tick fires immediately; nothing forwarded yet

    loop {
        interval.tick().await;

        let video_au = counters.video_au.swap(0, Ordering::Relaxed);
        let audio_frames = counters.audio_frames.swap(0, Ordering::Relaxed);
        let secs = SUMMARY_INTERVAL.as_secs_f64();
        let video_au_per_s = video_au as f64 / secs;
        let audio_frames_per_s = audio_frames as f64 / secs;

        let current_publisher = {
            let guard = publisher.lock().expect("relay publisher lock poisoned");
            guard.clone()
        };

        match current_publisher {
            Some(publisher) => {
                let viewers = rtsp.client_count().await;
                info!(
                    publisher = %publisher,
                    video_au_per_s,
                    audio_frames_per_s,
                    viewers,
                    "relay: throughput"
                );
            }
            None => info!("relay: waiting for a screen share in the room"),
        }
    }
}

/// Groups depacketized H264 output (Annex-B bytes, possibly empty for an
/// in-progress FU-A, possibly multiple NALs at once for a STAP-A) into
/// access units: packets sharing an RTP timestamp belong to the same AU: the
/// marker bit closes it, and -- tolerating loss -- so does a timestamp change
/// without one.
struct AuAssembler {
    ts: Option<u32>,
    buf: Vec<u8>,
}

impl AuAssembler {
    fn new() -> Self {
        Self { ts: None, buf: Vec::new() }
    }

    /// Feeds one packet's depacketized chunk. Returns zero, one, or (on a
    /// lost marker followed immediately by a new single-packet AU) two
    /// completed access units, each paired with its own RTP timestamp.
    fn push(&mut self, ts: u32, marker: bool, chunk: &[u8]) -> Vec<(u32, Vec<u8>)> {
        let mut completed = Vec::new();

        if let Some(current_ts) = self.ts {
            if current_ts != ts {
                if !self.buf.is_empty() {
                    completed.push((current_ts, std::mem::take(&mut self.buf)));
                }
                self.ts = None;
            }
        }
        self.ts.get_or_insert(ts);
        self.buf.extend_from_slice(chunk);

        if marker {
            completed.push((ts, std::mem::take(&mut self.buf)));
            self.ts = None;
        }

        completed
    }
}

/// SPS/PPS NAL bytes (header byte onward, start code stripped -- matching
/// what `RtspServer::update_sps`/`update_pps` and its SDP `sprop-parameter-
/// sets` base64 encoding expect) and whether the AU contains an IDR slice.
struct AuNals {
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    has_idr: bool,
}

fn scan_au_nals(au: &[u8]) -> AuNals {
    let mut result = AuNals { sps: None, pps: None, has_idr: false };
    for nal in iter_annex_b_nals(au) {
        if nal.is_empty() {
            continue;
        }
        match nal[0] & 0x1F {
            NAL_TYPE_SPS => result.sps = Some(nal.to_vec()),
            NAL_TYPE_PPS => result.pps = Some(nal.to_vec()),
            NAL_TYPE_IDR => result.has_idr = true,
            _ => {}
        }
    }
    result
}

/// Iterates the NAL units (header byte onward, 4-byte `00 00 00 01` start
/// codes stripped) in an Annex-B buffer. `H264Packet::depacketize` always
/// emits the 4-byte start code (see [`ANNEXB_NALUSTART_CODE`] in the `rtp`
/// crate), never the 3-byte variant, so that's the only form handled here.
fn iter_annex_b_nals(au: &[u8]) -> Vec<&[u8]> {
    const START: [u8; 4] = [0, 0, 0, 1];
    let mut nals = Vec::new();
    let mut i = 0;
    while i + 4 <= au.len() {
        if au[i..i + 4] == START {
            let start = i + 4;
            let mut j = start;
            while j + 4 <= au.len() && au[j..j + 4] != START {
                j += 1;
            }
            let end = if j + 4 <= au.len() { j } else { au.len() };
            if end > start {
                nals.push(&au[start..end]);
            }
            i = end;
        } else {
            i += 1;
        }
    }
    nals
}

/// Decodes Opus RTP payloads to i16 PCM, accumulates them, and encodes one
/// AAC-LC RAW frame per 1024 samples/channel -- mistlink's scheme: `ts` is
/// seeded from the first Opus packet's RTP timestamp, then stepped by 1024
/// per emitted frame (Opus's RTP clock is also 48 kHz, so the two stay
/// aligned).
struct OpusToAac {
    decoder: opus::Decoder,
    encoder: fdk_aac::enc::Encoder,
    ring: VecDeque<i16>,
    next_ts: Option<u32>,
}

impl OpusToAac {
    fn new() -> Result<Self> {
        let decoder = opus::Decoder::new(rtp_out::AUDIO_CLOCK_RATE, opus::Channels::Stereo)
            .map_err(|error| anyhow!("creating Opus decoder: {error}"))
            .context("relay audio setup")?;

        let encoder = fdk_aac::enc::Encoder::new(fdk_aac::enc::EncoderParams {
            bit_rate: fdk_aac::enc::BitRate::Cbr(128_000),
            sample_rate: rtp_out::AUDIO_CLOCK_RATE,
            transport: fdk_aac::enc::Transport::Raw,
            channels: fdk_aac::enc::ChannelMode::Stereo,
            audio_object_type: fdk_aac::enc::AudioObjectType::Mpeg4LowComplexity,
        })
        .map_err(|error| anyhow!("creating AAC encoder: {error}"))?;

        Ok(Self {
            decoder,
            encoder,
            ring: VecDeque::new(),
            next_ts: None,
        })
    }

    /// Feeds one Opus RTP payload (with its packet's RTP timestamp),
    /// returning zero or more `(aac_frame_bytes, rtp_ts)` pairs ready to send.
    fn push(&mut self, opus_payload: &[u8], rtp_ts: u32) -> Vec<(Vec<u8>, u32)> {
        self.next_ts.get_or_insert(rtp_ts);

        let mut pcm = [0i16; MAX_OPUS_DECODE_SAMPLES];
        let samples_per_channel = match self.decoder.decode(opus_payload, &mut pcm, false) {
            Ok(n) => n,
            Err(error) => {
                warn!(%error, "relay: Opus decode failed, dropping packet");
                return Vec::new();
            }
        };
        self.ring.extend(pcm[..samples_per_channel * AAC_CHANNELS].iter().copied());

        let mut out = Vec::new();
        let mut output_buf = [0u8; 4096];
        while self.ring.len() >= AAC_FRAME_SAMPLES * AAC_CHANNELS {
            let frame: Vec<i16> = self.ring.drain(..AAC_FRAME_SAMPLES * AAC_CHANNELS).collect();
            let out_ts = *self.next_ts.get_or_insert(rtp_ts);
            self.next_ts = Some(out_ts.wrapping_add(AAC_FRAME_SAMPLES as u32));

            match self.encoder.encode(&frame, &mut output_buf) {
                Ok(info) if info.output_size > 0 => {
                    out.push((output_buf[..info.output_size].to_vec(), out_ts));
                }
                Ok(_) => {} // encoder priming: no output yet for this frame
                Err(error) => warn!(%error, "relay: AAC encode failed, dropping frame"),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mistlib::webrtc::rtp::packet::Packet as RtpPacket;

    // --- publisher lock-in policy (no cascade / cascade disabled) ----------

    #[test]
    fn decide_video_locks_when_unlocked() {
        assert!(decide_video(None, "peer-a", &CascadePolicy::Disabled));
    }

    #[test]
    fn decide_video_ignores_when_already_locked() {
        assert!(!decide_video(Some("peer-a"), "peer-b", &CascadePolicy::Disabled));
    }

    #[test]
    fn decide_audio_buffers_when_unlocked() {
        assert_eq!(
            decide_audio(None, "peer-a", false, &CascadePolicy::Disabled),
            AudioDecision::Buffer
        );
    }

    #[test]
    fn decide_audio_attaches_for_the_locked_peer() {
        assert_eq!(
            decide_audio(Some("peer-a"), "peer-a", false, &CascadePolicy::Disabled),
            AudioDecision::Attach
        );
    }

    #[test]
    fn decide_audio_ignores_second_audio_track_from_locked_peer() {
        assert_eq!(
            decide_audio(Some("peer-a"), "peer-a", true, &CascadePolicy::Disabled),
            AudioDecision::Ignore
        );
    }

    #[test]
    fn decide_audio_ignores_other_peers_while_locked() {
        assert_eq!(
            decide_audio(Some("peer-a"), "peer-b", false, &CascadePolicy::Disabled),
            AudioDecision::Ignore
        );
    }

    // --- cascade policy matrix ----------------------------------------------

    fn active_policy(role: ConsensusRole, leader: Option<&str>, relay_peers: &[&str]) -> CascadePolicy {
        CascadePolicy::Active {
            role,
            leader: leader.map(str::to_string),
            relay_peers: relay_peers.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn decide_video_leader_accepts_a_non_relay_peer_as_the_sharer() {
        let policy = active_policy(ConsensusRole::Leader, Some("self-id"), &["self-id", "relay-2"]);
        assert!(decide_video(None, "browser-sharer", &policy));
    }

    #[test]
    fn decide_video_leader_ignores_tracks_from_other_relay_peers() {
        let policy = active_policy(ConsensusRole::Leader, Some("self-id"), &["self-id", "relay-2"]);
        assert!(!decide_video(None, "relay-2", &policy));
    }

    #[test]
    fn decide_video_follower_accepts_only_the_leader_and_ignores_the_sharer() {
        let policy = active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert!(decide_video(None, "relay-1", &policy));
        assert!(!decide_video(None, "browser-sharer", &policy));
    }

    #[test]
    fn decide_video_does_not_lock_before_a_role_or_leader_is_known() {
        for policy in [
            active_policy(ConsensusRole::Unknown, None, &["self-id"]),
            active_policy(ConsensusRole::Candidate, None, &["self-id"]),
            active_policy(ConsensusRole::Follower, None, &["self-id"]),
        ] {
            assert!(!decide_video(None, "anyone", &policy), "{policy:?} should not lock yet");
        }
    }

    #[test]
    fn decide_video_role_transition_relocks_under_the_new_policy() {
        // Following the old leader...
        let following_old_leader = active_policy(
            ConsensusRole::Follower,
            Some("relay-1"),
            &["self-id", "relay-1", "relay-2"],
        );
        assert!(decide_video(None, "relay-1", &following_old_leader));

        // ...after a leader change (control_loop unlocks first), the same
        // node now follows the new leader instead, and no longer the old one.
        let following_new_leader = active_policy(
            ConsensusRole::Follower,
            Some("relay-2"),
            &["self-id", "relay-1", "relay-2"],
        );
        assert!(decide_video(None, "relay-2", &following_new_leader));
        assert!(!decide_video(None, "relay-1", &following_new_leader));
    }

    #[test]
    fn decide_video_role_transition_from_follower_to_leader_accepts_the_sharer() {
        let as_follower = active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert!(!decide_video(None, "browser-sharer", &as_follower));

        let as_leader = active_policy(ConsensusRole::Leader, Some("self-id"), &["self-id", "relay-1"]);
        assert!(decide_video(None, "browser-sharer", &as_leader));
    }

    #[test]
    fn decide_audio_follower_ignores_the_sharer_but_buffers_the_leader() {
        let policy = active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert_eq!(
            decide_audio(None, "browser-sharer", false, &policy),
            AudioDecision::Ignore
        );
        assert_eq!(decide_audio(None, "relay-1", false, &policy), AudioDecision::Buffer);
    }

    #[test]
    fn decide_audio_leader_ignores_other_relay_peers_but_buffers_the_sharer() {
        let policy = active_policy(ConsensusRole::Leader, Some("self-id"), &["self-id", "relay-2"]);
        assert_eq!(decide_audio(None, "relay-2", false, &policy), AudioDecision::Ignore);
        assert_eq!(
            decide_audio(None, "browser-sharer", false, &policy),
            AudioDecision::Buffer
        );
    }

    // --- cascade status JSON shape ------------------------------------------

    #[test]
    fn cascade_status_json_leader_locked_reports_source_sharer() {
        let value = cascade_status_json(
            ConsensusRole::Leader,
            Some("self-id".to_string()),
            "self-id",
            vec!["self-id".to_string(), "relay-2".to_string()],
            true,
        );
        assert_eq!(value["enabled"], json!(true));
        assert_eq!(value["role"], json!("leader"));
        assert_eq!(value["leader"], json!("self-id"));
        assert_eq!(value["self"], json!("self-id"));
        assert_eq!(value["relay_peers"], json!(["self-id", "relay-2"]));
        assert_eq!(value["source"], json!("sharer"));
    }

    #[test]
    fn cascade_status_json_follower_locked_reports_source_leader() {
        let value = cascade_status_json(
            ConsensusRole::Follower,
            Some("relay-1".to_string()),
            "self-id",
            vec!["self-id".to_string(), "relay-1".to_string()],
            true,
        );
        assert_eq!(value["role"], json!("follower"));
        assert_eq!(value["leader"], json!("relay-1"));
        assert_eq!(value["source"], json!("leader"));
    }

    #[test]
    fn cascade_status_json_unlocked_reports_null_source() {
        let value = cascade_status_json(
            ConsensusRole::Follower,
            Some("relay-1".to_string()),
            "self-id",
            vec!["self-id".to_string()],
            false,
        );
        assert_eq!(value["source"], Value::Null);
    }

    #[test]
    fn cascade_status_json_no_leader_yet_reports_null_leader_and_source() {
        let value = cascade_status_json(
            ConsensusRole::Candidate,
            None,
            "self-id",
            vec!["self-id".to_string()],
            false,
        );
        assert_eq!(value["leader"], Value::Null);
        assert_eq!(value["source"], Value::Null);
    }

    // --- H264 RTP -> Annex-B AU regrouping ---------------------------------

    fn rtp_packet(payload: Vec<u8>, timestamp: u32, marker: bool, seq: u16) -> RtpPacket {
        let mut packet = RtpPacket::default();
        packet.header.timestamp = timestamp;
        packet.header.marker = marker;
        packet.header.sequence_number = seq;
        packet.payload = payload.into();
        packet
    }

    /// A tiny synthetic H264 NAL payload: header byte + a few RBSP bytes.
    fn fake_nal(nal_type: u8, rbsp: &[u8]) -> Vec<u8> {
        let mut nal = vec![nal_type & 0x1F];
        nal.extend_from_slice(rbsp);
        nal
    }

    fn stap_a(nals: &[Vec<u8>]) -> Vec<u8> {
        let mut payload = vec![24u8]; // STAP-A NAL header type
        for nal in nals {
            payload.extend_from_slice(&(nal.len() as u16).to_be_bytes());
            payload.extend_from_slice(nal);
        }
        payload
    }

    fn fu_a_fragments(nal_type: u8, nal_ref_idc: u8, rbsp: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
        let mut fragments = Vec::new();
        let chunks: Vec<&[u8]> = rbsp.chunks(chunk_size).collect();
        let last = chunks.len() - 1;
        for (i, chunk) in chunks.iter().enumerate() {
            let indicator = 28u8 | nal_ref_idc; // FU-A
            let mut header = nal_type & 0x1F;
            if i == 0 {
                header |= 0x80; // start bit
            }
            if i == last {
                header |= 0x40; // end bit
            }
            let mut fragment = vec![indicator, header];
            fragment.extend_from_slice(chunk);
            fragments.push(fragment);
        }
        fragments
    }

    #[test]
    fn au_assembler_regroups_stap_a_sps_pps_then_fua_idr() {
        let sps = fake_nal(NAL_TYPE_SPS, &[0xAA, 0xBB]);
        let pps = fake_nal(NAL_TYPE_PPS, &[0xCC]);
        let idr_rbsp = vec![0x11u8; 30];

        let stap_payload = stap_a(&[sps.clone(), pps.clone()]);
        let fua_payloads = fu_a_fragments(NAL_TYPE_IDR, 0x60, &idr_rbsp, 10);

        let mut depacketizer = H264Packet::default();
        let mut assembler = AuAssembler::new();
        let ts = 90_000;

        let mut completed = Vec::new();

        // STAP-A: SPS+PPS, not the marker packet.
        let packet = rtp_packet(stap_payload, ts, false, 1);
        let chunk = depacketizer.depacketize(&packet.payload).unwrap();
        completed.extend(assembler.push(packet.header.timestamp, packet.header.marker, &chunk));

        // FU-A fragments of the IDR slice; only the last carries the marker.
        for (i, fragment) in fua_payloads.iter().enumerate() {
            let marker = i == fua_payloads.len() - 1;
            let packet = rtp_packet(fragment.clone(), ts, marker, 2 + i as u16);
            let chunk = depacketizer.depacketize(&packet.payload).unwrap();
            completed.extend(assembler.push(packet.header.timestamp, packet.header.marker, &chunk));
        }

        assert_eq!(completed.len(), 1, "exactly one AU should close on the marker");
        let (au_ts, au) = &completed[0];
        assert_eq!(*au_ts, ts, "the AU should carry its own RTP timestamp");

        let nals = scan_au_nals(au);
        assert_eq!(nals.sps, Some(sps));
        assert_eq!(nals.pps, Some(pps));
        assert!(nals.has_idr, "IDR NAL (type 5) should be detected");

        // Sanity-check the raw bytes: three start-code-prefixed NALs in order.
        let found: Vec<&[u8]> = iter_annex_b_nals(au);
        assert_eq!(found.len(), 3);
        assert_eq!(found[0][0] & 0x1F, NAL_TYPE_SPS);
        assert_eq!(found[1][0] & 0x1F, NAL_TYPE_PPS);
        assert_eq!(found[2][0] & 0x1F, NAL_TYPE_IDR);
        let mut reassembled_idr = vec![found[2][0]];
        reassembled_idr.extend_from_slice(&idr_rbsp);
        assert_eq!(found[2], &reassembled_idr[..]);
    }

    #[test]
    fn au_assembler_closes_au_on_timestamp_change_without_marker() {
        let mut assembler = AuAssembler::new();

        // First AU never gets a marker (simulated packet loss) but a new
        // timestamp arrives -- it should still close.
        let first = assembler.push(1000, false, &[0xAA]);
        assert!(first.is_empty());
        let second = assembler.push(2000, false, &[0xBB]);
        assert_eq!(second, vec![(1000, vec![0xAA])], "closed AU keeps its own timestamp");
    }

    #[test]
    fn au_assembler_single_nal_no_fragmentation() {
        let mut depacketizer = H264Packet::default();
        let mut assembler = AuAssembler::new();
        let nal = fake_nal(NAL_TYPE_IDR, &[1, 2, 3]);
        let packet = rtp_packet(nal.clone(), 500, true, 1);
        let chunk = depacketizer.depacketize(&packet.payload).unwrap();
        let completed = assembler.push(packet.header.timestamp, packet.header.marker, &chunk);
        assert_eq!(completed.len(), 1);
        let mut expected = vec![0, 0, 0, 1];
        expected.extend_from_slice(&nal);
        assert_eq!(completed[0], (500, expected));
    }

    // --- Opus -> AAC audio pipeline ----------------------------------------

    /// Encodes a 48 kHz stereo sine wave with the `opus` crate's own encoder
    /// (round-tripping through the real codec, not a hand-rolled fixture),
    /// then verifies the decode -> ring-buffer -> AAC-LC path emits frames
    /// with timestamps stepping by exactly 1024.
    #[test]
    fn opus_to_aac_emits_frames_stepping_by_1024() {
        let sample_rate = rtp_out::AUDIO_CLOCK_RATE;
        let channels = 2usize;
        let frame_samples = 960usize; // 20ms @ 48kHz, a common Opus frame size
        let num_frames = 40; // 800ms of audio, several AAC frames' worth

        let mut opus_encoder = opus::Encoder::new(sample_rate, opus::Channels::Stereo, opus::Application::Audio)
            .expect("creating Opus encoder");

        let mut transcoder = OpusToAac::new().expect("creating Opus->AAC transcoder");

        let start_ts: u32 = 12_345;
        let mut ts = start_ts;
        let mut emitted: Vec<(usize, u32)> = Vec::new();

        for frame_index in 0..num_frames {
            let mut pcm = vec![0i16; frame_samples * channels];
            for i in 0..frame_samples {
                let t = (frame_index * frame_samples + i) as f32 / sample_rate as f32;
                let sample = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * i16::MAX as f32 * 0.25;
                pcm[i * channels] = sample as i16;
                pcm[i * channels + 1] = sample as i16;
            }

            let mut opus_payload = vec![0u8; 4000];
            let len = opus_encoder.encode(&pcm, &mut opus_payload).expect("Opus encode");
            opus_payload.truncate(len);

            for (frame, out_ts) in transcoder.push(&opus_payload, ts) {
                assert!(frame.len() > 0, "AAC frame should be non-empty");
                emitted.push((frame.len(), out_ts));
            }

            ts = ts.wrapping_add(frame_samples as u32);
        }

        assert!(
            emitted.len() >= 5,
            "expected multiple AAC frames from {num_frames} Opus frames, got {}",
            emitted.len()
        );

        for pair in emitted.windows(2) {
            let (_, ts_a) = pair[0];
            let (_, ts_b) = pair[1];
            assert_eq!(
                ts_b.wrapping_sub(ts_a),
                AAC_FRAME_SAMPLES as u32,
                "consecutive AAC frame timestamps must step by exactly {AAC_FRAME_SAMPLES}"
            );
        }

        for (size, _) in &emitted {
            assert!(*size > 0 && *size < 1024, "AAC-LC frame at 128kbps/1024 samples should be well under 1KB, got {size}");
        }
    }
}
