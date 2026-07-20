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
//!    periodically thereafter (browsers only emit keyframes on request; this
//!    also bounds a late-joining viewer's or a loss-recovery's keyframe wait
//!    to one [`PLI_INTERVAL`]). mistlib registers `nack`/`pli` RTCP feedback
//!    in its SDP but never wires up an interceptor registry, so no NACK
//!    retransmission or reorder/jitter buffering actually happens upstream of
//!    `read_rtp` (see `video_task`'s handling of `awaiting_keyframe`); every
//!    RTP packet is checked for a sequence-number gap and any access unit
//!    that might be corrupt as a result is dropped until the next IDR rather
//!    than forwarded to `rtsp`; the gap (or a depacketizer reset) also fires
//!    an *immediate*, [`IMMEDIATE_PLI_DEBOUNCE`]-debounced PLI so the
//!    recovery keyframe is requested right away instead of waiting out the
//!    periodic cadence.
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
//! ## Screen switches (same peer, new track)
//!
//! When the locked publisher changes *which* screen/window it's sharing,
//! tc-chat unpublishes its old `tc-chat-screen:<uuid>`/`tc-chat-screen-audio:
//! <uuid>` tracks and publishes fresh ones (new uuids) -- but mistlib's WebRTC
//! stack has no "track removed" signal: renegotiation just marks the old
//! m-line inactive, so the old `TrackRemote::read_rtp` loop keeps blocking
//! rather than erroring out. Relying on that error to clear the lock (as a
//! plain "first video wins" policy would) leaves `locked` pointing at a dead
//! track forever, so the arriving replacement video/audio tracks -- same
//! peer, different track -- would be silently ignored and VRChat would stay
//! frozen on the switch's last frame. [`decide_video`]/[`decide_audio`] treat
//! a new track from the *already-locked* peer as [`VideoDecision::Switch`]/
//! [`AudioDecision::Replace`] rather than `Ignore`: `control_loop` aborts just
//! the superseded video/PLI (or audio) task and spawns fresh ones for the new
//! track, via [`replace_task`], leaving the lock, any cascade re-broadcast,
//! and the other media type's task untouched. The new video task starts
//! `awaiting_keyframe`, and its PLI task requests one immediately on spawn, so
//! output resumes as soon as the browser answers that PLI -- typically well
//! under the PLI round trip plus one encoder keyframe, not the old
//! [`PLI_INTERVAL`] cadence. A genuine "video track ended" notification for a
//! track a switch has already superseded is recognized and ignored by ssrc
//! (see `control_loop`'s `ended_rx` arm) rather than misread as the *new*
//! track ending.
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
//!
//! ## Audio observability
//!
//! "No sound in VRChat" is otherwise invisible from the outside: the server
//! plumbing (this module, plus `rtp_out`/`rtsp`) has no bug in the common
//! case -- the far more frequent cause is the browser sharer never ticking
//! "share audio" in the `getDisplayMedia` picker, which tc-chat's
//! `useScreenShare` now surfaces client-side. To make the rest of the
//! pipeline diagnosable from the relay side too, [`RelayCounters`] tracks,
//! cumulatively, how many audio RTP packets/bytes were received from the
//! locked publisher's track, how many frames (post-transcode, if
//! [`AudioCodec::Aac`]) were actually forwarded to `rtsp`, and how many
//! Opus-decode/AAC-encode calls failed. [`RelayCapture::audio_status`]
//! exposes these plus whether an audio track is currently attached, under
//! `stream.status`'s `audio` field. `summary_task` also logs a throttled
//! (once per [`SUMMARY_INTERVAL`]) WARN whenever video is visibly flowing
//! but no audio frame was forwarded that interval -- the two possible
//! causes (no track attached at all, vs. an attached track producing
//! nothing) get distinct messages so the log alone usually points at the
//! right stage.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

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

/// Minimum spacing between the *immediate* loss-triggered PLIs `video_task`
/// sends when it detects an RTP sequence gap (or has to reset a desynced
/// depacketizer). Without the immediate send, a loss event would sit in the
/// dropping-until-IDR state for up to a full [`PLI_INTERVAL`] waiting for
/// `pli_task`'s next scheduled tick -- long enough to trip the RTSP dummy
/// keepalive (1s) and, on lossy links, to make VRChat viewers reach for the
/// manual Resync. The debounce keeps a burst of back-to-back gaps (one lossy
/// spike is many gapped packets) from turning into a PLI storm at the
/// browser: one immediate PLI per window, with the periodic `pli_task` still
/// running as the fallback.
const IMMEDIATE_PLI_DEBOUNCE: Duration = Duration::from_secs(1);

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
    /// Cumulative (never reset) audio observability counters, exposed via
    /// `stream.status`'s `audio` field (see [`RelayCapture::audio_status`])
    /// so someone debugging "no sound in VRChat" can tell apart, from the
    /// outside, which stage of the pipeline actually stalled:
    /// `rtp_packets`/`rtp_bytes` stuck at zero means no audio RTP was ever
    /// received from the publisher's track (most likely: they never ticked
    /// "share audio" in the browser's picker -- see tc-chat's
    /// `useScreenShare`); `rtp_packets` climbing but `frames_sent` flat means
    /// the Opus->AAC transcode is failing (check `transcode_errors`);
    /// `frames_sent` climbing steadily means audio is reaching `rtsp` fine
    /// and the problem is downstream (AVPro/VRChat, or the RTSP session
    /// itself -- cross-check `RtspServer::flow_ages_ms`'s `audio` age).
    audio_rtp_packets_total: AtomicU64,
    audio_rtp_bytes_total: AtomicU64,
    audio_frames_sent_total: AtomicU64,
    audio_transcode_errors_total: AtomicU64,
}

/// Identifies one of the forwarding tasks kept in [`RelayCapture::active_tasks`]
/// (now keyed rather than a flat list) so a screen switch can replace just the
/// video/PLI (or just the audio) task for the current publisher without
/// disturbing the others -- in particular without aborting [`TaskRole::Summary`],
/// which must survive across lock/unlock cycles for the entire lifetime of the
/// relay, not just one publisher's lock. See [`replace_task`]/[`abort_role`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TaskRole {
    /// The periodic throughput/idle log line (`summary_task`) -- lives for the
    /// whole `RelayCapture`, spawned once in `RelayCapture::spawn`.
    Summary,
    /// The current publisher's video-forwarding task (`video_task`).
    Video,
    /// The current publisher's PLI-request task (`pli_task`).
    Pli,
    /// The current publisher's audio-forwarding task (`audio_task`).
    Audio,
}

/// Installs `handle` under `role`, aborting whatever task was previously
/// registered there (if any) -- e.g. a screen switch replacing the old
/// video/PLI task with a fresh one for the new track. Dropping a `JoinHandle`
/// does *not* abort the task it refers to, so the previous handle must be
/// aborted explicitly or its task would keep running detached forever.
fn replace_task(active_tasks: &Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>>, role: TaskRole, handle: JoinHandle<()>) {
    let previous = active_tasks
        .lock()
        .expect("relay active tasks lock poisoned")
        .insert(role, handle);
    if let Some(previous) = previous {
        previous.abort();
    }
}

/// Aborts and removes the task registered under `role`, if any -- used to
/// tear down one specific role (e.g. just video/PLI/audio on unlock) without
/// touching the others, unlike the full drain-everything abort `stop()` does.
fn abort_role(active_tasks: &Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>>, role: TaskRole) {
    if let Some(handle) = active_tasks
        .lock()
        .expect("relay active tasks lock poisoned")
        .remove(&role)
    {
        handle.abort();
    }
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
    active_tasks: Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>>,
    control_task: JoinHandle<()>,
    cascade: Cascade,
    /// Whether the locked publisher's audio track is currently attached to a
    /// forwarding task (mirrors `control_loop`'s local `audio_attached`, see
    /// [`Self::audio_status`]).
    audio_attached: Arc<AtomicBool>,
    /// Cumulative audio observability counters, shared with the
    /// audio-forwarding tasks (see [`RelayCounters`]'s doc).
    counters: Arc<RelayCounters>,
    /// This node's mistlib node id -- needed for the `cascade.self` status
    /// field independent of whether consensus is active.
    node_id: String,
    /// The room id joined on the wire -- the same raw room id tc-chat joins
    /// under (no derivation), and what `mistlib::publish_local_track` /
    /// `unpublish_local_track` expect as their `room`.
    room: String,
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
        // tc-chat joins its rooms under the raw room id -- no derivation, no
        // prefix -- so this joins the exact same swarm topic tc-chat uses,
        // and lands the relay in the same room it can see the shared screen
        // from. The same room id is also what the consensus control plane and
        // `mistlib::publish_local_track`/`unpublish_local_track` need as
        // their `room` -- it's the actual on-wire session id.
        crate::net::ensure_started(state, room.clone()).await?;

        let node_id = crate::identity::current(state)
            .await
            .context("relay: loading identity for cascade")?
            .node_id();

        let cascade = if state.config().stream.cascade {
            match RelayConsensus::start(state, room.clone()).await {
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
        let active_tasks: Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>> = Arc::new(StdMutex::new(HashMap::new()));
        let counters = Arc::new(RelayCounters::default());
        let republish: Arc<StdMutex<Option<RepublishTracks>>> = Arc::new(StdMutex::new(None));
        let audio_attached = Arc::new(AtomicBool::new(false));

        let control_task = tokio::spawn(control_loop(
            rx,
            rtsp.clone(),
            audio_codec,
            publisher.clone(),
            active_tasks.clone(),
            counters.clone(),
            cascade.clone(),
            room.clone(),
            republish.clone(),
            audio_attached.clone(),
        ));

        let summary_handle = tokio::spawn(summary_task(
            rtsp,
            publisher.clone(),
            counters.clone(),
            audio_attached.clone(),
        ));
        active_tasks
            .lock()
            .expect("relay active tasks lock poisoned")
            .insert(TaskRole::Summary, summary_handle);

        Ok(Self {
            publisher,
            active_tasks,
            control_task,
            cascade,
            node_id,
            room,
            republish,
            audio_attached,
            counters,
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

    /// `stream.status`'s `audio` field: whether the locked publisher's audio
    /// track is currently attached, plus the cumulative observability
    /// counters from [`RelayCounters`] -- see that type's doc for how to read
    /// them (in short: `rtp_packets` stuck at zero means the publisher never
    /// sent audio RTP at all, most likely a "share audio" checkbox never
    /// ticked in the browser's picker; `rtp_packets` growing but
    /// `frames_sent` flat points at the transcode; `frames_sent` growing
    /// points downstream of `rtsp`).
    pub fn audio_status(&self) -> Value {
        audio_status_json(
            self.audio_attached.load(Ordering::Relaxed),
            self.counters.audio_rtp_packets_total.load(Ordering::Relaxed),
            self.counters.audio_rtp_bytes_total.load(Ordering::Relaxed),
            self.counters.audio_frames_sent_total.load(Ordering::Relaxed),
            self.counters.audio_transcode_errors_total.load(Ordering::Relaxed),
        )
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
        for (_, task) in self
            .active_tasks
            .lock()
            .expect("relay active tasks lock poisoned")
            .drain()
        {
            task.abort();
        }

        let tracks = self
            .republish
            .lock()
            .expect("relay republish lock poisoned")
            .take();
        if let Some((video, audio)) = tracks {
            if let Err(error) = mistlib::unpublish_local_track(&self.room, video).await {
                debug!(%error, "cascade: unpublishing video re-broadcast track on stop failed");
            }
            if let Err(error) = mistlib::unpublish_local_track(&self.room, audio).await {
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

/// Pure JSON builder for the `audio` field of `stream.status` (see
/// [`RelayCapture::audio_status`]'s doc for how to read the shape).
fn audio_status_json(attached: bool, rtp_packets: u64, rtp_bytes: u64, frames_sent: u64, transcode_errors: u64) -> Value {
    json!({
        "attached": attached,
        "rtp_packets": rtp_packets,
        "rtp_bytes": rtp_bytes,
        "frames_sent": frames_sent,
        "transcode_errors": transcode_errors,
    })
}

/// What to do with an incoming *audio* track, given the current lock state.
/// Kept (with [`decide_video`]) as pure functions -- no network/task types --
/// so the lock-in policy is unit-testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AudioDecision {
    /// Peer is already locked and this is its (not-yet-attached) audio track.
    Attach,
    /// Peer is already locked *and already has an attached audio task*, but
    /// this is a new (different) audio track from that same peer -- e.g. tc-chat
    /// re-sharing with a fresh `getDisplayMedia` capture, which publishes a
    /// brand-new audio track uuid alongside the new video track. Since mistlib
    /// never signals "track ended" on the old one (see [`VideoDecision::Switch`]'s
    /// doc), this is the only way the relay learns the old audio track is
    /// superseded: replace the old audio task with a new one for this track.
    Replace,
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

/// Decision for an incoming *video* track, given the current lock state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoDecision {
    /// No publisher locked yet, and this one is accepted: lock onto it.
    Lock,
    /// This is a new video track from the *already-locked* peer -- a screen
    /// switch (tc-chat unpublishes the old `tc-chat-screen:<uuid>` track and
    /// publishes a new one with a fresh uuid when the user picks a different
    /// window/screen). mistlib's WebRTC stack never signals "track ended" for
    /// the old sender -- renegotiation just marks its m-line inactive, so the
    /// old `TrackRemote::read_rtp` loop keeps blocking rather than erroring
    /// out -- so without this arm the lock-in policy's `locked.is_none()`
    /// check would stay false forever and silently ignore every video track
    /// the peer ever publishes again after the first. Resuming on the new
    /// track (aborting and replacing just the video/PLI forwarding tasks, see
    /// `control_loop`) is what makes a screen switch resume output instead of
    /// leaving VRChat frozen on the last frame from the old share.
    Switch,
    /// Not relevant right now (unlocked but not accepted, or locked to a
    /// different peer).
    Ignore,
}

/// Whether an incoming *video* track locks its peer as the publisher, or
/// (already locked to this same peer) supersedes its current video track.
fn decide_video(locked: Option<&str>, remote_id: &str, policy: &CascadePolicy) -> VideoDecision {
    match locked {
        None if accepts_remote(remote_id, policy) => VideoDecision::Lock,
        None => VideoDecision::Ignore,
        Some(id) if id == remote_id => VideoDecision::Switch,
        Some(_) => VideoDecision::Ignore,
    }
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
        Some(id) if id == remote_id => AudioDecision::Replace,
        Some(_) => AudioDecision::Ignore,
        None if accepts_remote(remote_id, policy) => AudioDecision::Buffer,
        None => AudioDecision::Ignore,
    }
}

/// Whether a lock on `locked` should survive a consensus view change, given
/// the *new* policy computed from the view that just arrived. This is the
/// keep-lock predicate `control_loop`'s `watch_changed` arm applies before
/// unlocking on a role/leader change: if the already-locked publisher is
/// still someone `policy` would accept, unlocking would just force an
/// unnecessary re-lock (interrupted output, a keyframe wait) onto the exact
/// same source. Mirrors `decide_video`/`decide_audio` in being kept as pure
/// logic so the cascade takeover matrix stays unit-testable without a real
/// `control_loop`.
fn lock_survives_view_change(locked: Option<&str>, policy: &CascadePolicy) -> bool {
    locked.is_some_and(|id| accepts_remote(id, policy))
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
/// publishes both into `room` via `mistlib::publish_local_track`. Codec
/// parameters match the sharer's own tracks (H264 @ 90kHz, Opus @ 48kHz
/// stereo) since this is a raw RTP passthrough, not a transcode -- see
/// `mistlib-native/tests/loopback_media.rs` for the same shape used against
/// a live connection. If publishing the audio track fails after the video
/// track already succeeded, the video track is unpublished again so a
/// partially-published pair is never left live.
async fn create_and_publish_republish_tracks(room: &str) -> Result<RepublishTracks> {
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

    mistlib::publish_local_track(room, video.clone())
        .await
        .context("cascade: publishing re-broadcast video track")?;
    if let Err(error) = mistlib::publish_local_track(room, audio.clone()).await {
        let _ = mistlib::unpublish_local_track(room, video).await;
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
    room: &str,
    publisher: &Arc<StdMutex<Option<String>>>,
    active_tasks: &Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>>,
    republish: &Arc<StdMutex<Option<RepublishTracks>>>,
) {
    *publisher.lock().expect("relay publisher lock poisoned") = None;
    // Only the per-publisher roles -- `TaskRole::Summary` outlives any single
    // lock/unlock cycle (it logs "waiting for a screen share" while unlocked)
    // and must not be aborted here.
    for role in [TaskRole::Video, TaskRole::Pli, TaskRole::Audio] {
        abort_role(active_tasks, role);
    }

    let tracks = republish
        .lock()
        .expect("relay republish lock poisoned")
        .take();
    if let Some((video, audio)) = tracks {
        if let Err(error) = mistlib::unpublish_local_track(room, video).await {
            debug!(%error, "cascade: unpublishing video re-broadcast track failed");
        }
        if let Err(error) = mistlib::unpublish_local_track(room, audio).await {
            debug!(%error, "cascade: unpublishing audio re-broadcast track failed");
        }
    }
}

/// [`MediaTrackEvent`] isn't `Clone` upstream, but every field is an `Arc`
/// (or a cheap id), so a field-wise clone shares the same live track.
fn clone_event(event: &MediaTrackEvent) -> MediaTrackEvent {
    MediaTrackEvent {
        remote_id: event.remote_id.clone(),
        track: event.track.clone(),
        receiver: event.receiver.clone(),
        pc: event.pc.clone(),
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
    active_tasks: Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>>,
    counters: Arc<RelayCounters>,
    cascade: Cascade,
    room: String,
    republish: Arc<StdMutex<Option<RepublishTracks>>>,
    audio_attached_shared: Arc<AtomicBool>,
) {
    // Carries (remote_id, video ssrc) rather than just remote_id: a screen
    // switch replaces the video task for the same peer (see `VideoDecision::Switch`
    // below) without going through a full unlock, so a *stale* end notification
    // from the superseded track (if the old `read_rtp` loop ever does error out
    // once the old track is fully torn down) must not be mistaken for the
    // *current* track ending and unlock a perfectly healthy new one. Only an
    // end notification whose ssrc matches `current_video_ssrc` triggers unlock.
    let (ended_tx, mut ended_rx) = mpsc::unbounded_channel::<(String, u32)>();
    let mut locked: Option<String> = None;
    // The ssrc of the video task currently forwarding, so a stale "ended"
    // notification from a track a screen-switch already superseded (see the
    // `ended_tx` doc above) can be told apart from the current track actually
    // ending.
    let mut current_video_ssrc: Option<u32> = None;
    let mut audio_attached = false;
    let mut pending_audio: HashMap<String, MediaTrackEvent> = HashMap::new();
    // The leader's own copy of the audio re-broadcast track, kept alongside
    // `locked` so a sibling audio track arriving after the video lock (the
    // common case; see `pending_audio`) still gets republished.
    let mut audio_republish: Option<Arc<TrackLocalStaticRTP>> = None;
    // The newest video/audio `MediaTrackEvent` seen per peer, kept around so
    // a cascade view change that newly *accepts* a peer whose track already
    // arrived (and was ignored under the old policy) has something to
    // re-decide against -- mistlib fires `on_track` exactly once per track,
    // so without this cache a takeover whose sharer/leader track predates the
    // view change would wait forever for an event that's never coming again.
    let mut last_video: HashMap<String, MediaTrackEvent> = HashMap::new();
    let mut last_audio: HashMap<String, MediaTrackEvent> = HashMap::new();
    // Synthetic events re-fed through the same decision path as a live
    // `media_rx` receive, populated from `last_video`/`last_audio` when a
    // view change makes a previously-ignored cached track relevant again
    // (see the `watch_changed` arm below).
    let mut replay: VecDeque<MediaTrackEvent> = VecDeque::new();

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

        // A replayed event (queued by the `watch_changed` arm below, from
        // `last_video`/`last_audio`) is taken first and re-enters the exact
        // same decision path as a live receive, evaluated against the
        // `policy` just rebuilt above from the already-updated
        // `current_view` -- that's what lets a cached track from before a
        // takeover finally lock in, since mistlib never fires `on_track`
        // twice for the same track.
        let event = if let Some(event) = replay.pop_front() {
            event
        } else {
            tokio::select! {
                event = media_rx.recv() => {
                    let Some(event) = event else { break };
                    event
                }
                Some((ended_id, ended_ssrc)) = ended_rx.recv() => {
                    // A dead track must never be replayed: if the ended
                    // track is the one cached for this peer, drop the cache
                    // entries too. If a stale one slips through anyway (e.g.
                    // a race with a screen switch), the spawned `video_task`
                    // for the replay just errors out immediately, fires
                    // `ended_tx` again, and this arm cleans up on the next
                    // pass -- self-healing, not a correctness risk.
                    if last_video.get(&ended_id).is_some_and(|cached| cached.track.ssrc() == ended_ssrc) {
                        last_video.remove(&ended_id);
                        last_audio.remove(&ended_id);
                    }

                    if locked.as_deref() == Some(ended_id.as_str()) && current_video_ssrc == Some(ended_ssrc) {
                        info!(remote_id = %ended_id, "relay: publisher's video track ended; unlocking");
                        unlock(&room, &publisher, &active_tasks, &republish).await;
                        locked = None;
                        current_video_ssrc = None;
                        audio_attached = false;
                        audio_attached_shared.store(false, Ordering::Relaxed);
                        audio_republish = None;
                        pending_audio.remove(&ended_id);
                    } else {
                        debug!(remote_id = %ended_id, ssrc = ended_ssrc, "relay: ignoring stale end notification for a superseded video track");
                    }
                    continue;
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
                                let new_policy = build_policy(&cascade, Some(&new_view));

                                if locked.is_some() {
                                    if lock_survives_view_change(locked.as_deref(), &new_policy) {
                                        // The locked publisher is still the
                                        // right source under the new policy
                                        // (e.g. we were following leader X and
                                        // the view just re-confirmed X, or
                                        // relabeled roles without actually
                                        // changing who we should watch) --
                                        // unlocking here would only force an
                                        // avoidable re-lock (a keyframe wait)
                                        // onto the exact peer already flowing.
                                        // Known acceptable imperfection: if
                                        // this lock was taken as leader and
                                        // republished re-broadcast tracks,
                                        // those stay alive across a demotion
                                        // to follower here -- benign, since
                                        // the real leader ignores tracks from
                                        // relay peers and followers only
                                        // accept the leader, and it heals on
                                        // the next actual unlock.
                                        info!("cascade: view changed but the locked publisher is still the right source; keeping lock");
                                    } else {
                                        info!("cascade: leader changed -> re-locking");
                                        unlock(&room, &publisher, &active_tasks, &republish).await;
                                        locked = None;
                                        current_video_ssrc = None;
                                        audio_attached = false;
                                        audio_attached_shared.store(false, Ordering::Relaxed);
                                        audio_republish = None;
                                        pending_audio.clear();
                                    }
                                }

                                if locked.is_none() {
                                    // The correct publisher's track may have
                                    // already arrived and been ignored under
                                    // the old policy (e.g. a follower->leader
                                    // takeover where the sharer's track
                                    // predates this view change) -- mistlib
                                    // never fires `on_track` again for it, so
                                    // replay the cached event instead of
                                    // waiting forever for one that isn't
                                    // coming.
                                    let found = last_video
                                        .iter()
                                        .find(|(id, _)| accepts_remote(id, &new_policy))
                                        .map(|(id, event)| (id.clone(), clone_event(event)));
                                    if let Some((id, video_event)) = found {
                                        info!(remote_id = %id, "cascade: replaying cached track from the new publisher");
                                        replay.push_back(video_event);
                                        // Only replay the sibling audio track
                                        // if the Lock arm won't already
                                        // attach one via `pending_audio` --
                                        // otherwise the replayed audio event
                                        // would just churn a redundant
                                        // `Replace` right after `Attach`.
                                        if !pending_audio.contains_key(&id) {
                                            if let Some(audio_event) = last_audio.get(&id) {
                                                replay.push_back(clone_event(audio_event));
                                            }
                                        }
                                    }
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
                    continue;
                }
            }
        };

        let remote_id = event.remote_id.0.clone();
        let kind = event.track.kind();

        // Cache the newest event per peer for every incoming media event --
        // including replays, harmlessly overwriting the entry with an
        // identical clone -- so a future view change can replay it (see
        // above) even though mistlib will never fire `on_track` for this
        // track again.
        match kind {
            RTPCodecType::Video => {
                last_video.insert(remote_id.clone(), clone_event(&event));
            }
            RTPCodecType::Audio => {
                last_audio.insert(remote_id.clone(), clone_event(&event));
            }
            RTPCodecType::Unspecified => {}
        }

        match kind {
            RTPCodecType::Video => match decide_video(locked.as_deref(), &remote_id, &policy) {
                VideoDecision::Ignore => {
                    debug!(%remote_id, "relay: ignoring video track (publisher already locked, or not accepted by cascade policy)");
                }
                VideoDecision::Lock => {
                    info!(%remote_id, "relay: locking onto publisher");
                    *publisher.lock().expect("relay publisher lock poisoned") = Some(remote_id.clone());
                    locked = Some(remote_id.clone());
                    current_video_ssrc = Some(event.track.ssrc());
                    audio_attached = false;
                    audio_attached_shared.store(false, Ordering::Relaxed);

                    let is_leader = matches!(&policy, CascadePolicy::Active { role: ConsensusRole::Leader, .. });
                    let is_follower = matches!(&policy, CascadePolicy::Active { role: ConsensusRole::Follower, .. });
                    let (video_republish, new_audio_republish) = if is_leader {
                        // Lock-in happens right when the sharer's own
                        // renegotiation traffic peaks, so this publish's
                        // offer is often rejected with "signaling is not
                        // stable". That's transient: retry briefly
                        // before degrading to direct relay for good.
                        const REPUBLISH_ATTEMPTS: u32 = 3;
                        let mut published = None;
                        for attempt in 1..=REPUBLISH_ATTEMPTS {
                            match create_and_publish_republish_tracks(&room).await {
                                Ok(tracks) => {
                                    published = Some(tracks);
                                    break;
                                }
                                Err(error) if attempt < REPUBLISH_ATTEMPTS => {
                                    warn!(%error, attempt, "cascade: publishing re-broadcast tracks failed; retrying");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                                Err(error) => {
                                    warn!(%error, "cascade: failed to publish re-broadcast tracks; continuing as direct relay only");
                                }
                            }
                        }
                        match published {
                            Some((video, audio)) => {
                                *republish.lock().expect("relay republish lock poisoned") =
                                    Some((video.clone(), audio.clone()));
                                info!("cascade: re-publishing share into room");
                                (Some(video), Some(audio))
                            }
                            None => (None, None),
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
                        event.pc.clone(),
                        rtsp.clone(),
                        remote_id.clone(),
                        ended_tx.clone(),
                        counters.clone(),
                        video_republish,
                    ));
                    let pli_handle = tokio::spawn(pli_task(event.pc.clone(), event.track.ssrc()));
                    replace_task(&active_tasks, TaskRole::Video, video_handle);
                    replace_task(&active_tasks, TaskRole::Pli, pli_handle);

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
                        replace_task(&active_tasks, TaskRole::Audio, audio_handle);
                        audio_attached = true;
                        audio_attached_shared.store(true, Ordering::Relaxed);
                    }
                }
                VideoDecision::Switch => {
                    info!(%remote_id, "relay: publisher republished video (screen switch); resuming on new track");
                    current_video_ssrc = Some(event.track.ssrc());

                    // The leader's re-broadcast tracks (if any) are the
                    // same live `mistlib::publish_local_track`-registered
                    // tracks for this whole lock's lifetime -- only the
                    // *source* track changed, so reuse what's already
                    // published rather than recreating it.
                    let video_republish = republish
                        .lock()
                        .expect("relay republish lock poisoned")
                        .as_ref()
                        .map(|(video, _)| video.clone());

                    let video_handle = tokio::spawn(video_task(
                        event.track.clone(),
                        event.pc.clone(),
                        rtsp.clone(),
                        remote_id.clone(),
                        ended_tx.clone(),
                        counters.clone(),
                        video_republish,
                    ));
                    let pli_handle = tokio::spawn(pli_task(event.pc.clone(), event.track.ssrc()));
                    replace_task(&active_tasks, TaskRole::Video, video_handle);
                    replace_task(&active_tasks, TaskRole::Pli, pli_handle);
                }
            },
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
                    replace_task(&active_tasks, TaskRole::Audio, audio_handle);
                    audio_attached = true;
                    audio_attached_shared.store(true, Ordering::Relaxed);
                }
                AudioDecision::Replace => {
                    info!(%remote_id, codec = audio_codec.as_str(), "relay: publisher republished audio (screen switch); resuming on new track");
                    let audio_handle = tokio::spawn(audio_task(
                        event.track,
                        rtsp.clone(),
                        audio_codec,
                        remote_id.clone(),
                        counters.clone(),
                        audio_republish.clone(),
                    ));
                    replace_task(&active_tasks, TaskRole::Audio, audio_handle);
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
}

/// Reads RTP off the locked video track, depacketizes H264 into Annex-B
/// access units, extracts SPS/PPS, and forwards each complete AU to `rtsp`.
/// When `republish` is `Some` (leader, cascade active), the raw RTP packet
/// is also written straight through to it -- before depacketization, so
/// followers receive the exact same H264 bitstream the sharer sent, no
/// transcode. Notifies `ended_tx` with `(remote_id, ssrc)` when the track
/// ends (read error/EOF) so the control task can clear the lock -- `ssrc`
/// lets `control_loop` tell this track's end apart from a screen-switch
/// having already superseded it with a new one from the same peer (see
/// `VideoDecision::Switch`), which in practice is the common case: mistlib's
/// WebRTC stack never signals "track ended" on renegotiation, so this arm
/// rarely fires for a live switch at all -- it exists for genuine
/// disconnects/EOF.
async fn video_task(
    track: Arc<TrackRemote>,
    pc: Arc<RTCPeerConnection>,
    rtsp: Arc<RtspServer>,
    remote_id: String,
    ended_tx: mpsc::UnboundedSender<(String, u32)>,
    counters: Arc<RelayCounters>,
    republish: Option<Arc<TrackLocalStaticRTP>>,
) {
    let ssrc = track.ssrc();
    let mut depacketizer = H264Packet::default();
    let mut assembler = AuAssembler::new();
    let mut seen_first_keyframe = false;
    // No packet has been forwarded yet (nothing to reference), so gate on
    // the first IDR just like a mid-stream loss recovery -- see
    // `awaiting_keyframe`'s doc below.
    let mut awaiting_keyframe = true;
    let mut last_seq: Option<u16> = None;
    // Debounce state for the immediate loss-triggered PLI (see
    // [`IMMEDIATE_PLI_DEBOUNCE`]); `pli_task` remains the periodic fallback.
    let mut last_immediate_pli: Option<Instant> = None;

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

        // Sequence-number continuity check -- tracked on every packet
        // (including short/padding ones, which still consume sequence
        // number space) so a gap is never masked by the padding-skip below.
        // mistlib's `WebRtcTransport` registers `nack`/`pli` in the SDP RTCP
        // feedback lines (see `register_h264_opus_codecs` in
        // `.mistlib-src/mistlib-native/src/transports/webrtc.rs`) but never
        // builds an `InterceptorRegistry` (no
        // `register_default_interceptors`/`.with_interceptor_registry` call
        // on the `APIBuilder`), so no NACK generator ever asks the browser to
        // retransmit a lost packet, and `TrackRemote::read_rtp` hands packets
        // straight through in arrival order with no jitter/reorder buffer.
        // `H264Packet::depacketize` (rtp 0.13.0) has no idea any of this
        // happened either -- on a lost FU-A fragment it just keeps
        // concatenating whatever arrives next into `fua_buffer` and emits a
        // "complete" NAL once the end bit shows up, silently truncated/
        // corrupt in the middle. That corrupt NAL used to sail straight
        // through to `rtsp` (and get cached as `last_idr_au` if it happened
        // to carry an IDR type byte), which is a real freeze mechanism: a
        // decoder fed a mangled reference frame has nothing good to show
        // until the *next* real IDR, and a late joiner unlucky enough to
        // land on the cached corrupt IDR would freeze immediately. Since
        // mistlib exposes no hook to fix this upstream (no interceptor
        // registry, no reorder buffer to plug into), the mitigation lives
        // here: detect the discontinuity from the RTP sequence numbers we
        // already have, discard whatever NAL/AU was in flight, and drop
        // every subsequent access unit until a fresh IDR arrives -- solicited
        // by an *immediate* (debounced) PLI right below, with `pli_task`'s
        // periodic cadence as the fallback.
        let seq = packet.header.sequence_number;
        let gap = last_seq.is_some_and(|last| seq != last.wrapping_add(1));
        last_seq = Some(seq);

        if gap {
            if !awaiting_keyframe {
                warn!(
                    %remote_id,
                    seq,
                    "relay: RTP sequence gap on video track; dropping frames until next keyframe"
                );
            }
            depacketizer = H264Packet::default();
            assembler = AuAssembler::new();
            awaiting_keyframe = true;
            // Ask for the recovery keyframe *now* instead of waiting up to a
            // full PLI_INTERVAL for pli_task's next tick -- debounced so a
            // burst of gapped packets from one loss spike sends one PLI, not
            // dozens.
            if immediate_pli_due(&mut last_immediate_pli, Instant::now(), IMMEDIATE_PLI_DEBOUNCE) {
                debug!(%remote_id, "relay: sending immediate PLI after sequence gap");
                send_pli(&pc, ssrc).await;
            }
        }

        if packet.payload.len() <= 2 {
            // RTP padding-only packets -- browsers routinely send these on
            // the media SSRC for bandwidth-estimation probing (most visibly
            // in a burst right after the connection comes up, which is
            // exactly when this used to flood the log) -- have their
            // padding stripped by `rtp::packet::Packet::unmarshal` down to
            // an empty (or otherwise too-short-to-carry-a-NAL) payload.
            // `H264Packet::depacketize` treats anything this short as
            // `ErrShortPacket` unconditionally; that's not a bitstream
            // problem, just "no NAL here", so skip it rather than warn.
            debug!(%remote_id, len = packet.payload.len(), "relay: skipping short/padding-only RTP payload");
            continue;
        }

        let chunk = match depacketizer.depacketize(&packet.payload) {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(%remote_id, %error, "relay: failed to depacketize H264 RTP packet");
                // The depacketizer's own FU-A reassembly state may now be
                // out of sync too (e.g. this packet was a stray FU-A
                // continuation with no matching start) -- treat it the same
                // as a sequence gap rather than risk stitching a later
                // fragment onto whatever's left in `fua_buffer`, including
                // the same immediate (debounced) recovery PLI.
                depacketizer = H264Packet::default();
                assembler = AuAssembler::new();
                awaiting_keyframe = true;
                if immediate_pli_due(&mut last_immediate_pli, Instant::now(), IMMEDIATE_PLI_DEBOUNCE) {
                    debug!(%remote_id, "relay: sending immediate PLI after depacketizer reset");
                    send_pli(&pc, ssrc).await;
                }
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
                awaiting_keyframe = false;
                if !seen_first_keyframe {
                    seen_first_keyframe = true;
                    info!(%remote_id, "relay: first keyframe (IDR) received from publisher");
                } else {
                    debug!(%remote_id, "relay: forwarded IDR access unit");
                }
            }

            if awaiting_keyframe {
                debug!(%remote_id, "relay: dropping access unit while awaiting a keyframe");
                continue;
            }

            rtsp.send_video_access_unit_at(&au, au_ts).await;
            counters.video_au.fetch_add(1, Ordering::Relaxed);
        }
    }

    let _ = ended_tx.send((remote_id, ssrc));
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

/// Whether an immediate loss-triggered PLI may be sent right now, given the
/// time the last one went out. Updates `last` when it says yes, so callers
/// just gate `send_pli` on the return value. Pure (time injected) so the
/// debounce window is unit-testable without a live `RTCPeerConnection` or
/// tokio time control.
fn immediate_pli_due(last: &mut Option<Instant>, now: Instant, debounce: Duration) -> bool {
    match last {
        Some(at) if now.duration_since(*at) < debounce => false,
        _ => {
            *last = Some(now);
            true
        }
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
                counters.audio_rtp_packets_total.fetch_add(1, Ordering::Relaxed);
                counters.audio_rtp_bytes_total.fetch_add(packet.payload.len() as u64, Ordering::Relaxed);
                if let Some(republish) = &republish {
                    if let Err(error) = republish.write_rtp(&packet).await {
                        debug!(%remote_id, %error, "cascade: republishing audio RTP packet failed");
                    }
                }
                rtsp.send_audio_frame(&packet.payload, packet.header.timestamp).await;
                counters.audio_frames.fetch_add(1, Ordering::Relaxed);
                counters.audio_frames_sent_total.fetch_add(1, Ordering::Relaxed);
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
    let mut transcoder = match OpusToAac::new(counters.clone()) {
        Ok(t) => t,
        Err(error) => {
            warn!(%remote_id, %error, "relay: failed to set up Opus->AAC transcoder; audio disabled for this share");
            return;
        }
    };

    loop {
        match track.read_rtp().await {
            Ok((packet, _attrs)) => {
                counters.audio_rtp_packets_total.fetch_add(1, Ordering::Relaxed);
                counters.audio_rtp_bytes_total.fetch_add(packet.payload.len() as u64, Ordering::Relaxed);
                if let Some(republish) = &republish {
                    if let Err(error) = republish.write_rtp(&packet).await {
                        debug!(%remote_id, %error, "cascade: republishing audio RTP packet failed");
                    }
                }
                for (frame, ts) in transcoder.push(&packet.payload, packet.header.timestamp) {
                    rtsp.send_audio_frame(&frame, ts).await;
                    counters.audio_frames.fetch_add(1, Ordering::Relaxed);
                    counters.audio_frames_sent_total.fetch_add(1, Ordering::Relaxed);
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
async fn summary_task(
    rtsp: Arc<RtspServer>,
    publisher: Arc<StdMutex<Option<String>>>,
    counters: Arc<RelayCounters>,
    audio_attached: Arc<AtomicBool>,
) {
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

                // "User thinks audio is broken" signature: video is visibly
                // flowing but no audio frame reached `rtsp` this interval.
                // Naturally throttled to once per `SUMMARY_INTERVAL` since
                // this whole function only wakes up that often. Two distinct
                // causes get two distinct messages: no audio track was ever
                // attached for this publisher (most likely they didn't tick
                // "share audio" in the browser's picker), versus a track *is*
                // attached but is producing no output (decode/transcode
                // stalled -- check `audio.transcode_errors` in
                // `stream.status`).
                if video_au > 0 && audio_frames == 0 {
                    if audio_attached.load(Ordering::Relaxed) {
                        warn!(
                            publisher = %publisher,
                            transcode_errors = counters.audio_transcode_errors_total.load(Ordering::Relaxed),
                            "relay: video is flowing but no audio frames were forwarded this interval \
                             (audio track attached but producing nothing -- check for transcode errors)"
                        );
                    } else {
                        warn!(
                            publisher = %publisher,
                            "relay: video is flowing but no audio track has been received from the \
                             publisher (did they tick \"share audio\" in the browser's screen-share picker?)"
                        );
                    }
                }
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
    /// Shared with `audio_task_aac`'s counters so a decode/encode failure
    /// here shows up in `stream.status`'s `audio.transcode_errors` -- see
    /// [`RelayCounters`]'s doc for why that field exists.
    counters: Arc<RelayCounters>,
}

impl OpusToAac {
    fn new(counters: Arc<RelayCounters>) -> Result<Self> {
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
            counters,
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
                self.counters.audio_transcode_errors_total.fetch_add(1, Ordering::Relaxed);
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
                Err(error) => {
                    warn!(%error, "relay: AAC encode failed, dropping frame");
                    self.counters.audio_transcode_errors_total.fetch_add(1, Ordering::Relaxed);
                }
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
        assert_eq!(decide_video(None, "peer-a", &CascadePolicy::Disabled), VideoDecision::Lock);
    }

    #[test]
    fn decide_video_ignores_when_already_locked_to_someone_else() {
        assert_eq!(
            decide_video(Some("peer-a"), "peer-b", &CascadePolicy::Disabled),
            VideoDecision::Ignore
        );
    }

    #[test]
    fn decide_video_switches_when_the_locked_peer_republishes_a_new_track() {
        // Same peer, second video track (e.g. a screen switch: tc-chat
        // unpublishes the old `tc-chat-screen:<uuid>` and publishes a new one)
        // -- must resume on the new track rather than being ignored forever.
        assert_eq!(
            decide_video(Some("peer-a"), "peer-a", &CascadePolicy::Disabled),
            VideoDecision::Switch
        );
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
    fn decide_audio_replaces_a_second_audio_track_from_the_locked_peer() {
        // Same peer, already has an attached audio task, and a *new* audio
        // track shows up (the screen switch's fresh `tc-chat-screen-audio:
        // <uuid>`) -- replace the stale audio task instead of ignoring it.
        assert_eq!(
            decide_audio(Some("peer-a"), "peer-a", true, &CascadePolicy::Disabled),
            AudioDecision::Replace
        );
    }

    #[test]
    fn decide_audio_ignores_other_peers_while_locked() {
        assert_eq!(
            decide_audio(Some("peer-a"), "peer-b", false, &CascadePolicy::Disabled),
            AudioDecision::Ignore
        );
    }

    // --- immediate loss-triggered PLI debounce ------------------------------

    #[test]
    fn immediate_pli_due_allows_the_first_send_and_debounces_the_burst() {
        let debounce = Duration::from_secs(1);
        let t0 = Instant::now();
        let mut last: Option<Instant> = None;

        // First loss event: send immediately.
        assert!(immediate_pli_due(&mut last, t0, debounce));
        // Back-to-back gaps within the window (one lossy spike is many
        // gapped packets): no PLI storm.
        assert!(!immediate_pli_due(&mut last, t0 + Duration::from_millis(10), debounce));
        assert!(!immediate_pli_due(&mut last, t0 + Duration::from_millis(999), debounce));
        // Window elapsed: the next loss event may send again.
        assert!(immediate_pli_due(&mut last, t0 + Duration::from_millis(1000), debounce));
        // ...and that send re-arms the debounce from its own time.
        assert!(!immediate_pli_due(&mut last, t0 + Duration::from_millis(1500), debounce));
        assert!(immediate_pli_due(&mut last, t0 + Duration::from_millis(2000), debounce));
    }

    // --- task role bookkeeping (screen-switch task replacement) ------------

    #[tokio::test]
    async fn replace_task_aborts_the_previous_handle_for_the_same_role() {
        let active_tasks: Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>> = Arc::new(StdMutex::new(HashMap::new()));

        let first = tokio::spawn(std::future::pending::<()>());
        replace_task(&active_tasks, TaskRole::Video, first);
        // Let the runtime schedule the spawned task before we replace it.
        tokio::task::yield_now().await;

        let second = tokio::spawn(std::future::pending::<()>());
        let second_id = second.id();
        replace_task(&active_tasks, TaskRole::Video, second);

        let guard = active_tasks.lock().expect("lock poisoned");
        assert_eq!(guard.len(), 1, "replacing the same role must not accumulate handles");
        assert_eq!(guard.get(&TaskRole::Video).map(|h| h.id()), Some(second_id));
    }

    #[tokio::test]
    async fn abort_role_only_touches_its_own_role() {
        let active_tasks: Arc<StdMutex<HashMap<TaskRole, JoinHandle<()>>>> = Arc::new(StdMutex::new(HashMap::new()));

        replace_task(&active_tasks, TaskRole::Video, tokio::spawn(std::future::pending::<()>()));
        replace_task(&active_tasks, TaskRole::Pli, tokio::spawn(std::future::pending::<()>()));
        replace_task(&active_tasks, TaskRole::Audio, tokio::spawn(std::future::pending::<()>()));
        replace_task(&active_tasks, TaskRole::Summary, tokio::spawn(std::future::pending::<()>()));

        abort_role(&active_tasks, TaskRole::Video);
        abort_role(&active_tasks, TaskRole::Pli);
        abort_role(&active_tasks, TaskRole::Audio);

        let guard = active_tasks.lock().expect("lock poisoned");
        assert_eq!(
            guard.len(),
            1,
            "only the Summary task (outliving any single lock/unlock cycle) should remain"
        );
        assert!(guard.contains_key(&TaskRole::Summary));
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
        assert_eq!(decide_video(None, "browser-sharer", &policy), VideoDecision::Lock);
    }

    #[test]
    fn decide_video_leader_ignores_tracks_from_other_relay_peers() {
        let policy = active_policy(ConsensusRole::Leader, Some("self-id"), &["self-id", "relay-2"]);
        assert_eq!(decide_video(None, "relay-2", &policy), VideoDecision::Ignore);
    }

    #[test]
    fn decide_video_follower_accepts_only_the_leader_and_ignores_the_sharer() {
        let policy = active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert_eq!(decide_video(None, "relay-1", &policy), VideoDecision::Lock);
        assert_eq!(decide_video(None, "browser-sharer", &policy), VideoDecision::Ignore);
    }

    #[test]
    fn decide_video_does_not_lock_before_a_role_or_leader_is_known() {
        for policy in [
            active_policy(ConsensusRole::Unknown, None, &["self-id"]),
            active_policy(ConsensusRole::Candidate, None, &["self-id"]),
            active_policy(ConsensusRole::Follower, None, &["self-id"]),
        ] {
            assert_eq!(
                decide_video(None, "anyone", &policy),
                VideoDecision::Ignore,
                "{policy:?} should not lock yet"
            );
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
        assert_eq!(decide_video(None, "relay-1", &following_old_leader), VideoDecision::Lock);

        // ...after a leader change (control_loop unlocks first), the same
        // node now follows the new leader instead, and no longer the old one.
        let following_new_leader = active_policy(
            ConsensusRole::Follower,
            Some("relay-2"),
            &["self-id", "relay-1", "relay-2"],
        );
        assert_eq!(decide_video(None, "relay-2", &following_new_leader), VideoDecision::Lock);
        assert_eq!(decide_video(None, "relay-1", &following_new_leader), VideoDecision::Ignore);
    }

    #[test]
    fn decide_video_role_transition_from_follower_to_leader_accepts_the_sharer() {
        let as_follower = active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert_eq!(decide_video(None, "browser-sharer", &as_follower), VideoDecision::Ignore);

        let as_leader = active_policy(ConsensusRole::Leader, Some("self-id"), &["self-id", "relay-1"]);
        assert_eq!(decide_video(None, "browser-sharer", &as_leader), VideoDecision::Lock);
    }

    #[test]
    fn decide_video_switches_when_the_locked_peer_republishes_under_an_active_cascade_policy() {
        // The screen-switch "same peer, new track" arm must win regardless of
        // cascade role -- a follower re-locked onto the leader, or a leader
        // re-locked onto the sharer, both just resume on the replacement track.
        let follower = active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert_eq!(decide_video(Some("relay-1"), "relay-1", &follower), VideoDecision::Switch);

        let leader = active_policy(ConsensusRole::Leader, Some("self-id"), &["self-id", "relay-2"]);
        assert_eq!(
            decide_video(Some("browser-sharer"), "browser-sharer", &leader),
            VideoDecision::Switch
        );
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

    // --- keep-lock-on-view-change predicate (cascade re-lock hang fix) ------

    #[test]
    fn lock_survives_view_change_when_the_new_policy_still_accepts_the_locked_peer() {
        // The exact production scenario: locked onto a peer while we were
        // (stale) leader, then the view updates to "we're actually a
        // follower of that same peer" -- the locked publisher is still
        // correct, so the lock must survive instead of unlocking into a
        // no-new-`on_track`-ever-fires hang.
        let now_following_the_locked_peer =
            active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert!(lock_survives_view_change(Some("relay-1"), &now_following_the_locked_peer));
    }

    #[test]
    fn lock_survives_view_change_is_false_when_the_new_policy_rejects_the_locked_peer() {
        let now_following_someone_else =
            active_policy(ConsensusRole::Follower, Some("relay-2"), &["self-id", "relay-1", "relay-2"]);
        assert!(!lock_survives_view_change(Some("relay-1"), &now_following_someone_else));
    }

    #[test]
    fn lock_survives_view_change_is_false_when_nothing_is_locked() {
        let policy = active_policy(ConsensusRole::Follower, Some("relay-1"), &["self-id", "relay-1"]);
        assert!(!lock_survives_view_change(None, &policy));
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

    // --- audio status JSON shape --------------------------------------------

    #[test]
    fn audio_status_json_reports_all_counters_and_attached_flag() {
        let value = audio_status_json(true, 100, 12_000, 40, 2);
        assert_eq!(value["attached"], json!(true));
        assert_eq!(value["rtp_packets"], json!(100));
        assert_eq!(value["rtp_bytes"], json!(12_000));
        assert_eq!(value["frames_sent"], json!(40));
        assert_eq!(value["transcode_errors"], json!(2));
    }

    #[test]
    fn audio_status_json_not_attached_before_any_audio_track_arrives() {
        let value = audio_status_json(false, 0, 0, 0, 0);
        assert_eq!(value["attached"], json!(false));
        assert_eq!(value["rtp_packets"], json!(0));
        assert_eq!(value["frames_sent"], json!(0));
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

        let mut transcoder =
            OpusToAac::new(Arc::new(RelayCounters::default())).expect("creating Opus->AAC transcoder");

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

    #[test]
    fn opus_to_aac_counts_a_decode_failure_as_a_transcode_error() {
        let counters = Arc::new(RelayCounters::default());
        let mut transcoder = OpusToAac::new(counters.clone()).expect("creating Opus->AAC transcoder");

        // Not a valid Opus payload -- the decoder must reject it, and `push`
        // should record that in the shared counters (surfaced via
        // `stream.status`'s `audio.transcode_errors`) rather than silently
        // dropping it.
        let garbage = vec![0xFFu8; 8];
        let out = transcoder.push(&garbage, 0);

        assert!(out.is_empty(), "a failed decode should emit no frames");
        assert_eq!(counters.audio_transcode_errors_total.load(Ordering::Relaxed), 1);
    }

    // --- empirical pitch-conservation audit ---------------------------------
    //
    // Three code audits found `OpusToAac` clean, but VRChat playback reportedly
    // sounds slightly sharp. Rather than re-reading the code a fourth time,
    // this proves (or disproves) pitch-correctness by *sample-count
    // conservation*: the Opus decoder and the AAC encoder share the same
    // 48 kHz clock, so if X input samples/channel produce Y emitted AAC
    // frames, the implied pitch ratio VRChat would hear is X / (Y * 1024). A
    // ratio of 1.000 is pitch-perfect; 1024/960 (~1.0667, +469 Hz off a 440 Hz
    // tone) would indicate an Opus-frame-size vs. AAC-frame-size confusion;
    // 48000/44100 (~1.0884, +479 Hz) would indicate a sample-rate mismatch.
    #[test]
    fn opus_to_aac_conserves_sample_count_10s_stereo_440hz() {
        let sample_rate = rtp_out::AUDIO_CLOCK_RATE;
        assert_eq!(sample_rate, 48_000, "test's Hz math assumes the relay's 48kHz clock");
        let channels = 2usize;
        let frame_samples = 960usize; // 20ms @ 48kHz -- what browsers send
        let seconds = 10u32;
        let total_input_samples = sample_rate * seconds; // 480_000 samples/channel
        let num_packets = total_input_samples as usize / frame_samples;

        let mut opus_encoder = opus::Encoder::new(sample_rate, opus::Channels::Stereo, opus::Application::Audio)
            .expect("creating Opus encoder");

        let mut transcoder =
            OpusToAac::new(Arc::new(RelayCounters::default())).expect("creating Opus->AAC transcoder");

        let start_ts: u32 = 1_000_000;
        let mut ts = start_ts;
        let mut emitted: Vec<(Vec<u8>, u32)> = Vec::new();

        for packet_index in 0..num_packets {
            let mut pcm = vec![0i16; frame_samples * channels];
            for i in 0..frame_samples {
                let t = (packet_index * frame_samples + i) as f64 / sample_rate as f64;
                let sample = (2.0 * std::f64::consts::PI * 440.0 * t).sin() * i16::MAX as f64 * 0.25;
                pcm[i * channels] = sample as i16;
                pcm[i * channels + 1] = sample as i16;
            }

            let mut opus_payload = vec![0u8; 4000];
            let len = opus_encoder.encode(&pcm, &mut opus_payload).expect("Opus encode");
            opus_payload.truncate(len);

            for pair in transcoder.push(&opus_payload, ts) {
                emitted.push(pair);
            }

            ts = ts.wrapping_add(frame_samples as u32);
        }

        // --- (b) consecutive rtp_ts must step by exactly AAC_FRAME_SAMPLES ---
        for pair in emitted.windows(2) {
            let ts_a = pair[0].1;
            let ts_b = pair[1].1;
            assert_eq!(
                ts_b.wrapping_sub(ts_a),
                AAC_FRAME_SAMPLES as u32,
                "consecutive AAC frame timestamps must step by exactly {AAC_FRAME_SAMPLES}"
            );
        }

        // --- (a) sample-count conservation == pitch conservation ------------
        let frames_emitted = emitted.len();
        let implied_samples = frames_emitted * AAC_FRAME_SAMPLES;
        let pitch_ratio = implied_samples as f64 / total_input_samples as f64;

        println!("=== OpusToAac sample-count conservation (stereo, {seconds}s @ {sample_rate}Hz) ===");
        println!("input samples/channel:   {total_input_samples}");
        println!("opus packets pushed:     {num_packets} ({frame_samples} samples/channel each)");
        println!("AAC frames emitted:      {frames_emitted} ({AAC_FRAME_SAMPLES} samples/channel each)");
        println!("implied samples/channel: {implied_samples}");
        println!("pitch ratio (implied/input): {pitch_ratio:.6}  (1.000000 == pitch-perfect)");
        println!(
            "  for reference: 1024/960 = {:.6} (+{:.1}Hz on 440Hz), 48000/44100 = {:.6} (+{:.1}Hz on 440Hz)",
            1024.0 / 960.0,
            440.0 * (1024.0 / 960.0 - 1.0),
            48000.0 / 44100.0,
            440.0 * (48000.0 / 44100.0 - 1.0)
        );

        // Tolerance: +/-2 frames for encoder priming delay (fdk-aac's AAC-LC
        // encoder has ~1 frame of algorithmic delay) and any final partial
        // ring-buffer remainder that never reaches a full 1024-sample frame.
        let tolerance_samples = 2 * AAC_FRAME_SAMPLES as i64;
        let diff = implied_samples as i64 - total_input_samples as i64;
        assert!(
            diff.abs() <= tolerance_samples,
            "sample count not conserved: input={total_input_samples} implied={implied_samples} \
             diff={diff} (tolerance +/-{tolerance_samples}) -- pitch ratio {pitch_ratio:.6} is NOT 1.0"
        );

        // --- (5) BONUS: decode the emitted AAC back to PCM and measure the
        // dominant frequency by zero-crossing count over the middle 5s. -----
        let asc_info = transcoder.encoder.info().expect("reading AAC encoder ASC info");
        let asc = asc_info.confBuf[..asc_info.confSize as usize].to_vec();
        assert!(!asc.is_empty(), "encoder should have produced a non-empty AudioSpecificConfig");

        let mut aac_decoder = fdk_aac::dec::Decoder::new(fdk_aac::dec::Transport::Raw);
        aac_decoder.config_raw(&asc).expect("configuring AAC decoder with encoder's ASC");

        let mut decoded_pcm: Vec<i16> = Vec::new();
        for (frame, _ts) in &emitted {
            aac_decoder.fill(frame).expect("feeding AAC decoder");
            let mut out = vec![0i16; 2048 * channels];
            match aac_decoder.decode_frame(&mut out) {
                Ok(()) => {
                    let frame_size = aac_decoder.decoded_frame_size();
                    out.truncate(frame_size);
                    decoded_pcm.extend_from_slice(&out);
                }
                Err(error) => {
                    panic!("AAC decode of our own encoder's output failed: {error}");
                }
            }
        }

        let decoded_samples_per_channel = decoded_pcm.len() / channels;
        println!("decoded PCM samples/channel (incl. encoder priming delay): {decoded_samples_per_channel}");

        // fdk-aac's AAC-LC encoder inserts one frame (1024 samples) of
        // priming delay at the start of the encoded stream; skip it before
        // measuring frequency so the analysis window is clean signal.
        let priming_samples = AAC_FRAME_SAMPLES.min(decoded_samples_per_channel);
        let analysis_start = priming_samples;
        let analysis_seconds = 5.0f64;
        let analysis_len = ((analysis_seconds * sample_rate as f64) as usize)
            .min(decoded_samples_per_channel.saturating_sub(analysis_start));
        // Center the 5s analysis window in the middle of the signal.
        let usable_after_start = decoded_samples_per_channel.saturating_sub(analysis_start);
        let center_offset = usable_after_start.saturating_sub(analysis_len) / 2;
        let window_start = analysis_start + center_offset;
        let window_end = (window_start + analysis_len).min(decoded_samples_per_channel);

        let mut zero_crossings = 0u64;
        let mut prev = decoded_pcm[window_start * channels]; // left channel
        for i in (window_start + 1)..window_end {
            let cur = decoded_pcm[i * channels];
            if (prev < 0) != (cur < 0) && !(prev == 0 && cur == 0) {
                zero_crossings += 1;
            }
            prev = cur;
        }
        let window_duration_s = (window_end - window_start) as f64 / sample_rate as f64;
        // A full sine cycle produces 2 zero crossings.
        let measured_hz = (zero_crossings as f64 / 2.0) / window_duration_s;

        println!(
            "zero-crossing analysis window: samples [{window_start}, {window_end}) = {window_duration_s:.3}s"
        );
        println!("zero crossings: {zero_crossings}");
        println!("measured dominant frequency: {measured_hz:.3} Hz (input tone: 440.000 Hz)");
        println!(
            "  for reference: 1024/960 bug would measure ~{:.1}Hz, 48k/44.1k bug would measure ~{:.1}Hz",
            440.0 * (1024.0 / 960.0),
            440.0 * (48000.0 / 44100.0)
        );

        assert!(
            (measured_hz - 440.0).abs() < 2.0,
            "measured frequency {measured_hz:.3}Hz deviates from input 440Hz by more than 2Hz -- pitch is NOT conserved"
        );
    }

    // --- mono Opus stream through the (stereo-decoding) OpusToAac -----------
    //
    // Browsers commonly send mono Opus for a single mic/share track. `OpusToAac`
    // always constructs its `opus::Decoder` as `Channels::Stereo` (see `new`
    // above); this checks that decoding a genuinely mono-encoded Opus stream
    // through that stereo decoder still conserves sample count end-to-end.
    #[test]
    fn opus_to_aac_conserves_sample_count_for_mono_opus_source() {
        let sample_rate = rtp_out::AUDIO_CLOCK_RATE;
        let frame_samples = 960usize; // 20ms @ 48kHz
        let seconds = 10u32;
        let total_input_samples = sample_rate * seconds;
        let num_packets = total_input_samples as usize / frame_samples;

        // Mono encoder: one channel of samples per Opus frame.
        let mut opus_encoder = opus::Encoder::new(sample_rate, opus::Channels::Mono, opus::Application::Audio)
            .expect("creating mono Opus encoder");

        let mut transcoder =
            OpusToAac::new(Arc::new(RelayCounters::default())).expect("creating Opus->AAC transcoder");

        let start_ts: u32 = 500_000;
        let mut ts = start_ts;
        let mut emitted: Vec<(Vec<u8>, u32)> = Vec::new();

        for packet_index in 0..num_packets {
            let mut pcm = vec![0i16; frame_samples]; // mono: 1 sample per frame index
            for (i, sample_slot) in pcm.iter_mut().enumerate() {
                let t = (packet_index * frame_samples + i) as f64 / sample_rate as f64;
                let sample = (2.0 * std::f64::consts::PI * 440.0 * t).sin() * i16::MAX as f64 * 0.25;
                *sample_slot = sample as i16;
            }

            let mut opus_payload = vec![0u8; 4000];
            let len = opus_encoder.encode(&pcm, &mut opus_payload).expect("mono Opus encode");
            opus_payload.truncate(len);

            for pair in transcoder.push(&opus_payload, ts) {
                emitted.push(pair);
            }

            ts = ts.wrapping_add(frame_samples as u32);
        }

        for pair in emitted.windows(2) {
            let ts_a = pair[0].1;
            let ts_b = pair[1].1;
            assert_eq!(
                ts_b.wrapping_sub(ts_a),
                AAC_FRAME_SAMPLES as u32,
                "mono-source: consecutive AAC frame timestamps must step by exactly {AAC_FRAME_SAMPLES}"
            );
        }

        let frames_emitted = emitted.len();
        let implied_samples = frames_emitted * AAC_FRAME_SAMPLES;
        let pitch_ratio = implied_samples as f64 / total_input_samples as f64;

        println!("=== OpusToAac sample-count conservation (MONO opus source -> stereo decoder, {seconds}s) ===");
        println!("input samples/channel:   {total_input_samples}");
        println!("opus packets pushed:     {num_packets} ({frame_samples} samples/channel each, mono-encoded)");
        println!("AAC frames emitted:      {frames_emitted} ({AAC_FRAME_SAMPLES} samples/channel each)");
        println!("implied samples/channel: {implied_samples}");
        println!("pitch ratio (implied/input): {pitch_ratio:.6}  (1.000000 == pitch-perfect)");

        let tolerance_samples = 2 * AAC_FRAME_SAMPLES as i64;
        let diff = implied_samples as i64 - total_input_samples as i64;
        assert!(
            diff.abs() <= tolerance_samples,
            "mono-source sample count not conserved: input={total_input_samples} implied={implied_samples} \
             diff={diff} (tolerance +/-{tolerance_samples}) -- pitch ratio {pitch_ratio:.6} is NOT 1.0"
        );
    }
}
