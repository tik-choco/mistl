# Screen-share relay: findings from field debugging

What we learned while debugging "the VRChat stream doesn't start" and
"the VRChat stream is choppy / blurry / freezes" with a real tc-chat screen
share (2026-11). Each section records the symptom, what was measured, the
cause, what fixed it, and what was tried and **did not** help (or made it
worse), so the next investigation doesn't repeat it.

For the basic how-to-verify workflow see [stream-verify.md](stream-verify.md).

## The pipeline and where quality can be lost

```
Chrome tab (tc-chat)            mistl daemon                       VRChat
getDisplayMedia -> H264 enc --WebRTC--> relay (depacketize, AU) --RTSP--> AVPro
   (1) capture/encode        (2) transport    (3) relay           (4) RTSP leg  (5) player
```

"Everything runs on one PC" only makes hop (4) local. Every stage is
real-time and drops rather than waits, and all of them share one CPU/GPU with
VRChat:

| Stage | What can go wrong |
| --- | --- |
| (1) capture/encode | Chrome encodes **once per peer** (tc-chat is a mesh), in software for H264. A big screen at 30 fps times several peers (other tabs, several mistl instances in the room) costs real CPU; the encoder then sheds frames or resolution. |
| (2) WebRTC | Chrome's congestion control decides the bitrate; the browser also caps video at ~2.5 Mbps by default (see below). |
| (3) relay | A lost/undecodable frame can only be repaired by a keyframe; keyframes are requested with PLI. |
| (4) RTSP | AVPro has **no feedback path to the browser's encoder**. Anything lost here stays broken until the next keyframe the relay happens to request. UDP has no retransmission, even on loopback. |
| (5) AVPro | Decodes in-game alongside VR rendering. |

The key structural point: browser-to-browser WebRTC self-heals (the receiver
NACKs/PLIs the sender immediately). The RTSP hop to AVPro cannot, so the relay
must avoid losing anything on (4) and must request keyframes itself.

## 1. Relay never locks onto the share ("waiting for a screen share")

**Symptom.** `mistl stream relay --room X` logs `relay: waiting for a screen
share in the room` forever although the browser is connected and sharing.
It works for some browser sessions and not others, which looks random.

**Diagnosis that worked.** Compare the *first* signaling state the native
side logs for that peer connection:

```
[IceState] peer=<browser> pc_id=N kind=signaling state=HaveRemoteOffer   <- browser offered first: works
[IceState] peer=<browser> pc_id=N kind=signaling state=HaveLocalOffer    <- native offered first: fails
```

In the failing case the browser's later renegotiation offer (adding the
screen tracks) is applied and answered (`HaveRemoteOffer -> Stable`), but no
`MediaTrackEvent` ever arrives. Five sessions all followed this rule. Which
side offers first is decided by node-id order (lower id offers), so success
depended on the random browser id vs. the daemon's id.

**Cause.** webrtc-rs 0.13 `create_answer` always used the default answering
DTLS role. When native was the initial offerer, Chrome had answered as DTLS
`active`; Chrome's later renegotiation offer got a native answer that flipped
the role, which Chrome rejects ("Failed to set SSL role for the transport"),
so no RTP was ever sent on the new m-lines. Native still reached `Stable`,
hiding the failure. (RFC 8842 §5.3: renegotiation must keep the role.)
Two webrtc-rs peers do **not** reproduce it — webrtc-rs accepts the flipped
role — so the regression test checks the DTLS role in the SDP, not only track
delivery.

**Fix.** mistlib-native builds a second webrtc-rs `API` whose
`SettingEngine::set_answering_dtls_role(DTLSRole::Server)` is used for every
peer connection on which native sends the first offer, so webrtc-rs itself
answers renegotiations with `a=setup:passive` (in both the sent and its own
local description). Tests `renegotiated_tracks_reach_initial_offerer/answerer`
check the role on every bundled section. No downstream `[patch]` is needed.

Three fixes were tried, compared with stream-lab (native offers first):

| Approach | Verdict |
| --- | --- |
| Vendor the whole `webrtc` crate, patch `create_answer` | Works, but 1.2 MB of third-party code to maintain, every downstream repo needs a `[patch.crates-io]` line (a fresh mistl checkout stopped building), and the publish gate flagged vendored files. Dropped. |
| Rewrite `a=setup:` in the answer after `set_local_description` | Works for Chrome, but native's own local description keeps the flipped role — two sources of truth. Dropped. |
| Per-pc `answering_dtls_role` (chosen) | Same lab results as the vendored patch in a 3-run A/B; no rewrite, no vendoring. |

**Ruled out** (checked and not the cause): the relay's room filter
(`peer_in_room`), the cascade lock policy, the media-track handler
registration order, codec mismatch (native registers H264 only and Chrome
offers it), the browser being connected to the daemon in two rooms at once.

## 2. Choppy video (drops to ~1 fps)

**Measured.** Relay throughput logs: `video_au_per_s` mostly 1.0, peaks of
10-24; RTSP outflow ~0.25 Mbps idle, ≤2.2 Mbps peak.

**Causes.**
- A static screen legitimately produces ~1 fps from Chrome's capturer.
- Chrome caps a WebRTC video sender at **~2.5 Mbps** unless the sender's
  `maxBitrate` is raised. Screen tracks default to the "detail" content
  behaviour (keep resolution, shed frame rate), so at a large resolution the
  cap shows up as a low frame rate.

**Fix.** tc-chat asks for `frameRate: {ideal: 30, max: 30}` (and at most
1920x1080) in `getDisplayMedia`, and raises the sender cap to 8 Mbps / 30 fps
through mistlib-wasm's `set_local_track_encoding(track_id, max_bitrate_bps,
max_framerate, degradation_preference)` (applied to every peer and re-applied
after each renegotiation, since `RTCRtpSender.getParameters().encodings` is
empty until negotiated). Result: 2354x1076 at 24-30 fps, 3-6 Mbps.

## 3. Things that did not help or made it worse

| Tried | Result | Verdict |
| --- | --- | --- |
| `track.contentHint = "motion"` (+ `degradationPreference: "balanced"`) | No more 1 fps, but resolution hopped between 110x50 and 386x176 every few frames; blurry, and browser viewers saw the size change too | **Don't.** Screen content should keep the default hint; raise the bitrate cap instead. |
| Raising `maxBitrate` while `motion` was set | Bitrate *fell* to 0.15-0.8 Mbps — the encoder chose tiny resolutions, so it never needed the bandwidth | The hint, not the cap, was the limiter. |
| Moving `TrackRemote::read_rtp()` into a dedicated pump task (webrtc-rs stamps TWCC arrival time at read time, so slow processing between reads could look like congestion) | No measurable bitrate change | Kept (cheap, more correct timestamps) but not a fix. |
| Disabling the TWCC receiver interceptor (local experiment) | No improvement (and not a clean test: `goog-remb` stayed advertised while nothing sends REMB) | TWCC is not the bottleneck. |

When Chrome's webrtc-internals said `qualityLimitationReason = bandwidth`
over a loopback connection, the real reason was the encoder settings above,
not the network or the native receiver.

## 4. Freezes and corrupted ("jumping") frames on the RTSP leg

**Measured.** With the WebRTC leg healthy (no inbound loss), reading the RTSP
output with ffmpeg:

- over UDP (`-rtsp_transport udp`, what AVPro uses for `rtsp://`):
  intermittent `A non-intra slice in an IDR NAL unit`,
  `reference picture missing`, `no frame!` — i.e. lost packets, typically in
  100+-packet keyframes sent back-to-back;
- over TCP-interleaved: decodes cleanly.

Users confirmed `rtspt://` (forces TCP-interleaved in AVPro) looks better but
still freezes occasionally.

**Causes found in code.** The TCP-interleaved path used `try_send` on a
bounded queue and silently dropped packets when it was full (mid-frame →
corruption until the next keyframe). Keyframes were requested only every 5 s,
so one loss could freeze the picture for seconds. UDP sent whole keyframes in
one burst.

**Fixes.**
- TCP-interleaved: each session's writer queue holds 2048 packets; a whole
  access unit is reserved up front (`try_reserve_many`). If it doesn't fit,
  the entire AU is dropped and that session's video resumes at the next IDR
  that fits (never a partial frame), and a keyframe is requested.
- UDP: access units are sent in bursts of 16 packets with ~250 µs yields in
  between, capped at 4 ms of pacing per AU. A 122-packet IDR reached a
  loopback receiver complete and in order in 5.5-7.2 ms.
- Keyframes: the relay's PLI task also listens for requests from the RTSP side
  (TCP resync, a viewer starting `PLAY` with no cached IDR) and from inbound
  RTP loss, debounced to one per second (requests inside the window are
  deferred, not dropped); every new `PLAY` asks for one. The periodic PLI
  stays at 5 s — a 2 s interval was tried and made latency spike every 2 s
  (see "Iterating on the relay with the lab" below).
- `stream.relay` / `stream.status` / selftest return `vrchat_url`
  (`rtspt://`, TCP-interleaved in AVPro), and the CLI prints it; `rtsp_url`
  (`rtsp://`) stays for ffmpeg/VLC, which don't know `rtspt`.

## Measurement recipes

Resolution mix, frame count and bitrate of what a player receives (20 s):

```sh
ffprobe -v error -rtsp_transport tcp -read_intervals "%+20" -select_streams v:0 \
  -show_entries frame=width,height,pkt_size -of flat rtsp://127.0.0.1:<port>/stream |
awk -F'[.=]' '/\.width=/{w=$NF} /\.height=/{h=$NF} /\.pkt_size=/{gsub(/"/,"",$NF); b+=$NF; c++; r[w"x"h]++}
  END {for (k in r) print k, r[k]; printf "frames=%d kbps=%.0f\n", c, b*8/20/1000}'
```

Use `-of flat` and parse by key: with `-of csv` the column order does not
follow `-show_entries`, which silently mislabels fields.

Decode errors on each transport (run while the share is moving):

```sh
ffmpeg -hide_banner -rtsp_transport udp -i rtsp://127.0.0.1:<port>/stream -map 0:v -t 20 -f null - 2>&1 | grep h264
ffmpeg -hide_banner -rtsp_transport tcp -i rtsp://127.0.0.1:<port>/stream -map 0:v -t 20 -f null - 2>&1 | grep h264
```

Per-5 s output bitrate from the daemon log's `RTP outflow summary` lines
(cumulative `bytes=` per `session_id`): difference consecutive lines, `* 8 / 5`.

In the browser, `chrome://webrtc-internals` → the video `outbound-rtp`:
`qualityLimitationReason`, `frameWidth`/`frameHeight`, `targetBitrate`, and
the candidate pair's `availableOutgoingBitrate`.

## Testing tips

- A dev build's daemon is isolated from the stable install. Run it in the
  foreground with `RUST_LOG="info,mistl::stream=debug,..."` to see the
  relay's per-track decisions; release builds are needed for realistic
  throughput (debug builds slow SRTP/depacketizing).
- To test the other "who offers first" case, use another dev instance
  (`--instance <name>`): each has its own identity and therefore node id. A
  fresh instance has the network off (`mistl network on --instance <name>`),
  and any CLI call (even `key did`) auto-starts that instance's daemon —
  stop the ones you don't need.
- Two relays in the same room elect a cascade leader and one follows the
  other; stop the stable install's relay when measuring a dev build.
- Windows locks a running `.exe`: stop daemons started from
  `target/release/mistl.exe` before rebuilding, or the link step fails.
- `just mistlib-local` snapshots `../mistlib-dev` (including `third_party/`)
  into `.mistlib-src` to build against unreleased mistlib changes;
  `just mistlib-npm` goes back to the fetched release. tc-chat can load a
  local mistlib-wasm build via `MISTLIB_LOCAL` in its `.env`.
- Building a second checkout (e.g. a `git worktree` of an older commit) from
  an agent shell: `audiopus_sys` needs cmake, which is not on PATH; the one
  bundled with VS Build Tools lives under
  `...\Microsoft Visual Studio\18\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin`.

## Automated A/B with stream-lab (2026-11-19)

Most of the time above went into a human starting shares, reloading tabs and
judging VRChat by eye. [stream-lab](stream-lab.md) replaces that loop: a real
headless Chrome tab-captures a page drawing a frame counter + sender clock,
publishes it with real mistlib-wasm to a real isolated mistl relay, and
ffmpeg reads the RTSP output over UDP and TCP; every scenario runs both
offer orders. 20 s per scenario, same sender settings (8 Mbps cap, 30 fps):

| | `b7762ed` (before the fixes) | working tree with the fixes |
| --- | --- | --- |
| native offers first | **no lock** (the original bug, reproduced on the first run) | locks, first frame 3.0 s after publish |
| browser offers first | locks, first frame 11.3 s after publish | 3.2 s |
| frames at the RTSP reader | 30.2 fps wall-clock, no gaps | 28-30 fps, no gaps |
| freezes > 250 ms / decode errors (UDP and TCP) | 0 / 0 | 0 / 0 |
| browser encoder | 1904x984 @ 30, limitation `none` | same |

What the tool caught that eyeballing never would:

- The pre-fix relay's RTSP timestamps started ~8.6 s ahead (they kept
  advancing during the keyframe wait), so a reader that paces by media time
  (ffmpeg `-t`, and likely AVPro's buffer) saw a different clock than the wall
  clock. The analyzer now measures on the wall clock and reports
  media-vs-wall drift and timestamp jumps as their own metric.
- The first run's "53 missing frames" were measurement artefacts: the cached
  IDR a new viewer gets first (counter from before it joined) and single
  frames Chrome's capturer didn't encode. The analyzer separates `startup`,
  `senderSkips` and real `gaps` — check identical counters on UDP and TCP
  before blaming the network.
- ffmpeg's default RTSP buffering added ~500 ms to measured latency; the
  analyzer runs it in low-delay mode. Latency is for comparing builds, not an
  AVPro number.

### Iterating on the relay with the lab (same day)

Each row is one `just stream-lab --duration 20` (incremental `lab`-profile
build ~45 s + 4 scenarios ~2.5 min). "Lock→first frame" is from the relay
locking to the RTSP reader's first decoded frame; latency p95 is reader-side.

| Change | Lock→first frame | p95 latency | Notes |
| --- | --- | --- | --- |
| periodic PLI 2 s (first stability patch) | 2.0-2.9 s | 132 ms | latency spikes every ~2 s |
| periodic PLI back to 5 s | 4.8-5.1 s | 83 ms | spikes every ~5 s, but the first keyframe now waits for the periodic PLI |
| + request a keyframe on every `PLAY` | 5.1-5.6 s | 72 ms | no gain: the request was **dropped** by the 1 s debounce, because a viewer joins right after the lock-time PLI |
| + defer debounced requests to the end of the window | **~1.0 s** (startup 2.9-3.3 s from publish) | **73 ms** | all scenarios pass |

Lessons:
- Every keyframe is a latency spike for a few frames (bigger frame to encode,
  send and decode). A short periodic PLI interval trades steady stutter for
  faster recovery; recover with event-driven PLIs (loss, resync, new viewer)
  and keep the periodic one long.
- A debounce that drops requests hides them; defer instead. The symptom was
  "startup time equals the PLI interval", which only showed up because the lab
  measured lock→first-frame separately.

### Open: slow first frame when native offers first

Across 13 native-offers-first runs (vendored patch, SDP rewrite, and the
chosen answering-role fix alike) the reader's first frame came 5.3-10.9 s
after the relay locked, against ~2 s when the browser offers first; 2 of the
13 runs also degraded (18-23 fps, freezes, timestamp jumps). It does not
depend on which DTLS fix is used. What is known: the relay receives an IDR
right after locking and forwards a fresh IDR ~0.3 s after the reader's
`PLAY` (deferred PLI), yet ffmpeg's first decoded frame is the *next periodic*
IDR ~5 s later; audio arrives at the same time in both orders; the
negotiation-lock change made no difference. Next step: capture the RTP the
reader receives around `PLAY` (sequence numbers / NAL types) to see whether
that first IDR reaches it intact.

Lesson from the A/B: one good run is not a measurement. The vendored patch
first looked 3x faster (1.8 s) on a single sample; three more runs put it in
the same 5-11 s band. Repeat scenarios before comparing builds.

Not covered by the lab: AVPro itself (buffering, `rtspt://` handling), real
network links between machines (everything is loopback), and CPU contention
with VRChat running. A final look in VRChat is still needed, but only once per
change instead of once per hypothesis.
