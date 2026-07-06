# Verifying the VRChat screen-share relay

The screen-share feature has two layers that are verified separately:

1. the **RTSP serving** half — the two-track (H264 video + AAC audio) stream
   VRChat's AVPro player actually reads — which can be checked **locally, with
   no p2p network**, using the built-in synthetic self-test feed; and
2. the **live relay** half — receiving a real [tc-chat](https://github.com/tik-choco/tc-chat)
   screen share over the p2p network — which needs reachable signaling and is
   verified by watching the daemon log while a real share is running.

All the interesting log lines are at the default `info` level, so "watch the
log and confirm it works" is the intended workflow.

## 1. Local RTSP / AVPro serving — `mistl stream selftest`

This feeds a synthetic, decoder-valid H264 stream (a visibly moving test
pattern) plus an optional audio tone into the real RTSP server, so you can
confirm the exact output VRChat consumes without needing the p2p leg or a real
screen share.

```console
$ mistl stream selftest --audio aac
  rtsp://127.0.0.1:8554/stream
```

Options: `--audio aac|opus|none` (default `aac`, what AVPro plays),
`--width`/`--height`/`--fps` (default 640x360@30), `--seconds N` (auto-stop;
default: run until `mistl stream stop`).

Verify it with any standard RTSP client (`ffprobe`/`ffmpeg`/`ffplay`):

```console
# Two tracks are advertised: H264 video + AAC 48 kHz stereo audio.
$ ffprobe -rtsp_transport tcp rtsp://127.0.0.1:8554/stream
#   -> Stream #0: Video: h264, 640x360
#   -> Stream #1: Audio: aac, 48000 Hz, stereo

# Both tracks actually decode (frame count climbs, no fatal errors).
$ ffmpeg -rtsp_transport tcp -i rtsp://127.0.0.1:8554/stream -t 3 -f null -

# Watch it live.
$ ffplay -rtsp_transport tcp rtsp://127.0.0.1:8554/stream
```

**Multi-viewer fan-out** ("any number of people"): open the URL from two (or
more) clients at once — each gets the full stream, and `mistl stream status`
reports the live count:

```console
$ mistl stream status
{ "backend": "selftest", "clients": 2, "running": true, ... }
```

Stop with `mistl stream stop`.

> Note: on Windows, each `mistl <subcommand>` CLI call can take a few seconds
> because Defender re-scans the freshly built, unsigned `mistl.exe` on every
> spawn. This affects only the short-lived client process, not the resident
> daemon or the RTSP path — add an exclusion for `mistl.exe` if it bothers you.
> If you poll `stream status` in a tight loop, remember each call is delayed by
> that scan, so the sample lands later than the `sleep` suggests.

## 2. The live tc-chat -> VRChat path — watch the daemon log

Someone shares their screen (with audio) in a tc-chat room. On the machine that
should feed VRChat:

```console
$ mistl stream relay --room <the tc-chat room id>
```

To watch the pipeline come alive, run the daemon in the **foreground** (its log
goes to stderr; the background daemon instead writes
`%APPDATA%\tik-choco\mistl\data\daemon.log`):

```console
$ set RUST_LOG=mistl=info          # default; use mistl=debug for per-packet detail
$ mistl daemon run
```

Log lines to look for, in order:

| Log line (`info`) | Confirms |
| --- | --- |
| `relay: waiting for a screen share in the room` | joined, no publisher yet |
| `relay: locking onto publisher <id>` | a share was found and locked onto |
| `relay: first keyframe (IDR) received from publisher` | video is decoding |
| `relay: audio track attached codec=aac` | the share's audio was picked up |
| `relay: throughput video_au_per_s=.. audio_frames_per_s=.. viewers=N` | live rates + local VRChat viewers, every 5s |
| `RTSP ... SETUP ... trackID=0` / `trackID=1` | a VRChat client set up video / audio |
| `PLAY: session marked playing` / `first RTP packet sent to session` | RTP is flowing to that viewer |
| `relay: publisher's video track ended; unlocking` | the share stopped |

If `audio_frames_per_s` stays 0, the sharer didn't tick the browser's "share
audio" box; if `viewers` stays 0, no VRChat client has opened the URL yet.

## Topology — how "any number of viewers" works

VRChat's AVPro can only play a URL; it can't join the p2p swarm. The scalable,
lowest-latency arrangement is therefore a **mesh of local relays**:

- each viewer runs `mistl stream relay --room X` on their own machine;
- each receives the share directly over mistlib p2p and re-serves it on **their
  own localhost**;
- each viewer's VRChat plays `rtsp://127.0.0.1:8554/stream` (localhost).

No public IP or port-forwarding is needed — everyone talks to their own
loopback. The ceiling is the **sharer's uplink**: in a mesh the publisher sends
one copy per viewer, so this scales to a handful comfortably and to more with a
fast uplink. Scaling to very large audiences would need a cascade/SFU relay
tree (mistl re-publishing to downstream relays) — a future addition.

> Consensus (`../mistlib-consensus`, Raft/HotStuff) is **not** used for the
> media data-plane and should not be: agreeing on an ordered log adds
> round-trips per commit, the opposite of low latency. It is only relevant to
> control-plane coordination (e.g. electing relay nodes in a future cascade),
> never to carrying the video/audio itself.

## Latency

The relay adds very little: roughly one frame of access-unit assembly, ~21 ms
of AAC framing on the audio path, and a localhost hop. The dominant latency is
outside mistl's control — the browser's encoder and, especially, VRChat AVPro's
own RTSP jitter/decode buffer. Because each viewer serves RTSP over their own
loopback, the mesh topology already gives the lowest achievable last-mile
latency; there is no relay-side buffering left to remove.
