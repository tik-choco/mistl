# mistl

A Rust daemon CLI that unifies tik-choco ecosystem features in a single binary.
It integrates the [tc-storage](https://github.com/tik-choco/tc-storage) CLI and the
[mistlink](https://github.com/tik-choco-lab/mistlink) CLI, built on top of
[mistlib](https://github.com/tik-choco-lab/mistlib) (path dependencies).

- **identity** — user profile and `did:key` Ed25519 key management (tc-storage compatible)
- **store** — content-addressed storage (CIDv1 / sha2-256 / 1 MiB chunks, mistlib StorageEngine)
- **stream** — screen-share RTSP server (playable by VRChat's AVPro video player; Rust
  port of mistlink), from local screen capture or relayed from a
  [tc-chat](https://github.com/tik-choco/tc-chat) screen share (video + audio) over the
  p2p network
- **mailbox** — p2p store-and-forward messaging ("p2p mail server"): when the recipient
  is offline, a bot node holds the deposit and forwards it once they come online
- **ui** — embedded web dashboard (`mistl ui`): operate all of the above from a
  browser at `http://127.0.0.1:6480/`
- **ai** — p2p AI network, wire-compatible with
  [mistai](https://github.com/tik-choco-lab/mistai) protocol v1 (tc-mistllm /
  tc-translate peers can share the room): *provide* LLM inference to peers from any
  OpenAI-compatible upstream, and/or *serve* a local OpenAI-compatible API endpoint
  whose requests are answered by the network

The release build is a single, fully standalone `mistl.exe`: screen capture uses
Windows.Graphics.Capture, H264 encoding uses OpenH264 compiled into the binary, and
the MSVC runtime is statically linked — no external tools, DLLs, or installers.

## Requirements

- Rust (edition 2024) with [mistlib](https://github.com/tik-choco-lab/mistlib) checked
  out as `../mistlib-dev` (path dependencies)
- [cmake](https://cmake.org/) on PATH at build time (libopus is compiled from source
  for the relay's audio pipeline). With cmake ≥ 4.0, also set
  `CMAKE_POLICY_VERSION_MINIMUM=3.5` in the build environment.
- Optional: [ffmpeg](https://ffmpeg.org/) on PATH, only if you switch
  `stream.capture_backend` to `"ffmpeg"` (the default `"native"` backend has no
  external dependencies)

## Build

```console
$ cargo build --release
```

## Usage

The CLI acts as a client to a resident daemon (IPC: loopback TCP with token auth).

```console
# Daemon management
$ mistl daemon start          # start in the background
$ mistl daemon status
$ mistl daemon stop

# Profile / DID keys
$ mistl key did               # generates a key on first call
$ mistl key list
$ mistl profile set display_name "yourname"
$ mistl profile show

# Storage
$ mistl store put .\file.bin  # -> returns a CID
$ mistl store ls
$ mistl store get <cid> --output .\file.bin

# Screen share (VRChat)
$ mistl stream start          # local screen -> rtsp://<LAN IP>:8554/stream
$ mistl stream relay --room my-room   # tc-chat share (video+audio) -> same URL
$ mistl stream status
$ mistl stream stop

# Mailbox
$ mistl mailbox send <did|node-id> --message "hello"
$ mistl mailbox send <did|node-id> --file .\data.bin
$ mistl mailbox fetch         # receive messages pending for me
$ mistl mailbox ls            # deposits this node is holding as a bot

# Web dashboard
$ mistl ui                    # starts the daemon if needed, opens the browser

# AI network
$ mistl ai provide start      # serve LLM inference to peers from [ai] upstream_url
$ mistl ai serve start        # local OpenAI-compatible API -> http://127.0.0.1:6478/v1
$ mistl ai chat "hello" --model mock-echo-1
$ mistl ai models
$ mistl ai status
```

With `ai serve` running, any OpenAI client works against this node — requests go to
the local provider when one is running, otherwise to the first provider discovered
on the p2p network:

```console
$ curl http://127.0.0.1:6478/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{"messages":[{"role":"user","content":"hi"}],"stream":true}'
```

### Sharing a tc-chat screen into VRChat

Someone shares their screen in a [tc-chat](https://github.com/tik-choco/tc-chat) room
(browser screen share, optionally with tab/system audio). On the machine that should
feed VRChat, run:

```console
$ mistl stream relay --room <the tc-chat room id>

  Paste this URL into the VRChat video player:

      rtsp://192.168.x.x:8554/stream
```

mistl joins the room as a WebRTC peer, receives the share (H264 video + Opus audio),
transcodes the audio to AAC (what AVPro plays over RTSP), and serves both tracks on
the RTSP URL. Paste the printed URL into any AVPro-based VRChat video player.
Note: mistlib supports one room per process, so the relay room is shared with
mailbox/ai (set `stream.relay_room`, `mailbox.room_id`, `ai.room_id` consistently,
or leave the others unset).

`mailbox send` returns one of three `status` values:

| status | meaning |
| --- | --- |
| `delivered` | recipient was online; delivered directly |
| `deposited` | deposited with a connected bot node |
| `queued` | no reachable peers; saved to the local outbox for retry |

## Configuration

`%APPDATA%\tik-choco\mistl\config\config.toml` (created with defaults on first run):

```toml
[identity]
# display_name = "yourname"

[storage]
# blocks_dir = 'D:\mistl-blocks'
capacity_bytes = 10737418240

[stream]
rtsp_url = "rtsp://127.0.0.1:8554/stream"   # use 0.0.0.0 to expose on the LAN
frame_rate = 30
audio_capture = false                        # local capture audio: not implemented yet
capture_backend = "native"                   # "native" (built-in) or "ffmpeg"
max_width = 1920                             # native backend: downscale wider screens
# relay_room = "my-room"                     # tc-chat room for `stream relay`
audio_codec = "aac"                          # relay audio track: "aac" (AVPro) or "opus"

[mailbox]
# room_id = "my-private-room"               # default: "mistl-mailbox-v1"
serve_as_bot = true                          # hold deposits for other peers

[ai]
# room_id = "my-llm-room"                   # default: the mailbox room (see note)
# upstream_url = "http://127.0.0.1:11434/v1" # OpenAI-compatible upstream (provide)
# upstream_api_key = "sk-..."
# default_model = "llama3"                  # default: first model from upstream
# advertised_models = ["llama3"]            # default: fetched from upstream /models
# temperature = 0.7
api_listen = "127.0.0.1:6478"                # local OpenAI-compatible API (serve)
request_timeout_secs = 120                   # p2p inactivity timeout (resets per chunk)

[ui]
enabled = true                               # serve the dashboard from the daemon
listen = "127.0.0.1:6480"                    # keep on loopback (no auth)
```

Note: mistlib supports **one room per process**, so mailbox and ai share it.
`[ai] room_id` defaults to the mailbox room; to join an existing mistai app room,
set both to the same value.

Data lives in `%APPDATA%\tik-choco\mistl\data\` (keys, blocks, spools, logs).

## Architecture

```
mistl <subcommand>  --(JSON over loopback TCP)-->  mistl daemon run
                                                     ├─ identity  (did:key ed25519, profile)
                                                     ├─ storage   (mistlib StorageEngine + NativeBlockStore)
                                                     ├─ stream    (screen capture or p2p WebRTC relay → RTP → RTSP server)
                                                     ├─ mailbox   (signed envelope spools over net)
                                                     ├─ ai        (mistai protocol v1 over net + local OpenAI-compatible HTTP)
                                                     ├─ web       (embedded dashboard + /api/call bridge into the same router)
                                                     └─ net       (shared mistlib WebRTC/Nostr transport: one engine, one room)
```

- IPC discovery info is written to `data\daemon.json` (port + random token); only the
  local user can connect
- tc-storage interop: DID format, AES-256-GCM + PBKDF2-SHA256 (210k) envelopes
  (`identity::crypto`), CID semantics
- mistlink interop: AVPro-friendly dummy SPS/PPS RTSP keepalive, PT 96 / SSRC 0x12345678
- `stream` backends: `native` captures via Windows.Graphics.Capture and encodes with
  OpenH264 in-process; `ffmpeg` spawns ffmpeg (gdigrab → MPEG-TS → demux) as a fallback;
  `relay` (via `stream relay`) receives a tc-chat WebRTC screen share over mistlib and
  re-serves it (H264 passthrough, Opus→AAC transcode, RTCP sender reports for lipsync)
- `ai` speaks mistai protocol v1 on the wire (`provider_hello` / `llm_request` /
  `llm_response_chunk` with seq reordering / `llm_response_done`), so browser-based
  mistai consumers and providers in the same room interoperate; the `net` module
  multiplexes mailbox and ai traffic over mistlib's single raw-message handler by
  message shape

## Known limitations (v0.1)

- `mailbox` file sends forward only the envelope (cid/name/size); p2p block transfer of
  the file bytes is not implemented yet
- `stream` local capture is video-only (audio_capture is ignored); relayed shares carry
  audio. Inbound NACK is not implemented (loss shows until the next keyframe; the relay
  requests one every 5s)
- No bot capability advertisement (every connected peer is treated as a bot candidate)
- `ai` implements the LLM part of the mistai protocol; voice (tts/stt) messages are
  decoded but not served, and `raft_message` scheduling is passed through untouched
- The local API server (`ai serve`) has no auth; keep `api_listen` on loopback unless
  the network is trusted
- The web dashboard has no login; it rejects cross-origin and non-localhost requests,
  but anyone with local access can use it — keep `[ui] listen` on loopback

## License

[MPL-2.0](LICENSE)

Binary builds statically link the
[Fraunhofer FDK AAC Codec Library for Android](https://github.com/mstorsjo/fdk-aac)
(via the `fdk-aac` crate) for the relay's Opus→AAC transcoding, which is licensed
under its own terms (© Fraunhofer-Gesellschaft; see the fdk-aac NOTICE), and
[libopus](https://opus-codec.org/) (BSD-3-Clause).
