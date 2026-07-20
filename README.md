# MISTL

**MISTL** (binary name: `mistl`) is a Rust daemon CLI that unifies tik-choco ecosystem features in a single binary.
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
- **ui** — embedded web dashboard (`mistl` with no args, or `mistl ui`): operate all
  of the above from a browser at `http://127.0.0.1:6480/`, bilingual EN/JA, with
  drag-and-drop file storage and live settings
- **ai** — p2p AI network, wire-compatible with
  [mistai](https://github.com/tik-choco-lab/mistai) protocol v1 (tc-mistllm /
  tc-translate peers can share the room): *provide* LLM inference to peers from any
  OpenAI-compatible upstream, and/or *serve* a local OpenAI-compatible API endpoint
  whose requests are answered by the network

The release build is a single, fully standalone `mistl.exe`: screen capture uses
Windows.Graphics.Capture, H264 encoding uses OpenH264 compiled into the binary, and
the MSVC runtime is statically linked — no external tools, DLLs, or installers.

## Requirements

- Rust (edition 2024) and git (with access to the private
  [mistlib](https://github.com/tik-choco-lab/mistlib-dev) repository)
- [just](https://github.com/casey/just) (optional but recommended task runner)
- [cmake](https://cmake.org/) on PATH at build time (libopus is compiled from source
  for the relay's audio pipeline). With cmake ≥ 4.0, also set
  `CMAKE_POLICY_VERSION_MINIMUM=3.5` in the build environment.
- Optional: [ffmpeg](https://ffmpeg.org/) on PATH, only if you switch
  `stream.capture_backend` to `"ffmpeg"` (the default `"native"` backend has no
  external dependencies)

## Build

mistlib is a path dependency fetched into `.mistlib-src/` (a plain git clone,
not committed) by `scripts/fetch-mistlib`, configured through `.env`:

```console
$ cp .env.example .env        # set MISTLIB_REPO to a URL your git auth can clone
$ just release                # fetches mistlib on first build, then cargo build --release
```

Without `just`: run `scripts/fetch-mistlib.sh` (or `scripts\fetch-mistlib.ps1`
on Windows) once, then `cargo build --release`. Re-run `just fetch-mistlib`
whenever you want to update to the latest `MISTLIB_REF`; the clone in
`.mistlib-src/` is a normal git checkout, so you can also edit, branch, and
push mistlib changes from there while developing against it.

## Quick start

Double-click `mistl.exe` (or run `mistl` with no arguments): the daemon starts in
the background and the dashboard opens in your browser — everything below can be
done from there, including settings. The dashboard is bilingual (English/日本語,
following your browser language).

## Usage (CLI)

The CLI acts as a client to a resident daemon (IPC: loopback TCP with token auth).
Client commands start the daemon automatically if it isn't running.

```console
# Daemon management
$ mistl status                # combined overview (daemon, stream, ai)
$ mistl daemon start          # start in the background (also happens automatically)
$ mistl daemon status
$ mistl daemon stop

# Configuration (no config.toml editing needed)
$ mistl config show           # secrets masked
$ mistl config set ai.default_preset_id default
$ mistl config set stream.room my-room

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
$ mistl stream start --room my-room   # ...and also publish it into a mistlib room (native capture, Windows)
$ mistl stream relay --room my-room   # tc-chat share (video+audio) -> same URL
$ mistl stream selftest --audio aac   # synthetic video+audio feed for local testing
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
$ mistl ai provide start      # serve LLM inference to peers using the resolved default preset
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
Note: the p2p transport supports multiple simultaneous rooms per process, so
`stream.room` (shared by both `stream relay` and `stream share`) can name its
own room independent of `mailbox.room_id` and `ai.room_id` -- or reuse one of
them if you'd rather keep everything in one room.

**Any number of viewers (mesh + cascade):** VRChat's AVPro can only play a URL,
not join the p2p swarm, so the scalable and lowest-latency arrangement is for
*each* viewer to run `mistl stream relay --room X` on their own machine and
point their VRChat at their own `rtsp://127.0.0.1:8554/stream`. Everyone talks
to their own loopback (no public IP or port-forwarding).

With `[stream] cascade = true` (the default), every relay node in the room
runs a small Raft control plane to elect a **leader** among themselves (this
is relay-node-only leader election — separate from, and invisible to, the
tc-chat browser peers sharing the screen). The leader is the one that locks
onto the sharer directly and re-publishes what it receives back into the
room as its own tracks (raw passthrough, no re-encoding); every other relay
node (**follower**) locks onto the leader's re-published tracks instead of
the sharer. This means the sharer's browser only ever uplinks to one peer
(the leader) no matter how many relays are watching, and it also reaches
relays that have no direct p2p connection to the sharer, since mistlib's
overlay is a selective mesh rather than a full one. If the leader goes away,
the remaining relays elect a new one automatically and re-lock — no manual
intervention. Set `cascade = false` to opt a node out and fall back to the
original "lock onto the sharer directly" behavior.

See [VERIFY.md](VERIFY.md) for the full topology, the log lines that confirm
each stage (including cascade role/leader changes), and `mistl stream
selftest` — a synthetic video+audio feed that exercises the exact two-track
RTSP output VRChat consumes, so you can confirm the playback path locally
(ffprobe/ffplay) without a live p2p share.

`mailbox send` returns one of three `status` values:

| status | meaning |
| --- | --- |
| `delivered` | recipient was online; delivered directly |
| `deposited` | deposited with a connected bot node |
| `queued` | no reachable peers; saved to the local outbox for retry |

## Configuration

Configuration lives in `%APPDATA%\tik-choco\mistl\config\config.toml` (created with
defaults on first run) and can be changed with `mistl config set` or in the
dashboard's Settings panel — changes hot-reload into the running daemon and apply
the next time the affected service starts (room ids and `[ui]` need a daemon
restart):

```toml
[identity]
# display_name = "yourname"

[storage]
# blocks_dir = 'D:\mistl-blocks'
capacity_bytes = 10737418240
# room_ids = ["my-storage-room"]             # tc-chat room(s) for peer block exchange; default: local-only, no room joined

[stream]
rtsp_url = "rtsp://127.0.0.1:8554/stream"   # use 0.0.0.0 to expose on the LAN
frame_rate = 30
audio_capture = false                        # local capture audio: not implemented yet
capture_backend = "native"                   # "native" (built-in) or "ffmpeg"
max_width = 1920                             # native backend: downscale wider screens
# room = "my-room"                           # tc-chat room for `stream relay`/`stream share`
audio_codec = "aac"                          # relay audio track: "aac" (AVPro) or "opus"
cascade = true                               # cascade distribution across relay nodes (see below)

[mailbox]
# room_id = "my-private-room"               # default: "mistl-mailbox-v1"
serve_as_bot = true                          # hold deposits for other peers

[ai]
# room_id = "my-llm-room"                   # default: the mailbox room (see note)
# default_preset_id = "default"             # which [[ai.presets]] entry `ai provide`/`ai serve` use by default
# advertised_models = ["llama3"]            # default: fetched from the resolved preset's provider /models
api_listen = "127.0.0.1:6478"                # local OpenAI-compatible API (serve)
request_timeout_secs = 120                   # p2p inactivity timeout (resets per chunk)

# [[ai.providers]]                          # connection info ("where to connect")
# id = "default"
# label = "Default"
# base_url = "http://127.0.0.1:11434/v1"    # OpenAI-compatible upstream, e.g. Ollama
# api_key = ""

# [[ai.presets]]                            # named model config ("how to call it")
# id = "default"
# label = "Default"
# provider_id = "default"                   # references an [[ai.providers]] id
# model = "llama3"
# temperature = 0.7
# reasoning_effort = "medium"               # optional: "none" | "minimal" | "low" | "medium" | "high"

[ui]
enabled = true                               # serve the dashboard from the daemon
listen = "127.0.0.1:6480"                    # keep on loopback (no auth)
```

Note: storage, mailbox, ai, and stream relay can each join their **own** room --
the p2p transport supports multiple simultaneous rooms per process. `[ai] room_id`
defaults to the mailbox room for convenience when unset; set it explicitly to
join a different room, e.g. an existing mistai app room. `[storage] room_ids` has
no such fallback -- leave it unset (or empty) to keep the store purely local (no
network join at all). Unlike the other room settings, `room_ids` is a list: the
store joins **all** listed rooms simultaneously, and the list can be changed at
any time without a daemon restart. The legacy single-room form (`room_id =
"my-room"`) still parses, loading as a one-element `room_ids` list.

Data lives in `%APPDATA%\tik-choco\mistl\data\` (keys, blocks, spools, logs).

## Profile document (ecosystem interop)

The user profile is a small JSON document, persisted at
`data\identity\profile.json` and returned (merged with the identity's `did`) by
`profile.show`. It is designed to be read as-is by sibling apps (tc-chat,
tc-storage) that share the same `did:key` identities and CID-addressed content
store:

```json
{
  "did": "did:key:z6Mk…",
  "display_name": "Ada",
  "bio": "Loves math",
  "avatar_cid": "baf…",
  "updated_at": "2026-07-06T12:34:56+00:00"
}
```

- `did` — the owner's `did:key` (Ed25519), the stable key other apps index by.
- `display_name`, `bio` — optional free-text fields.
- `avatar_cid` — optional. The **root CID of the profile image in the shared
  content store** (the same store as `store put` / `POST /api/store/upload`,
  CIDv1 / sha2-256 / dag-cbor `FileManifest`). The image itself is *not* inlined;
  any peer holding the block (or able to resolve it over the store) fetches the
  bytes by this CID — e.g. `GET /api/store/download?cid=<avatar_cid>` locally.
- `updated_at` — RFC 3339 timestamp of the last change, stamped on every
  `profile.set`, so a peer that has seen several copies can pick the freshest.
- Any additional string fields set via `profile.set` are preserved verbatim
  (they round-trip through the top level of the document).

Read/write it over the dashboard bridge or IPC:

- `profile.show` `{}` → the document above.
- `profile.set` `{field, value}` → sets one field (`display_name`, `bio`,
  `avatar_cid`, or a custom name); an empty `value` clears it.

The dashboard's **Profile** panel sets the avatar end-to-end: it downscales the
chosen image to 256 px client-side, uploads it to the content store
(`POST /api/store/upload`), and points `avatar_cid` at the returned CID.

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
- Cascade (`[stream] cascade`) v1: a follower's own PLI targets the leader's
  re-published track, which isn't forwarded back to the original sharer -- only the
  leader's PLI actually reaches it, so a late-joining follower's keyframe wait is
  bounded by the leader's ~5s PLI cadence rather than its own. A relay that's already
  locked onto the sharer when a new leader is elected doesn't re-request the sharer's
  tracks either; it relies on either already being connected to the new leader's
  re-publish or receiving a fresh negotiation
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
