# mistl

A Rust daemon CLI that unifies tik-choco ecosystem features in a single binary.
It integrates the [tc-storage](https://github.com/tik-choco/tc-storage) CLI and the
[mistlink](https://github.com/tik-choco-lab/mistlink) CLI, built on top of
[mistlib](https://github.com/tik-choco-lab/mistlib) (path dependencies).

- **identity** — user profile and `did:key` Ed25519 key management (tc-storage compatible)
- **store** — content-addressed storage (CIDv1 / sha2-256 / 1 MiB chunks, mistlib StorageEngine)
- **stream** — screen-share RTSP server (playable by VRChat's AVPro video player; Rust port of mistlink)
- **mailbox** — p2p store-and-forward messaging ("p2p mail server"): when the recipient
  is offline, a bot node holds the deposit and forwards it once they come online

The release build is a single, fully standalone `mistl.exe`: screen capture uses
Windows.Graphics.Capture, H264 encoding uses OpenH264 compiled into the binary, and
the MSVC runtime is statically linked — no external tools, DLLs, or installers.

## Requirements

- Rust (edition 2024) with [mistlib](https://github.com/tik-choco-lab/mistlib) checked
  out as `../mistlib-dev` (path dependencies)
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
$ mistl stream start          # -> rtsp://<LAN IP>:8554/stream
$ mistl stream status
$ mistl stream stop

# Mailbox
$ mistl mailbox send <did|node-id> --message "hello"
$ mistl mailbox send <did|node-id> --file .\data.bin
$ mistl mailbox fetch         # receive messages pending for me
$ mistl mailbox ls            # deposits this node is holding as a bot
```

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
audio_capture = false                        # not implemented yet
capture_backend = "native"                   # "native" (built-in) or "ffmpeg"
max_width = 1920                             # native backend: downscale wider screens

[mailbox]
# room_id = "my-private-room"               # default: "mistl-mailbox-v1"
serve_as_bot = true                          # hold deposits for other peers
```

Data lives in `%APPDATA%\tik-choco\mistl\data\` (keys, blocks, spools, logs).

## Architecture

```
mistl <subcommand>  --(JSON over loopback TCP)-->  mistl daemon run
                                                     ├─ identity  (did:key ed25519, profile)
                                                     ├─ storage   (mistlib StorageEngine + NativeBlockStore)
                                                     ├─ stream    (Windows.Graphics.Capture → OpenH264 → RTP → RTSP server)
                                                     └─ mailbox   (mistlib WebRTC/Nostr + signed envelope spools)
```

- IPC discovery info is written to `data\daemon.json` (port + random token); only the
  local user can connect
- tc-storage interop: DID format, AES-256-GCM + PBKDF2-SHA256 (210k) envelopes
  (`identity::crypto`), CID semantics
- mistlink interop: AVPro-friendly dummy SPS/PPS RTSP keepalive, PT 96 / SSRC 0x12345678
- `stream` backends: `native` captures via Windows.Graphics.Capture and encodes with
  OpenH264 in-process; `ffmpeg` spawns ffmpeg (gdigrab → MPEG-TS → demux) as a fallback

## Known limitations (v0.1)

- `mailbox` file sends forward only the envelope (cid/name/size); p2p block transfer of
  the file bytes is not implemented yet
- `stream` is video-only (audio_capture is ignored); no RTCP (NACK/PLI)
- No bot capability advertisement (every connected peer is treated as a bot candidate)

## License

[MPL-2.0](LICENSE)
