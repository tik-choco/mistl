# CLI reference

Every feature is also available from the web dashboard (`mistl ui`).

## Usage (CLI)

The CLI acts as a client to a resident daemon (IPC: loopback TCP with token auth).
Client commands start the daemon automatically if it isn't running.

```console
# Daemon management
$ mistl status                # combined overview (daemon, stream, ai)
$ mistl daemon start          # start in the background (prints the dashboard URL)
$ mistl daemon start --host 0.0.0.0  # also bind the dashboard for other devices on the LAN
$ mistl daemon status
$ mistl daemon stop

# Configuration (no config.toml editing needed)
$ mistl config show           # secrets masked
$ mistl config set ai.default_ref '{"provider_id":"http","model":"example-model"}'
$ mistl config set stream.room my-room

# Profile / DID keys
$ mistl key did               # generates a key on first call
$ mistl key list

# DID delegation (root/leaf chain): let a browser app's per-origin identity
# be recognized as the same person as this mistl-held root identity
$ mistl key delegate --leaf did:key:z6Mk...   # manual transfer: prints a DelegationV1 JSON to paste into the app
$ mistl key delegations                       # list delegations issued so far
$ mistl key pair                              # no-copy-paste: prints a pairing code, waits for the app to claim it

$ mistl profile set display_name "yourname"
$ mistl profile show

# Storage
$ mistl store put .\file.bin  # -> returns a CID
$ mistl store ls
$ mistl store get <cid> --output .\file.bin

# Encrypted checkpoint escrow
$ mistl escrow status            # configured rooms and automatic replication status
$ mistl escrow status-json       # machine-readable status
$ mistl escrow slots <room>      # automatic replication status for each slot
$ mistl escrow rooms add <room>

# Screen share (VRChat)
$ mistl stream start          # local screen -> rtsp://<LAN IP>:8554/stream
$ mistl stream start --room my-room   # ...and also publish it into a mistlib room (native capture, Windows)
$ mistl stream relay --room my-room   # tc-chat share (video+audio) -> same URL
$ mistl stream selftest --audio aac   # synthetic video+audio feed for local testing
$ mistl stream status
$ mistl stream stop

# Chat relay
$ mistl chat rooms            # rooms currently joined by the chat relay
$ mistl chat log <room>       # replay persisted history for a room
$ mistl chat log <room> --limit 50

# Tunnel (P2P port forwarding)
$ mistl tunnel start          # join the configured room (generates one on first run)
$ mistl tunnel room           # show the room id to hand to the other side
$ mistl tunnel room --new     # issue a fresh room id and switch to it
$ mistl tunnel room --list    # recently used rooms
$ mistl tunnel serve 22       # offer local port 22 to peers in the room
$ mistl tunnel connect <room> 10022:22   # from the other machine: local 10022 -> peer's 22
$ mistl tunnel status         # peers, forwards, pending approvals, trust list
$ mistl tunnel approve <id> --remember   # allow a peer's connection, and remember it
$ mistl tunnel tui            # interactive terminal UI (talks to the daemon)

# Web dashboard
$ mistl ui                    # starts the daemon if needed, opens the browser

# AI network
$ mistl ai provide start      # turn provide on for enabled Rooms with a nonempty shared list
$ mistl ai provide stop       # turn provide off for every Room
$ mistl ai serve start        # local OpenAI-compatible API -> http://127.0.0.1:6478/v1
$ mistl ai chat "hello" --model mock-echo-1
$ mistl ai models
$ mistl ai status
$ mistl ai external apply --owner my-app < registration.json
$ mistl ai external get --owner my-app
$ mistl ai external remove --owner my-app
```

With `ai serve` running, OpenAI clients use the configured `ai.default_ref` or
resolve an explicit raw model id through the enabled providers' model caches.
HTTP providers call their endpoint directly; Room providers use the p2p network:

```console
$ curl http://127.0.0.1:6478/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{"messages":[{"role":"user","content":"hi"}],"stream":true}'
```

OpenAI tool calling (`tools`, `tool_choice`, `tool_calls`, `role: "tool"`) works too:
providers advertise a `tools` service next to `chat`, and the request is executed by the
provider's upstream. If the selected network provider does not advertise `tools` (an
older peer), the API answers `400` with `code: "tools_unsupported"` instead of
sending the tool fields to it.

Both default and room chat routes forward `reasoning_effort` (including the
explicit `"none"` value) to the upstream. To select an enabled Room provider
directly, use `GET /v1/rooms/{room}/models` or
`POST /v1/rooms/{room}/chat/completions`, `/audio/speech` and
`/audio/transcriptions` (the audio suffixes follow the same room prefix).
URL-encode the room id; missing or disabled rooms return 404. Room chat supports
JSON and SSE. Image content parts use a mistai-compatible `oai` tunnel, with
buffered responses adapted to SSE when requested. Providers advertise `oai` for
OpenAI chat, model-list and embedding requests. See
[AI configuration and room API](ai-config.md) for routes and limits.

Local applications can register HTTP connections and rooms through `ai external`
instead of editing mistl's configuration. Registrations are private, stored
separately in `ai-external.json`, and merged live with user settings. A disabled
user Room stays disabled. `get` masks API keys and reports room status; the
dashboard shows each owner's contributions read-only and can unregister them.

### Terminal AI agent

`mistl agent` opens a terminal UI: ask in natural language to check status,
manage stored files, or work on a project, and it runs tools and summarizes
the results. It uses the LLM selected by mistl's `ai.default_ref` / `ai.providers`
through the local OpenAI-compatible API, starting the daemon and API as needed.
See [AI configuration](ai-config.md) for model setup.

```console
$ mistl agent                              # interactive TUI
$ mistl agent -p "Is the AI network up?"    # one prompt on stdout, then exit
$ mistl agent -m model-id --reasoning-effort high
$ mistl agent --list-models                 # list model ids from the local API
```

Flags: `-p` / `--prompt` runs without the TUI; `-m` / `--model` selects a model;
`--reasoning-effort` overrides effort for this session; `-y` / `--yes` approves
state-changing tools automatically. `--tool-mode native|prompt` selects OpenAI
tools or fenced tool blocks (default: native, with automatic fallback when the
provider reports tools unsupported).

Read-only queries run directly; state-changing tools ask for `y` (yes), `n`
(no), or `a` (approve all for this session). Headless `-p` refuses changes unless
`-y` is given. Interactive or never-ending mistl commands are refused.

The startup directory is the workspace. File tools (`list_dir`, `find_files`,
`grep`, `read_file`) inspect files within it. With `just` on PATH, the agent
discovers justfile recipes and can run builds, tests, or background dev servers.
Model-requested recipes, shell commands, and background-process stops need
approval; process listing and output do not. Background processes started by the
agent are stopped on exit. Type `!<command>` to run a shell command yourself
without approval; its output is shared with the model on your next message.

| Command | Action |
| --- | --- |
| `/model [id]` | Pick a model from the local API, or set a raw model id |
| `/effort [level]` | Pick or set session effort; `default` uses mistl's default |
| `/mistl [start]` | Show instance info, or start the daemon |
| `/just` | List workspace recipes |
| `/clear`, `/help`, `/quit` | Clear the conversation, show help, or quit |

Typing `/` opens completion: `Up` / `Down` select, `Tab` completes, `Enter`
runs, and `Esc` hides it. `!` also completes `!just <recipe>`.

| Key | Action |
| --- | --- |
| `Enter` | Send |
| `Alt+Enter` / `Ctrl+J` | New line |
| `Esc` | Cancel the running turn or user shell command |
| `PgUp` / `PgDn` | Scroll |
| `Ctrl+O` | Expand/collapse tool output |
| `Ctrl+C` | Cancel, or quit when idle |

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
own room independent of `chat_relay.rooms` and AI Room providers -- or reuse one of
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

See [Verifying the screen-share relay](stream-verify.md) for the full topology, the log lines that confirm
each stage (including cascade role/leader changes), and `mistl stream
selftest` — a synthetic video+audio feed that exercises the exact two-track
RTSP output VRChat consumes, so you can confirm the playback path locally
(ffprobe/ffplay) without a live p2p share.

### Standing tc-chat relay

With `[chat_relay] enabled = true` and one or more `rooms`, the daemon joins
those tc-chat rooms at startup (once per daemon run — changing either setting
needs a restart) and stays in them for as long as it runs. It verifies every
inbound signed `tc-chat:*` wire against the sender's `did:key`, appends it
verbatim to a per-room log under `<data_dir>/relay/<room>/wirelog.jsonl`, and
answers other peers' `tc-chat:history-request` broadcasts by replaying that
log — so a tab joining later catches up even if nobody else was online.
Replayed wires keep their original signature, so receivers re-verify them
exactly like live traffic: the relay is never trusted, only present.

`mistl chat rooms` shows the configured rooms and whether each is currently
joined; `mistl chat log <room>` prints the tail of that room's stored wires
(the same view the dashboard's tc-chat Relay panel renders).

These two settings used to live in the (now removed) `[mailbox]` section as
`chat_rooms`/`chat_relay`; a config.toml still carrying them is migrated to
`[chat_relay]` on the next daemon start, and the old section is dropped.

### Encrypted checkpoint escrow

An opt-in [Raft replication prototype](escrow-replication.md) can replicate
new escrow rooms across a fixed set of storage nodes. It has bounded history and
requires additional work before unattended long-running use.

The optional tc-escrow v1 service keeps browser-encrypted application checkpoints
for late joiners. Run `mistl escrow rooms add <room>`, restart the daemon, and enable
network connections; configure that same escrow room in the application.
`mistl escrow status` reports rooms, slots, bytes, and automatic replication without
exposing contents. Use `mistl escrow status-json` for JSON. For per-slot replica
counts, use `mistl escrow slots <room>`.
The dashboard's **Store → Escrow** page offers the same room controls, status,
enabled switch and advanced capacity limits, with a daemon restart action. In
tc-linkai, copy the auto-generated `tce-…` **mistl room** into **Add room**.
Move encrypted checkpoints between nodes with `mistl escrow export <FILE>` and
`mistl escrow import <FILE>` (optional repeated `--room`; export accepts `--force`),
or use the dashboard's **Export** and **Import** buttons.
Signed heads use CAS and a pinned writer, with durable current/previous blobs that
remain downloadable after restart. See [escrow setup and protocol](escrow.md).
