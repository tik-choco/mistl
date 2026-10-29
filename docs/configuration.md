# Configuration

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

[vault]
replicas = 3                                 # copies including the owner; 1–8
capacity_bytes = 10737418240                 # total vault capacity, including own data
min_free_bytes = 1073741824                  # free disk space to reserve
max_object_bytes = 4294967296                # maximum file size
dead_after_secs = 86400                      # replace a member after this long unseen

[stream]
rtsp_url = "rtsp://127.0.0.1:8554/stream"   # use 0.0.0.0 to expose on the LAN
frame_rate = 30
audio_capture = false                        # local capture audio: not implemented yet
capture_backend = "native"                   # "native" (built-in) or "ffmpeg"
max_width = 1920                             # native backend: downscale wider screens
# room = "my-room"                           # tc-chat room for `stream relay`/`stream share`
audio_codec = "aac"                          # relay audio track: "aac" (AVPro) or "opus"
cascade = true                               # cascade distribution across relay nodes (see the CLI reference)

[chat_relay]
enabled = false                              # join configured tc-chat rooms at daemon start (requires restart)
rooms = []                                   # tc-chat room(s) to relay (requires restart)

[tunnel]
enabled = false                              # join the tunnel room automatically at daemon start
# room_id = "a1b2c3d4"                      # default: generated on first `tunnel start` and saved here
auto_accept = false                          # approve every inbound connection without asking (use with care)
# allow_peers = ["<node-id>"]               # auto-approve only these peers; anything else still prompts
stdio_enabled = false                        # let approved peers run `stdio_command` on this machine
# stdio_command = ["pwsh", "-NoLogo"]       # only used when stdio_enabled = true

[ai]
# default_ref = { provider_id = "http", model = "example-model" }
# tts = { provider_id = "http", model = "speech-model", voice = "speaker", lang_voices = { en = "english-speaker" } }
# stt = { provider_id = "http", model = "transcription-model" }
api_listen = "127.0.0.1:6478"                # local OpenAI-compatible API (serve)
# api_allowed_origins = ["https://example.com"]  # web apps allowed to call that API (CORS)
request_timeout_secs = 120                   # p2p inactivity timeout (resets per chunk)

# [[ai.providers]]                          # HTTP endpoint
# id = "http"
# label = "My endpoint"
# base_url = "http://127.0.0.1:8000/v1"
# api_key = ""
# enabled = true                            # absent = true; disabling preserves references
# models = ["example-model"]                # discovery cache
# models_fetched_at = "2026-10-01T00:00:00Z"
# provide = false
# shared = []

# [[ai.providers]]                          # Room provider
# id = "room"
# label = "Team"
# base_url = "mist-network://team-room"
# api_key = ""
# enabled = true
# provide = true                            # automatically provide while this Room is enabled
# shared = [{ provider_id = "http", model = "example-model" }]

[ui]
enabled = true                               # serve the dashboard from the daemon
listen = "127.0.0.1:6480"                    # keep on loopback (no auth)
```

Note: storage, chat_relay, ai, and stream relay can each join their **own** room --
the p2p transport supports multiple simultaneous rooms per process. AI rooms are
providers with a mist-network://<room> URL. Each enabled room can share its own
HTTP model references, and joins/leaves apply live. See [AI configuration](ai-config.md)
for model resolution, IPC shapes and automatic migration of legacy presets.
`[storage] room_ids` has no such fallback -- leave it unset (or empty) to keep
the store purely local (no network join at all). Unlike the other room
settings, `room_ids` is a list: the store joins **all** listed rooms
simultaneously, and the list can be changed at any time without a daemon
restart. The legacy single-room form (`room_id =
"my-room"`) still parses, loading as a one-element `room_ids` list.

Data lives in `%APPDATA%\tik-choco\mistl\data\` (keys, blocks, spools, logs).
Vault settings apply after daemon restart. Its group, recovery key, and encrypted
blocks are runtime state in the data directory, not configuration. See
[Vault](vault.md) for setup and recovery.
