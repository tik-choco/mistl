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

[escrow]
enabled = false
rooms = []
max_blob_bytes = 33554432
max_total_bytes = 1073741824
max_slots_per_room = 1024
auto_replicas = 3                            # target copies, 1–8; 1 disables automatic replication
dead_after_secs = 86400                      # replace providers unseen for this long; minimum 60

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

[nostr]
enabled = false                              # start the relay with the daemon
listen = "127.0.0.1:7447"                    # loopback by default
tls = false                                  # plain ws://; true enables wss://
cert_path = ""                               # with TLS: PEM chain; empty generates a self-signed certificate
key_path = ""                                # PEM private key; set together with cert_path
tls_hosts = []                               # extra DNS names or IPs on the self-signed certificate
rooms = []                                   # mistl rooms for the Nostr relay cluster
upstreams = []                               # external ws:// or wss:// relay URLs
upstream_filters = []                        # JSON filter strings; empty disables subscriptions
upstream_publish = true                      # publish accepted events to upstream relays
signaling_bridge = true                      # bridge client signaling subscriptions at the gateway
bridge_relays = []                           # empty uses mistlib's default relays, excluding loopback
mirror_kinds = [25049, 25050]                # signaling kinds the bridge can mirror and publish
write_pubkeys = []                           # permitted hex public keys; empty allows all authors
name = "mistl relay"
description = "Nostr relay hosted by a mistl node"
contact = ""
max_events = 50000
max_event_bytes = 65536
max_connections = 256
max_subscriptions = 32                       # per connection
max_filters = 16                             # per REQ / COUNT
max_limit = 500                              # per query
sync_days = 7                                # history requested when a mesh peer connects

[tunnel]
enabled = false                              # join the tunnel room at daemon start (set by `tunnel start`, cleared by `tunnel stop`)
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

### Nostr relay

`mistl nostr start` serves plain `ws://` by default, listening only on
`127.0.0.1:7447`. Set `listen` to an appropriate network address to accept
other machines. Set `tls = true` to serve `wss://`: with no PEM paths, the
relay generates a self-signed certificate under `<data_dir>/nostr/tls/`.
Add client-facing DNS names or IPs to `tls_hosts` for the generated certificate;
clients must explicitly trust or pin it. Public clients such as Damus and
Amethyst reject self-signed certificates. For those clients, set both
`cert_path` and `key_path` to a trusted PEM chain and key (for example, a
Let's Encrypt certificate), or serve `ws://` behind a TLS-terminating reverse
proxy that presents a trusted certificate.

Relays in the same mistl room elect a Raft leader for that room. Every node
forwards accepted events to the leader, which redistributes them to all nodes
in one sequence so their state converges. Nodes re-fetch gaps and catch up after
leader changes; without an elected leader, they fall back to direct broadcast.
An empty `rooms` list keeps the relay standalone. `write_pubkeys` restricts
writes from both clients and mesh peers to the listed hexadecimal Nostr public
keys; an empty list allows any author.

Configure `upstreams` to connect to external Nostr relays. The elected cluster
leader (gateway), or a standalone relay, connects; followers receive upstream
events through the cluster. The gateway subscribes using the JSON filter
objects in `upstream_filters`. An empty filter list disables subscriptions,
leaving a publish-only connection when `upstream_publish` is enabled. With
`upstream_publish = true` (the default), the gateway publishes events accepted
by the cluster to those relays. Configure these settings on each relay that may
become a gateway:

```toml
[nostr]
rooms = ["my-room"]
upstreams = ["wss://relay.example.com"]
upstream_filters = ['{"kinds":[1],"authors":["<hex>"]}']
```

The signaling bridge is enabled by default. With `nostr_auto`, mistlib prefers
its local relay at `ws://localhost:7447` when available, so the gateway must
bridge signaling to external relays for peers on other machines to discover
each other. It mirrors the cluster clients' signaling subscriptions using merged
filters, rather than subscribing to every signaling event, and publishes only
events whose kinds are in `mirror_kinds` (by default 25049 and 25050). Dedicated
bridge connections open only on the gateway while there is matching subscription
interest. A standalone relay acts as its own gateway. Set `bridge_relays` to
choose the external relays; an empty list uses mistlib's default relay list.
Loopback addresses are excluded. Configured upstream connections also carry
signaling subscriptions when the bridge is enabled, and remain open according
to their upstream settings even when there is no signaling interest. Configure
the bridge on each relay that may become a gateway.

Set `signaling_bridge = false` to disable the bridge. This does not change
mistlib's local-first behavior: if a relay still listens on port 7447, mistlib
continues to use it, and cross-machine signaling needs another bridge. When no
local relay is reachable (for example, the relay on port 7447 is stopped),
mistlib can instead use the public relays directly.

`upstreams`, `upstream_filters`, `upstream_publish`, `signaling_bridge`,
`bridge_relays`, and `mirror_kinds` cannot be changed from a remote dashboard
session. Changes apply the next time the relay starts. See the
[CLI reference](cli.md) for status and queries.

Data lives in `%APPDATA%\tik-choco\mistl\data\` (keys, blocks, spools, logs).
