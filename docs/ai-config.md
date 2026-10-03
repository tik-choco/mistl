# AI providers and model references

Configure an HTTP endpoint in `ai.providers`, then select models directly with
`{ provider_id, model }`. Model names are raw upstream ids; labels identify
providers for display only. Temperature is never sent upstream. Bot chat tasks
can carry their own `reasoning_effort`.

```toml
[ai]
default_ref = { provider_id = "http", model = "example-model" }
tts = { provider_id = "http", model = "speech-model", voice = "speaker", lang_voices = { en = "english-speaker" } }
stt = { provider_id = "http", model = "transcription-model" }

[[ai.providers]]
id = "http"
label = "My endpoint"
base_url = "http://localhost:8000/v1"
api_key = ""
enabled = true
models = ["example-model"]
provide = false
shared = []

[[ai.providers]]
id = "room"
label = "Team"
base_url = "mist-network://team-room"
api_key = ""
enabled = true
models = []
provide = true
shared = [{ provider_id = "http", model = "example-model" }]
```

`models` and optional RFC 3339 `models_fetched_at` are discovery caches. HTTP
providers default to enabled, with empty caches. Room providers use an empty
API key and a `mist-network://<room>` URL.

Model lists refresh automatically when AI settings, a picker or a shared-model
checklist opens. Cached entries remain visible while discovery runs. Creation,
connection edits and re-enabling also trigger discovery. Each provider has one
in-flight request; successful results throttle revalidation for ten seconds.
Disabled providers are never fetched, and failures retain the previous cache.
Cards show fetching, success with a model count, or a short error with the full
message in its tooltip. No model-list polling runs while these views are closed.
The daemon also discovers HTTP models at startup and after connection edits,
subject to the external connection switch. Accepted room hellos update the
room cache and timestamp, including shared-list changes and withdrawals.

Providing runs automatically whenever any enabled Room has `provide = true`,
including after daemon startup and live `config.set ai.providers` changes.
The Sharing tab's room chips control these flags. `mistl ai provide start`
sets `provide = true` on every enabled Room with a nonempty shared list and
prints those room ids; it preserves other rooms' flags. `mistl ai provide stop`
clears `provide` on all Rooms, including disabled ones. Both commands save the
same configuration the dashboard uses. External connections OFF pause sharing
while keeping the room flags saved. Each room advertises only its
`shared` references that resolve to enabled HTTP providers. Room models cannot
be re-shared. Duplicate raw ids select the first usable reference in that room's
shared list.

An inbound named LLM request must match that room's shared models. A nonempty
unshared name is rejected with `model_not_shared` when its shared list is
nonempty, including when its references are currently disabled or unavailable.
An empty model uses an enabled HTTP `default_ref`, otherwise the first usable
shared reference. With no shared list, requests use the configured default.

Rooms referenced by the default, a bot task, TTS/STT, or `provide = true` stay
joined. Other enabled rooms join on demand during model discovery. Disabling or
removing a room releases the AI service's room membership. Other daemon services
may still use the same room independently.

Disabling a provider preserves all model references. A task whose provider is
disabled can use the configured default at runtime if it is usable; otherwise
it errors. A missing provider, missing default or blank model does not select
some other provider or the first cached model.

The local OpenAI-compatible API uses enabled providers' raw model ids in
`GET /v1/models`. Chat requests resolve through `default_ref` and the provider
model caches; Room defaults retain network routing. Voice endpoints use `ai.tts`
and `ai.stt`; a Room voice model can use `network-auto` to defer model selection
to a remote voice provider. The voice protocol does not carry output format,
speed or reasoning effort overrides.

Bot transforms use `model = { provider_id, model }`, optional `reasoning_effort`
for summarize/translate, and optional `voice` for TTS. A missing TTS voice uses
`ai.tts.voice` when the same provider is selected.

## IPC

- `config.set` accepts whole-array `ai.providers`, `ai.default_ref`, `ai.tts`
  and `ai.stt`. Null clears either voice setting. Membership and advertisements
  are applied live. Masked `api_key = "***"` values retain the existing secret
  only when the provider id and endpoint origin match.
- `ai.provide.start` / `ai.provide.stop` edit room flags with the CLI semantics
  above. Their replies include `rooms: string[]` (selected room ids for start;
  previously flagged room ids for stop) and the actual `providing` state.
- `ai.upstream_models { "provider_id": "http" }` returns
  `{ "models": ["example-model"], "live": true }`. An HTTP discovery failure
  returns the cache with `live: false` and optional `error: string`. Room
  discovery joins on demand and waits up to ten seconds for an advertisement;
  success returns live models, while failure returns the cache and error.
  Successful live reads persist the cache and timestamp. Startup discovery and
  dashboard calls share the same deduplication and throttle.
- `ai.status.rooms` is an array of
  `{ "provider_id": "room", "room": "team-room", "enabled": true,
  "joined": true, "providing": true, "peers": 2, "models": ["example-model"] }`;
  `peers` is a count. `models` is null before an accepted advertisement, otherwise
  the current live catalog. The dashboard uses its existing status poll to update
  open lists when room hellos arrive. Top-level `providing` is true iff at least
  one `rooms[].providing` is true; it reports active sharing, not a global switch.

## Migration

`Config::load` migrates legacy upstream fields, presets, default/voice preset
ids, bot preset ids and `advertised_models` once, then saves the current shape.
New default/voice/task assignments win. Task reasoning effort is preserved,
otherwise inherited from the old preset. TTS voice/language settings survive.
An unresolved legacy bot preset remains an unusable reference so it cannot
silently become the default.

Legacy `room_id` creates a Room provider only if its URL does not already
exist. Legacy shared preset ids (or matching raw ids) become that room's shared
references; network presets are excluded. If advertisements exist without a
legacy room, migration uses the compatibility rendezvous room. Any legacy
advertisements enable that room's `provide` flag. Legacy fields remain readable
but are never serialized or accepted by `config.set`. Migration is idempotent.

The retired `ai-provide-state.json` global flag is ignored and removed at daemon
startup, even when it contains invalid JSON. Room flags take precedence over
either legacy flag value; no global provide state is written anymore.
