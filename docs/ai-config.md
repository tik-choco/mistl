# AI providers and model references

## External application registrations

Other local applications use the versioned external registration API rather than
reading or changing `config.toml`. The CLI transports JSON unchanged, with the
owner supplied by `--owner` (a JSON owner, if present, must match):

```sh
mistl ai external apply --owner my-app < registration.json
mistl ai external get --owner my-app
mistl ai external get
mistl ai external remove --owner my-app
```

`apply` reads one JSON object from stdin; every command writes JSON to stdout.
Failures return a nonzero exit code and an `error: ...` line on stderr. Global
`--instance` and `--state-dir` select the same daemon instance as other commands.

```json
{
  "providers": [
    {"id":"local","label":"Local HTTP","base_url":"http://localhost:11434/v1","api_key":"","enabled":true}
  ],
  "rooms": [
    {"room":"shared-room","consume":true,"provide":true,"shared":[{"provider_id":"local","model":"example-model"}]}
  ]
}
```

IPC commands and responses:

- `ai.external.apply {owner,label?,providers,rooms}` returns
  `{owner,applied:true,warnings:[...]}`. It replaces that owner's entire registration;
  an identical payload is a no-op, including its update timestamp.
- `ai.external.remove {owner}` returns `{owner,removed:bool}`; repeated removal is safe.
- `ai.external.get {owner?}` returns `{registrations:[{owner,label,providers,rooms,
  updated_at,status:{rooms:[{room,joined,providing,peers,models}]},warnings:[...]}]}`.
  Every provider API key is masked as `"***"`, including empty keys.

Only the token-authenticated local CLI IPC caller can apply registrations.
Dashboard callers, including remote sessions, can get and remove registrations.
The dashboard shows external contributions read-only, with an owner badge and a
confirmed unregister action per owner. Registration commands also work offline;
room activity resumes when network connections are enabled.

Owners must match `[a-z0-9][a-z0-9-]{0,31}`; the display label defaults to the owner.
Each registration accepts at most 64 HTTP/HTTPS providers, 64 rooms and 512 total
shared references. Provider ids and room ids must be unique within the registration.
Room ids use 1–64 characters from `[A-Za-z0-9_-]`. Shared references must identify
providers in the same registration and have nonempty model ids. Validation failure
rejects the entire replacement, preserving the existing registration.

Registrations, including their real keys, persist atomically in the instance's
private `<data_dir>/ai-external.json`; they are never copied into `config.toml`.
At runtime, HTTP provider ids become `ext:<owner>:<id>`. Contributions to a user's
enabled Room are merged with `provide = user OR external` and an ordered union of
shared references (user references first). Owners merge with the same OR/union
rules. A missing Room becomes an enabled effective provider with id
`ext:<owner>:room:<room>`; `consume` keeps the room joined. A user-disabled Room
wins, remains unavailable, and produces a warning in apply/get.

Apply/remove reconcile room membership and advertisements live. `ai.status.rooms`
includes external rooms and an `owners` array. The local `/v1/rooms/{room}/...`
routes accept enabled rooms created solely by external registrations. Removing
one owner leaves other owners' and user contributions intact. An application that
registers again can restore its removed contribution.

## User configuration

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

Chat requests carry the task's `reasoning_effort` through `llm_request`, including
bot transforms and the default local API routes. Providers forward a supplied
string verbatim, overriding their upstream default; absence preserves that
default. `none` is an explicit override, and unknown future values pass through.
Ordinary room chat always uses the streaming LLM protocol, even when the local
HTTP client asks for a non-streaming result.

## Room-scoped local API and OpenAI tunnel

With `mistl ai serve start`, the default base is `http://127.0.0.1:6478/v1`.
Alongside the existing default routes, the following routes select a room
directly, independently of `ai.default_ref`, `ai.tts` and `ai.stt`:

| Method | Route (relative to the base) | Behavior |
| --- | --- | --- |
| GET | `/rooms/{room}/models` | Currently advertised raw model ids in that room; no cached or other-room models |
| POST | `/rooms/{room}/chat/completions` | `llm_request` with the body's `reasoning_effort`; supports JSON and SSE responses |
| POST | `/rooms/{room}/audio/speech` | That room's advertised TTS service |
| POST | `/rooms/{room}/audio/transcriptions` | Multipart upload to that room's advertised STT service |

URL-encode the whole room id as one path segment (for example `team/one` becomes
`team%2Fone`). It must exactly match an enabled `mist-network://<room>` entry in
`ai.providers`; an absent or disabled room returns HTTP 404 with a clear message.
Requests join enabled rooms on demand. An omitted, empty or `network-auto` chat
model defers selection to the remote provider. Voice model/voice/language fields
are forwarded under the existing voice protocol rules.

```console
$ curl http://127.0.0.1:6478/v1/rooms/team-room/chat/completions \
    -H "Content-Type: application/json" \
    -d '{"model":"example-model","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"high","stream":true}'
```

Room providers also advertise `oai` when a usable HTTP target exists. The
mistai-compatible `oai_request` / `oai_response` / `oai_error` tunnel allows only
`POST /chat/completions`, `GET /models` and `POST /embeddings`. It resolves the
model against that room's shared refs with the same rules as LLM requests,
including `model_not_shared`, first-match ordering and empty-model defaults.
Only the resolved provider's API key is used. Temperature is removed and chat
is forced to `stream:false`; upstream status, content type and body are relayed.
Redirects are not followed.

The tunnel base64-encodes the entire body before splitting into 12 KiB character
chunks. Camel-case metadata (`contentType`, plus response `status`) appears on
sequence zero only. Requests and responses are capped at 24 MiB of base64
(18 MiB decoded), with a 120-second deadline. Provider reassembly additionally
limits per-peer/global pending requests and total buffered bytes, expires idle
buffers and drops them on disconnect. Replies are bound to the transport sender.
The local API retains its body limits: 2 MiB JSON and 10 MiB multipart.

Image `content` parts on a room-scoped chat route select a peer advertising
`oai`. The upstream response is buffered; `stream:true` clients receive a local
SSE adaptation once that response completes. Text-only chat still uses
`llm_request`. Local `/rooms/{room}/embeddings` and tunnel consumers for arbitrary
paths are not exposed; mistai consumers can call all three provider tunnel paths.

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
