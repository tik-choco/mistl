# Service API v1

Use the Service API to connect your own billing or membership backend to a
mistl node that you operate. mistl enforces access and usage limits for AI
and escrow, issues consumer credentials, and exposes usage meters and events.
Your app owns customer accounts, pricing, payments, subscriptions, invoices,
and aggregation across multiple nodes. Each node's policy and usage are local.

## Setup and authentication

On the provider node, start the daemon with the dashboard listener enabled.
Create a management credential locally:

```console
mistl authz token create billing --scope manage
mistl authz mode ai allowlist
```

Use `mistl authz token create metering --scope usage` instead for a read-only
metering worker. `manage` implies `usage`; `infer` is for consumers of the
OpenAI-compatible AI API and does not grant control-plane access. A token can
have multiple scopes when created locally, but this API can create only
infer-only tokens. Management credentials should stay on your backend, not in
customer applications or browser code. Keep the returned `mst_` secret in
your secret store; mistl shows it only at creation.

The base URL is the dashboard listener address reported by `mistl daemon
status`, **without** the dashboard login query token. For example, if the
dashboard listens on `127.0.0.1:8080`, the API base is
`http://127.0.0.1:8080/service/v1`. Use loopback, a TLS reverse proxy, or an
appropriately configured mistl tunnel; never send credentials over plain HTTP
across the internet. The existing external-connections switch still applies.

Send the scoped token on every request:

```sh
curl -H "Authorization: Bearer $MISTL_SERVICE_TOKEN" \
  "$MISTL_SERVICE_URL/info"
```

No dashboard cookies, instance headers, or browser CORS access are supported.
POST, PUT and PATCH require a JSON object, `Content-Type: application/json`,
and `Content-Length`, with at most 65,536 bytes. For an empty grant use `{}`.
Success responses are the JSON result directly, with HTTP 200 or 201 and
`Cache-Control: no-store`. Error responses have the same no-store policy:

```json
{"error":{"code":"scope_required","message":"API token lacks the required scope"}}
```

| Status | Code | Meaning |
| --- | --- | --- |
| 400 | `invalid_request` | Invalid JSON, arguments, path/query parameters, or command failure |
| 401 | `unauthorized` | Missing, invalid, revoked or expired bearer token |
| 403 | `scope_required` | Live token without the required scope |
| 404 | `not_found` | Unknown path or unavailable token ID |
| 405 | `method_not_allowed` | Unsupported method (including OPTIONS) |
| 411 | `length_required` | Missing Content-Length |
| 413 | `payload_too_large` | Body larger than 64 KiB |
| 415 | `unsupported_media_type` | Missing or wrong Content-Type |
| 500 | `internal_error` | Unexpected server result |

Authentication comes before routing errors. An unauthenticated call to an
unknown Service API path is also refused. The host's external-connection
policy may reject a request before it reaches this API.

## Endpoints

All paths below are relative to `/service/v1`. `GET` on info, usage and usage
events needs `usage` (or `manage`); all other endpoints need `manage`.
Examples use illustrative IDs, DIDs, times and models. `<token-id>` denotes
a token ID, not a bearer secret. Substitute your own real IDs and credentials.
JSON responses show representative results.

### Node information and usage

**GET `/info`** — discover the provider identity, access modes, and whether
the separate OpenAI-compatible API requires consumer credentials.

Request: `GET /service/v1/info`

```json
{"api":1,"did":"did:key:<provider>","node_id":"0123456789abcdef","services":{"ai":{"mode":"allowlist"},"escrow":{"mode":"open"}},"require_token":true}
```

**GET `/usage`** — current cumulative meters. Optional `service=ai` or
`service=escrow` restricts which meters are populated; without it both
services are returned. AI meters are keyed by account (node, token or
capability); escrow rows report owned stored bytes. Counters are not invoice
totals: they follow each configured limit period (UTC).

Request: `GET /service/v1/usage?service=ai`

```json
{
  "ai":[{
    "account":"token:<token-id>","token":"<token-id>","name":"customer",
    "period":"day","period_start":"2026-11-27T00:00:00Z",
    "requests":7,"output_chars":800,
    "limits":{"period":"day","max_requests":100,"max_output_chars":null,"max_stored_bytes":null}
  }],
  "escrow":[]
}
```

An escrow usage row has `node`, `did` (possibly null), `stored_bytes` and
`limit` (possibly null).

**GET `/usage/events`** — incremental request-level usage. Query parameters:
`after` is an unsigned 64-bit sequence number (default 0); `limit` is 1–1000
(default 100). There is no service filter on this endpoint; filter events in
your backend.

Request: `GET /service/v1/usage/events?after=42&limit=100`

```json
{
  "events":[{
    "seq":43,"at":"2026-11-27T12:00:00.123Z","service":"ai",
    "action":"infer","op":"chat","token":"<token-id>",
    "model":"example-model","requests":1,"output_chars":120
  }],
  "next":43,"oldest":1,"gap":false
}
```

### P2P grants

A grant combines an allow rule and optional per-subject limit override.
`service` must be `ai` or `escrow`; a subject is a verified Ed25519 `did:key`
or its 16-character lowercase hex node ID. Percent-encode the subject as a
path segment (for example `did%3Akey%3A...`). Grant access applies to a
verified peer, and explicit deny rules still win. Configure the service's
mode to `allowlist` if customers should need a grant; open mode permits
unlisted customers.

**PUT `/grants/{service}/{subject}`** — create or replace a grant. Only
`expires_at` (RFC3339), `ttl_secs` (seconds), `note`, and `limits` are accepted.
Provide at most one expiry form; omit both for no expiry. Missing or null
`limits` removes the override. A PUT with `{}` replaces the grant with a
non-expiring allow and no override or note. Limits accept `period`
(`day`, `month`, `total`), `max_requests`, `max_output_chars`, and
`max_stored_bytes`; a null/omitted maximum is unlimited in that override.

Request: `PUT /service/v1/grants/ai/did%3Akey%3A<customer>`

```json
{"ttl_secs":86400,"note":"subscription","limits":{"period":"day","max_requests":100}}
```

Response (200):

```json
{"service":"ai","subject":"did:key:<customer>","expires_at":"2026-11-28T12:00:00Z","note":"subscription","limits":{"period":"day","max_requests":100,"max_output_chars":null,"max_stored_bytes":null}}
```

**GET `/grants`** — active allow rules with their overrides. Optional
`service=ai` or `service=escrow` filters the list.

Request: `GET /service/v1/grants?service=ai`

```json
[{"service":"ai","subject":"did:key:<customer>","expires_at":null,"note":null,"limits":null}]
```

**DELETE `/grants/{service}/{subject}`** — remove both allow rule and limit
override; repeat safely if the grant is already absent.

Request: `DELETE /service/v1/grants/ai/did%3Akey%3A<customer>`

```json
{"removed":true}
```

### HTTP consumer tokens

The API only lists and modifies tokens whose scopes are exactly `["infer"]`.
Other tokens (including mixed-scope tokens) are indistinguishable from
unknown IDs here; creating or administering usage/manage tokens remains a
local CLI/dashboard operation.

**POST `/tokens`** — create a consumer credential. Allowed keys: `name`
(required), `models` (empty/omitted = no restriction), `limits`, `expires_at`,
`ttl_secs`. `scopes` is not accepted; all issued tokens have scope `infer`.
The expiry and limits fields follow the grant conventions.

Request: `POST /service/v1/tokens`

```json
{"name":"customer","models":["example-model"],"limits":{"period":"month","max_requests":1000},"ttl_secs":2592000}
```

Response (201):

```json
{
  "token":"mst_<secret>",
  "info":{
    "id":"<token-id>","name":"customer","hint":"mst_abcd…",
    "created_at":"2026-11-27T12:00:00Z","expires_at":"2026-12-27T12:00:00Z",
    "revoked_at":null,"last_used_at":null,"models":["example-model"],
    "limits":{"period":"month","max_requests":1000,"max_output_chars":null,"max_stored_bytes":null},"scopes":["infer"]
  }
}
```

Save the ID to associate usage with the customer. Give the secret to the
customer securely; subsequent API responses cannot recover it.

**GET `/tokens`** — list inference-only token metadata (including revoked
and expired credentials). No secret or hash is included.

Request: `GET /service/v1/tokens`

```json
[]
```

When tokens exist, each element has the same fields as `info` above.

**PATCH `/tokens/{id}`** — update metadata, model restrictions, limits, or
expiry. Accepted keys: `name`, `models`, `limits`, `expires_at` and
`ttl_secs`. Omitted fields are unchanged; a supplied limits object replaces
the token's limits. `expires_at: null` (without a TTL) removes expiry. Scopes
cannot be changed, and the secret remains the same.

Request: `PATCH /service/v1/tokens/<token-id>`

```json
{"ttl_secs":604800,"models":["example-model"]}
```

Response (200):

```json
{"id":"<token-id>","name":"customer","hint":"mst_abcd…","created_at":"2026-11-27T12:00:00Z","expires_at":"2026-12-04T12:00:00Z","revoked_at":null,"last_used_at":null,"models":["example-model"],"limits":{"period":"month","max_requests":1000,"max_output_chars":null,"max_stored_bytes":null},"scopes":["infer"]}
```

**DELETE `/tokens/{id}`** — revoke, not erase, a consumer token.

Request: `DELETE /service/v1/tokens/<token-id>`

Response (200):

```json
{"id":"<token-id>","name":"customer","hint":"mst_abcd…","created_at":"2026-11-27T12:00:00Z","expires_at":"2026-12-04T12:00:00Z","revoked_at":"2026-11-27T12:30:00Z","last_used_at":null,"models":["example-model"],"limits":{"period":"month","max_requests":1000,"max_output_chars":null,"max_stored_bytes":null},"scopes":["infer"]}
```

Future inference requests using that secret are refused.

### Portable grants (signed capabilities)

A capability is a signed grant bound to the holder's DID, service, action
and resource. See [Authorization](authorization.md#stage-3--delegation-with-signed-capabilities)
for trust, presentation, and delegation rules.

**POST `/caps`** — issue a capability from this node. Required keys: `aud`
(holder DID), `service`, `actions` (array: `infer` for AI, `read`/`write`
for escrow). Optional keys: `resource` (default `*`; exact escrow resource
is `<room>/<slot>`), `ttl_secs` or `expires_at` (default expiry seven days),
`limits`, `to` (array of provider DIDs; defaults to the root issuer), and
`deliver` (boolean, attempt P2P delivery). The Service API issues root
capabilities; parent delegation is a local command.

Request: `POST /service/v1/caps`

```json
{"aud":"did:key:<customer>","service":"ai","actions":["infer"],"ttl_secs":86400,"limits":{"period":"day","max_requests":100},"deliver":true}
```

Response (201):

```json
{
  "cap":{"t":"mistl-cap-v1","id":"0123456789abcdef0123456789abcdef",
    "fromId":"did:key:<provider>","aud":"did:key:<customer>","svc":"ai",
    "act":["infer"],"res":"*","nbf":1795780800000,"exp":1795867200000,
    "lim":{"period":"day","max_requests":100},"signature":"<signature>"},
  "delivered":false
}
```

`delivered` indicates whether delivery was sent, not a guarantee of receipt.
If it was not sent, pass the unchanged signed `cap` object to the holder to
import locally. The holder must present it to the named provider(s).

**GET `/caps`** — lists issued, held, presented and revoked capabilities.
For managing customers, use `issued`, whose entries contain `cap` (the
signed object from POST) and `revoked` (boolean).

Request: `GET /service/v1/caps`

```json
{"issued":[],"held":[],"presented":[],"revoked":[]}
```

**DELETE `/caps/{id}`** — revoke an issued capability on the issuing node
and attempt to broadcast the revocation. This is not local-only revocation;
online recipients can learn it through the broadcast, and disconnected
providers through periodic re-broadcast until expiry.

Request: `DELETE /service/v1/caps/0123456789abcdef0123456789abcdef`

```json
{"revoked":true,"broadcast":true}
```

`broadcast: false` means no immediate broadcast was sent, not that the local
revocation failed.

## Usage event schema and cursor

Events are recorded only while at least one **live usage or manage token**
exists. Create your metering credential before accepting customers, keep a
live credential throughout service, and poll continuously. Anonymous
requests with neither a principal nor a token have no event. The event feed
records actual usage separately from the cumulative limit counters.

| Field | Meaning |
| --- | --- |
| `seq` | Monotonically increasing per-node sequence, recovered on restart |
| `at` | RFC3339 UTC timestamp with milliseconds |
| `service` | `ai` or `escrow` |
| `action` | `infer`, `read` or `write` |
| `op` | Operation such as `chat`, when available |
| `node`, `did` | Transport peer's node ID and verified DID, when available |
| `token` | API token ID, never its secret |
| `caps` | IDs of capabilities used, if any |
| `model` | AI model identifier, when available |
| `requests` | Number of requests recorded |
| `output_chars` | AI output character count, when recorded |

`op` values: AI `chat`, `tts`, `stt`, `oai` (OpenAI passthrough over P2P),
`image_edit`, `gen3d`, `game`, `motion`, `decision`, `voices`, `status`;
escrow `get`, `put`, `delete`. Escrow events carry no sizes: bill storage by
sampling `stored_bytes` from `GET /service/v1/usage?service=escrow`.

Fields without a value are omitted. Keep the provider identity with the
cursor: sequences are unique **per node**, not globally. Start at `after=0`;
the feed is ordered by sequence and returns events strictly after the cursor.
Persist and deduplicate events by `(provider node ID, seq)` and atomically
advance the saved cursor to `next` only after processing the page. Repeat
until `events` is empty; on an empty page `next` equals the input `after`.

`oldest` is the oldest retained sequence (null before any events). `gap: true`
means some events after the requested cursor were dropped. Alert the billing
worker and reconcile separately rather than assuming the missing usage was
zero. The daemon keeps `<data_dir>/authz/events.jsonl` and one previous file,
`events.1.jsonl`, rotating at 8 MiB; older events are discarded. The log is
appended without an fsync on each event, so a crash may also interrupt
recording. It is not a durable financial ledger; persist the ingested events
in your own database and account for gaps and recording outages.

## Integration flows

### Customer with a mistl P2P node

1. Collect and verify the customer's DID in your application and associate
   it with their membership. Set the provider's relevant service to allowlist
   mode, e.g. `mistl authz mode ai allowlist`.
2. Upon entitlement, `PUT /grants/ai/{customer-did}` with the expiry, note,
   and requested limits. To provide a portable signed grant instead, use
   `POST /caps` with the customer's DID as `aud` and the intended service,
   actions and providers; deliver or pass the signed capability to the holder.
3. The customer connects to the AI provider or escrow service through mistl
   P2P. Mistl authenticates the peer and enforces its policy.
4. Poll `/usage/events`, attributing transactions by DID/node or cap ID.
   At renewal, PUT a replacement allow rule or issue a new capability;
   at cancellation, DELETE the grant or the issued capability (and any
   other independent grants for that customer, as appropriate).

### Customer using the OpenAI-compatible HTTP API

1. Start the AI API (`mistl ai serve start`), configure its own listener
   (`ai.api_listen`), and run `mistl authz api require-token on` to require
   consumer tokens. This is separate from the dashboard Service API address.
2. On entitlement, `POST /tokens` with a customer name, allowed models
   (e.g. `example-model`), limits and expiry. Store the returned `info.id`
   and hand the one-time `mst_` secret to the customer securely.
3. The customer uses that secret as the API key with the OpenAI-compatible
   API base URL (e.g. `http://127.0.0.1:6478/v1` on the provider), not the
   Service API. Send chat requests to `/v1/chat/completions` on the AI
   listener. Protect the AI listener with loopback, TLS, or a mistl tunnel too.
4. Poll `/usage/events` with the backend's usage/manage credential, matching
   the event `token` ID to the customer. For renewals call `PATCH /tokens/{id}`;
   for cancellation call `DELETE /tokens/{id}`.

See [Authorization](authorization.md) and [AI configuration](ai-config.md)
for the underlying policy and service configuration.
