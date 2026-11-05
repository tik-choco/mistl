# Authorization (`mistl authz`)

mistl decides locally who may use the services this node provides. Each
node keeps its own policy; nothing is synchronised between nodes.

With no policy configured, every service behaves exactly as before:
**access is open by default**, and nothing becomes stricter until you
configure it.

State lives in `<data_dir>/authz/` (`policy.json`, `tokens.json`,
`usage.json`, `issued.json`, `held.json`, `revoked.json`) and is written
with private file permissions. Manage it with `mistl authz ...` or the
dashboard's **Access** panel.

## Who is asking

A remote request is attributed to the transport node id it arrived from (16
hex characters). A node id is `sha256(did)[..16]`, so it maps 1:1 to the
peer's `did:key`. The DID is known only after the peer's signed DID hello
has been verified (`net::peer_auth`).

- An **allow** rule (DID or node id) matches only a peer whose DID is
  verified. A node id alone never grants access.
- A **deny** rule matches a node id even when it is not verified, or a
  verified DID. Deny always wins.
- Rules can expire (`--ttl 7d`, `--expires <RFC3339>`).

## Stage 1 — access control

```
mistl authz mode ai allowlist          # only allowed peers may run inference
mistl authz allow add ai did:key:z6Mk... --ttl 7d
mistl authz deny add escrow 0123456789abcdef
mistl authz limits escrow --max-stored 2GiB             # per-node storage cap
mistl authz limits escrow --subject did:key:z6Mk... --max-stored 10GiB
```

- **AI provider**: every request type (chat, TTS, STT, OpenAI passthrough)
  is checked before the existing per-peer concurrency limiter. A refused
  request gets an error reply whose `code` is `forbidden` or
  `usage_limit_exceeded`; direct hello replies are not sent to peers that are
  not allowed.
- **Escrow**: client `escrow_get` (read) and `escrow_put` / `escrow_delete`
  (write) are checked; `escrow_hello` stays open. Traffic between storage
  providers (mesh replication, Raft clusters) is not affected. The
  per-node cap counts the bytes that a node uploaded directly; replicas
  pulled from other providers, and records stored before this feature, count
  only toward the global `escrow.max_total_bytes`. Over-cap uploads are
  refused with the existing `quota` code.

## Stage 2 — API tokens and usage limits

The local OpenAI-compatible API (`ai.api_listen`) accepts `mst_...` bearer
tokens:

```
mistl authz token create laptop --model gpt-4o-mini --max-requests 500 --period day --ttl 30d
mistl authz api require-token on       # refuse requests without a live token
mistl authz token revoke <id>
```

- The secret is printed once and only its SHA-256 hash is stored.
- `require_token` is off by default. When it is off, requests without a
  token, or with a non-`mst_` key such as `sk-dummy`, keep working, but an
  unknown, revoked or expired `mst_` token is refused. Turn it on whenever
  `ai.api_listen` is reachable from other machines; the daemon logs a
  warning if it is not.
- A token can restrict models (`GET /v1/models` is filtered, other models
  get `403 model_not_allowed`) and usage (`429 usage_limit_exceeded`).
  A missing or invalid token gets `401`.

Cumulative usage limits (`--period day|month|total`, `--max-requests`,
`--max-output-chars`) apply per peer node for P2P inference and per
token for the API. They are separate from the concurrency limits, which are
unchanged. Periods use UTC. `mistl authz usage` shows the meters. To bound
memory, at most 4096 accounts are tracked; any account beyond that shares
the overflow meter `*`, which is checked against the default limits.

## Stage 3 — delegation with signed capabilities

A capability is a signed JSON grant from an issuer DID to a holder DID
(`aud`):

```json
{"t":"mistl-cap-v1","id":"<32 hex>","fromId":"did:key:<issuer>","aud":"did:key:<holder>",
 "svc":"ai","act":["infer"],"res":"*","nbf":1700000000000,"exp":1700604800000,
 "lim":{"period":"day","max_requests":100},"to":["did:key:<provider>"],"signature":"..."}
```

```
# Alice (the AI provider) lets Bob use her AI for 7 days, at most 100 requests a day
mistl authz cap issue did:key:<bob> ai --ttl 7d --max-requests 100 --deliver
# Alice lets Bob read one escrow slot she wrote
mistl authz cap issue did:key:<bob> escrow --action read --resource <room>/<slot> --deliver
# Bob passes a narrower part of his capability on to Carol
mistl authz cap issue did:key:<carol> ai --ttl 1d --parent <held id>
mistl authz cap revoke <id>
```

- The holder's node presents its capabilities to the providers they name
  (`to`, or else the root issuer) with `mistl-cap-present-v1`. The
  provider accepts a capability only if `aud` is the presenting peer's
  verified DID.
- Its root issuer must be trusted by the provider. A trusted root is one of:
  - the provider's own DID;
  - a DID in the service's issuer list (`mistl authz issuer add ai <did>`);
  - for an exact escrow slot, the DID that signed the slot's current head.
- Each link of a delegation chain (at most 3 links) must narrow the one
  before it: the same service, a subset of its actions and resources, an
  expiry no later than the parent's, and limits no looser than the parent's.
- The access order is: deny, then allow rule, then a valid capability, then
  open mode. A capability's `lim` is metered on its own account in addition
  to the node's limits.
- **Revocation**. `mistl authz cap revoke <id>` on the issuing node
  broadcasts a signed `mistl-cap-revoke-v1` right away and again every 5
  minutes until the capability expires. Providers accept a revocation only
  from a trusted issuer or from an issuer in a chain they hold.
  `--local` revokes a capability on this provider only, effective
  immediately and independent of the issuer.
- An issuer may push (`--deliver`) at most 16 capabilities to a holder,
  and only from its own node.

## Not covered (stage 4)

Usage meters and policies are per node. Limits are not aggregated across
several providers, and grants and revocations are not synchronised by
consensus. Revocation reaches offline providers only through the issuer's
periodic re-broadcast, or through `--local` on that provider.
