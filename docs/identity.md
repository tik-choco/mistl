# Identity

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

## DID delegation (root/leaf)

A browser app's `did:key` is scoped to its own origin, so the same person looks
like a different identity in every browser/tab/dev-port. mistl can hold a
permanent **root** identity and sign short-lived delegations to an app's
existing per-origin **leaf** key, so peers that understand the chain treat
`leaf` messages as belonging to `root`. Chain depth is fixed at 1 (root → leaf,
no sub-delegation) and there's no revocation list — delegations are short-lived
(`exp`, recommended 60 days) and reissued rather than revoked. Full spec:
`protocol/docs/data-contracts/docs/did-delegation.md` (shared byte-for-byte
with the TypeScript implementation in mistai).

Two issuance paths:

- **Pairing** (`mistl key pair`, recommended): joins a short-lived,
  code-derived mistlib room and prints a `XXXX-XXXX-XXXX-XXXX` code. Enter
  that code in the browser app's "pair with mistl" flow; once it claims the
  code (MAC-authenticated so only someone who read/typed the code can), mistl
  signs and broadcasts a delegation, then leaves the room (one-shot — the code
  can't be reused). `--ttl` sets the issued delegation's lifetime (default
  60d), `--timeout` how long to wait for a claim (default 5m).
- **Manual transfer** (`mistl key delegate --leaf <did:key> [--ttl 60d]`):
  prints the signed `DelegationV1` JSON directly, to copy/paste (or QR) into
  an app that supports importing one.

`mistl key delegations` lists every delegation this identity has issued as
root, including expired ones.

IPC/dashboard bridge:

- `key.delegate` `{leaf, ttl?}` → `DelegationV1` JSON
- `key.delegations` `{}` → `[DelegationV1 & {expired: bool}, ...]`
- `key.pair.start` `{ttl?, timeout?}` → `{code, formatted_code, room, expires_at}`
- `key.pair.status` `{}` → `{status, code?, leaf?, app?, delegation?, expires_at?}`
- `key.pair.cancel` `{}` → `{cancelled: bool}`
