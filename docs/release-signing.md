# Release signing

The self-updater (`mistl update`, `[update] auto_apply`) downloads the binary
and checks its SHA-256 against `SHA256SUMS.txt` from the same GitHub release.
That alone does not detect a tampered release, so `SHA256SUMS.txt` is signed
with [minisign](https://jedisct1.github.io/minisign/) and the updater verifies
the detached signature `SHA256SUMS.txt.minisig` with a public key compiled into
the binary **before** trusting any checksum.

## Behaviour

- Build has a public key (`MISTL_RELEASE_PUBKEY` set at compile time): a
  missing, malformed or invalid signature (including the trusted comment
  signature) aborts the update. There is no fallback.
- Build has no public key (local/dev builds, or until the key is set up): the
  updater keeps the old unsigned behaviour and logs a warning. `update.check`
  is never affected; only staging a download is.
- Only prehashed (`ED`, the minisign default) signatures are accepted. Do not
  sign with `minisign -l` (legacy).
- The key is pinned for every `update.repo`. A fork releasing from its own repo
  must build with its own key. Non-default repos still never auto-apply.

## One-time setup

1. Generate a keypair (on a trusted machine):

   ```sh
   minisign -G -p mistl-release.pub -s mistl-release.key
   ```

   Choose a password. Keep `mistl-release.key` out of the repository.
2. Repository secrets (Settings > Secrets and variables > Actions > Secrets):
   - `MINISIGN_SECRET_KEY`: the full contents of `mistl-release.key`.
   - `MINISIGN_PASSWORD`: the password chosen above.
3. Repository variable (same page > Variables):
   - `MISTL_RELEASE_PUBKEY`: the second line of `mistl-release.pub` (the
     `RW...` base64 string; the whole file also works).
4. Push to `main`. The release job signs `SHA256SUMS.txt` and uploads
   `SHA256SUMS.txt.minisig`; build jobs embed the public key.

If the secret is absent the signing step is skipped with a warning, and if the
variable is absent the build embeds no key, so CI keeps working while unset.

Local builds can embed a key too:
`MISTL_RELEASE_PUBKEY=RWQ... cargo build --release`.

## Verifying by hand

```sh
minisign -Vm SHA256SUMS.txt -P <public key>
```

## Rotation

Installed binaries only trust the key they were built with, so rotate in two
steps to avoid stranding users:

1. Release a version that is still signed with the OLD key but embeds the NEW
   public key (set `MISTL_RELEASE_PUBKEY` to the new key while
   `MINISIGN_SECRET_KEY` still holds the old one).
2. After users have updated, switch `MINISIGN_SECRET_KEY` / `MINISIGN_PASSWORD`
   to the new key. Later releases are signed with the new key.

If the old key is compromised, there is no safe in-band path: publish a manual
download notice, since anyone holding the old key can sign updates for old
binaries.
