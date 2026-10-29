# Vault

Vault keeps encrypted copies of each member's files across a storage group. A
single node works with a local copy; adding nodes lets the group maintain more
copies without a central server. Each member has its own dataset and writes its
own files. Holders can see ciphertext and metadata such as dataset sizes and
file counts, but do not receive the keys needed to decrypt another dataset.

The recovery code is the only way to decrypt and restore a dataset on another
node. Save it offline somewhere safe when creating or joining a group. Anyone
with the code can read and write that dataset. Losing the code and the original
node makes the encrypted copies unusable; a recovery code also cannot recover
data if every copy has been lost.

## Get started

Enable network connections on two or three nodes, then on node A:

```console
$ mistl vault init              # save the recovery code offline
$ mistl vault invite            # give this one-use code to node B
```

On node B, join with that code and save B's own recovery code offline:

```console
$ mistl vault join <INVITE_CODE>
```

To add node C, create another invite on a group member and join on C. Invites
expire after 24 hours and can be used once. Then on A:

```console
$ mistl vault put ./report.pdf
$ mistl vault ls
$ mistl vault status
```

The dashboard has the same file operations under **Store → Vault**. Creating
or joining a group, inviting members, and showing a recovery code require the
CLI or a dashboard opened on the daemon's machine. The CLI's `--json` flag
prints the daemon's response as JSON, including recovery codes when requested.

## Copies and recovery

Status reports each owner's confirmed `k/target` copies. `local_only` means a
single local copy; `replicating` means copies are being made; `healthy` means
the target is confirmed. `degraded` indicates missing copies, `conflict` a
conflicting owner history, and `insufficient_capacity` a lack of storage.
Confirmation follows signed member reports and can lag while nodes sync.
The target includes the owner's copy and is limited by the eligible members.

An offline node's existing copy is not immediately replaced. After it has been
unseen for `dead_after_secs`, placement excludes it and eligible online members
can make replacement copies, subject to capacity and available source copies.
Writes and reads of locally available files continue with the network disabled;
replication resumes when the network returns. Replication is not instantaneous,
so check status before relying on another copy or shutting down a node.

To restore a dataset on a new machine, first join the same group using an invite
from an active member, then run:

```console
$ mistl vault restore <RECOVERY_CODE>
$ mistl vault sync
$ mistl vault status
$ mistl vault ls
$ mistl vault get report.pdf -o ./report.pdf
```

Restoring adopts that dataset's owner key. A surviving group member must still
have its encrypted data, and the new node must be able to reach it. Avoid writing
the same dataset from multiple nodes at once: it has a single writer and
conflicting histories are reported in status.

## Limits and configuration

Files default to a maximum of 4 GiB, configurable up to the object format's
8 GiB limit. Names must be 1–255 characters, cannot be `.` or `..`, and cannot
contain slashes, backslashes, NUL, or control characters. Groups support up to
64 active members, 64 owners, and 256 membership entries. The encrypted catalog
is limited to 4 MiB. Dashboard uploads stream to the daemon (up to the file
size limit); downloads run at most two at a time.

## Membership

Members are admitted by signed entries that every member gossips; a member is
valid if a chain of admits leads back to the group's creator. `mistl vault
remove-member <DID>` signs a permanent revocation (a revoked DID cannot be
admitted again) and `mistl vault leave` revokes yourself. Revoked members stop
receiving blocks and their messages are ignored once the revocation has reached
each node.

The group trusts its members. A revocation takes effect node by node as it
spreads, so a removed member that still holds its key can, until then, get
entries accepted through a node that has not yet heard of the revocation.
Concurrent revocations of each other remove both members. If a group's member
list is compromised, create a new group and restore datasets into it with their
recovery codes.

Set these values in `config.toml` (changes apply after daemon restart):

```toml
[vault]
replicas = 3                  # target copies, including the owner; 1–8
capacity_bytes = 10737418240   # total vault capacity, including own data
min_free_bytes = 1073741824   # free space to leave on the volume
max_object_bytes = 4294967296 # maximum file size
dead_after_secs = 86400       # replace a member after this long unseen
```

Group membership, pending invites, the owner key, and blocks live under the
daemon's data directory in `vault/`; they are not stored in configuration.
See [configuration](configuration.md) and the [CLI reference](cli.md).
