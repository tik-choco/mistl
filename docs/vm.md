# Virtual machines (`mistl vm`)

A node runs QEMU guests from a pinned cloud image. SSH addresses use a stable **server
id** across boots and moves. Local checkpoints capture a guest's disk. An optional group
replicates those checkpoints to other hosts and elects a host after a failure.

## Requirements

- An x86-64 host and QEMU (`qemu-system-x86_64` + `qemu-img`). mistl looks at
  `[vm] qemu` (file or directory), then `PATH`, then the usual install locations.
- Hardware acceleration:
  - Linux: KVM. The user needs read/write access to `/dev/kvm` (normally via the `kvm`
    group).
  - Windows: WHPX. Enable the "Windows Hypervisor Platform" optional feature if
    `mistl vm doctor` doesn't list `whpx`.
  - macOS: HVF. Not tested yet.
  - Without acceleration QEMU falls back to TCG, which is very slow.
- OpenSSH client tools (`ssh`, `ssh-keygen`). Windows ships them in
  `C:\Windows\System32\OpenSSH`.
- Optional, Linux only: `passt`. When it is installed the guest cannot reach services on
  the host's loopback (see Isolation).

Run `mistl vm doctor` to see what was detected and what is missing.

## Usage

```sh
mistl vm doctor                  # host check
mistl vm create --name web       # first run downloads + verifies the base image
mistl vm ls
mistl vm ssh <id>                # interactive shell as user `mistl` (passwordless sudo)
mistl vm ssh <id> -- uptime      # run one command
mistl vm stop <id>               # ACPI power-off (60 s), then hard stop
mistl vm start <id>
mistl vm logs <id> -n 50         # serial console tail
mistl vm rm <id>                 # stopped VMs only; deletes the disk
```

`create` options: `--memory MB`, `--cpus N`, `--disk GB`, `--ssh-key FILE_OR_LINE`
(repeatable), `--allow DID` (repeatable), `--autostart` and `--no-start`.

Guests need at least 1024 MB of memory and an 8 GB disk; below that cloud-init and apt
run out of memory or space. The disk must also be at least as large as the base image's
virtual size (3.5 GiB for the default image), otherwise its root partition would be cut
off. `create` rejects smaller values.

### Dashboard

The VM page shows VM and HA status, host diagnostics, serial logs, and
checkpoints. The dashboard calls checkpoints "backups": one **Backups** button per VM
opens the list with "Back up now" and a restore action per entry. To create, start, stop,
or remove a VM or take or restore a checkpoint, open the dashboard at its loopback address on the machine running mistl. The
dashboard API also requires a local session to move a VM, manage HA groups, or
disconnect an SSH forward. Dashboard sessions over LAN or a tunnel have
read-only VM access. `vm.key` and `vm.connect` are CLI-only.

### Reaching a VM on another node

1. On the client node, print its key: `mistl vm key`. Get its DID with `mistl key did`.
2. On the executor, start the tunnel, then create the VM with the client's key and DID:
   `mistl vm create --name shared --ssh-key "<client key>" --allow <client DID>`.
   A VM started while the tunnel is running is published as `vm-<id>-ssh`. A VM that was
   already running when the tunnel started is published at its next start.
3. On the client, join the same tunnel room (`mistl tunnel room <room>`,
   `mistl tunnel start`), then: `mistl vm ssh <id> --node <executor node id>`.

Only the VM's owner and the `--allow` DIDs can open `vm-<id>-ssh`. Other peers are
rejected right away, without an approval prompt. The SSH host key is pinned under the
alias `mistl-vm-<id>`, so it follows the server id, not the loopback port.

### Checkpoints

```sh
mistl vm checkpoint <id>
mistl vm checkpoints <id>
mistl vm stop <id>
mistl vm restore <id> --seq 1
mistl vm start <id>
```

A running checkpoint attempts guest `sync` over SSH for up to 10 seconds. QEMU then
pauses while mistl reads the overlay. Each 1 MiB plaintext chunk has a VM-specific
HMAC-SHA256 address and is encrypted with AES-256-GCM. Unchanged chunks are reused.
Zero-filled chunks take no storage. The encrypted manifest includes the VM record, base
image hash, chunk order and pinned guest SSH host keys. `[vm] checkpoint_keep` retains
the newest 1–16 complete manifests per VM (default 3). Restore verifies and reconstructs
the overlay against the exact base image and allocates a local SSH port. The checkpoint
contains disk state. In-memory sessions end when the VM moves or fails over.

### Moving a VM and enabling automatic failover

Choose a new room and set it on every member with `mistl config set vm.room <room>`.
Enable external connections with `mistl network on`. For SSH from another host, start
the tunnel on both nodes in the same tunnel room. Use each member's 16-hex node ID
(`self_id` in `mistl tunnel status`).

VM HA requires Windows or Linux, where QEMU is tied to the daemon's lifetime.
`mistl vm ha enable` and `mistl vm ha join` fail on macOS and other unsupported
platforms. At startup the daemon checks the recorded QEMU PID and process start time
before rejoining a group. If the same process remains, it terminates it and waits for
exit. If it cannot confirm exit, the group stays fenced and the daemon logs the reason.

```sh
# On the current host
mistl vm ha enable <id> --peer <standby-node> --witness <witness-node> --interval 600

# Once on each named standby and witness, using the token printed above
mistl vm ha join <token>

# On the holder
mistl vm checkpoint <id>
mistl vm ha status <id>
mistl vm move <id> --to <standby-node>

# On a group member after the move
mistl vm ls
mistl vm ssh <id>
```

**Keep the join token secret.** It carries the VM's 256-bit encryption and control
authentication key. Transfer it privately and keep it out of shell history. All members,
including witnesses, must trust each other. Hosts also have access to plaintext while
running the guest. The SSH tunnel enforces the owner and allowed DIDs in the replicated
VM record. Guest SSH still requires an authorized key.

- The holder takes a checkpoint every 600 seconds by default. Use `--interval 0` for
  manual checkpoints. Standbys fetch the manifest and missing encrypted chunks. An
  acknowledgement follows verification and complete local storage, including verification
  of reused local chunks. A failed transfer triggers cleanup of unreferenced chunks.
  Witnesses store voting state only.
- `have_seq` is the local complete checkpoint. `committed_seq` is the highest sequence
  confirmed on a majority of **hosts**. Before taking a holder offline, wait for the
  desired sequence to appear in `committed_seq`. The recovery point depends on both the
  checkpoint interval and transfer time. Sequence numbers can jump when the epoch changes
  so a new holder cannot reuse numbers from an isolated host.
- HA retention also keeps `have_seq`, every checkpoint at or above `committed_seq`, and
  up to 16 of the most recently offered manifests for 10 minutes after their latest offer.
  Each retained manifest keeps its referenced chunks available for transfer.
- The holder renews its lease every 5 seconds. A majority of **all members** must
  acknowledge the same round. The lease expires 20 seconds after the round was sent.
  Losing quorum pauses QEMU and withdraws SSH. Renewing quorum in the same epoch resumes
  the guest. A higher epoch fences the former holder and marks its disk stale. A two-node
  group requires both nodes. Two hosts and a witness can elect a surviving host after
  one failure when it holds a sufficiently recent checkpoint.
- A standby waits 45 seconds after its last accepted lease, plus 10 seconds per host
  ahead of it and up to 5 seconds of jitter. Pool hosts are ranked as described below.
  Outside a pool, the configured host order applies. Voters persist their epoch promise
  before replying. A candidate must have a complete checkpoint at least as recent as the
  committed sequence known to each granting voter. At restore time the winner selects
  the newest complete checkpoint on the accepted branch at or above both its voted
  sequence and its committed sequence. It then boots and republishes the SSH target.
- A planned move shuts down the source guest, captures the final disk and waits for the
  target to acknowledge it. A release designates the target for an immediate election.
  The command returns when the target reports running. SSH becomes available after guest
  boot. A failure before release restores the source's previous run intent and permits
  `mistl vm start <id>` while it holds the lease. After release the source stays fenced.
- `mistl vm ha disable <id>` on the holder stops the VM and retires the group. Keep every
  member online for this operation. The holder keeps the local VM. The former standbys
  retain fenced copies that can be removed explicitly with `mistl vm rm <id>`.

`mistl vm ha status <id>` shows `moving` with `to` and `phase` (`stopping`,
`checkpointing`, `replicating`, or `released`), `disabling` with the `pending_peers`
that still owe an acknowledgement, and `last_operation_error` for a failed or
interrupted move or disable. These fields are backed by durable group state.

The maximum group size is 32 members. The VM room is held while an active group uses it
and external connections are on. Control packets are limited to 16 KiB and authenticated
against the transport sender, room, membership and a two-minute timestamp window. Blobs
use reliable direct sends with at most 15 KiB of ciphertext per frame. Both directions
allow at most eight outstanding requests per peer. Each request has a 60-second timeout,
and the 64 MiB aggregate inbound budget includes assembled blobs the receiver is still using.

### Pools

Use a VM pool to share host presence and available CPU, memory, and disk space
among trusted nodes.

```sh
mistl vm pool create       # Create a pool and print its secret invite code
mistl vm pool join <code>  # Run on each additional node
mistl vm pool status       # Show members and their capacity
mistl vm pool invite       # Print the invite code again
mistl vm pool leave        # Leave the pool on this node
mistl vm protect <id>      # Set up replication for a VM on its current host
```

Keep the invite code secret and share it only with nodes you trust. Members
use its shared key to authenticate pool messages and encrypt HA join tokens.
Pool hosts can read the disks of VMs they host. Creating or joining a pool sets
`[vm] room` to the pool room. An active HA group prevents switching away from
its room. Leaving requires retiring all local HA groups and keeps the configured
room. Announcements run while external connections are enabled.

Creating or joining a pool also sets an unset `[tunnel] room_id` to the pool
room. If a different tunnel room is already configured, the response includes
`tunnel_room_hint`. SSH requires the same tunnel room and a running tunnel on
both nodes. Use `mistl tunnel start` to start it.

While the pool room is joined, `mistl vm create` selects an online host with
enough CPU, RAM, disk space and free VM slots for the requested size. If no
node fits, the create falls back to the local node and its own limit checks.
It prefers more
available memory, then CPU headroom, and breaks ties in favor of the local
node. A remote VM belongs to the requesting identity and includes its client SSH
public key. The create output shows the remote host's node ID. When this node
knows the VM's group, `mistl vm ssh <id>`, `vm.connect`, and `mistl agent --vm
<id>` can use the current holder automatically. For a remote VM whose group
this node does not know, specify `--node <host>` (or `node` in `vm.connect`).

After creation, the host tries to protect the VM automatically before its first
boot. The guest then starts once the group holds a quorum lease. If no lease
arrives within a minute, the create reports a start failure. The VM keeps its
group, and `mistl vm start <id>` starts it once enough voters are online. The
host also checks for stopped local VMs without a group every minute. It backs
off failed attempts for ten minutes. Automatic protection skips running VMs,
because enabling HA would pause a guest that has no lease yet. It also skips VMs
whose group was retired with `mistl vm ha disable`. Use `mistl vm protect <id>` on the host to request
protection immediately. It selects up to two eligible standby hosts based on
free disk and adds online HA-capable witnesses to reach an odd number of voters where
possible, with a maximum of five. The command reports which members joined.
The holder starts an initial checkpoint after it obtains a quorum lease.
Check `mistl vm ha status <id>` for the committed checkpoint before relying on
failover. If some joins fail, the configured group stays in place and requires
a voting majority to run. In a two-node pool, both hosts must be online for
quorum.

VM disks are replicated by the group itself (encrypted, deduplicated 1 MiB checkpoint
chunks, a full copy on every host of the group), not by the escrow mesh. The mesh is built
for a bounded number of small signed slots per room; thousands of immutable chunks per VM
would hit its slot caps, keep tombstones forever and replicate about one slot per tick.
The pool reuses only its HRW placement function (`src/placement.rs`) to pick standbys.

For automatic failover, the group ranks its hosts other than the reference
holder by the latest available pool score for that VM's CPU, memory, and disk
size. Higher scores go first. Offline hosts, hosts at their running VM limit,
and other hosts that cannot fit the VM receive no score and go after the scored
hosts. Ties use the configured order. Failback is manual. A recovered node
rejoins as a standby. To move the VM back, use `mistl vm move <id> --to <node>`
or **Move** in the dashboard.

`[vm] max_vms` counts standalone VMs and HA groups this node leads, including
holders restoring a VM. Standby copies and witnesses use no slots. A node can
join additional groups while it has fewer than 1,024 groups loaded. A standby
needs enough free space on the VM filesystem for the checkpoint chunks it does not
already hold. A host at its VM limit does not stand for election, so it cannot take
over above `max_vms`. If every host is at its limit, the VM stays down until a
slot frees up.

### An AI agent with its own Linux computer

`mistl agent --vm <id>` runs the normal agent (same model selection, approval modes and
TUI; `-p` for one prompt) but its workspace is `/home/mistl/work` inside the VM:

```sh
mistl agent --vm <id>                         # Local VM or known HA holder
mistl agent --vm <id> --node <executor>       # Specify a remote executor
mistl agent --vm <id> -p "set up a cron job that ..."
```

- The model runs on the host side and reaches the LLM through mistl as usual. Only the
  tools move: `shell`, background processes and the file tools (`list_dir`, `find_files`,
  `grep`, `read_file`) execute over SSH as user `mistl` (passwordless sudo). The file tools
  use the guest's `python3` and stay inside `/home/mistl/work`.
- Host tools are not offered in VM mode: no `mistl` / `mistl_help` tool and no justfile
  recipes. The system prompt and the auto-mode safety reviewer are told that commands run
  inside a disposable VM, not on the host. Approval modes are unchanged.
- Files, installed packages and services persist with the VM disk, so they survive
  sessions, checkpoints, moves and failovers (up to the last checkpoint after a failure).
  A process the agent started stays only as long as the guest runs; use systemd units or
  cron inside the guest for anything that must survive a reboot or a failover.
- With slirp networking the guest can reach host loopback services (see Isolation). Use
  `[vm] network = "isolated"` or passt for an agent you do not trust with the host.
- Two agent sessions on the same remote VM share one SSH forward; ending one session
  removes it for the other. Start the second session again if that happens.

## What is stored where

`<data_dir>/vm/`:

- `images/<sha256>.img`: base images, verified against `[vm] image_sha256` or the
  upstream `SHA256SUMS`.
- `<id>/vm.json`: the VM record.
- `<id>/disk.qcow2`: an overlay on the base image that holds all guest state, including
  installed packages, `/etc`, `/home`, `/var/lib` and the SSH host keys.
- `<id>/seed/`: cloud-init NoCloud data, given to the guest as a read-only FAT drive
  labelled `cidata`.
- `<id>/serial.log`: the guest's serial console.
- `<id>/vm.key`, `<id>/ckpt/` and `<id>/ha.json` hold the per-VM secret, encrypted
  manifests and chunks, and durable group state. A standby can receive checkpoints
  before its first restore creates a local VM record.
- `ssh/id_ed25519` and `ssh/known_hosts`: this instance's client key and the pinned guest
  host keys.

## Isolation and limits

- QEMU is controlled over QMP on its stdio pipes. There is no monitor or QMP socket a guest
  could reach. Guests get no host folders and no host devices. On Linux QEMU runs with
  `-sandbox on`.
- QEMU processes are tied to the daemon. A daemon stop powers guests down (bounded wait),
  and a daemon crash kills them. After such a crash the guest's ext4 journal recovered and
  data the guest had fsync'd was intact.
- Networking:
  - `passt` (Linux, when installed): the guest has internet access but cannot reach host
    loopback services.
  - `slirp` (Windows/macOS default): **the guest can reach every service bound to the host's
    `127.0.0.1` through `10.0.2.2`**. That includes the mistl dashboard, which still demands
    its session cookie, and other VMs' SSH forwards, which still need their keys. Do not
    host untrusted guests on such a node while unauthenticated loopback services run there.
  - `isolated` (`[vm] network = "isolated"`): no guest egress at all; SSH still works.
- Host limits `[vm] max_vms`, `max_memory_mb`, `max_cpus` and `max_disk_gb` are checked at
  create/start. Stage 1 sets guest size with QEMU `-m` and `-smp` only. There are no cgroup
  or Job Object limits on the QEMU process yet.

## Measured on one Windows 11 machine (WHPX, i7-11700F, 2026-11-21)

| Step | Result |
| --- | --- |
| `vm create` including the 600 MB image download and verification | 29 s; 1 s with the image cached |
| Create to first SSH login | about 36 s |
| `vm stop` (ACPI) | 2 s |
| `vm start` to SSH login | 10 s |
| SSH session setup: local / over tunnel (two instances, same host) | 252 ms / 793 ms |
| 64 MiB stream through SSH: local / over tunnel | 1.5 s (~43 MB/s) / 41.7 s (~1.6 MB/s) |

The tunnel is fine for interactive use but slow for bulk transfers: it reads 4 KiB at a
time, base64-encodes twice and goes through reliable ordered sends. Copy large files
another way until that is improved.

### Move and failover on the same Windows host

Three isolated dev daemons shared one room. A and B hosted a 1 GiB guest with an 8 GiB
virtual disk. C voted as a witness and created no checkpoint directory. Both hosts had
the same verified base image cached. The following times are from one end-to-end run
on 2026-11-21 using WHPX and manual checkpoints.

| Operation | Result |
| --- | --- |
| Initial running checkpoint on A | Guest paused 5.47 s (before the change below) |
| Planned move from A to B, command start to target reporting running | 52.67 s, including 16.09 s for restore |
| B QEMU launch to SSH marker verification | 14.26 s |
| B daemon hard kill to A winning epoch 3 | 45.15 s |
| A restore after election | 19.70 s |
| A QEMU launch to SSH verification of B's second marker | 20.70 s |
| B daemon hard kill to successful SSH marker check on A | 85.95 s |

| Checkpoint transfer | Missing chunks | Encrypted chunk data | Receiver time | Effective throughput |
| --- | ---: | ---: | ---: | ---: |
| Initial A → B | 28 | 27.25 MiB | 11.86 s | 2.30 MiB/s |
| Final A → B during the move | 43 | 42.88 MiB | 18.11 s | 2.37 MiB/s |
| B → A after writing the second marker | 28 | 27.50 MiB | 12.41 s | 2.22 MiB/s |

The guest pause first grew with every new chunk (18.8 s for 97 new MiB) because chunks
were encrypted while QEMU was stopped, and AES-GCM is very slow in debug builds. New
chunks are now staged as plaintext during the pause and encrypted and synced after the
guest resumes: a 323 MiB overlay with 130 new chunks paused the guest for 409 ms (debug
build; SHA-256 is optimized in dev builds via `[profile.dev.package.sha2]`).

Receiver time includes the manifest request, missing-chunk requests, verification and
local persistence. The final transfer completed and reached the host majority before B
was killed. SSH from B locally and from A over the tunnel both found the first marker
after the move. A had no guest process at that point. B's guest exited with its daemon.
When B rejoined after failover, it learned epoch 3 and remained stale with no guest or
SSH Serve forward.

A second run used the build with the review fixes (paused HA boot, lease_ok renewals,
stale recovery, intersecting commit quorum). The same three daemons were restarted on
it, with the guest overlay at 361 MiB:

| Operation | Result |
| --- | --- |
| Former holder restarted after a failover | rejoined as `standby` (stale cleared by the holder's checkpoint), no guest |
| Running checkpoint, 64 new chunks | guest paused 422 ms; `committed_seq` reached it after ~70 s |
| A daemon hard kill → B elected | ~46 s |
| B restore of the 361 MiB overlay | 93.4 s (debug build: AES-GCM is unoptimized there) |
| A daemon hard kill → SSH marker check on B | 153 s |
| Planned move B → A, command start to running | 161 s, then SSH OK |

Restore and transfer times above are from debug builds and are dominated by unoptimized
AES-GCM and by 15 KiB frames on reliable direct sends. Release builds and larger frames
are expected to be much faster but have not been measured yet.

Not verified yet: the Linux/KVM/passt path through mistl (QEMU + passt were only tested by
hand in WSL2), macOS/HVF, and two physical machines.
