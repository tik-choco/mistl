# Personal Linux servers on mistl — feasibility study

Status: investigation only (2026-11-20). Nothing in this document is implemented.
Reviewed: mistl `develop` @ `e5a1915`, mistlib v0.7.3-beta.6 (`1ec9295`),
mistlib-consensus `e86579a`. Claims about existing behaviour come from source review;
no runtime measurement was done for this study.

## Goal under evaluation

Each user gets what behaves like a dedicated Linux server: SSH access, root inside the
VM, installed packages and daemons persist. Disk state is stored encrypted in mistl's
replicated storage. If the executing node dies, another node fetches the committed disk,
obtains the key without the owner's PC or the old executor, and cold-boots the same
system. The server id and volume are independent of the physical node.

## Verdict

Feasible as an MVP for a **small, trusted, permissioned cluster** (3 consensus voters,
2+ Linux/KVM executors), with a cold-restart failover contract. It is not a small
feature: the reusable parts (durable Raft, encrypted CID chunks, DID auth, TCP tunnel)
cover ordering, transport and identity, but the block volume, per-VM epoch fencing,
key custody and server-id routing are all new. The largest unknown is volume write
performance over the current escrow path; measure it before committing to a build
(see "Phase 0").

Explicit non-goals for the first version: confidential execution (the provider is
trusted), live migration, tolerance of malicious cluster members (Raft here is
crash-fault only), public ingress.

## What exists and what is missing

| Need | Existing | Gap |
| --- | --- | --- |
| Durable replicated commit | Escrow Raft (`src/escrow/replication/`): ACK only after a durable majority of voters has staged the ciphertext and the head is applied; disconnects never remove voters; joint membership changes; snapshots | Whole-object PUT only (default 32 MiB/generation, 30 s pull deadline `src/escrow/transfer.rs`, sequential 1 MiB block pulls, per-room leader request mutex, whole-log JSON persistence). No block map, no extent closure check, no group commit |
| Writer fencing | Per-slot Ed25519 writer pin + `parent`/`seq+1` CAS applied in Raft order | No epoch / executor grant. A stale executor holding the writer key can still sign a valid successor. Writer cannot be reassigned |
| Encrypted chunks | CIDv1 sha2-256 chunk store, AES-256-GCM envelope (`src/storage/crypto.rs`) | Ordinary store is a cache (evictable, no replica promise) — only escrow's archive is authoritative |
| Identity / authz | did:key, room-scoped `peer_auth`, signed capabilities with attenuation and revocation notices (`src/authz/`) | No VM/volume resource, no Execute/VolumeWrite/KeyFetch actions, no binding to committed epoch. Revocation is best-effort until expiry |
| Key handover | Per-recipient ECDH grant pattern (folder share), private statefiles | No custodian, no recipient key registry, no threshold scheme |
| SSH transport | Tunnel TCP Serve/Connect forwards carry OpenSSH byte-for-byte | Forwards bind to a physical peer or an unscoped advert; no "server id → current executor@epoch" resolution; 4 KiB reads + double base64 — throughput unmeasured |
| Executor | — | Entire VM runtime (QEMU process mgmt, cgroups, images, NBD backend) |

Relay consensus (`src/consensus/`) is not suitable for VM placement: it is in-memory
and drops peers after 15 s of silence, which would let a partition elect two owners.

## Design outline

### Roles (kept separate)

- **Control cluster**: a fixed-voter escrow-style Raft group per cluster (3 voters).
  Holds per-VM records: `server_id, owner DID, network id, volume_id, executor DID,
  epoch, committed root, key generation, resource limits`.
- **Storage holders**: Raft voters/learners that durably hold ciphertext extents.
- **Executors**: Linux hosts with `/dev/kvm` that run VMs assigned to them. Placement is
  per VM; no node runs everything.
- The Raft leader, the storage leader and each VM's executor are different roles even if
  one machine holds several.

### Runtime

QEMU + KVM, stock Ubuntu LTS cloud image, cloud-init NoCloud, virtio-blk over QEMU's
built-in NBD client, user-mode networking (slirp/passt) so the VM never gets host
devices, the VMM API, or a host TAP. cgroup v2 for CPU/memory/pids; QEMU throttling for
disk. Firecracker was rejected for v1 (separately managed kernel, no qcow2/NBD, more
image work); Cloud Hypervisor is a later candidate. Wasm/WASI does not meet the
"normal Linux server" requirement.

Executor support: Linux x86-64 with KVM. WSL2 nested KVM works on the development
machine and is a dev target only. Windows/macOS daemons keep all existing features and
may act as storage holders, voters and SSH clients, but not executors.

### Volume

Base image pinned as immutable ciphertext + a sparse copy-on-write block map of
encrypted extents (e.g. 1–4 MiB), committed as a small root in Raft. The executor runs a
local NBD server with an fsync'd local journal.

- `NBD_CMD_WRITE` → local journal (volatile from the distributed point of view).
- `NBD_CMD_FLUSH` / FUA → upload dirty extents, wait until every newly referenced
  extent is durably held by a storage quorum, then Raft-CAS the root on
  `(volume_id, epoch, expected_root)`; only then reply. This is the definition of
  "persisted": survives loss of the executor and of a minority of storage voters.
- Writes acknowledged but not covered by a successful flush may be lost on failover.
- An optional relaxed mode (flush = local journal fsync, async upload) is the same code
  with a different reply point, and must be reported as RPO > 0.

Without quorum, flushes block/fail and no failover happens.

### Placement and fencing

Failover = Raft command `Assign{server_id, new_executor, epoch+1}` committed by the
majority after the lease expires. Every root commit checks the epoch at apply time, so a
partitioned or returning old executor cannot change the committed disk; its unflushed
journal is discarded, never rebased. An executor that cannot renew its lease pauses the
VM. Storage fencing does **not** stop side effects (outbound HTTP, mail, API calls) of a
partitioned old VM; that needs egress through an epoch-aware gateway or idempotent
destinations, and is out of scope for v1.

### Keys

Separate rights: Execute (placement record), VolumeWrite (epoch in the record),
KeyFetch (custodian release). v1 options:

1. **Pre-wrap** the volume DEK to every approved executor's registered encryption key.
   Simplest and fully owner-independent, but every candidate can decrypt at any time —
   state that guarantee as such.
2. **Custodian on the voters** (recommended next): DEK shares (2-of-3) held by the
   control voters, each released only to the executor named by the committed record at
   the current epoch.

Neither option can make a former executor forget a key or plaintext it already saw.
Rotating the DEK per epoch protects only data written afterwards. Hiding guest memory
from the provider requires SEV-SNP/TDX with remote attestation (server-class EPYC/Xeon);
the consumer CPU used for development cannot do this.

### Networking: private network vs global

A mistlib room is a **rendezvous and transport namespace, not an access boundary**:
anyone who knows a room id can join discovery, and routed envelopes are only hop-by-hop
(DTLS) encrypted. Therefore:

- **Private network** = a signed membership list (owner DIDs, VM server ids, executor
  DIDs) + service ACL, using a high-entropy room id only as rendezvous. Every stream open
  is authorized against membership and the committed executor/epoch, independently of
  the room. This is the concept worth introducing.
- **No global L2/L3 network.** A shared "global" plane, if any, is a directory
  (provider advertisement, server-id → network lookup), never reachability.
- **v1 is L4**: `mistl vm ssh <server_id>` resolves the executor and opens a tunnel stream
  to the guest's sshd forward. The SSH host key lives on the volume, so the host
  identity survives failover (pin with `HostKeyAlias=<server_id>`). Existing sessions drop
  on cold restart; clients reconnect.
- **L3 later** (VM-to-VM arbitrary IP inside a private network): TUN/WireGuard-style
  adapters, address allocation bound to the server id, anti-spoofing, a datagram lane.
  Only when a workload needs it.
- **Public exposure** is a separate, explicit per-service gateway with a public IP.
- **Guest egress** goes out through the executor's IP (slirp/passt NAT); providers need
  an egress policy (abuse risk).

## Phase 0 — measurements that decide go/no-go

1. QEMU/KVM in WSL2: boot an Ubuntu cloud image from a userspace NBD export; measure
   boot time and 4 KiB random write + fsync through NBD.
2. Three local dev daemons with escrow Raft: commit latency and throughput for 1–4 MiB
   objects and a 32 MiB generation; behaviour while one voter is down.
3. SSH through the existing tunnel forward to a QEMU guest: interactive RTT and bulk scp
   throughput, direct vs relayed.

If (2) cannot sustain roughly 10–20 MiB/s with flush latency of a few seconds, the
volume path needs a dedicated extent transfer lane before anything else.

## Suggested phases after Phase 0

1. Single node: `mistl vm create/start/stop/status/ssh` with local NBD-backed disk (no
   distribution) — proves runtime, isolation and SSH-by-server-id.
2. Volume state machine + extent upload + flush contract on the 3-voter cluster.
3. Placement records, leases, epoch fencing, cold failover to a second executor.
4. Key custodian (pre-wrap first, then 2-of-3), failure tests (partition, old executor
   return, majority loss, storage node loss, kill during/after flush), RTO/RPO measurement.

## References

QEMU invocation/NBD/networking: https://www.qemu.org/docs/master/system/invocation.html,
https://www.qemu.org/docs/master/system/devices/net.html · NBD protocol:
https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md · Linux flush/FUA:
https://docs.kernel.org/block/writeback_cache_control.html · Ceph RBD exclusive lock and
blocklisting: https://docs.ceph.com/en/latest/rbd/rbd-exclusive-locks/ · Neon safekeeper
protocol: https://github.com/neondatabase/neon/blob/main/docs/safekeeper-protocol.md ·
Raft: https://raft.github.io/raft.pdf · Chubby sequencers (fencing tokens):
https://storage.googleapis.com/gweb-research2023-media/pubtools/4444.pdf · Firecracker
design: https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md ·
AMD SEV: https://www.amd.com/en/developer/sev.html · Intel TDX:
https://docs.kernel.org/arch/x86/tdx.html · Confidential Containers Trustee:
https://confidentialcontainers.org/docs/attestation/architecture/ · WSL nested KVM:
https://learn.microsoft.com/en-us/windows/wsl/wsl-config

## Direction update (2026-11-21)

Target use cases clarified: Discord bots, cron jobs, web apps; executors may include
friends' machines; no public exposure (overlay-only access).

Consequences:

- The hard requirement is **at most one running instance** per service (duplicate bot
  replies, double cron runs), not block-level disk replication.
- Friends' machines need host protection → keep a VM as the isolation boundary, but make
  it a **disposable sandbox** (one per owner per executor, immutable base image, resource
  caps set by the host owner). Services run as containers inside it. State lives at the
  service level (data directories / SQLite) and is checkpointed encrypted to escrow, so
  no distributed block device, NBD layer or strict flush contract is needed.
- A WSL2 distro is not an isolation boundary (Windows interop, drive automount); on
  Windows use a real VM (QEMU+WHPX, or QEMU+KVM nested in WSL2).
- Lease-based self-fencing stops services when the executor loses the control quorum.
  With home PCs as voters this can reduce availability; at least three mostly-on voters
  (or one always-on small node) are needed. A per-service policy (stop vs keep running
  on quorum loss) is an open design choice.
- Overlay access: stable service names resolved to the current executor; the local
  daemon exposes them as local ports or an HTTP proxy (`http://<name>.mistl`-style). L3
  is deferred.
- Secrets (bot tokens) are released only to executors the owner approves; those hosts can
  read them (no TEE).

## Direction update 2 (2026-11-21): OS-independent agent computers

Long-term goal: any mistl node, regardless of host OS, can host the same VM environment,
eventually running always-on personal AI agents (each agent gets its own computer,
browser and long-running tasks) on the user's and friends' machines.

- **QEMU is the common runtime** because it is the only mainstream VMM that spans the
  three host hypervisors: KVM (Linux), WHPX (Windows, needs the "Windows Hypervisor
  Platform" feature) and HVF (macOS, needs the hypervisor entitlement on the signed
  binary). Ship it as a separate managed download, not linked into mistl.
- **"Same environment" is per CPU architecture.** Base images are published per arch
  (x86-64, arm64). A persistent disk with installed binaries can only fail over to a host
  of the same arch; x86-64 on Apple Silicon means full emulation. Failover domains are
  therefore grouped by arch.
- **Agents need a persistent computer**, so whole-disk state is back in scope, but with a
  checkpoint contract rather than a distributed block device: immutable base image + a
  qcow2 overlay, quiesced with the guest agent (fsfreeze), incremental backups using
  QEMU persistent dirty bitmaps, changed clusters stored encrypted in escrow. RPO = the
  checkpoint interval; restore on another same-arch node. Strict flush durability stays
  a later option.
- **The brain stays outside the VM**: LLM calls go through the mistl AI network/local
  API, and `mistl agent` (ask/auto/yolo) is the natural agent runtime inside the VM.
- **At most one acting agent** matters more than for bots: two copies could act on the
  same accounts. Besides lease self-fencing, high-impact actions should be claimed in the
  control log (idempotency keys) before execution.
- **Credentials an agent uses are visible to whoever hosts it** (no TEE on consumer
  hardware). Hosts need trust tiers; high-sensitivity agents run only on the owner's
  own nodes.

## Phase 0 results (2026-11-21, one Windows 11 Home machine, i7-11700F)

Same-host measurements; no multi-machine or network numbers yet.

| Check | WSL2 Ubuntu 24.04, QEMU 8.2 + KVM | Windows native, QEMU 11.1 + WHPX |
| --- | --- | --- |
| First boot of Ubuntu 24.04 cloud image to SSH ready | 13.6 s (guest userspace 8.3 s) | guest boot 11.7 s; cloud-init finished at 11.7 s uptime |
| Reboot of an existing overlay to SSH ready | 7.0 s | not measured |
| 512 MiB sequential write, `oflag=direct conv=fsync` (local qcow2) | 988 MB/s | 401 MB/s |
| State after power-off + boot (file in `/var/lib`, SSH host key) | kept | — |
| Admin rights needed | none (user needs rw on `/dev/kvm`) | none on this machine (Windows Hypervisor Platform was already enabled) |

What worked and what did not:

- **cloud-init seed without ISO tools**: QEMU's vvfat driver with a volume label works on
  both hosts: `-blockdev driver=vvfat,node-name=seed,dir=<dir>,label=cidata,read-only=on`.
  The shorthand `-drive file=fat:label=cidata:<dir>` did **not** work (cloud-init found no
  datasource).
- **Boot order**: with two virtio disks the firmware may try the seed disk and hang with an
  empty serial log. Pin the root disk with `bootindex=0`.
- `-nographic` cannot be combined with `-daemonize`; use `-display none`.
- **Guest → host loopback leak**: with QEMU user-mode networking (slirp) the guest reached a
  host server bound to `127.0.0.1` through `10.0.2.2` (HTTP 200). This is the default
  behaviour on every host OS. On Linux, passt with `--no-map-gw` blocked it (gateway and
  host addresses both unreachable) while the guest kept internet access. Windows/macOS have
  no passt; there the options are slirp with a documented warning or `restrict=on`
  (no egress).
- **WSL2 `/dev/kvm` permissions**: an ACL granted with `setfacl` did not survive; the
  device node was recreated. Adding the user to the `kvm` group (effective after the WSL
  session restarts) is the durable fix.
- **Windows OpenSSH** rejects a private key that other principals can read ("bad
  permissions"). `icacls <key> /inheritance:r /grant:r <user>:F` fixes it.

## Stage 1 implemented (2026-11-21)

`mistl vm` (see [vm.md](vm.md)): single-node QEMU guests, SSH by server id locally and over
the tunnel, owner/allow-list gate on `vm-<id>-ssh`, persistence across stop/start and
daemon crash verified on Windows/WHPX. Findings from building it:

- QMP on the QEMU child's stdio pipes avoids any management socket the guest could reach.
- Windows Job Objects (existing `child_process::ProcessTree`) reliably kill QEMU when the
  daemon is killed; macOS has no equivalent yet.
- The tunnel carries SSH fine for interactive use (793 ms session setup on one host) but
  only ~1.6 MB/s bulk vs ~43 MB/s locally. Stage 2 checkpoint transfer must not ride the
  tunnel's TCP forward path.
- Quoting the `UserKnownHostsFile` path in a `-o` argument broke the MSYS `ssh` that Git
  Bash puts first on PATH ("invalid quotes") while Windows OpenSSH accepted it. Only quote
  paths that contain whitespace or quotes.
- Review items deferred to later stages are tracked privately (routed-origin
  authentication for VM ACLs, socket-level ACL at the dial boundary, macOS orphan QEMU).

## Stages 2–4 implemented (2026-11-21)

See [vm.md](vm.md) for usage and measurements.

- **Checkpoints (stage 2):** the qcow2 overlay is split into 1 MiB chunks addressed by a
  per-VM HMAC and encrypted deterministically with AES-256-GCM, so unchanged chunks dedupe.
  A running guest is paused with QMP `stop` only while the file is read and hashed; new
  chunks are staged as plaintext and encrypted/synced after `cont` (409–422 ms pause for a
  300+ MiB overlay in a debug build, versus 18.8 s when encryption ran inside the pause).
  Restore rebases the overlay onto the local copy of the exact base image.
- **Groups, move and failover (stages 2–3):** members join with a secret token; HMAC-
  authenticated control messages over a VM room; lease renewals acked by a majority of all
  members; HA guests boot paused and run only under a valid lease; pause at lease expiry,
  hard kill before voters may grant (2 × ttl); Raft-style vote rules on checkpoint
  sequences; commit quorum sized to intersect every election quorum. Planned move uses a
  release + immediate election. Verified live with three dev daemons on one Windows host:
  move, hard-kill failover, the killed holder rejoining as standby without booting.
- **Agent computers (stage 4):** `mistl agent --vm <id>` keeps the agent and its LLM path on
  the host and runs its shell/file/process tools in the guest over SSH.

Known limits: one shared group key (any member is trusted; no per-member signatures yet),
HA refused on macOS (QEMU is not tied to the daemon's lifetime there), transfer ~2 MiB/s
with 15 KiB frames, debug-build crypto makes restore slow, no multi-machine test yet.
