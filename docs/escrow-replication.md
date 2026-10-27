# Durable escrow replication

Escrow replication stores the same client-encrypted data on several configured mistl nodes sharing an escrow room. The client can send a request to any provider; followers forward it to the storage leader. This leader is independent of the application's multiplayer host. The application decides who writes checkpoints and how often; the storage cluster decides when a write has a durable majority.

The leader stages the original encrypted bytes and CBOR manifest before proposing the signed head. Each follower verifies the head and content, stores them durably, and only then acknowledges the corresponding Raft entry. An application ACK follows majority commitment. GET commits a current-term barrier before returning the head, so an isolated former leader cannot pass its stale state off as current. The cluster never receives decryption keys or plaintext. On election, a new leader automatically proposes a current-term barrier. Committing that barrier applies the preceding committed prefix, including previously acknowledged writes whose final commit notification did not reach every follower before the old leader stopped. Recovery does not require a new application write to advance that prefix.

Raft assumes members execute the storage protocol correctly. It tolerates node outages and network partitions, rather than malicious storage acknowledgements. A majority ACK confirms durable copies at that time; it does not guarantee all replicas are current or protect against simultaneous loss of the majority's disks. Keep independent copies of encryption keys and maintain separate recovery backups where needed.

## Bootstrap a cluster

Configure the same escrow room and original voter IDs on each participating daemon. Node IDs are the canonical 16-character lowercase hexadecimal IDs from each daemon's identity/status output; the values below are placeholders.

```toml
[escrow]
enabled = true
rooms = ["replicated-escrow"]
max_blob_bytes = 33554432
max_total_bytes = 1073741824
max_slots_per_room = 1024

[[escrow.replication]]
room = "replicated-escrow"
voters = ["1111111111111111", "2222222222222222", "3333333333333333"]
learners = []
```

Restart each configured daemon, enable its network connections, and point the client at this escrow room. A daemon must be an original voter or an explicitly configured initial learner to initialize its local replication state. Browser peers are never voters. Discovery in the same room alone does not authorize storage membership.

`voters` in the configuration is the immutable genesis list, which defines the cluster identity. It is not an editable representation of the current voting set. The initial `learners` list also becomes part of the local persisted bootstrap configuration. Preserve both when restarting an initialized daemon; change live membership through the command below. Changing the original voter list or silently removing a retained replicated room from the configuration is rejected. Restore the original configuration to reopen retained state.

A three-voter cluster needs two voters to commit and tolerates one unavailable voter. Two voters need both. Disconnected voters remain in the denominator: disconnects, discovery expiry, and process restarts never shrink quorum. Reconnected members catch up through log entries or a snapshot and the referenced encrypted generations.

## Add, promote, or retire members

Learners receive data but do not vote. Enroll a new node as a learner first, allow it to catch up durably, then promote it. Run membership commands against the current leader's local daemon; they are available through local IPC only. Use `mistl escrow status` to identify the current leader, learners, voting configuration, commit index, snapshot index, and errors. Include `--instance` or `--state-dir` when operating an isolated daemon.

The command replaces the entire desired live member set; the lists are not incremental additions. Existing learners that should remain learners must be included. For example, add node D to voters A, B, and C:

```text
mistl escrow membership replicated-escrow --voters 1111111111111111,2222222222222222,3333333333333333 --learners 4444444444444444
```

On the new daemon D, configure the same room and original `voters`, with `learners = ["4444444444444444"]`, then start it. Do not rewrite the initial learner list on already initialized daemons. Their committed live membership is persisted and recovered independently of the bootstrap list. The network admission policy must also permit D's signed identity.

Once D has acknowledged the committed state, promote it while preserving any other learners:

```text
mistl escrow membership replicated-escrow --voters 1111111111111111,2222222222222222,3333333333333333,4444444444444444
```

Promotion is rejected until the new voter is already an enrolled learner and has durably caught up. Voter changes use joint consensus: both the old and new majorities must agree during the transition, followed by a committed stable configuration. An `accepted: true` response refers to the final committed target configuration. Removing C after D has joined uses the same command with `--voters A,B,D`; keep C running until the transition completes whenever possible. Stop the retired node or disable its escrow service afterward; it no longer receives new replicas.

There are at most nine voters and nine learners in a desired configuration. The union of old voters, new voters, and learners during a joint transition is capped at 18, so a large replacement may need several transitions. Losing quorum is not repaired by automatically deleting absent voters. Restore enough existing voters to regain quorum before changing membership.

A timeout is ambiguous: inspect live status before retrying a membership change. Changing members does not alter the genesis cluster identity or require clients to use a different room.

## Migrate a standalone room

Portable [export/import](escrow.md#export-and-import-moving-to-another-node) can
export a replicated room from any member, reading that member's local committed
state. Import into a room that is replicated on the destination node is skipped
as `replicated`. To move a replicated room through an export file, import on one
node where the room is still standalone, then configure replication with that
node as `migration_seed`, following the procedure below.

Choose one authoritative standalone daemon as the explicit migration seed. Its node ID must be in the original voter list. Configure the same `migration_seed` on every member before starting the replicated room:

```toml
[[escrow.replication]]
room = "existing-escrow"
voters = ["1111111111111111", "2222222222222222", "3333333333333333"]
learners = []
migration_seed = "1111111111111111"
```

Only the designated seed may contain standalone data for that room. Other members must start with an empty standalone room; startup refuses conflicting pre-existing data instead of merging independent histories. Keep the seed online and preserve its frozen source until migration completes.

The service freezes the seed's current signed head list when the replicated room starts. The elected leader imports that list in bounded batches, stages each generation from the seed, and replicates it with the same durable-majority rule as normal writes. The import preserves signed head sequence numbers and parent references, including heads whose older predecessors are absent. Normal client reads and writes return `busy` until the committed completion marker is applied.

An interrupted import retries and skips heads already imported identically. It refuses conflicting source heads. Standalone files are retired only after the local replica applies the committed completion marker; failure before that point leaves the source intact. After successful migration, clients use the replicated room's existing room ID. Migration copies the seed's current heads, rather than merging multiple providers or preserving every standalone historical generation.

## Snapshots, retention, and capacity

Raft logs are internal ordering records, not permanent backup history. When the tail reaches 128 entries, or ciphertext usage exceeds half the archive quota, the daemon writes a checksummed application snapshot at the committed absolute log index and compacts the preceding log. The snapshot includes visible heads, retained predecessors, deletion tombstones, migration progress, and Raft membership. Restart verifies the snapshot and replays its remaining committed tail. This removes the prototype's 4096-entry lifetime limit.

A sufficiently lagging learner or follower downloads snapshot metadata in 8 KiB chunks. Signed, room-scoped offers identify the sender, transfer, size, hash, index, term, and membership. The receiver checks the complete metadata hash and stages every referenced encrypted generation before durably installing the snapshot and acknowledging it. Duplicate or reordered chunks cannot advance an unmatched transfer. Incoming metadata expires after 180 seconds without progress.

Each daemon bounds snapshot metadata to 64 MiB, durable Raft metadata to 80 MiB, and the active tail to 1024 entries. The transfer manager holds one incoming metadata buffer and shares one outgoing metadata payload across recipients. Incoming metadata expires after 180 seconds without progress. During metadata transfer, requests refresh the outgoing payload and generation lease's 660-second deadline. The first final metadata chunk fixes that deadline; repeated requests cannot extend it while the receiver prepares ciphertext. This covers the receiver's 600-second preparation deadline and bounds deferral when a recipient cannot persist the body. If the leader term advances, an existing recipient receives the same protected snapshot under a fresh current-term ticket and deadline. Acknowledgment then allows the latest snapshot to follow. An active older transfer retains its payload and generation leases until acknowledgment or expiry; a newer snapshot or new recipient may be deferred until it finishes. This bounds retained history while allowing an existing recipient to finish across later compactions. These are working capacity bounds; they do not count all historical updates. Updates and imports that would exceed the snapshot metadata budget return `quota` before mutating state. Unavailable durable storage fails closed.

Archive collection retains generations referenced by live state, the durable snapshot, the remaining log, and active transfer or staging leases. It removes generations after none of those references need them. Current and previous heads support retries across compaction. DELETE records a tombstone and hides the head immediately after commitment; a valid same-writer successor can recreate the slot with the next sequence and deleted head as its parent. Physical reclamation can wait for subsequent compaction and transfers, and offline replicas retain their copies until they catch up. Tombstones continue to count against the room's slot limit; recreating the same authorized slot reuses that entry. DELETE is not a promise of immediate erasure of every backup copy.

`max_total_bytes` is divided equally between the standalone repository and each configured replicated room on that daemon. With one replicated room and the default 1 GiB total, each partition receives 512 MiB. Unused standalone capacity is not automatically lent to replicated rooms. Archive quota includes ciphertext, manifests, and archive records; the separately bounded Raft files are additional disk usage. During migration, the standalone source and replicated copy temporarily occupy their respective partitions. Existing data above a reduced partition prevents startup, so check capacity before enabling additional rooms or lowering quota. Lowering `max_blob_bytes` limits new uploads and new generation downloads; already archived larger generations remain usable for retained snapshots and reads.

Providers advertise `raft`, `snapshot`, `membership`, and `delete-continue` alongside the existing delete feature. Ciphertext and manifest pulls use `mistlib::storage::fetch_block` pinned to the originating peer and room, with bounded receive sizes and the existing 30-second generation deadline. The service registers a room-scoped `BlockProvider` for retained standalone data and replicated archives; mistlib handles storage serving admission, per-peer fairness and CP50 backpressure. Escrow does not parse or emit storage frames in its raw room handler. Existing unconfigured standalone rooms continue to use their original service. Clients retain the public escrow request/head format and their responsibility for encryption and signed updates.

## Operations and verification

Use `mistl escrow status` to inspect a room's current leader, original genesis, voters, learners, joint configuration, quorum, committed/applied positions, snapshot index, remaining log entries, archive bytes, migration completion, and storage errors. A request timeout does not prove failure: retry the same signed request to discover whether it committed. Stop and investigate disk errors or a persistently missing quorum before replacing files or changing genesis settings.

Run `cargo check --all-targets` and `cargo test --bin mistl`. Focused replication tests use `cargo test --bin mistl escrow::replication`. The suites exercise durable quorum copies, partitions and leader replacement, restart, snapshot compaction and installation, learner promotion and joint membership, sender/room-bound signatures, replay and chunk bounds, corrupted data, archive pin lifetimes and collection, migration, CAS, and deletion continuation.

For a real three-daemon check, use an existing development binary:

```text
node scripts/test-escrow-replication.mjs path/to/existing/dev/mistl.exe
```

Use the corresponding executable path on other platforms. The script never builds or copies source trees and refuses a stable-channel binary. It requires working P2P public signaling connectivity; a connection timeout is a failed integration check. `--help` describes usage without starting daemons. Add `--keep-state` to retain isolated temporary state for diagnosis.

The harness starts three owned daemon instances under a fresh temporary state root, disables their dashboards and automatic updates, and checks a 32 MiB temporary disk budget. Its client encrypts data and signs public escrow heads before publishing ciphertext; encryption keys and plaintext stay in the client process. It checks replicated ciphertext copies and client-side decryption, stops the storage leader and writes through its replacement, exercises an upload originating on the new leader, triggers compaction with sustained updates, restarts the lagging member for snapshot catch-up, and finally restores the latest data with the publishing node offline. A signed oversized request checks size rejection without uploading a large blob. Cleanup stops only the harness's daemon instances and removes only its own temporary root when those processes have exited.

`escrow.client.request` is a local-IPC-only bridge for existing public `escrow_get`, `escrow_put`, and `escrow_delete` requests. It binds replies to the requested room, transport peer, and a generated request ID, checks returned signed heads or acknowledgements, and bounds outstanding requests. Publish encrypted blocks first, then supply `{room, peer, request}` through local IPC. The bridge forwards the public request format; it does not encrypt data or receive decryption keys. The integration harness uses this bridge without a browser. Availability of the script does not assert a successful live run; retain its actual result separately from unit and simulated-network results.

The CI workflow validates pushes to both `develop` and `main`. Its five-target matrix covers Windows x86_64, Linux x86_64 and ARM64, and macOS ARM64 and x86_64, running the serial test suite on each runner's native architecture and building each release target. Only a pushed `v*` tag (created with `just publish`) publishes a release. Real P2P integration remains an explicit check rather than a dependency on public signaling during every CI run.

CI and `.env.example` pin `MISTLIB_REF` to `0f5fd9e18a6a154fa8b826fd9a8c2fed8d8980a8` and `MISTLIB_CONSENSUS_REF` to `a80f80b22de5466387fd9f93e63a3e793ff2f456`. The consensus revision contains the required snapshot and joint-membership APIs. Use matching dependency revisions when rebuilding a clean checkout; a stale local dependency clone is not sufficient evidence that the release sources match.
