# Encrypted checkpoint escrow

Configured rooms use automatic replication across providers by default. An opt-in,
fixed-voter [Raft replication prototype](escrow-replication.md) is also available
for rooms that need its stronger consistency model. Raft rooms follow their own
membership and deletion rules and do not participate in automatic replication.

The optional daemon service implements tc-escrow v1. It stores encrypted application
checkpoints and signed mutable heads. Encryption and key derivation happen in the
browser; mistl receives only an opaque slot, encrypted blob, and Ed25519 public writer.
Anyone who knows the application's name and room code can derive its read/write keys
in this trial protocol. Use a separate mistlib escrow room, rather than the game room.

## Setup

Configure the same mistlib room in the application and the daemon:

~~~console
mistl escrow rooms add my-escrow-room
mistl daemon stop
mistl daemon start
mistl network on
mistl escrow status
mistl escrow rooms list
mistl escrow rooms remove my-escrow-room
~~~

Adding a room also sets enabled=true. Removing a room retains its persisted data.
Room changes, the enabled switch, and limits all require a daemon restart, like
chat_relay.rooms. Status and room configuration work with external connections off;
the service joins rooms only while external connections are enabled. Status reports
configured/joined rooms, slot counts, and byte totals, never head contents or blobs.
Per-room bytes count retained encrypted blobs; the global total also includes their
original storage manifests, including data for removed rooms.

~~~toml
[escrow]
enabled = false
rooms = []
max_blob_bytes = 33554432
max_total_bytes = 1073741824
max_slots_per_room = 1024
auto_replicas = 3
dead_after_secs = 86400
~~~

Use mistl config set escrow.<field> <value> to edit individual settings. Room IDs
contain 1–96 ASCII characters from [A-Za-z0-9_./:-]. Configuration accepts at most
64 rooms, blobs up to 1 GiB, a total limit up to 1 TiB, and up to 65536 slots per room.
The service additionally caps persisted slots globally at 65536. Defaults are 32 MiB,
1 GiB total, and 1024 slots per room.

## Automatic replication

Configure the same non-Raft escrow room on several providers and enable their
external connections. Providers discover each other in the room; no voter list or
invitation is needed. The target is `auto_replicas` copies per slot (default 3,
range 1–8), limited to the number of live providers. Providers exchange signed
head and deletion metadata; encrypted blobs are stored on the selected holders.
A single provider keeps its own copy. Capacity limits still apply, and a provider
without enough space cannot hold a copy.

A PUT ACK means that the accepting provider durably stored the data. Additional
copies are made asynchronously; an ACK does not guarantee that the target has
already been reached. `mistl escrow status` and the dashboard's **Copies** column
show healthy, replicating, degraded, and conflicting slots, while
`mistl escrow slots <room>` shows individual confirmation counts. A healthy slot
has confirmed copies on all target holders. Replication needs connected providers
with enough capacity, and does not replace independent backups.

Export from a holder or from each provider: omitted local copies are counted as
`not_held` in the export result, and with at most `auto_replicas` providers every
provider holds every slot once replication completes and capacity permits.

Providers not seen for `dead_after_secs` (default 86400, minimum 60) are excluded
from placement and replacement copies are made when capacity is available. Former
holders retain their copies until the selected holders confirm, plus a 24-hour
grace period. A mesh deletion is retained as a tombstone and propagated; recreating
that slot uses the same writer, the next sequence, and the deleted head as parent
(the `delete-continue` feature). Offline copies may persist until their holders
reconnect and reconcile.

Set `escrow.auto_replicas = 1` on each provider to disable automatic mesh
replication and use the standalone local-copy behavior. Configured Raft rooms
instead use their fixed membership and durable-majority ACKs; see [Raft replication](escrow-replication.md).
Room and replication settings take effect after daemon restart.

## Web dashboard

Open **Store → Escrow** (or the dashboard's `#panel-escrow` link). Escrow is a
Storage sub-page because it retains encrypted checkpoints alongside the other
storage tools. Copy the application's **mistl room** into **Add room**; tc-linkai
shows an automatically generated ID such as `tce-xxxxxxxx…` with a **Copy** button.
Use this exact ID, not the application's game room code.

The view shows configured and joined rooms, slots and stored bytes per room,
global slot/byte totals, the configured enabled switch and running status.
**Refresh** reloads status. **Remove** changes configuration without deleting
retained data; removed rooms with stored data remain visible. Global bytes include
storage manifests and data for removed rooms. No head contents or blobs are shown.

The **Escrow settings** drawer uses room-list chips and an enabled toggle.
**Advanced** contains maximum blob size and total capacity (GiB), slots per
room, target copies, and provider expiry. All changes require a daemon restart;
use the view's **Restart daemon** button or the existing restart badge after saving. Room additions also enable
Escrow. External connections must be enabled for configured rooms to be joined.

These controls use `escrow.status`, `escrow.rooms.add/remove` and `config.set`.
Local sessions have the existing local-dashboard permissions. Remote sessions use
exactly the `config.set escrow.rooms` policy for additions/removals (and
`escrow.enabled` for additions); the current policy permits these settings.
Dashboard session authentication and other remote configuration restrictions remain
in force.

## Export and import (moving to another node)

Export on the old node, copy the `.mistl-escrow` file to the new node, and import
it there. The CLI resolves relative paths against its current directory:

~~~console
mistl escrow export /absolute/path/checkpoints.mistl-escrow
mistl escrow export /absolute/path/selected.mistl-escrow --room my-escrow-room --room another-room --force
mistl escrow import /absolute/path/checkpoints.mistl-escrow
mistl escrow import /absolute/path/checkpoints.mistl-escrow --room my-escrow-room
mistl escrow rooms add my-escrow-room
~~~

On Windows, use an absolute path such as `C:\Backups\checkpoints.mistl-escrow`.
Omitting `--room` selects all rooms with data; repeat it to select several rooms.
Export refuses to overwrite an existing file unless `--force` is supplied. It
writes a temporary file in the destination directory and renames it into place
only on success. A slot changed or removed during export is skipped as `changed`.
Export includes only current heads, not previous generations or deletion tombstones.

In **Store → Escrow**, **Export all** downloads every room to a file in the browser,
and **Import all** uploads a selected file and imports every room in it after
confirmation. Each room row also has its own **Export** and **Import**: they
download only that room, or import only that room's slots from the chosen file
(other rooms in the file are ignored). The result shows imported,
unchanged and skipped counts, grouped skip reasons, and a reminder to configure
imported rooms when needed. These operations work with external connections off
and with the escrow service disabled. Remote dashboard sessions cannot use the
path-based IPC commands, but can use these download/upload buttons.

The version 1 portable format starts with a versioned JSON header, followed by
entries containing a room ID, the original writer-signed head, the original CBOR
storage manifest and the encrypted blob, then a counted JSON trailer. It contains
no plaintext or encryption keys. Still treat the file as sensitive: anyone with
the application name and room code can derive keys in this trial protocol.

Import first copies the file into the escrow data directory (so it needs that much
free space temporarily; an interrupted copy is removed at restart) and validates the
whole copy's structure, trailer, signatures and content hashes before applying any entries. It adds or updates slots and never deletes
existing slots. Each slot is handled independently after validation:

- An identical head is counted as unchanged.
- A head from the same writer with a higher sequence replaces the local head.
- A lower sequence is skipped as `older`.
- The same sequence with a different head is skipped as `conflict`.
- A different writer is skipped as `writer_mismatch`.
- An entry exceeding local capacity limits is skipped as `quota`.
- A room replicated on this node is skipped as `replicated` (see
  [replicated-room migration](escrow-replication.md#migrate-a-standalone-room)).

Import does not configure rooms automatically. Imported rooms are served only if
configured: run `mistl escrow rooms add <room>`, restart the daemon, and enable
external connections as in Setup above.

## Protocol

UTF-8 JSON control messages have v:1 and are capped at 16 KiB. The service directly
answers escrow_hello with escrow_provider, and broadcasts provider advertisements
on peer joins. escrow_get returns escrow_head (current signed head or null).
escrow_put returns escrow_ack only after durable persistence, or escrow_error.
Provider advertisements include the `delete` feature; absent features mean slot
deletion is unsupported. Mesh and Raft rooms also advertise `delete-continue`.
Unknown message types and additional fields are ignored.

A deletion request has `{v:1,type:"escrow_delete",id,slot,seq,head,sig}`. Here `head`
is the SHA-256 hash of the complete current canonical head and `seq` is its sequence.
The pinned writer signs UTF-8 `"tc-escrow/v1/delete\n"` followed by
`stable_stringify({slot,seq,head})`. A stale sequence or hash returns `conflict`
with the current head; an invalid writer signature returns `bad_signature`.
Success replies with `{v:1,type:"escrow_deleted",id,slot}`; repeating a deletion
of an absent slot returns the same reply. In standalone rooms with automatic
replication disabled, a later PUT may recreate it with parent=null and pins its
writer anew. Mesh and Raft rooms advertise `delete-continue` and require a
same-writer successor with the next sequence and the deleted head as parent.
This extension keeps protocol v:1; old providers ignore the new messages, so
clients check the feature before sending.

The portable client tracks all providers for each canonical head hash: verified
GETs add a provider, and PUT attempts record their target before sending. Failed
sends, lost ACKs and provider failover retain every possibly stored copy. DELETE
sends separately pinned requests to all recorded providers concurrently, waiting
for departed providers to rejoin within the request timeout. Only all-provider
delete confirmations with complete provenance return deleted; reachable copies are
still removed if another provider times out, conflicts or lacks delete support.
An absent-slot success from a different provider cannot prove deletion of a copy.

History is local to one handle, capped at 1024 head hashes and 64 providers per hash,
and retained after deletion for idempotent retries. Entries are not evicted; PUT
returns busy before sending if its target cannot be recorded. GET still works, but
unknown or incomplete provenance cannot prove deletion and returns a transport
error. Overlapping PUT/DELETE operations for the same head return busy; providers
learned during DELETE require a retry. Copies made by other handles or sessions
are outside this local inventory.

Heads use exactly the tc-escrow-head v1 fields. Signatures cover the UTF-8 prefix
tc-escrow/v1/head followed by a newline and wiresign::stable_stringify of the known
head fields with sig removed. The head hash is SHA-256 of the canonical complete
head, including sig. Writer keys are raw 32-byte Ed25519 keys encoded as unpadded
base64url. No DID or transport identity is substituted for the writer key.

The first accepted head pins a slot's writer. Later writes require that writer,
parent=hash(current), and seq=current.seq+1. An initial write requires parent=null,
with any positive JavaScript-safe integer sequence. An identical retained current or
previous head can be PUT again and receives its original seq/hash ACK without
downloading again or moving the current pointer backward. A head older than the
two retained generations returns conflict; historical acceptance is not tracked. Slots
are scoped to the mistlib room. Conflicts return the current head, or null if absent.

## Durable storage and browser downloads

Locally held blobs live under <data_dir>/escrow/blobs/, outside the ordinary
LRU block store and outside mistlib-native's TEMP store. Each generation contains
the exact original CBOR file manifest and encrypted blob bytes. Slot records under
<data_dir>/escrow/heads/ atomically reference the current and previous generation.
For standalone local-copy rooms, both generations are retained and served; older
generations are removed after commit. DELETE atomically removes the head record
and syncs its directory before removing both blob/manifest generations. It then rebuilds quotas and room-scoped block indices
from retained records, preserving CIDs shared with other slots. A crash between head
removal and blob cleanup leaves only orphan generations, removed at restart.
Deletion I/O failures disable subsequent mutations until restart reconciliation,
including identical current/previous PUT retries: their in-memory records may no
longer exist on disk. Healthy retained-head retries still ACK without a blob pull.
Mesh rooms instead retain deletion tombstones and distribute their head metadata;
blobs can reside on selected peers rather than every provider.

PUT pulls the root manifest and chunks through `mistlib::storage::fetch_block`,
with `FetchOptions.peer` fixed to the sending transport peer and the request's room.
The receive limit is the bounded manifest allowance for the root and the exact
expected length for each chunk. mistlib verifies the fetched CID without writing
it to its ordinary storage cache. Escrow also validates manifest CID, the constant
tc-escrow.bin name, size, chunk count, each raw chunk CID, the complete byte length,
and SHA-256. A CID-matching manifest that cannot be decoded as bounded CBOR returns
fetch_failed; a CID, manifest metadata, receive-limit, length, or content mismatch
returns hash_mismatch. Transport failures and timeouts return fetch_failed. These
content failures do not disable repository writes. The entire pull has a 30-second
deadline across all blocks. It never invokes the ordinary StorageEngine::get on an
untrusted manifest, avoiding that engine's whole-file allocation.

At service startup, escrow registers a `mistlib::storage::BlockProvider` and keeps
its registration handle for the service lifetime; dropping the handle unregisters
it. The provider returns original manifests and chunk slices from durable storage
only for configured rooms and CIDs indexed in that room. Mesh providers can fetch
a missing known block from a holder; Raft rooms also use their room's archive.
Data imported into an unconfigured room is unavailable through this provider. Browser `storage_get(cid)` uses mistlib's normal storage service,
including after daemon restart; storage frames no longer pass through escrow's
raw room handler.

Blob and manifest writes use statefile::write_private (temporary file, fsync,
atomic replacement); only then is the combined current/previous head record
replaced durably. Unix directories are also synced before publication. Restart
verifies signed records and all retained blob/manifest content, rebuilds the block
index, and removes unreferenced generations and temporary head/blob/manifest files
left by interrupted writes. Standalone corrupt retained state fails startup rather
than silently forgetting the pinned writer; mesh scrubbing drops corrupt blob copies
so they can be fetched again.
If a durable write or obsolete-generation cleanup fails, subsequent writes fail
closed until restart reconciles disk, including PUTs reserved before the failure.
The failure flag is set under the same index lock as publication, and no subsequent
commit or generation cleanup may use that potentially stale index. An ACKed current
head remains readable; restart reloads any head that was replaced before a sync error.

PUTs and DELETEs serialize through the same fixed slot-lock stripes. Shape/signature validation, writer and
CAS checks, and identical-head ACKs precede blob-pull admission. At most four blob
pulls run globally, with at most one per transport peer; identical retries still ACK
while those limits are full. There are no unbounded waiter or peer maps.
JSON admission happens before spawning tasks. The 32 task permits are divided into
20 PUT/DELETE requests, 8 GET/HELLO/provider responses, and 4 busy responses. Each transport
peer may occupy at most two permits in each class across all rooms, including PUTs and DELETEs
waiting on a slot stripe. PUT overload uses the busy-response pool and cannot consume
the GET/HELLO reserve. Slot waits have a 5-second deadline and return busy on expiry;
reply sends have a 2-second deadline. A flood of PUTs therefore cannot occupy all
response permits. GET/HELLO remain subject to their own bounded admission under a
separate flood of those messages.

Manifest size is capped at 128 KiB and chunks at 1 MiB. mistlib binds each fetch
to the requested room, CID and sender and enforces receive limits before copying or
reassembling data. JSON control requests retain escrow's own admission limits.
Storage serving admission, per-peer fairness, frame sizing and CP50 backpressure
are handled by mistlib's per-peer SERVING path. Escrow has no separate storage
serving queue, frame retries or elapsed-time pacing. In particular, there is no
escrow-specific four-worker serving limit or fixed 10 ms inter-frame delay.

Stored data plus in-flight reservations must fit the total quota; a reservation includes
blob bytes and a bounded manifest allowance.
This conservatively includes the transient new generation before old data is deleted,
so a nearly full service may reject replacement writes with quota.

## IPC and verification

IPC commands are escrow.status, escrow.rooms.list, escrow.rooms.add {room},
escrow.rooms.remove {room}, and escrow.auto.slots {room}. The CLI exposes the
same operations, with `mistl escrow slots <room>` showing at most 1024 rows.

Local IPC also supports `escrow.export` with
`{"path":"<absolute file path>","rooms":["room"],"overwrite":false}` and
`escrow.import` with `{"path":"<absolute file path>","rooms":["room"]}`.
For either command, omit `rooms` or set it to `null` to select all rooms.
Export's optional `overwrite` defaults to false. The `path` is on the daemon host
and must be absolute, so remote dashboard sessions cannot call these commands.
The browser uses `GET /api/escrow/export` and raw-file `POST /api/escrow/import`
instead, under the dashboard's session and instance checks. Both accept optional
repeated `?room=<percent-encoded id>` parameters with the same meaning as `rooms`.

Export returns `{path, slots, bytes, rooms: [{room, slots, bytes}], skipped}`;
byte counts sum encrypted blob sizes, excluding manifests. Import returns
`{imported, unchanged, skipped, rooms: [{room, imported, unchanged, skipped, configured}]}`.
Each skipped item is `{room, slot, reason}`. See the import rules above for reasons.

~~~console
cargo test --bin mistl escrow
~~~

Tests include the byte-exact shared derivation/encryption/signature/head-hash and
delete-signature vectors, deletion writer/CAS checks, absent-slot idempotence,
quota release and TOFU recreation, shared-CID preservation and interrupted-delete recovery,
CAS conflicts, TOFU writer mismatch, idempotence, bad signatures, size/hash failures,
quota reservations, sender-pinned fetch options and receive limits, fetch failure
classification, the whole-pull deadline, and restart recovery with both retained
generations available through the room-scoped BlockProvider, crash temporary file
cleanup, identical PUT retries under pull saturation, concurrent distinct provider
lookups, reserved commits after publication failure, a sync error after head
replacement, retained previous-head retries, malformed CID-matching CBOR, bounded
JSON admission, and slot-wait floods with responsive GET/HELLO. Fetch tests replace
the public fetch API boundary to exercise escrow validation and durable PUTs;
transport chunk assembly, frame sizing, serving admission and CP50 are owned and
tested by mistlib rather than simulated by an escrow-specific queue.
