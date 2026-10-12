# Encrypted checkpoint escrow

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
~~~

Use mistl config set escrow.<field> <value> to edit individual settings. Room IDs
contain 1–96 ASCII characters from [A-Za-z0-9_./:-]. Configuration accepts at most
64 rooms, blobs up to 1 GiB, a total limit up to 1 TiB, and up to 65536 slots per room.
The service additionally caps persisted slots globally at 65536. Defaults are 32 MiB,
1 GiB total, and 1024 slots per room.

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
**Advanced** contains maximum blob size and total capacity (GiB), and slots per
room. All changes require a daemon restart; use the view's **Restart daemon**
button or the existing restart badge after saving. Room additions also enable
Escrow. External connections must be enabled for configured rooms to be joined.

These controls use `escrow.status`, `escrow.rooms.add/remove` and `config.set`.
Local sessions have the existing local-dashboard permissions. Remote sessions use
exactly the `config.set escrow.rooms` policy for additions/removals (and
`escrow.enabled` for additions); the current policy permits these settings.
Dashboard session authentication and other remote configuration restrictions remain
in force.

## Protocol

UTF-8 JSON control messages have v:1 and are capped at 16 KiB. The service directly
answers escrow_hello with escrow_provider, and broadcasts provider advertisements
on peer joins. escrow_get returns escrow_head (current signed head or null).
escrow_put returns escrow_ack only after durable persistence, or escrow_error.
Provider advertisements include `"features":["delete"]`; absent features mean slot
deletion is unsupported. Unknown message types and additional fields are ignored.

A deletion request has `{v:1,type:"escrow_delete",id,slot,seq,head,sig}`. Here `head`
is the SHA-256 hash of the complete current canonical head and `seq` is its sequence.
The pinned writer signs UTF-8 `"tc-escrow/v1/delete\n"` followed by
`stable_stringify({slot,seq,head})`. A stale sequence or hash returns `conflict`
with the current head; an invalid writer signature returns `bad_signature`.
Success replies with `{v:1,type:"escrow_deleted",id,slot}`; repeating a deletion
of an absent slot returns the same reply. A later PUT may recreate it with
parent=null and pins its writer anew. This extension keeps protocol v:1; old
providers ignore the new messages, so clients check the feature before sending.

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

The authoritative copy lives under <data_dir>/escrow/blobs/, outside the ordinary
LRU block store and outside mistlib-native's TEMP store. Each generation contains
the exact original CBOR file manifest and encrypted blob bytes. Slot records under
<data_dir>/escrow/heads/ atomically reference the current and previous generation.
Both generations are retained and served; older generations are removed after commit.
DELETE atomically removes the head record and syncs its directory before removing
both blob/manifest generations. It then rebuilds quotas and room-scoped block indices
from retained records, preserving CIDs shared with other slots. A crash between head
removal and blob cleanup leaves only orphan generations, removed at restart.
Deletion I/O failures disable subsequent mutations until restart reconciliation,
including identical current/previous PUT retries: their in-memory records may no
longer exist on disk. Healthy retained-head retries still ACK without a blob pull.

PUT pulls the root manifest and chunks using ordinary QUERY/WANT/HAVE/HAVE_CHUNK
frames directed exclusively to the sending transport peer in the request's room.
It validates manifest CID, the constant tc-escrow.bin name, size, chunk count,
each raw chunk CID, the complete byte length, and SHA-256. A CID-matching manifest
that cannot be decoded as bounded CBOR returns fetch_failed (the blob pull could not
be completed); a CID, manifest metadata, length, or content mismatch returns
hash_mismatch. These content failures do not disable repository writes. It never invokes the
ordinary StorageEngine::get on an untrusted manifest, avoiding that engine's
unbounded whole-file allocation. No storage resolver change is required: escrow
registers its own room-scoped storage handler and serves the original manifest and
chunk slices directly from the durable blob. Browser storage_get(cid) therefore
uses the normal storage protocol, including after daemon restart.

Blob and manifest writes use statefile::write_private (temporary file, fsync,
atomic replacement); only then is the combined current/previous head record
replaced durably. Unix directories are also synced before publication. Restart
verifies signed records and all retained blob/manifest content, rebuilds the block
index, and removes unreferenced generations and temporary head/blob/manifest files
left by interrupted writes. Corrupt retained state fails startup rather than silently
forgetting the pinned writer.
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
The entire blob pull has a 30-second deadline. Manifest size is capped at 128 KiB,
chunks at 1 MiB, and incoming chunk assemblies at 1024 pieces. Transfers only accept
solicited CIDs from the original room and sender. JSON requests and storage responses
have separate bounded task pools. Storage serving queues at most 256 requests globally
and 64 per transport peer, including active requests, across all escrow rooms. Up to
four peers are served concurrently, one request per peer, with round-robin scheduling.
QUERY admission stops at 192 global / 48 per-peer requests to reserve space for WANTs;
over-cap discovery advertises nothing and never evicts accepted WANTs. Requests beyond
the hard WANT caps cannot be accepted. Queued requests contain only identifiers; blocks
are read by active workers. Storage frames use the core size helper with a 16 KiB
ceiling, subtracting the header, CID, and 512-byte overlay margin.

Serving retains the peer worker for a 10 ms delay after every frame, including the
last frame of a WANT and QUERY status replies. Consecutive WANTs across rooms share
the same peer worker, so four parallel 1 MiB downloads (268 frames) cannot become
a single enqueue burst: their frames take at least 2.67 seconds to enter the queue.
Observable enqueue errors retry the same frame, with a 5-second deadline per frame.
The pacing signal is elapsed monotonic time: net::send_direct only reports admission
to the app queue, and exposes neither the native 256-frame peer queue capacity nor
wire completion. At at most 100 frames/second, a one-second drainer pause leaves
about 100 frames queued, with room for other traffic. Regression tests model that
pause and subsequent draining at the producer rate, checking all four blocks.
This conservative fallback is not a delivery acknowledgement or a strict capacity
guarantee: a longer transport stall, slower drain, or competing non-escrow traffic
can still fill the downstream queue without an observable error. A transport
capacity/completion API would be needed to remove that limitation; no mistlib source
or storage wire format is changed here.
Stored data plus in-flight reservations must fit the total quota; a reservation includes
blob bytes and a bounded manifest allowance.
This conservatively includes the transient new generation before old data is deleted,
so a nearly full service may reject replacement writes with quota.

## IPC and verification

IPC commands are escrow.status, escrow.rooms.list, escrow.rooms.add {room}, and
escrow.rooms.remove {room}. The CLI exposes the same operations.

~~~console
cargo test --bin mistl escrow
~~~

Tests include the byte-exact shared derivation/encryption/signature/head-hash and
delete-signature vectors, deletion writer/CAS checks, absent-slot idempotence,
quota release and TOFU recreation, shared-CID preservation and interrupted-delete recovery,
CAS conflicts, TOFU writer mismatch, idempotence, bad signatures, size/hash failures,
quota reservations, sender/room-bound transfer assembly, and restart recovery with
both retained generations served through the normal block representation, crash temporary
file cleanup, identical PUT retries under pull saturation, bounded/fair serving queues,
twelve simultaneous distinct WANT replies, frames that fit a 16 KiB browser ceiling,
reserved commits after publication failure, a sync error after head replacement,
retained previous-head retries, malformed CID-matching CBOR, bounded JSON admission,
slot-wait floods with responsive GET/HELLO, and four parallel 1 MiB downloads through
a paced 256-frame peer queue with a temporary drainer stall.
