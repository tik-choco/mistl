# Architecture

```
mistl <subcommand>  --(JSON over loopback TCP)-->  mistl daemon run
                                                     ├─ identity  (did:key ed25519, profile)
                                                     │    ├─ crypto.rs      (tc-storage-compatible AES-GCM+PBKDF2 envelope)
                                                     │    ├─ delegation.rs  (root -> leaf DID delegation: issue/verify)
                                                     │    └─ pairing.rs     (mistlib-room code pairing, issuer side)
                                                     ├─ storage   (mistlib StorageEngine + NativeBlockStore)
                                                     ├─ stream    (screen capture or p2p WebRTC relay → RTP → RTSP server)
                                                     ├─ chat_relay (verifies/persists tc-chat wires over net)
                                                     ├─ ai        (mistai protocol v1 over net + local OpenAI-compatible HTTP)
                                                     ├─ web       (embedded dashboard + /api/call bridge into the same router)
                                                     └─ net       (shared mistlib WebRTC/Nostr transport: one engine, one room)
```

- IPC discovery info is written to `data\daemon.json` (port + random token); only the
  local user can connect
- tc-storage interop: DID format, AES-256-GCM + PBKDF2-SHA256 (210k) envelopes
  (`identity::crypto`), CID semantics
- DID delegation (`identity::delegation` + `identity::pairing`): root/leaf
  chain issuance, byte-compatible with mistai's independent TypeScript
  implementation of the same spec — see [identity.md](identity.md)
- mistlink interop: AVPro-friendly dummy SPS/PPS RTSP keepalive, PT 96 / SSRC 0x12345678
- `stream` backends: `native` captures via Windows.Graphics.Capture and encodes with
  OpenH264 in-process; `ffmpeg` spawns ffmpeg (gdigrab → MPEG-TS → demux) as a fallback;
  `relay` (via `stream relay`) receives a tc-chat WebRTC screen share over mistlib and
  re-serves it (H264 passthrough, Opus→AAC transcode, RTCP sender reports for lipsync)
- `ai` speaks mistai protocol v1 on the wire (`provider_hello` / `llm_request` /
  `llm_response_chunk` with seq reordering / `llm_response_done`), so browser-based
  mistai consumers and providers in the same room interoperate; the `net` module
  multiplexes `ai`, `chat_relay`, and the other room protocols over mistlib's
  single raw-message handler by message shape

## Bot delivery and scheduled jobs

Bot pipelines save transformed content and per-sink delivery receipts before
marking an article processed. Failed or timed-out sinks retry on later pipeline
runs, including after a daemon restart; successful sinks are skipped and LLM/TTS
transformations are reused. A sink has a 120-second deadline. An incomplete
delivery makes the run unsuccessful; inspect `mistl bot items` for individual
sink errors and `mistl bot logs` for run outcomes.

Pending deliveries are retained in `bot-outbox-<pipeline hash>.json` under the
data directory (up to 100 per pipeline). A full queue stops new imports rather
than deleting undelivered content. Restore the failed destination and run the
pipeline again, or let its schedule retry. Current sink settings are used for
retries; changing a sink configuration treats it as a new destination. Disabling
a pipeline pauses its retries. Existing processed-item files remain compatible;
items marked processed by older versions are not automatically replayed.

Delivery is at-least-once: if a remote destination accepts a send just before a
crash, timeout, or local checkpoint failure, a retry may duplicate that send.
Successful local checkpoints prevent ordinary retries from resending to healthy
sinks. Unavailable source CIDs rotate through the polling budget so an old failed
prefix does not indefinitely block newer items.

Scheduled jobs retain only the last 64 KiB of combined stdout/stderr while they
run. The one-hour deadline includes waiting for output pipes to close. Timeout
ends the scheduler's wait and kills the shell if it is still running; detached
descendant processes are not yet fully supervised.

## Known limitations (v0.1)

- `stream` local capture is video-only (audio_capture is ignored); relayed shares carry
  audio. Inbound NACK is not implemented (loss shows until the next keyframe; the relay
  requests one every 5s)
- Cascade (`[stream] cascade`) v1: a follower's own PLI targets the leader's
  re-published track, which isn't forwarded back to the original sharer -- only the
  leader's PLI actually reaches it, so a late-joining follower's keyframe wait is
  bounded by the leader's ~5s PLI cadence rather than its own. A relay that's already
  locked onto the sharer when a new leader is elected doesn't re-request the sharer's
  tracks either; it relies on either already being connected to the new leader's
  re-publish or receiving a fresh negotiation
- No bot capability advertisement (every connected peer is treated as a bot candidate)
- `ai` implements the mistai protocol's chat, and TTS/STT once `ai.tts`/
  `ai.stt` are configured (otherwise voice requests get an immediate
  `voice_error`); `raft_message` scheduling is passed through untouched
- The local API server (`ai serve`) has no auth; keep `api_listen` on loopback unless
  the network is trusted. Browser pages from other origins are refused unless listed
  in `ai.api_allowed_origins`
- The web dashboard requires a session. Open it with `mistl ui`, the tray, or the URL
  shown by `mistl daemon status`; keep `[ui] listen` on loopback unless the network is
  trusted
