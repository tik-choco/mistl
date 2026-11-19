# stream-lab design (contract between the parts)

Goal: one command runs a real Chrome screen-share sender -> real mistl relay ->
RTSP readers that stand in for VRChat's AVPro, and prints objective numbers
(lock success, startup time, fps, drops, freezes, resolution changes, bitrate,
decode errors, end-to-end latency, browser encoder stats) plus pass/fail.
No human in the loop except a final VRChat look.

Only the edges are replaced; the code under test stays real:

```
page/ (real Chrome + real mistlib-wasm, synthetic content)  --WebRTC-->  mistl daemon (dev build, isolated state dir) relay
                                                                              | RTSP (rtsp:// UDP and rtspt:// TCP)
                                                                              v
                                                              analyze.mjs (ffmpeg decode + pattern.mjs)
run.mjs orchestrates everything, browser.mjs drives Chrome over CDP.
```

## Files and owners

| File | Purpose |
| --- | --- |
| `pattern.mjs` | Frame-identity bands (counter + sender clock). The contract; don't change without updating both sides. |
| `page/index.html`, `page/sender.mjs` | Sender page. |
| `browser.mjs` | Launch Chrome/Edge headless, minimal CDP client. |
| `analyze.mjs` | RTSP reader + metrics. `analyze.test.mjs` unit tests. |
| `run.mjs` | Orchestrator, report, thresholds. |
| `../../docs/stream-lab.md` | User doc. |

The runner uses Node >= 22 built-ins (global `WebSocket`, `fetch`,
`node:http`, `node:child_process`, `node:test`). The browser packages can be
installed with npm into `target/stream-lab/npm/`. ffmpeg/ffprobe come from PATH.

## Interfaces

### Sender page (`page/index.html?...`)
Query parameters (all optional except `room` and `node`):
`room`, `node` (browser node id), `w`=1920, `h`=1080, `fps`=30,
`capture`=`tab|canvas` (default `tab`: `getDisplayMedia({preferCurrentTab:true})`
of the page itself, i.e. the real screen-share capture path; `canvas`:
`canvas.captureStream(fps)`), `audio`=1 (WebAudio tone), `maxBitrate` (bps,
default 8000000; `0` = don't call `set_local_track_encoding`), `maxFps`=30,
`hint` (`''|detail|motion|text`, default `''`), `degradation`
(`''|balanced|maintain-framerate|maintain-resolution`), `encoding`=1.

The page draws a full-viewport canvas: `pattern.mjs` bands at the top, moving
content below (a scrolling gradient, a moving box, small text that changes
every frame) so the encoder sees screen-like detail and motion. Counter
increments once per drawn frame; clock = `Date.now()` at draw time.

The page exposes `window.__lab` for the orchestrator:
```
{
  state: 'init'|'joining'|'joined'|'capturing'|'publishing'|'error',
  error: string|null,
  nodeId, room,
  startCapture(): Promise<void>,   // called by browser.mjs with userGesture:true
  framesDrawn: number,
  publishedAt: number|null,        // Date.now() when publish_local_track was called
  encodingApplied: boolean|null,   // set_local_track_encoding present & succeeded
  stats: Array<{t, pcs: [...]}>,   // 1 Hz samples, see below
  log: string[],                   // console errors/warnings captured
}
```
`stats` comes from wrapping `window.RTCPeerConnection` before mistlib loads
(capture every instance), calling `getStats()` once per second and keeping for
each pc: `connectionState`, `signalingState`, the video `outbound-rtp`
(`frameWidth`, `frameHeight`, `framesPerSecond`, `framesSent`, `bytesSent`,
`targetBitrate`, `qualityLimitationReason`, `qualityLimitationDurations`,
`nackCount`, `pliCount`, `firCount`, `retransmittedBytesSent`,
`encoderImplementation`), `remote-inbound-rtp` (`packetsLost`,
`roundTripTime`, `fractionLost`) and the selected candidate pair
(`availableOutgoingBitrate`, `currentRoundTripTime`). Cap the array (e.g. 600).

mistlib is loaded via an import map: `@tik-choco/mistlib` -> `/mistlib/`
(a mistlib-wasm `pkg` dir), `@tik-choco/mistlib-web` -> `/mistlib-web/`
(the wrapper's dir). The page mirrors tc-chat's `MistNode` setup with
`new MistNode(nodeId, {signaling:{mode:'nostr', nostr:{relays:[],
discoveryKind, messageKind, ttlSeconds, inviteSalt:'tik-choco-v1',
inviteCode:'tik-choco-public-v1'}}})`, `joinRoomAsync(room)`, `registerLocalTrack(id, track,
{publish:true, enabled:true})` with ids `tc-chat-screen:<uuid>` /
`tc-chat-screen-audio:<uuid>` (the relay doesn't care, but stay identical),
then `set_local_track_encoding` from the raw engine module if present.

### browser.mjs
```js
export async function launchBrowser({ executable, headless = true, userDataDir, extraArgs = [] })
  -> { cdp, close(): Promise<void> }
export async function openPage(browser, url) -> {
  evaluate(expr: string, { userGesture = false, awaitPromise = true } = {}): Promise<any>,
  waitFor(expr: string, timeoutMs: number): Promise<any>,   // polls until truthy
  consoleMessages: string[],
}
export function findBrowserExecutable(): string   // Chrome, then Edge; env STREAM_LAB_BROWSER overrides
```
Launch flags include `--auto-accept-this-tab-capture`,
`--use-fake-ui-for-media-stream`, `--autoplay-policy=no-user-gesture-required`,
`--disable-background-timer-throttling`, `--disable-renderer-backgrounding`,
`--disable-backgrounding-occluded-windows`, `--remote-debugging-port=0`
(read the port from `<userDataDir>/DevToolsActivePort`), a fixed
`--window-size` matching w x h. Headless = `--headless=new`.

### analyze.mjs
```js
export async function analyzeRtsp({
  url,                 // rtsp://host:port/path
  transport,           // 'udp' | 'tcp'
  durationMs,
  startedAt,           // Date.now() when the sender published (for startup time); optional
  outDir,              // write ffmpeg stderr log + per-frame CSV here
  ffmpeg = 'ffmpeg',
}) -> Promise<Metrics>
```
One ffmpeg process per call: `-rtsp_transport <t> -i <url> -t <s>`; output 1
`-map 0:v -vf showinfo,scale=320:180,format=gray -f rawvideo pipe:1`
(frames for pattern decode; showinfo on stderr gives the decoded resolution
per frame), output 2 `-map 0:v -c copy -f framecrc pipe:3` (packet sizes ->
bitrate). Timestamp each decoded frame with `Date.now()` when its bytes are
complete on stdout.

`Metrics`:
```
{ transport, url, durationMs,
  firstFrameAt, startupMs,                  // first decoded frame (relative to startedAt if given)
  framesDecoded, framesIdentified, unidentifiedFrames,
  uniqueFrames, fps,                        // unique counters per second over the window
  gaps: {events, missingFrames},            // counter jumps > 1
  duplicates,                               // same counter seen again consecutively
  reorders,                                 // counter went backwards
  freezes: {thresholdMs: 250, events, totalMs, maxMs},   // no new counter for > threshold
  resolution: {distribution: {"WxH": frames}, changes},
  bitrateKbps: {mean, p50, min, max, perSecond: [...]},
  latencyMs: {p50, p95, max},               // receive Date.now() - expanded sender clock
  decodeErrors: {count, samples: [first 10 lines]},
  ffmpegExit }
```

### run.mjs
`node tools/stream-lab/run.mjs [options]` (also `just stream-lab ...`):
`--bin <mistl exe>` (default `target/release/mistl(.exe)`), `--duration 30`
(seconds of measurement after lock), `--order browser-first|native-first|both`
(default both), `--transport udp|tcp|both` (default both, measured
concurrently), page passthrough (`--w --h --fps --capture --audio
--max-bitrate --max-fps --hint --degradation`), `--mistlib <pkg dir>`,
`--mistlib-web <dir>`, `--mistlib-tag <tag>`, `--refresh-npm`, `--headed`,
`--label <name>`, `--out <dir>` (default `target/stream-lab/<timestamp>-<label>`),
`--keep` (keep state dirs), threshold overrides.

Resolve each browser package from its CLI flag, then `STREAM_LAB_MISTLIB` or
`STREAM_LAB_MISTLIB_WEB`, then the published packages under
`target/stream-lab/npm/`. npm installs both packages using the requested tag
(`beta` by default) when a path needs the fallback. Reuse that install for
the same tag unless `--refresh-npm` is set.

Per scenario: fresh `mkdtemp` state dir, `--state-dir <dir> --instance lab
--no-tray` for every CLI call; `daemon run` spawned in the background with
its stderr captured to `<out>/<scenario>/daemon.log`; `network on`; random
room id; `stream relay --room <room>`; native node id = first 16 hex of
sha256(did) (`key did`). Offer order: mistlib's lower node id initiates
(verify in mistlib source and in the log: the first `[IceState] peer=<browser>
... kind=signaling state=` line must be `HaveRemoteOffer` for browser-first and
`HaveLocalOffer` for native-first — record which one actually happened in the
report). Browser node id: a UUID-shaped id that sorts below (`00000000-...`)
or above (`ffffffff-...`) the native id.

Then: serve `page/` + `pattern.mjs` + mistlib dirs over a local HTTP server,
open the page, `startCapture()` with a user gesture, wait for
`stream status` `publisher` (timeout 60 s -> scenario result `no_lock`), run
`analyzeRtsp` for each transport concurrently, sample `stream status` every
second, collect `window.__lab.stats`, stop everything (browser, daemon,
server), summarize.

Report: `<out>/report.json` (all raw metrics per scenario + environment:
mistl build info, both mistlib package versions, browser version, params) and
`<out>/summary.md` (one table row per scenario x transport). Print the summary.
Thresholds (defaults; exit code 1 if any scenario fails): locked, startupMs <=
10000, fps >= 0.8 * requested fps, freezes.events == 0 (> 250 ms), gaps.missingFrames
<= 1% of uniqueFrames, decodeErrors.count == 0, resolution.changes <= 2.
Clean up all spawned processes on exit, including on Ctrl+C and on failure.
