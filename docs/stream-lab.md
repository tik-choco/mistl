# Stream lab

The stream lab runs a browser screen share through the real mistl relay and
decodes the relay's output with FFmpeg over both UDP and TCP. Use it to compare
builds or encoder settings and check whether initial WebRTC offer order affects
relay lock. The synthetic feed carries a frame counter and sender clock so the
lab can measure startup, unique-frame rate, dropped frames, freezes, and
end-to-end latency. It also records bitrate, decode errors, and resolution
changes and compares video presentation timestamps with elapsed wall time.
[Stream relay findings](stream-findings.md)

## Run

Use a release-optimized mistl build, Node.js 22 or later, Chrome or Edge, and
FFmpeg and FFprobe on PATH. Both the browser and daemon need to reach public
Nostr signaling. The lab also needs a mistlib wasm package and the mistlib-web
wrapper. Run from the mistl repository root.

```console
just stream-lab
node tools/stream-lab/run.mjs --order native-first --transport tcp --duration 20 --label native-tcp
```

The default run creates a fresh daemon state directory, browser profile, and
random room for each of the two offer orders. It starts both RTSP readers
concurrently after the relay locks onto the test publisher. The lab waits up
to 60 seconds after publication for lock and reports `no_lock` if it times
out. The runner stops its browser and daemon after each scenario.

| Option | Default and usage |
| --- | --- |
| `--bin <path>` | `target/release/mistl` (`mistl.exe` on Windows) |
| `--duration <seconds>` | `30` wall-clock seconds per reader after lock. The analyzer sends FFmpeg `q` at the deadline and kills it if it has yet to exit after two more seconds. |
| `--order browser-first\|native-first\|both` | `both` |
| `--transport udp\|tcp\|both` | `both` |
| `--w <pixels>` and `--h <pixels>` | `1920` × `1080` |
| `--fps <rate>` | `30` frames per second drawn by the sender |
| `--capture tab\|canvas` | `tab` captures the browser tab through `getDisplayMedia`. Use `canvas` to capture the canvas directly if tab capture fails. |
| `--audio 0\|1` | `1` adds a synthetic audio tone |
| `--max-bitrate <bps>` | `8000000`. Use `0` to skip applying the encoding parameters. |
| `--max-fps <rate>` | `30` sets the encoder's frame-rate cap |
| `--hint detail\|motion\|text` | Empty by default. Sets the video track's content hint. |
| `--degradation balanced\|maintain-framerate\|maintain-resolution` | Empty by default. Sets the encoder's degradation preference. |
| `--mistlib <pkg dir>` | Override the mistlib WASM package directory |
| `--mistlib-web <dir>` | Override the mistlib-web wrapper directory |
| `--mistlib-tag <tag>` | `beta`. Select the npm dist-tag for both published packages. |
| `--refresh-npm` | Reinstall both published packages when the run uses the npm fallback |
| `--headed` | Show the browser window. The default is headless. |
| `--label <name>` | `run`. Used in the default output directory name. |
| `--out <dir>` | `target/stream-lab/<timestamp>-<label>` |
| `--keep` | Preserve temporary daemon state and browser profiles. Paths are recorded in `report.json`. |

### Choose the mistlib packages

The lab resolves each package independently in this order.

1. Pass `--mistlib <pkg dir>` or `--mistlib-web <dir>` for the run.
2. Set `STREAM_LAB_MISTLIB` or `STREAM_LAB_MISTLIB_WEB` to point to a package
   directory.
3. Leave the path unset to use the published packages. The lab uses npm to
   install `@tik-choco/mistlib` and `@tik-choco/mistlib-web` under
   `target/stream-lab/npm/`. The first install needs registry access. Later
   runs reuse the install for the same tag. Use `--refresh-npm` to reinstall
   the current tag or `--mistlib-tag` to select a different tag.

To test an unreleased WASM build, run `wasm-pack build --target web` in the
mistlib WASM crate and set `STREAM_LAB_MISTLIB` to the generated `pkg`
directory. Leave the wrapper path unset to use the published wrapper, or set
`STREAM_LAB_MISTLIB_WEB` to a compatible local wrapper directory.

Set `STREAM_LAB_BROWSER` to a browser executable path to override detection.
If the selected wasm build exports `set_local_track_encoding`, the sender
records whether applying the encoding parameters succeeded.

## Read the report

- `summary.md` has one row per offer order and RTSP transport. The runner also
  prints this table. The observed order comes from the first daemon
  `IceState` signaling line for that browser peer. `HaveRemoteOffer` means
  browser-first and `HaveLocalOffer` means native-first. The table includes the
  steady-state wall span, first decoded PTS, clock rate, and jump count.
- `report.json` includes parameters, build, browser, and mistlib package versions, publisher
  lock, once-per-second stream status, browser WebRTC stats, and per-transport
  metrics. `startupMs` runs from browser publication to the first decoded
  frame. If the counter first jumps past a missing value within one second
  of decoding, `startup.staleFrames` counts the decoded frames before the
  jump and `startup.jumpFrames` counts the skipped counters at the jump.
  Both counts are zero when there is no startup jump. The landing frame
  starts the steady-state FPS, continuity, and latency measurements.
  `wallSpanMs` runs from the first steady-state frame received to the last.
  FPS divides the number of unique-frame intervals by that span. Freeze timing
  ends at the last received frame. Bitrate counts compressed video packet bytes
  observed through FFmpeg's packet output during the same receive window.
  `senderSkips` counts single missing counters when the time between identified
  frames is less than 1.5 frame intervals at the requested `--fps`.
  `gaps.missingFrames` counts missing counters when the interval is at least
  1.5 frame intervals or the jump skips multiple counters.
  `mediaClock.firstPts` records the first decoded presentation timestamp in
  seconds, including startup. `ptsSpanSec` and `wallSpanSec` cover the
  steady-state endpoints. `rate` divides the PTS span by the wall span and is
  unavailable if the wall span is zero or either endpoint lacks a PTS.
  `jumps` counts backwards PTS changes and forward changes larger than three
  frame intervals at the requested FPS. `maxJumpSec` is the largest absolute
  flagged change, or zero if there were none. Drift and jumps can affect players
  such as AVPro that pace playback by presentation timestamps.
  Latency runs from drawing a frame in the browser to receiving its decoded
  bytes with FFmpeg's low-latency input settings. It includes FFmpeg decode
  and pipe time and is intended for comparing builds. Compare browser outbound
  resolution, target bitrate, quality-limitation reason, and available outgoing
  bitrate with the receiver metrics to investigate encoder adaptation.
- Each scenario directory contains `daemon.log`. Its transport directories
  contain `ffmpeg.stderr.log` and `frames.csv` when the reader produced them.
  The CSV contains receive times, frame counters, sender-clock values, and
  decoded frame dimensions, and `ptsTime` in seconds from FFmpeg's `showinfo`.

## Pass thresholds

The run exits with code 1 if any scenario or transport fails. Every scenario
must lock onto its publisher and confirm its requested offer order.

| Metric | Default | Override |
| --- | --- | --- |
| Startup | ≤ 10,000 ms | `--max-startup-ms` |
| Unique-frame rate | ≥ 80% of requested `--fps` | `--min-fps-ratio` |
| Freeze events | 0 events with a gap over 250 ms | `--max-freezes` |
| Missing counters in steady-state gaps | ≤ 1% of steady-state unique frames | `--max-missing-ratio` |
| Decode errors | 0 | `--max-decode-errors` |
| Resolution changes | ≤ 2 | `--max-resolution-changes` |
| Media clock drift | `rate` within 0.05 of 1.0 | `--max-media-clock-drift` |
| Media clock jumps | 0 | `--max-media-clock-jumps` |

The initial PTS is informational. The drift and jump thresholds apply to the
steady-state timestamps, and an unavailable rate fails the drift threshold.

Use decimals for the ratio overrides, such as `--min-fps-ratio 0.9` or
`--max-missing-ratio 0.02`.

## Limits

AVPro playback and buffering require a separate measurement in VRChat. Follow the
[manual stream verification guide](stream-verify.md) for in-world checks.
The lab measures video continuity and uses a synthetic scene. The tone
exercises the audio track, while the report's frame metrics describe video.
Public Nostr reachability and relay responsiveness can affect room join and
publisher lock.
