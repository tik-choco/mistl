import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { pipeline } from 'node:stream/promises';
import { decodeBands, expandClock } from './pattern.mjs';

const WIDTH = 320;
const HEIGHT = 180;
const FRAME_BYTES = WIDTH * HEIGHT;
const COUNTER_SPAN = 2 ** 24;
const HALF_COUNTER_SPAN = COUNTER_SPAN / 2;
const STARTUP_WINDOW_MS = 1000;
const FREEZE_THRESHOLD_MS = 250;
const FFMPEG_QUIT_TIMEOUT_MS = 2000;

export function percentile(values, fraction) {
  if (values.length === 0) return null;
  const sorted = values.toSorted((a, b) => a - b);
  const index = (sorted.length - 1) * fraction;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
}

export function parseShowinfo(line) {
  const match = line.match(/\[(?:Parsed_)?showinfo(?:_\d+)?\s*@[^\]]*\]\s+n:\s*\d+\b.*?\bs:(\d+)x(\d+)\b/);
  if (!match) return null;
  const ptsTime = Number(match[0].match(/\bpts_time:\s*(\S+)/)?.[1]);
  return { w: Number(match[1]), h: Number(match[2]), ptsTime: Number.isFinite(ptsTime) ? ptsTime : null };
}

export function parseFramecrcSize(line) {
  const match = line.match(/^0\s*,[^,]*,[^,]*,[^,]*,\s*(\d+)\s*,/);
  return match ? Number(match[1]) : null;
}

export function bitratePerSecond(packets, firstAt, wallSpanMs) {
  const bins = Math.ceil(wallSpanMs / 1000);
  const inWindow = packets.filter(({ receivedAt }) => receivedAt >= firstAt && receivedAt <= firstAt + wallSpanMs);
  if (!bins || !inWindow.length) {
    return { mean: null, p50: null, min: null, max: null, perSecond: [] };
  }

  const bytes = Array(bins).fill(0);
  for (const packet of inWindow) {
    const index = Math.floor((packet.receivedAt - firstAt) / 1000);
    bytes[Math.min(index, bins - 1)] += packet.bytes;
  }
  const perSecond = bytes.map((size, index) => size * 8 / Math.min(1000, wallSpanMs - index * 1000));
  return {
    mean: bytes.reduce((sum, size) => sum + size, 0) * 8 / wallSpanMs,
    p50: percentile(perSecond, 0.5),
    min: perSecond.reduce((min, rate) => Math.min(min, rate), Infinity),
    max: perSecond.reduce((max, rate) => Math.max(max, rate), -Infinity),
    perSecond,
  };
}

function expandCounter(counter, highest) {
  if (highest === undefined) return counter;
  // Place the wrapping 24-bit counter in the epoch nearest the high-water mark.
  return highest + ((counter - highest + HALF_COUNTER_SPAN) & (COUNTER_SPAN - 1)) - HALF_COUNTER_SPAN;
}

function measureStartup(frames) {
  const deadline = frames[0]?.receivedAt + STARTUP_WINDOW_MS;
  let highest;
  for (const [index, { counter, receivedAt }] of frames.entries()) {
    if (receivedAt > deadline) break;
    if (counter == null) continue;
    const current = expandCounter(counter, highest);
    if (current > highest + 1) {
      return { staleFrames: index, jumpFrames: current - highest - 1 };
    }
    // Cached frames can arrive out of order before the jump to live video.
    if (highest === undefined || current > highest) highest = current;
  }
  return { staleFrames: 0, jumpFrames: 0 };
}

export function measureCounterContinuity(frames, endedAt = frames.at(-1)?.receivedAt, expectedFps = 30) {
  const seen = new Set();
  const gaps = { events: 0, missingFrames: 0 };
  const stallMs = 1500 / expectedFps;
  const freezes = { thresholdMs: FREEZE_THRESHOLD_MS, events: 0, totalMs: 0, maxMs: 0 };
  let senderSkips = 0;
  let duplicates = 0;
  let reorders = 0;
  let highest;
  let previous;
  let previousAt;
  let lastNewAt;

  function finishFreeze(at) {
    const elapsed = at - lastNewAt;
    if (elapsed > FREEZE_THRESHOLD_MS) {
      freezes.events++;
      freezes.totalMs += elapsed;
      freezes.maxMs = Math.max(freezes.maxMs, elapsed);
    }
  }

  for (const { counter, receivedAt } of frames) {
    if (counter == null) continue;
    const current = expandCounter(counter, highest);
    if (current === previous) duplicates++;
    if (current < previous) reorders++;
    if (current > highest + 1) {
      const missing = current - highest - 1;
      if (missing === 1 && receivedAt - previousAt < stallMs) {
        senderSkips++;
      } else {
        gaps.events++;
        gaps.missingFrames += missing;
      }
    }
    if (highest === undefined || current > highest) highest = current;
    previous = current;
    previousAt = receivedAt;

    if (!seen.has(current)) {
      if (lastNewAt !== undefined) finishFreeze(receivedAt);
      seen.add(current);
      lastNewAt = receivedAt;
    }
  }
  if (lastNewAt !== undefined) finishFreeze(endedAt);

  return { uniqueFrames: seen.size, gaps, senderSkips, duplicates, reorders, freezes };
}

function measureMediaClock(frames, wallSpanMs, expectedFps) {
  const wallSpanSec = wallSpanMs / 1000;
  const first = frames[0]?.ptsTime;
  const last = frames.at(-1)?.ptsTime;
  const ptsSpanSec = first == null || last == null ? null : last - first;
  const jumpThreshold = 3 / expectedFps;
  // Small backward steps (well under one frame) are the reader re-anchoring
  // its clock on an RTCP sender report: measured identical on UDP and TCP at
  // the same frame, a few ms, not a playback discontinuity.
  const backwardTolerance = 0.5 / expectedFps;
  let jumps = 0;
  let maxJumpSec = 0;
  let previous;
  for (const { ptsTime } of frames) {
    if (previous != null && ptsTime != null) {
      const delta = ptsTime - previous;
      const tolerance = Number.EPSILON * Math.max(Math.abs(previous), Math.abs(ptsTime), jumpThreshold);
      if (delta < -backwardTolerance || delta - jumpThreshold > tolerance) {
        jumps++;
        maxJumpSec = Math.max(maxJumpSec, Math.abs(delta));
      }
    }
    previous = ptsTime;
  }
  return {
    ptsSpanSec,
    wallSpanSec,
    rate: wallSpanSec > 0 && ptsSpanSec != null ? ptsSpanSec / wallSpanSec : null,
    jumps,
    maxJumpSec,
  };
}

export function summarizeFrames(frames, { startedAt, expectedFps = 30 } = {}) {
  const firstFrameAt = frames[0]?.receivedAt ?? null;
  const startup = measureStartup(frames);
  const steadyFrames = frames.slice(startup.staleFrames);
  const wallSpanMs = steadyFrames.length ? Math.max(0, steadyFrames.at(-1).receivedAt - steadyFrames[0].receivedAt) : 0;
  const continuity = measureCounterContinuity(steadyFrames, undefined, expectedFps);
  const resolution = { distribution: {}, changes: 0 };
  let previousSize;
  let framesIdentified = 0;
  const latencies = [];
  for (const [index, frame] of frames.entries()) {
    if (frame.w != null && frame.h != null) {
      const size = `${frame.w}x${frame.h}`;
      resolution.distribution[size] = (resolution.distribution[size] ?? 0) + 1;
      if (previousSize && size !== previousSize) resolution.changes++;
      previousSize = size;
    }
    if (frame.counter == null) continue;
    framesIdentified++;
    if (index >= startup.staleFrames && frame.clockLow32 != null) {
      latencies.push(frame.receivedAt - expandClock(frame.clockLow32, frame.receivedAt));
    }
  }
  return {
    firstFrameAt,
    startupMs: firstFrameAt != null && startedAt != null ? firstFrameAt - startedAt : null,
    startup,
    framesDecoded: frames.length,
    framesIdentified,
    unidentifiedFrames: frames.length - framesIdentified,
    ...continuity,
    wallSpanMs,
    fps: wallSpanMs > 0 ? Math.max(0, continuity.uniqueFrames - 1) * 1000 / wallSpanMs : 0,
    mediaClock: { firstPts: frames[0]?.ptsTime ?? null, ...measureMediaClock(steadyFrames, wallSpanMs, expectedFps) },
    resolution,
    latencyMs: {
      p50: percentile(latencies, 0.5),
      p95: percentile(latencies, 0.95),
      max: latencies.reduce((max, latency) => max == null ? latency : Math.max(max, latency), null),
    },
  };
}

export function createFrameReader(onFrame) {
  const frame = Buffer.allocUnsafe(FRAME_BYTES);
  let filled = 0;
  return chunk => {
    for (let offset = 0; offset < chunk.length;) {
      const copied = chunk.copy(frame, filled, offset, offset + FRAME_BYTES - filled);
      offset += copied;
      filled += copied;
      if (filled === FRAME_BYTES) {
        onFrame(frame);
        filled = 0;
      }
    }
  };
}

const decodeError = /error (?:while )?decod|error submitting packet to decoder|decod\w*.*\berror\b|invalid nal|non-existing pps|no frame!|corrupt decoded frame|could not find ref with poc|concealing \d+ .*errors/i;

export async function analyzeRtsp({ url, transport, durationMs, startedAt, expectedFps = 30, outDir, ffmpeg = 'ffmpeg' }) {
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    throw new RangeError('durationMs must be positive and finite');
  }
  if (!Number.isFinite(expectedFps) || expectedFps <= 0) {
    throw new RangeError('expectedFps must be positive and finite');
  }
  if (outDir) await mkdir(outDir, { recursive: true });

  const inputOptions = ['-fflags', 'nobuffer', '-flags', 'low_delay'];
  if (transport != null) inputOptions.push('-rtsp_transport', transport);
  if (transport === 'udp') inputOptions.push('-max_delay', '0', '-reorder_queue_size', '0');
  const child = spawn(ffmpeg, [
    '-hide_banner', '-nostats', '-stdin', '-loglevel', 'repeat+info', ...inputOptions, '-i', url,
    '-map', '0:v:0', '-vf', `showinfo,scale=${WIDTH}:${HEIGHT},format=gray`,
    '-fps_mode', 'passthrough', '-c:v', 'rawvideo', '-threads:v', '1',
    '-flush_packets', '1', '-f', 'rawvideo', 'pipe:1',
    '-map', '0:v:0', '-c', 'copy', '-flush_packets', '1', '-f', 'framecrc', 'pipe:3',
  ], { stdio: ['pipe', 'pipe', 'pipe', 'pipe'], windowsHide: true });

  const frames = [];
  const frameInfo = [];
  const decodeErrors = { count: 0, samples: [] };
  const packets = [];
  const packetLines = createInterface({ input: child.stdio[3], crlfDelay: Infinity });
  packetLines.on('line', line => {
    const receivedAt = Date.now();
    const bytes = parseFramecrcSize(line);
    if (bytes != null) packets.push({ receivedAt, bytes });
  });
  child.stdout.on('data', createFrameReader(frame => {
    const receivedAt = Date.now();
    const pattern = decodeBands((x, y) => frame[y * WIDTH + x], WIDTH, HEIGHT);
    frames.push({ receivedAt, counter: pattern?.counter ?? null, clockLow32: pattern?.clockLow32 ?? null });
  }));
  const lines = createInterface({ input: child.stderr, crlfDelay: Infinity });
  lines.on('line', line => {
    const info = parseShowinfo(line);
    if (info) frameInfo.push(info);
    if (decodeError.test(line)) {
      decodeErrors.count++;
      if (decodeErrors.samples.length < 10) decodeErrors.samples.push(line);
    }
  });

  const exited = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.once('close', code => resolve(code ?? 1));
  });
  // The process can exit while the stop request is being written.
  child.stdin.on('error', () => {});
  const log = outDir ? pipeline(child.stderr, createWriteStream(join(outDir, 'ffmpeg.stderr.log'))) : null;
  let killTimeout;
  const timeout = setTimeout(() => {
    child.stdin.end('q');
    killTimeout = setTimeout(() => child.kill('SIGKILL'), FFMPEG_QUIT_TIMEOUT_MS);
  }, durationMs);
  try {
    const [ffmpegExit] = await Promise.all([exited, log]);
    for (const [index, frame] of frames.entries()) Object.assign(frame, frameInfo[index]);
    if (outDir) {
      const csv = ['receivedAt,counter,clockLow32,w,h,ptsTime'];
      for (const { receivedAt, counter, clockLow32, w, h, ptsTime } of frames) {
        csv.push([receivedAt, counter, clockLow32, w, h, ptsTime].join(','));
      }
      await writeFile(join(outDir, 'frames.csv'), csv.join('\n') + '\n');
    }
    const metrics = summarizeFrames(frames, { startedAt, expectedFps });
    return {
      transport, url, durationMs,
      ...metrics,
      bitrateKbps: bitratePerSecond(packets, frames[metrics.startup.staleFrames]?.receivedAt, metrics.wallSpanMs),
      decodeErrors,
      ffmpegExit,
    };
  } finally {
    clearTimeout(timeout);
    clearTimeout(killTimeout);
    if (child.exitCode == null && child.signalCode == null && child.pid) child.kill('SIGKILL');
  }
}
