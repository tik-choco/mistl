import assert from 'node:assert/strict';
import childProcess from 'node:child_process';
import { EventEmitter } from 'node:events';
import { syncBuiltinESMExports } from 'node:module';
import { PassThrough } from 'node:stream';
import { test } from 'node:test';
import {
  analyzeRtsp,
  bitratePerSecond,
  createFrameReader,
  measureCounterContinuity,
  parseFramecrcSize,
  parseShowinfo,
  percentile,
  summarizeFrames,
} from './analyze.mjs';

function csvFrames(rows) {
  return rows.trim().split('\n').map(row => {
    const [receivedAt, counter, clockLow32, w, h] = row.split(',');
    return {
      receivedAt: Number(receivedAt), counter: counter ? Number(counter) : null,
      clockLow32: clockLow32 ? Number(clockLow32) : null, w: Number(w), h: Number(h),
    };
  });
}

test('percentiles interpolate without changing the input', () => {
  const values = [40, 10, 30, 20];
  assert.equal(percentile(values, 0.5), 25);
  assert.equal(percentile([0, 100], 0.95), 95);
  assert.equal(percentile(values, 0), 10);
  assert.equal(percentile(values, 1), 40);
  assert.equal(percentile([15], 0.95), 15);
  assert.equal(percentile([], 0.5), null);
  assert.deepEqual(values, [40, 10, 30, 20]);
});

test('showinfo parsing uses the decoded dimensions and accepts a reset frame index', () => {
  const prefix = '[Parsed_showinfo_0 @ 000001ed97ed0700]';
  assert.deepEqual(parseShowinfo(`${prefix} n:  13 pts:39000 pts_time:0.4333 fmt:yuv420p sar:1/1 s:640x360 i:P`),
    { w: 640, h: 360, ptsTime: 0.4333 });
  assert.deepEqual(parseShowinfo(`${prefix} n:   0 pts:42000 pts_time:0.4667 fmt:yuv420p sar:1/1 s:1920x1080 i:P`),
    { w: 1920, h: 1080, ptsTime: 0.4667 });
  assert.deepEqual(parseShowinfo(`${prefix} n: 1 pts:NOPTS pts_time:NOPTS s:640x360`),
    { w: 640, h: 360, ptsTime: null });
  assert.equal(parseShowinfo(`${prefix} config in time_base: 1/90000, frame_rate: 30/1`), null);
  assert.equal(parseShowinfo(`${prefix} color_range:unknown color_space:unknown`), null);
  assert.equal(parseShowinfo('[h264 @ 123] n: 0 pts:0 s:640x360'), null);
});

test('framecrc reads the video packet size even when media timestamps are missing', () => {
  assert.equal(parseFramecrcSize('0, -3000, 6000, 3000, 12000, 0x23ab3421'), 12000);
  assert.equal(parseFramecrcSize('0, 6000, 3000, 3000, 900, 0x94928912, S=1, 12, 0xa71b'), 900);
  assert.equal(parseFramecrcSize('0, N/A, N/A, 3000, 9999, 0x12345678'), 9999);
  assert.equal(parseFramecrcSize('1, 0, 0, 3000, 9999, 0x12345678'), null);
  assert.equal(parseFramecrcSize('0, 0, 0, 3000, invalid, 0x12345678'), null);
  assert.equal(parseFramecrcSize('#tb 0: 1/90000'), null);
});

test('bitrate uses packets inside the steady receive window with empty and partial seconds', () => {
  const packets = [
    { receivedAt: 999, bytes: 999 },
    { receivedAt: 1000, bytes: 750 },
    { receivedAt: 1999, bytes: 250 },
    { receivedAt: 3500, bytes: 500 },
    { receivedAt: 3501, bytes: 999 },
  ];
  assert.deepEqual(bitratePerSecond(packets, 1000, 2500), {
    mean: 4.8, p50: 8, min: 0, max: 8, perSecond: [8, 0, 8],
  });
  assert.deepEqual(bitratePerSecond(packets, 2000, 1000), {
    mean: null, p50: null, min: null, max: null, perSecond: [],
  });
});

test('continuity counts gaps, consecutive duplicates and backwards counters', () => {
  const frames = [10, 11, 11, null, 14, 12, 14, 15].map((counter, index) => ({
    counter, receivedAt: index * 33,
  }));
  assert.deepEqual(measureCounterContinuity(frames), {
    uniqueFrames: 5,
    gaps: { events: 1, missingFrames: 2 },
    senderSkips: 0,
    duplicates: 1,
    reorders: 1,
    freezes: { thresholdMs: 250, events: 0, totalMs: 0, maxMs: 0 },
  });
});

test('continuity treats the 24-bit counter wrap as forward progress', () => {
  const frames = [0xfffffe, 0xffffff, 0, 1, 0xffffff, 1, 3].map((counter, index) => ({
    counter, receivedAt: index * 33,
  }));
  const { uniqueFrames, gaps, senderSkips, duplicates, reorders } = measureCounterContinuity(frames);
  assert.equal(uniqueFrames, 5);
  assert.deepEqual(gaps, { events: 0, missingFrames: 0 });
  assert.equal(senderSkips, 1);
  assert.equal(duplicates, 0);
  assert.equal(reorders, 1);
});

test('freeze timing ignores startup, includes the final stall and uses a strict threshold', () => {
  const frames = [
    { receivedAt: 0, counter: null },
    { receivedAt: 400, counter: 10 },
    { receivedAt: 600, counter: 10 },
    { receivedAt: 650, counter: 11 },
    { receivedAt: 800, counter: null },
    { receivedAt: 901, counter: 12 },
    { receivedAt: 1000, counter: 10 },
    { receivedAt: 1050, counter: 12 },
  ];
  assert.deepEqual(measureCounterContinuity(frames, 1301).freezes,
    { thresholdMs: 250, events: 2, totalMs: 651, maxMs: 400 });
  assert.equal(measureCounterContinuity([{ receivedAt: 0, counter: null }], 1000).freezes.events, 0);
});

test('startup excludes the cached UDP frames before the live jump in smoke2', () => {
  const frames = csvFrames(`
1795074318951,78,4072954302,1904,984
1795074318976,,,1904,984
1795074318981,79,4072954367,1904,984
1795074318985,,,1904,984
1795074318989,,,1904,984
1795074318992,,,1904,984
1795074318996,,,1904,984
1795074319000,,,1904,984
1795074319004,,,1904,984
1795074319007,,,1904,984
1795074319012,,,1904,984
1795074319016,76,4072954250,1904,984
1795074319020,77,4072954316,1904,984
1795074319029,78,4072954317,1904,984
1795074319041,79,4072955918,1904,984
1795074319049,129,4072956003,1904,984
1795074319057,130,4072956035,1904,984
`);
  const metrics = summarizeFrames(frames, { expectedFps: 30 });
  assert.deepEqual(metrics.startup, { staleFrames: 15, jumpFrames: 49 });
  assert.equal(metrics.firstFrameAt, frames[0].receivedAt);
  assert.equal(metrics.framesDecoded, 17);
  assert.equal(metrics.framesIdentified, 8);
  assert.equal(metrics.uniqueFrames, 2);
  assert.equal(metrics.wallSpanMs, 8);
  assert.equal(metrics.fps, 125);
  assert.equal(metrics.duplicates, 0);
  assert.equal(metrics.reorders, 0);
  assert.deepEqual(metrics.gaps, { events: 0, missingFrames: 0 });
  assert.equal(metrics.senderSkips, 0);
  assert.deepEqual(metrics.latencyMs, { p50: 602, p95: 612.8, max: 614 });
});

test('a startup jump at one second drops stale freeze time but a later jump stays a gap', () => {
  const frames = [{ counter: 79, receivedAt: 0 }, { counter: 129, receivedAt: 1000 }];
  const startup = summarizeFrames(frames);
  assert.deepEqual(startup.startup, { staleFrames: 1, jumpFrames: 49 });
  assert.equal(startup.freezes.events, 0);

  frames[1].receivedAt++;
  const steady = summarizeFrames(frames);
  assert.deepEqual(steady.startup, { staleFrames: 0, jumpFrames: 0 });
  assert.deepEqual(steady.gaps, { events: 1, missingFrames: 49 });
  assert.equal(steady.freezes.events, 1);
});

test('the matching UDP and TCP skips in smoke2 depend on the expected FPS', () => {
  for (const prevTime of [1795074325315, 1795074325314]) {
    const frames = csvFrames(`
${prevTime},318,4072962302,1904,984
1795074325354,320,4072962368,1904,984
`);
    const at30 = measureCounterContinuity(frames, undefined, 30);
    assert.equal(at30.senderSkips, 1);
    assert.deepEqual(at30.gaps, { events: 0, missingFrames: 0 });

    const at40 = measureCounterContinuity(frames, undefined, 40);
    assert.equal(at40.senderSkips, 0);
    assert.deepEqual(at40.gaps, { events: 1, missingFrames: 1 });
  }
});

test('a stall at 1.5 frame intervals or a jump over multiple counters counts as a gap', () => {
  const frames = [
    { counter: 10, receivedAt: 0 },
    { counter: 12, receivedAt: 50 },
    { counter: 15, receivedAt: 60 },
  ];
  const { gaps, senderSkips } = measureCounterContinuity(frames, undefined, 30);
  assert.deepEqual(gaps, { events: 2, missingFrames: 3 });
  assert.equal(senderSkips, 0);
});

test('frame metrics use receive time, original resolution and wrap-aware sender clocks', () => {
  const wrap = 2 ** 32;
  const metrics = summarizeFrames([
    { receivedAt: wrap - 10, counter: null, clockLow32: null, w: 640, h: 360 },
    { receivedAt: wrap + 5, counter: 10, clockLow32: wrap - 15, w: 640, h: 360 },
    { receivedAt: wrap + 30, counter: 11, clockLow32: 20, w: 1920, h: 1080 },
  ], { startedAt: wrap - 100 });
  assert.equal(metrics.firstFrameAt, wrap - 10);
  assert.equal(metrics.startupMs, 90);
  assert.equal(metrics.framesDecoded, 3);
  assert.equal(metrics.framesIdentified, 2);
  assert.equal(metrics.unidentifiedFrames, 1);
  assert.equal(metrics.uniqueFrames, 2);
  assert.equal(metrics.wallSpanMs, 40);
  assert.equal(metrics.fps, 25);
  assert.deepEqual(metrics.resolution, { distribution: { '640x360': 2, '1920x1080': 1 }, changes: 1 });
  assert.deepEqual(metrics.latencyMs, { p50: 15, p95: 19.5, max: 20 });
});

test('an empty frame stream has no startup, resolution or latency observations', () => {
  const metrics = summarizeFrames([]);
  assert.equal(metrics.firstFrameAt, null);
  assert.equal(metrics.startupMs, null);
  assert.equal(metrics.framesDecoded, 0);
  assert.equal(metrics.fps, 0);
  assert.equal(metrics.wallSpanMs, 0);
  assert.deepEqual(metrics.mediaClock,
    { firstPts: null, ptsSpanSec: null, wallSpanSec: 0, rate: null, jumps: 0, maxJumpSec: 0 });
  assert.deepEqual(metrics.resolution, { distribution: {}, changes: 0 });
  assert.deepEqual(metrics.latencyMs, { p50: null, p95: null, max: null });
});

test('before/after UDP captures report the receive span and preserve the startup PTS', () => {
  // First decoded, first steady and last decoded rows, paired with their showinfo lines.
  const captures = [
    {
      csv: `1795075230236,371,4073867692,1904,984
1795075231114,399,4073868625,1904,984
1795075241545,712,4073879058,1904,984`,
      showinfo: [
        '[Parsed_showinfo_0 @ 000001cd627d9500] n:   0 pts: 772939 pts_time:8.588211 duration:   3000 duration_time:0.0333333 fmt:yuv420p cl:left sar:1/1 s:1904x984',
        '[Parsed_showinfo_0 @ 000001cd627d9500] n:  27 pts: 857449 pts_time:9.527211 duration:   3000 duration_time:0.0333333 fmt:yuv420p cl:left sar:1/1 s:1904x984',
        '[Parsed_showinfo_0 @ 000001cd627d9500] n: 339 pts:1796020 pts_time:19.955778 duration:   3000 duration_time:0.0333333 fmt:yuv420p cl:left sar:1/1 s:1904x984',
      ],
      decodedSpanMs: 11_309,
      wallSpanMs: 10_431,
      firstPts: 8.588211,
      rate: 0.999767,
    },
    {
      csv: `1795075319165,123,4073956581,1904,984
1795075320079,153,4073957582,1904,984
1795075337929,689,4073975448,1904,984`,
      showinfo: [
        '[Parsed_showinfo_0 @ 0000027c3072f0c0] n:   0 pts:  97950 pts_time:1.088333 duration:   3000 duration_time:0.0333333 fmt:yuv420p cl:left sar:1/1 s:1904x984',
        '[Parsed_showinfo_0 @ 0000027c3072f0c0] n:  29 pts: 188040 pts_time:2.089333 duration:   3000 duration_time:0.0333333 fmt:yuv420p cl:left sar:1/1 s:1904x984',
        '[Parsed_showinfo_0 @ 0000027c3072f0c0] n: 563 pts:1796212 pts_time:19.957911 duration:   3000 duration_time:0.0333333 fmt:yuv420p cl:left sar:1/1 s:1904x984',
      ],
      decodedSpanMs: 18_764,
      wallSpanMs: 17_850,
      firstPts: 1.088333,
      rate: 1.001041,
    },
  ];
  for (const { csv, showinfo, decodedSpanMs, wallSpanMs, firstPts, rate } of captures) {
    const frames = csvFrames(csv).map((frame, index) => ({ ...frame, ...parseShowinfo(showinfo[index]) }));
    const metrics = summarizeFrames(frames);
    assert.equal(frames.at(-1).receivedAt - metrics.firstFrameAt, decodedSpanMs);
    assert.equal(metrics.wallSpanMs, wallSpanMs);
    assert.equal(metrics.mediaClock.firstPts, firstPts);
    assert.ok(Math.abs(metrics.mediaClock.rate - rate) < 1e-6);
  }
});

test('unique-frame rate uses the observed receive span when the run ends early', () => {
  const frames = Array.from({ length: 340 }, (_, index) => ({
    counter: index, receivedAt: Math.round(index * 11_309 / 339),
  }));
  const metrics = summarizeFrames(frames, { durationMs: 20_000, endedAt: 20_000 });
  assert.equal(metrics.wallSpanMs, 11_309);
  assert.ok(Math.abs(metrics.fps - 29.98) < 0.01);
  assert.equal(metrics.freezes.events, 0);
});

test('media clock excludes the startup jump and counts large forward and backwards deltas', () => {
  const frames = [
    { counter: 10, receivedAt: 0, ptsTime: 0 },
    { counter: 20, receivedAt: 40, ptsTime: 8.5 },
    { counter: 21, receivedAt: 140, ptsTime: 8.6 },
    { counter: 22, receivedAt: 390, ptsTime: 8.85 },
    { counter: 23, receivedAt: 415, ptsTime: 8.75 },
    { counter: 24, receivedAt: 440, ptsTime: 9 },
  ];
  const { mediaClock } = summarizeFrames(frames);
  assert.deepEqual(mediaClock, {
    firstPts: 0, ptsSpanSec: 0.5, wallSpanSec: 0.4, rate: 1.25, jumps: 3, maxJumpSec: 0.25,
  });
});

test('analyzer requests stdin quit on a wall timer and kills an unresponsive process', async t => {
  for (const responsive of [true, false]) {
    await t.test(responsive ? 'graceful quit' : 'kill fallback', async t => {
      const child = new EventEmitter();
      child.stdio = Array.from({ length: 4 }, () => new PassThrough());
      [child.stdin, child.stdout, child.stderr] = child.stdio;
      child.pid = 1;
      const close = (code, signal) => {
        child.exitCode = code;
        child.signalCode = signal;
        for (const pipe of child.stdio) pipe.end();
        child.emit('close', code);
      };
      const input = [];
      child.stdin.on('data', data => {
        input.push(data.toString());
        if (responsive) close(0, null);
      });
      const kill = t.mock.fn(signal => close(null, signal));
      child.kill = kill;
      const spawn = t.mock.method(childProcess, 'spawn', () => child);
      syncBuiltinESMExports();
      let analysis;
      try {
        analysis = analyzeRtsp({ url: 'rtsp://example.invalid', transport: 'udp', durationMs: 25 });
      } finally {
        spawn.mock.restore();
        syncBuiltinESMExports();
      }
      const [, args, options] = spawn.mock.calls[0].arguments;
      assert.ok(args.includes('-stdin'));
      assert.ok(!args.includes('-t'));
      assert.equal(options.stdio[0], 'pipe');
      assert.deepEqual(input, []);
      assert.equal((await analysis).ffmpegExit, responsive ? 0 : 1);
      assert.deepEqual(input, ['q']);
      assert.deepEqual(kill.mock.calls.map(call => call.arguments), responsive ? [] : [['SIGKILL']]);
    });
  }
});

test('rawvideo emits complete 320x180 frames across arbitrary chunk boundaries', () => {
  const frameBytes = 320 * 180;
  const expected = [Buffer.alloc(frameBytes, 17), Buffer.alloc(frameBytes, 46), Buffer.alloc(frameBytes, 93)];
  const bytes = Buffer.concat([...expected, Buffer.alloc(19, 122)]);
  const actual = [];
  const read = createFrameReader(frame => actual.push(Buffer.from(frame)));
  read(bytes.subarray(0, 1));
  read(bytes.subarray(1, frameBytes - 1));
  assert.equal(actual.length, 0);
  read(bytes.subarray(frameBytes - 1, frameBytes * 2 + 10));
  assert.equal(actual.length, 2);
  read(bytes.subarray(frameBytes * 2 + 10));
  assert.deepEqual(actual, expected);
});
