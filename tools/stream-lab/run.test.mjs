import assert from 'node:assert/strict';
import { resolve } from 'node:path';
import { test } from 'node:test';
import { browserNodeId, defaultThresholds, firstSignalingState, parseOptions, selectMistlibPaths, summarize } from './run.mjs';

function metrics(overrides = {}) {
  return {
    startupMs: 10_000,
    startup: { staleFrames: 15, jumpFrames: 49 },
    fps: 24,
    wallSpanMs: 11_309,
    mediaClock: { firstPts: 8.588211, rate: 1.05, jumps: 0 },
    framesDecoded: 100,
    uniqueFrames: 100,
    freezes: { events: 0 },
    gaps: { missingFrames: 1 },
    senderSkips: 4,
    decodeErrors: { count: 0 },
    resolution: { changes: 2 },
    latencyMs: { p50: 122, p95: 250 },
    bitrateKbps: { mean: 4600 },
    ...overrides,
  };
}

function scenario(order = 'browser-first', overrides = {}) {
  const browserFirst = order === 'browser-first';
  return {
    order,
    status: 'ok',
    locked: true,
    observedOrder: order,
    observedSignalingState: browserFirst ? 'HaveRemoteOffer' : 'HaveLocalOffer',
    transports: { udp: { metrics: metrics() }, tcp: { metrics: metrics() } },
    ...overrides,
  };
}

function report(scenarios, options = {}) {
  return { scenarios, params: { transport: 'both', page: { fps: 30 }, thresholds: defaultThresholds, ...options } };
}

test('the default thresholds include their endpoints for both orders and transports', () => {
  const summary = summarize(report([scenario(), scenario('native-first')]));
  assert.equal(summary.passed, true);
  assert.equal((summary.markdown.match(/\| PASS \|/g) ?? []).length, 4);
  assert.match(summary.markdown, /\| native-first \| tcp \| native-first \| yes \| 10000 \| 15 \/ 49 \| 24 \| 100 \| 100 \| 1 \| 4 \|/);
  assert.match(summary.markdown, /\| Wall span \(ms\) \| PTS start \(s\) \/ rate \/ jumps \|/);
  assert.match(summary.markdown, /\| 11309 \| 8\.588 \/ 1\.05 \/ 0 \| PASS \|/);
});

test('a failing transport reports every violated threshold and fails the run', () => {
  const bad = metrics({
    startupMs: 10_001, fps: 23.99, freezes: { events: 1 }, gaps: { missingFrames: 2 },
    decodeErrors: { count: 1 }, resolution: { changes: 3 },
    mediaClock: { firstPts: 0, rate: 0.949, jumps: 1 },
  });
  const summary = summarize(report([
    scenario('browser-first', { transports: { udp: { metrics: bad }, tcp: { metrics: metrics() } } }),
  ]));
  assert.equal(summary.passed, false);
  assert.match(summary.markdown, /FAIL \(startup, fps, freezes, missing frames, decode errors, resolution changes, media clock drift, media clock jumps\)/);
  assert.match(summary.markdown, /\| PASS \|/);
});

test('media clock checks both drift limits and fails if rate cannot be measured', () => {
  for (const [rate, passed] of [[0.95, true], [1.051, false], [null, false]]) {
    const summary = summarize(report([scenario('browser-first', {
      transports: { tcp: { metrics: metrics({ mediaClock: { firstPts: 20, rate, jumps: 0 } }) } },
    })], { transport: 'tcp' }));
    assert.equal(summary.passed, passed);
    if (!passed) assert.match(summary.markdown, /FAIL \(media clock drift\)/);
  }
});

test('FPS follows requested FPS and thresholds can be overridden', () => {
  const params = {
    transport: 'tcp',
    page: { fps: 20 },
    thresholds: {
      ...defaultThresholds, maxStartupMs: 13_000, minFpsRatio: 0.9, maxMissingRatio: 0.05,
      maxMediaClockDrift: 0.1, maxMediaClockJumps: 2,
    },
  };
  const summary = summarize(report([scenario('native-first', {
    transports: { tcp: { metrics: metrics({
      fps: 18, startupMs: 13_000, gaps: { missingFrames: 5 }, mediaClock: { rate: 1.1, jumps: 2 },
    }) } },
  })], params));
  assert.equal(summary.passed, true);
  assert.doesNotMatch(summary.markdown, /\| udp \|/);
});

test('a missing lock, missing or wrong offer, or absent startup measurement fails the run', () => {
  const unlocked = scenario('browser-first', {
    status: 'no_lock', locked: false, observedOrder: null, observedSignalingState: null, transports: {},
  });
  const wrongOffer = scenario('browser-first', {
    observedOrder: 'native-first', observedSignalingState: 'HaveLocalOffer',
    transports: { tcp: { metrics: metrics({ startupMs: null }) } },
  });
  const summary = summarize(report([unlocked, wrongOffer], { transport: 'tcp' }));
  assert.equal(summary.passed, false);
  assert.match(summary.markdown, /FAIL \(no_lock, publisher lock, offer order, no metrics\)/);
  assert.match(summary.markdown, /FAIL \(offer order, startup\)/);
});

test('an analyzer exception or missing scenario fails the run', () => {
  const failed = scenario('native-first', { transports: { tcp: { error: 'spawn ffmpeg ENOENT' } } });
  assert.match(summarize(report([failed], { transport: 'tcp' })).markdown, /FAIL \(analyzer error, no metrics\)/);
  assert.equal(summarize(report([])).passed, false);
});

test('offer selection and log parsing use the first signaling line for the exact peer', () => {
  const native = '629e4f47cc3d8d75';
  const lower = browserNodeId('browser-first');
  const higher = browserNodeId('native-first');
  assert.ok(lower < native && native < higher);
  assert.match(lower, /^00000000-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.match(higher, /^ffffffff-[0-9a-f-]{27}$/);
  const log = [
    `DEBUG [IceState] peer=${higher} pc_id=1 kind=signaling state=HaveLocalOffer`,
    `DEBUG [IceState] peer=${lower} pc_id=2 kind=connection state=Checking`,
    `DEBUG [IceState] peer=${lower} pc_id=2 kind=signaling state=HaveRemoteOffer`,
    `DEBUG [IceState] peer=${lower} pc_id=2 kind=signaling state=Stable`,
  ].join('\r\n');
  assert.equal(firstSignalingState(log, lower), 'HaveRemoteOffer');
  assert.equal(firstSignalingState(log, higher), 'HaveLocalOffer');
  assert.equal(firstSignalingState(log, 'missing'), null);
});

test('CLI options map to sender arguments and threshold overrides', () => {
  const options = parseOptions([
    '--duration', '3.5', '--audio', '0', '--max-bitrate', '0', '--order', 'native-first', '--max-freezes', '2',
    '--max-media-clock-drift', '0.07', '--max-media-clock-jumps', '3',
  ]);
  assert.equal(options.durationMs, 3500);
  assert.equal(options.page.audio, 0);
  assert.equal(options.page.maxBitrate, 0);
  assert.equal(options.order, 'native-first');
  assert.equal(options.thresholds.maxFreezes, 2);
  assert.equal(options.thresholds.maxMediaClockDrift, 0.07);
  assert.equal(options.thresholds.maxMediaClockJumps, 3);
  assert.throws(() => parseOptions(['--max-media-clock-jumps', '0.5']), /--max-media-clock-jumps/);
  assert.throws(() => parseOptions(['--transport', 'unknown']), /--transport/);
  assert.throws(() => parseOptions(['--w', '0']), /--w/);
});

test('package flags override environment paths independently, with missing paths left for npm', () => {
  const cwd = resolve('lab');
  const env = { STREAM_LAB_MISTLIB: 'env-wasm', STREAM_LAB_MISTLIB_WEB: 'env-web' };
  const defaults = parseOptions([]);
  assert.equal(defaults.mistlibTag, 'beta');
  assert.equal(defaults.refreshNpm, false);
  assert.deepEqual(selectMistlibPaths(defaults, env, cwd), {
    mistlib: resolve(cwd, 'env-wasm'), mistlibWeb: resolve(cwd, 'env-web'),
  });
  const wasmFlag = parseOptions(['--mistlib', 'flag-wasm', '--mistlib-tag', 'latest', '--refresh-npm']);
  assert.equal(wasmFlag.mistlibTag, 'latest');
  assert.equal(wasmFlag.refreshNpm, true);
  assert.deepEqual(selectMistlibPaths(wasmFlag, env, cwd), {
    mistlib: resolve(cwd, 'flag-wasm'), mistlibWeb: resolve(cwd, 'env-web'),
  });
  const webFlag = parseOptions(['--mistlib-web', 'flag-web']);
  assert.deepEqual(selectMistlibPaths(webFlag, env, cwd), {
    mistlib: resolve(cwd, 'env-wasm'), mistlibWeb: resolve(cwd, 'flag-web'),
  });
  assert.deepEqual(selectMistlibPaths(webFlag, {}, cwd), {
    mistlib: undefined, mistlibWeb: resolve(cwd, 'flag-web'),
  });
  assert.deepEqual(selectMistlibPaths(defaults, { STREAM_LAB_MISTLIB: '' }, cwd), {
    mistlib: undefined, mistlibWeb: undefined,
  });
  assert.equal(parseOptions(['--help']), null);
});
