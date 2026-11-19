import childProcess, { execFile, fork, spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { addAbortListener, once } from 'node:events';
import { closeSync, existsSync, openSync, statSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { syncBuiltinESMExports } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, extname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs, promisify } from 'node:util';

const windows = process.platform === 'win32';
const script = fileURLToPath(import.meta.url);
const labDir = fileURLToPath(new URL('.', import.meta.url));
const repoDir = resolve(labDir, '../..');
const npmDir = join(repoDir, 'target', 'stream-lab', 'npm');
const defaultMistlibTag = 'beta';
const exec = promisify(execFile);
const lockTimeoutMs = 60_000;
const signaling = { 'browser-first': 'HaveRemoteOffer', 'native-first': 'HaveLocalOffer' };

export const defaultThresholds = Object.freeze({
  maxStartupMs: 10_000,
  minFpsRatio: 0.8,
  maxFreezes: 0,
  maxMissingRatio: 0.01,
  maxDecodeErrors: 0,
  maxResolutionChanges: 2,
  maxMediaClockDrift: 0.05,
  maxMediaClockJumps: 0,
});

const thresholdFlags = {
  'max-startup-ms': 'maxStartupMs',
  'min-fps-ratio': 'minFpsRatio',
  'max-freezes': 'maxFreezes',
  'max-missing-ratio': 'maxMissingRatio',
  'max-decode-errors': 'maxDecodeErrors',
  'max-resolution-changes': 'maxResolutionChanges',
  'max-media-clock-drift': 'maxMediaClockDrift',
  'max-media-clock-jumps': 'maxMediaClockJumps',
};
const pageFlags = {
  w: 'w', h: 'h', fps: 'fps', capture: 'capture', audio: 'audio',
  'max-bitrate': 'maxBitrate', 'max-fps': 'maxFps', hint: 'hint', degradation: 'degradation',
};

const usage = `Usage: node tools/stream-lab/run.mjs [options]

  --bin <exe>                         default: target/release/mistl[.exe]
  --duration <seconds>                default: 30
  --order browser-first|native-first|both
  --transport udp|tcp|both            both order and transport default to both
  --w <pixels> --h <pixels> --fps <rate>
  --capture tab|canvas --audio 0|1
  --max-bitrate <bps> --max-fps <rate>
  --hint detail|motion|text
  --degradation balanced|maintain-framerate|maintain-resolution
  --mistlib <pkg dir> --mistlib-web <dir>
  --mistlib-tag <tag>                 default: beta
  --refresh-npm                      reinstall the published packages
  --headed --label <name> --out <dir> --keep

Package paths use flags, STREAM_LAB_MISTLIB / STREAM_LAB_MISTLIB_WEB, then npm.

Threshold overrides:
  --max-startup-ms --min-fps-ratio --max-freezes --max-missing-ratio
  --max-decode-errors --max-resolution-changes
  --max-media-clock-drift --max-media-clock-jumps
`;

/** Newest of the `lab` and `release` profile binaries (lab preferred on ties). */
function defaultBin(repoDir, windows) {
  const exe = windows ? 'mistl.exe' : 'mistl';
  const candidates = ['lab', 'release'].map(profile => join(repoDir, 'target', profile, exe)).filter(existsSync);
  if (!candidates.length) return join(repoDir, 'target', 'lab', exe);
  return candidates.reduce((best, bin) => (statSync(bin).mtimeMs > statSync(best).mtimeMs ? bin : best));
}

function numberOption(flag, value, { min = 0, integer = false, max = Infinity } = {}) {
  const number = Number(value);
  if (String(value).trim() === '' || !Number.isFinite(number) || number < min
      || number > max || (integer && !Number.isSafeInteger(number))) {
    throw new Error(`Invalid --${flag}: ${value}`);
  }
  return number;
}

function choice(flag, value, choices) {
  if (!choices.includes(value)) throw new Error(`--${flag} must be one of ${choices.join(', ')}`);
  return value;
}

export function parseOptions(args, now = new Date()) {
  const strings = [
    'bin', 'duration', 'order', 'transport', 'mistlib', 'mistlib-web', 'mistlib-tag', 'label', 'out',
    ...Object.keys(pageFlags), ...Object.keys(thresholdFlags),
  ];
  const { values } = parseArgs({
    args,
    options: {
      ...Object.fromEntries(strings.map(flag => [flag, { type: 'string' }])),
      headed: { type: 'boolean' },
      keep: { type: 'boolean' },
      'refresh-npm': { type: 'boolean' },
      help: { type: 'boolean', short: 'h' },
    },
  });
  if (values.help) return null;

  const label = values.label ?? 'run';
  const stamp = now.toISOString().replaceAll(':', '-');
  const page = {
    w: 1920, h: 1080, fps: 30, capture: 'tab', audio: 1, maxBitrate: 8_000_000,
    maxFps: 30, hint: '', degradation: '',
  };
  for (const [flag, key] of Object.entries(pageFlags)) {
    if (values[flag] !== undefined) page[key] = values[flag];
  }
  for (const key of ['w', 'h', 'fps', 'maxFps']) {
    const flag = key === 'maxFps' ? 'max-fps' : key;
    page[key] = numberOption(flag, page[key], { min: 1, integer: key === 'w' || key === 'h' });
  }
  page.audio = numberOption('audio', page.audio, { max: 1, integer: true });
  page.maxBitrate = numberOption('max-bitrate', page.maxBitrate, { integer: true });
  choice('capture', page.capture, ['tab', 'canvas']);
  choice('hint', page.hint, ['', 'detail', 'motion', 'text']);
  choice('degradation', page.degradation, ['', 'balanced', 'maintain-framerate', 'maintain-resolution']);

  const thresholds = { ...defaultThresholds };
  for (const [flag, key] of Object.entries(thresholdFlags)) {
    if (values[flag] !== undefined) {
      thresholds[key] = numberOption(flag, values[flag], {
        integer: ['maxFreezes', 'maxDecodeErrors', 'maxResolutionChanges', 'maxMediaClockJumps'].includes(key),
      });
    }
  }
  return {
    // `just stream-lab` builds the fast-rebuilding `lab` profile first.
    bin: resolve(values.bin ?? defaultBin(repoDir, windows)),
    durationMs: numberOption('duration', values.duration ?? 30, { min: 0.001 }) * 1000,
    order: choice('order', values.order ?? 'both', [...Object.keys(signaling), 'both']),
    transport: choice('transport', values.transport ?? 'both', ['udp', 'tcp', 'both']),
    mistlib: values.mistlib,
    mistlibWeb: values['mistlib-web'],
    mistlibTag: values['mistlib-tag'] ?? defaultMistlibTag,
    refreshNpm: values['refresh-npm'] ?? false,
    headed: values.headed ?? false,
    label,
    out: resolve(values.out ?? join(repoDir, 'target', 'stream-lab', `${stamp}-${label.replace(/[^\w.-]/g, '_')}`)),
    keep: values.keep ?? false,
    page,
    thresholds,
  };
}

export function selectMistlibPaths(options, env, cwd) {
  const mistlib = options.mistlib ?? (env.STREAM_LAB_MISTLIB || undefined);
  const mistlibWeb = options.mistlibWeb ?? (env.STREAM_LAB_MISTLIB_WEB || undefined);
  return {
    mistlib: mistlib === undefined ? undefined : resolve(cwd, mistlib),
    mistlibWeb: mistlibWeb === undefined ? undefined : resolve(cwd, mistlibWeb),
  };
}

export async function resolveMistlibPackages(options = {}, env = process.env, signal) {
  const selected = selectMistlibPaths(options, env, process.cwd());
  if (selected.mistlib && selected.mistlibWeb) return selected;

  const scope = join(npmDir, 'node_modules', '@tik-choco');
  const installed = { mistlib: join(scope, 'mistlib'), mistlibWeb: join(scope, 'mistlib-web') };
  const tag = options.mistlibTag ?? defaultMistlibTag;
  const tagFile = join(npmDir, 'mistlib-tag');
  const manifests = Object.values(installed).map(path => join(path, 'package.json'));
  if (options.refreshNpm || await readOptional(tagFile) !== `${tag}\n` || !manifests.every(existsSync)) {
    await mkdir(npmDir, { recursive: true });
    await rm(tagFile, { force: true });
    const npmArgs = [
      'install', '--no-save', '--no-audit', '--no-fund', '--prefix', npmDir,
      `@tik-choco/mistlib@${tag}`, `@tik-choco/mistlib-web@${tag}`,
    ];
    // Windows cannot run npm.cmd directly without a shell.
    const npmCli = join(dirname(process.execPath), 'node_modules', 'npm', 'bin', 'npm-cli.js');
    console.error(`stream-lab: installing mistlib packages with npm tag ${tag}`);
    try {
      await exec(windows ? process.execPath : 'npm', windows ? [npmCli, ...npmArgs] : npmArgs,
        { windowsHide: true, signal });
    } catch (error) {
      signal?.throwIfAborted();
      throw new Error(`npm install failed: ${error.stderr?.trim() || error.message}`, { cause: error });
    }
    await readVersions(installed);
    await writeFile(tagFile, `${tag}\n`);
  }

  return {
    mistlib: selected.mistlib ?? installed.mistlib,
    mistlibWeb: selected.mistlibWeb ?? installed.mistlibWeb,
  };
}

export function browserNodeId(order) {
  // The lower ID offers first. See .mistlib-src/mistlib-native/src/transports/webrtc/signaling.rs:1135
  // and .mistlib-src/mistlib-core/src/signaling/courtesy.rs:82.
  const prefix = order === 'browser-first' ? '00000000' : 'ffffffff';
  return prefix + randomUUID().slice(8);
}

export function firstSignalingState(log, nodeId) {
  const peer = `[IceState] peer=${nodeId} `;
  for (const line of log.split(/\r?\n/)) {
    if (!line.includes(peer)) continue;
    const state = line.match(/\bkind=signaling state=(\w+)/);
    if (state) return state[1];
  }
  return null;
}

function transportFailures(scenario, result, fps, thresholds) {
  const failures = [];
  if (scenario.status !== 'ok') failures.push(scenario.status);
  if (!scenario.locked) failures.push('publisher lock');
  if (scenario.observedSignalingState !== signaling[scenario.order]) failures.push('offer order');
  if (result?.error) failures.push('analyzer error');
  const metrics = result?.metrics;
  if (!metrics) return [...failures, 'no metrics'];

  const { maxMediaClockDrift, maxMediaClockJumps } = thresholds;
  const limits = [
    ['startup', metrics.startupMs, value => value <= thresholds.maxStartupMs],
    ['fps', metrics.fps, value => value >= thresholds.minFpsRatio * fps],
    ['freezes', metrics.freezes.events, value => value <= thresholds.maxFreezes],
    ['missing frames', metrics.gaps.missingFrames, value => value <= thresholds.maxMissingRatio * metrics.uniqueFrames],
    ['decode errors', metrics.decodeErrors.count, value => value <= thresholds.maxDecodeErrors],
    ['resolution changes', metrics.resolution.changes, value => value <= thresholds.maxResolutionChanges],
    ['media clock drift', metrics.mediaClock?.rate,
      value => value >= 1 - maxMediaClockDrift && value <= 1 + maxMediaClockDrift],
    ['media clock jumps', metrics.mediaClock?.jumps, value => value <= maxMediaClockJumps],
  ];
  for (const [label, value, withinLimit] of limits) {
    if (!Number.isFinite(value) || !withinLimit(value)) failures.push(label);
  }
  return failures;
}

export function summarize(report) {
  const { scenarios, params } = report;
  const transports = params.transport === 'both' ? ['udp', 'tcp'] : [params.transport];
  const cell = (value, digits = 2) => Number.isFinite(value) ? String(Number(value.toFixed(digits))) : '—';
  const lines = [
    '# Stream lab', '',
    '| Scenario | Transport | Observed offer | Lock | Startup (ms) | Startup stale / jump frames | FPS | Decoded | Unique | Missing | Sender skips | Freezes | Resolution changes | Kbps | Decode errors | Latency p50 / p95 (ms) | Wall span (ms) | PTS start (s) / rate / jumps | Result |',
    '| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |',
  ];
  let passed = scenarios.length > 0;
  for (const scenario of scenarios) {
    for (const transport of transports) {
      const result = scenario.transports[transport];
      const metrics = result?.metrics;
      const failures = transportFailures(scenario, result, params.page.fps, params.thresholds);
      if (failures.length) passed = false;
      const latency = metrics?.latencyMs;
      const clock = metrics?.mediaClock;
      const row = [
        scenario.order, transport, scenario.observedOrder ?? '—', scenario.locked ? 'yes' : 'no',
        cell(metrics?.startupMs), `${cell(metrics?.startup?.staleFrames)} / ${cell(metrics?.startup?.jumpFrames)}`,
        cell(metrics?.fps), cell(metrics?.framesDecoded), cell(metrics?.uniqueFrames),
        cell(metrics?.gaps.missingFrames), cell(metrics?.senderSkips), cell(metrics?.freezes.events),
        cell(metrics?.resolution.changes),
        cell(metrics?.bitrateKbps.mean), cell(metrics?.decodeErrors.count), `${cell(latency?.p50)} / ${cell(latency?.p95)}`,
        cell(metrics?.wallSpanMs), `${cell(clock?.firstPts, 3)} / ${cell(clock?.rate, 3)} / ${cell(clock?.jumps)}`,
        failures.length ? `FAIL (${failures.join(', ')})` : 'PASS',
      ];
      lines.push(`| ${row.join(' | ')} |`);
    }
  }
  if (report.error) passed = false;
  return { passed, markdown: [...lines, '', `Overall: **${passed ? 'PASS' : 'FAIL'}**`, ''].join('\n') };
}

function childState(child) {
  const done = new Promise(resolve => {
    child.once('error', error => resolve({ error }));
    child.once('exit', (code, signal) => resolve({ code, signal }));
  });
  return { child, done };
}

async function killTree(pid) {
  if (!pid) return;
  try {
    if (windows) await exec('taskkill.exe', ['/PID', String(pid), '/T', '/F'], { windowsHide: true, timeout: 5000 });
    else process.kill(-pid, 'SIGKILL');
  } catch (error) {
    if (processExists(pid)) throw error;
  }
}

function processExists(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === 'ESRCH') return false;
    throw error;
  }
}

async function waitUntil(check, timeoutMs, signal, intervalMs = 250) {
  const deadline = performance.now() + timeoutMs;
  do {
    signal?.throwIfAborted();
    const value = await check();
    if (value) return value;
    const remaining = deadline - performance.now();
    if (remaining <= 0) return null;
    await delay(Math.min(remaining, intervalMs), undefined, { signal });
  } while (performance.now() < deadline);
  return null;
}

function withAbort(promise, signal) {
  let listener;
  const interrupted = new Promise((_, reject) => {
    listener = addAbortListener(signal, () => reject(signal.reason));
  });
  return Promise.race([promise, interrupted]).finally(() => listener[Symbol.dispose]());
}

export function createDaemon(bin, stateDir, outDir) {
  const args = ['--state-dir', stateDir, '--instance', 'lab', '--no-tray'];
  const logPath = join(outDir, 'daemon.log');
  const foregroundLog = join(stateDir, 'foreground.log');
  let foreground;
  let stateLog;
  let activePid;
  let enabling = false;

  async function invoke(command, { timeout = 5000, signal } = {}) {
    const { stdout } = await exec(bin, [...args, ...command], { windowsHide: true, timeout, signal }).catch(error => {
      signal?.throwIfAborted();
      const detail = error.killed ? `timed out after ${timeout / 1000}s`
        : error.stderr?.trim().split(/\r?\n/).findLast(line => line.startsWith('error: '))
          ?? error.message.split('\n')[0];
      throw new Error(`mistl ${command.slice(0, 2).join(' ')}: ${detail}`, { cause: error });
    });
    // The relay command prints its JSON first, followed by VRChat instructions.
    const jsonEnd = stdout.indexOf('\n}');
    return JSON.parse(jsonEnd === -1 ? stdout : stdout.slice(0, jsonEnd + 2));
  }

  const cli = (...command) => invoke(command);

  async function status() {
    // daemon.status never auto-starts; the other CLI commands can.
    return cli('daemon', 'status').catch(() => null);
  }

  return {
    cli,
    // mistl's own IPC timeout for joining a room is 30 seconds.
    relay: (room, signal) => invoke(['stream', 'relay', '--room', room], { timeout: 35_000, signal }),
    async start(signal) {
      const build = await cli('build-info');
      stateLog = join(stateDir, build.channel, 'lab', 'data', 'daemon.log');
      signal.throwIfAborted();
      const log = openSync(foregroundLog, 'w', 0o600);
      try {
        foreground = childState(spawn(bin, [...args, 'daemon', 'run'], {
          env: { ...process.env, RUST_LOG: 'info,mistl::stream=debug,mistlib_native::transports::webrtc::peer=debug' },
          stdio: ['ignore', 'ignore', log],
          detached: true,
          windowsHide: true,
        }));
      } finally {
        closeSync(log);
      }
      const first = await withAbort(Promise.race([
        waitUntil(status, 15_000, signal),
        foreground.done.then(({ error }) => { throw error ?? new Error('Daemon exited before becoming ready'); }),
      ]), signal);
      if (!first) throw new Error('Daemon startup timed out');
      activePid = first.pid;

      enabling = true;
      // Enabling the network restarts the daemon. The replacement writes to
      // the instance's data log, which is combined with the foreground log.
      await cli('network', 'on');
      const online = await waitUntil(async () => {
        const current = await status();
        return current?.network?.state === 'on' ? current : null;
      }, 15_000, signal);
      if (!online) throw new Error('Timed out waiting for the daemon with networking enabled');
      activePid = online.pid;
      enabling = false;
      return build;
    },
    async close() {
      if (!foreground) return;
      if (enabling) {
        // If interrupted between the network command and restart, let the
        // replacement appear before asking the isolated daemon to stop.
        const online = await waitUntil(async () => {
          const current = await status();
          return current?.network?.state === 'on' ? current : null;
        }, 5000);
        if (online) activePid = online.pid;
      }
      const current = await status();
      const pids = new Set([current?.pid, activePid, foreground.child.pid].filter(Boolean));
      await cli('daemon', 'stop').catch(() => {});
      await waitUntil(() => [...pids].every(pid => !processExists(pid)), 3000);
      for (const pid of pids) {
        if (!processExists(pid)) continue;
        try {
          await killTree(pid);
        } catch (error) {
          process.kill(pid, 'SIGKILL');
          throw error;
        }
      }
      await foreground.done;
    },
    async collectLog() {
      const logs = await Promise.all([foregroundLog, stateLog].filter(Boolean).map(readOptional));
      const log = logs.join('').replace(/([?&]token=)[^\s"&]+/g, '$1<redacted>');
      await writeFile(logPath, log);
      return log;
    },
  };
}

function readOptional(path) {
  return readFile(path, 'utf8').catch(error => {
    if (error.code !== 'ENOENT') throw error;
    return '';
  });
}

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
};

export async function serveLab({ mistlib, mistlibWeb }, directory = labDir) {
  const roots = await Promise.all([join(directory, 'page'), mistlib, mistlibWeb].map(root => realpath(root)));
  const mounts = new Map([['page', roots[0]], ['mistlib', roots[1]], ['mistlib-web', roots[2]]]);
  const server = createServer(async (request, response) => {
    try {
      const { pathname } = new URL(request.url, 'http://127.0.0.1');
      const [, mount, ...parts] = decodeURIComponent(pathname).split('/');
      let file;
      if (pathname === '/pattern.mjs') file = join(directory, 'pattern.mjs');
      else {
        const root = mounts.get(mount);
        if (!root || parts.some(part => part.includes('\\') || part === '..')) {
          throw new Error('Unknown path');
        }
        file = await realpath(resolve(root, parts.join('/')));
        const path = relative(root, file);
        if (isAbsolute(path) || path === '..' || path.startsWith('..' + sep)) throw new Error('Outside mount');
      }
      const body = await readFile(file);
      response.writeHead(200, { 'Content-Type': mimeTypes[extname(file)] ?? 'application/octet-stream' });
      response.end(body);
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return {
    origin: `http://127.0.0.1:${server.address().port}`,
    close: () => new Promise((resolve, reject) => {
      server.close(error => error ? reject(error) : resolve());
      server.closeAllConnections();
    }),
  };
}

async function analyzerWorker() {
  // Keep handles to FFmpeg inside the disposable worker so it can terminate
  // its children even on Windows systems that deny taskkill's process lookup.
  const children = new Set();
  const spawnProcess = spawn;
  childProcess.spawn = (...args) => {
    const child = spawnProcess(...args);
    if (child.pid) children.add(child);
    child.once('close', () => children.delete(child));
    return child;
  };
  syncBuiltinESMExports();
  let stopping = false;
  async function stop() {
    if (stopping) return;
    stopping = true;
    await Promise.all([...children].map(async child => {
      const exited = once(child, 'close');
      child.kill();
      await exited;
    }));
    process.exit(1);
  }
  process.once('disconnect', stop);
  process.once('message', async jobs => {
    const results = await Promise.all(jobs.map(async job => {
      try {
        const { analyzeRtsp } = await import('./analyze.mjs');
        return [job.transport, { metrics: await analyzeRtsp(job) }];
      } catch (error) {
        return [job.transport, { error: error.message }];
      }
    }));
    if (!stopping && process.connected) {
      process.off('disconnect', stop);
      process.send(Object.fromEntries(results), () => process.disconnect());
    }
  });
}

function startAnalyzers(jobs) {
  // A child process lets an interrupted run stop ffmpeg even while analyzeRtsp
  // is awaiting its measurement window.
  const worker = childState(fork(script, ['--analyzer-worker'], {
    detached: true,
    execArgv: [],
    windowsHide: true,
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
  }));
  let results;
  let sendError;
  worker.child.once('message', message => { results = message; });
  worker.child.send(jobs, error => { sendError = error; });
  return {
    result: worker.done.then(exit => results
      ? { results }
      : { error: exit.error?.message ?? sendError?.message ?? `Analyzer worker exited (${exit.signal ?? exit.code})` }),
    async close() {
      if (worker.child.exitCode === null && worker.child.signalCode === null) {
        if (worker.child.connected) worker.child.disconnect();
        const stopped = await Promise.race([worker.done, delay(3000, null, { ref: false })]);
        if (!stopped) {
          try {
            await killTree(worker.child.pid);
          } catch (error) {
            worker.child.kill('SIGKILL');
            throw error;
          }
        }
      }
      await worker.done;
    },
  };
}

async function waitForLock(daemon, nodeId, samples, signal) {
  const lockSignal = AbortSignal.any([signal, AbortSignal.timeout(lockTimeoutMs)]);
  try {
    return await waitUntil(async () => {
      const status = await daemon.cli('stream', 'status');
      lockSignal.throwIfAborted();
      const sample = { t: Date.now(), status };
      samples.push(sample);
      return status.publisher === nodeId ? sample : null;
    }, lockTimeoutMs, lockSignal, 1000);
  } catch (error) {
    signal.throwIfAborted();
    if (!lockSignal.aborted) throw error;
    return null;
  }
}

async function sampleStream(daemon, samples, signal) {
  while (!signal.aborted) {
    await delay(1000, undefined, { signal });
    const status = await daemon.cli('stream', 'status');
    if (!signal.aborted) samples.push({ t: Date.now(), status });
  }
}

const browserInfo = `(async () => ({
  userAgent: navigator.userAgent,
  fullVersionList: (await navigator.userAgentData?.getHighEntropyValues(['fullVersionList']))?.fullVersionList,
}))()`;

async function runScenario(order, options, signal) {
  const outDir = join(options.out, order);
  const scenario = {
    order,
    expectedSignalingState: signaling[order],
    observedSignalingState: null,
    observedOrder: null,
    status: 'error',
    locked: false,
    room: randomUUID(),
    browserNodeId: browserNodeId(order),
    streamSamples: [],
    transports: {},
  };
  let stateDir;
  let daemon;
  let browser;
  let server;
  let page;
  let analyzers;
  try {
    await mkdir(outDir, { recursive: true });
    stateDir = await mkdtemp(join(tmpdir(), 'mistl-stream-lab-'));
    if (options.keep) scenario.stateDir = stateDir;
    daemon = createDaemon(options.bin, stateDir, outDir);
    scenario.mistl = await daemon.start(signal);
    const { did } = await daemon.cli('key', 'did');
    scenario.nativeNodeId = createHash('sha256').update(did).digest('hex').slice(0, 16);
    const ordered = order === 'browser-first'
      ? scenario.browserNodeId < scenario.nativeNodeId
      : scenario.nativeNodeId < scenario.browserNodeId;
    if (!ordered) throw new Error('Could not choose a UUID that sorts after this native node ID. Retry with a fresh identity.');
    const relay = await daemon.relay(scenario.room, signal);
    scenario.rtspUrl = relay.rtsp_url;
    if (!scenario.rtspUrl?.startsWith('rtsp://')) throw new Error('Relay did not return an rtsp_url');
    signal.throwIfAborted();

    const { findBrowserExecutable, launchBrowser, openPage } = await import('./browser.mjs');
    server = await serveLab(options);
    const executable = findBrowserExecutable();
    browser = await launchBrowser({
      executable,
      headless: !options.headed,
      userDataDir: join(stateDir, 'browser'),
      extraArgs: [`--window-size=${options.page.w},${options.page.h}`],
    });
    signal.throwIfAborted();
    const url = new URL('/page/index.html', server.origin);
    url.search = new URLSearchParams({ ...options.page, room: scenario.room, node: scenario.browserNodeId });
    page = await withAbort(openPage(browser, url.href), signal);
    scenario.browser = { executable, ...await withAbort(page.evaluate(browserInfo), signal) };
    const senderSignal = AbortSignal.any([signal, AbortSignal.timeout(lockTimeoutMs)]);
    const ready = await withAbort(page.waitFor(
      `['joined', 'error'].includes(window.__lab?.state) && window.__lab.state`, lockTimeoutMs,
    ), senderSignal);
    if (ready === 'error') throw new Error(await withAbort(page.evaluate('window.__lab.error'), senderSignal));
    await withAbort(page.evaluate('window.__lab.startCapture()', { userGesture: true }), senderSignal);
    const sender = await withAbort(page.evaluate(
      '({ state: window.__lab.state, error: window.__lab.error, publishedAt: window.__lab.publishedAt })',
    ), senderSignal);
    if (sender.error || !Number.isFinite(sender.publishedAt)) {
      throw new Error(sender.error ?? 'Sender did not publish a video track');
    }
    scenario.publishedAt = sender.publishedAt;
    const lock = await waitForLock(daemon, scenario.browserNodeId, scenario.streamSamples, signal);
    if (!lock) {
      scenario.status = 'no_lock';
      return scenario;
    }
    scenario.locked = true;
    scenario.lockedAt = lock.t;

    const transports = options.transport === 'both' ? ['udp', 'tcp'] : [options.transport];
    const jobs = await Promise.all(transports.map(async transport => {
      const outDirForTransport = join(outDir, transport);
      await mkdir(outDirForTransport, { recursive: true });
      return {
        url: scenario.rtspUrl, transport, durationMs: options.durationMs,
        startedAt: scenario.publishedAt, expectedFps: options.page.fps, outDir: outDirForTransport,
      };
    }));
    signal.throwIfAborted();
    analyzers = startAnalyzers(jobs);
    const sampling = new AbortController();
    const sampleTask = sampleStream(daemon, scenario.streamSamples, sampling.signal).catch(error => {
      if (!sampling.signal.aborted) scenario.streamSampleError = error.message;
    });
    try {
      const { results, error } = await withAbort(analyzers.result, signal);
      if (!results) throw new Error(error);
      scenario.transports = results;
      scenario.status = 'ok';
    } finally {
      sampling.abort();
      await sampleTask;
    }
  } catch (error) {
    scenario.status = signal.aborted ? 'interrupted' : 'error';
    scenario.error = signal.aborted ? signal.reason.message : error.message;
  } finally {
    async function cleanup(name, action) {
      try {
        await action();
      } catch (error) {
        (scenario.cleanupErrors ??= []).push(`${name}: ${error.message}`);
        if (!signal.aborted) scenario.status = 'error';
      }
    }
    if (page) {
      scenario.browserConsole = page.consoleMessages;
      if (!signal.aborted) {
        await cleanup('browser stats', async () => {
          scenario.sender = await withAbort(page.evaluate(
            `window.__lab && (({ state, error, framesDrawn, publishedAt, encodingApplied, stats, log, selfCheck }) =>
              ({ state, error, framesDrawn, publishedAt, encodingApplied, stats, log, selfCheck }))(window.__lab)`,
          ), AbortSignal.any([signal, AbortSignal.timeout(5000)]));
        });
      }
    }
    await Promise.all([
      cleanup('analyzers', () => analyzers?.close()),
      cleanup('browser', () => browser?.close()),
      cleanup('daemon', () => daemon?.close()),
    ]);
    await cleanup('HTTP server', () => server?.close());
    if (daemon) {
      await cleanup('daemon log', async () => {
        scenario.observedSignalingState = firstSignalingState(await daemon.collectLog(), scenario.browserNodeId);
        scenario.observedOrder = Object.keys(signaling).find(
          order => signaling[order] === scenario.observedSignalingState,
        ) ?? null;
      });
    }
    if (stateDir && !options.keep) {
      await cleanup('state directory', () => rm(stateDir, { recursive: true, force: true, maxRetries: 5 }));
    }
  }
  return scenario;
}

async function readVersions(options) {
  const [wasm, web] = await Promise.all([
    readFile(join(options.mistlib, 'package.json'), 'utf8'),
    readFile(join(options.mistlibWeb, 'package.json'), 'utf8'),
  ]);
  return { wasmVersion: JSON.parse(wasm).version, webVersion: JSON.parse(web).version };
}

async function main(args) {
  const options = parseOptions(args);
  if (!options) {
    process.stdout.write(usage);
    return;
  }
  const controller = new AbortController();
  const stop = name => () => {
    controller.abort(new Error(name));
    process.exitCode = name === 'SIGINT' ? 130 : 143;
  };
  const onInterrupt = stop('SIGINT');
  const onTerminate = stop('SIGTERM');
  process.on('SIGINT', onInterrupt);
  process.on('SIGTERM', onTerminate);
  try {
    await mkdir(options.out, { recursive: true });
    const report = {
      startedAt: new Date().toISOString(),
      params: options,
      environment: { node: process.version, platform: process.platform, arch: process.arch },
      scenarios: [],
    };
    try {
      Object.assign(options, await resolveMistlibPackages(options, process.env, controller.signal));
      report.environment.mistlib = await readVersions(options);
      const orders = options.order === 'both' ? Object.keys(signaling) : [options.order];
      for (const order of orders) {
        controller.signal.throwIfAborted();
        console.error(`stream-lab: ${order}`);
        report.scenarios.push(await runScenario(order, options, controller.signal));
      }
      controller.signal.throwIfAborted();
    } catch (error) {
      report.error = error.message;
      console.error(`stream-lab: ${report.error}`);
    }
    report.environment.mistl = report.scenarios.find(scenario => scenario.mistl)?.mistl;
    report.environment.browser = report.scenarios.find(scenario => scenario.browser)?.browser;
    const summary = summarize(report);
    report.passed = summary.passed;
    await Promise.all([
      writeFile(join(options.out, 'report.json'), JSON.stringify(report, null, 2) + '\n'),
      writeFile(join(options.out, 'summary.md'), summary.markdown),
    ]);
    process.stdout.write(summary.markdown);
    console.error(`stream-lab: report written to ${options.out}`);
    if (!summary.passed && !process.exitCode) process.exitCode = 1;
  } finally {
    process.off('SIGINT', onInterrupt);
    process.off('SIGTERM', onTerminate);
  }
}

export async function runCli(args) {
  try {
    if (process.send && process.argv[2] === '--analyzer-worker') await analyzerWorker();
    else await main(args);
  } catch (error) {
    console.error(`stream-lab: ${error.message}`);
    if (!process.exitCode) process.exitCode = 1;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === script) await runCli(process.argv.slice(2));
