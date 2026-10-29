// Live vault replication over isolated development daemons; no build or browser.
// Usage: node scripts/test-vault.mjs [binary] [--bin binary] [--scenario name] [--keep-state] [--dry-run]
// MISTL_BIN also overrides the default target/debug/mistl executable.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { closeSync, existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';

const MIB = 1024 * 1024;
const MAX_DISK = 64 * MIB;
const MAX_IPC = MIB;
const started = Date.now();
const log = message => console.log(`[${((Date.now() - started) / 1000).toFixed(1)}s] ${message}`);
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const redact = value => String(value)
  .replace(/\bmvi1-[A-Za-z0-9_-]+/gi, '[invite redacted]')
  .replace(/\bmvk1-[0-9a-f]{64}-[0-9a-f]{8}\b/gi, '[recovery code redacted]')
  .replace(/(token=)[^\s&"']+/gi, '$1***')
  .replace(/("(?:token|secret|recovery_code|code|mac|dek)"\s*:\s*")[^"]*(")/gi, '$1***$2');

function optionsFrom(argv) {
  const options = { keep: false, dryRun: false, help: false, binary: null, scenario: null };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === '--help') options.help = true;
    else if (arg === '--dry-run') options.dryRun = true;
    else if (arg === '--keep-state') options.keep = true;
    else if (arg === '--scenario') {
      assert.ok(argv[index + 1] && !argv[index + 1].startsWith('--'), '--scenario requires a name');
      assert.equal(options.scenario, null, 'specify only one scenario');
      options.scenario = argv[++index];
    }
    else if (arg === '--bin') {
      assert.ok(argv[index + 1] && !argv[index + 1].startsWith('--'), '--bin requires a path');
      assert.equal(options.binary, null, 'specify only one binary');
      options.binary = argv[++index];
    } else {
      assert.ok(!arg.startsWith('--'), `unknown option: ${arg}`);
      assert.equal(options.binary, null, 'specify only one binary');
      options.binary = arg;
    }
  }
  options.binary = path.resolve(options.binary || process.env.MISTL_BIN
    || `target/debug/mistl${process.platform === 'win32' ? '.exe' : ''}`);
  options.scenario ||= 'all';
  assert.ok(['all', 'existing', 'single', 'pair', 'replace'].includes(options.scenario),
    `unknown scenario: ${options.scenario}`);
  return options;
}

function sleep(milliseconds, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const timer = setTimeout(done, milliseconds);
    function done() { signal.removeEventListener('abort', abort); resolve(); }
    function abort() { clearTimeout(timer); reject(signal.reason); }
    signal.addEventListener('abort', abort, { once: true });
  });
}

async function until(label, signal, fn, { interval = 1000, retry } = {}) {
  let last;
  while (!signal.aborted) {
    try {
      const result = await fn();
      if (result) return result;
    } catch (error) {
      if (!retry || !retry(error)) throw error;
      last = error;
    }
    try { await sleep(interval, signal); } catch { break; }
  }
  throw new Error(`${label}: ${redact(signal.reason?.message || 'interrupted')}${last ? `; last error: ${redact(last.message)}` : ''}`);
}

function diskBytes(directory) {
  let total = 0;
  let entries;
  try { entries = readdirSync(directory, { withFileTypes: true }); }
  catch (error) { if (error.code === 'ENOENT') return 0; throw error; }
  for (const entry of entries) {
    const file = path.join(directory, entry.name);
    let stat;
    try { stat = lstatSync(file); }
    catch (error) { if (error.code === 'ENOENT') continue; throw error; }
    assert.ok(!stat.isSymbolicLink(), 'test state must not contain symbolic links');
    total += stat.isDirectory() ? diskBytes(file) : stat.size;
  }
  return total;
}

function checkBudget(root) {
  const bytes = diskBytes(root);
  assert.ok(bytes < MAX_DISK, `temporary state reached the 64 MiB limit: ${bytes} bytes`);
  return bytes;
}

class Node {
  constructor(name, root, binary, scenario) {
    this.name = name;
    this.root = root;
    this.binary = binary;
    this.scenario = scenario;
    this.directory = path.join(root, 'dev', name, 'data');
    this.infoPath = path.join(this.directory, 'daemon.json');
    this.child = null;
    this.exited = true;
    this.spawnError = null;
    this.did = null;
    this.owner = null;
    this.lastStatus = null;
    this.lastStatusAt = null;
  }

  prepare() {
    const config = path.join(this.root, 'dev', this.name, 'config');
    if (existsSync(config)) return;
    mkdirSync(config, { recursive: true });
    mkdirSync(this.directory, { recursive: true });
    writeFileSync(path.join(config, 'config.toml'), '[ui]\nenabled = false\nlisten = "127.0.0.1:0"\n'
      + '[update]\nauto_check = false\nauto_apply = false\n'
      + `[storage]\ncapacity_bytes = ${4 * MIB}\n`
      + '[vault]\n' + (this.scenario === 'pair' ? '' : 'replicas = 3\n')
      + `capacity_bytes = ${8 * MIB}\nmin_free_bytes = 0\nmax_object_bytes = ${4 * MIB}\n`
      + (this.scenario === 'replace' ? 'dead_after_secs = 60\n' : ''));
    // New instances default to offline; seed ON before the first foreground run.
    writeFileSync(path.join(this.directory, 'network-state.json'), JSON.stringify({ schema: 1, enabled: true }));
  }

  info() {
    const info = JSON.parse(readFileSync(this.infoPath, 'utf8'));
    assert.equal(info.channel, 'dev', `${this.name}: development channel`);
    assert.equal(info.instance, this.name, `${this.name}: instance identity`);
    assert.ok(this.child && !this.exited && info.pid === this.child.pid, `${this.name}: discovery belongs to the spawned process`);
    assert.ok(Number.isInteger(info.port) && info.port > 0 && info.port < 65536, `${this.name}: valid IPC port`);
    assert.ok(typeof info.token === 'string' && Number.isInteger(info.ipc_version), `${this.name}: valid IPC metadata`);
    return info;
  }

  async call(cmd, args = {}, signal, timeout = 15000) {
    if (signal?.aborted) throw signal.reason;
    const info = this.info();
    const request = JSON.stringify({ token: info.token, channel: info.channel, instance: info.instance,
      ipc_version: info.ipc_version, cmd, args }) + '\n';
    assert.ok(Buffer.byteLength(request) <= MAX_IPC, `${this.name} ${cmd}: IPC request too large`);
    return await new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: '127.0.0.1', port: info.port });
      const chunks = [];
      let length = 0;
      let settled = false;
      const finish = (error, value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener('abort', abort);
        socket.destroy();
        if (error) reject(new Error(`${this.name} ${cmd}: ${redact(error.message || error)}`));
        else {
          if (cmd === 'vault.status') { this.lastStatus = value; this.lastStatusAt = Date.now(); }
          resolve(value);
        }
      };
      const abort = () => finish(signal.reason || new Error('interrupted'));
      const timer = setTimeout(() => finish(new Error('IPC timeout')), timeout);
      signal?.addEventListener('abort', abort, { once: true });
      if (signal?.aborted) return abort();
      socket.on('error', error => finish(error));
      socket.on('connect', () => socket.write(request));
      socket.on('data', chunk => {
        length += chunk.length;
        if (length > MAX_IPC) return finish(new Error('oversized IPC response'));
        chunks.push(chunk);
        const combined = Buffer.concat(chunks, length);
        const end = combined.indexOf(10);
        if (end < 0) return;
        try {
          const response = JSON.parse(combined.subarray(0, end).toString('utf8'));
          if (response.ok === true) finish(null, response.data);
          else finish(new Error(typeof response.error === 'string' ? response.error : 'IPC request failed'));
        } catch { finish(new Error('invalid IPC response')); }
      });
      socket.on('end', () => finish(new Error('IPC closed before response')));
    });
  }

  async start(signal) {
    if (signal.aborted) throw signal.reason;
    assert.ok(this.exited, `${this.name}: previous child is still running`);
    this.prepare();
    this.spawnError = null;
    const logFile = openSync(path.join(this.directory, 'daemon.log'), 'a');
    let child;
    try {
      child = spawn(this.binary, ['--state-dir', this.root, '--instance', this.name, '--no-tray', 'daemon', 'run'],
        { windowsHide: true, stdio: ['ignore', logFile, logFile],
          env: { ...process.env, RUST_LOG: process.env.RUST_LOG || 'mistl=info,mistl::vault=debug' } });
    } finally { closeSync(logFile); }
    this.child = child;
    this.exited = false;
    child.once('error', error => { this.spawnError = error; this.exited = true; });
    child.once('exit', () => { this.exited = true; });
    await until(`${this.name} startup`, signal, async () => {
      if (this.spawnError) throw this.spawnError;
      if (this.exited) throw new Error(`${this.name}: spawned daemon exited before becoming ready`);
      if (!existsSync(this.infoPath)) return false;
      const status = await this.call('daemon.status', {}, signal, 5000);
      assert.equal(status.network?.state, 'on', `${this.name}: network on`);
      assert.equal(status.dashboard, null, `${this.name}: dashboard disabled`);
      return true;
    }, { interval: 250, retry: error => /ECONNREFUSED|ENOENT|IPC closed before response/i.test(error.message) });
  }

  async stop(signal) {
    assert.ok(this.child && !this.exited, `${this.name}: daemon is not running`);
    try { await this.call('vault.status', {}, signal, 5000); } catch { /* Preserve any earlier snapshot. */ }
    await this.call('daemon.stop', {}, signal, 10000);
    await until(`${this.name} shutdown`, signal, () => this.exited, { interval: 100 });
  }

  async cleanup() {
    if (!this.child || this.exited) return;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error('shutdown timeout')), 15000);
    try { await this.stop(controller.signal); }
    catch (error) {
      if (!this.exited) {
        // This is the child handle created by this script, never a PID from discovery.
        if (!this.child.kill()) throw new Error(`${this.name}: could not stop owned child: ${redact(error.message)}`);
        const deadline = Date.now() + 10000;
        while (!this.exited && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 100));
        if (!this.exited) throw new Error(`${this.name}: owned child did not exit`);
      }
    } finally { clearTimeout(timer); }
  }
}

function ownerEntry(status, owner) {
  assert.ok(Array.isArray(status?.owners), 'vault.status must contain owners');
  return status.owners.find(entry => entry.owner === owner);
}

async function statuses(nodes, signal) {
  return await Promise.all(nodes.map(node => node.call('vault.status', {}, signal)));
}

async function waitReplicas(nodes, owner, seq, count, state, signal, target = 3) {
  await until(`${owner.slice(0, 16)} replication on ${nodes.map(node => node.name).join(', ')}`, signal, async () => {
    const all = await statuses(nodes, signal);
    return all.every(status => {
      const entry = ownerEntry(status, owner);
      return entry && entry.seq === seq && entry.target === target && entry.confirmed === count && entry.state === state;
    });
  });
}

async function waitMembers(nodes, count, signal) {
  await until(`${count} online group members`, signal, async () => {
    const all = await statuses(nodes, signal);
    return all.every(status => {
      const members = status?.group?.members;
      return Array.isArray(members) && members.length === count
        && new Set(members.map(member => member.did)).size === count && members.every(member => member.online === true);
    });
  });
}

async function inviteAndJoin(inviter, joiner, signal) {
  const invite = await inviter.call('vault.invite', {}, signal);
  assert.ok(typeof invite?.code === 'string' && invite.code.startsWith('mvi1-'), 'vault.invite returned a code');
  const joined = await joiner.call('vault.join', { code: invite.code }, signal, 120000);
  assert.ok(typeof joined?.owner === 'string' && joined.owner.startsWith('did:key:'), 'vault.join returned an owner');
  assert.deepEqual(joined.group, inviter.group, 'joined the expected group');
  joiner.owner = joined.owner;
  joiner.group = joined.group;
}

function fixture(context, name, size) {
  const file = path.join(context.fixtures, name);
  const bytes = randomBytes(size);
  writeFileSync(file, bytes);
  checkBudget(context.root);
  return { file, bytes };
}

async function put(node, fixtureFile, signal) {
  const result = await node.call('vault.put', { path: fixtureFile.file }, signal, 120000);
  assert.equal(result?.name, path.basename(fixtureFile.file), 'vault.put file name');
  assert.equal(result?.size, fixtureFile.bytes.length, 'vault.put file size');
  assert.ok(Number.isSafeInteger(result?.seq) && result.seq >= 1, 'vault.put sequence');
  return result.seq;
}

async function initializeGroup(nodes, signal) {
  await Promise.all(nodes.map(node => node.start(signal)));
  for (const node of nodes) {
    node.did = (await node.call('key.did', {}, signal))?.did;
    assert.ok(typeof node.did === 'string' && node.did.startsWith('did:key:'), `${node.name}: node DID`);
  }
  assert.equal(new Set(nodes.map(node => node.did)).size, nodes.length, 'independent node identities');
  const [creator, ...joiners] = nodes;
  const init = await creator.call('vault.init', {}, signal, 120000);
  assert.ok(typeof init?.owner === 'string' && init.owner.startsWith('did:key:'), 'vault.init returned an owner');
  assert.ok(init.group && init.room, 'vault.init returned group and room');
  creator.owner = init.owner;
  creator.group = init.group;
  for (const node of joiners) await inviteAndJoin(creator, node, signal);
  await waitMembers(nodes, nodes.length, signal);
}

async function assertGet(node, file, signal) {
  const name = path.basename(file.file);
  const result = await node.call('vault.get', { name }, signal, 120000);
  assert.equal(result?.name, name, 'vault.get file name');
  assert.equal(result?.size, file.bytes.length, 'vault.get file size');
  assert.ok(typeof result?.data_b64 === 'string', 'vault.get returned inline data');
  assert.deepEqual(Buffer.from(result.data_b64, 'base64'), file.bytes, 'vault.get returned byte-identical data');
}

const steps = [
  { name: 'Start A, B and C; initialize and join the three-member group', timeout: 300000, async run(ctx, signal) {
    const { A, B, C } = ctx;
    await Promise.all([A, B, C].map(node => node.start(signal)));
    for (const node of [A, B, C]) {
      node.did = (await node.call('key.did', {}, signal))?.did;
      assert.ok(typeof node.did === 'string' && node.did.startsWith('did:key:'), `${node.name}: node DID`);
    }
    assert.equal(new Set([A.did, B.did, C.did]).size, 3, 'independent node identities');
    const init = await A.call('vault.init', {}, signal, 120000);
    assert.ok(typeof init?.owner === 'string' && init.owner.startsWith('did:key:'), 'vault.init returned an owner');
    assert.ok(init.group && init.room, 'vault.init returned group and room');
    A.owner = init.owner; A.group = init.group;
    await inviteAndJoin(A, B, signal);
    await inviteAndJoin(A, C, signal);
    await waitMembers([A, B, C], 3, signal);
  } },
  { name: 'Replicate A\'s 3 MiB and empty files to three holders', timeout: 300000, async run(ctx, signal) {
    ctx.files = [fixture(ctx, 'large.bin', 3 * MIB), fixture(ctx, 'empty.bin', 0)];
    await put(ctx.A, ctx.files[0], signal);
    ctx.aSeq = await put(ctx.A, ctx.files[1], signal);
    await waitReplicas([ctx.A, ctx.B], ctx.A.owner, ctx.aSeq, 3, 'healthy', signal);
  } },
  { name: 'Replicate B\'s file to three holders', timeout: 300000, async run(ctx, signal) {
    const file = fixture(ctx, 'b-file.bin', 16384);
    ctx.bSeq = await put(ctx.B, file, signal);
    await waitReplicas([ctx.A, ctx.B, ctx.C], ctx.B.owner, ctx.bSeq, 3, 'healthy', signal);
  } },
  { name: 'Stop A and retain its complete data on B and C', timeout: 180000, async run(ctx, signal) {
    await ctx.A.stop(signal);
    await until('B and C hold A\'s current dataset', signal, async () => {
      const all = await statuses([ctx.B, ctx.C], signal);
      return all.every(status => {
        const entry = ownerEntry(status, ctx.A.owner);
        return entry?.seq === ctx.aSeq && Array.isArray(entry.holders)
          && [ctx.B.did, ctx.C.did].every(did => entry.holders.some(holder =>
            holder.did === did && holder.complete === true && holder.seq === ctx.aSeq));
      });
    });
  } },
  { name: 'Restart A and recover three healthy replicas', timeout: 300000, async run(ctx, signal) {
    await ctx.A.start(signal);
    await waitMembers([ctx.A, ctx.B, ctx.C], 3, signal);
    await waitReplicas([ctx.A, ctx.B, ctx.C], ctx.A.owner, ctx.aSeq, 3, 'healthy', signal);
  } },
  { name: 'Stop C and publish a degraded two-holder update on A', timeout: 300000, async run(ctx, signal) {
    await ctx.C.stop(signal);
    const file = fixture(ctx, 'offline.bin', 24576);
    ctx.files.push(file);
    ctx.aSeq = await put(ctx.A, file, signal);
    await waitReplicas([ctx.A, ctx.B], ctx.A.owner, ctx.aSeq, 2, 'degraded', signal);
  } },
  { name: 'Restart C and recover three healthy replicas', timeout: 300000, async run(ctx, signal) {
    await ctx.C.start(signal);
    await waitMembers([ctx.A, ctx.B, ctx.C], 3, signal);
    await waitReplicas([ctx.A, ctx.B, ctx.C], ctx.A.owner, ctx.aSeq, 3, 'healthy', signal);
  } },
  { name: 'Start D, join through B, and restore A\'s byte-identical files', timeout: 420000, async run(ctx, signal) {
    const { A, B, C, D } = ctx;
    await D.start(signal);
    D.did = (await D.call('key.did', {}, signal))?.did;
    assert.ok(typeof D.did === 'string' && ![A.did, B.did, C.did].includes(D.did), 'D has an independent DID');
    await inviteAndJoin(B, D, signal);
    await waitMembers([A, B, C, D], 4, signal);
    const key = await A.call('vault.key.show', {}, signal);
    assert.equal(key?.owner, A.owner, 'recovery key belongs to A');
    assert.ok(typeof key.recovery_code === 'string' && /^mvk1-[0-9a-f]{64}-[0-9a-f]{8}$/i.test(key.recovery_code), 'valid recovery code');
    const restored = await D.call('vault.restore', { code: key.recovery_code }, signal, 180000);
    assert.equal(restored?.owner, A.owner, 'restored A\'s owner');
    const listing = await until('restored catalog', signal, async () => {
      const result = await D.call('vault.ls', { owner: A.owner }, signal);
      return Array.isArray(result?.files) && result.seq === ctx.aSeq && result.files.length === ctx.files.length ? result : false;
    }, { retry: error => /missing|not found|not yet|unavailable|not available|sync first|incomplete|not ready|fetch|timed out|timeout/i.test(error.message) });
    const restoreDir = path.join(ctx.root, 'restored');
    mkdirSync(restoreDir, { recursive: true });
    for (const file of ctx.files) {
      const name = path.basename(file.file);
      const entry = listing.files.find(item => item.name === name);
      assert.ok(entry, `restored listing contains ${name}`);
      assert.equal(entry.size, file.bytes.length, `${name}: listed size`);
      assert.equal(entry.sha256, sha256(file.bytes), `${name}: listed hash`);
      const output = path.join(restoreDir, name);
      // The catalog arrives before every chunk; get succeeds once the copy completes.
      const result = await until(`${name} restored`, signal, () => D.call('vault.get', { name, output }, signal, 120000),
        { retry: error => /not available|sync first|missing|timed out|timeout/i.test(error.message) });
      assert.equal(result?.size, file.bytes.length, `${name}: restored size`);
      assert.deepEqual(readFileSync(output), file.bytes, `${name}: byte-identical restoration`);
      checkBudget(ctx.root);
    }
  } },
  { name: 'Corrupt C\'s descriptor block and verify sync repairs it', timeout: 420000, async run(ctx, signal) {
    const { C } = ctx;
    const file = fixture(ctx, 'c-tamper.bin', 8192);
    const seq = await put(C, file, signal);
    await until('C dataset has three complete holders', signal, async () => {
      const entry = ownerEntry(await C.call('vault.status', {}, signal), C.owner);
      return entry?.seq === seq && entry.target === 3 && entry.confirmed >= 3 && entry.state === 'healthy';
    });
    const listing = await C.call('vault.ls', { owner: C.owner }, signal);
    assert.ok(Array.isArray(listing?.files), 'C catalog listing');
    const cid = listing.files.find(item => item.name === path.basename(file.file))?.object;
    assert.ok(typeof cid === 'string' && /^[a-zA-Z0-9]+$/.test(cid), 'descriptor CID is a safe block filename');
    const block = path.join(C.directory, 'vault', 'blocks', cid);
    const original = readFileSync(block);
    assert.ok(original.length > 0, 'descriptor block is nonempty');
    const damaged = Buffer.from(original);
    damaged[0] ^= 0x80;
    writeFileSync(block, damaged);
    assert.deepEqual(readFileSync(block), damaged, 'block was corrupted before sync');
    await C.call('vault.sync', {}, signal);
    await until('C re-fetches the corrupted block and becomes healthy', signal, async () => {
      let repaired;
      try { repaired = readFileSync(block); }
      catch (error) { if (error.code === 'ENOENT') return false; throw error; }
      const entry = ownerEntry(await C.call('vault.status', {}, signal), C.owner);
      return repaired.equals(original) && entry?.seq === seq && entry.confirmed >= 3 && entry.state === 'healthy';
    }, { interval: 2000 });
  } },
];

const singleSteps = [
  { name: 'Initialize one member and verify a local replica', timeout: 180000, async run(ctx, signal) {
    await initializeGroup([ctx.A], signal);
    ctx.file = fixture(ctx, 'single.bin', 16384);
    ctx.seq = await put(ctx.A, ctx.file, signal);
    await waitReplicas([ctx.A], ctx.A.owner, ctx.seq, 1, 'local_only', signal, 1);
    const listing = await ctx.A.call('vault.ls', {}, signal);
    assert.equal(listing?.seq, ctx.seq, 'listing has the published sequence');
    assert.ok(listing?.files?.some(file => file.name === path.basename(ctx.file.file)), 'listing contains the file');
    await assertGet(ctx.A, ctx.file, signal);
  } },
  { name: 'Remove the single-node file and advance the sequence', timeout: 60000, async run(ctx, signal) {
    const name = path.basename(ctx.file.file);
    const removed = await ctx.A.call('vault.rm', { name }, signal);
    assert.ok(Number.isSafeInteger(removed?.seq) && removed.seq > ctx.seq, 'remove advanced the sequence');
    const listing = await ctx.A.call('vault.ls', {}, signal);
    assert.equal(listing?.seq, removed.seq, 'listing has the removal sequence');
    assert.ok(Array.isArray(listing?.files) && !listing.files.some(file => file.name === name), 'removed file is absent');
    await waitReplicas([ctx.A], ctx.A.owner, removed.seq, 1, 'local_only', signal, 1);
  } },
];

const pairSteps = [
  { name: 'Initialize two members and confirm two healthy replicas', timeout: 300000, async run(ctx, signal) {
    await initializeGroup([ctx.A, ctx.B], signal);
    const file = fixture(ctx, 'pair-initial.bin', 16384);
    ctx.seq = await put(ctx.A, file, signal);
    await waitReplicas([ctx.A, ctx.B], ctx.A.owner, ctx.seq, 2, 'healthy', signal, 2);
  } },
  { name: 'Stop B, observe degradation, and publish locally on A', timeout: 180000, async run(ctx, signal) {
    await ctx.B.stop(signal);
    await waitReplicas([ctx.A], ctx.A.owner, ctx.seq, 1, 'degraded', signal, 2);
    const file = fixture(ctx, 'pair-offline.bin', 24576);
    const nextSeq = await put(ctx.A, file, signal);
    assert.ok(nextSeq > ctx.seq, 'offline put advanced the sequence');
    ctx.seq = nextSeq;
    await assertGet(ctx.A, file, signal);
    await waitReplicas([ctx.A], ctx.A.owner, ctx.seq, 1, 'degraded', signal, 2);
  } },
  { name: 'Restart B and confirm the new sequence on both members', timeout: 300000, async run(ctx, signal) {
    await ctx.B.start(signal);
    await waitMembers([ctx.A, ctx.B], 2, signal);
    await waitReplicas([ctx.A, ctx.B], ctx.A.owner, ctx.seq, 2, 'healthy', signal, 2);
  } },
];

const replaceSteps = [
  { name: 'Initialize four members and identify three initial holders', timeout: 300000, async run(ctx, signal) {
    const nodes = [ctx.A, ctx.B, ctx.C, ctx.D];
    await initializeGroup(nodes, signal);
    const file = fixture(ctx, 'replace.bin', 1024 * 1024);
    ctx.seq = await put(ctx.A, file, signal);
    const initial = await until('three holders and one member without the current replica', signal, async () => {
      const all = await statuses(nodes, signal);
      const locallyComplete = all.every((status, index) => {
        const entry = ownerEntry(status, ctx.A.owner);
        return entry?.seq === ctx.seq && entry.holders?.some(holder =>
          holder.did === nodes[index].did && holder.complete && holder.seq === ctx.seq);
      });
      const entry = ownerEntry(all[0], ctx.A.owner);
      if (entry?.seq === ctx.seq && entry.confirmed >= 3) {
        assert.ok(!locallyComplete, 'all four members already hold the current replica; no replacement candidate remains');
      }
      if (entry?.seq !== ctx.seq || entry.target !== 3 || entry.confirmed !== 3 || entry.state !== 'healthy') return false;
      const complete = new Set(entry.holders.filter(holder => holder.complete && holder.seq === ctx.seq).map(holder => holder.did));
      if (!complete.has(ctx.A.did) || complete.size !== 3) return false;
      const spareIndex = nodes.findIndex((node, index) => index > 0 && !complete.has(node.did));
      if (spareIndex < 0) return false;
      const spareEntry = ownerEntry(all[spareIndex], ctx.A.owner);
      if (spareEntry?.seq === ctx.seq && spareEntry.holders?.some(holder =>
        holder.did === nodes[spareIndex].did && holder.complete && holder.seq === ctx.seq)) return false;
      return { stopped: nodes.find(node => node !== ctx.A && complete.has(node.did)), spare: nodes[spareIndex] };
    }, { interval: 500 });
    ctx.stopped = initial.stopped;
    ctx.spare = initial.spare;
    log(`Initial holders include A and ${ctx.stopped.name}; ${ctx.spare.name} has no complete current replica`);
  } },
  { name: 'Replace a stopped holder with the previously unconfirmed member', timeout: 150000, async run(ctx, signal) {
    await ctx.stopped.stop(signal);
    await until('three current replicas including the replacement', signal, async () => {
      const status = await ctx.A.call('vault.status', {}, signal);
      const entry = ownerEntry(status, ctx.A.owner);
      return entry?.seq === ctx.seq && entry.target === 3 && entry.confirmed === 3 && entry.state === 'healthy'
        && entry.holders.some(holder => holder.did === ctx.spare.did && holder.complete && holder.seq === ctx.seq)
        && !entry.holders.some(holder => holder.did === ctx.stopped.did && holder.complete && holder.seq === ctx.seq);
    });
  } },
  { name: 'Restart the old holder and retain a healthy dataset', timeout: 300000, async run(ctx, signal) {
    await ctx.stopped.start(signal);
    const nodes = [ctx.A, ctx.B, ctx.C, ctx.D];
    await waitMembers(nodes, 4, signal);
    await until('healthy dataset after the old holder restarts', signal, async () => {
      const all = await statuses(nodes, signal);
      return all.every(status => {
        const entry = ownerEntry(status, ctx.A.owner);
        return status.last_error == null && entry?.seq === ctx.seq && entry.target === 3
          && entry.confirmed >= 3 && entry.state === 'healthy' && entry.fork === false;
      });
    });
  } },
];

const scenarios = {
  existing: { nodes: ['A', 'B', 'C', 'D'], steps },
  single: { nodes: ['A'], steps: singleSteps },
  pair: { nodes: ['A', 'B'], steps: pairSteps },
  replace: { nodes: ['A', 'B', 'C', 'D'], steps: replaceSteps },
};

async function dumpStatuses(nodes, outputPath) {
  const snapshots = await Promise.all(nodes.map(async node => {
    if (!node.child) {
      console.error(`${node.name} vault.status: not started`);
      return [node.name, { error: 'not started' }];
    }
    let status;
    let cached = node.exited;
    let failure;
    try { status = node.exited ? node.lastStatus : await node.call('vault.status', {}, undefined, 5000); }
    catch (error) { failure = error; status = node.lastStatus; cached = true; }
    if (!status) {
      const message = redact(failure?.message || 'stopped without a snapshot');
      console.error(`${node.name} vault.status unavailable: ${message}`);
      return [node.name, { error: message }];
    }
    const safe = redact(JSON.stringify(status, (key, value) =>
      /(?:^|_)(?:token|password|secret|recovery|private|seed|dek|mac|code|key)(?:$|_)/i.test(key) ? '***' : value));
    const capturedAt = new Date(node.lastStatusAt).toISOString();
    const when = cached ? ` (last snapshot ${capturedAt})` : '';
    console.error(`${node.name} vault.status${when}: ${safe}`);
    return [node.name, { captured_at: capturedAt, cached, status: JSON.parse(safe) }];
  }));
  if (outputPath) {
    writeFileSync(outputPath, JSON.stringify(Object.fromEntries(snapshots), null, 2) + '\n');
    log(`saved status snapshots: ${outputPath}`);
  }
}

async function runScenario(options, name) {
  const scenario = scenarios[name];
  const root = mkdtempSync(path.join(tmpdir(), 'mistl-vault-live-'));
  const nodes = scenario.nodes.map(node => new Node(node, root, options.binary, name));
  const ctx = { root, fixtures: path.join(root, 'fixtures'), ...Object.fromEntries(nodes.map(node => [node.name, node])) };
  const controller = new AbortController();
  const onSignal = name => controller.abort(new Error(`received ${name}`));
  const sigint = () => onSignal('SIGINT');
  const sigterm = () => onSignal('SIGTERM');
  process.on('SIGINT', sigint);
  process.on('SIGTERM', sigterm);
  let failure = null;
  const monitor = setInterval(() => {
    try { checkBudget(root); } catch (error) { controller.abort(error); }
  }, 500);
  try {
    log(`SCENARIO: ${name}`);
    mkdirSync(ctx.fixtures, { recursive: true });
    for (const step of scenario.steps) {
      const stepStart = Date.now();
      const timeout = new AbortController();
      const timer = setTimeout(() => timeout.abort(new Error(`step timed out after ${step.timeout / 1000}s`)), step.timeout);
      const signal = AbortSignal.any([controller.signal, timeout.signal]);
      log(`START: ${step.name}`);
      try {
        checkBudget(root);
        if (signal.aborted) throw signal.reason;
        await step.run(ctx, signal);
        if (signal.aborted) throw signal.reason;
        checkBudget(root);
        log(`PASS: ${step.name} (${((Date.now() - stepStart) / 1000).toFixed(1)}s)`);
      } catch (error) {
        log(`FAIL: ${step.name} (${((Date.now() - stepStart) / 1000).toFixed(1)}s): ${redact(error.message)}`);
        if (!timeout.signal.aborted) timeout.abort(error);
        throw error;
      } finally { clearTimeout(timer); }
    }
    log(`SCENARIO ${name} PASS; temporary state ${checkBudget(root)} bytes`);
  } catch (error) {
    failure = error;
    console.error(`FAIL: ${redact(error.stack || error)}`);
    await dumpStatuses(nodes, options.keep ? path.join(root, 'statuses.json') : undefined);
  } finally {
    clearInterval(monitor);
    const results = await Promise.allSettled(nodes.map(node => node.cleanup()));
    let cleanupFailed = false;
    for (const result of results) {
      if (result.status === 'rejected') {
        cleanupFailed = true;
        failure ||= result.reason;
        console.error(`cleanup: ${redact(result.reason?.message || result.reason)}`);
      }
    }
    if (cleanupFailed) await dumpStatuses(nodes);
    const live = nodes.filter(node => node.child && !node.exited);
    const safeRoot = path.dirname(root) === path.resolve(tmpdir()) && path.basename(root).startsWith('mistl-vault-live-');
    if (!options.keep && live.length === 0 && safeRoot) {
      try { rmSync(root, { recursive: true, force: true }); }
      catch (error) { failure ||= error; console.error(`cleanup: ${redact(error.message)}`); }
    } else log(`retained isolated state: ${root}${live.length ? ' (owned process still running)' : ''}`);
    process.removeListener('SIGINT', sigint);
    process.removeListener('SIGTERM', sigterm);
  }
  return !failure;
}

function selectedScenarios(options) {
  return options.scenario === 'all' ? Object.keys(scenarios) : [options.scenario];
}

async function main(options) {
  assert.ok(existsSync(options.binary), 'provide an existing development binary');
  const version = spawnSync(options.binary, ['--version'], { encoding: 'utf8', windowsHide: true, timeout: 10000 });
  assert.equal(version.status, 0, 'binary version check');
  assert.match(version.stdout, /\bdev\b/, 'integration runs only a development binary');
  for (const name of selectedScenarios(options)) {
    if (!await runScenario(options, name)) { process.exitCode = 1; return; }
  }
  log('ALL SCENARIOS PASS');
}

try {
  const options = optionsFrom(process.argv.slice(2));
  if (options.help) {
    console.log('Usage: node scripts/test-vault.mjs [binary] [--bin binary] [--scenario name] [--keep-state] [--dry-run]');
    console.log('Scenarios: all (default), existing, single, pair, replace. Each uses fresh daemons and state.');
    console.log('MISTL_BIN overrides the default development binary; --dry-run starts no processes.');
    console.log('Uses isolated temporary state, public P2P signaling, and less than 64 MiB of temporary disk.');
  } else if (options.dryRun) {
    console.log(`DRY RUN: development binary ${options.binary}`);
    console.log('Network on, UI and updates off; temporary disk limit 64 MiB per scenario.');
    for (const name of selectedScenarios(options)) {
      const scenario = scenarios[name];
      console.log(`Scenario ${name}: isolated ${scenario.nodes.join('/')}`);
      for (const [index, step] of scenario.steps.entries()) console.log(`${index + 1}. ${step.name} (timeout ${step.timeout / 1000}s)`);
    }
    console.log('On failure: dump vault.status; always stop owned daemons and clean up temporary state.');
    if (options.keep) console.log('Temporary state will be retained when run without --dry-run.');
  } else await main(options);
} catch (error) {
  console.error(`FAIL: ${redact(error.stack || error)}`);
  process.exitCode = 1;
}
