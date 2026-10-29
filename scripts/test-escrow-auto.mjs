// Live escrow mesh checks over isolated development daemons and real P2P signaling.
// Usage: node scripts/test-escrow-auto.mjs [binary] [--scenario all|basic|failover|delete|replace] [--dry-run] [--keep-state]
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createCipheriv, createDecipheriv, createHash, generateKeyPairSync, randomBytes, sign, verify } from 'node:crypto';
import { closeSync, existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readFileSync, readSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIB = 1024 * 1024;
const MAX_DISK = 64 * MIB;
const MAX_IPC = MIB;
const started = Date.now();
const log = message => console.log(`[${((Date.now() - started) / 1000).toFixed(1)}s] ${message}`);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const redact = value => String(value).replace(/(token=)[^\s&"']+/gi, '$1***')
  .replace(/("(?:token|secret|password|private_key|encryption_key)"\s*:\s*")[^"\r\n]*(")/gi, '$1***$2');
const stable = value => value === null || typeof value !== 'object' ? JSON.stringify(value)
  : Array.isArray(value) ? '[' + value.map(stable).join(',') + ']'
  : '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + stable(value[key])).join(',') + '}';
const transient = error => /busy|fetch_failed|fetch failed|not available|not found|timed out|timeout|ECONNRESET|ECONNREFUSED|IPC closed|no route|not connected|channel.*closed/i.test(error.message);

function optionsFrom(argv) {
  const options = { binary: null, scenario: null, keep: false, dryRun: false, help: false };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === '--help') options.help = true;
    else if (arg === '--dry-run') options.dryRun = true;
    else if (arg === '--keep-state') options.keep = true;
    else if (arg === '--bin' || arg === '--scenario') {
      const key = arg === '--bin' ? 'binary' : 'scenario';
      assert.equal(options[key], null, `specify only one ${key}`);
      assert.ok(argv[index + 1] && !argv[index + 1].startsWith('--'), `${arg} requires a value`);
      options[key] = argv[++index];
    } else {
      assert.ok(!arg.startsWith('--'), `unknown option: ${arg}`);
      assert.equal(options.binary, null, 'specify only one binary');
      options.binary = arg;
    }
  }
  options.binary = path.resolve(options.binary || process.env.MISTL_BIN
    || path.join(REPO, `target/debug/mistl${process.platform === 'win32' ? '.exe' : ''}`));
  options.scenario ||= 'all';
  assert.ok(['all', 'basic', 'failover', 'delete', 'replace'].includes(options.scenario), `unknown scenario: ${options.scenario}`);
  return options;
}

function sleep(milliseconds, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const timer = setTimeout(done, milliseconds);
    function done() { signal?.removeEventListener('abort', abort); resolve(); }
    function abort() { clearTimeout(timer); reject(signal.reason); }
    signal?.addEventListener('abort', abort, { once: true });
  });
}

async function until(label, signal, fn, { interval = 1000, retry } = {}) {
  let last;
  while (!signal.aborted) {
    try { const value = await fn(); if (value) return value; }
    catch (error) { if (!retry?.(error)) throw error; last = error; }
    try { await sleep(interval, signal); } catch { break; }
  }
  throw new Error(`${label}: ${redact(signal.reason?.message || 'interrupted')}${last ? `; last error: ${redact(last.message)}` : ''}`);
}

function diskBytes(directory) {
  let total = 0;
  let entries;
  try { entries = readdirSync(directory); }
  catch (error) { if (error.code === 'ENOENT') return 0; throw error; }
  for (const entry of entries) {
    const file = path.join(directory, entry);
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

// Refuse a stale executable or a partially implemented mesh. Build only after a
// quiet source window, and discard the result if another worker edits during it.
function sourceSnapshot() {
  const files = [];
  function visit(directory) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) visit(file);
      else if (entry.name.endsWith('.rs')) files.push(file);
    }
  }
  visit(path.join(REPO, 'src'));
  files.push(path.join(REPO, 'Cargo.toml'), path.join(REPO, 'Cargo.lock'));
  return hash(files.sort().map(file => `${file}:${lstatSync(file).mtimeMs}:${hash(readFileSync(file))}`).join('\n'));
}

async function preflight(options) {
  const mesh = path.join(REPO, 'src/escrow/mesh');
  const files = existsSync(mesh) ? readdirSync(mesh).filter(file => file.endsWith('.rs') && !/test/.test(file)) : [];
  const engine = files.filter(file => !['placement.rs', 'wire.rs'].includes(file))
    .map(file => readFileSync(path.join(mesh, file), 'utf8').replace(/\/\/[^\n]*/g, '')).join('\n');
  assert.ok(/\b(loop|while)\b/.test(engine) && /\b(?:replicat|reconcile|fetch|pull|tick)\w*/.test(engine),
    'mesh replication loop is not present in src/escrow/mesh/*.rs; use --dry-run until the engine is ready');
  log('Preflight: waiting for a 60s Rust source quiet window before cargo build');
  let snapshot = sourceSnapshot();
  let quietSince = Date.now();
  const deadline = Date.now() + 180000;
  while (Date.now() - quietSince < 60000) {
    assert.ok(Date.now() < deadline, 'workers are still editing Rust sources; retry the live harness later');
    await sleep(5000);
    const current = sourceSnapshot();
    if (current !== snapshot) { snapshot = current; quietSince = Date.now(); log('Preflight: source changed; waiting for workers to finish'); }
  }
  log('Preflight: cargo build');
  await new Promise((resolve, reject) => {
    const child = spawn('cargo', ['build'], { cwd: REPO, windowsHide: true, stdio: 'inherit' });
    child.once('error', reject);
    child.once('exit', code => code === 0 ? resolve() : reject(new Error(`cargo build failed (${code}); wait 1-2 minutes before retrying if another worker is mid-edit`)));
  });
  assert.equal(sourceSnapshot(), snapshot, 'Rust sources changed during cargo build; retry once workers finish');
  assert.ok(existsSync(options.binary), 'provide an existing development binary');
  const built = path.join(REPO, `target/debug/mistl${process.platform === 'win32' ? '.exe' : ''}`);
  if (path.resolve(options.binary) !== path.resolve(built)) {
    assert.equal(hash(readFileSync(options.binary)), hash(readFileSync(built)),
      'custom binary must match the freshly built development binary');
  }
  const version = spawnSync(options.binary, ['--version'], { encoding: 'utf8', windowsHide: true, timeout: 10000 });
  assert.equal(version.status, 0, 'binary version check');
  assert.match(version.stdout, /\bdev\b/, 'integration runs only a development binary');
}

class Node {
  constructor(name, ctx, binary) {
    this.name = name;
    this.ctx = ctx;
    this.binary = binary;
    this.directory = path.join(ctx.root, 'dev', name, 'data');
    this.infoPath = path.join(this.directory, 'daemon.json');
    this.child = null;
    this.exited = true;
    this.snapshots = {};
  }

  prepare() {
    const config = path.join(this.ctx.root, 'dev', this.name, 'config');
    if (existsSync(config)) return;
    mkdirSync(config, { recursive: true });
    mkdirSync(this.directory, { recursive: true });
    writeFileSync(path.join(config, 'config.toml'), '[ui]\nenabled = false\nlisten = "127.0.0.1:0"\n'
      + '[update]\nauto_check = false\nauto_apply = false\n'
      + `[storage]\ncapacity_bytes = ${4 * MIB}\nroom_ids = [${JSON.stringify(this.ctx.room)}]\n`
      + `[escrow]\nenabled = true\nrooms = [${JSON.stringify(this.ctx.room)}]\nauto_replicas = 3\ndead_after_secs = 60\n`
      + `max_blob_bytes = ${4 * MIB}\nmax_total_bytes = ${16 * MIB}\nmax_slots_per_room = 16\n`);
    writeFileSync(path.join(this.directory, 'network-state.json'), JSON.stringify({ schema: 1, enabled: true }) + '\n');
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
          if (cmd === 'escrow.status' || cmd === 'escrow.auto.slots') this.snapshots[cmd] = { at: new Date().toISOString(), data: value };
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
    try {
      this.child = spawn(this.binary, ['--state-dir', this.ctx.root, '--instance', this.name, '--no-tray', 'daemon', 'run'],
        { windowsHide: true, stdio: ['ignore', logFile, logFile],
          env: { ...process.env, RUST_LOG: process.env.RUST_LOG || 'mistl=info,mistl::escrow::mesh=debug' } });
    } finally { closeSync(logFile); }
    this.exited = false;
    this.child.once('error', error => { this.spawnError = error; this.exited = true; });
    this.child.once('exit', () => { this.exited = true; });
    await until(`${this.name} startup`, signal, async () => {
      if (this.spawnError) throw this.spawnError;
      if (this.exited) throw new Error(`${this.name}: daemon exited before becoming ready`);
      if (!existsSync(this.infoPath)) return false;
      // A previous discovery file can briefly survive a restart.
      if (JSON.parse(readFileSync(this.infoPath, 'utf8')).pid !== this.child.pid) return false;
      const status = await this.call('daemon.status', {}, signal, 5000);
      assert.equal(status.network?.state, 'on', `${this.name}: network on`);
      assert.equal(status.dashboard, null, `${this.name}: dashboard disabled`);
      return true;
    }, { interval: 250, retry: error => /ECONNREFUSED|ENOENT|IPC closed before response/.test(error.message) });
  }

  async stop(signal) {
    assert.ok(this.child && !this.exited, `${this.name}: daemon is not running`);
    await Promise.allSettled(['escrow.status', 'escrow.auto.slots'].map(cmd => this.call(cmd, { room: this.ctx.room }, signal, 5000)));
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
        // Kill only the child handle created here, never a discovery-file PID.
        if (!this.child.kill()) throw new Error(`${this.name}: could not stop owned child: ${redact(error.message)}`);
        const deadline = Date.now() + 10000;
        while (!this.exited && Date.now() < deadline) await sleep(100);
        assert.ok(this.exited, `${this.name}: owned child did not exit`);
      }
    } finally { clearTimeout(timer); }
  }

  async status(signal) {
    const result = await this.call('escrow.status', {}, signal);
    assert.ok(Array.isArray(result?.rooms), `${this.name}: escrow.status rooms`);
    const entry = result.rooms.find(entry => entry.room === this.ctx.room);
    assert.ok(entry?.auto, `${this.name}: automatic mesh status is present`);
    assert.ok(!entry.replication, `${this.name}: no Raft replication configured`);
    assert.ok(Array.isArray(entry.auto.providers) && entry.auto.providers.length <= 16, `${this.name}: bounded providers`);
    return entry.auto;
  }

  async slots(signal) {
    const result = await this.call('escrow.auto.slots', { room: this.ctx.room }, signal);
    assert.equal(result?.room, this.ctx.room);
    assert.ok(Array.isArray(result.slots) && result.slots.length <= 1024, `${this.name}: bounded slot listing`);
    return result.slots;
  }

  blob(head) {
    const key = hash(`${this.ctx.room}\n${head.slot}`);
    return path.join(this.directory, 'escrow', 'blobs', hash(`${key}\n${hash(stable(head))}`), 'blob.bin');
  }

  async request(provider, request, signal) {
    assert.notEqual(this, provider, 'public escrow requests use two different transport peers');
    return await this.call('escrow.client.request', { room: this.ctx.room, peer: provider.id, request }, signal, 40000);
  }
}

async function waitProviders(nodes, signal) {
  await until('all providers discover the shared escrow room', signal, async () => {
    const statuses = await Promise.all(nodes.map(node => node.status(signal)));
    return statuses.every(status => nodes.every(node => status.providers.some(peer => peer.did === node.did && peer.online)));
  });
}

async function initialize(ctx, signal) {
  await Promise.all(ctx.nodes.map(node => node.start(signal)));
  for (const node of ctx.nodes) {
    node.did = (await node.call('key.did', {}, signal))?.did;
    assert.ok(typeof node.did === 'string' && node.did.startsWith('did:key:'), `${node.name}: node DID`);
    node.id = hash(node.did).slice(0, 16);
  }
  assert.equal(new Set(ctx.nodes.map(node => node.did)).size, ctx.nodes.length, 'independent identities');
  await waitProviders(ctx.nodes, signal);
}

function signHead(slot, fields) {
  const head = { ...fields };
  delete head.sig;
  const payload = Buffer.from('tc-escrow/v1/head\n' + stable(head));
  head.sig = sign(null, payload, slot.privateKey).toString('base64url');
  assert.ok(verify(null, payload, slot.publicKey, Buffer.from(head.sig, 'base64url')), 'local head signature');
  return head;
}

function newSlot() {
  const { privateKey, publicKey } = generateKeyPairSync('ed25519');
  return { slot: randomBytes(16).toString('hex'), privateKey, publicKey, key: randomBytes(32),
    writer: publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('base64url'), current: null };
}

async function prepareHead(ctx, slot, source, size, signal) {
  const seq = (slot.current?.head.seq || 0) + 1;
  const plaintext = Buffer.concat([Buffer.from(`private escrow auto state ${seq}:`), randomBytes(size)]);
  const nonce = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', slot.key, nonce);
  cipher.setAAD(Buffer.from(`tc-escrow/v1|${slot.slot}|${seq}`));
  const ciphertext = Buffer.concat([nonce, cipher.update(plaintext), cipher.final(), cipher.getAuthTag()]);
  assert.ok(!ciphertext.includes(plaintext.subarray(0, 20)), 'ciphertext hides the test marker');
  const upload = path.join(ctx.root, 'tc-escrow.bin');
  writeFileSync(upload, ciphertext);
  const stored = await source.call('store.put', { path: upload }, signal, 60000);
  const head = signHead(slot, { v: 1, type: 'tc-escrow-head', slot: slot.slot, seq,
    parent: slot.current ? hash(stable(slot.current.head)) : null, cid: stored.cid, size: ciphertext.length,
    sha256: hash(ciphertext), comp: 'none', schema: 'escrow-auto-live/1', writer: slot.writer, at: Date.now() });
  checkBudget(ctx.root);
  return { head, plaintext, ciphertext };
}

async function putHead(source, provider, head, signal) {
  await until(`PUT ${head.slot} seq ${head.seq} via ${provider.name}`, signal, async () => {
    const reply = await source.request(provider, { v: 1, type: 'escrow_put', head }, signal);
    if (reply.type === 'escrow_error' && ['busy', 'fetch_failed'].includes(reply.code)) return false;
    assert.equal(reply.type, 'escrow_ack', `PUT rejected: ${reply.code || reply.type}`);
    assert.equal(reply.seq, head.seq);
    assert.equal(reply.hash, hash(stable(head)));
    return true;
  }, { retry: transient });
}

async function put(ctx, slot, source, provider, size, signal) {
  const value = await prepareHead(ctx, slot, source, size, signal);
  await putHead(source, provider, value.head, signal);
  slot.current = value;
  return value;
}

async function get(source, provider, slot, signal) {
  return await source.request(provider, { v: 1, type: 'escrow_get', slot: slot.slot }, signal);
}

async function waitHead(source, provider, slot, signal) {
  await until(`${provider.name} has seq ${slot.current.head.seq}`, signal, async () => {
    const reply = await get(source, provider, slot, signal);
    assert.equal(reply.type, 'escrow_head', `GET: ${reply.code || reply.type}`);
    if (reply.head === null || reply.head.seq < slot.current.head.seq) return false;
    assert.deepEqual(reply.head, slot.current.head, 'GET returns the exact newest signed head');
    return true;
  }, { retry: transient });
}

async function healthy(nodes, count, signal) {
  await until(`${count} healthy slots on ${nodes.map(node => node.name).join('/')}`, signal, async () => {
    const statuses = await Promise.all(nodes.map(node => node.status(signal)));
    return statuses.every(status => status.slots === count && status.healthy === count && status.target === 3
      && status.conflicts === 0 && status.insufficient_capacity === false);
  });
}

async function restore(ctx, node, slot, signal) {
  const output = path.join(ctx.root, `restored-${node.name}-${slot.slot}.bin`);
  assert.deepEqual(readFileSync(node.blob(slot.current.head)), slot.current.ciphertext, `${node.name}: durable escrow ciphertext`);
  // C has not published these CIDs into its ordinary block cache. Its store
  // resolver must retrieve escrow blocks, as in test-escrow-replication.mjs.
  await until(`${node.name} restores encrypted bytes`, signal,
    () => node.call('store.get', { id: slot.current.head.cid, output }, signal, 60000), { retry: transient });
  const ciphertext = readFileSync(output);
  assert.deepEqual(ciphertext, slot.current.ciphertext, 'retrieved byte-identical ciphertext');
  assert.equal(hash(ciphertext), slot.current.head.sha256, 'signed ciphertext digest');
  const decipher = createDecipheriv('aes-256-gcm', slot.key, ciphertext.subarray(0, 12));
  decipher.setAAD(Buffer.from(`tc-escrow/v1|${slot.slot}|${slot.current.head.seq}`));
  decipher.setAuthTag(ciphertext.subarray(-16));
  const plaintext = Buffer.concat([decipher.update(ciphertext.subarray(12, -16)), decipher.final()]);
  assert.deepEqual(plaintext, slot.current.plaintext, 'client-only decryption restores original bytes');
  checkBudget(ctx.root);
}

const setup = { name: 'Start isolated providers in one automatic escrow room', timeout: 210000, run: initialize };
const seed = { name: 'PUT an encrypted slot via A and confirm three healthy replicas', timeout: 180000, async run(ctx, signal) {
  ctx.slot = newSlot();
  await put(ctx, ctx.slot, ctx.B, ctx.A, 64 * 1024, signal);
  await healthy(ctx.nodes, 1, signal);
} };

const scenarios = {
  basic: { names: ['A', 'B', 'C'], steps: [setup,
    { name: 'PUT three signed slots via A, including a multi-chunk 3 MiB blob', timeout: 240000, async run(ctx, signal) {
      ctx.slots = [newSlot(), newSlot(), newSlot()];
      for (const [index, slot] of ctx.slots.entries()) await put(ctx, slot, ctx.B, ctx.A, [3 * MIB, 16384, 49152][index], signal);
      await healthy(ctx.nodes, 3, signal);
    } },
    { name: 'GET each exact head from C and verify ciphertext and decrypted bytes on C', timeout: 180000, async run(ctx, signal) {
      for (const slot of ctx.slots) {
        await waitHead(ctx.B, ctx.C, slot, signal);
        await restore(ctx, ctx.C, slot, signal);
      }
    } },
  ] },
  failover: { names: ['A', 'B', 'C'], steps: [setup, seed,
    { name: 'Stop A, PUT seq+1 via B, and GET the new head from C', timeout: 180000, async run(ctx, signal) {
      ctx.old = ctx.slot.current;
      await ctx.A.stop(signal);
      await put(ctx, ctx.slot, ctx.C, ctx.B, 24576, signal);
      await waitHead(ctx.B, ctx.C, ctx.slot, signal);
    } },
    { name: 'Restart A and converge to the new head without rollback', timeout: 210000, async run(ctx, signal) {
      await ctx.A.start(signal);
      await waitProviders(ctx.nodes, signal);
      await waitHead(ctx.B, ctx.A, ctx.slot, signal);
      await healthy(ctx.nodes, 1, signal);
      for (const provider of ctx.nodes) await waitHead(ctx.nodes.find(node => node !== provider), provider, ctx.slot, signal);
    } },
    { name: 'Reject a signed stale-parent PUT to restarted A with conflict', timeout: 60000, async run(ctx, signal) {
      // A retry of a retained previous head may ACK by design. Sign a new
      // successor with the old parent to exercise the CAS conflict instead.
      const stale = signHead(ctx.slot, { ...ctx.slot.current.head, seq: ctx.slot.current.head.seq + 1,
        parent: hash(stable(ctx.old.head)), at: Date.now() });
      const reply = await ctx.B.request(ctx.A, { v: 1, type: 'escrow_put', head: stale }, signal);
      assert.equal(reply.type, 'escrow_error');
      assert.equal(reply.code, 'conflict');
      assert.deepEqual(reply.head, ctx.slot.current.head, 'conflict includes the latest head');
      await waitHead(ctx.B, ctx.A, ctx.slot, signal);
    } },
  ] },
  delete: { names: ['A', 'B', 'C'], steps: [setup, seed,
    { name: 'DELETE via B and propagate the tombstone to A and C', timeout: 180000, async run(ctx, signal) {
      const fields = { slot: ctx.slot.slot, seq: ctx.slot.current.head.seq, head: hash(stable(ctx.slot.current.head)) };
      const sig = sign(null, Buffer.from('tc-escrow/v1/delete\n' + stable(fields)), ctx.slot.privateKey).toString('base64url');
      const reply = await ctx.A.request(ctx.B, { v: 1, type: 'escrow_delete', ...fields, sig }, signal);
      assert.equal(reply.type, 'escrow_deleted', `DELETE: ${reply.code || reply.type}`);
      await until('deleted GET on every provider', signal, async () => {
        const replies = await Promise.all(ctx.nodes.map(provider => get(ctx.nodes.find(node => node !== provider), provider, ctx.slot, signal)));
        return replies.every(reply => reply.type === 'escrow_head' && reply.head === null);
      }, { retry: transient });
    } },
    { name: 'Reject re-PUT of the deleted head on every provider', timeout: 150000, async run(ctx, signal) {
      for (const provider of ctx.nodes) {
        const source = ctx.nodes.find(node => node !== provider);
        const reply = await source.request(provider, { v: 1, type: 'escrow_put', head: ctx.slot.current.head }, signal);
        assert.equal(reply.type, 'escrow_error');
        assert.equal(reply.code, 'deleted', 'tombstone rejects resurrection with the replicated-room code');
      }
    } },
    { name: 'Accept same-writer delete-continue successor and replicate it', timeout: 180000, async run(ctx, signal) {
      const deleted = ctx.slot.current.head;
      await put(ctx, ctx.slot, ctx.A, ctx.B, 32768, signal);
      assert.equal(ctx.slot.current.head.seq, deleted.seq + 1);
      assert.equal(ctx.slot.current.head.parent, hash(stable(deleted)));
      assert.equal(ctx.slot.current.head.writer, deleted.writer);
      for (const provider of ctx.nodes) await waitHead(ctx.nodes.find(node => node !== provider), provider, ctx.slot, signal);
      await healthy(ctx.nodes, 1, signal);
      await restore(ctx, ctx.C, ctx.slot, signal);
    } },
  ] },
  replace: { names: ['A', 'B', 'C', 'D'], steps: [setup,
    { name: 'PUT via A and identify exactly three holders and an unused provider', timeout: 210000, async run(ctx, signal) {
      ctx.slot = newSlot();
      await put(ctx, ctx.slot, ctx.B, ctx.A, MIB, signal);
      await healthy(ctx.nodes, 1, signal);
      const initial = await until('consistent three-holder placement', signal, async () => {
        const rows = await Promise.all(ctx.nodes.map(node => node.slots(signal)));
        const entries = rows.map(slots => slots.find(row => row.slot === ctx.slot.slot));
        if (!entries.every(row => row?.seq === ctx.slot.current.head.seq && row.target === 3 && row.confirmed === 3
          && row.state === 'healthy' && Array.isArray(row.holders) && row.holders.length === 3)) return false;
        const holders = [...entries[0].holders].sort();
        if (!entries.every(row => stable([...row.holders].sort()) === stable(holders))) return false;
        assert.equal(new Set(holders).size, 3, 'distinct holder DIDs');
        assert.ok(holders.includes(ctx.A.did), 'home A holds its accepted write');
        assert.ok(holders.every(did => ctx.nodes.some(node => node.did === did)), 'holders are known providers');
        return { stopped: ctx.nodes.find(node => node !== ctx.A && holders.includes(node.did)),
          spare: ctx.nodes.find(node => !holders.includes(node.did)) };
      });
      ctx.stopped = initial.stopped;
      ctx.spare = initial.spare;
      assert.ok(ctx.stopped && ctx.spare, 'non-A holder and replacement candidate');
      assert.ok(!existsSync(ctx.spare.blob(ctx.slot.current.head)), 'spare has no durable escrow blob before replacement');
      for (const node of ctx.nodes.filter(node => node !== ctx.spare)) {
        assert.deepEqual(readFileSync(node.blob(ctx.slot.current.head)), ctx.slot.current.ciphertext, `${node.name}: initial durable holder`);
      }
      log(`Initial holders: ${ctx.nodes.filter(node => node !== ctx.spare).map(node => node.name).join('/')}; spare ${ctx.spare.name}; stopping ${ctx.stopped.name}`);
    } },
    { name: 'Replace a stopped non-A holder within 150s with dead_after_secs=60', timeout: 150000, async run(ctx, signal) {
      await ctx.stopped.stop(signal);
      const survivors = ctx.nodes.filter(node => node !== ctx.stopped);
      await until('new provider holds the slot and survivors are healthy', signal, async () => {
        const statuses = await Promise.all(survivors.map(node => node.status(signal)));
        const rows = await Promise.all(survivors.map(node => node.slots(signal)));
        return statuses.every(status => status.slots === 1 && status.healthy === 1 && status.target === 3)
          && rows.every(slots => {
            const row = slots.find(row => row.slot === ctx.slot.slot);
            return row?.seq === ctx.slot.current.head.seq && row.target === 3 && row.confirmed === 3 && row.state === 'healthy'
              && row.holders.includes(ctx.spare.did) && !row.holders.includes(ctx.stopped.did);
          });
      });
      await waitHead(ctx.A, ctx.spare, ctx.slot, signal);
      assert.deepEqual(readFileSync(ctx.spare.blob(ctx.slot.current.head)), ctx.slot.current.ciphertext, 'replacement has durable ciphertext');
    } },
    { name: 'Restart the old holder and retain a healthy current slot', timeout: 210000, async run(ctx, signal) {
      await ctx.stopped.start(signal);
      await waitProviders(ctx.nodes, signal);
      await healthy(ctx.nodes, 1, signal);
      for (const provider of ctx.nodes) await waitHead(ctx.nodes.find(node => node !== provider), provider, ctx.slot, signal);
    } },
  ] },
};

async function dumpDiagnostics(nodes) {
  const snapshots = {};
  for (const node of nodes) {
    snapshots[node.name] = {};
    for (const cmd of ['escrow.status', 'escrow.auto.slots']) {
      let error;
      if (node.child && !node.exited) {
        try { await node.call(cmd, { room: node.ctx.room }, undefined, 5000); }
        catch (failure) { error = redact(failure.message); }
      }
      const snapshot = node.snapshots[cmd];
      const result = { cached: node.exited || !!error, ...(snapshot || {}), ...(error ? { error } : {}) };
      if (!snapshot && !error) result.error = 'not started or no saved snapshot';
      snapshots[node.name][cmd] = result;
      console.error(`${node.name} ${cmd}: ${redact(JSON.stringify(result))}`);
    }
    const file = path.join(node.directory, 'daemon.log');
    if (existsSync(file)) {
      const descriptor = openSync(file, 'r');
      try {
        const size = lstatSync(file).size;
        const bytes = Buffer.alloc(Math.min(size, 64 * 1024));
        const count = readSync(descriptor, bytes, 0, bytes.length, Math.max(0, size - bytes.length));
        console.error(`${node.name} daemon.log (last ${count} bytes):\n${redact(bytes.subarray(0, count).toString('utf8'))}`);
      } finally { closeSync(descriptor); }
    }
  }
  return snapshots;
}

async function runScenario(options, name) {
  const scenario = scenarios[name];
  const root = mkdtempSync(path.join(tmpdir(), 'mistl-escrow-auto-'));
  const ctx = { root, room: 'escrow-auto-live-' + randomBytes(8).toString('hex') };
  ctx.nodes = scenario.names.map(name => new Node(name, ctx, options.binary));
  Object.assign(ctx, Object.fromEntries(ctx.nodes.map(node => [node.name, node])));
  const controller = new AbortController();
  const sigint = () => controller.abort(new Error('received SIGINT'));
  const sigterm = () => controller.abort(new Error('received SIGTERM'));
  process.on('SIGINT', sigint);
  process.on('SIGTERM', sigterm);
  let failure = null;
  let diagnosed = false;
  const monitor = setInterval(() => {
    try { checkBudget(root); } catch (error) { controller.abort(error); }
  }, 500);
  try {
    log(`SCENARIO: ${name}`);
    for (const step of scenario.steps) {
      const stepStart = Date.now();
      const timeout = new AbortController();
      const timer = setTimeout(() => timeout.abort(new Error(`step timed out after ${step.timeout / 1000}s`)), step.timeout);
      const signal = AbortSignal.any([controller.signal, timeout.signal]);
      log(`START: ${step.name}`);
      try {
        checkBudget(root);
        signal.throwIfAborted();
        await step.run(ctx, signal);
        signal.throwIfAborted();
        checkBudget(root);
        log(`PASS: ${step.name} (${((Date.now() - stepStart) / 1000).toFixed(1)}s)`);
      } catch (error) {
        log(`FAIL: ${step.name} (${((Date.now() - stepStart) / 1000).toFixed(1)}s): ${redact(error.message)}`);
        timeout.abort(error);
        throw error;
      } finally { clearTimeout(timer); }
    }
    log(`SCENARIO ${name} PASS; temporary state ${checkBudget(root)} bytes`);
  } catch (error) {
    failure = error;
    controller.abort(error);
    console.error(`FAIL: ${redact(error.stack || error)}`);
    const snapshots = await dumpDiagnostics(ctx.nodes);
    diagnosed = true;
    if (options.keep) writeFileSync(path.join(root, 'statuses.json'), redact(JSON.stringify(snapshots, null, 2)) + '\n');
  } finally {
    clearInterval(monitor);
    const results = await Promise.allSettled(ctx.nodes.map(node => node.cleanup()));
    for (const result of results) if (result.status === 'rejected') {
      failure ||= result.reason;
      console.error(`cleanup: ${redact(result.reason?.message || result.reason)}`);
    }
    if (failure && !diagnosed) await dumpDiagnostics(ctx.nodes);
    const live = ctx.nodes.filter(node => node.child && !node.exited);
    const safeRoot = path.dirname(root) === path.resolve(tmpdir()) && path.basename(root).startsWith('mistl-escrow-auto-');
    if (!options.keep && !live.length && safeRoot) {
      try { rmSync(root, { recursive: true, force: true }); }
      catch (error) { failure ||= error; console.error(`cleanup: ${redact(error.message)}`); }
    } else log(`retained isolated state: ${root}${live.length ? ' (owned process still running)' : ''}`);
    process.removeListener('SIGINT', sigint);
    process.removeListener('SIGTERM', sigterm);
  }
  return !failure;
}

try {
  const options = optionsFrom(process.argv.slice(2));
  const selected = options.scenario === 'all' ? Object.keys(scenarios) : [options.scenario];
  if (options.help) {
    console.log('Usage: node scripts/test-escrow-auto.mjs [binary] [--bin binary] [--scenario all|basic|failover|delete|replace] [--keep-state] [--dry-run]');
    console.log('MISTL_BIN overrides the default development binary. Each scenario uses fresh daemons and a unique room.');
    console.log('Live preflight requires the mesh engine, a 60s Rust source quiet window, and a successful cargo build.');
    console.log('Public P2P signaling required; network on, UI/updates off, temporary disk limit 64 MiB per scenario.');
  } else if (options.dryRun) {
    console.log(`DRY RUN: development binary ${options.binary}`);
    console.log('Starts no processes, runs no build, and writes no temporary state.');
    console.log('Live preflight: mesh replication loop present; 60s source quiet window; cargo build succeeds.');
    console.log('Network on, UI/updates off; escrow enabled, shared room, auto_replicas=3, dead_after_secs=60, no escrow.replication.');
    console.log('Temporary disk limit: 64 MiB per scenario.');
    for (const name of selected) {
      const scenario = scenarios[name];
      console.log(`Scenario ${name}: isolated ${scenario.names.join('/')}`);
      for (const [index, step] of scenario.steps.entries()) console.log(`${index + 1}. ${step.name} (timeout ${step.timeout / 1000}s)`);
    }
    console.log('On failure: dump each escrow.status, escrow.auto.slots, and bounded log tails; always clean up owned daemons in finally.');
    if (options.keep) console.log('Temporary state and failure snapshots will be retained during a live run.');
  } else {
    await preflight(options);
    for (const name of selected) {
      if (!await runScenario(options, name)) { process.exitCode = 1; break; }
    }
    if (!process.exitCode) log('ALL SCENARIOS PASS');
  }
} catch (error) {
  console.error(`FAIL: ${redact(error.stack || error)}`);
  process.exitCode = 1;
}
