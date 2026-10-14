// Three isolated daemons over the real P2P transport; no browser or build step.
// Usage: node scripts/test-escrow-replication.mjs [existing-dev-binary] [--keep-state]
// Requires public signaling connectivity and escrow.client.request local IPC.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import net from 'node:net';
import path from 'node:path';
import { createCipheriv, createDecipheriv, createHash, generateKeyPairSync, randomBytes, sign, verify } from 'node:crypto';

if (process.argv.includes('--help')) {
  console.log('Usage: node scripts/test-escrow-replication.mjs [existing-dev-binary] [--keep-state]');
  console.log('Runs three isolated daemons with real P2P signaling, encrypted writes/restoration, leader failover, and snapshot catch-up.');
  console.log('No build, browser, install, or existing-user-state changes. Temporary disk budget: 32 MiB.');
  process.exit(0);
}
const bin = path.resolve(process.argv.slice(2).find(value => !value.startsWith('--')) || `target/debug/mistl${process.platform === 'win32' ? '.exe' : ''}`);
assert.ok(existsSync(bin), 'provide an existing development binary');
const version = spawnSync(bin, ['--version'], { encoding: 'utf8', windowsHide: true, timeout: 10000 });
assert.equal(version.status, 0, 'binary version check');
assert.match(version.stdout, /\bdev\b/, 'integration runs only a development binary');
const keep = process.argv.includes('--keep-state');
const root = mkdtempSync(path.join(tmpdir(), 'mistl-escrow-replication-'));
const room = 'escrow-live-' + randomBytes(8).toString('hex');
const MAX_DISK = 32 * 1024 * 1024;
const started = Date.now();
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
const redact = message => String(message).replace(/token=[^\s&"']+/g, 'token=***').replace(/"token"\s*:\s*"[^"\r\n]*"/g, '"token":"***"');
const log = message => console.log(`[${((Date.now() - started) / 1000).toFixed(1)}s] ${message}`);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const stable = value => value === null || typeof value !== 'object' ? JSON.stringify(value)
  : Array.isArray(value) ? '[' + value.map(stable).join(',') + ']'
  : '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + stable(value[key])).join(',') + '}';

async function until(label, fn, milliseconds = 60000, every = 300) {
  const deadline = Date.now() + milliseconds;
  let last;
  while (Date.now() < deadline) {
    try { const result = await fn(); if (result) return result; }
    catch (error) { last = error; if (/unknown escrow command|unknown command|client.request.*not available|PUT rejected/.test(error.message)) throw error; }
    await sleep(every);
  }
  throw new Error(`timed out waiting for ${label}${last ? ': ' + redact(last.message) : ''}`);
}
function diskBytes(directory) {
  let bytes = 0;
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    assert.ok(!entry.isSymbolicLink(), 'test state must not contain links');
    const file = path.join(directory, entry.name);
    bytes += entry.isDirectory() ? diskBytes(file) : statSync(file).size;
  }
  return bytes;
}
function budget() { const bytes = diskBytes(root); assert.ok(bytes <= MAX_DISK, `temporary state exceeds 32 MiB: ${bytes}`); return bytes; }
function running(pid) { try { process.kill(pid, 0); return true; } catch { return false; } }

class Node {
  constructor(name) {
    this.name = name;
    this.directory = path.join(root, 'dev', name, 'data');
    this.infoPath = path.join(this.directory, 'daemon.json');
    this.owned = new Set();
    this.active = false;
  }
  info() {
    const info = JSON.parse(readFileSync(this.infoPath, 'utf8'));
    assert.equal(info.channel, 'dev'); assert.equal(info.instance, this.name);
    this.owned.add(info.pid);
    return info;
  }
  async call(cmd, args = {}, timeout = 70000) {
    const info = this.info();
    return await new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: '127.0.0.1', port: info.port });
      let buffer = '';
      const fail = error => { socket.destroy(); reject(new Error(`${this.name} ${cmd}: ${redact(error.message)}`)); };
      socket.setTimeout(timeout, () => fail(new Error('IPC timeout')));
      socket.on('error', fail);
      socket.on('connect', () => socket.write(JSON.stringify({ token: info.token, channel: info.channel,
        instance: info.instance, ipc_version: info.ipc_version, cmd, args }) + '\n'));
      socket.on('data', chunk => {
        buffer += chunk.toString('utf8');
        if (buffer.length > 1024 * 1024) return fail(new Error('oversized IPC response'));
        const end = buffer.indexOf('\n');
        if (end < 0) return;
        try {
          const response = JSON.parse(buffer.slice(0, end)); socket.destroy();
          if (response.ok) resolve(response.data); else reject(new Error(`${this.name} ${cmd}: ${redact(response.error)}`));
        } catch (error) { fail(error); }
      });
      socket.on('end', () => { if (!buffer.includes('\n')) fail(new Error('IPC closed before response')); });
    });
  }
  async start() {
    const config = path.join(root, 'dev', this.name, 'config');
    if (!existsSync(config)) {
      mkdirSync(config, { recursive: true });
      writeFileSync(path.join(config, 'config.toml'), '[ui]\nenabled = false\nlisten = "127.0.0.1:0"\n[update]\nauto_check = false\nauto_apply = false\n[storage]\ncapacity_bytes = 4194304\n');
    }
    const child = spawn(bin, ['--state-dir', root, '--instance', this.name, '--no-tray', 'daemon', 'run'],
      { windowsHide: true, stdio: 'ignore' });
    child.on('error', error => log(`${this.name} spawn failed: ${redact(error.message)}`));
    child.unref();
    if (child.pid) this.owned.add(child.pid);
    this.active = true;
    await until(`${this.name} IPC`, async () => { await this.call('daemon.status', {}, 5000); return true; }, 25000);
  }
  async stop() {
    const pid = this.info().pid;
    await this.call('daemon.stop', {}, 10000);
    await until(`${this.name} process exit`, () => !running(pid), 15000, 100);
    this.active = false;
  }
  async status() {
    const result = await this.call('escrow.status');
    const entry = result.rooms.find(entry => entry.room === room);
    assert.ok(entry?.replication, `${this.name}: replicated room configured`);
    assert.equal(entry.replication.error, null, `${this.name}: storage healthy`);
    assert.equal(entry.replication.quorum, 2, `${this.name}: quorum retained`);
    return entry.replication;
  }
  archive(head) {
    return path.join(this.directory, 'escrow', 'replication', hash(room), 'archive', hash(stable(head)), 'blob.bin');
  }
  async request(peer, request) {
    return await this.call('escrow.client.request', { room, peer: peer.id, request });
  }
}
const nodes = ['one', 'two', 'three'].map(name => new Node(name));
async function leader(candidates) {
  return await until('one storage leader', async () => {
    const statuses = await Promise.all(candidates.map(node => node.status()));
    const leaders = candidates.filter((_, index) => statuses[index].role === 'leader');
    return leaders.length === 1 && statuses.every(status => status.leader === leaders[0].id) ? leaders[0] : null;
  }, 150000, 500);
}
async function reconnect(candidates) {
  await until('real P2P connections', async () => {
    const statuses = await Promise.all(candidates.map(node => node.call('peers.ls')));
    return statuses.every((status, index) => candidates.filter((_, other) => index !== other).every(peer =>
      status.peers.some(known => known.did === peer.did && known.online && known.rooms.includes(room))));
  }, 180000, 500);
}
const { privateKey, publicKey } = generateKeyPairSync('ed25519');
const rawPublic = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32);
const encryptionKey = randomBytes(32);
const slot = randomBytes(16).toString('hex');
const upload = path.join(root, 'tc-escrow.bin');
let previous = null;
let current = null;
async function put(source, target, sequence, size = 512) {
  const plaintext = Buffer.concat([Buffer.from(`private escrow state ${sequence}:`), randomBytes(size)]);
  const nonce = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', encryptionKey, nonce);
  cipher.setAAD(Buffer.from(`tc-escrow/v1|${slot}|${sequence}`));
  const ciphertext = Buffer.concat([nonce, cipher.update(plaintext), cipher.final(), cipher.getAuthTag()]);
  assert.ok(!ciphertext.includes(plaintext.subarray(0, 20)), 'ciphertext hides the test marker');
  writeFileSync(upload, ciphertext);
  const stored = await source.call('store.put', { path: upload });
  const head = { v: 1, type: 'tc-escrow-head', slot, seq: sequence, parent: previous && hash(stable(previous)),
    cid: stored.cid, size: ciphertext.length, sha256: hash(ciphertext), comp: 'none', schema: 'escrow-live/1',
    writer: rawPublic.toString('base64url'), at: Date.now() };
  const payload = Buffer.from('tc-escrow/v1/head\n' + stable(head));
  head.sig = sign(null, payload, privateKey).toString('base64url');
  assert.ok(verify(null, payload, publicKey, Buffer.from(head.sig, 'base64url')));
  const request = { v: 1, type: 'escrow_put', id: randomBytes(8).toString('hex'), head };
  await until(`encrypted PUT ${sequence}`, async () => {
    const reply = await source.request(target, request);
    if (reply.type === 'escrow_error' && !['busy', 'fetch_failed'].includes(reply.code)) throw new Error(`PUT rejected: ${reply.code}`);
    return reply.type === 'escrow_ack' && reply.hash === hash(stable(head));
  }, 90000);
  previous = head; current = { head, ciphertext, plaintext };
  if (sequence % 16 === 0) budget();
}
async function restore(requester, provider) {
  const reply = await requester.request(provider, { v: 1, type: 'escrow_get', id: randomBytes(8).toString('hex'), slot });
  assert.equal(reply.type, 'escrow_head'); assert.deepEqual(reply.head, current.head);
  const output = path.join(root, `restored-${requester.name}.bin`);
  await requester.call('store.get', { id: reply.head.cid, output });
  const ciphertext = readFileSync(output);
  assert.deepEqual(ciphertext, current.ciphertext, 'retrieved encrypted bytes');
  const decipher = createDecipheriv('aes-256-gcm', encryptionKey, ciphertext.subarray(0, 12));
  decipher.setAAD(Buffer.from(`tc-escrow/v1|${slot}|${reply.head.seq}`));
  decipher.setAuthTag(ciphertext.subarray(-16));
  const plaintext = Buffer.concat([decipher.update(ciphertext.subarray(12, -16)), decipher.final()]);
  assert.deepEqual(plaintext, current.plaintext, 'client-only decryption restores original bytes');
}

let failure = null;
try {
  log('starting three isolated development daemons');
  await Promise.all(nodes.map(node => node.start()));
  for (const node of nodes) {
    node.did = (await node.call('key.did')).did;
    node.id = hash(node.did).slice(0, 16);
  }
  const voters = nodes.map(node => node.id).sort();
  for (const node of nodes) {
    for (const [setting, value] of [
      ['network.membership_allowlist', nodes.map(node => node.did)], ['storage.room_ids', [room]],
      ['escrow.rooms', [room]], ['escrow.enabled', true], ['escrow.max_blob_bytes', 1048576],
      ['escrow.max_total_bytes', MAX_DISK], ['escrow.max_slots_per_room', 16],
      ['escrow.replication', [{ room, voters, learners: [] }]],
    ]) await node.call('config.set', { path: setting, value });
  }
  for (const node of nodes) {
    const before = node.info().run_id;
    await node.call('network.set', { enabled: true });
    await until(`${node.name} network restart`, async () => node.info().run_id !== before
      && (await node.call('network.status')).state === 'on', 30000);
  }
  await reconnect(nodes);
  const firstLeader = await leader(nodes);
  const writer = nodes.find(node => node !== firstLeader);
  const survivor = nodes.find(node => node !== firstLeader && node !== writer);
  log('three peers connected; durable quorum ready');
  await put(writer, survivor, 1, 256 * 1024);
  await until('all three durable ciphertext copies', () => nodes.every(node => existsSync(node.archive(current.head))), 60000);
  for (const node of nodes) assert.deepEqual(readFileSync(node.archive(current.head)), current.ciphertext);
  await restore(firstLeader, survivor);
  log('PASS: public signed PUT, follower forwarding, three archive copies, encrypted GET restoration');

  await firstLeader.stop();
  const failoverLeader = await leader([writer, survivor]);
  await put(failoverLeader, failoverLeader === writer ? survivor : writer, 2);
  await restore(survivor, writer);
  log('PASS: stopped leader replaced; majority writes and restoration continue');
  for (let sequence = 3; sequence <= 132; sequence++) await put(writer, survivor, sequence);
  const majorityStatus = await survivor.status();
  assert.ok(majorityStatus.snapshot_index > 0, 'real daemon compacted its committed tail');
  assert.ok(majorityStatus.log_entries < 128, 'tail remains compacted');
  log('PASS: sustained encrypted updates triggered bounded snapshots');
  const oversized = { ...current.head, seq: current.head.seq + 1, parent: hash(stable(current.head)), size: 1048577 };
  delete oversized.sig;
  oversized.sig = sign(null, Buffer.from('tc-escrow/v1/head\n' + stable(oversized)), privateKey).toString('base64url');
  const refused = await writer.request(survivor, { v: 1, type: 'escrow_put', id: randomBytes(8).toString('hex'), head: oversized });
  assert.equal(refused.type, 'escrow_error'); assert.equal(refused.code, 'too_large');
  assert.equal((await survivor.status()).slots, 1, 'oversized request did not change saved slots');
  log('PASS: signed oversized request rejected before uploading any large data');

  await firstLeader.start();
  await reconnect(nodes);
  await until('restarted old leader installs snapshot and latest ciphertext', async () => {
    const status = await firstLeader.status();
    return status.snapshot_index > 0 && status.commit_index >= majorityStatus.commit_index
      && existsSync(firstLeader.archive(current.head));
  }, 180000, 500);
  assert.deepEqual(readFileSync(firstLeader.archive(current.head)), current.ciphertext);
  // Leave its generic store uncached for the latest CID until the publisher is offline.
  log('PASS: restarted lagging node caught up from durable snapshot and encrypted generations');

  await writer.stop();
  await leader([firstLeader, survivor]);
  await restore(firstLeader, survivor);
  const bytes = budget();
  log(`PASS: latest state restored with publishing node offline; temporary disk ${bytes} bytes`);
  log('ALL PASS');
} catch (error) {
  failure = error;
  console.error(`FAIL: ${redact(error.stack || error)}`);
  for (const node of nodes.filter(node => node.active)) {
    try { console.error(`${node.name} replication status: ${redact(JSON.stringify(await node.call('escrow.status', {}, 5000)))}`); }
    catch (statusError) { console.error(`${node.name} status unavailable: ${redact(statusError.message)}`); }
  }
} finally {
  for (const node of nodes) {
    if (!node.active) continue;
    try { await node.stop(); } catch (error) { failure ||= error; console.error(`cleanup: ${redact(error.message)}`); }
  }
  const liveOwned = nodes.flatMap(node => [...node.owned]).filter(running);
  const safeRoot = path.dirname(root) === path.resolve(tmpdir()) && path.basename(root).startsWith('mistl-escrow-replication-');
  if (!keep && !liveOwned.length && safeRoot) rmSync(root, { recursive: true, force: true });
  else log(`retained isolated state: ${root}${liveOwned.length ? ' (owned process still running)' : ''}`);
  process.exitCode = failure ? 1 : 0;
}
