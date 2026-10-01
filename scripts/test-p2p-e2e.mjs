// Live two-daemon end-to-end check: signed profile exchange, direct messages
// (text + file), and tunnel approval notices. Needs the public signaling
// relays (Nostr) to be reachable; fails loudly when peers never connect.
// Isolated state, no autostart/install changes. Usage:
//   node scripts/test-p2p-e2e.mjs [path-to-mistl-binary]
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import net from 'node:net';
import { randomBytes } from 'node:crypto';
import path from 'node:path';

const bin = path.resolve(process.argv[2] || `target/debug/mistl${process.platform === 'win32' ? '.exe' : ''}`);
const root = mkdtempSync(path.join(tmpdir(), 'mistl-p2p-e2e-'));
const common = instance => ['--state-dir', root, '--instance', instance, '--no-tray'];
const sleep = ms => new Promise(r => setTimeout(r, ms));
const t0 = Date.now();
const log = msg => console.log(`[${((Date.now() - t0) / 1000).toFixed(1)}s] ${msg}`);

const cli = (instance, args) => {
  const r = spawnSync(bin, [...common(instance), ...args], { encoding: 'utf8', windowsHide: true, timeout: 15000 });
  if (r.status !== 0) throw new Error((r.stderr || String(r.error)).replace(/token=[^\s&"]+/g, 'token=***'));
  return JSON.parse(r.stdout);
};
async function waitFor(what, fn, ms, every = 500) {
  const deadline = Date.now() + ms;
  let last;
  while (Date.now() < deadline) {
    try { const v = await fn(); if (v) return v; } catch (e) { last = e; }
    await sleep(every);
  }
  throw new Error(`timed out waiting for ${what}${last ? `: ${last.message}` : ''}`);
}
function start(instance) {
  const child = spawn(bin, [...common(instance), 'daemon', 'run'], { windowsHide: true, stdio: 'ignore' });
  child.unref();
}
async function stopped(pid) {
  for (let i = 0; i < 100; i++) {
    try { process.kill(pid, 0); } catch { return; }
    await sleep(100);
  }
  throw new Error(`owned process ${pid} did not exit`);
}

// One dashboard client per instance; re-logs in whenever the daemon restarts.
class Node {
  constructor(name) { this.name = name; this.cookie = null; this.status = null; }
  async refresh() {
    this.status = cli(this.name, ['daemon', 'status']);
    const login = await fetch(this.status.dashboard, { redirect: 'manual' });
    assert.equal(login.status, 303, `${this.name}: login exchange`);
    this.cookie = login.headers.get('set-cookie').split(';')[0];
    this.identity = `${this.status.build.instance_id}/${this.status.build.build_id}`;
    this.base = this.status.dashboard;
  }
  headers(extra = {}) {
    return { 'x-mistl-ui': '1', 'x-mistl-instance': this.identity, cookie: this.cookie, ...extra };
  }
  async call(cmd, args = {}) {
    const r = await fetch(new URL('/api/call', this.base), {
      method: 'POST', headers: this.headers({ 'content-type': 'application/json' }), body: JSON.stringify({ cmd, args }),
    });
    const body = await r.json();
    if (!body.ok) throw new Error(`${this.name} ${cmd}: ${body.error}`);
    return body.data;
  }
}

const owned = new Set();
const echo = net.createServer(s => { s.on('data', d => s.write(d)); s.on('error', () => {}); });
let failure = null;
try {
  log(`binary ${bin}`);
  start('one'); start('two');
  const one = new Node('one'), two = new Node('two');
  await waitFor('daemons up', async () => { await one.refresh(); await two.refresh(); return true; }, 20000, 300);
  owned.add(one.status.pid); owned.add(two.status.pid);

  // 1. network on for both (restarts each daemon).
  for (const n of [one, two]) {
    const before = n.status.pid;
    assert.equal(cli(n.name, ['network', 'on']).state, 'restarting');
    await waitFor(`${n.name} restart`, async () => {
      const s = cli(n.name, ['daemon', 'status']);
      return s.pid !== before && s.network.state === 'on';
    }, 30000, 300);
    await n.refresh(); owned.add(n.status.pid);
  }
  log('step 1 PASS: both daemons restarted with network on');

  const oneDid = (await one.call('key.did')).did, twoDid = (await two.call('key.did')).did;
  assert.notEqual(oneDid, twoDid);

  // 2. join one fresh random room via the tunnel session.
  const room = 'e2e' + randomBytes(6).toString('hex');
  for (const n of [one, two]) {
    await n.call('tunnel.room.set', { room });
    const r = await n.call('tunnel.start');
    assert.equal(r.room, room);
  }
  log(`step 2: tunnel started on both in room ${room}`);

  // 3. profiles. Set before the peers see each other so the first hello carries them,
  // then wait for each side to list the other online with the right name.
  const PNG = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/q842iQAAAABJRU5ErkJggg==';
  await one.call('profile.set', { field: 'display_name', value: 'Alice-e2e' });
  await one.call('profile.set', { field: 'avatar_thumb', value: PNG });
  await two.call('profile.set', { field: 'display_name', value: 'Bob-e2e' });
  const peerOf = (n, did) => async () => {
    const p = (await n.call('peers.ls')).peers.find(x => x.did === did);
    return p && p.online && p.rooms.includes(room) ? p : null;
  };
  const tConnect = Date.now();
  const bOnOne = await waitFor('one sees two online', peerOf(one, twoDid), 120000, 1000);
  log(`  peers online after ${((Date.now() - tConnect) / 1000).toFixed(1)}s (one sees two; name=${bOnOne.name ?? '-'})`);
  await waitFor('two sees one online', peerOf(two, oneDid), 60000, 1000);
  const named = (n, did, name, avatar) => async () => {
    const p = (await n.call('peers.ls')).peers.find(x => x.did === did);
    return p && p.online && p.name === name && (!avatar || p.avatar === avatar) ? p : null;
  };
  const tProfile = Date.now();
  const aOnTwo = await waitFor('two sees Alice-e2e + avatar', named(two, oneDid, 'Alice-e2e', PNG), 90000, 1000);
  const bOnOne2 = await waitFor('one sees Bob-e2e', named(one, twoDid, 'Bob-e2e'), 90000, 1000);
  assert.equal(aOnTwo.avatar, PNG);
  assert.equal(bOnOne2.avatar, undefined);
  const got = await two.call('peers.get', { did: oneDid });
  assert.equal(got.name, 'Alice-e2e');
  log(`step 3 PASS: profiles exchanged (${((Date.now() - tProfile) / 1000).toFixed(1)}s after online)`);
  // A later profile.set must propagate by broadcast.
  await one.call('profile.set', { field: 'display_name', value: 'Alice-e2e-2' });
  await waitFor('two sees renamed Alice', named(two, oneDid, 'Alice-e2e-2', PNG), 30000, 500);
  log('step 3b PASS: profile update propagated');

  // 4. DMs.
  const hist = async (n, did) => (await n.call('dm.history', { did })).messages;
  const find = (msgs, text) => msgs.find(m => m.text === text);
  const text1 = `hello from one ${randomBytes(3).toString('hex')}`;
  const sent = (await one.call('dm.send', { did: twoDid, text: text1 })).message;
  assert.equal(sent.mine, true);
  const r1 = await waitFor('two receives text', async () => find(await hist(two, oneDid), text1), 30000);
  assert.equal(r1.status, 'received'); assert.equal(r1.mine, false);
  const d1 = await waitFor('one marks delivered', async () => {
    const m = find(await hist(one, twoDid), text1); return m && m.status === 'delivered' ? m : null;
  }, 30000);
  assert.equal(d1.id, sent.id);
  const ls2 = (await two.call('dm.ls')).conversations;
  assert.ok(ls2.find(c => c.did === oneDid && c.unread >= 1), 'two has an unread conversation');
  log('step 4a PASS: one -> two text received, delivered receipt back');
  const text2 = `reply from two ${randomBytes(3).toString('hex')}`;
  await two.call('dm.send', { did: oneDid, text: text2 });
  const r2 = await waitFor('one receives reply', async () => find(await hist(one, twoDid), text2), 30000);
  assert.equal(r2.status, 'received');
  await waitFor('two marks reply delivered', async () => {
    const m = find(await hist(two, oneDid), text2); return m && m.status === 'delivered';
  }, 30000);
  log('step 4b PASS: two -> one reply received + delivered');

  // 5. file transfer through the sandbox.
  const bytes = randomBytes(200 * 1024 + 123);
  const up = await fetch(new URL('/api/store/sandbox-upload', one.base), {
    method: 'POST', body: bytes,
    headers: one.headers({ 'x-file-name': encodeURIComponent('e2e-file.bin'), 'content-type': 'application/octet-stream' }),
  });
  const upj = await up.json();
  assert.equal(upj.ok, true, `sandbox upload: ${JSON.stringify(upj)}`);
  const imported = upj.data.imported;
  const fileMsg = (await one.call('dm.send', { did: twoDid, sandbox: imported })).message;
  assert.equal(fileMsg.file.size, bytes.length);
  const rf = await waitFor('two receives file message', async () =>
    (await hist(two, oneDid)).find(m => m.file && m.id === fileMsg.id), 30000);
  assert.equal(rf.file.name, 'e2e-file.bin');
  const tDl = Date.now();
  const dl = await two.call('dm.download', { did: oneDid, id: fileMsg.id });
  assert.equal(dl.size, bytes.length);
  const back = await fetch(new URL('/api/store/sandbox-download?path=' + encodeURIComponent(dl.sandbox_path), two.base), { headers: two.headers() });
  assert.equal(back.status, 200);
  const got2 = Buffer.from(await back.arrayBuffer());
  assert.ok(got2.equals(bytes), 'downloaded bytes equal the original');
  log(`step 5 PASS: ${bytes.length} byte file sent and verified byte-for-byte (download ${((Date.now() - tDl) / 1000).toFixed(1)}s)`);

  // 6. tunnel approval notice.
  await new Promise(r => echo.listen(0, '127.0.0.1', r));
  const echoPort = echo.address().port;
  const probe = net.createServer(); await new Promise(r => probe.listen(0, '127.0.0.1', r));
  const listenPort = probe.address().port; await new Promise(r => probe.close(r));
  await two.call('tunnel.forward.add', { direction: 'serve', proto: 'tcp', addr: `127.0.0.1:${echoPort}`, listen_port: -1, target: `tcp:${echoPort}` });
  await one.call('tunnel.forward.add', { direction: 'connect', proto: 'tcp', addr: '', listen_port: listenPort, target: `tcp:${echoPort}` });
  const fwd = async () => (await one.call('tunnel.status')).forwards.find(f => f.target?.startsWith(`tcp:${echoPort}`));
  assert.ok(await waitFor('connect forward listed', fwd, 15000), 'forward visible');
  assert.notEqual((await fwd()).awaiting_approval, true, 'not awaiting before any connection');
  // Open a connection through the forward and keep it open; it parks on two.
  const sock = net.connect(listenPort, '127.0.0.1'); sock.on('error', () => {});
  const received = []; sock.on('data', d => received.push(d));
  const pend = await waitFor('two has pending auth', async () => (await two.call('tunnel.status')).pending_auth[0], 30000);
  const tAw = Date.now();
  const aw = await waitFor('one forward awaiting_approval', async () => { const f = await fwd(); return f.awaiting_approval ? f : null; }, 20000, 300);
  log(`  awaiting_approval observed ${((Date.now() - tAw) / 1000).toFixed(1)}s after pending (approval_peer_id=${aw.approval_peer_id})`);
  await two.call('tunnel.auth.approve', { id: pend.id });
  await waitFor('awaiting cleared', async () => !(await fwd()).awaiting_approval, 20000, 300);
  sock.write('ping-after-approval');
  await waitFor('echo through tunnel', async () => Buffer.concat(received).toString() === 'ping-after-approval', 20000, 300);
  sock.destroy();
  log('step 6a PASS: awaiting_approval shown until approved, data flows after approval');

  // 6b. deny path.
  const sock2 = net.connect(listenPort, '127.0.0.1'); sock2.on('error', () => {}); sock2.on('close', () => {});
  const pend2 = await waitFor('two has 2nd pending auth', async () => (await two.call('tunnel.status')).pending_auth[0], 30000);
  await waitFor('one awaiting again', async () => (await fwd()).awaiting_approval, 20000, 300);
  await two.call('tunnel.auth.deny', { id: pend2.id });
  const denied = await waitFor('one awaiting cleared after deny', async () => { const f = await fwd(); return !f.awaiting_approval ? f : null; }, 20000, 300);
  log(`step 6b PASS: deny clears awaiting (denied_at_ms=${denied.denied_at_ms ?? 'unset'})`);
  sock2.destroy();
  log('ALL PASS');
} catch (e) {
  failure = e;
  console.error(`FAIL: ${e.stack || e}`);
} finally {
  try { echo.close(); } catch {}
  for (const name of ['one', 'two']) { try { cli(name, ['daemon', 'stop']); } catch {} }
  await sleep(800);
  for (const pid of owned) { try { await stopped(pid); } catch { /* never kill unrelated processes by name */ } }
  if (path.dirname(root) === path.resolve(tmpdir()) && path.basename(root).startsWith('mistl-p2p-e2e-') && existsSync(root)) {
    try { rmSync(root, { recursive: true, force: true }); } catch {}
  }
  process.exit(failure ? 1 : 0);
}
