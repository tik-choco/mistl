// Real daemon integration: isolated state, no autostart/install changes.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
const bin = path.resolve(process.argv[2] || `target/debug/mistl${process.platform === 'win32' ? '.exe' : ''}`);
const root = mkdtempSync(path.join(tmpdir(), 'mistl-network-smoke-'));
const common = instance => ['--state-dir', root, '--instance', instance, '--no-tray'];
const call = (instance, args) => {
  const result = spawnSync(bin, [...common(instance), ...args], { encoding:'utf8', windowsHide:true, timeout:12000 });
  if (result.status !== 0) throw new Error(result.stderr || String(result.error));
  return JSON.parse(result.stdout);
};
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function wait(instance, predicate) {
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    try { const value = call(instance, ['daemon','status']); if (predicate(value)) return value; } catch {}
    await sleep(200);
  }
  throw new Error(`daemon ${instance} did not reach expected state`);
}
function start(instance) {
  const child = spawn(bin, [...common(instance), 'daemon','run'], { windowsHide:true, stdio:'ignore' });
  child.unref(); return child;
}
async function stopped(pid) {
  for (let i=0; i<100; i++) {
    try { process.kill(pid,0); } catch { return; }
    await sleep(100);
  }
  throw new Error(`owned process ${pid} did not exit`);
}
const owned = new Set();
try {
  start('one'); start('two');
  let a = await wait('one', () => true); let b = await wait('two', () => true);
  owned.add(a.pid); owned.add(b.pid);
  assert.equal(a.network.state,'off'); assert.equal(b.network.state,'off');
  assert.notEqual(a.dashboard,b.dashboard);
  assert.equal(a.build.channel,'dev');
  const identity = `${a.build.instance_id}/${a.build.build_id}`;
  // The dashboard URL carries ?token=; exchange it for the session cookie.
  assert.equal((await fetch(new URL('/api/call',a.dashboard),{method:'POST'})).status,401,'no session, no API');
  const login=await fetch(a.dashboard,{redirect:'manual'});
  assert.equal(login.status,303);
  const cookie=login.headers.get('set-cookie').split(';')[0];
  const api = async (cmd,args={},header=identity) => {
    const r=await fetch(new URL('/api/call',a.dashboard), {method:'POST',headers:{'content-type':'application/json','x-mistl-ui':'1','x-mistl-instance':header,cookie},body:JSON.stringify({cmd,args})});
    return {status:r.status,body:await r.json()};
  };
  const html=await (await fetch(new URL('/',a.dashboard),{headers:{cookie}})).text();
  assert.ok(html.includes('window.mistlRuntime=')); assert.ok(html.includes(a.build.build_id));
  assert.equal((await api('network.status')).body.data.state,'off');
  assert.equal((await api('network.set',{enabled:true},'stale-instance')).status,409);
  for (const cmd of ['update.check','ai.upstream_models','store.connect','sched.run','stream.start']) {
    const response=await api(cmd); assert.equal(response.body.ok,false,cmd);
    assert.match(response.body.error,/OFF|switching/);
  }
  const oneDid=call('one',['key','did']); const twoDid=call('two',['key','did']);
  assert.notDeepEqual(oneDid,twoDid,'instances must not share identity');
  const initial=a.pid;
  assert.equal(call('one',['network','on']).state,'restarting');
  a=await wait('one',s=>s.pid!==initial && s.network.state==='on'); owned.add(a.pid);
  assert.equal(call('two',['daemon','status']).pid,b.pid);
  const active=a.pid;
  assert.equal(call('one',['network','off']).state,'restarting');
  a=await wait('one',s=>s.pid!==active && s.network.state==='off'); owned.add(a.pid);
  await stopped(active);
  const savedPath=path.join(root,'dev','one','data','network-state.json');
  assert.equal(JSON.parse(readFileSync(savedPath,'utf8')).enabled,false);
  const previous=a.pid;
  call('one',['daemon','stop']); await stopped(previous); start('one');
  a=await wait('one',s=>s.pid!==previous); owned.add(a.pid); assert.equal(a.network.state,'off');
  const didAfter=call('one',['key','did']); assert.deepEqual(didAfter,oneDid);
  // A persisted ON survives abrupt process exit without a shutdown-time save.
  call('one',['network','on']); const prior=a.pid;
  a=await wait('one',s=>s.pid!==prior && s.network.state==='on'); owned.add(a.pid);
  process.kill(a.pid); await stopped(a.pid); start('one');
  a=await wait('one',s=>s.network.state==='on'); owned.add(a.pid);
  call('one',['daemon','stop']); await stopped(a.pid);
  writeFileSync(savedPath,'broken state'); start('one');
  a=await wait('one',s=>s.network.state==='off'); owned.add(a.pid);
  assert.equal(a.network.saved,false); assert.match(a.network.error,/invalid/);
  console.log('PASS: isolated daemons/keys/ports, stale-page rejection, offline gating, ON/OFF restart, durable intent, abrupt exit, corrupt-state fallback');
} finally {
  for (const name of ['one','two']) { try { call(name,['daemon','stop']); } catch {} }
  await sleep(600);
  for (const pid of owned) { try { await stopped(pid); } catch { /* never kill an unrelated process by name */ } }
  // Remove only this script's randomly allocated temporary directory.
  if (path.dirname(root) === path.resolve(tmpdir()) && path.basename(root).startsWith('mistl-network-smoke-') && existsSync(root)) rmSync(root,{recursive:true,force:true});
}
