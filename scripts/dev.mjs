// Stop/rebuild only the daemon belonging to this worktree; never kill by name.
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const bin = path.join(root, 'target', 'debug', process.platform === 'win32' ? 'mistl.exe' : 'mistl');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const run = (exe, args) => spawnSync(exe, args, { cwd: root, encoding: 'utf8', windowsHide: true });

async function rebuild() {
  if (existsSync(bin)) {
    const identity = run(bin, ['build-info']);
    let build;
    try { build = JSON.parse(identity.stdout); } catch { /* legacy binary: do not invoke stop */ }
    if (build && build.channel === 'dev' && build.instance.startsWith('worktree-')) {
      const status = run(bin, ['daemon', 'status']);
      if (status.status === 0) {
        const stopped = run(bin, ['daemon', 'stop']);
        if (stopped.status !== 0) throw new Error(stopped.stderr);
        const deadline = Date.now() + 10000;
        while (run(bin, ['daemon', 'status']).status === 0) {
          if (Date.now() >= deadline) throw new Error('Development daemon did not stop; no process was force-killed.');
          await sleep(150);
        }
        // Native threads exit with the process, shortly after IPC closes.
        await sleep(500);
      }
    }
  }
  const ok = await new Promise(resolve => {
    const child = spawn('cargo', ['build', '--locked'], { cwd: root, stdio: 'inherit', windowsHide: true,
      env: { ...process.env, MISTL_BUILD_CHANNEL: 'dev' } });
    child.on('error', error => { console.error(error.message); resolve(false); });
    child.on('exit', code => resolve(code === 0));
  });
  if (!ok) return false;
  const result = run(bin, ['daemon', 'start']);
  process.stdout.write(result.stdout || ''); process.stderr.write(result.stderr || '');
  return result.status === 0;
}
function snapshot() {
  const files = [];
  function walk(p) {
    const stat = statSync(p);
    if (stat.isDirectory()) for (const name of readdirSync(p).sort()) walk(path.join(p, name));
    else files.push(`${p}:${stat.mtimeMs}:${stat.size}`);
  }
  for (const p of ['src', 'Cargo.toml', 'Cargo.lock', 'build.rs']) walk(path.join(root, p));
  return files.join('\n');
}
const watch = process.argv.includes('--watch');
let ok = await rebuild();
if (!watch) process.exitCode = ok ? 0 : 1;
else {
  console.log('Watching this worktree. Ctrl+C leaves its last daemon running.');
  let previous = snapshot();
  for (;;) {
    await sleep(1000);
    const next = snapshot();
    if (next !== previous) {
      try { ok = await rebuild(); } catch (error) { console.error(error.message); }
      previous = snapshot();
    }
  }
}
