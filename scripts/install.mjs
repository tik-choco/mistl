// Build this checkout as a stable-channel binary and install it over the
// per-user install (`mistl install`), restarting the installed daemon if it
// was running. Extra args are passed to `mistl install` (e.g. --no-autostart).
// Version stamping: see stable-build.mjs.
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { cmakeEnv, latestTagVersion, root, run, stampedBuild } from './stable-build.mjs';
const exeName = process.platform === 'win32' ? 'mistl.exe' : 'mistl';
// Separate target dir: keeps target/release a dev build and avoids
// rebuilding between channels.
const targetDir = path.join(root, 'target', 'install');
const built = path.join(targetDir, 'release', exeName);
// Mirrors crate::install::installed_exe_path().
const installed = process.platform === 'win32'
  ? path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local'), 'Programs', 'mistl', exeName)
  : path.join(process.env.XDG_BIN_HOME || path.join(os.homedir(), '.local', 'bin'), exeName);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Stop the stable/default daemon, preferring the installed CLI (it matches
// the running daemon's IPC protocol). Returns whether one was running.
async function stopDaemon() {
  for (const exe of [installed, built].filter(existsSync)) {
    if (run(exe, ['daemon', 'status']).status !== 0) continue;
    const stopped = run(exe, ['daemon', 'stop']);
    if (stopped.status !== 0) throw new Error(stopped.stderr);
    const deadline = Date.now() + 10000;
    while (run(exe, ['daemon', 'status']).status === 0) {
      if (Date.now() >= deadline) throw new Error('Installed daemon did not stop; no process was force-killed.');
      await sleep(150);
    }
    // Native threads exit with the process, shortly after IPC closes.
    await sleep(500);
    return true;
  }
  return false;
}

const version = `${latestTagVersion()}+local`;
console.log(`Building stable ${version} from this checkout...`);
const ok = await stampedBuild(version, 'cargo', ['build', '--release', '--target-dir', targetDir],
  { ...cmakeEnv(), MISTL_BUILD_CHANNEL: 'stable' });
if (!ok) process.exit(1);
const wasRunning = await stopDaemon();
const install = spawnSync(built, ['install', ...process.argv.slice(2)], { cwd: root, stdio: 'inherit', windowsHide: true });
if (install.status !== 0) process.exit(install.status ?? 1);
if (wasRunning) {
  // stdio inherited, not piped: the daemon keeps its handles open.
  const started = spawnSync(installed, ['daemon', 'start'], { cwd: root, stdio: 'inherit', windowsHide: true });
  process.exitCode = started.status ?? 1;
}
