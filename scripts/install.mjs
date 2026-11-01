// Build this checkout as a stable-channel binary and install it over the
// per-user install (`mistl install`), restarting the installed daemon if it
// was running. Extra args are passed to `mistl install` (e.g. --no-autostart).
//
// The version is stamped as `<latest tag>+local` for the build only, so the
// self-updater (`is_update_available`: semver, build metadata ranks above
// the bare release) keeps this build until a newer release is published.
// Cargo.toml/Cargo.lock are restored byte-for-byte afterwards.
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
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
const run = (exe, args) => spawnSync(exe, args, { cwd: root, encoding: 'utf8', windowsHide: true });

function latestTagVersion() {
  const tags = run('git', ['-c', 'versionsort.suffix=-', 'tag', '--list', 'v*', '--sort=-v:refname']);
  const tag = (tags.stdout || '').split('\n').map(t => t.trim())
    .find(t => /^v\d+\.\d+\.\d+(-[0-9A-Za-z.]+)?$/.test(t));
  if (!tag) throw new Error('No vMAJOR.MINOR.PATCH tag found; run `git fetch --tags` first.');
  return tag.slice(1);
}

// audiopus_sys builds libopus via CMake. Outside a Developer shell the copy
// bundled with Visual Studio / Build Tools is not on PATH, so point the
// `cmake` crate at it through $CMAKE.
function cmakeEnv() {
  if (process.platform !== 'win32' || process.env.CMAKE) return {};
  if (run('cmake', ['--version']).status === 0) return {};
  const vswhere = path.join(process.env['ProgramFiles(x86)'] || 'C:\\Program Files (x86)',
    'Microsoft Visual Studio', 'Installer', 'vswhere.exe');
  if (!existsSync(vswhere)) return {};
  const found = run(vswhere, ['-nologo', '-latest', '-products', '*', '-find',
    'Common7\\IDE\\CommonExtensions\\Microsoft\\CMake\\CMake\\bin\\cmake.exe']);
  const cmake = (found.stdout || '').split(/\r?\n/).map(l => l.trim()).find(l => /cmake\.exe$/i.test(l));
  return cmake && existsSync(cmake) ? { CMAKE: cmake } : {};
}

async function build(version) {
  const manifest = path.join(root, 'Cargo.toml');
  const lock = path.join(root, 'Cargo.lock');
  const saved = [[manifest, readFileSync(manifest)], [lock, readFileSync(lock)]];
  const restore = () => { for (const [file, bytes] of saved) writeFileSync(file, bytes); };
  const onSignal = () => { restore(); process.exit(130); };
  process.on('SIGINT', onSignal);
  try {
    // The first top-level `version =` is [package]'s.
    const text = saved[0][1].toString('utf8');
    if (!/^version\s*=/m.test(text)) throw new Error('Cargo.toml has no package version line.');
    writeFileSync(manifest, text.replace(/^version\s*=\s*"[^"]*"/m, `version = "${version}"`));
    // No --locked: the stamped version rewrites mistl's own Cargo.lock entry
    // (restored below); every other entry stays as locked.
    return await new Promise(resolve => {
      const child = spawn('cargo', ['build', '--release', '--target-dir', targetDir], { cwd: root, stdio: 'inherit',
        windowsHide: true, env: { ...process.env, ...cmakeEnv(), MISTL_BUILD_CHANNEL: 'stable' } });
      child.on('error', error => { console.error(error.message); resolve(false); });
      child.on('exit', code => resolve(code === 0));
    });
  } finally {
    restore();
    process.off('SIGINT', onSignal);
  }
}

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
if (!await build(version)) process.exit(1);
const wasRunning = await stopDaemon();
const install = spawnSync(built, ['install', ...process.argv.slice(2)], { cwd: root, stdio: 'inherit', windowsHide: true });
if (install.status !== 0) process.exit(install.status ?? 1);
if (wasRunning) {
  // stdio inherited, not piped: the daemon keeps its handles open.
  const started = spawnSync(installed, ['daemon', 'start'], { cwd: root, stdio: 'inherit', windowsHide: true });
  process.exitCode = started.status ?? 1;
}
