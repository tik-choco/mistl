// Shared by install.mjs and install-ssh.mjs: build this checkout as a
// stable-channel binary stamped `<latest tag>+local`.
//
// The stamped version only lives in Cargo.toml for the build, so the
// self-updater (`is_update_available`: semver, build metadata ranks above
// the bare release) keeps such a build until a newer release is published.
// Cargo.toml/Cargo.lock are restored byte-for-byte afterwards.
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const run = (exe, args) => spawnSync(exe, args, { cwd: root, encoding: 'utf8', windowsHide: true });

export function latestTagVersion() {
  const tags = run('git', ['-c', 'versionsort.suffix=-', 'tag', '--list', 'v*', '--sort=-v:refname']);
  const tag = (tags.stdout || '').split('\n').map(t => t.trim())
    .find(t => /^v\d+\.\d+\.\d+(-[0-9A-Za-z.]+)?$/.test(t));
  if (!tag) throw new Error('No vMAJOR.MINOR.PATCH tag found; run `git fetch --tags` first.');
  return tag.slice(1);
}

// audiopus_sys builds libopus via CMake. Outside a Developer shell the copy
// bundled with Visual Studio / Build Tools is not on PATH, so point the
// `cmake` crate at it through $CMAKE.
export function cmakeEnv() {
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

// A run killed before its restore (closed terminal, killed process tree)
// leaves the `+local` stamp behind, and the next run would snapshot and
// "restore" that. Put the committed package version back first.
function unstampLeftover(manifest, lock) {
  const versionOf = text => /^version\s*=\s*"([^"]*)"/m.exec(text)?.[1];
  const text = readFileSync(manifest, 'utf8');
  const current = versionOf(text);
  if (!current?.endsWith('+local')) return;
  const committed = versionOf(run('git', ['show', 'HEAD:Cargo.toml']).stdout || '');
  if (!committed || committed === current) return;
  console.warn(`Cargo.toml still has the stamp ${current} from an interrupted build; resetting it to ${committed}.`);
  writeFileSync(manifest, text.replace(/^version\s*=\s*"[^"]*"/m, `version = "${committed}"`));
  writeFileSync(lock, readFileSync(lock, 'utf8')
    .replace(`name = "mistl"\nversion = "${current}"`, `name = "mistl"\nversion = "${committed}"`));
}

// Run a build command (`exe args`, stdio inherited) with the package version
// stamped to `version`. The command itself must select the stable channel
// (MISTL_BUILD_CHANNEL=stable). Resolves to whether it succeeded.
export async function stampedBuild(version, exe, args, env = {}) {
  const manifest = path.join(root, 'Cargo.toml');
  const lock = path.join(root, 'Cargo.lock');
  unstampLeftover(manifest, lock);
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
      const child = spawn(exe, args, { cwd: root, stdio: 'inherit', windowsHide: true,
        env: { ...process.env, ...env } });
      child.on('error', error => { console.error(error.message); resolve(false); });
      child.on('exit', code => resolve(code === 0));
    });
  } finally {
    restore();
    process.off('SIGINT', onSignal);
  }
}
