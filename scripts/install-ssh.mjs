// Install mistl on other machines over SSH, addressed by ssh config alias:
//
//   just install-ssh <alias>... [--start] [--release] [mistl install flags]
//
// Each host's OS/arch is probed first. When this machine can build for it
// (its own target, or x86_64 Linux through WSL on Windows) the host gets this
// checkout built exactly like `just install` (stable, `<latest tag>+local`);
// any other target (e.g. aarch64 Linux, macOS) gets the newest GitHub release
// binary (prereleases included), sha256-checked against the release's
// SHA256SUMS.txt. `--release` uses the release binary for every host.
//
// On the host the binary is uploaded to the home dir, run once as
// `mistl install` and deleted. The installed daemon is stopped first and
// started again afterwards if it was running; `--start` also starts it when
// it was not (headless servers have no login session for autostart). Other
// flags (e.g. --no-autostart) are passed to `mistl install`.
import { spawnSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { cmakeEnv, latestTagVersion, root, stampedBuild } from './stable-build.mjs';

const REPO = 'tik-choco/mistl';
const LINUX_X64 = 'x86_64-unknown-linux-gnu';
const WINDOWS_X64 = 'x86_64-pc-windows-msvc';
const HOST_TARGET = {
  'win32-x64': WINDOWS_X64,
  'linux-x64': LINUX_X64,
  'linux-arm64': 'aarch64-unknown-linux-gnu',
  'darwin-arm64': 'aarch64-apple-darwin',
  'darwin-x64': 'x86_64-apple-darwin',
}[`${process.platform}-${process.arch}`];

const hosts = [];
const forward = [];
let start = false;
let releaseOnly = false;
for (const arg of process.argv.slice(2)) {
  if (arg === '--start') start = true;
  else if (arg === '--release') releaseOnly = true;
  else if (arg.startsWith('-')) forward.push(arg);
  else hosts.push(arg);
}
if (!hosts.length) {
  console.error('usage: just install-ssh <ssh-alias>... [--start] [--release] [mistl install flags]');
  process.exit(2);
}
// Forwarded into remote shell scripts: plain long flags only.
for (const flag of forward) {
  if (!/^--[a-z][a-z-]*$/.test(flag)) {
    console.error(`unsupported argument: ${flag}`);
    process.exit(2);
  }
}

const isWindowsTarget = target => target.includes('windows');

function ssh(host, command, opts = {}) {
  return spawnSync('ssh', [host, command], { encoding: 'utf8', windowsHide: true, ...opts });
}

// -> target triple of `host`, from `uname -sm` or (Windows) cmd's
// %PROCESSOR_ARCHITECTURE%. Works whether the host's ssh shell is sh, cmd or
// PowerShell.
function probe(host) {
  const stdio = ['inherit', 'pipe', 'pipe'];
  const unix = ssh(host, 'uname -sm', { stdio });
  if (unix.status === 255) throw new Error(`ssh ${host} failed: ${unix.stderr.trim()}`);
  if (unix.status === 0) {
    const [os, machine] = unix.stdout.trim().split(/\s+/);
    const arch = { x86_64: 'x86_64', amd64: 'x86_64', aarch64: 'aarch64', arm64: 'aarch64' }[machine];
    if (os === 'Linux' && arch) return `${arch}-unknown-linux-gnu`;
    if (os === 'Darwin' && arch) return `${arch}-apple-darwin`;
    throw new Error(`${host}: unsupported platform "${unix.stdout.trim()}"`);
  }
  const win = ssh(host, 'cmd /c echo %PROCESSOR_ARCHITECTURE%', { stdio });
  if (win.status === 0 && win.stdout.trim() === 'AMD64') return WINDOWS_X64;
  throw new Error(`${host}: could not detect the platform (uname: ${unix.stderr.trim() || unix.status}, `
    + `windows: ${win.stdout.trim() || win.stderr.trim()})`);
}

// Local stable build for `target`, or null when it can't be built here.
function localBuild(target) {
  const env = { ...cmakeEnv(), MISTL_BUILD_CHANNEL: 'stable' };
  if (target === HOST_TARGET) {
    // Same target dir as `just install`, so both share one build.
    const dir = path.join(root, 'target', 'install');
    return {
      file: path.join(dir, 'release', isWindowsTarget(target) ? 'mistl.exe' : 'mistl'),
      build: version => stampedBuild(version, 'cargo', ['build', '--release', '--target-dir', dir], env),
    };
  }
  if (target === LINUX_X64 && process.platform === 'win32'
    && spawnSync('wsl', ['--', 'bash', '-lc', 'command -v cargo'], { stdio: 'ignore', windowsHide: true }).status === 0) {
    // Same toolchain requirements as `just release-linux`.
    return {
      file: path.join(root, 'target', 'install-linux', LINUX_X64, 'release', 'mistl'),
      build: version => stampedBuild(version, 'wsl', ['--cd', root, '--', 'bash', '-lc',
        `MISTL_BUILD_CHANNEL=stable cargo build --release --target ${LINUX_X64} --target-dir target/install-linux`]),
    };
  }
  return null;
}

let releases;
async function githubJson(url) {
  const res = await fetch(url, { headers: { accept: 'application/vnd.github+json', 'user-agent': 'mistl-install-ssh' } });
  if (!res.ok) throw new Error(`GET ${url}: HTTP ${res.status}`);
  return res.json();
}
async function download(url) {
  const res = await fetch(url, { headers: { 'user-agent': 'mistl-install-ssh' } });
  if (!res.ok) throw new Error(`GET ${url}: HTTP ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

// Binary for `target` from the newest release that ships one (prereleases
// included: `releases/latest` skips them), verified against SHA256SUMS.txt.
async function releaseBinary(target) {
  releases ??= await githubJson(`https://api.github.com/repos/${REPO}/releases?per_page=10`);
  const assetName = release => `mistl-${release.tag_name}-${target}${isWindowsTarget(target) ? '.exe' : ''}`;
  const release = releases.find(r => !r.draft && r.assets.some(a => a.name === assetName(r)));
  if (!release) throw new Error(`no recent release has a binary for ${target}`);
  const name = assetName(release);
  const version = release.tag_name.replace(/^v/, '');
  const asset = release.assets.find(a => a.name === name);
  const sums = release.assets.find(a => a.name === 'SHA256SUMS.txt');
  if (!sums) throw new Error(`release v${version} has no SHA256SUMS.txt`);
  const expected = (await download(sums.browser_download_url)).toString('utf8').split('\n')
    .map(line => line.trim().split(/\s+\*?/)).find(([, file]) => file === name)?.[0];
  const bytes = await download(asset.browser_download_url);
  const actual = createHash('sha256').update(bytes).digest('hex');
  if (!expected || expected.toLowerCase() !== actual) throw new Error(`${name}: sha256 mismatch`);
  const dir = path.join(root, 'target', 'install-ssh');
  mkdirSync(dir, { recursive: true });
  const file = path.join(dir, name);
  writeFileSync(file, bytes);
  return { file, label: `release v${version}` };
}

const sh = text => `'${text.replace(/'/g, `'\\''`)}'`;

function unixScript(name) {
  return `set -u
tmp="$HOME/${name}"
inst="\${XDG_BIN_HOME:-$HOME/.local/bin}/mistl"
trap 'rm -f "$tmp"' EXIT
chmod +x "$tmp"
"$tmp" --version >/dev/null || { echo "the uploaded mistl does not run on this host" >&2; exit 1; }
was=0
if [ -x "$inst" ] && "$inst" daemon status >/dev/null 2>&1; then
  was=1
  "$inst" daemon stop >/dev/null || exit 1
  n=0
  while "$inst" daemon status >/dev/null 2>&1; do
    n=$((n + 1))
    [ "$n" -ge 20 ] && { echo "installed daemon did not stop; no process was force-killed" >&2; exit 1; }
    sleep 0.5
  done
  sleep 1
fi
"$tmp" install ${forward.map(sh).join(' ')} || exit $?
if [ "$was" = 1 ] || [ ${start ? 1 : 0} = 1 ]; then
  # Detached from this SSH session: no tty, no inherited pipes, SIGHUP ignored.
  nohup "$inst" daemon start </dev/null >/dev/null 2>&1 || { echo "daemon start failed" >&2; exit 1; }
  echo "daemon started"
fi
`;
}

function windowsScript(name) {
  const args = forward.map(f => `'${f}'`).join(' ');
  return `$tmp = Join-Path $HOME '${name}'
$inst = Join-Path $env:LOCALAPPDATA 'Programs\\mistl\\mistl.exe'
function Running { & $inst daemon status > $null 2>&1; $LASTEXITCODE -eq 0 }
try {
  & $tmp --version > $null
  if ($LASTEXITCODE -ne 0) { Write-Error 'the uploaded mistl does not run on this host'; exit 1 }
  $was = (Test-Path $inst) -and (Running)
  if ($was) {
    & $inst daemon stop > $null
    if ($LASTEXITCODE -ne 0) { exit 1 }
    $deadline = (Get-Date).AddSeconds(10)
    while (Running) {
      if ((Get-Date) -gt $deadline) { Write-Error 'installed daemon did not stop; no process was force-killed'; exit 1 }
      Start-Sleep -Milliseconds 300
    }
    Start-Sleep -Milliseconds 500
  }
  & $tmp install ${args}
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  if ($was -or $${start}) {
    # Through WMI so the daemon is not part of this SSH session, whose
    # processes Windows OpenSSH ends on disconnect.
    $null = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = '"' + $inst + '" daemon start' }
    $deadline = (Get-Date).AddSeconds(15)
    while (-not (Running)) {
      if ((Get-Date) -gt $deadline) { Write-Error 'daemon start failed'; exit 1 }
      Start-Sleep -Milliseconds 300
    }
    'daemon started'
  }
} finally {
  Remove-Item -Force -ErrorAction SilentlyContinue $tmp
}
`;
}

function installOn(host, target, file) {
  const name = `.mistl-install-${randomBytes(4).toString('hex')}${isWindowsTarget(target) ? '.exe' : ''}`;
  const upload = spawnSync('scp', ['-q', file, `${host}:${name}`], { stdio: 'inherit', windowsHide: true });
  if (upload.status !== 0) throw new Error(`scp to ${host} failed`);
  const result = isWindowsTarget(target)
    ? ssh(host, `powershell -NoProfile -NonInteractive -EncodedCommand ${Buffer.from(windowsScript(name), 'utf16le').toString('base64')}`,
      { stdio: 'inherit' })
    : ssh(host, 'sh -s', { input: unixScript(name), stdio: ['pipe', 'inherit', 'inherit'] });
  if (result.status !== 0) throw new Error(`install on ${host} failed (exit ${result.status})`);
}

const version = `${latestTagVersion()}+local`;
const binaries = new Map(); // target -> Promise<{file, label}>
async function binaryFor(target) {
  if (!binaries.has(target)) {
    binaries.set(target, (async () => {
      const local = releaseOnly ? null : localBuild(target);
      if (!local) return releaseBinary(target);
      console.log(`Building stable ${version} for ${target} from this checkout...`);
      if (!await local.build(version)) throw new Error(`build for ${target} failed`);
      return { file: local.file, label: `local ${version}` };
    })());
  }
  return binaries.get(target);
}

const failed = [];
for (const host of hosts) {
  try {
    const target = probe(host);
    const { file, label } = await binaryFor(target);
    console.log(`==> ${host} (${target}): installing ${label}`);
    installOn(host, target, file);
    console.log(`==> ${host}: done`);
  } catch (error) {
    console.error(`==> ${host}: ${error.message}`);
    failed.push(host);
  }
}
if (failed.length) {
  console.error(`failed: ${failed.join(', ')}`);
  process.exit(1);
}
