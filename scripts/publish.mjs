// Tag-driven releases: just publish status | beta [minor|patch|major] | stable.
// Add --push to publish; Cargo.toml and commits are never changed here.
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const usage = `Usage: node scripts/publish.mjs <command>
  status
  beta [minor|patch|major] [--push] [--allow-changes] [--allow-branch]
  stable [--push] [--allow-changes] [--allow-branch]
  help | -h

Without --push, a release tag is created locally only.
--allow-changes permits a dirty tree; --allow-branch skips branch restrictions
and the remote HEAD check only when there is no remote branch.`;

export function parseVersion(tag) {
  const match = /^v(\d+)\.(\d+)\.(\d+)(?:-beta\.(\d+))?$/.exec(tag);
  if (!match) return null;
  return { tag, base:match.slice(1, 4).map(BigInt), beta:match[4] === undefined ? null : BigInt(match[4]) };
}

export function compareBase(a, b) {
  for (let i = 0; i < 3; i++) {
    if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1;
  }
  return 0;
}

export function compareVersions(a, b) {
  const base = compareBase(a.base, b.base);
  if (base) return base;
  if (a.beta === b.beta) return 0;
  if (a.beta === null) return 1;
  if (b.beta === null) return -1;
  return a.beta < b.beta ? -1 : 1;
}

export function releaseState(tags) {
  const versions = [...new Set(tags)].map(parseVersion).filter(Boolean).sort(compareVersions);
  const stable = versions.filter(v => v.beta === null).at(-1) || null;
  const open = versions.filter(v => v.beta !== null && (!stable || compareBase(v.base, stable.base) > 0)).at(-1) || null;
  return { versions, stable, open };
}

export function bumpVersion(base, bump) {
  const [major, minor, patch] = base;
  if (bump === 'major') return [major + 1n, 0n, 0n];
  if (bump === 'minor') return [major, minor + 1n, 0n];
  if (bump === 'patch') return [major, minor, patch + 1n];
  throw new Error(`unknown bump: ${bump}`);
}

export function nextBeta(state, cargo, bump) {
  const { versions, stable, open } = state;
  if (!bump && open) return `v${open.base.join('.')}-beta.${open.beta + 1n}`;
  const target = versions.length ? bumpVersion(stable?.base || cargo.base, bump || 'minor') : cargo.base;
  const comparison = open ? compareBase(target, open.base) : 1;
  if (comparison < 0) {
    throw new Error(`open beta line is ${open.tag}; v${target.join('.')} is lower and would never be offered to prerelease updaters`);
  }
  return `v${target.join('.')}-beta.${comparison === 0 ? open.beta + 1n : 1n}`;
}

export function nextStable(state) {
  if (!state.open) throw new Error('nothing to promote; run `just publish beta` first');
  return `v${state.open.base.join('.')}`;
}

function runGit(args) {
  return spawnSync('git', args, { cwd:root, windowsHide:true, encoding:'utf8' });
}

function gitFailure(args, result) {
  return new Error(`git ${args.join(' ')} failed: ${result.error?.message || result.stderr?.trim() || `exit ${result.status}`}`);
}

function git(...args) {
  const result = runGit(args);
  if (result.status !== 0) throw gitFailure(args, result);
  return result.stdout.trim();
}

function warn(message) {
  console.error(`warning: ${message}`);
}

function remoteTags(required) {
  const args = ['ls-remote', '--tags', 'origin', 'refs/tags/v*'];
  const result = runGit(args);
  if (result.status !== 0) {
    const error = gitFailure(args, result);
    if (required) throw error;
    warn(`${error.message}; using local tags only`);
    return new Set();
  }
  return new Set(result.stdout.split(/\r?\n/).filter(Boolean).map(line => line.split(/\s+/)[1].replace(/^refs\/tags\//, '').replace(/\^\{\}$/, '')));
}

function cargoVersion() {
  const toml = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
  const section = toml.match(/^\[package\][^\S\r\n]*(?:#.*)?\r?\n([\s\S]*?)(?=^\s*\[|(?![\s\S]))/m)?.[1];
  const version = section?.match(/^\s*version\s*=\s*['"]([^'"]+)['"]/m)?.[1];
  const parsed = parseVersion(`v${version}`);
  if (!parsed) throw new Error('cannot read a supported [package] version from Cargo.toml');
  return { version, parsed };
}

function checkRemoteHead(branch, allowBranch) {
  if (!branch) {
    if (allowBranch) return;
    throw new Error('detached HEAD has no remote branch');
  }
  const args = ['ls-remote', '--exit-code', '--heads', 'origin', `refs/heads/${branch}`];
  const remote = runGit(args);
  if (remote.status === 2 && allowBranch) {
    warn(`no remote branch ${branch}; skipping remote HEAD check`);
    return;
  }
  if (remote.status === 2) throw new Error(`push ${branch} first`);
  if (remote.status !== 0) throw gitFailure(args, remote);
  git('fetch', 'origin', branch, '--quiet');
  const ancestor = runGit(['merge-base', '--is-ancestor', 'HEAD', `refs/remotes/origin/${branch}`]);
  if (ancestor.status === 1) throw new Error(`push ${branch} first`);
  if (ancestor.status !== 0) throw gitFailure(['merge-base', '--is-ancestor', 'HEAD', `origin/${branch}`], ancestor);
}

function main(args) {
  if (!args.length || (args.length === 1 && ['help', '-h'].includes(args[0]))) {
    console.log(usage);
    return;
  }
  const [command, ...rest] = args;
  let bump;
  const flags = new Set();
  let valid = ['status', 'beta', 'stable'].includes(command);
  for (const arg of rest) {
    if (command !== 'status' && ['--push', '--allow-changes', '--allow-branch'].includes(arg) && !flags.has(arg)) flags.add(arg);
    else if (command === 'beta' && ['minor', 'patch', 'major'].includes(arg) && !bump) bump = arg;
    else valid = false;
  }
  if (!valid) {
    console.error(usage);
    process.exitCode = 2;
    return;
  }

  const branch = git('branch', '--show-current');
  const dirty = Boolean(git('status', '--porcelain'));
  const cargo = cargoVersion();
  if (command !== 'status') git('fetch', 'origin', '--tags', '--quiet');
  const remote = remoteTags(command !== 'status');
  const local = git('tag', '--list').split(/\r?\n/).filter(Boolean);
  // The remote is the source of truth: stray local-only tags (e.g. fetched
  // from another repository) must not move the version line.
  const localOnly = local.filter(tag => parseVersion(tag) && !remote.has(tag));
  const tags = remote.size ? remote : new Set(local);
  const state = releaseState(tags);

  if (command === 'status') {
    const label = tag => `${tag} (${remote.has(tag) ? 'remote' : 'local only'})`;
    const line = (key, value) => console.log(`${key.padEnd(16)}${value}`);
    line('branch', `${branch || '(detached HEAD)'} @ ${git('rev-parse', '--short', 'HEAD')} (${dirty ? 'dirty' : 'clean'})`);
    line('Cargo.toml', `${cargo.version} (CI stamps the tag version at build time)`);
    line('latest stable', state.stable ? label(state.stable.tag) : 'none');
    line('open beta', state.open ? label(state.open.tag) : 'none');
    line('next beta', `${nextBeta(state, cargo.parsed)}   (\`just publish beta\`)`);
    line('next stable', state.open ? nextStable(state) : 'none (no open beta)');
    if (remote.size && localOnly.length) line('ignored', `${localOnly.join(', ')} (local only)`);
    return;
  }

  if (!flags.has('--allow-branch') && !(command === 'beta' ? ['develop', 'main'] : ['main']).includes(branch)) {
    throw new Error(`${command} requires ${command === 'beta' ? 'develop or main' : 'main'}; current branch is ${branch || 'detached HEAD'} (use --allow-branch to override)`);
  }
  if (dirty) {
    if (!flags.has('--allow-changes')) throw new Error('working tree is dirty; commit or stash changes, or use --allow-changes');
    warn('uncommitted changes are NOT part of the tag');
  }
  checkRemoteHead(branch, flags.has('--allow-branch'));
  const tag = command === 'beta' ? nextBeta(state, cargo.parsed, bump) : nextStable(state);
  if (tags.has(tag)) throw new Error(`tag ${tag} already exists on origin`);
  if (local.includes(tag)) throw new Error(`tag ${tag} exists locally but not on origin; push it or delete it (git tag -d ${tag})`);
  if (command === 'stable') {
    const args = ['merge-base', '--is-ancestor', state.open.tag, 'HEAD'];
    const ancestor = runGit(args);
    if (ancestor.status === 1) throw new Error(`HEAD must contain the latest beta tag commit (${state.open.tag})`);
    if (ancestor.status !== 0) throw gitFailure(args, ancestor);
  }
  git('tag', '-a', tag, '-m', `mistl ${tag}`);
  if (flags.has('--push')) {
    git('push', 'origin', `refs/tags/${tag}`);
    console.log(`Pushed ${tag}; follow the release build: https://github.com/tik-choco/mistl/actions`);
  } else {
    console.log(`Created ${tag} (local only).\nPush: git push origin ${tag}\nUndo: git tag -d ${tag}`);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(process.argv.slice(2)); }
  catch (error) {
    console.error(`error: ${error.message}`);
    process.exitCode = 1;
  }
}
