#!/usr/bin/env node
// Toggles .mistlib-src between the git-fetched mistlib (MISTLIB_REPO/REF in
// .env, see scripts/fetch-mistlib.*) and a snapshot copied straight from a
// sibling mistlib-dev checkout, so engine changes can be tried out before
// they're committed/pushed anywhere.
//
// Deliberately a COPY, not a symlink/junction: scripts/fetch-mistlib.* runs
// `git remote set-url` / `stash` / `checkout --detach` / `reset --hard`
// directly against .mistlib-src. If .mistlib-src were linked straight at the
// live mistlib-dev checkout, any run of `just fetch-mistlib` (including the
// automatic one `just build`'s _ensure-mistlib triggers) would perform those
// destructive git operations on the actual mistlib-dev working tree —
// including auto-stashing (and potentially losing, on a conflict) uncommitted
// engine work. A copy can never do that; the cost is that it's a snapshot,
// not live — re-run `on` after further mistlib-dev edits to refresh it.
//
// Usage:
//   node scripts/mistlib-local.mjs on
//   node scripts/mistlib-local.mjs off
//   node scripts/mistlib-local.mjs status
import { existsSync, rmSync, mkdirSync, cpSync, readFileSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.dirname(__dirname);
const MISTLIB_DEV_DIR = path.resolve(ROOT, '..', 'mistlib-dev');
const CACHE = path.join(ROOT, '.mistlib-src');
const MARKER = path.join(CACHE, '.mistlib-local-source');
const CRATES = ['mistlib-core', 'mistlib-native'];

function hasMeaningfulGitChanges(dir) {
  if (!existsSync(path.join(dir, '.git'))) return false;
  try {
    const untracked = execFileSync(
      'git',
      ['-C', dir, 'ls-files', '--others', '--exclude-standard'],
      { encoding: 'utf8' }
    );
    if (untracked.trim().length > 0) return true;

    // A Windows checkout can appear dirty solely because every LF was read
    // back as CRLF (for example after cargo fmt touched the vendored crates).
    // Ignore only CR-at-EOL differences; staged or unstaged content changes
    // still make git exit non-zero and remain protected.
    execFileSync(
      'git',
      ['-C', dir, 'diff', 'HEAD', '--ignore-cr-at-eol', '--quiet', '--exit-code'],
      { stdio: 'ignore' }
    );
    return false;
  } catch {
    return true;
  }
}

function on() {
  if (!existsSync(MISTLIB_DEV_DIR)) {
    console.error(`mistlib-local: no sibling checkout at ${MISTLIB_DEV_DIR}`);
    process.exit(1);
  }
  for (const crate of CRATES) {
    if (!existsSync(path.join(MISTLIB_DEV_DIR, crate, 'Cargo.toml'))) {
      console.error(`mistlib-local: ${crate}/Cargo.toml not found under ${MISTLIB_DEV_DIR}`);
      process.exit(1);
    }
  }
  if (existsSync(CACHE) && !existsSync(MARKER) && hasMeaningfulGitChanges(CACHE)) {
    console.error('mistlib-local: .mistlib-src is a git clone with uncommitted changes — refusing to overwrite it.');
    console.error('Commit/stash them yourself, or remove .mistlib-src, then re-run.');
    process.exit(1);
  }

  rmSync(CACHE, { recursive: true, force: true });
  mkdirSync(CACHE, { recursive: true });
  for (const crate of CRATES) {
    cpSync(path.join(MISTLIB_DEV_DIR, crate), path.join(CACHE, crate), {
      recursive: true,
      filter: (src) => !/[\\/](target|\.git)([\\/]|$)/.test(src)
    });
  }
  // Dummy .git file: _ensure-mistlib only checks Test-Path .mistlib-src/.git
  // before skipping an automatic fetch. A gitfile pointing at a nonexistent
  // directory also prevents `git -C .mistlib-src` from walking upward and
  // accidentally operating on mistl's parent repository.
  writeFileSync(path.join(CACHE, '.git'), 'gitdir: .git-disabled\n');
  writeFileSync(MARKER, `${MISTLIB_DEV_DIR}\n${new Date().toISOString()}\n`);

  console.log(`mistlib-local: .mistlib-src is now a snapshot copy of ${MISTLIB_DEV_DIR}`);
  console.log('Edited mistlib-dev again? Re-run `just mistlib-local` to refresh the snapshot.');
}

function off() {
  if (!existsSync(MARKER)) {
    console.log('mistlib-local: .mistlib-src is not a local snapshot — nothing to do.');
    return;
  }
  rmSync(CACHE, { recursive: true, force: true });
  console.log('mistlib-local: removed the local snapshot, re-fetching from MISTLIB_REPO/REF...');
  try {
    execFileSync('just', ['fetch-mistlib'], { cwd: ROOT, stdio: 'inherit' });
  } catch {
    console.error('mistlib-local: fetch-mistlib failed (offline?) — .mistlib-src is empty; `just build` will retry the fetch.');
  }
}

function status() {
  if (existsSync(MARKER)) {
    const [source, stamp] = readFileSync(MARKER, 'utf8').trim().split('\n');
    console.log(`local snapshot of ${source} (copied ${stamp})`);
    return;
  }
  if (existsSync(path.join(CACHE, '.git'))) {
    try {
      const repo = execFileSync('git', ['-C', CACHE, 'remote', 'get-url', 'origin'], { encoding: 'utf8' }).trim();
      const sha = execFileSync('git', ['-C', CACHE, 'rev-parse', '--short', 'HEAD'], { encoding: 'utf8' }).trim();
      console.log(`git clone: ${repo} @ ${sha}`);
    } catch {
      console.log('.mistlib-src exists but is not a usable git clone (run `just fetch-mistlib`)');
    }
    return;
  }
  console.log('not fetched yet (run `just fetch-mistlib` or `just mistlib-local`)');
}

const cmd = process.argv[2];
switch (cmd) {
  case 'on': on(); break;
  case 'off': off(); break;
  case 'status': status(); break;
  default:
    console.error('usage: node scripts/mistlib-local.mjs <on|off|status>');
    process.exit(1);
}
