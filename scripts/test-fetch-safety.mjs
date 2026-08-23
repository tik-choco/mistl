#!/usr/bin/env node
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
const fetchScripts = [
  'fetch-mistlib.ps1',
  'fetch-mistlib.sh',
  'fetch-mistlib-consensus.ps1',
  'fetch-mistlib-consensus.sh'
];

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

// Keep the implementation fail-closed: no dependency-cache Git command may
// rely on -C repository discovery, and both the git dir and worktree must be
// explicit in every platform implementation.
for (const name of fetchScripts) {
  const source = readFileSync(path.join(scriptsDir, name), 'utf8');
  const code = source
    .split(/\r?\n/)
    .filter((line) => !line.trimStart().startsWith('#'))
    .join('\n');
  assert(!/\bgit\s+-C\s+["']?\$cache\b/.test(code), `${name} uses unsafe git -C cache discovery`);
  assert(source.includes('--git-dir='), `${name} does not pin --git-dir`);
  assert(source.includes('--work-tree='), `${name} does not pin --work-tree`);
}

const fixture = mkdtempSync(path.join(tmpdir(), 'mistl-fetch-safety-'));
const cache = path.join(fixture, '.dependency-cache');
const sentinel = 'https://example.invalid/parent.git';
const wrong = 'https://example.invalid/dependency.git';

try {
  execFileSync('git', ['init', '-q', fixture], { stdio: 'ignore' });
  execFileSync('git', ['-C', fixture, 'remote', 'add', 'origin', sentinel]);
  mkdirSync(cache);

  // Reproduce the incident: with no cache/.git, -C discovers fixture/.git and
  // silently changes the parent repository.
  execFileSync('git', ['-C', cache, 'remote', 'set-url', 'origin', wrong]);
  const reproduced = execFileSync('git', ['-C', fixture, 'remote', 'get-url', 'origin'], {
    encoding: 'utf8'
  }).trim();
  assert(reproduced === wrong, 'failed to reproduce Git parent discovery');
  execFileSync('git', ['-C', fixture, 'remote', 'set-url', 'origin', sentinel]);

  // The hardened form must fail because cache/.git is absent, without falling
  // back to fixture/.git or changing its origin.
  const guarded = spawnSync(
    'git',
    [`--git-dir=${path.join(cache, '.git')}`, `--work-tree=${cache}`, 'remote', 'set-url', 'origin', wrong],
    { stdio: 'ignore' }
  );
  assert(guarded.status !== 0, 'guarded Git command unexpectedly succeeded');
  const after = execFileSync('git', ['-C', fixture, 'remote', 'get-url', 'origin'], {
    encoding: 'utf8'
  }).trim();
  assert(after === sentinel, 'guarded Git command changed the parent origin');
} finally {
  const tempRoot = path.resolve(tmpdir()) + path.sep;
  const resolvedFixture = path.resolve(fixture);
  assert(resolvedFixture.startsWith(tempRoot), `refusing to remove non-temporary path: ${resolvedFixture}`);
  rmSync(resolvedFixture, { recursive: true, force: true });
}

console.log('fetch safety: parent discovery reproduced; explicit git-dir/work-tree remained isolated');
