# Fetches mistlib (MISTLIB_REPO/MISTLIB_REF from .env) into .mistlib-src, a
# plain git clone that the Cargo path dependencies point into. Safe to re-run:
# updates the existing clone to the configured ref (detached checkout).
# Local uncommitted changes in the cache are auto-stashed around the update
# and restored afterwards; if they conflict with the new upstream commit, the
# stash is rolled back and the cache stays on its previous commit instead of
# discarding anything.
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$cache = Join-Path $root ".mistlib-src"
$gitDir = Join-Path $cache ".git"
$localMarker = Join-Path $cache ".mistlib-local-source"

# Never use `git -C $cache` here. If the cache disappears or is not a Git
# worktree, Git walks up to mistl's own .git directory and destructive commands
# such as remote set-url/stash/checkout operate on the parent repository.
# An explicit git-dir/work-tree pair fails closed instead of discovering a
# parent repository.
function Invoke-CacheGit {
    & git "--git-dir=$gitDir" "--work-tree=$cache" @args
}

if (Test-Path -LiteralPath $localMarker) {
    Write-Host "mistlib: using local snapshot in .mistlib-src; skipping fetch"
    exit 0
}

$envFile = Join-Path $root ".env"

if (-not (Test-Path $envFile)) {
    Write-Host "error: $envFile not found -- copy .env.example to .env and fill it in."
    exit 1
}

$cfg = @{}
foreach ($line in Get-Content $envFile) {
    if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$') {
        $cfg[$Matches[1]] = $Matches[2]
    }
}

$repo = $cfg["MISTLIB_REPO"]
$ref = if ($cfg["MISTLIB_REF"]) { $cfg["MISTLIB_REF"] } else { "develop" }

if (-not $repo) {
    Write-Host "error: MISTLIB_REPO is not set in .env"
    exit 1
}

if (-not (Test-Path -LiteralPath $gitDir -PathType Container)) {
    if (Test-Path $cache) { Remove-Item -Recurse -Force $cache }
    git clone $repo $cache
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$gitDirItem = Get-Item -LiteralPath $gitDir -ErrorAction SilentlyContinue
if (-not $gitDirItem -or -not $gitDirItem.PSIsContainer -or
    ($gitDirItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
    Write-Host "error: $gitDir is not a safe, standalone Git directory"
    exit 1
}

# The clone is created only once, so a later MISTLIB_REPO change in .env would
# otherwise keep fetching from the original remote. Re-point it on every run so
# .env stays the single source of truth (public mistlib vs private mistlib-dev).
Invoke-CacheGit remote set-url origin $repo
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# LF working files show as phantom-modified under a global core.autocrlf=true;
# force this clone to leave line endings alone.
Invoke-CacheGit config core.autocrlf false
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Invoke-CacheGit fetch origin $ref
if ($LASTEXITCODE -ne 0) {
    $shortHead = Invoke-CacheGit rev-parse --short HEAD
    Write-Host "warning: could not fetch mistlib (offline?); keeping $shortHead"
    exit 0
}

$old = Invoke-CacheGit rev-parse HEAD
$new = Invoke-CacheGit rev-parse FETCH_HEAD

if ($old -eq $new) {
    $commit = Invoke-CacheGit rev-parse --short HEAD
    Write-Host "mistlib ($ref @ $commit) ready in .mistlib-src"
    exit 0
}

$dirty = Invoke-CacheGit status --porcelain
$stashed = $false
if ($dirty) {
    Invoke-CacheGit stash push --include-untracked -m "fetch auto-stash"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "error: failed to stash local changes in .mistlib-src"
        exit 1
    }
    $stashed = $true
}

Invoke-CacheGit checkout --detach FETCH_HEAD
if ($LASTEXITCODE -ne 0) {
    if ($stashed) {
        Invoke-CacheGit reset --hard
        Invoke-CacheGit checkout --detach $old
        Invoke-CacheGit stash pop
    }
    Write-Host "error: failed to checkout FETCH_HEAD in .mistlib-src"
    exit 1
}

if ($stashed) {
    Invoke-CacheGit stash pop
    if ($LASTEXITCODE -ne 0) {
        Invoke-CacheGit reset --hard
        Invoke-CacheGit checkout --detach $old
        Invoke-CacheGit stash pop
        $oldShort = Invoke-CacheGit rev-parse --short $old
        Write-Host "warning: the upstream update to mistlib conflicts with local uncommitted changes in .mistlib-src."
        Write-Host "warning: staying on the previous commit $oldShort."
        Write-Host "warning: commit/push or resolve the local changes, then re-run 'just fetch-mistlib'."
        exit 0
    }
}

$commit = Invoke-CacheGit rev-parse --short HEAD
Write-Host "mistlib ($ref @ $commit) ready in .mistlib-src"
