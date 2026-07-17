# Fetches mistlib-consensus (MISTLIB_CONSENSUS_REPO/MISTLIB_CONSENSUS_REF from
# .env) into .mistlib-consensus-src, a plain git clone that the Cargo path
# dependencies point into. Safe to re-run: updates the existing clone to the
# configured ref (detached checkout). Mirrors fetch-mistlib.ps1.
# Local uncommitted changes in the cache are auto-stashed around the update
# and restored afterwards; if they conflict with the new upstream commit, the
# stash is rolled back and the cache stays on its previous commit instead of
# discarding anything.
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
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

$repo = $cfg["MISTLIB_CONSENSUS_REPO"]
$ref = if ($cfg["MISTLIB_CONSENSUS_REF"]) { $cfg["MISTLIB_CONSENSUS_REF"] } else { "main" }

if (-not $repo) {
    Write-Host "error: MISTLIB_CONSENSUS_REPO is not set in .env"
    exit 1
}

$cache = Join-Path $root ".mistlib-consensus-src"

if (-not (Test-Path (Join-Path $cache ".git"))) {
    if (Test-Path $cache) { Remove-Item -Recurse -Force $cache }
    git clone $repo $cache
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

# LF working files show as phantom-modified under a global core.autocrlf=true;
# force this clone to leave line endings alone.
git -C $cache config core.autocrlf false
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

git -C $cache fetch origin $ref
if ($LASTEXITCODE -ne 0) {
    $shortHead = git -C $cache rev-parse --short HEAD
    Write-Host "warning: could not fetch mistlib-consensus (offline?); keeping $shortHead"
    exit 0
}

$old = git -C $cache rev-parse HEAD
$new = git -C $cache rev-parse FETCH_HEAD

if ($old -eq $new) {
    $commit = git -C $cache rev-parse --short HEAD
    Write-Host "mistlib-consensus ($ref @ $commit) ready in .mistlib-consensus-src"
    exit 0
}

$dirty = git -C $cache status --porcelain
$stashed = $false
if ($dirty) {
    git -C $cache stash push --include-untracked -m "fetch auto-stash"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "error: failed to stash local changes in .mistlib-consensus-src"
        exit 1
    }
    $stashed = $true
}

git -C $cache checkout --detach FETCH_HEAD
if ($LASTEXITCODE -ne 0) {
    if ($stashed) {
        git -C $cache reset --hard
        git -C $cache checkout --detach $old
        git -C $cache stash pop
    }
    Write-Host "error: failed to checkout FETCH_HEAD in .mistlib-consensus-src"
    exit 1
}

if ($stashed) {
    git -C $cache stash pop
    if ($LASTEXITCODE -ne 0) {
        git -C $cache reset --hard
        git -C $cache checkout --detach $old
        git -C $cache stash pop
        $oldShort = git -C $cache rev-parse --short $old
        Write-Host "warning: the upstream update to mistlib-consensus conflicts with local uncommitted changes in .mistlib-consensus-src."
        Write-Host "warning: staying on the previous commit $oldShort."
        Write-Host "warning: commit/push or resolve the local changes, then re-run 'just fetch-mistlib-consensus'."
        exit 0
    }
}

$commit = git -C $cache rev-parse --short HEAD
Write-Host "mistlib-consensus ($ref @ $commit) ready in .mistlib-consensus-src"
