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
$cache = Join-Path $root ".mistlib-consensus-src"
$gitDir = Join-Path $cache ".git"
$envFile = Join-Path $root ".env"

# Pin every command to the dependency clone. `git -C` may discover mistl's
# parent .git directory when this cache is missing or concurrently replaced.
function Invoke-CacheGit {
    & git "--git-dir=$gitDir" "--work-tree=$cache" @args
}

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

# See fetch-mistlib.ps1: re-point the remote so a MISTLIB_CONSENSUS_REPO change
# in .env takes effect on an existing clone too.
Invoke-CacheGit remote set-url origin $repo
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# LF working files show as phantom-modified under a global core.autocrlf=true;
# force this clone to leave line endings alone.
Invoke-CacheGit config core.autocrlf false
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Invoke-CacheGit fetch origin $ref
if ($LASTEXITCODE -ne 0) {
    $shortHead = Invoke-CacheGit rev-parse --short HEAD
    Write-Host "warning: could not fetch mistlib-consensus (offline?); keeping $shortHead"
    exit 0
}

$old = Invoke-CacheGit rev-parse HEAD
$new = Invoke-CacheGit rev-parse FETCH_HEAD

if ($old -eq $new) {
    $commit = Invoke-CacheGit rev-parse --short HEAD
    Write-Host "mistlib-consensus ($ref @ $commit) ready in .mistlib-consensus-src"
    exit 0
}

$dirty = Invoke-CacheGit status --porcelain
$stashed = $false
if ($dirty) {
    Invoke-CacheGit stash push --include-untracked -m "fetch auto-stash"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "error: failed to stash local changes in .mistlib-consensus-src"
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
    Write-Host "error: failed to checkout FETCH_HEAD in .mistlib-consensus-src"
    exit 1
}

if ($stashed) {
    Invoke-CacheGit stash pop
    if ($LASTEXITCODE -ne 0) {
        Invoke-CacheGit reset --hard
        Invoke-CacheGit checkout --detach $old
        Invoke-CacheGit stash pop
        $oldShort = Invoke-CacheGit rev-parse --short $old
        Write-Host "warning: the upstream update to mistlib-consensus conflicts with local uncommitted changes in .mistlib-consensus-src."
        Write-Host "warning: staying on the previous commit $oldShort."
        Write-Host "warning: commit/push or resolve the local changes, then re-run 'just fetch-mistlib-consensus'."
        exit 0
    }
}

$commit = Invoke-CacheGit rev-parse --short HEAD
Write-Host "mistlib-consensus ($ref @ $commit) ready in .mistlib-consensus-src"
