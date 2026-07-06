# Fetches mistlib (MISTLIB_REPO/MISTLIB_REF from .env) into .mistlib-src, a
# plain git clone that the Cargo path dependencies point into. Safe to re-run:
# updates the existing clone to the configured ref (detached checkout).
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

$repo = $cfg["MISTLIB_REPO"]
$ref = if ($cfg["MISTLIB_REF"]) { $cfg["MISTLIB_REF"] } else { "develop" }

if (-not $repo) {
    Write-Host "error: MISTLIB_REPO is not set in .env"
    exit 1
}

$cache = Join-Path $root ".mistlib-src"

if (-not (Test-Path (Join-Path $cache ".git"))) {
    if (Test-Path $cache) { Remove-Item -Recurse -Force $cache }
    git clone $repo $cache
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

git -C $cache fetch origin $ref
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
git -C $cache checkout --detach FETCH_HEAD
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$commit = git -C $cache rev-parse --short HEAD
Write-Host "mistlib ($ref @ $commit) ready in .mistlib-src"
