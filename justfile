# mistl task runner — `just <recipe>` (run `just` or `just help` to list)
#
# Requires: `just` (cargo install just) and a Rust toolchain.
# On Windows the .cargo/config.toml statically links the MSVC CRT, so the
# release exe is a single standalone file (no VC++ redistributable needed).

set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

bin := "mistl"

# List available recipes
default:
    @just --list

# --- mistlib dependency ------------------------------------------------------

# Fetch/update mistlib into .mistlib-src (MISTLIB_REPO/MISTLIB_REF from .env)
[windows]
fetch-mistlib:
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts/fetch-mistlib.ps1

[unix]
fetch-mistlib:
    sh scripts/fetch-mistlib.sh

# Clone mistlib on first build; run `just fetch-mistlib` to update it later
[windows]
_ensure-mistlib:
    if (-not (Test-Path .mistlib-src/.git)) { just fetch-mistlib }

[unix]
_ensure-mistlib:
    @test -d .mistlib-src/.git || just fetch-mistlib

# --- mistlib-consensus dependency -------------------------------------------

# Fetch/update mistlib-consensus into .mistlib-consensus-src
# (MISTLIB_CONSENSUS_REPO/MISTLIB_CONSENSUS_REF from .env)
[windows]
fetch-mistlib-consensus:
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts/fetch-mistlib-consensus.ps1

[unix]
fetch-mistlib-consensus:
    sh scripts/fetch-mistlib-consensus.sh

# Clone mistlib-consensus on first build; run `just fetch-mistlib-consensus`
# to update it later
[windows]
_ensure-mistlib-consensus:
    if (-not (Test-Path .mistlib-consensus-src/.git)) { just fetch-mistlib-consensus }

[unix]
_ensure-mistlib-consensus:
    @test -d .mistlib-consensus-src/.git || just fetch-mistlib-consensus

# --- release ---------------------------------------------------------------

# Optimized release build (lto=thin, stripped — see Cargo.toml), then
# (re)start the daemon from the freshly built exe.
# Unlike the dev recipes below, release fetches/updates both mistlib clones
# first (fetch-mistlib fetch-mistlib-consensus) rather than only cloning them
# if missing, so a release build always picks up the latest pinned refs. The
# fetch scripts are offline-tolerant — if the fetch fails they warn and keep
# the existing checkout — and they auto-stash/re-apply any local changes in
# the clone, rolling back with a warning on conflict, so this won't fail
# outright when offline or clobber uncommitted work in the dependency clones.
# On Windows, first free the exe if a running daemon is holding it (the
# release link fails on a locked target). `Stop-Process -ErrorAction
# SilentlyContinue` hides the "process not found" message but still leaves
# $? = false, and `powershell -Command` returns that as exit code 1 — which
# just treats as a failed recipe. `; exit 0` forces a clean exit whether or
# not a mistl process was running. The final `daemon start` gets the same
# treatment: a build that succeeded shouldn't fail the recipe just because
# the daemon was, say, already started some other way in the meantime.
[windows]
release: fetch-mistlib fetch-mistlib-consensus
    Stop-Process -Name "{{bin}}" -Force -ErrorAction SilentlyContinue; exit 0
    cargo build --release
    .\target\release\{{bin}}.exe daemon start; exit 0

[unix]
release: fetch-mistlib fetch-mistlib-consensus
    cargo build --release
    ./target/release/{{bin}} daemon start || true

# Full release: format check, lint (deny warnings), test, then build
dist: fmt-check lint test release
    @echo "release binary: target/release/{{bin}}"

# Copy the release exe into ./dist for handoff
package: release
    cargo run --quiet --release -- --version || true
    just _copy-exe

[windows]
_copy-exe:
    New-Item -ItemType Directory -Force dist | Out-Null
    Copy-Item "target/release/{{bin}}.exe" "dist/" -Force
    Write-Host "packaged: dist/{{bin}}.exe"

[unix]
_copy-exe:
    mkdir -p dist
    cp "target/release/{{bin}}" "dist/"
    @echo "packaged: dist/{{bin}}"

# --- dev -------------------------------------------------------------------

# Debug build
build: _ensure-mistlib _ensure-mistlib-consensus
    cargo build

# Run the release binary, passing through args: `just run daemon start`
run *args: _ensure-mistlib _ensure-mistlib-consensus
    cargo run --release -- {{args}}

# Fast type-check without codegen
check: _ensure-mistlib _ensure-mistlib-consensus
    cargo check

# Run the test suite
test: _ensure-mistlib _ensure-mistlib-consensus
    cargo test

# --- quality ---------------------------------------------------------------

# Format the whole workspace
fmt:
    cargo fmt --all

# Verify formatting (used by `dist`/CI)
fmt-check:
    cargo fmt --all -- --check

# Clippy with warnings promoted to errors
lint:
    cargo clippy --all-targets -- -D warnings

# --- housekeeping ----------------------------------------------------------

# Remove build artifacts and ./dist
clean:
    cargo clean
    -just _rm-dist

[windows]
_rm-dist:
    if (Test-Path dist) { Remove-Item -Recurse -Force dist }

[unix]
_rm-dist:
    rm -rf dist
