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

# Point .mistlib-src at a snapshot copy of ../mistlib-dev instead of a git fetch —
# lets you build against engine changes before they're committed/pushed anywhere.
# Re-run after further mistlib-dev edits to refresh the snapshot.
mistlib-local:
    node scripts/mistlib-local.mjs on

# Drop the local snapshot and go back to the git-fetched mistlib (MISTLIB_REPO/REF in .env)
mistlib-npm:
    node scripts/mistlib-local.mjs off

# Show whether .mistlib-src is currently a local snapshot or a git clone
mistlib-status:
    node scripts/mistlib-local.mjs status

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

# --- cross build: Linux binary via WSL --------------------------------------

# Build a native Linux release binary by running cargo inside WSL2 against
# this same checkout, rather than a true host cross-compile. mistlib links
# several C libraries (openh264/opus built via cmake, fdk-aac via cc, OpenSSL
# for reqwest's native-tls), which need a real Linux toolchain/sysroot to
# link against -- running inside WSL sidesteps setting one up on Windows.
# `wsl --cd <path>` accepts an absolute Windows path directly, so no
# `wslpath` translation is needed; `just fetch-mistlib fetch-mistlib-consensus`
# runs its [unix] variant automatically once inside WSL's Linux shell.
# Requires WSL2 with a distro that has: rustup (with `cargo`), gcc, cmake,
# pkg-config, and libssl-dev -- e.g. on Ubuntu:
#   sudo apt install build-essential cmake pkg-config libssl-dev
#   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Uses --target explicitly so the Linux build lands in
# target/x86_64-unknown-linux-gnu/release, kept separate from the Windows
# build's target/release.
[windows]
release-linux:
    wsl --cd "{{justfile_directory()}}" -- bash -lc "just fetch-mistlib fetch-mistlib-consensus && cargo build --release --target x86_64-unknown-linux-gnu"
    just _copy-linux-exe

[windows]
_copy-linux-exe:
    New-Item -ItemType Directory -Force dist | Out-Null
    Copy-Item "target/x86_64-unknown-linux-gnu/release/{{bin}}" "dist/{{bin}}-linux-x86_64" -Force
    Write-Host "packaged: dist/{{bin}}-linux-x86_64"

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

# Rebuild (debug) and restart the daemon on every src/ change; the dashboard
# HTML gets a debug-only live-reload poll (src/web/server.rs) so a browser
# tab left open on it reloads itself once the new daemon is back up --
# no need to close/reopen the tab between edits. Ctrl+C stops the rebuild
# loop but leaves the last-built daemon running (same as `just release`).
[windows]
watch: _ensure-mistlib _ensure-mistlib-consensus
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts/watch.ps1

[unix]
watch: _ensure-mistlib _ensure-mistlib-consensus
    sh scripts/watch.sh

# --- quality ---------------------------------------------------------------

# Format this crate only. NOT `--all`: the mistlib/mistlib-consensus path
# dependencies live in .mistlib-src/.mistlib-consensus-src, and `--all` reaches
# into them and reformats upstream sources. That leaves the vendored clones
# dirty, which makes `just fetch-mistlib*` hit a stash conflict and silently
# keep the old commit -- how mistl once drifted 91 commits behind mistlib.
fmt:
    cargo fmt -p mistl

# Verify formatting (used by `dist`/CI). Same `-p mistl` scoping as `fmt`.
fmt-check:
    cargo fmt -p mistl -- --check

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
