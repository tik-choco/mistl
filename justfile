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

# --- release ---------------------------------------------------------------

# Optimized release build (lto=thin, stripped — see Cargo.toml)
release:
    cargo build --release

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
build:
    cargo build

# Run the release binary, passing through args: `just run daemon start`
run *args:
    cargo run --release -- {{args}}

# Fast type-check without codegen
check:
    cargo check

# Run the test suite
test:
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
