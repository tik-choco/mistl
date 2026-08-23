#!/usr/bin/env sh
# Fetches mistlib (MISTLIB_REPO/MISTLIB_REF from .env) into .mistlib-src, a
# plain git clone that the Cargo path dependencies point into. Safe to re-run:
# updates the existing clone to the configured ref (detached checkout).
# Local uncommitted changes in the cache are auto-stashed around the update
# and restored afterwards; never discarded. Tolerates being offline once the
# clone exists.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cache="$root/.mistlib-src"
git_dir="$cache/.git"

# Never use `git -C "$cache"` here. If the cache disappears or is not a Git
# worktree, Git walks up to mistl's own .git directory. Pinning both paths makes
# every command fail closed instead of operating on the parent repository.
cache_git() {
    git --git-dir="$git_dir" --work-tree="$cache" "$@"
}

if [ -f "$cache/.mistlib-local-source" ]; then
    echo "mistlib: using local snapshot in .mistlib-src; skipping fetch"
    exit 0
fi

env_file="$root/.env"

if [ ! -f "$env_file" ]; then
    echo "error: $env_file not found -- copy .env.example to .env and fill it in." >&2
    exit 1
fi

MISTLIB_REPO=$(sed -n 's/^ *MISTLIB_REPO *= *//p' "$env_file" | tail -1)
MISTLIB_REF=$(sed -n 's/^ *MISTLIB_REF *= *//p' "$env_file" | tail -1)
: "${MISTLIB_REF:=develop}"

if [ -z "$MISTLIB_REPO" ]; then
    echo "error: MISTLIB_REPO is not set in .env" >&2
    exit 1
fi

if [ ! -d "$git_dir" ]; then
    rm -rf "$cache"
    git clone "$MISTLIB_REPO" "$cache"
fi

if [ ! -d "$git_dir" ] || [ -L "$git_dir" ]; then
    echo "error: $git_dir is not a safe, standalone Git directory" >&2
    exit 1
fi

# The clone is created only once, so a later MISTLIB_REPO change in .env would
# otherwise keep fetching from the original remote. Re-point it on every run so
# .env stays the single source of truth (public mistlib vs private mistlib-dev).
cache_git remote set-url origin "$MISTLIB_REPO"

cache_git config core.autocrlf false

if ! cache_git fetch origin "$MISTLIB_REF"; then
    echo "warning: could not fetch mistlib (offline?); keeping $(cache_git rev-parse --short HEAD)" >&2
    exit 0
fi

old=$(cache_git rev-parse HEAD)
new=$(cache_git rev-parse FETCH_HEAD)

if [ "$old" = "$new" ]; then
    echo "mistlib ($MISTLIB_REF @ $(cache_git rev-parse --short HEAD)) ready in .mistlib-src"
    exit 0
fi

stashed=0
if [ -n "$(cache_git status --porcelain)" ]; then
    cache_git stash push --include-untracked -m "fetch auto-stash"
    stashed=1
fi

cache_git checkout --detach FETCH_HEAD

if [ "$stashed" -eq 1 ]; then
    if ! cache_git stash pop; then
        cache_git reset --hard
        cache_git checkout --detach "$old"
        cache_git stash pop
        echo "warning: upstream update for mistlib conflicts with local uncommitted changes in .mistlib-src; staying on the previous commit ($(cache_git rev-parse --short "$old")). Commit/push or resolve those changes, then re-run 'just fetch-mistlib'." >&2
        exit 0
    fi
fi

echo "mistlib ($MISTLIB_REF @ $(cache_git rev-parse --short HEAD)) ready in .mistlib-src"
