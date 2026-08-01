#!/usr/bin/env sh
# Fetches mistlib (MISTLIB_REPO/MISTLIB_REF from .env) into .mistlib-src, a
# plain git clone that the Cargo path dependencies point into. Safe to re-run:
# updates the existing clone to the configured ref (detached checkout).
# Local uncommitted changes in the cache are auto-stashed around the update
# and restored afterwards; never discarded. Tolerates being offline once the
# clone exists.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
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

cache="$root/.mistlib-src"

if [ ! -d "$cache/.git" ]; then
    rm -rf "$cache"
    git clone "$MISTLIB_REPO" "$cache"
fi

# The clone is created only once, so a later MISTLIB_REPO change in .env would
# otherwise keep fetching from the original remote. Re-point it on every run so
# .env stays the single source of truth (public mistlib vs private mistlib-dev).
git -C "$cache" remote set-url origin "$MISTLIB_REPO"

git -C "$cache" config core.autocrlf false

if ! git -C "$cache" fetch origin "$MISTLIB_REF"; then
    echo "warning: could not fetch mistlib (offline?); keeping $(git -C "$cache" rev-parse --short HEAD)" >&2
    exit 0
fi

old=$(git -C "$cache" rev-parse HEAD)
new=$(git -C "$cache" rev-parse FETCH_HEAD)

if [ "$old" = "$new" ]; then
    echo "mistlib ($MISTLIB_REF @ $(git -C "$cache" rev-parse --short HEAD)) ready in .mistlib-src"
    exit 0
fi

stashed=0
if [ -n "$(git -C "$cache" status --porcelain)" ]; then
    git -C "$cache" stash push --include-untracked -m "fetch auto-stash"
    stashed=1
fi

git -C "$cache" checkout --detach FETCH_HEAD

if [ "$stashed" -eq 1 ]; then
    if ! git -C "$cache" stash pop; then
        git -C "$cache" reset --hard
        git -C "$cache" checkout --detach "$old"
        git -C "$cache" stash pop
        echo "warning: upstream update for mistlib conflicts with local uncommitted changes in .mistlib-src; staying on the previous commit ($(git -C "$cache" rev-parse --short "$old")). Commit/push or resolve those changes, then re-run 'just fetch-mistlib'." >&2
        exit 0
    fi
fi

echo "mistlib ($MISTLIB_REF @ $(git -C "$cache" rev-parse --short HEAD)) ready in .mistlib-src"
