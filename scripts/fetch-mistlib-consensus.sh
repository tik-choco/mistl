#!/usr/bin/env sh
# Fetches mistlib-consensus (MISTLIB_CONSENSUS_REPO/MISTLIB_CONSENSUS_REF from
# .env) into .mistlib-consensus-src, a plain git clone that the Cargo path
# dependencies point into. Safe to re-run: updates the existing clone to the
# configured ref (detached checkout). Mirrors fetch-mistlib.sh.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
env_file="$root/.env"

if [ ! -f "$env_file" ]; then
    echo "error: $env_file not found -- copy .env.example to .env and fill it in." >&2
    exit 1
fi

MISTLIB_CONSENSUS_REPO=$(sed -n 's/^ *MISTLIB_CONSENSUS_REPO *= *//p' "$env_file" | tail -1)
MISTLIB_CONSENSUS_REF=$(sed -n 's/^ *MISTLIB_CONSENSUS_REF *= *//p' "$env_file" | tail -1)
: "${MISTLIB_CONSENSUS_REF:=main}"

if [ -z "$MISTLIB_CONSENSUS_REPO" ]; then
    echo "error: MISTLIB_CONSENSUS_REPO is not set in .env" >&2
    exit 1
fi

cache="$root/.mistlib-consensus-src"

if [ ! -d "$cache/.git" ]; then
    rm -rf "$cache"
    git clone "$MISTLIB_CONSENSUS_REPO" "$cache"
fi

git -C "$cache" fetch origin "$MISTLIB_CONSENSUS_REF"
git -C "$cache" checkout --detach FETCH_HEAD

echo "mistlib-consensus ($MISTLIB_CONSENSUS_REF @ $(git -C "$cache" rev-parse --short HEAD)) ready in .mistlib-consensus-src"
