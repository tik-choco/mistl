#!/usr/bin/env sh
# Fetches mistlib-consensus (MISTLIB_CONSENSUS_REPO/MISTLIB_CONSENSUS_REF from
# .env) into .mistlib-consensus-src, a plain git clone that the Cargo path
# dependencies point into. Safe to re-run: updates the existing clone to the
# configured ref (detached checkout). Mirrors fetch-mistlib.sh. Local
# uncommitted changes in the cache are auto-stashed around the update and
# restored afterwards; never discarded. Tolerates being offline once the
# clone exists.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cache="$root/.mistlib-consensus-src"
git_dir="$cache/.git"

# Pin every command to the dependency clone. `git -C` may discover mistl's
# parent .git directory when this cache is missing or concurrently replaced.
cache_git() {
    git --git-dir="$git_dir" --work-tree="$cache" "$@"
}

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

# A Windows-style absolute path (C:\...) in .env -- what you write to clone from
# a sibling checkout instead of the network -- is a valid local path to the
# host's native Git, but this script also runs inside WSL (`just release-linux`),
# where Git reads "C:\..." as scp-like host:path syntax and tries to ssh to a
# host named "C". Translate it to the /mnt/c mount there. `wslpath` exists only
# under WSL, which is exactly when the translation applies: Git Bash on the host
# drives native Git and must keep the path as written. The `remote set-url`
# below runs on every invocation, so alternating host and WSL builds each
# re-point the clone at the form their own Git understands.
case "$MISTLIB_CONSENSUS_REPO" in
    [A-Za-z]:[\\/]*)
        if command -v wslpath >/dev/null 2>&1; then
            MISTLIB_CONSENSUS_REPO=$(wslpath -u "$MISTLIB_CONSENSUS_REPO")
        fi
        ;;
esac

if [ ! -d "$git_dir" ]; then
    rm -rf "$cache"
    git clone "$MISTLIB_CONSENSUS_REPO" "$cache"
fi

if [ ! -d "$git_dir" ] || [ -L "$git_dir" ]; then
    echo "error: $git_dir is not a safe, standalone Git directory" >&2
    exit 1
fi

# See fetch-mistlib.sh: re-point the remote so a MISTLIB_CONSENSUS_REPO change
# in .env takes effect on an existing clone too.
cache_git remote set-url origin "$MISTLIB_CONSENSUS_REPO"

cache_git config core.autocrlf false

if ! cache_git fetch origin "$MISTLIB_CONSENSUS_REF"; then
    echo "warning: could not fetch mistlib-consensus (offline?); keeping $(cache_git rev-parse --short HEAD)" >&2
    exit 0
fi

old=$(cache_git rev-parse HEAD)
new=$(cache_git rev-parse FETCH_HEAD)

if [ "$old" = "$new" ]; then
    echo "mistlib-consensus ($MISTLIB_CONSENSUS_REF @ $(cache_git rev-parse --short HEAD)) ready in .mistlib-consensus-src"
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
        echo "warning: upstream update for mistlib-consensus conflicts with local uncommitted changes in .mistlib-consensus-src; staying on the previous commit ($(cache_git rev-parse --short "$old")). Commit/push or resolve those changes, then re-run 'just fetch-mistlib-consensus'." >&2
        exit 0
    fi
fi

echo "mistlib-consensus ($MISTLIB_CONSENSUS_REF @ $(cache_git rev-parse --short HEAD)) ready in .mistlib-consensus-src"
