#!/bin/sh
# Rebuilds (debug) and restarts the daemon whenever a file under src/ or
# Cargo.toml/Cargo.lock changes. Paired with the debug-only live-reload poll
# injected into the dashboard HTML (see src/web/server.rs), so a browser tab
# left open on the dashboard reloads itself once the new daemon is back up --
# no need to close/reopen the tab after every edit.
#
# Polling an mtime snapshot (not inotify) so this runs with no extra tools
# installed; a ~1s detection delay is unnoticeable next to the build itself.
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

bin="mistl"

snapshot() {
    find src Cargo.toml Cargo.lock -type f -exec stat -c '%n %Y' {} + 2>/dev/null | sort
}

build_and_restart() {
    echo "[watch] building..."
    pkill -f "target/debug/$bin( |\$)" 2>/dev/null || true
    sleep 0.3

    if ! cargo build; then
        echo "[watch] build failed -- fix the error and save again"
        return
    fi

    "./target/debug/$bin" daemon start
    echo "[watch] daemon restarted"
}

build_and_restart
last_state=$(snapshot)

# The daemon runs detached (`daemon start` backgrounds it), so it survives
# Ctrl+C on this script -- stopping `just watch` just stops rebuilding, the
# last build keeps running like it would after `just release`.
echo "[watch] watching src/, Cargo.toml, Cargo.lock for changes (Ctrl+C to stop)"

while true; do
    sleep 1
    state=$(snapshot)
    if [ "$state" != "$last_state" ]; then
        build_and_restart
        # Builds touch files under target/, which isn't watched, so
        # re-snapshotting after the build picks up only source edits that
        # landed while it was running (not the build's own output).
        last_state=$(snapshot)
    fi
done
