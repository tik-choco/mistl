# Rebuilds (debug) and restarts the daemon whenever a file under src/ or
# Cargo.toml/Cargo.lock changes. Paired with the debug-only live-reload poll
# injected into the dashboard HTML (see src/web/server.rs), so a browser tab
# left open on the dashboard reloads itself once the new daemon is back up --
# no need to close/reopen the tab after every edit.
#
# Polling (not FileSystemWatcher) on a plain mtime check: simple, and this is
# a dev-loop tool where a ~1s detection delay is unnoticeable next to the
# build itself.
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$bin = "mistl"
$watchPaths = @("src", "Cargo.toml", "Cargo.lock")

function Get-State {
    Get-ChildItem -Path $watchPaths -Recurse -File -ErrorAction SilentlyContinue |
        ForEach-Object { "$($_.FullName)|$($_.LastWriteTimeUtc.Ticks)" } |
        Sort-Object |
        Out-String
}

function Build-And-Restart {
    Write-Host "[watch] building..." -ForegroundColor Cyan
    Stop-Process -Name $bin -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 300

    cargo build
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[watch] build failed -- fix the error and save again" -ForegroundColor Red
        return
    }

    & ".\target\debug\$bin.exe" daemon start
    Write-Host "[watch] daemon restarted" -ForegroundColor Green
}

Build-And-Restart
$lastState = Get-State

# The daemon runs detached (`daemon start` backgrounds it), so it survives
# Ctrl+C on this script -- stopping `just watch` just stops rebuilding, the
# last build keeps running like it would after `just release`.
Write-Host "[watch] watching $($watchPaths -join ', ') for changes (Ctrl+C to stop)" -ForegroundColor Cyan

while ($true) {
    Start-Sleep -Seconds 1
    $state = Get-State
    if ($state -ne $lastState) {
        Build-And-Restart
        # Builds touch files under target/, which isn't in $watchPaths, so
        # re-snapshotting after the build picks up only source edits that
        # landed while it was running (not the build's own output).
        $lastState = Get-State
    }
}
