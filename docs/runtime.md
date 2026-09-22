# Development instances and external connections

Local builds, including `cargo build --release`, identify themselves as **dev**.
The release workflow stamps **stable**. Optimization is independent of the
distribution channel. `mistl --version` and `mistl build-info` describe the CLI
binary; `mistl daemon status` reports the daemon's build and connection mode.

Development dashboards show the version, build ID, instance, and mistlib version
at the top even when developer logging is disabled. The build ID hashes source
inputs and build settings; it is not the executable's checksum. Unknown source
provenance is reported as unknown, not inferred from the current checkout.

## Separate instances

```console
just dev
just watch
mistl --instance repro-123 daemon start
mistl --instance repro-123 network status
```

The default dev instance is unique to the build's worktree. Each instance has
separate configuration, identity keys, data, IPC token, log, and default ports.
Stable/default retains its existing OS data/config paths. Other instances use
`instances/<channel>/<instance>` under those OS paths. `--state-dir <root>` uses
`<root>/<channel>/<instance>/{config,data}` for isolated tests or portable state.
Do not manually point storage blocks, exports, sync folders, or jobs at another
instance's files. Existing configuration is not automatically copied into dev.

`just release` now builds without stopping or starting a daemon. `just dev` and
`just watch` stop only the worktree's own recognized daemon. Old executables that
cannot identify their instance must be stopped with their original CLI before
rebuilding; no process is killed by name.

## External connections ON/OFF

```console
mistl network off
mistl network on
mistl network status
```

The Windows tray and dashboard expose the same control. It applies to the
selected instance, including P2P, external AI requests, synchronization, webhooks,
updates, external listeners, and automatic jobs. Local dashboard/IPC and local
storage remain available. This is not a system-wide firewall and does not control
other applications or proxies. AI/stream/tunnel services are paused while OFF;
this initial implementation also pauses local AI and local stream services.

The choice is saved immediately to `network-state.json`. Applying a change
briefly restarts **only that instance** to terminate native network threads and
existing connections. `restarting` means the transition is pending; `off` in the
new daemon means the transition has completed. ON permits configured services
to run; it does not imply that any particular remote peer is reachable.

An OFF startup never starts the network services first. OFF survives shutdown,
restart, sleep, and hibernation. ON survives restart as user intent; individual
services use their existing reconnect behavior when the network returns.
Enabled AI provide/serve, relay, folder sync, scheduler, bot and tunnel settings
remain saved; an interrupted one-shot request is not replayed. Screen-capture
sessions are not automatically restarted by this switch.

New instances and legacy instances without a saved choice initially start OFF.
Select ON once to permit external connections. A corrupt/unreadable state starts
OFF with an error. Failed saves are not shown as saved; a failed OFF save stops
the daemon without restarting into a previously saved ON state.

## Tray and login startup

On Windows, each daemon has an icon labeled with channel, instance and version.
The menu provides external connection control, dashboard, login startup, and exit.
`--no-tray` is available for unattended tests. Linux/macOS retain the CLI/dashboard
controls; this change adds a notification-area implementation on Windows only.

```console
mistl autostart enable
mistl autostart disable
```

Autostart registration is independent from connection mode and is scoped to the
instance, including a custom state root. Dev registrations launch their current
binary path; keep that path available or disable the entry before moving it.
Stable/default keeps the existing `mistl` login entry. Dev cannot install over or
uninstall stable. Self-update is disabled in dev/preview, including manual apply.
No autostart entry is created just by running the daemon.

## Verification

```console
cargo test --locked -- --test-threads=1
cargo build --locked
node scripts/test-network.mjs
node scripts/test-runtime-ui.mjs
node scripts/test-ui.mjs
```

The network smoke test creates disposable instances, verifies independent keys
and ports, blocks offline operations, exercises ON/OFF restarts, verifies abrupt
exit recovery and corrupt-state handling, and stops only its own daemons.
It does not register login startup or reboot the PC.
