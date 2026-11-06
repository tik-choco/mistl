<div align="center">

# MISTL

**One daemon for the tik-choco P2P ecosystem.**

[![release](https://img.shields.io/github/v/release/tik-choco/mistl?include_prereleases&label=release&color=2f6feb)](https://github.com/tik-choco/mistl/releases/latest)
[![build](https://img.shields.io/github/actions/workflow/status/tik-choco/mistl/release.yml?label=build)](https://github.com/tik-choco/mistl/actions/workflows/release.yml)
[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

English · [日本語](README.ja.md) · [简体中文](README.zh.md)

</div>

---

`mistl` is a single Rust binary that runs a background daemon on top of
[mistlib](https://github.com/tik-choco-lab/mistlib). It keeps your identity,
files, chat history, tunnels, screen shares and AI models available to peers on
the mist network — with no server to run. Drive it from a web dashboard or the
CLI.

The Windows release is one standalone `mistl.exe`: screen capture, H264
encoding and the C runtime are built in. No DLLs, no installers, no external
tools.

## Features

|  |  |
| --- | --- |
| **Identity** | `did:key` Ed25519 keys and a profile shared with tc-chat / tc-storage, plus root → leaf delegation for browser apps |
| **Storage** | Content-addressed store (CIDv1, 1 MiB chunks), compatible with [tc-storage](https://github.com/tik-choco/tc-storage) |
| **Screen share** | Serve your screen, or a [tc-chat](https://github.com/tik-choco/tc-chat) share, as RTSP for the VRChat video player |
| **Chat relay** | Stays in tc-chat rooms and replays signed history to late joiners |
| **Distributed vault** | Keeps other apps' encrypted checkpoints replicated across peers (tc-escrow v1, `mistl escrow`) ([details](docs/escrow.md)) |
| **Nostr relay** | Local `ws://` Nostr relay with clustered replication and optional TLS and upstream relays ([setup](docs/configuration.md#nostr-relay)) |
| **Tunnel** | TCP/UDP port forwarding through NAT, with per-connection approval |
| **AI network** | Share LLMs with peers ([mistai](https://github.com/tik-choco-lab/mistai) protocol), and a local OpenAI-compatible API |
| **Agent** | `mistl agent`: a terminal AI assistant that can operate mistl and your workspace |
| **Dashboard** | Everything above from the browser, in English, 日本語 and 中文, with a live network topology and a tunnel node graph |

## Install

Download the binary for your platform from
[Releases](https://github.com/tik-choco/mistl/releases/latest):

| Platform | Asset |
| --- | --- |
| Windows | `mistl-v<version>-x86_64-pc-windows-msvc.zip` |
| Linux | `mistl-v<version>-x86_64-unknown-linux-gnu.tar.gz` / `aarch64-…` |
| macOS | `mistl-v<version>-aarch64-apple-darwin.tar.gz` / `x86_64-…` |

`mistl install` copies it to a per-user location and enables start at login.
`mistl update` keeps it current; `mistl update list` and
`mistl update install <version>` pick a specific release, including an older one.

## Quick start

Double-click `mistl.exe`, or run `mistl` with no arguments. The daemon starts in
the background and the dashboard opens in your browser.

> [!NOTE]
> Network connections start **off**. Turn them on in the dashboard, from the
> tray icon, or with `mistl network on`.

The same things from the CLI:

```console
$ mistl status                          # daemon, stream and AI at a glance
$ mistl store put ./file.bin            # -> CID
$ mistl stream relay --room <room>      # tc-chat share -> rtsp://<LAN IP>:8554/stream
$ mistl tunnel serve 22                 # offer local port 22 to the tunnel room
$ mistl tunnel rule ls                  # list automatic proposal rules
$ mistl ai serve start                  # OpenAI-compatible API on http://127.0.0.1:6478/v1
$ mistl agent                           # terminal AI agent
```

See [Tunnel Node Graph](docs/tunnel-graph.md#proposal-rules) for proposal rule
options and approval behavior.

## Documentation

| Topic | |
| --- | --- |
| [CLI reference](docs/cli.md) | Every command, VRChat relay, chat relay, distributed vault, updates, agent |
| [Distributed vault](docs/escrow.md) | Encrypted checkpoint escrow and replication |
| [Tunnel node graph](docs/tunnel-graph.md) | Dashboard links, port forwards, proposal rules |
| [Configuration](docs/configuration.md) | `config.toml` keys and where data lives |
| [AI configuration](docs/ai-config.md) | Providers, rooms and the local API |
| [Identity](docs/identity.md) | Profile document and DID delegation |
| [Authorization](docs/authorization.md) | Allow/deny peers, API tokens, usage limits, capabilities |
| [Architecture](docs/architecture.md) | Modules, interop, bot delivery, known limitations |
| [Runtime](docs/runtime.md) | Dev and stable instances, connection controls |
| [Building from source](docs/building.md) | Requirements, build, releases |

## Building from source

Requires Rust, git, cmake and (optionally) [just](https://github.com/casey/just).

```console
$ cp .env.example .env    # selects the mistlib source
$ just release            # fetches mistlib, then cargo build --release
```

Local builds run as an isolated **dev** instance, separate from an installed
release. See [Building from source](docs/building.md) for details.

## Status

Under active development. Releases are tagged; beta tags are prereleases.

## License

[Mozilla Public License 2.0](LICENSE)

Binary builds statically link the
[Fraunhofer FDK AAC Codec Library for Android](https://github.com/mstorsjo/fdk-aac)
(under its own terms; see the fdk-aac NOTICE) and
[libopus](https://opus-codec.org/) (BSD-3-Clause).
