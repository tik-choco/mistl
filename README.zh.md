<div align="center">

# MISTL

**一个守护进程，连接整个 tik-choco P2P 生态。**

[![release](https://img.shields.io/github/v/release/tik-choco/mistl?include_prereleases&label=release&color=2f6feb)](https://github.com/tik-choco/mistl/releases/latest)
[![build](https://img.shields.io/github/actions/workflow/status/tik-choco/mistl/release.yml?label=build)](https://github.com/tik-choco/mistl/actions/workflows/release.yml)
[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

[English](README.md) · [日本語](README.ja.md) · 简体中文

</div>

---

`mistl` 是一个基于 Rust 的独立可执行文件，在 [mistlib](https://github.com/tik-choco-lab/mistlib) 之上运行后台守护进程。
它让 mist 网络中的节点随时可以访问你的身份信息、文件、聊天记录、隧道、屏幕共享和 AI 模型，无需自建服务器。
通过 Web 控制面板或 CLI 即可操作。

Windows 版本只有一个独立的 `mistl.exe`：内置屏幕捕获、H264 编码和 C 运行时。无需 DLL、安装程序或外部工具。

## 特性

|  |  |
| --- | --- |
| **身份** | `did:key` Ed25519 密钥，与 tc-chat / tc-storage 共享的个人资料，以及供浏览器应用使用的根密钥 → 叶密钥委托 |
| **存储** | 内容寻址存储（CIDv1，1 MiB 分块），兼容 [tc-storage](https://github.com/tik-choco/tc-storage) |
| **屏幕共享** | 将你的屏幕或 [tc-chat](https://github.com/tik-choco/tc-chat) 中的共享内容作为 RTSP 提供给 VRChat 视频播放器 |
| **聊天中继** | 持续驻留 tc-chat 房间，向后加入的用户重放带签名的历史记录 |
| **隧道** | 穿透 NAT 的 TCP/UDP 端口转发，每次连接都需批准 |
| **AI 网络** | 与其他节点共享 LLM（[mistai](https://github.com/tik-choco-lab/mistai) 协议），并提供兼容 OpenAI 的本地 API |
| **智能体** | `mistl agent`：可操作 mistl 和你的工作区的终端 AI 助手 |
| **控制面板** | 在浏览器中使用上述所有功能，支持英语和日语 |

## 安装

从 [Releases](https://github.com/tik-choco/mistl/releases/latest)
下载适合你平台的可执行文件：

| 平台 | 文件 |
| --- | --- |
| Windows | `mistl-v<version>-x86_64-pc-windows-msvc.zip` |
| Linux | `mistl-v<version>-x86_64-unknown-linux-gnu.tar.gz` / `aarch64-…` |
| macOS | `mistl-v<version>-aarch64-apple-darwin.tar.gz` / `x86_64-…` |

`mistl install` 将其复制到当前用户的安装位置，并启用登录时启动。
`mistl update` 用于更新到最新版本。

## 快速开始

双击 `mistl.exe`，或不带参数运行 `mistl`。守护进程会在后台启动，并在浏览器中打开控制面板。

> [!NOTE]
> 网络连接默认**关闭**。可在控制面板中、通过托盘图标或运行 `mistl network on` 开启。

也可以通过 CLI 完成相同操作：

```console
$ mistl status                          # daemon, stream and AI at a glance
$ mistl store put ./file.bin            # -> CID
$ mistl stream relay --room <room>      # tc-chat share -> rtsp://<LAN IP>:8554/stream
$ mistl tunnel serve 22                 # offer local port 22 to the tunnel room
$ mistl ai serve start                  # OpenAI-compatible API on http://127.0.0.1:6478/v1
$ mistl agent                           # terminal AI agent
```

## 文档（英文）

| 主题 | |
| --- | --- |
| [CLI 参考](docs/cli.md) | 所有命令、VRChat 中继、聊天中继、托管、智能体 |
| [配置](docs/configuration.md) | `config.toml` 配置键和数据存放位置 |
| [Vault](docs/vault.md) | 加密副本存储和恢复 |
| [AI 配置](docs/ai-config.md) | 提供商、房间和本地 API |
| [身份](docs/identity.md) | 个人资料文档和 DID 委托 |
| [架构](docs/architecture.md) | 模块、互操作、机器人投递、已知限制 |
| [运行环境](docs/runtime.md) | dev 和稳定版实例、连接控制 |
| [从源码构建](docs/building.md) | 环境要求、构建、发布 |

## 从源码构建

需要 Rust、git、cmake，以及可选的 [just](https://github.com/casey/just)。

```console
$ cp .env.example .env    # selects the mistlib source
$ just release            # fetches mistlib, then cargo build --release
```

本地构建作为独立的 **dev** 实例运行，与已安装的发布版本隔离。详见[从源码构建](docs/building.md)。

## 状态

仍在积极开发中。每次发布都会打标签；beta 标签对应预发布版本。

## 许可

[Mozilla Public License 2.0](LICENSE)

可执行文件静态链接了
[Fraunhofer FDK AAC Codec Library for Android](https://github.com/mstorsjo/fdk-aac)
（遵循其自身条款，参见 fdk-aac NOTICE）和
[libopus](https://opus-codec.org/)（BSD-3-Clause）。
