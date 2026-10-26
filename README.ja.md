<div align="center">

# MISTL

**tik-choco の P2P エコシステムをひとつのデーモンで。**

[![release](https://img.shields.io/github/v/release/tik-choco/mistl?include_prereleases&label=release&color=2f6feb)](https://github.com/tik-choco/mistl/releases/latest)
[![build](https://img.shields.io/github/actions/workflow/status/tik-choco/mistl/release.yml?label=build)](https://github.com/tik-choco/mistl/actions/workflows/release.yml)
[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)

[English](README.md) · 日本語 · [简体中文](README.zh.md)

</div>

---

`mistl` は [mistlib](https://github.com/tik-choco-lab/mistlib) の上でバックグラウンドデーモンを動かす、
単一の Rust バイナリ。アイデンティティ、ファイル、チャット履歴、トンネル、画面共有、AI モデルを
mist ネットワーク上のピアから利用できる状態に保つ。サーバの運用は不要。
Web ダッシュボードか CLI から操作する。

Windows 版は単体で動く `mistl.exe` ひとつ。画面キャプチャ、H264 エンコード、
C ランタイムを内蔵しており、DLL、インストーラ、外部ツールは不要。

## 特徴

|  |  |
| --- | --- |
| **アイデンティティ** | `did:key` Ed25519 鍵と tc-chat / tc-storage 共通のプロフィール。ブラウザアプリ向けのルート → リーフ委任にも対応 |
| **ストレージ** | コンテンツアドレス指定ストレージ（CIDv1、1 MiB チャンク）。[tc-storage](https://github.com/tik-choco/tc-storage) と互換 |
| **画面共有** | 自分の画面や [tc-chat](https://github.com/tik-choco/tc-chat) の共有映像を、VRChat の動画プレイヤー向けに RTSP で配信 |
| **チャットリレー** | tc-chat のルームに常駐し、後から参加したピアへ署名付きの履歴を再送 |
| **トンネル** | NAT を越える TCP/UDP ポート転送。接続ごとの承認に対応 |
| **AI ネットワーク** | ピアとの LLM 共有（[mistai](https://github.com/tik-choco-lab/mistai) プロトコル）と、ローカルの OpenAI 互換 API |
| **エージェント** | `mistl agent`: mistl とワークスペースを操作できるターミナル AI アシスタント |
| **ダッシュボード** | 上記の機能をすべてブラウザから操作。英語と日本語に対応 |

## 導入

[Releases](https://github.com/tik-choco/mistl/releases/latest) から、
使う環境向けのバイナリを取得する。

| 環境 | アセット |
| --- | --- |
| Windows | `mistl-v<version>-x86_64-pc-windows-msvc.zip` |
| Linux | `mistl-v<version>-x86_64-unknown-linux-gnu.tar.gz` / `aarch64-…` |
| macOS | `mistl-v<version>-aarch64-apple-darwin.tar.gz` / `x86_64-…` |

`mistl install` でユーザーごとの場所へコピーし、ログイン時の自動起動を有効にする。
`mistl update` で最新版に更新できる。

## クイックスタート

`mistl.exe` をダブルクリックするか、引数なしで `mistl` を実行する。
デーモンがバックグラウンドで起動し、ブラウザでダッシュボードが開く。

> [!NOTE]
> ネットワーク接続は**オフ**の状態で起動する。ダッシュボード、トレイアイコン、
> または `mistl network on` で有効にすること。

同じ操作を CLI から行うには:

```console
$ mistl status                          # daemon, stream and AI at a glance
$ mistl store put ./file.bin            # -> CID
$ mistl stream relay --room <room>      # tc-chat share -> rtsp://<LAN IP>:8554/stream
$ mistl tunnel serve 22                 # offer local port 22 to the tunnel room
$ mistl ai serve start                  # OpenAI-compatible API on http://127.0.0.1:6478/v1
$ mistl agent                           # terminal AI agent
```

## ドキュメント

以下のドキュメントは英語。

| トピック | |
| --- | --- |
| [CLI リファレンス](docs/cli.md) | 全コマンド、VRChat リレー、チャットリレー、エスクロー、エージェント |
| [設定](docs/configuration.md) | `config.toml` のキーとデータの保存場所 |
| [AI 設定](docs/ai-config.md) | プロバイダ、ルーム、ローカル API |
| [アイデンティティ](docs/identity.md) | プロフィールドキュメントと DID 委任 |
| [アーキテクチャ](docs/architecture.md) | モジュール、相互運用、ボットの配信、既知の制約 |
| [ランタイム](docs/runtime.md) | dev と stable のインスタンス、接続制御 |
| [ソースからのビルド](docs/building.md) | 必要なもの、ビルド、リリース |

## ソースからのビルド

Rust、git、cmake と、必要に応じて [just](https://github.com/casey/just) を用意する。

```console
$ cp .env.example .env    # selects the mistlib source
$ just release            # fetches mistlib, then cargo build --release
```

ローカルビルドは、インストール済みのリリース版とは分離された **dev** インスタンスとして動く。
詳細は[ソースからのビルド](docs/building.md)を参照。

## ステータス

活発に開発中。リリースにはタグを打っており、ベータ版のタグはプレリリース扱い。

## ライセンス

[Mozilla Public License 2.0](LICENSE)

バイナリビルドは
[Fraunhofer FDK AAC Codec Library for Android](https://github.com/mstorsjo/fdk-aac)
（独自の利用条件に従う。fdk-aac の NOTICE を参照）と
[libopus](https://opus-codec.org/)（BSD-3-Clause）を静的リンクしている。
