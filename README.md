# mistl

tik-choco エコシステムの機能を1バイナリに統合した Rust 製デーモン型 CLI。
[tc-storage](../tc-storage) の CLI と [mistlink](../mistlink) の CLI を統合し、
[mistlib](../mistlib-dev)(path 依存)の上に構築されています。

- **identity** — ユーザープロファイルと `did:key` Ed25519 鍵管理(tc-storage 互換)
- **store** — コンテンツアドレス型ストレージ(CIDv1 / sha2-256 / 1 MiB チャンク、mistlib StorageEngine)
- **stream** — 画面共有 RTSP サーバー(VRChat の AVPro ビデオプレイヤーで再生可能、mistlink の Rust 移植)
- **mailbox** — P2P store-and-forward メッセージング(P2P 版メールサーバー)。相手がオフラインなら
  bot ノードがデータを預かり、オンライン復帰時に転送

## 必要環境

- Rust(edition 2024)。`../mistlib-dev` がチェックアウトされていること(path 依存)
- `stream` 機能には PATH 上に [ffmpeg](https://ffmpeg.org/)

## ビルド

```console
$ cargo build --release
```

## 使い方

CLI は常駐デーモンへのクライアントとして動作します(IPC: loopback TCP + トークン認証)。

```console
# デーモン管理
$ mistl daemon start          # バックグラウンド起動
$ mistl daemon status
$ mistl daemon stop

# プロファイル / DID 鍵
$ mistl key did               # 初回呼び出しで鍵を自動生成
$ mistl key list
$ mistl profile set display_name "yourname"
$ mistl profile show

# ストレージ
$ mistl store put .\file.bin  # -> CID を返す
$ mistl store ls
$ mistl store get <cid> --output .\file.bin

# 画面共有 (VRChat)
$ mistl stream start          # -> rtsp://<LAN IP>:8554/stream
$ mistl stream status
$ mistl stream stop

# メールボックス
$ mistl mailbox send <did|node-id> --message "hello"
$ mistl mailbox send <did|node-id> --file .\data.bin
$ mistl mailbox fetch         # 自分宛の保留メッセージを受信
$ mistl mailbox ls            # このノードが bot として預かり中の預託一覧
```

`mailbox send` の結果 `status` は 3 値:

| status | 意味 |
| --- | --- |
| `delivered` | 相手がオンラインで直接配送済み |
| `deposited` | 接続中の bot ノードに預託済み |
| `queued` | 到達可能なピアなし。ローカル outbox に保存し、後で再送 |

## 設定

`%APPDATA%\tik-choco\mistl\config\config.toml`(初回起動時に既定値で生成):

```toml
[identity]
# display_name = "yourname"

[storage]
# blocks_dir = 'D:\mistl-blocks'
capacity_bytes = 10737418240

[stream]
rtsp_url = "rtsp://127.0.0.1:8554/stream"   # 0.0.0.0 にすると LAN 公開
frame_rate = 30
audio_capture = false                        # 未実装(将来対応)

[mailbox]
# room_id = "my-private-room"               # 既定: "mistl-mailbox-v1"
serve_as_bot = true                          # 他人の預託を預かる bot になる
```

データは `%APPDATA%\tik-choco\mistl\data\`(鍵・ブロック・spool・ログ)。

## アーキテクチャ

```
mistl <subcommand>  --(JSON over loopback TCP)-->  mistl daemon run
                                                     ├─ identity  (did:key ed25519, profile)
                                                     ├─ storage   (mistlib StorageEngine + NativeBlockStore)
                                                     ├─ stream    (ffmpeg gdigrab → MPEG-TS → RTP → RTSP server)
                                                     └─ mailbox   (mistlib WebRTC/Nostr + 署名付き envelope spool)
```

- IPC 発見情報は `data\daemon.json`(port + ランダムトークン)。ローカルユーザーのみ接続可能
- tc-storage 互換: DID フォーマット、AES-256-GCM + PBKDF2-SHA256(210k)エンベロープ
  (`identity::crypto`)、CID セマンティクス
- mistlink 互換: RTSP の AVPro 向けダミー SPS/PPS keepalive、PT 96 / SSRC 0x12345678

## 既知の制限(v0.1)

- `mailbox` のファイル送信はエンベロープ(cid/名前/サイズ)のみ転送。ブロック本体の p2p 転送は未実装
- `stream` は映像のみ(audio_capture は無視される)。RTCP(NACK/PLI)未実装
- bot の能力広告プロトコルはなし(接続中ピアはすべて bot 候補として扱う)

## License

[MPL-2.0](LICENSE)
