# Tunnel Node Graph

Enter a Room ID in the Tunnel panel and click Connect to start a session. The graph shows nodes in the same room and lets you configure connection permissions and TCP/UDP port forwarding.

## Operation

1. Drag from the round port at the right edge of one node to a port on another node. You can also click the ports one after the other to connect them. Drag a node header, or focus the header and use the arrow keys, to move the node.
2. Click a link (or press Enter / Space with it focused) to show its status and a remove button. A link configured on only one side is shown as a dashed line.
3. Add port forwards from `+ Port forward` on each node. A popup opens and asks one question at a time, one per screen. Answers are kept when you go `Back`; press Esc or click × to cancel.
   1. **What do you want to do?** On your own node, choose from these three options. On another node's card, only `Publish a service on this node` and `Connect to a service a peer published` are available.
      - **Use a port on a peer** (`propose`): Choose the peer node (selected automatically if there is only one node in the room; the list always shows the full Node ID), then the address on the peer (for example, `127.0.0.1:22`) and TCP/UDP, then the address on this node that will accept connections (`127.0.0.1:port`). Sending the proposal delivers it to the peer; forwarding does not start until the peer accepts. This also works with nodes that do not support the graph.
      - **Connect to a service a peer published** (`connect`): Choose from the list of published services (the service determines the protocol), then specify the address on this node that will accept connections.
      - **Publish a service on this node** (`serve`): Specify the host:port and TCP/UDP for a service listening on this node.
   The daemon checks while you type whether it can listen on the address for this node. If another program is using it, `Next` stays disabled and an available port is suggested. The daemon also performs the same check when adding the forward and rejects it if the address is unavailable.
   2. Finally, review the details in the form “this node → peer” and confirm with `Add` (`Send proposal` for `propose`).
4. Added port forwards appear in each node card's list. Their status, active connection count, and bytes sent and received update automatically. Click × on a row to remove it; a confirmation dialog appears first. On your own node, you can remove any forward with ×, whether it is graph-managed or in the legacy format. On another node, you can remove only graph-managed forwards, and only if you have permission.
5. A proposal from **Use a port on a peer** appears in your node card as **Waiting for approval** until the peer accepts it, manually or through a proposal rule (a yellow animated pill, `⏳ Waiting for <peer> to accept`, and a row showing `local → peer (remote address)` with the full Node ID). The link between your node and the peer also becomes a yellow animated dashed line, even if there is no explicit link. Click × on the row to cancel the proposal (`tunnel.forward.propose.cancel`). The peer also receives the cancellation, and the proposal disappears from its pending list. The result is always reported when a response arrives. If accepted, the proposal becomes a regular forward row. If denied, unanswered (5 minutes), or the peer leaves, a toast or status row gives the reason. The `Waiting for your decision` card lists only items that require your decision; your own proposals are not included.
6. When you connect to a service published by a peer, the forwarding row on the connecting node shows `⏳ Waiting for <peer> to approve the connection` while the peer waits for approval of that connection. If denied, `✕ <peer> denied the connection` remains for about 2 minutes (see “Approval notices”).
7. If multiple connections to the same forward arrive from the same peer at once, they are grouped into one pending approval, and a single approval applies to all waiting connections. When communication requires approval, the peer node must allow it (by responding to a `propose` or approving a connection to a service). Permission to edit links is separate from approval to connect to a service.

### Proposal rules

Proposal rules automatically approve or deny incoming forward proposals. Manage
them in the tunnel dashboard or with the CLI, even while the tunnel session is
stopped. With no matching rule, the proposal waits for a manual decision.

A rule matches the proposing node, protocol, and the host and port on this node
that the proposal asks to reach. A matching deny always wins over any matching
allow, regardless of rule order. A matching allow only auto-approves a peer with
a verified DID; an unverified peer still needs a manual decision unless a deny
rule matches. Adding a rule also re-evaluates proposals already waiting for a
decision. If automatic approval cannot complete, the proposal stays pending.

By default, a rule matches any peer, protocol, and port, but only loopback
hosts: `localhost`, addresses in `127.0.0.0/8`, or `::1`. A peer can be a
16-character node ID, a verified `did:key`, or `*` for any node. Ports can be a
number, an inclusive range, a comma-separated list of either, or `*`. Hosts can
be `loopback`, a comma-separated list of exact host names or IP addresses, or
`*`. Host list matching is case-insensitive. In an allow rule, `--hosts "*"`
can permit access to non-loopback hosts, including LAN hosts.

```console
$ mistl tunnel rule ls
$ mistl tunnel rule add allow --peer 0123456789abcdef --proto tcp --ports "22,80,8000-8100" --note "SSH and web"
$ mistl tunnel rule add deny --peer 1234567890abcdef --hosts "*"
$ mistl tunnel rule rm r-1a2b3c4d
```

The daemon assigns the rule ID shown by `ls`. The separate trust list governs
connection authorization and is still managed with `mistl tunnel trust`.

### Approval notices (when connecting)

When a service owner puts a connection on hold for approval, the owner sends the connecting node a small notice `{"t":"mistl-tunnel-auth-v1","state":"pending|allowed|denied","forward_key":..,"target":..,"proto":..,"ts":..}`. Waits from the same connecting node, forward, and destination are grouped into one row, so a notice is sent once while pending and once when resolved. This message uses a `t` tag and has no `kind` / `type` / `v`, so existing `P2pPayload`, tc-chat, and mistai handlers, as well as older mistl and `p2p`, ignore it.

- Notices are display-only. The receiver accepts one only if the transport sender is the peer of one of its `connect` forwards and `forward_key` identifies that same forward (`…@<peer>` with a node scope must match the sender; without a scope, the sender must be a peer that could actually be the destination of that forward). A notice from another node cannot put a different forward into “Waiting for approval”.
- State is retained with count and time limits (pending approvals for the owner's 180-second decision window plus a margin; denied notices for 120 seconds) and appears in `tunnel.status` `forwards[]` (`connect`) as `awaiting_approval` / `awaiting_since_ms` / `approval_peer_id` / `denied_at_ms`.
- Proposal cancellation also uses a `t`-tagged message (`mistl-tunnel-propose-cancel-v1`); only the `req_id` of the proposing sender can be cancelled.

### Peer names

If the profile feature (`window.MistlPeers`) is available, node cards, the proposal wizard, and the pending approval table show avatars and display names. Names are self-reported by peers, so pending rows and confirmation screens always show the full Node ID next to the name.

### Tunnel chat removed

Room chat (`tunnel.chat.send`, `mistl tunnel chat`, the dashboard chat tab, and TUI `c`) has been replaced by direct messaging and removed. Messages `{"kind":"chat"}` from older mistl or `p2p` are still received and ignored (`P2pPayload::Chat` remains for parsing). “Trust & history” is a collapsed section below the graph.

Only node positions are saved in the browser. Connection permissions, link edit permissions, edit locks, and forwards added from the graph are saved per room in the daemon data directory at `tunnel/graphs/<room-hash>.json`. Forwards are restored when the session resumes.

## Broadcast and locking

Configure these settings from ⚙ at the top right of your own node card. Other node cards show badges only for settings that are ON.

- **Broadcast ON**: Allows communication with all nodes in the room. Existing forward approvals and trust settings still apply. ON is the default for compatibility.
- **Broadcast OFF**: Allows communication only with peers connected by an explicit link. Both ends must allow the peer. To limit communication to one other node, turn OFF on both ends and connect them with a link.
- **Edit lock**: Rejects changes to links and forward settings from other nodes. Existing communication continues, and the owner can still edit. This is not a password setting.

Broadcast means making the node visible in the room and allowing communication. It does not replicate the same TCP/UDP data to every node. A link represents permission to communicate over the tunnel; it does not represent a change to the physical WebRTC connection or confirm connectivity.

## Permissions

On your own node, open ⚙ → “Connection permissions” to grant each of the following individually for a peer's Node ID. All are denied by default.

- Change links involving this node
- Add forwards (including publishing local services)
- Remove forwards

With permission, you can connect nodes other than the one you are operating. Changes at both ends require link-edit permission from both owners. Only an owner can change Broadcast, the lock, or permissions themselves. The owner's daemon validates requests using the identity of the actual transport sender node.

Changes at each end are confirmed separately, so if one end rejects or disconnects, only part of the change may take effect. An error is shown; inspect the updated links and retry. A missing response is never reported as success.

## Compatibility and verification

The peer must also run a compatible version to share the graph or edit it remotely. Existing `p2p` message formats are preserved, and `kind: graph` has been added for graph messages. An unsupported node is shown as “Graph unsupported or waiting for state”, and remote editing is disabled.

Verification commands:

```text
npm test
cargo test --offline tunnel:: -- --test-threads=1
```

This includes DOM interaction tests and daemon tests for permissions, locks, save failures, cancellation of connections while awaiting approval, room isolation, and data-path control. Rendering in a real browser and communication between multiple real devices require separate verification.
