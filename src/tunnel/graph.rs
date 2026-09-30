//! Room-scoped graph control plane. Transport identity is the actor; wire
//! messages cannot choose an owner or grant themselves permissions.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock, oneshot};

use super::controller::{Direction, ForwardSpec, Proto};
use super::forward_store::PersistedForward;
use super::session::SessionContext;

struct GraphAuthorizer {
    graph: Arc<GraphState>,
    inner: super::auth::SharedAuthorizer,
}

impl super::auth::ConnectionAuthorizer for GraphAuthorizer {
    fn authorize<'a>(
        &'a self,
        request: &'a super::auth::AuthRequest,
    ) -> super::auth::AuthFuture<'a> {
        Box::pin(async move {
            let epoch = self.graph.epoch.load(Ordering::Acquire);
            if !self.graph.allows_traffic(&request.peer_id).await {
                return super::auth::AuthDecision::Deny;
            }
            let decision = self.inner.authorize(request).await;
            // A human may approve after a link was removed or the room changed.
            if self.graph.epoch.load(Ordering::Acquire) == epoch
                && self.graph.allows_traffic(&request.peer_id).await
            {
                decision
            } else {
                super::auth::AuthDecision::Deny
            }
        })
    }
}

pub fn authorizer(
    graph: Arc<GraphState>,
    inner: super::auth::SharedAuthorizer,
) -> super::auth::SharedAuthorizer {
    Arc::new(GraphAuthorizer { graph, inner })
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Permissions {
    pub edit_links: bool,
    pub add_forwards: bool,
    pub remove_forwards: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub broadcast: bool,
    pub locked: bool,
    pub links: BTreeSet<String>,
    pub permissions: BTreeMap<String, Permissions>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            broadcast: true,
            locked: false,
            links: BTreeSet::new(),
            permissions: BTreeMap::new(),
        }
    }
}

impl Policy {
    pub fn allows_traffic(&self, peer: &str) -> bool {
        self.broadcast || self.links.contains(peer)
    }

    fn authorize(&self, actor: &str, owner: &str, action: &Action) -> Result<()> {
        if actor == owner {
            return Ok(());
        }
        ensure!(!self.locked, "node is locked by its owner");
        let grants = self.permissions.get(actor).cloned().unwrap_or_default();
        let allowed = match action {
            Action::Link { .. } => grants.edit_links,
            Action::AddForward { .. } => grants.add_forwards,
            Action::RemoveForward { .. } => grants.remove_forwards,
            Action::Broadcast { .. } | Action::Lock { .. } | Action::Permission { .. } => false,
        };
        ensure!(
            allowed,
            "node owner has not granted permission for this operation"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Link {
        peer_id: String,
        connected: bool,
    },
    Broadcast {
        enabled: bool,
    },
    Lock {
        locked: bool,
    },
    Permission {
        peer_id: String,
        permissions: Permissions,
    },
    AddForward {
        direction: String,
        proto: String,
        addr: String,
        target: String,
    },
    RemoveForward {
        target: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeView {
    pub broadcast: bool,
    pub locked: bool,
    pub links: BTreeSet<String>,
    /// Only the recipient's grants are disclosed over the network.
    pub permissions: Permissions,
    pub forwards: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    State { node: NodeView },
    Command { id: String, action: Action },
    Reply { id: String, error: Option<String> },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Saved {
    policy: Policy,
    forwards: Vec<PersistedForward>,
}

pub(crate) struct PreparedRoom(PathBuf, Saved);

type ReplyWaiter = (String, oneshot::Sender<std::result::Result<(), String>>);

#[derive(Default)]
pub struct GraphState {
    pub mutation: Mutex<()>,
    pub suspended: AtomicBool,
    pub closed: AtomicBool,
    epoch: AtomicU64,
    saved: RwLock<Saved>,
    path: RwLock<Option<PathBuf>>,
    remote: RwLock<BTreeMap<String, NodeView>>,
    inbox: Mutex<VecDeque<(String, String, Action)>>,
    waiters: Mutex<BTreeMap<String, ReplyWaiter>>,
    tick: Mutex<u64>,
}

impl GraphState {
    pub async fn allows_traffic(&self, peer: &str) -> bool {
        !self.suspended.load(Ordering::Acquire)
            && !self.closed.load(Ordering::Acquire)
            && self.saved.read().await.policy.allows_traffic(peer)
    }

    pub async fn receive(&self, peer: String, message: Message) {
        match message {
            Message::State { node } => {
                self.remote.write().await.insert(peer, node);
            }
            Message::Command { id, action } => {
                let mut inbox = self.inbox.lock().await;
                if inbox.len() < 256 && id.len() <= 128 {
                    inbox.push_back((peer, id, action));
                }
            }
            Message::Reply { id, error } => {
                let mut waiters = self.waiters.lock().await;
                if waiters
                    .get(&id)
                    .is_some_and(|(expected, _)| expected == &peer)
                    && let Some((_, tx)) = waiters.remove(&id)
                {
                    let _ = tx.send(error.map_or(Ok(()), Err));
                }
            }
        }
    }

    pub(crate) async fn prepare_room(room: &str) -> Result<PreparedRoom> {
        let key = hex::encode(Sha256::digest(room.as_bytes()));
        let path = crate::config::data_dir()?
            .join("tunnel/graphs")
            .join(format!("{key}.json"));
        let saved = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Saved::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(PreparedRoom(path, saved))
    }

    pub async fn load_room(&self, room: &str) -> Result<()> {
        self.install_room(Self::prepare_room(room).await?).await;
        Ok(())
    }

    pub(crate) async fn install_room(&self, PreparedRoom(path, saved): PreparedRoom) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        *self.path.write().await = Some(path);
        *self.saved.write().await = saved;
        self.remote.write().await.clear();
        self.inbox.lock().await.clear();
        // Dropping senders cancels requests from the previous room.
        self.waiters.lock().await.clear();
    }

    pub async fn forget_peer(&self, peer: &str) {
        self.remote.write().await.remove(peer);
        self.waiters.lock().await.retain(|_, (id, _)| id != peer);
    }

    async fn persist(&self, next: &Saved) -> Result<()> {
        if let Some(path) = self.path.read().await.as_ref() {
            tokio::fs::create_dir_all(path.parent().unwrap()).await?;
            let temporary = path.with_extension("json.tmp");
            tokio::fs::write(&temporary, serde_json::to_vec_pretty(next)?).await?;
            tokio::fs::rename(&temporary, path).await?;
        }
        *self.saved.write().await = next.clone();
        Ok(())
    }
}

fn validate_peer(peer: &str, owner: &str) -> Result<()> {
    ensure!(
        !peer.trim().is_empty() && peer.len() <= 256 && peer != owner && !peer.contains('@'),
        "invalid peer id"
    );
    Ok(())
}

fn forward_spec(
    direction: &str,
    proto: &str,
    addr: &str,
    target: &str,
    owner: &str,
) -> Result<ForwardSpec> {
    let proto = Proto::from_name(proto)?;
    ensure!(
        addr.len() <= 512 && !addr.chars().any(char::is_whitespace),
        "invalid host:port"
    );
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("address must be host:port"))?;
    let port: u16 = port.parse()?;
    ensure!(port > 0 && !host.is_empty(), "port must be 1–65535");
    let (direction, listen_port, target) = match direction {
        "serve" => (
            Direction::Serve,
            -1,
            format!("graph:{}@{owner}", uuid::Uuid::new_v4()),
        ),
        "connect" => {
            ensure!(
                host == "127.0.0.1",
                "local listeners use 127.0.0.1; enter 127.0.0.1:port"
            );
            ensure!(
                !target.is_empty() && target.len() <= 1024,
                "select a published service"
            );
            (Direction::Connect, i32::from(port), target.to_string())
        }
        _ => bail!("direction must be serve or connect"),
    };
    Ok(ForwardSpec {
        direction,
        proto,
        addr: addr.into(),
        listen_port,
        target,
    })
}

impl SessionContext {
    pub async fn graph_snapshot(&self) -> Value {
        let graph = self.manager.graph();
        let peers = self.manager.connected_peers().await;
        let remote = graph.remote.read().await;
        json!({
            "policy": graph.saved.read().await.policy,
            "nodes": peers.iter().map(|id| json!({"id": id, "node": remote.get(id)})).collect::<Vec<_>>()
        })
    }

    pub async fn restore_graph_forwards(&self) {
        for entry in self.manager.graph().saved.read().await.forwards.clone() {
            if let Ok(spec) = entry.to_spec()
                && let Err(error) = self.controller.add_forward(spec).await
            {
                tracing::warn!(%error, "restoring graph forward");
            }
        }
    }

    pub async fn graph_remove_room_forwards(&self) {
        for entry in self.manager.graph().saved.read().await.forwards.clone() {
            let _ = self.controller.remove_forward(&entry.target).await;
        }
    }

    /// Caller holds graph.mutation; authorization is checked against live policy.
    async fn graph_apply(&self, actor: &str, action: Action) -> Result<()> {
        let graph = self.manager.graph();
        let owner = self.manager.self_id();
        let mut next = graph.saved.read().await.clone();
        let previous_policy = next.policy.clone();
        next.policy.authorize(actor, owner, &action)?;
        match action {
            Action::Link { peer_id, connected } => {
                validate_peer(&peer_id, owner)?;
                if connected {
                    next.policy.links.insert(peer_id);
                } else {
                    next.policy.links.remove(&peer_id);
                }
            }
            Action::Broadcast { enabled } => next.policy.broadcast = enabled,
            Action::Lock { locked } => next.policy.locked = locked,
            Action::Permission {
                peer_id,
                permissions,
            } => {
                validate_peer(&peer_id, owner)?;
                next.policy.permissions.insert(peer_id, permissions);
            }
            Action::AddForward {
                direction,
                proto,
                addr,
                target,
            } => {
                let spec = forward_spec(&direction, &proto, &addr, &target, owner)?;
                // A remote actor with the add-forwards grant must not be able to aim
                // a serve forward at link-local targets (e.g. cloud metadata).
                ensure!(
                    actor == owner
                        || spec.direction != Direction::Serve
                        || !super::forward_args::is_link_local_host(&spec.addr),
                    "link-local targets cannot be proposed remotely"
                );
                if spec.direction == Direction::Connect {
                    let (_, peer) = super::forward_args::split_node_scope(&target);
                    let peer = peer.ok_or_else(|| {
                        anyhow::anyhow!("direct forwards require a node-scoped target")
                    })?;
                    ensure!(next.policy.allows_traffic(peer), "connect the nodes first");
                    let remote = graph.remote.read().await;
                    let node = remote
                        .get(peer)
                        .ok_or_else(|| anyhow::anyhow!("peer graph is unavailable"))?;
                    ensure!(
                        node.broadcast || node.links.contains(owner),
                        "peer must allow this connection first"
                    );
                    ensure!(
                        node.forwards.iter().any(|f| f["target"] == target
                            && f["proto"] == proto
                            && f["direction"] == "serve"),
                        "published service is no longer available"
                    );
                }
                // Refuse a duplicate listener before changing persisted state.
                ensure!(
                    !self
                        .controller
                        .list_forwards()
                        .await
                        .iter()
                        .any(|f| spec.listen_port > 0
                            && f.spec.proto == spec.proto
                            && f.spec.listen_port == spec.listen_port),
                    "local port already configured"
                );
                self.controller.add_forward(spec.clone()).await?;
                next.forwards.push(PersistedForward::from_spec(&spec));
                if let Err(error) = graph.persist(&next).await {
                    let _ = self.controller.remove_forward(&spec.target).await;
                    return Err(error);
                }
                return Ok(());
            }
            Action::RemoveForward { target } => {
                ensure!(
                    next.forwards.iter().any(|f| f.target == target),
                    "only graph-managed forwards can be removed here; use the existing forward list for legacy rules"
                );
                let old = next.clone();
                next.forwards.retain(|f| f.target != target);
                graph.persist(&next).await?;
                if let Err(error) = self.controller.remove_forward(&target).await {
                    graph.persist(&old).await?;
                    return Err(error);
                }
                return Ok(());
            }
        }
        graph.persist(&next).await?;
        for peer in self.manager.connected_peers().await {
            let allowed = next.policy.allows_traffic(&peer);
            if previous_policy.allows_traffic(&peer) != allowed {
                self.manager.graph_connection_changed(&peer, allowed).await;
            }
        }
        Ok(())
    }

    pub async fn graph_command(
        &self,
        node_id: &str,
        action: Action,
        expected_room: &str,
    ) -> Result<Value> {
        let graph = self.manager.graph();
        let guard = graph.mutation.lock().await;
        ensure!(
            !graph.closed.load(Ordering::Acquire),
            "tunnel session stopped"
        );
        ensure!(
            self.manager.current_room().await == expected_room,
            "room changed; refresh before editing"
        );
        if node_id == self.manager.self_id() {
            self.graph_apply(self.manager.self_id(), action).await?;
            drop(guard);
        } else {
            ensure!(
                self.manager
                    .connected_peers()
                    .await
                    .iter()
                    .any(|p| p == node_id),
                "peer is offline"
            );
            ensure!(
                graph.remote.read().await.contains_key(node_id),
                "peer does not support graph editing"
            );
            let id = uuid::Uuid::new_v4().to_string();
            let (tx, rx) = oneshot::channel();
            graph
                .waiters
                .lock()
                .await
                .insert(id.clone(), (node_id.into(), tx));
            let sent = self
                .manager
                .send_graph(
                    node_id,
                    Message::Command {
                        id: id.clone(),
                        action,
                    },
                )
                .await;
            if let Err(error) = sent {
                graph.waiters.lock().await.remove(&id);
                return Err(error);
            }
            drop(guard);
            let result = tokio::time::timeout(std::time::Duration::from_secs(10), rx).await;
            graph.waiters.lock().await.remove(&id);
            match result {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => bail!("{error}"),
                _ => bail!("peer did not confirm the change; refresh the graph before retrying"),
            }
        }
        self.graph_publish().await;
        Ok(json!({"applied": true}))
    }

    pub async fn graph_tick(&self) {
        let graph = self.manager.graph();
        let _guard = graph.mutation.lock().await;
        let commands = std::mem::take(&mut *graph.inbox.lock().await);
        let changed = !commands.is_empty();
        for (peer, id, action) in commands {
            if !self.manager.connected_peers().await.contains(&peer) {
                continue;
            }
            let error = self
                .graph_apply(&peer, action)
                .await
                .err()
                .map(|e| e.to_string());
            let _ = self
                .manager
                .send_graph(&peer, Message::Reply { id, error })
                .await;
        }
        let mut tick = graph.tick.lock().await;
        *tick += 1;
        if changed || *tick % 4 == 1 {
            self.graph_publish().await;
        }
    }

    async fn graph_publish(&self) {
        let graph = self.manager.graph();
        let saved = graph.saved.read().await.clone();
        let forwards: Vec<_> = self
            .controller
            .list_forwards()
            .await
            .iter()
            .map(|f| {
                let mut value = f.to_json();
                value["graph_managed"] =
                    json!(saved.forwards.iter().any(|s| s.target == f.spec.target));
                value
            })
            .collect();
        for peer in self.manager.connected_peers().await {
            let node = NodeView {
                broadcast: saved.policy.broadcast,
                locked: saved.policy.locked,
                links: saved.policy.links.clone(),
                permissions: saved
                    .policy
                    .permissions
                    .get(&peer)
                    .cloned()
                    .unwrap_or_default(),
                forwards: forwards.clone(),
            };
            let _ = self
                .manager
                .send_graph(&peer, Message::State { node })
                .await;
        }
    }

    pub async fn graph_managed_targets(&self) -> Vec<String> {
        self.manager
            .graph()
            .saved
            .read()
            .await
            .forwards
            .iter()
            .map(|f| f.target.clone())
            .collect()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) async fn context() -> (SessionContext, PathBuf) {
        use super::super::{
            auth::*, controller::ForwardController, forward_store::ForwardStore,
            negotiation::ForwardNegotiator, rtc::RTCManager,
        };
        let dir = std::env::temp_dir().join(format!("mistl-graph-test-{}", uuid::Uuid::new_v4()));
        let ctx = SessionContext {
            room: Arc::new(Mutex::new("test-room".into())),
            manager: RTCManager::for_test("self"),
            controller: ForwardController::new_inert(),
            trust_store: TrustStore::load(dir.join("trust.json")).await.unwrap(),
            audit_log: AuthAuditLog::default(),
            pending_auth: PendingAuthorizations::new(),
            negotiator: ForwardNegotiator::new(),
            forward_store: ForwardStore::load(dir.join("forwards.json")).await.unwrap(),
            notices: Arc::new(Mutex::new(Vec::new())),
            chat_log: Arc::new(Mutex::new(Vec::new())),
        };
        *ctx.manager.graph().path.write().await = Some(dir.join("graph.json"));
        (ctx, dir)
    }

    #[tokio::test]
    async fn commands_enforce_grants_lock_room_and_persistence() {
        let (ctx, dir) = context().await;
        let link = Action::Link {
            peer_id: "other".into(),
            connected: true,
        };
        assert!(ctx.graph_apply("editor", link.clone()).await.is_err());
        ctx.graph_command(
            "self",
            Action::Permission {
                peer_id: "editor".into(),
                permissions: Permissions {
                    edit_links: true,
                    ..Default::default()
                },
            },
            "test-room",
        )
        .await
        .unwrap();
        ctx.graph_apply("editor", link.clone()).await.unwrap();
        let saved: Saved =
            serde_json::from_slice(&tokio::fs::read(dir.join("graph.json")).await.unwrap())
                .unwrap();
        assert!(saved.policy.links.contains("other"));
        ctx.graph_command("self", Action::Lock { locked: true }, "test-room")
            .await
            .unwrap();
        assert!(
            ctx.graph_apply(
                "editor",
                Action::Link {
                    peer_id: "other".into(),
                    connected: false
                }
            )
            .await
            .is_err()
        );
        assert!(
            ctx.graph_command("self", Action::Lock { locked: false }, "old-room")
                .await
                .is_err()
        );
        assert!(ctx.manager.graph().saved.read().await.policy.locked);
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    #[tokio::test]
    async fn forwards_restore_and_remove_through_existing_api() {
        let (ctx, dir) = context().await;
        let add = Action::AddForward {
            direction: "serve".into(),
            proto: "tcp".into(),
            addr: "127.0.0.1:8080".into(),
            target: String::new(),
        };
        assert!(ctx.graph_apply("stranger", add.clone()).await.is_err());
        ctx.graph_command("self", add, "test-room").await.unwrap();
        let key = ctx.controller.list_forwards().await[0].key.clone();
        assert!(key.ends_with("@self"));
        ctx.graph_remove_room_forwards().await;
        assert!(ctx.controller.list_forwards().await.is_empty());
        ctx.restore_graph_forwards().await;
        assert_eq!(ctx.controller.list_forwards().await.len(), 1);
        ctx.remove_forward(&key).await.unwrap();
        ctx.restore_graph_forwards().await;
        assert!(ctx.controller.list_forwards().await.is_empty());
        let saved: Saved =
            serde_json::from_slice(&tokio::fs::read(dir.join("graph.json")).await.unwrap())
                .unwrap();
        assert!(saved.forwards.is_empty());
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    #[tokio::test]
    async fn write_failure_does_not_apply_policy_or_leave_forward_running() {
        let (ctx, dir) = context().await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let blocker = dir.join("file");
        tokio::fs::write(&blocker, b"not a directory")
            .await
            .unwrap();
        *ctx.manager.graph().path.write().await = Some(blocker.join("graph.json"));
        assert!(
            ctx.graph_command("self", Action::Broadcast { enabled: false }, "test-room")
                .await
                .is_err()
        );
        assert!(ctx.manager.graph().allows_traffic("any").await);
        assert!(
            ctx.graph_command(
                "self",
                Action::AddForward {
                    direction: "serve".into(),
                    proto: "tcp".into(),
                    addr: "localhost:80".into(),
                    target: String::new()
                },
                "test-room"
            )
            .await
            .is_err()
        );
        assert!(ctx.controller.list_forwards().await.is_empty());
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    #[tokio::test]
    async fn loading_another_room_clears_grants_views_and_waiters() {
        let graph = GraphState::default();
        graph.saved.write().await.policy.permissions.insert(
            "peer".into(),
            Permissions {
                edit_links: true,
                ..Default::default()
            },
        );
        let (tx, rx) = oneshot::channel();
        graph
            .waiters
            .lock()
            .await
            .insert("id".into(), ("peer".into(), tx));
        graph
            .install_room(PreparedRoom(
                PathBuf::from("unused-test-path"),
                Saved::default(),
            ))
            .await;
        assert!(graph.saved.read().await.policy.permissions.is_empty());
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn link_revocation_while_approval_is_pending_denies_the_connection() {
        use super::super::auth::*;
        let graph = Arc::new(GraphState::default());
        let (ctx, dir) = context().await;
        let pending = ctx.pending_auth.clone();
        let inner = PendingAuthorizer::shared(ctx.trust_store.clone(), pending.clone());
        let gated = authorizer(graph.clone(), inner);
        let task = tokio::spawn(async move {
            gated
                .authorize(&AuthRequest {
                    peer_id: "peer".into(),
                    forward_key: "service".into(),
                    target_addr: "localhost:80".into(),
                    proto: "tcp".into(),
                })
                .await
        });
        let id = loop {
            if let Some(request) = pending.list().await.first() {
                break request.id;
            }
            tokio::task::yield_now().await;
        };
        graph.saved.write().await.policy.broadcast = false;
        pending.resolve(id, AuthDecision::Allow).await;
        assert_eq!(task.await.unwrap(), AuthDecision::Deny);
        if dir.exists() {
            tokio::fs::remove_dir_all(dir).await.unwrap();
        }
    }

    #[test]
    fn permission_and_lock_are_enforced_at_owner() {
        let mut policy = Policy::default();
        let link = Action::Link {
            peer_id: "b".into(),
            connected: true,
        };
        assert!(policy.authorize("a", "owner", &link).is_err());
        policy.permissions.insert(
            "a".into(),
            Permissions {
                edit_links: true,
                ..Default::default()
            },
        );
        assert!(policy.authorize("a", "owner", &link).is_ok());
        assert!(
            policy
                .authorize("a", "owner", &Action::Lock { locked: false })
                .is_err()
        );
        assert!(
            policy
                .authorize("a", "owner", &Action::RemoveForward { target: "x".into() })
                .is_err()
        );
        policy.locked = true;
        assert!(policy.authorize("a", "owner", &link).is_err());
        assert!(policy.authorize("owner", "owner", &link).is_ok());
    }

    #[test]
    fn broadcast_and_explicit_links_control_traffic() {
        let mut policy = Policy::default();
        assert!(policy.allows_traffic("a"));
        policy.broadcast = false;
        policy.links.insert("b".into());
        assert!(!policy.allows_traffic("a"));
        assert!(policy.allows_traffic("b"));
        policy.links.remove("b");
        assert!(!policy.allows_traffic("b"));
    }

    #[test]
    fn listeners_and_ports_are_validated() {
        for addr in [
            "127.0.0.1:0",
            "127.0.0.1:65536",
            "0.0.0.0:80",
            "localhost:80",
            "127.0.0.1:-1",
        ] {
            assert!(forward_spec("connect", "tcp", addr, "x@peer", "self").is_err());
        }
        assert!(forward_spec("connect", "tcp", "127.0.0.1:8080", "x@peer", "self").is_ok());
        assert!(forward_spec("serve", "udp", "localhost:9000", "", "self").is_ok());
    }

    #[tokio::test]
    async fn replies_are_bound_to_transport_sender() {
        let graph = GraphState::default();
        let (tx, mut rx) = oneshot::channel();
        graph
            .waiters
            .lock()
            .await
            .insert("id".into(), ("expected".into(), tx));
        graph
            .receive(
                "intruder".into(),
                Message::Reply {
                    id: "id".into(),
                    error: None,
                },
            )
            .await;
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        graph
            .receive(
                "expected".into(),
                Message::Reply {
                    id: "id".into(),
                    error: None,
                },
            )
            .await;
        assert!(rx.await.unwrap().is_ok());
    }
}
