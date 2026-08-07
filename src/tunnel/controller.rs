//! Forward controller: owns the set of active local port forwards (both
//! `serve` and `connect` direction, TCP and UDP) and the background task
//! driving each one. Ported near-verbatim from the standalone `p2p` crate's
//! `src/controller.rs`; the only seam that changed is the import paths
//! (`crate::X` -> `crate::tunnel::X`, see the integration contract) and the
//! `rtc_manager` type, which is now `crate::tunnel::rtc::RTCManagerHandle`
//! (a room-scoped handle backed by `crate::net`, not a standalone WebRTC
//! mesh owning its own room).
//!
//! [`ForwardStatus`] gets a hand-written [`ForwardStatus::to_json`] rather
//! than a `Serialize` derive: its `spec.direction`/`spec.proto` fields are
//! plain enums (`Direction`, `Proto`) used throughout `src/tunnel/tcp.rs` and
//! `src/tunnel/udp.rs` for pattern matching, and `ForwardState::Error(String)`
//! needs custom shaping into a flat `state`/`error` pair rather than serde's
//! default externally-tagged encoding -- adding derives there would ripple
//! into files this worker doesn't own. See the doc comment on
//! [`ForwardStatus::to_json`] for the exact JSON shape.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::error;

use crate::tunnel::auth::{SharedAuthorizer, allow_all};
use crate::tunnel::forward_runtime::{ForwardRuntime, PeerMetrics};
use crate::tunnel::rtc::RTCManager;
use crate::tunnel::{tcp, udp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Serve,
    Connect,
}

impl Direction {
    /// `"serve"` / `"connect"` -- matches [`crate::tunnel::forward_store::PersistedForward::direction`]
    /// and the `tunnel.status` wire shape.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Serve => "serve",
            Self::Connect => "connect",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proto {
    Tcp,
    Udp,
}

impl Proto {
    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            _ => Err(anyhow!("unsupported protocol: {}", name)),
        }
    }

    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardSpec {
    pub direction: Direction,
    pub proto: Proto,
    pub addr: String,
    pub listen_port: i32,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardState {
    Listening,
    Error(String),
    #[allow(dead_code)]
    Stopped,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardStatus {
    pub key: String,
    pub spec: ForwardSpec,
    pub active_conns: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub state: ForwardState,
    pub peers: Vec<PeerMetrics>,
}

impl ForwardStatus {
    /// Renders this status as the `tunnel.status` wire shape used by the
    /// dashboard (`src/web/assets/index.html`) and the TUI. Chosen as a
    /// hand-written method rather than a `Serialize` derive -- see the
    /// module doc comment for why.
    ///
    /// Shape:
    /// ```json
    /// {
    ///   "key": "tcp:80",
    ///   "direction": "serve" | "connect",
    ///   "proto": "tcp" | "udp",
    ///   "addr": "127.0.0.1:80",
    ///   "listen_port": -1,
    ///   "target": "tcp:80",
    ///   "active_conns": 0,
    ///   "bytes_in": 0,
    ///   "bytes_out": 0,
    ///   "state": "listening" | "stopped" | "error",
    ///   "error": null | "message",
    ///   "peers": [{"peer_id": "..", "active_conns": 0, "bytes_in": 0, "bytes_out": 0}]
    /// }
    /// ```
    pub fn to_json(&self) -> Value {
        let (state, error) = match &self.state {
            ForwardState::Listening => ("listening", None),
            ForwardState::Stopped => ("stopped", None),
            ForwardState::Error(msg) => ("error", Some(msg.clone())),
        };
        json!({
            "key": self.key,
            "direction": self.spec.direction.as_str(),
            "proto": self.spec.proto.as_str(),
            "addr": self.spec.addr,
            "listen_port": self.spec.listen_port,
            "target": self.spec.target,
            "active_conns": self.active_conns,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
            "state": state,
            "error": error,
            "peers": self.peers.iter().map(PeerMetrics::to_json).collect::<Vec<_>>(),
        })
    }
}

#[allow(dead_code)]
struct ForwardHandle {
    spec: ForwardSpec,
    runtime: ForwardRuntime,
    state: ForwardState,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct ForwardController {
    rtc_manager: Option<RTCManager>,
    authorizer: SharedAuthorizer,
    forwards: Arc<RwLock<HashMap<String, ForwardHandle>>>,
}

impl ForwardController {
    // Ported convenience constructor; current call sites all go through
    // `with_authorizer` (or `new_inert` in tests) directly, so this is
    // presently unreachable but kept for upstream-signature parity.
    #[allow(dead_code)]
    pub fn new(rtc_manager: RTCManager) -> Self {
        Self::with_authorizer(rtc_manager, allow_all())
    }

    pub fn with_authorizer(rtc_manager: RTCManager, authorizer: SharedAuthorizer) -> Self {
        Self {
            rtc_manager: Some(rtc_manager),
            authorizer,
            forwards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_inert() -> Self {
        Self {
            rtc_manager: None,
            authorizer: allow_all(),
            forwards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_forward(&self, spec: ForwardSpec) -> Result<String> {
        let key = spec.target.clone();
        if key.is_empty() {
            return Err(anyhow!("forward target must not be empty"));
        }

        let mut forwards = self.forwards.write().await;
        if forwards.contains_key(&key) {
            return Err(anyhow!("forward already exists: {}", key));
        }
        let runtime = ForwardRuntime::new();
        forwards.insert(
            key.clone(),
            ForwardHandle {
                spec: spec.clone(),
                runtime: runtime.clone(),
                state: ForwardState::Listening,
                task: None,
            },
        );
        drop(forwards);

        let task = self.spawn_forward(spec, key.clone(), runtime).await;
        if let Some(task) = task
            && let Some(handle) = self.forwards.write().await.get_mut(&key)
        {
            handle.task = Some(task);
        }
        Ok(key)
    }

    #[allow(dead_code)]
    pub async fn remove_forward(&self, key: &str) -> Result<()> {
        let Some(handle) = self.forwards.write().await.remove(key) else {
            return Err(anyhow!("forward not found: {}", key));
        };

        handle.runtime.cancel();
        if let Some(task) = handle.task {
            task.abort();
        }
        if handle.spec.direction == Direction::Serve
            && let Some(manager) = &self.rtc_manager
        {
            manager.unpublish_tunnel_target(key).await;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn list_forwards(&self) -> Vec<ForwardStatus> {
        let forwards = self.forwards.read().await;
        let mut statuses = forwards
            .iter()
            .map(|(key, handle)| {
                let metrics = handle.runtime.metrics();
                ForwardStatus {
                    key: key.clone(),
                    spec: handle.spec.clone(),
                    active_conns: metrics.active_conns,
                    bytes_in: metrics.bytes_in,
                    bytes_out: metrics.bytes_out,
                    state: handle.state.clone(),
                    peers: handle.runtime.peer_metrics(),
                }
            })
            .collect::<Vec<_>>();
        statuses.sort_by(|a, b| a.key.cmp(&b.key));
        statuses
    }

    async fn spawn_forward(
        &self,
        spec: ForwardSpec,
        key: String,
        runtime: ForwardRuntime,
    ) -> Option<JoinHandle<()>> {
        let manager = self.rtc_manager.clone()?;
        let authorizer = self.authorizer.clone();
        let state = self.forwards.clone();

        Some(tokio::spawn(async move {
            // Only Serve forwards publish/serve the tunnel target: a
            // non-empty remote-addr param makes `tcp`/`udp`'s
            // `listen_and_serve_with_target_and_auth` call
            // `publish_tunnel_target`, advertising this node as able to
            // serve `spec.target`. For Connect forwards, `spec.addr` holds
            // the *local* listen address (display-only, see
            // `web::state::local_endpoint`) -- passing it through here would
            // make the requester wrongly advertise itself as a server for
            // the target it's merely connecting to. Pass an empty string
            // instead so only the actual Serve side ever publishes.
            let remote_addr = match spec.direction {
                Direction::Serve => spec.addr.clone(),
                Direction::Connect => String::new(),
            };
            let result = match spec.proto {
                Proto::Tcp => {
                    tcp::TcpManager::listen_and_serve_with_target_and_auth(
                        manager,
                        spec.listen_port,
                        remote_addr,
                        spec.target.clone(),
                        runtime,
                        authorizer,
                    )
                    .await
                }
                Proto::Udp => {
                    udp::UdpManager::listen_and_serve_with_target_and_auth(
                        manager,
                        spec.listen_port,
                        remote_addr,
                        spec.target.clone(),
                        runtime,
                        authorizer,
                    )
                    .await
                }
            };

            if let Err(err) = result {
                error!("forward {} failed: {}", key, err);
                if let Some(handle) = state.write().await.get_mut(&key) {
                    handle.state = ForwardState::Error(err.to_string());
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests;
