//! Persists approved forward definitions to disk so they are re-established
//! the next time the daemon starts (or the tunnel is (re)started). Ported
//! from `p2p/src/forward_store.rs`; the only change is
//! [`default_forward_store_path`], which used to resolve `%APPDATA%\p2p` (or
//! `$P2P_CONFIG_DIR`) and now resolves `crate::config::data_dir()/tunnel/`
//! -- see that function's doc comment. `ForwardStore::load` itself still
//! takes an explicit path, so callers (and the ported tests, which each use
//! their own temp-dir path) are unaffected.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::tunnel::controller::{Direction, ForwardSpec, Proto};

/// A persisted forward definition. Stored with primitive fields so the
/// controller enums don't need serde derives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedForward {
    /// "serve" or "connect".
    pub direction: String,
    /// "tcp" or "udp".
    pub proto: String,
    pub addr: String,
    pub listen_port: i32,
    pub target: String,
}

impl PersistedForward {
    pub fn from_spec(spec: &ForwardSpec) -> Self {
        let direction = match spec.direction {
            Direction::Serve => "serve",
            Direction::Connect => "connect",
        };
        Self {
            direction: direction.to_string(),
            proto: spec.proto.as_str().to_string(),
            addr: spec.addr.clone(),
            listen_port: spec.listen_port,
            target: spec.target.clone(),
        }
    }

    pub fn to_spec(&self) -> Result<ForwardSpec> {
        let direction = match self.direction.as_str() {
            "serve" => Direction::Serve,
            "connect" => Direction::Connect,
            other => anyhow::bail!("unknown direction: {}", other),
        };
        Ok(ForwardSpec {
            direction,
            proto: Proto::from_name(&self.proto)?,
            addr: self.addr.clone(),
            listen_port: self.listen_port,
            target: self.target.clone(),
        })
    }
}

/// Persists approved forward definitions so they are re-established on the next
/// launch. Keyed by target so each forward appears once.
#[derive(Debug, Clone)]
pub struct ForwardStore {
    path: PathBuf,
    entries: Arc<RwLock<Vec<PersistedForward>>>,
}

impl ForwardStore {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let entries = match tokio::fs::read_to_string(&path).await {
            Ok(text) => serde_json::from_str::<Vec<PersistedForward>>(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            entries: Arc::new(RwLock::new(entries)),
        })
    }

    pub async fn list(&self) -> Vec<PersistedForward> {
        self.entries.read().await.clone()
    }

    pub async fn add(&self, spec: &ForwardSpec) -> Result<()> {
        let entry = PersistedForward::from_spec(spec);
        {
            let mut entries = self.entries.write().await;
            entries.retain(|e| e.target != entry.target);
            entries.push(entry);
        }
        self.persist().await
    }

    pub async fn remove(&self, target: &str) -> Result<bool> {
        let removed = {
            let mut entries = self.entries.write().await;
            let before = entries.len();
            entries.retain(|e| e.target != target);
            entries.len() != before
        };
        if removed {
            self.persist().await?;
        }
        Ok(removed)
    }

    async fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let text = serde_json::to_string_pretty(&*self.entries.read().await)?;
        tokio::fs::write(&self.path, text).await?;
        Ok(())
    }
}

/// Default location for the persisted forward list:
/// `crate::config::data_dir()/tunnel/forwards.json`. Upstream `p2p` resolved
/// `$P2P_CONFIG_DIR` (falling back to `%APPDATA%`/`$HOME/.config`) and joined
/// `p2p/forwards.json`; mistl instead reuses the daemon's own per-user data
/// directory so tunnel state lives alongside the rest of mistl's state
/// (keys, blocks, mailbox spool) rather than in a separate app folder.
///
/// Kept infallible (`-> PathBuf`, not `-> Result<PathBuf>`) so callers don't
/// change: if `data_dir()` fails (e.g. the platform's home directory can't
/// be resolved), this falls back to a relative `./tunnel/forwards.json` and
/// logs a warning rather than propagating the error, matching upstream's
/// own "always returns something" contract for this helper.
pub fn default_forward_store_path() -> PathBuf {
    match crate::config::data_dir() {
        Ok(dir) => dir.join("tunnel").join("forwards.json"),
        Err(err) => {
            tracing::warn!(
                "could not resolve data dir for forward store ({}); using ./tunnel/forwards.json",
                err
            );
            PathBuf::from("./tunnel").join("forwards.json")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ForwardSpec {
        ForwardSpec {
            direction: Direction::Connect,
            proto: Proto::Tcp,
            addr: String::new(),
            listen_port: 8080,
            target: "tcp:127.0.0.1:80".into(),
        }
    }

    #[tokio::test]
    async fn round_trips_specs_through_disk() {
        let dir = std::env::temp_dir().join(format!("mistl-tunnel-fwd-{}", uuid::Uuid::new_v4()));
        let path = dir.join("forwards.json");

        let store = ForwardStore::load(&path).await.unwrap();
        store.add(&spec()).await.unwrap();

        let reloaded = ForwardStore::load(&path).await.unwrap();
        let entries = reloaded.list().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].to_spec().unwrap(), spec());

        assert!(reloaded.remove("tcp:127.0.0.1:80").await.unwrap());
        assert!(
            ForwardStore::load(&path)
                .await
                .unwrap()
                .list()
                .await
                .is_empty()
        );

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn add_is_idempotent_per_target() {
        let dir = std::env::temp_dir().join(format!("mistl-tunnel-fwd-{}", uuid::Uuid::new_v4()));
        let path = dir.join("forwards.json");
        let store = ForwardStore::load(&path).await.unwrap();
        store.add(&spec()).await.unwrap();
        store.add(&spec()).await.unwrap();
        assert_eq!(store.list().await.len(), 1);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
