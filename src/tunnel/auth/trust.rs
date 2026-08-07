//! Persists "always allow" / "always deny" decisions per `(peer_id,
//! forward_key)`, consulted before policy/pending authorization so a
//! previously-remembered choice short-circuits future prompts. Ported from
//! `p2p/src/auth/trust.rs`; the only change is [`default_trust_store_path`]
//! -- see its doc comment. `TrustStore::load` still takes an explicit path,
//! so callers (and the ported tests, which each use their own temp-dir
//! path) are unaffected.
//!
//! [`TrustEntry`] already derived `Serialize`/`Deserialize` upstream (it's
//! the on-disk row shape), so no change was needed there for the
//! `tunnel.status` IPC payload (see the integration contract's `trust`
//! field) -- it can be embedded as-is. [`TrustDecision`] additionally gains
//! `#[serde(rename_all = "lowercase")]` (`Allow`->`"allow"`,
//! `Deny`->`"deny"`) for a nicer wire/storage format; this is a fresh
//! storage location (`crate::config::data_dir()`, not upstream's
//! `%APPDATA%\p2p`) so there's no on-disk backward-compat concern.
//!
//! JSON shape of `TrustEntry`: `{"key":{"peer_id":"..","forward_key":".."},"decision":"allow"}`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrustKey {
    pub peer_id: String,
    pub forward_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrustDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustEntry {
    pub key: TrustKey,
    pub decision: TrustDecision,
}

#[derive(Debug, Clone)]
pub struct TrustStore {
    path: PathBuf,
    entries: Arc<RwLock<HashMap<TrustKey, TrustDecision>>>,
}

impl TrustStore {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let entries = match tokio::fs::read_to_string(&path).await {
            Ok(text) => serde_json::from_str::<Vec<TrustEntry>>(&text)?
                .into_iter()
                .map(|entry| (entry.key, entry.decision))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            entries: Arc::new(RwLock::new(entries)),
        })
    }

    pub async fn get(&self, key: &TrustKey) -> Option<TrustDecision> {
        self.entries.read().await.get(key).copied()
    }

    pub async fn list(&self) -> Vec<TrustEntry> {
        let mut entries = self
            .entries
            .read()
            .await
            .iter()
            .map(|(key, decision)| TrustEntry {
                key: key.clone(),
                decision: *decision,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| {
            a.key
                .peer_id
                .cmp(&b.key.peer_id)
                .then_with(|| a.key.forward_key.cmp(&b.key.forward_key))
        });
        entries
    }

    pub async fn remember(&self, key: TrustKey, decision: TrustDecision) -> Result<()> {
        self.entries.write().await.insert(key, decision);
        self.persist().await
    }

    pub async fn remove(&self, key: &TrustKey) -> Result<bool> {
        let removed = self.entries.write().await.remove(key).is_some();
        if removed {
            self.persist().await?;
        }
        Ok(removed)
    }

    async fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let text = serde_json::to_string_pretty(&self.list().await)?;
        tokio::fs::write(&self.path, text).await?;
        Ok(())
    }
}

/// Default location for the trust store:
/// `crate::config::data_dir()/tunnel/trust.json`. See
/// [`crate::tunnel::forward_store::default_forward_store_path`] for the
/// rationale (replaces upstream's `$P2P_CONFIG_DIR`/`%APPDATA%\p2p`) and the
/// infallible-with-a-warning fallback contract.
pub fn default_trust_store_path() -> PathBuf {
    match crate::config::data_dir() {
        Ok(dir) => dir.join("tunnel").join("trust.json"),
        Err(err) => {
            tracing::warn!(
                "could not resolve data dir for trust store ({}); using ./tunnel/trust.json",
                err
            );
            PathBuf::from("./tunnel").join("trust.json")
        }
    }
}
