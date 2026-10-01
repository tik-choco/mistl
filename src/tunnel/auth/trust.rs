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
use tokio::sync::{Mutex, RwLock};

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
    /// Serializes persists so two concurrent `remember`/`remove` calls can't
    /// interleave their temp-file writes and renames.
    persist_lock: Arc<Mutex<()>>,
}

impl TrustStore {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let entries = match tokio::fs::read_to_string(&path).await {
            Ok(text) => match serde_json::from_str::<Vec<TrustEntry>>(&text) {
                Ok(entries) => entries
                    .into_iter()
                    .map(|entry| (entry.key, entry.decision))
                    .collect(),
                Err(err) => {
                    // Keep the unreadable file for inspection rather than
                    // failing the whole tunnel start (or silently overwriting
                    // it on the next persist), and start with no remembered
                    // decisions.
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis());
                    let mut name = path.file_name().unwrap_or_default().to_os_string();
                    name.push(format!(".corrupt-{ts}"));
                    let backup = path.with_file_name(name);
                    tracing::warn!(
                        "trust store {} is unreadable ({}); moving it to {} and starting empty",
                        path.display(),
                        err,
                        backup.display()
                    );
                    tokio::fs::rename(&path, &backup).await?;
                    HashMap::new()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            entries: Arc::new(RwLock::new(entries)),
            persist_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn get(&self, key: &TrustKey) -> Option<TrustDecision> {
        self.entries.read().await.get(key).copied()
    }

    /// The remembered decision for a peer on `forward_key`.
    ///
    /// Entries are keyed on the peer's DID (`peer_id` field = `did:key:...`).
    /// Old node-id-keyed entries still load, but a node id alone is not an
    /// identity, so they are honored like this:
    /// - verified `did` (must hash to `peer_id`): the DID entry wins; a
    ///   legacy `(peer_id, key)` entry is migrated to the DID on the spot
    ///   (afterwards both the DID and its node id must match, since lookups
    ///   are by the DID verified for this very `peer_id`);
    /// - unverified (`did == None`): only a legacy *deny* applies. A legacy
    ///   allow is ignored (the peer is prompted instead): trust is only
    ///   granted to a verified DID.
    pub async fn lookup(
        &self,
        peer_id: &str,
        did: Option<&str>,
        forward_key: &str,
    ) -> Option<TrustDecision> {
        let legacy = TrustKey {
            peer_id: peer_id.to_string(),
            forward_key: forward_key.to_string(),
        };
        // `peer_id` is attacker-chosen: never let it alias a DID-keyed row.
        let legacy_ok = !peer_id.starts_with("did:");
        let did = did.filter(|did| crate::identity::node_id_for_did(did) == peer_id);
        let Some(did) = did else {
            if !legacy_ok {
                return None;
            }
            return match self.get(&legacy).await {
                Some(TrustDecision::Deny) => Some(TrustDecision::Deny),
                _ => None,
            };
        };
        let did_key = TrustKey {
            peer_id: did.to_string(),
            forward_key: forward_key.to_string(),
        };
        if let Some(decision) = self.get(&did_key).await {
            return Some(decision);
        }
        if !legacy_ok {
            return None;
        }
        let decision = self.get(&legacy).await?;
        // Migrate: attach the DID, drop the node-id row. If persisting the
        // new row fails keep the old one so nothing is lost.
        if self.remember(did_key, decision).await.is_ok() {
            let _ = self.remove(&legacy).await;
        }
        Some(decision)
    }

    /// Remembers `decision` for a *verified* peer, keyed on its DID. Returns
    /// `Ok(false)` (storing nothing) when the peer is unverified: decisions
    /// about unverified peers are one-off only.
    pub async fn remember_for(
        &self,
        peer_id: &str,
        did: Option<&str>,
        forward_key: &str,
        decision: TrustDecision,
    ) -> Result<bool> {
        let Some(did) = did.filter(|did| crate::identity::node_id_for_did(did) == peer_id) else {
            return Ok(false);
        };
        self.remember(
            TrustKey {
                peer_id: did.to_string(),
                forward_key: forward_key.to_string(),
            },
            decision,
        )
        .await?;
        Ok(true)
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
        let _guard = self.persist_lock.lock().await;
        // Snapshot under the persist lock so the last writer always writes the
        // newest state; `write_private` swaps it in atomically (0600 on Unix).
        let text = serde_json::to_string_pretty(&self.list().await)?;
        crate::statefile::write_private(&self.path, text.as_bytes())
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
