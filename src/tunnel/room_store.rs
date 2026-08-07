//! Persists recently-used room ids so the dashboard/TUI can offer them for
//! reuse instead of requiring the user to retype a room id. Ported from
//! `p2p/src/room_store.rs`; the only change is [`default_room_store_path`]
//! -- see its doc comment. `RoomStore::load` still takes an explicit path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Maximum number of recent rooms retained in the store.
const MAX_ROOMS: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomHistoryEntry {
    pub room_id: String,
    pub last_used_ms: u128,
}

/// Persists recently-used room ids so the Web UI can offer them for reuse
/// instead of requiring the user to retype a room id.
#[derive(Debug, Clone)]
pub struct RoomStore {
    path: PathBuf,
    entries: Arc<RwLock<Vec<RoomHistoryEntry>>>,
}

impl RoomStore {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let entries = match tokio::fs::read_to_string(&path).await {
            Ok(text) => serde_json::from_str::<Vec<RoomHistoryEntry>>(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            entries: Arc::new(RwLock::new(entries)),
        })
    }

    /// Newest-first (by last_used_ms descending).
    pub async fn list(&self) -> Vec<RoomHistoryEntry> {
        let mut entries = self.entries.read().await.clone();
        entries.sort_by(|a, b| b.last_used_ms.cmp(&a.last_used_ms));
        entries
    }

    /// Upsert: if `room_id` already has an entry, remove it first, then push
    /// a fresh entry with `last_used_ms = now`. After inserting, if the
    /// store has more than MAX_ROOMS entries, drop the oldest (by
    /// last_used_ms) until back at the cap. Persists to disk.
    pub async fn record_use(&self, room_id: &str) -> Result<()> {
        {
            let mut entries = self.entries.write().await;
            entries.retain(|e| e.room_id != room_id);
            entries.push(RoomHistoryEntry {
                room_id: room_id.to_string(),
                last_used_ms: now_ms(),
            });
            while entries.len() > MAX_ROOMS {
                if let Some((idx, _)) = entries
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, e)| e.last_used_ms)
                {
                    entries.remove(idx);
                } else {
                    break;
                }
            }
        }
        self.persist().await
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

/// Current Unix time in milliseconds. Mirrors `session::now_ms` and
/// `auth::audit::now_ms` (kept as a separate copy since those are private
/// to their own modules).
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

/// Default location for the persisted room history:
/// `crate::config::data_dir()/tunnel/rooms.json`. See
/// [`crate::tunnel::forward_store::default_forward_store_path`] for the
/// rationale (replaces upstream's `$P2P_CONFIG_DIR`/`%APPDATA%\p2p`) and the
/// infallible-with-a-warning fallback contract.
pub fn default_room_store_path() -> PathBuf {
    match crate::config::data_dir() {
        Ok(dir) => dir.join("tunnel").join("rooms.json"),
        Err(err) => {
            tracing::warn!(
                "could not resolve data dir for room store ({}); using ./tunnel/rooms.json",
                err
            );
            PathBuf::from("./tunnel").join("rooms.json")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trips_through_disk() {
        let dir = std::env::temp_dir().join(format!("mistl-tunnel-room-{}", uuid::Uuid::new_v4()));
        let path = dir.join("rooms.json");

        let store = RoomStore::load(&path).await.unwrap();
        store.record_use("abc").await.unwrap();

        let reloaded = RoomStore::load(&path).await.unwrap();
        let entries = reloaded.list().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].room_id, "abc");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn record_use_moves_existing_id_to_front() {
        let dir = std::env::temp_dir().join(format!("mistl-tunnel-room-{}", uuid::Uuid::new_v4()));
        let path = dir.join("rooms.json");
        let store = RoomStore::load(&path).await.unwrap();

        store.record_use("a").await.unwrap();
        store.record_use("b").await.unwrap();
        store.record_use("a").await.unwrap();

        let entries = store.list().await;
        assert_eq!(entries[0].room_id, "a");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn enforces_max_rooms_cap() {
        let dir = std::env::temp_dir().join(format!("mistl-tunnel-room-{}", uuid::Uuid::new_v4()));
        let path = dir.join("rooms.json");
        let store = RoomStore::load(&path).await.unwrap();

        let total = MAX_ROOMS + 3;
        for i in 0..total {
            store.record_use(&format!("room-{i}")).await.unwrap();
        }

        let entries = store.list().await;
        assert_eq!(entries.len(), MAX_ROOMS);

        let ids: Vec<String> = entries.iter().map(|e| e.room_id.clone()).collect();
        for i in 0..3 {
            assert!(!ids.contains(&format!("room-{i}")));
        }
        for i in (total - MAX_ROOMS)..total {
            assert!(ids.contains(&format!("room-{i}")));
        }

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
