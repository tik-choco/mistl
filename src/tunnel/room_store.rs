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

    /// Newest-first. Ordering comes from the backing vector's position, not
    /// from sorting on `last_used_ms`: several `record_use` calls landing in
    /// the same millisecond (routine on a fast clock) would otherwise tie
    /// under a `last_used_ms`-based sort, and a *stable* sort resolves ties
    /// by leaving tied elements in their prior relative order -- which is
    /// not necessarily recency order. `record_use` already maintains the
    /// vector oldest-first/newest-last (see its doc comment), so producing
    /// the newest-first contract here is just a reverse.
    pub async fn list(&self) -> Vec<RoomHistoryEntry> {
        let mut entries = self.entries.read().await.clone();
        entries.reverse();
        entries
    }

    /// Upsert: if `room_id` already has an entry, remove it first, then push
    /// a fresh entry with `last_used_ms = now`. This keeps the backing
    /// vector ordered oldest-first / newest-last purely by position -- the
    /// vector's order is the single source of truth for recency, since
    /// `last_used_ms` can tie across calls made within the same millisecond
    /// and can no longer be trusted to break ties correctly. After
    /// inserting, if the store has more than MAX_ROOMS entries, drop from
    /// the front (the oldest end) until back at the cap, rather than
    /// searching for a minimum `last_used_ms` (which has the same tie
    /// problem). Persists to disk.
    pub async fn record_use(&self, room_id: &str) -> Result<()> {
        {
            let mut entries = self.entries.write().await;
            entries.retain(|e| e.room_id != room_id);
            entries.push(RoomHistoryEntry {
                room_id: room_id.to_string(),
                last_used_ms: now_ms(),
            });
            if entries.len() > MAX_ROOMS {
                let excess = entries.len() - MAX_ROOMS;
                entries.drain(0..excess);
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

    /// Pins the exact flake this module used to have: `record_use` calls
    /// made back-to-back with no artificial delay routinely land in the
    /// same millisecond, so `last_used_ms` alone cannot distinguish their
    /// order. `list()` must still come back strictly newest-first, using
    /// the backing vector's position rather than the timestamp.
    #[tokio::test]
    async fn list_is_newest_first_even_when_all_timestamps_tie() {
        let dir = std::env::temp_dir().join(format!("mistl-tunnel-room-{}", uuid::Uuid::new_v4()));
        let path = dir.join("rooms.json");
        let store = RoomStore::load(&path).await.unwrap();

        store.record_use("a").await.unwrap();
        store.record_use("b").await.unwrap();
        store.record_use("c").await.unwrap();
        store.record_use("a").await.unwrap();

        let ids: Vec<String> = store.list().await.into_iter().map(|e| e.room_id).collect();
        assert_eq!(ids, vec!["a", "c", "b"]);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    /// Same tie hazard as above, but for the MAX_ROOMS trim: when every
    /// entry shares a `last_used_ms`, picking a victim by minimum timestamp
    /// is not well-defined. Trimming from the front of the vector (the
    /// oldest end by position) evicts the genuinely oldest entries
    /// regardless of timestamp ties.
    #[tokio::test]
    async fn trims_oldest_by_position_when_all_timestamps_tie() {
        let dir = std::env::temp_dir().join(format!("mistl-tunnel-room-{}", uuid::Uuid::new_v4()));
        let path = dir.join("rooms.json");
        let store = RoomStore::load(&path).await.unwrap();

        let total = MAX_ROOMS + 3;
        for i in 0..total {
            store.record_use(&format!("room-{i}")).await.unwrap();
        }

        let ids: Vec<String> = store.list().await.into_iter().map(|e| e.room_id).collect();

        // Newest-first, and the exact surviving set/order is deterministic
        // even though every entry was written with a tied (or near-tied)
        // clock reading: the three oldest ("room-0".."room-2") are gone and
        // the remaining ids appear strictly newest-first.
        let expected: Vec<String> = (3..total).rev().map(|i| format!("room-{i}")).collect();
        assert_eq!(ids, expected);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
