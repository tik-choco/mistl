//! Bounded, persisted conversation history.
//!
//! Caps (everything here can be driven by remote peers):
//! - at most [`MAX_MESSAGES`] messages per conversation, and at most
//!   [`MAX_CONV_BYTES`] of text/metadata per conversation (oldest dropped);
//! - at most [`MAX_CONVERSATIONS`] conversations. Eviction prefers
//!   *unsolicited* conversations (ones we never wrote in), so a stranger
//!   minting identities cannot push out the conversations that matter; when
//!   every conversation is a real one, an unsolicited newcomer is refused
//!   instead.
//!
//! One file per conversation under `<data_dir>/dm/`, written through
//! [`crate::statefile::write_private`] (received file keys live there, so
//! the files are private) by a debounced flusher; see `dm::Service`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::wire::{FileMeta, MAX_TEXT_CHARS};

pub const MAX_MESSAGES: usize = 500;
pub const MAX_CONVERSATIONS: usize = 200;
/// Approximate per-conversation byte budget (text + file metadata).
pub const MAX_CONV_BYTES: usize = 256 * 1024;
/// Largest conversation file we read back from disk.
const MAX_FILE_BYTES_ON_DISK: u64 = 4 * 1024 * 1024;
const MAX_UNREAD: u32 = 9999;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Sent,
    Delivered,
    Failed,
    Received,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Sent => "sent",
            Status::Delivered => "delivered",
            Status::Failed => "failed",
            Status::Received => "received",
        }
    }
}

/// A stored message. `file.key` is persisted (the receiver needs it to
/// download later) but never serialized for the UI: use [`StoredMsg::ui`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMsg {
    pub id: String,
    pub mine: bool,
    pub ts_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<FileMeta>,
    pub status: Status,
}

impl StoredMsg {
    /// The message as shown to the UI. Never includes the file key.
    pub fn ui(&self) -> Value {
        let mut out = json!({
            "id": self.id,
            "mine": self.mine,
            "ts_ms": self.ts_ms,
            "status": self.status.as_str(),
        });
        let map = out.as_object_mut().expect("object literal");
        if let Some(text) = &self.text {
            map.insert("text".into(), json!(text));
        }
        if let Some(file) = &self.file {
            map.insert(
                "file".into(),
                json!({
                    "cid": file.cid, "name": file.name,
                    "size": file.size, "mime": file.mime,
                }),
            );
        }
        out
    }

    fn weight(&self) -> usize {
        64 + self.text.as_ref().map_or(0, String::len)
            + self.file.as_ref().map_or(0, |f| {
                f.cid.len() + f.name.len() + f.mime.len() + f.key.len()
            })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Conversation {
    did: String,
    #[serde(default)]
    unread: u32,
    #[serde(default)]
    updated_ms: u64,
    #[serde(default)]
    messages: VecDeque<StoredMsg>,
}

impl Conversation {
    fn bytes(&self) -> usize {
        self.messages.iter().map(StoredMsg::weight).sum()
    }

    /// Whether we ever wrote in this conversation.
    fn solicited(&self) -> bool {
        self.messages.iter().any(|m| m.mine)
    }

    fn trim(&mut self) {
        while self.messages.len() > MAX_MESSAGES {
            self.messages.pop_front();
        }
        let mut bytes = self.bytes();
        while bytes > MAX_CONV_BYTES && self.messages.len() > 1 {
            if let Some(old) = self.messages.pop_front() {
                bytes -= old.weight();
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddError {
    /// A message with this id is already in the conversation.
    Duplicate,
    /// The conversation table is full of conversations we wrote in, so a new
    /// unsolicited one is refused.
    Full,
}

#[derive(Debug, Default)]
pub struct History {
    convs: HashMap<String, Conversation>,
    dirty: HashSet<String>,
    removed: HashSet<String>,
}

pub fn file_name_for(did: &str) -> String {
    let digest = Sha256::digest(did.as_bytes());
    let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
    format!("{hex}.json")
}

impl History {
    /// Evicts one conversation to make room (see the module doc for the
    /// policy). Returns `false` when nothing may be evicted.
    fn make_room(&mut self, for_remote: bool) -> bool {
        let lru = |convs: &HashMap<String, Conversation>, only_unsolicited: bool| {
            convs
                .iter()
                .filter(|(_, c)| !only_unsolicited || !c.solicited())
                .min_by_key(|(_, c)| c.updated_ms)
                .map(|(did, _)| did.clone())
        };
        let victim = lru(&self.convs, true).or_else(|| {
            if for_remote {
                None
            } else {
                lru(&self.convs, false)
            }
        });
        let Some(victim) = victim else {
            return false;
        };
        self.convs.remove(&victim);
        self.dirty.remove(&victim);
        self.removed.insert(victim);
        true
    }

    /// Appends `msg` to `did`'s conversation. `remote` marks an inbound
    /// message (counts as unread, and may be refused when the table is full).
    pub fn add(
        &mut self,
        did: &str,
        msg: StoredMsg,
        now_ms: u64,
        remote: bool,
    ) -> Result<(), AddError> {
        if let Some(conv) = self.convs.get(did)
            && conv.messages.iter().any(|m| m.id == msg.id)
        {
            return Err(AddError::Duplicate);
        }
        if !self.convs.contains_key(did)
            && self.convs.len() >= MAX_CONVERSATIONS
            && !self.make_room(remote)
        {
            return Err(AddError::Full);
        }
        self.removed.remove(did);
        let conv = self
            .convs
            .entry(did.to_string())
            .or_insert_with(|| Conversation {
                did: did.to_string(),
                ..Default::default()
            });
        if remote {
            conv.unread = (conv.unread + 1).min(MAX_UNREAD);
        }
        conv.updated_ms = now_ms.max(conv.updated_ms);
        conv.messages.push_back(msg);
        conv.trim();
        self.dirty.insert(did.to_string());
        Ok(())
    }

    /// `sent` -> `delivered`, only for a message *we* sent in `did`'s
    /// conversation. Anything else (unknown id, a received message, an
    /// already-final status) is a no-op.
    pub fn mark_delivered(&mut self, did: &str, id: &str) -> bool {
        let Some(conv) = self.convs.get_mut(did) else {
            return false;
        };
        let Some(msg) = conv.messages.iter_mut().find(|m| m.id == id) else {
            return false;
        };
        if !msg.mine || msg.status != Status::Sent {
            return false;
        }
        msg.status = Status::Delivered;
        self.dirty.insert(did.to_string());
        true
    }

    #[cfg(test)]
    pub fn set_status(&mut self, did: &str, id: &str, status: Status) {
        if let Some(conv) = self.convs.get_mut(did)
            && let Some(msg) = conv.messages.iter_mut().find(|m| m.id == id && m.mine)
        {
            msg.status = status;
            self.dirty.insert(did.to_string());
        }
    }

    /// Re-queues every conversation for writing (after a failed flush).
    pub fn mark_all_dirty(&mut self) {
        self.dirty.extend(self.convs.keys().cloned());
    }

    pub fn mark_read(&mut self, did: &str) {
        if let Some(conv) = self.convs.get_mut(did)
            && conv.unread != 0
        {
            conv.unread = 0;
            self.dirty.insert(did.to_string());
        }
    }

    pub fn clear(&mut self, did: &str) {
        if self.convs.remove(did).is_some() {
            self.dirty.remove(did);
            self.removed.insert(did.to_string());
        }
    }

    pub fn message(&self, did: &str, id: &str) -> Option<&StoredMsg> {
        self.convs
            .get(did)
            .and_then(|c| c.messages.iter().find(|m| m.id == id))
    }

    /// Conversation summaries, most recently updated first.
    pub fn list(&self) -> Vec<Value> {
        let mut convs: Vec<&Conversation> = self.convs.values().collect();
        convs.sort_by(|a, b| b.updated_ms.cmp(&a.updated_ms).then(a.did.cmp(&b.did)));
        convs
            .into_iter()
            .map(|c| {
                json!({
                    "did": c.did,
                    "unread": c.unread,
                    "updated_ms": c.updated_ms,
                    "last": c.messages.back().map(StoredMsg::ui),
                })
            })
            .collect()
    }

    /// The newest `limit` messages (oldest first).
    pub fn recent(&self, did: &str, limit: usize) -> Vec<Value> {
        let limit = limit.clamp(1, MAX_MESSAGES);
        let Some(conv) = self.convs.get(did) else {
            return Vec::new();
        };
        let skip = conv.messages.len().saturating_sub(limit);
        conv.messages.iter().skip(skip).map(StoredMsg::ui).collect()
    }

    pub fn has_sent_files(&self) -> bool {
        self.convs
            .values()
            .any(|c| c.messages.iter().any(|m| m.mine && m.file.is_some()))
    }

    #[cfg(test)]
    pub fn conversation_count(&self) -> usize {
        self.convs.len()
    }

    #[cfg(test)]
    pub fn message_count(&self, did: &str) -> usize {
        self.convs.get(did).map_or(0, |c| c.messages.len())
    }

    /// Takes the pending disk work: `(file name, Some(bytes))` to write,
    /// `(file name, None)` to delete.
    pub fn take_pending(&mut self) -> Vec<(String, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        for did in self.removed.drain() {
            out.push((file_name_for(&did), None));
        }
        for did in self.dirty.drain() {
            if let Some(conv) = self.convs.get(&did)
                && let Ok(bytes) = serde_json::to_vec(conv)
            {
                out.push((file_name_for(&did), Some(bytes)));
            }
        }
        out
    }

    /// Loads every conversation file in `dir` (missing dir = empty history),
    /// re-applying the caps and ignoring anything malformed.
    pub fn load(dir: &Path) -> History {
        let mut history = History::default();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return history;
        };
        let mut loaded: Vec<Conversation> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            match entry.metadata() {
                Ok(meta) if meta.is_file() && meta.len() <= MAX_FILE_BYTES_ON_DISK => {}
                _ => continue,
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(mut conv) = serde_json::from_slice::<Conversation>(&bytes) else {
                continue;
            };
            // The did is the file's identity: a file under a name that does
            // not match its did is not ours.
            if conv.did.len() != crate::identity::ED25519_DID_KEY_LEN
                || path.file_name().and_then(|n| n.to_str()) != Some(&file_name_for(&conv.did))
            {
                continue;
            }
            conv.messages.retain(|m| {
                super::wire::is_valid_id(&m.id)
                    && m.text
                        .as_ref()
                        .is_none_or(|t| t.chars().count() <= MAX_TEXT_CHARS)
            });
            conv.trim();
            loaded.push(conv);
        }
        loaded.sort_by(|a, b| b.updated_ms.cmp(&a.updated_ms));
        loaded.truncate(MAX_CONVERSATIONS);
        for conv in loaded {
            history.convs.insert(conv.did.clone(), conv);
        }
        history
    }
}

/// Writes (or deletes) the files produced by [`History::take_pending`].
pub fn apply_pending(dir: &Path, pending: Vec<(String, Option<Vec<u8>>)>) -> Result<()> {
    for (name, bytes) in pending {
        let path: PathBuf = dir.join(&name);
        match bytes {
            Some(bytes) => crate::statefile::write_private(&path, &bytes)
                .with_context(|| format!("writing {}", path.display()))?,
            None => match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("removing {}", path.display())),
            },
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn did(n: usize) -> String {
        // Well-formed length (56); content is irrelevant to History.
        format!("did:key:z6Mk{n:044}")
    }

    fn msg(id: u32, mine: bool, text: &str) -> StoredMsg {
        StoredMsg {
            id: format!("{id:032x}"),
            mine,
            ts_ms: id as u64,
            text: Some(text.to_string()),
            file: None,
            status: if mine { Status::Sent } else { Status::Received },
        }
    }

    fn file_msg(id: u32) -> StoredMsg {
        StoredMsg {
            id: format!("{id:032x}"),
            mine: false,
            ts_ms: 1,
            text: None,
            file: Some(FileMeta {
                cid: "bafycid".into(),
                name: "a.txt".into(),
                size: 3,
                mime: "text/plain".into(),
                key: "SECRET-KEY".into(),
            }),
            status: Status::Received,
        }
    }

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(label: &str) -> Self {
            let p = std::env::temp_dir().join(format!(
                "mistl-dm-test-{label}-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir_all(&p).unwrap();
            TempDir(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn ui_json_never_contains_the_file_key() {
        let m = file_msg(1);
        let ui = m.ui();
        assert!(ui["file"].get("key").is_none());
        assert!(!ui.to_string().contains("SECRET-KEY"));
        assert_eq!(ui["file"]["name"], "a.txt");
        assert_eq!(ui["status"], "received");
        // The summary and history views go through the same projection.
        let mut h = History::default();
        h.add(&did(1), m, 10, true).unwrap();
        assert!(
            !serde_json::to_string(&h.list())
                .unwrap()
                .contains("SECRET-KEY")
        );
        assert!(
            !serde_json::to_string(&h.recent(&did(1), 10))
                .unwrap()
                .contains("SECRET-KEY")
        );
    }

    #[test]
    fn conversation_is_capped_at_500_messages() {
        let mut h = History::default();
        let d = did(1);
        for i in 0..(MAX_MESSAGES as u32 + 25) {
            h.add(&d, msg(i, false, "x"), i as u64, true).unwrap();
        }
        assert_eq!(h.message_count(&d), MAX_MESSAGES);
        let recent = h.recent(&d, 1000);
        assert_eq!(recent.len(), MAX_MESSAGES);
        assert_eq!(recent[0]["id"], format!("{:032x}", 25));
    }

    #[test]
    fn conversation_is_capped_by_bytes() {
        let mut h = History::default();
        let d = did(1);
        let big = "y".repeat(4000);
        for i in 0..200 {
            h.add(&d, msg(i, false, &big), 1, true).unwrap();
        }
        let conv = &h.convs[&d];
        assert!(conv.bytes() <= MAX_CONV_BYTES);
        assert!(!conv.messages.is_empty());
        // The newest message always survives.
        assert_eq!(conv.messages.back().unwrap().id, format!("{:032x}", 199));
    }

    #[test]
    fn duplicate_ids_are_refused() {
        let mut h = History::default();
        h.add(&did(1), msg(1, false, "a"), 1, true).unwrap();
        assert_eq!(
            h.add(&did(1), msg(1, false, "b"), 2, true),
            Err(AddError::Duplicate)
        );
        assert_eq!(h.message_count(&did(1)), 1);
    }

    #[test]
    fn conversations_are_capped_and_least_recently_updated_is_evicted() {
        let mut h = History::default();
        for n in 0..MAX_CONVERSATIONS {
            h.add(&did(n), msg(1, false, "hi"), 100 + n as u64, true)
                .unwrap();
        }
        assert_eq!(h.conversation_count(), MAX_CONVERSATIONS);
        // Touch conversation 0 so conversation 1 is now the oldest.
        h.add(&did(0), msg(2, false, "again"), 10_000, true)
            .unwrap();
        h.add(&did(999), msg(1, false, "new"), 10_001, true)
            .unwrap();
        assert_eq!(h.conversation_count(), MAX_CONVERSATIONS);
        assert!(h.convs.contains_key(&did(0)));
        assert!(!h.convs.contains_key(&did(1)));
        assert!(h.convs.contains_key(&did(999)));
    }

    #[test]
    fn unsolicited_floods_cannot_evict_real_conversations() {
        let mut h = History::default();
        // Fill the table with conversations we wrote in.
        for n in 0..MAX_CONVERSATIONS {
            h.add(&did(n), msg(1, true, "hello"), 100 + n as u64, false)
                .unwrap();
        }
        // A stranger minting identities is refused, nothing is evicted.
        for n in 1000..1050 {
            assert_eq!(
                h.add(&did(n), msg(1, false, "spam"), 1_000_000, true),
                Err(AddError::Full)
            );
        }
        assert_eq!(h.conversation_count(), MAX_CONVERSATIONS);
        assert!(h.convs.contains_key(&did(0)));
        // With one unsolicited conversation present, only that one is evicted.
        h.clear(&did(5));
        h.add(&did(2000), msg(1, false, "stranger"), 2_000_000, true)
            .unwrap();
        h.add(&did(2001), msg(1, false, "stranger2"), 2_000_001, true)
            .unwrap();
        assert!(!h.convs.contains_key(&did(2000)));
        assert!(h.convs.contains_key(&did(2001)));
        assert!(h.convs.contains_key(&did(0)));
        // Writing to someone new ourselves may evict the LRU real one.
        h.add(&did(3000), msg(1, true, "hi"), 3_000_000, false)
            .unwrap();
        assert_eq!(h.conversation_count(), MAX_CONVERSATIONS);
        assert!(h.convs.contains_key(&did(3000)));
    }

    #[test]
    fn delivery_ack_only_upgrades_our_own_sent_messages() {
        let mut h = History::default();
        let d = did(1);
        h.add(&d, msg(1, true, "out"), 1, false).unwrap();
        h.add(&d, msg(2, false, "in"), 2, true).unwrap();
        assert!(
            !h.mark_delivered(&did(2), &format!("{:032x}", 1)),
            "wrong peer"
        );
        assert!(
            !h.mark_delivered(&d, &format!("{:032x}", 2)),
            "received message"
        );
        assert!(!h.mark_delivered(&d, &format!("{:032x}", 9)), "unknown id");
        assert!(h.mark_delivered(&d, &format!("{:032x}", 1)));
        assert!(
            !h.mark_delivered(&d, &format!("{:032x}", 1)),
            "already delivered"
        );
        assert_eq!(
            h.message(&d, &format!("{:032x}", 1)).unwrap().status,
            Status::Delivered
        );
        // A failed message is not resurrected by a late ack.
        h.add(&d, msg(3, true, "x"), 3, false).unwrap();
        h.set_status(&d, &format!("{:032x}", 3), Status::Failed);
        assert!(!h.mark_delivered(&d, &format!("{:032x}", 3)));
    }

    #[test]
    fn unread_read_and_clear() {
        let mut h = History::default();
        let d = did(1);
        h.add(&d, msg(1, false, "a"), 1, true).unwrap();
        h.add(&d, msg(2, false, "b"), 2, true).unwrap();
        assert_eq!(h.list()[0]["unread"], 2);
        h.mark_read(&d);
        assert_eq!(h.list()[0]["unread"], 0);
        h.clear(&d);
        assert!(h.list().is_empty());
        assert!(h.recent(&d, 10).is_empty());
    }

    #[test]
    fn persistence_round_trips_and_keeps_the_key_on_disk() {
        let tmp = TempDir::new("persist");
        let mut h = History::default();
        let d = did(1);
        h.add(&d, file_msg(1), 5, true).unwrap();
        h.add(&d, msg(2, true, "yo"), 6, false).unwrap();
        h.add(&did(2), msg(1, false, "gone"), 7, true).unwrap();
        apply_pending(&tmp.0, h.take_pending()).unwrap();
        assert!(h.take_pending().is_empty());

        let loaded = History::load(&tmp.0);
        assert_eq!(loaded.conversation_count(), 2);
        assert_eq!(loaded.message_count(&d), 2);
        let m = loaded.message(&d, &format!("{:032x}", 1)).unwrap();
        assert_eq!(m.file.as_ref().unwrap().key, "SECRET-KEY");

        let mut loaded = loaded;
        loaded.clear(&did(2));
        apply_pending(&tmp.0, loaded.take_pending()).unwrap();
        assert_eq!(History::load(&tmp.0).conversation_count(), 1);
    }

    #[test]
    fn load_ignores_foreign_oversized_and_corrupt_files() {
        let tmp = TempDir::new("load");
        std::fs::write(tmp.0.join("junk.json"), b"{not json").unwrap();
        // A well-formed conversation under the wrong file name is not trusted.
        let wrong = serde_json::to_vec(&json!({"did": did(7), "messages": []})).unwrap();
        std::fs::write(tmp.0.join("0000.json"), wrong).unwrap();
        // Traversal-looking names are never produced or read as paths.
        assert!(!file_name_for("../../x").contains('/'));
        assert_eq!(History::load(&tmp.0).conversation_count(), 0);
        assert_eq!(
            History::load(&tmp.0.join("missing")).conversation_count(),
            0
        );
    }

    #[test]
    fn load_reapplies_the_message_cap() {
        let tmp = TempDir::new("cap");
        let d = did(1);
        let messages: Vec<Value> = (0..(MAX_MESSAGES as u32 + 40))
            .map(|i| serde_json::to_value(msg(i, false, "x")).unwrap())
            .collect();
        let bytes = serde_json::to_vec(
            &json!({"did": d, "unread": 3, "updated_ms": 9, "messages": messages}),
        )
        .unwrap();
        std::fs::write(tmp.0.join(file_name_for(&d)), bytes).unwrap();
        let h = History::load(&tmp.0);
        assert_eq!(h.message_count(&d), MAX_MESSAGES);
    }

    #[test]
    fn has_sent_files_only_counts_our_attachments() {
        let mut h = History::default();
        h.add(&did(1), file_msg(1), 1, true).unwrap();
        assert!(!h.has_sent_files());
        let mut mine = file_msg(2);
        mine.mine = true;
        mine.status = Status::Sent;
        h.add(&did(1), mine, 2, false).unwrap();
        assert!(h.has_sent_files());
    }
}
