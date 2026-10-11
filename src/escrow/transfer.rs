//! Bounded QUERY/WANT/HAVE transfers, bound to transport room and sender.
use super::{
    protocol::{Error, Head},
    repository::{manifest_limit, validate_blob, validate_manifest},
};
use mistlib_core::storage::cid::{MULTICODEC_RAW, verify_cid};
use mistlib_core::storage::{CHUNK_SIZE, protocol::*};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;

type Key = (String, String, String);
struct Pending {
    cap: usize,
    total: Option<u16>,
    chunks: HashMap<u16, Vec<u8>>,
    bytes: usize,
    tx: Option<oneshot::Sender<Result<Vec<u8>, Error>>>,
}
#[derive(Default)]
pub struct Transfers {
    pending: Mutex<HashMap<Key, Pending>>,
}

impl Transfers {
    /// Parse only solicited frames; no allocation based on unsolicited chunk counts.
    pub fn receive(&self, room: &str, from: &str, raw: &[u8]) {
        if raw.len() < 2 || !matches!(raw[0], MSG_HAVE | MSG_HAVE_CHUNK) {
            return;
        }
        let chunked = raw[0] == MSG_HAVE_CHUNK;
        let start = if chunked { 6 } else { 2 };
        let end = start + raw[1] as usize;
        if raw.len() < end {
            return;
        }
        let Ok(cid) = std::str::from_utf8(&raw[start..end]) else {
            return;
        };
        let mut pending = self.pending.lock().unwrap();
        let key = (room.into(), from.into(), cid.into());
        let Some(request) = pending.get_mut(&key) else {
            return;
        };
        let payload = &raw[end..];
        if payload.len() > request.cap {
            if let Some(tx) = request.tx.take() {
                let _ = tx.send(Err(Error::new("hash_mismatch")));
            }
            return;
        }
        if !chunked {
            if let Some(tx) = request.tx.take() {
                let _ = tx.send(Ok(payload.to_vec()));
            }
            return;
        }
        let index = u16::from_be_bytes([raw[2], raw[3]]);
        let total = u16::from_be_bytes([raw[4], raw[5]]);
        // A block is at most 1 MiB (manifest <=128 KiB); bound tiny-chunk attacks too.
        if total == 0
            || index >= total
            || total as usize > 1024
            || request.total.is_some_and(|t| t != total)
            || request.chunks.contains_key(&index)
            || request.bytes.saturating_add(payload.len()) > request.cap
        {
            return;
        }
        request.total = Some(total);
        request.bytes += payload.len();
        request.chunks.insert(index, payload.to_vec());
        if request.chunks.len() == total as usize {
            let mut bytes = Vec::with_capacity(request.bytes);
            for i in 0..total {
                bytes.extend_from_slice(request.chunks.get(&i).unwrap());
            }
            if let Some(tx) = request.tx.take() {
                let _ = tx.send(Ok(bytes));
            }
            request.chunks.clear();
        }
    }

    async fn block(&self, room: &str, from: &str, cid: &str, cap: usize) -> Result<Vec<u8>, Error> {
        let key = (room.into(), from.into(), cid.into());
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= 4 || pending.contains_key(&key) {
                return Err(Error::new("busy"));
            }
            pending.insert(
                key.clone(),
                Pending {
                    cap,
                    total: None,
                    chunks: HashMap::new(),
                    bytes: 0,
                    tx: Some(tx),
                },
            );
        }
        let _guard = PendingGuard {
            transfers: self,
            key,
        };
        let _ = crate::net::send_direct(room, from, build_query_message(cid)).await;
        let mut want = vec![MSG_WANT];
        want.extend_from_slice(cid.as_bytes());
        crate::net::send_direct(room, from, want)
            .await
            .map_err(|_| Error::new("fetch_failed"))?;
        rx.await.map_err(|_| Error::new("fetch_failed"))?
    }

    pub async fn pull(
        &self,
        room: &str,
        from: &str,
        head: &Head,
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let bytes = self
                .block(room, from, &head.cid, manifest_limit(head.size))
                .await?;
            let manifest = validate_manifest(head, &bytes)?;
            let mut blob = Vec::with_capacity(head.size as usize);
            for cid in &manifest.chunks {
                let expected = (head.size as usize - blob.len()).min(CHUNK_SIZE);
                let chunk = self.block(room, from, cid, expected).await?;
                if chunk.len() != expected || !verify_cid(cid, &chunk, MULTICODEC_RAW) {
                    return Err(Error::new("hash_mismatch"));
                }
                blob.extend_from_slice(&chunk);
            }
            validate_blob(head, &blob)?;
            Ok((blob, bytes))
        })
        .await
        .map_err(|_| Error::new("fetch_failed"))?
    }
}

struct PendingGuard<'a> {
    transfers: &'a Transfers,
    key: Key,
}
impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.transfers.pending.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escrow_transfer_binds_sender_room_and_bounds_assembly() {
        let transfers = Transfers::default();
        let key = ("room".into(), "sender".into(), "bcid".into());
        let (tx, mut rx) = oneshot::channel();
        transfers.pending.lock().unwrap().insert(
            key.clone(),
            Pending {
                cap: 4,
                total: None,
                chunks: HashMap::new(),
                bytes: 0,
                tx: Some(tx),
            },
        );
        transfers.receive("wrong", "sender", &build_have_message("bcid", b"test"));
        transfers.receive("room", "other", &build_have_message("bcid", b"test"));
        transfers.receive(
            "room",
            "sender",
            &build_have_chunk_message("bcid", 0, 65535, b"x"),
        );

        assert!(rx.try_recv().is_err());
        assert!(transfers.pending.lock().unwrap()[&key].chunks.is_empty());
        transfers.receive(
            "room",
            "sender",
            &build_have_chunk_message("bcid", 1, 2, b"st"),
        );
        transfers.receive(
            "room",
            "sender",
            &build_have_chunk_message("bcid", 0, 2, b"te"),
        );
        assert_eq!(rx.try_recv().unwrap().unwrap(), b"test");
    }
}
