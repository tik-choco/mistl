//! Bounded QUERY/WANT/HAVE transfers, bound to transport room and sender.
use super::{
    protocol::{Error, Head},
    repository::{manifest_limit, validate_blob, validate_manifest},
};
use mistlib_core::storage::cid::{MULTICODEC_RAW, verify_cid};
use mistlib_core::storage::{CHUNK_SIZE, protocol::*};
use std::collections::HashMap;
use std::future::Future;
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

/// Reconstruct a generation from local raw blocks without peer resolution.
/// A partial local copy returns None so the caller can use its normal pull path.
pub(super) async fn local_generation<F, Fut>(
    head: &Head,
    max_blob: u64,
    mut read: F,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<Option<Vec<u8>>, Error>>,
{
    head.validate(max_blob)?;
    head.verify()?;
    let size = usize::try_from(head.size).map_err(|_| Error::new("too_large"))?;
    let Some(bytes) = read(head.cid.clone()).await? else {
        return Ok(None);
    };
    let manifest = validate_manifest(head, &bytes)?;
    let mut blob = Vec::new();
    for cid in &manifest.chunks {
        let expected = size.saturating_sub(blob.len()).min(CHUNK_SIZE);
        let Some(chunk) = read(cid.clone()).await? else {
            return Ok(None);
        };
        if chunk.len() != expected || !verify_cid(cid, &chunk, MULTICODEC_RAW) {
            return Err(Error::new("hash_mismatch"));
        }
        blob.try_reserve_exact(chunk.len())
            .map_err(|_| Error::new("too_large"))?;
        blob.extend_from_slice(&chunk);
    }
    validate_blob(head, &blob)?;
    Ok(Some((blob, bytes)))
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

    fn signed(head: &mut Head) {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        use ed25519_dalek::{Signer, SigningKey};
        let key = SigningKey::from_bytes(&[7; 32]);
        head.writer = URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes());
        head.sig = URL_SAFE_NO_PAD.encode(key.sign(&head.signing_payload()).to_bytes());
    }

    fn local_fixture(blob: &[u8]) -> (Head, HashMap<String, Vec<u8>>, Vec<u8>) {
        use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, compute_cid};
        let chunks: Vec<_> = blob
            .chunks(CHUNK_SIZE)
            .map(|bytes| compute_cid(bytes, MULTICODEC_RAW))
            .collect();
        let manifest = mistlib_core::storage::FileManifest {
            name: "tc-escrow.bin".into(),
            size: blob.len() as u64,
            chunks: chunks.clone(),
        };
        // Preserve extensions and the original CBOR representation.
        let mut value: serde_cbor::Value =
            serde_cbor::from_slice(&serde_cbor::to_vec(&manifest).unwrap()).unwrap();
        if let serde_cbor::Value::Map(fields) = &mut value {
            fields.insert(
                serde_cbor::Value::Text("future".into()),
                serde_cbor::Value::Text("opaque".into()),
            );
        }
        let bytes = serde_cbor::to_vec(&value).unwrap();
        let mut head = Head {
            v: 1,
            kind: "tc-escrow-head".into(),
            slot: "ab".repeat(16),
            seq: 1,
            parent: None,
            cid: compute_cid(&bytes, MULTICODEC_DAG_CBOR),
            size: blob.len() as u64,
            sha256: super::super::protocol::digest(blob),
            comp: "none".into(),
            schema: "test/1".into(),
            writer: String::new(),
            at: 1,
            sig: String::new(),
        };
        signed(&mut head);
        let mut blocks: HashMap<_, _> = chunks
            .into_iter()
            .zip(blob.chunks(CHUNK_SIZE).map(|bytes| bytes.to_vec()))
            .collect();
        blocks.insert(head.cid.clone(), bytes.clone());
        (head, blocks, bytes)
    }

    #[tokio::test]
    async fn local_generation_reconstructs_multiple_chunks_and_preserves_original_manifest() {
        let blob: Vec<_> = (0..CHUNK_SIZE + 19).map(|n| (n % 251) as u8).collect();
        let (head, blocks, original) = local_fixture(&blob);
        let mut reads = Vec::new();
        let (actual, manifest) = local_generation(&head, blob.len() as u64, |cid| {
            reads.push(cid.clone());
            std::future::ready(Ok(blocks.get(&cid).cloned()))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(actual, blob);
        assert_eq!(manifest, original);
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[0], head.cid);
    }

    #[tokio::test]
    async fn local_generation_validates_size_and_signature_before_reading_blocks() {
        let (mut head, _, _) = local_fixture(b"ciphertext");
        let mut reads = 0;
        let result = local_generation(&head, head.size - 1, |_| {
            reads += 1;
            std::future::ready(Ok(None))
        })
        .await;
        assert_eq!(result.unwrap_err().code, "too_large");
        assert_eq!(reads, 0);
        head.seq += 1;
        let result = local_generation(&head, 1024, |_| {
            reads += 1;
            std::future::ready(Ok(None))
        })
        .await;
        assert_eq!(result.unwrap_err().code, "bad_sig");
        assert_eq!(reads, 0);
    }

    #[tokio::test]
    async fn local_generation_returns_none_for_missing_root_or_chunk() {
        let (head, mut blocks, _) = local_fixture(&vec![7; CHUNK_SIZE + 3]);
        assert!(
            local_generation(&head, head.size, |_| std::future::ready(Ok(None)))
                .await
                .unwrap()
                .is_none()
        );
        let chunks: Vec<_> = blocks
            .keys()
            .filter(|cid| **cid != head.cid)
            .cloned()
            .collect();
        blocks.remove(&chunks[0]);
        assert!(
            local_generation(&head, head.size, |cid| std::future::ready(Ok(blocks
                .get(&cid)
                .cloned())))
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn local_generation_rejects_bad_manifest_chunk_and_total_hash() {
        let (head, blocks, _) = local_fixture(b"ciphertext");
        let mut bad_manifest = blocks.clone();
        bad_manifest.get_mut(&head.cid).unwrap().push(0);
        assert_eq!(
            local_generation(&head, head.size, |cid| std::future::ready(Ok(bad_manifest
                .get(&cid)
                .cloned())))
            .await
            .unwrap_err()
            .code,
            "hash_mismatch"
        );
        let mut bad_chunk = blocks.clone();
        let chunk = bad_chunk
            .iter_mut()
            .find(|(cid, _)| **cid != head.cid)
            .unwrap()
            .1;
        chunk[0] ^= 1;
        assert_eq!(
            local_generation(&head, head.size, |cid| std::future::ready(Ok(bad_chunk
                .get(&cid)
                .cloned())))
            .await
            .unwrap_err()
            .code,
            "hash_mismatch"
        );
        let mut bad_hash = head.clone();
        bad_hash.sha256 = "00".repeat(32);
        signed(&mut bad_hash);
        assert_eq!(
            local_generation(&bad_hash, bad_hash.size, |cid| std::future::ready(Ok(
                blocks.get(&cid).cloned()
            )))
            .await
            .unwrap_err()
            .code,
            "hash_mismatch"
        );
    }

    #[tokio::test]
    async fn local_generation_rejects_invalid_manifest_chunk_cid_before_chunk_lookup() {
        use mistlib_core::storage::cid::{MULTICODEC_DAG_CBOR, compute_cid};
        let (mut head, _, _) = local_fixture(b"ciphertext");
        let bytes = serde_cbor::to_vec(&mistlib_core::storage::FileManifest {
            name: "tc-escrow.bin".into(),
            size: head.size,
            chunks: vec!["invalid-cid".into()],
        })
        .unwrap();
        head.cid = compute_cid(&bytes, MULTICODEC_DAG_CBOR);
        signed(&mut head);
        let mut reads = 0;
        assert!(
            local_generation(&head, head.size, |_| {
                reads += 1;
                std::future::ready(Ok(Some(bytes.clone())))
            })
            .await
            .is_err()
        );
        assert_eq!(reads, 1);
    }
}
