//! Bounded block fetches pinned to the PUT transport room and sender.
use super::{
    protocol::{Error, Head},
    repository::{manifest_limit, validate_blob, validate_manifest},
};
use mistlib::storage::{FetchOptions, fetch_block};
use mistlib_core::storage::CHUNK_SIZE;
use mistlib_core::storage::cid::{MULTICODEC_RAW, verify_cid};
use mistlib_core::types::NodeId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

const PULL_TIMEOUT: Duration = Duration::from_secs(30);
type FetchFuture = Pin<Box<dyn Future<Output = mistlib::Result<Vec<u8>>> + Send>>;
type Fetcher = Arc<dyn Fn(String, String, FetchOptions) -> FetchFuture + Send + Sync>;

pub struct Transfers {
    pub(super) fetch: Fetcher,
}

impl Default for Transfers {
    fn default() -> Self {
        Self {
            fetch: Arc::new(|room, cid, opts| {
                Box::pin(async move { fetch_block(&room, &cid, opts).await })
            }),
        }
    }
}

// fetch_block reports these failures as string-bearing errors. Keep the escrow
// error contract even though CID validation now happens inside fetch_block.
fn fetch_error(error: mistlib::MistError) -> Error {
    let content_failure = match &error {
        mistlib::MistError::Network(message) => {
            message == "Block CID/hash mismatch"
                || message.starts_with("Block exceeds receive byte limit (observed ")
        }
        mistlib::MistError::Config(message) => message == "Invalid or unsupported block CID",
        _ => false,
    };
    Error::new(if content_failure {
        "hash_mismatch"
    } else {
        "fetch_failed"
    })
}

impl Transfers {
    async fn block(&self, room: &str, from: &str, cid: &str, cap: usize) -> Result<Vec<u8>, Error> {
        let bytes = (self.fetch)(
            room.into(),
            cid.into(),
            FetchOptions {
                peer: Some(NodeId(from.into())),
                max_bytes: cap,
                timeout: PULL_TIMEOUT,
            },
        )
        .await
        .map_err(fetch_error)?;
        if bytes.len() > cap {
            return Err(Error::new("hash_mismatch"));
        }
        Ok(bytes)
    }

    pub async fn pull(
        &self,
        room: &str,
        from: &str,
        head: &Head,
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        tokio::time::timeout(PULL_TIMEOUT, async {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
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
