//! Unheld data is proxied only through a verified current manifest and a reported holder.
use super::{Mesh, now_ms};
use crate::escrow::{
    protocol::Head,
    repository::{manifest_limit, validate_manifest},
};
use mistlib::storage::FetchOptions;
use mistlib_core::{
    storage::{
        CHUNK_SIZE,
        cid::{MULTICODEC_DAG_CBOR, MULTICODEC_RAW, verify_cid},
    },
    types::NodeId,
};
use std::time::Duration;

impl Mesh {
    pub async fn proxy(&self, room: &str, from: &str, cid: &str) -> Option<Vec<u8>> {
        let _permit = self.proxies.clone().try_acquire_owned().ok()?;
        tokio::time::timeout(Duration::from_secs(20), self.proxy_inner(room, from, cid))
            .await
            .ok()?
    }

    async fn proxy_inner(&self, room: &str, from: &str, cid: &str) -> Option<Vec<u8>> {
        let service = self.service.upgrade()?;
        let records = service.repo.mesh_records(room);
        let mut reference: Option<(Head, usize, u64)> = records
            .iter()
            .find(|r| r.deleted.is_none() && r.head.cid == cid)
            .map(|r| {
                (
                    r.head.clone(),
                    manifest_limit(r.head.size),
                    MULTICODEC_DAG_CBOR,
                )
            });
        if reference.is_none() {
            let mut rooms = self.rooms.lock().unwrap();
            let state = rooms.get_mut(room)?;
            state.manifests.retain(|_, (head, _, seen)| {
                now_ms().saturating_sub(*seen) < 300_000
                    && records
                        .iter()
                        .any(|r| r.deleted.is_none() && r.head == *head)
            });
            for (head, manifest, seen) in state.manifests.values_mut() {
                if let Some(i) = manifest.chunks.iter().position(|c| c == cid) {
                    *seen = now_ms();
                    reference = Some((
                        head.clone(),
                        (head.size.saturating_sub((i * CHUNK_SIZE) as u64)).min(CHUNK_SIZE as u64)
                            as usize,
                        MULTICODEC_RAW,
                    ));
                    break;
                }
            }
        }
        let (head, cap, codec) = reference?;
        if head.size > service.cfg.max_blob_bytes {
            return None;
        }
        let sources: Vec<_> = {
            let rooms = self.rooms.lock().unwrap();
            let state = rooms.get(room)?;
            state
                .peers
                .iter()
                .filter(|(node, p)| {
                    node.as_str() != from
                        && now_ms().saturating_sub(p.seen) <= 45_000
                        && p.entries.get(&head.slot).is_some_and(|e| {
                            e.kind == "head" && e.seq == head.seq && e.hash == head.hash() && e.held
                        })
                })
                .map(|(node, _)| node.clone())
                .take(4)
                .collect()
        };
        for source in sources {
            let Ok(bytes) = (service.transfers.fetch)(
                room.into(),
                cid.into(),
                FetchOptions {
                    peer: Some(NodeId(source)),
                    max_bytes: cap,
                    timeout: Duration::from_secs(20),
                },
            )
            .await
            else {
                continue;
            };
            if bytes.len() > cap || !verify_cid(cid, &bytes, codec) {
                continue;
            }
            if codec == MULTICODEC_RAW && bytes.len() != cap {
                continue;
            }
            // Recheck after I/O: a deletion or replacement revokes this reference.
            if service.repo.head(room, &head.slot).as_ref() != Some(&head) {
                return None;
            }
            if codec == MULTICODEC_DAG_CBOR {
                let manifest = validate_manifest(&head, &bytes).ok()?;
                let mut rooms = self.rooms.lock().unwrap();
                // At most 64 bounded manifests globally (8 MiB encoded upper bound).
                let count: usize = rooms.values().map(|r| r.manifests.len()).sum();
                if count >= 64 {
                    let oldest = rooms
                        .iter()
                        .flat_map(|(room, r)| {
                            r.manifests.iter().map(move |(slot, (_, _, seen))| {
                                (room.clone(), slot.clone(), *seen)
                            })
                        })
                        .min_by_key(|(_, _, seen)| *seen);
                    if let Some((room, slot, _)) = oldest {
                        if let Some(r) = rooms.get_mut(&room) {
                            r.manifests.remove(&slot);
                        }
                    }
                }
                rooms
                    .get_mut(room)?
                    .manifests
                    .insert(head.slot.clone(), (head.clone(), manifest, now_ms()));
            }
            tracing::debug!(room,slot=%head.slot,"escrow mesh: served proxied block");
            return Some(bytes);
        }
        None
    }
}
