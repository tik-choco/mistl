//! Placement, bounded fetch scheduling, completeness, scrub and conservative GC.
use super::{
    object,
    service::{Core, Service, now_ms},
    state::{self, Graph, Owner},
};
use anyhow::{Context, Result, ensure};
use mistlib::storage::{FetchOptions, fetch_block};
use mistlib_core::types::NodeId;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const TICK_BYTES: usize = 64 * 1024 * 1024;
const PLACEMENT_SETTLE_MS: u64 = 60_000;
const BLOCK_LIMIT: usize = 2 * 1024 * 1024;

impl Core {
    pub fn graph(&self, pinset: &str) -> Result<Graph> {
        let mut graph = Graph::default();
        let Some(pin) = self.graph_block(&mut graph, pinset)? else {
            return Ok(graph);
        };
        let roots = object::parse_pinset(&pin)?;
        let mut references = roots.roots.len();
        for (i, cid) in roots.roots.iter().enumerate() {
            let Some(bytes) = self.graph_block(&mut graph, cid)? else {
                continue;
            };
            if i == 0 {
                ensure!(
                    bytes.len() <= object::MAX_CATALOG_BYTES && bytes.starts_with(b"MVC1"),
                    "invalid catalog block"
                );
                continue;
            }
            let descriptor = object::parse_descriptor(&bytes)?;
            references = references
                .checked_add(descriptor.chunks.len())
                .context("reference count overflow")?;
            ensure!(
                references <= state::MAX_REFERENCES,
                "dataset reference limit exceeded"
            );
            let mut actual = 0u64;
            let mut all = true;
            for chunk in &descriptor.chunks {
                if let Some(len) = self.graph_chunk(&mut graph, chunk)? {
                    ensure!(
                        (16..=object::SEGMENT as u64 + 16).contains(&len),
                        "invalid ciphertext segment length"
                    );
                    actual = actual.checked_add(len).context("object size overflow")?;
                } else {
                    all = false;
                }
            }
            ensure!(
                !all || actual == descriptor.bytes,
                "descriptor byte count mismatch"
            );
        }
        let missing: HashSet<_> = graph.missing.iter().collect();
        graph.expanded = !roots.roots.iter().skip(1).any(|cid| missing.contains(cid));
        Ok(graph)
    }

    fn graph_block(&self, graph: &mut Graph, cid: &str) -> Result<Option<Vec<u8>>> {
        ensure!(
            graph.keep.len() < state::MAX_REFERENCES || graph.keep.contains(cid),
            "dataset reference limit exceeded"
        );
        let first = graph.keep.insert(cid.to_string());
        let bytes = self.store.get(cid);
        if first {
            match &bytes {
                Some(bytes) => {
                    graph.bytes = graph
                        .bytes
                        .checked_add(bytes.len() as u64)
                        .context("dataset size overflow")?;
                }
                None => graph.missing.push(cid.to_string()),
            }
        }
        Ok(bytes)
    }

    /// Chunks are only checked for presence and length: reading and hashing
    /// every chunk on each tick would cost a full pass over the dataset.
    /// Repeated references are counted once.
    fn graph_chunk(&self, graph: &mut Graph, cid: &str) -> Result<Option<u64>> {
        ensure!(
            graph.keep.len() < state::MAX_REFERENCES || graph.keep.contains(cid),
            "dataset reference limit exceeded"
        );
        if let Some(len) = graph.sizes.get(cid) {
            return Ok(Some(*len));
        }
        let first = graph.keep.insert(cid.to_string());
        let len = self.store.size(cid);
        match len {
            Some(len) => {
                graph.sizes.insert(cid.to_string(), len);
                graph.bytes = graph
                    .bytes
                    .checked_add(len)
                    .context("dataset size overflow")?;
            }
            None if first => graph.missing.push(cid.to_string()),
            None => {}
        }
        Ok(len)
    }

    fn peer_complete(&self, did: &str, owner: &Owner, now: u64) -> bool {
        if did == self.me.did() {
            return owner.complete;
        }
        self.peers.get(did).is_some_and(|p| {
            now.saturating_sub(p.last_seen) <= state::ONLINE_MS
                && p.holds.iter().any(|h| state::confirms(h, &owner.cur))
        })
    }

    pub fn prepare(&mut self) -> Result<Vec<String>> {
        let now = now_ms();
        let corrupted = self.store.scrub_step(64);
        if !corrupted.is_empty() {
            tracing::info!(
                count = corrupted.len(),
                "vault: scrub removed corrupt blocks"
            );
            for owner in self.owners.values_mut() {
                owner.complete = false;
            }
        }
        let own = self.own_id();
        // Placement from a partial view of the group would start copies that
        // later turn out to be extra, so new holdings wait until every member
        // has sent a hello or two hello periods have passed since startup.
        let informed = now.saturating_sub(self.since) >= PLACEMENT_SETTLE_MS
            || self.group.as_ref().is_some_and(|group| {
                group
                    .members()
                    .iter()
                    .all(|did| did == self.me.did() || self.peers.contains_key(did))
            });
        let mut selected = Vec::new();
        let ids: Vec<_> = self.owners.keys().cloned().collect();
        for id in ids {
            let owner = &self.owners[&id];
            let owned = own.as_deref() == Some(id.as_str());
            if !informed && !owned && !owner.holding {
                continue;
            }
            let desired = self.desired(owner, now);
            let replacements = !desired.is_empty()
                && desired
                    .iter()
                    .all(|did| self.peer_complete(did, owner, now));
            let (holding, release_at) = state::holding_decision(
                owned,
                desired.iter().any(|did| did == self.me.did()),
                replacements,
                owner.holding,
                owner.release_at,
                now,
            );
            let owner = self.owners.get_mut(&id).context("owner disappeared")?;
            owner.holding = holding;
            owner.release_at = release_at;
            if holding {
                selected.push(id);
            } else {
                owner.complete = false;
            }
        }
        self.save()?;
        if !selected.is_empty() {
            let shift = (now / 15_000) as usize % selected.len();
            selected.rotate_left(shift);
        }
        Ok(selected)
    }

    fn fetch_plan(&mut self, id: &str, max: usize) -> Result<Option<Plan>> {
        ensure!(
            self.group
                .as_ref()
                .is_some_and(|g| g.is_member(self.me.did())),
            "local node is not a member"
        );
        let Some(owner) = self.owners.get(id).cloned().filter(|o| o.holding) else {
            return Ok(None);
        };
        let graph = self.graph(&owner.cur.pinset)?;
        ensure!(
            graph.bytes <= owner.cur.bytes,
            "head byte count is smaller than its blocks"
        );
        let complete = graph.complete(owner.cur.bytes);
        let insufficient = owner.cur.bytes.saturating_sub(graph.bytes) > self.free();
        if let Some(record) = self.owners.get_mut(id) {
            record.complete = complete;
            record.held_bytes = graph.bytes;
            record.missing = graph.missing.len();
            if insufficient {
                record.last_error = Some("insufficient capacity".into());
            } else if complete {
                record.last_error = None;
            } else if graph.expanded && graph.missing.is_empty() {
                record.last_error = Some("head byte count mismatch".into());
            }
        }
        if complete || insufficient || graph.missing.is_empty() {
            return Ok(None);
        }
        let group = self.group.as_ref().context("missing group")?;
        let now = now_ms();
        let mut peers: Vec<_> = self
            .peers
            .iter()
            .filter(|(did, p)| {
                group.is_member(did)
                    && now.saturating_sub(p.last_seen) <= state::ONLINE_MS
                    && p.holds.iter().any(|h| state::confirms(h, &owner.cur))
            })
            .map(|(did, _)| crate::identity::node_id_for_did(did))
            .collect();
        if group.is_member(&owner.home) && owner.home != self.me.did() {
            let home = crate::identity::node_id_for_did(&owner.home);
            if !peers.contains(&home) {
                peers.push(home);
            }
        }
        let cids: Vec<_> = graph.missing.into_iter().take(max).collect();
        self.inflight.extend(cids.iter().cloned());
        let catalog = self
            .store
            .get(&owner.cur.pinset)
            .and_then(|bytes| object::parse_pinset(&bytes).ok())
            .and_then(|pinset| pinset.roots.first().cloned());
        Ok(Some(Plan {
            room: group.room(),
            hash: owner.cur.hash(),
            peers,
            cids,
            catalog,
        }))
    }

    fn finish_tick(&mut self) -> Result<()> {
        let now = now_ms();
        if now.saturating_sub(self.last_gc) >= 10 * 60 * 1000 {
            let mut graphs = Vec::new();
            for owner in self.owners.values() {
                for head in state::retained_heads(
                    owner,
                    self.own_id().as_deref() == Some(owner.cur.owner.as_str()),
                    now,
                ) {
                    graphs.push(self.graph(&head.pinset)?);
                }
            }
            if let Some(keep) = state::gc_keep(&graphs, &self.inflight) {
                let freed = self.store.retain(&keep)?;
                tracing::debug!(freed, "vault: garbage collection complete");
                self.last_gc = now;
            }
        }
        self.last_error = self.owners.values().find_map(|o| o.last_error.clone());
        self.save()
    }
}

struct Plan {
    room: String,
    hash: String,
    peers: Vec<String>,
    cids: Vec<String>,
    catalog: Option<String>,
}

impl Plan {
    fn limit(&self, cid: &str) -> usize {
        // Catalogs are one encrypted block up to 4 MiB. Other vault blocks
        // retain the normal 2 MiB receive cap.
        if self.catalog.as_deref() == Some(cid) {
            object::MAX_CATALOG_BYTES
        } else {
            BLOCK_LIMIT
        }
    }
}

impl Service {
    pub async fn tick(self: &Arc<Self>) -> Result<()> {
        if !self.app.network.permitted() {
            return Ok(());
        }
        let owners = self.work(|core| core.prepare()).await?;
        let budget = Arc::new(AtomicUsize::new(TICK_BYTES));
        for owner in owners {
            let mut attempted = HashSet::new();
            loop {
                let remaining = budget.load(Ordering::Relaxed);
                if !self.app.network.permitted() || remaining < BLOCK_LIMIT {
                    break;
                }
                let id = owner.clone();
                let max = (remaining / BLOCK_LIMIT).min(4);
                let plan = match self.work(move |core| core.fetch_plan(&id, max)).await {
                    Ok(Some(plan)) => plan,
                    Ok(None) => break,
                    Err(error) => {
                        self.owner_error(owner.clone(), error.to_string()).await;
                        break;
                    }
                };
                let mut tasks = tokio::task::JoinSet::new();
                for cid in &plan.cids {
                    if !attempted.insert(cid.clone()) {
                        continue;
                    }
                    let budget = budget.clone();
                    let max_bytes = plan.limit(cid);
                    let (room, cid, peers, service) = (
                        plan.room.clone(),
                        cid.clone(),
                        plan.peers.clone(),
                        self.clone(),
                    );
                    tasks.spawn(async move {
                        let result = async {
                            let mut last = anyhow::anyhow!("no block provider available");
                            for peer in peers
                                .iter()
                                .map(|p| Some(NodeId(p.clone())))
                                .chain(std::iter::once(None))
                            {
                                if !service.app.network.permitted() {
                                    anyhow::bail!("network is offline");
                                }
                                ensure!(
                                    budget
                                        .fetch_update(
                                            Ordering::Relaxed,
                                            Ordering::Relaxed,
                                            |remaining| remaining.checked_sub(max_bytes)
                                        )
                                        .is_ok(),
                                    "tick byte budget exhausted"
                                );
                                let options = FetchOptions {
                                    peer,
                                    max_bytes,
                                    timeout: Duration::from_secs(20),
                                };
                                match fetch_block(&room, &cid, options).await {
                                    Ok(bytes) => {
                                        ensure!(
                                            bytes.len() <= max_bytes,
                                            "fetched block exceeds limit"
                                        );
                                        budget
                                            .fetch_add(max_bytes - bytes.len(), Ordering::Relaxed);
                                        return Ok(bytes);
                                    }
                                    Err(error) => last = anyhow::anyhow!("{error}"),
                                }
                            }
                            Err(last)
                        };
                        let result = tokio::time::timeout(Duration::from_secs(60), result)
                            .await
                            .unwrap_or_else(|_| Err(anyhow::anyhow!("block fetch timed out")));
                        (cid, result)
                    });
                }
                if tasks.is_empty() {
                    let cids = plan.cids;
                    self.work(move |core| {
                        for cid in cids {
                            core.inflight.remove(&cid);
                        }
                        Ok(())
                    })
                    .await?;
                    break;
                }
                let mut failed = false;
                while let Some(task) = tasks.join_next().await {
                    let (cid, result) = task?;
                    let (id, hash) = (owner.clone(), plan.hash.clone());
                    let outcome = self.work(move |core| {
                        core.inflight.remove(&cid);
                        let bytes = result?;
                        ensure!(core.group.as_ref().is_some_and(|g| g.is_member(core.me.did())), "local node is not a member");
                        ensure!(core.owners.get(&id).is_some_and(|o| o.cur.hash() == hash && o.holding), "head changed during fetch");
                        ensure!(core.store.has(&cid) || bytes.len() as u64 <= core.free(), "insufficient capacity");
                        core.store.put(&cid, &bytes)?;
                        tracing::debug!(owner = %id, %cid, bytes = bytes.len(), "vault: fetched block");
                        Ok(())
                    }).await;
                    if let Err(error) = outcome {
                        failed = true;
                        self.owner_error(owner.clone(), error.to_string()).await;
                    }
                }
                let cids = plan.cids;
                self.work(move |core| {
                    for cid in cids {
                        core.inflight.remove(&cid);
                    }
                    Ok(())
                })
                .await?;
                if failed {
                    break;
                }
            }
            let id = owner.clone();
            let _ = self
                .work(move |core| {
                    let _ = core.fetch_plan(&id, 0)?;
                    Ok(())
                })
                .await;
        }
        self.work(|core| core.finish_tick()).await?;
        let messages = self
            .work(|core| {
                if now_ms().saturating_sub(core.last_hello) < 30_000 {
                    return Ok(Vec::new());
                }
                core.last_hello = now_ms();
                core.hello(None)
            })
            .await?;
        self.send(messages).await;
        Ok(())
    }

    async fn owner_error(self: &Arc<Self>, id: String, error: String) {
        tracing::debug!(owner = %id, %error, "vault: replication deferred");
        let _ = self
            .work(move |core| {
                if let Some(owner) = core.owners.get_mut(&id) {
                    owner.complete = false;
                    owner.last_error = Some(error.chars().take(512).collect());
                }
                core.save()
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::{group::Group, head::Head, keys::OwnerKey};

    #[test]
    fn missing_catalog_receives_its_full_format_limit() {
        let dir =
            std::env::temp_dir().join(format!("mistl-vault-plan-{:016x}", rand::random::<u64>()));
        let key = OwnerKey::generate();
        let me = Arc::new(OwnerKey::generate().identity());
        let cfg = crate::config::VaultConfig {
            min_free_bytes: 0,
            ..Default::default()
        };
        let mut core = Core::open(dir.clone(), cfg, me.clone()).unwrap();
        let group = Group::create(&me, now_ms()).unwrap();
        let catalog = object::raw_cid(&vec![0; 3 * 1024 * 1024]);
        let (pinset, bytes) = object::build_pinset(&catalog, &[]).unwrap();
        core.store.put(&pinset, &bytes).unwrap();
        let head = Head::sign(
            &key,
            &group.id,
            None,
            &pinset,
            3 * 1024 * 1024 + bytes.len() as u64,
            0,
            now_ms(),
        )
        .unwrap();
        core.group = Some(group);
        core.key = Some(key);
        let owner = head.owner.clone();
        state::accept_head(&mut core.owners, head, me.did(), now_ms()).unwrap();
        core.owners.get_mut(&owner).unwrap().holding = true;
        let plan = core.fetch_plan(&owner, 4).unwrap().unwrap();
        assert_eq!(plan.cids, vec![catalog.clone()]);
        assert_eq!(plan.limit(&catalog), object::MAX_CATALOG_BYTES);
        assert_eq!(plan.limit(&pinset), BLOCK_LIMIT);
        core.store.delete(&pinset).unwrap();
        let plan = core.fetch_plan(&owner, 4).unwrap().unwrap();
        assert_eq!(plan.cids, vec![pinset.clone()]);
        assert_eq!(plan.limit(&pinset), BLOCK_LIMIT);
        drop(core);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
