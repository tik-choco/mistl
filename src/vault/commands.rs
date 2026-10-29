//! Local commands, catalog transactions and status snapshots.
use super::{
    group::{Group, InviteCode},
    head::Head,
    keys::OwnerKey,
    object::{self, Catalog, CatalogEntry},
    service::{Core, Outbound, PendingJoin, now_ms},
    state,
};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::Path;

const INLINE_LIMIT: usize = 64 * 1024 * 1024;

fn required<'a>(args: &'a Value, field: &str) -> Result<&'a str> {
    args[field]
        .as_str()
        .with_context(|| format!("vault requires {field}"))
}

#[cfg(windows)]
fn replace_output(from: &Path, to: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let parent = to
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let from = std::fs::canonicalize(from)?;
    let to =
        std::fs::canonicalize(parent)?.join(to.file_name().context("missing output filename")?);
    let mut from: Vec<u16> = from.as_os_str().encode_wide().collect();
    let mut to: Vec<u16> = to.as_os_str().encode_wide().collect();
    ensure!(
        !from.contains(&0) && !to.contains(&0),
        "invalid output filename"
    );
    from.push(0);
    to.push(0);
    // SAFETY: both UTF-16 strings are terminated and live for this call.
    ensure!(
        unsafe {
            MoveFileExW(
                from.as_ptr(),
                to.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        } != 0,
        "replacing output: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(not(windows))]
fn replace_output(from: &Path, to: &Path) -> Result<()> {
    Ok(std::fs::rename(from, to)?)
}

impl Core {
    pub fn command(
        &mut self,
        cmd: &str,
        args: Value,
        running: bool,
    ) -> Result<(Value, Vec<Outbound>)> {
        let mut out = Vec::new();
        let result = match cmd {
            "vault.status" => self.status(running),
            "vault.init" | "vault.join" => {
                if cmd == "vault.join" && self.group.is_some() {
                    let text = required(&args, "code")?.to_string();
                    let code = InviteCode::parse(&text)?;
                    let group = self.group.as_ref().context("missing group")?;
                    ensure!(
                        code.group == group.id,
                        "vault already belongs to another group"
                    );
                    ensure!(code.expires_at > now_ms(), "invite has expired");
                    let key = self.key.as_ref().context("vault has no owner key")?;
                    let result = json!({"group":group.id,"room":group.room(),"owner":key.owner_did(),"recovery_code":key.recovery_code()});
                    if !group.is_member(self.me.did()) {
                        self.join = Some(PendingJoin {
                            code,
                            text,
                            started: now_ms(),
                            last_sent: 0,
                        });
                        self.save()?;
                    }
                    return Ok((result, out));
                }
                ensure!(self.group.is_none(), "vault already belongs to a group");
                let now = now_ms();
                let (group, pending) = if cmd == "vault.join" {
                    let code = InviteCode::parse(required(&args, "code")?)?;
                    ensure!(code.expires_at > now, "invite has expired");
                    (
                        Group::from_invite(&code)?,
                        Some(PendingJoin {
                            code,
                            text: required(&args, "code")?.to_string(),
                            started: now,
                            last_sent: 0,
                        }),
                    )
                } else {
                    (Group::create(&self.me, now)?, None)
                };
                let key = OwnerKey::generate();
                key.save(&self.dir)?;
                group.save(&self.dir)?;
                let result = json!({"group":group.id,"room":group.room(),"owner":key.owner_did(),"recovery_code":key.recovery_code()});
                self.group = Some(group);
                self.key = Some(key);
                self.join = pending;
                self.publish(&Catalog {
                    v: 1,
                    ..Catalog::default()
                })?;
                result
            }
            "vault.invite" => {
                let mut group = self.group.clone().context("vault is not initialized")?;
                let (code, expires_at) = group.invite(&self.me, now_ms())?;
                group.save(&self.dir)?;
                self.group = Some(group);
                json!({"code":code,"expires_at":expires_at})
            }
            "vault.members.remove" | "vault.leave" => {
                let did = if cmd == "vault.leave" {
                    self.me.did()
                } else {
                    required(&args, "did")?
                };
                let mut group = self.group.clone().context("vault is not initialized")?;
                group.revoke(&self.me, did, now_ms())?;
                group.save(&self.dir)?;
                self.peers.remove(did);
                self.last_seen.remove(did);
                self.group = Some(group);
                self.save()?;
                out.extend(self.member_messages(None)?);
                if cmd == "vault.leave" {
                    self.join = None;
                    json!({"left":true})
                } else {
                    json!({"removed":true})
                }
            }
            "vault.key.show" => {
                let key = self.key.as_ref().context("vault has no owner key")?;
                json!({"recovery_code":key.recovery_code(),"owner":key.owner_did()})
            }
            "vault.restore" => {
                ensure!(
                    self.group.is_some(),
                    "initialize or join a vault before restoring"
                );
                let key = OwnerKey::from_recovery_code(required(&args, "code")?)?;
                let owner = key.owner_did();
                ensure!(
                    self.owners.contains_key(&owner) || self.owners.len() < state::MAX_OWNERS,
                    "vault owner limit reached"
                );
                // A durable marker prevents an empty seq=1 publish after restoring
                // an owner whose current head has not arrived yet.
                crate::statefile::write_private(
                    &self.dir.join("restore.pending"),
                    owner.as_bytes(),
                )?;
                key.save(&self.dir)?;
                self.key = Some(key);
                if let Some(record) = self.owners.get_mut(&owner) {
                    record.holding = true;
                    record.home = self.me.did().into();
                }
                self.save()?;
                out.push(self.message(None, "head_req", json!({"owner":owner}))?);
                match self.owners.get(&owner) {
                    Some(record) => json!({"owner":owner,"seq":record.cur.seq}),
                    None => json!({"owner":owner}),
                }
            }
            "vault.ls" => {
                let owner = self.own_id().context("vault has no owner key")?;
                ensure!(
                    args.get("owner")
                        .is_none_or(|v| v.as_str() == Some(owner.as_str())),
                    "catalog belongs to another owner; restore its recovery key first"
                );
                let catalog = self.catalog()?;
                let files: Vec<_> = catalog.files.iter().map(|(name, e)| json!({"name":name,"size":e.size,"mtime":e.mtime,"sha256":e.sha256,"object":e.object})).collect();
                json!({"files":files,"seq":self.owners.get(&owner).map_or(0, |o| o.cur.seq)})
            }
            "vault.put" => {
                let mut catalog = self.catalog()?;
                let (name, encrypted) = if let Some(path) = args.get("path") {
                    ensure!(args.get("data_b64").is_none(), "choose path or data_b64");
                    let path = Path::new(path.as_str().context("invalid path")?);
                    let name = args
                        .get("name")
                        .and_then(Value::as_str)
                        .or_else(|| path.file_name().and_then(|n| n.to_str()))
                        .context("missing file name")?;
                    ensure!(object::valid_name(name), "invalid vault file name");
                    // Check capacity before encrypting: ciphertext adds 16 bytes per
                    // segment, and segments are written as they are produced.
                    let size = std::fs::metadata(path)
                        .context("reading vault input metadata")?
                        .len();
                    ensure!(
                        size.saturating_add(size / 65536 + 1024 * 1024) <= self.free(),
                        "insufficient capacity"
                    );
                    let store = &self.store;
                    let encrypted = object::encrypt_file(
                        path,
                        self.cfg.max_object_bytes,
                        &mut |cid, bytes| store.put(cid, bytes),
                    )?;
                    (name.to_string(), encrypted)
                } else {
                    let name = required(&args, "name")?;
                    ensure!(object::valid_name(name), "invalid vault file name");
                    let encoded = required(&args, "data_b64")?;
                    ensure!(
                        encoded.len() <= INLINE_LIMIT.div_ceil(3) * 4,
                        "inline upload exceeds 64 MiB"
                    );
                    let bytes = STANDARD.decode(encoded).context("invalid base64")?;
                    ensure!(
                        bytes.len() <= INLINE_LIMIT
                            && bytes.len() as u64 <= self.cfg.max_object_bytes,
                        "object exceeds size limit"
                    );
                    (name.to_string(), object::encrypt_object(&bytes)?)
                };
                ensure!(
                    catalog.files.contains_key(&name)
                        || catalog.files.len() < object::MAX_PINSET_ROOTS - 1,
                    "catalog file limit reached"
                );
                self.write_blocks(&encrypted.blocks)?;
                catalog.files.insert(
                    name.clone(),
                    CatalogEntry {
                        object: encrypted.root,
                        dek: hex::encode(encrypted.dek),
                        size: encrypted.size,
                        mtime: now_ms(),
                        sha256: encrypted.sha256,
                    },
                );
                let head = self.publish(&catalog)?;
                out.push(self.message(None, "head", json!({"head":head.raw}))?);
                json!({"name":name,"size":encrypted.size,"seq":head.seq})
            }
            "vault.get" => {
                let name = required(&args, "name")?;
                ensure!(object::valid_name(name), "invalid vault file name");
                let catalog = self.catalog()?;
                let entry = catalog.files.get(name).context("vault file not found")?;
                let output = args
                    .get("output")
                    .map(|v| v.as_str().context("invalid output path"))
                    .transpose()?;
                ensure!(
                    output.is_some() || entry.size <= INLINE_LIMIT as u64,
                    "download exceeds 64 MiB; supply output"
                );
                let descriptor = object::parse_descriptor(
                    &self
                        .store
                        .get(&entry.object)
                        .context("object descriptor is not available locally; sync first")?,
                )?;
                let dek: [u8; 32] = hex::decode(&entry.dek)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid object key"))?;
                if let Some(output) = output {
                    self.write_plain_file(Path::new(output), &descriptor, &dek, entry)?;
                    json!({"name":name,"size":entry.size,"written":output})
                } else {
                    // Validate the descriptor's implied size before allocating plaintext.
                    ensure!(
                        descriptor.bytes <= INLINE_LIMIT as u64 + (object::MAX_CHUNKS as u64 * 16),
                        "inline download exceeds size limit"
                    );
                    let bytes =
                        object::decrypt_object(&descriptor, &dek, &|cid| self.store.get(cid))?;
                    ensure!(
                        bytes.len() as u64 == entry.size
                            && hex::encode(Sha256::digest(&bytes)) == entry.sha256,
                        "plaintext integrity mismatch"
                    );
                    json!({"name":name,"size":entry.size,"data_b64":STANDARD.encode(bytes)})
                }
            }
            "vault.rm" => {
                let name = required(&args, "name")?;
                let mut catalog = self.catalog()?;
                ensure!(catalog.files.remove(name).is_some(), "vault file not found");
                let head = self.publish(&catalog)?;
                out.push(self.message(None, "head", json!({"head":head.raw}))?);
                json!({"seq":head.seq})
            }
            "vault.sync" => json!({}),
            _ => bail!("unknown vault command: {cmd}"),
        };
        Ok((result, out))
    }

    pub fn catalog(&self) -> Result<Catalog> {
        let key = self.key.as_ref().context("vault has no owner key")?;
        let Some(owner) = self.owners.get(&key.owner_did()) else {
            ensure!(
                !self.dir.join("restore.pending").exists(),
                "restored owner head is not available yet; sync first"
            );
            return Ok(Catalog {
                v: 1,
                ..Catalog::default()
            });
        };
        let pinset = object::parse_pinset(
            &self
                .store
                .get(&owner.cur.pinset)
                .context("pinset is not available locally; sync first")?,
        )?;
        let cid = pinset.roots.first().context("missing catalog root")?;
        object::open_catalog(
            &self
                .store
                .get(cid)
                .context("catalog is not available locally; sync first")?,
            key,
        )
    }

    fn write_blocks(&self, blocks: &[object::Block]) -> Result<()> {
        let needed = blocks
            .iter()
            .filter(|(cid, _)| !self.store.has(cid))
            .try_fold(0u64, |n, (_, b)| {
                n.checked_add(b.len() as u64).context("block size overflow")
            })?;
        ensure!(needed <= self.free(), "insufficient capacity");
        for (cid, bytes) in blocks {
            self.store.put(cid, bytes)?;
        }
        Ok(())
    }

    fn publish(&mut self, catalog: &Catalog) -> Result<Head> {
        let key = self.key.as_ref().context("vault has no owner key")?;
        let group = self.group.as_ref().context("vault is not initialized")?;
        let owner = key.owner_did();
        ensure!(
            self.owners.contains_key(&owner) || self.owners.len() < state::MAX_OWNERS,
            "vault owner limit reached"
        );
        let catalog_block = object::seal_catalog(catalog, key)?;
        let roots: Vec<_> = catalog.files.values().map(|e| e.object.clone()).collect();
        let pinset = object::build_pinset(&catalog_block.0, &roots)?;
        self.write_blocks(&[catalog_block, pinset.clone()])?;
        let graph = self.graph(&pinset.0)?;
        ensure!(
            graph.expanded && graph.missing.is_empty(),
            "dataset blocks are not all available; sync before publishing"
        );
        let head = Head::sign(
            key,
            &group.id,
            self.owners.get(&owner).map(|o| &o.cur),
            &pinset.0,
            graph.bytes,
            catalog.files.len() as u64,
            now_ms(),
        )?;
        let previous = self.owners.clone();
        state::accept_head(&mut self.owners, head.clone(), self.me.did(), now_ms())?;
        if let Some(record) = self.owners.get_mut(&owner) {
            record.holding = true;
            record.complete = true;
            record.held_bytes = graph.bytes;
            record.missing = 0;
        }
        if let Err(error) = self.save() {
            self.owners = previous;
            return Err(error);
        }
        tracing::info!(%owner, seq = head.seq, files = head.files, "vault: published head");
        Ok(head)
    }

    fn write_plain_file(
        &self,
        path: &Path,
        descriptor: &object::Descriptor,
        dek: &[u8; 32],
        entry: &CatalogEntry,
    ) -> Result<()> {
        use aes_gcm::{
            Aes256Gcm, KeyInit, Nonce,
            aead::{Aead, Payload},
        };
        use std::io::Write;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let temp = parent.join(format!(".mistl-vault-{:016x}.tmp", rand::random::<u64>()));
        let mut created = false;
        let result = (|| -> Result<()> {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp)?;
            created = true;
            let cipher =
                Aes256Gcm::new_from_slice(dek).map_err(|_| anyhow::anyhow!("invalid key"))?;
            let mut digest = Sha256::new();
            let mut size = 0u64;
            for (i, cid) in descriptor.chunks.iter().enumerate() {
                let bytes = self
                    .store
                    .get(cid)
                    .context("object chunk is not available locally; sync first")?;
                let mut nonce = [0u8; 12];
                nonce[4..].copy_from_slice(&(i as u64).to_be_bytes());
                let mut aad = b"mistl-vault-seg-v1".to_vec();
                aad.extend_from_slice(&(i as u64).to_be_bytes());
                aad.extend_from_slice(&(descriptor.chunks.len() as u64).to_be_bytes());
                let plain = cipher
                    .decrypt(
                        Nonce::from_slice(&nonce),
                        Payload {
                            msg: &bytes,
                            aad: &aad,
                        },
                    )
                    .map_err(|_| anyhow::anyhow!("object authentication failed"))?;
                ensure!(plain.len() <= object::SEGMENT, "oversized object segment");
                size = size
                    .checked_add(plain.len() as u64)
                    .context("object size overflow")?;
                ensure!(size <= entry.size, "plaintext size mismatch");
                digest.update(&plain);
                file.write_all(&plain)?;
            }
            ensure!(
                size == entry.size && hex::encode(digest.finalize()) == entry.sha256,
                "plaintext integrity mismatch"
            );
            file.sync_all()?;
            drop(file);
            replace_output(&temp, path)?;
            Ok(())
        })();
        if result.is_err() && created {
            let _ = std::fs::remove_file(temp);
        }
        result
    }

    pub fn desired(&self, owner: &state::Owner, now: u64) -> Vec<String> {
        let Some(group) = &self.group else {
            return Vec::new();
        };
        let free = self.free();
        let candidates: Vec<_> = group
            .members()
            .into_iter()
            .filter_map(|did| {
                let available = if did == self.me.did() {
                    free.saturating_add(if owner.holding { owner.held_bytes } else { 0 })
                } else {
                    let peer = self.peers.get(&did)?;
                    if now.saturating_sub(peer.last_seen)
                        > self.cfg.dead_after_secs.saturating_mul(1000)
                    {
                        return None;
                    }
                    peer.free.saturating_add(
                        peer.holds
                            .iter()
                            .find(|h| h.owner == owner.cur.owner)
                            .map_or(0, |h| h.bytes.min(owner.cur.bytes)),
                    )
                };
                Some(super::placement::Candidate { did, available })
            })
            .collect();
        let home = if self.own_id().as_deref() == Some(owner.cur.owner.as_str()) {
            Some(self.me.did())
        } else if candidates.iter().any(|c| c.did == owner.home) {
            Some(owner.home.as_str())
        } else {
            None
        };
        super::placement::place(
            &owner.cur.owner,
            owner.cur.bytes,
            self.cfg.replicas,
            home,
            &candidates,
        )
    }

    pub fn status(&self, running: bool) -> Value {
        let now = now_ms();
        let members = self.group.as_ref().map_or_else(Vec::new, Group::members);
        let own = self.own_id();
        let group = self.group.as_ref().map(|g| {
            let entries: Vec<_> = members.iter().map(|did| {
                let peer = self.peers.get(did);
                let myself = did == self.me.did();
                json!({"did":did,"node":crate::identity::node_id_for_did(did),"self":myself,
                    "online":running && (myself || peer.is_some_and(|p| now.saturating_sub(p.last_seen) <= state::ONLINE_MS)),
                    "last_seen":if myself { now } else { self.last_seen.get(did).copied().unwrap_or(0) },
                    "free":if myself { self.free() } else { peer.map_or(0, |p| p.free) },
                    "capacity":if myself { self.cfg.capacity_bytes } else { peer.map_or(0, |p| p.capacity) }})
            }).collect();
            json!({"id":g.id,"room":g.room(),"members":entries,"digest":g.digest()})
        });
        let owners: Vec<_> = self.owners.values().map(|owner| {
            let mut holders = Vec::new();
            let mut confirmed = 0;
            for did in &members {
                if did == self.me.did() {
                    if owner.holding {
                        confirmed += usize::from(owner.complete);
                        holders.push(json!({"did":did,"complete":owner.complete,"seq":owner.cur.seq}));
                    }
                } else if let Some(peer) = self.peers.get(did).filter(|p| running && now.saturating_sub(p.last_seen) <= state::ONLINE_MS) {
                    if let Some(summary) = peer.holds.iter().find(|h| h.owner == owner.cur.owner) {
                        let complete = state::confirms(summary, &owner.cur);
                        confirmed += usize::from(complete);
                        holders.push(json!({"did":did,"complete":complete,"seq":summary.seq}));
                    }
                }
            }
            let target = self.cfg.replicas.min(members.len());
            let insufficient = owner.last_error.as_deref() == Some("insufficient capacity");
            json!({"owner":owner.cur.owner,"self_owned":own.as_deref() == Some(owner.cur.owner.as_str()),
                "seq":owner.cur.seq,"files":owner.cur.files,"bytes":owner.cur.bytes,"target":target,
                "holders":holders,"confirmed":confirmed,"state":state::classification(owner.fork, insufficient, owner.complete, confirmed, target, members.len()),"fork":owner.fork})
        }).collect();
        json!({"group":group,"network":running,"local":{"used":self.store.usage(),"capacity":self.cfg.capacity_bytes,"free_advertised":self.free()},"owners":owners,"last_error":self.last_error})
    }
}
