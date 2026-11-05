//! Capability storage, transport presentation and local commands.
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration as StdDuration, Instant};

use anyhow::{Context as _, Result, bail, ensure};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::capability::{self, Capability};
use super::{Action, Limits, Principal, STORE, Service, Store, field, optional_field};
use crate::identity::Identity;

#[cfg(test)]
#[path = "delegation_tests.rs"]
mod tests;

const MAX_HELD: usize = 256;
/// Held capabilities a single issuer may push via `mistl-cap-deliver-v1`.
const MAX_DELIVERED_PER_ISSUER: usize = 16;
const MAX_ISSUED: usize = 1024;
const MAX_PRESENTED: usize = 1024;
const MAX_PER_NODE: usize = 8;
const MAX_REVOKED: usize = 4096;
const MAX_THROTTLES: usize = 1024;
const PRESENT: &str = "mistl-cap-present-v1";
const DELIVER: &str = "mistl-cap-deliver-v1";
const REVOKE: &str = "mistl-cap-revoke-v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct Issued {
    cap: Value,
    revoked: bool,
}

impl Default for Issued {
    fn default() -> Self {
        Self {
            cap: Value::Null,
            revoked: false,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct Revoked {
    id: String,
    exp: i64,
    at: i64,
}

#[derive(Clone, Debug)]
struct Presented {
    node: String,
    did: String,
    cap: Capability,
    at: u64,
    // Exact-slot owner authority is rechecked against the live head at request time.
    trusted: bool,
}

#[derive(Clone, Default)]
pub(super) struct Book {
    issued: Vec<Issued>,
    held: Vec<Value>,
    revoked: Vec<Revoked>,
    presented: Vec<Presented>,
    receive: HashMap<String, Instant>,
    sequence: u64,
    pub dirty: bool,
}

fn validate_caps(caps: &[Value], max: usize) -> Result<()> {
    ensure!(caps.len() <= max, "too many capabilities");
    for cap in caps {
        capability::parse(cap)?;
    }
    Ok(())
}

impl Book {
    pub fn mark_trusted(&mut self, node: &str, id: &str) {
        for p in &mut self.presented {
            if p.node == node && p.cap.id == id {
                p.trusted = true;
            }
        }
    }
    pub fn load(dir: &Path, now: DateTime<Utc>) -> Self {
        Self {
            issued: super::load_or_recover(dir, "issued.json", now, |v: &Vec<Issued>| {
                validate_caps(
                    &v.iter().map(|i| i.cap.clone()).collect::<Vec<_>>(),
                    MAX_ISSUED,
                )
            }),
            held: super::load_or_recover(dir, "held.json", now, |v: &Vec<Value>| {
                validate_caps(v, MAX_HELD)
            }),
            revoked: super::load_or_recover(dir, "revoked.json", now, |v: &Vec<Revoked>| {
                ensure!(
                    v.len() <= MAX_REVOKED
                        && v.iter()
                            .all(|r| capability::valid_id(&r.id) && r.exp >= r.at && r.at >= 0),
                    "invalid revocations"
                );
                Ok(())
            }),
            ..Self::default()
        }
    }

    pub fn limits_for(&self, id: &str) -> Option<Limits> {
        self.presented
            .iter()
            .find(|p| p.cap.id == id)
            .and_then(|p| p.cap.limits.clone())
            .or_else(|| {
                self.issued
                    .iter()
                    .map(|i| &i.cap)
                    .chain(self.held.iter())
                    .filter_map(|v| capability::parse(v).ok())
                    .find(|c| c.id == id)
                    .and_then(|c| c.limits)
            })
    }

    fn is_revoked(&self, id: &str, now: i64) -> bool {
        self.revoked.iter().any(|r| r.id == id && r.exp > now)
            || self
                .issued
                .iter()
                .any(|i| i.revoked && i.cap["id"].as_str() == Some(id))
    }

    fn live(&self, cap: &Capability, now: i64) -> bool {
        cap.chain()
            .iter()
            .all(|c| c.exp > now && !self.is_revoked(&c.id, now))
    }

    fn prune(&mut self, now: i64) {
        let before = (self.held.len(), self.revoked.len(), self.issued.len());
        self.held
            .retain(|v| capability::parse(v).is_ok_and(|c| c.exp > now));
        self.issued
            .retain(|i| i.cap["exp"].as_i64().is_some_and(|exp| exp > now));
        self.revoked.retain(|r| r.exp > now);
        let revoked = self
            .revoked
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>();
        self.presented.retain(|p| {
            p.cap
                .chain()
                .iter()
                .all(|c| c.exp > now && !revoked.contains(&c.id.as_str()))
        });
        self.dirty |= before != (self.held.len(), self.revoked.len(), self.issued.len());
    }

    fn revoke(&mut self, id: &str, now: i64) {
        let exp = self
            .presented
            .iter()
            .map(|p| &p.cap)
            .flat_map(Capability::chain)
            .filter(|c| c.id == id)
            .map(|c| c.exp)
            .chain(
                self.issued
                    .iter()
                    .map(|i| &i.cap)
                    .chain(self.held.iter())
                    .filter_map(|v| capability::parse(v).ok())
                    .flat_map(|c| {
                        c.chain()
                            .into_iter()
                            .filter(|link| link.id == id)
                            .map(|link| link.exp)
                            .collect::<Vec<_>>()
                    }),
            )
            .chain(self.revoked.iter().filter(|r| r.id == id).map(|r| r.exp))
            .max()
            .unwrap_or(now.saturating_add(capability::MAX_LIFETIME_MS));
        if exp <= now {
            self.presented.retain(|p| !p.cap.contains_id(id));
            return;
        }
        if let Some(old) = self.revoked.iter_mut().find(|r| r.id == id) {
            old.exp = old.exp.max(exp);
        } else {
            if self.revoked.len() >= MAX_REVOKED {
                let oldest = self
                    .revoked
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, r)| r.at)
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                self.revoked.remove(oldest);
            }
            self.revoked.push(Revoked {
                id: id.into(),
                exp,
                at: now,
            });
        }
        self.presented.retain(|p| !p.cap.contains_id(id));
        self.dirty = true;
    }

    fn take_receive(&mut self, from: &str, now: Instant) -> bool {
        self.receive
            .retain(|_, t| now.saturating_duration_since(*t) < StdDuration::from_secs(1));
        if self.receive.contains_key(from) || self.receive.len() >= MAX_THROTTLES {
            return false;
        }
        self.receive.insert(from.into(), now);
        true
    }

    fn insert_presented(&mut self, node: &str, did: &str, cap: Capability, trusted: bool) {
        self.presented
            .retain(|p| !(p.node == node && p.cap.id == cap.id));
        if self.presented.iter().filter(|p| p.node == node).count() >= MAX_PER_NODE
            && let Some(oldest) = self
                .presented
                .iter()
                .enumerate()
                .filter(|(_, p)| p.node == node)
                .min_by_key(|(_, p)| p.at)
                .map(|(i, _)| i)
        {
            self.presented.remove(oldest);
        }
        if self.presented.len() >= MAX_PRESENTED
            && let Some(oldest) = self
                .presented
                .iter()
                .enumerate()
                .min_by_key(|(_, p)| p.at)
                .map(|(i, _)| i)
        {
            self.presented.remove(oldest);
        }
        self.sequence = self.sequence.saturating_add(1);
        self.presented.push(Presented {
            node: node.into(),
            did: did.into(),
            cap,
            at: self.sequence,
            trusted,
        });
    }
}

impl Store {
    pub(super) fn presented_access_allowed(
        &self,
        service: Service,
        principal: &Principal,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(did) = principal.did.as_deref() else {
            return false;
        };
        self.caps
            .presented
            .iter()
            .filter(|p| p.node == principal.node && p.did == did && p.cap.service == service)
            .any(|p| {
                self.verify_cap(
                    &p.cap.wire,
                    did,
                    service,
                    p.cap.actions[0],
                    &p.cap.resource,
                    None,
                    now,
                )
                .is_ok()
            })
    }

    pub(super) fn save_caps(&self) -> Result<()> {
        self.save("issued.json", &self.caps.issued)?;
        self.save("held.json", &self.caps.held)?;
        self.save("revoked.json", &self.caps.revoked)
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_cap(
        &self,
        wire: &Value,
        holder: &str,
        service: Service,
        action: Action,
        resource: &str,
        owner: Option<&str>,
        now: DateTime<Utc>,
    ) -> std::result::Result<Capability, capability::Error> {
        let local = self.local_did.as_deref().unwrap_or("");
        capability::verify(
            wire,
            &capability::Context {
                now_ms: now.timestamp_millis(),
                holder,
                local_did: local,
                service,
                action,
                resource,
                trusted_root: |did: &str| {
                    did == local
                        || self
                            .policy
                            .service(service)
                            .issuers
                            .iter()
                            .any(|d| d == did)
                        || (service == Service::Escrow
                            && capability::exact_resource(resource).is_some()
                            && owner == Some(did))
                },
                is_revoked: |id: &str| self.caps.is_revoked(id, now.timestamp_millis()),
            },
        )
    }

    pub(super) fn presented_cap(
        &self,
        service: Service,
        action: Action,
        principal: &Principal,
        resource: Option<&str>,
        owner: Option<&str>,
        now: DateTime<Utc>,
    ) -> Option<Capability> {
        let did = principal.did.as_deref()?;
        self.caps
            .presented
            .iter()
            .filter(|p| p.node == principal.node && p.did == did)
            .find_map(|p| {
                self.verify_cap(
                    &p.cap.wire,
                    did,
                    service,
                    action,
                    resource.unwrap_or("*"),
                    owner,
                    now,
                )
                .ok()
            })
    }

    fn import_cap(
        &mut self,
        wire: Value,
        identity: &Identity,
        now: DateTime<Utc>,
    ) -> Result<Value> {
        let cap = capability::parse(&wire)?;
        // A holder need not trust the issuer as a provider. It must still check
        // the complete chain, signatures, audience, validity and attenuation.
        capability::verify(
            &wire,
            &capability::Context {
                now_ms: now.timestamp_millis(),
                holder: identity.did(),
                local_did: cap
                    .to
                    .as_ref()
                    .and_then(|v| v.first())
                    .map(String::as_str)
                    .unwrap_or(identity.did()),
                service: cap.service,
                action: cap.actions[0],
                resource: &cap.resource,
                trusted_root: |_: &str| true,
                is_revoked: |id: &str| self.caps.is_revoked(id, now.timestamp_millis()),
            },
        )?;
        if let Some(old) = self.caps.held.iter_mut().find(|v| v["id"] == wire["id"]) {
            ensure!(
                *old == wire,
                "capability id collides with a different held capability"
            );
        } else {
            ensure!(
                self.caps.held.len() < MAX_HELD,
                "too many held capabilities"
            );
            self.caps.held.push(wire.clone());
        }
        self.caps.dirty = true;
        Ok(wire)
    }

    fn cap_command(
        &mut self,
        cmd: &str,
        args: &Value,
        identity: &Identity,
        now: DateTime<Utc>,
    ) -> Result<Value> {
        ensure!(args.is_object(), "arguments must be an object");
        let mut next = self.clone();
        next.local_did = Some(identity.did().into());
        next.caps.prune(now.timestamp_millis());
        let result = match cmd {
            "authz.cap.issue" => {
                ensure!(
                    next.caps.issued.len() < MAX_ISSUED,
                    "too many issued capabilities"
                );
                let aud: String = field(args, "aud")?;
                let service: Service = field(args, "service")?;
                let actions: Vec<Action> = field(args, "actions")?;
                let parent_id: Option<String> = optional_field(args, "parent")?;
                let parent = parent_id
                    .as_ref()
                    .map(|id| {
                        next.caps
                            .held
                            .iter()
                            .find(|v| v["id"].as_str() == Some(id))
                            .cloned()
                            .context("unknown held parent capability")
                    })
                    .transpose()?;
                let parent_cap = parent.as_ref().map(capability::parse).transpose()?;
                if let Some(parent) = &parent_cap {
                    ensure!(
                        parent.aud == identity.did()
                            && next.caps.live(parent, now.timestamp_millis()),
                        "parent is not a live held capability"
                    );
                }
                let expiry = super::expiry(args, now)?.unwrap_or_else(|| now + Duration::days(7));
                let resource: String =
                    optional_field(args, "resource")?.unwrap_or_else(|| "*".into());
                let mut cap = json!({"t":"mistl-cap-v1", "id":crate::net::peer_auth::fresh_nonce(),
                    "fromId":identity.did(), "aud":aud, "svc":service, "act":actions,
                    "res":resource, "nbf":now.timestamp_millis(), "exp":expiry.timestamp_millis()});
                if let Some(to) = args.get("to") {
                    cap["to"] = to.clone();
                }
                if let Some(limits) = args.get("limits") {
                    cap["lim"] = limits.clone();
                }
                if let Some(parent) = parent {
                    cap["prf"] = parent;
                }
                let parsed = capability::parse(&cap)?;
                if let Some(parent) = parsed.parent.as_deref() {
                    ensure!(
                        capability::attenuates(&parsed, parent),
                        "delegation widens parent authority"
                    );
                }
                crate::wiresign::sign_wire(&mut cap, identity)?;
                // Also validates ancestors after signing and rejects stale/tampered held state.
                let parsed = capability::parse(&cap)?;
                capability::verify(
                    &cap,
                    &capability::Context {
                        now_ms: now.timestamp_millis(),
                        holder: &parsed.aud,
                        local_did: parsed
                            .to
                            .as_ref()
                            .and_then(|v| v.first())
                            .map(String::as_str)
                            .unwrap_or(identity.did()),
                        service,
                        action: parsed.actions[0],
                        resource: &parsed.resource,
                        trusted_root: |_: &str| true,
                        is_revoked: |id: &str| next.caps.is_revoked(id, now.timestamp_millis()),
                    },
                )?;
                next.caps.issued.push(Issued {
                    cap: cap.clone(),
                    revoked: false,
                });
                json!({"cap":cap,"delivered":false})
            }
            "authz.cap.import" => next.import_cap(cap_arg(args)?, identity, now)?,
            "authz.cap.remove" => {
                let id: String = field(args, "id")?;
                let before = next.caps.held.len();
                next.caps.held.retain(|v| v["id"].as_str() != Some(&id));
                json!({"removed": before != next.caps.held.len()})
            }
            "authz.cap.revoke" => {
                let id: String = field(args, "id")?;
                ensure!(capability::valid_id(&id), "invalid capability id");
                let local: bool = optional_field(args, "local")?.unwrap_or(false);
                if !local {
                    let issued = next.caps.issued.iter_mut().find(|i| i.cap["id"].as_str() == Some(&id) && i.cap["fromId"].as_str() == Some(identity.did())).context("capability was not issued by this node; use local:true for a local revocation")?;
                    issued.revoked = true;
                }
                next.caps.revoke(&id, now.timestamp_millis());
                json!({"revoked":true,"broadcast":false})
            }
            "authz.cap.list" => json!({"issued":next.caps.issued,"held":next.caps.held,
                "presented":next.caps.presented.iter().map(|p| json!({"node":p.node,"did":p.did,"cap":{"id":p.cap.id,"fromId":p.cap.issuer,"aud":p.cap.aud,"svc":p.cap.service,"act":p.cap.actions,"res":p.cap.resource,"exp":p.cap.exp,"lim":p.cap.limits}})).collect::<Vec<_>>(),
                "revoked":next.caps.revoked}),
            "authz.cap.verify" => {
                let wire = cap_arg(args)?;
                match capability::parse(&wire) {
                    Err(error) => {
                        json!({"ok":false,"reason":error.to_string(),"root":null,"chain":[]})
                    }
                    Ok(cap) => {
                        let service = optional_field(args, "service")?.unwrap_or(cap.service);
                        let action = optional_field(args, "action")?.unwrap_or(cap.actions[0]);
                        let resource: String = optional_field(args, "resource")?
                            .unwrap_or_else(|| cap.resource.clone());
                        let verification =
                            next.verify_cap(&wire, &cap.aud, service, action, &resource, None, now);
                        let mut result = json!({"ok":verification.is_ok(),"root":cap.root().issuer,"chain":cap.chain().iter().map(|c| &c.id).collect::<Vec<_>>()});
                        if let Err(error) = verification {
                            result["reason"] = json!(error.to_string());
                        }
                        result
                    }
                }
            }
            _ => bail!("unknown capability command: {cmd}"),
        };
        next.save_caps()?;
        next.caps.dirty = false;
        *self = next;
        Ok(result)
    }

    fn receive_cap(
        &mut self,
        from: &str,
        holder: Option<&str>,
        wire: &Value,
        identity: &Identity,
        now: DateTime<Utc>,
    ) -> bool {
        match wire["t"].as_str() {
            Some(PRESENT) => {
                let Some(holder) = holder else {
                    return false;
                };
                let Ok(cap) = capability::parse(&wire["cap"]) else {
                    return false;
                };
                let result = self.verify_cap(
                    &cap.wire,
                    holder,
                    cap.service,
                    cap.actions[0],
                    &cap.resource,
                    None,
                    now,
                );
                let trusted = result.is_ok();
                // The escrow repository is private to the service. A bounded
                // exact-slot candidate can be retained after all cryptographic
                // checks; only authorize_p2p_owned can grant its owner authority.
                let owner_candidate = matches!(result, Err(capability::Error::UntrustedRoot))
                    && cap.service == Service::Escrow
                    && capability::exact_resource(&cap.resource).is_some();
                if !trusted && !owner_candidate {
                    return false;
                }
                self.caps.insert_presented(from, holder, cap, trusted);
                false
            }
            Some(DELIVER) => {
                // Only the issuer's own node may push a capability, and one
                // issuer can occupy at most MAX_DELIVERED_PER_ISSUER held
                // slots, so strangers cannot crowd out real grants.
                let Some(issuer) = wire["cap"]["fromId"].as_str() else {
                    return false;
                };
                if holder != Some(issuer) {
                    return false;
                }
                let from_issuer = self
                    .caps
                    .held
                    .iter()
                    .filter(|v| {
                        v["fromId"].as_str() == Some(issuer) && v["id"] != wire["cap"]["id"]
                    })
                    .count();
                if from_issuer >= MAX_DELIVERED_PER_ISSUER {
                    return false;
                }
                self.import_cap(wire["cap"].clone(), identity, now).is_ok()
            }
            Some(REVOKE) => {
                let Some(did) = wire["fromId"].as_str() else {
                    return false;
                };
                if crate::identity::node_id_for_did(did) != from || holder != Some(did) {
                    return false;
                }
                let Some(ts) = wire["ts"].as_i64() else {
                    return false;
                };
                if ts.abs_diff(now.timestamp_millis()) > crate::net::peer_auth::MAX_CLOCK_SKEW_MS {
                    return false;
                }
                let Some(ids) = wire["ids"]
                    .as_array()
                    .filter(|v| v.len() <= 64 && !v.is_empty())
                else {
                    return false;
                };
                if ids
                    .iter()
                    .any(|id| !id.as_str().is_some_and(capability::valid_id))
                {
                    return false;
                }
                let trusted = did == identity.did()
                    || self
                        .policy
                        .ai
                        .issuers
                        .iter()
                        .chain(&self.policy.escrow.issuers)
                        .any(|d| d == did)
                    || self.caps.presented.iter().any(|p| {
                        p.trusted
                            && self.caps.live(&p.cap, now.timestamp_millis())
                            && p.cap.chain().iter().any(|c| c.issuer == did)
                    });
                if !trusted || !crate::wiresign::verify_wire(wire).unwrap_or(false) {
                    return false;
                }
                for id in ids.iter().filter_map(Value::as_str) {
                    self.caps.revoke(id, now.timestamp_millis());
                }
                false
            }
            _ => false,
        }
    }
}

fn cap_arg(args: &Value) -> Result<Value> {
    let cap = args.get("cap").context("missing `cap`")?;
    if let Some(text) = cap.as_str() {
        ensure!(
            text.len() <= capability::MAX_BYTES,
            "capability exceeds 8 KiB"
        );
        Ok(serde_json::from_str(text).context("invalid capability JSON")?)
    } else {
        Ok(cap.clone())
    }
}

async fn deliver(cap: &Value) -> bool {
    let Ok(bytes) = serde_json::to_vec(&json!({"t":DELIVER,"cap":cap})) else {
        return false;
    };
    let mut sent = false;
    for (room, node, did) in crate::net::peer_auth::verified_peers() {
        if cap["aud"].as_str() == Some(&did) {
            sent |= crate::net::send_direct(&room, &node, bytes.clone())
                .await
                .is_ok();
        }
    }
    sent
}

async fn present_held() {
    let peers = crate::net::peer_auth::verified_peers();
    let mut sent_ids = HashSet::<String>::new();
    for _ in 0..MAX_HELD {
        // Re-read between paced sends: prioritize new imports and never keep
        // sending a removed held cap from an old snapshot. Bound each round.
        let cap = {
            let store = STORE.lock().expect("authz store lock");
            store
                .caps
                .held
                .iter()
                .rev()
                .filter(|v| v["id"].as_str().is_some_and(|id| !sent_ids.contains(id)))
                .filter_map(|v| capability::parse(v).ok())
                .find(|c| {
                    Some(c.aud.as_str()) == store.local_did.as_deref()
                        && store.caps.live(c, Utc::now().timestamp_millis())
                })
        };
        let Some(cap) = cap else {
            break;
        };
        sent_ids.insert(cap.id.clone());
        let Ok(bytes) = serde_json::to_vec(&json!({"t":PRESENT,"cap":cap.wire})) else {
            continue;
        };
        let mut attempted = false;
        for (room, node, did) in &peers {
            if cap
                .to
                .as_ref()
                .map_or(did == &cap.root().issuer, |to| to.contains(did))
            {
                attempted = true;
                let _ = crate::net::send_direct(room, node, bytes.clone()).await;
            }
        }
        if attempted {
            tokio::time::sleep(StdDuration::from_millis(1100)).await;
        }
    }
}

static PRESENTING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn schedule_presentation(runtime: &tokio::runtime::Handle) {
    let Ok(guard) = PRESENTING.try_lock() else {
        return;
    };
    runtime.spawn(async move {
        let _guard = guard;
        present_held().await;
    });
}

async fn broadcast_revocations(identity: &Identity) -> bool {
    let ids = {
        let store = STORE.lock().expect("authz store lock");
        store
            .caps
            .issued
            .iter()
            .filter(|i| {
                i.revoked
                    && i.cap["fromId"].as_str() == Some(identity.did())
                    && i.cap["exp"]
                        .as_i64()
                        .is_some_and(|exp| exp > Utc::now().timestamp_millis())
            })
            .filter_map(|i| i.cap["id"].as_str().map(str::to_owned))
            .collect::<Vec<_>>()
    };
    let rooms = crate::net::joined_rooms().await;
    let mut sent = false;
    for (index, chunk) in ids.chunks(64).enumerate() {
        let mut wire = json!({"t":REVOKE,"fromId":identity.did(),"ids":chunk,"ts":Utc::now().timestamp_millis()});
        if crate::wiresign::sign_wire(&mut wire, identity).is_err() {
            continue;
        }
        let Ok(bytes) = serde_json::to_vec(&wire) else {
            continue;
        };
        for room in &rooms {
            sent |= crate::net::send_broadcast(room, bytes.clone())
                .await
                .is_ok();
        }
        if !rooms.is_empty() && index * 64 + chunk.len() < ids.len() {
            tokio::time::sleep(StdDuration::from_millis(1100)).await;
        }
    }
    sent
}

pub(super) async fn handle(
    cmd: &str,
    args: Value,
    state: &Arc<crate::daemon::AppState>,
) -> Result<Value> {
    let identity = crate::identity::current(state).await?;
    let mut result =
        STORE
            .lock()
            .expect("authz store lock")
            .cap_command(cmd, &args, &identity, Utc::now())?;
    if state.network.permitted() {
        match cmd {
            "authz.cap.import" => schedule_presentation(&tokio::runtime::Handle::current()),
            "authz.cap.issue" if args["deliver"] == true => {
                result["delivered"] = json!(deliver(&result["cap"]).await)
            }
            "authz.cap.revoke" if args["local"] != true => {
                result["broadcast"] = json!(broadcast_revocations(&identity).await)
            }
            _ => {}
        }
    }
    Ok(result)
}

static STARTED: AtomicBool = AtomicBool::new(false);

pub(super) fn start(identity: Arc<Identity>, runtime: tokio::runtime::Handle) {
    if STARTED.swap(true, Ordering::AcqRel) {
        return;
    }
    STORE.lock().expect("authz store lock").local_did = Some(identity.did().into());
    let handler_identity = identity.clone();
    let handler_runtime = runtime.clone();
    crate::net::register_room_handler(move |event, room, from, data| {
        if event != crate::net::EVENT_RAW
            || from.len() != 16
            || from == handler_identity.node_id()
            || data.len() > capability::MAX_BYTES + 128
            || !data
                .windows(b"mistl-cap-".len())
                .any(|w| w == b"mistl-cap-")
        {
            return;
        }
        let holder = crate::tunnel::auth::verified_did_for_peer(Some(room), from);
        let Ok(mut store) = STORE.lock() else {
            return;
        };
        if !store.caps.take_receive(from, Instant::now()) {
            return;
        }
        let Ok(wire) = serde_json::from_slice::<Value>(data) else {
            return;
        };
        if ["type", "v", "kind"]
            .iter()
            .any(|key| wire.get(key).is_some())
        {
            return;
        }
        let received = store.receive_cap(
            from,
            holder.as_deref(),
            &wire,
            &handler_identity,
            Utc::now(),
        );
        store.flush_best_effort(Utc::now());
        drop(store);
        if received {
            schedule_presentation(&handler_runtime);
        }
    });
    runtime.spawn(async move {
        let mut tick = tokio::time::interval(StdDuration::from_secs(60));
        let mut rounds = 0_u8;
        loop {
            tick.tick().await;
            {
                let mut store = STORE.lock().expect("authz store lock");
                store.caps.prune(Utc::now().timestamp_millis());
                store.flush_best_effort(Utc::now());
            }
            schedule_presentation(&tokio::runtime::Handle::current());
            if rounds == 0 {
                broadcast_revocations(&identity).await;
            }
            rounds = (rounds + 1) % 5;
        }
    });
}
