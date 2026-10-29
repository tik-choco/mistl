//! Storage group: signed grow-only membership (admit/revoke) and invites.
use crate::identity::{Identity, pubkey_from_did};
use crate::wiresign::{sign_wire, stable_stringify, verify_wire};
use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};
use std::io::Read;
use std::path::Path;

pub const MEMBER_TAG: &str = "mistl-vault-member-v1";
pub const MAX_ENTRIES: usize = 256;
pub const MAX_MEMBERS: usize = 64;
pub const MAX_INVITES: usize = 16;
pub const INVITE_TTL_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invite {
    /// hex secret (16 bytes)
    pub secret: String,
    pub expires_at: u64,
}

/// Persisted group state (`<dir>/group.json`, private).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub id: String,
    pub creator: String,
    /// hex salt used to derive `id`.
    pub salt: String,
    /// Signed membership entries (raw JSON).
    pub entries: Vec<Value>,
    #[serde(default)]
    pub invites: Vec<Invite>,
}

/// Parsed invite code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InviteCode {
    pub group: String,
    pub creator: String,
    pub salt: String,
    pub by: String,
    pub secret: String,
    pub expires_at: u64,
}

impl Group {
    /// New group created by `me`, with the signed genesis admit.
    pub fn create(me: &Identity, now_ms: u64) -> Result<Group> {
        let mut salt = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut salt);
        let salt = hex::encode(salt);
        let mut group = Group {
            id: derive_id(me.did(), &salt)?,
            creator: me.did().to_owned(),
            salt,
            entries: Vec::new(),
            invites: Vec::new(),
        };
        group
            .entries
            .push(group.sign_entry(me, "admit", me.did(), now_ms)?);
        Ok(group)
    }

    /// Group record for a joiner (no entries yet; filled by gossip). Verifies
    /// that `id` derives from creator + salt.
    pub fn from_invite(code: &InviteCode) -> Result<Group> {
        code.validate()?;
        Ok(Group {
            id: code.group.clone(),
            creator: code.creator.clone(),
            salt: code.salt.clone(),
            entries: Vec::new(),
            invites: Vec::new(),
        })
    }

    pub fn room(&self) -> String {
        format!("mistl-vault-{}", self.id)
    }

    pub fn load(dir: &Path) -> Result<Option<Group>> {
        let file = match std::fs::File::open(dir.join("group.json")) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut bytes = Vec::new();
        file.take(MAX_STATE_BYTES + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_STATE_BYTES,
            "vault group state too large"
        );
        let mut group: Group = serde_json::from_slice(&bytes)?;
        group.validate_record()?;
        let rows = group.valid_entries(&group.entries);
        ensure!(
            rows.len() == group.entries.len(),
            "invalid membership state"
        );
        group.entries = rows;
        Ok(Some(group))
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        self.validate_record()?;
        ensure!(
            self.valid_entries(&self.entries).len() == self.entries.len(),
            "invalid membership state"
        );
        let bytes = serde_json::to_vec_pretty(self)?;
        ensure!(
            bytes.len() as u64 <= MAX_STATE_BYTES,
            "vault group state too large"
        );
        crate::statefile::write_private(&dir.join("group.json"), &bytes)
    }

    /// Currently admitted, non-revoked member DIDs (sorted).
    pub fn members(&self) -> Vec<String> {
        let valid = self.valid_entries(&self.entries);
        active_members(&valid, &self.creator)
    }

    pub fn is_member(&self, did: &str) -> bool {
        self.members().iter().any(|member| member == did)
    }

    /// Digest over sorted valid entry ids.
    pub fn digest(&self) -> String {
        let mut ids: Vec<_> = self
            .valid_entries(&self.entries)
            .iter()
            .map(entry_id)
            .collect();
        ids.sort_unstable();
        hex::encode(Sha256::digest(ids.concat().as_bytes()))
    }

    /// Merge entries received from a peer: verify each (signature, tag, group),
    /// dedupe, bound, keep only entries valid under the fixpoint rule.
    /// Returns true if membership changed.
    pub fn merge(&mut self, entries: &[Value]) -> bool {
        if entries.is_empty()
            || self.entries.len() >= MAX_ENTRIES
            || self.id != derive_id(&self.creator, &self.salt).unwrap_or_default()
        {
            return false;
        }
        let old_digest = self.digest();
        let mut candidates = self.entries.clone();
        let mut ids: HashSet<_> = candidates.iter().map(entry_id).collect();
        // A caller may pass a larger slice than a single wire chunk.
        let mut incoming: Vec<_> = entries
            .iter()
            .take(MAX_ENTRIES)
            .filter(|v| self.check_entry(v))
            .filter(|v| ids.insert(entry_id(v)))
            .collect();
        incoming.sort_unstable_by_key(|v| (v["at"].as_u64().unwrap_or(0), entry_id(v)));
        candidates.extend(
            incoming
                .into_iter()
                .take(MAX_ENTRIES - candidates.len())
                .cloned(),
        );
        let next = self.valid_entries(&candidates);
        self.entries = next;
        self.digest() != old_digest
    }

    /// Sign an admit for `member` (caller must be a member).
    pub fn admit(&mut self, me: &Identity, member: &str, now_ms: u64) -> Result<Value> {
        self.add_entry(me, "admit", member, now_ms)
    }

    /// Sign a revoke for `member` (self-revoke = leave).
    pub fn revoke(&mut self, me: &Identity, member: &str, now_ms: u64) -> Result<Value> {
        self.add_entry(me, "revoke", member, now_ms)
    }

    /// Create a pending invite; returns (code text `mvi1-...`, expires_at).
    pub fn invite(&mut self, me: &Identity, now_ms: u64) -> Result<(String, u64)> {
        ensure!(self.is_member(me.did()), "inviter is not an active member");
        self.invites.retain(|invite| invite.expires_at > now_ms);
        ensure!(self.invites.len() < MAX_INVITES, "too many pending invites");
        let expires_at = now_ms
            .checked_add(INVITE_TTL_MS)
            .context("invite expiry overflow")?;
        let mut secret = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut secret);
        let secret = hex::encode(secret);
        let code = InviteCode {
            group: self.id.clone(),
            creator: self.creator.clone(),
            salt: self.salt.clone(),
            by: me.did().to_owned(),
            secret: secret.clone(),
            expires_at,
        };
        let text = code.encode()?;
        self.invites.push(Invite { secret, expires_at });
        Ok((text, expires_at))
    }

    /// Verify a join (`invite` id + `mac`) for `joiner`; consumes the invite on
    /// success. Expired invites are pruned.
    pub fn redeem(&mut self, invite: &str, mac: &str, joiner: &str, now_ms: u64) -> bool {
        self.invites.retain(|pending| pending.expires_at > now_ms);
        if !valid_hex(invite, 16) || !valid_hex(mac, 64) || pubkey_from_did(joiner).is_err() {
            return false;
        }
        let Ok(mac) = hex::decode(mac) else {
            return false;
        };
        let Some(index) = self.invites.iter().take(MAX_INVITES).position(|pending| {
            let Ok(secret) = decode_secret(&pending.secret) else {
                return false;
            };
            if invite_id(&secret) != invite {
                return false;
            }
            let Ok(mut hmac) = Hmac::<Sha256>::new_from_slice(&secret) else {
                return false;
            };
            update_join_mac(&mut hmac, &self.id, joiner);
            hmac.verify_slice(&mac).is_ok()
        }) else {
            return false;
        };
        self.invites.remove(index);
        true
    }

    fn validate_record(&self) -> Result<()> {
        ensure!(
            derive_id(&self.creator, &self.salt)? == self.id,
            "group id does not match creator and salt"
        );
        ensure!(
            self.entries.len() <= MAX_ENTRIES,
            "too many membership entries"
        );
        ensure!(
            self.invites.len() <= MAX_INVITES,
            "too many pending invites"
        );
        for invite in &self.invites {
            decode_secret(&invite.secret)?;
        }
        Ok(())
    }

    fn sign_entry(&self, me: &Identity, op: &str, member: &str, at: u64) -> Result<Value> {
        pubkey_from_did(member)?;
        let mut entry = json!({"t": MEMBER_TAG, "fromId": me.did(), "group": self.id,
            "op": op, "member": member, "at": at});
        sign_wire(&mut entry, me)?;
        Ok(entry)
    }

    fn add_entry(&mut self, me: &Identity, op: &str, member: &str, at: u64) -> Result<Value> {
        ensure!(
            self.entries.len() < MAX_ENTRIES,
            "membership entry limit reached"
        );
        ensure!(self.is_member(me.did()), "signer is not an active member");
        if op == "admit" {
            ensure!(
                !self
                    .entries
                    .iter()
                    .any(|e| e["op"] == "revoke" && e["member"] == member),
                "member permanently revoked"
            );
            ensure!(
                self.is_member(member) || self.members().len() < MAX_MEMBERS,
                "member limit reached"
            );
        }
        let entry = self.sign_entry(me, op, member, at)?;
        let mut next = self.clone();
        next.merge(std::slice::from_ref(&entry));
        ensure!(
            next.entries.contains(&entry),
            "membership entry is not authorized"
        );
        self.entries = next.entries;
        Ok(entry)
    }

    fn check_entry(&self, entry: &Value) -> bool {
        // Fixed scalar fields also bound signed extensions and nested input.
        entry.as_object().is_some_and(|m| m.len() == 7)
            && entry["t"] == MEMBER_TAG
            && entry["group"] == self.id
            && matches!(entry["op"].as_str(), Some("admit" | "revoke"))
            && entry["at"].as_u64().is_some()
            && entry["member"]
                .as_str()
                .is_some_and(|s| pubkey_from_did(s).is_ok())
            && verify_wire(entry).unwrap_or(false)
    }

    /// Validity is a pure function of the entry set, so every member converges
    /// on the same membership regardless of arrival order: an admit is valid iff
    /// its signer was admitted through a chain of admits from the genesis.
    /// Revocations only remove from the active set and are permanent; a revoked
    /// member's earlier admits stay valid. Only the first admit and the first
    /// revoke per DID (by `at`, then id) are kept, and admits use at most half
    /// of the entry budget, so redundant entries cannot crowd out revocations.
    fn valid_entries(&self, entries: &[Value]) -> Vec<Value> {
        let mut seen = HashSet::new();
        let mut checked: Vec<_> = entries
            .iter()
            .take(MAX_ENTRIES * 2)
            .filter(|entry| self.check_entry(entry))
            .map(|entry| (entry["at"].as_u64().unwrap_or(0), entry_id(entry), entry))
            .filter(|(_, id, _)| seen.insert(id.clone()))
            .collect();
        checked.sort_unstable_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        let Some(genesis) = checked
            .iter()
            .position(|(_, _, e)| is_genesis(e, &self.creator))
        else {
            return Vec::new();
        };
        let mut kept = vec![checked.remove(genesis)];
        let mut admitted = BTreeSet::from([self.creator.clone()]);
        loop {
            let before = admitted.len();
            for row in &checked {
                let (signer, member) = (signer_of(row.2), member_of(row.2));
                if row.2["op"] == "admit"
                    && kept.len() < MAX_ENTRIES / 2
                    && admitted.contains(signer)
                    && !admitted.contains(member)
                {
                    admitted.insert(member.to_owned());
                    kept.push(row.clone());
                }
            }
            if before == admitted.len() {
                break;
            }
        }
        let mut revoked = HashSet::new();
        for row in &checked {
            if row.2["op"] == "revoke"
                && admitted.contains(signer_of(row.2))
                && revoked.insert(member_of(row.2))
            {
                kept.push(row.clone());
            }
        }
        kept.sort_unstable_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        kept.into_iter()
            .map(|(_, _, entry)| entry.clone())
            .collect()
    }
}

fn signer_of(entry: &Value) -> &str {
    entry["fromId"].as_str().unwrap_or("")
}

fn member_of(entry: &Value) -> &str {
    entry["member"].as_str().unwrap_or("")
}

impl InviteCode {
    pub fn parse(code: &str) -> Result<InviteCode> {
        ensure!(code.len() <= MAX_CODE_BYTES, "invite code too large");
        let encoded = code
            .trim()
            .strip_prefix("mvi1-")
            .context("invalid invite code prefix")?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .context("invalid invite code encoding")?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Code {
            g: String,
            c: String,
            s: String,
            by: String,
            k: String,
            exp: u64,
        }
        let wire: Code = serde_json::from_slice(&bytes).context("invalid invite code JSON")?;
        let code = InviteCode {
            group: wire.g,
            creator: wire.c,
            salt: wire.s,
            by: wire.by,
            secret: wire.k,
            expires_at: wire.exp,
        };
        code.validate()?;
        Ok(code)
    }

    /// (`invite` id, `mac`) a joiner sends.
    pub fn join_proof(&self, joiner_did: &str) -> (String, String) {
        let Ok(secret) = decode_secret(&self.secret) else {
            return (String::new(), String::new());
        };
        let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(&secret) else {
            return (String::new(), String::new());
        };
        update_join_mac(&mut mac, &self.group, joiner_did);
        (invite_id(&secret), hex::encode(mac.finalize().into_bytes()))
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            derive_id(&self.creator, &self.salt)? == self.group,
            "invite group id mismatch"
        );
        pubkey_from_did(&self.by)?;
        decode_secret(&self.secret)?;
        Ok(())
    }

    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        let raw = json!({"g": self.group, "c": self.creator, "s": self.salt,
            "by": self.by, "k": self.secret, "exp": self.expires_at});
        Ok(format!(
            "mvi1-{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&raw)?)
        ))
    }
}

const MAX_STATE_BYTES: u64 = 512 * 1024;
const MAX_CODE_BYTES: usize = 2048;

fn valid_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn decode_secret(secret: &str) -> Result<[u8; 16]> {
    ensure!(valid_hex(secret, 32), "invalid invite secret");
    hex::decode(secret)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid invite secret length"))
}

fn derive_id(creator: &str, salt: &str) -> Result<String> {
    pubkey_from_did(creator)?;
    ensure!(valid_hex(salt, 32), "invalid group salt");
    let mut hash = Sha256::new();
    hash.update(b"mistl-vault-group-v1");
    hash.update(creator.as_bytes());
    hash.update(hex::decode(salt)?);
    Ok(hex::encode(&hash.finalize()[..16]))
}

fn entry_id(entry: &Value) -> String {
    hex::encode(Sha256::digest(stable_stringify(entry).as_bytes()))
}

fn is_genesis(entry: &Value, creator: &str) -> bool {
    entry["op"] == "admit" && entry["fromId"] == creator && entry["member"] == creator
}

/// Every DID reachable from the genesis through admits, ignoring revocations.
fn ever_admitted<'a>(
    entries: impl IntoIterator<Item = &'a Value>,
    creator: &str,
) -> BTreeSet<String> {
    let entries: Vec<_> = entries.into_iter().take(MAX_ENTRIES * 2).collect();
    let mut ever = BTreeSet::new();
    if entries.iter().any(|e| is_genesis(e, creator)) {
        ever.insert(creator.to_owned());
    }
    loop {
        let before = ever.len();
        for e in &entries {
            let signer = e["fromId"].as_str().unwrap_or("");
            if e["op"] == "admit" && ever.contains(signer) {
                ever.insert(e["member"].as_str().unwrap_or("").to_owned());
            }
        }
        if before == ever.len() {
            break;
        }
    }
    ever
}

/// Active members of an already validated entry set: ever admitted minus
/// revoked, capped deterministically at MAX_MEMBERS (creator first).
fn active_members<'a>(entries: impl IntoIterator<Item = &'a Value>, creator: &str) -> Vec<String> {
    let entries: Vec<_> = entries.into_iter().take(MAX_ENTRIES).collect();
    let revoked: HashSet<_> = entries
        .iter()
        .filter(|e| e["op"] == "revoke")
        .filter_map(|e| e["member"].as_str())
        .collect();
    let ever = ever_admitted(entries.iter().copied(), creator);
    let mut members: Vec<String> = ever
        .into_iter()
        .filter(|did| !revoked.contains(did.as_str()))
        .collect();
    if members.len() > MAX_MEMBERS {
        let creator_active = members.iter().any(|m| m == creator);
        members.retain(|m| m != creator);
        members.truncate(MAX_MEMBERS - usize::from(creator_active));
        if creator_active {
            members.push(creator.to_owned());
        }
        members.sort();
    }
    members
}

fn invite_id(secret: &[u8]) -> String {
    hex::encode(&Sha256::digest(secret)[..8])
}

fn update_join_mac(mac: &mut Hmac<Sha256>, group: &str, joiner: &str) {
    mac.update(b"mistl-vault-join-v1");
    mac.update(group.as_bytes());
    mac.update(joiner.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_like(group: &Group) -> Group {
        Group {
            entries: Vec::new(),
            invites: Vec::new(),
            ..group.clone()
        }
    }

    #[test]
    fn genesis_derivation_and_fixpoint() {
        let creator = crate::identity::for_test();
        let alice = crate::identity::for_test();
        let bob = crate::identity::for_test();
        let mut group = Group::create(&creator, 100).unwrap();
        assert_eq!(group.members(), vec![creator.did()]);
        assert_eq!(group.room(), format!("mistl-vault-{}", group.id));
        assert_eq!(group.id, derive_id(creator.did(), &group.salt).unwrap());
        let a = group
            .sign_entry(&creator, "admit", alice.did(), 200)
            .unwrap();
        // Dependency order need not match timestamps or input order.
        let b = group.sign_entry(&alice, "admit", bob.did(), 50).unwrap();
        assert!(group.merge(&[b.clone(), a.clone()]));
        assert_eq!(group.members().len(), 3);
        assert!(group.is_member(bob.did()));
        let mut other = empty_like(&group);
        let mut shuffled = group.entries.clone();
        shuffled.reverse();
        assert!(other.merge(&shuffled));
        assert_eq!(other.digest(), group.digest());
        assert!(!group.merge(&[a, b]));
        assert_eq!(group.entries.len(), 3);
    }

    #[test]
    fn unknown_signers_tampering_and_wrong_groups_are_ignored() {
        let me = crate::identity::for_test();
        let stranger = crate::identity::for_test();
        let mut group = Group::create(&me, 1).unwrap();
        let invalid = group
            .sign_entry(&stranger, "admit", stranger.did(), 2)
            .unwrap();
        assert!(!group.merge(&[invalid]));
        assert!(group.admit(&stranger, me.did(), 3).is_err());
        let mut tampered = group.sign_entry(&me, "admit", stranger.did(), 2).unwrap();
        tampered["at"] = json!(3);
        assert!(!group.merge(&[tampered]));
        for (field, value) in [
            ("t", json!("wrong")),
            ("group", json!("0".repeat(32))),
            ("member", json!("did:key:bad")),
            ("at", json!(-1)),
            ("op", json!("other")),
        ] {
            let mut entry = group.sign_entry(&me, "admit", stranger.did(), 2).unwrap();
            entry[field] = value;
            sign_wire(&mut entry, &me).unwrap();
            assert!(!group.merge(&[entry]));
        }
        assert_eq!(group.members(), vec![me.did()]);
        let mut no_genesis = empty_like(&group);
        let admission = group.sign_entry(&me, "admit", stranger.did(), 2).unwrap();
        assert!(!no_genesis.merge(&[admission]));
    }

    #[test]
    fn revocation_is_permanent_and_self_revoke_survives_merge() {
        let me = crate::identity::for_test();
        let alice = crate::identity::for_test();
        let bob = crate::identity::for_test();
        let mut group = Group::create(&me, 1).unwrap();
        group.admit(&me, alice.did(), 2).unwrap();
        group.revoke(&me, alice.did(), 3).unwrap();
        assert!(!group.is_member(alice.did()));
        assert!(group.admit(&me, alice.did(), 4).is_err());
        let readmit = group.sign_entry(&me, "admit", alice.did(), 5).unwrap();
        group.merge(&[readmit]);
        assert!(!group.is_member(alice.did()));
        let unauthorized = group.sign_entry(&bob, "admit", bob.did(), 6).unwrap();
        assert!(!group.merge(&[unauthorized]));
        assert!(!group.is_member(bob.did()));
        group.revoke(&me, me.did(), 7).unwrap();
        assert!(group.members().is_empty());
        let mut copy = empty_like(&group);
        copy.merge(&group.entries);
        assert!(copy.members().is_empty());
        assert_eq!(copy.digest(), group.digest());
        assert!(group.invite(&me, 8).is_err());
    }

    #[test]
    fn validity_depends_only_on_the_entry_set() {
        let me = crate::identity::for_test();
        let alice = crate::identity::for_test();
        let bob = crate::identity::for_test();
        let outsider = crate::identity::for_test();
        let mut group = Group::create(&me, 0).unwrap();
        group.admit(&me, alice.did(), 1).unwrap();
        group.admit(&alice, bob.did(), 2).unwrap();
        group.revoke(&me, alice.did(), 3).unwrap();
        // Revoking the admitter does not undo earlier admits.
        assert!(group.is_member(bob.did()));
        let stranger = group
            .sign_entry(&outsider, "admit", outsider.did(), 4)
            .unwrap();
        assert!(!group.merge(&[stranger]));

        // Concurrent mutual revocations converge in any arrival order.
        let base = group.clone();
        let bob_revokes_me = base.sign_entry(&bob, "revoke", me.did(), 5).unwrap();
        let me_revokes_bob = base.sign_entry(&me, "revoke", bob.did(), 5).unwrap();
        let mut first = base.clone();
        first.merge(std::slice::from_ref(&bob_revokes_me));
        first.merge(std::slice::from_ref(&me_revokes_bob));
        let mut second = base.clone();
        second.merge(std::slice::from_ref(&me_revokes_bob));
        second.merge(std::slice::from_ref(&bob_revokes_me));
        assert_eq!(first.digest(), second.digest());
        assert_eq!(first.members(), second.members());
        assert!(first.members().is_empty());
    }

    #[test]
    fn invite_proof_single_use_expiry_and_bounds() {
        let me = crate::identity::for_test();
        let joiner = crate::identity::for_test();
        let other = crate::identity::for_test();
        let mut group = Group::create(&me, 1).unwrap();
        let (text, expiry) = group.invite(&me, 100).unwrap();
        assert_eq!(expiry, 100 + INVITE_TTL_MS);
        let code = InviteCode::parse(&text).unwrap();
        assert_eq!(code.encode().unwrap(), text);
        let joining = Group::from_invite(&code).unwrap();
        assert_eq!(joining.id, group.id);
        assert!(joining.members().is_empty());
        let (id, mac) = code.join_proof(joiner.did());
        assert!(!group.redeem(&id, &"0".repeat(64), joiner.did(), 101));
        assert!(!group.redeem(&id, &mac, other.did(), 101));
        assert!(!group.redeem(&id, &mac, "bad did", 101));
        assert!(group.redeem(&id, &mac, joiner.did(), 101));
        assert!(!group.redeem(&id, &mac, joiner.did(), 101));
        assert!(!group.is_member(joiner.did()));
        let (text, expiry) = group.invite(&me, 200).unwrap();
        let (id, mac) = InviteCode::parse(&text).unwrap().join_proof(joiner.did());
        assert!(!group.redeem(&id, &mac, joiner.did(), expiry));
        assert!(group.invites.is_empty());
        for _ in 0..MAX_INVITES {
            group.invite(&me, 300).unwrap();
        }
        assert!(group.invite(&me, 300).is_err());
        assert!(group.invite(&me, 300 + INVITE_TTL_MS).is_ok());
        assert_eq!(group.invites.len(), 1);
        assert!(group.invite(&me, u64::MAX).is_err());
    }

    #[test]
    fn malformed_invites_are_rejected() {
        let me = crate::identity::for_test();
        let mut group = Group::create(&me, 0).unwrap();
        let (text, _) = group.invite(&me, 0).unwrap();
        let code = InviteCode::parse(&text).unwrap();
        for text in [
            "mvi2-abc".to_owned(),
            "mvi1-!".to_owned(),
            format!("mvi1-{}", "a".repeat(MAX_CODE_BYTES)),
        ] {
            assert!(InviteCode::parse(&text).is_err());
        }
        let mut broken = code.clone();
        broken.group = "0".repeat(32);
        assert!(Group::from_invite(&broken).is_err());
        broken = code.clone();
        broken.salt = "0".repeat(31);
        assert!(Group::from_invite(&broken).is_err());
        broken = code.clone();
        broken.by = "did:key:invalid".to_owned();
        assert!(broken.encode().is_err());
        broken = code;
        broken.secret = "g".repeat(32);
        assert!(broken.encode().is_err());
        assert_eq!(broken.join_proof(me.did()), (String::new(), String::new()));
    }

    #[test]
    fn membership_and_entry_limits_preserve_history() {
        let me = crate::identity::for_test();
        let mut group = Group::create(&me, 0).unwrap();
        let entries: Vec<_> = (1..=MAX_MEMBERS)
            .map(|n| {
                group
                    .sign_entry(
                        &me,
                        "admit",
                        &crate::identity::did_for_seed(n as u8),
                        n as u64,
                    )
                    .unwrap()
            })
            .collect();
        group.merge(&entries);
        assert_eq!(group.members().len(), MAX_MEMBERS);
        assert!(
            group
                .admit(&me, &crate::identity::did_for_seed(200), 200)
                .is_err()
        );
        // Redundant admits cannot fill the entry budget and block revocation.
        let mut flooded = Group::create(&me, 0).unwrap();
        let alice = crate::identity::did_for_seed(1);
        flooded.admit(&me, &alice, 1).unwrap();
        let redundant: Vec<_> = (2..MAX_ENTRIES as u64 * 2)
            .map(|n| flooded.sign_entry(&me, "admit", &alice, n).unwrap())
            .collect();
        let digest = flooded.digest();
        assert!(!flooded.merge(&redundant));
        assert_eq!(flooded.digest(), digest);
        assert_eq!(flooded.entries.len(), 2);
        flooded.revoke(&me, &alice, 1000).unwrap();
        assert!(!flooded.is_member(&alice));
        let again = flooded.sign_entry(&me, "revoke", &alice, 1001).unwrap();
        assert!(!flooded.merge(&[again]));
        assert_eq!(flooded.entries.len(), 3);
    }

    #[test]
    fn private_state_roundtrip_and_corruption() {
        let me = crate::identity::for_test();
        let dir = std::env::temp_dir().join(format!(
            "mistl-vault-group-{}",
            hex::encode(rand::random::<[u8; 16]>())
        ));
        assert!(Group::load(&dir).unwrap().is_none());
        let mut group = Group::create(&me, 1).unwrap();
        group.invite(&me, 2).unwrap();
        group.save(&dir).unwrap();
        let loaded = Group::load(&dir).unwrap().unwrap();
        assert_eq!(loaded.digest(), group.digest());
        assert_eq!(loaded.invites[0].secret, group.invites[0].secret);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join("group.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let mut raw = serde_json::to_value(&group).unwrap();
        raw["id"] = json!("f".repeat(32));
        std::fs::write(dir.join("group.json"), serde_json::to_vec(&raw).unwrap()).unwrap();
        assert!(Group::load(&dir).is_err());
        std::fs::write(dir.join("group.json"), b"broken").unwrap();
        assert!(Group::load(&dir).is_err());
        std::fs::write(
            dir.join("group.json"),
            vec![b' '; MAX_STATE_BYTES as usize + 1],
        )
        .unwrap();
        assert!(Group::load(&dir).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
