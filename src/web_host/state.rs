//! Local mistl web state: `<data_dir>/web/state.json`.
//!
//! Known sites (each with its verified signed record), which version is
//! installed, and who is trusted. Every record is re-verified on load, so a
//! tampered file can at worst drop entries, never inject an unsigned one.
//!
//! Also the web pin ledger, `<data_dir>/web/pins.json`: every store root CID
//! mistl web has pinned. Store pins are per root with no refcount, so web
//! never unpins ad hoc; it reconciles the ledger against what it still needs
//! ([`desired_cids`] / [`release_plan`]).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::site::{self, Manifest, SiteRecord};

/// Cap on known sites. Records only arrive for sites someone asked for, but
/// any local web page can make the browser ask, so resolved-only entries
/// (not trusted, installed or own) are evicted oldest-first past this.
pub const MAX_SITES: usize = 1024;

/// On-disk shape of one site.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSite {
    record: Value,
    #[serde(default)]
    installed: Option<String>,
    /// Record seq of the installed version (absent in older files).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed_seq: Option<u64>,
    #[serde(default)]
    trusted: bool,
    #[serde(default)]
    own: bool,
    #[serde(default)]
    added_at: u64,
    #[serde(default)]
    last_open: Option<u64>,
}

/// On-disk shape of `state.json`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct StoredState {
    #[serde(default)]
    sites: BTreeMap<String, StoredSite>,
    #[serde(default)]
    trusted_publishers: Vec<String>,
}

/// One known site, with its record already verified.
#[derive(Debug, Clone)]
pub struct Entry {
    pub record: SiteRecord,
    /// Manifest CID of the fully installed version, if any.
    pub installed: Option<String>,
    /// Record seq of the installed version (`None` = unknown, treated as 0).
    pub installed_seq: Option<u64>,
    pub trusted: bool,
    pub own: bool,
    pub added_at: u64,
    pub last_open: Option<u64>,
}

/// Outcome of [`WebState::upsert_record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Upsert {
    /// A new site entry was created.
    Inserted,
    /// The stored record was replaced by a newer one.
    Updated,
    /// Same seq as stored (kept the stored one).
    Unchanged,
    /// Older than the stored record: refused.
    Rollback,
    /// Unknown site and no room left for it.
    Full,
}

#[derive(Debug, Default)]
pub struct WebState {
    pub sites: BTreeMap<String, Entry>,
    pub trusted_publishers: BTreeSet<String>,
}

impl WebState {
    /// Load `path` (missing file = empty state). Entries whose record no
    /// longer verifies ([`site::verify_record_stored`]: no clock-skew check),
    /// or is filed under the wrong siteId, are dropped with a warning; an
    /// unreadable file is an error (callers must not silently overwrite it).
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", path.display()));
            }
        };
        let stored: StoredState = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        Ok(Self::from_stored(stored))
    }

    fn from_stored(stored: StoredState) -> Self {
        let mut state = Self::default();
        for (id, s) in stored.sites {
            let record = match site::verify_record_stored(&s.record) {
                Ok(record) if record.site == id => record,
                Ok(_) => {
                    tracing::warn!(site = %id, "web: dropping a stored record filed under the wrong site id");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(site = %id, %error, "web: dropping a stored record that does not verify");
                    continue;
                }
            };
            let installed = s.installed.filter(|cid| site::sane_cid(cid));
            // Older files lack `installed_seq`: the current record's seq if
            // that is the installed version, else unknown.
            let installed_seq = match &installed {
                None => None,
                Some(cid) => s
                    .installed_seq
                    .or_else(|| (*cid == record.manifest).then_some(record.seq)),
            };
            state.sites.insert(
                id,
                Entry {
                    record,
                    installed,
                    installed_seq,
                    trusted: s.trusted,
                    own: s.own,
                    added_at: s.added_at,
                    last_open: s.last_open,
                },
            );
        }
        for did in stored.trusted_publishers {
            if crate::identity::pubkey_from_did(&did).is_ok() {
                state.trusted_publishers.insert(did);
            } else {
                tracing::warn!(%did, "web: dropping an invalid trusted publisher");
            }
        }
        state
    }

    fn to_stored(&self) -> StoredState {
        StoredState {
            sites: self
                .sites
                .iter()
                .map(|(id, e)| {
                    (
                        id.clone(),
                        StoredSite {
                            record: e.record.raw.clone(),
                            installed: e.installed.clone(),
                            installed_seq: e.installed_seq,
                            trusted: e.trusted,
                            own: e.own,
                            added_at: e.added_at,
                            last_open: e.last_open,
                        },
                    )
                })
                .collect(),
            trusted_publishers: self.trusted_publishers.iter().cloned().collect(),
        }
    }

    /// The serialized file contents.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(&self.to_stored())?)
    }

    /// Persist atomically.
    pub fn save(&self, path: &Path) -> Result<()> {
        crate::statefile::write_bytes(path, &self.to_bytes()?)
            .with_context(|| format!("writing {}", path.display()))
    }

    /// Trusted per site, or via its publisher.
    pub fn is_trusted(&self, site: &str) -> bool {
        self.sites
            .get(site)
            .is_some_and(|e| e.trusted || self.trusted_publishers.contains(&e.record.did))
    }

    /// Store a verified record: inserts unknown sites, replaces only with a
    /// strictly newer seq, refuses rollbacks. Trust/install flags of an
    /// existing entry are kept (the siteId binds the publisher DID).
    pub fn upsert_record(&mut self, record: SiteRecord, now_ms: u64) -> Upsert {
        if let Some(entry) = self.sites.get_mut(&record.site) {
            return if site::is_newer(&record, &entry.record) {
                entry.record = record;
                Upsert::Updated
            } else if record.seq == entry.record.seq {
                Upsert::Unchanged
            } else {
                Upsert::Rollback
            };
        }
        if self.sites.len() >= MAX_SITES && !self.evict_one() {
            return Upsert::Full;
        }
        self.sites.insert(
            record.site.clone(),
            Entry {
                record,
                installed: None,
                installed_seq: None,
                trusted: false,
                own: false,
                added_at: now_ms,
                last_open: None,
            },
        );
        Upsert::Inserted
    }

    /// Drop the oldest resolved-only entry. False if none is evictable.
    fn evict_one(&mut self) -> bool {
        let victim = self
            .sites
            .iter()
            .filter(|(id, e)| !e.own && e.installed.is_none() && !self.is_trusted(id))
            .min_by_key(|(_, e)| e.last_open.unwrap_or(e.added_at))
            .map(|(id, _)| id.clone());
        match victim {
            Some(id) => self.sites.remove(&id).is_some(),
            None => false,
        }
    }

    pub fn remove(&mut self, site: &str) -> Option<Entry> {
        self.sites.remove(site)
    }
}

/// Whether a version with record seq `seq` may replace the installed one:
/// strictly newer than what is installed and not newer than the record held
/// (a stale install finishing late must not roll the site back).
pub fn may_install(entry: &Entry, seq: u64) -> bool {
    seq > entry.installed_seq.unwrap_or(0) && seq <= entry.record.seq
}

/// The web pin ledger: every root CID web has pinned. Entries are persisted
/// *before* pinning, so a crash can only leave extra entries, which the next
/// reconcile releases.
#[derive(Debug)]
pub struct PinLedger {
    path: PathBuf,
    pub roots: BTreeSet<String>,
}

#[derive(Serialize, Deserialize)]
struct StoredLedger {
    v: u32,
    roots: Vec<String>,
}

impl PinLedger {
    /// Load `path`; `Ok(None)` if it does not exist yet (the caller seeds
    /// it). An unreadable file is an error.
    pub fn load(path: PathBuf) -> Result<Option<Self>> {
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", path.display()));
            }
        };
        let stored: StoredLedger = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        if stored.v != 1 {
            anyhow::bail!("{}: unsupported version {}", path.display(), stored.v);
        }
        let roots = stored
            .roots
            .into_iter()
            .filter(|c| site::sane_cid(c))
            .collect();
        Ok(Some(Self { path, roots }))
    }

    pub fn empty(path: PathBuf) -> Self {
        Self {
            path,
            roots: BTreeSet::new(),
        }
    }

    /// Persist atomically.
    pub fn save(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&StoredLedger {
            v: 1,
            roots: self.roots.iter().cloned().collect(),
        })?;
        crate::statefile::write_bytes(&self.path, &bytes)
            .with_context(|| format!("writing {}", self.path.display()))
    }
}

/// Everything web must keep pinned: each installed version (manifest +
/// files) and every CID an in-flight publish/install has claimed.
pub fn desired_cids<'a>(
    installed: impl IntoIterator<Item = (&'a str, &'a Manifest)>,
    claims: impl IntoIterator<Item = &'a HashSet<String>>,
) -> HashSet<String> {
    let mut out = HashSet::new();
    for (cid, manifest) in installed {
        out.extend(version_cids(cid, Some(manifest)));
    }
    for set in claims {
        out.extend(set.iter().cloned());
    }
    out
}

/// Ledger roots nothing needs any more (sorted): unpin these.
pub fn release_plan(ledger: &BTreeSet<String>, desired: &HashSet<String>) -> Vec<String> {
    ledger
        .iter()
        .filter(|cid| !desired.contains(*cid))
        .cloned()
        .collect()
}

/// Every store CID a version pins: its manifest and each file.
pub fn version_cids(manifest_cid: &str, manifest: Option<&Manifest>) -> HashSet<String> {
    let mut cids = HashSet::new();
    cids.insert(manifest_cid.to_string());
    if let Some(m) = manifest {
        cids.extend(m.files.iter().map(|f| f.cid.clone()));
    }
    cids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::Identity;
    use serde_json::json;

    const NOW: u64 = 1_764_300_000_000;

    fn rec(id: &Identity, name: &str, seq: u64) -> SiteRecord {
        let raw = site::build_record(id, name, "bafkreiabc", 10, 1, seq, NOW).unwrap();
        site::verify_record(&raw, NOW).unwrap()
    }

    fn temp_path() -> std::path::PathBuf {
        std::env::temp_dir()
            .join(format!("mistl-web-{:016x}", rand::random::<u64>()))
            .join("state.json")
    }

    #[test]
    fn upsert_keeps_newer_and_refuses_rollback() {
        let id = crate::identity::for_test();
        let mut st = WebState::default();
        assert_eq!(st.upsert_record(rec(&id, "g", 5), NOW), Upsert::Inserted);
        let site = rec(&id, "g", 5).site;
        st.sites.get_mut(&site).unwrap().trusted = true;
        assert_eq!(st.upsert_record(rec(&id, "g", 5), NOW), Upsert::Unchanged);
        assert_eq!(st.upsert_record(rec(&id, "g", 4), NOW), Upsert::Rollback);
        assert_eq!(st.sites[&site].record.seq, 5);
        assert_eq!(st.upsert_record(rec(&id, "g", 6), NOW), Upsert::Updated);
        assert_eq!(st.sites[&site].record.seq, 6);
        assert!(st.sites[&site].trusted, "trust survives updates");
        assert!(st.remove(&site).is_some());
        assert!(st.remove(&site).is_none());
    }

    #[test]
    fn trust_by_site_or_publisher() {
        let id = crate::identity::for_test();
        let mut st = WebState::default();
        let r = rec(&id, "g", 1);
        let site = r.site.clone();
        st.upsert_record(r, NOW);
        assert!(!st.is_trusted(&site));
        st.trusted_publishers.insert(id.did().to_string());
        assert!(st.is_trusted(&site));
        st.trusted_publishers.clear();
        st.sites.get_mut(&site).unwrap().trusted = true;
        assert!(st.is_trusted(&site));
        assert!(!st.is_trusted("unknown"));
    }

    #[test]
    fn save_load_round_trip_drops_invalid_entries() {
        let id = crate::identity::for_test();
        let path = temp_path();
        let mut st = WebState::default();
        let r = rec(&id, "g", 1);
        let site = r.site.clone();
        st.upsert_record(r, NOW);
        {
            let e = st.sites.get_mut(&site).unwrap();
            e.installed = Some("bafkreiabc".into());
            e.own = true;
            e.trusted = true;
        }
        st.trusted_publishers.insert(id.did().to_string());
        st.save(&path).unwrap();

        let back = WebState::load(&path).unwrap();
        let e = &back.sites[&site];
        assert_eq!(e.installed.as_deref(), Some("bafkreiabc"));
        assert!(e.own && e.trusted);
        assert!(back.trusted_publishers.contains(id.did()));

        // Tamper: change a signed field, file under a wrong id, bad DID.
        let mut raw: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let good = raw["sites"][&site].clone();
        raw["sites"][&site]["record"]["manifest"] = json!("bafkreievil");
        raw["sites"]["a".repeat(32)] = good;
        raw["trusted_publishers"] = json!(["did:key:nope"]);
        std::fs::write(&path, serde_json::to_vec(&raw).unwrap()).unwrap();
        let back = WebState::load(&path).unwrap();
        assert!(back.sites.is_empty());
        assert!(back.trusted_publishers.is_empty());

        assert!(
            WebState::load(&path.with_file_name("missing.json"))
                .unwrap()
                .sites
                .is_empty()
        );
        std::fs::write(&path, b"garbage").unwrap();
        assert!(WebState::load(&path).is_err());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn eviction_spares_trusted_installed_and_own() {
        let id = crate::identity::for_test();
        let mut st = WebState::default();
        for i in 0..MAX_SITES {
            let r = rec(&id, &format!("s{i}"), 1);
            let site = r.site.clone();
            assert_eq!(st.upsert_record(r, NOW + i as u64), Upsert::Inserted);
            if i > 0 {
                st.sites.get_mut(&site).unwrap().trusted = true;
            }
        }
        // Only `s0` is evictable.
        let s0 = rec(&id, "s0", 1).site;
        let extra = rec(&id, "extra", 1);
        let extra_site = extra.site.clone();
        assert_eq!(st.upsert_record(extra, NOW), Upsert::Inserted);
        assert!(!st.sites.contains_key(&s0));
        st.sites.get_mut(&extra_site).unwrap().own = true;
        assert_eq!(st.upsert_record(rec(&id, "extra2", 1), NOW), Upsert::Full);
        assert_eq!(st.sites.len(), MAX_SITES);
    }

    fn manifest(cids: &[&str]) -> Manifest {
        Manifest::new(
            "g",
            "a",
            false,
            false,
            true,
            cids.iter()
                .enumerate()
                .map(|(i, c)| site::FileEntry {
                    p: if i == 0 { "a".into() } else { format!("f{i}") },
                    cid: c.to_string(),
                    size: 1,
                })
                .collect(),
        )
    }

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn reconcile_plan_releases_only_unneeded_roots() {
        let new = manifest(&["c2", "c4"]);
        let other = manifest(&["c3"]);
        let claim: HashSet<String> = ["c9".to_string(), "m9".to_string()].into();
        let ledger = set(&["mold", "c1", "c2", "c3", "c4", "mnew", "mother", "c9", "m9"]);
        // The old version's manifest and unshared file go; shared and
        // claimed CIDs stay.
        let desired = desired_cids([("mnew", &new), ("mother", &other)], [&claim]);
        assert_eq!(release_plan(&ledger, &desired), ["c1", "mold"]);
        // Dropping the claim releases what only it protected.
        let desired = desired_cids([("mnew", &new), ("mother", &other)], []);
        assert_eq!(release_plan(&ledger, &desired), ["c1", "c9", "m9", "mold"]);
        assert!(release_plan(&BTreeSet::new(), &desired).is_empty());
        assert_eq!(version_cids("m", None).len(), 1);
    }

    #[test]
    fn reconcile_preserves_shared_installed_and_claimed_roots() {
        let first = manifest(&["shared", "old"]);
        let second = manifest(&["shared", "new"]);
        let claim: HashSet<String> = ["old".to_string()].into();
        let ledger = set(&["a", "b", "shared", "old", "new"]);
        let desired = desired_cids([("a", &first), ("b", &second)], [&claim]);
        assert!(release_plan(&ledger, &desired).is_empty());
        let desired = desired_cids([("b", &second)], [&claim]);
        assert_eq!(release_plan(&ledger, &desired), ["a"]);
        let desired = desired_cids([("b", &second)], []);
        assert_eq!(release_plan(&ledger, &desired), ["a", "old"]);
    }

    #[test]
    fn pin_ledger_round_trip() {
        let path = temp_path().with_file_name("pins.json");
        assert!(PinLedger::load(path.clone()).unwrap().is_none());
        let mut ledger = PinLedger::empty(path.clone());
        ledger.roots.insert("bafkreiabc".into());
        ledger.save().unwrap();
        let back = PinLedger::load(path.clone()).unwrap().unwrap();
        assert_eq!(back.roots, ledger.roots);
        std::fs::write(&path, b"garbage").unwrap();
        assert!(PinLedger::load(path.clone()).is_err());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn swap_seq_guard() {
        let id = crate::identity::for_test();
        let mut st = WebState::default();
        st.upsert_record(rec(&id, "g", 5), NOW);
        let site = rec(&id, "g", 5).site;
        let e = st.sites.get_mut(&site).unwrap();
        assert!(may_install(e, 5), "first install of the held record");
        assert!(!may_install(e, 6), "newer than the record held");
        e.installed = Some("bafkreiabc".into());
        e.installed_seq = Some(5);
        assert!(!may_install(e, 5), "same version again");
        assert!(!may_install(e, 4), "rollback");
        st.upsert_record(rec(&id, "g", 7), NOW);
        assert!(may_install(&st.sites[&site], 7));
    }

    #[test]
    fn load_skips_the_clock_skew_check_and_migrates_installed_seq() {
        let id = crate::identity::for_test();
        let path = temp_path();
        // Dated a day ahead: accepted while the clock was ahead, it must
        // survive a reload after the clock is corrected.
        let future = NOW + 24 * 3600 * 1000;
        let raw = site::build_record(&id, "g", "bafkreiabc", 10, 1, 3, future).unwrap();
        let site_id = raw["site"].as_str().unwrap().to_string();
        let mut sites = serde_json::Map::new();
        sites.insert(
            site_id.clone(),
            json!({"record": raw, "installed": "bafkreiabc", "trusted": true}),
        );
        let file = json!({ "sites": sites });
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();
        let back = WebState::load(&path).unwrap();
        let e = &back.sites[&site_id];
        assert_eq!(e.record.at, future);
        assert_eq!(
            e.installed_seq,
            Some(3),
            "migrated from the matching record"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
