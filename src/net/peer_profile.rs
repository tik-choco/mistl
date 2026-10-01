//! Signed, transport-bound peer profiles. Names and avatars are self-claimed
//! presentation metadata and must never be used for trust decisions.

use crate::daemon::AppState;
use crate::identity::{Identity, Profile};
use anyhow::{Context, Result, bail};
use base64::Engine as _;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::sync::{Arc, LazyLock, Mutex, Once};
use std::time::{Duration, Instant};

const TAG: &str = "mistl-profile-v1";
const MAX_BYTES: usize = 24 * 1024;
const MAX_PROFILES: usize = 512;
const MAX_THROTTLES: usize = 1024;
const RECEIVE_INTERVAL: Duration = Duration::from_secs(5);
const DIRECT_INTERVAL: Duration = Duration::from_secs(60);
const BROADCAST_INTERVAL: Duration = Duration::from_secs(300);
const SAVE_INTERVAL: Duration = Duration::from_secs(30);
const FILE: &str = "peer-profiles.json";

/// A valid signature over self-claimed metadata is not an endorsement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerProfile {
    pub did: String,
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_cid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    pub last_seen_ms: u64,
    // Persisted for ordering across restarts; omitted from IPC.
    ts_ms: u64,
}

/// Shared by `profile.set` and wire validation. No SVG or external URLs.
pub(crate) fn valid_avatar(value: &str) -> bool {
    if value.len() > 16384 {
        return false;
    }
    let Some(encoded) = [
        "data:image/webp;base64,",
        "data:image/png;base64,",
        "data:image/jpeg;base64,",
    ]
    .iter()
    .find_map(|prefix| value.strip_prefix(prefix)) else {
        return false;
    };
    !encoded.is_empty()
        && base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .is_ok()
}

fn optional_string(value: &Value, key: &str, max: usize) -> Result<Option<String>> {
    match value.get(key) {
        None => Ok(None),
        Some(Value::String(s)) if s.chars().count() <= max => Ok(Some(s.clone())),
        _ => bail!("invalid profile field: {key}"),
    }
}

fn timestamp(value: Option<&str>) -> Option<DateTime<FixedOffset>> {
    value.and_then(|s| DateTime::parse_from_rfc3339(s).ok())
}

fn valid_fields(p: &PeerProfile) -> bool {
    p.did.len() == crate::identity::ED25519_DID_KEY_LEN
        && crate::identity::pubkey_from_did(&p.did).is_ok()
        && crate::identity::node_id_for_did(&p.did) == p.node_id
        && p.name.as_ref().is_none_or(|v| v.chars().count() <= 64)
        && p.bio.as_ref().is_none_or(|v| v.chars().count() <= 280)
        && p.avatar_cid
            .as_ref()
            .is_none_or(|v| v.chars().count() <= 128)
        && p.avatar.as_ref().is_none_or(|v| valid_avatar(v))
        && p.updated_at
            .as_ref()
            .is_none_or(|v| v.len() <= 64 && timestamp(Some(v)).is_some())
}

fn check_wire(data: &[u8], from: &str, now_ms: u64) -> Result<PeerProfile> {
    if data.len() > MAX_BYTES || !data.windows(TAG.len()).any(|w| w == TAG.as_bytes()) {
        bail!("foreign or oversized profile");
    }
    let value: Value = serde_json::from_slice(data)?;
    if value.get("t").and_then(Value::as_str) != Some(TAG)
        || ["type", "v", "kind"]
            .iter()
            .any(|key| value.get(key).is_some())
    {
        bail!("foreign profile tag");
    }
    let node = value
        .get("node")
        .and_then(Value::as_str)
        .context("missing node")?;
    if node != from {
        bail!("profile node differs from transport sender");
    }
    let did = value
        .get("fromId")
        .and_then(Value::as_str)
        .context("missing DID")?;
    if did.len() != crate::identity::ED25519_DID_KEY_LEN
        || crate::identity::node_id_for_did(did) != from
    {
        bail!("profile DID differs from transport sender");
    }
    let ts_ms = value
        .get("ts")
        .and_then(Value::as_u64)
        .context("invalid timestamp")?;
    if ts_ms.abs_diff(now_ms) > super::peer_auth::MAX_CLOCK_SKEW_MS {
        bail!("stale profile timestamp");
    }
    let profile = PeerProfile {
        did: did.to_string(),
        node_id: node.to_string(),
        name: optional_string(&value, "name", 64)?,
        bio: optional_string(&value, "bio", 280)?,
        avatar_cid: optional_string(&value, "avatar_cid", 128)?,
        avatar: optional_string(&value, "avatar", 16384)?,
        updated_at: optional_string(&value, "updated_at", 64)?,
        ts_ms,
        last_seen_ms: now_ms,
    };
    if !valid_fields(&profile)
        || timestamp(profile.updated_at.as_deref()).is_some_and(|t| {
            t.timestamp_millis() > now_ms.saturating_add(super::peer_auth::MAX_CLOCK_SKEW_MS) as i64
        })
    {
        bail!("invalid profile fields");
    }
    // Expensive signature verification is deliberately last.
    if !crate::wiresign::verify_wire(&value).unwrap_or(false) {
        bail!("invalid profile signature");
    }
    Ok(profile)
}

fn build_wire(identity: &Identity, profile: &Profile, now_ms: u64) -> Result<Vec<u8>> {
    let mut wire =
        json!({"t": TAG, "node": identity.node_id(), "fromId": identity.did(), "ts": now_ms});
    for (key, value) in [
        ("name", &profile.display_name),
        ("bio", &profile.bio),
        ("avatar_cid", &profile.avatar_cid),
        ("avatar", &profile.avatar_thumb),
        ("updated_at", &profile.updated_at),
    ] {
        if let Some(value) = value {
            wire[key] = json!(value);
        }
    }
    crate::wiresign::sign_wire(&mut wire, identity)?;
    let bytes = serde_json::to_vec(&wire)?;
    check_wire(&bytes, &identity.node_id(), now_ms)?;
    Ok(bytes)
}

#[derive(Default)]
struct Throttle {
    entries: HashMap<(String, String), Instant>,
}

impl Throttle {
    fn allowed(&self, key: &(String, String), now: Instant, interval: Duration) -> bool {
        self.entries
            .get(key)
            .is_none_or(|last| now.saturating_duration_since(*last) >= interval)
    }
    fn take(&mut self, key: (String, String), now: Instant, interval: Duration) -> bool {
        if !self.allowed(&key, now, interval) {
            return false;
        }
        self.entries
            .retain(|_, last| now.saturating_duration_since(*last) < interval);
        if self.entries.len() >= MAX_THROTTLES {
            return false;
        }
        self.entries.insert(key, now);
        true
    }
}

#[derive(Default)]
struct Cache {
    profiles: HashMap<String, PeerProfile>,
    receive: Throttle,
    generation: u64,
}

impl Cache {
    fn accept(&mut self, mut profile: PeerProfile, now: Instant, wall_ms: u64) -> bool {
        if !self.receive.take(
            (String::new(), profile.node_id.clone()),
            now,
            RECEIVE_INTERVAL,
        ) {
            return false;
        }
        profile.last_seen_ms = wall_ms;
        if let Some(old) = self.profiles.get_mut(&profile.did) {
            let newer = (timestamp(profile.updated_at.as_deref()), profile.ts_ms)
                > (timestamp(old.updated_at.as_deref()), old.ts_ms);
            if newer {
                *old = profile;
            } else {
                old.last_seen_ms = wall_ms;
            }
        } else {
            self.insert(profile);
        }
        self.generation += 1;
        true
    }
    fn insert(&mut self, profile: PeerProfile) {
        if self.profiles.len() >= MAX_PROFILES
            && !self.profiles.contains_key(&profile.did)
            && let Some(oldest) = self
                .profiles
                .values()
                .min_by_key(|p| p.last_seen_ms)
                .map(|p| p.did.clone())
        {
            self.profiles.remove(&oldest);
        }
        self.profiles.insert(profile.did.clone(), profile);
    }
}

static CACHE: LazyLock<Mutex<Cache>> = LazyLock::new(|| Mutex::new(Cache::default()));
static LOADED: Once = Once::new();
static DIRECT: LazyLock<Mutex<Throttle>> = LazyLock::new(|| Mutex::new(Throttle::default()));

fn read_cache(path: &std::path::Path) -> Result<Vec<PeerProfile>> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let limit = (MAX_PROFILES * MAX_BYTES) as u64;
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!("oversized profile cache");
    }
    let mut profiles: Vec<PeerProfile> = serde_json::from_slice(&bytes)?;
    profiles.retain(valid_fields);
    profiles.sort_by_key(|p| p.last_seen_ms);
    Ok(profiles)
}

fn ensure_loaded() {
    LOADED.call_once(|| {
        let result = (|| -> Result<Vec<PeerProfile>> {
            read_cache(&crate::config::data_dir()?.join(FILE))
        })();
        match result {
            Ok(profiles) => {
                let mut cache = CACHE.lock().expect("profile cache poisoned");
                for profile in profiles {
                    cache.insert(profile);
                }
            }
            Err(error) => tracing::warn!(%error, "peer profiles: cannot load cache"),
        }
    });
}

pub fn lookup(did: &str) -> Option<PeerProfile> {
    ensure_loaded();
    CACHE
        .lock()
        .expect("profile cache poisoned")
        .profiles
        .get(did)
        .cloned()
}

pub fn display_name(did: &str) -> Option<String> {
    lookup(did)
        .and_then(|p| p.name)
        .filter(|name| !name.is_empty())
}

fn peers_view(
    profiles: Vec<PeerProfile>,
    verified: Vec<(String, String, String)>,
    own_did: &str,
) -> Vec<Value> {
    let mut entries = BTreeMap::new();
    for profile in profiles {
        if profile.did == own_did {
            continue;
        }
        let mut value = serde_json::to_value(profile).expect("profile serialization");
        value.as_object_mut().unwrap().remove("ts_ms");
        value["online"] = json!(false);
        value["rooms"] = json!([]);
        entries.insert(value["did"].as_str().unwrap().to_string(), value);
    }
    for (room, node, did) in verified {
        if did == own_did {
            continue;
        }
        let value = entries
            .entry(did.clone())
            .or_insert_with(|| json!({"did": did, "node_id": node, "rooms": [], "online": true}));
        value["online"] = json!(true);
        let rooms = value["rooms"].as_array_mut().unwrap();
        if !rooms.contains(&json!(room)) {
            rooms.push(json!(room));
        }
    }
    let mut peers: Vec<Value> = entries.into_values().collect();
    for peer in &mut peers {
        peer["rooms"]
            .as_array_mut()
            .unwrap()
            .sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    }
    peers.sort_by(|a, b| {
        b["online"]
            .as_bool()
            .cmp(&a["online"].as_bool())
            .then_with(|| {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .to_lowercase()
                    .cmp(&b["name"].as_str().unwrap_or("").to_lowercase())
            })
            .then_with(|| a["did"].as_str().cmp(&b["did"].as_str()))
    });
    peers
}

pub async fn handle(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value> {
    if !matches!(cmd, "peers.ls" | "peers.get") {
        bail!("unknown command: {cmd}");
    }
    let identity = crate::identity::current(state).await?;
    ensure_loaded();
    let profiles = CACHE
        .lock()
        .expect("profile cache poisoned")
        .profiles
        .values()
        .cloned()
        .collect();
    let peers = peers_view(profiles, super::peer_auth::verified_peers(), identity.did());
    match cmd {
        "peers.ls" => Ok(json!({"peers": peers})),
        _ => {
            let did = args
                .get("did")
                .and_then(Value::as_str)
                .context("missing `did`")?;
            Ok(peers
                .into_iter()
                .find(|p| p["did"] == did)
                .unwrap_or(Value::Null))
        }
    }
}

/// Sends after a successful local save, only into rooms already joined.
///
/// Receivers accept one profile per sender per [`RECEIVE_INTERVAL`], so a
/// second edit made right after the first (the dashboard saves one field per
/// call) would be dropped until the next periodic broadcast. A single
/// trailing re-send of the latest saved profile, issued once the receivers'
/// window has passed, closes that gap; rapid edits coalesce into one.
pub(crate) async fn broadcast(identity: &Arc<Identity>, profile: &Profile) {
    send_to_rooms(identity, profile).await;
    let ticket = TRAILING.claim();
    let identity = Arc::clone(identity);
    tokio::spawn(async move {
        tokio::time::sleep(RECEIVE_INTERVAL + Duration::from_secs(1)).await;
        if TRAILING.is_latest(ticket)
            && let Ok(profile) = crate::identity::load_profile()
        {
            send_to_rooms(&identity, &profile).await;
        }
    });
}

async fn send_to_rooms(identity: &Identity, profile: &Profile) {
    let bytes = match build_wire(identity, profile, super::peer_auth::now_ms()) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::debug!(%error, "peer profiles: cannot build local profile");
            return;
        }
    };
    for room in super::joined_rooms().await {
        let _ = super::send_broadcast(&room, bytes.clone()).await;
    }
}

/// Hands out tickets so only the most recent broadcast's trailing re-send
/// actually goes out.
struct Trailing(std::sync::atomic::AtomicU64);

impl Trailing {
    const fn new() -> Self {
        Self(std::sync::atomic::AtomicU64::new(0))
    }
    fn claim(&self) -> u64 {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
    }
    fn is_latest(&self, ticket: u64) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst) == ticket
    }
}

static TRAILING: Trailing = Trailing::new();

/// Called on the dispatch thread after a verified hello. Reads and sends run
/// on the captured runtime; the throttle also bounds spawned work.
pub(super) fn send_to(
    room: &str,
    peer: &str,
    identity: Arc<Identity>,
    runtime: &tokio::runtime::Handle,
) {
    if !DIRECT.lock().expect("profile throttle poisoned").take(
        (room.to_string(), peer.to_string()),
        Instant::now(),
        DIRECT_INTERVAL,
    ) {
        return;
    }
    let room = room.to_string();
    let peer = peer.to_string();
    runtime.spawn(async move {
        if let Ok(profile) = crate::identity::load_profile()
            && let Ok(bytes) = build_wire(&identity, &profile, super::peer_auth::now_ms())
        {
            let _ = super::send_direct(&room, &peer, bytes).await;
        }
    });
}

pub(super) fn start(identity: Arc<Identity>, runtime: tokio::runtime::Handle) {
    ensure_loaded();
    let own_node = identity.node_id();
    super::register_room_handler(move |event, _room, from, data| {
        if event != super::EVENT_RAW
            || from.len() != 16
            || from == own_node
            || data.len() > MAX_BYTES
            || !data.windows(TAG.len()).any(|w| w == TAG.as_bytes())
        {
            return;
        }
        let now = Instant::now();
        if !CACHE
            .lock()
            .expect("profile cache poisoned")
            .receive
            .allowed(&(String::new(), from.to_string()), now, RECEIVE_INTERVAL)
        {
            return;
        }
        let wall_ms = super::peer_auth::now_ms();
        if let Ok(profile) = check_wire(data, from, wall_ms) {
            CACHE
                .lock()
                .expect("profile cache poisoned")
                .accept(profile, now, wall_ms);
        }
    });
    runtime.spawn(async move {
        let mut tick = tokio::time::interval(BROADCAST_INTERVAL);
        loop {
            tick.tick().await;
            if let Ok(profile) = crate::identity::load_profile() {
                broadcast(&identity, &profile).await;
            }
        }
    });
    runtime.spawn(async move {
        let mut tick = tokio::time::interval(SAVE_INTERVAL);
        let mut saved_generation = 0;
        loop {
            tick.tick().await;
            let snapshot = {
                let cache = CACHE.lock().expect("profile cache poisoned");
                (cache.generation != saved_generation).then(|| {
                    (
                        cache.generation,
                        cache.profiles.values().cloned().collect::<Vec<_>>(),
                    )
                })
            };
            let Some((generation, profiles)) = snapshot else {
                continue;
            };
            let result = tokio::task::spawn_blocking(move || -> Result<()> {
                let bytes = serde_json::to_vec(&profiles)?;
                crate::statefile::write_private(&crate::config::data_dir()?.join(FILE), &bytes)
            })
            .await;
            match result {
                Ok(Ok(())) => saved_generation = generation,
                error => tracing::warn!(?error, "peer profiles: cannot save cache"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_cache_round_trip_preserves_ordering_and_rejects_invalid_fields() {
        let id = crate::identity::for_test();
        let path = std::env::temp_dir().join(format!(
            "mistl-profile-test-{:032x}.json",
            rand::random::<u128>()
        ));
        assert!(read_cache(&path).unwrap().is_empty());
        let mut p = sample(&id, 123);
        p.updated_at = Some("2026-01-01T00:00:00Z".into());
        p.avatar = Some("data:image/png;base64,YWJj".into());
        let mut invalid = p.clone();
        invalid.avatar = Some("https://example.com/avatar.png".into());
        crate::statefile::write_private(
            &path,
            &serde_json::to_vec(&vec![p.clone(), invalid]).unwrap(),
        )
        .unwrap();
        assert_eq!(read_cache(&path).unwrap(), vec![p]);
        crate::statefile::write_private(&path, b"corrupt").unwrap();
        assert!(read_cache(&path).is_err());
        let huge = vec![b' '; MAX_PROFILES * MAX_BYTES + 1];
        crate::statefile::write_private(&path, &huge).unwrap();
        assert!(
            read_cache(&path)
                .unwrap_err()
                .to_string()
                .contains("oversized")
        );
        std::fs::remove_file(path).unwrap();
    }

    fn sample(id: &Identity, ts: u64) -> PeerProfile {
        PeerProfile {
            did: id.did().into(),
            node_id: id.node_id(),
            name: Some("Ada".into()),
            bio: None,
            avatar: None,
            avatar_cid: None,
            updated_at: None,
            last_seen_ms: ts,
            ts_ms: ts,
        }
    }

    fn wire(id: &Identity, now: u64) -> Value {
        serde_json::from_slice(
            &build_wire(
                id,
                &Profile {
                    display_name: Some("Ada".into()),
                    bio: Some("Hello".into()),
                    avatar_thumb: Some("data:image/webp;base64,YWJj".into()),
                    ..Profile::default()
                },
                now,
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn checked(value: &Value, from: &str, now: u64) -> Result<PeerProfile> {
        check_wire(&serde_json::to_vec(value).unwrap(), from, now)
    }

    #[test]
    fn trailing_resend_only_fires_for_the_latest_edit() {
        let t = Trailing::new();
        let first = t.claim();
        assert!(t.is_latest(first));
        let second = t.claim();
        assert!(!t.is_latest(first), "an older edit's re-send is superseded");
        assert!(t.is_latest(second));
    }

    #[test]
    fn wire_round_trip_and_demux() {
        let id = crate::identity::for_test();
        let now = super::super::peer_auth::now_ms();
        let value = wire(&id, now);
        let p = checked(&value, &id.node_id(), now).unwrap();
        assert_eq!(p.did, id.did());
        assert_eq!(p.name.as_deref(), Some("Ada"));
        assert_eq!(p.avatar.as_deref(), Some("data:image/webp;base64,YWJj"));
        for key in ["type", "v", "kind"] {
            assert!(value.get(key).is_none());
        }
        assert!(check_wire(br#"{"t":"someone-else"}"#, &id.node_id(), now).is_err());
    }

    #[test]
    fn forged_node_did_and_signature_rejected() {
        let id = crate::identity::for_test();
        let other = crate::identity::for_test();
        let now = super::super::peer_auth::now_ms();
        let value = wire(&id, now);
        assert!(
            checked(&value, &other.node_id(), now)
                .unwrap_err()
                .to_string()
                .contains("node")
        );
        let mut forged = value.clone();
        forged["node"] = json!(other.node_id());
        crate::wiresign::sign_wire(&mut forged, &id).unwrap();
        assert!(
            checked(&forged, &other.node_id(), now)
                .unwrap_err()
                .to_string()
                .contains("DID")
        );
        let mut bad_sig = value;
        bad_sig["name"] = json!("Mallory");
        assert!(
            checked(&bad_sig, &id.node_id(), now)
                .unwrap_err()
                .to_string()
                .contains("signature")
        );
    }

    #[test]
    fn oversized_and_invalid_fields_rejected_before_signature() {
        let id = crate::identity::for_test();
        let now = super::super::peer_auth::now_ms();
        assert!(check_wire(&vec![b' '; MAX_BYTES + 1], &id.node_id(), now).is_err());
        for (key, bad) in [
            ("name", json!("名".repeat(65))),
            ("bio", json!("x".repeat(281))),
            ("avatar_cid", json!("x".repeat(129))),
            ("name", json!(null)),
            ("avatar", json!("data:image/svg+xml;base64,YWJj")),
            ("avatar", json!("data:image/png;base64,!!!!")),
            (
                "avatar",
                json!(format!("data:image/png;base64,{}", "A".repeat(16384))),
            ),
            ("updated_at", json!("yesterday")),
            ("type", json!("other-module")),
        ] {
            let mut value = wire(&id, now);
            value[key] = bad;
            crate::wiresign::sign_wire(&mut value, &id).unwrap();
            assert!(
                checked(&value, &id.node_id(), now).is_err(),
                "accepted {key}"
            );
        }
        let mut huge = wire(&id, now);
        huge["extra"] = json!("x".repeat(MAX_BYTES));
        crate::wiresign::sign_wire(&mut huge, &id).unwrap();
        assert!(checked(&huge, &id.node_id(), now).is_err());
    }

    #[test]
    fn unicode_field_caps_and_avatar_boundary() {
        let id = crate::identity::for_test();
        let now = super::super::peer_auth::now_ms();
        let mut value = wire(&id, now);
        value["name"] = json!("名".repeat(64));
        value["bio"] = json!("あ".repeat(280));
        value["avatar_cid"] = json!("x".repeat(128));
        let prefix = "data:image/png;base64,";
        value["avatar"] = json!(format!(
            "{prefix}{}",
            "A".repeat((16384 - prefix.len()) / 4 * 4)
        ));
        crate::wiresign::sign_wire(&mut value, &id).unwrap();
        assert!(checked(&value, &id.node_id(), now).is_ok());
    }

    #[test]
    fn stale_and_future_timestamps_rejected() {
        let id = crate::identity::for_test();
        let now = super::super::peer_auth::now_ms();
        let mut value = wire(&id, now);
        value["ts"] = json!(now - super::super::peer_auth::MAX_CLOCK_SKEW_MS - 1);
        crate::wiresign::sign_wire(&mut value, &id).unwrap();
        assert!(checked(&value, &id.node_id(), now).is_err());
        value["ts"] = json!(now);
        value["updated_at"] = json!("9999-01-01T00:00:00Z");
        crate::wiresign::sign_wire(&mut value, &id).unwrap();
        assert!(checked(&value, &id.node_id(), now).is_err());
    }

    #[test]
    fn newest_wins_by_instant_then_wire_timestamp_and_clear() {
        let id = crate::identity::for_test();
        let mut cache = Cache::default();
        let now = Instant::now();
        let mut first = sample(&id, 10);
        first.updated_at = Some("2026-01-01T01:00:00+01:00".into());
        assert!(cache.accept(first.clone(), now, 10));
        let mut older = first.clone();
        older.name = Some("Old".into());
        older.updated_at = Some("2025-12-31T23:59:59Z".into());
        older.ts_ms = 999;
        assert!(cache.accept(older, now + RECEIVE_INTERVAL, 20));
        assert_eq!(cache.profiles[id.did()].name.as_deref(), Some("Ada"));
        assert_eq!(cache.profiles[id.did()].last_seen_ms, 20);
        first.updated_at = Some("2026-01-01T00:00:00Z".into());
        first.ts_ms = 11;
        first.name = None;
        assert!(cache.accept(first, now + RECEIVE_INTERVAL * 2, 30));
        assert!(cache.profiles[id.did()].name.is_none());
    }

    #[test]
    fn cache_evicts_least_recently_seen() {
        let mut cache = Cache::default();
        let id = crate::identity::for_test();
        for i in 0..MAX_PROFILES {
            let mut p = sample(&id, i as u64);
            p.did = format!("did-{i}");
            cache.insert(p);
        }
        cache.profiles.get_mut("did-0").unwrap().last_seen_ms = 10000;
        cache.insert(sample(&id, 10001));
        assert_eq!(cache.profiles.len(), MAX_PROFILES);
        assert!(cache.profiles.contains_key("did-0"));
        assert!(!cache.profiles.contains_key("did-1"));
    }

    #[test]
    fn receive_rate_limit_is_per_sender_across_rooms() {
        let id = crate::identity::for_test();
        let other = crate::identity::for_test();
        let mut cache = Cache::default();
        let now = Instant::now();
        assert!(cache.accept(sample(&id, 0), now, 0));
        assert!(!cache.accept(sample(&id, 1), now + Duration::from_millis(4999), 1));
        assert!(cache.accept(sample(&other, 1), now, 1));
        assert!(cache.accept(sample(&id, 2), now + RECEIVE_INTERVAL, 2));
    }

    #[test]
    fn direct_throttle_is_room_scoped_and_bounded() {
        let mut throttle = Throttle::default();
        let now = Instant::now();
        let key = ("a".into(), "peer".into());
        assert!(throttle.take(key.clone(), now, DIRECT_INTERVAL));
        assert!(!throttle.take(key.clone(), now + Duration::from_secs(59), DIRECT_INTERVAL));
        assert!(throttle.take(("b".into(), "peer".into()), now, DIRECT_INTERVAL));
        for i in 2..MAX_THROTTLES {
            assert!(throttle.take(("a".into(), i.to_string()), now, DIRECT_INTERVAL));
        }
        assert!(!throttle.take(("a".into(), "overflow".into()), now, DIRECT_INTERVAL));
        assert_eq!(throttle.entries.len(), MAX_THROTTLES);
        assert!(throttle.take(key, now + DIRECT_INTERVAL, DIRECT_INTERVAL));
    }

    #[test]
    fn peers_ls_shape_includes_online_unknown_and_cached_offline() {
        let own = crate::identity::for_test();
        let online = crate::identity::for_test();
        let unknown = crate::identity::for_test();
        let offline = crate::identity::for_test();
        let mut online_profile = sample(&online, 1);
        online_profile.name = Some("Zed".into());
        let mut offline_profile = sample(&offline, 1);
        offline_profile.name = Some("Aaron".into());
        let bindings = vec![
            ("b".into(), online.node_id(), online.did().into()),
            ("a".into(), online.node_id(), online.did().into()),
            ("a".into(), online.node_id(), online.did().into()),
            ("a".into(), unknown.node_id(), unknown.did().into()),
            ("a".into(), own.node_id(), own.did().into()),
        ];
        let result = json!({"peers": peers_view(vec![sample(&own, 1), online_profile, offline_profile], bindings, own.did())});
        let peers = result["peers"].as_array().unwrap();
        assert_eq!(peers.len(), 3);
        assert_eq!(peers[0]["did"], unknown.did());
        assert!(peers[0].get("name").is_none());
        assert_eq!(peers[1]["name"], "Zed");
        assert_eq!(peers[1]["rooms"], json!(["a", "b"]));
        assert_eq!(peers[1]["online"], true);
        assert!(peers[1].get("ts_ms").is_none());
        assert_eq!(peers[2]["online"], false);
        assert_eq!(peers[2]["rooms"], json!([]));
        assert_eq!(peers[2]["last_seen_ms"], 1);
    }
}
