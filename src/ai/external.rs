//! Local application registrations, kept separate from the user's configuration.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::config::{AiConfig, AiProviderConfig, Config, ModelRef};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalProvider {
    pub id: String,
    pub label: String,
    pub base_url: String,
    pub api_key: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalRoom {
    pub room: String,
    pub consume: bool,
    pub provide: bool,
    pub shared: Vec<ModelRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub owner: String,
    #[serde(default)]
    pub label: Option<String>,
    pub providers: Vec<ExternalProvider>,
    pub rooms: Vec<ExternalRoom>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredRegistration {
    #[serde(flatten)]
    registration: Registration,
    updated_at: String,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    registrations: Vec<StoredRegistration>,
}

#[derive(Default)]
pub(crate) struct Store {
    path: Option<PathBuf>,
    registrations: BTreeMap<String, StoredRegistration>,
    caches: HashMap<String, (AiProviderConfig, Vec<String>, String)>,
}

pub fn validate_owner(owner: &str) -> Result<()> {
    ensure!(
        !owner.is_empty()
            && owner.len() <= 32
            && owner
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            && owner
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "owner must match [a-z0-9][a-z0-9-]{{0,31}}"
    );
    Ok(())
}

fn text(value: &str, name: &str) -> Result<()> {
    ensure!(
        !value.trim().is_empty() && !value.chars().any(char::is_control),
        "invalid {name}"
    );
    Ok(())
}

impl Registration {
    pub fn validate(&mut self) -> Result<()> {
        validate_owner(&self.owner)?;
        if self.label.is_none() {
            self.label = Some(self.owner.clone());
        }
        text(self.label.as_deref().unwrap(), "label")?;
        ensure!(
            self.providers.len() <= 64,
            "at most 64 providers are allowed"
        );
        ensure!(self.rooms.len() <= 64, "at most 64 rooms are allowed");
        let mut ids = HashSet::new();
        for provider in &self.providers {
            text(&provider.id, "provider id")?;
            text(&provider.label, "provider label")?;
            ensure!(ids.insert(provider.id.as_str()), "duplicate provider id");
            let url =
                reqwest::Url::parse(&provider.base_url).context("invalid provider base_url")?;
            ensure!(
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
                "provider base_url must use http or https"
            );
            ensure!(
                url.username().is_empty() && url.password().is_none(),
                "base_url must not contain credentials"
            );
        }
        let mut rooms = HashSet::new();
        let mut refs = 0;
        for room in &self.rooms {
            ensure!(
                !room.room.is_empty()
                    && room.room.len() <= 64
                    && room
                        .room
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                "room must be 1-64 characters of [A-Za-z0-9_-]"
            );
            ensure!(rooms.insert(room.room.as_str()), "duplicate room");
            refs += room.shared.len();
            ensure!(refs <= 512, "at most 512 shared refs are allowed");
            for reference in &room.shared {
                ensure!(
                    ids.contains(reference.provider_id.as_str()),
                    "shared provider_id must belong to this registration"
                );
                text(&reference.model, "shared model")?;
            }
        }
        Ok(())
    }
}

impl Store {
    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join("ai-external.json");
        crate::statefile::restrict_existing(&path);
        let file: File = crate::statefile::read(data_dir, "ai-external.json")?;
        let mut registrations = BTreeMap::new();
        for mut stored in file.registrations {
            stored.registration.validate()?;
            ensure!(
                !registrations.contains_key(&stored.registration.owner),
                "duplicate external owner on disk"
            );
            registrations.insert(stored.registration.owner.clone(), stored);
        }
        Ok(Self {
            path: Some(path),
            registrations,
            ..Default::default()
        })
    }

    fn persist(&self, registrations: &BTreeMap<String, StoredRegistration>) -> Result<()> {
        if let Some(path) = &self.path {
            let file = File {
                registrations: registrations.values().cloned().collect(),
            };
            crate::statefile::write_private(path, &serde_json::to_vec_pretty(&file)?)?;
        }
        Ok(())
    }

    pub fn apply(&mut self, mut registration: Registration) -> Result<bool> {
        registration.validate()?;
        if self
            .registrations
            .get(&registration.owner)
            .is_some_and(|old| old.registration == registration)
        {
            return Ok(false);
        }
        let mut next = self.registrations.clone();
        next.insert(
            registration.owner.clone(),
            StoredRegistration {
                registration,
                updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            },
        );
        self.persist(&next)?;
        self.registrations = next;
        self.prune_caches();
        Ok(true)
    }

    pub fn remove(&mut self, owner: &str) -> Result<bool> {
        validate_owner(owner)?;
        let mut next = self.registrations.clone();
        if next.remove(owner).is_none() {
            return Ok(false);
        }
        self.persist(&next)?;
        self.registrations = next;
        self.prune_caches();
        Ok(true)
    }

    fn prune_caches(&mut self) {
        let effective = self.merge(Config::default());
        self.caches.retain(|id, (old, _, _)| {
            effective
                .ai
                .providers
                .iter()
                .any(|p| &p.id == id && super::model_discovery::same_connection(old, p))
        });
    }

    pub fn owners(&self, room: &str) -> Vec<String> {
        self.registrations
            .values()
            .filter(|s| s.registration.rooms.iter().any(|r| r.room == room))
            .map(|s| s.registration.owner.clone())
            .collect()
    }

    pub fn consuming_rooms(&self, ai: &AiConfig) -> HashSet<String> {
        self.registrations
            .values()
            .flat_map(|s| &s.registration.rooms)
            .filter(|r| {
                r.consume
                    && ai
                        .providers
                        .iter()
                        .any(|p| p.enabled && p.room() == Some(r.room.as_str()))
            })
            .map(|r| r.room.clone())
            .collect()
    }

    pub fn warnings(&self, owner: &str, ai: &AiConfig) -> Vec<String> {
        self.registrations
            .get(owner)
            .into_iter()
            .flat_map(|s| &s.registration.rooms)
            .filter(|r| {
                ai.providers
                    .iter()
                    .any(|p| !p.enabled && p.room() == Some(r.room.as_str()))
            })
            .map(|r| format!("room {} is disabled by the user", r.room))
            .collect()
    }

    pub fn merge(&self, mut config: Config) -> Config {
        for stored in self.registrations.values() {
            let reg = &stored.registration;
            for p in &reg.providers {
                config.ai.providers.push(AiProviderConfig {
                    id: format!("ext:{}:{}", reg.owner, p.id),
                    label: p.label.clone(),
                    base_url: p.base_url.clone(),
                    api_key: p.api_key.clone(),
                    enabled: p.enabled,
                    ..Default::default()
                });
            }
            for room in &reg.rooms {
                if config
                    .ai
                    .providers
                    .iter()
                    .any(|p| !p.enabled && p.room() == Some(room.room.as_str()))
                {
                    continue;
                }
                let index = config
                    .ai
                    .providers
                    .iter()
                    .position(|p| p.room() == Some(room.room.as_str()))
                    .unwrap_or_else(|| {
                        config.ai.providers.push(AiProviderConfig {
                            id: format!("ext:{}:room:{}", reg.owner, room.room),
                            label: room.room.clone(),
                            base_url: format!("mist-network://{}", room.room),
                            ..Default::default()
                        });
                        config.ai.providers.len() - 1
                    });
                let target = &mut config.ai.providers[index];
                target.provide |= room.provide;
                for reference in &room.shared {
                    let mapped = ModelRef {
                        provider_id: format!("ext:{}:{}", reg.owner, reference.provider_id),
                        model: reference.model.clone(),
                    };
                    if !target.shared.contains(&mapped) {
                        target.shared.push(mapped);
                    }
                }
            }
        }
        for p in &mut config.ai.providers {
            if let Some((old, models, fetched)) = self.caches.get(&p.id)
                && super::model_discovery::same_connection(old, p)
            {
                p.models = models.clone();
                p.models_fetched_at = Some(fetched.clone());
            }
        }
        config
    }

    pub fn cache_models(&mut self, provider: &AiProviderConfig, models: &[String]) {
        let effective = self.merge(Config::default());
        if effective
            .ai
            .providers
            .iter()
            .any(|p| p.id == provider.id && super::model_discovery::same_connection(p, provider))
        {
            self.caches.insert(
                provider.id.clone(),
                (
                    provider.clone(),
                    models.to_vec(),
                    chrono::Utc::now().to_rfc3339(),
                ),
            );
        }
    }

    pub fn get(&self, owner: Option<&str>, ai: &AiConfig, statuses: &[Value]) -> Result<Value> {
        if let Some(owner) = owner {
            validate_owner(owner)?;
        }
        let mut registrations = Vec::new();
        for stored in self
            .registrations
            .values()
            .filter(|s| owner.is_none_or(|o| o == s.registration.owner))
        {
            let mut value = serde_json::to_value(stored)?;
            for provider in value["providers"].as_array_mut().unwrap() {
                provider["api_key"] = json!("***");
            }
            value["warnings"] = json!(self.warnings(&stored.registration.owner, ai));
            let rooms: Vec<_> = stored.registration.rooms.iter().map(|r| {
                    statuses.iter().find(|s| s["room"] == r.room).map(|s| json!({"room":r.room,"joined":s["joined"],"providing":s["providing"],"peers":s["peers"],"models":s["models"].as_array().cloned().unwrap_or_default()})).unwrap_or_else(|| json!({"room":r.room,"joined":false,"providing":false,"peers":0,"models":[]}))
            }).collect();
            value["status"] = json!({"rooms":rooms});
            registrations.push(value);
        }
        Ok(json!({"registrations":registrations}))
    }
}

pub fn stdin_payload(owner: &str, mut value: Value) -> Result<Value> {
    let object = value
        .as_object_mut()
        .context("external apply payload must be a JSON object")?;
    if let Some(existing) = object.get("owner") {
        if existing.as_str() != Some(owner) {
            bail!("payload owner must match --owner");
        }
    }
    object.insert("owner".into(), json!(owner));
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration(owner: &str) -> Registration {
        serde_json::from_value(json!({"owner":owner,"providers":[{"id":"http","label":"Local","base_url":"http://localhost:11434/v1","api_key":"secret","enabled":true}],
            "rooms":[{"room":"test","consume":true,"provide":true,"shared":[{"provider_id":"http","model":"raw"}]}]})).unwrap()
    }

    #[test]
    fn validation_is_all_or_nothing_and_checks_boundaries() {
        let mut store = Store::default();
        store.apply(registration("app")).unwrap();
        let original = store.get(None, &AiConfig::default(), &[]).unwrap();
        let mut invalid = Vec::new();
        for owner in [
            "",
            "App",
            "-app",
            "a_b",
            "abcdefghijklmnopqrstuvwxyz1234567",
        ] {
            invalid.push(registration(owner));
        }
        for url in [
            "mist-network://test",
            "file:///secret",
            "bad",
            "http://user:secret@localhost/v1",
        ] {
            let mut reg = registration("app");
            reg.providers[0].base_url = url.into();
            invalid.push(reg);
        }
        let mut reg = registration("app");
        reg.providers.push(reg.providers[0].clone());
        invalid.push(reg);
        let mut reg = registration("app");
        reg.rooms.push(reg.rooms[0].clone());
        invalid.push(reg);
        let mut reg = registration("app");
        reg.rooms[0].room = "bad/room".into();
        invalid.push(reg);
        let mut reg = registration("app");
        reg.rooms[0].shared[0].provider_id = "other-owner".into();
        invalid.push(reg);
        let mut reg = registration("app");
        reg.rooms[0].shared[0].model.clear();
        invalid.push(reg);
        let mut reg = registration("app");
        reg.providers = (0..65)
            .map(|i| {
                let mut p = reg.providers[0].clone();
                p.id = format!("p{i}");
                p
            })
            .collect();
        invalid.push(reg);
        let mut reg = registration("app");
        reg.rooms = (0..65)
            .map(|i| {
                let mut r = reg.rooms[0].clone();
                r.room = format!("r{i}");
                r
            })
            .collect();
        invalid.push(reg);
        let mut reg = registration("app");
        reg.rooms[0].shared = vec![reg.rooms[0].shared[0].clone(); 513];
        invalid.push(reg);
        for reg in invalid {
            assert!(store.apply(reg).is_err());
            assert_eq!(
                store.get(None, &AiConfig::default(), &[]).unwrap(),
                original
            );
        }
        let mut limit = registration("limit");
        limit.providers = (0..64)
            .map(|i| {
                let mut p = limit.providers[0].clone();
                p.id = format!("p{i}");
                p
            })
            .collect();
        limit.rooms = (0..64)
            .map(|i| ExternalRoom {
                room: format!("r{i}"),
                consume: true,
                provide: false,
                shared: vec![
                    ModelRef {
                        provider_id: "p0".into(),
                        model: "raw".into()
                    };
                    8
                ],
            })
            .collect();
        assert!(store.apply(limit).unwrap());
    }

    #[test]
    fn private_storage_round_trip_idempotence_masking_and_removal() {
        let dir =
            std::env::temp_dir().join(format!("mistl-external-{:016x}", rand::random::<u64>()));
        let mut store = Store::load(&dir).unwrap();
        assert!(store.apply(registration("app")).unwrap());
        let bytes = std::fs::read(dir.join("ai-external.json")).unwrap();
        assert!(!store.apply(registration("app")).unwrap());
        assert_eq!(std::fs::read(dir.join("ai-external.json")).unwrap(), bytes);
        let mut restored = Store::load(&dir).unwrap();
        let shown = restored.get(Some("app"), &AiConfig::default(), &[json!({"room":"test","joined":true,"providing":true,"peers":3,"models":["remote"]})]).unwrap();
        assert_eq!(shown["registrations"][0]["label"], "app");
        assert_eq!(shown["registrations"][0]["providers"][0]["api_key"], "***");
        assert_eq!(shown["registrations"][0]["status"]["rooms"][0]["peers"], 3);
        assert!(!shown.to_string().contains("secret"));
        assert_eq!(
            restored.merge(Config::default()).ai.providers[0].api_key,
            "secret"
        );
        assert!(
            restored
                .get(Some("missing"), &AiConfig::default(), &[])
                .unwrap()["registrations"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(restored.remove("app").unwrap());
        assert!(!restored.remove("app").unwrap());
        assert!(Store::load(&dir).unwrap().registrations.is_empty());
        assert!(!dir.join("config.toml").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join("ai-external.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn merge_unions_user_and_owners_preserves_order_and_removal_restores_user_config() {
        let mut store = Store::default();
        store.apply(registration("a")).unwrap();
        let mut second = registration("b");
        second.rooms[0].provide = false;
        let duplicate = second.rooms[0].shared[0].clone();
        second.rooms[0].shared.push(duplicate);
        store.apply(second).unwrap();
        let mut user = Config::default();
        let own = ModelRef {
            provider_id: "own".into(),
            model: "raw".into(),
        };
        user.ai.providers.push(AiProviderConfig {
            id: "user-room".into(),
            base_url: "mist-network://test".into(),
            shared: vec![own.clone()],
            ..Default::default()
        });
        let before = serde_json::to_value(&user).unwrap();
        let effective = store.merge(user.clone());
        assert_eq!(serde_json::to_value(&user).unwrap(), before);
        let room = &effective.ai.providers[0];
        assert_eq!(room.id, "user-room");
        assert!(room.provide);
        assert_eq!(
            room.shared,
            vec![
                own,
                ModelRef {
                    provider_id: "ext:a:http".into(),
                    model: "raw".into()
                },
                ModelRef {
                    provider_id: "ext:b:http".into(),
                    model: "raw".into()
                }
            ]
        );
        assert_eq!(store.owners("test"), ["a", "b"]);
        assert!(store.consuming_rooms(&effective.ai).contains("test"));
        store.remove("a").unwrap();
        assert!(!store.merge(user.clone()).ai.providers[0].provide);
        user.ai.providers[0].provide = true;
        assert!(store.merge(user.clone()).ai.providers[0].provide);
        store.remove("b").unwrap();
        assert_eq!(
            serde_json::to_value(store.merge(user.clone())).unwrap(),
            serde_json::to_value(user).unwrap()
        );
    }

    #[test]
    fn owners_share_one_synthetic_room_but_user_disabled_room_wins() {
        let mut store = Store::default();
        store.apply(registration("a")).unwrap();
        store.apply(registration("b")).unwrap();
        let effective = store.merge(Config::default());
        let rooms: Vec<_> = effective
            .ai
            .providers
            .iter()
            .filter(|p| p.room().is_some())
            .collect();
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].id, "ext:a:room:test");
        assert_eq!(rooms[0].shared.len(), 2);
        let mut user = Config::default();
        user.ai.providers.push(AiProviderConfig {
            id: "disabled".into(),
            base_url: "mist-network://test".into(),
            enabled: false,
            ..Default::default()
        });
        let effective = store.merge(user.clone());
        assert!(!effective.ai.providers[0].enabled);
        assert!(!effective.ai.providers[0].provide);
        assert!(effective.ai.providers[0].shared.is_empty());
        assert!(store.consuming_rooms(&effective.ai).is_empty());
        assert_eq!(store.warnings("a", &user.ai).len(), 1);
        assert_eq!(
            store.get(None, &user.ai, &[]).unwrap()["registrations"][0]["warnings"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn replace_all_removes_old_contributions_and_stale_model_caches() {
        let mut store = Store::default();
        store.apply(registration("app")).unwrap();
        let p = store.merge(Config::default()).ai.providers[0].clone();
        store.cache_models(&p, &["cached".into()]);
        assert_eq!(
            store.merge(Config::default()).ai.providers[0].models,
            ["cached"]
        );
        let mut changed = registration("app");
        changed.providers[0].api_key = "new-key".into();
        changed.rooms.clear();
        store.apply(changed).unwrap();
        let effective = store.merge(Config::default());
        assert_eq!(effective.ai.providers.len(), 1);
        assert!(effective.ai.providers[0].models.is_empty());
        store.remove("app").unwrap();
        assert!(store.merge(Config::default()).ai.providers.is_empty());
    }

    #[test]
    fn cli_owner_must_match_and_payload_must_be_an_object() {
        assert_eq!(
            stdin_payload("app", json!({"providers":[],"rooms":[]})).unwrap()["owner"],
            "app"
        );
        assert!(stdin_payload("app", json!({"owner":"app"})).is_ok());
        assert!(stdin_payload("app", json!({"owner":"other"})).is_err());
        assert!(stdin_payload("app", json!({"owner":null})).is_err());
        assert!(stdin_payload("app", json!([])).is_err());
    }
}
