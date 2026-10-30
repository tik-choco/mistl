//! Local rules for incoming forward proposals, separate from connection trust.

use std::collections::HashSet;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, OnceCell};

const MAX_RULES: usize = 256;
const MAX_FILE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub action: Action,
    pub peer: String,
    pub proto: String,
    pub ports: String,
    pub hosts: String,
    #[serde(default)]
    pub note: String,
    pub created_ms: u64,
}

/// Optional fields shared by add and update. Add requires an action; update
/// preserves every omitted field, including the daemon-minted id and time.
#[derive(Debug, Default, Deserialize)]
pub struct RulePatch {
    pub action: Option<String>,
    pub peer: Option<String>,
    pub proto: Option<String>,
    pub ports: Option<String>,
    pub hosts: Option<String>,
    pub note: Option<String>,
}

impl Rule {
    fn apply(&mut self, patch: RulePatch) -> Result<()> {
        if let Some(action) = patch.action {
            self.action = match action.trim() {
                "allow" => Action::Allow,
                "deny" => Action::Deny,
                _ => bail!("action must be allow or deny"),
            };
        }
        if let Some(peer) = patch.peer {
            self.peer = peer;
        }
        if let Some(proto) = patch.proto {
            self.proto = proto;
        }
        if let Some(ports) = patch.ports {
            self.ports = ports;
        }
        if let Some(hosts) = patch.hosts {
            self.hosts = hosts;
        }
        if let Some(note) = patch.note {
            self.note = note;
        }
        self.normalize()
    }

    fn normalize(&mut self) -> Result<()> {
        let peer = self.peer.trim();
        if peer == "*" {
            self.peer = peer.to_string();
        } else if peer.len() == 16 && peer.bytes().all(|c| c.is_ascii_hexdigit()) {
            self.peer = peer.to_ascii_lowercase();
        } else if peer.len() <= 256
            && peer.strip_prefix("did:key:z").is_some_and(|key| {
                !key.is_empty()
                    && key.bytes().all(|c| {
                        b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&c)
                    })
            })
        {
            self.peer = peer.to_string();
        } else {
            bail!(
                "peer must be *, a 16-hex node id, or a did:key:z identifier (at most 256 chars)"
            );
        }
        self.proto = self.proto.trim().to_ascii_lowercase();
        if !matches!(self.proto.as_str(), "*" | "tcp" | "udp") {
            bail!("proto must be *, tcp, or udp");
        }
        self.ports = normalize_ports(&self.ports)?;
        self.hosts = normalize_hosts(&self.hosts)?;
        self.note = self.note.trim().to_string();
        if self.note.chars().count() > 200 {
            bail!("note must be at most 200 chars");
        }
        Ok(())
    }
}

fn port_number(text: &str) -> Result<u16> {
    if text.is_empty() || !text.bytes().all(|c| c.is_ascii_digit()) {
        bail!("ports must contain numbers in 1..=65535 or ranges A-B");
    }
    text.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .context("port must be in 1..=65535")
}

fn port_range(text: &str) -> Result<(u16, u16)> {
    if let Some((start, end)) = text.split_once('-') {
        let (start, end) = (port_number(start.trim())?, port_number(end.trim())?);
        if start > end {
            bail!("port range start must not exceed end");
        }
        Ok((start, end))
    } else {
        let port = port_number(text.trim())?;
        Ok((port, port))
    }
}

fn normalize_ports(text: &str) -> Result<String> {
    let text = text.trim();
    if text == "*" {
        return Ok(text.to_string());
    }
    if text.len() > 4096 {
        bail!("ports specification is too long");
    }
    let mut items = Vec::new();
    for item in text.split(',') {
        if items.len() == 32 {
            bail!("ports must contain at most 32 items");
        }
        let (start, end) = port_range(item)?;
        items.push(if item.contains('-') {
            format!("{start}-{end}")
        } else {
            start.to_string()
        });
    }
    Ok(items.join(","))
}

fn normalize_host(host: &str) -> Result<String> {
    let host = host.trim();
    let host = if host.starts_with('[') && host.ends_with(']') {
        let inner = &host[1..host.len() - 1];
        if !matches!(inner.parse::<IpAddr>(), Ok(IpAddr::V6(_))) {
            bail!("bracketed host must be an IPv6 address");
        }
        inner
    } else {
        host
    };
    if host.is_empty()
        || host.len() > 253
        || host.chars().any(|c| c.is_whitespace() || c.is_control())
        || host.contains(['[', ']', '/', '\\', '@', '*', '?', '#', ','])
        || (host.contains(':') && !matches!(host.parse::<IpAddr>(), Ok(IpAddr::V6(_))))
    {
        bail!("hosts must be host names or IP addresses (at most 253 chars, no whitespace)");
    }
    Ok(host.to_ascii_lowercase())
}

fn normalize_hosts(text: &str) -> Result<String> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("loopback") || text == "*" {
        return Ok(text.to_ascii_lowercase());
    }
    if text.len() > 8192 {
        bail!("hosts specification is too long");
    }
    let mut items = Vec::new();
    for host in text.split(',') {
        if items.len() == 16 {
            bail!("hosts must contain at most 16 items");
        }
        items.push(normalize_host(host)?);
    }
    Ok(items.join(","))
}

fn remote_host_port(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    // IPv6 address/port pairs must be bracketed to be unambiguous.
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        return None;
    }
    Some((normalize_host(host).ok()?, port_number(port).ok()?))
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    AutoAllow(String),
    AutoDeny(String),
    Ask,
}

/// Evaluate only the address the serve side actually connects to, and bind
/// verified identity to the transport node id even if a caller misuses it.
pub fn evaluate(
    rules: &[Rule],
    peer_id: &str,
    verified_did: Option<&str>,
    proto: &str,
    remote_addr: &str,
) -> Decision {
    let Some((host, port)) = remote_host_port(remote_addr) else {
        return Decision::Ask;
    };
    let did = verified_did.filter(|did| crate::identity::node_id_for_did(did) == peer_id);
    let mut allow = None;
    for rule in rules {
        let peer_matches = rule.peer == "*"
            || (rule.peer.starts_with("did:key:") && did == Some(rule.peer.as_str()))
            || (!rule.peer.starts_with("did:") && rule.peer == peer_id);
        if !peer_matches
            || (rule.proto != "*" && !rule.proto.eq_ignore_ascii_case(proto))
            || (rule.ports != "*"
                && !rule.ports.split(',').any(|item| {
                    port_range(item).is_ok_and(|(start, end)| (start..=end).contains(&port))
                }))
            || !(rule.hosts == "*"
                || (rule.hosts == "loopback" && is_loopback(&host))
                || (rule.hosts != "loopback"
                    && rule
                        .hosts
                        .split(',')
                        .any(|item| item.eq_ignore_ascii_case(&host))))
        {
            continue;
        }
        if rule.action == Action::Deny {
            return Decision::AutoDeny(rule.id.clone());
        }
        if did.is_some() && allow.is_none() {
            allow = Some(rule.id.clone());
        }
    }
    allow.map_or(Decision::Ask, Decision::AutoAllow)
}

#[derive(Debug)]
pub struct RuleStore {
    path: PathBuf,
    /// Serialize edits and disk writes, publishing a change only after the
    /// private atomic write succeeds so an error never installs a new grant.
    rules: Mutex<Vec<Rule>>,
}

impl RuleStore {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let rules = match tokio::fs::File::open(&path).await {
            Ok(file) => {
                let mut text = Vec::new();
                file.take(MAX_FILE_BYTES + 1).read_to_end(&mut text).await?;
                let parsed = (|| -> Result<Vec<Rule>> {
                    if text.len() as u64 > MAX_FILE_BYTES {
                        bail!("proposal rules file is too large");
                    }
                    let mut rules: Vec<Rule> = serde_json::from_slice(&text)?;
                    if rules.len() > MAX_RULES {
                        bail!("at most {MAX_RULES} proposal rules are allowed");
                    }
                    let mut ids = HashSet::new();
                    for rule in &mut rules {
                        if rule.id.len() != 10
                            || !rule.id.starts_with("r-")
                            || !rule.id[2..].bytes().all(|c| c.is_ascii_hexdigit())
                            || !ids.insert(rule.id.clone())
                        {
                            bail!("invalid or duplicate proposal rule id");
                        }
                        rule.normalize()?;
                    }
                    sort_rules(&mut rules);
                    Ok(rules)
                })();
                match parsed {
                    Ok(rules) => rules,
                    Err(error) => {
                        let mut name = path.file_name().unwrap_or_default().to_os_string();
                        name.push(format!(
                            ".corrupt-{}-{:08x}",
                            now_ms(),
                            rand::random::<u32>()
                        ));
                        let backup = path.with_file_name(name);
                        tracing::warn!(%error, path = %path.display(), backup = %backup.display(), "moving corrupt proposal rules aside and starting empty");
                        tokio::fs::rename(&path, &backup).await?;
                        Vec::new()
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            rules: Mutex::new(rules),
        })
    }

    pub async fn list(&self) -> Vec<Rule> {
        self.rules.lock().await.clone()
    }

    async fn persist(&self, rules: &[Rule]) -> Result<()> {
        let text = serde_json::to_vec_pretty(rules)?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || crate::statefile::write_private(&path, &text))
            .await??;
        Ok(())
    }

    pub async fn add(&self, patch: RulePatch) -> Result<Rule> {
        if patch.action.is_none() {
            bail!("action is required");
        }
        let mut rules = self.rules.lock().await;
        if rules.len() >= MAX_RULES {
            bail!("at most {MAX_RULES} proposal rules are allowed");
        }
        let id = loop {
            let id = format!("r-{:08x}", rand::random::<u32>());
            if !rules.iter().any(|rule| rule.id == id) {
                break id;
            }
        };
        let mut rule = Rule {
            id,
            action: Action::Deny,
            peer: "*".into(),
            proto: "*".into(),
            ports: "*".into(),
            hosts: "loopback".into(),
            note: String::new(),
            created_ms: now_ms(),
        };
        rule.apply(patch)?;
        let mut next = rules.clone();
        next.push(rule.clone());
        sort_rules(&mut next);
        self.persist(&next).await?;
        *rules = next;
        Ok(rule)
    }

    pub async fn update(&self, id: &str, patch: RulePatch) -> Result<Rule> {
        let mut rules = self.rules.lock().await;
        let mut next = rules.clone();
        let rule = next
            .iter_mut()
            .find(|rule| rule.id == id)
            .context("proposal rule not found")?;
        rule.apply(patch)?;
        let rule = rule.clone();
        self.persist(&next).await?;
        *rules = next;
        Ok(rule)
    }

    pub async fn remove(&self, id: &str) -> Result<bool> {
        let mut rules = self.rules.lock().await;
        let mut next = rules.clone();
        next.retain(|rule| rule.id != id);
        if next.len() == rules.len() {
            return Ok(false);
        }
        self.persist(&next).await?;
        *rules = next;
        Ok(true)
    }
}

fn sort_rules(rules: &mut [Rule]) {
    rules.sort_by(|a, b| {
        a.created_ms
            .cmp(&b.created_ms)
            .then_with(|| a.id.cmp(&b.id))
    });
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_millis() as u64)
}

pub fn default_proposal_rules_path() -> PathBuf {
    super::auth::default_trust_store_path().with_file_name("proposal-rules.json")
}

static STORE: OnceCell<Arc<RuleStore>> = OnceCell::const_new();

pub async fn store() -> Result<&'static Arc<RuleStore>> {
    STORE
        .get_or_try_init(|| async {
            Ok(Arc::new(
                RuleStore::load(default_proposal_rules_path()).await?,
            ))
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn patch(value: serde_json::Value) -> RulePatch {
        serde_json::from_value(value).unwrap()
    }

    fn rule(action: Action) -> Rule {
        Rule {
            id: "r-12345678".into(),
            action,
            peer: "*".into(),
            proto: "*".into(),
            ports: "*".into(),
            hosts: "loopback".into(),
            note: String::new(),
            created_ms: 1,
        }
    }

    fn path() -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "mistl-proposal-rules-{:016x}",
                rand::random::<u64>()
            ))
            .join("proposal-rules.json")
    }

    #[test]
    fn normalizes_fields_and_keeps_did_case() {
        let mut r = rule(Action::Deny);
        r.apply(patch(json!({
            "action": " allow ", "peer": " ABCDEF0123456789 ",
            "proto": " TCP ", "ports": " 22, 80, 8000 - 8100 ",
            "hosts": " LOCALHOST, [::1], Example.COM ", "note": " hello "
        })))
        .unwrap();
        assert_eq!(r.action, Action::Allow);
        assert_eq!(r.peer, "abcdef0123456789");
        assert_eq!(r.proto, "tcp");
        assert_eq!(r.ports, "22,80,8000-8100");
        assert_eq!(r.hosts, "localhost,::1,example.com");
        assert_eq!(r.note, "hello");
        let did = crate::identity::for_test().did().to_string();
        r.apply(patch(json!({"peer": format!(" {did} ")}))).unwrap();
        assert_eq!(r.peer, did);
    }

    #[test]
    fn rejects_invalid_fields_and_item_limits() {
        for value in [
            json!({"action":"accept"}),
            json!({"peer":"short"}),
            json!({"peer":"did:key:z"}),
            json!({"peer":"did:key:z abc"}),
            json!({"peer":format!("did:key:z{}", "a".repeat(256))}),
            json!({"proto":"http"}),
            json!({"ports":"0"}),
            json!({"ports":"65536"}),
            json!({"ports":"80-22"}),
            json!({"ports":"22,,80"}),
            json!({"ports":"*,22"}),
            json!({"ports":"+22"}),
            json!({"ports":"22-23-24"}),
            json!({"ports":vec!["22";33].join(",")}),
            json!({"hosts":""}),
            json!({"hosts":"host name"}),
            json!({"hosts":"host:22"}),
            json!({"hosts":"[localhost]"}),
            json!({"hosts":"foo,*"}),
            json!({"hosts":"a".repeat(254)}),
            json!({"hosts":vec!["localhost";17].join(",")}),
            json!({"note":"a".repeat(201)}),
        ] {
            assert!(
                rule(Action::Deny).apply(patch(value.clone())).is_err(),
                "{value}"
            );
        }
        let mut r = rule(Action::Deny);
        r.apply(patch(json!({"ports":vec!["22";32].join(","), "hosts":vec!["host";16].join(","), "note":"日".repeat(200)}))).unwrap();
    }

    #[test]
    fn loopback_is_only_localhost_ipv4_loopback_and_ipv6_loopback() {
        let rules = [rule(Action::Deny)];
        for addr in [
            "localhost:22",
            "LOCALHOST:22",
            "127.0.0.1:22",
            "127.2.3.4:22",
            "[::1]:22",
            "[0:0:0:0:0:0:0:1]:22",
        ] {
            assert!(
                matches!(
                    evaluate(&rules, "peer", None, "tcp", addr),
                    Decision::AutoDeny(_)
                ),
                "{addr}"
            );
        }
        for addr in [
            "192.168.0.1:22",
            "example.com:22",
            "localhost.example:22",
            "[::ffff:127.0.0.1]:22",
            "[::]:22",
            "::1:22",
            "127.0.0.1:0",
            "localhost:65536",
            "localhost",
            "localhost:http",
        ] {
            assert_eq!(
                evaluate(&rules, "peer", None, "tcp", addr),
                Decision::Ask,
                "{addr}"
            );
        }
    }

    #[test]
    fn matches_protocol_ports_and_exact_case_insensitive_hosts() {
        let mut r = rule(Action::Deny);
        r.apply(patch(json!({"peer":"abcdef0123456789", "proto":"udp", "ports":"22,80,8000-8100", "hosts":"Example.COM,[::1]"}))).unwrap();
        for addr in [
            "example.com:22",
            "EXAMPLE.COM:80",
            "example.com:8000",
            "example.com:8100",
            "[::1]:8001",
        ] {
            assert!(
                matches!(
                    evaluate(&[r.clone()], "abcdef0123456789", None, "udp", addr),
                    Decision::AutoDeny(_)
                ),
                "{addr}"
            );
        }
        for (peer, proto, addr) in [
            ("other", "udp", "example.com:22"),
            ("abcdef0123456789", "tcp", "example.com:22"),
            ("abcdef0123456789", "udp", "example.com:7999"),
            ("abcdef0123456789", "udp", "example.com:8101"),
            ("abcdef0123456789", "udp", "other.example.com:22"),
        ] {
            assert_eq!(
                evaluate(&[r.clone()], peer, None, proto, addr),
                Decision::Ask
            );
        }
    }

    #[test]
    fn wildcard_host_reaches_lan_but_allow_always_requires_bound_verified_did() {
        let identity = crate::identity::for_test();
        let peer = identity.node_id();
        let did = identity.did();
        let mut r = rule(Action::Allow);
        r.hosts = "*".into();
        assert_eq!(
            evaluate(&[r.clone()], &peer, Some(did), "tcp", "192.168.1.2:22"),
            Decision::AutoAllow(r.id.clone())
        );
        assert_eq!(
            evaluate(&[r.clone()], &peer, None, "tcp", "192.168.1.2:22"),
            Decision::Ask
        );
        assert_eq!(
            evaluate(&[r.clone()], "spoofed", Some(did), "tcp", "localhost:22"),
            Decision::Ask
        );
        r.peer = did.to_string();
        assert!(matches!(
            evaluate(&[r.clone()], &peer, Some(did), "tcp", "localhost:22"),
            Decision::AutoAllow(_)
        ));
        r.action = Action::Deny;
        assert_eq!(
            evaluate(&[r.clone()], &peer, None, "tcp", "localhost:22"),
            Decision::Ask
        );
        assert_eq!(
            evaluate(&[r.clone()], did, None, "tcp", "localhost:22"),
            Decision::Ask
        );
        r.peer = peer.clone();
        assert!(matches!(
            evaluate(&[r], &peer, None, "tcp", "localhost:22"),
            Decision::AutoDeny(_)
        ));
    }

    #[test]
    fn deny_wins_independent_of_order() {
        let identity = crate::identity::for_test();
        let allow = rule(Action::Allow);
        let mut deny = rule(Action::Deny);
        deny.id = "r-87654321".into();
        for rules in [vec![allow.clone(), deny.clone()], vec![deny.clone(), allow]] {
            assert_eq!(
                evaluate(
                    &rules,
                    &identity.node_id(),
                    Some(identity.did()),
                    "tcp",
                    "localhost:22"
                ),
                Decision::AutoDeny(deny.id.clone())
            );
        }
    }

    #[tokio::test]
    async fn store_defaults_update_remove_and_reload() {
        let path = path();
        let store = RuleStore::load(&path).await.unwrap();
        assert!(store.list().await.is_empty());
        assert!(store.add(RulePatch::default()).await.is_err());
        let original = store.add(patch(json!({"action":"allow"}))).await.unwrap();
        assert_eq!(
            (
                &original.peer[..],
                &original.proto[..],
                &original.ports[..],
                &original.hosts[..],
                &original.note[..]
            ),
            ("*", "*", "*", "loopback", "")
        );
        let updated = store
            .update(
                &original.id,
                patch(json!({"ports":"22, 80", "note":" SSH "})),
            )
            .await
            .unwrap();
        assert_eq!(updated.id, original.id);
        assert_eq!(updated.created_ms, original.created_ms);
        assert_eq!(updated.action, Action::Allow);
        assert_eq!(updated.ports, "22,80");
        assert_eq!(updated.note, "SSH");
        assert!(
            store
                .update(&original.id, patch(json!({"hosts":""})))
                .await
                .is_err()
        );
        assert_eq!(store.list().await, [updated.clone()]);
        let reload = RuleStore::load(&path).await.unwrap();
        assert_eq!(reload.list().await, [updated]);
        assert!(reload.remove(&original.id).await.unwrap());
        assert!(!reload.remove(&original.id).await.unwrap());
        assert!(
            reload
                .update(&original.id, RulePatch::default())
                .await
                .is_err()
        );
        assert!(
            RuleStore::load(&path)
                .await
                .unwrap()
                .list()
                .await
                .is_empty()
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn store_rejects_over_capacity_and_sorts_by_creation_then_id() {
        let path = path();
        let mut rules = (0..MAX_RULES)
            .map(|n| {
                let mut r = rule(Action::Deny);
                r.id = format!("r-{n:08x}");
                r.created_ms = (n % 2) as u64;
                r
            })
            .collect::<Vec<_>>();
        rules.reverse();
        crate::statefile::write_private(&path, &serde_json::to_vec(&rules).unwrap()).unwrap();
        let store = RuleStore::load(&path).await.unwrap();
        assert!(store.add(patch(json!({"action":"deny"}))).await.is_err());
        sort_rules(&mut rules);
        assert_eq!(store.list().await, rules);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn store_moves_corrupt_invalid_and_duplicate_rules_aside() {
        for text in [
            b"bad json".to_vec(),
            serde_json::to_vec(&vec![rule(Action::Deny); 2]).unwrap(),
            serde_json::to_vec(&{
                let mut r = rule(Action::Allow);
                r.ports = "0".into();
                vec![r]
            })
            .unwrap(),
            serde_json::to_vec(&vec![rule(Action::Deny); MAX_RULES + 1]).unwrap(),
        ] {
            let path = path();
            crate::statefile::write_private(&path, &text).unwrap();
            assert!(
                RuleStore::load(&path)
                    .await
                    .unwrap()
                    .list()
                    .await
                    .is_empty()
            );
            assert!(!path.exists());
            let backups = std::fs::read_dir(path.parent().unwrap())
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(backups.len(), 1);
            assert_eq!(
                std::fs::read(backups[0].as_ref().unwrap().path()).unwrap(),
                text
            );
            std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
        }
    }

    #[tokio::test]
    async fn failed_persist_does_not_install_rules() {
        let path = path();
        let store = RuleStore::load(&path).await.unwrap();
        // Occupy the would-be parent directory with a regular file.
        std::fs::write(path.parent().unwrap(), b"blocked").unwrap();
        assert!(store.add(patch(json!({"action":"allow"}))).await.is_err());
        assert!(store.list().await.is_empty());
        std::fs::remove_file(path.parent().unwrap()).unwrap();
    }
}
